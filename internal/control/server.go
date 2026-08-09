// Package control implements the gRPC control plane: the hub (API side)
// that tracks live worker streams, and the client (worker side) that
// maintains one.
//
// Scope discipline, restated from the proto: jobs never travel here. This
// exists for the two things HTTP polling does badly -- live worker stats
// flowing up without a scrape interval, and operator commands (drain,
// pause, resize) landing NOW instead of at the next poll.
//
// Transport security: the stream is plaintext gRPC, which is an explicit
// deployment assumption, not an oversight -- the control plane rides the
// private compose/k8s network alongside Redis and Postgres, which are
// deployed exactly the same way. Exposing any of the three past that
// boundary is the same mistake; mTLS is the upgrade path if the boundary
// ever moves.
package control

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/jguapp/caligraphy/internal/control/pb"
)

// WorkerView is a snapshot of one connected worker, for the API and
// caligraphyctl.
type WorkerView struct {
	ID          string    `json:"id"`
	Hostname    string    `json:"hostname"`
	PID         int32     `json:"pid"`
	Active      int32     `json:"active"`
	Target      int32     `json:"target"`
	Processed   uint64    `json:"processed"`
	Utilization float64   `json:"utilization"`
	Paused      bool      `json:"paused"`
	Types       []string  `json:"types"`
	ConnectedAt time.Time `json:"connectedAt"`
	LastStatsAt time.Time `json:"lastStatsAt"`
}

type session struct {
	hello       *pb.WorkerHello
	stats       *pb.WorkerStats
	connectedAt time.Time
	lastStatsAt time.Time
	// cmds is buffered so an operator command never blocks the hub on a
	// slow stream; a worker that can't drain 16 commands has bigger
	// problems and gets disconnected by the send error instead.
	cmds chan *pb.Command
	// done severs this session's stream. Closed exactly once (terminate),
	// either by a replacement registering over us or by our own teardown.
	// Without it, an evicted session's stream would linger open and its
	// client would never learn it should reconnect -- which is exactly
	// what the reconnect test caught before this field existed.
	done     chan struct{}
	stopOnce sync.Once
}

func (s *session) terminate() {
	s.stopOnce.Do(func() { close(s.done) })
}

// Hub tracks connected workers and routes commands. It implements the
// api package's ControlHub interface.
type Hub struct {
	log *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session
}

func NewHub(log *slog.Logger) *Hub {
	if log == nil {
		log = slog.Default()
	}
	return &Hub{log: log, sessions: make(map[string]*session)}
}

// NewGRPCServer returns a grpc.Server with the hub's service registered.
func NewGRPCServer(h *Hub) *grpc.Server {
	s := grpc.NewServer()
	pb.RegisterControlServer(s, &controlService{hub: h})
	return s
}

type controlService struct {
	pb.UnimplementedControlServer
	hub *Hub
}

// WorkerStream handles one worker's lifetime connection.
func (c *controlService) WorkerStream(stream pb.Control_WorkerStreamServer) error {
	// The contract: first message is the hello. Anything else is a
	// protocol bug on the client, worth failing loudly.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return fmt.Errorf("control: first message must be WorkerHello")
	}

	sess := &session{
		hello:       hello,
		connectedAt: time.Now().UTC(),
		cmds:        make(chan *pb.Command, 16),
		done:        make(chan struct{}),
	}
	c.hub.register(hello.WorkerId, sess)
	defer c.hub.unregister(hello.WorkerId, sess)
	c.hub.log.Info("control: worker connected", "worker", hello.WorkerId, "host", hello.Hostname)

	// Sender: one goroutine owns stream.Send (gRPC allows one concurrent
	// sender), fed by the session's command channel.
	sendErr := make(chan error, 1)
	go func() {
		for {
			select {
			case cmd := <-sess.cmds:
				if err := stream.Send(cmd); err != nil {
					sendErr <- err
					return
				}
			case <-sess.done:
				return
			}
		}
	}()

	// Receiver: stats and acks until the stream dies.
	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if stats := msg.GetStats(); stats != nil {
				c.hub.updateStats(hello.WorkerId, sess, stats)
			}
			if ack := msg.GetAck(); ack != nil {
				c.hub.log.Info("control: command acked",
					"worker", hello.WorkerId, "command", ack.CommandId, "detail", ack.Detail)
			}
		}
	}()

	select {
	case err := <-recvErr:
		c.hub.log.Info("control: worker disconnected", "worker", hello.WorkerId, "err", err)
		return nil
	case err := <-sendErr:
		c.hub.log.Warn("control: send failed; dropping worker", "worker", hello.WorkerId, "err", err)
		return err
	case <-sess.done:
		// Evicted: a replacement session registered over us (or the hub
		// is tearing down). Closing the stream is what tells the far end.
		return fmt.Errorf("control: session replaced")
	case <-stream.Context().Done():
		return nil
	}
}

func (h *Hub) register(id string, s *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// A reconnect (worker restarted faster than the old stream died)
	// replaces the session and actively severs the old stream.
	if old, ok := h.sessions[id]; ok {
		old.terminate()
	}
	h.sessions[id] = s
}

func (h *Hub) unregister(id string, s *session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s.terminate()
	// Only remove OUR session -- a replacement may already be registered.
	if cur, ok := h.sessions[id]; ok && cur == s {
		delete(h.sessions, id)
	}
}

func (h *Hub) updateStats(id string, s *session, stats *pb.WorkerStats) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s.stats = stats
	s.lastStatsAt = time.Now().UTC()
}

// Workers snapshots every live session.
func (h *Hub) Workers() []WorkerView {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]WorkerView, 0, len(h.sessions))
	for id, s := range h.sessions {
		v := WorkerView{
			ID: id, Hostname: s.hello.Hostname, PID: s.hello.Pid,
			Types: s.hello.Types, ConnectedAt: s.connectedAt, LastStatsAt: s.lastStatsAt,
		}
		if s.stats != nil {
			v.Active, v.Target = s.stats.Active, s.stats.Target
			v.Processed, v.Utilization = s.stats.Processed, s.stats.Utilization
			v.Paused = s.stats.Paused
		}
		out = append(out, v)
	}
	return out
}

func (h *Hub) send(workerID string, cmd *pb.Command) error {
	h.mu.Lock()
	s, ok := h.sessions[workerID]
	h.mu.Unlock()
	if !ok {
		return fmt.Errorf("control: worker %q is not connected", workerID)
	}
	select {
	case s.cmds <- cmd:
		return nil
	default:
		return fmt.Errorf("control: worker %q command queue is full", workerID)
	}
}

func cmdID() string { return fmt.Sprintf("cmd-%d", time.Now().UnixNano()) }

func (h *Hub) Drain(workerID string) error {
	return h.send(workerID, &pb.Command{CommandId: cmdID(), Cmd: &pb.Command_Drain{Drain: &pb.Drain{}}})
}

func (h *Hub) Pause(workerID string) error {
	return h.send(workerID, &pb.Command{CommandId: cmdID(), Cmd: &pb.Command_Pause{Pause: &pb.Pause{}}})
}

func (h *Hub) Resume(workerID string) error {
	return h.send(workerID, &pb.Command{CommandId: cmdID(), Cmd: &pb.Command_Resume{Resume: &pb.Resume{}}})
}

func (h *Hub) SetConcurrency(workerID string, target int) error {
	if target < 1 {
		return fmt.Errorf("control: target must be >= 1")
	}
	return h.send(workerID, &pb.Command{
		CommandId: cmdID(),
		Cmd:       &pb.Command_SetConcurrency{SetConcurrency: &pb.SetConcurrency{Target: int32(target)}},
	})
}
