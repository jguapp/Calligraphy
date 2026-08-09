package control

import (
	"context"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/jguapp/forge/internal/control/pb"
)

// PoolControls is what the client needs from the worker's pool -- defined
// here (the consumer) so control doesn't import worker.
type PoolControls interface {
	Active() int
	Target() int
	Processed() uint64
	Paused() bool
	SetTarget(int)
	SetPaused(bool)
}

// Client maintains the worker's control stream: hello on connect, stats
// every interval, commands dispatched as they arrive, reconnect with
// backoff forever. The worker is fully functional without it (jobs flow
// through Redis) -- losing the control plane costs operability, never
// throughput, and the reconnect loop reflects that calm.
type Client struct {
	Addr     string
	WorkerID string
	Types    []string
	Pool     PoolControls
	Log      *slog.Logger
	// OnDrain triggers the worker's normal graceful shutdown (main wires
	// it to the run-context cancel).
	OnDrain func()

	StatsEvery time.Duration
}

func (c *Client) Run(ctx context.Context) {
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.StatsEvery == 0 {
		c.StatsEvery = 5 * time.Second
	}
	backoff := time.Second
	for ctx.Err() == nil {
		if err := c.connectOnce(ctx); err != nil && ctx.Err() == nil {
			c.Log.Warn("control: stream lost; reconnecting", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func (c *Client) connectOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(c.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := pb.NewControlClient(conn).WorkerStream(ctx)
	if err != nil {
		return err
	}
	host, _ := os.Hostname()
	err = stream.Send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{Hello: &pb.WorkerHello{
		WorkerId: c.WorkerID, Hostname: host, Pid: int32(os.Getpid()),
		Concurrency: int32(c.Pool.Target()), Types: c.Types,
	}}})
	if err != nil {
		return err
	}
	c.Log.Info("control: connected", "addr", c.Addr)

	// Stats sender.
	sendErr := make(chan error, 1)
	go func() {
		t := time.NewTicker(c.StatsEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			target := c.Pool.Target()
			util := 0.0
			if target > 0 {
				util = float64(c.Pool.Active()) / float64(target)
			}
			err := stream.Send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Stats{Stats: &pb.WorkerStats{
				Active: int32(c.Pool.Active()), Target: int32(target),
				Processed: c.Pool.Processed(), Utilization: util, Paused: c.Pool.Paused(),
			}}})
			if err != nil {
				sendErr <- err
				return
			}
		}
	}()

	// Command receiver.
	for {
		select {
		case err := <-sendErr:
			return err
		default:
		}
		cmd, err := stream.Recv()
		if err != nil {
			return err
		}
		c.dispatch(stream, cmd)
	}
}

func (c *Client) dispatch(stream pb.Control_WorkerStreamClient, cmd *pb.Command) {
	ack := func(detail string) {
		stream.Send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Ack{ //nolint:errcheck
			Ack: &pb.CommandAck{CommandId: cmd.CommandId, Detail: detail},
		}})
	}
	switch v := cmd.Cmd.(type) {
	case *pb.Command_Drain:
		c.Log.Info("control: drain commanded")
		ack("draining")
		if c.OnDrain != nil {
			c.OnDrain()
		}
	case *pb.Command_Pause:
		c.Pool.SetPaused(true)
		ack("paused")
	case *pb.Command_Resume:
		c.Pool.SetPaused(false)
		ack("resumed")
	case *pb.Command_SetConcurrency:
		c.Pool.SetTarget(int(v.SetConcurrency.Target))
		c.Log.Info("control: concurrency set", "target", v.SetConcurrency.Target)
		ack("concurrency updated")
	}
}
