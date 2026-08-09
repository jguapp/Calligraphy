// Package httpcallback delivers a job's payload to an HTTP endpoint --
// the handler that turns Caligraphy into generic infrastructure.
//
// This is the Cloud Tasks / SQS-to-webhook model: the submitting
// application keeps its business logic (Booklet keeps Readability, JSDOM,
// its TTS pipeline); Caligraphy supplies what that application's own comments
// say it lacks -- durability, retries with backoff, and a dead-letter
// queue -- by POSTing the payload back when it's time to do the work.
//
// Deliveries are signed the way Booklet already signs its outgoing
// webhooks (HMAC-SHA256 over the body, hex in a header), with a timestamp
// folded into the MAC so a captured request can't be replayed usefully
// later. The receiver recomputes and compares.
package httpcallback

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jguapp/caligraphy/internal/handler"
	"github.com/jguapp/caligraphy/internal/job"
)

const Type = "http.callback"

// maxResponseSnippet bounds how much of the receiver's response is kept as
// the job result -- enough to debug with, too little to hoard.
const maxResponseSnippet = 1024

type payload struct {
	URL  string          `json:"url"`
	Body json.RawMessage `json:"body"`
	// Event names what this delivery is, carried in X-Caligraphy-Event so one
	// endpoint can route several kinds.
	Event string `json:"event"`
	// TimeoutMs bounds this delivery attempt (default 10s, cap 30s).
	TimeoutMs int `json:"timeoutMs"`
}

type result struct {
	Status      int    `json:"status"`
	BodySnippet string `json:"bodySnippet,omitempty"`
	DeliveredAt string `json:"deliveredAt"`
}

// Handler is configured once at worker startup.
type Handler struct {
	// Secret signs deliveries. Empty disables signing (the headers are
	// simply omitted) -- acceptable in dev, logged loudly by the worker.
	Secret string
	// AllowedHosts, when non-empty, restricts targets. Deliberately NOT a
	// private-IP block: callback URLs come from an authenticated service
	// holding an API token, and the common target IS a private address
	// (the submitting app on the same compose network). Booklet's
	// user-supplied webhook URLs need SSRF hardening; a trusted service's
	// callback URLs need an allowlist at most. Different threat, different
	// control.
	AllowedHosts []string
	Client       *http.Client
}

func New(secret string, allowedHosts []string) handler.Registration {
	h := &Handler{
		Secret:       secret,
		AllowedHosts: allowedHosts,
		Client: &http.Client{
			// Redirects are refused, same reasoning as Booklet's webhook
			// sender: replaying a signed POST to a redirect target would
			// have Caligraphy vouch for a body to a host nobody registered. A
			// callback endpoint is a fixed URL; a redirect is a
			// misconfiguration and gets reported as one.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	return handler.Registration{
		Type:    Type,
		Handler: h,
		Options: handler.Options{ExecTimeout: 45 * time.Second},
	}
}

// Sign computes the signature for (timestamp, body). Exported so the
// TypeScript client's verification and these tests agree on one
// implementation's test vectors.
func Sign(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", ts)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func (h *Handler) Handle(ctx context.Context, j *job.Job) (json.RawMessage, error) {
	var p payload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return nil, job.NonRetryable(fmt.Errorf("httpcallback: bad payload: %w", err))
	}
	if !strings.HasPrefix(p.URL, "http://") && !strings.HasPrefix(p.URL, "https://") {
		return nil, job.NonRetryable(fmt.Errorf("httpcallback: url must be http(s), got %q", p.URL))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, strings.NewReader(string(p.Body)))
	if err != nil {
		return nil, job.NonRetryable(fmt.Errorf("httpcallback: building request: %w", err))
	}
	if len(h.AllowedHosts) > 0 && !hostAllowed(req.URL.Hostname(), h.AllowedHosts) {
		return nil, job.NonRetryable(fmt.Errorf("httpcallback: host %q not in allowlist", req.URL.Hostname()))
	}

	timeout := 10 * time.Second
	if p.TimeoutMs > 0 {
		timeout = time.Duration(p.TimeoutMs) * time.Millisecond
	}
	if timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req = req.WithContext(tctx)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "caligraphy/1.0")
	if p.Event != "" {
		req.Header.Set("X-Caligraphy-Event", p.Event)
	}
	req.Header.Set("X-Caligraphy-Job-Id", j.ID)
	req.Header.Set("X-Caligraphy-Attempt", strconv.Itoa(j.AttemptCount))
	if h.Secret != "" {
		ts := time.Now().Unix()
		req.Header.Set("X-Caligraphy-Timestamp", strconv.FormatInt(ts, 10))
		req.Header.Set("X-Caligraphy-Signature", "v1="+Sign(h.Secret, ts, p.Body))
	}

	resp, err := h.Client.Do(req)
	if err != nil {
		// Network-level failure: unreachable, reset, timeout. The
		// canonical transient error -- retry (the default classification).
		return nil, fmt.Errorf("httpcallback: delivering to %s: %w", p.URL, err)
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseSnippet))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		out, err := json.Marshal(result{
			Status:      resp.StatusCode,
			BodySnippet: string(snippet),
			DeliveredAt: time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			return nil, fmt.Errorf("httpcallback: encoding result: %w", err)
		}
		return out, nil

	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return nil, job.NonRetryable(fmt.Errorf(
			"httpcallback: endpoint redirected (%d); callback URLs must be final destinations", resp.StatusCode))

	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests:
		// The two 4xx codes that mean "again later" rather than "never".
		return nil, fmt.Errorf("httpcallback: receiver says try later (%d)", resp.StatusCode)

	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// The payload is wrong for this receiver. Retrying a 400 five
		// times is just five 400s -- permanent.
		return nil, job.NonRetryable(fmt.Errorf(
			"httpcallback: receiver rejected delivery (%d): %s", resp.StatusCode, snippet))

	default:
		// 5xx: the receiver is having a bad time. Classic retry.
		return nil, fmt.Errorf("httpcallback: receiver error (%d): %s", resp.StatusCode, snippet)
	}
}

func hostAllowed(host string, allowed []string) bool {
	for _, a := range allowed {
		if strings.EqualFold(host, a) {
			return true
		}
	}
	return false
}
