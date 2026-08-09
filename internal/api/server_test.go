package api

// Integration tests: a real Server over real Postgres + Redis, driven
// through httptest. Env-gated like every other integration suite.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jguapp/caligraphy/internal/job"
	"github.com/jguapp/caligraphy/internal/metrics"
	"github.com/jguapp/caligraphy/internal/queue"
	"github.com/jguapp/caligraphy/internal/store"
)

const testToken = "cg_test_token_abc123"

func newTestServer(t *testing.T) (*httptest.Server, *store.Store, *queue.Queue) {
	t.Helper()
	dsn := os.Getenv("CALIGRAPHY_TEST_DATABASE_URL")
	addr := os.Getenv("CALIGRAPHY_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("CALIGRAPHY_TEST_DATABASE_URL / CALIGRAPHY_TEST_REDIS_ADDR not set")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.TruncateForTest(ctx); err != nil {
		t.Fatal(err)
	}
	q, err := queue.New(ctx, queue.Config{Addr: addr, Prefix: "caligraphytest", Queue: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.FlushForTest(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(st, q, metrics.New(), Config{
		Tokens:          []string{testToken},
		KnownTypes:      []string{"article.analysis", "bench.sleep", "http.callback"},
		MaxPayloadBytes: 64 * 1024,
		Version:         "test",
	}, nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); st.Close(); q.Close() })
	return ts, st, q
}

func call(t *testing.T, ts *httptest.Server, method, path, body string, auth bool) (*http.Response, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body == "" {
		rdr = bytes.NewReader(nil)
	} else {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, ts.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+testToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	return resp, m
}

func TestAuthIsRequired(t *testing.T) {
	ts, _, _ := newTestServer(t)

	resp, _ := call(t, ts, "POST", "/api/v1/jobs", `{"type":"bench.sleep"}`, false)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no token: %d, want 401", resp.StatusCode)
	}

	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/queues/depths", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp2, _ := http.DefaultClient.Do(req)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong token: %d, want 401", resp2.StatusCode)
	}

	// Probes and metrics stay open: a probe cannot log in.
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		resp, _ := call(t, ts, "GET", path, "", false)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: %d, want 200", path, resp.StatusCode)
		}
	}
}

func TestSubmitLifecycle(t *testing.T) {
	ts, st, q := newTestServer(t)
	ctx := context.Background()

	resp, body := call(t, ts, "POST", "/api/v1/jobs",
		`{"type":"bench.sleep","payload":{"ms":50},"options":{"priority":"high","maxAttempts":3}}`, true)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("submit: %d %v", resp.StatusCode, body)
	}
	id, _ := body["id"].(string)
	if id == "" || body["status"] != "PENDING" || body["priority"] != "high" {
		t.Fatalf("submit body: %v", body)
	}

	// The envelope really is on the wire, and the handoff receipt stored.
	d, _ := q.Depths(ctx)
	if d.Ready[job.PriorityHigh] != 1 {
		t.Errorf("depths after submit: %+v", d)
	}

	// Status while pending: result endpoint says 202.
	resp, _ = call(t, ts, "GET", "/api/v1/jobs/"+id+"/result", "", true)
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("result while pending: %d, want 202", resp.StatusCode)
	}

	// Simulate the worker completing it (the worker suite owns the real
	// path; this suite owns the API's view of it).
	claim, ok, _ := st.ClaimJob(ctx, id, "w1", 30*time.Second)
	if !ok {
		t.Fatal("claim failed")
	}
	st.CompleteJob(ctx, id, claim.Epoch, json.RawMessage(`{"answer":42}`))

	resp, body = call(t, ts, "GET", "/api/v1/jobs/"+id+"/result", "", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("result after completion: %d", resp.StatusCode)
	}
	if body["status"] != "COMPLETED" {
		t.Errorf("result body: %v", body)
	}
	if res, _ := body["result"].(map[string]any); res["answer"] != float64(42) {
		t.Errorf("result payload: %v", body["result"])
	}
}

func TestSubmitValidation(t *testing.T) {
	ts, _, _ := newTestServer(t)
	tests := []struct {
		name, body string
		wantStatus int
		wantCode   string
	}{
		{"not json", `{nope`, 400, "invalid_json"},
		{"unknown type", `{"type":"no.such.handler"}`, 400, "unknown_type"},
		{"bad priority", `{"type":"bench.sleep","options":{"priority":"urgent"}}`, 400, "invalid_job"},
		{"both schedules", `{"type":"bench.sleep","options":{"delaySeconds":5,"scheduledAt":"2030-01-01T00:00:00Z"}}`, 400, "invalid_schedule"},
		{"payload too large", fmt.Sprintf(`{"type":"bench.sleep","payload":{"x":%q}}`, strings.Repeat("a", 70*1024)), 413, "payload_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, body := call(t, ts, "POST", "/api/v1/jobs", tt.body, true)
			if resp.StatusCode != tt.wantStatus || body["error"] != tt.wantCode {
				t.Errorf("got %d %v, want %d %s", resp.StatusCode, body, tt.wantStatus, tt.wantCode)
			}
		})
	}
	// The unknown-type message must name the known types -- an error a
	// human can act on beats a bare 400.
	_, body := call(t, ts, "POST", "/api/v1/jobs", `{"type":"no.such.handler"}`, true)
	if msg, _ := body["message"].(string); !strings.Contains(msg, "article.analysis") {
		t.Errorf("unknown_type message doesn't list known types: %q", body["message"])
	}
}

func TestIdempotentSubmission(t *testing.T) {
	ts, _, _ := newTestServer(t)
	payload := `{"type":"bench.sleep","payload":{"ms":1},"options":{"idempotencyKey":"article-42"}}`

	resp1, body1 := call(t, ts, "POST", "/api/v1/jobs", payload, true)
	resp2, body2 := call(t, ts, "POST", "/api/v1/jobs", payload, true)

	if resp1.StatusCode != 201 || resp2.StatusCode != 200 {
		t.Errorf("statuses = %d, %d; want 201 then 200", resp1.StatusCode, resp2.StatusCode)
	}
	if body1["id"] != body2["id"] {
		t.Errorf("idempotent replay returned a different job: %v vs %v", body1["id"], body2["id"])
	}
}

func TestScheduledSubmissionGoesToDelayedSet(t *testing.T) {
	ts, _, q := newTestServer(t)
	resp, body := call(t, ts, "POST", "/api/v1/jobs",
		`{"type":"bench.sleep","options":{"delaySeconds":60}}`, true)
	if resp.StatusCode != 201 {
		t.Fatalf("submit: %d %v", resp.StatusCode, body)
	}
	d, _ := q.Depths(context.Background())
	if d.Delayed[job.PriorityDefault] != 1 || d.TotalReady() != 0 {
		t.Errorf("depths: %+v, want 1 delayed 0 ready", d)
	}
}

func TestCancelSemantics(t *testing.T) {
	ts, st, _ := newTestServer(t)
	ctx := context.Background()

	// PENDING: cancels outright.
	_, body := call(t, ts, "POST", "/api/v1/jobs", `{"type":"bench.sleep"}`, true)
	id := body["id"].(string)
	resp, body := call(t, ts, "DELETE", "/api/v1/jobs/"+id, "", true)
	if resp.StatusCode != 200 || body["cancellation"] != "done" {
		t.Errorf("cancel pending: %d %v", resp.StatusCode, body)
	}

	// RUNNING: cooperative, 202.
	_, body = call(t, ts, "POST", "/api/v1/jobs", `{"type":"bench.sleep"}`, true)
	id2 := body["id"].(string)
	st.ClaimJob(ctx, id2, "w1", 30*time.Second)
	resp, body = call(t, ts, "DELETE", "/api/v1/jobs/"+id2, "", true)
	if resp.StatusCode != 202 || body["cancellation"] != "requested" {
		t.Errorf("cancel running: %d %v", resp.StatusCode, body)
	}

	// Terminal: 409.
	resp, _ = call(t, ts, "DELETE", "/api/v1/jobs/"+id, "", true)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("cancel cancelled: %d, want 409", resp.StatusCode)
	}
}

func TestDLQRequeueEndpoint(t *testing.T) {
	ts, st, q := newTestServer(t)
	ctx := context.Background()

	_, body := call(t, ts, "POST", "/api/v1/jobs", `{"type":"bench.sleep","options":{"maxAttempts":1}}`, true)
	id := body["id"].(string)
	// Consume the delivery the way a real worker would (claiming through
	// the store alone would leave the original stream entry behind, and
	// the depth assertions below would be counting that ghost).
	deliveries, _ := q.Fetch(ctx, "w1", 10, 0)
	for _, d := range deliveries {
		q.Ack(ctx, d.Priority, d.EntryID)
	}
	claim, _, _ := st.ClaimJob(ctx, id, "w1", 30*time.Second)
	st.DeadLetterJob(ctx, id, claim.Epoch, "died horribly")

	resp, body := call(t, ts, "GET", "/api/v1/dlq", "", true)
	if resp.StatusCode != 200 {
		t.Fatalf("dlq list: %d", resp.StatusCode)
	}
	if jobs, _ := body["jobs"].([]any); len(jobs) != 1 {
		t.Fatalf("dlq list: %v", body)
	}

	resp, _ = call(t, ts, "POST", "/api/v1/dlq/"+id+"/requeue", "", true)
	if resp.StatusCode != 200 {
		t.Fatalf("requeue: %d", resp.StatusCode)
	}
	j, _ := st.GetJob(ctx, id)
	if j.Status != job.StatusPending || j.AttemptCount != 0 {
		t.Errorf("after requeue: %+v", j)
	}
	d, _ := q.Depths(ctx)
	if d.TotalReady() != 1 {
		t.Errorf("depths after requeue: %+v", d)
	}

	// Requeueing something not dead-lettered: 409.
	resp, _ = call(t, ts, "POST", "/api/v1/dlq/"+id+"/requeue", "", true)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("double requeue: %d, want 409", resp.StatusCode)
	}
}

func TestSummaryAndMeta(t *testing.T) {
	ts, _, _ := newTestServer(t)

	call(t, ts, "POST", "/api/v1/jobs", `{"type":"bench.sleep"}`, true)
	resp, body := call(t, ts, "GET", "/api/v1/stats/summary?since="+
		time.Now().Add(-time.Minute).UTC().Format(time.RFC3339), "", true)
	if resp.StatusCode != 200 {
		t.Fatalf("summary: %d", resp.StatusCode)
	}
	if counts, _ := body["counts"].(map[string]any); counts["PENDING"] != float64(1) {
		t.Errorf("summary counts: %v", body)
	}

	resp, body = call(t, ts, "GET", "/api/v1/meta", "", true)
	if resp.StatusCode != 200 || body["numCPU"] == nil {
		t.Errorf("meta: %d %v", resp.StatusCode, body)
	}
}

func TestUnknownJobIs404(t *testing.T) {
	ts, _, _ := newTestServer(t)
	resp, _ := call(t, ts, "GET", "/api/v1/jobs/nope", "", true)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown job: %d, want 404", resp.StatusCode)
	}
}
