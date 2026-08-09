package httpcallback

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/jguapp/caligraphy/internal/job"
)

func newJob(payload string) *job.Job {
	return &job.Job{ID: "j1", Type: Type, AttemptCount: 1, Payload: json.RawMessage(payload)}
}

func handlerFor(t *testing.T, secret string, hosts []string) *Handler {
	t.Helper()
	reg := New(secret, hosts)
	return reg.Handler.(*Handler)
}

func TestSuccessfulDeliveryIsSignedAndVerifiable(t *testing.T) {
	var got struct {
		body    []byte
		sig, ts string
		event   string
		jobID   string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.body = make([]byte, r.ContentLength)
		r.Body.Read(got.body)
		got.sig = r.Header.Get("X-Caligraphy-Signature")
		got.ts = r.Header.Get("X-Caligraphy-Timestamp")
		got.event = r.Header.Get("X-Caligraphy-Event")
		got.jobID = r.Header.Get("X-Caligraphy-Job-Id")
		w.WriteHeader(200)
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer srv.Close()

	h := handlerFor(t, "shh", nil)
	out, err := h.Handle(context.Background(),
		newJob(`{"url":"`+srv.URL+`","body":{"articleId":"a1"},"event":"article.analyzed"}`))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}

	var res result
	json.Unmarshal(out, &res)
	if res.Status != 200 || res.BodySnippet != `{"ok":true}` {
		t.Errorf("result = %+v", res)
	}
	if got.event != "article.analyzed" || got.jobID != "j1" {
		t.Errorf("headers: %+v", got)
	}

	// The receiver-side verification recipe from the docs must accept the
	// signature the handler produced.
	ts, _ := strconv.ParseInt(got.ts, 10, 64)
	mac := hmac.New(sha256.New, []byte("shh"))
	fmt.Fprintf(mac, "%d.", ts)
	mac.Write(got.body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))
	if got.sig != want {
		t.Errorf("signature mismatch:\n  got  %s\n  want %s", got.sig, want)
	}
}

func TestStatusClassification(t *testing.T) {
	tests := []struct {
		status        int
		wantRetryable bool
	}{
		{500, true},
		{503, true},
		{429, true}, // try later
		{408, true}, // try later
		{400, false},
		{404, false},
		{422, false},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
			}))
			defer srv.Close()
			h := handlerFor(t, "", nil)
			_, err := h.Handle(context.Background(), newJob(`{"url":"`+srv.URL+`","body":{}}`))
			if err == nil {
				t.Fatal("no error for non-2xx")
			}
			if gotRetryable := !job.IsNonRetryable(err); gotRetryable != tt.wantRetryable {
				t.Errorf("status %d: retryable = %v, want %v (%v)", tt.status, gotRetryable, tt.wantRetryable, err)
			}
		})
	}
}

func TestRedirectIsRefusedPermanently(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://elsewhere.example/", http.StatusFound)
	}))
	defer srv.Close()
	h := handlerFor(t, "s", nil)
	_, err := h.Handle(context.Background(), newJob(`{"url":"`+srv.URL+`","body":{}}`))
	if !job.IsNonRetryable(err) || !strings.Contains(err.Error(), "redirect") {
		t.Errorf("redirect error = %v, want non-retryable redirect refusal", err)
	}
}

func TestNetworkFailureIsRetryable(t *testing.T) {
	// A closed port: connection refused.
	h := handlerFor(t, "", nil)
	_, err := h.Handle(context.Background(), newJob(`{"url":"http://127.0.0.1:1","body":{}}`))
	if err == nil || job.IsNonRetryable(err) {
		t.Errorf("network failure = %v, want retryable", err)
	}
}

func TestAllowlist(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	// Not on the list: refused permanently, and the request never leaves.
	h := handlerFor(t, "", []string{"booklet-api"})
	_, err := h.Handle(context.Background(), newJob(`{"url":"`+srv.URL+`","body":{}}`))
	if !job.IsNonRetryable(err) || !strings.Contains(err.Error(), "allowlist") {
		t.Errorf("off-list delivery = %v, want allowlist refusal", err)
	}

	// On the list (by hostname): delivered.
	h = handlerFor(t, "", []string{"booklet-api", u.Hostname()})
	if _, err := h.Handle(context.Background(), newJob(`{"url":"`+srv.URL+`","body":{}}`)); err != nil {
		t.Errorf("allowlisted delivery failed: %v", err)
	}
}

func TestBadPayloads(t *testing.T) {
	h := handlerFor(t, "", nil)
	for _, p := range []string{`{oops`, `{"url":"ftp://x","body":{}}`, `{"url":"","body":{}}`} {
		if _, err := h.Handle(context.Background(), newJob(p)); !job.IsNonRetryable(err) {
			t.Errorf("payload %q: err = %v, want NonRetryable", p, err)
		}
	}
}
