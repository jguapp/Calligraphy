package job

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)

func TestNewDefaults(t *testing.T) {
	j, err := New("article.analysis", json.RawMessage(`{"a":1}`), Options{}, now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if j.ID == "" {
		t.Error("no ID assigned")
	}
	if j.Status != StatusPending {
		t.Errorf("status = %s, want PENDING", j.Status)
	}
	if j.Queue != DefaultQueue || j.Priority != PriorityDefault || j.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("defaults not applied: queue=%q prio=%q max=%d", j.Queue, j.Priority, j.MaxAttempts)
	}
	if !j.ScheduledAt.Equal(now) {
		t.Errorf("scheduledAt = %v, want now", j.ScheduledAt)
	}
}

func TestNewValidation(t *testing.T) {
	big := json.RawMessage(`"` + strings.Repeat("x", MaxPayloadBytes) + `"`)
	future := now.Add(time.Hour)

	tests := []struct {
		name    string
		typ     string
		payload json.RawMessage
		opts    Options
		wantErr bool
	}{
		{"valid minimal", "t", nil, Options{}, false},
		{"valid full", "bench.sleep", json.RawMessage(`{}`), Options{
			Queue: "critical", Priority: PriorityHigh, MaxAttempts: 3,
			IdempotencyKey: "k1", ScheduledAt: future,
		}, false},
		{"empty type", "", nil, Options{}, true},
		{"type with space", "a b", nil, Options{}, true},
		{"type with slash", "a/b", nil, Options{}, true},
		{"type leading dot", ".a", nil, Options{}, true},
		{"type too long", strings.Repeat("a", 101), nil, Options{}, true},
		{"payload too large", "t", big, Options{}, true},
		{"payload invalid json", "t", json.RawMessage(`{nope`), Options{}, true},
		{"bad queue", "t", nil, Options{Queue: "no spaces"}, true},
		{"bad priority", "t", nil, Options{Priority: "urgent"}, true},
		{"attempts zero uses default", "t", nil, Options{MaxAttempts: 0}, false},
		{"attempts negative", "t", nil, Options{MaxAttempts: -1}, true},
		{"attempts over ceiling", "t", nil, Options{MaxAttempts: MaxAttemptsCeiling + 1}, true},
		{"idempotency key too long", "t", nil, Options{IdempotencyKey: strings.Repeat("k", 257)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.typ, tt.payload, tt.opts, now)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	j, err := New("bench.cpu", json.RawMessage(`{"iterations":100}`), Options{Priority: PriorityHigh}, now)
	if err != nil {
		t.Fatal(err)
	}
	env := NewEnvelope(j, now)
	s, err := env.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeEnvelope(s)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != j.ID || got.Type != j.Type || got.Priority != PriorityHigh || got.Attempt != 1 {
		t.Errorf("round trip mismatch: %+v", got)
	}
	if string(got.Payload) != `{"iterations":100}` {
		t.Errorf("payload mismatch: %s", got.Payload)
	}
}

func TestDecodeEnvelopeRejectsGarbage(t *testing.T) {
	for _, s := range []string{"", "not json", "{}", `{"id":"x"}`, `{"type":"t"}`} {
		if _, err := DecodeEnvelope(s); err == nil {
			t.Errorf("DecodeEnvelope(%q) accepted, want error", s)
		}
	}
}
