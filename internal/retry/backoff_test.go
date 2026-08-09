package retry

import (
	"testing"
	"time"
)

func TestDelayCeilings(t *testing.T) {
	// rng pinned to 1.0 exposes the ceiling itself.
	p := NewDeterministic(time.Second, 5*time.Minute, func() float64 { return 1.0 })

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, // clamped to attempt 1
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{9, 256 * time.Second},
		{10, 5 * time.Minute}, // 512s capped to 300s
		{20, 5 * time.Minute}, // stays capped, no overflow
		{63, 5 * time.Minute}, // would overflow int64 without the early stop
	}
	for _, tt := range tests {
		if got := p.Delay(tt.attempt); got != tt.want {
			t.Errorf("Delay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestDelayJitterSpansWindow(t *testing.T) {
	// rng pinned to 0 gives the floor: zero delay is a legal draw (that's
	// what makes it FULL jitter).
	p := NewDeterministic(time.Second, time.Minute, func() float64 { return 0 })
	if got := p.Delay(5); got != 0 {
		t.Errorf("floor draw = %v, want 0", got)
	}

	// Real rng: every draw within [0, ceiling), and they actually vary.
	p = New(time.Second, time.Minute)
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		d := p.Delay(3) // ceiling 4s
		if d < 0 || d > 4*time.Second {
			t.Fatalf("draw %v outside [0, 4s]", d)
		}
		seen[d] = true
	}
	if len(seen) < 50 {
		t.Errorf("only %d distinct draws in 200 -- jitter looks broken", len(seen))
	}
}
