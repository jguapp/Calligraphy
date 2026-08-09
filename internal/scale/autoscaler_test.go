package scale

import "testing"

func TestNextPolicy(t *testing.T) {
	a := &Autoscaler{Cfg: Config{Min: 2, Max: 16, GrowAt: 2.0, IdleAfter: 3}}

	// A scripted day in the life: each step feeds the previous target
	// back in, exactly like the loop does.
	steps := []struct {
		name           string
		ready          int64
		active, target int
		want           int
	}{
		{"below min snaps up", 0, 0, 1, 2},
		{"steady load holds", 6, 4, 4, 4},           // 6 < 2.0*4
		{"backlog grows by half", 8, 4, 4, 6},       // 8 >= 8
		{"deep backlog keeps growing", 40, 6, 6, 9}, // 40 >= 12
		{"growth respects max", 500, 9, 9, 13},      // 9+4 (integer half)
		{"growth caps at max", 500, 13, 13, 16},     // 13+6 -> capped 16
		{"idle tick 1 holds", 0, 3, 16, 16},
		{"idle tick 2 holds", 0, 2, 16, 16},
		{"idle tick 3 shrinks one", 0, 1, 16, 15},
		{"work resets idle counter", 5, 10, 15, 15},
		{"idle restarts from zero (1)", 0, 0, 15, 15},
		{"idle restarts from zero (2)", 0, 0, 15, 15},
		{"idle restarts from zero (3)", 0, 0, 15, 14},
	}
	for _, s := range steps {
		if got := a.Next(s.ready, s.active, s.target); got != s.want {
			t.Fatalf("%s: Next(ready=%d active=%d target=%d) = %d, want %d",
				s.name, s.ready, s.active, s.target, got, s.want)
		}
	}
}

func TestShrinkNeverBelowMin(t *testing.T) {
	a := &Autoscaler{Cfg: Config{Min: 2, Max: 8, GrowAt: 2.0, IdleAfter: 1}}
	target := 3
	for i := 0; i < 10; i++ {
		target = a.Next(0, 0, target)
	}
	if target != 2 {
		t.Errorf("floor violated: target = %d, want 2", target)
	}
}

// A busy pool with an empty queue is NOT idle: jobs in flight mean the
// system is working exactly at capacity, and shrinking would cut it.
func TestBusyPoolEmptyQueueIsNotIdle(t *testing.T) {
	a := &Autoscaler{Cfg: Config{Min: 1, Max: 8, GrowAt: 2.0, IdleAfter: 1}}
	if got := a.Next(0, 4, 4); got != 4 {
		t.Errorf("fully-busy pool shrunk to %d", got)
	}
}
