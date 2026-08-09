package articleanalysis

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jguapp/calligraphy/internal/job"
)

// A fixture with known shape: 3 paragraphs, clear topic words, and enough
// prose for the language vote to be confident.
const fixture = `Distributed systems fail in ways that single machines cannot. A worker
can crash halfway through a job, a network can partition, and a queue can
deliver the same message twice. The systems that survive are the ones
designed around those failures rather than around their absence.

Calligraphy treats the queue as a transport and the database as the truth. The
queue moves jobs between machines quickly. The database records what
actually happened to every job, every attempt, and every retry. When the
two disagree, the database wins.

Mr. Lamport wrote about clocks in 1978. His insight was that ordering
matters more than time. Version 2.0 of that idea appears in every fencing
token, including the one this system uses to stop zombie workers.`

func TestAnalyzeCounts(t *testing.T) {
	a := Analyze(fixture, 10, 3)

	if a.Paragraphs != 3 {
		t.Errorf("paragraphs = %d, want 3", a.Paragraphs)
	}
	// 10 real sentences (counted by hand); the abbreviation "Mr." and the
	// decimal "2.0" must not split. Exact count pins the tokenizer.
	if a.Sentences != 10 {
		t.Errorf("sentences = %d, want 10", a.Sentences)
	}
	if a.Words < 110 || a.Words > 145 {
		t.Errorf("words = %d, want ~125", a.Words)
	}
	if a.Language != "en" {
		t.Errorf("language = %q, want en", a.Language)
	}
	if a.ReadingTimeMinutes != 1 {
		t.Errorf("readingTime = %d, want 1", a.ReadingTimeMinutes)
	}
	// Plausibility windows, not exact values: the formulas are fixed but
	// the syllable counter is an estimator.
	if a.FleschReadingEase < 20 || a.FleschReadingEase > 90 {
		t.Errorf("reading ease = %v, outside plausible prose range", a.FleschReadingEase)
	}
	if a.FleschKincaidGrade < 4 || a.FleschKincaidGrade > 16 {
		t.Errorf("grade = %v, outside plausible prose range", a.FleschKincaidGrade)
	}
}

func TestKeywordsSurfaceTopicTerms(t *testing.T) {
	a := Analyze(fixture, 10, 3)
	if len(a.Keywords) == 0 {
		t.Fatal("no keywords")
	}
	terms := map[string]bool{}
	for _, k := range a.Keywords {
		terms[k.Term] = true
	}
	// The text is about queues, databases, and jobs; TextRank should
	// notice at least two of the words the text is actually about.
	hits := 0
	for _, want := range []string{"queue", "database", "job", "jobs", "workers", "worker", "systems"} {
		if terms[want] {
			hits++
		}
	}
	if hits < 2 {
		t.Errorf("topic terms in keywords = %d, want >= 2 (got %v)", hits, a.Keywords)
	}
	// Stopwords must never appear.
	for _, k := range a.Keywords {
		if stopwordsEN[k.Term] {
			t.Errorf("stopword %q ranked as keyword", k.Term)
		}
	}
}

func TestSummaryIsInReadingOrder(t *testing.T) {
	a := Analyze(fixture, 10, 3)
	if len(a.Summary) != 3 {
		t.Fatalf("summary length = %d, want 3", len(a.Summary))
	}
	// Each summary sentence exists in the source, and they appear in
	// source order.
	lastIdx := -1
	for _, s := range a.Summary {
		idx := strings.Index(fixture, strings.Split(s, " ")[0])
		if idx < 0 {
			t.Errorf("summary sentence not from source: %q", s)
		}
		pos := strings.Index(fixture, s[:min(40, len(s))])
		if pos < lastIdx {
			t.Errorf("summary out of reading order at %q", s)
		}
		lastIdx = pos
	}
}

func TestShortTextDegradesGracefully(t *testing.T) {
	a := Analyze("One sentence.", 10, 3)
	if a.Sentences != 1 || a.Words != 2 {
		t.Errorf("short text: %+v", a)
	}
	if a.Language != "unknown" {
		t.Errorf("language on 2 words = %q, want unknown", a.Language)
	}
	if len(a.Summary) != 1 {
		t.Errorf("summary of one sentence = %v", a.Summary)
	}
	empty := Analyze("", 10, 3)
	if empty.Words != 0 || len(empty.Keywords) != 0 || len(empty.Summary) != 0 {
		t.Errorf("empty text: %+v", empty)
	}
}

func TestSpanishDetection(t *testing.T) {
	es := `El sistema distribuido procesa los trabajos en una cola. Los
	trabajadores toman las tareas de la cola y las ejecutan. Cuando un
	trabajador falla, el sistema reintenta la tarea con otro trabajador
	porque la cola es duradera y no pierde los mensajes que recibe.`
	if a := Analyze(es, 5, 2); a.Language != "es" {
		t.Errorf("language = %q, want es", a.Language)
	}
}

func TestSyllables(t *testing.T) {
	tests := map[string]int{
		"cat": 1, "table": 2, "distributed": 4, "queue": 1,
		"idempotency": 5, "the": 1, "beautiful": 3, "a": 1,
	}
	for w, want := range tests {
		if got := countSyllables(w); got != want {
			t.Errorf("syllables(%q) = %d, want %d", w, got, want)
		}
	}
}

func TestHandlerPayloadValidation(t *testing.T) {
	reg := New()
	ctx := context.Background()

	// Garbage payload: permanently failed, never retried.
	j := &job.Job{ID: "j1", Type: Type, Payload: json.RawMessage(`{broken`)}
	if _, err := reg.Handler.Handle(ctx, j); !job.IsNonRetryable(err) {
		t.Errorf("bad payload error = %v, want NonRetryable", err)
	}
	j.Payload = json.RawMessage(`{"articleId":"a1"}`)
	if _, err := reg.Handler.Handle(ctx, j); !job.IsNonRetryable(err) {
		t.Errorf("missing text error = %v, want NonRetryable", err)
	}

	// Happy path round-trips the article id and produces the full shape.
	j.Payload = json.RawMessage(`{"articleId":"a1","text":` + strconv(fixture) + `}`)
	out, err := reg.Handler.Handle(ctx, j)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	var res result
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("result not JSON: %v", err)
	}
	if res.ArticleID != "a1" || res.Words == 0 || len(res.Keywords) == 0 {
		t.Errorf("result = %+v", res)
	}
}

func strconv(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
