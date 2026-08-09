package articleanalysis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jguapp/calligraphy/internal/handler"
	"github.com/jguapp/calligraphy/internal/job"
)

// Type is the job type this handler serves. Booklet submits
// {"type": "article.analysis", "payload": {"articleId": ..., "text": ...}}.
const Type = "article.analysis"

type payload struct {
	ArticleID string `json:"articleId"`
	Text      string `json:"text"`
	// Optional knobs, defaulted and capped below.
	TopKeywords      int `json:"topKeywords"`
	SummarySentences int `json:"summarySentences"`
}

type result struct {
	Analysis
	ElapsedMs int64 `json:"elapsedMs"`
}

// New returns the registration for the analysis handler. Pure CPU work:
// there is nothing to inject, so the handler is a bare function.
func New() handler.Registration {
	return handler.Registration{
		Type:    Type,
		Handler: handler.Func(handle),
		Options: handler.Options{
			// A full-length article analyzes in tens of milliseconds; a
			// minute of budget means only a pathological input times out.
			ExecTimeout: time.Minute,
		},
	}
}

func handle(ctx context.Context, j *job.Job) (json.RawMessage, error) {
	var p payload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		// A payload that doesn't parse will not parse better on attempt
		// four -- the whole reason NonRetryable exists.
		return nil, job.NonRetryable(fmt.Errorf("articleanalysis: bad payload: %w", err))
	}
	if p.Text == "" {
		return nil, job.NonRetryable(fmt.Errorf("articleanalysis: payload.text is required"))
	}
	if p.TopKeywords <= 0 {
		p.TopKeywords = 10
	}
	if p.TopKeywords > 50 {
		p.TopKeywords = 50
	}
	if p.SummarySentences <= 0 {
		p.SummarySentences = 3
	}
	if p.SummarySentences > 10 {
		p.SummarySentences = 10
	}
	// Analysis is fast but not instant on huge inputs; honor cancellation
	// at the boundary rather than pretending we can interrupt math.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	start := time.Now()
	a := Analyze(p.Text, p.TopKeywords, p.SummarySentences)
	a.ArticleID = p.ArticleID

	out, err := json.Marshal(result{Analysis: a, ElapsedMs: time.Since(start).Milliseconds()})
	if err != nil {
		return nil, fmt.Errorf("articleanalysis: encoding result: %w", err)
	}
	return out, nil
}
