// Package articleanalysis is Caligraphy's flagship built-in handler: real,
// CPU-bound text analysis with no external API in sight.
//
// That constraint is the point. The obvious demo handler calls an LLM and
// waits; this one does actual work on the worker's own CPU -- TextRank
// (the PageRank-over-text algorithm) for keywords and extractive summary,
// Flesch-Kincaid readability, and a stopword-ratio language guess -- which
// makes it an honest subject for the CPU-bound half of the benchmarks,
// and free to run ten thousand times.
package articleanalysis

import (
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// Analysis is the structured result. Field names are the public contract
// Booklet (or any client) reads back.
type Analysis struct {
	ArticleID          string    `json:"articleId,omitempty"`
	Language           string    `json:"language"`
	Words              int       `json:"words"`
	Sentences          int       `json:"sentences"`
	Paragraphs         int       `json:"paragraphs"`
	ReadingTimeMinutes int       `json:"readingTimeMinutes"`
	FleschReadingEase  float64   `json:"fleschReadingEase"`
	FleschKincaidGrade float64   `json:"fleschKincaidGrade"`
	Keywords           []Keyword `json:"keywords"`
	Summary            []string  `json:"summary"`
}

type Keyword struct {
	Term  string  `json:"term"`
	Score float64 `json:"score"`
}

const (
	wordsPerMinute = 200 // same convention Booklet's reading-time estimate uses

	// TextRank parameters -- the standard ones from Mihalcea & Tarau
	// (2004). Not tunables: changing them silently changes every stored
	// result, so they're constants until there's a reason with a benchmark
	// attached.
	damping    = 0.85
	iterations = 30
	coocWindow = 4

	// maxSummarySentences caps the sentence-similarity graph. It is O(n²)
	// in sentences; 600² comparisons is milliseconds, while a pathological
	// 50k-sentence input would be minutes. Past the cap, later sentences
	// simply don't compete for the summary -- acceptable for prose, where
	// leads carry weight anyway.
	maxGraphSentences = 600
)

// Analyze runs the whole pipeline. Deterministic: same text, same result.
func Analyze(text string, topKeywords, summaryLen int) Analysis {
	paragraphs := splitParagraphs(text)
	sentences := splitSentences(text)
	words := tokenize(text)

	a := Analysis{
		Words:      len(words),
		Sentences:  len(sentences),
		Paragraphs: len(paragraphs),
		Language:   detectLanguage(words),
	}
	a.ReadingTimeMinutes = (len(words) + wordsPerMinute - 1) / wordsPerMinute

	if len(words) > 0 && len(sentences) > 0 {
		syl := 0
		for _, w := range words {
			syl += countSyllables(w)
		}
		wps := float64(len(words)) / float64(len(sentences))
		spw := float64(syl) / float64(len(words))
		a.FleschReadingEase = round1(206.835 - 1.015*wps - 84.6*spw)
		a.FleschKincaidGrade = round1(0.39*wps + 11.8*spw - 15.59)
	}

	a.Keywords = keywordTextRank(words, topKeywords)
	a.Summary = summaryTextRank(sentences, summaryLen)
	return a
}

// ---------------------------------------------------------------- tokenize

var paragraphRe = regexp.MustCompile(`\n\s*\n`)

func splitParagraphs(text string) []string {
	var out []string
	for _, p := range paragraphRe.Split(text, -1) {
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return out
}

// abbreviations that end with a period but don't end a sentence. Small on
// purpose -- a full sentence tokenizer is a project; this list covers the
// cases that actually distort counts in ordinary prose.
var abbreviations = map[string]bool{
	"mr": true, "mrs": true, "ms": true, "dr": true, "prof": true,
	"sr": true, "jr": true, "st": true, "vs": true, "etc": true,
	"e.g": true, "i.e": true, "fig": true, "no": true, "vol": true,
}

// splitSentences is a rune scanner, not a regex: it needs one token of
// lookbehind (is the word before this period an abbreviation?) and
// lookahead (does a capital or digit follow?), both awkward in Go's re2.
func splitSentences(text string) []string {
	var sentences []string
	var cur strings.Builder
	runes := []rune(text)

	flush := func() {
		s := strings.TrimSpace(cur.String())
		if s != "" {
			sentences = append(sentences, s)
		}
		cur.Reset()
	}

	for i := 0; i < len(runes); i++ {
		r := runes[i]
		cur.WriteRune(r)
		if r != '.' && r != '!' && r != '?' {
			continue
		}
		// Consume a run of terminators ("?!", "...") as one boundary.
		for i+1 < len(runes) && (runes[i+1] == '.' || runes[i+1] == '!' || runes[i+1] == '?') {
			i++
			cur.WriteRune(runes[i])
		}
		if r == '.' {
			// Look back at the word this period ends.
			prev := lastWord(cur.String())
			if abbreviations[strings.ToLower(strings.TrimSuffix(prev, "."))] {
				continue
			}
			// "3.14", "v2.0" -- a digit on both sides is not a boundary.
			if i+1 < len(runes) && unicode.IsDigit(runes[i+1]) {
				continue
			}
		}
		// A boundary needs something sentence-like after it: whitespace
		// then a capital, digit, or quote -- or end of text.
		j := i + 1
		for j < len(runes) && (runes[j] == ' ' || runes[j] == '\n' || runes[j] == '\t' || runes[j] == '\r') {
			j++
		}
		if j >= len(runes) {
			flush()
			continue
		}
		if j > i+1 && (unicode.IsUpper(runes[j]) || unicode.IsDigit(runes[j]) || runes[j] == '"' || runes[j] == '“') {
			flush()
		}
	}
	flush()
	return sentences
}

func lastWord(s string) string {
	s = strings.TrimRight(s, ". ")
	if i := strings.LastIndexAny(s, " \n\t"); i >= 0 {
		return s[i+1:]
	}
	return s
}

// tokenize lowercases and keeps letter-runs (with internal apostrophes and
// hyphens), so "don't" and "state-machine" stay single tokens.
func tokenize(text string) []string {
	var words []string
	var cur strings.Builder
	for _, r := range text {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			cur.WriteRune(unicode.ToLower(r))
		case (r == '\'' || r == '-') && cur.Len() > 0:
			cur.WriteRune(r)
		default:
			if cur.Len() > 0 {
				words = append(words, strings.TrimRight(cur.String(), "'-"))
				cur.Reset()
			}
		}
	}
	if cur.Len() > 0 {
		words = append(words, strings.TrimRight(cur.String(), "'-"))
	}
	return words
}

// countSyllables estimates by vowel groups with the two adjustments that
// matter most in English: silent trailing e, and "-le" after a consonant
// ("table"). An estimate is fine -- Flesch-Kincaid was calibrated on
// estimates.
func countSyllables(word string) int {
	w := strings.ToLower(word)
	if len(w) <= 2 {
		return 1
	}
	isVowel := func(r byte) bool { return strings.IndexByte("aeiouy", r) >= 0 }
	count, prevVowel := 0, false
	for i := 0; i < len(w); i++ {
		v := isVowel(w[i])
		if v && !prevVowel {
			count++
		}
		prevVowel = v
	}
	if strings.HasSuffix(w, "e") && !strings.HasSuffix(w, "le") && count > 1 {
		count--
	}
	if count == 0 {
		count = 1
	}
	return count
}

func round1(f float64) float64 { return math.Round(f*10) / 10 }

// ---------------------------------------------------------------- pagerank

// pagerank over a weighted undirected graph, dense-ish adjacency as maps.
// Shared by keywords (word co-occurrence) and summary (sentence
// similarity) -- one algorithm, two graphs, which is the whole elegance
// of TextRank.
func pagerank(adj []map[int]float64, iters int) []float64 {
	n := len(adj)
	if n == 0 {
		return nil
	}
	rank := make([]float64, n)
	outSum := make([]float64, n)
	for i := range rank {
		rank[i] = 1.0 / float64(n)
		for _, w := range adj[i] {
			outSum[i] += w
		}
	}
	next := make([]float64, n)
	for it := 0; it < iters; it++ {
		for i := range next {
			next[i] = (1 - damping) / float64(n)
		}
		for j := range adj {
			if outSum[j] == 0 {
				continue
			}
			share := damping * rank[j] / outSum[j]
			for i, w := range adj[j] {
				next[i] += share * w
			}
		}
		rank, next = next, rank
	}
	return rank
}

func keywordTextRank(words []string, topK int) []Keyword {
	if topK <= 0 {
		topK = 10
	}
	// Candidates: non-stopword tokens of length >= 3.
	index := map[string]int{}
	var vocab []string
	seq := make([]int, 0, len(words))
	for _, w := range words {
		if len(w) < 3 || stopwordsEN[w] || isNumeric(w) {
			seq = append(seq, -1)
			continue
		}
		id, ok := index[w]
		if !ok {
			id = len(vocab)
			index[w] = id
			vocab = append(vocab, w)
		}
		seq = append(seq, id)
	}
	if len(vocab) == 0 {
		return []Keyword{}
	}

	adj := make([]map[int]float64, len(vocab))
	for i := range adj {
		adj[i] = map[int]float64{}
	}
	// Co-occurrence within a sliding window over the original sequence
	// (stopwords occupy positions but form no edges -- distance matters).
	for i, a := range seq {
		if a < 0 {
			continue
		}
		for j := i + 1; j < len(seq) && j <= i+coocWindow; j++ {
			b := seq[j]
			if b < 0 || b == a {
				continue
			}
			adj[a][b]++
			adj[b][a]++
		}
	}

	rank := pagerank(adj, iterations)
	kws := make([]Keyword, len(vocab))
	for i, term := range vocab {
		kws[i] = Keyword{Term: term, Score: rank[i]}
	}
	sort.Slice(kws, func(i, j int) bool {
		if kws[i].Score != kws[j].Score {
			return kws[i].Score > kws[j].Score
		}
		return kws[i].Term < kws[j].Term // deterministic ties
	})
	if len(kws) > topK {
		kws = kws[:topK]
	}
	for i := range kws {
		kws[i].Score = math.Round(kws[i].Score*10000) / 10000
	}
	return kws
}

func summaryTextRank(sentences []string, summaryLen int) []string {
	if summaryLen <= 0 {
		summaryLen = 3
	}
	n := len(sentences)
	if n == 0 {
		return []string{}
	}
	if n <= summaryLen {
		return sentences
	}
	if n > maxGraphSentences {
		n = maxGraphSentences
	}

	// Token sets per sentence, stopwords removed.
	sets := make([]map[string]bool, n)
	for i := 0; i < n; i++ {
		set := map[string]bool{}
		for _, w := range tokenize(sentences[i]) {
			if len(w) >= 3 && !stopwordsEN[w] {
				set[w] = true
			}
		}
		sets[i] = set
	}

	// Similarity: |overlap| / (log|a| + log|b|) -- the original TextRank
	// normalization, which keeps long sentences from winning on bulk.
	adj := make([]map[int]float64, n)
	for i := range adj {
		adj[i] = map[int]float64{}
	}
	for i := 0; i < n; i++ {
		if len(sets[i]) == 0 {
			continue
		}
		for j := i + 1; j < n; j++ {
			if len(sets[j]) == 0 {
				continue
			}
			overlap := 0
			small, large := sets[i], sets[j]
			if len(small) > len(large) {
				small, large = large, small
			}
			for w := range small {
				if large[w] {
					overlap++
				}
			}
			if overlap == 0 {
				continue
			}
			norm := math.Log(float64(len(sets[i]))+1) + math.Log(float64(len(sets[j]))+1)
			w := float64(overlap) / norm
			adj[i][j] = w
			adj[j][i] = w
		}
	}

	rank := pagerank(adj, iterations)
	type scored struct {
		idx   int
		score float64
	}
	order := make([]scored, n)
	for i := range order {
		order[i] = scored{i, rank[i]}
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].score != order[j].score {
			return order[i].score > order[j].score
		}
		return order[i].idx < order[j].idx
	})
	picked := order[:summaryLen]
	// Present in reading order, not rank order -- a summary that jumps
	// around the article reads like a ransom note.
	sort.Slice(picked, func(i, j int) bool { return picked[i].idx < picked[j].idx })
	out := make([]string, len(picked))
	for i, s := range picked {
		out[i] = sentences[s.idx]
	}
	return out
}

func isNumeric(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return len(s) > 0
}

// ---------------------------------------------------------------- language

// detectLanguage is a stopword-ratio vote across four languages. Crude and
// honest: it answers "which language's little words saturate this text",
// which is reliable past ~50 words of real prose and is exactly as far as
// this feature needs to go. Below the confidence floor it says "unknown"
// rather than guessing.
func detectLanguage(words []string) string {
	if len(words) < 10 {
		return "unknown"
	}
	scores := map[string]int{}
	for _, w := range words {
		if stopwordsEN[w] {
			scores["en"]++
		}
		if stopwordsES[w] {
			scores["es"]++
		}
		if stopwordsFR[w] {
			scores["fr"]++
		}
		if stopwordsDE[w] {
			scores["de"]++
		}
	}
	best, bestN := "unknown", 0
	for lang, n := range scores {
		if n > bestN {
			best, bestN = lang, n
		}
	}
	// Confidence floor: at least 8% of tokens voted for the winner.
	if bestN*100 < len(words)*8 {
		return "unknown"
	}
	return best
}

var stopwordsEN = wordSet(`the a an and or but if then else when while of to in on at by for with about as into from up down out off over under around again is are was were be been being have has had do does did will would can cannot could should must not no nor this that these those it its i you he she we they them his her their my your our me him us what which who whom where why how all any both each few more most other some such only own same so than too very just there here once during rather before after above below within without s t don now`)

var stopwordsES = wordSet(`el la los las un una unos unas y o pero si de del en a al por para con sobre como desde hasta es son era eran ser está están fue fueron hay que no ni este esta estos estas ese esa eso aquellos yo tú él ella nosotros ellos ellas su sus mi mis tu tus lo le les se me te nos os cuando donde porque qué cómo todo toda todos todas más menos muy ya también entre sin`)

var stopwordsFR = wordSet(`le la les un une des et ou mais si de du en à au aux par pour avec sur comme depuis est sont était étaient être été a ont il elle nous vous ils elles son sa ses mon ma mes ton ta tes ce cette ces cela qui que quoi où quand pourquoi comment tout toute tous toutes plus moins très déjà aussi entre sans dans ne pas`)

var stopwordsDE = wordSet(`der die das ein eine einer eines dem den und oder aber wenn dann von zu in auf an bei für mit über unter als aus nach vor ist sind war waren sein gewesen hat haben hatte werden wird nicht kein keine dieser diese dieses jener ich du er sie wir ihr mein dein sein ihre was welche wer wo warum wie alle jeder mehr sehr auch zwischen ohne durch noch schon`)

func wordSet(s string) map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(s) {
		m[w] = true
	}
	return m
}
