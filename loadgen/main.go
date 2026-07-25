// Command loadgen measures Forge-Serve under load and writes one CSV row
// per request.
//
// Two firing modes, because they answer different questions:
//   - closed: N virtual users, each sends -> waits -> sends again. Models a
//     fixed user population; used for throughput comparisons. It
//     self-throttles when the server slows, so it CANNOT show latency
//     collapse.
//   - open: requests fire on a timer at a fixed rate regardless of whether
//     earlier ones finished. Models real arrivals and is the honest way to
//     measure latency percentiles under load.
//
// Measured per request: TTFT (time to first token — what a user feels),
// total duration, token count, and TPOT (mean time per output token after
// the first).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	id      int
	mode    string
	start   time.Time
	ttft    time.Duration
	total   time.Duration
	tokens  int
	status  int
	warmup  bool
	errText string
}

type config struct {
	url         string
	key         string
	mode        string
	rate        float64
	concurrency int
	duration    time.Duration
	warmup      time.Duration
	maxTokens   int
	promptWords int
	out         string
	label       string
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", "http://localhost:8080", "gateway base URL")
	flag.StringVar(&c.key, "key", "sk-forge-dev-key", "API key")
	flag.StringVar(&c.mode, "mode", "closed", "closed|open")
	flag.Float64Var(&c.rate, "rate", 5, "open mode: requests per second")
	flag.IntVar(&c.concurrency, "concurrency", 16, "closed mode: virtual users")
	flag.DurationVar(&c.duration, "duration", 60*time.Second, "measured duration (excludes warmup)")
	flag.DurationVar(&c.warmup, "warmup", 15*time.Second, "warmup period, rows tagged and excluded from stats")
	flag.IntVar(&c.maxTokens, "max-tokens", 64, "max_tokens per request")
	flag.IntVar(&c.promptWords, "prompt-words", 32, "approximate prompt length in words")
	flag.StringVar(&c.out, "out", "", "CSV output path (required)")
	flag.StringVar(&c.label, "label", "", "label recorded in the CSV (e.g. engine name)")
	flag.Parse()

	if c.out == "" {
		log.Fatal("-out is required (benchmark numbers must come from committed CSVs)")
	}

	results := run(c)
	if err := writeCSV(c, results); err != nil {
		log.Fatalf("write csv: %v", err)
	}
	summarize(c, results)
}

func run(c config) []result {
	deadline := time.Now().Add(c.warmup + c.duration)
	warmupEnd := time.Now().Add(c.warmup)

	var (
		mu      sync.Mutex
		results []result
		counter int64
	)
	record := func(r result) {
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
	}

	client := &http.Client{} // no timeout: generation is legitimately slow
	prompt := makePrompt(c.promptWords)

	var wg sync.WaitGroup
	switch c.mode {
	case "closed":
		for i := 0; i < c.concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					id := int(atomic.AddInt64(&counter, 1))
					r := fire(client, c, id, prompt, warmupEnd)
					record(r)
				}
			}()
		}
	case "open":
		interval := time.Duration(float64(time.Second) / c.rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for time.Now().Before(deadline) {
			<-ticker.C
			id := int(atomic.AddInt64(&counter, 1))
			wg.Add(1)
			// Fire and do NOT wait: arrivals are independent of completions,
			// which is what makes this open-loop.
			go func() {
				defer wg.Done()
				record(fire(client, c, id, prompt, warmupEnd))
			}()
		}
	default:
		log.Fatalf("unknown mode %q", c.mode)
	}
	wg.Wait()
	return results
}

// fire sends one streaming completion and times it.
func fire(client *http.Client, c config, id int, prompt string, warmupEnd time.Time) result {
	body, _ := json.Marshal(map[string]any{
		"stream":     true,
		"max_tokens": c.maxTokens,
		"messages":   []map[string]string{{"role": "user", "content": prompt}},
	})
	start := time.Now()
	res := result{id: id, mode: c.mode, start: start, warmup: start.Before(warmupEnd)}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		c.url+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		res.errText = err.Error()
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.key)

	resp, err := client.Do(req)
	if err != nil {
		res.errText = err.Error()
		res.total = time.Since(start)
		return res
	}
	defer func() { _ = resp.Body.Close() }()
	res.status = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		res.total = time.Since(start)
		if resp.StatusCode == http.StatusTooManyRequests {
			// Honor the server's backoff like a well-behaved client.
			// Without this, closed-loop users retry instantly against a
			// fail-fast server and the run becomes a retry storm that
			// measures nothing (observed: 17k errors in one 60s run).
			d := time.Second
			if v, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && v > 0 {
				d = time.Duration(v) * time.Second
			}
			time.Sleep(d)
		}
		return res
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) == 0 || chunk.Choices[0].Delta.Content == "" {
			continue
		}
		if res.tokens == 0 {
			res.ttft = time.Since(start)
		}
		res.tokens++
	}
	res.total = time.Since(start)
	return res
}

func makePrompt(words int) string {
	base := strings.Fields("explain how distributed systems handle partial failure " +
		"and why idempotency matters when clients retry requests across a network " +
		"that can duplicate or reorder messages under load")
	var sb strings.Builder
	for i := 0; i < words; i++ {
		sb.WriteString(base[i%len(base)])
		sb.WriteByte(' ')
	}
	return strings.TrimSpace(sb.String())
}

func writeCSV(c config, results []result) error {
	if dir := filepath.Dir(c.out); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.Create(c.out)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{"id", "label", "mode", "param", "start_unix_ms",
		"ttft_ms", "total_ms", "tokens", "tpot_ms", "status", "warmup", "error"}
	if err := w.Write(header); err != nil {
		return err
	}
	param := strconv.Itoa(c.concurrency)
	if c.mode == "open" {
		param = strconv.FormatFloat(c.rate, 'f', 2, 64)
	}
	for _, r := range results {
		tpot := 0.0
		if r.tokens > 1 {
			tpot = float64((r.total - r.ttft).Milliseconds()) / float64(r.tokens-1)
		}
		row := []string{
			strconv.Itoa(r.id), c.label, r.mode, param,
			strconv.FormatInt(r.start.UnixMilli(), 10),
			strconv.FormatInt(r.ttft.Milliseconds(), 10),
			strconv.FormatInt(r.total.Milliseconds(), 10),
			strconv.Itoa(r.tokens),
			strconv.FormatFloat(tpot, 'f', 2, 64),
			strconv.Itoa(r.status),
			strconv.FormatBool(r.warmup),
			r.errText,
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func summarize(c config, results []result) {
	var (
		ttfts   []float64
		tokens  int
		ok      int
		errs    int
		over    int
		first   time.Time
		last    time.Time
		haveAny bool
	)
	for _, r := range results {
		if r.warmup {
			continue
		}
		switch {
		case r.status == http.StatusTooManyRequests:
			over++
			continue
		case r.status != http.StatusOK || r.errText != "":
			errs++
			continue
		}
		ok++
		tokens += r.tokens
		ttfts = append(ttfts, float64(r.ttft.Milliseconds()))
		end := r.start.Add(r.total)
		if !haveAny || r.start.Before(first) {
			first = r.start
		}
		if !haveAny || end.After(last) {
			last = end
		}
		haveAny = true
	}
	window := last.Sub(first).Seconds()
	tps := 0.0
	if window > 0 {
		tps = float64(tokens) / window
	}
	fmt.Printf("\n=== %s | mode=%s param=%v ===\n", c.label, c.mode,
		map[bool]any{true: c.rate, false: c.concurrency}[c.mode == "open"])
	fmt.Printf("requests ok=%d  429=%d  errors=%d\n", ok, over, errs)
	fmt.Printf("completion tokens=%d over %.1fs -> %.2f tok/s\n", tokens, window, tps)
	fmt.Printf("TTFT ms p50=%.0f p95=%.0f p99=%.0f\n",
		pct(ttfts, 50), pct(ttfts, 95), pct(ttfts, 99))
	fmt.Printf("csv: %s\n", c.out)
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	idx := int(p / 100 * float64(len(s)-1))
	return s[idx]
}
