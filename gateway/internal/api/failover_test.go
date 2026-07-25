package api

import (
	"bufio"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// deadWorker refuses every connection attempt, like a killed container.
func deadWorker(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s.Close() // closed immediately: connections are refused
	return s
}

func TestFailoverRetriesOnAnotherWorkerWhenNoTokensSent(t *testing.T) {
	dead := deadWorker(t)
	good := fakeWorker(t, 0, happyLines)
	defer good.Close()

	// Dead worker is listed first and has zero outstanding tokens, so
	// least-outstanding routing picks it first.
	srv := httptest.NewServer(newTestMux(t, dead.URL, good.URL))
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (failover should have rescued this)", resp.StatusCode)
	}
	var got strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") || strings.Contains(line, "[DONE]") {
			continue
		}
		if strings.Contains(line, `"content":"`) {
			for _, part := range []string{"Hello", " world"} {
				if strings.Contains(line, part) {
					got.WriteString(part)
				}
			}
		}
	}
	if got.String() != "Hello world" {
		t.Fatalf("body = %q, want the full response from the surviving worker", got.String())
	}
}

func TestNonStreamFailsOverToo(t *testing.T) {
	dead := deadWorker(t)
	good := fakeWorker(t, 0, happyLines)
	defer good.Close()

	srv := httptest.NewServer(newTestMux(t, dead.URL, good.URL))
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"messages":[{"role":"user","content":"hi"}]}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// dyingWorker streams a few tokens and then drops the connection, which is
// what a worker killed mid-response looks like to the gateway.
func dyingWorker(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		f := w.(http.Flusher)
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintln(w, `{"token": "Hel", "done": false}`)
		f.Flush()
		// Hijack and close: an abrupt transport failure, not a clean EOF.
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, err := hj.Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
}

func TestMidStreamFailureIsNotRetriedAndSendsErrorEvent(t *testing.T) {
	var hits atomic.Int32
	dying := dyingWorker(t, &hits)
	defer dying.Close()
	good := fakeWorker(t, 0, happyLines)
	defer good.Close()

	srv := httptest.NewServer(newTestMux(t, dying.URL, good.URL))
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	defer func() { _ = resp.Body.Close() }()

	body, _ := readAll(resp.Body)
	// No mid-stream migration (ADR-005): the client must be told, not
	// silently handed a second, duplicated generation.
	if !strings.Contains(body, "worker_lost") {
		t.Fatalf("expected a worker_lost error event, got: %q", body)
	}
	if strings.Count(body, "Hel") > 1 {
		t.Fatalf("response was duplicated by a retry: %q", body)
	}
}

func TestNoHealthyWorkerReturns503(t *testing.T) {
	dead := deadWorker(t)
	srv := httptest.NewServer(newTestMux(t, dead.URL))
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra == "" {
		t.Fatal("503 should carry Retry-After so clients back off")
	}
}

func readAll(r interface{ Read([]byte) (int, error) }) (string, error) {
	var sb strings.Builder
	buf := make([]byte, 4096)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n, err := r.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			return sb.String(), nil
		}
	}
	return sb.String(), nil
}
