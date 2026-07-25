package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func fakeWorker(t *testing.T, lines []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, l := range lines {
			fmt.Fprintln(w, l)
		}
	}))
}

func TestGenerateStreamsEvents(t *testing.T) {
	srv := fakeWorker(t, []string{
		`{"token": "Hello", "done": false}`,
		`{"token": " world", "done": false}`,
		`{"done": true, "prompt_tokens": 3, "completion_tokens": 2, "finish_reason": "stop"}`,
	})
	defer srv.Close()

	var tokens []string
	var final Event
	err := New(srv.URL).Generate(context.Background(),
		Request{Messages: []Message{{Role: "user", Content: "hi"}}},
		func(ev Event) bool {
			if ev.Done {
				final = ev
			} else {
				tokens = append(tokens, ev.Token)
			}
			return true
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0] != "Hello" {
		t.Fatalf("tokens = %v", tokens)
	}
	if !final.Done || final.PromptTokens != 3 || final.CompletionTokens != 2 {
		t.Fatalf("final = %+v", final)
	}
}

func TestGenerateStopsWhenCallbackReturnsFalse(t *testing.T) {
	srv := fakeWorker(t, []string{
		`{"token": "a", "done": false}`,
		`{"token": "b", "done": false}`,
		`{"done": true}`,
	})
	defer srv.Close()

	calls := 0
	err := New(srv.URL).Generate(context.Background(), Request{}, func(ev Event) bool {
		calls++
		return false // stop after first event
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestGenerateReturnsErrorOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := New(srv.URL).Generate(context.Background(), Request{}, func(Event) bool { return true })
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}
}
