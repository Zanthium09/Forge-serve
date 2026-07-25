package api

import "testing"

func TestValidateAcceptsMinimalRequest(t *testing.T) {
	r := ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "hi"}}}
	if err := r.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
}

func TestValidateRejectsBadRequests(t *testing.T) {
	temp := 3.5
	neg := -1
	cases := []struct {
		name string
		req  ChatRequest
	}{
		{"no messages", ChatRequest{}},
		{"empty role", ChatRequest{Messages: []ChatMessage{{Content: "hi"}}}},
		{"temperature out of range", ChatRequest{
			Messages: []ChatMessage{{Role: "user", Content: "hi"}}, Temperature: &temp}},
		{"negative max_tokens", ChatRequest{
			Messages: []ChatMessage{{Role: "user", Content: "hi"}}, MaxTokens: &neg}},
	}
	for _, c := range cases {
		if err := c.req.Validate(); err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
		}
	}
}
