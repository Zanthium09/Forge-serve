package api

import "errors"

// OpenAI /v1/chat/completions request — the fields Phase 1 honors.
// Unknown JSON fields are ignored by encoding/json, which matches
// OpenAI's own be-liberal-in-what-you-accept behavior.
type ChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Temperature *float64      `json:"temperature"`
	MaxTokens   *int          `json:"max_tokens"`
	Stream      bool          `json:"stream"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Validate enforces the trust boundary: everything past here assumes a
// well-formed request.
func (r *ChatRequest) Validate() error {
	if len(r.Messages) == 0 {
		return errors.New("messages: required, non-empty")
	}
	for _, m := range r.Messages {
		if m.Role == "" {
			return errors.New("messages[].role: required")
		}
	}
	if r.Temperature != nil && (*r.Temperature < 0 || *r.Temperature > 2) {
		return errors.New("temperature: must be in [0, 2]")
	}
	if r.MaxTokens != nil && *r.MaxTokens <= 0 {
		return errors.New("max_tokens: must be positive")
	}
	return nil
}

// Non-stream response (object "chat.completion").
type ChatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []RespChoice `json:"choices"`
	Usage   Usage        `json:"usage"`
}

type RespChoice struct {
	Index        int         `json:"index"`
	Message      ChatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}
