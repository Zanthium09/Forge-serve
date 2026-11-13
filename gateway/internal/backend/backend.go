// Package backend is the gateway's view of an inference engine: one
// interface, implemented once per protocol (OpenAI-compatible engines, and
// our own NDJSON worker), not once per engine. Engines differ mainly in
// how they report load, which the Load method covers.
package backend

import (
	"context"
	"errors"
	"fmt"
)

type Endpoint string

const (
	EndpointChat        Endpoint = "chat"
	EndpointCompletions Endpoint = "completions"
	EndpointEmbeddings  Endpoint = "embeddings"
)

// Path is the OpenAI route for the endpoint.
func (e Endpoint) Path() string {
	switch e {
	case EndpointCompletions:
		return "/v1/completions"
	case EndpointEmbeddings:
		return "/v1/embeddings"
	default:
		return "/v1/chat/completions"
	}
}

// ProxyRequest is one attempt against one backend.
type ProxyRequest struct {
	Endpoint        Endpoint
	Body            []byte // final body, already edited by the gateway
	Stream          bool
	StripUsageChunk bool // the gateway injected include_usage; hide that chunk
}

type Usage struct {
	PromptTokens     int
	CompletionTokens int
	Estimated        bool // the engine reported no usage
}

// Result says what reached the client. Sent is the ADR-005 retry signal:
// content chunks written for a stream, 1 for a delivered JSON body.
type Result struct {
	Sent  int
	Usage Usage
}

// Sink is the client side. Start is called at most once, right before the
// first Write. Nothing reaches the Sink before the first content chunk of a
// stream, or before the full successful body of a non-stream reply, so a
// failure before that point is still safe to retry elsewhere.
type Sink interface {
	Start(contentType string)
	Write(p []byte) error
}

// Load is what the engine says about its own queue. Known is false for
// engines that expose no queue data; only health is learned from them.
type Load struct {
	Waiting int
	Running int
	Known   bool
}

type Backend interface {
	Forward(ctx context.Context, r *ProxyRequest, sink Sink) (Result, error)
	Load(ctx context.Context) (Load, error)
}

// ErrOverloaded: the backend refused at admission (HTTP 429). The request
// was never accepted, so the client gets a 429, not a 502.
var ErrOverloaded = errors.New("backend overloaded")

// ErrUnsupported: this backend kind cannot serve the endpoint.
var ErrUnsupported = errors.New("endpoint not supported by backend")

// StatusError is a non-2xx, non-429 reply. 4xx means the request itself is
// wrong for this engine (e.g. context too long): relay it, never retry.
type StatusError struct {
	Code int
	Body []byte
}

func (e *StatusError) Error() string { return fmt.Sprintf("backend returned %d", e.Code) }
