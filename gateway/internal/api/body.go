package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Zanthium09/forge-serve/gateway/internal/backend"
)

// parsedRequest holds the few fields the gateway reads, plus every field as
// raw JSON so the body can be forwarded unchanged (ADR-007).
type parsedRequest struct {
	Endpoint  backend.Endpoint
	Model     string
	Stream    bool
	MaxTokens int
	fields    map[string]json.RawMessage
}

// readBody enforces the size limit at the trust boundary.
func readBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, error) {
	return io.ReadAll(http.MaxBytesReader(w, r.Body, max))
}

var requiredField = map[backend.Endpoint]string{
	backend.EndpointChat:        "messages",
	backend.EndpointCompletions: "prompt",
	backend.EndpointEmbeddings:  "input",
}

func parseRequest(ep backend.Endpoint, body []byte) (*parsedRequest, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return nil, errors.New("request body must be a JSON object")
	}
	if f := requiredField[ep]; fields[f] == nil {
		return nil, fmt.Errorf("%s: required", f)
	}
	p := &parsedRequest{Endpoint: ep, fields: fields}
	if raw, ok := fields["model"]; ok {
		if err := json.Unmarshal(raw, &p.Model); err != nil {
			return nil, errors.New("model: must be a string")
		}
	}
	if raw, ok := fields["stream"]; ok {
		if err := json.Unmarshal(raw, &p.Stream); err != nil {
			return nil, errors.New("stream: must be a boolean")
		}
	}
	if raw, ok := fields["max_tokens"]; ok {
		if err := json.Unmarshal(raw, &p.MaxTokens); err != nil || p.MaxTokens < 0 {
			return nil, errors.New("max_tokens: must be a non-negative integer")
		}
	}
	return p, nil
}

// build makes the body for one attempt. The only edits allowed (spec §6.2):
// model rewritten to the upstream name, include_usage set on streams, and
// client priority replaced by the gateway's (or removed).
func (p *parsedRequest) build(upstreamModel string, priority *int) ([]byte, bool, error) {
	out := make(map[string]json.RawMessage, len(p.fields)+1)
	for k, v := range p.fields {
		out[k] = v
	}
	out["model"], _ = json.Marshal(upstreamModel)

	// A client must never choose its own priority (ADR-013).
	delete(out, "priority")
	if priority != nil {
		out["priority"], _ = json.Marshal(*priority)
	}

	injected := false
	if p.Stream {
		so := map[string]json.RawMessage{}
		if raw, ok := out["stream_options"]; ok {
			if err := json.Unmarshal(raw, &so); err != nil || so == nil {
				so = map[string]json.RawMessage{}
			}
		}
		var include bool
		_ = json.Unmarshal(so["include_usage"], &include)
		if !include {
			so["include_usage"] = json.RawMessage("true")
			injected = true
			out["stream_options"], _ = json.Marshal(so)
		}
	}
	b, err := json.Marshal(out)
	return b, injected, err
}

// resolveModel lets clients that send no model (the MVP's loadgen and chaos
// scripts) keep working when only one model is configured.
func resolveModel(requested string, models []string) (string, error) {
	if requested != "" {
		return requested, nil
	}
	if len(models) == 1 {
		return models[0], nil
	}
	return "", errors.New("model: required when more than one model is configured")
}

// writeError answers in OpenAI's error shape.
func writeError(w http.ResponseWriter, status int, typ, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"message": msg, "type": typ, "code": code},
	})
}
