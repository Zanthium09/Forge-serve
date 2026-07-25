package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsEndpointExposesForgeMetrics(t *testing.T) {
	srv := httptest.NewServer(newTestMux(t, "http://unused.invalid"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "forge_ttft_seconds") {
		t.Fatal("forge_ttft_seconds not exposed")
	}
}
