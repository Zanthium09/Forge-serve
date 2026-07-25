package api

import (
	_ "embed"
	"net/http"
)

// The playground is embedded in the binary rather than served from disk:
// the container ships one static file with no volume mount, and there is
// no path-traversal surface because there is no file lookup at all.
//
//go:embed playground.html
var playgroundHTML []byte

func handlePlayground(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(playgroundHTML)
}
