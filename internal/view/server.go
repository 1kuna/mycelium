package view

import (
	"embed"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"strings"
)

//go:embed assets
var assets embed.FS

// Handler serves the embedded page and the shared frame stream. Every route
// is GET/HEAD; nothing the browser sends reaches the queue.
//
// When the view listens on loopback, requests must name a loopback host, so
// a web page on another origin cannot read the stream through DNS rebinding.
func Handler(p *Poller, listen string) http.Handler {
	static, _ := fs.Sub(assets, "assets")
	files := http.FileServer(http.FS(static))
	loopbackOnly := isLoopback(listen)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			h.Set("Allow", "GET, HEAD")
			http.Error(w, "read-only view", http.StatusMethodNotAllowed)
			return
		}
		if loopbackOnly && !isLoopback(r.Host) {
			http.Error(w, "loopback host required", http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/events":
			stream(w, r, p)
		case "/", "/data.js", "/scene.js", "/view.js", "/view.css":
			h.Set("Cache-Control", "no-cache")
			files.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

// stream sends the newest frame as a server-sent event whenever it changes.
func stream(w http.ResponseWriter, r *http.Request, p *Poller) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		return
	}
	frames, cancel := p.Subscribe()
	defer cancel()
	fmt.Fprint(w, "retry: 2000\n\n")
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-frames:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func isLoopback(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
