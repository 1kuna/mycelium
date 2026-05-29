package node

import (
	"fmt"
	"net/http"
)

func WriteLoadingState(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	fmt.Fprintf(w, "event: loading\ndata: %s\n\n", message)
}
