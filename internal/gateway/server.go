package gateway

import (
	"encoding/json"
	"io"
	"net/http"

	"mycelium/internal/gateway/translate"
)

type Server struct {
	Router *Router
}

func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Router == nil {
		writeGatewayError(w, http.StatusInternalServerError, "gateway router is not configured")
		return
	}
	req, err := parseRequest(r)
	if err != nil {
		status := http.StatusBadRequest
		if routeErr, ok := err.(*routeError); ok {
			status = routeErr.status
		}
		writeGatewayError(w, status, err.Error())
		return
	}
	resp, err := s.Router.Route(r.Context(), req)
	if err != nil {
		writeGatewayError(w, http.StatusBadGateway, err.Error())
		return
	}
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if resp.Status == 0 {
		resp.Status = http.StatusOK
	}
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

func parseRequest(r *http.Request) (translate.IngressRequest, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return translate.IngressRequest{}, err
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		return translate.ParseOpenAIChat(body)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/completions":
		return translate.ParseOpenAICompletion(body)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/messages":
		return translate.ParseAnthropicMessages(body)
	default:
		return translate.IngressRequest{}, &routeError{status: http.StatusNotFound, msg: "not found"}
	}
}

type routeError struct {
	status int
	msg    string
}

func (e *routeError) Error() string {
	return e.msg
}

func writeGatewayError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":` + quoteJSON(msg) + `}`))
}

func quoteJSON(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(data)
}
