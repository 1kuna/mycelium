package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"mycelium/internal/domain"
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
	var req translate.IngressRequest
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		req, err = translate.ParseOpenAIChat(body)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/completions":
		req, err = translate.ParseOpenAICompletion(body)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/messages":
		req, err = translate.ParseAnthropicMessages(body)
	default:
		return translate.IngressRequest{}, &routeError{status: http.StatusNotFound, msg: "not found"}
	}
	if err != nil {
		return translate.IngressRequest{}, err
	}
	req.Project = r.Header.Get(HeaderProject)
	req.Priority = domain.Priority(r.Header.Get(HeaderPriority))
	req.SpeedPref = domain.SpeedPref(r.Header.Get(HeaderSpeedPref))
	req.Preemption = domain.Preemption(r.Header.Get(HeaderPreemption))
	req.ConversationKey = r.Header.Get(HeaderConversation)
	if !validPriority(req.Priority) {
		return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Priority"}
	}
	if !validSpeedPref(req.SpeedPref) {
		return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Speed-Pref"}
	}
	if !validPreemption(req.Preemption) {
		return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Preemption"}
	}
	if raw := r.Header.Get(HeaderContextCap); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Context-Cap"}
		}
		req.ContextRequest = value
	}
	return req, nil
}

func validPriority(priority domain.Priority) bool {
	switch priority {
	case "", domain.PriorityInteractive, domain.PriorityNormal, domain.PriorityBackground:
		return true
	default:
		return false
	}
}

func validSpeedPref(speed domain.SpeedPref) bool {
	switch speed {
	case "", domain.SpeedThroughput, domain.SpeedLatency, domain.SpeedAuto:
		return true
	default:
		return false
	}
}

func validPreemption(preemption domain.Preemption) bool {
	switch preemption {
	case "", domain.PreemptInherit, domain.PreemptSoft, domain.PreemptHardForInteractive, domain.PreemptHard:
		return true
	default:
		return false
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
