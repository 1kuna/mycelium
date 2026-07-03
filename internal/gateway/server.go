package gateway

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/gateway/translate"
)

type Server struct {
	Router              *Router
	RequireAuth         bool
	AuthToken           string
	AuthTokenProjects   map[string]string
	TrustControlHeaders bool
}

func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Router == nil {
		writeGatewayError(w, http.StatusInternalServerError, "gateway router is not configured")
		return
	}
	auth := s.authorize(r)
	if s.RequireAuth && !auth.authorized {
		writeGatewayError(w, http.StatusUnauthorized, "gateway token required")
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/v1/models" {
		s.writeModels(w)
		return
	}
	trustControlHeaders := s.TrustControlHeaders
	req, err := parseRequestWithControlHeaders(r, trustControlHeaders)
	if err != nil {
		status := http.StatusBadRequest
		if routeErr, ok := err.(*routeError); ok {
			status = routeErr.status
		}
		writeGatewayError(w, status, err.Error())
		return
	}
	if auth.project != "" {
		req.Project = auth.project
	}
	if req.Stream {
		if err := s.Router.Stream(r.Context(), req, w); err != nil {
			writeGatewayRouteError(w, err)
		}
		return
	}
	resp, err := s.Router.Route(r.Context(), req)
	if err != nil {
		writeGatewayRouteError(w, err)
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

func (s Server) writeModels(w http.ResponseWriter) {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		OwnedBy string `json:"owned_by"`
	}
	out := struct {
		Object string  `json:"object"`
		Data   []model `json:"data"`
	}{
		Object: "list",
	}
	for _, id := range s.Router.Presets.PublicModelIDs() {
		out.Data = append(out.Data, model{ID: id, Object: "model", OwnedBy: "mycelium"})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func parseRequest(r *http.Request) (translate.IngressRequest, error) {
	return parseRequestWithControlHeaders(r, true)
}

func parseRequestWithControlHeaders(r *http.Request, trustControlHeaders bool) (translate.IngressRequest, error) {
	body, err := readLimited(r.Body, MaxGatewayRequestBodyBytes, "gateway request body")
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
	if raw := r.Header.Get(HeaderContextCap); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Context-Cap"}
		}
		req.ContextRequest = value
	}
	if raw := r.Header.Get(HeaderMaxWait); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Max-Wait-Ms"}
		}
		req.MaxWaitMS = value
		req.HasMaxWait = true
	}
	req.WorkloadID = r.Header.Get(HeaderWorkload)
	req.LeaseID = r.Header.Get(HeaderLease)
	if raw := r.Header.Get(HeaderLeaseHold); raw != "" {
		hold, err := strconv.ParseBool(raw)
		if err != nil {
			return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Lease-Hold"}
		}
		if hold && (req.WorkloadID == "" || req.LeaseID == "") {
			return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "X-Myc-Lease-Hold requires X-Myc-Workload and X-Myc-Lease"}
		}
		req.HoldLease = hold
	}
	if !trustControlHeaders {
		return req, nil
	}
	req.Project = r.Header.Get(HeaderProject)
	req.Priority = domain.Priority(r.Header.Get(HeaderPriority))
	req.SpeedPref = domain.SpeedPref(r.Header.Get(HeaderSpeedPref))
	req.Preemption = domain.Preemption(r.Header.Get(HeaderPreemption))
	req.ConversationKey = r.Header.Get(HeaderConversation)
	req.Handling = domain.HandlingClass(r.Header.Get(HeaderHandling))
	req.Submitter = r.Header.Get(HeaderSubmitter)
	if !validPriority(req.Priority) {
		return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Priority"}
	}
	if !validSpeedPref(req.SpeedPref) {
		return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Speed-Pref"}
	}
	if !validPreemption(req.Preemption) {
		return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Preemption"}
	}
	if !validHandling(req.Handling) {
		return translate.IngressRequest{}, &routeError{status: http.StatusBadRequest, msg: "invalid X-Myc-Handling"}
	}
	return req, nil
}

type gatewayAuthResult struct {
	authorized bool
	project    string
}

func (s Server) authorize(r *http.Request) gatewayAuthResult {
	const prefix = "Bearer "
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, prefix) {
		return gatewayAuthResult{}
	}
	got := strings.TrimPrefix(value, prefix)
	for token, project := range s.AuthTokenProjects {
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
			return gatewayAuthResult{authorized: true, project: project}
		}
	}
	if s.AuthToken != "" && subtle.ConstantTimeCompare([]byte(got), []byte(s.AuthToken)) == 1 {
		return gatewayAuthResult{authorized: true}
	}
	return gatewayAuthResult{}
}

func validHandling(handling domain.HandlingClass) bool {
	switch handling {
	case "":
		return true
	default:
		return false
	}
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

func writeGatewayRouteError(w http.ResponseWriter, err error) {
	var statusErr interface {
		StatusCode() int
	}
	if errors.As(err, &statusErr) {
		writeGatewayError(w, statusErr.StatusCode(), err.Error())
		return
	}
	var noInstance noInstanceAvailableError
	if errors.As(err, &noInstance) {
		w.Header().Set("Retry-After", strconv.Itoa(noInstance.retryAfterSeconds()))
		writeDecisionHeaders(w.Header(), noInstance.Decision, noInstance.Instance, noInstance.Profile, noInstance.Attempts)
		writeGatewayError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeGatewayError(w, http.StatusBadGateway, err.Error())
}

func quoteJSON(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(data)
}
