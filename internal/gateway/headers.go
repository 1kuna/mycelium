package gateway

import (
	"encoding/json"
	"net/http"
	"strconv"

	"mycelium/internal/domain"
	"mycelium/internal/gateway/profiles"
)

const (
	HeaderDecision     = "X-Myc-Decision"
	HeaderNode         = "X-Myc-Node"
	HeaderInstance     = "X-Myc-Instance"
	HeaderBackend      = "X-Myc-Backend"
	HeaderAttempts     = "X-Myc-Attempts"
	HeaderTrace        = "X-Myc-Trace"
	HeaderJob          = "X-Myc-Job"
	HeaderProject      = "X-Myc-Project"
	HeaderPriority     = "X-Myc-Priority"
	HeaderSpeedPref    = "X-Myc-Speed-Pref"
	HeaderContextCap   = "X-Myc-Context-Cap"
	HeaderPreemption   = "X-Myc-Preemption"
	HeaderConversation = "X-Myc-Conversation"
	HeaderHandling     = "X-Myc-Handling"
	HeaderSubmitter    = "X-Myc-Submitter"
)

func writeDecisionHeaders(h http.Header, decision domain.PlacementDecision, inst domain.ModelInstance, profile profiles.Profile, attempts int) {
	h.Set(HeaderDecision, string(decision.Action))
	h.Set(HeaderNode, inst.NodeID)
	h.Set(HeaderInstance, inst.ID)
	h.Set(HeaderBackend, profile.ID)
	h.Set(HeaderAttempts, strconv.Itoa(attempts))
	if decision.JobID != "" {
		h.Set(HeaderJob, decision.JobID)
	}
	if len(decision.Trace) > 0 {
		data, err := json.Marshal(decision.Trace)
		if err == nil {
			h.Set(HeaderTrace, string(data))
		}
	}
}
