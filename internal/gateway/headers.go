package gateway

import (
	"net/http"
	"strconv"

	"mycelium/internal/domain"
	"mycelium/internal/gateway/profiles"
)

const (
	HeaderDecision   = "X-Myc-Decision"
	HeaderNode       = "X-Myc-Node"
	HeaderInstance   = "X-Myc-Instance"
	HeaderBackend    = "X-Myc-Backend"
	HeaderAttempts   = "X-Myc-Attempts"
	HeaderProject    = "X-Myc-Project"
	HeaderPriority   = "X-Myc-Priority"
	HeaderSpeedPref  = "X-Myc-Speed-Pref"
	HeaderContextCap = "X-Myc-Context-Cap"
	HeaderPreemption = "X-Myc-Preemption"
)

func writeDecisionHeaders(h http.Header, decision domain.PlacementDecision, inst domain.ModelInstance, profile profiles.Profile, attempts int) {
	h.Set(HeaderDecision, string(decision.Action))
	h.Set(HeaderNode, inst.NodeID)
	h.Set(HeaderInstance, inst.ID)
	h.Set(HeaderBackend, profile.ID)
	h.Set(HeaderAttempts, strconv.Itoa(attempts))
}
