package myce

import (
	"reflect"
	"slices"
	"testing"
)

func TestDecisionReplyBoundedScratchAndOrder(t *testing.T) {
	q, _, _ := decisionFixture(t)
	reply := Reply{}
	for i := 0; i < 4096; i++ {
		reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: ID(), Attempt: 1}, Session: "session", Kill: true})
	}
	c := &decisionCapture{}
	budget := decisionCost(&DecisionReceipt{})
	first, detail := q.summarizeDecisionReply(c, reply, "reply_ready", budget)
	size := decisionCost(&DecisionReceipt{Reply: DecisionReply{Directives: detail}}) - decisionCost(&DecisionReceipt{})
	if size > budget || len(detail) >= len(reply.Attempts) || first.Kills != len(reply.Attempts) || first.DirectivesOmitted != len(reply.Attempts)-len(detail) {
		t.Fatalf("scratch/detail did not follow share: size=%d budget=%d count=%d omitted=%d", size, budget, len(detail), first.DirectivesOmitted)
	}
	slices.Reverse(reply.Attempts)
	second, reversed := q.summarizeDecisionReply(c, reply, "reply_ready", budget)
	if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(detail, reversed) {
		t.Fatal("bounded canonical summary depends on report order")
	}
	empty, noDetail := q.summarizeDecisionReply(c, reply, "reply_ready", 0)
	if len(noDetail) != 0 || empty.Identity != first.Identity || empty.DirectivesOmitted != len(reply.Attempts) {
		t.Fatal("zero scratch lost full streaming identity or retained detail")
	}
}
