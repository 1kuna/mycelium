package myce

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"
	"slices"
	"strings"
	"unsafe"
)

// Visit the adapted reply without copying or indexing its attempt collection.
func visitDecisionDirectives(c *decisionCapture, reply Reply, visit func(DecisionDirective)) {
	for _, a := range reply.Attempts {
		d := DecisionDirective{Execution: a.Execution, Addressed: true, Session: a.Session, Offer: a.Job != nil, Stop: a.Stop, Kill: a.Kill, Hold: a.Hold}
		if a.Job != nil && a.Job.Batch != nil {
			d.Parent = a.Job.Batch.Parent
		}
		visit(d)
	}
	if j := reply.Job; j != nil {
		d := DecisionDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Addressed: true, Session: j.Session, Offer: true, Stop: reply.Stop, Kill: reply.Kill, Hold: reply.Hold}
		if j.Batch != nil {
			d.Parent = j.Batch.Parent
		}
		visit(d)
	} else if c.legacy.ID != "" && (reply.Stop || reply.Kill || reply.Hold) {
		visit(DecisionDirective{Execution: c.legacy, Session: c.session, Stop: reply.Stop, Kill: reply.Kill, Hold: reply.Hold})
	}
}

func directiveBits(d DecisionDirective) byte {
	var n byte
	for _, v := range [...]bool{d.Addressed, d.Offer, d.Stop, d.Kill, d.Hold} {
		n *= 2
		if v {
			n++
		}
	}
	return n
}

func compareDirective(a, b DecisionDirective) int {
	for _, pair := range [][2]string{{a.ID, b.ID}, {a.Session, b.Session}, {a.Parent, b.Parent}} {
		if n := strings.Compare(pair[0], pair[1]); n != 0 {
			return n
		}
	}
	if a.Attempt != b.Attempt {
		if a.Attempt < b.Attempt {
			return -1
		}
		return 1
	}
	return int(directiveBits(a)) - int(directiveBits(b))
}

// Length framing and SHA block-sized writes avoid scratch proportional to an
// incoming identity's length. No JSON of the full reply or offer map is built.
type decisionHasher struct {
	hash.Hash
	frame  [9]byte
	block  [sha256.BlockSize]byte
	digest [sha256.Size]byte
}

func (h *decisionHasher) text(value string) {
	binary.BigEndian.PutUint64(h.frame[:8], uint64(len(value)))
	h.Write(h.frame[:8])
	for len(value) > 0 {
		n := min(len(value), sha256.BlockSize)
		copy(h.block[:], value[:n])
		h.Write(h.block[:n])
		value = value[n:]
	}
}

func (q *Queue) summarizeDecisionReply(c *decisionCapture, reply Reply, outcome string, scratch int64) (DecisionReply, []DecisionDirective) {
	d := DecisionReply{Stop: reply.Stop, Kill: reply.Kill, Hold: reply.Hold, Outcome: outcome, Leave: reply.Leave, Preparation: len(reply.EnvironmentPlan) > 0, Copy: len(reply.CopyPlan) > 0, Model: len(reply.ModelPlan) > 0, AdmissionBlocked: reply.AdmissionBlocked}
	var sum [sha256.Size]byte
	count, maxCost := 0, int64(1)
	sessionBytes := len(q.control.anonymous(""))
	h := &decisionHasher{Hash: sha256.New()}
	visitDecisionDirectives(c, reply, func(item DecisionDirective) {
		count++
		// Conservative uniform slots make the retained lexicographic prefix
		// independent of input order, including variable-length execution IDs.
		cost := 2*int64(unsafe.Sizeof(item)) + 3*int64(len(item.ID)+max(len(item.Session), sessionBytes)+len(item.Parent))
		maxCost = max(maxCost, cost)
		if item.Offer {
			d.Assignments++
		}
		if item.Stop {
			d.Stops++
		}
		if item.Kill {
			d.Kills++
		}
		if item.Hold {
			d.Holds++
		}
		h.Reset()
		h.text(item.ID)
		h.text(item.Session)
		h.text(item.Parent)
		binary.BigEndian.PutUint64(h.frame[:8], uint64(item.Attempt))
		h.frame[8] = directiveBits(item)
		h.Write(h.frame[:])
		h.Sum(h.digest[:0])
		// Addition modulo 2^256 is order-independent and preserves multiplicity.
		carry := uint16(0)
		for i := len(sum) - 1; i >= 0; i-- {
			carry += uint16(sum[i]) + uint16(h.digest[i])
			sum[i] = byte(carry)
			carry >>= 8
		}
	})
	h.Reset()
	json.NewEncoder(h).Encode(d)
	h.Write(sum[:])
	d.Identity = q.control.anonymous(hex.EncodeToString(h.Sum(nil)))
	slots := min(int64(count), scratch/maxCost)
	retained := make([]DecisionDirective, 0, int(slots))
	if slots > 0 {
		visitDecisionDirectives(c, reply, func(item DecisionDirective) {
			i, _ := slices.BinarySearchFunc(retained, item, compareDirective)
			if len(retained) < cap(retained) {
				retained = append(retained, DecisionDirective{})
			} else if i == len(retained) {
				return
			}
			copy(retained[i+1:], retained[i:len(retained)-1])
			retained[i] = item
		})
	}
	for i := range retained {
		retained[i].Session = q.control.anonymous(retained[i].Session)
	}
	slices.SortFunc(retained, compareDirective)
	d.DirectivesOmitted = count - len(retained)
	return d, retained
}
