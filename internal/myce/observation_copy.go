package myce

import (
	"reflect"
	"slices"
	"sync"
	"time"
)

// Copy the JSON-owned, acyclic observation inputs in memory. The previous
// marshal/unmarshal copy formatted times, sorted map keys and encoded every
// payload while holding q.mu. Only copying mutable memory belongs in that
// critical section. Private/worker-only fields retain the old JSON boundary;
// queue-private diagnostic scalars are captured separately by the caller.
func copyObservation(in observationState) observationState {
	return copyObservationValue(reflect.ValueOf(in)).Interface().(observationState)
}

var observationTimeType = reflect.TypeFor[time.Time]()
var observationPlans sync.Map // reflect.Type -> observationCopyPlan; never queue data.

type observationCopyPlan struct {
	fields, values, clear []int
	private               bool
}

func observationHasReferences(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
		return true
	case reflect.Array:
		return observationHasReferences(t.Elem())
	case reflect.Struct:
		if t == observationTimeType {
			return false
		}
		p := observationPlan(t)
		return p.private || len(p.fields) > 0 || len(p.clear) > 0
	}
	return false
}

func observationPlan(t reflect.Type) observationCopyPlan {
	if plan, ok := observationPlans.Load(t); ok {
		return plan.(observationCopyPlan)
	}
	var p observationCopyPlan
	for i := range t.NumField() {
		field := t.Field(i)
		if field.PkgPath != "" {
			p.private = true
			continue
		}
		if field.Tag.Get("json") == "-" {
			p.clear = append(p.clear, i)
			continue
		}
		if observationHasReferences(field.Type) {
			p.fields = append(p.fields, i)
		} else {
			p.values = append(p.values, i)
		}
	}
	stored, _ := observationPlans.LoadOrStore(t, p)
	return stored.(observationCopyPlan)
}

func copyObservationValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(copyObservationValue(v.Elem()))
		return out
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(copyObservationValue(v.Elem()))
		return out
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), copyObservationValue(iter.Value()))
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		// Scalar slices (including payload bytes and IDs) need only a copy.
		if !observationHasReferences(v.Type().Elem()) {
			reflect.Copy(out, v)
			return out
		}
		for i := range v.Len() {
			out.Index(i).Set(copyObservationValue(v.Index(i)))
		}
		return out
	case reflect.Struct:
		if !observationHasReferences(v.Type()) {
			return v
		}
		p := observationPlan(v.Type())
		out := reflect.New(v.Type()).Elem()
		if p.private {
			for _, i := range p.values {
				out.Field(i).Set(v.Field(i))
			}
		} else {
			out.Set(v)
			for _, i := range p.clear {
				out.Field(i).SetZero()
			}
		}
		for _, i := range p.fields {
			out.Field(i).Set(copyObservationValue(v.Field(i)))
		}
		return out
	case reflect.Array:
		if !observationHasReferences(v.Type().Elem()) {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		for i := range v.Len() {
			out.Index(i).Set(copyObservationValue(v.Index(i)))
		}
		return out
	default:
		return v
	}
}

// Keep only declaration, identity and custody for cancellation projection. Cold
// execution payloads and ledger history are not inputs to descendant traversal.
type cancellationSubmissionInput struct {
	Kind, JobID string
	Job         cancellationObservationInput
}

type cancellationObservationInput struct {
	ID, State, Project, SubmissionKey string
	Attempt                           int
	ReleaseConfirmed                  bool
	CancelledAt                       *time.Time
	Copies                            []Copy
	FanOut                            *FanOutProgress
	Declaration                       *FanOut
	After                             *PieceDependency
	Batch                             *BatchWork
}

func cancellationInputs(j *Job) cancellationObservationInput {
	in := cancellationObservationInput{ID: j.ID, State: j.State, Project: j.Spec.Project, SubmissionKey: j.Spec.SubmissionKey, Attempt: j.Attempt, ReleaseConfirmed: j.ReleaseConfirmed}
	if j.CancelledAt != nil {
		at := *j.CancelledAt
		in.CancelledAt = &at
	}
	for _, copy := range j.Copies {
		in.Copies = append(in.Copies, Copy{Attempt: copy.Attempt, ReleaseConfirmed: copy.ReleaseConfirmed})
	}
	if j.FanOut != nil {
		in.FanOut = &FanOutProgress{Children: slices.Clone(j.FanOut.Children), Results: map[string]PieceResult{}}
		for key := range j.FanOut.Results {
			in.FanOut.Results[key] = PieceResult{}
		}
	}
	pieces := func(in []FanOutPiece) []FanOutPiece {
		out := make([]FanOutPiece, len(in))
		for i, piece := range in {
			out[i].Key = piece.Key
		}
		return out
	}
	if j.Spec.FanOut != nil {
		in.Declaration = &FanOut{Pieces: pieces(j.Spec.FanOut.Pieces)}
	}
	if j.Spec.After != nil {
		in.After = &PieceDependency{Job: j.Spec.After.Job, Piece: j.Spec.After.Piece}
	}
	if j.Batch != nil {
		in.Batch = &BatchWork{Parent: j.Batch.Parent, Pieces: pieces(j.Batch.Pieces)}
	}
	return in
}
func (in cancellationObservationInput) job() *Job {
	return &Job{ID: in.ID, State: in.State, Spec: Spec{Project: in.Project, SubmissionKey: in.SubmissionKey, FanOut: in.Declaration, After: in.After}, Attempt: in.Attempt, ReleaseConfirmed: in.ReleaseConfirmed, CancelledAt: in.CancelledAt, Copies: in.Copies, FanOut: in.FanOut, Batch: in.Batch}
}
