package myce

import (
	"context"
	"fmt"
	"reflect"
)

// A handler's existing allocation survives through encoding. Direct queue API
// callers get the same accounting for the lifetime of that operation. No
// physical probe runs while q.mu is held: admission measures before capture.
func (b *requestBudgets) projectionContext(ctx context.Context) (context.Context, func(), error) {
	if a, _ := ctx.Value(requestBudgetKey{}).(*requestAllocation); a != nil && a.budgets == b {
		return ctx, func() {}, nil
	}
	n, err := b.capacity(ctx)
	if err != nil {
		return ctx, func() {}, err
	}
	a := &requestAllocation{budgets: b, limit: n}
	return context.WithValue(ctx, requestBudgetKey{}, a), a.close, nil
}

func projectionAllocation(ctx context.Context) *requestAllocation {
	a, _ := ctx.Value(requestBudgetKey{}).(*requestAllocation)
	return a
}
func projectionRefusal(fitting int64) error {
	return &HTTPError{Status: 503, Code: "projection_capacity_exceeded", Message: fmt.Sprintf("projection exceeds measured memory capacity; fitting additional bytes=%d; request a smaller page or retry after memory recovers", max(0, fitting))}
}
func (a *requestAllocation) rewind(mark int64) {
	a.budgets.mu.Lock()
	a.budgets.held -= a.held - mark
	a.budgets.mu.Unlock()
	a.held = mark
}

// Charge the variable-sized, JSON-owned, acyclic representation BEFORE copying
// it. The walk follows the same visibility boundary as copyObservationValue.
// Three stages own detached state, projection and encoding; strings allow JSON's
// six-byte escape representation. Map/slice growth includes spare capacity.
// The work is memory-only. Holding q.mu here makes mutable source reads safe;
// filesystem probes, hashing, JSON encoding and immutable ledger work stay out.
func chargeProjectionValue(ctx context.Context, value any) error {
	a := projectionAllocation(ctx)
	if a == nil {
		return nil
	}
	fitting := a.limit - a.held
	var cost int64
	charge := func(n int64) bool {
		if n < 0 || n > fitting-cost {
			return false
		}
		cost += n
		return true
	}
	var walk func(reflect.Value, bool) bool
	walk = func(v reflect.Value, inline bool) bool {
		if !v.IsValid() {
			return true
		}
		if !inline && !charge(3*int64(v.Type().Size())) {
			return false
		}
		switch v.Kind() {
		case reflect.String:
			return charge(18 * int64(v.Len()))
		case reflect.Pointer, reflect.Interface:
			return v.IsNil() || walk(v.Elem(), false)
		case reflect.Map:
			if v.IsNil() {
				return true
			}
			width := int64(v.Type().Key().Size() + v.Type().Elem().Size() + reflect.TypeFor[uintptr]().Size())
			if int64(v.Len()) > (fitting-cost)/max(1, 6*width) || !charge(6*int64(v.Len())*width) {
				return false
			}
			iter := v.MapRange()
			for iter.Next() {
				if !walk(iter.Key(), true) || !walk(iter.Value(), true) {
					return false
				}
			}
		case reflect.Slice, reflect.Array:
			if v.Kind() == reflect.Slice {
				width := int64(v.Type().Elem().Size())
				if int64(v.Len()) > (fitting-cost)/max(1, 6*width) || !charge(6*int64(v.Len())*width) {
					return false
				}
			}
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return true
			}
			for i := 0; i < v.Len(); i++ {
				if !walk(v.Index(i), true) {
					return false
				}
			}
		case reflect.Struct:
			if v.Type() == observationTimeType {
				return true
			}
			for i := 0; i < v.NumField(); i++ {
				f := v.Type().Field(i)
				if f.PkgPath != "" || f.Tag.Get("json") == "-" {
					continue
				}
				if !walk(v.Field(i), true) {
					return false
				}
			}
		}
		return true
	}
	if !walk(reflect.ValueOf(value), false) || !a.charge(cost) {
		return projectionRefusal(fitting)
	}
	return nil
}
