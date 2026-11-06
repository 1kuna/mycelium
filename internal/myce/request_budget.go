package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const requestBudgetProtocol = "capacity-v1"

type requestBudgetKey struct{}
type peerResponseBudgetKey struct{}
type requestAllocation struct {
	budgets     *requestBudgets
	limit, held int64
}

// Admission accounts for the wire buffer, decoder scratch and typed allocations.
// There is one share per runnable CPU plus a control share: a single large read
// cannot consume the memory needed by the queue's next control transaction.
// Shares are derived afresh from OS headroom, GC growth and the process limit;
// they are not installation-sized body ceilings. Charges live through the handler.
type requestBudgets struct {
	mu                   sync.Mutex
	memory               func(context.Context) (int64, error) // injected physical headroom
	lanes                int                                  // test seam; production uses runnable CPUs + control
	held, ceiling        int64
	rowBytes             int64
	rowTime, controlTime time.Duration
}

func requestMemory(ctx context.Context) (int64, error) {
	gb, err := hostAvailableMemory(ctx, runtime.GOOS, os.ReadFile, func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, name, args...).Output()
	})
	if err != nil {
		return 0, err
	}
	available := int64(min(gb*(1<<30), float64(math.MaxInt64)))
	available = processMemoryHeadroom(available)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// The OS reading excludes resident process memory. Reserve the next GC growth
	// and a copy of the current goroutine stacks for control work, not a fixed %.
	growth := int64(m.NextGC-min(m.NextGC, m.HeapAlloc)) + int64(m.StackInuse)
	available = max(0, available-growth)
	// GOMEMLIMIT is an explicit process boundary, including in containers.
	limit := debug.SetMemoryLimit(-1)
	used := int64(m.Sys - m.HeapReleased)
	return max(0, min(available, limit-used-growth)), nil
}

func (b *requestBudgets) capacity(ctx context.Context) (int64, error) {
	source := b.memory
	if source == nil {
		source = requestMemory
	}
	n, err := source(ctx)
	if err != nil {
		return 0, &HTTPError{Status: 503, Code: "request_capacity_unavailable", Message: "queue memory headroom is unavailable; fitting bytes=0; retry after memory measurement recovers"}
	}
	lanes := b.lanes
	if lanes == 0 {
		lanes = runtime.GOMAXPROCS(0) + 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ceiling = n
	return max(0, n-b.held) / int64(max(1, lanes)), nil
}
func (a *requestAllocation) charge(n int64) bool {
	if n < 0 || n > a.limit-a.held {
		return false
	}
	a.budgets.mu.Lock()
	if n > a.budgets.ceiling-a.budgets.held {
		a.budgets.mu.Unlock()
		return false
	}
	a.budgets.held += n
	a.budgets.mu.Unlock()
	a.held += n
	return true
}
func (a *requestAllocation) close() {
	a.budgets.mu.Lock()
	a.budgets.held -= a.held
	a.budgets.mu.Unlock()
	a.held = 0
}
func budgetRefusal(limit int64) error {
	return &HTTPError{Status: 413, Code: "request_too_large", Message: fmt.Sprintf("request exceeds measured queue memory capacity; fitting wire bytes=%d (decoded payload allocations also charged); split the request or retry with more headroom", max(0, limit))}
}
func (q *Queue) withRequestBudget(w http.ResponseWriter, r *http.Request) (*http.Request, func(), error) {
	n, err := q.requestBudgets.capacity(r.Context())
	if err != nil {
		return r, func() {}, err
	}
	if r.Header.Get("X-Myce-Request-Budgets") == requestBudgetProtocol && r.Header.Get("X-Myce-Max-Response-Bytes") != "" {
		peer, parseErr := strconv.ParseInt(r.Header.Get("X-Myce-Max-Response-Bytes"), 10, 64)
		if parseErr != nil || peer <= 0 {
			return r, func() {}, &HTTPError{Status: 400, Code: "request_budget_invalid", Message: "max response bytes must be positive"}
		}
		r = r.WithContext(context.WithValue(r.Context(), peerResponseBudgetKey{}, peer))
	}
	allocation := &requestAllocation{budgets: &q.requestBudgets, limit: n}
	w.Header().Set("X-Myce-Request-Budgets", requestBudgetProtocol)
	// Growing input and decoder buffers each need up to two copies; token and
	// decoded strings need another two. These are representation costs.
	w.Header().Set("X-Myce-Max-Body-Bytes", strconv.FormatInt(n/6, 10))
	r = r.WithContext(context.WithValue(r.Context(), requestBudgetKey{}, allocation))
	return r, allocation.close, nil
}

// Preflight the actual shape before encoding/json allocates typed slices/maps.
// Sparse objects in a slice can allocate much more than their wire bytes.
// Reflection charges their Go sizes, map entry/key/value storage and slice growth.
// Token strings are transient; the six wire copies cover them. The standard
// decoder retains its own syntax/depth limits; this adds no nesting ceiling.
func jsonAllocationCost(data []byte, target reflect.Type, limit int64) (int64, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var cost int64
	add := func(n int64) error {
		if n < 0 || n > limit-cost {
			return budgetRefusal(int64(float64(len(data)) * float64(limit) / float64(max(1, cost+n))))
		}
		cost += n
		return nil
	}
	var scan func(reflect.Type) error
	scan = func(t reflect.Type) error {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		if tok == nil {
			return nil
		}
		for t != nil && t.Kind() == reflect.Pointer {
			t = t.Elem()
			if err = add(int64(t.Size())); err != nil {
				return err
			}
		}
		delim, composite := tok.(json.Delim)
		if !composite {
			return nil
		}
		if delim == '{' {
			if t != nil && (t.Kind() == reflect.Map || t.Kind() == reflect.Interface) {
				if err = add(int64(reflect.TypeFor[map[string]any]().Size()) + int64(8*reflect.TypeFor[uintptr]().Size())); err != nil {
					return err
				}
			}
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				var child reflect.Type
				if t != nil {
					switch t.Kind() {
					case reflect.Struct:
						child = jsonFieldType(t, key.(string))
					case reflect.Map:
						child = t.Elem()
						// Spare map capacity plus key/value storage, control word and indirection.
						if e = add(2 * int64(t.Key().Size()+child.Size()+reflect.TypeFor[uintptr]().Size())); e != nil {
							return e
						}
					case reflect.Interface:
						child = t
						if e = add(2 * int64(reflect.TypeFor[string]().Size()+reflect.TypeFor[any]().Size()+reflect.TypeFor[uintptr]().Size())); e != nil {
							return e
						}
					}
				}
				if e = scan(child); e != nil {
					return e
				}
			}
		} else if delim == '[' {
			for d.More() {
				var child reflect.Type
				if t != nil {
					switch t.Kind() {
					case reflect.Slice, reflect.Array:
						child = t.Elem()
					case reflect.Interface:
						child = t
					}
				}
				if child != nil {
					if err = add(2 * int64(child.Size())); err != nil {
						return err
					}
				}
				if err = scan(child); err != nil {
					return err
				}
			}
		} else {
			return errors.New("invalid JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	err := scan(target)
	if err == nil {
		_, err = d.Token()
		if err == io.EOF {
			err = nil
		} else if err == nil {
			err = errors.New("expected one JSON object")
		}
	}
	return cost, err
}
func jsonFieldType(t reflect.Type, name string) reflect.Type {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if tag == "-" {
			continue
		}
		if tag == "" && f.Anonymous {
			nested := f.Type
			for nested.Kind() == reflect.Pointer {
				nested = nested.Elem()
			}
			if nested.Kind() == reflect.Struct {
				if typ := jsonFieldType(nested, name); typ != nil {
					return typ
				}
			}
		}
		if tag == "" {
			tag = f.Name
		}
		if strings.EqualFold(tag, name) {
			return f.Type
		}
	}
	return nil
}

func readBudgetJSON(r *http.Request, v any) ([]byte, error) {
	a, _ := r.Context().Value(requestBudgetKey{}).(*requestAllocation)
	if a == nil { // Standalone handlers/tests still use physical capacity.
		b := &requestBudgets{}
		n, err := b.capacity(r.Context())
		if err != nil {
			return nil, err
		}
		a = &requestAllocation{budgets: b, limit: n}
		defer a.close()
	}
	limit := a.limit / 6
	if r.ContentLength > limit {
		return nil, budgetRefusal(limit)
	}
	var data bytes.Buffer
	// Allocate in OS pages. Charge all growing-buffer capacity, decoder scratch
	// and string copies before reading, including chunked/false-length bodies.
	page := make([]byte, os.Getpagesize())
	for {
		remaining := limit - int64(data.Len())
		n, err := r.Body.Read(page[:min(int64(len(page)), remaining+1)])
		if int64(n) > remaining {
			return nil, budgetRefusal(limit)
		}
		if n > 0 {
			if !a.charge(6 * int64(n)) {
				return nil, budgetRefusal(limit)
			}
			data.Write(page[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if err = r.Context().Err(); err != nil {
			return nil, err
		}
	}
	cost, err := jsonAllocationCost(data.Bytes(), reflect.TypeOf(v), a.limit-a.held)
	if err != nil {
		return nil, err
	}
	if !a.charge(cost) {
		return nil, budgetRefusal(limit)
	}
	return data.Bytes(), nil
}

func (b *requestBudgets) recordControl(d time.Duration) {
	if d <= 0 {
		return
	}
	b.mu.Lock()
	b.controlTime = d
	b.mu.Unlock()
}
func (b *requestBudgets) recordProjection(n, bytes int, d time.Duration) {
	if n == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rowBytes = max(1, int64(bytes)/int64(n))
	b.rowTime = max(1, d/time.Duration(n))
}
func (b *requestBudgets) pageLimit(ctx context.Context, element reflect.Type) (int, error) {
	capacity, err := b.capacity(ctx)
	if err != nil {
		return 0, err
	}
	if peer, ok := ctx.Value(peerResponseBudgetKey{}).(int64); ok {
		capacity = min(capacity, peer)
	}
	b.mu.Lock()
	row, cost, control := b.rowBytes, b.rowTime, b.controlTime
	b.mu.Unlock()
	// Detached state, projection and JSON each own a representation. Before the
	// first sample, Go's actual type size is the only available local cost.
	if element != reflect.TypeFor[Job]() {
		row, cost = 0, 0
	}
	row = max(row, int64(element.Size()))
	if capacity < 3*row {
		return 0, &HTTPError{Status: 503, Code: "projection_capacity_exceeded", Message: fmt.Sprintf("projection requires at least %d bytes; fitting bytes=%d", 3*row, capacity)}
	}
	limit := capacity / max(1, 3*row)
	if element == reflect.TypeFor[Job]() {
		if cost > 0 && control > 0 {
			limit = min(limit, max(1, int64(control/cost)))
		} else {
			// Before comparable timings exist, probe one row per execution
			// lane. This physical concurrency bound prevents a cold wide read
			// monopolizing control; observed costs replace it after check-in.
			lanes := b.lanes
			if lanes == 0 {
				lanes = runtime.GOMAXPROCS(0) + 1
			}
			limit = min(limit, int64(max(1, lanes)))
		}
	}
	return int(min(int64(math.MaxInt), max(1, limit))), nil
}

// Header negotiation leaves strict legacy JSON schemas untouched. Unknown
// protocols cannot impose new semantics. HTTP contexts and the peer deadline
// are upper bounds; measured control latency reserves response delivery time.
func (b *requestBudgets) waitDuration(r *http.Request, seconds int, now time.Time) (time.Duration, error) {
	if seconds < 0 || int64(seconds) > math.MaxInt64/int64(time.Second) {
		return 0, &HTTPError{Status: 400, Code: "wait_invalid", Message: "seconds must be nonnegative and representable as a duration"}
	}
	duration := time.Duration(seconds) * time.Second
	deadline, has := r.Context().Deadline()
	if r.Header.Get("X-Myce-Request-Budgets") == requestBudgetProtocol && r.Header.Get("X-Myce-Transport-Deadline") != "" {
		peer, err := time.Parse(time.RFC3339Nano, r.Header.Get("X-Myce-Transport-Deadline"))
		if err != nil {
			return 0, &HTTPError{Status: 400, Code: "wait_invalid", Message: "transport deadline must be RFC3339"}
		}
		if !has || peer.Before(deadline) {
			deadline, has = peer, true
		}
	}
	if has {
		b.mu.Lock()
		delivery := b.controlTime
		b.mu.Unlock()
		duration = min(duration, max(0, deadline.Sub(now)-delivery))
	}
	return duration, nil
}

// Read to the measured allowance even if a source file grows after stat.
func readCapacityFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, max(0, limit)+1))
	if err == nil && int64(len(data)) > limit {
		err = budgetRefusal(limit)
	}
	return data, err
}

func decodeBudget(w http.ResponseWriter, r *http.Request, v any) error {
	data, err := readBudgetJSON(r, v)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(v)
}
