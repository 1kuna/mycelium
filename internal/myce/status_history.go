package myce

import (
	"bytes"
	"cmp"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"slices"
	"time"
)

// One response pins one publication, without a cursor registry or a second copy
// of retained execution payloads. Only a private row and its encoding are live
// at once. The ordering index is proportional to job count, not payload size.
func (q *Queue) serveStatusHistory(w http.ResponseWriter, r *http.Request, options statusOptions) {
	ctx := pinRead(r.Context(), q.publishedReads.Load())
	ctx, finish, err := q.requestBudgets.projectionContext(ctx)
	if err != nil {
		respond(w, nil, err)
		return
	}
	defer finish()
	if q.readIndex(ctx) == nil || q.unpublishedRecords() {
		if err = admitObservationProjection(ctx); err != nil {
			respond(w, nil, err)
			return
		}
	}
	options.stream = true
	v, files, err := q.captureReadObservation("", &options, ctx)
	if err != nil {
		respond(w, nil, err)
		return
	}
	v.memoryPlanning = newMemoryPlanningCache()
	v.observationScan = v.placementScan()
	v.observationScan.observationOwner = v
	v.observationScan.memory = v.memoryPlanning
	selection := v.observationSelection
	order, reasons, holds := v.queueProjection()
	type row struct {
		job                     *Job
		call                    *ModelCall
		priority, active, order int
	}
	allocation := projectionAllocation(ctx)
	if !allocation.charge(3 * int64(len(selection.ids)) * int64(reflect.TypeFor[row]().Size())) {
		respond(w, nil, projectionRefusal(allocation.limit-allocation.held))
		return
	}
	rows := make([]row, 0, len(selection.ids))
	add := func(j *Job, c *ModelCall) {
		if !selection.ids[j.ID] {
			return
		}
		active := 2
		state := j.State
		if c != nil {
			if c.Error != "" {
				state = "failed"
			} else if c.Machine != "" {
				state = "running"
			}
		}
		if state == "running" || state == "stopping" {
			active = 0
		} else if state == "queued" {
			active = 1
		}
		rows = append(rows, row{j, c, v.priorityMatch(j.Spec).Index, active, order[j.ID]})
	}
	for _, j := range v.s.Jobs {
		add(j, nil)
	}
	for _, c := range v.calls {
		add(v.callView(c), c)
	}
	slices.SortFunc(rows, func(a, b row) int {
		if n := cmp.Compare(a.priority, b.priority); n != 0 {
			return n
		}
		if n := cmp.Compare(a.active, b.active); n != 0 {
			return n
		}
		if a.order != b.order {
			if a.order == 0 {
				return 1
			}
			if b.order == 0 {
				return -1
			}
			return cmp.Compare(a.order, b.order)
		}
		if n := a.job.Queued.Compare(b.job.Queued); n != 0 {
			return n
		}
		return cmp.Compare(a.job.ID, b.job.ID)
	})
	// Project fleet metadata once. Full-history selection/counts remain the same;
	// only job payload materialization is deferred until encoding each row.
	selection.ids = map[string]bool{}
	out := v.projectSnapshot(true)
	selection.ids = nil
	out.RequestLatencies = q.requestLatencies.snapshot()
	out.Usage, err = v.readUsage(q.usageReader, out.Now.Add(-7*24*time.Hour), ctx)
	if err != nil {
		respond(w, nil, err)
		return
	}
	out.FileStore = files.status(ctx)
	out.Freshness = q.snapshotFreshness(readCommitted(v.clock), false)
	out = publicStatusValue(out)
	if err = chargeProjectionValue(ctx, out); err != nil {
		respond(w, nil, err)
		return
	}
	data, err := json.Marshal(out)
	if err != nil {
		respond(w, nil, err)
		return
	}
	// Snapshot has one non-omitted jobs array. Splicing its contents preserves
	// the existing JSON shape and field order without holding the whole document.
	decoder := json.NewDecoder(bytes.NewReader(data))
	_, _ = decoder.Token() // Metadata was just marshaled as a Snapshot object.
	position := 0
	for decoder.More() {
		key, _ := decoder.Token()
		var field json.RawMessage
		_ = decoder.Decode(&field)
		if key == "jobs" {
			position = int(decoder.InputOffset()) - 1
			break
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if _, err = w.Write(data[:position]); err != nil {
		return
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	for i, row := range rows {
		if ctx.Err() != nil {
			return
		}
		mark := allocation.markProjection()
		if err = chargeProjectionValue(ctx, row.job); err != nil {
			panic(http.ErrAbortHandler)
		}
		var j *Job
		if row.call != nil {
			j = v.projectCall(row.call, order, reasons)
		} else {
			stored := copyObservationValue(reflect.ValueOf(row.job)).Interface().(*Job)
			if original := selection.custody.s.Jobs[stored.ID]; original != nil {
				selection.custody.projectCancellation(stored, original)
			}
			j = v.projectJob(stored, order, reasons, true)
			j.wait = holds[j.ID]
		}
		v.projectJobDiagnostics(j, true, false)
		encoded, encodeErr := json.Marshal(j)
		if encodeErr != nil {
			panic(http.ErrAbortHandler)
		}
		if i > 0 {
			if _, err = io.WriteString(w, ","); err != nil {
				return
			}
		}
		if _, err = w.Write(encoded); err != nil {
			return
		}
		allocation.rewindProjection(mark)
	}
	_, _ = w.Write(append(data[position:], '\n'))
}
