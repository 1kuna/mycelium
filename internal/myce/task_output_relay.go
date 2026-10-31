package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

// One demand-driven stream per queue. io.Pipe applies backpressure all the way
// to the source worker; there is no shared-disk staging or accumulating buffer.
// Cancellation/restart closes the stream; a fresh pull reuses the same manifest.
// The existing client transport's progress timeout bounds an absent source.
type taskOutputRelay struct {
	job     Job
	name    string
	reader  *io.PipeReader
	writer  *io.PipeWriter
	claimed bool // taskOutputMu
}

type TaskOutputDownload struct {
	CollectionPending bool              `json:"collection_pending,omitempty"`
	Protocol          string            `json:"protocol"`
	Kind              string            `json:"kind"`
	Receipt           TaskOutputReceipt `json:"receipt"`
	DirectoryReceipt  deliveryReceipt   `json:"directory_receipt"`
}

func taskOutputDirectoryReceipt(j Job) deliveryReceipt {
	return deliveryReceipt{DestinationID: j.Delivery.DestinationID, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Policy: taskOutputProtocol, ManifestSHA256: j.Delivery.ManifestSHA256, Files: maps.Clone(j.LocalOutputs.Files)}
}

func (a API) taskOutputRelayHandlers(mux *requestMux) {
	mux.HandleFunc("GET /api/task-outputs/{job}", func(w http.ResponseWriter, r *http.Request) {
		q := a.Queue
		q.mu.RLock()
		j := q.s.Jobs[r.PathValue("job")]
		if j == nil || j.LocalOutputs == nil || j.Delivery == nil {
			q.mu.RUnlock()
			respondOperation(w, nil, ErrStale, "task_output_invalid")
			return
		}
		out := TaskOutputDownload{Protocol: taskOutputProtocol, Kind: j.Spec.OutputDestination.Kind, Receipt: taskOutputReceipt(*j), DirectoryReceipt: taskOutputDirectoryReceipt(*j)}
		out.CollectionPending = q.taskOutputCollectionPolicyReady(j)
		q.mu.RUnlock()
		respondOperation(w, out, nil, "")
	})
	mux.HandleFunc("GET /api/task-outputs/{job}/files/{name...}", func(w http.ResponseWriter, r *http.Request) {
		q := a.Queue
		q.mu.RLock()
		j := q.s.Jobs[r.PathValue("job")]
		if j == nil || j.LocalOutputs == nil || j.Delivery == nil || j.Spec.OutputDestination.Kind != "submitter" || j.Delivery.ManifestSHA256 != r.URL.Query().Get("manifest") {
			q.mu.RUnlock()
			respondOperation(w, nil, ErrStale, "task_output_invalid")
			return
		}
		if j.LocalOutputs.CollectedAt != nil {
			q.mu.RUnlock()
			respondOperation(w, nil, resultExpired(), "")
			return
		}
		ref, ok := j.LocalOutputs.Files[r.PathValue("name")]
		if !ok {
			q.mu.RUnlock()
			respondOperation(w, nil, ErrStale, "task_output_invalid")
			return
		}
		snapshot := cloneJob(j)
		q.mu.RUnlock()
		reader, writer := io.Pipe()
		relay := &taskOutputRelay{job: snapshot, name: r.PathValue("name"), reader: reader, writer: writer}
		q.taskOutputMu.Lock()
		if len(q.taskOutputRelays) != 0 {
			q.taskOutputMu.Unlock()
			reader.Close()
			writer.Close()
			respondOperation(w, nil, &HTTPError{Status: 409, Code: "output_relay_busy", Message: "another submitter stream is active; retry after it finishes"}, "")
			return
		}
		if q.taskOutputRelays == nil {
			q.taskOutputRelays = map[string]*taskOutputRelay{}
		}
		id := ID()
		q.taskOutputRelays[id] = relay
		q.taskOutputMu.Unlock()
		defer func() {
			q.taskOutputMu.Lock()
			delete(q.taskOutputRelays, id)
			q.taskOutputMu.Unlock()
			reader.Close()
			writer.Close()
		}()
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-r.Context().Done():
				reader.CloseWithError(r.Context().Err())
				writer.CloseWithError(r.Context().Err())
			case <-done:
			}
		}()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprint(ref.SizeBytes))
		w.Header().Set("ETag", `"`+ref.SHA256+`"`)
		// Flush headers so the client can apply its normal progress deadline while
		// the worker picks up this exact request over its control connection.
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = io.Copy(w, reader)
	})
	mux.HandleFunc("PUT /api/workers/task-output-relay/{relay}", func(w http.ResponseWriter, r *http.Request) {
		q := a.Queue
		q.taskOutputMu.Lock()
		relay := q.taskOutputRelays[r.PathValue("relay")]
		if relay == nil || relay.claimed || r.Header.Get("X-Myce-Delivery") != relay.job.Delivery.ManifestSHA256 || r.Header.Get("X-Myce-Session") != relay.job.Session || r.Header.Get("X-Myce-Machine") != relay.job.Machine {
			q.taskOutputMu.Unlock()
			respondOperation(w, nil, ErrStale, "task_output_invalid")
			return
		}
		relay.claimed = true
		q.taskOutputMu.Unlock()
		q.mu.RLock()
		valid := sameDelivery(q.s.Jobs[relay.job.ID], relay.job)
		q.mu.RUnlock()
		if !valid {
			relay.writer.CloseWithError(ErrStale)
			respondOperation(w, nil, ErrStale, "task_output_invalid")
			return
		}
		ref := relay.job.LocalOutputs.Files[relay.name]
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(relay.writer, h), io.LimitReader(r.Body, ref.SizeBytes+1))
		if err == nil && (n != ref.SizeBytes || hex.EncodeToString(h.Sum(nil)) != ref.SHA256) {
			err = errors.New("output relay content mismatch")
		}
		q.mu.RLock()
		valid = sameDelivery(q.s.Jobs[relay.job.ID], relay.job)
		q.mu.RUnlock()
		if !valid {
			err = ErrStale
		}
		relay.writer.CloseWithError(err)
		respondOperation(w, nil, err, "task_output_invalid")
	})
}

func (w Worker) relayTaskOutput(work taskOutputWork, root string) error {
	ref, ok := work.Job.LocalOutputs.Files[work.Name]
	if !ok {
		return ErrStale
	}
	path, err := confinedPath(root, work.Name)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	progress := outputDeliveryProgress([]func(){w.outputDeliveryProgress})
	src := io.TeeReader(f, deliveryProgressWriter{io.Discard, progress})
	res, err := w.Client.fileRequest(context.Background(), "PUT", "/api/workers/task-output-relay/"+url.PathEscape(work.RelayID), src, map[string]string{"X-Myce-Start-Gate": startGateProtocol, "X-Myce-Delivery": work.Job.Delivery.ManifestSHA256, "X-Myce-Session": work.Job.Session, "X-Myce-Machine": work.Job.Machine}, ref.SizeBytes)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fileTransferError(res, "task output relay refused")
	}
	return nil
}

// DeliverTaskOutputs is a submitter-local operation. The queue receives only
// identities and verified receipts, never this destination path. Complete trees
// are synced before atomic rename and receipt acknowledgement. Existing trees
// must verify exactly, making an uncertain acknowledgement safe to replay.
func (c Client) DeliverTaskOutputs(ctx context.Context, id, root string) error {
	var download TaskOutputDownload
	if err := c.Call(ctx, "GET", "/api/task-outputs/"+url.PathEscape(id), nil, &download); err != nil {
		return err
	}
	if download.Protocol != taskOutputProtocol || download.Kind != "submitter" || download.Receipt.Execution.ID != id {
		return errors.New("submitter task-output/1 required")
	}
	if err := download.Receipt.Files.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	for p := abs; ; p = filepath.Dir(p) {
		if err = syncWorkerDir(p); err != nil {
			return err
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	// Reuse NAS staging/verification/sync with the submitter stream as the source.
	r := download.Receipt
	j := Job{ID: id, Attempt: r.Execution.Attempt, Machine: r.Machine, Session: r.Session, LocalOutputs: &TaskOutputCustody{Files: r.Files}, Spec: Spec{Outputs: map[string]string{}, OutputDestination: &OutputDestination{Protocol: taskOutputProtocol, Kind: "submitter", ID: r.DestinationID}}, Delivery: &OutputDelivery{DestinationID: r.DestinationID, Execution: r.Execution, ManifestSHA256: r.ManifestSHA256}}
	for name := range r.Files {
		j.Spec.Outputs[name] = "."
	}
	open := func(name string) (io.ReadCloser, error) {
		path := "/api/task-outputs/" + url.PathEscape(id) + "/files/" + url.PathEscape(name) + "?manifest=" + r.ManifestSHA256
		res, err := c.fileRequest(ctx, "GET", path, nil, nil, r.Files[name].SizeBytes)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK {
			defer res.Body.Close()
			return nil, fileTransferError(res, "task output pull refused")
		}
		if res.Header.Get("ETag") != `"`+r.Files[name].SHA256+`"` {
			res.Body.Close()
			return nil, errors.New("task output identity mismatch")
		}
		return res.Body, nil
	}
	if err := publishOutputDeliverySource(j, root, open); err != nil {
		return err
	}
	if err := c.Call(ctx, "POST", "/api/task-outputs/verified", r, nil); err != nil {
		return err
	}
	// Re-evaluate after confirming a formerly failed delivery: its own pending
	// custody pins disappear only in that transaction. Other pins still win.
	var collection TaskOutputDownload
	if err := c.Call(ctx, "GET", "/api/task-outputs/"+url.PathEscape(id), nil, &collection); err != nil {
		return err
	}
	if collection.CollectionPending {
		return c.preserveTaskOutputs(ctx, j, root)
	}
	return nil
}

// Root identities are explicitly qualified by the operator; a missing NAS
// mount cannot turn a directory on the worker's local disk into a destination.
type OutputDestinationRoot struct {
	Path           string `json:"path"`
	NativeIdentity string `json:"native_identity"`
}

func LoadOutputDestinations(path string) (map[string]OutputDestinationRoot, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var roots map[string]OutputDestinationRoot
	if err = json.Unmarshal(data, &roots); err != nil {
		return nil, err
	}
	for id, root := range roots {
		identity, err := hex.DecodeString(root.NativeIdentity)
		if !extensionName.MatchString(id) || !filepath.IsAbs(root.Path) || err != nil || len(identity) != sha256.Size {
			return nil, errors.New("output destinations require stable IDs, absolute roots and qualified native identities")
		}
	}
	return roots, nil
}
