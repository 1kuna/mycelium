package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
)

// A submitter path is private and unreachable from the worker. When collection
// is due a reconnecting submitter streams the current destination bytes back
// through the existing bounded HTTP transport. The collecting worker hashes
// this live stream, never a cached acknowledgement. No payload is staged on
// shared storage, and a restart/cancel discards the entire verification round.
type taskOutputPreservation struct {
	id      string
	job     Job
	reader  *io.PipeReader
	writer  *io.PipeWriter
	claimed bool // taskOutputMu
	done    chan struct{}
	closed  bool // taskOutputMu
}

func (a API) taskOutputPreservationHandlers(mux *requestMux) {
	mux.HandleFunc("PUT /api/task-outputs/{job}/preservation", func(w http.ResponseWriter, r *http.Request) {
		q := a.Queue
		q.mu.RLock()
		j := q.s.Jobs[r.PathValue("job")]
		valid := q.taskOutputCollectible(j) && j.Spec.OutputDestination.Kind == "submitter" &&
			r.Header.Get("X-Myce-Delivery") == j.Delivery.ManifestSHA256 && r.Header.Get("X-Myce-Session") == j.Session && r.Header.Get("X-Myce-Machine") == j.Machine
		var snapshot Job
		if valid {
			snapshot = cloneJob(j)
		}
		q.mu.RUnlock()
		if !valid {
			respondOperation(w, nil, ErrStale, "task_output_invalid")
			return
		}
		reader, writer := io.Pipe()
		p := &taskOutputPreservation{id: ID(), job: snapshot, reader: reader, writer: writer, done: make(chan struct{})}
		q.taskOutputMu.Lock()
		if q.taskOutputPreservation != nil {
			q.taskOutputMu.Unlock()
			reader.Close()
			writer.Close()
			respondOperation(w, nil, &HTTPError{Status: 409, Code: "output_preservation_busy", Message: "another destination verification is active"}, "")
			return
		}
		q.taskOutputPreservation = p
		q.taskOutputMu.Unlock()
		defer func() {
			q.taskOutputMu.Lock()
			if q.taskOutputPreservation == p {
				q.taskOutputPreservation = nil
			}
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
		_, err := io.Copy(writer, io.LimitReader(r.Body, int64(manifestDiskBytes(snapshot.LocalOutputs.Files))+1))
		writer.CloseWithError(err)
		// The round stays live through the collecting worker's fence check and
		// deletion. It is removed only when the worker releases the GET below.
		select {
		case <-r.Context().Done():
			err = r.Context().Err()
		case <-p.done:
		}
		q.mu.RLock()
		collected := q.s.Jobs[snapshot.ID]
		valid = sameDelivery(collected, snapshot) && collected.LocalOutputs.CollectedAt != nil
		q.mu.RUnlock()
		if err == nil && !valid {
			err = &HTTPError{Status: 409, Code: "output_collection_pending", Message: "destination verified but collection remains pending; retry"}
		}
		respondOperation(w, nil, err, "task_output_invalid")
	})
	mux.HandleFunc("GET /api/task-output-preservation/{id}", func(w http.ResponseWriter, r *http.Request) {
		q := a.Queue
		q.taskOutputMu.Lock()
		p := q.taskOutputPreservation
		valid := p != nil && p.id == r.PathValue("id") && !p.claimed && r.Header.Get("X-Myce-Delivery") == p.job.Delivery.ManifestSHA256 && r.Header.Get("X-Myce-Session") == p.job.Session && r.Header.Get("X-Myce-Machine") == p.job.Machine
		if valid {
			p.claimed = true
		}
		q.taskOutputMu.Unlock()
		if !valid {
			respondOperation(w, nil, ErrStale, "task_output_invalid")
			return
		}
		q.mu.RLock()
		valid = sameDelivery(q.s.Jobs[p.job.ID], p.job) && q.taskOutputCollectible(q.s.Jobs[p.job.ID])
		q.mu.RUnlock()
		if !valid {
			respondOperation(w, nil, ErrStale, "task_output_invalid")
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, p.reader)
	})
	mux.HandleFunc("DELETE /api/task-output-preservation/{id}", func(w http.ResponseWriter, r *http.Request) {
		q := a.Queue
		q.taskOutputMu.Lock()
		if p := q.taskOutputPreservation; p != nil && p.id == r.PathValue("id") && !p.closed {
			p.closed = true
			p.reader.Close()
			p.writer.Close()
			close(p.done)
		}
		q.taskOutputMu.Unlock()
		respondOperation(w, nil, nil, "")
	})
}

func (w Worker) verifySubmitterPreservation(work taskOutputWork) error {
	if work.PreservationID == "" {
		return errors.New("submitter must reconnect for current destination verification")
	}
	j := work.Job
	res, err := w.Client.fileRequest(context.Background(), "GET", "/api/task-output-preservation/"+url.PathEscape(work.PreservationID), nil, map[string]string{"X-Myce-Delivery": j.Delivery.ManifestSHA256, "X-Myce-Session": j.Session, "X-Myce-Machine": j.Machine}, int64(manifestDiskBytes(j.LocalOutputs.Files)))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fileTransferError(res, "destination preservation unavailable")
	}
	for _, name := range slices.Sorted(maps.Keys(j.LocalOutputs.Files)) {
		ref := j.LocalOutputs.Files[name]
		h := sha256.New()
		if _, err = io.CopyN(h, res.Body, ref.SizeBytes); err != nil {
			return err
		}
		if hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
			return errors.New("destination preservation content mismatch")
		}
	}
	var extra [1]byte
	if n, err := res.Body.Read(extra[:]); n != 0 || err != io.EOF {
		return errors.New("destination preservation stream incomplete")
	}
	return nil
}

// This reader opens one file at a time and never buffers a complete payload.
type preservationReader struct {
	mu     sync.Mutex
	closed bool
	root   *os.Root
	names  []string
	file   *os.File
}

func (r *preservationReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	for {
		if r.file == nil {
			if len(r.names) == 0 {
				return 0, io.EOF
			}
			f, err := r.root.Open(r.names[0])
			if err != nil {
				return 0, err
			}
			r.file, r.names = f, r.names[1:]
		}
		n, err := r.file.Read(p)
		if err == io.EOF {
			err = r.file.Close()
			r.file = nil
			if n == 0 && err == nil {
				continue
			}
		}
		return n, err
	}
}
func (r *preservationReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var err error
	if r.file != nil {
		err = r.file.Close()
	}
	return errors.Join(err, r.root.Close())
}

func (c Client) preserveTaskOutputs(ctx context.Context, j Job, path string) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	src := &preservationReader{root: root}
	defer src.Close()
	for _, name := range slices.Sorted(maps.Keys(j.LocalOutputs.Files)) {
		src.names = append(src.names, filepath.Join(j.ID, strconv.Itoa(j.Attempt), name))
	}
	res, err := c.fileRequest(ctx, "PUT", "/api/task-outputs/"+url.PathEscape(j.ID)+"/preservation", src, map[string]string{"X-Myce-Delivery": j.Delivery.ManifestSHA256, "X-Myce-Session": j.Session, "X-Myce-Machine": j.Machine}, int64(manifestDiskBytes(j.LocalOutputs.Files)))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fileTransferError(res, "destination preservation refused")
	}
	return nil
}
