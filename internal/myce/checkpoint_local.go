package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
)

const localFirstProtocol = "checkpoint-local-first-v1"
const generationMarker = "generation.json"
const checkpointAck = ".myce-accepted.json"

func (s Spec) localFirst() bool { return s.CheckpointPolicy == "local_first" }
func checkpointRefusal(code, message string) error {
	return &HTTPError{Status: 400, Code: code, Message: message}
}
func validateLocalFirst(s Spec) error {
	if s.CheckpointPolicy == "" {
		return nil
	}
	if !s.localFirst() || !s.Resumable || s.extensionJob() || s.FanOut != nil || s.After != nil || s.WorkerData != "" {
		return rejectFieldRule("checkpoint_policy_unsupported", errors.New("local_first v1 requires an individual resumable plain command and the cooperative JSON generation protocol"))
	}
	return nil
}

type checkpointGeneration struct {
	Protocol   string         `json:"protocol"`
	Checkpoint FileCheckpoint `json:"myce_checkpoint"`
}

// v1 deliberately admits one self-contained JSON payload, bounded by the
// existing 1 MiB metadata contract. Binary/multi-file saves need a separately
// qualified allocation/custody protocol; never fall back to directory capture.
func decodeGeneration(data []byte) (checkpointGeneration, error) {
	var g checkpointGeneration
	if len(data) > 1<<20 || json.Unmarshal(data, &g) != nil || g.Protocol != localFirstProtocol || g.Checkpoint.Generation <= 0 || len(g.Checkpoint.Files) != 1 || g.Checkpoint.Files.validate() != nil {
		return g, checkpointRefusal("checkpoint_generation_invalid", "expected a complete local-first-v1 generation manifest")
	}
	for name, ref := range g.Checkpoint.Files {
		if name != fmt.Sprintf("generation-%d.json", g.Checkpoint.Generation) || ref.SizeBytes > 1<<20 {
			return g, checkpointRefusal("checkpoint_protocol_unsupported", "v1 supports one self-contained JSON payload of at most 1 MiB")
		}
	}
	return g, nil
}

func verifyLocalFirstStore(store *FileStore, save FileCheckpoint) error {
	ref, ok := save.Files[generationMarker]
	if !ok || ref.SizeBytes > 1<<20 {
		return checkpointRefusal("checkpoint_generation_invalid", "generation manifest missing")
	}
	data, err := os.ReadFile(store.blob(ref))
	if err != nil {
		return err
	}
	g, err := decodeGeneration(data)
	if err != nil {
		return err
	}
	expected := g.Checkpoint.Files
	expected[generationMarker] = ref
	if save.Generation != g.Checkpoint.Generation || !reflect.DeepEqual(save.Files, expected) {
		return checkpointRefusal("checkpoint_generation_invalid", "generation does not match immutable manifest")
	}
	// Check the payload's own sequence, not only the manifest's claim.
	for name, ref := range g.Checkpoint.Files {
		if name == generationMarker {
			continue
		}
		data, err = os.ReadFile(store.blob(ref))
		if err != nil {
			return err
		}
		var payload struct {
			Meta struct {
				Generation int64 `json:"generation"`
			} `json:"myce_checkpoint"`
			Value json.RawMessage `json:"value"`
		}
		if json.Unmarshal(data, &payload) != nil || payload.Meta.Generation != save.Generation || payload.Value == nil {
			return checkpointRefusal("checkpoint_generation_invalid", "payload generation mismatch")
		}
	}
	return nil
}

func captureLocalFirst(root, parent string, observers ...*storageObserver) (stage string, save FileCheckpoint, err error) {
	marker := filepath.Join(root, generationMarker)
	st, err := os.Lstat(marker)
	if os.IsNotExist(err) {
		entries, e := os.ReadDir(root)
		if e != nil && !os.IsNotExist(e) {
			return "", save, e
		}
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) == ".json" {
				return "", save, checkpointRefusal("checkpoint_protocol_unsupported", "save must publish generation.json; legacy directory saves are not captured")
			}
		}
		return "", FileCheckpoint{Generation: -1}, nil
	}
	if err != nil {
		return "", save, err
	}
	if !st.Mode().IsRegular() || st.Size() > 1<<20 {
		return "", save, checkpointRefusal("checkpoint_generation_invalid", "invalid generation marker")
	}
	data, err := readLocalGeneration(root, generationMarker, 1<<20)
	if err != nil {
		return "", save, err
	}
	g, err := decodeGeneration(data)
	if err != nil {
		return "", save, err
	}
	stage, err = os.MkdirTemp(parent, ".file-checkpoint-")
	if err != nil {
		return "", save, err
	}
	defer func() {
		for _, o := range observers {
			o.sample()
		}
		if err != nil {
			_ = os.RemoveAll(stage)
		}
	}()
	save = g.Checkpoint
	// Only named, immutable bytes are read; unrelated partial or mutable files
	// can never enter the transport. Recheck marker and payload identities.
	for name, ref := range save.Files {
		payload, e := readLocalGeneration(root, name, ref.SizeBytes)
		if e != nil {
			err = e
			return stage, save, err
		}
		if err = writeGenerationCopy(filepath.Join(stage, name), payload); err != nil {
			return stage, save, err
		}
		if err = verifyFile(context.Background(), filepath.Join(stage, name), ref); err != nil {
			return stage, save, err
		}
	}
	if err = writeGenerationCopy(filepath.Join(stage, generationMarker), data); err != nil {
		return stage, save, err
	}
	if err = syncModelDirectory(stage); err != nil {
		return stage, save, err
	}
	ref, err := IdentifyFile(filepath.Join(stage, generationMarker))
	if err != nil {
		return stage, save, err
	}
	if err = verifyFile(context.Background(), marker, ref); err != nil {
		return stage, save, err
	}
	save.Files[generationMarker] = ref
	return stage, save, nil
}

func writeLocalFirstAck(j Job, save FileCheckpoint) error {
	data, _ := json.Marshal(struct {
		Protocol   string  `json:"protocol"`
		Generation int64   `json:"generation"`
		Marker     FileRef `json:"marker"`
	}{localFirstProtocol, save.Generation, save.Files[generationMarker]})
	root := attemptCheckpoint(j)
	f, err := os.CreateTemp(root, ".accepted-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(root, checkpointAck)); err != nil {
		return err
	}
	return syncWorkerDir(root)
}

// CheckpointMirror retains accepted-generation custody independently of compute
// and later attempts. The output publisher owns all NAS syscalls and receipts.
type CheckpointMirror struct {
	Checkpoint FileCheckpoint  `json:"checkpoint"`
	Delivery   *OutputDelivery `json:"delivery"`
	Prefix     string          `json:"prefix"`
}

func (q *Queue) acceptCheckpointMirror(j *Job, p checkpointPublication) error {
	if q.fileCheckpoints {
		return nil
	} // No configured NAS: queue is authoritative.
	key := strconv.Itoa(p.Attempt) + "-" + strconv.FormatInt(p.Checkpoint.Generation, 10)
	if prior := j.CheckpointMirrors[key]; prior != nil {
		if !reflect.DeepEqual(prior.Checkpoint, p.Checkpoint) || prior.Delivery.Machine != p.Machine || prior.Delivery.Session != p.Session {
			return ErrStale
		}
		return nil
	}
	rel, err := filepath.Rel(q.root, j.Checkpoint)
	if err != nil || !safeRelative(filepath.ToSlash(rel)) {
		return ErrStale
	}
	prefix := filepath.ToSlash(filepath.Join(rel, "local-first", strconv.FormatInt(p.Checkpoint.Generation, 10)))
	if j.CheckpointMirrors == nil {
		j.CheckpointMirrors = map[string]*CheckpointMirror{}
	}
	m := &CheckpointMirror{Checkpoint: p.Checkpoint, Prefix: prefix, Delivery: &OutputDelivery{Kind: localFirstProtocol, Generation: p.Checkpoint.Generation, DestinationID: deliveryRootID(q.root), Execution: p.Execution, Machine: p.Machine, Session: p.Session, State: "pending", PendingAt: q.clock.Now()}}
	view := checkpointMirrorJob(j, m)
	m.Delivery.ManifestSHA256 = deliveryDigest(*view)
	j.CheckpointMirrors[key] = m
	return nil
}
func checkpointMirrorJob(j *Job, m *CheckpointMirror) *Job {
	outputs := map[string]string{}
	for name := range m.Checkpoint.Files {
		outputs[name] = m.Prefix
	}
	return &Job{ID: j.ID, Attempt: m.Delivery.Attempt, Machine: m.Delivery.Machine, Session: m.Delivery.Session, State: "succeeded", Spec: Spec{OutputDelivery: asyncNAS, Outputs: outputs}, OutputRefs: m.Checkpoint.Files, Delivery: m.Delivery}
}
func checkpointMirrorsPending(j *Job) bool {
	for _, m := range j.CheckpointMirrors {
		if m.Delivery.State != "verified" {
			return true
		}
	}
	return false
}
func (q *Queue) deliveryJobs() []*Job {
	var jobs []*Job
	for _, j := range q.liveJobs() {
		if deliveryPending(j) {
			jobs = append(jobs, j)
		}
		for _, m := range j.CheckpointMirrors {
			if m.Delivery.State != "verified" {
				jobs = append(jobs, checkpointMirrorJob(j, m))
			}
		}
	}
	return jobs
}
func (q *Queue) touchDelivery(snapshot *Job) *Job { return q.deliveryView(snapshot, true) }
func (q *Queue) deliveryView(snapshot *Job, touch bool) *Job {
	j := q.s.Jobs[snapshot.ID]
	if touch {
		j = q.touchJob(snapshot.ID)
	}
	if j == nil || snapshot.Delivery == nil || snapshot.Delivery.Kind != localFirstProtocol {
		return j
	}
	key := strconv.Itoa(snapshot.Attempt) + "-" + strconv.FormatInt(snapshot.Delivery.Generation, 10)
	m := j.CheckpointMirrors[key]
	if m == nil {
		return nil
	}
	return checkpointMirrorJob(j, m)
}
func (q *Queue) deliveryPath(j *Job) string {
	if j.Delivery != nil && j.Delivery.Kind == localFirstProtocol {
		return q.root
	}
	return q.deliveryRoot
}

func localFirstProgress(root string) (int64, error) {
	data, err := readLocalGeneration(root, generationMarker, 1<<20)
	if os.IsNotExist(err) {
		return -1, nil
	}
	if err != nil {
		return -1, err
	}
	g, err := decodeGeneration(data)
	if err != nil {
		return -1, err
	}
	return g.Checkpoint.Generation, nil
}

// Bound the actual read, not just a racy metadata size. Published v1 files are
// single basenames; os.Root confines a replacement symlink, and SameFile plus
// Lstat refuses it before interpreting content.
func readLocalGeneration(root, name string, limit int64) ([]byte, error) {
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	st, err := r.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > limit {
		return nil, checkpointRefusal("checkpoint_generation_invalid", "generation file must be regular and fit its manifest")
	}
	f, err := r.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(st, opened) {
		return nil, checkpointRefusal("checkpoint_generation_invalid", "generation file changed during capture")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, checkpointRefusal("checkpoint_generation_invalid", "generation grew during capture")
	}
	return data, nil
}
func writeGenerationCopy(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return errors.Join(err, f.Sync(), f.Close())
}
