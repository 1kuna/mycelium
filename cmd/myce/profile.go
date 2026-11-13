package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

type executionProfile struct {
	myce.Spec   `yaml:",inline"`
	Source      profileSource   `yaml:"source"`
	Check       []string        `yaml:"check"`
	MemoryClass string          `yaml:"memory_class,omitempty"`
	Adapter     bool            `yaml:"-"`
	Native      *myce.Extension `yaml:"-"`
}

type profileReceipt struct {
	PendingNotification *profileNotification `json:"pending_notification,omitempty"`
	PathDiagnostic      *myce.PathDiagnostic `json:"path_diagnostic,omitempty"`
	SubmissionReused    bool                 `json:"submission_reused"`
	Nonce               string               `json:"nonce"`
	ArmNonce            string               `json:"arm_nonce,omitempty"`
	Key                 string               `json:"key"`
	Project             string               `json:"project"`
	Intent              string               `json:"intent"`
	JobID               string               `json:"job_id,omitempty"`
	Status              string               `json:"status"`
	Observation         string               `json:"observation"`
	Delivery            string               `json:"delivery"`
	ObservationError    string               `json:"observation_error,omitempty"`
	DeliveryStatus      string               `json:"delivery_status,omitempty"`
	FilesExpired        bool                 `json:"files_expired,omitempty"`
	RefusalCode         string               `json:"refusal_code,omitempty"`
	RefusalReason       string               `json:"refusal_reason,omitempty"`
	ResultReference     myce.FileManifest    `json:"result_reference,omitempty"`
	Spec                myce.Spec            `json:"spec"`
	Uploads             map[string]string    `json:"uploads"`
}

type profileObserver struct {
	Queue   string `json:"queue,omitempty"`
	Config  string `json:"config,omitempty"`
	Joining string `json:"joining,omitempty"`
	Notify  string `json:"notify,omitempty"`
}

func profileHash(value any) string {
	b, _ := json.Marshal(value)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

var profileJSONSync = (*os.File).Sync

func saveProfileJSON(path string, value any) error {
	return saveAtomicProfileJSON(path, value, true)
}

// Diagnostic snapshots need atomic replacement, but are reconstructible. Arms,
// receipts and delivery acknowledgements retain file and directory durability.
func saveAtomicProfileJSON(path string, value any, durable bool) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if durable {
		err = profileJSONSync(f)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	if !durable {
		return nil
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return profileJSONSync(dir)
}
func readProfileJSON[T any](path string, value *T) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var next T
	if err := json.Unmarshal(b, &next); err != nil {
		return err
	}
	*value = next
	return nil
}

func profileLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// submitLockWait bounds waiting for another local submission's preparation
// when the caller set no deadline. The holder keeps the lock through source
// freezing, input upload and submit, so the bound leaves room for large
// uploads; it exists so a hung holder ends in a typed refusal, not a silent
// indefinite wait.
const submitLockWait = 30 * time.Minute

// submitLockPoll is how often a waiter retries the non-blocking lock; flock
// has no cancellable blocking form.
const submitLockPoll = 50 * time.Millisecond

func profileHolderLabel(p executionProfile, key string) string {
	label := "myce run pid " + strconv.Itoa(os.Getpid())
	if p.Name != "" {
		label += " (" + p.Name
		if key != "" {
			label += ", key " + key
		}
		label += ")"
	} else if key != "" {
		label += " (key " + key + ")"
	}
	return label
}

// waitSubmitLock takes the checkout's submission lock, waiting while another
// submission prepares. The wait ends at the earliest of the context deadline,
// the job deadline and submitLockWait; only then is the caller refused with
// submission_busy, naming the recorded holder.
func waitSubmitLock(ctx context.Context, path string, jobDeadline *time.Time, holder string, errOut io.Writer) (*os.File, error) {
	bound := time.Now().Add(submitLockWait)
	if d, ok := ctx.Deadline(); ok && d.Before(bound) {
		bound = d
	}
	if jobDeadline != nil && jobDeadline.Before(bound) {
		bound = *jobDeadline
	}
	announced := false
	for {
		lock, err := profileLock(path)
		if err == nil {
			// Record the holder for waiters; the lock is the fd's flock, not the bytes.
			if lock.Truncate(0) == nil {
				_, _ = lock.WriteAt([]byte(holder+" since "+time.Now().UTC().Format(time.RFC3339)), 0)
			}
			return lock, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, err
		}
		current := "another local submission"
		if b, e := os.ReadFile(path); e == nil && len(bytes.TrimSpace(b)) > 0 {
			current = string(bytes.TrimSpace(b))
		}
		remaining := time.Until(bound)
		if remaining <= 0 {
			return nil, waitError(75, "submission_busy: this checkout's submission lock is still held by "+current+"; retry once it finishes")
		}
		if !announced {
			fmt.Fprintf(errOut, "waiting for this checkout's submission lock, held by %s (up to %s)\n", current, remaining.Round(time.Second))
			announced = true
		}
		t := time.NewTimer(min(submitLockPoll, remaining))
		select {
		case <-ctx.Done():
			t.Stop()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, waitError(75, "submission_busy: this checkout's submission lock is still held by "+current+"; retry once it finishes")
			}
			return nil, ctx.Err()
		case <-t.C:
		}
	}
}

// Typed failures always print the recoverable receipt. Only explicit safe
// diagnostics may become the failure reason; raw transport errors stay local.
// Qualified-target and path refusals use the queue's sanitized diagnostics.
// Path refusals may echo caller-authored paths in this private receipt.
func profileFailure(out io.Writer, r *profileReceipt, code int, reason string) error {
	if r != nil {
		_ = json.NewEncoder(out).Encode(r)
	}
	return waitError(code, reason)
}

type profileOverrides struct{ Class, Deadline, Platform *string }

func runProfile(ctx context.Context, name string, args []string, itemInputs map[string]string, key, notify, config, joining, address, token string, out, errOut io.Writer, overrides ...profileOverrides) error {
	p, root, data, err := readProfile(name)
	if err != nil {
		return err
	}
	return runPreparedProfile(ctx, root, p, data, args, itemInputs, key, notify, config, joining, address, token, out, errOut, overrides...)
}

// readProfile locates, strictly decodes and validates a profile; run and
// spec check --resolve share it.
func readProfile(name string) (executionProfile, string, []byte, error) {
	var p executionProfile
	cwd, err := os.Getwd()
	if err != nil {
		return p, "", nil, err
	}
	path := name
	if !strings.ContainsAny(name, "/\\") && !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
		path = filepath.Join(".mycelium", "profiles", name+".yaml")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return p, "", nil, err
	}
	root := filepath.Dir(path)
	if filepath.Base(root) == "profiles" && filepath.Base(filepath.Dir(root)) == ".mycelium" {
		root = filepath.Dir(filepath.Dir(root))
	} else {
		root = cwd
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return p, "", nil, waitError(64, "profile_invalid: "+myce.PathReadError("profile", "submitter", name, "submitter-working-directory", "", err).Error())
	}
	if len(data) > 1<<20 {
		return p, "", nil, waitError(64, "profile_invalid: profile exceeds 1 MiB")
	}
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err = d.Decode(&p); err != nil {
		return p, "", nil, waitError(64, "profile_invalid: invalid profile YAML")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return p, "", nil, waitError(64, "profile_invalid: expected one YAML profile")
	}
	if err = foldAuthoredAdvanced(path, &p.Spec); err != nil {
		return p, "", nil, waitError(64, "profile_invalid: "+err.Error())
	}
	if len(p.Check) > 0 && p.Check[0] == "" || len(p.Run.Command) == 0 || p.Project == "" {
		return p, "", nil, waitError(64, "profile_invalid: declare run.command and project; check, class, needs and targets are optional")
	}
	if p.Class == "" && p.Priority == "" {
		p.Class = "experiment"
	}
	if reflect.ValueOf(p.Needs).IsZero() {
		p.Needs = defaultEntryNeeds()
	}
	if p.Run.AsksForDocker() {
		return p, "", nil, waitError(64, "profile_invalid: "+myce.ErrDockerRetired.Error())
	}
	if p.Extension != "" || p.Kind != "" || p.Program != nil || p.Execution != nil || p.Run.Workdir != "" || len(p.Run.Sidecars) > 0 || len(p.Files) > 0 || p.SubmissionKey != "" {
		return p, "", nil, waitError(64, "profile_invalid: profile uses plain commands in an isolated source folder")
	}
	if p.MemoryClass != "" && p.MemoryClass != "per_item" {
		return p, "", nil, waitError(64, "profile_invalid: memory_class must be per_item or omitted")
	}
	return p, root, data, nil
}

// attachEnvironmentLock uploads a relative inline lock and its companions,
// read from the project root like inputs. Absolute paths stay queue-side reads.
func attachEnvironmentLock(s *myce.Spec, root string) error {
	e := &s.Environment
	if e.LockFile == "" || filepath.IsAbs(e.LockFile) || e.Files != nil {
		return nil
	}
	files, err := myce.ReadEnvironmentLock(e.Backend, e.LockFile, filepath.Join(root, e.LockFile), e.LockFile, "submitter", "project-root")
	if err != nil {
		return err
	}
	e.Files = files
	return nil
}

func (p executionProfile) placementAdvisories() []string {
	var variants []myce.ExtensionVariant
	if p.Native != nil {
		variants = p.Native.Variants
	}
	return myce.PlacementAdvisories(p.Spec, p.RequirementReasons, variants...)
}

func runPreparedProfile(ctx context.Context, root string, p executionProfile, data []byte, args []string, itemInputs map[string]string, key, notify, config, joining, address, token string, out, errOut io.Writer, overrides ...profileOverrides) error {
	var override profileOverrides
	if len(overrides) > 0 {
		override = overrides[0]
	}
	if override.Class != nil {
		if *override.Class == "" {
			return waitError(64, "class must not be empty")
		}
		p.Class = *override.Class
		if p.Priority != "" {
			// Match submit's legacy normalization: built-in classes use their
			// matching priority; custom classes have no legacy priority.
			switch p.Class {
			case "production", "deadline", "experiment":
				p.Priority = p.Class
			default:
				p.Priority = ""
			}
		}
	}
	if override.Deadline != nil {
		d, err := time.Parse(time.RFC3339, *override.Deadline)
		if err != nil {
			return waitError(64, "deadline must be RFC3339")
		}
		p.Deadline = &d
	}
	if p.Native == nil && len(p.Targets) == 0 && !myce.WantsCommandDiscovery(p.Spec) && entryProduction(p.Spec) {
		return waitError(64, "profile_invalid: "+productionTargetsRequired)
	}
	for _, advice := range p.placementAdvisories() {
		fmt.Fprintln(errOut, "placement advisory: "+advice)
	}
	var err error
	itemRefs := myce.FileManifest{}
	for dest, local := range itemInputs {
		authored := local
		if !profileInputName(dest) || local == "" {
			return waitError(64, "profile_input_invalid: invalid per-submission input")
		}
		if _, exists := p.Inputs[dest]; exists {
			return waitError(64, "profile_input_invalid: destination overlaps a profile input")
		}
		if !filepath.IsAbs(local) {
			local = filepath.Join(root, local)
		}
		itemRefs[dest], err = myce.IdentifyFile(local)
		if err != nil {
			return waitError(64, "profile_input_invalid: "+myce.PathReadError("inputs."+dest, "submitter", authored, "project-root", "", err).Error())
		}
	}
	// A profile declares its inputs as local file mappings; they lower to the
	// existing immutable files manifest. Outputs retain their ordinary job names.
	intent := profileHash(struct {
		Profile string
		Args    []string
	}{string(data), args})
	if override.Class != nil || override.Deadline != nil {
		intent = profileHash(struct {
			Intent, Class string
			Deadline      *time.Time
		}{intent, p.Class, p.Deadline})
	}
	// Uploaded lock bytes are intent: an in-place edit is a new submission, and
	// reusing an explicit key with changed bytes conflicts.
	if err = attachEnvironmentLock(&p.Spec, root); err != nil {
		return waitError(64, "profile_input_invalid: "+err.Error())
	}
	if len(p.Environment.Files) > 0 {
		intent = profileHash(struct {
			Intent      string
			Environment map[string][]byte
		}{intent, p.Environment.Files})
	}
	if len(itemRefs) > 0 {
		// Local filenames are not identity: identical bytes at another path adopt.
		intent = profileHash(struct {
			Intent string
			Inputs myce.FileManifest
		}{intent, itemRefs})
	}
	receipts := filepath.Join(root, ".mycelium", "receipts")
	if err = os.MkdirAll(receipts, 0700); err != nil {
		return err
	}
	// Serializing local profile preparation also protects identical generated keys.
	// Concurrent submissions from one checkout wait their turn (#1383).
	lock, err := waitSubmitLock(ctx, filepath.Join(receipts, "submit.lock"), p.Deadline, profileHolderLabel(p, key), errOut)
	if err != nil {
		return err
	}
	defer lock.Close()
	stages, err := filepath.Glob(filepath.Join(receipts, ".prepare-*"))
	if err != nil {
		return err
	}
	for _, stage := range stages {
		if err = os.RemoveAll(stage); err != nil {
			return err
		}
	}
	var r profileReceipt
	var dir string
	if key != "" {
		if !regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`).MatchString(key) {
			return waitError(64, "profile_invalid: invalid submission key")
		}
		dir = filepath.Join(receipts, profileHash([]string{p.Project, key}))
		err = readProfileJSON(filepath.Join(dir, "receipt.json"), &r)
		if err == nil && r.Intent != intent {
			return waitError(65, "submission_key_conflict: local intent differs; use a new key")
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	var sourceIdentity string
	inputRefs := myce.FileManifest{}
	commonRefs := myce.FileManifest{}
	sharedRefs := myce.FileManifest{}
	if r.Key == "" {
		p.Source, sourceIdentity, err = identifyProfileSource(ctx, root, p.Source)
		if err != nil {
			return waitError(64, "profile_source_invalid: source unavailable or dirty HEAD; commit it or declare allow_dirty")
		}
		for dest, local := range p.Inputs {
			authored := local
			if !profileInputName(dest) {
				return waitError(64, "profile_invalid: invalid input name")
			}
			if !filepath.IsAbs(local) {
				local = filepath.Join(root, local)
			}
			inputRefs[dest], err = myce.IdentifyFile(local)
			if err != nil {
				return waitError(64, "profile_input_invalid: "+myce.PathReadError("inputs."+dest, "submitter", authored, "project-root", "", err).Error())
			}
		}
		for dest, ref := range inputRefs {
			// Per-piece bytes differ by piece; keep learned-memory identity common.
			if !strings.HasPrefix(dest, myce.PlainPiecePrefix) {
				commonRefs[dest] = ref
			}
		}
		if p.Inputs == nil {
			p.Inputs = map[string]string{}
		}
		for dest, local := range itemInputs {
			p.Inputs[dest] = local
			inputRefs[dest] = itemRefs[dest]
		}
		for name := range inputRefs {
			for parent := filepath.Dir(name); parent != "."; parent = filepath.Dir(parent) {
				if _, exists := inputRefs[parent]; exists {
					return waitError(64, "profile_input_invalid: input destinations overlap")
				}
			}
		}
		for name, input := range p.SharedInputs {
			if input.Local != "" {
				if input.NAS != "" || len(input.Manifest) > 0 {
					return waitError(64, "profile_input_invalid: local shared input cannot also use NAS")
				}
				local := input.Local
				if !filepath.IsAbs(local) {
					local = filepath.Join(root, local)
				}
				ref, e := myce.IdentifyFile(local)
				if e != nil {
					return waitError(64, "profile_input_invalid: cannot identify shared input")
				}
				if input.SHA256 != "" && input.SHA256 != ref.SHA256 {
					return waitError(64, "profile_input_invalid: shared input "+name+": sha256 differs")
				}
				if input.SizeBytes != 0 && input.SizeBytes != ref.SizeBytes {
					return waitError(64, "profile_input_invalid: shared input "+name+": size differs")
				}
				input.SHA256, input.SizeBytes = ref.SHA256, ref.SizeBytes
				p.SharedInputs[name] = input
				sharedRefs[name] = ref
			}
		}
		if key == "" {
			key = "profile-" + profileHash(struct {
				Intent, Source string
				Inputs         myce.FileManifest
			}{intent, sourceIdentity, inputRefs})
			if len(p.SharedInputs) > 0 {
				key = "profile-" + profileHash(struct {
					Base   string
					Shared map[string]myce.SharedInput
				}{key, p.SharedInputs})
			}
		}
		dir = filepath.Join(receipts, profileHash([]string{p.Project, key}))
		err = readProfileJSON(filepath.Join(dir, "receipt.json"), &r)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if r.Key == "" {
		stage, err := os.MkdirTemp(receipts, ".prepare-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(stage)
		source := filepath.Join(stage, "source.tar")
		ref, err := freezeProfileSource(ctx, root, p.Source, source)
		if err != nil {
			return waitError(64, "profile_source_invalid: cannot snapshot declared source")
		}
		_, afterIdentity, err := identifyProfileSource(ctx, root, p.Source)
		if err != nil || afterIdentity != sourceIdentity {
			return waitError(65, "profile_source_changed: source changed while freezing")
		}
		s := p.Spec
		s.AuthoringOrigin = myce.OriginProfile
		if p.Adapter {
			s.AuthoringOrigin = myce.OriginAdapter
		}
		s.SubmissionKey = ""
		s.RequireTarget = p.Native == nil
		s.Files = myce.FileManifest{".myce-source.tar": ref}
		// All newly frozen profiles prepare source and input overlays on the worker
		// before prelaunch. Existing receipts retain their frozen wire spec.
		uploadPrefix := "files/"
		uploads := map[string]string{uploadPrefix + ".myce-source.tar": "source.tar"}
		for dest, local := range p.Inputs {
			if !profileRelative(dest) || dest == ".myce-source.tar" {
				return waitError(64, "profile_invalid: invalid input name")
			}
			if !filepath.IsAbs(local) {
				local = filepath.Join(root, local)
			}
			inputRef := inputRefs[dest]
			var e error
			frozen := filepath.Join(stage, "input-"+profileHash(dest))
			if e = copyProfileInput(local, frozen); e != nil {
				return e
			}
			actual, e := myce.IdentifyFile(frozen)
			if e != nil || actual != inputRef {
				return waitError(65, "profile_input_changed: input changed while freezing")
			}
			stored := ".myce-inputs/" + profileHash(dest)
			uploads[uploadPrefix+stored] = filepath.Base(frozen)
			s.Files[stored] = inputRef
		}
		for name, input := range p.SharedInputs {
			if input.Local == "" {
				continue
			}
			local := input.Local
			if !filepath.IsAbs(local) {
				local = filepath.Join(root, local)
			}
			frozen := filepath.Join(stage, "shared-"+profileHash(name))
			if e := copyProfileInput(local, frozen); e != nil {
				return e
			}
			actual, e := myce.IdentifyFile(frozen)
			if e != nil || actual != sharedRefs[name] {
				return waitError(65, "profile_input_changed: shared input changed while freezing")
			}
			input.Local = ""
			s.SharedInputs[name] = input
			uploads["shared/"+name] = filepath.Base(frozen)
		}
		if p.Native != nil {
			e, err := freezeAdapterExtension(*p.Native, source)
			if err != nil {
				return waitError(64, "adapter_invalid: "+err.Error())
			}
			p.Native = &e
			s.Extension, s.ExtensionDigest = e.Name+"@"+e.Version, e.Digest
			if err = saveProfileJSON(filepath.Join(stage, "extension.json"), e); err != nil {
				return err
			}
		}
		s.ProfileMemory = profileHash(struct {
			Run    myce.RunSpec
			Check  []string
			Inputs myce.FileManifest
			Class  string
		}{p.Run, p.Check, commonRefs, p.MemoryClass})
		// Shared read-only data is common to every item. Keep its identity separate
		// from private destinations and include it in measured memory comparability.
		if len(s.SharedInputs) > 0 {
			s.ProfileMemory = profileHash(struct {
				Base   string
				Shared map[string]myce.SharedInput
			}{s.ProfileMemory, s.SharedInputs})
		}
		if p.Execution != nil {
			s.ProfileMemory = profileHash(struct {
				Base      string
				Execution *myce.ExecutionSpec
			}{s.ProfileMemory, p.Execution})
		}
		if p.MemoryClass == "per_item" {
			s.ProfileMemory = profileHash(struct {
				Base   string
				Args   []string
				Inputs myce.FileManifest
			}{s.ProfileMemory, args, itemRefs})
		}
		s.Inputs = nil
		command := append(append([]string{}, p.Run.Command...), args...)
		// Reuse native staging: extract once, then replace source paths with
		// explicit inputs before linkage checks and before any member launches.
		s.PrepareSource = true
		for dest := range p.Inputs {
			stored := ".myce-inputs/" + profileHash(dest)
			s.Files[dest], uploads["files/"+dest] = s.Files[stored], uploads["files/"+stored]
			delete(s.Files, stored)
			delete(uploads, "files/"+stored)
		}
		if p.Native != nil {
			s.ProfileMemory = ""
			s.Args = append(s.Args, args...)
			s.Run = myce.RunSpec{}
		} else if s.Execution != nil {
			s.Execution.Main.Command = checkedAdapterCommand(append(s.Execution.Main.Command, args...), p.Check)
		} else {
			s.Run.Command = checkedAdapterCommand(command, p.Check)
		}

		s.SubmissionKey = key
		if err := myce.ValidateTargetPortability(s); err != nil {
			return waitError(64, "profile_invalid: "+err.Error())
		}
		if err = s.Validate(); err != nil {
			return waitError(64, "profile_invalid: "+err.Error())
		}
		dir = filepath.Join(receipts, profileHash([]string{p.Project, key}))
		err = readProfileJSON(filepath.Join(dir, "receipt.json"), &r)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if r.Key == "" {
			r = profileReceipt{Key: key, Nonce: rand.Text(), Project: p.Project, Intent: intent, Status: "intent", Observation: "not_armed", Delivery: "not_configured", Spec: s, Uploads: uploads}
			if err = saveProfileJSON(filepath.Join(stage, "receipt.json"), r); err != nil {
				return err
			}
			if err = os.Rename(stage, dir); err != nil {
				return err
			}
			parent, e := os.Open(receipts)
			if e != nil {
				return e
			}
			err = parent.Sync()
			parent.Close()
			if err != nil {
				return err
			}
		}
	}
	if token == "" {
		token = os.Getenv("MYCE_TOKEN")
	}
	authority := runAuthority{Config: config, Joining: joining}
	c, err := authority.client(address, token)
	if err != nil {
		return profileFailure(out, &r, 69, "queue_unavailable: cannot resolve queue authority; intent retained")
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	if r.JobID == "" {
		capacityFailure := func(operation, input string, cause error) error {
			var h *myce.HTTPError
			if errors.As(cause, &h) && (h.Code == "file_store_capacity" || h.Code == "project_file_store_quota") {
				r.RefusalCode, r.RefusalReason = h.Code, h.Message
				if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
					return err
				}
				return profileFailure(out, &r, 75, h.Code+": "+h.Message)
			}
			if c.Token != "" {
				input = strings.ReplaceAll(input, c.Token, "[redacted]")
			}
			detail := fmt.Sprintf("operation=%s input=%q %s", operation, input, myce.ClientErrorCause(cause))
			if c.Token != "" {
				detail = strings.ReplaceAll(detail, c.Token, "[redacted]")
			}
			return profileFailure(out, &r, 69, "queue_unavailable: input storage unconfirmed; "+detail+"; intent retained; rerunning the same command resumes from the receipt without duplicating anything")
		}
		ctx = myce.WithFileStoreSubmission(ctx, r.Spec)
		operation, input := "preflight", profileInputNames(r, p, itemInputs)
		preflightDone := false
		uploaded := map[string]bool{}
		err = c.RetryTransient(ctx, func(attempt context.Context, one myce.Client) error {
			if !preflightDone {
				operation, input = "preflight", profileInputNames(r, p, itemInputs)
				if e := one.PreflightFileStore(attempt, r.Spec.StoredInputFiles()); e != nil {
					return e
				}
				preflightDone = true
			}
			for name, file := range r.Uploads {
				if uploaded[name] {
					continue
				}
				operation, input = "upload", profileStorageInputName(name, p, itemInputs)
				// Each retry rechecks the content hash and durable server offset.
				if e := one.UploadJobInput(attempt, filepath.Join(dir, file), profileUploadRef(r.Spec, name)); e != nil {
					return e
				}
				uploaded[name] = true
			}
			return nil
		}, func(cause error, delay time.Duration) {
			if c.Token != "" {
				input = strings.ReplaceAll(input, c.Token, "[redacted]")
			}
			fmt.Fprintf(errOut, "run: retrying input storage operation=%s input=%q %s; next attempt in %s; same intent retained\n", operation, input, myce.ClientErrorCause(cause), delay)
		})
		if err != nil {
			return capacityFailure(operation, input, err)
		}
		if r.Spec.Extension != "" {
			var e myce.Extension
			if err = readProfileJSON(filepath.Join(dir, "extension.json"), &e); err != nil {
				return err
			}
			var registration myce.ExtensionRegistration
			if err = c.Call(ctx, "POST", "/api/extensions", e, &registration); err != nil {
				return profileFailure(out, &r, 69, "queue_unavailable: frozen adapter registration unconfirmed; intent retained")
			}
			for _, warning := range registration.Warnings {
				fmt.Fprintln(errOut, "portability advisory: "+warning)
			}
		}
		j, e := c.Submit(ctx, r.Spec)
		if e != nil {
			var h *myce.HTTPError
			if errors.As(e, &h) && h.Code == "project_file_store_quota" {
				return capacityFailure("submit", profileInputNames(r, p, itemInputs), e)
			}
			overloaded := errors.As(e, &h) && (h.Status == 429 || h.Status >= 500)
			if errors.As(e, &h) && !overloaded {
				if h.Code == "no_qualified_target" || h.Code == "job_environment_invalid" && h.PathDiagnostic != nil {
					r.PathDiagnostic = h.PathDiagnostic
					r.RefusalCode, r.RefusalReason = h.Code, h.Message
					if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
						return err
					}
					return profileFailure(out, &r, 78, h.Code+": "+h.Message)
				}
				return profileFailure(out, &r, 65, "submission_rejected: queue rejected the saved intent")
			}
			// Name the cause (overload, stall, transport) so a timeout is not mistaken for an outage (#1016).
			return profileFailure(out, &r, 69, "queue_unavailable: submission unconfirmed; retry the saved key (cause: "+e.Error()+")")
		}
		r.JobID, r.Status = j.ID, j.State
		r.SubmissionReused = j.SubmissionReused
		r.RefusalCode, r.RefusalReason = "", ""
		r.PathDiagnostic = nil
		if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
			return err
		}
	}
	// The verified queue store owns these bytes after JobID is durable. Keep
	// only intent/hashes locally; an uncertain submission still retains uploads.
	if len(r.Uploads) > 0 {
		for _, file := range r.Uploads {
			if filepath.Base(file) != file {
				return errors.New("invalid saved upload path")
			}
			if err = os.Remove(filepath.Join(dir, file)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		r.Uploads = nil
		if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
			return err
		}
	}
	// Receipts can outlive the store's recovery window. Refresh the retained
	// answer even when terminal notification was already delivered.
	if r.Status == "succeeded" || r.Status == "failed" || r.Status == "cancelled" {
		current, err := c.GetJob(ctx, r.JobID)
		if err != nil {
			return profileFailure(out, &r, 69, "queue_unavailable: retained job unavailable; terminal receipt retained")
		}
		r.Status, r.FilesExpired = current.State, current.FilesExpired
		if r.FilesExpired {
			if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
				return err
			}
			return profileFailure(out, &r, 65, "result_expired: stored job files exceeded the queue retention window")
		}
	}
	// Observer configuration is private caller state, never part of the job.
	observer := profileObserver{Queue: c.URL, Config: authority.Config, Joining: authority.Joining, Notify: notify}
	observerToken := ""
	if token != "" && authority.Joining == "" {
		// Direct credentials cannot be reopened from an installed authority.
		observerToken = c.Token
	}
	if err = armProfileObserver(ctx, dir, &r, observer, observerToken, errOut); err != nil {
		return profileFailure(out, &r, 74, "wakeup_not_armed: job retained; check notifier configuration and retry")
	}
	return json.NewEncoder(out).Encode(r)
}

func copyProfileInput(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	return errors.Join(err, out.Sync(), out.Close())
}

func profileUploadRef(s myce.Spec, name string) myce.FileRef {
	if s.PrepareSource {
		return s.StoredInputFiles()[name]
	}
	if input, ok := s.SharedInputs[strings.TrimPrefix(name, "shared/")]; strings.HasPrefix(name, "shared/") && ok {
		return myce.FileRef{SHA256: input.SHA256, SizeBytes: input.SizeBytes}
	}
	return s.Files[name]
}

// Receipt keys use hashed destinations for legacy profiles; report the authored
// input name rather than its frozen local filename or opaque storage key.
func profileStorageInputName(name string, p executionProfile, itemInputs map[string]string) string {
	name = strings.TrimPrefix(name, "files/")
	if name == ".myce-source.tar" {
		return "source"
	}
	for _, inputs := range []map[string]string{p.Inputs, itemInputs} {
		for dest := range inputs {
			if name == ".myce-inputs/"+profileHash(dest) {
				return dest
			}
		}
	}
	return name
}
func profileInputNames(r profileReceipt, p executionProfile, itemInputs map[string]string) string {
	names := make([]string, 0, len(r.Spec.StoredInputFiles()))
	for name := range r.Spec.StoredInputFiles() {
		names = append(names, profileStorageInputName(name, p, itemInputs))
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}
