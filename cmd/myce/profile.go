package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"

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
	SubmissionReused bool              `json:"submission_reused"`
	Nonce            string            `json:"nonce"`
	ArmNonce         string            `json:"arm_nonce,omitempty"`
	Key              string            `json:"key"`
	Project          string            `json:"project"`
	Intent           string            `json:"intent"`
	JobID            string            `json:"job_id,omitempty"`
	Status           string            `json:"status"`
	Observation      string            `json:"observation"`
	Delivery         string            `json:"delivery"`
	ObservationError string            `json:"observation_error,omitempty"`
	DeliveryStatus   string            `json:"delivery_status,omitempty"`
	FilesExpired     bool              `json:"files_expired,omitempty"`
	RefusalCode      string            `json:"refusal_code,omitempty"`
	RefusalReason    string            `json:"refusal_reason,omitempty"`
	ResultReference  myce.FileManifest `json:"result_reference,omitempty"`
	Spec             myce.Spec         `json:"spec"`
	Uploads          map[string]string `json:"uploads"`
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

func saveProfileJSON(path string, value any) error {
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
	err = errors.Join(f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func readProfileJSON(path string, value any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, value)
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

// Typed failures always print the recoverable receipt; remote messages, local
// paths and transport diagnostics never become the public failure reason.
// Qualified-target refusals use the queue's sanitized fit diagnostic.
func profileFailure(out io.Writer, r *profileReceipt, code int, reason string) error {
	if r != nil {
		_ = json.NewEncoder(out).Encode(r)
	}
	return waitError(code, reason)
}

func runProfile(ctx context.Context, name string, args []string, itemInputs map[string]string, key, notify, config, joining, address, token string, out, errOut io.Writer) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	path := name
	if !strings.ContainsAny(name, "/\\") && !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
		path = filepath.Join(".mycelium", "profiles", name+".yaml")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	root := filepath.Dir(path)
	if filepath.Base(root) == "profiles" && filepath.Base(filepath.Dir(root)) == ".mycelium" {
		root = filepath.Dir(filepath.Dir(root))
	} else {
		root = cwd
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return waitError(64, "profile_invalid: cannot read profile")
	}
	if len(data) > 1<<20 {
		return waitError(64, "profile_invalid: profile exceeds 1 MiB")
	}
	var p executionProfile
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err = d.Decode(&p); err != nil {
		return waitError(64, "profile_invalid: invalid profile YAML")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return waitError(64, "profile_invalid: expected one YAML profile")
	}
	if len(p.Check) == 0 || p.Check[0] == "" || len(p.Run.Command) == 0 || p.Project == "" || p.Class == "" || len(p.Targets) == 0 {
		return waitError(64, "profile_invalid: declare run.command, project, class, targets and check")
	}
	if p.Extension != "" || p.Kind != "" || p.Program != nil || p.Execution != nil || p.Run.Workdir != "" || p.Run.Docker || len(p.Run.Sidecars) > 0 || len(p.Run.DockerSidecars) > 0 || len(p.Files) > 0 || p.SubmissionKey != "" {
		return waitError(64, "profile_invalid: profile uses plain commands in an isolated source folder")
	}
	if p.MemoryClass != "" && p.MemoryClass != "per_item" {
		return waitError(64, "profile_invalid: memory_class must be per_item or omitted")
	}
	return runPreparedProfile(ctx, root, p, data, args, itemInputs, key, notify, config, joining, address, token, out, errOut)
}

func runPreparedProfile(ctx context.Context, root string, p executionProfile, data []byte, args []string, itemInputs map[string]string, key, notify, config, joining, address, token string, out, errOut io.Writer) error {
	var err error
	itemRefs := myce.FileManifest{}
	for dest, local := range itemInputs {
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
			return waitError(64, "profile_input_invalid: cannot identify per-submission input")
		}
	}
	// A profile declares its inputs as local file mappings; they lower to the
	// existing immutable files manifest. Outputs retain their ordinary job names.
	intent := profileHash(struct {
		Profile string
		Args    []string
	}{string(data), args})
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
	lock, err := profileLock(filepath.Join(receipts, "submit.lock"))
	if err != nil {
		return waitError(75, "submission_busy: another local profile submission is preparing; retry")
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
			if !profileInputName(dest) {
				return waitError(64, "profile_invalid: invalid input name")
			}
			if !filepath.IsAbs(local) {
				local = filepath.Join(root, local)
			}
			inputRefs[dest], err = myce.IdentifyFile(local)
			if err != nil {
				return waitError(64, "profile_input_invalid: cannot identify declared input")
			}
		}
		for dest, ref := range inputRefs {
			commonRefs[dest] = ref
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
				if input.SHA256 != "" && (input.SHA256 != ref.SHA256 || input.SizeBytes != ref.SizeBytes) {
					return waitError(64, "profile_input_invalid: shared input hash differs")
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
		s.SubmissionKey = ""
		s.RequireTarget = p.Native == nil
		s.Files = myce.FileManifest{".myce-source.tar": ref}
		uploads := map[string]string{".myce-source.tar": "source.tar"}
		var inputCommands []string
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
			uploads[stored] = filepath.Base(frozen)
			s.Files[stored] = inputRef
			inputCommands = append(inputCommands, "mkdir -p "+shellArg("./"+filepath.Dir(dest))+"; cp "+shellArg("./"+stored)+" "+shellArg("./"+dest))
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
		if p.MemoryClass == "per_item" {
			s.ProfileMemory = profileHash(struct {
				Base   string
				Args   []string
				Inputs myce.FileManifest
			}{s.ProfileMemory, args, itemRefs})
		}
		s.Inputs = nil
		command := append(append([]string{}, p.Run.Command...), args...)
		// Extraction is confined by freezeProfileSource's link/path checks. Inputs
		// are already in the attempt cwd; no checkout or caller environment is copied.
		slices.Sort(inputCommands)
		if p.Adapter {
			s.PrepareSource = true
			// Inputs stage after source extraction and keep their public filenames.
			for dest := range p.Inputs {
				stored := ".myce-inputs/" + profileHash(dest)
				s.Files[dest], uploads[dest] = s.Files[stored], uploads[stored]
				delete(s.Files, stored)
				delete(uploads, stored)
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
		} else {
			s.Run.Command = []string{"sh", "-ec", "tar -xf .myce-source.tar\n" + strings.Join(inputCommands, "\n") + "\n" + shellCommand(command) + "\n" + shellCommand(p.Check)}
		}

		s.SubmissionKey = key
		if err = s.Validate(); err != nil {
			return waitError(64, "profile_invalid: job fields failed validation")
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
		capacityFailure := func(cause error) error {
			var h *myce.HTTPError
			if errors.As(cause, &h) && h.Code == "file_store_capacity" {
				r.RefusalCode, r.RefusalReason = h.Code, h.Message
				if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
					return err
				}
				return profileFailure(out, &r, 75, h.Code+": "+h.Message)
			}
			return profileFailure(out, &r, 69, "queue_unavailable: input storage unconfirmed; intent retained")
		}
		if err = c.PreflightFileStore(ctx, r.Spec.StoredInputFiles()); err != nil {
			return capacityFailure(err)
		}
		for name, file := range r.Uploads {
			if err = c.UploadJobInput(ctx, filepath.Join(dir, file), profileUploadRef(r.Spec, name)); err != nil {
				return capacityFailure(err)
			}
		}
		if r.Spec.Extension != "" {
			var e myce.Extension
			if err = readProfileJSON(filepath.Join(dir, "extension.json"), &e); err != nil {
				return err
			}
			if err = c.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
				return profileFailure(out, &r, 69, "queue_unavailable: frozen adapter registration unconfirmed; intent retained")
			}
		}
		j, e := c.Submit(ctx, r.Spec)
		if e != nil {
			var h *myce.HTTPError
			if errors.As(e, &h) {
				if h.Code == "no_qualified_target" {
					r.RefusalCode, r.RefusalReason = h.Code, h.Message
					if err = saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
						return err
					}
					return profileFailure(out, &r, 78, h.Code+": "+h.Message)
				}
				return profileFailure(out, &r, 65, "submission_rejected: queue rejected the saved intent")
			}
			return profileFailure(out, &r, 69, "queue_unavailable: submission unconfirmed; retry the saved key")
		}
		r.JobID, r.Status = j.ID, j.State
		r.SubmissionReused = j.SubmissionReused
		r.RefusalCode, r.RefusalReason = "", ""
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
	if input, ok := s.SharedInputs[strings.TrimPrefix(name, "shared/")]; strings.HasPrefix(name, "shared/") && ok {
		return myce.FileRef{SHA256: input.SHA256, SizeBytes: input.SizeBytes}
	}
	return s.Files[name]
}
