package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"time"
)

// The immutable manifest and append-only acknowledgements share the existing
// managed publication lifetime. They are deleted only with exact-attempt local
// cleanup. Restart does metadata checks, not another walk/hash/remote HEAD pass.
type outputPublication struct {
	Owner    fileStoreAttempt  `json:"owner"`
	Queue    string            `json:"queue"`
	Workdir  string            `json:"workdir"`
	Outputs  map[string]string `json:"outputs"`
	Files    FileManifest      `json:"files"`
	Metadata map[string]string `json:"metadata"`
}

func outputMetadata(path string) (string, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() && !st.IsDir() {
		return "", errors.New("output must be a regular file or directory")
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("output identity unavailable")
	}
	v := reflect.ValueOf(stat).Elem()
	ct := v.FieldByName("Ctim")
	if !ct.IsValid() {
		ct = v.FieldByName("Ctimespec")
	}
	if !ct.IsValid() {
		return "", errors.New("output change time unavailable")
	}
	// Like sharedSignature, exclude atime but include ctime and inode: restoring
	// mtime after an in-place write must not make cached hashes authoritative.
	return identityHash([]any{st.Size(), st.Mode(), st.ModTime().UnixNano(), stat.Dev, stat.Ino, ct.Interface()}), nil
}

func (p outputPublication) check(ctx context.Context) error {
	for name, want := range p.Metadata {
		if err := ctx.Err(); err != nil {
			return err
		}
		got, err := outputMetadata(filepath.Join(p.Workdir, name))
		if err != nil {
			return err
		}
		if got != want {
			return errors.New("output changed after publication began")
		}
	}
	return nil
}

func (w Worker) outputPublication(ctx context.Context, j Job, root string, step, outputName *string) (outputPublication, bool, error) {
	p := outputPublication{Owner: fileStoreAttempt{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session}, Queue: w.Client.URL, Workdir: j.Spec.Run.Workdir, Outputs: j.Spec.Outputs}
	data, err := os.ReadFile(filepath.Join(root, "outputs.json"))
	if err == nil {
		var saved outputPublication
		if json.Unmarshal(data, &saved) != nil || saved.Owner != p.Owner || saved.Queue != p.Queue || saved.Workdir != p.Workdir || !maps.Equal(saved.Outputs, p.Outputs) || saved.Files.validate() != nil || validateOutputManifest(p.Outputs, saved.Files) != nil {
			return p, false, errors.New("conflicting output publication snapshot")
		}
		*step, *outputName = "output_validation", ""
		return saved, true, saved.check(ctx)
	}
	if !os.IsNotExist(err) {
		return p, false, err
	}
	directories := map[string]bool{}
	files, terminal, err := w.identifyOutputFiles(ctx, j, step, outputName, directories)
	if err != nil {
		return p, terminal, err
	}
	p.Files, p.Metadata = files, map[string]string{}
	for name := range files {
		directories[name] = true
	}
	for name := range directories {
		for {
			if _, ok := p.Metadata[name]; ok {
				break
			}
			stamp, err := outputMetadata(filepath.Join(p.Workdir, name))
			if err != nil {
				return p, true, err
			}
			p.Metadata[name] = stamp
			if name == "." {
				break
			}
			name = filepath.Dir(name)
		}
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return p, false, err
	}
	if err = saveWorkerJSON(root, "outputs.json", p); err != nil {
		return p, false, err
	}
	return p, false, nil
}

// One indivisible new transfer probes capacity. Start with one, double the
// proven window while there is less than one queued request, then keep the
// measured bandwidth-delay product plus that probe. This bounds outstanding
// work by useful service, rather than a host count or deployment-specific cap.
// A refusal stops dispatch altogether; the caller's existing retry pace owns
// recovery. The pending manifest is the physical upper bound.
type outputUploadWindow struct {
	limit, samples int
	baseline       time.Duration
	service        time.Duration
	started        time.Time
	congested      bool
}

func (w *outputUploadWindow) observe(now time.Time, service time.Duration) {
	if service <= 0 {
		return // No observation cannot justify additional outstanding requests.
	}
	if w.baseline == 0 || service < w.baseline {
		w.baseline = service
	}
	w.samples++
	w.service += service
	if w.samples < w.limit {
		return
	}
	elapsed := now.Sub(w.started)
	if elapsed > 0 {
		rate := float64(w.samples) / float64(elapsed)
		queued := rate * float64(w.service/time.Duration(w.samples)-w.baseline)
		w.congested = w.congested || queued >= 1
		if w.congested {
			w.limit = max(1, int(math.Ceil(rate*float64(w.baseline)))+1)
		} else {
			w.limit *= 2
		}
	}
	w.samples, w.service, w.started = 0, 0, now
}

func openOutputConfirmations(root string, files FileManifest) (*os.File, map[string]bool, error) {
	path := filepath.Join(root, "confirmed")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	data, err := os.ReadFile(path)
	known := map[string]bool{}
	for _, ref := range files {
		known[ref.SHA256] = true
	}
	confirmed := map[string]bool{}
	// Ignore/truncate only a torn final append, never a malformed full record.
	end := bytes.LastIndexByte(data, '\n') + 1
	for _, hash := range strings.Split(string(data[:end]), "\n") {
		if hash == "" {
			continue
		}
		if !known[hash] {
			err = errors.New("conflicting output publication acknowledgement")
			break
		}
		confirmed[hash] = true
	}
	if err == nil {
		err = f.Truncate(int64(end))
	}
	if err == nil {
		_, err = f.Seek(int64(end), 0)
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = syncWorkerDir(root)
	}
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, confirmed, nil
}

func (w Worker) publishOutputFiles(ctx context.Context, j Job, parent string, step, outputName *string) (FileManifest, bool, error) {
	return w.publishOutputFilesAt(ctx, j, parent, filepath.Join(attemptPublicationRoot(j, parent), "outputs"), step, outputName)
}

func (w Worker) publishOutputFilesAt(ctx context.Context, j Job, parent, root string, step, outputName *string) (FileManifest, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	done := w.Client.publication.begin(w.Client.clock(), "filesystem/output_manifest")
	p, terminal, err := w.outputPublication(ctx, j, root, step, outputName)
	done()
	if err != nil {
		if ctx.Err() != nil {
			terminal = false
		}
		return nil, terminal, err
	}
	// Persist newly created managed ancestors too, before any acknowledgement
	// can make a restart skip remote work.
	parentRoot, err := os.OpenRoot(parent)
	if err != nil {
		return nil, false, err
	}
	relative, err := filepath.Rel(parent, root)
	if err == nil {
		err = syncDeliveryAncestors(parentRoot, relative)
	}
	err = errors.Join(err, parentRoot.Close())
	if err != nil {
		return nil, false, err
	}
	journal, confirmed, err := openOutputConfirmations(root, p.Files)
	if err != nil {
		return nil, false, err
	}
	defer journal.Close()
	var pending []string
	seen := map[string]bool{}
	for _, name := range slices.Sorted(maps.Keys(p.Files)) {
		ref := p.Files[name]
		if confirmed[ref.SHA256] {
			w.Client.publication.confirm(ref)
		} else if !seen[ref.SHA256] {
			pending = append(pending, name)
			seen[ref.SHA256] = true
		}
	}
	if len(pending) > 0 && outputPatterns(j.Spec.Outputs) {
		*step, *outputName = "output_preflight", ""
		remaining := FileManifest{}
		for _, name := range pending {
			remaining[name] = p.Files[name]
		}
		if err := w.Client.PreflightFileStore(ctx, remaining); err != nil {
			if len(confirmed) == 0 && fileStoreLimitRefusal(err) {
				return nil, true, outputLimitError{err}
			}
			return nil, false, err
		}
	}
	*step, *outputName = "output_upload", ""
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type completion struct {
		name    string
		service time.Duration
		err     error
	}
	completed := make(chan completion)
	window := outputUploadWindow{limit: 1, started: w.Client.clock().Now()}
	next, active := 0, 0
	for next < len(pending) || active > 0 {
		for err == nil && next < len(pending) && active < window.limit {
			name := pending[next]
			next++
			active++
			go func() {
				started := w.Client.clock().Now()
				err := w.Client.uploadOutputFile(ctx, filepath.Join(p.Workdir, name), p.Files[name])
				completed <- completion{name, w.Client.clock().Now().Sub(started), err}
			}()
		}
		if active == 0 {
			break
		}
		got := <-completed
		active--
		if got.err == nil {
			_, writeErr := journal.WriteString(p.Files[got.name].SHA256 + "\n")
			if writeErr == nil {
				writeErr = journal.Sync()
			}
			got.err = writeErr
			window.observe(w.Client.clock().Now(), got.service)
		}
		if got.err != nil && err == nil {
			err = got.err
			*outputName, _ = outputDeclarationFor(j.Spec.Outputs, got.name)
			cancel() // Drain all senders before returning or reopening the journal.
		}
	}
	if err != nil {
		return nil, false, err
	}
	*step, *outputName = "output_validation", ""
	if err := p.check(ctx); err != nil {
		return nil, ctx.Err() == nil, err
	}
	return p.Files, false, nil
}
