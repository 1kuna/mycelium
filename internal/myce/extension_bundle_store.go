package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// Bundle maps use the same canonical JSON as Extension.Digest. The existing
// file store owns immutable bytes; queue transactions carry only FileRefs.
// Callers hold fileLifecycle.RLock from publication through reference commit.
func (s *FileStore) freezeFiles(files *map[string][]byte, ref **FileRef, digest string, verified map[FileRef]bool, contexts ...context.Context) error {
	if len(*files) == 0 {
		return nil
	}
	data, err := json.Marshal(*files)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	next := FileRef{SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(data))}
	if digest != "" && digest != next.SHA256 {
		return errors.New("extension bundle digest mismatch")
	}
	if *ref != nil && **ref != next {
		return errors.New("bundle reference mismatch")
	}
	// Startup holds the publication fence for the whole pass. Verify every
	// embedded copy's digest above, but repeat disk durability only once per blob.
	if verified[next] {
		*files, *ref = nil, &next
		return nil
	}
	// Share tus's per-identity fence: an existing PATCH must not complete
	// import promotion between our missing check and eligibility enrollment.
	// Creation precedes the identity lock, matching tus POST-with-upload order.
	// Neither lock holds q.mu; an active transfer can still finish publication.
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, err := s.uploadLocker.NewLock(next.SHA256)
	if err != nil {
		return err
	}
	if err = lock.Lock(context.Background(), func() {}); err != nil {
		return err
	}
	defer lock.Unlock()
	err = s.verify(context.Background(), next)
	missing := os.IsNotExist(err)
	if err != nil {
		status, payloads, e := s.capacity(requestContext(contexts))
		if e != nil {
			status.Error = "file store capacity unavailable"
		}
		if payloads == nil {
			payloads = map[string]storedPayload{}
		}
		// Replacement of corrupt bytes can grow an existing allocation too.
		growth := uint64(next.SizeBytes) - min(uint64(next.SizeBytes), payloads[next.SHA256].length)
		payloads[next.SHA256] = storedPayload{length: uint64(next.SizeBytes)}
		need := addDiskBytes(growth, s.reservedOutputs(payloads))
		if e = s.admit(requestContext(contexts), status, payloads, FileManifest{"bundle": next}, need, growth); e != nil {
			return e
		}
	}
	if s.recordBundle != nil {
		if e := s.recordBundle(next.SHA256, missing, requestContext(contexts)); e != nil {
			return e
		}
	}
	if err != nil {
		// A private temporary name cannot truncate or consume an interrupted tus
		// upload of the same bytes. Preserve any existing verified blob.
		f, e := os.CreateTemp(filepath.Join(s.root, "uploads"), ".bundle-")
		if e != nil {
			return e
		}
		defer os.Remove(f.Name())
		_, writeErr := f.Write(data)
		if e = errors.Join(writeErr, f.Sync(), f.Close()); e != nil {
			return e
		}
		if missing {
			if e = os.Link(f.Name(), s.blob(next)); e != nil && !os.IsExist(e) {
				return e
			}
		} else {
			// The embedded bytes have passed the digest/reference checks above.
			// Replace an unreadable or corrupt stored copy atomically; never
			// truncate a blob that another reader may already have open.
			if e = os.Rename(f.Name(), s.blob(next)); e != nil {
				return e
			}
		}
	}
	if err = s.verify(context.Background(), next); err != nil {
		return err
	}
	if err = s.refreshBlob(s.blob(next)); err != nil {
		return err
	}
	if err = s.syncDir(filepath.Join(s.root, "blobs")); err != nil {
		return err
	}
	if verified != nil {
		verified[next] = true
	}
	*files, *ref = nil, &next
	return nil
}

func (s *FileStore) freezeRecipe(r *EnvironmentRecipe, verified map[FileRef]bool, contexts ...context.Context) error {
	return s.freezeFiles(&r.Files, &r.FilesRef, "", verified, contexts...)
}

func (s *FileStore) freezeRecipes(recipes map[string]EnvironmentRecipe, verified map[FileRef]bool, contexts ...context.Context) error {
	for platform, r := range recipes {
		if err := s.freezeRecipe(&r, verified, contexts...); err != nil {
			return err
		}
		recipes[platform] = r
	}
	return nil
}
func (s *FileStore) freezeExtension(e *Extension, contexts ...context.Context) error {
	if err := s.freezeFiles(&e.Files, &e.FilesRef, e.Digest, nil, contexts...); err != nil {
		return err
	}
	for i := range e.Variants {
		for name, r := range e.Variants[i].Environments {
			if err := s.freezeRecipe(&r, nil, contexts...); err != nil {
				return err
			}
			e.Variants[i].Environments[name] = r
		}
	}
	return nil
}
func (s *FileStore) hydrateFiles(files *map[string][]byte, ref *FileRef) error {
	if ref == nil {
		return nil
	}
	if err := ref.validate(); err != nil {
		return err
	}
	data, err := os.ReadFile(s.blob(*ref))
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if int64(len(data)) != ref.SizeBytes || hex.EncodeToString(sum[:]) != ref.SHA256 {
		return errors.New("bundle size or digest mismatch")
	}
	if err = json.Unmarshal(data, files); err != nil {
		return err
	}
	return nil
}
func (s *FileStore) hydrateRecipe(r *EnvironmentRecipe) error {
	if err := s.hydrateFiles(&r.Files, r.FilesRef); err != nil {
		return err
	}
	r.FilesRef = nil // Keep hydrated transport/identity identical to the frozen input.
	return nil
}
func (s *FileStore) hydrateExtension(e *Extension) error {
	if e.FilesRef != nil && e.FilesRef.SHA256 != e.Digest {
		return errors.New("extension bundle digest mismatch")
	}
	if err := s.hydrateFiles(&e.Files, e.FilesRef); err != nil {
		return err
	}
	e.FilesRef = nil
	for i := range e.Variants {
		for name, r := range e.Variants[i].Environments {
			if err := s.hydrateRecipe(&r); err != nil {
				return err
			}
			e.Variants[i].Environments[name] = r
		}
	}
	return nil
}
func (s *FileStore) hydrateJob(j *Job) error {
	if j == nil {
		return nil
	}
	for i := range j.Extensions {
		if err := s.hydrateExtension(&j.Extensions[i]); err != nil {
			return err
		}
	}
	for name, r := range j.EnvironmentRecipes {
		if err := s.hydrateRecipe(&r); err != nil {
			return err
		}
		j.EnvironmentRecipes[name] = r
	}
	for name, recipes := range j.SidecarEnvironmentRecipes {
		for key, r := range recipes {
			if err := s.hydrateRecipe(&r); err != nil {
				return err
			}
			recipes[key] = r
		}
		j.SidecarEnvironmentRecipes[name] = recipes
	}
	return nil
}
func (q *Queue) hydrateReply(reply *Reply, requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	requestUnlock1 := requestReadLock(requestCtx, &q.mu)
	store := q.files
	// Plans can share maps or recipe pointers with frozen state. Own the whole
	// response before loading any bytes; assignments already carry small refs.
	data, err := json.Marshal(reply)
	requestUnlock1()
	if err != nil {
		return err
	}
	var out Reply
	if err = json.Unmarshal(data, &out); err != nil {
		return err
	}
	*reply = out
	var failedJobs []*Job
	var failedPlans []HostLaunch
	plans := reply.EnvironmentPlan[:0]
	for _, r := range reply.EnvironmentPlan {
		if err := store.hydrateRecipe(&r); err != nil {
			log.Printf("check-in environment delivery deferred key=%s: %v", r.Key, err)
			if r.Launch != nil {
				failedPlans = append(failedPlans, *r.Launch)
			}
			continue
		}
		plans = append(plans, r)
	}
	reply.EnvironmentPlan = plans
	hydrate := func(j **Job) {
		if *j == nil {
			return
		}
		if err := store.hydrateJob(*j); err != nil {
			log.Printf("check-in assignment delivery failed job=%s attempt=%d: %v", (*j).ID, (*j).Attempt, err)
			failedJobs = append(failedJobs, *j)
			*j = nil
		}
	}
	hydrate(&reply.Job)
	for i := range reply.Attempts {
		hydrate(&reply.Attempts[i].Job)
	}
	models := reply.ModelPlan[:0]
	for _, slot := range reply.ModelPlan {
		if r := slot.EnvironmentRecipe; r != nil {
			if err := store.hydrateRecipe(r); err != nil {
				log.Printf("check-in model delivery deferred model=%s: %v", slot.Model, err)
				if slot.State == "loading" {
					continue
				}
				// Unload/control decisions do not need preparation bytes.
				slot.EnvironmentRecipe = nil
			}
		}
		models = append(models, slot)
	}
	reply.ModelPlan = models
	if len(failedJobs) == 0 && len(failedPlans) == 0 {
		return nil
	}
	return q.updateRequest(requestCtx, func() error {
		for _, l := range failedPlans {
			if current, ok := q.s.PreparationPlans[l.Attempt]; ok && !current.Started && current.Machine == l.Machine && current.Session == l.Session && current.Key == l.Key && current.Kind == l.Kind {
				delete(q.s.PreparationPlans, l.Attempt)
			}
		}
		for _, offered := range failedJobs {
			j, err := q.current(Result{Execution: Execution{ID: offered.ID, Attempt: offered.Attempt}, Machine: offered.Machine, Session: offered.Session})
			if err != nil {
				continue
			} // Never stop a replacement execution.
			// An older response might already have reached the worker. Preserve
			// custody until exact release acknowledgement, using ordinary stops.
			q.stop(j, "assignment files unavailable")
			a := AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Stop: true, Kill: expired(j.StopAt, q.clock.Now()), GraceSeconds: j.StopAt.Sub(q.clock.Now()).Seconds(), PriorityStop: priorityStop(j), UserCancel: cancelled(j)}
			if reply.AttemptProtocol == "" {
				reply.Stop, reply.Kill, reply.GraceSeconds, reply.PriorityStop, reply.UserCancel = a.Stop, a.Kill, a.GraceSeconds, a.PriorityStop, a.UserCancel
			} else {
				for i := range reply.Attempts {
					if reply.Attempts[i].ID == a.ID && reply.Attempts[i].Attempt == a.Attempt && reply.Attempts[i].Session == a.Session {
						reply.Attempts[i] = a
					}
				}
			}
		}
		return nil
	}, nil)
}

// Startup only, before serving traffic: publish and verify every blob first,
// then commit the small references together. A crash before the SQLite commit
// leaves embedded state authoritative; re-opening safely repeats publication.
func (q *Queue) migrateBundleFiles() error {
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	// Enroll all new bytes in one transaction before changing any embedded
	// record. Per-blob commits here would repeatedly copy the old large state.
	if err := q.prepareBundleUploads(); err != nil {
		return err
	}
	changed := false
	checked := map[FileRef]bool{}
	verify := func(ref *FileRef) error {
		if ref == nil || checked[*ref] {
			return nil
		}
		if err := q.files.verify(context.Background(), *ref); err != nil {
			return err
		}
		checked[*ref] = true
		return nil
	}
	recipe := func(r *EnvironmentRecipe) error {
		if len(r.Files) > 0 {
			if err := q.files.freezeRecipe(r, checked); err != nil {
				return err
			}
			changed = true
		}
		return verify(r.FilesRef)
	}
	extension := func(e *Extension) error {
		if len(e.Files) > 0 {
			if err := q.files.freezeFiles(&e.Files, &e.FilesRef, e.Digest, checked); err != nil {
				return err
			}
			changed = true
		}
		if e.FilesRef != nil && e.FilesRef.SHA256 != e.Digest {
			return errors.New("extension bundle digest mismatch")
		}
		if err := verify(e.FilesRef); err != nil {
			return err
		}
		for i := range e.Variants {
			for name, r := range e.Variants[i].Environments {
				if err := recipe(&r); err != nil {
					return err
				}
				e.Variants[i].Environments[name] = r
			}
		}
		return nil
	}
	job := func(j *Job) error {
		if j == nil {
			return nil
		}
		for i := range j.Extensions {
			if err := extension(&j.Extensions[i]); err != nil {
				return err
			}
		}
		for name, r := range j.EnvironmentRecipes {
			if err := recipe(&r); err != nil {
				return err
			}
			j.EnvironmentRecipes[name] = r
		}
		for name, recipes := range j.SidecarEnvironmentRecipes {
			for key, r := range recipes {
				if err := recipe(&r); err != nil {
					return err
				}
				recipes[key] = r
			}
			j.SidecarEnvironmentRecipes[name] = recipes
		}
		return nil
	}
	for name, e := range q.s.Extensions {
		if err := extension(&e); err != nil {
			return fmt.Errorf("migrate frozen bundle: %w", err)
		}
		q.s.Extensions[name] = e
	}
	for _, j := range q.s.Jobs {
		if err := job(j); err != nil {
			return fmt.Errorf("migrate job bundle: %w", err)
		}
	}
	for _, r := range q.s.Submissions {
		if err := job(r.Job); err != nil {
			return fmt.Errorf("migrate keyed bundle: %w", err)
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.EnvironmentRecipe != nil {
			if err := recipe(slot.EnvironmentRecipe); err != nil {
				return err
			}
		}
	}
	if !changed {
		return nil
	}
	return q.update(func() error {
		for id := range q.s.Jobs {
			q.touchJob(id)
		}
		for id := range q.s.Submissions {
			q.touchSubmission(id)
		}
		return nil
	})
}

// Startup only. Hashing and disk checks run before the small eligibility commit.
func (q *Queue) prepareBundleUploads() error {
	created := map[string]bool{}
	files := func(data map[string][]byte) error {
		if len(data) == 0 {
			return nil
		}
		encoded, err := json.Marshal(data)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(encoded)
		hash := hex.EncodeToString(sum[:])
		if _, marked := q.s.BundleUploads[hash]; marked {
			return nil
		}
		if _, err := os.Lstat(q.files.blob(FileRef{SHA256: hash})); os.IsNotExist(err) {
			created[hash] = true
		} else if err != nil {
			return err
		}
		return nil
	}
	extension := func(e Extension) error {
		if err := files(e.Files); err != nil {
			return err
		}
		for _, v := range e.Variants {
			for _, r := range v.Environments {
				if err := files(r.Files); err != nil {
					return err
				}
			}
		}
		return nil
	}
	job := func(j *Job) error {
		if j == nil {
			return nil
		}
		for _, e := range j.Extensions {
			if err := extension(e); err != nil {
				return err
			}
		}
		for _, r := range j.EnvironmentRecipes {
			if err := files(r.Files); err != nil {
				return err
			}
		}
		for _, recipes := range j.SidecarEnvironmentRecipes {
			for _, r := range recipes {
				if err := files(r.Files); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, e := range q.s.Extensions {
		if err := extension(e); err != nil {
			return err
		}
	}
	for _, j := range q.s.Jobs {
		if err := job(j); err != nil {
			return err
		}
	}
	for _, record := range q.s.Submissions {
		if err := job(record.Job); err != nil {
			return err
		}
	}
	for _, slot := range q.s.ModelSlots {
		if r := slot.EnvironmentRecipe; r != nil {
			if err := files(r.Files); err != nil {
				return err
			}
		}
	}
	if len(created) == 0 {
		return nil
	}
	return q.update(func() error {
		for hash := range created {
			q.markBundleUpload(hash)
		}
		return nil
	})
}

// q.mu held. Retained history and keyed answers own bundles independently of
// result-file expiry. Both P3-2 sweep passes recheck this set at their revision
// fence before unlinking; publication holds the matching lifecycle read lock.
func (q *Queue) bundleReferences(subset ...map[string]*Job) map[string]bool {
	refs := map[string]bool{}
	add := func(ref *FileRef) {
		if ref != nil {
			refs[ref.SHA256] = true
		}
	}
	extension := func(e Extension) {
		add(e.FilesRef)
		for _, v := range e.Variants {
			for _, r := range v.Environments {
				add(r.FilesRef)
			}
		}
	}
	job := func(j *Job) {
		if j == nil {
			return
		}
		for _, e := range j.Extensions {
			extension(e)
		}
		for _, r := range j.EnvironmentRecipes {
			add(r.FilesRef)
		}
		for _, recipes := range j.SidecarEnvironmentRecipes {
			for _, r := range recipes {
				add(r.FilesRef)
			}
		}
	}
	for _, e := range q.s.Extensions {
		extension(e)
	}
	for _, recipes := range q.environments.Environments {
		for _, r := range recipes {
			add(r.FilesRef)
		}
	}
	jobs := q.s.Jobs
	if len(subset) > 0 {
		jobs = subset[0]
	}
	for _, j := range jobs {
		job(j)
	}
	if len(subset) == 0 {
		for _, r := range q.s.Submissions {
			job(r.Job)
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.EnvironmentRecipe != nil {
			add(slot.EnvironmentRecipe.FilesRef)
		}
	}
	return refs
}
