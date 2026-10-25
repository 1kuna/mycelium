package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Bundle maps use the same canonical JSON as Extension.Digest. The existing
// file store owns immutable bytes; queue transactions carry only FileRefs.
// Callers hold fileLifecycle.RLock from publication through reference commit.
func (s *FileStore) freezeFiles(files *map[string][]byte, ref **FileRef, digest string, verified map[FileRef]bool) error {
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
	if err = s.verify(context.Background(), next); err != nil {
		missing := os.IsNotExist(err)
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

func (s *FileStore) freezeRecipe(r *EnvironmentRecipe, verified map[FileRef]bool) error {
	return s.freezeFiles(&r.Files, &r.FilesRef, "", verified)
}

func (s *FileStore) freezeRecipes(recipes map[string]EnvironmentRecipe, verified map[FileRef]bool) error {
	for platform, r := range recipes {
		if err := s.freezeRecipe(&r, verified); err != nil {
			return err
		}
		recipes[platform] = r
	}
	return nil
}
func (s *FileStore) freezeExtension(e *Extension) error {
	if err := s.freezeFiles(&e.Files, &e.FilesRef, e.Digest, nil); err != nil {
		return err
	}
	for i := range e.Variants {
		for name, r := range e.Variants[i].Environments {
			if err := s.freezeRecipe(&r, nil); err != nil {
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
func (q *Queue) hydrateReply(reply *Reply) error {
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	q.mu.RLock()
	store := q.files
	// Plans can share maps or recipe pointers with frozen state. Own the whole
	// response before loading any bytes; assignments already carry small refs.
	data, err := json.Marshal(reply)
	q.mu.RUnlock()
	if err != nil {
		return err
	}
	var out Reply
	if err = json.Unmarshal(data, &out); err != nil {
		return err
	}
	*reply = out
	for i := range reply.EnvironmentPlan {
		if err = store.hydrateRecipe(&reply.EnvironmentPlan[i]); err != nil {
			return err
		}
	}
	if err = store.hydrateJob(reply.Job); err != nil {
		return err
	}
	for i := range reply.Attempts {
		if err = store.hydrateJob(reply.Attempts[i].Job); err != nil {
			return err
		}
	}
	for i := range reply.ModelPlan {
		if r := reply.ModelPlan[i].EnvironmentRecipe; r != nil {
			if err = store.hydrateRecipe(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// Startup only, before serving traffic: publish and verify every blob first,
// then commit the small references together. A crash before the SQLite commit
// leaves embedded state authoritative; re-opening safely repeats publication.
func (q *Queue) migrateBundleFiles() error {
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
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
	return q.update(func() error { return nil })
}

// q.mu held. Retained history and keyed answers own bundles independently of
// result-file expiry. Both P3-2 sweep passes recheck this set at their revision
// fence before unlinking; publication holds the matching lifecycle read lock.
func (q *Queue) bundleReferences() map[string]bool {
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
	for _, j := range q.s.Jobs {
		job(j)
	}
	for _, r := range q.s.Submissions {
		job(r.Job)
	}
	for _, slot := range q.s.ModelSlots {
		if slot.EnvironmentRecipe != nil {
			add(slot.EnvironmentRecipe.FilesRef)
		}
	}
	return refs
}
