package myce

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// SharedInput names reproducible read-only data, independent of an attempt.
// NAS names are relative to the worker's checkpoint root. Local is profile-only.
type SharedInput struct {
	NAS           string       `json:"nas,omitempty" yaml:"nas,omitempty"`
	Local         string       `json:"-" yaml:"local,omitempty"`
	SHA256        string       `json:"sha256,omitempty" yaml:"sha256,omitempty"`
	SizeBytes     int64        `json:"size_bytes,omitempty" yaml:"size_bytes,omitempty"`
	Archive       string       `json:"archive,omitempty" yaml:"archive,omitempty"`
	UnpackedBytes int64        `json:"unpacked_bytes,omitempty" yaml:"unpacked_bytes,omitempty"`
	Manifest      FileManifest `json:"manifest,omitempty" yaml:"manifest,omitempty"`
}

func (s SharedInput) ref() FileRef { return FileRef{s.SHA256, s.SizeBytes} }
func (s SharedInput) key() string {
	// Source locations are not identity: identical bytes share across NAS aliases.
	return identityHash(struct {
		Ref      FileRef
		Archive  string
		Manifest FileManifest
	}{s.ref(), s.Archive, s.Manifest})
}
func (s SharedInput) bytes() int64 {
	if len(s.Manifest) > 0 {
		var n int64
		for _, f := range s.Manifest {
			n += f.SizeBytes
		}
		return n
	}
	if s.Archive != "" {
		return s.UnpackedBytes
	}
	return s.SizeBytes
}
func (s SharedInput) validate() error {
	if s.Local != "" || s.NAS != "" && !safeRelative(s.NAS) {
		return errors.New("shared input requires a NAS-relative path or stored hash")
	}
	if len(s.Manifest) > 0 {
		if s.NAS == "" || s.Archive != "" || s.SHA256 != "" || s.SizeBytes != 0 || s.UnpackedBytes != 0 {
			return errors.New("tree manifest requires only a NAS path and file manifest")
		}
		if err := s.Manifest.validate(); err != nil {
			return err
		}
	} else {
		if err := s.ref().validate(); err != nil {
			return err
		}
		if s.Archive != "" && s.Archive != "tar" && s.Archive != "tar.gz" {
			return errors.New("shared archive must be tar or tar.gz")
		}
		if s.Archive != "" && s.UnpackedBytes <= 0 || s.Archive == "" && s.UnpackedBytes != 0 {
			return errors.New("shared archive requires positive unpacked_bytes")
		}
	}
	if s.bytes() < 0 {
		return errors.New("shared input size overflow")
	}
	return nil
}
func sharedStoredFiles(s Spec) FileManifest {
	files := FileManifest{}
	for n, input := range s.SharedInputs {
		if input.NAS == "" {
			files["shared/"+n] = input.ref()
		}
	}
	return files
}
func submissionFiles(s Spec) FileManifest {
	files := sharedStoredFiles(s)
	for n, ref := range s.Files {
		files["files/"+n] = ref
	}
	return files
}
func validateSharedInputs(s Spec) error {
	if len(s.SharedInputs) == 0 {
		return nil
	}
	if s.Run.Docker || s.Run.Workdir != "" || s.extensionJob() {
		return errors.New("shared inputs require plain native commands in an isolated work folder")
	}
	for n, input := range s.SharedInputs {
		if n == "" || strings.Trim(n, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_") != "" {
			return errors.New("shared input names must use letters, digits and underscores")
		}
		for other := range s.SharedInputs {
			if other != n && strings.EqualFold(n, other) {
				return errors.New("shared input environment names collide")
			}
		}
		if err := input.validate(); err != nil {
			return fmt.Errorf("shared input %s: %w", n, err)
		}
	}
	return nil
}
func (w Worker) acquireSharedInputs(ctx context.Context, j Job) (map[string]string, func(), error) {
	env := map[string]string{}
	releases := []func(){}
	release := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	for n, input := range j.Spec.SharedInputs {
		if w.sharedInputs == nil {
			release()
			return nil, nil, errors.New("shared input cache unavailable")
		}
		path, done, err := w.sharedInputs.Acquire(ctx, input, w.CheckpointRoot)
		if err != nil {
			release()
			return nil, nil, err
		}
		releases = append(releases, done)
		env["MYCE_INPUT_"+strings.ToUpper(n)] = path
	}
	return env, release, nil
}

// StoredInputFiles returns the queue-backed input references for upload/preflight.
func (s Spec) StoredInputFiles() FileManifest { return submissionFiles(s) }
