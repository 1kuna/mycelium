package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FileCache contains only reproducible store copies. Attempt folders hold private
// copies, so commands cannot mutate shared blobs and cache eviction cannot race use.
type FileCache struct {
	Root   string
	Limit  int64
	Clock  Clock
	Client Client
	mu     sync.Mutex
}

func (c *FileCache) copyTo(ctx context.Context, ref FileRef, dest string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ref.validate(); err != nil {
		return err
	}
	if c.Limit <= 0 || ref.SizeBytes > c.Limit {
		return errors.New("file exceeds local cache limit")
	}
	if err := os.MkdirAll(c.Root, 0700); err != nil {
		return err
	}
	root, err := filepath.EvalSymlinks(c.Root)
	if err != nil {
		return err
	}
	p := filepath.Join(root, ref.SHA256)
	if verifyFile(ctx, p, ref) == nil {
		now := c.Clock.Now()
		if err = os.Chtimes(p, now, now); err != nil {
			return err
		}
		return copyJobFile(p, dest)
	}
	// Never replace corrupt cache bytes as if they were complete. They are disposable
	// only after the durable authoritative copy has been checked.
	if err = c.Client.HasFile(ctx, ref); err != nil {
		return err
	}
	if _, err = os.Lstat(p); err == nil {
		if err = os.Remove(p); err != nil {
			return err
		}
	}
	partial := p + ".partial"
	type victim struct {
		name string
		size int64
		used int64
	}
	var victims []victim
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	used := int64(0)
	prefix := int64(0)
	for _, entry := range entries {
		info, e := entry.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return errors.New("invalid cache entry")
		}
		used += info.Size()
		if entry.Name() == ref.SHA256+".partial" {
			prefix = info.Size()
			continue
		}
		victims = append(victims, victim{entry.Name(), info.Size(), info.ModTime().UnixNano()})
	}
	if prefix > ref.SizeBytes {
		return errors.New("file partial exceeds declared size")
	}
	free, err := diskFree(root)
	if err != nil {
		return err
	}
	need := ref.SizeBytes - prefix
	sort.Slice(victims, func(i, j int) bool {
		if victims[i].used == victims[j].used {
			return victims[i].name < victims[j].name
		}
		return victims[i].used < victims[j].used
	})
	for _, v := range victims {
		if used+need <= c.Limit && free >= need {
			break
		}
		hash := strings.TrimSuffix(v.name, ".partial")
		if (FileRef{SHA256: hash}).validate() != nil {
			return errors.New("unknown file in cache; refusing eviction")
		}
		if strings.HasSuffix(v.name, ".partial") {
			// Partial downloads are not unique progress: the complete store copy exists.
			res, e := c.Client.fileRequest(ctx, "HEAD", "/api/files/blobs/"+hash, nil, nil)
			if e != nil {
				return e
			}
			if e = fileResponse(res, 200); e != nil {
				return e
			}
		} else if e := c.Client.HasFile(ctx, FileRef{hash, v.size}); e != nil {
			return e
		}
		if err = os.Remove(filepath.Join(root, v.name)); err != nil {
			return err
		}
		used -= v.size
		free, err = diskFree(root)
		if err != nil {
			return err
		}
	}
	if used+need > c.Limit || free < need {
		return errors.New("insufficient local file cache space")
	}
	if err = c.Client.DownloadFile(ctx, ref, partial); err != nil {
		return err
	}
	if err = os.Rename(partial, p); err != nil {
		return err
	}
	if err = syncWorkerDir(root); err != nil {
		return err
	}
	now := c.Clock.Now()
	if err = os.Chtimes(p, now, now); err != nil {
		return err
	}
	return copyJobFile(p, dest)
}

func validateJobFiles(s Spec) error {
	if err := s.Files.validate(); err != nil {
		return err
	}
	if len(s.Files) > 0 && (len(s.Inputs) > 0 || s.Run.Workdir != "" || s.Run.Docker) {
		return errors.New("stored files use a private working folder; cannot mix NAS inputs, workdir or Docker")
	}
	all := FileManifest{}
	for n, f := range s.Files {
		all[n] = f
	}
	for n, p := range s.Outputs {
		if !safeRelative(n) || !safeRelative(p) {
			return errors.New("outputs require clean relative names and prefixes")
		}
		if _, ok := all[n]; ok {
			return errors.New("input and output names overlap")
		}
		all[n] = FileRef{SHA256: strings.Repeat("0", 64)}
	}
	return all.validate()
}
