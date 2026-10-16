package myce

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func localApprovalFixture(t *testing.T) (*ModelCache, Model, string, string) {
	t.Helper()
	root := t.TempDir()
	snapshot := filepath.Join(root, "snapshots", "revision-placeholder")
	blobs := filepath.Join(root, "blobs")
	if err := os.MkdirAll(snapshot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(blobs, 0700); err != nil {
		t.Fatal(err)
	}
	blob := filepath.Join(blobs, "blob-placeholder")
	if err := os.WriteFile(blob, []byte("weight bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../blobs/blob-placeholder", filepath.Join(snapshot, "weights.bin")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshot, "config.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	approvals := filepath.Join(t.TempDir(), "approvals")
	model, err := ApproveLocalModel(context.Background(), approvals, "approved", snapshot, root)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	model.Name = "test"
	model.Engine = "server"
	model.MemoryGB = 1
	model.Platforms = []string{"fake"}
	model.StartCommand = []string{"placeholder", "{model_path}"}
	model.HealthURL = "http://127.0.0.1:{port}/health"
	cache := &ModelCache{ApprovalRoot: approvals, Root: filepath.Join(t.TempDir(), "cache")}
	return cache, model, canonical, blob
}
func TestEntryLocalApprovalUsesOriginalFolderAndInvalidates(t *testing.T) {
	for _, change := range []string{"file", "blob", "added", "symlink", "inode", "host", "identity", "unapproved"} {
		t.Run(change, func(t *testing.T) {
			c, m, snapshot, blob := localApprovalFixture(t)
			path, err := c.Prepare(context.Background(), m)
			if err != nil || path != snapshot {
				t.Fatal("in-place approval not used", err)
			}
			if _, err := os.Stat(c.Root); !os.IsNotExist(err) {
				t.Fatal("local source touched copy cache")
			}
			if c.ApprovedModels([]Model{m})[m.LocalSource] != m.SHA256 {
				t.Fatal("approved identity not advertised")
			}
			switch change {
			case "file":
				err = os.WriteFile(filepath.Join(snapshot, "config.json"), []byte("changed"), 0600)
			case "blob":
				err = os.WriteFile(blob, []byte("changed weight bytes"), 0600)
			case "added":
				err = os.WriteFile(filepath.Join(snapshot, "new-file"), []byte("new"), 0600)
			case "symlink":
				err = os.Rename(filepath.Join(snapshot, "weights.bin"), filepath.Join(snapshot, "old-link"))
			case "inode":
				path := filepath.Join(snapshot, "config.json")
				info, e := os.Stat(path)
				if e != nil {
					t.Fatal(e)
				}
				replacement := filepath.Join(t.TempDir(), "replacement")
				if e = os.WriteFile(replacement, []byte("{}"), 0600); e != nil {
					t.Fatal(e)
				}
				if e = os.Chtimes(replacement, info.ModTime(), info.ModTime()); e != nil {
					t.Fatal(e)
				}
				err = os.Rename(replacement, path)
			case "host":
				path := filepath.Join(c.ApprovalRoot, "approved.json")
				data, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				var a localModelApproval
				if e = json.Unmarshal(data, &a); e != nil {
					t.Fatal(e)
				}
				a.Host = "another-host-placeholder"
				data, e = json.Marshal(a)
				if e != nil {
					t.Fatal(e)
				}
				err = os.WriteFile(path, data, 0600)
			case "identity":
				m.SHA256 = "different-identity"
			case "unapproved":
				m.LocalSource = "not-approved"
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.Prepare(context.Background(), m); err == nil {
				t.Fatal("changed/unapproved folder accepted")
			}
			if len(c.ApprovedModels([]Model{m})) != 0 {
				t.Fatal("invalid approval advertised")
			}
			if change == "file" || change == "blob" || change == "added" || change == "inode" {
				refreshed, e := ApproveLocalModel(context.Background(), c.ApprovalRoot, m.LocalSource, snapshot, filepath.Dir(filepath.Dir(snapshot)))
				if e != nil {
					t.Fatal(e)
				}
				refreshed.Name = m.Name
				if _, e = c.Prepare(context.Background(), refreshed); e != nil {
					t.Fatal("explicit reapproval failed", e)
				}
			}
		})
	}
}
func TestEntryLocalApprovalRejectsEscapingSymlinksAndMixedSources(t *testing.T) {
	c, m, snapshot, _ := localApprovalFixture(t)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(snapshot, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := ApproveLocalModel(context.Background(), c.ApprovalRoot, "unsafe", snapshot, snapshot); err == nil {
		t.Fatal("escaping blob accepted")
	}
	for _, mode := range []string{"file_store", "direct_nas", "path", "missing-files"} {
		copy := m
		switch mode {
		case "file_store":
			copy.FileStore = true
		case "direct_nas":
			copy.DirectNAS = true
		case "path":
			copy.Path = snapshot
		case "missing-files":
			copy.Files = nil
		}
		if err := validateModels(ModelConfig{Models: []Model{copy}, Idle: 1, Wait: 1}); err == nil {
			t.Fatal("mixed/invalid local source accepted", mode)
		}
	}
}
