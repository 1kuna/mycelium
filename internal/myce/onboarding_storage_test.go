package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func onboardingNAS(t *testing.T, osname string) (ServiceConfig, func(context.Context, string, ...string) ([]byte, error)) {
	t.Helper()
	home := t.TempDir()
	nas := filepath.Join(home, "mounted nas")
	if err := os.Mkdir(nas, 0700); err != nil {
		t.Fatal(err)
	}
	c, _ := PlanInstall("worker", home, "", nas, "http://queue.invalid", "hypha", Detection{OS: osname, Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
	command := func(_ context.Context, n string, args ...string) ([]byte, error) {
		if osname == "linux" && n == "cat" && len(args) == 1 && args[0] == "/proc/mounts" {
			return []byte("credentials-must-not-print " + strings.ReplaceAll(nas, " ", `\040`) + " nfs4 rw,password=private-value 0 0\n"), nil
		}
		if osname == "darwin" && n == "/sbin/mount" {
			return []byte("private-value on " + nas + " (smbfs, nodev, mounted by user)"), nil
		}
		t.Fatalf("unexpected command: %s %v", n, args)
		return nil, errors.New("unexpected")
	}
	return c, command
}
func TestOnboardingNASRootsAndCheckpointRoundTrip(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		t.Run(osname, func(t *testing.T) {
			c, command := onboardingNAS(t, osname)
			c.Checkpoints = filepath.Join(c.NAS, "existing-checkpoints")
			c.ModelRoot = filepath.Join(c.NAS, "masters")
			c.EnvironmentRoot = filepath.Join(c.NAS, "recipes")
			before := c
			got, err := PrepareOnboardingStorage(context.Background(), c, false, command)
			if err != nil {
				t.Fatal(err)
			}
			before.RequireMountedNAS = true
			if !reflect.DeepEqual(before, got) {
				t.Fatal("changed existing roots or policy")
			}
			job := filepath.Join(got.Checkpoints, "job-1")
			if err = os.Mkdir(job, 0700); err != nil {
				t.Fatal(err)
			}
			data := []byte(`{"generation":1}`)
			if err = os.WriteFile(filepath.Join(job, "checkpoint.tmp"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Rename(filepath.Join(job, "checkpoint.tmp"), filepath.Join(job, "checkpoint.json")); err != nil {
				t.Fatal(err)
			}
			rerun, err := PrepareOnboardingStorage(context.Background(), got, false, command)
			if err != nil || rerun.Checkpoints != got.Checkpoints {
				t.Fatal(err)
			}
			b, err := os.ReadFile(filepath.Join(rerun.Checkpoints, "job-1", "checkpoint.json"))
			if err != nil || string(b) != string(data) {
				t.Fatal("checkpoint round trip failed", err)
			}
		})
	}
}
func TestOnboardingNASMissingUnreachableNoLocalSubstitute(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		for _, mode := range []string{"absent", "unreachable", "local-filesystem"} {
			t.Run(osname+"/"+mode, func(t *testing.T) {
				c, _ := onboardingNAS(t, osname)
				c.NAS = filepath.Join(c.WorkerHome, "missing")
				c.Checkpoints = filepath.Join(c.NAS, "checkpoints")
				before := stateTree(t, c.WorkerHome)
				_, err := PrepareOnboardingStorage(context.Background(), c, false, func(context.Context, string, ...string) ([]byte, error) {
					if mode == "unreachable" {
						return nil, fmt.Errorf("private-value")
					}
					if mode == "local-filesystem" {
						return []byte("source " + c.NAS + " ext4 rw 0 0\n"), nil
					}
					return nil, nil
				})
				code := "nas_not_mounted"
				if mode == "unreachable" {
					code = "nas_unreachable"
				}
				requirePreflightCode(t, err, code)
				if strings.Contains(err.Error(), "private-value") {
					t.Fatal("credential content emitted")
				}
				if !reflect.DeepEqual(before, stateTree(t, c.WorkerHome)) {
					t.Fatal("NAS refusal changed local files")
				}
			})
		}
	}
}
func TestOnboardingNASDiscoveryAndBoundaries(t *testing.T) {
	c, command := onboardingNAS(t, "linux")
	old := c.NAS
	c.NAS = "/mnt/nas"
	c.Checkpoints = filepath.Join(c.NAS, "mycelium", "checkpoints")
	c.ModelRoot = filepath.Join(c.NAS, "mycelium", "models")
	c.EnvironmentRoot = filepath.Join(c.NAS, "mycelium", "environments")
	got, err := PrepareOnboardingStorage(context.Background(), c, true, command)
	requirePreflightCode(t, err, "nas_not_configured")
	if got.NAS != c.NAS {
		t.Fatal("candidate adopted without selection", got.NAS)
	}
	got.NAS = old
	got.Checkpoints = filepath.Join(old, "mycelium", "checkpoints")
	got.EnvironmentRoot = filepath.Join(old, "mycelium", "environments")
	got.ModelRoot = filepath.Join(c.WorkerHome, "local-models")
	_, err = PrepareOnboardingStorage(context.Background(), got, false, command)
	requirePreflightCode(t, err, "nas_root_outside_mount")
	if err = os.Symlink(c.WorkerHome, filepath.Join(old, "escape")); err != nil {
		t.Fatal(err)
	}
	got.ModelRoot = filepath.Join(old, "escape", "models")
	_, err = PrepareOnboardingStorage(context.Background(), got, false, command)
	requirePreflightCode(t, err, "nas_root_invalid")
}
