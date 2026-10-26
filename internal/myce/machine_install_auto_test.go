package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
)

func automaticSparkSource(t *testing.T) (ServiceConfig, ServiceConfig) {
	t.Helper()
	sources, dest := m4Sources(t)
	source := sources[0]
	source.HostID, source.Name, source.Device = "spark", "spark", ""
	source.Detection = Detection{OS: "linux", Arch: "arm64", Platform: "cuda", MemoryMode: "unified", ReserveGB: 16, GPU: "NVIDIA GB10"}
	source.Service = dest.Service
	data, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(source.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	return source, dest
}

func TestMachineInstallAutomaticDevices(t *testing.T) {
	for _, tc := range []struct {
		name, inventory string
		files           fstest.MapFS
		probeErr        error
		want            []string
	}{
		{name: "GB10", inventory: "GPU-gb10-fixture, driver, 0000:01:00.0\n", want: []string{"GPU-gb10-fixture"}},
		{name: "all CUDA devices", inventory: "GPU-first, driver, 0000:01:00.0\nGPU-second, driver, 0000:02:00.0\n", want: []string{"GPU-first", "GPU-second"}},
		// A failed unrelated family must not hide a valid CUDA inventory (#115).
		{name: "unavailable XPU", inventory: "GPU-gb10-fixture, driver, 0000:01:00.0\n", files: fstest.MapFS{"dev/dri/renderD128": {}}, want: []string{"GPU-gb10-fixture"}},
		{name: "no matching devices"},
		{name: "CUDA probe failed", probeErr: errors.New("fixture unavailable")},
		{name: "ambiguous UUIDs", inventory: "GPU-duplicate, driver, 0000:01:00.0\nGPU-duplicate, driver, 0000:02:00.0\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			source, dest := automaticSparkSource(t)
			probe := MachineProbe{OS: "linux", Arch: "arm64", Files: tc.files, Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != "nvidia-smi" || strings.Join(args, " ") != "--query-gpu=uuid,driver_version,pci.bus_id --format=csv,noheader,nounits" {
					t.Fatalf("unexpected command %s %q", name, args)
				}
				return []byte(tc.inventory), tc.probeErr
			}}
			facts := []pciFixture{}
			for i := 1; i <= max(1, len(strings.Split(strings.TrimSpace(tc.inventory), "\n"))); i++ {
				facts = append(facts, pciFixture{fmt.Sprintf("0000:%02x:00.0", i), "0x10de", "0", "fixture", "", true})
			}
			if tc.name == "unavailable XPU" {
				facts = append(facts, pciFixture{"0000:03:00.0", "0x8086", "16", "fixture", "", false})
			}
			physical := pciFixtureProbe(facts...)
			probe.Glob, probe.ReadFile = physical.Glob, physical.ReadFile
			got, err := PlanMachineInstallWithProbe(context.Background(), dest.Config, []string{source.Config}, nil, probe)
			if tc.want == nil {
				if err == nil {
					t.Fatal("unresolved automatic selection accepted", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Device != strings.Join(tc.want, ",") || len(got.MachineDevices) != len(tc.want) {
				t.Fatal("did not bind every detected device", got)
			}
			for i, selector := range tc.want {
				if got.MachineDevices[i] != (DeviceInstall{selector, source.Detection}) {
					t.Fatal("lost UUID or saved capacity contract", got.MachineDevices)
				}
			}
			if got.Name != "spark" || got.HostID != "spark" || got.ModelCache != source.ModelCache || got.EnvironmentCache != source.EnvironmentCache || got.EnvironmentFile != source.EnvironmentFile || got.HostMemoryBudgetGB != source.HostMemoryBudgetGB {
				t.Fatal("lost source settings", got)
			}
			if old, err := ReadServiceConfig(source.Config); err != nil || !reflect.DeepEqual(old, source) {
				t.Fatal("planning changed source", err)
			}
		})
	}
}

func TestMachineInstallAutomaticSourceOwnership(t *testing.T) {
	source, dest := automaticSparkSource(t)
	for _, tc := range []struct {
		name  string
		paths []string
		probe MachineProbe
	}{
		{"platform mismatch", []string{source.Config}, MachineProbe{OS: "darwin", Arch: "arm64"}},
		{"mixed sources", []string{source.Config, filepath.Join(filepath.Dir(source.Config), "other.json")}, MachineProbe{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := PlanMachineInstallWithProbe(context.Background(), dest.Config, tc.paths, nil, tc.probe); err == nil || !strings.Contains(err.Error(), "machine_migration_conflict") {
				t.Fatal("ambiguous source ownership accepted", err)
			}
		})
	}
}

func TestMachineInstallAutomaticPreviewWithoutQueueRegistration(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCE_TOKEN", "synthetic-local-test")
	source, dest := automaticSparkSource(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("conversion wrote to queue", r.Method)
		}
		switch r.URL.Path {
		case "/api/hosts/spark/maintenance":
			json.NewEncoder(w).Encode(HostMaintenanceStatus{Policy: HostMaintenance{Mode: "disabled"}, Healthy: true, Quiescent: true})
		case "/api/inspection":
			json.NewEncoder(w).Encode(Snapshot{Machines: []*Machine{}})
		default:
			t.Error("unexpected queue request", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	probe := MachineProbe{OS: "linux", Arch: "arm64", Files: fstest.MapFS{}, Command: func(context.Context, string, ...string) ([]byte, error) {
		return []byte("GPU-gb10-fixture, driver, 0000:01:00.0\n"), nil
	}}
	probe = withCUDAFacts(probe, true, 1)
	c, err := PlanMachineInstallWithProbe(context.Background(), dest.Config, []string{source.Config}, nil, probe)
	if err != nil {
		t.Fatal(err)
	}
	c.Queue = server.URL
	run := func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("unregistered conversion touched services")
		return nil, nil
	}
	if err = ConvertWorkerInstall(context.Background(), c, "unused-binary", true, run); err != nil {
		t.Fatal("preview required queue registration", err)
	}
	err = ConvertWorkerInstall(context.Background(), c, "unused-binary", false, run)
	if err == nil || !strings.Contains(err.Error(), "source is missing from the maintained queue") {
		t.Fatal("installation bypassed queue registration", err)
	}
	if _, err = os.Stat(machineJournal(c)); !os.IsNotExist(err) {
		t.Fatal("unregistered conversion wrote journal", err)
	}
	if _, err = os.Stat(filepath.Dir(c.Config)); !os.IsNotExist(err) {
		t.Fatal("unregistered conversion wrote destination", err)
	}
}
