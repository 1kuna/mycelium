package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestExcludeCLI(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	m := maintenanceCLIWorker("worker")
	m.Devices = []myce.MachineDevice{{ID: "0", Platform: "fake", Identity: "stable-a", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}, {ID: "1", Platform: "fake", Identity: "stable-b", Selector: "1", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	const token = "exclusion-test-token"
	server := httptest.NewTLSServer(myce.API{Queue: q, Token: token}.Handler())
	defer server.Close()
	for _, name := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"} {
		t.Setenv(name, "")
	}
	hash := sha256.Sum256(server.Certificate().Raw)
	invitation := myce.Invitation{Version: 1, Token: token, Address: server.URL, Fingerprint: hex.EncodeToString(hash[:])}
	joining := filepath.Join(t.TempDir(), "joining.json")
	if err := myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	call := func(args ...string) error {
		out.Reset()
		return excludeCommand(context.Background(), append(args, "--joining-file", joining), &out)
	}
	if err := call("add", "host=host;devices=1,0", "--reason", "driver investigation", "--by", "operator-a"); err != nil {
		t.Fatal(err)
	}
	var p myce.ComponentExclusion
	if err := json.Unmarshal(out.Bytes(), &p); err != nil || p.Selector != "host=host;devices=stable-a,stable-b" || p.SetBy != "operator-a" || p.Reason != "driver investigation" || !p.UpdatedAt.Equal(clock.Now()) {
		t.Fatal("missing canonical selector/metadata", out.String(), err)
	}
	clock.advance(time.Second)
	printed, _, err := captureUsageOutput(t, []string{"status", "--joining-file", joining})
	if err != nil || !strings.Contains(printed, p.Selector) || !strings.Contains(printed, "operator-a") || !strings.Contains(printed, p.Reason) || !strings.Contains(printed, p.UpdatedAt.UTC().Format(time.RFC3339Nano)) {
		t.Fatal("status hid exclusion", printed, err)
	}
	if err := call("list"); err != nil || !strings.Contains(out.String(), p.Selector) || strings.Contains(out.String(), token) {
		t.Fatal("list hid exclusion or exposed authority", out.String(), err)
	}
	// Removal works after the devices leave inventory, using the printed
	// stable selector rather than relying on ordinals from a past checkin.
	m.Devices = []myce.MachineDevice{}
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	if err := call("remove", p.Selector); err != nil {
		t.Fatal(err)
	}
	if err := call("list"); err != nil || strings.TrimSpace(out.String()) != "[]" {
		t.Fatal("removed setting listed as active", out.String(), err)
	}
	for _, args := range [][]string{{"add"}, {"list", "host=host"}, {"remove", "platform=fake", "--reason", "invalid"}, {"list", "--by", "invalid"}, {"add", "host=host;devices=0;kind=gpu"}} {
		if err := call(args...); err == nil {
			t.Fatal("invalid command accepted", args)
		}
	}
	invitation.Fingerprint = strings.Repeat("0", 64)
	if err := myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
	}
	if err := call("add", "platform=fake"); err == nil {
		t.Fatal("wrong authority pin accepted")
	}
	for _, p := range q.ComponentExclusions() {
		if p.Active {
			t.Fatal("pin failure changed admission", p)
		}
	}
}
