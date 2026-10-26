package myce

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSharedInputTransferRenewsProgressAndIgnoresWholeRequestCap(t *testing.T) {
	source := t.TempDir()
	ref := sharedFixtureFile(t, source, "file", "healthy progress")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Millisecond)
		w.Header().Set("ETag", fmt.Sprintf("%q", ref.SHA256))
		w.Header().Set("Content-Length", fmt.Sprint(ref.SizeBytes))
		_, _ = w.Write([]byte("healthy progress"))
	}))
	defer server.Close()
	original := Client{URL: server.URL, HTTP: &http.Client{Timeout: time.Millisecond}}
	c, closeIdle := original.inputTransfer(ref)
	defer closeIdle()
	dest := filepath.Join(t.TempDir(), "download")
	if err := c.DownloadFile(context.Background(), ref, dest); err != nil {
		t.Fatal(err)
	}
	if original.HTTP.Timeout != time.Millisecond {
		t.Fatal("mutated API client timeout")
	}
	if got, err := os.ReadFile(dest); err != nil || string(got) != "healthy progress" {
		t.Fatal(string(got), err)
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	conn := &inputConn{Conn: a, idle: 100 * time.Millisecond}
	go func() {
		for i := 0; i < 4; i++ {
			time.Sleep(40 * time.Millisecond)
			_, _ = b.Write([]byte("x"))
		}
	}()
	buf := make([]byte, 1)
	for i := 0; i < 4; i++ {
		if _, err := conn.Read(buf); err != nil {
			t.Fatal("progress did not renew idle budget", err)
		}
	}
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("stalled input did not time out")
	}
}
func TestSharedInputsStoredReferencesHaveRetentionAndAdmission(t *testing.T) {
	q, clock := fixture(t)
	input := SharedInput{SHA256: "0000000000000000000000000000000000000000000000000000000000000000", SizeBytes: 10}
	s := Spec{Name: "shared", Project: "test", Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1}, SharedInputs: map[string]SharedInput{"asset": input}}
	if _, err := q.Submit(s); err == nil {
		t.Fatal("unavailable stored shared input admitted")
	}
	source := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(source, []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	if err = c.UploadJobInput(context.Background(), source, ref); err != nil {
		t.Fatal(err)
	}
	input.SHA256, input.SizeBytes = ref.SHA256, ref.SizeBytes
	s.SharedInputs["asset"] = input
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(8 * 24 * time.Hour)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("queued shared input lost", err)
	}
	if err = q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(8 * 24 * time.Hour)
	if err = q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("terminal shared input not reclaimed", err)
	}
	s.SharedInputs["ASSET"] = input
	if err := s.Validate(); err == nil {
		t.Fatal("colliding environment names accepted")
	}
}
