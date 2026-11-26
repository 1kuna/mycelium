package myce

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

type proofUploadReply struct {
	status int
	body   string
	err    error
}

// proofUploadRequest sends one raw tus request, so the client's retries and
// transfer pacing cannot hide the member's answer.
func proofUploadRequest(ctx context.Context, member *proofProcess, method, id string, offset int64, body io.Reader, headers map[string]string) proofUploadReply {
	req, err := http.NewRequestWithContext(ctx, method, member.client.URL+"/api/files/uploads/"+id, body)
	if err != nil {
		return proofUploadReply{err: err}
	}
	req.Header.Set("Authorization", "Bearer "+proofToken)
	req.Header.Set("Tus-Resumable", "1.0.0")
	if method == http.MethodPatch {
		req.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
		req.Header.Set("Content-Type", "application/offset+octet-stream")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	res, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return proofUploadReply{err: err}
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	reply := proofUploadReply{status: res.StatusCode, body: string(bytes.TrimSpace(data))}
	if method == http.MethodHead {
		reply.body = res.Header.Get("Upload-Offset")
	}
	return reply
}

// Upload locks on a shared store that keeps no hard-link identity (#2152).
// tusd's file lock links a PID file to <id>.lock and requires the two names
// to be one file. A Linux CIFS mount without serverino links the name but
// reports another inode, and the lock name then holds the acquirer's own
// live PID: it waits until ERR_LOCK_TIMEOUT. macOS smbfs refuses link(), so
// the attempt fails at once; a directory at the lock name fails the same way.
// The test leaves each state on the store and checks that uploads still
// work, that concurrent PATCHes have one writer, and that a coordinator
// suspended mid-PATCH cannot corrupt the upload its successor published.
func TestSharedQueueMemberUploadLocks(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	old := startProofMember(t, dir, "old", "127.0.0.1:0", pg.dsn)
	first := proofLeader(t, a, old, 0)
	ctx := t.Context()

	content := bytes.Repeat([]byte("shared upload lock bytes "), 400) // 10,000 bytes
	sum := sha256.Sum256(content)
	id := hex.EncodeToString(sum[:])
	ref := FileRef{SHA256: id, SizeBytes: int64(len(content))}
	uploads := filepath.Join(dir, "files", "uploads")
	data := filepath.Join(uploads, id)
	metadata := "sha256 " + base64.StdEncoding.EncodeToString([]byte(id)) +
		",purpose " + base64.StdEncoding.EncodeToString([]byte(jobInputUploadPurpose))
	proofEventually(t, "upload creation on the shared store", func() bool {
		r := proofUploadRequest(ctx, old, http.MethodPost, "", 0, nil, map[string]string{
			"Upload-Length": strconv.Itoa(len(content)), "Upload-Metadata": metadata})
		return r.status == http.StatusCreated
	})
	lockName := filepath.Join(uploads, id+".lock")
	differentIdentity := func(member *proofProcess) {
		t.Helper()
		_ = os.RemoveAll(lockName)
		if err := os.WriteFile(lockName, []byte(fmt.Sprintf("%d\n", member.cmd.Process.Pid)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	linkUnsupported := func() {
		t.Helper()
		_ = os.RemoveAll(lockName)
		if err := os.Mkdir(lockName, 0700); err != nil {
			t.Fatal(err)
		}
	}
	size := func() int64 {
		st, err := os.Stat(data)
		if err != nil {
			return -1
		}
		return st.Size()
	}
	patch := func(member *proofProcess, from, to int64) {
		t.Helper()
		r := proofUploadRequest(ctx, member, http.MethodPatch, id, from, bytes.NewReader(content[from:to]), nil)
		if r.err != nil || r.status != http.StatusNoContent {
			t.Fatalf("PATCH [%d,%d) = %d %q, %v", from, to, r.status, r.body, r.err)
		}
	}
	// pending starts a PATCH whose body the test feeds by hand.
	pending := func(member *proofProcess, from int64) (*io.PipeWriter, chan proofUploadReply) {
		body, feed := io.Pipe()
		done := make(chan proofUploadReply, 1)
		go func() { done <- proofUploadRequest(ctx, member, http.MethodPatch, id, from, body, nil) }()
		return feed, done
	}
	write := func(feed *io.PipeWriter, from, to int64) {
		t.Helper()
		if _, err := feed.Write(content[from:to]); err != nil {
			t.Fatal(err)
		}
	}

	// 1. The lock name reports another file holding this member's PID.
	differentIdentity(old)
	patch(old, 0, 1000)

	// 2. Concurrent PATCHes: the holder is interrupted, the newer one writes.
	feed, held := pending(old, 1000)
	write(feed, 1000, 1001)
	proofEventually(t, "first PATCH to hold the upload", func() bool { return size() == 1001 })
	patch(old, 1001, 2000) // Waits until the holder lets go.
	_ = feed.Close()
	select {
	case r := <-held:
		if r.err == nil && r.status < 300 {
			t.Fatalf("the interrupted PATCH also succeeded: %d %q", r.status, r.body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the holding PATCH did not answer")
	}
	if got, err := os.ReadFile(data); err != nil || !bytes.Equal(got, content[:2000]) {
		t.Fatalf("after concurrent PATCHes: %d bytes, %v", len(got), err)
	}

	// 3. Handover mid-PATCH. The old coordinator is suspended with bytes
	// still in its socket; its successor completes and publishes the upload.
	// A raw connection lets the test read the answer with the body still open.
	next := startProofMember(t, dir, "next", "127.0.0.1:0", pg.dsn)
	conn, err := net.Dial("tcp", old.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "PATCH /api/files/uploads/%s HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\n"+
		"Tus-Resumable: 1.0.0\r\nUpload-Offset: 2000\r\nContent-Type: application/offset+octet-stream\r\n"+
		"Transfer-Encoding: chunked\r\n\r\n", id, old.addr, proofToken)
	chunk := func(from, to int64) {
		t.Helper()
		if _, err := fmt.Fprintf(conn, "%x\r\n%s\r\n", to-from, content[from:to]); err != nil {
			t.Fatal(err)
		}
	}
	chunk(2000, 3000)
	proofEventually(t, "old coordinator to write its PATCH", func() bool { return size() == 3000 })
	if err := old.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	resumed := false
	defer func() {
		if !resumed {
			_ = old.cmd.Process.Signal(syscall.SIGCONT)
		}
	}()
	chunk(3000, 4000)
	proofLeader(t, a, next, first.number)
	linkUnsupported()
	if r := proofUploadRequest(ctx, next, http.MethodHead, id, 0, nil, nil); r.status != http.StatusOK || r.body != "3000" {
		t.Fatalf("successor HEAD = %d offset %q, %v", r.status, r.body, r.err)
	}
	patch(next, 3000, int64(len(content)))
	if err := next.client.HasFile(ctx, ref); err != nil {
		t.Fatalf("successor publication: %v", err)
	}
	if err := old.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	resumed = true
	// The resumed coordinator finds its turn gone and stops the transfer
	// itself; the test never ends the request body.
	if err := conn.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if res, err := http.ReadResponse(bufio.NewReader(conn), nil); err == nil && res.StatusCode < 300 {
		t.Fatalf("the former coordinator accepted its PATCH: %d", res.StatusCode)
	} else if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("the former coordinator's PATCH did not stop after its turn ended")
	}
	blob, err := os.ReadFile(filepath.Join(dir, "files", "blobs", id))
	if err != nil || !bytes.Equal(blob, content) {
		t.Fatalf("published blob after the former coordinator resumed: %d bytes, %v", len(blob), err)
	}
	if err := next.client.HasFile(ctx, ref); err != nil {
		t.Fatalf("publication after the former coordinator resumed: %v", err)
	}
}
