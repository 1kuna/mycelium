package myce

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"
)

type serviceMember struct {
	cmd    *exec.Cmd
	url    string
	log    string
	client Client
}

// startServiceMember runs the installed-service command line of a member:
// `myce queue --shared-authority DSN --shared-authority-password-file FILE`,
// listening on the LAN address and advertising it to the other members.
func startServiceMember(t *testing.T, binary, dir, name string, pg *remoteProofPostgres) *serviceMember {
	t.Helper()
	probe, err := net.Listen("tcp", net.JoinHostPort(pg.address, "0"))
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	probe.Close()
	m := &serviceMember{url: "http://" + address, log: filepath.Join(dir, name+".log")}
	m.cmd = exec.Command(binary, "queue", "--listen", address, "--queue", m.url, "--discovery=false",
		"--db", filepath.Join(dir, name, "queue.db"), "--checkpoints", filepath.Join(dir, "checkpoints"), "--file-store", filepath.Join(dir, "files"),
		"--shared-authority", pg.dsn, "--shared-authority-password-file", pg.passwordFile)
	// The service wrapper supplies the fleet token through the environment.
	m.cmd.Env = append(os.Environ(), "MYCE_TOKEN="+proofToken, "MYCE_SHARED_QUEUE_PG_DSN=", "PGPASSWORD=")
	log, err := os.Create(m.log)
	if err != nil {
		t.Fatal(err)
	}
	m.cmd.Stdout, m.cmd.Stderr = log, log
	if err := m.cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = m.cmd.Process.Kill()
		_ = m.cmd.Wait()
		log.Close()
		if t.Failed() {
			data, _ := os.ReadFile(m.log)
			t.Logf("%s log:\n%s", name, data)
		}
	})
	m.client = Client{URL: m.url, Token: proofToken, HTTP: &http.Client{Timeout: 5 * time.Second}, singleAttempt: true}
	proofEventually(t, name+" listens", func() bool {
		res, err := http.Get(m.url + "/api/ping")
		if err != nil {
			return false
		}
		res.Body.Close()
		return true
	})
	return m
}

// processExposes reports whether a member's command line, or its environment
// where the OS shows it, contains secret.
func processExposes(t *testing.T, pid int, secret string) bool {
	t.Helper()
	argv, err := exec.Command("ps", "-ww", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(argv, []byte(secret)) {
		return true
	}
	var environment []byte
	if runtime.GOOS == "linux" {
		environment, err = os.ReadFile("/proc/" + strconv.Itoa(pid) + "/environ")
	} else {
		environment, err = exec.Command("ps", "-E", "-ww", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	}
	if err != nil || !bytes.Contains(environment, []byte("MYCE_TOKEN=")) {
		t.Logf("process environment is not visible here; checked the command line only")
		return false
	}
	return bytes.Contains(environment, []byte(secret))
}

// Members installed as services on two hosts share a PostgreSQL authority on
// a third address with a password file. They wait for cutover, coordinate,
// forward over the LAN, survive the coordinator's loss, and never expose the
// password to argv, the environment, logs or a job.
func TestSharedQueueMemberServiceRemoteAuthority(t *testing.T) {
	pg := startRemoteProofPostgres(t)
	binary := proofCLI(t)
	authority := pg.authority(t)
	dir := t.TempDir()
	single := filepath.Join(dir, "single", "queue.db")
	q, err := Open(single, filepath.Join(dir, "checkpoints"), RealClock{}, WithFileStore(filepath.Join(dir, "files"), 1<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	first := startServiceMember(t, binary, dir, "first", pg)
	// Members installed before cutover must leave the authority empty.
	time.Sleep(2500 * time.Millisecond) // More than two renewal intervals.
	if turn, err := authority.current(t.Context()); err != nil || turn.number != 0 || turn.head != 0 {
		t.Fatalf("a member took the empty authority before cutover: %+v, %v", turn, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	result, err := SharedQueueCutover(ctx, SharedQueueMigration{Postgres: pg.dsn, PostgresPasswordFile: pg.passwordFile, Database: single,
		Checkpoints: filepath.Join(dir, "checkpoints"), FileStore: filepath.Join(dir, "files"), Clock: RealClock{}})
	if err != nil || result.Sequence == 0 {
		t.Fatalf("cutover against the remote authority: %+v, %v", result, err)
	}
	var turn sharedQueueTurn
	proofEventually(t, "first member coordinates", func() bool {
		turn, err = authority.current(t.Context())
		return err == nil && turn.ready && turn.endpoint == first.url
	})
	second := startServiceMember(t, binary, dir, "second", pg)
	// The worker uses its own member, which forwards to the coordinator over the LAN.
	startProofWorker(t, second.client, dir)
	s := proofSpec("environment")
	seed := filepath.Join(dir, "seed")
	if err := os.WriteFile(seed, []byte("seed"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.client.UploadFile(ctx, seed, ref); err != nil {
		t.Fatal(err)
	}
	s.Files = FileManifest{"seed": ref}
	s.Outputs = map[string]string{"result.txt": "results"}
	s.Run.Command = []string{"/bin/sh", "-c", "env > result.txt"}
	job := proofSubmit(t, second.client, s)
	proofJobState(t, second.client, job.ID, proofSucceeded)
	dest := filepath.Join(dir, "environment.txt")
	if err := second.client.DownloadResult(ctx, job.ID, "result.txt", dest); err != nil {
		t.Fatal(err)
	}
	environment, err := os.ReadFile(dest)
	if err != nil || !bytes.Contains(environment, []byte("PATH=")) {
		t.Fatalf("job environment = %q, %v", environment, err)
	}
	for _, secret := range []string{pg.password, proofToken, pg.passwordFile} {
		if bytes.Contains(environment, []byte(secret)) {
			t.Fatal("the job inherited the authority password, its file or the fleet token")
		}
	}

	// The coordinator's host goes away. The other member takes over and an
	// external client with both endpoints keeps working.
	_ = first.cmd.Process.Signal(syscall.SIGKILL)
	proofEventually(t, "second member coordinates", func() bool {
		next, err := authority.current(t.Context())
		return err == nil && next.ready && next.endpoint == second.url && next.number > turn.number
	})
	external, err := NewMultiEndpointClient(first.url+","+second.url, proofToken)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := external.GetJob(ctx, job.ID); err != nil || got.State != "succeeded" {
		t.Fatalf("external client after coordinator loss: %+v, %v", got, err)
	}
	if processExposes(t, second.cmd.Process.Pid, pg.password) {
		t.Fatal("the member's command line or environment contains the password")
	}
	for _, m := range []*serviceMember{first, second} {
		data, err := os.ReadFile(m.log)
		if err != nil || bytes.Contains(data, []byte(pg.password)) {
			t.Fatalf("member log contains the password or is unreadable: %v", err)
		}
	}
	var refusal *HTTPError
	unauthenticated := second.client
	unauthenticated.Token = "wrong"
	if err := unauthenticated.Call(ctx, http.MethodGet, "/api/ping", nil, nil); !errors.As(err, &refusal) || refusal.Status != http.StatusUnauthorized {
		t.Fatalf("wrong fleet token = %v", err)
	}
}
