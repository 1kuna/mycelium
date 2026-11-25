package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const warmDevMain = `
import json, os, signal, socket, struct, subprocess, sys, urllib.request
from pathlib import Path
from warm_pieces import WarmPieces

assert "MYCE_TOKEN" not in os.environ
mode = sys.argv[1]
if mode == "child":
    fd = int(os.environ["MYCE_WARM_PIECES_FD"])
    subprocess.run([sys.executable, "-u", __file__, "child-payload"], check=True, pass_fds=(fd,), timeout=12)
    sys.exit(0)
if mode in ("oversized", "unknown-op", "partial"):
    with socket.socket(fileno=int(os.environ["MYCE_WARM_PIECES_FD"])) as channel:
        channel.settimeout(10)
        if mode == "oversized":
            channel.sendall(struct.pack("!I", 2))
        elif mode == "unknown-op":
            channel.sendall(struct.pack("!I", 1) + b"?")
        else:
            channel.sendall(b"\0")
        assert channel.recv(1) == b""
        print("channel-closed", flush=True)
    sys.exit(0)
with WarmPieces() as pieces:
    weights = json.loads(Path("model.json").read_text())
    print("model-loaded", flush=True)
    while item := pieces.next():
        print("item:" + item.key, flush=True)
        if item.key == "b" and sys.argv[1] == "failure":
            pieces.failure()
            continue
        restored = False
        if item.key == "b" and sys.argv[1] in ("preempt", "cancel", "loss"):
            save = item.checkpoint_dir / "progress.json"
            if save.exists():
                assert json.loads(save.read_text())["progress"] == 7
                restored = True
            else:
                tmp = save.with_suffix(".tmp")
                tmp.write_text(json.dumps({"myce_checkpoint": {"generation": 1}, "progress": 7}))
                tmp.replace(save)
                pieces.checkpoint()
                print("checkpoint-accepted", flush=True)
                signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
                while True:
                    signal.pause()
        value = int(item.inputs["in"].read_text())
        if sys.argv[1] == "success":
            request = urllib.request.urlopen(os.environ["MYCE_SIDECAR_HELPER_URL"], timeout=5)
            assert request.read() == b"ready"
        out = f"{item.key}:{value * weights['multiplier']}:{item.args[0]}:{restored}"
        (item.output_dir / "out").write_text(out)
        pieces.success()
`

const warmDevSidecar = `
import http.server, os, sys
assert "MYCE_TOKEN" not in os.environ and "MYCE_WARM_PIECES_FD" not in os.environ
try:
    os.fstat(5)
except OSError as err:
    import errno
    assert err.errno == errno.EBADF
else:
    raise AssertionError("sidecar inherited main's warm descriptor")
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ready")
    def log_message(self, *_):
        pass
print("sidecar-loaded", flush=True)
http.server.HTTPServer(("localhost", int(sys.argv[1])), Handler).serve_forever()
`

func warmDevFile(t *testing.T, ctx context.Context, c Client, contents string) FileRef {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.UploadFile(ctx, path, ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func warmDevSpec(t *testing.T, ctx context.Context, c Client, mode string) Spec {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	helper, err := os.ReadFile(filepath.Join("..", "..", "clients", "python", "src", "mycelium", "warm_pieces.py"))
	if err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(2, .5)
	s.Advanced = &Advanced{WarmPieces: warmPiecesFixed}
	s.SubmissionKey = "warm-dev-" + mode
	s.Resumable = mode != "success" && mode != "failure"
	s.Run.Command = []string{python, "-u", "main.py", mode}
	s.Files = FileManifest{
		"main.py":        warmDevFile(t, ctx, c, warmDevMain),
		"warm_pieces.py": warmDevFile(t, ctx, c, string(helper)),
		"model.json":     warmDevFile(t, ctx, c, `{"multiplier":3}`),
	}
	if mode == "success" {
		port := 0
		s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{python, "-u", "sidecar.py", "{port}"}, Port: &port, Readiness: &NativeReadiness{Path: "/", Timeout: "15s"}}}
		s.Files["sidecar.py"] = warmDevFile(t, ctx, c, warmDevSidecar)
	}
	s.Outputs = map[string]string{"out": "results"}
	s.FanOut = &FanOut{}
	for i, key := range []string{"a", "b", "c"} {
		s.FanOut.Pieces = append(s.FanOut.Pieces, FanOutPiece{Key: key, Args: []string{"arg-" + key}})
		s.Files[PlainPiecePrefix+key+"/in"] = warmDevFile(t, ctx, c, strconv.Itoa(i+1))
	}
	return s
}

func startWarmDev(t *testing.T, cores int) (*Dev, context.Context) {
	t.Helper()
	root := t.TempDir()
	d, err := startTestDev(t, DevConfig{
		Address: net.JoinHostPort(net.IPv4(127, 0, 0, 1).String(), "0"), Database: filepath.Join(root, "queue.db"),
		Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "files"), GPU: "cpu", Workers: 1,
		CPUBudgetCores: cores, Token: "warm-dev-fixture", Interval: defaultWorkerInterval, EnvironmentCacheLimit: 1 << 30,
	}, func(q *Queue) {
		if err := q.SetLossTimeouts(45*time.Second, 45*time.Second, 45*time.Second); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	if err := d.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	return d, ctx
}

func warmDevLogs(t *testing.T, ctx context.Context, c Client, id string) string {
	t.Helper()
	var logs []Log
	if err := c.Call(ctx, "GET", "/api/jobs/"+id+"/logs", nil, &logs); err != nil {
		t.Logf("logs unavailable: %v", err)
	}
	var output strings.Builder
	for _, log := range logs {
		output.WriteString(log.Text)
	}
	return output.String()
}

func warmDevCheckOutput(t *testing.T, ctx context.Context, c Client, id, key, expected string) {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "out")
	if err := c.DownloadResult(ctx, id, key+"/out", dst); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != expected {
		t.Fatalf("%s output = %q, %v; want %q", key, got, err, expected)
	}
}

func TestWarmPiecesDevSingleLoad(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "inherited-fixture-token")
	d, ctx := startWarmDev(t, 2)
	c := d.Client()
	for _, mode := range []string{"success", "failure"} {
		t.Run(mode, func(t *testing.T) {
			s := warmDevSpec(t, ctx, c, mode)
			j, err := c.Submit(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := c.Submit(ctx, s)
			if err != nil || replay.ID != j.ID {
				t.Fatalf("keyed replay: %v %v", replay, err)
			}
			final := awaitTerminal(t, ctx, c, j.ID)
			logs := warmDevLogs(t, ctx, c, j.ID)
			wantState, wantOutputs, wantSidecars := "succeeded", 3, 1
			if mode == "failure" {
				wantState, wantOutputs, wantSidecars = "failed", 2, 0
			}
			loads, sidecars := strings.Count(logs, "model-loaded"), strings.Count(logs, "sidecar-loaded")
			t.Logf("model loads=%d, sidecar loads=%d, accepted keys=%d, attempts=%d", loads, sidecars, len(final.FanOut.Results), final.Attempt)
			if final.State != wantState || final.Attempt != 1 || loads != 1 || sidecars != wantSidecars ||
				len(final.FanOut.Results) != wantOutputs || len(final.FanOut.Children) != 0 {
				t.Fatalf("state %s (%s), attempt %d, log:\n%s", final.State, final.Reason, final.Attempt, logs)
			}
			warmDevCheckOutput(t, ctx, c, j.ID, "a", "a:3:arg-a:False")
			warmDevCheckOutput(t, ctx, c, j.ID, "c", "c:9:arg-c:False")
			if mode == "success" {
				warmDevCheckOutput(t, ctx, c, j.ID, "b", "b:6:arg-b:False")
			} else if final.FanOut.Warm.Failures["b"].Attempt != 1 {
				t.Fatal("item failure did not settle in the running attempt")
			}
		})
	}
}

func TestWarmPiecesDevPreemptionAndCancellation(t *testing.T) {
	for _, mode := range []string{"preempt", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			d, ctx := startWarmDev(t, 2)
			c := d.Client()
			s := warmDevSpec(t, ctx, c, mode)
			j, err := c.Submit(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			var running *Job
			probeWait(t, ctx, func() bool {
				running, err = c.GetJob(ctx, j.ID)
				return err == nil && len(running.FileCheckpoints) == 1
			})
			old := warmRequest(running, "failure", running.FanOut.Warm.Delivery)
			warmDevCheckOutput(t, ctx, c, j.ID, "a", "a:3:arg-a:False")
			if mode == "cancel" {
				if _, err := c.Cancel(ctx, j.ID); err != nil {
					t.Fatal(err)
				}
				final := awaitTerminal(t, ctx, c, j.ID)
				if final.State != "cancelled" || len(final.FanOut.Results) != 1 {
					t.Fatalf("wrong cancellation: %s %+v", final.State, final.FanOut)
				}
			} else {
				priority := cpuSpec(2, .25)
				priority.Class, priority.Priority = "production", "production"
				priority.Run.Command = []string{"true"}
				demand, err := c.Submit(ctx, priority)
				if err != nil {
					t.Fatal(err)
				}
				if got := awaitTerminal(t, ctx, c, demand.ID); got.State != "succeeded" {
					t.Fatalf("preemptor state %s: %s", got.State, got.Reason)
				}
				final := awaitTerminal(t, ctx, c, j.ID)
				logs := warmDevLogs(t, ctx, c, j.ID)
				loads := strings.Count(logs, "model-loaded")
				t.Logf("preempted run loaded %d times across %d attempts", loads, final.Attempt)
				if final.State != "succeeded" || final.Attempt != 2 || final.PreemptionCount != 1 || loads != 2 ||
					final.FanOut.Results["a"].Execution.Attempt != 1 || final.FanOut.Results["b"].Execution.Attempt != 2 {
					t.Fatalf("state %s (%s), attempt %d, log:\n%s", final.State, final.Reason, final.Attempt, logs)
				}
				warmDevCheckOutput(t, ctx, c, j.ID, "b", "b:6:arg-b:True")
			}
			err = c.Call(ctx, "POST", "/api/workers/warm-piece", old, nil)
			var reply *HTTPError
			if !errors.As(err, &reply) || reply.Status != 409 {
				t.Fatalf("stale item report returned %v", err)
			}
		})
	}
}

type warmLossTransport struct {
	disconnected atomic.Bool
}

func (tr *warmLossTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if tr.disconnected.Load() {
		return nil, errors.New("fixture worker transport disconnected")
	}
	return clientTransport.RoundTrip(req)
}

// startWarmFixtureWorker runs a second dev worker whose transport the test
// controls. configure can adjust the worker before it starts.
func startWarmFixtureWorker(t *testing.T, d *Dev, transport http.RoundTripper, configure func(*Worker)) Machine {
	t.Helper()
	other := cpuMachine("fixture-worker", "fixture-host", "cpu", 2, 8)
	other.Devices = []MachineDevice{}
	other.CapabilityProfile.OS, other.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
	other.CPUBudgetCores = 2
	workerClient := d.Client()
	workerClient.HTTP = &http.Client{Transport: transport}
	w := Worker{EnvironmentLimit: 1 << 30, sharedCPUDev: true, devEnvironmentSizing: true, CPUIsolationProbe: func(context.Context) error { return nil },
		Machine: other, StateDir: t.TempDir(), Client: workerClient, ModelConfig: d.Queue.models, Clock: d.Queue.clock, Interval: defaultWorkerInterval,
		HostMemorySource: func(context.Context) (float64, error) { return 8, nil },
	}
	if workerDevProcessMemory != nil {
		w.processMemory = workerDevProcessMemory()
	}
	if configure != nil {
		configure(&w)
	}
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(workerCtx) }()
	t.Cleanup(func() {
		stopWorker()
		select {
		case err := <-workerDone:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("fixture worker did not finish cleanup")
		}
	})
	return other
}

// warmBlockDevWorker occupies the dev worker so the warm parent runs on the
// fixture worker. No mocked launch or result is involved.
func warmBlockDevWorker(t *testing.T, ctx context.Context, c Client) string {
	t.Helper()
	block := cpuSpec(2, .25)
	block.Run.Command = []string{"sh", "-c", "while :; do sleep 1; done"}
	block.AllowedMachines = []string{"fake-1"}
	busy, err := c.Submit(ctx, block)
	if err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool {
		j, err := c.GetJob(ctx, busy.ID)
		return err == nil && j.StartedAt != nil
	})
	return busy.ID
}

func TestWarmPiecesDevWorkerLoss(t *testing.T) {
	d, ctx := startWarmDev(t, 2)
	c := d.Client()
	// Occupy the replacement until the warm job starts on the worker whose
	// transport this test can cut.
	busy := warmBlockDevWorker(t, ctx, c)
	link := &warmLossTransport{}
	other := startWarmFixtureWorker(t, d, link, nil)
	t.Cleanup(func() { link.disconnected.Store(false) }) // Runs before the worker stops.
	s := warmDevSpec(t, ctx, c, "loss")
	j, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	var first *Job
	probeWait(t, ctx, func() bool {
		first, err = c.GetJob(ctx, j.ID)
		return err == nil && len(first.FileCheckpoints) == 1
	})
	if first.Machine != other.ID || first.FanOut.Warm.Delivery.Key != "b" || first.Attempt != 1 {
		t.Fatalf("wrong attempt before simulated loss: %d %v", first.Attempt, first.FanOut.Warm.Delivery)
	}
	probeWait(t, ctx, func() bool {
		return strings.Contains(warmDevLogs(t, ctx, c, j.ID), "checkpoint-accepted")
	})
	if _, err := c.Cancel(ctx, busy); err != nil {
		t.Fatal(err)
	}
	if got := awaitTerminal(t, ctx, c, busy); got.State != "cancelled" {
		t.Fatal(got.State)
	}
	if err := d.Queue.SetLossTimeouts(4*time.Second, 4*time.Second, 4*time.Second); err != nil {
		t.Fatal(err)
	}
	link.disconnected.Store(true)
	final := awaitTerminal(t, ctx, c, j.ID)
	link.disconnected.Store(false)
	if final.State != "succeeded" || final.Attempt != 2 || final.Losses != 1 ||
		final.FanOut.Results["a"].Execution.Attempt != 1 || final.FanOut.Results["b"].Execution.Attempt != 2 {
		t.Fatalf("loss recovery state %s, attempt %d, losses %d, reason %s", final.State, final.Attempt, final.Losses, final.Reason)
	}
	old := warmRequest(first, "failure", first.FanOut.Warm.Delivery)
	if err := c.Call(ctx, "POST", "/api/workers/warm-piece", old, nil); !stale(err) {
		t.Fatalf("old worker returned %v", err)
	}
	warmDevCheckOutput(t, ctx, c, j.ID, "b", "b:6:arg-b:True")
	logs := warmDevLogs(t, ctx, c, j.ID)
	if strings.Count(logs, "item:a") != 1 {
		t.Fatalf("settled item was not run exactly once: %s", logs)
	}
}

// warmSettleTransport refuses every first-attempt output upload with the
// queue's retryable storage envelope, as a queue that cannot accept items.
type warmSettleTransport struct{ refused atomic.Int32 }

func (tr *warmSettleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var attempt fileStoreAttempt
	if req.Method == http.MethodPost && strings.HasPrefix(req.URL.Path, "/api/files/uploads/") &&
		json.Unmarshal([]byte(req.Header.Get("X-Myce-File-Attempt")), &attempt) == nil && attempt.Attempt == 1 {
		if req.Body != nil {
			req.Body.Close()
		}
		tr.refused.Add(1)
		header := http.Header{"Content-Type": {"application/json"}, "X-Myce-Error-Code": {"file_upload_unavailable"}}
		body := `{"code":"file_upload_unavailable","error":"upload storage unavailable"}`
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	}
	return clientTransport.RoundTrip(req)
}

// A queue that stays unable to accept an item ends the warm loop cleanly. The
// worker reports the interruption and the next attempt settles every item.
func TestWarmPiecesDevSettleUnavailableRequeues(t *testing.T) {
	d, ctx := startWarmDev(t, 2)
	c := d.Client()
	busy := warmBlockDevWorker(t, ctx, c)
	t.Cleanup(func() {
		// Dev shutdown waits for its worker; end the blocker first.
		if _, err := c.Cancel(context.Background(), busy); err != nil {
			t.Error(err)
		}
	})
	link := &warmSettleTransport{}
	// Real pacing retries about once a second; a short bound ends the loop
	// after a few refusals instead of the production default.
	other := startWarmFixtureWorker(t, d, link, func(w *Worker) { w.warmSettleBound = 2 * time.Second })
	j, err := c.Submit(ctx, warmDevSpec(t, ctx, c, "settle"))
	if err != nil {
		t.Fatal(err)
	}
	final := awaitTerminal(t, ctx, c, j.ID)
	if final.State != "succeeded" || final.Attempt != 2 || final.Losses != 0 || final.Machine != other.ID {
		t.Fatalf("settle recovery state %s, attempt %d, losses %d, machine %s, reason %s", final.State, final.Attempt, final.Losses, final.Machine, final.Reason)
	}
	for _, key := range []string{"a", "b", "c"} {
		if final.FanOut.Results[key].Execution.Attempt != 2 {
			t.Fatalf("item %s settled by attempt %d", key, final.FanOut.Results[key].Execution.Attempt)
		}
	}
	if link.refused.Load() < 2 {
		t.Fatalf("first attempt did not retry its upload: %d refusals", link.refused.Load())
	}
	warmDevCheckOutput(t, ctx, c, j.ID, "a", "a:3:arg-a:False")
}

func TestWarmPiecesDevChildWithChannel(t *testing.T) {
	d, ctx := startWarmDev(t, 2)
	c := d.Client()
	j, err := c.Submit(ctx, warmDevSpec(t, ctx, c, "child"))
	if err != nil {
		t.Fatal(err)
	}
	final := awaitTerminal(t, ctx, c, j.ID)
	if final.State != "succeeded" || final.Attempt != 1 || len(final.FanOut.Results) != 3 {
		t.Fatalf("main's child could not settle the attempt: state=%s reason=%q logs=\n%s",
			final.State, final.Reason, warmDevLogs(t, ctx, c, j.ID))
	}
	warmDevCheckOutput(t, ctx, c, j.ID, "c", "c:9:arg-c:False")
}

func TestWarmPiecesDevInvalidChannelFrames(t *testing.T) {
	d, ctx := startWarmDev(t, 2)
	c := d.Client()
	for _, mode := range []string{"oversized", "unknown-op", "partial"} {
		t.Run(mode, func(t *testing.T) {
			j, err := c.Submit(ctx, warmDevSpec(t, ctx, c, mode))
			if err != nil {
				t.Fatal(err)
			}
			final := awaitTerminal(t, ctx, c, j.ID)
			if logs := warmDevLogs(t, ctx, c, j.ID); !strings.Contains(logs, "channel-closed") ||
				final.State != "failed" || job(t, d.Queue, j.ID).Reason != "warm pieces main exited before all items settled" {
				t.Fatalf("invalid frame should close only its channel: state=%s reason=%q logs=\n%s", final.State, final.Reason, logs)
			}
		})
	}
	// The same worker can still run a fresh attempt after the refused frames.
	j, err := c.Submit(ctx, warmDevSpec(t, ctx, c, "success"))
	if err != nil {
		t.Fatal(err)
	}
	if final := awaitTerminal(t, ctx, c, j.ID); final.State != "succeeded" || final.Attempt != 1 {
		t.Fatalf("worker did not finish fresh work: state=%s reason=%q logs=\n%s", final.State, final.Reason, warmDevLogs(t, ctx, c, j.ID))
	}
}

func TestWarmPiecesDevHTTPRefusal(t *testing.T) {
	d, ctx := startWarmDev(t, 2)
	s := cpuSpec(2, .5)
	s.WarmPieces = "elastic"
	s.FanOut = &FanOut{Pieces: []FanOutPiece{{Key: "a"}}}
	_, err := d.Client().Submit(ctx, s)
	var reply *HTTPError
	if !errors.As(err, &reply) || reply.Status != 400 || reply.Code != "job_files_invalid" ||
		!strings.Contains(reply.Message, "warm_pieces currently accepts fixed") {
		t.Fatalf("wanted HTTP validation refusal, got %v", err)
	}
}
