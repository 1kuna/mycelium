package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/probeguard"
)

const (
	nativeCUDANamingPrefix    = "MYCE_TEST_NATIVE_CUDA_NAMING"
	nativeCUDANamingServerSHA = "5696b570959b0982406d4f698869e1c872e0faaab6c76102a2e81a5ada117082"
	nativeCUDANamingModelSHA  = "31629f53165ab6a7dad8c9847dcfd1fdf55829dac1e6e748f4a68581b0033d34"
)

// Explicit, reserved, production-idle scheduler job ONLY. No downloads, copied
// runtime/model, service setup, cgroup writes or policy overrides. Configure:
//
//	MYCE_TEST_NATIVE_CUDA_NAMING=1
//	MYCE_TEST_NATIVE_CUDA_NAMING_RUNTIME_ROOT=/absolute/trusted/llama-runtime
//	MYCE_TEST_NATIVE_CUDA_NAMING_SERVER=/absolute/trusted/llama-runtime/bin/llama-server
//	MYCE_TEST_NATIVE_CUDA_NAMING_SERVER_SHA256=<nativeCUDANamingServerSHA>
//	MYCE_TEST_NATIVE_CUDA_NAMING_MODEL_ROOT=/absolute/exact/cached-gguf-directory
//	MYCE_TEST_NATIVE_CUDA_NAMING_MODEL=/absolute/exact/cached-gguf-directory/Qwen3.8-27B-Q4_K_M.gguf
//	MYCE_TEST_NATIVE_CUDA_NAMING_MODEL_SHA256=<nativeCUDANamingModelSHA>
//	MYCE_TEST_NATIVE_CUDA_NAMING_LOG_DIR=/absolute/private/qualification-logs
//
// Retain outer MYCE_PLATFORM=cuda, single MYCE_DEVICES, CUDA UUID mask and
// trusted loader paths (if needed) from the scheduler/operator. Never pass a
// whole home, shared temporary tree or private application tree as a root.
// Reserve one GPU, 2 CPU cores, 28 GiB GPU and 40 GiB host RAM externally; the
// inner worker is admission-only and inherits that existing finite cgroup.
// Run -test.run='^TestNativeIsolationCUDANaming$' -test.timeout=4m.
// Both hashes are read/verified only during this opt-in run (the GGUF is large).
// 1024 context / <=64 generated tokens prove backend loading and synthetic
// naming, not full production context, quality parity or determinism. Linux is
// offline: no native_network claim; fallback pathname SEQPACKET remains partial.
func TestNativeIsolationCUDANaming(t *testing.T) {
	opt, requested := os.LookupEnv(nativeCUDANamingPrefix)
	if !requested {
		t.Skip("real CUDA naming requires explicit " + nativeCUDANamingPrefix + "=1")
	}
	if opt != "1" {
		t.Fatal("CUDA naming opt-in must equal 1")
	}
	runtimeRoot := nativeCUDANamingPath(t, "RUNTIME_ROOT", true)
	modelRoot := nativeCUDANamingPath(t, "MODEL_ROOT", true)
	server := nativeCUDANamingPath(t, "SERVER", false)
	model := nativeCUDANamingPath(t, "MODEL", false)
	logRoot := nativeCUDANamingPath(t, "LOG_DIR", true)
	if rel, err := filepath.Rel(runtimeRoot, server); err != nil || !filepath.IsLocal(rel) || rel == "." {
		t.Fatal("server must resolve inside the narrow trusted runtime root")
	}
	if filepath.Dir(model) != modelRoot || filepath.Base(model) != "Qwen3.8-27B-Q4_K_M.gguf" {
		t.Fatal("model root must be the exact cached GGUF directory")
	}
	info, err := os.Stat(server)
	if err != nil || info.Mode()&0111 == 0 {
		t.Fatal("configured server is not executable")
	}
	assigned, selector := os.Getenv("MYCE_DEVICES"), os.Getenv("CUDA_VISIBLE_DEVICES")
	if os.Getenv("MYCE_PLATFORM") != "cuda" || assigned == "" || singleDeviceMask(assigned) != nil ||
		!strings.HasPrefix(selector, "GPU-") || strings.ContainsAny(selector, ",; \r\n") {
		t.Fatal("requires outer scheduler CUDA placement and one physical CUDA UUID mask")
	}
	inventory := func(ctx context.Context) ([]MachineDevice, error) {
		before := probeguard.Before
		probeguard.Before = nil
		defer func() { probeguard.Before = before }()
		return LocalMachineProbe().Accelerators(ctx)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	devices, err := inventory(ctx)
	cancel()
	if err != nil {
		t.Fatal("actual CUDA inventory failed")
	}
	var selected *MachineDevice
	for _, d := range devices {
		if d.Platform == "cuda" && d.Selector == selector {
			if selected != nil || d.Reason != "" || d.Identity == "" || d.pciAddress == "" && d.pciFact == nil {
				t.Fatal("assigned CUDA physical identity is unavailable or ambiguous")
			}
			copy := d
			selected = &copy
		}
	}
	if selected == nil {
		t.Fatal("assigned CUDA UUID is absent from actual inventory")
	}
	selected.ID = assigned
	outer, err := nativeHardwareReadCgroup(os.Getpid())
	limit, parseErr := strconv.ParseUint(outer.Max, 10, 64)
	if err != nil || parseErr != nil || limit < 40<<30 {
		t.Fatal("requires existing outer scheduler cgroup with finite memory.max >= 40 GiB")
	}
	support := nativeLinuxDetection()
	if support.mode == "unavailable" {
		t.Fatal("explicit CUDA qualification requested but native isolation is unavailable")
	}
	nativeCUDANamingHash(t, "SERVER", server, nativeCUDANamingServerSHA)
	nativeCUDANamingHash(t, "MODEL", model, nativeCUDANamingModelSHA)
	t.Logf("native mode=%s; inherited memory.max=%s; inner caps admission-only, no member/OOM qualification", support.mode, outer.Max)
	setWorkerSession(t)
	w := recoveryWorker(t) // Queue authorization only is synthetic; processes are real.
	w.Machine = Machine{ID: "native-cuda-naming", GPU: "cuda", Device: assigned, Devices: []MachineDevice{*selected}}
	w.DeviceBinding, w.selectedDevice, w.InventoryProbe = selector, selected, inventory
	w.AcceleratorStopGrace = time.Second
	// Admission uses the already reserved outer ceiling, not synthetic free
	// host memory. No inner cgroup/delegation or additional reservation exists.
	w.HostMemorySource = func(context.Context) (float64, error) { return float64(limit) / (1 << 30), nil }
	// Initialize ordinary managed spool/cache markers before configuring read
	// grants for an externally installed runtime. This start-only fixture never
	// runs the environment cache manager against the installed runtime root.
	store, err := w.openStore()
	if err != nil {
		t.Fatal("prepare private marked worker state failed")
	}
	defer store.close()
	w.NativeExecuteRoots, w.NativeReadRoots = []string{runtimeRoot}, []string{modelRoot}
	credential := filepath.Join(store.dir, "service", "joining.json")
	sibling := filepath.Join(store.dir, "job-files", "sibling", "1", "secret")
	protected := []string{credential, sibling, filepath.Join(filepath.Dir(sibling), ".tmp", "secret")}
	for _, path := range protected {
		nativeCompatibilityWrite(t, path, "synthetic-naming-private")
	}
	w.ServiceConfig = &ServiceConfig{JoiningFile: credential}
	control, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal("create synthetic TCP control listener failed")
	}
	defer control.Close()
	w.ModelListen = control.Addr().String() // Ordinary worker control-port denial.
	socket := nativeIsolationLinuxControlSocket(t)
	unixControl, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal("create synthetic Unix stream control listener failed")
	}
	defer unixControl.Close()
	for _, target := range []struct{ network, address string }{{"tcp4", control.Addr().String()}, {"unix", socket}} {
		conn, err := net.DialTimeout(target.network, target.address, time.Second)
		if err != nil {
			t.Fatal("synthetic control listener is unavailable before confinement")
		}
		conn.Close()
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal("resolve qualification executable failed")
	}
	bound, err := w.deviceEnvironment()
	if err != nil {
		t.Fatal("bind assigned CUDA device failed")
	}
	config := nativeCUDANamingConfig{Assigned: assigned, Masks: bound, Protected: protected,
		WorkerPID: os.Getpid(), ControlTCP: control.Addr().String(), ControlUnix: socket,
		Server: server, Model: model}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal("encode qualification configuration failed")
	}
	job := recoveryJob(t, []string{executable, "_native-cuda-naming-test", "main", string(data)})
	job.Spec.Resumable, job.Spec.Logs = false, "keep"
	job.Spec.Needs.MemoryGB, job.Spec.Needs.HostMemoryGB, job.Spec.Needs.CPUCores = 28, 40, 2
	job.Spec.Run.Env = map[string]string{"TMPDIR": "/tmp/ignored-authored-temp"}
	zero := 0
	job.Spec.Run.Sidecars = []NativeSidecar{{Name: "llama", Command: []string{executable, "_native-cuda-naming-test", "side", string(data)},
		Port: &zero, Readiness: &NativeReadiness{Path: "/health", Timeout: "150s"}}}
	attempt, err := newAttemptStore(store, job)
	if err != nil {
		t.Fatal("prepare qualification attempt failed")
	}
	// Raw llama logs can include operator paths. Keep them in an explicitly
	// private directory, never t.Log/public receipts. Inputs are synthetic only.
	logFile, err := os.OpenFile(filepath.Join(logRoot, "native-cuda-naming-"+job.ID+".log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal("create private qualification log failed")
	}
	defer logFile.Close()
	defer func() {
		if attempt.state.Log != "" {
			out, err := os.ReadFile(filepath.Join(attempt.dir, attempt.state.Log))
			if err != nil {
				t.Error("read private attempt log failed")
			} else if _, err = logFile.Write(out); err != nil {
				t.Error("preserve private attempt log failed")
			}
		}
	}()
	p, err := w.start(attempt, job)
	if err != nil {
		fmt.Fprintln(logFile, err)
		t.Fatal("real Worker.start failed; inspect private qualification log")
	}
	finished := false
	defer func() {
		if !finished {
			p.stopDirective(w.Clock.Now(), 0, true, true)
			select {
			case f := <-p.done:
				if f.err != nil || f.result.ReleasePending {
					t.Error("failed qualification child cleanup unconfirmed")
				}
			case <-time.After(10 * time.Second):
				t.Error("failed qualification child cleanup timed out")
			}
		}
		p.mu.Lock()
		groups := make([]int, 0, len(p.native.custody.Groups))
		for pgid := range p.native.custody.Groups {
			groups = append(groups, pgid)
		}
		p.mu.Unlock()
		for _, pgid := range groups {
			if members, err := workerGroupMembers(pgid); err != nil || len(members) != 0 {
				t.Error("native naming group/sidecar cleanup incomplete")
			}
		}
		for _, path := range protected {
			if data, err := os.ReadFile(path); err != nil || string(data) != "synthetic-naming-private" {
				t.Error("synthetic protected fixture changed")
			}
		}
	}()
	// Hold each confined member before GPU exec/inference to observe the real
	// inherited cgroup, then release it without changing its process identity.
	tmp := filepath.Join(store.dir, "job-files", job.ID, strconv.Itoa(job.Attempt), ".tmp")
	ready := time.NewTimer(150 * time.Second)
	defer ready.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for _, member := range []string{"side", "main"} {
		for {
			if _, err := os.Stat(filepath.Join(tmp, member+"-ready")); err == nil {
				break
			}
			select {
			case f := <-p.done:
				finished = true
				fmt.Fprintln(logFile, f.err, f.result.Error)
				t.Fatal("confined naming member exited before release; inspect private log")
			case <-ready.C:
				t.Fatal("CUDA model readiness exceeded 150s")
			case <-tick.C:
			}
		}
		p.mu.Lock()
		pgid := p.cmd.Process.Pid
		p.mu.Unlock()
		members, err := workerGroupMembers(pgid)
		if err != nil || len(members) < 2 {
			t.Fatal("confined naming process group unavailable")
		}
		for _, pid := range members {
			if child, err := nativeHardwareReadCgroup(pid); err != nil || child != outer {
				t.Fatal("native naming child lost outer scheduler cgroup/memory ceiling")
			}
		}
		if err := os.WriteFile(filepath.Join(tmp, member+"-release"), nil, 0600); err != nil {
			t.Fatal("release observed confined naming member failed")
		}
	}
	select {
	case f := <-p.done:
		finished = true
		if f.err != nil || !f.result.Success || f.result.ReleasePending {
			fmt.Fprintln(logFile, f.err, f.result.Error)
			t.Error("real CUDA naming/generation or verified cleanup failed; inspect private log")
		}
	case <-time.After(45 * time.Second):
		t.Fatal("short naming inference and group cleanup exceeded 45s")
	}
	out, err := os.ReadFile(filepath.Join(attempt.dir, attempt.state.Log))
	// Positive offloaded layers and a nonzero CUDA model buffer distinguish
	// actual model GPU loading from CUDA discovery followed by CPU fallback.
	offloaded := regexp.MustCompile(`offloaded [1-9][0-9]*/[1-9][0-9]* layers to GPU`)
	gpuModel := regexp.MustCompile(`CUDA[0-9]+[^\n]*model buffer size\s*=\s*[1-9][0-9]*(?:\.[0-9]+)?\s*MiB`)
	if err != nil || !offloaded.Match(out) || !gpuModel.Match(out) || !bytes.Contains(out, []byte("native-cuda-naming-ok")) {
		t.Error("missing real CUDA model offload and structured naming receipts; inspect private log")
	}
}

func nativeCUDANamingPath(t *testing.T, key string, directory bool) string {
	t.Helper()
	path := os.Getenv(nativeCUDANamingPrefix + "_" + key)
	resolved, err := filepath.EvalSymlinks(path)
	if !filepath.IsAbs(path) || err != nil {
		t.Fatal("explicit absolute existing " + key + " is required")
	}
	info, err := os.Stat(resolved)
	if err != nil || info.IsDir() != directory || !directory && !info.Mode().IsRegular() {
		t.Fatal("invalid " + key + " file type")
	}
	if key == "LOG_DIR" && info.Mode().Perm()&0077 != 0 {
		t.Fatal("LOG_DIR must be private to its owner")
	}
	for _, broad := range []string{"/", "/home", "/tmp", "/var/tmp", nativeCUDANamingOperatorHome} {
		if directory && resolved == broad {
			t.Fatal(key + " must be a narrow dedicated directory")
		}
	}
	return resolved
}

func nativeCUDANamingHash(t *testing.T, key, path, pinned string) {
	t.Helper()
	if os.Getenv(nativeCUDANamingPrefix+"_"+key+"_SHA256") != pinned {
		t.Fatal(key + "_SHA256 must match the pinned qualification artifact")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal("open " + key + " for qualification hashing failed")
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil || hex.EncodeToString(h.Sum(nil)) != pinned {
		t.Fatal(key + " content SHA256 mismatch or read failure")
	}
}

type nativeCUDANamingConfig struct {
	Assigned, Server, Model, ControlTCP, ControlUnix string
	WorkerPID                                        int
	Masks                                            map[string]string
	Protected                                        []string
}

// Capture the operator home before TestMain isolates HOME. Helpers dispatch
// before TestMain so they cannot provision state or change probe guards.
var nativeCUDANamingOperatorHome = os.Getenv("HOME")

func init() {
	if len(os.Args) > 1 && os.Args[1] == "_native-cuda-naming-test" {
		if err := nativeCUDANamingPayload(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

func nativeCUDANamingPayload(args []string) error {
	if len(args) != 2 || args[0] != "side" && args[0] != "main" {
		return errors.New("invalid CUDA naming helper arguments")
	}
	var config nativeCUDANamingConfig
	if err := json.Unmarshal([]byte(args[1]), &config); err != nil {
		return err
	}
	if os.Getenv("MYCE_PLATFORM") != "cuda" || os.Getenv("MYCE_DEVICES") != config.Assigned {
		return errors.New("naming payload lost scheduler placement")
	}
	for key, want := range config.Masks {
		if os.Getenv(key) != want {
			return errors.New("naming payload lost assigned runtime mask")
		}
	}
	for key := range workerSessionEnv {
		if os.Getenv(key) != "" {
			return errors.New("naming payload inherited worker control/session environment")
		}
	}
	work, err := os.Getwd()
	if err != nil {
		return err
	}
	tmp, err := filepath.EvalSymlinks(os.Getenv("TMPDIR"))
	if err != nil || tmp != filepath.Join(work, ".tmp") || os.Getenv("TMP") != tmp || os.Getenv("TEMP") != tmp {
		return errors.New("naming payload temp escaped managed attempt")
	}
	for _, path := range config.Protected {
		if _, err := os.ReadFile(path); err == nil {
			return errors.New("naming payload read synthetic credential/sibling")
		}
		if err := os.WriteFile(path, []byte("escaped"), 0600); err == nil {
			return errors.New("naming payload wrote synthetic credential/sibling")
		}
	}
	if _, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", config.WorkerPID)); err == nil {
		return errors.New("naming payload read worker process environment")
	}
	for _, target := range []struct{ network, address string }{{"tcp4", config.ControlTCP}, {"unix", config.ControlUnix}} {
		if conn, err := net.DialTimeout(target.network, target.address, time.Second); err == nil {
			conn.Close()
			return errors.New("naming payload reached synthetic worker control listener")
		}
	}
	address := os.Getenv("MYCE_SIDECAR_LLAMA_URL")
	u, err := url.Parse(address)
	port := os.Getenv("MYCE_SIDECAR_LLAMA_PORT")
	n, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || portErr != nil || n == 0 || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.Port() != port {
		return errors.New("naming payload lacks exact worker-reserved sidecar URL/port")
	}
	if args[0] == "side" && (os.Getenv("MYCE_SIDECAR_PORT") != port || os.Getenv("MYCE_SIDECAR_URL") != address) {
		return errors.New("llama sidecar lost its own reserved endpoint")
	}
	if err := os.WriteFile(filepath.Join(tmp, args[0]+"-ready"), nil, 0600); err != nil {
		return err
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(filepath.Join(tmp, args[0]+"-release")); err == nil {
			break
		}
		<-tick.C
	}
	if args[0] == "side" {
		// Exec retains the real group, physical-device mask, inherited cgroup,
		// Landlock/seccomp and ordinary trusted loader environment ordering.
		// The pinned runtime maps libllama INFO (offload/buffer receipts) to
		// common TRACE (4), above its default INFO (3). Set this on argv rather
		// than relying on operator env surviving the job environment allowlist.
		// Keep plain text so terminal coloring cannot obscure strict receipts.
		argv := []string{config.Server, "--model", config.Model, "--host", "127.0.0.1", "--port", port,
			"--ctx-size", "1024", "--n-gpu-layers", "999", "--parallel", "1", "--batch-size", "128",
			"--ubatch-size", "128", "--threads", "2", "--n-predict", "64",
			"--verbosity", "4", "--log-colors", "off"}
		return syscall.Exec(argv[0], argv, os.Environ())
	}
	if _, err := os.Stat(filepath.Join(tmp, "side-ready")); err != nil {
		return errors.New("main and real llama sidecar did not share managed temp")
	}
	// Synthetic naming-only transcript. The raw completion prompt closes the
	// thinking prefix so the 64-token budget belongs to the structured answer.
	prompt := "<|im_start|>system\nExtract the speaker's name from the synthetic transcript. Return only JSON mapping SPEAKER_00 to the name, including the title.\n<|im_end|>\n" +
		"<|im_start|>user\nSPEAKER_00: I am Judge Avery. This is a synthetic naming test. /no_think\n<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
	body, err := json.Marshal(map[string]any{"prompt": prompt, "n_predict": 64, "temperature": 0, "stream": false, "cache_prompt": false,
		"json_schema": map[string]any{"type": "object", "properties": map[string]any{"SPEAKER_00": map[string]string{"type": "string"}},
			"required": []string{"SPEAKER_00"}, "additionalProperties": false}})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 35 * time.Second, Transport: &http.Transport{Proxy: nil},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	response, err := client.Post(address+"/completion", "application/json", bytes.NewReader(body))
	if err != nil {
		return errors.New("confined real llama inference request failed")
	}
	defer response.Body.Close()
	var completion struct {
		Content         string `json:"content"`
		TokensPredicted int    `json:"tokens_predicted"`
		TokensEvaluated int    `json:"tokens_evaluated"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&completion); err != nil || response.StatusCode != http.StatusOK ||
		completion.TokensPredicted <= 0 || completion.TokensPredicted > 64 || completion.TokensEvaluated <= 0 {
		return errors.New("real llama did not return a successful bounded generation")
	}
	var naming map[string]string
	if err := json.Unmarshal([]byte(completion.Content), &naming); err != nil || len(naming) != 1 || strings.TrimSpace(naming["SPEAKER_00"]) != "Judge Avery" {
		return errors.New("real llama returned no valid synthetic Judge Avery naming response")
	}
	fmt.Fprintln(os.Stdout, "native-cuda-naming-ok: SPEAKER_00=Judge Avery; context=1024; max_output=64")
	return nil
}
