package myce

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FleetRuntimeProof describes this process, not the executable currently at its
// installation path. It is separate from enrollment so missing fleet evidence
// never prevents an ordinary worker joining or finishing its work.
type FleetRuntimeProof struct {
	InstalledSlot         *ReleaseSlotIdentity `json:"installed_slot,omitempty"`
	SlotMismatch          bool                 `json:"slot_mismatch,omitempty"`
	SlotError             string               `json:"slot_error,omitempty"`
	ObservationAgeSeconds float64              `json:"observation_age_seconds"`
	Failure               string               `json:"failure,omitempty"`
	Version               string               `json:"version"`
	SHA256                string               `json:"sha256"`
	Session               string               `json:"session"`
	PID                   int                  `json:"pid"`
	ServiceHealthy        bool                 `json:"service_healthy"`
	StartGate             string               `json:"start_gate"`
	AttemptProtocol       string               `json:"attempt_protocol"`
	RunnerSmoke           bool                 `json:"runner_smoke"`
	ObservedAt            time.Time            `json:"observed_at"`
}

// Capture before command dispatch, including before an installed pathname can
// be replaced. Keep the descriptor for the process lifetime; never reopen it.
var fleetStartupImage = captureFleetImage()

type fleetImage struct {
	file *os.File
	err  error
	once sync.Once
	hash string
}

func captureFleetImage() *fleetImage {
	f, err := openFleetRunningImage()
	return &fleetImage{file: f, err: err}
}

func (image *fleetImage) sha256() (string, error) {
	image.once.Do(func() {
		if image.err != nil {
			return
		}
		before, err := image.file.Stat()
		if err == nil {
			err = verifyFleetRunningImage(image.file)
		}
		if err != nil || !before.Mode().IsRegular() {
			image.err = errors.New("running executable identity unavailable")
			return
		}
		hash := sha256.New()
		_, err = io.Copy(hash, io.NewSectionReader(image.file, 0, before.Size()))
		after, statErr := image.file.Stat()
		if err != nil || statErr != nil || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
			image.err = errors.New("running executable changed during observation")
			return
		}
		image.hash = hex.EncodeToString(hash.Sum(nil))
	})
	return image.hash, image.err
}

type fleetSmokeResult struct {
	once sync.Once
	err  error
}

var fleetSmokeResults sync.Map // state directory -> *fleetSmokeResult

func fleetRunnerSmoke(ctx context.Context, dir string) error {
	value, _ := fleetSmokeResults.LoadOrStore(dir, &fleetSmokeResult{})
	r := value.(*fleetSmokeResult)
	r.once.Do(func() {
		// A failed smoke withholds fleet activation for this process; normal work
		// continues. No retry can silently turn a failed startup into readiness.
		probe, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		r.err = cleanupRecordedProcesses(dir, "enrollment-command-*.json", RealClock{})
		if r.err == nil {
			r.err = enrollmentRunnerSmoke(probe, dir, RealClock{})
		}
	})
	return r.err
}

// CollectFleetRuntime performs local hashing, recorded runner smoke, and a fresh
// service-manager probe. Call outside queue locks and heartbeat loops. Image
// hashing and runner smoke happen once per process (smoke per state directory).
// A partial proof on error is diagnostic only and cannot authorize activation.
func CollectFleetRuntime(ctx context.Context, c ServiceConfig, session string) (*FleetRuntimeProof, error) {
	return collectFleetRuntime(ctx, c, session, LocalMachineProbe().Command)
}

func collectFleetRuntime(ctx context.Context, c ServiceConfig, session string, command func(context.Context, string, ...string) ([]byte, error)) (*FleetRuntimeProof, error) {
	return collectFleetRuntimeAt(ctx, c, session, command, RealClock{})
}

func collectFleetRuntimeAt(ctx context.Context, c ServiceConfig, session string, command func(context.Context, string, ...string) ([]byte, error), clock Clock) (*FleetRuntimeProof, error) {
	if session == "" || c.State == "" || c.Config == "" || c.Service == "" || (c.Role != "worker" && c.Role != "queue") {
		return nil, errors.New("installed service identity unavailable")
	}
	hash, err := fleetStartupImage.sha256()
	if err != nil {
		return nil, errors.New("running executable proof unavailable")
	}
	p := &FleetRuntimeProof{Version: BuildVersion, SHA256: hash, Session: session, PID: os.Getpid(), StartGate: startGateProtocol, AttemptProtocol: keyedAttemptProtocol}
	slot, slotErr := observeReleaseSlot(c.Binary)
	if slotErr != nil {
		p.SlotError = "fleet_binary_unavailable"
	} else {
		p.InstalledSlot = &slot
		p.SlotMismatch = slot.SHA256 != hash
		if !p.SlotMismatch {
			slot.Version = p.Version
		}
	}

	if err := fleetRunnerSmoke(ctx, c.State); err != nil {
		p.Failure = "runner_smoke_failed"
		return p, errors.New("fleet runner smoke unavailable")
	}
	p.RunnerSmoke = true
	// A stuck service manager must not monopolize the asynchronous observer.
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	identity, err := EnrollmentServiceHealth(probe, c, command)
	pid, _, _ := strings.Cut(identity, "/")
	if err != nil || pid != strconv.Itoa(p.PID) {
		return p, errors.New("running service identity unavailable")
	}
	p.ServiceHealthy = true
	p.ObservedAt = clock.Now()
	return p, nil
}

// Worker-loop-owned state. Background calls receive immutable snapshots and
// deliver a value through pending; they never write Worker or Machine fields.
type workerFleetRuntime struct {
	failureReceipt string
	config         ServiceConfig
	client         Client
	collect        func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error)
	launch         func(context.Context, ServiceConfig, FleetUpdateInstruction) error
	pending        chan fleetPollResult
	proof          *FleetRuntimeProof
	attempted      string
	failed         *fleetLaunchFailure
	clock          Clock
	generation     uint64
	acks           map[string]fleetAckRetry
	// From the last check-in reply. Until a reply carries the protocol (a
	// previous-release queue, or before the first reply) every poll fetches.
	instructionGated, instructionWaiting bool
}

type fleetPollResult struct {
	failureReceipt string
	proof          *FleetRuntimeProof
	attempted      string
	failed         *fleetLaunchFailure
	acks           map[string]fleetAckRetry
	// Set on the poll's input when the queue says no instruction can be
	// waiting; proof, receipts and acknowledgements still go out.
	skipInstruction bool
}

// noteReply records whether the queue says this worker's upgrade instruction
// may be waiting. Call from the worker loop after each accepted check-in.
func (r *workerFleetRuntime) noteReply(reply Reply) {
	if r == nil {
		return
	}
	r.instructionGated = reply.FleetInstructionProtocol == fleetInstructionProtocol
	r.instructionWaiting = reply.FleetInstructionWaiting
}

// Delivery state for one acknowledgement body, owned by the worker loop.
type fleetAckRetry struct {
	failures int
	retryAt  time.Time
	settled  bool
}

// Bounded exponential retry for undelivered acknowledgements (#600). The 2 s
// base is about one worker poll, so one transient blip costs one poll; the
// 5 min cap turns a dead control path into 12 posts an hour, not thousands.
const fleetAckRetryBase, fleetAckRetryMax = 2 * time.Second, 5 * time.Minute

// A 4xx other than authentication, timeout or rate limiting is a definitive
// refusal: resending the same body cannot change the queue's answer. The queue
// types every fleet and decode refusal, so its untyped 400 request_failed means
// an internal failure such as a state commit error, which a retry can clear.
func fleetAckDefinitive(err error) bool {
	var rejection *HTTPError
	if !errors.As(err, &rejection) || rejection.Status < 400 || rejection.Status >= 500 || rejection.Code == "request_failed" {
		return false
	}
	return rejection.Status != 401 && rejection.Status != 403 && rejection.Status != 408 && rejection.Status != 429
}

// deliverFleetAck posts ack unless key is settled or still backing off. Success
// or a definitive refusal settles it. Any other failure backs off
// exponentially on the injected clock.
func deliverFleetAck(ctx context.Context, client Client, clock Clock, acks map[string]fleetAckRetry, key string, ack any) bool {
	state := acks[key]
	if state.settled || clock.Now().Before(state.retryAt) {
		return state.settled
	}
	request, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := client.Call(request, "POST", "/api/fleet/ack", ack, nil)
	cancel()
	if err == nil || fleetAckDefinitive(err) {
		acks[key] = fleetAckRetry{settled: true}
		return true
	}
	state.failures++
	state.retryAt = clock.Now().Add(fleetAckRetryMax)
	if state.failures <= 8 {
		state.retryAt = clock.Now().Add(min(fleetAckRetryMax, fleetAckRetryBase<<(state.failures-1)))
	}
	acks[key] = state
	return false
}

type fleetLaunchFailure struct {
	Operation string `json:"operation"`
	Revision  int    `json:"revision"`
	Host      string `json:"host"`
	Machine   string `json:"machine"`
	Session   string `json:"session"`
	Phase     string `json:"phase"`
	Failure   string `json:"failure"`
}

func (w Worker) newFleetRuntime() *workerFleetRuntime {
	if w.ServiceConfig == nil || w.ServiceConfig.Role != "worker" || w.ServiceConfig.Config == "" || w.ServiceConfig.State == "" || w.ServiceConfig.Service == "" {
		return nil
	}
	// Hash once at startup, outside the worker loop. Failure withholds fleet
	// support only; it is never an error returned by Worker.Run.
	if _, err := fleetStartupImage.sha256(); err != nil {
		return nil
	}
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	return &workerFleetRuntime{config: *w.ServiceConfig, client: w.Client, clock: clock, collect: func(ctx context.Context, c ServiceConfig, session string) (*FleetRuntimeProof, error) {
		return collectFleetRuntimeAt(ctx, c, session, LocalMachineProbe().Command, clock)
	}, launch: LaunchFleetUpdate}
}

// Reuse the shortest default worker-loss threshold rather than extending fleet
// authority indefinitely while a manager/control request is stuck. Queues may
// use their configured, shorter loss threshold when evaluating the timestamp.
const fleetRuntimeFreshFor = min(ProductionLossAfter, DeadlineLossAfter, LossAfter)

func (r *workerFleetRuntime) observe(ctx context.Context, m Machine, eligible bool) *FleetRuntimeProof {
	if r == nil || m.Session == "" {
		return nil
	}
	if r.pending != nil {
		select {
		case result := <-r.pending:
			r.proof, r.attempted, r.failed = result.proof, result.attempted, result.failed
			r.failureReceipt, r.acks = result.failureReceipt, result.acks
			r.generation++
			r.pending = nil
		default:
		}
	}
	if r.pending == nil && ctx.Err() == nil {
		pending := make(chan fleetPollResult, 1)
		r.pending = pending
		config, client, collect, launch := r.config, r.client, r.collect, r.launch
		prior, clock := fleetPollResult{attempted: r.attempted, failed: r.failed, failureReceipt: r.failureReceipt, acks: maps.Clone(r.acks), skipInstruction: r.instructionGated && !r.instructionWaiting}, r.clock
		if clock == nil {
			clock = RealClock{}
		}
		// m is used only for scalar identity, never mutable Machine pointers.
		host, machine, session := hostID(&m), m.ID, m.Session
		go func() {
			pending <- pollFleetRuntimeAt(ctx, config, client, collect, launch, host, machine, session, eligible, prior, clock)
		}()
	}
	if r.proof == nil || r.proof.Session != m.Session {
		return nil
	}
	clock := r.clock
	if clock == nil {
		clock = RealClock{}
	}
	now := clock.Now()
	if r.proof.ObservedAt.IsZero() || now.Before(r.proof.ObservedAt) || now.Sub(r.proof.ObservedAt) >= fleetRuntimeFreshFor {
		return nil
	}
	p := *r.proof
	p.ObservationAgeSeconds = now.Sub(r.proof.ObservedAt).Seconds()
	return &p
}

func pollFleetRuntime(ctx context.Context, config ServiceConfig, client Client, collect func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error), launch func(context.Context, ServiceConfig, FleetUpdateInstruction) error, host, machine, session string, eligible bool, attempted string, failed *fleetLaunchFailure, handled ...string) fleetPollResult {
	prior := fleetPollResult{attempted: attempted, failed: failed}
	if len(handled) > 0 {
		prior.failureReceipt = handled[0]
	}
	return pollFleetRuntimeAt(ctx, config, client, collect, launch, host, machine, session, eligible, prior, RealClock{})
}

func pollFleetRuntimeAt(ctx context.Context, config ServiceConfig, client Client, collect func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error), launch func(context.Context, ServiceConfig, FleetUpdateInstruction) error, host, machine, session string, eligible bool, prior fleetPollResult, clock Clock) fleetPollResult {
	result := prior
	result.proof = nil
	if result.acks == nil {
		result.acks = map[string]fleetAckRetry{}
	}
	// A restarted service offers a durable helper failure before its new proof.
	// Keep the local receipt; only deduplicate and pace delivery. An undelivered
	// receipt never withholds the proof (#600), but blocks new instructions.
	receipt, readErr := ReadFleetUpdateFailure(config)
	if readErr != nil {
		return result
	}
	receiptPending := false
	if receipt != nil {
		key := receipt.Operation + "/" + strconv.Itoa(receipt.Revision)
		if result.failureReceipt != key {
			if deliverFleetAck(ctx, client, clock, result.acks, "receipt/"+key, receipt) {
				result.failureReceipt = key
			} else {
				receiptPending = true
			}
		}
	}
	proof, err := collect(ctx, config, session)
	result.proof = proof
	if receiptPending || err != nil || proof == nil || !proof.ServiceHealthy || !eligible || ctx.Err() != nil {
		return result
	}
	// Match the existing client's control-request budget even for injected HTTP
	// clients without a timeout. A stalled control path never stalls heartbeats.
	request, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if failed := result.failed; failed != nil {
		// A definitive (for example revision-fenced) refusal settles an obsolete
		// acknowledgement, so its retry never conceals a newer recovery.
		if deliverFleetAck(ctx, client, clock, result.acks, "launch/"+failed.Operation+"/"+strconv.Itoa(failed.Revision), failed) {
			result.failed = nil
		}
		return result
	}
	if prior.skipInstruction {
		return result
	}
	key, err := provisionFleetPublisher(request, config.State, client)
	if err != nil {
		return result
	}
	query := url.Values{"host": {host}, "machine": {machine}, "session": {session}}
	var raw json.RawMessage
	err = client.Call(request, "GET", "/api/fleet/instruction?"+query.Encode(), nil, &raw)
	if err != nil && !errors.Is(err, io.EOF) {
		return result
	}
	var instruction FleetUpdateInstruction
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return result
	}
	var wrapped struct {
		Instruction *FleetUpdateInstruction `json:"instruction"`
	}
	if json.Unmarshal(raw, &instruction) != nil || json.Unmarshal(raw, &wrapped) != nil {
		return result
	}
	if wrapped.Instruction != nil {
		instruction = *wrapped.Instruction
	}
	if instruction.Operation == "" || instruction.Revision <= 0 || fleetInstructionKey(instruction) == prior.attempted || instruction.Host != host || instruction.Machine != machine || instruction.Session != session || ctx.Err() != nil {
		return result
	}
	result.attempted = fleetInstructionKey(instruction)
	// Instructions can only agree with existing trust. They never provision or
	// rotate it. The finite helper independently rechecks this same local key.
	var launchErr error
	if !bytes.Equal(key, instruction.PublisherKey) {
		launchErr = errors.New("fleet_publisher_mismatch")
	} else {
		launchErr = launch(request, config, instruction)
	}
	if launchErr != nil {
		var refetch *fleetInstructionRefetchError
		if errors.As(launchErr, &refetch) {
			result.attempted = ""
			return result // The next ordinary poll refetches; never loop here.
		}
		code := fleetFailureCode(launchErr.Error())
		var refusal *HTTPError
		if errors.As(launchErr, &refusal) {
			code = fleetFailureCode(refusal.Code)
		}
		if code == "fleet_failure_code_unrecognized" {
			code = "fleet_helper_launch_unclassified"
		}
		failure := &fleetLaunchFailure{instruction.Operation, instruction.Revision, host, machine, session, "failed", code}
		if !deliverFleetAck(ctx, client, clock, result.acks, "launch/"+fleetInstructionKey(instruction), failure) {
			result.failed = failure
		}
	}
	return result
}

func fleetInstructionKey(i FleetUpdateInstruction) string {
	return i.Operation + "/" + strconv.Itoa(i.Revision)
}

func readFleetPublisher(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != ed25519.PublicKeySize {
		return nil, errors.New("local fleet publisher trust invalid")
	}
	key, err := os.ReadFile(path)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("local fleet publisher trust unavailable")
	}
	return key, nil
}

func provisionFleetPublisher(ctx context.Context, state string, client Client) ([]byte, error) {
	path := filepath.Join(state, "fleet-publisher.pub")
	key, err := readFleetPublisher(path)
	if !errors.Is(err, os.ErrNotExist) {
		return key, err
	}
	var bootstrap struct {
		PublicKey []byte `json:"public_key"`
	}
	if err := client.Call(ctx, "GET", "/api/fleet/publisher", nil, &bootstrap); err != nil {
		return nil, err
	}
	if len(bootstrap.PublicKey) != ed25519.PublicKeySize {
		return nil, errors.New("fleet publisher bootstrap unavailable")
	}
	// Publish a complete, synced key without ever overwriting concurrent local
	// provisioning/rotation. A partial key after interruption is never trusted.
	f, err := os.CreateTemp(state, ".fleet-publisher-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(bootstrap.PublicKey); err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	if err = os.Link(f.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	dir, err := os.Open(state)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return nil, err
	}
	return readFleetPublisher(path)
}
