package contract

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
)

type fatalPanic string

type fakeFatalReporter struct{}

func (fakeFatalReporter) Helper() {}

func (fakeFatalReporter) Fatal(args ...any) {
	panic(fatalPanic(fmt.Sprint(args...)))
}

func (fakeFatalReporter) Fatalf(format string, args ...any) {
	panic(fatalPanic(fmt.Sprintf(format, args...)))
}

func expectFatal(t *testing.T, want string, fn func(fatalReporter)) {
	t.Helper()
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("expected fatal containing %q", want)
		}
		msg, ok := recovered.(fatalPanic)
		if !ok {
			panic(recovered)
		}
		if !strings.Contains(string(msg), want) {
			t.Fatalf("fatal = %q, want %q", msg, want)
		}
	}()
	fn(fakeFatalReporter{})
}

func TestFederationHelperFatalBranches(t *testing.T) {
	oldTimeout := conformanceWatchTimeout
	t.Cleanup(func() { conformanceWatchTimeout = oldTimeout })

	conformanceWatchTimeout = time.Second
	closedJobs := make(chan domain.JobRecord)
	close(closedJobs)
	expectFatal(t, "job watch channel closed", func(reporter fatalReporter) {
		_ = receiveJobRecord(reporter, closedJobs, "job-a")
	})
	closedPeers := make(chan domain.Peer)
	close(closedPeers)
	expectFatal(t, "peer watch channel closed", func(reporter fatalReporter) {
		_ = receivePeer(reporter, closedPeers, "peer-a")
	})
	stillOpen := make(chan domain.JobRecord, 1)
	stillOpen <- domain.JobRecord{JobID: "job-a"}
	expectFatal(t, "watch channel remained open", func(reporter fatalReporter) {
		assertChannelClosed(reporter, stillOpen)
	})

	conformanceWatchTimeout = time.Nanosecond
	openJobs := make(chan domain.JobRecord)
	expectFatal(t, "timed out waiting for job watch update", func(reporter fatalReporter) {
		_ = receiveJobRecord(reporter, openJobs, "job-a")
	})

	openPeers := make(chan domain.Peer)
	expectFatal(t, "timed out waiting for peer watch update", func(reporter fatalReporter) {
		_ = receivePeer(reporter, openPeers, "peer-a")
	})

	neverClosed := make(chan domain.JobRecord)
	expectFatal(t, "timed out waiting for watch channel to close", func(reporter fatalReporter) {
		assertChannelClosed(reporter, neverClosed)
	})
}
