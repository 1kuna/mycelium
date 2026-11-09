package myce

import (
	"context"

	"net/http/httptest"

	"strings"

	"testing"
	"time"
)

func TestWorkerHygieneCollectionDoesNotHoldCancelledRelease(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
	if err := cache.init(); err != nil {
		t.Fatal(err)
	}
	old := pruneFixture(t, cache, 1, 48*time.Hour)
	recent := pruneFixture(t, cache, 2, time.Hour)
	attempt, cancelAttempt := context.WithCancel(context.Background())
	_, release, err := cache.Acquire(attempt, EnvironmentRecipe{Key: recent})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	defer cancelAttempt()
	cache.Limit = 1
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan CacheCollection, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- cache.collectBudget(ctx, func(ctx context.Context, key string) (bool, error) {
			if key != old {
				return true, nil
			}
			close(entered)
			select {
			case <-resume:
				return false, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		})
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("collection did not reach reference lookup")
	}
	cancelAttempt()
	released := make(chan struct{})
	go func() { release(); close(released) }()
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated collection blocked cancelled attempt release")
	}
	// Release/admission can also own the gate while reference lookup is blocked.
	gate, err := cache.tryLock("admission")
	if err != nil || gate == nil {
		t.Fatal("collector retained admission across network lookup", err)
	}
	unlockEnvironment(gate)
	close(resume)
}

func TestWorkerHygieneCollectionPublicDiagnostics(t *testing.T) {
	q, _ := fixture(t)
	m := machine("collector-status", "fake")
	m.HygieneCollection = &WorkerHygieneCollection{FileCache: CacheCollection{Error: "synthetic-private-file"}, Environments: CacheCollection{Error: "synthetic-private-environment"}, StagingError: "synthetic-private-staging"}
	check(t, q, m, nil)
	request := httptest.NewRequest("GET", "/api/status", nil)
	request.Header.Set("Authorization", "Bearer test")
	response := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(response, request)
	if response.Code != 200 || strings.Contains(response.Body.String(), "synthetic-private") {
		t.Fatal("worker diagnostics escaped public projection", response.Code)
	}
	for _, code := range []string{"file_cache_collect_failed", "environment_collect_failed", "installer_staging_retained"} {
		if !strings.Contains(response.Body.String(), code) {
			t.Fatal("failure evidence was hidden", code)
		}
	}
	if q.s.Machines[m.ID].HygieneCollection.StagingError != m.HygieneCollection.StagingError {
		t.Fatal("public projection mutated queue evidence")
	}
	m.HygieneCollection = &WorkerHygieneCollection{FileCache: CacheCollection{Error: "file_cache_collect_changed"}, Environments: CacheCollection{Error: "environment_references_unavailable"}, StagingError: "installer_staging_retained"}
	check(t, q, m, nil)
	request = httptest.NewRequest("GET", "/api/status", nil)
	request.Header.Set("Authorization", "Bearer test")
	response = httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), "file_cache_collect_changed") || !strings.Contains(response.Body.String(), "environment_references_unavailable") {
		t.Fatal("known diagnostic was lost")
	}
}
