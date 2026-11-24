package myce

import (
	"context"
	"errors"
	"testing"
)

func TestEnvironmentRecipeStorePublicationFence(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	catalog := EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"fake": r}}}
	if err := q.ConfigureEnvironments(catalog); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "tools"
	entered, unblock := gateSubmitVerification(t, q, nil)
	done := startSubmitVerification(q, context.Background(), s)
	submitVerificationWait(t, "recipe publication", entered)
	// Configuration must remain responsive while blob durability is blocked,
	// and its replacement must fence the stale recipe before commit.
	submitVerificationProgress(t, "catalog replacement", func() error { return q.ConfigureEnvironments(catalog) })
	unblock()
	got := submitVerificationWait(t, "submission", done)
	if !errors.Is(got.err, ErrStale) {
		t.Fatalf("catalog replacement did not fence submission: %v", got.err)
	}
	requireNoSubmission(t, q)
}
