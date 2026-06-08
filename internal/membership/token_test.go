package membership

import (
	"context"
	"errors"
	"testing"

	"mycelium/internal/domain"
)

func TestTokenManagerValidateRotateRevoke(t *testing.T) {
	manager, err := NewTokenManager("one")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	if err := manager.Validate("one"); err != nil {
		t.Fatalf("Validate one: %v", err)
	}
	if err := manager.Rotate("two"); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if err := manager.Validate("one"); err != nil {
		t.Fatalf("old token remains active until revoked: %v", err)
	}
	if err := manager.Validate("two"); err != nil {
		t.Fatalf("new token: %v", err)
	}
	hash, err := manager.CurrentHash()
	if err != nil {
		t.Fatalf("CurrentHash: %v", err)
	}
	if err := manager.ValidateHash(hash); err != nil {
		t.Fatalf("ValidateHash current: %v", err)
	}
	if err := manager.ValidateHash(""); err == nil {
		t.Fatal("empty hash validated")
	}
	if err := manager.Revoke("one"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if err := manager.Validate("one"); err == nil {
		t.Fatal("revoked token validated")
	}
}

func TestTokenManagerRejectsEmptyTokenOperations(t *testing.T) {
	if _, err := NewTokenManager(""); err == nil {
		t.Fatal("empty initial token accepted")
	}
	manager, err := NewTokenManager("one")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	if err := manager.Validate(""); err == nil {
		t.Fatal("empty validate accepted")
	}
	if err := manager.Rotate(""); err == nil {
		t.Fatal("empty rotate accepted")
	}
	if err := manager.Revoke(""); err == nil {
		t.Fatal("empty revoke accepted")
	}
	if err := manager.Revoke("one"); err != nil {
		t.Fatalf("Revoke current: %v", err)
	}
	if _, err := manager.CurrentHash(); err == nil {
		t.Fatal("revoked current hash returned")
	}
}

func TestPersistentTokenManagerLoadsRotatesAndRevokes(t *testing.T) {
	ctx := context.Background()
	store := &tokenStore{}
	manager, err := NewPersistentTokenManager(ctx, "one", store)
	if err != nil {
		t.Fatalf("NewPersistentTokenManager: %v", err)
	}
	if len(store.records) != 1 {
		t.Fatalf("records after seed = %+v", store.records)
	}
	if err := manager.Rotate("two"); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	reopenedWithRotation, err := NewPersistentTokenManager(ctx, "one", store)
	if err != nil {
		t.Fatalf("reopen with rotation: %v", err)
	}
	if err := reopenedWithRotation.Validate("one"); err != nil {
		t.Fatalf("old token should remain active after rotation restart: %v", err)
	}
	if hash, secret, err := reopenedWithRotation.CurrentSecret(); err != nil || hash != tokenHash("two") || secret != "two" {
		t.Fatalf("current rotated secret = hash:%s secret:%q err:%v", hash, secret, err)
	}
	if err := manager.Revoke("one"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	reopened, err := NewPersistentTokenManager(ctx, "one", store)
	if err != nil {
		t.Fatalf("reopen persistent manager: %v", err)
	}
	if err := reopened.Validate("one"); err == nil {
		t.Fatal("revoked token validated after reopen")
	}
	if err := reopened.Validate("two"); err != nil {
		t.Fatalf("rotated token did not persist: %v", err)
	}
}

func TestPersistentTokenManagerRevokesLegacyHashOnlyActiveTokens(t *testing.T) {
	ctx := context.Background()
	initialHash := tokenHash("one")
	legacyHash := tokenHash("two")
	store := &tokenStore{records: map[string]domain.JoinTokenRecord{
		initialHash: {Hash: initialHash, Active: true},
		legacyHash:  {Hash: legacyHash, Active: true, Current: true},
	}}
	manager, err := NewPersistentTokenManager(ctx, "one", store)
	if err != nil {
		t.Fatalf("NewPersistentTokenManager: %v", err)
	}
	if err := manager.Validate("one"); err != nil {
		t.Fatalf("initial token should be reconstructed: %v", err)
	}
	if err := manager.Validate("two"); err == nil {
		t.Fatal("legacy hash-only token validated after migration")
	}
	if hash, secret, err := manager.CurrentSecret(); err != nil || hash != initialHash || secret != "one" {
		t.Fatalf("current after migration = hash:%s secret:%q err:%v", hash, secret, err)
	}
	legacy := store.records[legacyHash]
	if legacy.Active || legacy.Current || legacy.MigrationNote == "" {
		t.Fatalf("legacy token migration record = %+v", legacy)
	}
}

func TestPersistentTokenManagerErrors(t *testing.T) {
	if _, err := NewPersistentTokenManager(context.Background(), "", &tokenStore{}); err == nil {
		t.Fatal("empty persistent initial token accepted")
	}
	if _, err := NewPersistentTokenManager(context.Background(), "one", &tokenStore{listErr: errors.New("list")}); err == nil {
		t.Fatal("expected list error")
	}
	if _, err := NewPersistentTokenManager(context.Background(), "one", &tokenStore{saveErr: errors.New("save")}); err == nil {
		t.Fatal("expected save error")
	}
	if _, err := NewPersistentTokenManager(context.Background(), "one", &tokenStore{records: map[string]domain.JoinTokenRecord{"bad": {}}}); err == nil || err.Error() != "persisted join token hash is required" {
		t.Fatalf("expected malformed record error, got %v", err)
	}
	if _, err := NewPersistentTokenManager(context.Background(), "one", &tokenStore{records: map[string]domain.JoinTokenRecord{"bad": {Hash: "bad", Secret: "two", Active: true}}}); err == nil || err.Error() != "persisted join token secret does not match hash" {
		t.Fatalf("expected mismatched secret error, got %v", err)
	}
	revoked := tokenHash("one")
	if _, err := NewPersistentTokenManager(context.Background(), "one", &tokenStore{records: map[string]domain.JoinTokenRecord{revoked: {Hash: revoked, Active: false}}}); err == nil || err.Error() != "no active join token is available" {
		t.Fatalf("expected no active token error, got %v", err)
	}
}

type tokenStore struct {
	records map[string]domain.JoinTokenRecord
	listErr error
	saveErr error
}

func (s *tokenStore) SaveJoinToken(_ context.Context, token domain.JoinTokenRecord) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.records == nil {
		s.records = map[string]domain.JoinTokenRecord{}
	}
	s.records[token.Hash] = token
	return nil
}

func (s *tokenStore) ListJoinTokens(context.Context) ([]domain.JoinTokenRecord, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]domain.JoinTokenRecord, 0, len(s.records))
	for _, record := range s.records {
		out = append(out, record)
	}
	return out, nil
}
