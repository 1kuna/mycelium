package membership

import "testing"

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
}
