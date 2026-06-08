package membership

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"sync"

	"mycelium/internal/domain"
)

type TokenStore interface {
	SaveJoinToken(ctx context.Context, token domain.JoinTokenRecord) error
	ListJoinTokens(ctx context.Context) ([]domain.JoinTokenRecord, error)
}

type TokenManager struct {
	mu      sync.Mutex
	current string
	active  map[string]struct{}
	revoked map[string]struct{}
	secrets map[string]string
	store   TokenStore
}

const hashOnlyTokenMigrationNote = "revoked hash-only active join token during startup because its secret was not persisted; rotate to issue a new join secret"

func NewTokenManager(initial string) (*TokenManager, error) {
	if initial == "" {
		return nil, fmt.Errorf("join token is required")
	}
	hash := tokenHash(initial)
	return &TokenManager{
		current: hash,
		active:  map[string]struct{}{hash: {}},
		revoked: map[string]struct{}{},
		secrets: map[string]string{hash: initial},
	}, nil
}

func NewPersistentTokenManager(ctx context.Context, initial string, store TokenStore) (*TokenManager, error) {
	if store == nil {
		return NewTokenManager(initial)
	}
	if initial == "" {
		return nil, fmt.Errorf("join token is required")
	}
	records, err := store.ListJoinTokens(ctx)
	if err != nil {
		return nil, err
	}
	manager := &TokenManager{
		active:  map[string]struct{}{},
		revoked: map[string]struct{}{},
		secrets: map[string]string{},
		store:   store,
	}
	initialHash := tokenHash(initial)
	for _, record := range records {
		if record.Hash == "" {
			return nil, fmt.Errorf("persisted join token hash is required")
		}
		if record.Active && record.Secret == "" {
			if record.Hash == initialHash {
				record.Secret = initial
				if err := store.SaveJoinToken(ctx, record); err != nil {
					return nil, err
				}
			} else {
				record.Active = false
				record.Current = false
				record.MigrationNote = hashOnlyTokenMigrationNote
				if err := store.SaveJoinToken(ctx, record); err != nil {
					return nil, err
				}
			}
		}
		if record.Active && tokenHash(record.Secret) != record.Hash {
			return nil, fmt.Errorf("persisted join token secret does not match hash")
		}
		if record.Active {
			manager.active[record.Hash] = struct{}{}
			manager.secrets[record.Hash] = record.Secret
		} else {
			manager.revoked[record.Hash] = struct{}{}
		}
		if record.Current && record.Active {
			manager.current = record.Hash
		}
	}
	hash := initialHash
	manager.secrets[hash] = initial
	if _, revoked := manager.revoked[hash]; !revoked {
		if _, active := manager.active[hash]; !active {
			manager.active[hash] = struct{}{}
			manager.current = hash
			if err := manager.save(ctx, hash, true, true); err != nil {
				return nil, err
			}
		} else if manager.current == "" {
			manager.current = hash
			if err := manager.save(ctx, hash, true, true); err != nil {
				return nil, err
			}
		}
	}
	if len(manager.active) == 0 {
		return nil, fmt.Errorf("no active join token is available")
	}
	return manager, nil
}

func (m *TokenManager) CurrentSecret() (string, string, error) {
	hash, err := m.CurrentHash()
	if err != nil {
		return "", "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	secret := m.secrets[hash]
	if secret == "" {
		return "", "", fmt.Errorf("current join token secret is unavailable")
	}
	return hash, secret, nil
}

func (m *TokenManager) SecretForHash(hash string) (string, bool, error) {
	if err := m.ValidateHash(hash); err != nil {
		return "", false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	secret := m.secrets[hash]
	return secret, secret != "", nil
}

func (m *TokenManager) Validate(token string) error {
	if token == "" {
		return fmt.Errorf("join token is required")
	}
	return m.ValidateHash(tokenHash(token))
}

func (m *TokenManager) ValidateHash(hash string) error {
	if hash == "" {
		return fmt.Errorf("join token hash is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, revoked := m.revoked[hash]; revoked {
		return fmt.Errorf("join token has been revoked")
	}
	for active := range m.active {
		if subtle.ConstantTimeCompare([]byte(active), []byte(hash)) == 1 {
			return nil
		}
	}
	return fmt.Errorf("join token is invalid")
}

func (m *TokenManager) CurrentHash() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == "" {
		return "", fmt.Errorf("no current join token is available")
	}
	if _, revoked := m.revoked[m.current]; revoked {
		return "", fmt.Errorf("current join token has been revoked")
	}
	if _, active := m.active[m.current]; !active {
		return "", fmt.Errorf("current join token is not active")
	}
	return m.current, nil
}

func (m *TokenManager) Rotate(next string) error {
	if next == "" {
		return fmt.Errorf("join token is required")
	}
	hash := tokenHash(next)
	m.mu.Lock()
	defer m.mu.Unlock()
	oldCurrent := m.current
	m.current = hash
	m.active[hash] = struct{}{}
	delete(m.revoked, hash)
	m.secrets[hash] = next
	if m.store != nil {
		if oldCurrent != "" && oldCurrent != hash {
			if err := m.save(context.Background(), oldCurrent, true, false); err != nil {
				return err
			}
		}
		if err := m.save(context.Background(), hash, true, true); err != nil {
			return err
		}
	}
	return nil
}

func (m *TokenManager) Revoke(token string) error {
	if token == "" {
		return fmt.Errorf("join token is required")
	}
	hash := tokenHash(token)
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.active, hash)
	m.revoked[hash] = struct{}{}
	delete(m.secrets, hash)
	if m.current == hash {
		m.current = ""
	}
	if m.store != nil {
		if err := m.save(context.Background(), hash, false, false); err != nil {
			return err
		}
	}
	return nil
}

func (m *TokenManager) save(ctx context.Context, hash string, active, current bool) error {
	if m.store == nil {
		return nil
	}
	secret := ""
	if active {
		secret = m.secrets[hash]
	}
	return m.store.SaveJoinToken(ctx, domain.JoinTokenRecord{Hash: hash, Secret: secret, Active: active, Current: current})
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
