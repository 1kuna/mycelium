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
	store   TokenStore
}

func NewTokenManager(initial string) (*TokenManager, error) {
	if initial == "" {
		return nil, fmt.Errorf("join token is required")
	}
	hash := tokenHash(initial)
	return &TokenManager{
		current: hash,
		active:  map[string]struct{}{hash: {}},
		revoked: map[string]struct{}{},
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
		store:   store,
	}
	for _, record := range records {
		if record.Hash == "" {
			return nil, fmt.Errorf("persisted join token hash is required")
		}
		if record.Active {
			manager.active[record.Hash] = struct{}{}
		} else {
			manager.revoked[record.Hash] = struct{}{}
		}
		if record.Current {
			manager.current = record.Hash
		}
	}
	hash := tokenHash(initial)
	if _, revoked := manager.revoked[hash]; !revoked {
		if _, active := manager.active[hash]; !active {
			manager.active[hash] = struct{}{}
		}
		if manager.current == "" {
			manager.current = hash
		}
		if err := manager.save(ctx, hash, true, manager.current == hash); err != nil {
			return nil, err
		}
	}
	if len(manager.active) == 0 {
		return nil, fmt.Errorf("no active join token is available")
	}
	return manager, nil
}

func (m *TokenManager) Validate(token string) error {
	if token == "" {
		return fmt.Errorf("join token is required")
	}
	hash := tokenHash(token)
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
	return m.store.SaveJoinToken(ctx, domain.JoinTokenRecord{Hash: hash, Active: active, Current: current})
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
