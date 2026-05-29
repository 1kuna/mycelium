package membership

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"sync"
)

type TokenManager struct {
	mu      sync.Mutex
	current string
	active  map[string]struct{}
	revoked map[string]struct{}
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
	m.current = hash
	m.active[hash] = struct{}{}
	delete(m.revoked, hash)
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
	return nil
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
