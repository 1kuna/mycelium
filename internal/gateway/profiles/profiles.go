package profiles

import (
	"fmt"

	"mycelium/internal/domain"
)

type WireFormat string

const (
	FormatOpenAI    WireFormat = "openai"
	FormatAnthropic WireFormat = "anthropic"
)

type Profile struct {
	ID             string
	Format         WireFormat
	ChatPath       string
	CompletionPath string
	AnthropicPath  string
	SupportsChat   bool
	SupportsStream bool
	Backend        domain.Backend
}

type Registry struct {
	byBackend map[domain.Backend]Profile
	byID      map[string]Profile
}

func DefaultRegistry() Registry {
	return NewRegistry(
		Profile{
			ID:             "llamacpp",
			Format:         FormatOpenAI,
			ChatPath:       "/v1/chat/completions",
			CompletionPath: "/v1/completions",
			SupportsChat:   true,
			SupportsStream: true,
			Backend:        domain.BackendLlamaCpp,
		},
		Profile{
			ID:             "anthropic",
			Format:         FormatAnthropic,
			AnthropicPath:  "/v1/messages",
			SupportsChat:   true,
			SupportsStream: true,
			Backend:        domain.BackendCustom,
		},
	)
}

func NewRegistry(items ...Profile) Registry {
	r := Registry{byBackend: map[domain.Backend]Profile{}, byID: map[string]Profile{}}
	for _, item := range items {
		r.byID[item.ID] = item
		if item.Backend != "" {
			r.byBackend[item.Backend] = item
		}
	}
	return r
}

func (r Registry) ForBackend(backend domain.Backend) (Profile, error) {
	profile, ok := r.byBackend[backend]
	if !ok {
		return Profile{}, fmt.Errorf("unknown provider profile for backend %q", backend)
	}
	return profile, nil
}

func (r Registry) ByID(id string) (Profile, error) {
	profile, ok := r.byID[id]
	if !ok {
		return Profile{}, fmt.Errorf("unknown provider profile %q", id)
	}
	return profile, nil
}

func (r Registry) IsZero() bool {
	return r.byBackend == nil && r.byID == nil
}
