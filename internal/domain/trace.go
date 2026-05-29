package domain

type TraceStep struct {
	Step   string         `json:"step"`
	Result string         `json:"result,omitempty"`
	Data   map[string]any `json:"data,omitempty"`
}
