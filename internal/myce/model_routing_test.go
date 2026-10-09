package myce

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIRoutingKeepsBufferedEngineCompatibility(t *testing.T) {
	q, _, m := modelFixture(t)
	runtime := &modelRuntime{models: map[string]*workerModel{"test": {state: "ready", engine: &FakeEngine{Model: "test"}}}, session: m.Session, token: "test"}
	worker := httptest.NewServer(http.HandlerFunc(runtime.serve))
	defer worker.Close()
	m.ModelURL = worker.URL
	check(t, q, m, nil)
	q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: "ready", Priority: "production", LastUsed: q.clock.Now()}
	api := API{Queue: q, Token: "test"}.Handler()
	for _, path := range []string{"/v1/chat/completions", "/v1/embeddings", "/v1/completions"} {
		req := httptest.NewRequest("POST", path, strings.NewReader(`{"model":"test","input":"hello"}`))
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("X-Myce-Project", "atlas")
		req.Header.Set("X-Myce-Priority", "production")
		out := httptest.NewRecorder()
		api.ServeHTTP(out, req)
		data, _ := io.ReadAll(out.Result().Body)
		if out.Code != 200 || strings.Contains(string(data), "streaming request bodies") {
			t.Fatal(path, out.Code, string(data))
		}
	}
}
