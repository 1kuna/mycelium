package hostbridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestSessionCoverage(t *testing.T) {
	for _, tc := range []struct {
		name          string
		self, console uint32
		sessions      []Session
		known         bool
	}{
		{"interactive console", 1, 1, []Session{{1, 0}}, true},
		{"interop session zero", 0, 1, []Session{{1, 0}}, false},
		{"remote session", 2, 1, []Session{{2, 0}}, false},
		{"switch in progress", 1, 0xffffffff, []Session{{1, 0}}, false},
		{"other active session", 1, 1, []Session{{1, 0}, {2, 0}}, false},
		{"disconnected user", 1, 1, []Session{{1, 0}, {2, 4}}, false},
		{"missing console", 1, 1, nil, false},
		{"not active", 1, 1, []Session{{1, 4}}, false},
		{"listener and session zero", 1, 1, []Session{{0, 4}, {1, 0}, {65536, 6}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Covered(tc.self, tc.console, tc.sessions); got != tc.known {
				t.Fatal(got)
			}
		})
	}
}

func TestIdleAndPower(t *testing.T) {
	for _, tc := range []struct {
		now, last uint32
		seconds   float64
		known     bool
	}{
		{500000, 100000, 400, true}, {500, 0xfffffff0, .516, true}, {100, 101, 0, false},
	} {
		s, k := Idle(tc.now, tc.last)
		if s != tc.seconds || k != tc.known {
			t.Fatal(tc, s, k)
		}
	}
	for _, tc := range []struct {
		ac, battery, saver byte
		power              string
		laptop             bool
	}{
		{1, 128, 0, "ac", false}, {0, 1, 0, "battery", true}, {255, 128, 0, "unknown", false},
		{1, 255, 0, "unknown", true}, {1, 129, 0, "unknown", true}, {1, 1, 1, "battery", true}, {0, 128, 0, "battery", false},
	} {
		p, l := Power(tc.ac, tc.battery, tc.saver)
		if p != tc.power || l != tc.laptop {
			t.Fatal(tc, p, l)
		}
	}
}

func TestAuthenticatedFreshRoundTrip(t *testing.T) {
	c := Config{Address: "127.0.0.1:47353", Key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	calls := 0
	h, err := Handler(c, func(context.Context) Signals {
		calls++
		return Signals{IdleKnown: true, IdleSeconds: 400, Power: "ac", Heat: "unknown"}
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	c.Address = server.Listener.Addr().String()
	s, err := Fetch(context.Background(), c)
	if err != nil || !s.IdleKnown || s.IdleSeconds != 400 || s.Heat != "unknown" || calls != 1 {
		t.Fatal(s, err, calls)
	}
	bad := c
	bad.Key = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	if _, err := Fetch(context.Background(), bad); err == nil {
		t.Fatal("wrong key accepted")
	}
	if calls != 1 {
		t.Fatal("unauthenticated caller probed signals")
	}
	var savedHeader http.Header
	var savedBody []byte
	replay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if savedHeader == nil {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			savedHeader = rec.Header().Clone()
			savedBody = append([]byte(nil), rec.Body.Bytes()...)
		}
		for k, v := range savedHeader {
			w.Header()[k] = v
		}
		w.Write(savedBody)
	}))
	defer replay.Close()
	c.Address = replay.Listener.Addr().String()
	if _, err := Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if _, err := Fetch(context.Background(), c); err == nil {
		t.Fatal("replayed response accepted")
	}
	for _, address := range []string{"0.0.0.0:47353", "localhost:47353", "192.0.2.1:47353", "[::1]:47353", "127.0.0.1:0"} {
		c.Address = address
		if _, err := Fetch(context.Background(), c); err == nil {
			t.Fatal("unsafe endpoint", address)
		}
	}
}

func TestInstallUninstallOwnership(t *testing.T) {
	root := filepath.Join(t.TempDir(), "bridge")
	source := filepath.Join(t.TempDir(), "source.exe")
	if err := os.WriteFile(source, []byte("fake executable"), 0600); err != nil {
		t.Fatal(err)
	}
	registered := ""
	hooks := InstallHooks{
		Secure:       func(string) error { return nil },
		Registration: func() (string, error) { return registered, nil },
		Register:     func(s string) error { registered = s; return nil },
		Unregister:   func() error { registered = ""; return nil },
	}
	if err := Install(root, source, hooks); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(root, "bridge.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Install(root, source, hooks); err == nil {
		t.Fatal("overwrote existing installation")
	}
	again, _ := os.ReadFile(filepath.Join(root, "bridge.json"))
	if string(original) != string(again) {
		t.Fatal("rotated key on failed install")
	}
	c, err := ReadConfig(filepath.Join(root, "bridge.json"))
	if err != nil || c.Address != "127.0.0.1:47353" {
		t.Fatal(c.Address, err)
	}
	registered = "foreign command"
	if err := Uninstall(context.Background(), root, source, hooks); err == nil {
		t.Fatal("removed another startup command")
	}
	registered = StartupCommand(root)
	if err := Uninstall(context.Background(), root, source, hooks); err != nil {
		t.Fatal(err)
	}
	if registered != "" {
		t.Fatal("startup retained")
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("owned install retained", err)
	}
}

func TestContinuityDoesNotCountStartupSleepOrBadTicks(t *testing.T) {
	var c Continuity
	if s, k := c.Observe(600000, 0); !k || s != 0 {
		t.Fatal(s, k)
	}
	for now := uint64(605000); now <= 900000; now += 5000 {
		c.Observe(now, 0)
	}
	if s, k := c.Observe(900000, 0); !k || s != 300 {
		t.Fatal(s, k)
	}
	if s, k := c.Observe(900001, 900000); !k || s != 0 {
		t.Fatal("new input", s, k)
	}
	if s, k := c.Observe(4500000, 900000); !k || s != 0 {
		t.Fatal("sleep counted", s, k)
	}
	if s, k := c.Observe(4500100, 4500101); k || s != 0 {
		t.Fatal("future tick", s, k)
	}
	if s, k := c.Observe(4500200, 0); !k || s != 0 {
		t.Fatal("anomaly did not reset", s, k)
	}
	if s, k := c.Observe(4500100, 0); !k || s != 0 {
		t.Fatal("clock rollback", s, k)
	}
}

func TestInstallFailurePreservesForeignData(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "bridge")
	source := filepath.Join(parent, "source.exe")
	os.WriteFile(source, []byte("test"), 0600)
	hooks := InstallHooks{Secure: func(string) error { return os.ErrPermission }, Registration: func() (string, error) { return "", nil }}
	if err := Install(root, source, hooks); err == nil {
		t.Fatal("installed without secure ACL")
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatal("partial insecure directory retained", err)
	}
	os.Mkdir(root, 0700)
	os.WriteFile(filepath.Join(root, "keep"), []byte("unique"), 0600)
	if err := Install(root, source, hooks); err == nil {
		t.Fatal("overwrote directory")
	}
	if b, err := os.ReadFile(filepath.Join(root, "keep")); err != nil || string(b) != "unique" {
		t.Fatal("foreign data lost")
	}
}

func TestHTTPBoundary(t *testing.T) {
	c := Config{Address: "127.0.0.1:47353", Key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	calls := 0
	h, err := Handler(c, func(context.Context) Signals { calls++; return Unknown() })
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, peer string
		status             int
	}{
		{"GET", "/v1/signals", "192.0.2.2:5000", 403},
		{"POST", "/v1/signals", "127.0.0.1:5000", 404},
		{"GET", "/v1/signals?debug=1", "127.0.0.1:5000", 404},
		{"GET", "/v1/signals", "127.0.0.1:5000", 401},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.RemoteAddr = tc.peer
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatal(tc, w.Code)
		}
	}
	if calls != 0 {
		t.Fatal("boundary failure probed Windows")
	}
	// A loopback impersonator cannot forge a host response, even with valid JSON.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"idle_known":true,"idle_seconds":900,"power":"ac","heat":"nominal"}`))
	}))
	defer server.Close()
	c.Address = server.Listener.Addr().String()
	if s, err := Fetch(context.Background(), c); err == nil || s.IdleKnown || s.Heat != "unknown" {
		t.Fatal("unsigned host accepted", s, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Fetch(ctx, c); err == nil {
		t.Fatal("cancelled request accepted")
	}
}

func TestRejectMalformedSignedResponses(t *testing.T) {
	c := Config{Address: "127.0.0.1:47353", Key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	key, _ := c.validate()
	for _, body := range []string{
		`{"idle_known":true,"idle_seconds":400,"power":"ac","heat":"unknown","gpu_heat":"nominal","heat_coverage":"gpu-only"}`,
		`{"idle_known":true,"idle_seconds":400,"laptop":null,"power":"ac","heat":"unknown","gpu_heat":"nominal","heat_coverage":"gpu-only"}`,
		`{"idle_seconds":-1,"power":"ac","heat":"nominal"}`,
		`{"power":"desktop","heat":"nominal"}`,
		`{"power":"ac","heat":"cool"}`,
		`{"power":"ac","heat":"nominal"} trailing`,
		string(make([]byte, 4097)),
	} {
		t.Run(body[:min(20, len(body))], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Myce-MAC", mac(key, "response", r.Header.Get("X-Myce-Nonce"), []byte(body)))
				w.Write([]byte(body))
			}))
			defer server.Close()
			c.Address = server.Listener.Addr().String()
			if _, err := Fetch(context.Background(), c); err == nil {
				t.Fatal("invalid signed body accepted")
			}
		})
	}
}
