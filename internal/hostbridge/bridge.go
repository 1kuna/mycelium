// Package hostbridge exposes aggregate Windows owner signals over IPv4 loopback.
// A separate command keeps the Windows build independent of the Unix worker.
package hostbridge

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Signals struct {
	IdleSeconds float64 `json:"idle_seconds"`
	IdleKnown   bool    `json:"idle_known"`
	Laptop      bool    `json:"laptop"`
	Power       string  `json:"power"`
	Heat        string  `json:"heat"`
}

func Unknown() Signals { return Signals{Power: "unknown", Heat: "unknown"} }

type Config struct {
	Address string `json:"address"`
	Key     string `json:"key"`
}

func (c Config) validate() ([]byte, error) {
	host, port, err := net.SplitHostPort(c.Address)
	n, e := strconv.Atoi(port)
	if err != nil || host != "127.0.0.1" || e != nil || n < 1 || n > 65535 {
		return nil, errors.New("bridge requires an IPv4 loopback endpoint")
	}
	key, err := hex.DecodeString(c.Key)
	if err != nil || len(key) != 32 {
		return nil, errors.New("invalid bridge key")
	}
	return key, nil
}
func ReadConfig(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, errors.New("bridge config unavailable")
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 4097))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&c); err != nil {
		return Config{}, errors.New("invalid bridge config")
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return Config{}, errors.New("invalid bridge config trailer")
	}
	_, err = c.validate()
	return c, err
}
func mac(key []byte, direction, nonce string, body []byte) string {
	h := hmac.New(sha256.New, key)
	h.Write([]byte("myce-host-bridge-v1\n" + direction + "\n" + nonce + "\n"))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}
func validMAC(got, want string) bool {
	a, e := hex.DecodeString(got)
	b, _ := hex.DecodeString(want)
	return e == nil && hmac.Equal(a, b)
}

// Handler samples only after authentication. Nonces bind fresh, signed responses
// to a single request; the shared key never traverses the channel. No access log.
func Handler(c Config, sample func(context.Context) Signals) (http.Handler, error) {
	key, err := c.validate()
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, e := net.SplitHostPort(r.RemoteAddr)
		if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			http.Error(w, "loopback required", 403)
			return
		}
		if r.Method != "GET" || r.URL.RequestURI() != "/v1/signals" {
			http.NotFound(w, r)
			return
		}
		nonce := r.Header.Get("X-Myce-Nonce")
		b, e := hex.DecodeString(nonce)
		if e != nil || len(b) != 32 || !validMAC(r.Header.Get("X-Myce-MAC"), mac(key, "request", nonce, nil)) {
			http.Error(w, "unauthorized", 401)
			return
		}
		s := sample(r.Context())
		body, e := json.Marshal(s)
		if e != nil {
			http.Error(w, "signals unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Myce-MAC", mac(key, "response", nonce, body))
		w.Write(body)
	}), nil
}

// Fetch uses a fresh nonce, no proxies, no redirects, bounded body and deadline.
// Sampling is synchronous, so no cached host timestamp or cross-OS clock trust
// is needed. Requests older than the deadline never produce usable signals.
func Fetch(ctx context.Context, c Config) (Signals, error) {
	fail := func() (Signals, error) { return Unknown(), errors.New("host signals unavailable") }
	key, err := c.validate()
	if err != nil {
		return fail()
	}
	nonceBytes := make([]byte, 32)
	if _, err = rand.Read(nonceBytes); err != nil {
		return fail()
	}
	nonce := hex.EncodeToString(nonceBytes)
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", "http://"+c.Address+"/v1/signals", nil)
	if err != nil {
		return fail()
	}
	req.Header.Set("X-Myce-Nonce", nonce)
	req.Header.Set("X-Myce-MAC", mac(key, "request", nonce, nil))
	tr := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	client := http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return fail()
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(body) > 4096 || resp.StatusCode != 200 || ctx.Err() != nil || !validMAC(resp.Header.Get("X-Myce-MAC"), mac(key, "response", nonce, body)) {
		return fail()
	}
	var s Signals
	if json.Unmarshal(body, &s) != nil || math.IsNaN(s.IdleSeconds) || math.IsInf(s.IdleSeconds, 0) || s.IdleSeconds < 0 {
		return fail()
	}
	if s.Power != "ac" && s.Power != "battery" && s.Power != "unknown" {
		return fail()
	}
	if s.Heat != "nominal" && s.Heat != "fair" && s.Heat != "serious" && s.Heat != "critical" && s.Heat != "unknown" {
		return fail()
	}
	return s, nil
}
