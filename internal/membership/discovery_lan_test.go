package membership

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJoinTokenRoundTrip(t *testing.T) {
	raw, err := BuildJoinToken("http://127.0.0.1:51846", "secret")
	if err != nil {
		t.Fatalf("BuildJoinToken: %v", err)
	}
	info, err := ParseJoinToken(raw)
	if err != nil {
		t.Fatalf("ParseJoinToken: %v", err)
	}
	if info.ServerURL != "http://127.0.0.1:51846" || info.Token != "secret" {
		t.Fatalf("info = %+v", info)
	}
}

func TestJoinTokenErrors(t *testing.T) {
	if _, err := BuildJoinToken("", "secret"); err == nil {
		t.Fatal("missing server URL accepted")
	}
	if _, err := BuildJoinToken("127.0.0.1:1", ""); err == nil {
		t.Fatal("missing token accepted")
	}
	if _, err := ParseJoinToken("http://127.0.0.1:1?token=secret"); err == nil {
		t.Fatal("wrong scheme accepted")
	}
	if _, err := ParseJoinToken("mycjoin://127.0.0.1:1"); err == nil {
		t.Fatal("missing query token accepted")
	}
}

func TestAnnouncePostsJoinRequest(t *testing.T) {
	manager, err := NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	registry := NewRegistry(manager, NewLANTunnel())
	server := httptest.NewServer(registry)
	defer server.Close()
	token, err := BuildJoinToken(server.URL, "secret")
	if err != nil {
		t.Fatalf("BuildJoinToken: %v", err)
	}
	node := readyJoinNode("node-a", "127.0.0.1:1")
	joined, err := Announce(context.Background(), server.Client(), token, node)
	if err != nil {
		t.Fatalf("Announce: %v", err)
	}
	if joined.ID != node.ID || joined.Status == "" {
		t.Fatalf("joined = %+v", joined)
	}
}

func TestAnnounceReturnsServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"bad token"}`, http.StatusForbidden)
	}))
	defer server.Close()
	token, err := BuildJoinToken(server.URL, "secret")
	if err != nil {
		t.Fatalf("BuildJoinToken: %v", err)
	}
	_, err = Announce(context.Background(), server.Client(), token, readyJoinNode("node-a", "127.0.0.1:1"))
	if err == nil || !strings.Contains(err.Error(), "join failed") {
		t.Fatalf("err = %v", err)
	}
}

func TestAdvertiseAddrInfersLANAddressForWildcardListen(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	got, err := AdvertiseAddr("0.0.0.0:51847", "http://"+listener.Addr().String())
	if err != nil {
		t.Fatalf("AdvertiseAddr: %v", err)
	}
	if got == "0.0.0.0:51847" || got == "" {
		t.Fatalf("advertise = %s", got)
	}
}
