package membership

import "testing"

func TestJoinTokenRoundTrip(t *testing.T) {
	raw, err := BuildJoinToken("secret")
	if err != nil {
		t.Fatalf("BuildJoinToken: %v", err)
	}
	info, err := ParseJoinToken(raw)
	if err != nil {
		t.Fatalf("ParseJoinToken: %v", err)
	}
	if info.Token != "secret" {
		t.Fatalf("info = %+v", info)
	}
	raw, err = BuildJoinTokenForPeer("192.0.2.63:51846", "secret")
	if err != nil {
		t.Fatalf("BuildJoinTokenForPeer: %v", err)
	}
	info, err = ParseJoinToken(raw)
	if err != nil {
		t.Fatalf("ParseJoinToken peer: %v", err)
	}
	if info.Address != "192.0.2.63:51846" || info.Token != "secret" {
		t.Fatalf("peer info = %+v", info)
	}
	info, err = ParseJoinToken("mycjoin://192.0.2.63:51846?token=secret")
	if err != nil {
		t.Fatalf("ParseJoinToken seed: %v", err)
	}
	if info.Address != "192.0.2.63:51846" || info.Token != "secret" {
		t.Fatalf("seed info = %+v", info)
	}
}

func TestJoinTokenErrors(t *testing.T) {
	if _, err := BuildJoinToken(""); err == nil {
		t.Fatal("missing token accepted")
	}
	if _, err := BuildJoinTokenWithRPC("", "rpc"); err == nil {
		t.Fatal("missing token with rpc accepted")
	}
	if _, err := BuildJoinTokenWithRPC("secret", ""); err == nil {
		t.Fatal("rpc helper accepted empty rpc token")
	}
	if _, err := BuildJoinTokenWithRPC("secret", "rpc"); err == nil {
		t.Fatal("rpc helper accepted rpc token")
	}
	if _, err := ParseJoinToken("http://127.0.0.1:1?token=secret"); err == nil {
		t.Fatal("bad scheme accepted")
	}
	if _, err := ParseJoinToken("mycjoin://peer"); err == nil {
		t.Fatal("missing token accepted")
	}
	if _, err := ParseJoinToken("mycjoin://peer?token=secret&rpc_token=rpc"); err == nil {
		t.Fatal("rpc token in join uri accepted")
	}
}
