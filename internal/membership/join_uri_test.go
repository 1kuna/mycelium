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
	raw, err = BuildJoinTokenWithRPC("secret", "rpc-secret")
	if err != nil {
		t.Fatalf("BuildJoinTokenWithRPC: %v", err)
	}
	info, err = ParseJoinToken(raw)
	if err != nil {
		t.Fatalf("ParseJoinToken rpc: %v", err)
	}
	if info.Token != "secret" || info.RPCToken != "rpc-secret" {
		t.Fatalf("rpc info = %+v", info)
	}
	info, err = ParseJoinToken("mycjoin://192.0.2.63:51846?token=secret&rpc_token=rpc-secret")
	if err != nil {
		t.Fatalf("ParseJoinToken seed: %v", err)
	}
	if info.Address != "192.0.2.63:51846" || info.Token != "secret" || info.RPCToken != "rpc-secret" {
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
		t.Fatal("missing rpc token accepted")
	}
	if _, err := ParseJoinToken("http://127.0.0.1:1?token=secret"); err == nil {
		t.Fatal("bad scheme accepted")
	}
	if _, err := ParseJoinToken("mycjoin://peer"); err == nil {
		t.Fatal("missing token accepted")
	}
}
