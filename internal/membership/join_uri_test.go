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
}

func TestJoinTokenErrors(t *testing.T) {
	if _, err := BuildJoinToken(""); err == nil {
		t.Fatal("missing token accepted")
	}
	if _, err := ParseJoinToken("http://127.0.0.1:1?token=secret"); err == nil {
		t.Fatal("bad scheme accepted")
	}
	if _, err := ParseJoinToken("mycjoin://peer"); err == nil {
		t.Fatal("missing token accepted")
	}
}
