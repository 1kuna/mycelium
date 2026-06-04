package membership

import (
	"fmt"
	"net/url"
)

type JoinInfo struct {
	Token   string
	Address string
}

func BuildJoinToken(token string) (string, error) {
	if token == "" {
		return "", fmt.Errorf("join token is required")
	}
	out := url.URL{Scheme: "mycjoin", Host: "peer"}
	q := out.Query()
	q.Set("token", token)
	out.RawQuery = q.Encode()
	return out.String(), nil
}

func BuildJoinTokenWithRPC(token, rpcToken string) (string, error) {
	return "", fmt.Errorf("join URIs must not contain rpc tokens; pass the RPC token through config or --rpc-token")
}

func BuildJoinTokenForPeer(address, token string) (string, error) {
	if token == "" {
		return "", fmt.Errorf("join token is required")
	}
	if address == "" {
		address = "peer"
	}
	out := url.URL{Scheme: "mycjoin", Host: address}
	q := out.Query()
	q.Set("token", token)
	out.RawQuery = q.Encode()
	return out.String(), nil
}

func ParseJoinToken(raw string) (JoinInfo, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return JoinInfo{}, err
	}
	if parsed.Scheme != "mycjoin" {
		return JoinInfo{}, fmt.Errorf("join token must use mycjoin://")
	}
	token := parsed.Query().Get("token")
	if token == "" {
		return JoinInfo{}, fmt.Errorf("join token is missing token query")
	}
	if parsed.Query().Get("rpc_token") != "" {
		return JoinInfo{}, fmt.Errorf("join URI must not include rpc_token; pass the RPC token through config or --rpc-token")
	}
	address := parsed.Host
	if address == "peer" {
		address = ""
	}
	return JoinInfo{Token: token, Address: address}, nil
}
