package membership

import (
	"fmt"
	"net/url"
)

type JoinInfo struct {
	Token    string
	RPCToken string
	Address  string
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
	if token == "" {
		return "", fmt.Errorf("join token is required")
	}
	if rpcToken == "" {
		return "", fmt.Errorf("rpc token is required")
	}
	out := url.URL{Scheme: "mycjoin", Host: "peer"}
	q := out.Query()
	q.Set("token", token)
	q.Set("rpc_token", rpcToken)
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
	address := parsed.Host
	if address == "peer" {
		address = ""
	}
	return JoinInfo{Token: token, RPCToken: parsed.Query().Get("rpc_token"), Address: address}, nil
}
