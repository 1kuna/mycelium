package membership

import (
	"fmt"
	"net/url"
)

type JoinInfo struct {
	Token string
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
	return JoinInfo{Token: token}, nil
}
