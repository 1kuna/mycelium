package membership

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"mycelium/internal/domain"
)

type JoinInfo struct {
	ServerURL string
	Token     string
}

type JoinRequest struct {
	Token string      `json:"token"`
	Node  domain.Node `json:"node"`
}

type JoinResponse struct {
	Node domain.Node `json:"node"`
}

func BuildJoinToken(serverURL, token string) (string, error) {
	if serverURL == "" {
		return "", fmt.Errorf("server URL is required")
	}
	if token == "" {
		return "", fmt.Errorf("join token is required")
	}
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" {
		parsed, err = url.Parse("http://" + serverURL)
		if err != nil {
			return "", err
		}
	}
	out := url.URL{Scheme: "mycjoin", Host: parsed.Host}
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
	return JoinInfo{ServerURL: "http://" + parsed.Host, Token: token}, nil
}

func Announce(ctx context.Context, client *http.Client, joinRaw string, node domain.Node) (domain.Node, error) {
	info, err := ParseJoinToken(joinRaw)
	if err != nil {
		return domain.Node{}, err
	}
	if client == nil {
		client = http.DefaultClient
	}
	body, err := json.Marshal(JoinRequest{Token: info.Token, Node: node})
	if err != nil {
		return domain.Node{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(info.ServerURL, "/")+"/join", bytes.NewReader(body))
	if err != nil {
		return domain.Node{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return domain.Node{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var wire struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&wire); err == nil && wire.Error != "" {
			return domain.Node{}, fmt.Errorf("join failed: %s", wire.Error)
		}
		return domain.Node{}, fmt.Errorf("join failed: %s", resp.Status)
	}
	var out JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return domain.Node{}, err
	}
	return out.Node, nil
}

func AdvertiseAddr(listenAddr, serverURL string) (string, error) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", err
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return listenAddr, nil
	}
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return "", err
	}
	conn, err := net.Dial("udp", parsed.Host)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	local, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return "", fmt.Errorf("could not infer LAN address")
	}
	return net.JoinHostPort(local.IP.String(), port), nil
}
