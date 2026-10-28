package myce

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type nativeSeqpacketEndpoint struct {
	Name, Inode string
}

// /proc/net/unix exposes the current network namespace without invoking a
// helper or traversing every process's descriptors. Preserve spaces in names.
func nativeSeqpacketEndpoints(data []byte) ([]nativeSeqpacketEndpoint, error) {
	var endpoints []nativeSeqpacketEndpoint
	header := false
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "Num") {
			header = true
			continue
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		var fields [7]string
		rest := line
		for i := range fields {
			rest = strings.TrimLeft(rest, " \t")
			end := strings.IndexAny(rest, " \t")
			if end < 0 {
				fields[i], rest = rest, ""
			} else {
				fields[i], rest = rest[:end], rest[end:]
			}
			if fields[i] == "" {
				return nil, fmt.Errorf("incomplete Unix socket inventory")
			}
		}
		flags, e1 := strconv.ParseUint(fields[3], 16, 32)
		kind, e2 := strconv.ParseUint(fields[4], 16, 16)
		if e1 != nil || e2 != nil {
			return nil, fmt.Errorf("invalid Unix socket inventory")
		}
		if kind == 5 /* SOCK_SEQPACKET */ && flags&0x10000 != 0 /* SO_ACCEPTCON */ {
			endpoints = append(endpoints, nativeSeqpacketEndpoint{Name: strings.TrimLeft(rest, " \t"), Inode: fields[6]})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if !header {
		return nil, fmt.Errorf("missing Unix socket inventory header")
	}
	return endpoints, nil
}

// This is only the observed CUDA naming convention, not authenticated process
// identity. Inventory warnings never grant authority or strengthen isolation.
func nativeCUDASeqpacketName(name string) bool {
	name, ok := strings.CutPrefix(name, "@cuda-uvmfd-")
	if !ok {
		return false
	}
	name = strings.TrimSuffix(name, "@") // procfs renders the trailing NUL as @.
	parts := strings.Split(name, "-")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, ch := range part {
			if ch < '0' || ch > '9' {
				return false
			}
		}
	}
	return true
}

func nativeUnexpectedSeqpacket(endpoints []nativeSeqpacketEndpoint, owner func(string) (uint32, bool)) []nativeSeqpacketEndpoint {
	var unexpected []nativeSeqpacketEndpoint
	for _, endpoint := range endpoints {
		if nativeCUDASeqpacketName(endpoint.Name) {
			continue
		}
		uid, known := owner(endpoint.Name)
		if known && uid == 0 {
			continue
		}
		// Unknown ownership, including non-CUDA abstract names, warns visibly.
		unexpected = append(unexpected, endpoint)
	}
	return unexpected
}
