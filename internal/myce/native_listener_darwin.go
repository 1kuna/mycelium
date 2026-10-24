//go:build darwin && cgo

package myce

/*
#include <libproc.h>
#include <sys/proc_info.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <errno.h>

// Inspect only socket descriptors of an owned process. Never request vnode
// information (including cwd/text), which can block on a stalled mount.
// Return 0 for no listener, 1 for loopback, 2 for non-loopback, or -errno.
static int myce_owned_listener(int pid, int port) {
    errno = 0;
    int size = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, NULL, 0);
    if (size <= 0) return errno == ESRCH ? 0 : -errno;
    struct proc_fdinfo *fds;
    int bytes;
    for (;;) {
        fds = malloc(size);
        if (!fds) return -ENOMEM;
        errno = 0;
        bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, fds, size);
        if (bytes <= 0) {
            int error = errno;
            free(fds);
            return error == ESRCH ? 0 : -error;
        }
        if (bytes < size) break;
        free(fds);
        size *= 2; // Retry a full buffer so newly opened descriptors aren't lost.
    }
    int found = 0;
    for (int i = 0; i < bytes / sizeof(*fds); i++) {
        if (fds[i].proc_fdtype != PROX_FDTYPE_SOCKET) continue;
        struct socket_fdinfo info;
        if (proc_pidfdinfo(pid, fds[i].proc_fd, PROC_PIDFDSOCKETINFO,
                           &info, sizeof(info)) != sizeof(info)) continue;
        struct socket_info *socket = &info.psi;
        if (socket->soi_kind != SOCKINFO_TCP) continue;
        struct tcp_sockinfo *tcp = &socket->soi_proto.pri_tcp;
        if (tcp->tcpsi_state != TSI_S_LISTEN ||
            ntohs((uint16_t)tcp->tcpsi_ini.insi_lport) != port) continue;
        if (socket->soi_family != AF_INET ||
            ntohl(tcp->tcpsi_ini.insi_laddr.ina_46.i46a_addr4.s_addr) != INADDR_LOOPBACK) {
            free(fds);
            return 2;
        }
        found = 1;
    }
    free(fds);
    return found;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
	"time"
)

func verifyDarwinNativeListener(port string, owned map[int]bool) error {
	number, err := strconv.Atoi(port)
	if err != nil {
		return err
	}
	found := false
	for pid := range owned {
		switch result := int(C.myce_owned_listener(C.int(pid), C.int(number))); result {
		case 0:
		case 1:
			found = true
		case 2:
			return errors.New("native sidecar must listen on 127.0.0.1")
		default:
			return fmt.Errorf("native listener ownership: %w", syscall.Errno(-result))
		}
	}
	if !found {
		// Distinguish a foreign listener from a helper that exited before
		// listening, so an application failure isn't retried as a port collision.
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return errNativeForeignListener
		}
		if errors.Is(err, syscall.ECONNREFUSED) {
			return errors.New("native readiness listener ownership unconfirmed")
		}
		return fmt.Errorf("native listener ownership: %w", err)
	}
	return nil
}
