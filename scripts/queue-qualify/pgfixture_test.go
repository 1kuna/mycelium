package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// disposablePostgres starts a private cluster with its own data directory,
// socket directory and port, like the stage 2 fixture. It never touches an
// installed PostgreSQL service. Tests skip when no initdb is available.
const fixtureDB = "qualify"

type disposablePostgres struct {
	bin, data, sockets, log string
	port                    int
	t                       *testing.T
}

func pgBin(t *testing.T) string {
	if path, err := exec.LookPath("initdb"); err == nil {
		return filepath.Dir(path)
	}
	for _, pattern := range []string{"/opt/homebrew/opt/postgresql@*/bin", "/usr/local/opt/postgresql@*/bin", "/usr/lib/postgresql/*/bin"} {
		matches, _ := filepath.Glob(pattern)
		for i := len(matches) - 1; i >= 0; i-- {
			if _, err := os.Stat(filepath.Join(matches[i], "initdb")); err == nil {
				return matches[i]
			}
		}
	}
	t.Skip("PostgreSQL initdb is not installed")
	return ""
}

func startPostgres(t *testing.T) *disposablePostgres {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("initdb refuses to run as root")
	}
	bin := pgBin(t)
	// Unix socket paths are short on macOS; keep the cluster under /tmp.
	root, err := os.MkdirTemp("/tmp", "qq-pg-")
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	pg := &disposablePostgres{bin: bin, data: filepath.Join(root, "data"), sockets: root, log: filepath.Join(root, "log"), port: port, t: t}
	t.Cleanup(func() {
		pg.ctl("stop", "-m", "fast")
		os.RemoveAll(root)
	})
	out, err := exec.Command(filepath.Join(bin, "initdb"), "-D", pg.data, "-U", "qualify", "--auth=trust",
		"--data-checksums", "-E", "UTF8", "--locale-provider=builtin", "--builtin-locale=C.UTF-8").CombinedOutput()
	if err != nil {
		t.Fatalf("initdb: %v\n%s", err, out)
	}
	if err := pg.ctl("start"); err != nil {
		t.Fatal(err)
	}
	// The harness runs only in a dedicated qualification database, as in the
	// plan's compose sketch, whose default database is named for its role.
	out, err = exec.Command(filepath.Join(bin, "createdb"), "-h", root, "-p", fmt.Sprint(port), "-U", "qualify", fixtureDB).CombinedOutput()
	if err != nil {
		t.Fatalf("createdb: %v\n%s", err, out)
	}
	return pg
}

func (pg *disposablePostgres) ctl(args ...string) error {
	full := append([]string{"-D", pg.data, "-l", pg.log, "-w", "-o",
		fmt.Sprintf("-p %d -k %s -c listen_addresses=127.0.0.1 -c fsync=on", pg.port, pg.sockets)}, args...)
	out, err := exec.Command(filepath.Join(pg.bin, "pg_ctl"), full...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_ctl %v: %v\n%s", args, err, out)
	}
	return nil
}

func (pg *disposablePostgres) dsn() string {
	return fmt.Sprintf("postgres://qualify@127.0.0.1:%d/%s?sslmode=disable", pg.port, fixtureDB)
}

// target returns a connection target read from an environment variable, the
// same path the command uses.
func (pg *disposablePostgres) target(t *testing.T) *target {
	t.Setenv("QQ_TEST_DSN", pg.dsn())
	tg, err := (&connFlags{env: "QQ_TEST_DSN"}).load()
	if err != nil {
		t.Fatal(err)
	}
	return tg
}
