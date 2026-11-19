#!/usr/bin/env python3
"""CPU-only Linux reproduction for #1692; needs cc and permitted io_uring.

Pins and locks 64 KiB in a sibling, exits the leader via SYS_exit, then checks
that its missing counters never imply the live sibling owns zero pins. Also
samples single-task exit with descriptor cleanup widening the R/no-mm window.
No cgroup writes, GPU, third-party modules or elevated privileges.
"""

import os
import subprocess
import time
import pathlib
import collections
import tempfile

SOURCE = r'''
#define _GNU_SOURCE
#include <pthread.h>
#include <linux/io_uring.h>
#include <sys/uio.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
static int ready, release_fd;
static void pin(void *p) {
    struct io_uring_params params = {0};
    int ring = syscall(SYS_io_uring_setup, 2, &params);
    struct iovec iov = {p, 65536};
    if (ring < 0 || syscall(SYS_io_uring_register, ring, IORING_REGISTER_BUFFERS, &iov, 1)) {
        perror("register pinned buffer"); _exit(9);
    }
}
static void *sibling(void *unused) {
    (void)unused;
    void *p = mmap(NULL, 65536, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED || mlock(p, 65536)) _exit(2);
    pin(p);
    pthread_mutex_lock(&mu);
    printf("%ld\n", syscall(SYS_gettid)); fflush(stdout);
    ready = 1; pthread_cond_signal(&cv); pthread_mutex_unlock(&mu);
    /* The parent releases this thread only after inspecting the exited leader. */
    char b;
    if (read(release_fd, &b, 1) != 1) _exit(3);
    return NULL;
}
int main(int argc, char **argv) {
    release_fd = atoi(argv[2]);
    if (argc > 1 && argv[1][0] == 's') {
        pthread_t t;
        pthread_mutex_lock(&mu);
        if (pthread_create(&t, NULL, sibling, NULL)) return 4;
        while (!ready) pthread_cond_wait(&cv, &mu);
        pthread_mutex_unlock(&mu);
    } else {
        void *p = mmap(NULL, 65536, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        if (p == MAP_FAILED || mlock(p, 65536)) return 5;
        pin(p);
        /* Slow exit_files after exit_mm, making the R/no-mm window observable. */
        for (int i = 0; i < 256; i++) { int fds[2]; if (pipe(fds)) return 6; }
        puts("0"); fflush(stdout);
    }
    char b;
    if (read(0, &b, 1) != 1) return 7;
    syscall(SYS_exit, 0); /* Exit this task, preserving any sibling's mm. */
    return 8;
}
'''

with tempfile.TemporaryDirectory(prefix='myce-proc-exit-') as tmp:
    root = pathlib.Path(tmp)
    (root / 'repro.c').write_text(SOURCE)
    subprocess.run(['cc', '-O2', '-pthread', str(root / 'repro.c'), '-o', str(root / 'repro')], check=True)
    counts = collections.Counter()
    for mode in ('sibling', 'alone'):
        for trial in range(100):
            r, w = os.pipe()
            p = subprocess.Popen([str(root / 'repro'), mode, str(r)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 pass_fds=(r,))
            os.close(r)
            try:
                tid = int(p.stdout.readline())
                status = pathlib.Path(f'/proc/{p.pid}/status')
                initial = status.read_text()
                assert any(x.startswith('VmLck:') and int(x.split()[1]) == 64 for x in initial.splitlines())
                assert any(x.startswith('VmPin:') and int(x.split()[1]) >= 64 for x in initial.splitlines())
                p.stdin.write(b'x'); p.stdin.flush()
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    s = status.read_text()
                    state = next(x.split()[1] for x in s.splitlines() if x.startswith('State:'))
                    if 'VmPin:' not in s:
                        counts[mode + ' no-mm ' + state] += 1
                        if mode == 'sibling':
                            ts = pathlib.Path(f'/proc/{p.pid}/task/{tid}/status').read_text()
                            assert any(x.startswith('VmLck:') and int(x.split()[1]) == 64 for x in ts.splitlines())
                            assert any(x.startswith('VmPin:') and int(x.split()[1]) >= 64 for x in ts.splitlines())
                            task_ids = os.listdir(f'/proc/{p.pid}/task')
                            assert str(tid) in task_ids
                            cg = next(x.split(':', 2)[2] for x in pathlib.Path(f'/proc/{p.pid}/cgroup').read_text().splitlines() if x.startswith('0::'))
                            assert str(p.pid) in pathlib.Path('/sys/fs/cgroup' + cg + '/cgroup.procs').read_text().splitlines()
                            counts['sibling live-mm/cgroup-list verified'] += 1
                        if state == 'Z': break
                else: raise AssertionError('did not observe exit')
                if mode == 'sibling': os.write(w, b'x')
                os.close(w); w = -1
                assert p.wait(timeout=3) == 0
            finally:
                if w >= 0: os.close(w)
                if p.poll() is None:
                    p.kill(); p.wait()
    print(dict(counts))
    assert counts['alone no-mm R'] > 0
    assert counts['sibling live-mm/cgroup-list verified'] >= 100
