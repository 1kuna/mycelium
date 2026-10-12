#!/usr/bin/env python3
"""Collector-only gate: no queue, worker, workload, counter subprocess or GPU allocation."""
import json
import os
from pathlib import Path
import time

from kernel_reads import kernel_read
from pressure import DiagnosticReaders, drm_owners
from stage_ab import ROOT, PCI, QUEUE, atomic


def soak(root=ROOT, duration=1800, interval=.5):
    baseline=json.loads(kernel_read(root/'display-baseline.json'))
    readers=DiagnosticReaders(json.loads(kernel_read(root/'diagnostic-executables.json')),root/'soak-diagnostics.jsonl')
    began=time.monotonic();samples=0;peak=0;error=None
    try:
        while time.monotonic()-began<duration:
            at=time.monotonic();evidence={}
            assert drm_owners([PCI],baseline=baseline,allowed=[],diagnostics=readers,evidence=evidence)=={PCI:[]}
            samples+=1
            peak=max(peak,time.monotonic()-at)
            with (root/'collector-soak-samples.jsonl').open('a') as stream:
                stream.write(json.dumps(dict(at=at,holders=len(evidence['holders']),baseline_clients=evidence['baseline_clients']))+'\n')
            time.sleep(max(0,interval-(time.monotonic()-at)))
        assert not readers.pending,'diagnostic release pending at soak end'
    except Exception as exc:
        error=dict(type=type(exc).__name__,detail=str(exc))
    result=dict(result='PASS' if error is None else 'HOLD',seconds=time.monotonic()-began,
                required_seconds=duration,interval_seconds=interval,samples=samples,
                peak_scan_seconds=peak,holds=int(error is not None),failures=int(error is not None),error=error)
    atomic(root/'collector-soak.json',result)
    atomic(root/('COLLECTOR-DONE' if error is None else 'COLLECTOR-FAILED'),result)
    return result


if __name__=='__main__':
    # Fence legacy admissions throughout the soak; finalizer releases only these bytes.
    pause=(json.dumps(dict(owner='myce-test-stage-ab-r11b',nonce=os.urandom(16).hex()))+'\n').encode()
    with (QUEUE/'PAUSED').open('xb') as stream:
        stream.write(pause);stream.flush();os.fsync(stream.fileno())
    (ROOT/'owned-pause').write_bytes(pause)
    result=soak()
    if result['result']!='PASS':raise SystemExit(1)
    # Start scenarios only after the complete zero-hold soak receipt is durable.
    from stage_ab import Run
    Run().run()
