#!/usr/bin/env python3
"""Compatibility entry point: ordinary admission already reclaims dead holders."""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('gate_capacity', Path(__file__).with_name('gate-capacity.py'))
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)


def reconcile(folders, repo):
    control = capacity.root(repo)
    with capacity.admission_lock(control):
        capacity.reclaim_dead(control)
    capacity._cache.trim_backups(control / 'selfheal-backups')
    return dict(status='automatic', requested=[str(p) for p in folders])
