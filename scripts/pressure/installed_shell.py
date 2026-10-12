"""Run the actual installer in the fenced shell; no unrelated-unit join probe."""
import os
import sys


def install(args):
    assert len(args) >= 3 and args[1:3] == ['install', 'worker']
    instance = args[args.index('--instance') + 1]
    assert instance == 'versotest-r14-gpu1', 'installer instance outside disposable scope'
    os.execv(args[0], args)


if __name__ == '__main__':
    install(sys.argv[1:])
