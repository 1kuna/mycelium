"""Observation pacing and transfer evidence, independent of wall-clock budgets."""
import email.utils
import random
import sys


def retry_after(value, now):
    try:
        if value.isdigit():
            return max(0, int(value))
        return max(0, email.utils.parsedate_to_datetime(value).timestamp() - now)
    except (AttributeError, ValueError, TypeError, OverflowError):
        return 0


class Pace:
    def __init__(self, floor=0, jitter=None):
        self.cost = self.delay = floor
        self.jitter = jitter or random.random

    def observe(self, elapsed, changed=False):
        elapsed = max(elapsed, sys.float_info.epsilon)
        self.cost = (self.cost + elapsed) / 2 if self.cost else elapsed
        self.delay = self.cost if changed or not self.delay else min(sys.float_info.max / 2, max(self.cost, self.delay)) * 2

    def next(self, now, event=None, error=None):
        delay = self.delay
        if error is not None:
            return max(delay, getattr(error, "retry_after", 0)) * (1 + self.jitter())
        if event is not None and event > now:
            delay = min(delay, max(self.cost, (event - now) / 2))
        return delay


class Progress:
    """The slowest observed byte cost and gap allow four periods of jitter.

    Before two wire observations only the owner's timeout can judge a stall.
    Total transfer duration never enters the decision.
    """
    def __init__(self):
        self.gap = self.per_byte = 0
        self.seen = False
        self.period = None
        self.count = 0

    def record(self, now, elapsed, count, waited):
        if count <= 0:
            return
        self.count += count
        if not waited:
            return
        if self.period is not None:
            elapsed = now - self.period
        self.observe(elapsed, self.count)
        self.period, self.count = now, 0

    def observe(self, elapsed, count):
        if count > 0:
            if not self.seen:
                self.seen = True
                return
            self.gap = max(self.gap, elapsed)
            self.per_byte = max(self.per_byte, elapsed / count)

    def window(self, count):
        return 4 * max(self.gap, self.per_byte * count) or None
