package main

import (
	"time"
)

type bindClock struct{ now time.Time }

func (c *bindClock) Now() time.Time { return c.now }
