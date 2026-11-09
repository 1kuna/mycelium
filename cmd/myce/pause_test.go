package main

import (
	"time"
)

type pauseCLIClock struct{ now time.Time }

func (c pauseCLIClock) Now() time.Time { return c.now }
