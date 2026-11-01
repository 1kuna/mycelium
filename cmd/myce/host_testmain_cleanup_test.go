package main

import (
	"mycelium/internal/testenv"
	"testing"
)

func TestPackageFixtureLifecycle(t *testing.T) { testenv.CheckFixtureLifecycle(t) }
