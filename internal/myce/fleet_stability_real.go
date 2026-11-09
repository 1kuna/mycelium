//go:build !myce_test

package myce

// Normal builds retain the caller's clock and wait without model state.
type fleetStability struct{}

func (*fleetStability) clock(source Clock, _ bool) Clock { return source }

func (*fleetStability) managerOptions(options EnrollmentWait) EnrollmentWait { return options }

func (*fleetStability) observeManager(bool) {}
