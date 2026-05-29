package assert

import "testing"

func NoError(t testing.TB, op string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
}

func Error(t testing.TB, msg string, err error) {
	t.Helper()
	if err == nil {
		t.Fatal(msg)
	}
}

func True(t testing.TB, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}

func Equal[T comparable](t testing.TB, want, got T, msg string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: want %v, got %v", msg, want, got)
	}
}
