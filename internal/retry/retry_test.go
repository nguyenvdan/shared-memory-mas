package retry

import (
	"errors"
	"testing"
	"time"
)

func TestBackoffIsDeterministicExponentialCapped(t *testing.T) {
	p := Policy{MaxAttempts: 10, BaseDelay: time.Millisecond}
	if p.Backoff(0) != time.Millisecond {
		t.Fatalf("backoff(0) = %v", p.Backoff(0))
	}
	if p.Backoff(3) != 8*time.Millisecond {
		t.Fatalf("backoff(3) = %v", p.Backoff(3))
	}
	if p.Backoff(20) != 32*time.Millisecond { // capped
		t.Fatalf("backoff(20) = %v, want cap 32ms", p.Backoff(20))
	}
}

func TestDoStopsOnSuccess(t *testing.T) {
	calls := 0
	attempts, err := Do(Default(), func() (bool, error) {
		calls++
		return false, nil // success, not retryable
	})
	if err != nil || attempts != 1 || calls != 1 {
		t.Fatalf("attempts=%d calls=%d err=%v", attempts, calls, err)
	}
}

func TestDoRetriesUntilMaxThenReturnsLastError(t *testing.T) {
	sentinel := errors.New("conflict")
	calls := 0
	attempts, err := Do(Policy{MaxAttempts: 3, BaseDelay: time.Millisecond}, func() (bool, error) {
		calls++
		return true, sentinel // always retryable failure
	})
	if attempts != 3 || calls != 3 || !errors.Is(err, sentinel) {
		t.Fatalf("attempts=%d calls=%d err=%v", attempts, calls, err)
	}
}

func TestDoStopsRetryingWhenNotRetryable(t *testing.T) {
	fatal := errors.New("fatal")
	calls := 0
	attempts, err := Do(Default(), func() (bool, error) {
		calls++
		return false, fatal // failure, but not retryable
	})
	if attempts != 1 || calls != 1 || !errors.Is(err, fatal) {
		t.Fatalf("attempts=%d calls=%d err=%v", attempts, calls, err)
	}
}
