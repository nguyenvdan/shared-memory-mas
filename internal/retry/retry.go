package retry

import "time"

// Policy is a bounded retry policy with deterministic exponential backoff.
type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
}

// Default is the standard policy: 5 attempts, 1ms base delay.
func Default() Policy { return Policy{MaxAttempts: 5, BaseDelay: time.Millisecond} }

// Backoff returns the delay before the given 0-based attempt, exponential and
// capped at 32×BaseDelay. Deterministic (no jitter) so tests are reproducible.
func (p Policy) Backoff(attempt int) time.Duration {
	mult := 1 << attempt
	if mult > 32 {
		mult = 32
	}
	return time.Duration(mult) * p.BaseDelay
}

// Do invokes fn up to MaxAttempts times. fn returns (retryable, err): a nil err
// stops immediately (success); a non-nil err with retryable==true is retried
// while attempts remain; retryable==false stops immediately. Returns the number
// of attempts made and the last error. Do does not sleep — callers that want to
// pace retries use Backoff(attempt) themselves.
func Do(p Policy, fn func() (retryable bool, err error)) (int, error) {
	var lastErr error
	for attempt := 0; attempt < p.MaxAttempts; attempt++ {
		retryable, err := fn()
		if err == nil {
			return attempt + 1, nil
		}
		lastErr = err
		if !retryable {
			return attempt + 1, err
		}
	}
	return p.MaxAttempts, lastErr
}
