package clock

import (
	"sync"
	"time"
)

// Clock is the only interface allowed to read the current time.
type Clock interface {
	Now() time.Time
}

// Real is the production clock.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

// Mock is a manually-advanced, concurrency-safe clock for tests.
type Mock struct {
	mu sync.Mutex
	t  time.Time
}

func NewMock(t time.Time) *Mock { return &Mock{t: t} }

func (m *Mock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.t
}

func (m *Mock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t = m.t.Add(d)
}
