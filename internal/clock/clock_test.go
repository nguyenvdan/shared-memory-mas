package clock

import (
	"testing"
	"time"
)

func TestMockNowIsStableUntilAdvanced(t *testing.T) {
	start := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	m := NewMock(start)

	if !m.Now().Equal(start) {
		t.Fatalf("Now() = %v, want %v", m.Now(), start)
	}
	m.Advance(90 * time.Second)
	want := start.Add(90 * time.Second)
	if !m.Now().Equal(want) {
		t.Fatalf("after Advance, Now() = %v, want %v", m.Now(), want)
	}
}

func TestMockAdvanceIsRaceSafe(t *testing.T) {
	m := NewMock(time.Unix(0, 0))
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				m.Advance(time.Millisecond)
				_ = m.Now()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if m.Now().Sub(time.Unix(0, 0)) != 800*time.Millisecond {
		t.Fatalf("total advance = %v, want 800ms", m.Now().Sub(time.Unix(0, 0)))
	}
}
