package store

import (
	"testing"
	"time"

	"quorum/internal/model"
)

func TestLeaseLive(t *testing.T) {
	now := time.Unix(1000, 0)
	live := model.Claim{DocID: "d", AgentID: "a", LeaseExpiry: now.Add(time.Second)}
	if !leaseLive(live, now) {
		t.Fatal("expected live lease")
	}
	expired := model.Claim{DocID: "d", AgentID: "a", LeaseExpiry: now.Add(-time.Second)}
	if leaseLive(expired, now) {
		t.Fatal("expected expired lease")
	}
	empty := model.Claim{DocID: "d", AgentID: "", LeaseExpiry: now.Add(time.Second)}
	if leaseLive(empty, now) {
		t.Fatal("empty AgentID must not be a live lease")
	}
}
