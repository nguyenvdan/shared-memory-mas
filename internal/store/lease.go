package store

import (
	"time"

	"quorum/internal/model"
)

// leaseLive reports whether the claim represents a currently-held lease at the
// given time: a non-empty holder and an expiry still in the future.
func leaseLive(c model.Claim, now time.Time) bool {
	return c.AgentID != "" && now.Before(c.LeaseExpiry)
}
