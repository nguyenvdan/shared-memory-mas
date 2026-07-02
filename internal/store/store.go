package store

import (
	"errors"
	"strings"
	"sync"
	"time"

	"quorum/internal/clock"
	"quorum/internal/model"
)

var (
	ErrVersionConflict = errors.New("version conflict")
	ErrLeaseHeld       = errors.New("lease held by another agent")
	ErrNotHolder       = errors.New("caller does not hold the lease")
)

// MemStore is the in-memory store: a derived version map plus the append-only
// log that is the source of truth. Single mutex now; leases arrive in Phase 2.
type MemStore struct {
	mu      sync.RWMutex
	entries map[string]model.Entry
	claims  map[string]model.Claim
	log     *Log
	clk     clock.Clock
}

func NewMemStore(clk clock.Clock) *MemStore {
	return &MemStore{
		entries: make(map[string]model.Entry),
		claims:  make(map[string]model.Claim),
		log:     NewLog(),
		clk:     clk,
	}
}

func (s *MemStore) Read(docID string) model.Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.entries[docID]; ok {
		return e
	}
	return model.Entry{DocID: docID, Version: 0, Exists: false}
}

// Write commits an annotation iff baseVersion matches the current version.
func (s *MemStore) Write(docID, agentID, payload string, baseVersion int) (model.Finding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.entries[docID] // zero Entry => Version 0 when absent
	if cur.Version != baseVersion {
		return model.Finding{}, ErrVersionConflict
	}
	next := cur.Version + 1
	f := s.log.Append(model.Finding{
		DocID:            docID,
		AgentID:          agentID,
		Payload:          payload,
		BaseVersion:      baseVersion,
		CommittedVersion: next,
		Timestamp:        s.clk.Now(),
	})
	s.entries[docID] = model.Entry{DocID: docID, Version: next, Payload: payload, Exists: true}
	return f, nil
}

func (s *MemStore) Findings() []model.Finding { return s.log.Snapshot() }

func (s *MemStore) Lookup(keyword string) []model.Finding {
	kw := strings.ToLower(keyword)
	var out []model.Finding
	for _, f := range s.log.Snapshot() {
		if strings.Contains(strings.ToLower(f.Payload), kw) {
			out = append(out, f)
		}
	}
	return out
}

// Claim grants a lease on docID to agentID for ttl. A free doc, an expired
// lease, or a lease already held by the same agent all succeed (the same-agent
// case renews). A live lease held by a different agent returns ErrLeaseHeld.
func (s *MemStore) Claim(docID, agentID string, ttl time.Duration) (model.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clk.Now()
	cur := s.claims[docID]
	if leaseLive(cur, now) && cur.AgentID != agentID {
		return model.Claim{}, ErrLeaseHeld
	}
	c := model.Claim{
		DocID:       docID,
		AgentID:     agentID,
		LeaseExpiry: now.Add(ttl),
		Version:     cur.Version + 1,
	}
	s.claims[docID] = c
	return c, nil
}

// Renew extends the lease iff agentID currently holds a live lease on docID.
func (s *MemStore) Renew(docID, agentID string, ttl time.Duration) (model.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clk.Now()
	cur := s.claims[docID]
	if !leaseLive(cur, now) || cur.AgentID != agentID {
		return model.Claim{}, ErrNotHolder
	}
	cur.LeaseExpiry = now.Add(ttl)
	s.claims[docID] = cur
	return cur, nil
}

// Release drops the lease iff agentID holds it. Idempotent-unsafe: releasing a
// lease you do not hold (or an already-expired one) returns ErrNotHolder.
func (s *MemStore) Release(docID, agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clk.Now()
	cur := s.claims[docID]
	if !leaseLive(cur, now) || cur.AgentID != agentID {
		return ErrNotHolder
	}
	s.claims[docID] = model.Claim{DocID: docID, Version: cur.Version} // cleared holder
	return nil
}
