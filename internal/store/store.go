package store

import (
	"errors"
	"strings"
	"sync"

	"quorum/internal/clock"
	"quorum/internal/model"
)

// ErrVersionConflict is returned by Write when the supplied base version does
// not match the entry's current version (optimistic-concurrency miss).
var ErrVersionConflict = errors.New("version conflict")

// MemStore is the in-memory store: a derived version map plus the append-only
// log that is the source of truth. Single mutex now; leases arrive in Phase 2.
type MemStore struct {
	mu      sync.RWMutex
	entries map[string]model.Entry
	log     *Log
	clk     clock.Clock
}

func NewMemStore(clk clock.Clock) *MemStore {
	return &MemStore{
		entries: make(map[string]model.Entry),
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
