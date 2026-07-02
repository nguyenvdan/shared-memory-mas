package store

import (
	"sync"

	"quorum/internal/model"
)

// Log is the append-only source of truth. Lock ordering: a MemStore may call
// Log.Append while holding its own mutex; Log.mu is a leaf and never calls
// back into the store, so no cycle is possible.
type Log struct {
	mu      sync.Mutex
	records []model.Finding
	seq     int64
}

func NewLog() *Log { return &Log{} }

// Append stamps the next sequence number (starting at 1) and stores the record.
func (l *Log) Append(f model.Finding) model.Finding {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	f.Seq = l.seq
	l.records = append(l.records, f)
	return f
}

// Snapshot returns a defensive copy of the log in append order.
func (l *Log) Snapshot() []model.Finding {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]model.Finding, len(l.records))
	copy(out, l.records)
	return out
}
