package model

import "time"

// Doc is one corpus document. Categories is kept for optional future
// topic work but is unused by the doc-level duplication metric.
type Doc struct {
	ID         string   `json:"id"`
	Title      string   `json:"title"`
	Abstract   string   `json:"abstract"`
	Categories []string `json:"categories"`
}

// Entry is the current versioned state of a document in the store.
type Entry struct {
	DocID   string `json:"doc_id"`
	Version int    `json:"version"`
	Payload string `json:"payload"`
	Exists  bool   `json:"exists"`
}

// Finding is one append-only log record: a committed write.
type Finding struct {
	Seq              int64     `json:"seq"`
	DocID            string    `json:"doc_id"`
	AgentID          string    `json:"agent_id"`
	Payload          string    `json:"payload"`
	BaseVersion      int       `json:"base_version"`
	CommittedVersion int       `json:"committed_version"`
	Timestamp        time.Time `json:"timestamp"`
}

// Claim is a lease held by an agent over a document.
type Claim struct {
	DocID       string    `json:"doc_id"`
	AgentID     string    `json:"agent_id"`
	LeaseExpiry time.Time `json:"lease_expiry"`
	Version     int       `json:"version"`
}
