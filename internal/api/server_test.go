package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"quorum/internal/clock"
	"quorum/internal/model"
	"quorum/internal/store"
)

func newTestServer() *httptest.Server {
	s := store.NewMemStore(clock.NewMock(time.Unix(1_700_000_000, 0)))
	return httptest.NewServer(NewServer(s))
}

func TestWriteThenReadOverHTTP(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{
		"doc_id": "d1", "agent_id": "a", "payload": "alpha", "base_version": 0,
	})
	resp, err := http.Post(ts.URL+"/write", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write status = %d", resp.StatusCode)
	}

	r, _ := http.Get(ts.URL + "/read?doc=d1")
	var e model.Entry
	json.NewDecoder(r.Body).Decode(&e)
	if !e.Exists || e.Version != 1 || e.Payload != "alpha" {
		t.Fatalf("read = %+v", e)
	}
}

func TestWriteConflictReturns409(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	first, _ := json.Marshal(map[string]any{"doc_id": "d1", "agent_id": "a", "payload": "x", "base_version": 0})
	http.Post(ts.URL+"/write", "application/json", bytes.NewReader(first))

	stale, _ := json.Marshal(map[string]any{"doc_id": "d1", "agent_id": "b", "payload": "y", "base_version": 0})
	resp, _ := http.Post(ts.URL+"/write", "application/json", bytes.NewReader(stale))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", resp.StatusCode)
	}
}
