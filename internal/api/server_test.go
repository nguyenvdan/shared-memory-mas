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
	resp.Body.Close()

	r, err := http.Get(ts.URL + "/read?doc=d1")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
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
	w1, err := http.Post(ts.URL+"/write", "application/json", bytes.NewReader(first))
	if err != nil {
		t.Fatal(err)
	}
	w1.Body.Close()

	stale, _ := json.Marshal(map[string]any{"doc_id": "d1", "agent_id": "b", "payload": "y", "base_version": 0})
	resp, err := http.Post(ts.URL+"/write", "application/json", bytes.NewReader(stale))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", resp.StatusCode)
	}
}

func TestFindingsListsAllAndFiltersByQuery(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	for _, d := range []struct{ id, payload string }{
		{"d1", "quorum coordination"},
		{"d2", "unrelated topic"},
	} {
		body, _ := json.Marshal(map[string]any{
			"doc_id": d.id, "agent_id": "a", "payload": d.payload, "base_version": 0,
		})
		resp, err := http.Post(ts.URL+"/write", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}

	rAll, err := http.Get(ts.URL + "/findings")
	if err != nil {
		t.Fatal(err)
	}
	defer rAll.Body.Close()
	var all []model.Finding
	json.NewDecoder(rAll.Body).Decode(&all)
	if len(all) != 2 {
		t.Fatalf("findings = %d, want 2", len(all))
	}

	rQ, err := http.Get(ts.URL + "/findings?q=coordination")
	if err != nil {
		t.Fatal(err)
	}
	defer rQ.Body.Close()
	var filtered []model.Finding
	json.NewDecoder(rQ.Body).Decode(&filtered)
	if len(filtered) != 1 || filtered[0].DocID != "d1" {
		t.Fatalf("filtered = %+v, want 1 finding d1", filtered)
	}
}

func TestWriteBadJSONReturns400(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/write", "application/json", bytes.NewReader([]byte("{not json")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad-json status = %d, want 400", resp.StatusCode)
	}
}

func TestClaimEndpointGrantsAndConflicts(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	claim := func(agent string) int {
		body, _ := json.Marshal(map[string]any{"doc_id": "d1", "agent_id": agent, "ttl_ms": 60000})
		resp, err := http.Post(ts.URL+"/claim", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if got := claim("agent-a"); got != http.StatusOK {
		t.Fatalf("first claim = %d", got)
	}
	if got := claim("agent-b"); got != http.StatusConflict {
		t.Fatalf("second claim = %d, want 409", got)
	}
}

func TestReleaseEndpointReturns204(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	cb, _ := json.Marshal(map[string]any{"doc_id": "d1", "agent_id": "a", "ttl_ms": 60000})
	rc, err := http.Post(ts.URL+"/claim", "application/json", bytes.NewReader(cb))
	if err != nil {
		t.Fatal(err)
	}
	rc.Body.Close()

	rb, _ := json.Marshal(map[string]any{"doc_id": "d1", "agent_id": "a"})
	rr, err := http.Post(ts.URL+"/release", "application/json", bytes.NewReader(rb))
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Body.Close()
	if rr.StatusCode != http.StatusNoContent {
		t.Fatalf("release = %d, want 204", rr.StatusCode)
	}
}
