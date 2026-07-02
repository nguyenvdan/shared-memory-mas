package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"quorum/internal/model"
	"quorum/internal/store"
)

type Server struct {
	store *store.MemStore
	mux   *http.ServeMux
}

func NewServer(s *store.MemStore) *Server {
	srv := &Server{store: s, mux: http.NewServeMux()}
	srv.mux.HandleFunc("/read", srv.handleRead)
	srv.mux.HandleFunc("/write", srv.handleWrite)
	srv.mux.HandleFunc("/findings", srv.handleFindings)
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Read(r.URL.Query().Get("doc")))
}

type writeRequest struct {
	DocID       string `json:"doc_id"`
	AgentID     string `json:"agent_id"`
	Payload     string `json:"payload"`
	BaseVersion int    `json:"base_version"`
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	var req writeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f, err := s.store.Write(req.DocID, req.AgentID, req.Payload, req.BaseVersion)
	switch {
	case errors.Is(err, store.ErrVersionConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "version conflict"})
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusOK, f)
	}
}

func (s *Server) handleFindings(w http.ResponseWriter, r *http.Request) {
	var res []model.Finding
	if q := r.URL.Query().Get("q"); q != "" {
		res = s.store.Lookup(q)
	} else {
		res = s.store.Findings()
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
