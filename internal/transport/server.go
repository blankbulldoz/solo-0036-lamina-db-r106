package transport

import (
	"encoding/json"
	"errors"
	"io"
	"lamina/internal/engine"
	"log/slog"
	"net/http"
	"time"
)

type Server struct {
	db  *engine.DB
	mux *http.ServeMux
}

func New(db *engine.DB) *Server {
	s := &Server{db: db, mux: http.NewServeMux()}
	s.routes()
	return s
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	s.mux.ServeHTTP(w, r)
	slog.Debug("request", "method", r.Method, "path", r.URL.Path, "elapsed", time.Since(started))
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		respondError(w, &engine.Error{Code: "invalid", Message: "invalid request: " + err.Error()})
		return false
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		respondError(w, &engine.Error{Code: "invalid", Message: "exactly one JSON value required"})
		return false
	}
	return true
}
func write(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "response encoding failed", 500)
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
func respondError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal"
	var domain *engine.Error
	if errors.As(err, &domain) {
		code = domain.Code
		switch code {
		case "invalid":
			status = 400
		case "not_found":
			status = 404
		case "conflict":
			status = 409
		case "snapshot":
			status = 410
		case "capacity":
			status = 429
		case "unavailable":
			status = 503
		}
	}
	message := err.Error()
	if code == "internal" {
		slog.Error("request failed", "error", err)
		message = "internal database error"
	}
	write(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func (s *Server) routes() {
	const p = "/v1/lamina"
	s.mux.HandleFunc("GET "+p+"/health", s.health)
	s.mux.HandleFunc("GET "+p+"/catalog", s.catalog)
	s.mux.HandleFunc("POST "+p+"/relations", s.createRelation)
	s.mux.HandleFunc("POST "+p+"/indexes", s.createIndex)
	s.mux.HandleFunc("POST "+p+"/transactions", s.begin)
	s.mux.HandleFunc("POST "+p+"/transactions/{tx}/put", s.put)
	s.mux.HandleFunc("POST "+p+"/transactions/{tx}/delete", s.deleteRow)
	s.mux.HandleFunc("POST "+p+"/transactions/{tx}/commit", s.commit)
	s.mux.HandleFunc("POST "+p+"/transactions/{tx}/rollback", s.rollback)
	s.mux.HandleFunc("GET "+p+"/transactions/{tx}/outcome", s.outcome)
	s.mux.HandleFunc("POST "+p+"/get", s.get)
	s.mux.HandleFunc("POST "+p+"/query", s.query)
	s.mux.HandleFunc("POST "+p+"/sql", s.sql)
	s.mux.HandleFunc("POST "+p+"/checkpoint", s.checkpoint)
	s.mux.HandleFunc("POST "+p+"/vacuum", s.vacuum)
	s.mux.HandleFunc("GET "+p+"/backup", s.backup)
	s.mux.HandleFunc("GET "+p+"/changes", s.changes)
	s.mux.HandleFunc("GET "+p+"/stats", s.stats)
}
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	stats, err := s.db.Stats()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"status": "ready", "sequence": stats.Sequence})
}
