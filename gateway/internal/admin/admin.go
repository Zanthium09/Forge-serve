// Package admin is the admin API (spec §7.6): users, teams, keys, model
// grants, usage reports and the audit log, so nobody manages the gateway
// with SQL. Routes are mounted behind auth + RequireAdmin by package api.
// Every write is audited by the store in the same transaction.
package admin

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
	"github.com/Zanthium09/forge-serve/gateway/internal/store"
)

// Evictor drops cached identities after changes (auth.Authenticator).
type Evictor interface {
	Evict(keyHash string)
	EvictUser(userID int64)
}

type handler struct {
	st       *store.Store
	ev       Evictor
	models   map[string]bool
	defaults config.UserDefaults
}

// New returns the /admin/v1/ routes. models are the configured model names
// (grants must name one); defaults fill omitted user limits.
func New(st *store.Store, ev Evictor, models []string, defaults config.UserDefaults) http.Handler {
	h := &handler{st: st, ev: ev, models: map[string]bool{}, defaults: defaults}
	for _, m := range models {
		h.models[m] = true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/v1/users", h.createUser)
	mux.HandleFunc("GET /admin/v1/users", h.listUsers)
	mux.HandleFunc("GET /admin/v1/users/inactive", h.inactiveUsers)
	mux.HandleFunc("GET /admin/v1/users/{id}", h.getUser)
	mux.HandleFunc("PATCH /admin/v1/users/{id}", h.patchUser)
	mux.HandleFunc("POST /admin/v1/users/{id}/disable", h.setStatus("disabled"))
	mux.HandleFunc("POST /admin/v1/users/{id}/enable", h.setStatus("active"))
	mux.HandleFunc("POST /admin/v1/users/{id}/keys", h.createKey)
	mux.HandleFunc("GET /admin/v1/users/{id}/keys", h.listKeys)
	mux.HandleFunc("POST /admin/v1/keys/{id}/revoke", h.revokeKey)
	mux.HandleFunc("POST /admin/v1/teams", h.createTeam)
	mux.HandleFunc("GET /admin/v1/teams", h.listTeams)
	mux.HandleFunc("PATCH /admin/v1/teams/{id}", h.patchTeam)
	mux.HandleFunc("DELETE /admin/v1/teams/{id}", h.deleteTeam)
	mux.HandleFunc("POST /admin/v1/grants", h.createGrant)
	mux.HandleFunc("GET /admin/v1/grants", h.listGrants)
	mux.HandleFunc("DELETE /admin/v1/grants/{id}", h.deleteGrant)
	mux.HandleFunc("GET /admin/v1/usage", h.usage(false))
	mux.HandleFunc("GET /admin/v1/usage.csv", h.usage(true))
	mux.HandleFunc("GET /admin/v1/audit", h.audit)
	return mux
}

// --- helpers ---------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, code, msg string) {
	typ := map[int]string{400: "invalid_request_error", 403: "permission_error", 404: "invalid_request_error",
		409: "invalid_request_error"}[status]
	if typ == "" {
		typ = "server_error"
	}
	writeJSON(w, status, map[string]any{"error": map[string]string{"message": msg, "type": typ, "code": code}})
}

// storeErr maps store errors to status codes; anything unknown is a 500
// with no detail (the detail may contain internals).
func storeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		fail(w, http.StatusNotFound, "not_found", "not found")
	case errors.Is(err, store.ErrConflict):
		fail(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, store.ErrInvalid):
		fail(w, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		fail(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func actor(r *http.Request) *int64 {
	id, ok := auth.FromContext(r.Context())
	if !ok {
		return nil
	}
	return &id.UserID
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		fail(w, http.StatusBadRequest, "invalid_request", "invalid id")
		return 0, false
	}
	return id, true
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		fail(w, http.StatusBadRequest, "invalid_request", "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func page(r *http.Request) (limit, offset int) {
	limit, offset = 100, 0
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = min(v, 1000)
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && v >= 0 {
		offset = v
	}
	return limit, offset
}

// --- users -----------------------------------------------------------

type userBody struct {
	Email              string   `json:"email"`
	DisplayName        string   `json:"display_name"`
	TeamID             *int64   `json:"team_id"`
	Role               string   `json:"role"`
	CanViewUsage       bool     `json:"can_view_usage"`
	RPMBurst           *int     `json:"rpm_burst"`
	RPMRefillPerSec    *float64 `json:"rpm_refill_per_sec"`
	TokensPerMinute    *int     `json:"tokens_per_minute"`
	DailyTokenBudget   *int64   `json:"daily_token_budget"`
	MonthlyTokenBudget *int64   `json:"monthly_token_budget"`
	Priority           *int     `json:"priority"`
}

func validRole(r string) bool { return r == "member" || r == "admin" }

func (h *handler) createUser(w http.ResponseWriter, r *http.Request) {
	var b userBody
	if !decode(w, r, &b) {
		return
	}
	b.Email = strings.TrimSpace(b.Email)
	if !strings.Contains(b.Email, "@") {
		fail(w, http.StatusBadRequest, "invalid_request", "email: required")
		return
	}
	if b.Role == "" {
		b.Role = "member"
	}
	if !validRole(b.Role) {
		fail(w, http.StatusBadRequest, "invalid_request", "role: must be member or admin")
		return
	}
	u := store.User{Email: b.Email, DisplayName: b.DisplayName, TeamID: b.TeamID, Role: b.Role,
		CanViewUsage: b.CanViewUsage, RPMBurst: h.defaults.RPMBurst, RPMRefillPerSec: h.defaults.RPMRefillPerSec,
		TokensPerMinute: b.TokensPerMinute, DailyTokenBudget: b.DailyTokenBudget,
		MonthlyTokenBudget: b.MonthlyTokenBudget, Priority: b.Priority}
	if b.RPMBurst != nil {
		u.RPMBurst = *b.RPMBurst
	}
	if b.RPMRefillPerSec != nil {
		u.RPMRefillPerSec = *b.RPMRefillPerSec
	}
	// Config defaults apply to omitted limits; zero in config = unlimited.
	if u.TokensPerMinute == nil && h.defaults.TokensPerMinute > 0 {
		v := h.defaults.TokensPerMinute
		u.TokensPerMinute = &v
	}
	if u.DailyTokenBudget == nil && h.defaults.DailyTokenBudget > 0 {
		v := h.defaults.DailyTokenBudget
		u.DailyTokenBudget = &v
	}
	if u.MonthlyTokenBudget == nil && h.defaults.MonthlyTokenBudget > 0 {
		v := h.defaults.MonthlyTokenBudget
		u.MonthlyTokenBudget = &v
	}
	out, err := h.st.CreateUser(r.Context(), actor(r), u)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *handler) listUsers(w http.ResponseWriter, r *http.Request) {
	var team *int64
	if v := r.URL.Query().Get("team_id"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_request", "team_id: invalid")
			return
		}
		team = &id
	}
	limit, offset := page(r)
	users, err := h.st.ListUsers(r.Context(), team, r.URL.Query().Get("status"), limit, offset)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, users)
}

func (h *handler) inactiveUsers(w http.ResponseWriter, r *http.Request) {
	days := 30
	if v, err := strconv.Atoi(r.URL.Query().Get("days")); err == nil && v > 0 {
		days = v
	}
	users, err := h.st.InactiveUsers(r.Context(), days)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, users)
}

func (h *handler) getUser(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	u, err := h.st.GetUser(r.Context(), id)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, u)
}

// patchFields turns a JSON object into column -> value for the store, where
// an explicit null clears a nullable column.
func patchFields(w http.ResponseWriter, r *http.Request, allowed map[string]bool) (map[string]any, bool) {
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&raw); err != nil {
		fail(w, http.StatusBadRequest, "invalid_request", "invalid JSON")
		return nil, false
	}
	set := map[string]any{}
	for k, v := range raw {
		nullable, ok := allowed[k]
		if !ok {
			fail(w, http.StatusBadRequest, "invalid_request", k+": cannot be changed")
			return nil, false
		}
		if string(v) == "null" {
			if !nullable {
				fail(w, http.StatusBadRequest, "invalid_request", k+": cannot be null")
				return nil, false
			}
			set[k] = nil
			continue
		}
		var val any
		if err := json.Unmarshal(v, &val); err != nil {
			fail(w, http.StatusBadRequest, "invalid_request", k+": invalid")
			return nil, false
		}
		set[k] = val
	}
	if role, ok := set["role"].(string); ok && !validRole(role) {
		fail(w, http.StatusBadRequest, "invalid_request", "role: must be member or admin")
		return nil, false
	}
	return set, true
}

func (h *handler) patchUser(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	set, ok := patchFields(w, r, store.UserColumns)
	if !ok {
		return
	}
	u, err := h.st.UpdateUser(r.Context(), actor(r), id, set)
	if err != nil {
		storeErr(w, err)
		return
	}
	h.ev.EvictUser(id) // limits/role changed: drop the cached identity now
	writeJSON(w, http.StatusOK, u)
}

func (h *handler) setStatus(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		u, err := h.st.SetUserStatus(r.Context(), actor(r), id, status)
		if err != nil {
			storeErr(w, err)
			return
		}
		h.ev.EvictUser(id)
		writeJSON(w, http.StatusOK, u)
	}
}

// --- keys ------------------------------------------------------------

func (h *handler) createKey(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var b struct {
		Label     string     `json:"label"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	if !decode(w, r, &b) {
		return
	}
	k, raw, err := h.st.CreateKey(r.Context(), actor(r), id, b.Label, b.ExpiresAt)
	if err != nil {
		storeErr(w, err)
		return
	}
	// The only response that ever carries the raw key.
	writeJSON(w, http.StatusCreated, map[string]any{
		"id": k.ID, "key": raw, "key_prefix": k.KeyPrefix, "label": k.Label, "expires_at": k.ExpiresAt,
	})
}

func (h *handler) listKeys(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	keys, err := h.st.ListKeys(r.Context(), id)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, keys)
}

func (h *handler) revokeKey(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	k, hash, err := h.st.RevokeKey(r.Context(), actor(r), id)
	if err != nil {
		storeErr(w, err)
		return
	}
	h.ev.Evict(hash)
	writeJSON(w, http.StatusOK, k)
}

// --- teams -----------------------------------------------------------

func (h *handler) createTeam(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Name            string `json:"name"`
		MonthlyTokenCap *int64 `json:"monthly_token_cap"`
		Priority        *int   `json:"priority"`
	}
	if !decode(w, r, &b) {
		return
	}
	if strings.TrimSpace(b.Name) == "" {
		fail(w, http.StatusBadRequest, "invalid_request", "name: required")
		return
	}
	t, err := h.st.CreateTeam(r.Context(), actor(r), store.Team{Name: b.Name, MonthlyTokenCap: b.MonthlyTokenCap, Priority: b.Priority})
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *handler) listTeams(w http.ResponseWriter, r *http.Request) {
	teams, err := h.st.ListTeams(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, teams)
}

// patchTeam: members' cached identities pick the change up within the
// auth cache TTL (spec §7.4), so no per-member eviction here.
func (h *handler) patchTeam(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	set, ok := patchFields(w, r, store.TeamColumns)
	if !ok {
		return
	}
	t, err := h.st.UpdateTeam(r.Context(), actor(r), id, set)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (h *handler) deleteTeam(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := h.st.DeleteTeam(r.Context(), actor(r), id); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- grants ----------------------------------------------------------

func (h *handler) createGrant(w http.ResponseWriter, r *http.Request) {
	var g store.Grant
	if !decode(w, r, &g) {
		return
	}
	if !h.models[g.Model] {
		fail(w, http.StatusBadRequest, "invalid_request", "model: not a configured model")
		return
	}
	if (g.UserID == nil) == (g.TeamID == nil) {
		fail(w, http.StatusBadRequest, "invalid_request", "exactly one of user_id or team_id is required")
		return
	}
	out, err := h.st.CreateGrant(r.Context(), actor(r), g)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (h *handler) listGrants(w http.ResponseWriter, r *http.Request) {
	gs, err := h.st.ListGrants(r.Context())
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, gs)
}

func (h *handler) deleteGrant(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := h.st.DeleteGrant(r.Context(), actor(r), id); err != nil {
		storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- usage & audit ---------------------------------------------------

func (h *handler) usage(asCSV bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		groupBy := q.Get("group_by")
		if groupBy == "" {
			groupBy = "model"
		}
		// Per-person usage is personal data: a separate permission from
		// managing keys (spec §4, §7.6).
		if groupBy == "user" {
			if id, _ := auth.FromContext(r.Context()); !id.CanViewUsage {
				fail(w, http.StatusForbidden, "usage_not_allowed", "per-person usage needs can_view_usage")
				return
			}
		}
		to := time.Now()
		from := to.AddDate(0, 0, -30)
		var err error
		if v := q.Get("from"); v != "" {
			if from, err = time.Parse(time.RFC3339, v); err != nil {
				fail(w, http.StatusBadRequest, "invalid_request", "from: RFC 3339 required")
				return
			}
		}
		if v := q.Get("to"); v != "" {
			if to, err = time.Parse(time.RFC3339, v); err != nil {
				fail(w, http.StatusBadRequest, "invalid_request", "to: RFC 3339 required")
				return
			}
		}
		rows, err := h.st.UsageBy(r.Context(), groupBy, from, to)
		if err != nil {
			storeErr(w, err)
			return
		}
		if !asCSV {
			writeJSON(w, http.StatusOK, rows)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"key", "requests", "prompt_tokens", "completion_tokens", "total_tokens"})
		for _, u := range rows {
			_ = cw.Write([]string{u.Key, strconv.FormatInt(u.Requests, 10), strconv.FormatInt(u.PromptTokens, 10),
				strconv.FormatInt(u.CompletionTokens, 10), strconv.FormatInt(u.TotalTokens, 10)})
		}
		cw.Flush()
	}
}

func (h *handler) audit(w http.ResponseWriter, r *http.Request) {
	limit, offset := page(r)
	entries, err := h.st.ListAudit(r.Context(), limit, offset)
	if err != nil {
		storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}
