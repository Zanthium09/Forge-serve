package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
	"github.com/Zanthium09/forge-serve/gateway/internal/store"
	"github.com/Zanthium09/forge-serve/gateway/internal/store/storetest"
)

type spy struct {
	hashes []string
	users  []int64
}

func (s *spy) Evict(h string)          { s.hashes = append(s.hashes, h) }
func (s *spy) EvictUser(id int64)      { s.users = append(s.users, id) }
func (s *spy) evictedUser(id int64) bool {
	for _, u := range s.users {
		if u == id {
			return true
		}
	}
	return false
}

type fixture struct {
	st    *store.Store
	h     http.Handler
	spy   *spy
	admin auth.Identity
}

func setup(t *testing.T) fixture {
	t.Helper()
	st := storetest.Open(t)
	var id int64
	if err := st.DB().QueryRow(`INSERT INTO users (email, role) VALUES ('root@x.com', 'admin') RETURNING id`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	sp := &spy{}
	h := New(st, sp, []string{"big", "small"}, config.UserDefaults{RPMBurst: 60, RPMRefillPerSec: 10})
	return fixture{st: st, h: h, spy: sp, admin: auth.Identity{UserID: id, Role: "admin"}}
}

func (f fixture) call(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return f.callAs(t, f.admin, method, path, body)
}

func (f fixture) callAs(t *testing.T, who auth.Identity, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req = req.WithContext(auth.WithIdentity(context.Background(), who))
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	return rec
}

func decodeMap(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return m
}

func createUser(t *testing.T, f fixture, body string) int64 {
	t.Helper()
	rec := f.call(t, http.MethodPost, "/admin/v1/users", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create user: %d %s", rec.Code, rec.Body)
	}
	return int64(decodeMap(t, rec)["id"].(float64))
}

func TestCreateUserAppliesDefaults(t *testing.T) {
	f := setup(t)
	rec := f.call(t, http.MethodPost, "/admin/v1/users", `{"email":"a@x.com","display_name":"A"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d %s", rec.Code, rec.Body)
	}
	u := decodeMap(t, rec)
	if u["rpm_burst"].(float64) != 60 || u["priority"] != nil || u["status"] != "active" || u["role"] != "member" {
		t.Fatalf("user = %v", u)
	}
	var n int
	_ = f.st.DB().QueryRow(`SELECT count(*) FROM admin_audit WHERE action = 'user.create' AND actor_user_id = $1`, f.admin.UserID).Scan(&n)
	if n != 1 {
		t.Fatalf("audit rows = %d", n)
	}
}

func TestCreateUserDuplicateEmail409(t *testing.T) {
	f := setup(t)
	createUser(t, f, `{"email":"a@x.com"}`)
	if rec := f.call(t, http.MethodPost, "/admin/v1/users", `{"email":"A@X.com"}`); rec.Code != http.StatusConflict {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestCreateUserValidates(t *testing.T) {
	f := setup(t)
	for _, b := range []string{`{}`, `{"email":"nope"}`, `{"email":"a@x.com","role":"god"}`, `not json`} {
		if rec := f.call(t, http.MethodPost, "/admin/v1/users", b); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d", b, rec.Code)
		}
	}
}

func TestPatchUserNullClears(t *testing.T) {
	f := setup(t)
	id := createUser(t, f, `{"email":"a@x.com","tokens_per_minute":100}`)
	path := "/admin/v1/users/" + strconv.FormatInt(id, 10)
	rec := f.call(t, http.MethodPatch, path, `{"tokens_per_minute":null,"priority":3}`)
	u := decodeMap(t, rec)
	if rec.Code != 200 || u["tokens_per_minute"] != nil || u["priority"].(float64) != 3 {
		t.Fatalf("status = %d user = %v", rec.Code, u)
	}
	if !f.spy.evictedUser(id) {
		t.Fatal("PATCH must evict the user's cached identity")
	}
	if rec := f.call(t, http.MethodPatch, path, `{"email":"b@x.com"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("email change status = %d, want 400", rec.Code)
	}
}

func TestCreateKeyReturnsRawOnce(t *testing.T) {
	f := setup(t)
	id := createUser(t, f, `{"email":"a@x.com"}`)
	base := "/admin/v1/users/" + strconv.FormatInt(id, 10) + "/keys"
	rec := f.call(t, http.MethodPost, base, `{"label":"laptop"}`)
	k := decodeMap(t, rec)
	raw, _ := k["key"].(string)
	if rec.Code != http.StatusCreated || !strings.HasPrefix(raw, "sk-forge-") || k["key_prefix"] != raw[9:17] {
		t.Fatalf("status = %d key = %v", rec.Code, k)
	}
	list := f.call(t, http.MethodGet, base, "")
	if strings.Contains(list.Body.String(), "sk-forge-") || strings.Contains(list.Body.String(), auth.HashKey(raw)) ||
		strings.Contains(list.Body.String(), `"key"`) {
		t.Fatalf("key list leaks secrets: %s", list.Body)
	}
	var leaks int
	_ = f.st.DB().QueryRow(`SELECT count(*) FROM admin_audit WHERE detail::text LIKE '%sk-forge-%'`).Scan(&leaks)
	if leaks != 0 {
		t.Fatal("audit detail contains a raw key")
	}
	if rec, err := f.st.LookupKey(context.Background(), auth.HashKey(raw)); err != nil || rec.UserID != id {
		t.Fatalf("created key does not authenticate: %v", err)
	}
}

func TestRevokeKeyEvictsCache(t *testing.T) {
	f := setup(t)
	id := createUser(t, f, `{"email":"a@x.com"}`)
	k := decodeMap(t, f.call(t, http.MethodPost, "/admin/v1/users/"+strconv.FormatInt(id, 10)+"/keys", `{}`))
	raw := k["key"].(string)
	keyID := strconv.FormatInt(int64(k["id"].(float64)), 10)
	if rec := f.call(t, http.MethodPost, "/admin/v1/keys/"+keyID+"/revoke", ""); rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if len(f.spy.hashes) != 1 || f.spy.hashes[0] != auth.HashKey(raw) {
		t.Fatalf("evicted = %v", f.spy.hashes)
	}
	if rec, _ := f.st.LookupKey(context.Background(), auth.HashKey(raw)); !rec.Revoked {
		t.Fatal("key not revoked in the store")
	}
}

func TestDisableUserEvictsAndBlocks(t *testing.T) {
	f := setup(t)
	id := createUser(t, f, `{"email":"a@x.com"}`)
	k := decodeMap(t, f.call(t, http.MethodPost, "/admin/v1/users/"+strconv.FormatInt(id, 10)+"/keys", `{}`))
	if rec := f.call(t, http.MethodPost, "/admin/v1/users/"+strconv.FormatInt(id, 10)+"/disable", ""); rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if !f.spy.evictedUser(id) {
		t.Fatal("disable must evict")
	}
	if rec, _ := f.st.LookupKey(context.Background(), auth.HashKey(k["key"].(string))); rec.Active {
		t.Fatal("disabled user still active")
	}
}

func TestDeleteTeamWithUsers409(t *testing.T) {
	f := setup(t)
	team := decodeMap(t, f.call(t, http.MethodPost, "/admin/v1/teams", `{"name":"ml"}`))
	tid := strconv.FormatInt(int64(team["id"].(float64)), 10)
	createUser(t, f, `{"email":"a@x.com","team_id":`+tid+`}`)
	if rec := f.call(t, http.MethodDelete, "/admin/v1/teams/"+tid, ""); rec.Code != http.StatusConflict {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestGrantUnknownModel400(t *testing.T) {
	f := setup(t)
	id := createUser(t, f, `{"email":"a@x.com"}`)
	sid := strconv.FormatInt(id, 10)
	if rec := f.call(t, http.MethodPost, "/admin/v1/grants", `{"model":"nope","user_id":`+sid+`}`); rec.Code != 400 {
		t.Fatalf("unknown model status = %d", rec.Code)
	}
	if rec := f.call(t, http.MethodPost, "/admin/v1/grants", `{"model":"big"}`); rec.Code != 400 {
		t.Fatalf("no target status = %d", rec.Code)
	}
	if rec := f.call(t, http.MethodPost, "/admin/v1/grants", `{"model":"big","user_id":`+sid+`}`); rec.Code != 201 {
		t.Fatalf("valid grant status = %d %s", rec.Code, rec.Body)
	}
}

func seedUsage(t *testing.T, f fixture, userID int64, ago string) {
	t.Helper()
	db := f.st.DB()
	var key int64
	err := db.QueryRow(`INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES ($1, md5(random()::text), 'p') RETURNING id`, userID).Scan(&key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO usage_events (user_id, key_id, model, endpoint, prompt_tokens, completion_tokens, created_at)
		VALUES ($1, $2, 'big', 'chat', 3, 4, now() - $3::interval)`, userID, key, ago); err != nil {
		t.Fatal(err)
	}
}

func TestUsageByUserNeedsCanViewUsage(t *testing.T) {
	f := setup(t)
	id := createUser(t, f, `{"email":"a@x.com"}`)
	seedUsage(t, f, id, "1 hour")
	if rec := f.call(t, http.MethodGet, "/admin/v1/usage?group_by=user", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("without can_view_usage: %d", rec.Code)
	}
	rec := f.call(t, http.MethodGet, "/admin/v1/usage?group_by=model", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"total_tokens":7`) {
		t.Fatalf("by model: %d %s", rec.Code, rec.Body)
	}
	viewer := f.admin
	viewer.CanViewUsage = true
	if rec := f.callAs(t, viewer, http.MethodGet, "/admin/v1/usage?group_by=user", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), "a@x.com") {
		t.Fatalf("with can_view_usage: %d %s", rec.Code, rec.Body)
	}
}

func TestUsageCSV(t *testing.T) {
	f := setup(t)
	id := createUser(t, f, `{"email":"a@x.com"}`)
	seedUsage(t, f, id, "1 hour")
	rec := f.call(t, http.MethodGet, "/admin/v1/usage.csv?group_by=model", "")
	lines := strings.Split(strings.TrimSpace(rec.Body.String()), "\n")
	if rec.Code != 200 || strings.TrimSpace(lines[0]) != "key,requests,prompt_tokens,completion_tokens,total_tokens" ||
		strings.TrimSpace(lines[1]) != "big,1,3,4,7" {
		t.Fatalf("csv = %q", rec.Body.String())
	}
}

func TestInactiveUsers(t *testing.T) {
	f := setup(t)
	idle := createUser(t, f, `{"email":"idle@x.com"}`)
	busy := createUser(t, f, `{"email":"busy@x.com"}`)
	_, _ = f.st.DB().Exec(`UPDATE users SET created_at = now() - interval '60 days'`)
	seedUsage(t, f, idle, "40 days")
	seedUsage(t, f, busy, "1 day")
	body := f.call(t, http.MethodGet, "/admin/v1/users/inactive?days=30", "").Body.String()
	if !strings.Contains(body, "idle@x.com") || strings.Contains(body, "busy@x.com") {
		t.Fatalf("inactive = %s", body)
	}
}

func TestAuditListNewestFirst(t *testing.T) {
	f := setup(t)
	createUser(t, f, `{"email":"a@x.com"}`)
	f.call(t, http.MethodPost, "/admin/v1/teams", `{"name":"ml"}`)
	var entries []map[string]any
	_ = json.Unmarshal(f.call(t, http.MethodGet, "/admin/v1/audit", "").Body.Bytes(), &entries)
	if len(entries) != 2 || entries[0]["action"] != "team.create" {
		t.Fatalf("audit = %v", entries)
	}
}

func TestNonAdminGets403(t *testing.T) {
	f := setup(t)
	a := auth.New(nil, 0, 0, nil)
	guarded := a.RequireAdmin(f.h)
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/users", nil)
	req = req.WithContext(auth.WithIdentity(context.Background(), auth.Identity{UserID: 99, Role: "member"}))
	rec := httptest.NewRecorder()
	guarded.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}
