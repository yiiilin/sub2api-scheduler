package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//go:embed web/*
var webFS embed.FS

// API 与页面路由（仅泳道图）
type Server struct {
	db          *DB
	cfg         *Config
	auth        *Auth
	laneMonitor *LaneBoardMonitor
	sub         *Sub2APIClient
}

func NewServer(db *DB, cfg *Config, auth *Auth, laneMonitor *LaneBoardMonitor) *Server {
	sub := NewSub2APIClient(cfg.Sub2API.BaseURL, cfg.Sub2API.AdminAPIKey, db)
	return &Server{db: db, cfg: cfg, auth: auth, laneMonitor: laneMonitor, sub: sub}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// 页面
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {})

	// API（均需 token 鉴权）
	mux.HandleFunc("/api/boards", s.withAuth(s.handleBoards))
	mux.HandleFunc("/api/boards/", s.withAuth(s.handleBoardDetail))
	mux.HandleFunc("/api/board-candidates", s.withAuth(s.handleBoardCandidates))
	mux.HandleFunc("/api/history", s.withAuth(s.handleHistory))
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true, "ts": time.Now().Unix()})
	})
	return mux
}

// withAuth 校验 iframe token（sub2api 会话）
func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := ""
		if authorization := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(authorization, "Bearer ") {
			token = strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))
		}
		if token == "" {
			token = r.URL.Query().Get("token") // compatibility with older iframe URLs
		}
		isAdmin, err := s.auth.ValidateToken(token)
		if err != nil {
			writeJSON(w, 502, map[string]any{"error": "auth backend unreachable"})
			return
		}
		if !isAdmin {
			writeJSON(w, 401, map[string]any{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(webFS, "web/index.html")
	if err != nil {
		http.Error(w, "template error", 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, map[string]any{
		"SiteName": "泳道图调度",
	})
}

// GET /api/history?page=1&page_size=15 — 记录（泳道图事件 + 历史切换记录，分页）
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 15
	}
	hist, total, err := s.db.ListHistory(ctx, page, pageSize)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{
		"history":   hist,
		"count":     len(hist),
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// ============================ 泳道图 API ============================

// BoardAccountView 泳道图里账号的展示视图
type BoardAccountView struct {
	ID              int64   `json:"id"`
	Name            string  `json:"name"`
	Priority        int     `json:"priority"`
	Schedulable     bool    `json:"schedulable"`
	Status          string  `json:"status"`
	HasModelMapping bool    `json:"has_model_mapping"`
	ModelDisabled   bool    `json:"model_disabled"` // model_rate_limits 里有该模型（禁用/限流）
	ModelLimitInfo  *string `json:"model_limit_info"`
	State           string  `json:"state"` // healthy / disabled / suppressed
	DisabledAt      *string `json:"disabled_at"`
	LastProbeAt     *string `json:"last_probe_at"`
	LastProbeOK     *bool   `json:"last_probe_ok"`
	LastProbeMsg    string  `json:"last_probe_msg"`
	FailCount       int     `json:"fail_count"`
	CheckedAt       *string `json:"checked_at"`
	LastSuccessAt   *string `json:"last_success_at"` // 最近实际调用成功（usage_logs）
}

func boardMutationStatus(err error) int {
	if errors.Is(err, ErrBoardNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, ErrInvalidBoard) {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func decodeBoardJSON(w http.ResponseWriter, r *http.Request, board *LaneBoard) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(board); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("request body must contain a single JSON object")
		}
		return err
	}
	return nil
}

// GET /api/boards — 泳道图列表（含泳道、账号实时状态）
func (s *Server) handleBoards(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	switch r.Method {
	case http.MethodGet:
		boards, err := s.db.ListBoards(ctx)
		if err != nil {
			writeJSON(w, 500, map[string]any{"error": err.Error()})
			return
		}
		accs, err := s.sub.ListAccounts(ctx)
		if err != nil {
			writeJSON(w, 500, map[string]any{"error": "load accounts: " + err.Error()})
			return
		}
		accMap := make(map[int64]SubAccount, len(accs))
		for _, a := range accs {
			accMap[a.ID] = a
		}
		out := make([]map[string]any, 0, len(boards))
		for _, b := range boards {
			out = append(out, s.boardView(ctx, b, accMap))
		}
		writeJSON(w, 200, map[string]any{"boards": out, "count": len(out)})
	case http.MethodPost:
		var b LaneBoard
		if err := decodeBoardJSON(w, r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"error": "invalid json: " + err.Error()})
			return
		}
		if b.Name == "" || b.Model == "" {
			writeJSON(w, 400, map[string]any{"error": "name and model required"})
			return
		}
		if err := s.laneMonitor.SaveBoard(ctx, &b); err != nil {
			writeJSON(w, boardMutationStatus(err), map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"board": b})
	default:
		writeJSON(w, 405, map[string]any{"error": "method not allowed"})
	}
}

// GET /api/board-candidates?model=xxx — 有该模型映射的账号（可加入泳道）
func (s *Server) handleBoardCandidates(w http.ResponseWriter, r *http.Request) {
	model := r.URL.Query().Get("model")
	if model == "" {
		writeJSON(w, 400, map[string]any{"error": "model required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	rows, err := s.db.pool.Query(ctx, `
SELECT id, name, platform, type, priority, schedulable, status,
       credentials->'model_mapping',
       COALESCE(credentials->>'oauth_type', ''),
       COALESCE(credentials->>'project_id', ''),
       COALESCE((extra->>'openai_passthrough') = 'true', false),
       COALESCE((extra->>'openai_oauth_passthrough') = 'true', false)
FROM accounts
WHERE deleted_at IS NULL
ORDER BY priority ASC, id ASC`)
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	defer rows.Close()
	type cand struct {
		ID          int64  `json:"id"`
		Name        string `json:"name"`
		Platform    string `json:"-"`
		Type        string `json:"-"`
		Priority    int    `json:"priority"`
		Schedulable bool   `json:"schedulable"`
		Status      string `json:"status"`
	}
	var out []cand
	for rows.Next() {
		var c cand
		var mappingJSON []byte
		var oauthType, projectID string
		var passThrough, oauthPassThrough bool
		if err := rows.Scan(&c.ID, &c.Name, &c.Platform, &c.Type, &c.Priority, &c.Schedulable, &c.Status, &mappingJSON, &oauthType, &projectID, &passThrough, &oauthPassThrough); err != nil {
			writeJSON(w, 500, map[string]any{"error": err.Error()})
			return
		}
		credentials := map[string]any{
			"oauth_type":               oauthType,
			"project_id":               projectID,
			"openai_passthrough":       passThrough,
			"openai_oauth_passthrough": oauthPassThrough,
		}
		if len(mappingJSON) > 0 && string(mappingJSON) != "null" {
			var mapping map[string]any
			if err := json.Unmarshal(mappingJSON, &mapping); err != nil {
				writeJSON(w, 500, map[string]any{"error": "decode account model mapping: " + err.Error()})
				return
			}
			credentials["model_mapping"] = mapping
		}
		if modelMappingSupportsRequestedModelForAccount(c.Platform, c.Type, credentials, model) {
			out = append(out, c)
		}
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"accounts": out, "count": len(out)})
}

// PUT/DELETE /api/boards/{id}
// POST /api/boards/{id}/probe/{accountId} — 手动探测
func (s *Server) handleBoardDetail(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/boards/")
	parts := strings.Split(rest, "/")
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "bad board id"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	// 手动探测
	if len(parts) == 3 && parts[1] == "probe" {
		if r.Method != http.MethodPost {
			writeJSON(w, 405, map[string]any{"error": "POST only"})
			return
		}
		accID, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			writeJSON(w, 400, map[string]any{"error": "bad account id"})
			return
		}
		ok, msg, err := s.laneMonitor.ManualProbe(ctx, id, accID)
		if err != nil {
			writeJSON(w, boardMutationStatus(err), map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"healthy": ok, "message": msg})
		return
	}

	switch r.Method {
	case http.MethodPut:
		var b LaneBoard
		if err := decodeBoardJSON(w, r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"error": "invalid json: " + err.Error()})
			return
		}
		b.ID = id
		if err := s.laneMonitor.SaveBoard(ctx, &b); err != nil {
			writeJSON(w, boardMutationStatus(err), map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"board": b})
	case http.MethodDelete:
		if err := s.laneMonitor.DeleteBoard(ctx, id); err != nil {
			writeJSON(w, boardMutationStatus(err), map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	default:
		writeJSON(w, 405, map[string]any{"error": "method not allowed"})
	}
}

// boardView 组装泳道图的展示数据
func (s *Server) boardView(ctx context.Context, b LaneBoard, accMap map[int64]SubAccount) map[string]any {
	states, _ := s.db.GetAccountStates(ctx, b.ID)
	dbAccs, _ := s.db.GetAccountsByIDs(ctx, boardAllAccountIDs(b))
	// 最近实际调用成功时间（usage_logs 只记成功请求；窗口 7 天足够展示）
	lastSuccess, _ := s.db.LastSuccessfulCalls(ctx, boardAllAccountIDs(b), b.Model, 7*24*time.Hour)
	lanes := make([]map[string]any, 0, len(b.Lanes))
	laneAllDown := 0
	for _, l := range b.Lanes {
		accs := make([]BoardAccountView, 0, len(l.AccountIDs))
		down := 0
		suppressedCount := 0
		for _, aid := range l.AccountIDs {
			v := BoardAccountView{ID: aid}
			if da, ok := dbAccs[aid]; ok {
				v.Name = da.Name
				v.Priority = da.Priority
				v.Schedulable = da.Schedulable
				v.Status = da.Status
			} else {
				v.Name = fmt.Sprintf("#%d (not found)", aid)
			}
			if sa, ok := accMap[aid]; ok {
				if raw, ok := sa.Extra["model_rate_limits"].(map[string]any); ok {
					for _, modelKey := range accountModelRateLimitKeys(&sa, b.Model) {
						entry, exists := raw[modelKey]
						if !exists {
							continue
						}
						if !activeModelRateLimit(entry, time.Now()) {
							continue
						}
						v.ModelDisabled = true
						if s2, ok := entry.(map[string]any); ok {
							if reason, ok := s2["reason"].(string); ok {
								v.ModelLimitInfo = &reason
							}
						}
						break
					}
				}
			}
			if t, ok := lastSuccess[aid]; ok {
				s2 := t.Format(time.RFC3339)
				v.LastSuccessAt = &s2
			}
			if st, ok := states[aid]; ok {
				v.State = st.State
				if st.DisabledAt != nil {
					s2 := st.DisabledAt.Format(time.RFC3339)
					v.DisabledAt = &s2
				}
				if st.LastProbeAt != nil {
					s2 := st.LastProbeAt.Format(time.RFC3339)
					v.LastProbeAt = &s2
				}
				v.LastProbeOK = st.LastProbeOK
				v.LastProbeMsg = st.LastProbeMsg
				v.FailCount = st.FailCount
				if st.CheckedAt != nil {
					s2 := st.CheckedAt.Format(time.RFC3339)
					v.CheckedAt = &s2
				}
			}
			if v.State == LaneStateDisabled {
				down++
			}
			if v.State == LaneStateSuppressed {
				suppressedCount++
			}
			accs = append(accs, v)
		}
		if len(l.AccountIDs) > 0 && down == len(l.AccountIDs) {
			laneAllDown++
		}
		lanes = append(lanes, map[string]any{
			"id":               l.ID,
			"position":         l.Position,
			"name":             l.Name,
			"account_ids":      l.AccountIDs,
			"accounts":         accs,
			"all_down":         len(l.AccountIDs) > 0 && down == len(l.AccountIDs),
			"down_count":       down,
			"suppressed_count": suppressedCount,
		})
	}
	return map[string]any{
		"id":             b.ID,
		"name":           b.Name,
		"model":          b.Model,
		"enabled":        b.Enabled,
		"fail_threshold": b.FailThreshold,
		"window_seconds": b.WindowSeconds,
		"probe_interval": b.ProbeInterval,
		"lanes":          lanes,
		"lane_all_down":  laneAllDown,
	}
}

// boardAllAccountIDs 收集泳道图所有账号 ID
func boardAllAccountIDs(b LaneBoard) []int64 {
	return uniqueBoardAccountIDs(&b)
}
