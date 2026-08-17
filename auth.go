package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// Auth 用 sub2api 内部 API 验证 iframe 传来的 token
type Auth struct {
	sub2apiBase string
	client      *http.Client
}

func NewAuth(baseURL string) *Auth {
	return &Auth{
		sub2apiBase: strings.TrimSuffix(baseURL, "/"),
		client:      &http.Client{Timeout: 5 * time.Second},
	}
}

// ValidateToken 调 sub2api /api/v1/auth/me 验证 token，返回是否管理员
func (a *Auth) ValidateToken(token string) (bool, error) {
	if token == "" {
		return false, nil
	}
	req, err := http.NewRequest("GET", a.sub2apiBase+"/api/v1/auth/me", nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := a.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false, nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var me struct {
		Data struct {
			Role string `json:"role"` // "admin" / "user"
		} `json:"data"`
	}
	_ = json.Unmarshal(body, &me)
	return me.Data.Role == "admin", nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
