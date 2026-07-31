package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	defaultBackupListPageLimit = 100
	maxBackupListPageLimit     = 500
	maxBackupListCursorLength  = 1024
)

var errInvalidBackupListCursor = errors.New("invalid cursor; restart pagination without cursor")

type backupListPagination struct {
	Cursor *store.BackupListCursor
	Kind   string
	Limit  int
	Scope  string
}

type backupListCursorPayload struct {
	CreatedAt string `json:"created_at"`
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Scope     string `json:"scope"`
	Version   int    `json:"v"`
}

type backupListPageInfo struct {
	HasNextPage bool   `json:"has_next_page"`
	Limit       int    `json:"limit"`
	NextCursor  string `json:"next_cursor,omitempty"`
}

func readBackupListPagination(r *http.Request, principal model.Principal, kind string, filters map[string]string) (backupListPagination, error) {
	limit := defaultBackupListPageLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return backupListPagination{}, fmt.Errorf("limit must be an integer between 1 and %d", maxBackupListPageLimit)
		}
		limit = parsed
	}
	if limit < 1 || limit > maxBackupListPageLimit {
		return backupListPagination{}, fmt.Errorf("limit must be between 1 and %d", maxBackupListPageLimit)
	}

	kind = strings.TrimSpace(kind)
	scope := backupListCursorScope(principal, kind, filters)
	pagination := backupListPagination{Kind: kind, Limit: limit, Scope: scope}
	if raw := strings.TrimSpace(r.URL.Query().Get("cursor")); raw != "" {
		cursor, err := decodeBackupListCursor(raw, kind, scope)
		if err != nil {
			return backupListPagination{}, err
		}
		pagination.Cursor = &cursor
	}
	return pagination, nil
}

func backupListCursorScope(principal model.Principal, kind string, filters map[string]string) string {
	parts := []string{
		"kind=" + kind,
		fmt.Sprintf("admin=%t", principal.IsPlatformAdmin()),
		"actor_type=" + strings.TrimSpace(principal.ActorType),
		"actor_id=" + strings.TrimSpace(principal.ActorID),
		"principal_tenant=" + strings.TrimSpace(principal.TenantID),
		"principal_project=" + strings.TrimSpace(principal.ProjectID),
	}
	keys := make([]string, 0, len(filters))
	for key := range filters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key+"="+strings.TrimSpace(filters[key]))
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(digest[:16])
}

func decodeBackupListCursor(raw, kind, scope string) (store.BackupListCursor, error) {
	if len(raw) > maxBackupListCursorLength {
		return store.BackupListCursor{}, errInvalidBackupListCursor
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) > maxBackupListCursorLength {
		return store.BackupListCursor{}, errInvalidBackupListCursor
	}
	var payload backupListCursorPayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return store.BackupListCursor{}, errInvalidBackupListCursor
	}
	if payload.Version != 1 || payload.Kind != kind || payload.Scope != scope || strings.TrimSpace(payload.ID) == "" {
		return store.BackupListCursor{}, errInvalidBackupListCursor
	}
	createdAt, err := time.Parse(time.RFC3339Nano, payload.CreatedAt)
	if err != nil || createdAt.IsZero() {
		return store.BackupListCursor{}, errInvalidBackupListCursor
	}
	return store.BackupListCursor{CreatedAt: createdAt.UTC(), ID: strings.TrimSpace(payload.ID)}, nil
}

func encodeBackupListCursor(createdAt time.Time, id, kind, scope string) string {
	payload, _ := json.Marshal(backupListCursorPayload{
		CreatedAt: createdAt.UTC().Format(time.RFC3339Nano),
		ID:        strings.TrimSpace(id),
		Kind:      kind,
		Scope:     scope,
		Version:   1,
	})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func buildBackupListPageInfo(pagination backupListPagination, hasNext bool, createdAt time.Time, id string) backupListPageInfo {
	info := backupListPageInfo{HasNextPage: hasNext, Limit: pagination.Limit}
	if hasNext {
		info.NextCursor = encodeBackupListCursor(createdAt, id, pagination.Kind, pagination.Scope)
	}
	return info
}
