package model

import "time"

// A job's result is observation evidence, never authority to delete objects.
// Execution always re-reads references and applies the engine's safety checks.
type StorageMaintenanceJob struct {
	ID        string         `json:"id"`
	Kind      string         `json:"kind"`
	ScopeID   string         `json:"scope_id,omitempty"`
	Execute   bool           `json:"execute"`
	Status    string         `json:"status"`
	Attempts  int            `json:"attempts"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	RetryAt   *time.Time     `json:"retry_at,omitempty"`
	Error     string         `json:"error,omitempty"`
	Result    map[string]any `json:"result,omitempty"`
}
