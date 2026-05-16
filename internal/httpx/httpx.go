package httpx

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type ErrorResponse struct {
	Error     string         `json:"error"`
	Code      string         `json:"code,omitempty"`
	Category  string         `json:"category,omitempty"`
	Retryable bool           `json:"retryable,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

func WriteJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func DecodeJSON(r *http.Request, dst any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("request body must contain a single JSON document")
	}
	return nil
}

func WriteError(w http.ResponseWriter, status int, message string) {
	code, category, retryable := classifyError(status, message)
	WriteJSON(w, status, ErrorResponse{
		Error:     message,
		Code:      code,
		Category:  category,
		Retryable: retryable,
	})
}

func classifyError(status int, message string) (string, string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(message))
	switch {
	case status == http.StatusUnauthorized:
		return "auth_required", "auth", false
	case status == http.StatusForbidden:
		return "permission_denied", "auth", false
	case status == http.StatusNotFound:
		return "not_found", "not_found", false
	case status == http.StatusConflict:
		return "conflict", "conflict", false
	case status == http.StatusPaymentRequired:
		return "billing_required", "billing", false
	case status == http.StatusBadRequest && (strings.Contains(normalized, "unexpected eof") || strings.Contains(normalized, "connection reset")):
		return "upload_retryable", "transport", true
	case status == http.StatusBadRequest && strings.Contains(normalized, "parse multipart form"):
		return "invalid_upload", "validation", false
	case status == http.StatusBadRequest:
		return "invalid_request", "validation", false
	case status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout:
		return "temporarily_unavailable", "transport", true
	case status >= 500:
		return "server_error", "system", true
	default:
		return "request_failed", "request", false
	}
}
