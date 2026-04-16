package workloadidentity

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

const (
	tokenPrefix = "fugue_wk_"
	tokenV1     = "v1"
)

var (
	ErrInvalidToken = errors.New("invalid workload identity token")
)

type Claims struct {
	Version   string   `json:"v"`
	TenantID  string   `json:"t"`
	ProjectID string   `json:"p"`
	AppID     string   `json:"a,omitempty"`
	Scopes    []string `json:"s,omitempty"`
	IssuedAt  int64    `json:"iat,omitempty"`
}

func Issue(signingKey string, claims Claims) (string, error) {
	signingKey = strings.TrimSpace(signingKey)
	claims.TenantID = strings.TrimSpace(claims.TenantID)
	claims.ProjectID = strings.TrimSpace(claims.ProjectID)
	if signingKey == "" || claims.TenantID == "" || claims.ProjectID == "" {
		return "", ErrInvalidToken
	}
	claims.Version = tokenV1
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	payloadEncoded := base64.RawURLEncoding.EncodeToString(payload)
	sig := sign(signingKey, payloadEncoded)
	return tokenPrefix + payloadEncoded + "." + sig, nil
}

func Parse(signingKey, token string) (Claims, error) {
	signingKey = strings.TrimSpace(signingKey)
	token = strings.TrimSpace(token)
	if signingKey == "" || token == "" || !strings.HasPrefix(token, tokenPrefix) {
		return Claims{}, ErrInvalidToken
	}
	raw := strings.TrimPrefix(token, tokenPrefix)
	parts := strings.Split(raw, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Claims{}, ErrInvalidToken
	}
	payloadEncoded := parts[0]
	expected := sign(signingKey, payloadEncoded)
	if !hmac.Equal([]byte(expected), []byte(parts[1])) {
		return Claims{}, ErrInvalidToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadEncoded)
	if err != nil {
		return Claims{}, ErrInvalidToken
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return Claims{}, ErrInvalidToken
	}
	if claims.Version != tokenV1 || strings.TrimSpace(claims.TenantID) == "" || strings.TrimSpace(claims.ProjectID) == "" {
		return Claims{}, ErrInvalidToken
	}
	return claims, nil
}

func sign(signingKey, payloadEncoded string) string {
	mac := hmac.New(sha256.New, []byte(signingKey))
	_, _ = mac.Write([]byte(payloadEncoded))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
