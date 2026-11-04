package objectstorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
)

const archiveLockRule = "fugue-compiler-input-archive"

// ProtectArchiveBucket must only be called for a newly platform-provisioned
// dedicated bucket. Never remove or rewrite somebody else's lifecycle rules.
func (c *Client) ProtectArchiveBucket(ctx context.Context, bucket string) (string, error) {
	path := "/r2/buckets/" + url.PathEscape(bucket)
	var lifecycle struct {
		Rules []json.RawMessage `json:"rules"`
	}
	if err := c.call(ctx, http.MethodGet, path+"/lifecycle", nil, &lifecycle); err != nil {
		return "", err
	}
	// R2 adds an incomplete-multipart abort rule by default. It cannot remove
	// committed archive objects. Reject every other action, including unknown
	// future transitions; never silently overwrite an external lifecycle.
	for _, raw := range lifecycle.Rules {
		var rule map[string]json.RawMessage
		if err := json.Unmarshal(raw, &rule); err != nil {
			return "", err
		}
		for key := range rule {
			if key != "id" && key != "conditions" && key != "enabled" && key != "abortMultipartUploadsTransition" {
				return "", errors.New("archive bucket has external lifecycle actions")
			}
		}
		if len(rule["abortMultipartUploadsTransition"]) == 0 || string(rule["abortMultipartUploadsTransition"]) == "null" {
			return "", errors.New("archive bucket has unrecognized lifecycle rules")
		}
	}
	rules := map[string]any{"rules": []any{map[string]any{"id": archiveLockRule, "enabled": true, "prefix": "", "condition": map[string]string{"type": "Indefinite"}}}}
	var current struct {
		Rules []struct {
			ID        string `json:"id"`
			Enabled   bool   `json:"enabled"`
			Prefix    string `json:"prefix"`
			Condition struct {
				Type string `json:"type"`
			} `json:"condition"`
		} `json:"rules"`
	}
	if err := c.call(ctx, http.MethodGet, path+"/lock", nil, &current); err != nil {
		return "", err
	}
	if len(current.Rules) == 0 {
		if err := c.call(ctx, http.MethodPut, path+"/lock", rules, nil); err != nil {
			return "", err
		}
		if err := c.call(ctx, http.MethodGet, path+"/lock", nil, &current); err != nil {
			return "", err
		}
	}
	if len(current.Rules) != 1 || current.Rules[0].ID != archiveLockRule || !current.Rules[0].Enabled || current.Rules[0].Prefix != "" || current.Rules[0].Condition.Type != "Indefinite" {
		return "", errors.New("archive bucket lock differs from protected policy")
	}
	encoded, _ := json.Marshal(map[string]any{"lock": rules, "lifecycle": lifecycle.Rules})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
