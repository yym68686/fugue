// Package objectstorage provisions private R2 resources; it never handles
// application object payloads or shares the backup credential namespace.
package objectstorage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const APIBase = "https://api.cloudflare.com/client/v4"
const ReadPermission = "6a018a9f2fc74eb6b293b0c548f38b39"
const WritePermission = "2efd5506f9c8494dacb1fa10a3e7d5b6"

type Client struct {
	AccountID, Token, BaseURL string
	HTTP                      *http.Client
}
type APIError struct {
	Status int
	Codes  []int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("object storage provider returned HTTP %d (codes %v)", e.Status, e.Codes)
}
func New(account, token string) *Client {
	return &Client{AccountID: account, Token: token, BaseURL: APIBase, HTTP: &http.Client{Timeout: 25 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+"/accounts/"+url.PathEscape(c.AccountID)+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return errors.New("object storage provider request failed; operation can be retried")
	}
	defer resp.Body.Close()
	var env struct {
		Success bool            `json:"success"`
		Result  json.RawMessage `json:"result"`
		Errors  []struct {
			Code int `json:"code"`
		} `json:"errors"`
	}
	err = json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&env)
	if err != nil || resp.StatusCode >= 300 || !env.Success {
		codes := []int{}
		for _, e := range env.Errors {
			codes = append(codes, e.Code)
		}
		return &APIError{Status: resp.StatusCode, Codes: codes}
	}
	if out != nil {
		return json.Unmarshal(env.Result, out)
	}
	return nil
}
func (c *Client) Verify(ctx context.Context) error {
	if err := c.call(ctx, "GET", "/r2/buckets?per_page=1", nil, nil); err != nil {
		return err
	}
	return c.call(ctx, "GET", "/tokens/permission_groups", nil, nil)
}
func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	var b struct {
		Name string `json:"name"`
	}
	err := c.call(ctx, "GET", "/r2/buckets/"+url.PathEscape(bucket), nil, &b)
	if err == nil {
		if b.Name != bucket {
			return errors.New("object storage bucket identity mismatch")
		}
		return nil
	}
	var api *APIError
	if !errors.As(err, &api) || api.Status != 404 {
		return err
	}
	return c.call(ctx, "POST", "/r2/buckets", map[string]string{"name": bucket}, nil)
}

type Token struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Value  string `json:"value"`
	Status string `json:"status"`
}

// RevokeNamed removes only tokens created for this durable grant identity.
// It reconciles an ambiguous create response without exposing an unknown secret.
func (c *Client) RevokeNamed(ctx context.Context, name string) error {
	ids := []string{}
	for page := 1; page <= 100; page++ {
		var tokens []Token
		if err := c.call(ctx, "GET", fmt.Sprintf("/tokens?page=%d&per_page=50", page), nil, &tokens); err != nil {
			return err
		}
		for _, t := range tokens {
			if t.Name == name {
				ids = append(ids, t.ID)
			}
		}
		if len(tokens) < 50 {
			break
		}
		if page == 100 {
			return errors.New("too many provider credentials to reconcile safely")
		}
	}
	for _, id := range ids {
		if err := c.Revoke(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
func (c *Client) CreateCredential(ctx context.Context, name, bucket, permission string) (Token, error) {
	if err := c.RevokeNamed(ctx, name); err != nil {
		return Token{}, err
	}
	group := ReadPermission
	if permission == "read-write" {
		group = WritePermission
	} else if permission != "read-only" {
		return Token{}, errors.New("invalid permission")
	}
	policy := map[string]any{"effect": "allow", "resources": map[string]string{"com.cloudflare.edge.r2.bucket." + c.AccountID + "_default_" + bucket: "*"}, "permission_groups": []map[string]string{{"id": group}}}
	var t Token
	err := c.call(ctx, "POST", "/tokens", map[string]any{"name": name, "policies": []any{policy}}, &t)
	if err == nil && (t.ID == "" || len(t.Value) < 32) {
		return Token{}, errors.New("provider returned an incomplete credential; reconciliation required")
	}
	return t, err
}
func (c *Client) Revoke(ctx context.Context, id string) error {
	err := c.call(ctx, "DELETE", "/tokens/"+url.PathEscape(id), nil, nil)
	var api *APIError
	if errors.As(err, &api) && api.Status == 404 {
		return nil
	}
	return err
}
func SecretAccessKey(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (c *Client) Identity(ctx context.Context) (string, error) {
	var t Token
	if err := c.call(ctx, "GET", "/tokens/verify", nil, &t); err != nil {
		return "", err
	}
	if t.ID == "" || t.Status != "active" {
		return "", errors.New("provider management token is inactive")
	}
	return t.ID, nil
}
