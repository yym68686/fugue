package entryfailover

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
)

type Record struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Type     string          `json:"type"`
	Content  string          `json:"content"`
	TTL      int             `json:"ttl"`
	Proxied  bool            `json:"proxied"`
	Comment  string          `json:"comment,omitempty"`
	Tags     []string        `json:"tags,omitempty"`
	Settings json.RawMessage `json:"settings,omitempty"`
}

type Cloudflare struct {
	base   string
	zoneID string
	token  string
	client *http.Client
}

func NewCloudflare(base, zoneID, token string, timeout time.Duration) (*Cloudflare, error) {
	if base == "" {
		base = "https://api.cloudflare.com/client/v4"
	}
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid Cloudflare API base URL")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && net.ParseIP(u.Hostname()) != nil && net.ParseIP(u.Hostname()).IsLoopback()) {
		return nil, errors.New("Cloudflare API requires HTTPS")
	}
	if zoneID == "" || len(zoneID) > 128 || token == "" || len(token) > 512 || strings.ContainsAny(token, "\r\n \t") || timeout < time.Second || timeout > time.Minute {
		return nil, errors.New("zone ID, private token and bounded timeout required")
	}
	return &Cloudflare{base: strings.TrimRight(base, "/"), zoneID: zoneID, token: token,
		client: &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *Cloudflare) call(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		if len(raw) > 64<<10 {
			return errors.New("DNS write exceeds size bound")
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("Cloudflare %s request failed: %w", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("Cloudflare %s returned HTTP %d", method, resp.StatusCode)
	}
	var envelope struct {
		Success bool            `json:"success"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&envelope); err != nil {
		return fmt.Errorf("decode Cloudflare response: %w", err)
	}
	if !envelope.Success {
		return errors.New("Cloudflare reported DNS operation failure")
	}
	if out != nil {
		if len(envelope.Result) == 0 {
			return errors.New("Cloudflare response has no result")
		}
		return json.Unmarshal(envelope.Result, out)
	}
	return nil
}

func (c *Cloudflare) records(ctx context.Context, host string) ([]Record, error) {
	query := url.Values{"name.exact": {host}, "per_page": {"100"}}
	var result []Record
	err := c.call(ctx, http.MethodGet, "/zones/"+url.PathEscape(c.zoneID)+"/dns_records?"+query.Encode(), nil, &result)
	if err != nil {
		return nil, err
	}
	if len(result) >= 100 {
		return nil, errors.New("DNS record inspection exceeded 100-record bound")
	}
	for _, r := range result {
		if r.Name != host {
			return nil, errors.New("Cloudflare returned non-exact DNS name")
		}
	}
	return result, nil
}

// Snapshot accepts TXT/MX at an apex but rejects competing address and service
// bindings. One A or CNAME is required for each exact hostname.
func (c *Cloudflare) Snapshot(ctx context.Context, hosts []string) (map[string]Record, error) {
	result := make(map[string]Record, len(hosts))
	for _, h := range hosts {
		rs, err := c.records(ctx, h)
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			switch r.Type {
			case "A", "CNAME":
				if _, exists := result[h]; exists {
					return nil, fmt.Errorf("%s has multiple address records", h)
				}
				if r.ID == "" || r.Proxied || r.Content == "" || r.TTL < 1 {
					return nil, fmt.Errorf("%s requires one DNS-only address record", h)
				}
				result[h] = r
			case "AAAA", "HTTPS", "SVCB", "NS", "ALIAS", "ANAME":
				return nil, fmt.Errorf("%s has competing %s record", h, r.Type)
			}
		}
		if _, ok := result[h]; !ok {
			return nil, fmt.Errorf("%s has no managed A or CNAME record", h)
		}
	}
	return result, nil
}

func recordMatches(a, b Record) bool {
	if !recordBindingMatches(a, b) || a.Type != b.Type || a.Content != b.Content {
		return false
	}
	return settingsEqual(a.Settings, b.Settings)
}

func recordBindingMatches(a, b Record) bool {
	if a.ID != b.ID || a.Name != b.Name || a.TTL != b.TTL || a.Proxied != b.Proxied || a.Comment != b.Comment || len(a.Tags) != len(b.Tags) {
		return false
	}
	at, bt := append([]string(nil), a.Tags...), append([]string(nil), b.Tags...)
	sort.Strings(at)
	sort.Strings(bt)
	for i := range at {
		if at[i] != bt[i] {
			return false
		}
	}
	return true
}

func settingsEqual(a, b json.RawMessage) bool {
	var aSettings, bSettings any
	if len(a) != 0 {
		if err := json.Unmarshal(a, &aSettings); err != nil {
			return false
		}
	}
	if len(b) != 0 {
		if err := json.Unmarshal(b, &bSettings); err != nil {
			return false
		}
	}
	return reflect.DeepEqual(aSettings, bSettings)
}

func allowedSettings(current, baseline Record, zone string) bool {
	decode := func(raw json.RawMessage) (map[string]any, bool) {
		if len(raw) == 0 || string(raw) == "null" {
			return map[string]any{}, true
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil || out == nil {
			return nil, false
		}
		return out, true
	}
	got, ok := decode(current.Settings)
	if !ok {
		return false
	}
	want, ok := decode(baseline.Settings)
	if !ok {
		return false
	}
	// Cloudflare may add type-specific default flags when A becomes CNAME.
	// Only known false defaults and mandatory apex flattening may differ.
	for _, settings := range []map[string]any{got, want} {
		for k, v := range settings {
			if v == false && (k == "ipv4_only" || k == "ipv6_only" || k == "flatten_cname") {
				delete(settings, k)
			}
		}
	}
	if current.Type == "CNAME" && current.Name == zone && got["flatten_cname"] == true && want["flatten_cname"] == nil {
		delete(got, "flatten_cname")
	}
	if current.Type == "A" && current.Name == zone && want["flatten_cname"] == true && got["flatten_cname"] == nil {
		delete(want, "flatten_cname")
	}
	return reflect.DeepEqual(got, want)
}

func targetRecord(original Record, target Target) Record {
	result := original
	if target.Kind == "static-ip" {
		result.Type = "A"
	} else {
		result.Type = "CNAME"
	}
	result.Content = target.Address
	return result
}

// Batch writes only the ID, type and content of the exact saved records. It
// cannot provide a server-side compare-and-swap; the caller must own the writer.
func (c *Cloudflare) Batch(ctx context.Context, original map[string]Record, target Target) error {
	hosts := make([]string, 0, len(original))
	for h := range original {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	type patch struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Content string `json:"content"`
	}
	patches := make([]patch, 0, len(hosts))
	for _, h := range hosts {
		r := original[h]
		if r.ID == "" || r.Name != h || r.Proxied {
			return errors.New("unsafe DNS snapshot")
		}
		want := targetRecord(r, target)
		if !recordMatches(r, want) {
			patches = append(patches, patch{r.ID, want.Type, want.Content})
		}
	}
	if len(patches) == 0 {
		return nil
	}
	return c.call(ctx, http.MethodPost, "/zones/"+url.PathEscape(c.zoneID)+"/dns_records/batch", map[string]any{"patches": patches}, nil)
}
