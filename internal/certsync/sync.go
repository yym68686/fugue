// Package certsync replicates an independent edge's current public certificates
// to a managed standby without making the independent edge depend on Fugue.
package certsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fugue/internal/tlscertificate"
)

type Config struct {
	APIURL          string   `json:"api_url"`
	APITokenFile    string   `json:"api_token_file"`
	AppID           string   `json:"app_id"`
	Hostnames       []string `json:"hostnames"`
	CaddyDataDir    string   `json:"caddy_data_dir"`
	MinimumDaysLeft int      `json:"minimum_days_left"`
}

type Result struct {
	Hostname          string    `json:"hostname"`
	CertificateSHA256 string    `json:"certificate_sha256"`
	NotAfter          time.Time `json:"not_after"`
	Action            string    `json:"action"`
}

type metadata struct {
	Hostname          string     `json:"hostname"`
	AppID             string     `json:"app_id"`
	Present           bool       `json:"present"`
	CertificateSHA256 string     `json:"certificate_sha256"`
	NotAfter          *time.Time `json:"not_after"`
}

type Synchronizer struct {
	Config Config
	Client *http.Client
	Roots  *x509.CertPool // nil uses the system roots
}

func (s Synchronizer) Run(ctx context.Context) ([]Result, error) {
	cfg := s.Config
	u, err := url.Parse(strings.TrimRight(cfg.APIURL, "/"))
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("certificate sync requires an HTTPS API URL")
	}
	if !filepath.IsAbs(cfg.APITokenFile) || !filepath.IsAbs(cfg.CaddyDataDir) || cfg.AppID == "" ||
		len(cfg.Hostnames) == 0 || len(cfg.Hostnames) > 16 || cfg.MinimumDaysLeft < 14 || cfg.MinimumDaysLeft > 45 {
		return nil, errors.New("invalid certificate sync configuration")
	}
	seen := map[string]bool{}
	for _, host := range cfg.Hostnames {
		if !validHost(host) || seen[host] {
			return nil, errors.New("certificate sync hostnames must be unique exact DNS names")
		}
		seen[host] = true
	}
	fileInfo, err := os.Lstat(cfg.APITokenFile)
	if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Mode().Perm()&0077 != 0 {
		return nil, errors.New("certificate sync API token file must be a private regular file")
	}
	rawToken, err := os.ReadFile(cfg.APITokenFile)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(string(rawToken))
	if token == "" || len(token) > 512 || strings.ContainsAny(token, "\r\n \t") {
		return nil, errors.New("invalid certificate sync API token")
	}
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	results := make([]Result, 0, len(cfg.Hostnames))
	for _, host := range cfg.Hostnames {
		result, err := s.syncHost(ctx, client, u, token, host)
		if err != nil {
			return results, fmt.Errorf("certificate sync for %s: %w", host, err)
		}
		results = append(results, result)
	}
	return results, nil
}

func validHost(host string) bool {
	if host == "" || len(host) > 253 || strings.ToLower(host) != host || strings.ContainsAny(host, "*/\\ :") || !strings.Contains(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	return true
}

type source struct {
	cert, key, sha string
	notAfter       time.Time
}

func (s Synchronizer) sourceCertificate(host string) (source, error) {
	pattern := filepath.Join(s.Config.CaddyDataDir, "certificates", "*", host, host+".crt")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return source{}, err
	}
	sort.Strings(matches)
	var best source
	for _, certPath := range matches {
		keyPath := filepath.Join(filepath.Dir(certPath), host+".key")
		certInfo, certErr := os.Lstat(certPath)
		keyInfo, keyErr := os.Lstat(keyPath)
		if certErr != nil || keyErr != nil || !certInfo.Mode().IsRegular() || !keyInfo.Mode().IsRegular() ||
			certInfo.Size() > 65536 || keyInfo.Size() > 16384 {
			continue
		}
		certPEM, certErr := os.ReadFile(certPath)
		keyPEM, keyErr := os.ReadFile(keyPath)
		if certErr != nil || keyErr != nil {
			continue
		}
		leaf, err := tlscertificate.ValidatePublic(host, string(certPEM), string(keyPEM), time.Now().UTC(), s.Roots)
		if err != nil || !leaf.NotAfter.After(best.notAfter) {
			continue
		}
		fingerprint := sha256.Sum256(leaf.Raw)
		best = source{cert: strings.TrimSpace(string(certPEM)), key: strings.TrimSpace(string(keyPEM)),
			sha: hex.EncodeToString(fingerprint[:]), notAfter: leaf.NotAfter.UTC()}
	}
	if best.sha == "" {
		return source{}, errors.New("no valid publicly trusted Caddy certificate was found")
	}
	if !best.notAfter.After(time.Now().Add(time.Duration(s.Config.MinimumDaysLeft) * 24 * time.Hour)) {
		return source{}, errors.New("source Caddy certificate is inside the renewal alert window")
	}
	return best, nil
}

func (s Synchronizer) syncHost(ctx context.Context, client *http.Client, base *url.URL, token, host string) (Result, error) {
	source, err := s.sourceCertificate(host)
	if err != nil {
		return Result{}, err
	}
	path := strings.TrimRight(base.Path, "/") + "/v1/apps/" + url.PathEscape(s.Config.AppID) +
		"/domains/" + url.PathEscape(host) + "/certificate"
	url := *base
	url.Path = path
	readMetadata := func() (metadata, error) {
		var m metadata
		if err := call(ctx, client, token, http.MethodGet, url.String(), nil, &m); err != nil {
			return m, err
		}
		if m.Hostname != host || m.AppID != s.Config.AppID {
			return m, errors.New("API certificate metadata identity differs from configuration")
		}
		return m, nil
	}
	current, err := readMetadata()
	if err != nil {
		return Result{}, err
	}
	result := Result{Hostname: host, CertificateSHA256: source.sha, NotAfter: source.notAfter}
	if current.Present && current.CertificateSHA256 == source.sha {
		result.Action = "unchanged"
		return result, nil
	}
	if current.Present && current.NotAfter != nil && !source.notAfter.After(*current.NotAfter) && current.NotAfter.After(time.Now()) {
		result.Action = "standby_newer"
		return result, nil
	}
	body := map[string]any{"certificate_pem": source.cert, "private_key_pem": source.key,
		"expected_certificate_sha256": current.CertificateSHA256}
	if err = call(ctx, client, token, http.MethodPut, url.String(), body, &current); err != nil {
		return Result{}, err
	}
	readback, err := readMetadata()
	if err != nil || readback.CertificateSHA256 != source.sha {
		return Result{}, errors.New("certificate import readback did not match the source fingerprint")
	}
	result.Action = "imported"
	return result, nil
}

func call(ctx context.Context, client *http.Client, token, method, endpoint string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil || len(raw) > 112<<10 {
			return errors.New("certificate import body exceeds bounds")
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("certificate API request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("certificate API returned HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<10)).Decode(out); err != nil {
		return errors.New("certificate API response is invalid")
	}
	return nil
}
