package edge

import (
	"context"
	"fugue/internal/tlsissuance"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Challenge forwarding returns only an exact public proof, never credentials,
// arbitrary origin content, a redirect, or a cached success on control outage.
func (s *Service) handleSharedHTTP01Challenge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	host := r.Host
	if parsed, _, err := net.SplitHostPort(host); err == nil {
		host = parsed
	}
	host = strings.ToLower(host)
	token := strings.TrimPrefix(r.URL.Path, "/.well-known/acme-challenge/")
	if !tlsissuance.ValidHostname(host) || !tlsissuance.ValidToken(token) {
		http.NotFound(w, r)
		return
	}
	origin := s.Config.ACMEChallengeAPIURL
	if origin == "" {
		origin = s.Config.APIURL
	}
	base, err := url.Parse(origin)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil {
		http.Error(w, "challenge temporarily unavailable", 503)
		return
	}
	base.Path = "/v1/acme/http-01/" + url.PathEscape(host) + "/" + url.PathEscape(token)
	base.RawPath = ""
	base.RawQuery = ""
	base.Fragment = ""
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		http.Error(w, "challenge temporarily unavailable", 503)
		return
	}
	client := *s.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		http.Error(w, "challenge temporarily unavailable", 503)
		return
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		http.NotFound(w, r)
		return
	}
	if response.StatusCode != 200 {
		http.Error(w, "challenge temporarily unavailable", 503)
		return
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 || !strings.HasPrefix(string(body), token+".") {
		http.Error(w, "challenge temporarily unavailable", 503)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain")
	w.Write(body)
}
