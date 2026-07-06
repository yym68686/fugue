package api

import (
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/httpx"
)

type geoIPMetadataResponse struct {
	PublicIP    string `json:"public_ip"`
	CountryCode string `json:"country_code,omitempty"`
	Source      string `json:"source,omitempty"`
	Error       string `json:"error,omitempty"`
}

func (s *Server) handleGeoIPMetadata(w http.ResponseWriter, r *http.Request) {
	publicIP := firstPublicIP(
		r.URL.Query().Get("ip"),
		firstForwardedIP(r.Header.Get("X-Forwarded-For")),
		r.Header.Get("X-Real-IP"),
		remoteHost(r.RemoteAddr),
	)
	response := geoIPMetadataResponse{PublicIP: publicIP}
	country, source, err := lookupCountryCodeForPublicIP(r.Context().Done(), publicIP)
	if err != nil {
		response.Error = err.Error()
	} else {
		response.CountryCode = country
		response.Source = source
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func firstPublicIP(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		ip := net.ParseIP(value)
		if ip == nil {
			continue
		}
		if isPrivateOrLoopbackIP(ip) {
			continue
		}
		return ip.String()
	}
	return ""
}

func firstForwardedIP(raw string) string {
	for _, part := range strings.Split(raw, ",") {
		if ip := strings.TrimSpace(part); ip != "" {
			return ip
		}
	}
	return ""
}

func remoteHost(remoteAddr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err == nil {
		return host
	}
	return strings.TrimSpace(remoteAddr)
}

func isPrivateOrLoopbackIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

func lookupCountryCodeForPublicIP(done <-chan struct{}, publicIP string) (string, string, error) {
	endpoint := "https://ipapi.co/json/"
	if strings.TrimSpace(publicIP) != "" {
		endpoint = "https://ipapi.co/" + url.PathEscape(strings.TrimSpace(publicIP)) + "/json/"
	}
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return "", "", err
	}
	client := &http.Client{Timeout: 4 * time.Second}
	type result struct {
		country string
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		defer resp.Body.Close()
		var payload struct {
			CountryCode string `json:"country_code"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			resultCh <- result{err: err}
			return
		}
		country := strings.ToLower(strings.TrimSpace(payload.CountryCode))
		if country == "" {
			resultCh <- result{err: errGeoIPCountryMissing{}}
			return
		}
		resultCh <- result{country: country}
	}()
	select {
	case <-done:
		return "", "", http.ErrAbortHandler
	case result := <-resultCh:
		if result.err != nil {
			return "", "", result.err
		}
		return result.country, "ipapi.co", nil
	}
}

type errGeoIPCountryMissing struct{}

func (errGeoIPCountryMissing) Error() string { return "country_code missing" }
