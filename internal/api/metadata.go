package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	country, source, err := lookupCountryCodeForPublicIP(r.Context(), publicIP)
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

type geoIPLookupEndpoint struct {
	Name   string
	URL    func(publicIP string) string
	Decode func(io.Reader) (string, error)
}

var geoIPHTTPClient = &http.Client{Timeout: 4 * time.Second}

var geoIPLookupEndpoints = []geoIPLookupEndpoint{
	{
		Name: "ipapi.co",
		URL: func(publicIP string) string {
			if strings.TrimSpace(publicIP) != "" {
				return "https://ipapi.co/" + url.PathEscape(strings.TrimSpace(publicIP)) + "/json/"
			}
			return "https://ipapi.co/json/"
		},
		Decode: decodeIPAPICountryCode,
	},
	{
		Name: "ipinfo.io",
		URL: func(publicIP string) string {
			if strings.TrimSpace(publicIP) != "" {
				return "https://ipinfo.io/" + url.PathEscape(strings.TrimSpace(publicIP)) + "/country"
			}
			return "https://ipinfo.io/country"
		},
		Decode: decodePlainCountryCode,
	},
	{
		Name: "ip-api.com",
		URL: func(publicIP string) string {
			path := ""
			if strings.TrimSpace(publicIP) != "" {
				path = "/" + url.PathEscape(strings.TrimSpace(publicIP))
			}
			return "http://ip-api.com/json" + path + "?fields=status,countryCode,message,query"
		},
		Decode: decodeIPAPIComCountryCode,
	},
}

func lookupCountryCodeForPublicIP(ctx context.Context, publicIP string) (string, string, error) {
	var lastErr error
	for _, endpoint := range geoIPLookupEndpoints {
		endpointURL := strings.TrimSpace(endpoint.URL(publicIP))
		if endpointURL == "" {
			continue
		}
		country, err := lookupCountryCodeAtEndpoint(ctx, endpoint, endpointURL)
		if err == nil {
			return country, endpoint.Name, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errGeoIPCountryMissing{}
	}
	return "", "", lastErr
}

func lookupCountryCodeAtEndpoint(ctx context.Context, endpoint geoIPLookupEndpoint, endpointURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := geoIPHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("%s returned status %d", endpoint.Name, resp.StatusCode)
	}
	country, err := endpoint.Decode(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	country = normalizeCountryCode(country)
	if country == "" {
		return "", errGeoIPCountryMissing{}
	}
	return country, nil
}

func decodeIPAPICountryCode(r io.Reader) (string, error) {
	var payload struct {
		CountryCode string `json:"country_code"`
	}
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return "", err
	}
	return payload.CountryCode, nil
}

func decodeIPAPIComCountryCode(r io.Reader) (string, error) {
	var payload struct {
		Status      string `json:"status"`
		CountryCode string `json:"countryCode"`
		Message     string `json:"message"`
	}
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return "", err
	}
	if strings.EqualFold(strings.TrimSpace(payload.Status), "fail") {
		if strings.TrimSpace(payload.Message) != "" {
			return "", fmt.Errorf("ip-api.com: %s", strings.TrimSpace(payload.Message))
		}
		return "", errGeoIPCountryMissing{}
	}
	return payload.CountryCode, nil
}

func decodePlainCountryCode(r io.Reader) (string, error) {
	raw, err := io.ReadAll(io.LimitReader(r, 128))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func normalizeCountryCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 2 {
		return ""
	}
	for _, ch := range value {
		if ch < 'a' || ch > 'z' {
			return ""
		}
	}
	return value
}

type errGeoIPCountryMissing struct{}

func (errGeoIPCountryMissing) Error() string { return "country_code missing" }
