package passiveasn

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"time"
)

const googleResolverSourcesURL = "https://www.gstatic.com/ipranges/publicdns.json"

type ResolverSources struct {
	PublishedAt time.Time
	Prefixes    []netip.Prefix
}

// FetchResolverSources recognizes only the vendor-published authoritative
// egress CIDRs, not inbound anycast resolver addresses or all of its ASN.
func FetchResolverSources(ctx context.Context, now time.Time) (*ResolverSources, error) {
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Timeout: 12 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, googleResolverSourcesURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("resolver source download failed")
	}
	return parseResolverSources(resp.Body, now)
}

func parseResolverSources(source io.Reader, now time.Time) (*ResolverSources, error) {
	raw, err := io.ReadAll(io.LimitReader(source, (512<<10)+1))
	var body struct {
		CreationTime time.Time `json:"creationTime"`
		Prefixes     []struct {
			IPv4    string `json:"ipv4Prefix"`
			IPv6    string `json:"ipv6Prefix"`
			Service string `json:"service"`
		} `json:"prefixes"`
	}
	if err != nil || len(raw) > 512<<10 || json.Unmarshal(raw, &body) != nil ||
		body.CreationTime.After(now.Add(time.Minute)) || now.Sub(body.CreationTime) > 8*24*time.Hour ||
		len(body.Prefixes) < 1 || len(body.Prefixes) > MaxResolvers {
		return nil, errors.New("resolver sources outside bounds")
	}
	result := &ResolverSources{PublishedAt: body.CreationTime}
	seen := map[netip.Prefix]bool{}
	for _, row := range body.Prefixes {
		ipv4 := row.IPv4 != ""
		text := row.IPv6
		if ipv4 {
			text = row.IPv4
		}
		p, err := netip.ParsePrefix(text)
		if err != nil || ipv4 && row.IPv6 != "" || p.Addr().Is4() != ipv4 || !validPrefix(p) ||
			p.Bits() < cohortBits(p.Addr()) || row.Service != "Google Public DNS" || seen[p] {
			return nil, errors.New("invalid resolver source prefix")
		}
		seen[p] = true
		result.Prefixes = append(result.Prefixes, p)
	}
	return result, nil
}
