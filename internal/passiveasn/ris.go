// Package passiveasn handles local, conservative BGP origin attribution.
package passiveasn

import (
	"bufio"
	"cmp"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	RISIPv4URL   = "https://www.ris.ripe.net/dumps/riswhoisdump.IPv4.gz"
	RISIPv6URL   = "https://www.ris.ripe.net/dumps/riswhoisdump.IPv6.gz"
	MaxSourceAge = 48 * time.Hour
	// Inconsistent origins, including a minority origin, are barriers rather
	// than votes. A sparse single-origin announcement is also not trusted.
	minPeers = 5
	maxRows  = 3_000_000
)

type origin struct {
	Prefix netip.Prefix
	ASN    uint32
}

// Index retains all known subroutes, including ambiguous/low-visibility
// routes. Dropping those rows could falsely classify an enclosing ECS prefix.
type Index struct {
	v4, v6   []origin
	SourceAt time.Time
}

func (i *Index) Lookup(prefix netip.Prefix) uint32 {
	if i == nil || !prefix.IsValid() || prefix != prefix.Masked() || prefix.Bits() == 0 ||
		prefix.Addr().Is4In6() || !prefix.Addr().IsGlobalUnicast() || prefix.Addr().IsPrivate() {
		return 0
	}
	rows := i.v6
	if prefix.Addr().Is4() {
		rows = i.v4
	}
	start, _ := slices.BinarySearchFunc(rows, prefix, compareOriginAddress)
	asn := uint32(0)
	// A containing route must cover all addresses before we examine more
	// specifics; an isolated subordinate announcement cannot fill a gap.
	for bits := prefix.Bits(); bits > 0; bits-- {
		parent := netip.PrefixFrom(prefix.Addr(), bits).Masked()
		position, found := slices.BinarySearchFunc(rows, parent, compareOriginPrefix)
		if found {
			asn = rows[position].ASN
			break
		}
	}
	if asn == 0 {
		return 0
	}
	const maxSubroutes = 512
	for j := start; j < len(rows) && prefix.Contains(rows[j].Prefix.Addr()); j++ {
		if j-start >= maxSubroutes || rows[j].Prefix.Bits() > prefix.Bits() && rows[j].ASN != asn {
			return 0
		}
	}
	return asn
}

func compareOriginAddress(a origin, b netip.Prefix) int {
	return a.Prefix.Addr().Compare(b.Addr())
}

func compareOriginPrefix(a origin, b netip.Prefix) int {
	if c := compareOriginAddress(a, b); c != 0 {
		return c
	}
	return cmp.Compare(a.Prefix.Bits(), b.Bits())
}

func parseRIS(reader io.Reader, v4 bool, rowsLimit int) ([]origin, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 4096)
	var rows []origin
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line[0] == '%' {
			continue
		}
		originText, tail, ok := strings.Cut(line, "\t")
		prefixText, peersText, rest := strings.Cut(tail, "\t")
		prefix, pErr := netip.ParsePrefix(prefixText)
		asn, aErr := risOrigin(originText)
		peers, peerErr := strconv.ParseUint(peersText, 10, 16)
		if !ok || !rest || pErr != nil || aErr != nil || peerErr != nil ||
			prefix != prefix.Masked() || prefix.Addr().Is4() != v4 {
			return nil, fmt.Errorf("malformed RIS dump row %d (family4=%t; prefix=%t origin=%t peers=%t)", lineNumber, v4, pErr == nil && prefix == prefix.Masked() && prefix.Addr().Is4() == v4, aErr == nil, peerErr == nil)
		}
		if prefix.Bits() == 0 || prefix.Addr().Is4In6() || !prefix.Addr().IsGlobalUnicast() || prefix.Addr().IsPrivate() {
			continue
		}
		if peers < minPeers || asn == 23456 || asn >= 64496 && asn <= 131071 || asn >= 4200000000 {
			asn = 0
		}
		if len(rows) == rowsLimit {
			return nil, errors.New("RIS row limit exceeded")
		}
		rows = append(rows, origin{prefix, uint32(asn)})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(rows, func(a, b origin) int { return compareOriginPrefix(a, b.Prefix) })
	n := 0
	for _, row := range rows {
		if n > 0 && rows[n-1].Prefix == row.Prefix {
			if rows[n-1].ASN != row.ASN {
				rows[n-1].ASN = 0
			}
			continue
		}
		rows[n], n = row, n+1
	}
	return rows[:n], nil
}

func risOrigin(raw string) (uint64, error) {
	if strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}") {
		for value := range strings.SplitSeq(raw[1:len(raw)-1], ",") {
			if _, err := strconv.ParseUint(value, 10, 32); err != nil {
				return 0, err
			}
		}
		return 0, nil // AS_SET never authenticates one origin.
	}
	return strconv.ParseUint(raw, 10, 32)
}

// FetchRIS downloads only public bulk dumps. No client IP, prefix, hostname,
// or credential leaves Fugue. The caller publishes the index atomically and
// retains its previous index on a failed refresh.
func FetchRIS(ctx context.Context, now time.Time) (*Index, error) {
	return fetchRIS(ctx, now, RISIPv4URL, RISIPv6URL)
}

func fetchRIS(ctx context.Context, now time.Time, v4URL, v6URL string) (*Index, error) {
	httpClient := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       30 * time.Second,
	}
	read := func(url string, v4 bool) ([]origin, time.Time, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, time.Time{}, err
		}
		req.Header.Set("Accept-Encoding", "identity")
		res, err := httpClient.Do(req)
		if err != nil {
			return nil, time.Time{}, err
		}
		defer res.Body.Close()
		at, err := http.ParseTime(res.Header.Get("Last-Modified"))
		const maxCompressed = 32 << 20
		const maxPlain = 128 << 20
		if res.StatusCode != http.StatusOK || err != nil || at.After(now.Add(5*time.Minute)) || now.Sub(at) > MaxSourceAge ||
			res.ContentLength > maxCompressed {
			return nil, time.Time{}, errors.New("RIS dump status, size or timestamp invalid")
		}
		compressed := &io.LimitedReader{R: res.Body, N: maxCompressed + 1}
		decoded, err := gzip.NewReader(compressed)
		if err != nil {
			return nil, time.Time{}, err
		}
		defer decoded.Close()
		plain := &io.LimitedReader{R: decoded, N: maxPlain + 1}
		rows, err := parseRIS(plain, v4, maxRows)
		minimum := 50_000
		if v4 {
			minimum = 300_000
		}
		if err != nil || len(rows) < minimum || compressed.N <= 0 || plain.N <= 0 {
			return nil, time.Time{}, fmt.Errorf("RIS dump parse or completeness check failed: %v", err)
		}
		return rows, at.UTC(), nil
	}
	v4, at4, err := read(v4URL, true)
	if err != nil {
		return nil, err
	}
	v6, at6, err := read(v6URL, false)
	if err != nil {
		return nil, err
	}
	at := at4
	if at6.Before(at) {
		at = at6
	}
	return &Index{v4: v4, v6: v6, SourceAt: at}, nil
}
