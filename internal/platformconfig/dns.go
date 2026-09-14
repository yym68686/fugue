package platformconfig

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/miekg/dns"
)

// ValidateDNSIntents proves that every record can be represented on the wire.
// Symbolic application/flatten targets must be resolved before compilation;
// accepting their names as RR types would silently drop answers in consumers.
func ValidateDNSIntents(records []DNSIntent) error {
	if len(records) > 10000 {
		return fmt.Errorf("too many DNS records")
	}
	seen := make(map[string]bool, len(records))
	typesByHost := make(map[string]map[string]bool, len(records))
	for _, record := range records {
		if record.Flatten != nil {
			return fmt.Errorf("DNS flatten policy requires a resolver before compilation")
		}
		if record.Hostname == "" || record.Hostname != normalizedImportHostname(record.Hostname) || strings.ContainsAny(record.Hostname, " \t\r\n") {
			return fmt.Errorf("DNS record requires a canonical hostname")
		}
		if _, valid := dns.IsDomainName(dns.Fqdn(record.Hostname)); !valid {
			return fmt.Errorf("DNS record hostname is invalid")
		}
		if record.TTL < 1 || record.TTL > 2147483647 || len(record.Values) == 0 || len(record.Values) > 4096 {
			return fmt.Errorf("DNS record TTL or value count is invalid")
		}
		if record.Status != "" && !validImportedStatus(record.Status) {
			return fmt.Errorf("DNS record status is invalid")
		}
		key := record.Hostname + "\x00" + record.Type
		if seen[key] {
			return fmt.Errorf("duplicate DNS RRset: %s %s", record.Hostname, record.Type)
		}
		seen[key] = true
		if typesByHost[record.Hostname] == nil {
			typesByHost[record.Hostname] = map[string]bool{}
		}
		typesByHost[record.Hostname][record.Type] = true
		if record.Type == "CNAME" && len(record.Values) != 1 {
			return fmt.Errorf("CNAME requires exactly one target")
		}
		for _, value := range record.Values {
			if strings.TrimSpace(value) == "" || len(value) > 65500 {
				return fmt.Errorf("DNS value is empty or too large")
			}
			var rr dns.RR
			var err error
			header := dns.RR_Header{Name: dns.Fqdn(record.Hostname), Class: dns.ClassINET, Ttl: uint32(record.TTL)}
			switch record.Type {
			case "TXT":
				chunks := []string{}
				for remaining := value; len(remaining) > 0; {
					size := min(255, len(remaining))
					chunks = append(chunks, remaining[:size])
					remaining = remaining[size:]
				}
				header.Rrtype = dns.TypeTXT
				rr = &dns.TXT{Hdr: header, Txt: chunks}
			case "A", "AAAA":
				ip := net.ParseIP(value)
				if ip == nil || (record.Type == "A") != (ip.To4() != nil) {
					return fmt.Errorf("invalid %s DNS address", record.Type)
				}
				if record.Type == "A" {
					header.Rrtype = dns.TypeA
					rr = &dns.A{Hdr: header, A: ip.To4()}
				} else {
					header.Rrtype = dns.TypeAAAA
					rr = &dns.AAAA{Hdr: header, AAAA: ip}
				}
			case "CNAME", "NS", "MX", "SRV", "CAA":
				if strings.ContainsAny(value, "\r\n") {
					return fmt.Errorf("DNS value contains multiple records")
				}
				rr, err = dns.NewRR(dns.Fqdn(record.Hostname) + " " + strconv.Itoa(record.TTL) + " IN " + record.Type + " " + value)
				if err != nil {
					return fmt.Errorf("DNS %s value cannot be decoded", record.Type)
				}
			default:
				return fmt.Errorf("DNS type %q requires a supported resolver before compilation", record.Type)
			}
			msg := new(dns.Msg)
			msg.Answer = []dns.RR{rr}
			if _, err := msg.Pack(); err != nil {
				return fmt.Errorf("DNS record cannot be encoded: %w", err)
			}
		}
	}
	for host, kinds := range typesByHost {
		if kinds["CNAME"] && len(kinds) > 1 {
			return fmt.Errorf("CNAME conflicts with another RRset at %s", host)
		}
	}
	return nil
}

// TXT values are byte strings: leading/trailing spaces are meaningful.
// Keep invalid empty inputs visible to validation instead of dropping them.
func normalizeDNSValues(kind string, values []string) []string {
	if values == nil {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		if kind != "TXT" {
			value = strings.TrimSpace(value)
		}
		if !seen[value] {
			out = append(out, value)
			seen[value] = true
		}
	}
	sort.Strings(out)
	return out
}
