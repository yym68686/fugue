package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"fugue/internal/model"
)

const (
	staticEdgeDNSProviderCloudflare = "cloudflare"
	staticEdgeDNSProviderFugue      = "fugue"
)

type staticEdgeDNSBackend interface {
	ensureZoneID(context.Context, string) (string, error)
	staticEdgeA(context.Context, string, string) (staticEdgeDNSRecord, error)
	staticEdgePatch(context.Context, string, staticEdgeDNSRecord, string) error
}

func staticEdgeDNSProvider(o staticEdgeCutoverOptions) string {
	if strings.TrimSpace(o.DNSProvider) == "" {
		return staticEdgeDNSProviderCloudflare
	}
	return strings.ToLower(strings.TrimSpace(o.DNSProvider))
}

func (cli *CLI) newStaticEdgeDNSBackend(o staticEdgeCutoverOptions) (staticEdgeDNSBackend, error) {
	switch staticEdgeDNSProvider(o) {
	case staticEdgeDNSProviderCloudflare:
		client, err := newStaticEdgeCloudflareClient(o.Zone, o.Timeout)
		if err != nil {
			return nil, err
		}
		client.zoneID = o.ZoneID
		return client, nil
	case staticEdgeDNSProviderFugue:
		client, err := cli.newClient()
		if err != nil {
			return nil, err
		}
		if o.Timeout > 0 {
			client.httpClient.Timeout = o.Timeout
		}
		return &fugueHostedDNSClient{client: client}, nil
	default:
		return nil, fmt.Errorf("unsupported DNS provider %q; use %q or %q", o.DNSProvider, staticEdgeDNSProviderCloudflare, staticEdgeDNSProviderFugue)
	}
}

type fugueHostedDNSClient struct {
	client              *Client
	zoneName            string
	zoneStatus          string
	delegationStatus    string
	publicCutoverReady  bool
	expectedNameservers []string
}

func (x *fugueHostedDNSClient) ensureZoneID(ctx context.Context, zoneName string) (string, error) {
	client := x.withContext(ctx)
	zone, err := client.GetHostedDNSZone(zoneName)
	if err != nil {
		return "", err
	}
	if normalizeDNSName(zone.ZoneName) != normalizeDNSName(zoneName) {
		return "", errors.New("hosted DNS API returned a different zone")
	}
	x.expectedNameservers = append([]string(nil), zone.ExpectedNameservers...)
	x.zoneName = normalizeDNSName(zoneName)
	x.zoneStatus = zone.Status
	x.delegationStatus = zone.DelegationStatus
	x.publicCutoverReady = zone.Status == model.HostedZoneStatusActive && zone.DelegationStatus == model.HostedZoneDelegationStatusReady
	if zone.ID == "" {
		return "", errors.New("hosted DNS zone has no ID")
	}
	return zone.ID, nil
}

func (x *fugueHostedDNSClient) requirePublicCutoverReady() error {
	if x.publicCutoverReady {
		if len(x.expectedNameservers) == 0 || len(x.expectedNameservers) > 16 {
			return errors.New("hosted DNS requires 1-16 expected authoritative nameservers")
		}
		for _, ns := range x.expectedNameservers {
			if err := validateStaticEdgeZone(strings.TrimSuffix(strings.TrimSpace(ns), ".")); err != nil {
				return fmt.Errorf("invalid authoritative nameserver %q: %w", ns, err)
			}
		}
		return nil
	}
	return fmt.Errorf("hosted DNS zone %s is not ready for public cutover (status=%s delegation=%s)", x.zoneName, x.zoneStatus, x.delegationStatus)
}

func hostedDNSRecordMatches(record model.DNSRecord, zone, host string) bool {
	host = normalizeDNSName(host)
	zone = normalizeDNSName(zone)
	for _, candidate := range []string{record.FQDN, record.Name} {
		candidate = normalizeDNSName(candidate)
		if candidate == host {
			return true
		}
		if candidate == "@" && host == zone {
			return true
		}
		if candidate != "" && candidate != "@" && !strings.Contains(candidate, ".") && candidate+"."+zone == host {
			return true
		}
	}
	return false
}

func hostedDNSRoutingConflictType(recordType string) bool {
	switch strings.ToUpper(strings.TrimSpace(recordType)) {
	case model.DNSRecordTypeAAAA, model.DNSRecordTypeCNAME, model.DNSRecordTypeALIAS, model.DNSRecordTypeANAME, model.DNSRecordTypeFUGUEAPP, "HTTPS", "SVCB":
		return true
	default:
		return false
	}
}

func hostedDNSRecordToStatic(record model.DNSRecord) (staticEdgeDNSRecord, error) {
	if record.ID == "" || normalizeDNSName(record.FQDN) == "" {
		return nil, errors.New("hosted DNS record is missing identity")
	}
	if record.Type != model.DNSRecordTypeA {
		return nil, fmt.Errorf("%s has %s; refusing partial routing cutover", record.FQDN, record.Type)
	}
	if len(record.Values) != 1 || net.ParseIP(record.Values[0]) == nil || net.ParseIP(record.Values[0]).To4() == nil {
		return nil, fmt.Errorf("%s must have exactly one IPv4 A value", record.FQDN)
	}
	if record.Status != "" && record.Status != model.DNSRecordStatusActive {
		return nil, fmt.Errorf("%s is not active (status=%s)", record.FQDN, record.Status)
	}
	if mode := strings.TrimSpace(record.FlattenMode); mode != "" && mode != model.DNSRecordFlattenModeNone {
		return nil, fmt.Errorf("%s has flattening enabled; refusing partial routing cutover", record.FQDN)
	}
	values := append([]string(nil), record.Values...)
	return staticEdgeDNSRecord{
		"id":           mustJSON(record.ID),
		"name":         mustJSON(record.FQDN),
		"type":         mustJSON(record.Type),
		"content":      mustJSON(record.Values[0]),
		"values":       mustJSON(values),
		"ttl":          mustJSON(record.TTL),
		"status":       mustJSON(record.Status),
		"source":       mustJSON(record.Source),
		"flatten_mode": mustJSON(record.FlattenMode),
	}, nil
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func (x *fugueHostedDNSClient) staticEdgeA(ctx context.Context, _ string, host string) (staticEdgeDNSRecord, error) {
	zoneName := x.zoneName
	if zoneName == "" {
		return nil, errors.New("hosted DNS zone is not initialized")
	}
	client := x.withContext(ctx)
	records, err := client.ListHostedDNSRecords(zoneName)
	if err != nil {
		return nil, err
	}
	var found model.DNSRecord
	matches := 0
	for _, record := range records {
		if !hostedDNSRecordMatches(record, zoneName, host) {
			continue
		}
		if record.Type != model.DNSRecordTypeA {
			if hostedDNSRoutingConflictType(record.Type) {
				return nil, fmt.Errorf("%s has %s; refusing partial routing cutover", host, record.Type)
			}
			continue
		}
		matches++
		if found.ID != "" {
			return nil, fmt.Errorf("%s has multiple A records", host)
		}
		found = record
	}
	if matches == 0 || found.ID == "" {
		return nil, fmt.Errorf("%s must have exactly one A record", host)
	}
	return hostedDNSRecordToStatic(found)
}

func (x *fugueHostedDNSClient) staticEdgePatch(ctx context.Context, _ string, old staticEdgeDNSRecord, ip string) error {
	if x.zoneName == "" {
		return errors.New("hosted DNS zone is not initialized")
	}
	id := old.str("id")
	if id == "" {
		return errors.New("hosted DNS record has no ID")
	}
	client := x.withContext(ctx)
	got, err := client.PatchHostedDNSRecord(x.zoneName, id, patchHostedDNSRecordClientRequest{Values: []string{ip}})
	if err != nil {
		return err
	}
	actual, err := hostedDNSRecordToStatic(got)
	if err != nil {
		return err
	}
	if !staticEdgeRecordEqual(actual, old.withIP(ip)) {
		return errors.New("PATCH response differs from expected hosted DNS record")
	}
	return nil
}

func (x *fugueHostedDNSClient) staticEdgeAuthoritativeNS() []string {
	return append([]string(nil), x.expectedNameservers...)
}

// Keep per-operation cancellation without mutating a shared client's context.
func (x *fugueHostedDNSClient) withContext(ctx context.Context) *Client {
	client := *x.client
	if ctx != nil {
		client.context = ctx
	}
	return &client
}
