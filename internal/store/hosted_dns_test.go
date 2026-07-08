package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestHostedDNSZonesAreTenantScoped(t *testing.T) {
	t.Parallel()

	s := newHostedDNSTestStore(t)
	putHostedDNSTestZone(t, s, "tenant-a", "example.com")
	putHostedDNSTestZone(t, s, "tenant-b", "example.net")

	zones, err := s.ListHostedZones("tenant-a", false)
	if err != nil {
		t.Fatalf("list tenant hosted zones: %v", err)
	}
	if len(zones) != 1 || zones[0].ZoneName != "example.com" {
		t.Fatalf("expected tenant scoped zones, got %+v", zones)
	}

	allZones, err := s.ListHostedZones("", true)
	if err != nil {
		t.Fatalf("list platform hosted zones: %v", err)
	}
	if len(allZones) != 2 {
		t.Fatalf("expected platform admin to see both zones, got %+v", allZones)
	}
}

func TestHostedDNSRecordConflictsProtectSystemRecords(t *testing.T) {
	t.Parallel()

	s := newHostedDNSTestStore(t)
	zone := putHostedDNSTestZone(t, s, "tenant-a", "example.com")
	putHostedDNSTestRecord(t, s, zone, model.DNSRecord{
		Name:   "@",
		Type:   model.DNSRecordTypeNS,
		Values: []string{"ns1.dns.fugue.pro"},
		Source: model.DNSRecordSourceSystem,
		Status: model.DNSRecordStatusActive,
	})

	_, err := s.PutDNSRecord(zone, model.DNSRecord{
		Name:   "@",
		Type:   model.DNSRecordTypeA,
		Values: []string{"203.0.113.20"},
		Source: model.DNSRecordSourceUser,
		Status: model.DNSRecordStatusActive,
	}, true)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected system record conflict, got %v", err)
	}
}

func TestHostedDNSRecordConflictsRejectApexAddressPublishers(t *testing.T) {
	t.Parallel()

	s := newHostedDNSTestStore(t)
	zone := putHostedDNSTestZone(t, s, "tenant-a", "example.com")
	putHostedDNSTestRecord(t, s, zone, model.DNSRecord{
		Name:   "@",
		Type:   model.DNSRecordTypeA,
		Values: []string{"203.0.113.20"},
		Source: model.DNSRecordSourceUser,
		Status: model.DNSRecordStatusActive,
	})

	_, err := s.PutDNSRecord(zone, model.DNSRecord{
		Name:   "@",
		Type:   model.DNSRecordTypeALIAS,
		Values: []string{"target.example.net"},
		Source: model.DNSRecordSourceUser,
		Status: model.DNSRecordStatusActive,
	}, false)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected apex A/ALIAS conflict, got %v", err)
	}
}

func TestHostedDNSRecordsDeleteBySourceRef(t *testing.T) {
	t.Parallel()

	s := newHostedDNSTestStore(t)
	zone := putHostedDNSTestZone(t, s, "tenant-a", "example.com")
	managed := putHostedDNSTestRecord(t, s, zone, model.DNSRecord{
		Name:          "@",
		Type:          model.DNSRecordTypeFUGUEAPP,
		Values:        []string{"app_123"},
		Source:        model.DNSRecordSourceAppDomain,
		SourceRefType: model.DNSRecordSourceRefTypeAppDomain,
		SourceRefID:   "example.com",
		Status:        model.DNSRecordStatusActive,
	})
	manual := putHostedDNSTestRecord(t, s, zone, model.DNSRecord{
		Name:   "www",
		Type:   model.DNSRecordTypeA,
		Values: []string{"203.0.113.30"},
		Source: model.DNSRecordSourceUser,
		Status: model.DNSRecordStatusActive,
	})

	deleted, err := s.DeleteDNSRecordsBySourceRef(zone.ID, model.DNSRecordSourceAppDomain, model.DNSRecordSourceRefTypeAppDomain, "example.com")
	if err != nil {
		t.Fatalf("delete by source ref: %v", err)
	}
	if len(deleted) != 1 || deleted[0].ID != managed.ID {
		t.Fatalf("expected only managed source ref record to be deleted, got %+v", deleted)
	}
	if _, err := s.GetDNSRecord(zone.ID, manual.ID); err != nil {
		t.Fatalf("manual record must survive source ref delete: %v", err)
	}
}

func newHostedDNSTestStore(t *testing.T) *Store {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	return s
}

func putHostedDNSTestZone(t *testing.T, s *Store, tenantID, zoneName string) model.HostedZone {
	t.Helper()
	now := time.Now().UTC()
	zone, err := s.PutHostedZone(model.HostedZone{
		TenantID:            tenantID,
		ZoneName:            zoneName,
		Status:              model.HostedZoneStatusActive,
		DelegationStatus:    model.HostedZoneDelegationStatusReady,
		ExpectedNameservers: []string{"ns1.dns.fugue.pro", "ns2.dns.fugue.pro"},
		CreatedAt:           now,
		UpdatedAt:           now,
	})
	if err != nil {
		t.Fatalf("put hosted DNS zone %s: %v", zoneName, err)
	}
	return zone
}

func putHostedDNSTestRecord(t *testing.T, s *Store, zone model.HostedZone, record model.DNSRecord) model.DNSRecord {
	t.Helper()
	out, err := s.PutDNSRecord(zone, record, false)
	if err != nil {
		t.Fatalf("put hosted DNS record %s %s: %v", record.Name, record.Type, err)
	}
	return out
}
