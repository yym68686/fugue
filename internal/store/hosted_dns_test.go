package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestHostedDNSFlattenObservationFencesConcurrentIntent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newHostedDNSTestStore(t)
	installHostedDNSWorkerFunctions(t, s)
	zone := putHostedDNSTestZone(t, s, createHostedDNSTestTenant(t, s, "dns-owner"), "example.com")
	record := putHostedDNSTestRecord(t, s, zone, model.DNSRecord{
		Name: "@", Type: model.DNSRecordTypeALIAS, Values: []string{"old.example.net"},
		TTL: 300, Source: model.DNSRecordSourceUser, Status: model.DNSRecordStatusActive,
	})
	now := time.Now().UTC()
	result := record
	result.FlattenStatus = model.DNSRecordFlattenStatusResolved
	result.FlattenedA = []string{"1.1.1.1"}
	result.LastResolvedAt = &now

	changedIntent := record
	changedIntent.Values = []string{"new.example.net"}
	changedIntent.FlattenTarget = "new.example.net"
	changedIntent.TTL = 420
	changedIntent, err := s.PutDNSRecord(zone, changedIntent, true)
	if err != nil {
		t.Fatal(err)
	}
	// The exact current revision accepts derived answers without receiving
	// authority to apply any user fields copied into the observation.
	result.Values = []string{"other.example.net"}
	result.TTL = 300
	commit := func(revision model.DNSRecord) (bool, error) {
		var changed bool
		err := s.db.QueryRowContext(ctx, `SELECT fugue_hosted_dns_maintenance.commit_v1(
$1,$2,$3,$4,$5,$6,'["1.1.1.1"]'::jsonb,'[]'::jsonb,$7,$8,$9,'','')`,
			zone.ID, zone.TenantID, zone.ProjectID, revision.ID, revision.CreatedAt, revision.UpdatedAt,
			result.FlattenStatus, result.Status, result.LastResolvedAt).Scan(&changed)
		return changed, err
	}
	if changed, err := commit(changedIntent); err != nil || changed {
		t.Fatalf("standby worker wrote an answer: changed=%t err=%v", changed, err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_meta (key,value) VALUES('hosted-dns-flatten/v1/writer','worker')`); err != nil {
		t.Fatal(err)
	}
	if changed, err := commit(record); err != nil || changed {
		t.Fatalf("stale answer changed current intent: changed=%t err=%v", changed, err)
	}
	if changed, err := commit(changedIntent); err != nil || !changed {
		t.Fatalf("current answer did not commit: changed=%t err=%v", changed, err)
	}
	current, err := s.GetDNSRecord(zone.ID, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.TTL != 420 || current.FlattenTarget != "new.example.net" ||
		len(current.FlattenedA) != 1 || current.FlattenedA[0] != "1.1.1.1" ||
		!current.UpdatedAt.After(changedIntent.UpdatedAt) {
		t.Fatalf("wrong persisted intent or answer: %+v", current)
	}
	if changed, err := commit(changedIntent); err != nil || changed {
		t.Fatalf("competing observation reused a consumed revision: changed=%t err=%v", changed, err)
	}
	suspended := zone
	suspended.Status = model.HostedZoneStatusSuspended
	suspended, err = s.UpdateHostedZoneStatus(suspended)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := commit(current); err != nil || changed {
		t.Fatalf("suspended zone accepted an answer: changed=%t err=%v", changed, err)
	}
	suspended.Status = zone.Status
	if _, err := s.UpdateHostedZoneStatus(suspended); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteDNSRecord(zone.ID, record.ID); err != nil {
		t.Fatal(err)
	}
	if changed, err := commit(current); err != nil || changed {
		t.Fatalf("observation recreated a deleted record: changed=%t err=%v", changed, err)
	}
}

func TestHostedDNSZonesAreTenantScoped(t *testing.T) {
	t.Parallel()

	s := newHostedDNSTestStore(t)
	a := createHostedDNSTestTenant(t, s, "tenant-a")
	b := createHostedDNSTestTenant(t, s, "tenant-b")
	putHostedDNSTestZone(t, s, a, "example.com")
	putHostedDNSTestZone(t, s, b, "example.net")

	zones, err := s.ListHostedZones(a, false)
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

func TestHostedDNSPatchKeepsObservationCommittedWhileWaitingForZone(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	s := newHostedDNSTestStore(t)
	zone := putHostedDNSTestZone(t, s, createHostedDNSTestTenant(t, s, "dns-patch"), "example.com")
	record := putHostedDNSTestRecord(t, s, zone, model.DNSRecord{
		Name: "@", Type: model.DNSRecordTypeALIAS, Values: []string{"target.example.net"},
		TTL: 300, Source: model.DNSRecordSourceUser, Status: model.DNSRecordStatusActive,
	})
	worker, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Rollback()
	if _, err := worker.ExecContext(ctx, `SELECT 1 FROM fugue_dns_zones WHERE id=$1 FOR UPDATE`, zone.ID); err != nil {
		t.Fatal(err)
	}
	type patchResult struct {
		record model.DNSRecord
		err    error
	}
	finished := make(chan patchResult, 1)
	go func() {
		ttl := 600
		out, err := s.PatchDNSRecord(zone, record.ID, model.DNSRecordPatch{TTL: &ttl}, false)
		finished <- patchResult{out, err}
	}()
	// A waiter at the zone lock distinguishes the old pre-read from the fixed
	// locked read without relying on a sleep to schedule the request.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%fugue_dns_zones%FOR UPDATE%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("patch did not wait for the zone lock")
		}
	}
	if _, err := worker.ExecContext(ctx, `UPDATE fugue_dns_records SET flattened_a_json='["1.1.1.1"]',
flatten_status='resolved', last_resolved_at=clock_timestamp(), updated_at=clock_timestamp() WHERE id=$1`, record.ID); err != nil {
		t.Fatal(err)
	}
	if err := worker.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-finished:
		if got.err != nil || got.record.TTL != 600 || got.record.FlattenStatus != model.DNSRecordFlattenStatusResolved ||
			len(got.record.FlattenedA) != 1 || got.record.FlattenedA[0] != "1.1.1.1" || got.record.LastResolvedAt == nil {
			t.Fatalf("patch lost the observation or intent: %+v, %v", got.record, got.err)
		}
	case <-ctx.Done():
		t.Fatal("patch did not finish after the observation committed")
	}
}

func TestHostedDNSRecordConflictsProtectSystemRecords(t *testing.T) {
	t.Parallel()

	s := newHostedDNSTestStore(t)
	zone := putHostedDNSTestZone(t, s, createHostedDNSTestTenant(t, s, "tenant-a"), "example.com")
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
	zone := putHostedDNSTestZone(t, s, createHostedDNSTestTenant(t, s, "tenant-a"), "example.com")
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
	zone := putHostedDNSTestZone(t, s, createHostedDNSTestTenant(t, s, "tenant-a"), "example.com")
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

func createHostedDNSTestTenant(t *testing.T, s *Store, name string) string {
	t.Helper()
	tenant, err := s.CreateTenant(name)
	if err != nil {
		t.Fatal(err)
	}
	return tenant.ID
}

func newHostedDNSTestStore(t *testing.T) *Store {
	t.Helper()
	s := newStoreTestDatabase(t, "")
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
