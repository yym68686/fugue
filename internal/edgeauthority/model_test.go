package edgeauthority

import (
	"strings"
	"testing"
	"time"
)

func TestRouteBundleRecordSealsImmutableIdentity(t *testing.T) {
	record, err := (RouteBundleRecord{
		GroupID: "edge-group-country-de", Epoch: 7,
		BundleDigest: "sha256:" + strings.Repeat("1", 64), SourceSHA: strings.Repeat("2", 40),
		ControlImageDigest: "sha256:" + strings.Repeat("3", 64), InventoryDigest: "sha256:" + strings.Repeat("4", 64),
		ManifestDigest: "sha256:" + strings.Repeat("5", 64), HealthContractDigest: "sha256:" + strings.Repeat("6", 64),
		IssuedAt: time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), KeyID: "edge-signing-v1",
		Signature: strings.Repeat("A", 43),
	}).Seal()
	if err != nil || record.Validate() != nil {
		t.Fatalf("sealed record is invalid: record=%+v err=%v", record, err)
	}
	drifted := record
	drifted.ControlImageDigest = "sha256:" + strings.Repeat("7", 64)
	if drifted.Validate() == nil {
		t.Fatal("route bundle record digest survived OCI drift")
	}
}
