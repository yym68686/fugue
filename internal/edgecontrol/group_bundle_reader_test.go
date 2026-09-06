package edgecontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestLoadGroupBundleReaderKeyringAllowsSharedTokenAcrossEdges(t *testing.T) {
	now := time.Unix(1786136000, 0).UTC()
	digest := sha256.Sum256([]byte("shared-reader-token"))
	keyring := groupBundleReaderKeyring{
		Schema:     GroupBundleReaderKeyringSchemaV1,
		Generation: 1,
		GroupID:    "edge-group-country-us",
		Credentials: []groupBundleReaderCredential{
			{CredentialID: "reader-us-vps", EdgeID: "vps-591f4447", TokenDigest: "sha256:" + hex.EncodeToString(digest[:]), NotBeforeUnix: now.Add(-time.Minute).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()},
			{CredentialID: "reader-us-bwg", EdgeID: "bwg", TokenDigest: "sha256:" + hex.EncodeToString(digest[:]), NotBeforeUnix: now.Add(-time.Minute).Unix(), NotAfterUnix: now.Add(time.Hour).Unix()},
		},
	}
	path := t.TempDir() + "/reader.json"
	writePrivateJSONFixture(t, path, keyring)
	if _, err := loadGroupBundleReaderKeyring(path, keyring.GroupID); err != nil {
		t.Fatalf("shared reader token must be valid for multiple edge IDs: %v", err)
	}
	if err := authenticateGroupBundleReader(path, keyring.GroupID, "bwg", "shared-reader-token", now); err != nil {
		t.Fatalf("shared reader token must authenticate bwg: %v", err)
	}
}
