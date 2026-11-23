package platformconfig

import (
	"fugue/internal/model"
	"testing"
)

func TestHostedPublicationIsolationChecksWholeDeclaration(t *testing.T) {
	record := func(name, owner string, ttl int, values ...string) map[string]any {
		return map[string]any{"hostname": name, "type": "A", "values": values, "ttl": ttl, "tenant_id": owner, "record_kind": "hosted"}
	}
	dns := func(v ...any) model.PlatformArtifact {
		return model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Content: map[string]any{"records": v}}
	}
	root := record("root.example.test", "tenant-a", 60, "8.8.8.8")
	other := record("other.example.test", "tenant-b", 60, "8.8.4.4", "9.9.9.9")
	old := dns(root, other)
	for name, a := range map[string]model.PlatformArtifact{
		"order":                    dns(record("other.example.test", "tenant-b", 60, "9.9.9.9", "8.8.4.4"), root),
		"same count replaced name": dns(root, record("new.example.test", "tenant-b", 60, "9.9.9.9", "8.8.4.4")),
		"other TTL":                dns(root, record("other.example.test", "tenant-b", 45, "8.8.4.4", "9.9.9.9")),
		"other owner":              dns(root, record("other.example.test", "tenant-a", 60, "8.8.4.4", "9.9.9.9")),
		"other held address":       dns(root, record("other.example.test", "tenant-b", 60, "8.8.4.4")),
		"orphan":                   dns(root), "duplicate": dns(root, root),
	} {
		if e := ValidateHostedPublicationIsolation(old, a); (e == nil) != (name == "order") {
			t.Fatal(name, e)
		}
	}
}
