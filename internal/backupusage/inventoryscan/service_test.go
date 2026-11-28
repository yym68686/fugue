package inventoryscan

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupusage"
	"fugue/internal/model"
)

type memoryStore struct {
	backend model.BackupBackend
	pages   Pages
	legacy  []byte
	key     string
	updates int
}

func (m *memoryStore) Backends() ([]model.BackupBackend, error) {
	return []model.BackupBackend{m.backend}, nil
}
func (m *memoryStore) BackendForInventory(id string) (model.BackupBackend, error) {
	if id != m.backend.ID {
		return model.BackupBackend{}, fmt.Errorf("unexpected inventory backend %q", id)
	}
	return m.backend, nil
}
func (*memoryStore) Artifacts() ([]model.BackupArtifact, error) { return nil, nil }
func (m *memoryStore) ReadPages(_ context.Context, key string) (Pages, []byte, error) {
	if key != m.key {
		return Pages{}, nil, fmt.Errorf("unexpected checkpoint key %q", key)
	}
	return m.pages, m.legacy, nil
}
func (m *memoryStore) UpdatePages(_ context.Context, key string, update func(Pages, []byte) (Pages, error), _ func([]byte) ([]byte, error)) (bool, error) {
	if key != m.key {
		return false, fmt.Errorf("unexpected checkpoint key %q", key)
	}
	pages, err := update(m.pages, m.legacy)
	if err != nil || pages.Metadata == nil {
		return false, err
	}
	m.pages = pages
	m.legacy = nil
	m.updates++
	return true, nil
}

func fixtureStore() (*memoryStore, backupusage.Namespace) {
	b := model.BackupBackend{
		ID: "storage", Provider: model.DataBackendProviderCloudflareR2,
		TenantID: "a", Endpoint: "https://objects.test", Bucket: "backups", Prefix: "backup",
	}
	return &memoryStore{backend: b, key: checkpointKey([]model.BackupBackend{b})},
		backupusage.NamespaceOf(model.BackupBackendAsDataBackend(b))
}

func TestInventoryResumesLegacyCursorAndPublishesDetachedView(t *testing.T) {
	db, nm := fixtureStore()
	started := time.Now().UTC().Add(-time.Hour)
	raw, err := json.Marshal(started)
	if err != nil {
		t.Fatal(err)
	}
	// Use the v1 fields directly so the test can catch a schema-changing move.
	db.legacy = fmt.Appendf(nil, `{"Version":1,"Current":{"Generation":"legacy-gen","StartedAt":%s,"Cursor":"legacy-cursor","LastKey":"backup/apps/a/01","Pages":1,"Objects":1,"Wanted":{"backup/apps/a/01":true},"References":{"backup/apps/a/01":{"Key":"backup/apps/a/01","Size":7}},"Totals":{"all":{"Count":1,"Bytes":7},"tenant:a":{"Count":1,"Bytes":7}}}}`, raw)
	var lists int
	svc := New(db, func(model.BackupBackend) (ObjectReader, error) {
		return ObjectReader{Namespace: nm, ListPage: func(_ context.Context, cursor string) (Page, error) {
			lists++
			if cursor != "legacy-cursor" {
				return Page{}, fmt.Errorf("wrong cursor %q", cursor)
			}
			return Page{Objects: []backupusage.InventoryObject{
				{Key: "backup/apps/b/02", Size: 11},
				{Key: "backup-other/apps/a/03", Size: 1000},
				{Key: "backup/apps/a/01", Size: 7},
			}}, nil
		}}, nil
	}, nil, Config{}, 0)
	before, err := svc.Read(t.Context(), nm.Key())
	if err != nil || lists != 0 || db.updates != 0 || before.Current == nil || before.Complete != nil {
		t.Fatalf("read ran or advanced scan: %+v, %v", before, err)
	}
	groups, err := svc.groups()
	if err != nil {
		t.Fatal(err)
	}
	due, err := svc.advance(t.Context(), groups[nm.Key()])
	if err != nil || time.Until(due) < 14*time.Minute || db.updates != 1 || lists != 1 {
		t.Fatalf("finishing legacy scan: due %s, writes %d, lists %d, error %v", due, db.updates, lists, err)
	}
	after, err := svc.Read(t.Context(), nm.Key())
	if err != nil || after.Current != nil || after.Complete == nil {
		t.Fatalf("read complete scan: %+v, %v", after, err)
	}
	view := after.Complete
	if view.Generation != "legacy-gen" || view.Pages != 2 || view.Totals["all"].Bytes != 18 ||
		view.Totals["tenant:a"].Bytes != 7 || view.Totals["tenant:b"].Bytes != 11 {
		t.Fatalf("checkpoint or ownership changed: %+v", view)
	}
	view.Totals["all"] = backupusage.InventoryTotals{Bytes: 1}
	after, err = svc.Read(t.Context(), nm.Key())
	if err != nil || after.Complete == nil || after.Complete.Totals["all"].Bytes != 18 ||
		db.updates != 1 || lists != 1 {
		t.Fatalf("a read changed durable checkpoint: %+v, %v", after, err)
	}
}

func TestInventoryFailsClosedOnInvalidPagination(t *testing.T) {
	db, nm := fixtureStore()
	svc := New(db, func(model.BackupBackend) (ObjectReader, error) {
		return ObjectReader{Namespace: nm, ListPage: func(context.Context, string) (Page, error) {
			return Page{More: true}, nil
		}}, nil
	}, nil, Config{}, 0)
	if err := svc.Advance(t.Context()); err != nil {
		t.Fatal(err)
	}
	view, err := svc.Read(t.Context(), nm.Key())
	if err != nil || view.Current == nil || view.Complete != nil || view.Current.Pages != 0 || view.Error != "invalid_pagination" {
		t.Fatalf("invalid pagination became a successful scan: %+v, %v", view, err)
	}
	if !strings.Contains(string(db.pages.Metadata), "invalid_pagination") {
		t.Fatal("pagination failure was not persisted")
	}
}
