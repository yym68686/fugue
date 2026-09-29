package store

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestAuthorityConsumerObservationPreservesScopesAndRejectsTruncation(t *testing.T) {
	path := t.TempDir() + "/state.json"
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	base := model.PlatformConsumerInstance{ConsumerID: "dns-server:node-a", NodeID: "node-a", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "global"}
	if err := s.withLockedState(true, func(state *model.State) error {
		for i, scope := range []string{"global", "authority-cell:cell-a", "authority-cell:cell-b", "tenant:private"} {
			c := base
			c.ID = fmt.Sprintf("fact-%d", i)
			c.ScopeKey = scope
			state.PlatformConsumerInstances = append(state.PlatformConsumerInstances, c)
		}
		c := base
		c.ID = "other-node"
		c.NodeID = "node-b"
		state.PlatformConsumerInstances = append(state.PlatformConsumerInstances, c)
		c.ID = "other-kind"
		c.ArtifactKind = model.PlatformArtifactKindEdgeRouteBundle
		state.PlatformConsumerInstances = append(state.PlatformConsumerInstances, c)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	for _, tc := range []struct {
		node  string
		count int
	}{{"node-a", 3}, {"node-b", 1}, {"", 4}, {"absent", 0}} {
		got, err := s.ListPlatformAuthorityConsumers(base.ArtifactKind, tc.node)
		if err != nil || len(got) != tc.count {
			t.Fatal(tc, got, err)
		}
	}
	after, _ := os.ReadFile(path)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("observation mutated retained facts")
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		state.PlatformConsumerInstances = nil
		for i := 0; i < 65; i++ {
			c := base
			c.ID = fmt.Sprintf("bound-%d", i)
			c.ConsumerID = c.ID
			state.PlatformConsumerInstances = append(state.PlatformConsumerInstances, c)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ListPlatformAuthorityConsumers(base.ArtifactKind, base.NodeID); !errors.Is(err, ErrConflict) || got != nil {
		t.Fatal("truncated authority set returned", len(got), err)
	}
}

func TestPostgresAuthorityConsumerObservationIsBounded(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	for _, tc := range []struct {
		node  string
		limit int
	}{{"node-a", 65}, {"", 4097}} {
		c := model.PlatformConsumerInstance{ConsumerID: "dns-server:cell-a:node-a", NodeID: "node-a", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "authority-cell:cell-a"}
		mock.ExpectQuery(`(?s)WHERE artifact_kind = \$1 AND \(\$2 = '' OR node_id = \$2\).*scope_key = 'global'.*scope_key LIKE 'authority-cell:cell-%'.*LIMIT \$3`).WithArgs(c.ArtifactKind, tc.node, tc.limit).WillReturnRows(platformConsumerRows(t, c))
		got, err := s.ListPlatformAuthorityConsumers(c.ArtifactKind, tc.node)
		if err != nil || len(got) != 1 || got[0].ScopeKey != c.ScopeKey {
			t.Fatal(got, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
