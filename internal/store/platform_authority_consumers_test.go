package store

import (
	"testing"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

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
