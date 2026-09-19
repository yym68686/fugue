package store

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestObservationQueriesHonorCallerBudget(t *testing.T) {
	tests := []struct {
		name, query string
		read        func(context.Context, *Store) error
	}{
		{"policies", `SELECT .* FROM fugue_app_traffic_policies`, func(ctx context.Context, s *Store) error {
			_, err := s.ListAppTrafficPoliciesContext(ctx, "", true)
			return err
		}},
		{"releases", `SELECT .* FROM fugue_app_releases`, func(ctx context.Context, s *Store) error {
			_, err := s.ListAppReleaseMetadataContext(ctx, model.AppReleaseFilter{PlatformAdmin: true})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := &Store{db: db, databaseURL: "postgres://example", dbReady: true}
			mock.ExpectQuery(tt.query).WillDelayFor(time.Second).WillReturnRows(sqlmock.NewRows([]string{"unused"}))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			start := time.Now()
			if err := tt.read(ctx, s); err == nil {
				t.Fatal("cancelled query succeeded")
			}
			if time.Since(start) > 500*time.Millisecond {
				t.Fatal("query ignored caller budget")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
