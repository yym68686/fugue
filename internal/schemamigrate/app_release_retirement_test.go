package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAppReleaseRetirementFencePostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL for disposable PostgreSQL integration")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires a disposable loopback fugue_test database")
	}
	root, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	schema := fmt.Sprintf("retirement_%d", time.Now().UnixNano())
	if _, err = root.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	defer root.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE fugue_app_releases(id text PRIMARY KEY,app_id text NOT NULL,status text NOT NULL,role text NOT NULL,message text NOT NULL DEFAULT '');
CREATE TABLE fugue_app_traffic_policies(app_id text PRIMARY KEY,stable_release_id text NOT NULL,candidate_release_id text NOT NULL DEFAULT '',stable_weight int NOT NULL DEFAULT 100);
INSERT INTO fugue_app_releases(id,app_id,status,role) VALUES ('old','app','draining','previous'),('stable','app','serving','stable');
INSERT INTO fugue_app_traffic_policies(app_id,stable_release_id) VALUES ('app','stable');`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = MigrateAppReleaseRetirement(context.Background(), u.String()); err != nil {
			t.Fatal(err)
		}
	}
	// Unretired writers and current serving policy remain compatible.
	if _, err = db.Exec(`UPDATE fugue_app_releases SET message='old writer' WHERE id='old';UPDATE fugue_app_traffic_policies SET stable_weight=100 WHERE app_id='app'`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE fugue_app_releases SET status='retired',role='retired' WHERE id='old'`); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`UPDATE fugue_app_releases SET status='serving',role='stable' WHERE id='old'`,
		`UPDATE fugue_app_releases SET message='stale writer' WHERE id='old'`,
		`UPDATE fugue_app_releases SET app_id='other' WHERE id='old'`,
		`UPDATE fugue_app_traffic_policies SET stable_release_id='old' WHERE app_id='app'`,
		`UPDATE fugue_app_traffic_policies SET candidate_release_id='old' WHERE app_id='app'`,
		`INSERT INTO fugue_app_traffic_policies(app_id,stable_release_id) VALUES ('new-app','old')`,
		`UPDATE fugue_app_releases SET status='retired',role='retired' WHERE id='stable'`,
		`UPDATE fugue_app_releases SET status='retired' WHERE id='stable'`,
	} {
		if _, err = db.Exec(query); err == nil {
			t.Fatalf("unsafe old writer accepted: %s", query)
		}
	}
	if err = MigrateAppReleaseRetirement(context.Background(), u.String()); err != nil {
		t.Fatal(err)
	}
	// Race both an existing policy update and its first insertion against
	// retirement. Whichever wins must fence the opposing state transition.
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("race-%d", i)
		app := id + "-app"
		if _, err = db.Exec(`INSERT INTO fugue_app_releases(id,app_id,status,role) VALUES ($1,$2,'draining','previous')`, id, app); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			if _, err = db.Exec(`INSERT INTO fugue_app_traffic_policies(app_id,stable_release_id) VALUES ($1,'stable')`, app); err != nil {
				t.Fatal(err)
			}
		}
		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := db.ExecContext(ctx, `UPDATE fugue_app_releases SET status='retired',role='retired' WHERE id=$1`, id)
			results <- err
		}()
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := db.ExecContext(ctx, `INSERT INTO fugue_app_traffic_policies(app_id,stable_release_id) VALUES ($1,$2) ON CONFLICT (app_id) DO UPDATE SET stable_release_id=EXCLUDED.stable_release_id`, app, id)
			results <- err
		}()
		close(start)
		wg.Wait()
		close(results)
		wins := 0
		for err := range results {
			if err == nil {
				wins++
			}
		}
		if wins != 1 {
			t.Fatalf("competing retirement/reference writes: %d committed", wins)
		}
		var unsafe bool
		if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM fugue_app_releases r JOIN fugue_app_traffic_policies p ON r.id=p.stable_release_id OR r.id=p.candidate_release_id WHERE r.id=$1 AND r.status='retired')`, id).Scan(&unsafe); err != nil || unsafe {
			t.Fatalf("retired traffic reference committed: %v %v", unsafe, err)
		}
	}
}
