package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"
)

func TestAppMailMigrationPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_MAIL_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_MAIL_TEST_DATABASE_URL to a disposable loopback PostgreSQL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" {
		t.Fatal("disposable loopback PostgreSQL required")
	}
	control, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	schema := fmt.Sprintf("mail_schema_%d", time.Now().UnixNano())
	if _, err = control.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	defer control.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE serving(id int PRIMARY KEY, body text);INSERT INTO serving VALUES(1,'retained')`); err != nil {
		t.Fatal(err)
	}
	writer, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err = writer.Exec(`UPDATE serving SET body='still serving' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = MigrateAppMail(context.Background(), u.String()); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO fugue_app_mail_bindings VALUES('app_test',1,'{"state":"revoked"}')`); err != nil {
		t.Fatal(err)
	}
	if err = MigrateAppMail(context.Background(), u.String()); err != nil {
		t.Fatal(err)
	}
	var state, serving string
	if err = db.QueryRow(`SELECT body->>'state' FROM fugue_app_mail_bindings WHERE app_id='app_test'`).Scan(&state); err != nil || state != "revoked" {
		t.Fatal("mail tombstone changed", state, err)
	}
	if err = db.QueryRow(`SELECT body FROM serving WHERE id=1`).Scan(&serving); err != nil || serving != "still serving" {
		t.Fatal("unrelated serving write changed", serving, err)
	}
}
