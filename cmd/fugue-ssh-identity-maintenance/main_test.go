package main

import (
	"context"
	"io"
	"testing"
)

func TestMaintenanceRequiresExplicitSafeArguments(test *testing.T) {
	test.Setenv("FUGUE_DATABASE_URL", "")
	for _, args := range [][]string{nil, {"delete"}, {"apply"}, {"snapshot", "--keyring-file", "keyring", "--backup-file", "backup"}, {"apply", "--keyring-file", "keyring", "--backup-file", "backup", "--plan-file", "plan"}, {"plan", "--unknown", "value"}} {
		if err := run(context.Background(), args, io.Discard); err == nil {
			test.Fatal("maintenance accepted missing authority or an implicit operation", args)
		}
	}
}

func TestServiceRoutePreservesTLSAuthority(test *testing.T) {
	configuration, err := databaseConfiguration("postgres://fixture@database.test-system.svc.cluster.local:5432/fixture?sslmode=verify-full", "10.43.0.8")
	if err != nil {
		test.Fatal(err)
	}
	if configuration.Host != "database.test-system.svc.cluster.local" || configuration.TLSConfig == nil || configuration.TLSConfig.ServerName != configuration.Host || configuration.TLSConfig.InsecureSkipVerify {
		test.Fatal("Service resolution changed database TLS identity")
	}
	addresses, err := configuration.LookupFunc(context.Background(), configuration.Host)
	if err != nil || len(addresses) != 1 || addresses[0] != "10.43.0.8" {
		test.Fatal("explicit Service route was not used", addresses, err)
	}
	if _, err := configuration.LookupFunc(context.Background(), "foreign"); err == nil {
		test.Fatal("Service route resolved a foreign host")
	}
	for _, address := range []string{"0.0.0.0", "224.0.0.1", "8.8.8.8", "service"} {
		if _, err := databaseConfiguration("postgres://database/fixture", address); err == nil {
			test.Fatal("invalid Service address accepted", address)
		}
	}
}
