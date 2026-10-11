package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"reflect"
	"time"

	"fugue/internal/sshmaintenance"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "SSH identity maintenance:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	if len(args) == 0 || args[0] != "snapshot" && args[0] != "plan" && args[0] != "check" && args[0] != "apply" {
		return errors.New("explicit snapshot, plan, check, or apply operation required")
	}
	flags := flag.NewFlagSet("ssh-identity-maintenance", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var keyringFile, backupFile, planFile, actor string
	flags.StringVar(&keyringFile, "keyring-file", "", "Protected storage-encryption keyring")
	flags.StringVar(&backupFile, "backup-file", "", "Protected encrypted identity and ownership snapshot")
	flags.StringVar(&planFile, "plan-file", "", "Exact reviewed rewrap plan")
	flags.StringVar(&actor, "actor", "", "Explicit maintenance audit actor")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || keyringFile == "" || backupFile == "" || args[0] != "snapshot" && planFile == "" || args[0] == "apply" && actor == "" {
		return errors.New("complete protected-file arguments and an apply actor are required")
	}
	dsn := os.Getenv("FUGUE_DATABASE_URL")
	if dsn == "" {
		return errors.New("FUGUE_DATABASE_URL must provide an explicit authorized database connection")
	}
	keyring, keyringDigest, err := sshmaintenance.LoadMaterial(keyringFile)
	if err != nil {
		return err
	}
	configuration, err := databaseConfiguration(dsn, os.Getenv("FUGUE_SSH_MAINTENANCE_DB_IP"))
	if err != nil {
		return err
	}
	database := sql.OpenDB(stdlib.GetConnector(*configuration))
	defer database.Close()
	database.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	var backup sshmaintenance.Backup
	if args[0] != "snapshot" {
		raw, err := sshmaintenance.ReadPrivate(backupFile, 64<<20)
		if err != nil || sshmaintenance.Decode(raw, &backup) != nil || sshmaintenance.ValidateBackup(backup, keyring) != nil {
			return errors.New("protected encrypted backup is invalid or unavailable")
		}
	}
	if args[0] == "apply" {
		raw, err := sshmaintenance.ReadPrivate(planFile, 16<<20)
		var plan sshmaintenance.Plan
		if err != nil || sshmaintenance.Decode(raw, &plan) != nil {
			return errors.New("reviewed maintenance plan is invalid or unavailable")
		}
		changed, err := sshmaintenance.Apply(ctx, database, keyring, keyringDigest, backup, plan, actor)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(map[string]any{"verified": true, "changed": changed, "plan_digest": plan.Digest, "host_identities_unchanged": true, "ownership_retained": true})
	}
	transaction, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return errors.New("read-only identity inventory is unavailable")
	}
	defer transaction.Rollback()
	current, err := sshmaintenance.ReadInventory(ctx, transaction, keyring)
	if err != nil {
		return err
	}
	if args[0] == "snapshot" {
		if err := sshmaintenance.WritePrivate(backupFile, current); err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(map[string]any{"verified": true, "identities": len(current.Identities), "backup_digest": current.Digest, "encryption_key_ids": sshmaintenance.KeyUsage(current), "historical_backup_keys_must_be_retained": true})
	}
	plan, err := sshmaintenance.BuildPlan(current, backup, keyring, keyringDigest)
	if err != nil {
		return err
	}
	if args[0] == "check" {
		raw, err := sshmaintenance.ReadPrivate(planFile, 16<<20)
		var expected sshmaintenance.Plan
		if err != nil || sshmaintenance.Decode(raw, &expected) != nil || !reflect.DeepEqual(plan, expected) {
			return sshmaintenance.ErrPrecondition
		}
		return json.NewEncoder(output).Encode(map[string]any{"verified": true, "plan_digest": plan.Digest, "identities": len(plan.Entries), "read_only": true})
	}
	if err := sshmaintenance.WritePrivate(planFile, plan); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(plan)
}

func databaseConfiguration(dsn, selectedIP string) (*pgx.ConnConfig, error) {
	configuration, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("database connection configuration is invalid")
	}
	if selectedIP == "" {
		return configuration, nil
	}
	address, err := netip.ParseAddr(selectedIP)
	if err != nil || address.IsUnspecified() || address.IsMulticast() || !address.IsPrivate() {
		return nil, errors.New("database route must be an explicit private Service address")
	}
	logicalHost := configuration.Host
	if _, err := netip.ParseAddr(logicalHost); err == nil {
		return nil, errors.New("database route override requires a logical Service hostname")
	}
	configuration.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
		if host != logicalHost {
			return nil, errors.New("database route cannot resolve a different authority")
		}
		return []string{address.String()}, nil
	}
	return configuration, nil
}
