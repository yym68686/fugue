package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/platformproducer"
	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/dnscontrol"
	"fugue/internal/store"
	"github.com/jackc/pgx/v5/stdlib"
	kubejson "sigs.k8s.io/json"
)

const dnsPolicyDispatchUsage = "usage: fugue-release-guardian dns-policy-dispatch CONFIG INTENT_DIGEST APPROVAL_DIGEST"

type dnsPolicyDispatchSettings struct {
	JournalFile     string `json:"journalFile"`
	APIOrigin       string `json:"apiOrigin"`
	ReadTokenFile   string `json:"readTokenFile"`
	ArtifactKeyFile string `json:"artifactKeyFile"`
}

type dnsArtifactSigningSettings struct {
	PrimaryKey    string   `json:"primaryKey"`
	PrimaryKeyID  string   `json:"primaryKeyId"`
	PreviousKey   string   `json:"previousKey,omitempty"`
	PreviousKeyID string   `json:"previousKeyId,omitempty"`
	RevokedKeyIDs []string `json:"revokedKeyIds,omitempty"`
}

func runDNSPolicyDispatch(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 3 || !platformproducer.ValidDigest(args[1]) || !platformproducer.ValidDigest(args[2]) {
		return errors.New(dnsPolicyDispatchUsage)
	}
	raw, err := readOfflineDispatchInput(args[0], 16<<10, false)
	if err != nil {
		return err
	}
	var settings dnsPolicyDispatchSettings
	if strict, err := kubejson.UnmarshalStrict(raw, &settings); err != nil || len(strict) != 0 || settings.JournalFile == "" ||
		settings.APIOrigin == "" || settings.ReadTokenFile == "" || settings.ArtifactKeyFile == "" ||
		settings.ReadTokenFile == settings.ArtifactKeyFile || settings.JournalFile == settings.ReadTokenFile || settings.JournalFile == settings.ArtifactKeyFile {
		return errors.New("native DNS dispatch requires separate runtime database, read token and artifact verification profiles")
	}
	token, err := readOfflineDispatchInput(settings.ReadTokenFile, 4096, true)
	if err != nil {
		return err
	}
	defer clear(token)
	reader, err := dnscontrol.NewReader(settings.APIOrigin, string(bytes.TrimSpace(token)))
	if err != nil {
		return err
	}
	keyRaw, err := readOfflineDispatchInput(settings.ArtifactKeyFile, 32<<10, true)
	if err != nil {
		return err
	}
	defer clear(keyRaw)
	var signing dnsArtifactSigningSettings
	if strict, err := kubejson.UnmarshalStrict(keyRaw, &signing); err != nil || len(strict) != 0 || signing.PrimaryKey == "" || signing.PrimaryKeyID == "" ||
		(signing.PreviousKey == "") != (signing.PreviousKeyID == "") || signing.PreviousKeyID == signing.PrimaryKeyID {
		return errors.New("native DNS artifact verification profile is invalid")
	}
	keyring := bundleauth.NewKeyring(signing.PrimaryKey, signing.PrimaryKeyID, signing.PreviousKey, signing.PreviousKeyID, signing.RevokedKeyIDs)
	// The existing journal endpoint parser enforces hostname-verified TLS.
	// Its profile also contains the dedicated SQL login and must be private.
	secret, err := readOfflineDispatchInput(settings.JournalFile, 32<<10, true)
	if err != nil {
		return err
	}
	clear(secret)
	dbConfig, owners, cluster, err := productioncontrol.LoadJournalEndpoint(settings.JournalFile)
	if err != nil {
		return err
	}
	db := stdlib.OpenDB(*dbConfig)
	defer db.Close()
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	ping, stopPing := context.WithTimeout(ctx, 5*time.Second)
	err = db.PingContext(ping)
	stopPing()
	if err != nil {
		return errors.New("native DNS provider database unavailable")
	}
	control, err := store.NewNativeDNSControl(db, cluster, owners, keyring, reader)
	if err != nil {
		return err
	}
	result, err := control.Dispatch(ctx, args[1], args[2])
	if result.Claim.Fence > 0 {
		return errors.Join(err, json.NewEncoder(out).Encode(result))
	}
	return err
}
