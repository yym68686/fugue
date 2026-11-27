package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian/participation"
	"github.com/jackc/pgx/v5/stdlib"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const offlineDispatchUsage = "usage: fugue-release-guardian participation-offline-dispatch CONFIG INTENT_DIGEST APPROVAL_DIGEST PROVIDER_ATTESTATION"

var offlineDispatchDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type offlineDispatchSettings struct {
	JournalFile       string            `json:"journalFile"`
	Actor             string            `json:"actor"`
	Writer            string            `json:"writer"`
	WriterKubeconfig  string            `json:"writerKubeconfig"`
	WriterContext     string            `json:"writerContext"`
	ReaderKubeconfig  string            `json:"readerKubeconfig"`
	ReaderContext     string            `json:"readerContext"`
	ObserverPublicKey ed25519.PublicKey `json:"observerPublicKey"`
	DNSObserverToken  string            `json:"dnsObserverTokenFile,omitempty"`
}

func runOfflineHoldDispatch(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 4 || !offlineDispatchDigest.MatchString(args[1]) || !offlineDispatchDigest.MatchString(args[2]) {
		return errors.New(offlineDispatchUsage)
	}
	raw, err := readOfflineDispatchInput(args[0], 16<<10, false)
	if err != nil {
		return err
	}
	var cfg offlineDispatchSettings
	if err := decodeStrictJSON(raw, &cfg); err != nil || cfg.Actor == "" || cfg.Writer == "" ||
		cfg.ReaderKubeconfig == cfg.WriterKubeconfig && cfg.ReaderContext == cfg.WriterContext {
		return errors.New("offline hold requires dedicated writer and reader profiles")
	}
	attestation, err := readOfflineDispatchInput(args[3], 8<<10, false)
	if err != nil {
		return err
	}
	dbConfig, keys, cluster, err := productioncontrol.LoadJournalEndpoint(cfg.JournalFile)
	if err != nil {
		return err
	}
	reader, err := offlineKubeConfig(cfg.ReaderKubeconfig, cfg.ReaderContext)
	if err != nil {
		return err
	}
	writer, err := offlineKubeConfig(cfg.WriterKubeconfig, cfg.WriterContext)
	if err != nil {
		return err
	}
	db := stdlib.OpenDB(*dbConfig)
	defer db.Close()
	db.SetMaxOpenConns(2)
	j, err := productioncontrol.NewJournal(db, keys)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	var dns *participation.DNSObserver
	if cfg.DNSObserverToken != "" {
		topo, err := j.ReadParticipationTopology(ctx, cluster)
		if err != nil {
			return err
		}
		token, err := readOfflineDispatchInput(cfg.DNSObserverToken, 4096, true)
		if err != nil {
			return err
		}
		defer clear(token)
		dns, err = participation.NewDNSObserver(topo.Policy.Origin, string(bytes.TrimSpace(token)))
		if err != nil {
			return err
		}
	}
	result, err := participation.DispatchOfflineHold(ctx, j, reader, writer, dns, cluster, cfg.Actor, cfg.Writer, args[1], args[2], cfg.ObserverPublicKey, attestation)
	if result.Claim.Fence != 0 {
		return errors.Join(err, json.NewEncoder(out).Encode(result))
	}
	return err
}

func offlineKubeConfig(path, name string) (*rest.Config, error) {
	if !filepath.IsAbs(path) || name == "" {
		return nil, errors.New("offline hold requires an explicit Kubernetes profile")
	}
	loader := &clientcmd.ClientConfigLoadingRules{ExplicitPath: path}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, &clientcmd.ConfigOverrides{CurrentContext: name}).ClientConfig()
	if err != nil {
		return nil, errors.New("offline hold could not load a Kubernetes profile")
	}
	cfg.Timeout = 5 * time.Second
	return cfg, nil
}

func readOfflineDispatchInput(path string, limit int64, private bool) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("offline hold input files require absolute paths")
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("offline hold input file is unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > limit {
		return nil, errors.New("offline hold input file is invalid or too large")
	}
	if private && info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("offline hold token file is not private")
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > limit {
		return nil, errors.New("offline hold input file could not be read")
	}
	return raw, nil
}
