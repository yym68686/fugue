package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"fugue/internal/certsync"
	"fugue/internal/entryfailover"
	sc "fugue/internal/staticedgecontract"
	m "fugue/internal/staticedgemanager"
)

type settings struct {
	StateDir              string                      `json:"state_dir"`
	PolicyPath            string                      `json:"policy_path"`
	VerificationKeys      map[string]string           `json:"verification_keys"`
	VaultKeyPath          string                      `json:"vault_key_path"`
	CloudflareAPI         string                      `json:"cloudflare_api,omitempty"`
	Listen                string                      `json:"listen"`
	CredentialSlots       map[string]m.CredentialSlot `json:"credential_slots"`
	InitialCredentialSlot string                      `json:"initial_credential_slot"`
}

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("run, probe, or cert-sync command required")
	}
	switch args[0] {
	case "version":
		if len(args) != 1 {
			return errors.New("version takes no arguments")
		}
		fmt.Println(version)
		return nil
	case "run":
		return runServer(args[1:])
	case "cert-sync":
		return runCertificateSync(args[1:])
	case "probe":
		return runProbe(args[1:])
	case "probe-rpc":
		return runProbeRPC(args[1:])
	default:
		return errors.New("run, probe, or cert-sync command required")
	}
}

func runCertificateSync(args []string) error {
	fs := flag.NewFlagSet("cert-sync", flag.ContinueOnError)
	file := fs.String("config", "", "private certificate sync config JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *file == "" {
		return errors.New("cert-sync requires --config")
	}
	var cfg certsync.Config
	if err := m.ReadConfig(*file, &cfg); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	results, err := (certsync.Synchronizer{Config: cfg}).Run(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"certificates": results})
}

func runProbeRPC(args []string) error {
	fs := flag.NewFlagSet("probe-rpc", flag.ContinueOnError)
	keyPath := fs.String("key", "", "trusted policy signer public key file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *keyPath == "" {
		return errors.New("probe-rpc requires --key")
	}
	key, err := os.ReadFile(*keyPath)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 64<<10+1))
	if err != nil {
		return err
	}
	result, err := entryfailover.ProbeRPC(context.Background(), raw, key)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func runProbe(args []string) error {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	keyPath := fs.String("key", "", "trusted policy signer public key file")
	target := fs.String("target", "", "target ID")
	vantage := fs.String("vantage", "", "vantage ID")
	nonce := fs.String("nonce", "", "one-time nonce")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *keyPath == "" {
		return errors.New("probe requires --key and exact target/vantage/nonce")
	}
	key, err := os.ReadFile(*keyPath)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 64<<10+1))
	if err != nil {
		return err
	}
	if len(raw) > 64<<10 {
		return errors.New("signed policy exceeds size bound")
	}
	result, err := entryfailover.ProbeFromSignedPolicy(context.Background(), raw, key, *target, *vantage, *nonce)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	file := fs.String("config", "", "independent executor config JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *file == "" {
		return errors.New("run requires --config")
	}
	var cfg settings
	if err := m.ReadConfig(*file, &cfg); err != nil {
		return err
	}
	for _, path := range []string{cfg.StateDir, cfg.PolicyPath, cfg.VaultKeyPath} {
		if !filepath.IsAbs(path) {
			return errors.New("absolute state, policy and vault-key paths required")
		}
	}
	if cfg.Listen == "" || len(cfg.VerificationKeys) == 0 {
		return errors.New("listen and policy verification keys required")
	}
	keys := make(map[string]ed25519.PublicKey, len(cfg.VerificationKeys))
	for id, path := range cfg.VerificationKeys {
		if !sc.ValidID(id) || !filepath.IsAbs(path) {
			return errors.New("invalid verification key identity or path")
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		key, err := sc.ParsePublicKeyPEM(raw)
		if err != nil {
			return err
		}
		keys[id] = key
	}
	key, err := entryfailover.LoadVaultKey(cfg.VaultKeyPath)
	if err != nil {
		return err
	}
	credentials, err := m.NewCredentials(cfg.CredentialSlots, cfg.InitialCredentialSlot)
	if err != nil {
		return err
	}
	service, err := entryfailover.NewService(cfg.PolicyPath, keys,
		entryfailover.CredentialVault{StateDir: cfg.StateDir, Key: key}, cfg.CloudflareAPI)
	if err != nil {
		return err
	}
	service.BuildVersion = version
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: service.Handler(credentials.Authorize), TLSConfig: credentials.Config(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 2 * time.Minute, IdleTimeout: 20 * time.Second,
		MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go service.Run(ctx, func(err error) { fmt.Fprintln(os.Stderr, "entry failover cycle:", err) })
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
