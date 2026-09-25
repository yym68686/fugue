package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
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

	c "fugue/internal/staticedgecontract"
	m "fugue/internal/staticedgemanager"
)

type settings struct {
	EdgeID                string                      `json:"edge_id"`
	Role                  string                      `json:"role"`
	StateDir              string                      `json:"state_dir"`
	Listen                string                      `json:"listen"`
	Socket                string                      `json:"socket"`
	SSHGrant              string                      `json:"ssh_grant"`
	VerificationKeys      map[string]string           `json:"verification_keys"`
	CredentialSlots       map[string]m.CredentialSlot `json:"credential_slots"`
	InitialCredentialSlot string                      `json:"initial_credential_slot"`
	Caddy                 m.CaddyConfig               `json:"caddy"`
}

func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) > 0 && args[0] == "ssh-rpc" {
		return rpc(args[1:])
	}
	fs := flag.NewFlagSet("fugue-static-edge-manager", flag.ContinueOnError)
	file := fs.String("config", "", "local manager policy JSON")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *file == "" || fs.NArg() != 0 {
		return fmt.Errorf("--config required")
	}
	var cfg settings
	if e := m.ReadConfig(*file, &cfg); e != nil {
		return e
	}
	if !filepath.IsAbs(cfg.Socket) || cfg.Listen == "" || !m.GrantValid(cfg.SSHGrant) {
		return fmt.Errorf("listen, absolute socket and ssh_grant required")
	}
	keys := map[string]ed25519.PublicKey{}
	for id, path := range cfg.VerificationKeys {
		raw, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		key, e := c.ParsePublicKeyPEM(raw)
		if e != nil {
			return e
		}
		keys[id] = key
	}
	credentials, e := m.NewCredentials(cfg.CredentialSlots, cfg.InitialCredentialSlot)
	if e != nil {
		return e
	}
	runtime, e := m.NewCaddyRuntime(cfg.Caddy)
	if e != nil {
		return e
	}
	manager, e := m.New(m.Config{EdgeID: cfg.EdgeID, Role: cfg.Role, StateDir: cfg.StateDir, VerificationKeys: keys, Runtime: runtime, Credentials: credentials})
	if e != nil {
		return e
	}
	defer manager.Close()
	// Holding the exclusive state lock prevents a second manager from replacing
	// the live socket. A stale socket from a dead process is the only removal.
	if info, e := os.Lstat(cfg.Socket); e == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("socket path exists and is not a socket")
		}
		if e = os.Remove(cfg.Socket); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	local, e := net.Listen("unix", cfg.Socket)
	if e != nil {
		return e
	}
	defer local.Close()
	if e = os.Chmod(cfg.Socket, 0600); e != nil {
		return e
	}
	remote, e := net.Listen("tcp", cfg.Listen)
	if e != nil {
		return e
	}
	defer remote.Close()
	server := func(h http.Handler) *http.Server {
		return &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 20 * time.Second, MaxHeaderBytes: 16 << 10}
	}
	https := server(manager.Handler(credentials.Authorize))
	unix := server(manager.Handler(m.LocalAuth(cfg.SSHGrant)))
	errs := make(chan error, 2)
	go func() { errs <- https.Serve(tls.NewListener(remote, credentials.Config())) }()
	go func() { errs <- unix.Serve(local) }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
	case e = <-errs:
	}
	// Manager shutdown drains management operations only. Business Caddy is a
	// separately supervised process and receives no signal.
	shutdown, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	_ = https.Shutdown(shutdown)
	_ = unix.Shutdown(shutdown)
	if e == http.ErrServerClosed {
		return nil
	}
	return e
}
func rpc(args []string) error {
	fs := flag.NewFlagSet("ssh-rpc", flag.ContinueOnError)
	socket := fs.String("socket", "/run/fugue-static-edge-manager/manager.sock", "local manager socket")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if fs.NArg() != 0 || !filepath.IsAbs(*socket) {
		return fmt.Errorf("absolute socket required")
	}
	raw, e := io.ReadAll(io.LimitReader(os.Stdin, c.MaxBytes+1))
	if e != nil || len(raw) > c.MaxBytes {
		return fmt.Errorf("invalid request size")
	}
	var req c.Request
	if e = c.StrictJSON(raw, &req); e != nil {
		return e
	}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", *socket)
	}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}
	resp, e := client.Post("http://localhost/v1/static-edge/manager", "application/json", bytes.NewReader(raw))
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	body, e := io.ReadAll(io.LimitReader(resp.Body, c.MaxBytes+1))
	if e != nil || len(body) > c.MaxBytes {
		return fmt.Errorf("invalid response size")
	}
	var out c.Response
	if e = c.StrictJSON(body, &out); e != nil {
		return fmt.Errorf("invalid manager response HTTP %d", resp.StatusCode)
	}
	_, e = os.Stdout.Write(append(body, '\n'))
	return e
}
