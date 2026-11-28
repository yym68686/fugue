package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	kubejson "sigs.k8s.io/json"
)

const (
	registryRehomeFenceUsage     = "usage: fugue-release-guardian registry-rehome-render-fence CONFIG"
	registryRehomeDispatchUsage  = "usage: fugue-release-guardian registry-rehome-dispatch CONFIG INTENT_DIGEST APPROVAL_DIGEST EVIDENCE"
	registryRehomeAdmissionUsage = "usage: fugue-release-guardian registry-rehome-admission CONFIG INTENT_DIGEST APPROVAL_DIGEST"
)

type registryRehomeDispatchSettings struct {
	JournalFile           string `json:"journalFile"`
	WriterKubeconfig      string `json:"writerKubeconfig"`
	WriterContext         string `json:"writerContext"`
	ArchiveFile           string `json:"archiveFile"`
	ArchiveDigest         string `json:"archiveDigest"`
	AdmissionCABundleFile string `json:"admissionCABundleFile"`
	ObserverPublicKey     []byte `json:"observerPublicKey"`
	Actor                 string `json:"actor"`
	releaseguardian.RegistryRehomeKubeEnrollment
}

type registryRehomeAdmissionSettings struct {
	JournalFile           string `json:"journalFile"`
	AdmissionCABundleFile string `json:"admissionCABundleFile"`
	TLSCertificateFile    string `json:"tlsCertificateFile"`
	TLSPrivateKeyFile     string `json:"tlsPrivateKeyFile"`
	ListenAddress         string `json:"listenAddress"`
	ObserverPublicKey     []byte `json:"observerPublicKey"`
	releaseguardian.RegistryRehomeKubeEnrollment
}

func runRegistryRehomeRenderFence(_ context.Context, args []string, out io.Writer) error {
	if len(args) != 1 || out == nil {
		return errors.New(registryRehomeFenceUsage)
	}
	profile, err := readOfflineDispatchInput(args[0], 32<<10, true)
	if err != nil {
		return err
	}
	var settings struct {
		AdmissionCABundleFile string `json:"admissionCABundleFile"`
		releaseguardian.RegistryRehomeKubeEnrollment
	}
	if strict, err := kubejson.UnmarshalStrict(profile, &settings); err != nil || len(strict) != 0 ||
		!filepathAbsClean(settings.AdmissionCABundleFile) || len(settings.AdmissionCABundle) != 0 {
		return errors.New("invalid frozen registry rehome fence enrollment")
	}
	ca, err := readOfflineDispatchInput(settings.AdmissionCABundleFile, 16<<10, false)
	if err != nil {
		return err
	}
	settings.RegistryRehomeKubeEnrollment.AdmissionCABundle = ca
	bundle, err := releaseguardian.RenderRegistryRehomeKubeFence(settings.RegistryRehomeKubeEnrollment)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(bundle)
}

func runRegistryRehomeDispatch(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 4 || out == nil || !exactSHA256Digest(args[1]) || !exactSHA256Digest(args[2]) {
		return errors.New(registryRehomeDispatchUsage)
	}
	profile, err := readOfflineDispatchInput(args[0], 32<<10, true)
	if err != nil {
		return err
	}
	var settings registryRehomeDispatchSettings
	if strict, err := kubejson.UnmarshalStrict(profile, &settings); err != nil || len(strict) != 0 || validateRegistryRehomeDispatchSettings(settings) != nil {
		return errors.New("invalid frozen registry rehome dispatcher enrollment")
	}
	if err := readPrivateJournalProfile(settings.JournalFile); err != nil {
		return err
	}
	archive, err := readOfflineDispatchInput(settings.ArchiveFile, releaseguardian.MaxReleaseArchiveBytes, true)
	if err != nil {
		return err
	}
	defer clear(archive)
	ca, err := readOfflineDispatchInput(settings.AdmissionCABundleFile, 16<<10, false)
	if err != nil {
		return err
	}
	defer clear(ca)
	evidence, err := readOfflineDispatchInput(args[3], productioncontrol.MaxWitnessBytes, true)
	if err != nil {
		return err
	}
	defer clear(evidence)
	settings.RegistryRehomeKubeEnrollment.AdmissionCABundle = ca
	kube, err := loadRegistryRehomeWriter(settings.WriterKubeconfig, settings.WriterContext)
	if err != nil {
		return err
	}
	dbConfig, owners, cluster, err := productioncontrol.LoadJournalEndpoint(settings.JournalFile)
	if err != nil {
		return err
	}
	db := stdlib.OpenDB(*dbConfig)
	defer db.Close()
	db.SetMaxOpenConns(2)
	ledger, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := pingJournal(ctx, db); err != nil {
		return err
	}
	result, err := releaseguardian.DispatchRegistryRehomeKubernetes(ctx, kube,
		settings.RegistryRehomeKubeEnrollment, cluster, ledger, settings.ObserverPublicKey,
		archive, settings.ArchiveDigest, settings.Actor, args[1], args[2], evidence)
	if result.Claim.Fence > 0 {
		return errors.Join(err, json.NewEncoder(out).Encode(result))
	}
	return err
}

func loadRegistryRehomeWriter(path, name string) (*rest.Config, error) {
	raw, err := readOfflineDispatchInput(path, 128<<10, true)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	profile, err := clientcmd.Load(raw)
	if err != nil || profile.Contexts[name] == nil {
		return nil, errors.New("registry rehome writer requires an explicit static Kubernetes context")
	}
	settings, err := clientcmd.NewNonInteractiveClientConfig(*profile, name, &clientcmd.ConfigOverrides{CurrentContext: name}, nil).ClientConfig()
	if err != nil || settings == nil || settings.CAFile != "" || settings.CertFile != "" || settings.KeyFile != "" ||
		settings.BearerTokenFile != "" || settings.ExecProvider != nil || settings.AuthProvider != nil {
		return nil, errors.New("registry rehome writer requires self-contained static Kubernetes credentials")
	}
	return settings, nil
}

func validateRegistryRehomeDispatchSettings(s registryRehomeDispatchSettings) error {
	if s.JournalFile == "" || s.WriterContext == "" || s.Actor == "" ||
		!filepathAbsClean(s.WriterKubeconfig) || !filepathAbsClean(s.JournalFile) ||
		!filepathAbsClean(s.ArchiveFile) || !filepathAbsClean(s.AdmissionCABundleFile) ||
		!exactSHA256Digest(s.ArchiveDigest) || len(s.ObserverPublicKey) != ed25519.PublicKeySize ||
		s.Cutover.Phase != releaseguardian.KubeCutoverFrozen || len(s.AdmissionCABundle) != 0 {
		return errors.New("invalid dispatcher settings")
	}
	paths := []string{s.JournalFile, s.WriterKubeconfig, s.ArchiveFile, s.AdmissionCABundleFile}
	seen := map[string]bool{}
	for _, path := range paths {
		if seen[path] {
			return errors.New("registry rehome files must use separate paths")
		}
		seen[path] = true
	}
	return nil
}

func runRegistryRehomeAdmission(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 3 || out == nil || !exactSHA256Digest(args[1]) || !exactSHA256Digest(args[2]) {
		return errors.New(registryRehomeAdmissionUsage)
	}
	profile, err := readOfflineDispatchInput(args[0], 32<<10, true)
	if err != nil {
		return err
	}
	var settings registryRehomeAdmissionSettings
	if strict, err := kubejson.UnmarshalStrict(profile, &settings); err != nil || len(strict) != 0 || validateRegistryRehomeAdmissionSettings(settings) != nil {
		return errors.New("invalid read-only registry rehome admission enrollment")
	}
	if err := readPrivateJournalProfile(settings.JournalFile); err != nil {
		return err
	}
	ca, err := readOfflineDispatchInput(settings.AdmissionCABundleFile, 16<<10, false)
	if err != nil {
		return err
	}
	defer clear(ca)
	certPEM, err := readOfflineDispatchInput(settings.TLSCertificateFile, 32<<10, false)
	if err != nil {
		return err
	}
	defer clear(certPEM)
	keyPEM, err := readOfflineDispatchInput(settings.TLSPrivateKeyFile, 32<<10, true)
	if err != nil {
		return err
	}
	defer clear(keyPEM)
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil || len(certificate.Certificate) == 0 {
		return errors.New("registry rehome admission TLS certificate is invalid")
	}
	if err := verifyRegistryRehomeAdmissionCertificate(certificate.Certificate, ca, settings.AdmissionEndpoint); err != nil {
		return err
	}
	settings.RegistryRehomeKubeEnrollment.AdmissionCABundle = ca
	dbConfig, owners, cluster, err := productioncontrol.LoadJournalEndpoint(settings.JournalFile)
	if err != nil {
		return err
	}
	db := stdlib.OpenDB(*dbConfig, stdlib.OptionAfterConnect(func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET default_transaction_read_only = on")
		return err
	}))
	defer db.Close()
	db.SetMaxOpenConns(2)
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pingJournal(startup, db); err != nil {
		return err
	}
	ledger, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return err
	}
	intent, payload, err := ledger.ReadIntent(startup, args[1])
	if err != nil {
		return errors.New("registry rehome admission intent is not present in the journal")
	}
	submission := productioncontrol.Submission{Intent: intent, Payload: payload}
	handler, err := releaseguardian.NewRegistryRehomeKubernetesAdmissionHandler(settings.RegistryRehomeKubeEnrollment,
		cluster, submission, args[1], args[2], ledger, settings.ObserverPublicKey)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/registry-rehome-admission", handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		pingCtx, stop := context.WithTimeout(r.Context(), 2*time.Second)
		defer stop()
		if db.PingContext(pingCtx) != nil {
			http.Error(w, "journal unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	listener, err := net.Listen("tcp", settings.ListenAddress)
	if err != nil {
		return errors.New("registry rehome admission listener could not bind")
	}
	defer listener.Close()
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 4 * time.Second, ReadTimeout: 8 * time.Second,
		WriteTimeout: 8 * time.Second, IdleTimeout: 30 * time.Second,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, NextProtos: []string{"http/1.1"}}}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ServeTLS(listener, "", "") }()
	if err := json.NewEncoder(out).Encode(map[string]string{"status": "ready", "intentDigest": args[1]}); err != nil {
		_ = server.Close()
		return err
	}
	select {
	case <-ctx.Done():
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = server.Shutdown(shutdown)
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("registry rehome admission server stopped: %w", err)
	}
}

func validateRegistryRehomeAdmissionSettings(s registryRehomeAdmissionSettings) error {
	if !filepathAbsClean(s.JournalFile) || !filepathAbsClean(s.AdmissionCABundleFile) ||
		!filepathAbsClean(s.TLSCertificateFile) || !filepathAbsClean(s.TLSPrivateKeyFile) ||
		s.JournalFile == s.AdmissionCABundleFile || s.JournalFile == s.TLSCertificateFile || s.JournalFile == s.TLSPrivateKeyFile ||
		s.AdmissionCABundleFile == s.TLSCertificateFile || s.AdmissionCABundleFile == s.TLSPrivateKeyFile || s.TLSCertificateFile == s.TLSPrivateKeyFile ||
		len(s.ObserverPublicKey) != ed25519.PublicKeySize || s.Cutover.Phase != releaseguardian.KubeCutoverFrozen || len(s.AdmissionCABundle) != 0 {
		return errors.New("invalid read-only admission settings")
	}
	host, port, err := net.SplitHostPort(s.ListenAddress)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || portNumber < 1 || portNumber > 65535 || net.ParseIP(host) == nil {
		return errors.New("registry rehome admission listener must bind an explicit IP and port")
	}
	u, err := url.Parse(s.AdmissionEndpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.Path != "/registry-rehome-admission" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("registry rehome admission endpoint is invalid")
	}
	return nil
}

func verifyRegistryRehomeAdmissionCertificate(chain [][]byte, ca []byte, endpoint string) error {
	if len(chain) == 0 {
		return errors.New("registry rehome admission certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return errors.New("registry rehome admission leaf certificate is invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("registry rehome admission CA bundle is invalid")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return errors.New("registry rehome admission endpoint is invalid")
	}
	intermediates := x509.NewCertPool()
	for _, raw := range chain[1:] {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return errors.New("registry rehome admission certificate chain is invalid")
		}
		intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{DNSName: u.Hostname(), Roots: roots, Intermediates: intermediates}); err != nil {
		return errors.New("registry rehome admission certificate does not match its enrolled CA and service name")
	}
	return nil
}

func readPrivateJournalProfile(path string) error {
	raw, err := readOfflineDispatchInput(path, 32<<10, true)
	clear(raw)
	return err
}

func filepathAbsClean(path string) bool {
	return filepath.IsAbs(path) && path == filepath.Clean(path) && path == strings.TrimSpace(path)
}

func pingJournal(ctx context.Context, db *sql.DB) error {
	ping, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if err := db.PingContext(ping); err != nil {
		return errors.New("native registry rehome journal is unavailable")
	}
	return nil
}
