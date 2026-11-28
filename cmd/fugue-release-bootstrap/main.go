// fugue-release-bootstrap uses Kubernetes and the durable production
// journal directly. It cannot install admission, change permissions, or
// run a deployment outside Guardian.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/releaseguardian"
	"fugue/internal/releaseguardian/dnscontrol"
	"fugue/internal/releaseguardian/participation"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/jackc/pgx/v5/stdlib"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const usage = `usage: fugue-release-bootstrap verify-release-recovery-archive ARCHIVE EXPECTED_SHA256 [MIRROR_CONFIG]
       fugue-release-bootstrap draft-release-registry-rehome ARCHIVE EXPECTED_SHA256 MIRROR_CONFIG
       fugue-release-bootstrap verify-release-registry-rehome BUNDLE EXPECTED_SHA256 [REGISTRY_ENDPOINT_CONFIG]
       fugue-release-bootstrap CONFIG publish INTENT_DIGEST APPROVAL_DIGEST ATTESTATION_FILE
       fugue-release-bootstrap CONFIG observe INTENT_DIGEST
       fugue-release-bootstrap CONFIG anchor GENERATION VERSION_DIGEST WITNESS_DIGEST
       fugue-release-bootstrap CONFIG settle INTENT_DIGEST OUTCOME GENERATION VERSION_DIGEST WITNESS_DIGEST
       fugue-release-bootstrap CONFIG participation-checkpoint [INTENT_DIGEST]
       fugue-release-bootstrap CONFIG participation-anchor GENERATION VERSION_DIGEST WITNESS_DIGEST
       fugue-release-bootstrap CONFIG participation-settle INTENT_DIGEST OUTCOME GENERATION VERSION_DIGEST WITNESS_DIGEST
       fugue-release-bootstrap CONFIG dns-checkpoint [INTENT_DIGEST]
       fugue-release-bootstrap CONFIG dns-freeze INTENT_DIGEST APPROVAL_DIGEST
       fugue-release-bootstrap CONFIG dns-anchor GENERATION VERSION_DIGEST WITNESS_DIGEST
       fugue-release-bootstrap CONFIG dns-activate INTENT_DIGEST APPROVAL_DIGEST
       fugue-release-bootstrap CONFIG dns-settle INTENT_DIGEST OUTCOME GENERATION VERSION_DIGEST WITNESS_DIGEST`

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type settings struct {
	Mode              string                                `json:"mode"`
	JournalFile       string                                `json:"journalFile"`
	Actor             string                                `json:"actor"`
	Cutover           releaseguardian.KubeDeploymentCutover `json:"cutover"`
	DependencyService string                                `json:"dependencyService"`
	Registry          string                                `json:"registry"`
	RegistryAuthFile  string                                `json:"registryAuthFile,omitempty"`
	WitnessDirectory  string                                `json:"witnessDirectory"`
	ObserverPublicKey ed25519.PublicKey                     `json:"observerPublicKey"`
	Node              string                                `json:"node,omitempty"`
	DNSOrigin         string                                `json:"dnsOrigin,omitempty"`
	DNSReadTokenFile  string                                `json:"dnsReadTokenFile,omitempty"`
}

type request struct {
	command, intent, approval, attestation string
	version                                productioncontrol.Version
	outcome                                productioncontrol.Outcome
	witness                                string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 0 && args[0] == "verify-release-recovery-archive" {
		return verifyReleaseArchive(ctx, args[1:], out)
	}
	if len(args) != 0 && args[0] == "draft-release-registry-rehome" {
		return draftReleaseRegistryRehome(ctx, args[1:], out)
	}
	if len(args) != 0 && args[0] == "verify-release-registry-rehome" {
		return verifyRegistryRehomeRecovery(ctx, args[1:], out)
	}
	if len(args) < 2 {
		return errors.New(usage)
	}
	req, err := parseRequest(args[1:])
	if err != nil {
		return err
	}
	cfg, err := readSettings(args[0], req.command)
	if err != nil {
		return err
	}
	dsn, owners, cluster, err := productioncontrol.LoadJournalEndpoint(cfg.JournalFile)
	if err != nil {
		return err
	}
	if req.command == "participation-checkpoint" || req.command == "dns-checkpoint" {
		dsn.RuntimeParams["default_transaction_read_only"] = "on"
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	db := stdlib.OpenDB(*dsn)
	defer db.Close()
	db.SetMaxOpenConns(2)
	journal, err := productioncontrol.NewJournal(db, owners)
	if err != nil {
		return err
	}
	if cfg.Node != "" {
		return runParticipation(ctx, journal, cluster, cfg, req, out)
	}
	if cfg.DNSOrigin != "" {
		return runDNSSettlement(ctx, journal, cluster, cfg, req, out)
	}
	scope, err := releaseguardian.DeploymentCutoverScope(cfg.Cutover, cluster)
	if err != nil {
		return err
	}
	source, err := os.OpenRoot(cfg.WitnessDirectory)
	if err != nil {
		return errors.New("independent witness directory is unavailable")
	}
	defer source.Close()
	execution, err := productioncontrol.NewWitnessedExecutionJournal(journal, directoryWitnesses{source},
		map[string]productioncontrol.TrustedWitness{"provider-observer": {Scope: scope, PublicKey: cfg.ObserverPublicKey}})
	if err != nil {
		return err
	}
	emit := json.NewEncoder(out).Encode
	if req.command == "anchor" {
		created, err := execution.AnchorResource(ctx, cfg.Actor, productioncontrol.Baseline{Scope: scope, Verified: req.version, EvidenceDigest: req.witness})
		if err != nil {
			return err
		}
		return emit(productioncontrol.Recorded{Digest: req.witness, Created: created})
	}
	intent, _, err := execution.ReadIntent(ctx, req.intent)
	if err != nil {
		return err
	}
	if intent.ClusterID != scope.ClusterID || intent.ResourceID != scope.ResourceID || intent.Operation != productioncontrol.ComponentRelease {
		return errors.New("production intent is not a release for the enrolled bootstrap Deployment")
	}
	if req.command == "settle" {
		claim, _, err := execution.Execution(ctx, scope, req.intent)
		if err != nil {
			return err
		}
		result := productioncontrol.ExecutionResult{Outcome: req.outcome, Verified: req.version, EvidenceDigest: req.witness}
		created, err := execution.CompleteExecution(ctx, cfg.Actor, claim, result)
		if err != nil {
			return err
		}
		return emit(productioncontrol.Recorded{Digest: req.witness, Created: created})
	}
	kube, err := rest.InClusterConfig()
	if err != nil || kube.CAFile == "" || kube.BearerTokenFile == "" {
		return errors.New("bootstrap requires its dedicated in-cluster Kubernetes service account and mounted cluster CA")
	}
	kube.Timeout = 15 * time.Second
	if req.command == "observe" {
		kube.Wrap(func(next http.RoundTripper) http.RoundTripper { return observeTransport{next} })
	}
	transport, err := rest.HTTPClientFor(kube)
	if err != nil {
		return errors.New("could not create bootstrap Kubernetes transport")
	}
	transport.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client, err := kubernetes.NewForConfigAndClient(kube, transport)
	if err != nil {
		return errors.New("could not create bootstrap Kubernetes client")
	}
	target := releaseguardian.TargetConfig{Key: cfg.Cutover.Key, Namespace: cfg.Cutover.Namespace,
		MonitorComponent: cfg.Cutover.MonitorComponent, DependencyService: cfg.DependencyService}
	store, err := releaseguardian.NewKubeStore(client, []releaseguardian.TargetConfig{target})
	if err != nil {
		return err
	}
	if req.command == "observe" {
		observer, err := releaseguardian.NewNativeReleaseObserver(store, execution, cluster)
		if err != nil {
			return err
		}
		observation, err := observer.ObserveServingRelease(ctx, req.intent)
		if err != nil {
			return err
		}
		return emit(observation)
	}
	raw, err := readFile(req.attestation, releaseguardian.MaxCutoverAttestationBytes)
	if err != nil {
		return err
	}
	evidence, err := releaseguardian.NewSignedDeploymentCutoverEvidence(raw, cfg.ObserverPublicKey)
	if err != nil {
		return err
	}
	registry, err := openRegistry(cfg.Registry, cfg.RegistryAuthFile)
	if err != nil {
		return err
	}
	publisher, err := releaseguardian.NewKubeFencedNativePublisher(store, execution, cfg.Cutover, cluster, evidence, registry)
	if err != nil {
		return err
	}
	publication, err := publisher.Publish(ctx, cfg.Actor, req.intent, req.approval)
	if err != nil {
		return err
	}
	return emit(publication)
}

func parseRequest(args []string) (request, error) {
	if len(args) == 0 {
		return request{}, errors.New(usage)
	}
	r := request{command: args[0]}
	switch r.command {
	case "dns-freeze", "dns-activate":
		if len(args) == 3 && allDigests(args[1], args[2]) {
			r.intent, r.approval = args[1], args[2]
			return r, nil
		}
	case "publish":
		if len(args) == 4 && allDigests(args[1], args[2]) && filepath.IsAbs(args[3]) {
			r.intent, r.approval, r.attestation = args[1], args[2], args[3]
			return r, nil
		}
	case "observe", "participation-checkpoint", "dns-checkpoint":
		if r.command != "observe" && len(args) == 1 {
			return r, nil
		}
		if len(args) == 2 && allDigests(args[1]) {
			r.intent = args[1]
			return r, nil
		}
	case "anchor", "settle", "participation-anchor", "participation-settle", "dns-anchor", "dns-settle":
		version := args[1:]
		if r.command == "settle" || r.command == "participation-settle" || r.command == "dns-settle" {
			if len(version) != 5 || !allDigests(version[0]) {
				break
			}
			r.intent, r.outcome, version = version[0], productioncontrol.Outcome(version[1]), version[2:]
			if r.outcome != productioncontrol.Applied && r.outcome != productioncontrol.Restored && r.outcome != productioncontrol.NotApplied {
				break
			}
			if r.command != "settle" && r.outcome == productioncontrol.Restored {
				break
			}
		}
		if len(version) == 3 && allDigests(version[1], version[2]) {
			generation, err := strconv.ParseInt(version[0], 10, 64)
			if err == nil && generation > 0 {
				r.version, r.witness = productioncontrol.Version{Generation: generation, Digest: version[1]}, version[2]
				return r, nil
			}
		}
	}
	return request{}, errors.New(usage)
}

func allDigests(values ...string) bool {
	for _, value := range values {
		if !digestPattern.MatchString(value) {
			return false
		}
	}
	return true
}

func readSettings(path, command string) (settings, error) {
	var cfg settings
	raw, err := readFile(path, 16<<10)
	if err != nil || decode(raw, &cfg) != nil || !filepath.IsAbs(cfg.JournalFile) || !filepath.IsAbs(cfg.WitnessDirectory) ||
		len(cfg.ObserverPublicKey) != ed25519.PublicKeySize || cfg.Mode != command || cfg.Actor == "" {
		return settings{}, errors.New("bootstrap configuration is invalid or does not authorize this command")
	}
	switch cfg.Mode {
	case "dns-checkpoint", "dns-freeze", "dns-anchor", "dns-activate", "dns-settle":
		if cfg.DNSOrigin == "" || !filepath.IsAbs(cfg.DNSReadTokenFile) || cfg.DNSReadTokenFile == cfg.JournalFile ||
			cfg.Node != "" || cfg.Cutover != (releaseguardian.KubeDeploymentCutover{}) || cfg.DependencyService != "" {
			return settings{}, errors.New("bootstrap DNS profile requires an independent API reader and no Kubernetes settings")
		}
	case "participation-checkpoint", "participation-anchor", "participation-settle":
		if cfg.Node == "" || len(validation.IsDNS1123Subdomain(cfg.Node)) != 0 ||
			cfg.Cutover != (releaseguardian.KubeDeploymentCutover{}) || cfg.DependencyService != "" {
			return settings{}, errors.New("bootstrap participation profile requires one Node and no Deployment settings")
		}
	default:
		if cfg.Node != "" {
			return settings{}, errors.New("bootstrap Deployment profile must not specify a Node")
		}
	}
	if !strings.HasPrefix(cfg.Mode, "dns-") && (cfg.DNSOrigin != "" || cfg.DNSReadTokenFile != "") {
		return settings{}, errors.New("only DNS bootstrap may configure a DNS reader")
	}
	if cfg.Mode == "publish" {
		if cfg.Registry == "" || (cfg.RegistryAuthFile != "" && !filepath.IsAbs(cfg.RegistryAuthFile)) {
			return settings{}, errors.New("bootstrap requires a separately configured independent registry")
		}
	} else if cfg.Registry != "" || cfg.RegistryAuthFile != "" {
		return settings{}, errors.New("only bootstrap publication may configure registry credentials")
	}
	return cfg, nil
}

func runDNSSettlement(ctx context.Context, j *productioncontrol.Journal, cluster string, cfg settings, req request, out io.Writer) error {
	file, err := os.OpenFile(cfg.DNSReadTokenFile, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return errors.New("bootstrap DNS reader token is unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		return errors.New("bootstrap DNS reader token must be private")
	}
	token, err := boundedRead(file, 4096)
	if err != nil {
		return err
	}
	defer clear(token)
	r, err := dnscontrol.NewReader(cfg.DNSOrigin, string(bytes.TrimSpace(token)))
	if err != nil {
		return err
	}
	emit := json.NewEncoder(out).Encode
	if req.command == "dns-checkpoint" {
		c, coverage, err := r.ObserveListenerCoverage(ctx, j, cluster, req.intent, time.Now)
		if err != nil {
			return err
		}
		d, err := c.Digest()
		if err != nil {
			return err
		}
		coverageDigest, err := coverage.Digest()
		if err != nil {
			return err
		}
		return emit(struct {
			CheckpointDigest       string                       `json:"checkpointDigest"`
			Checkpoint             dnscontrol.ServingCheckpoint `json:"checkpoint"`
			ListenerCoverageDigest string                       `json:"listenerCoverageDigest"`
			ListenerCoverage       dnscontrol.ListenerCoverage  `json:"listenerCoverage"`
			ExecutionEvidence      bool                         `json:"executionEvidence"`
		}{d, c, coverageDigest, coverage, false})
	}
	if req.command == "dns-freeze" {
		record, err := dnscontrol.ApplyCutover(ctx, j, r, cluster, cfg.Actor, req.intent, req.approval, productioncontrol.DNSFreeze, nil, nil)
		if err != nil {
			return err
		}
		return emit(record)
	}
	root, err := os.OpenRoot(cfg.WitnessDirectory)
	if err != nil {
		return errors.New("independent DNS witness directory is unavailable")
	}
	defer root.Close()
	if req.command == "dns-activate" {
		record, err := dnscontrol.ApplyCutover(ctx, j, r, cluster, cfg.Actor, req.intent, req.approval, productioncontrol.DNSActivate,
			directoryWitnesses{root}, cfg.ObserverPublicKey)
		if err != nil {
			return err
		}
		return emit(record)
	}
	e, err := dnscontrol.NewSettlement(j, r, cluster, directoryWitnesses{root}, cfg.ObserverPublicKey)
	if err != nil {
		return err
	}
	scope := dnscontrol.Scope(cluster)
	var created bool
	switch req.command {
	case "dns-anchor":
		created, err = e.AnchorResource(ctx, cfg.Actor, productioncontrol.Baseline{Scope: scope, Verified: req.version, EvidenceDigest: req.witness})
	case "dns-settle":
		claim, _, readErr := j.ReadExecution(ctx, scope, req.intent)
		if readErr != nil {
			return readErr
		}
		created, err = e.CompleteExecution(ctx, cfg.Actor, claim, productioncontrol.ExecutionResult{Outcome: req.outcome, Verified: req.version, EvidenceDigest: req.witness})
	default:
		return errors.New("unsupported bootstrap DNS mode")
	}
	if err != nil {
		return err
	}
	return emit(struct {
		Created bool `json:"created"`
	}{created})
}

func runParticipation(ctx context.Context, j *productioncontrol.Journal, cluster string, cfg settings, req request, out io.Writer) error {
	kube, err := rest.InClusterConfig()
	if err != nil || kube.CAFile == "" || kube.BearerTokenFile == "" {
		return errors.New("participation bootstrap requires the dedicated read-only Kubernetes identity and mounted CA")
	}
	kube.Timeout = 5 * time.Second
	emit := json.NewEncoder(out).Encode
	if req.command == "participation-checkpoint" {
		checkpoint, err := participation.ObserveOfflineHoldCheckpoint(ctx, j, kube, cluster, cfg.Node, req.intent)
		if err != nil {
			return err
		}
		digest, err := checkpoint.Digest()
		if err != nil {
			return err
		}
		return emit(struct {
			ObservationDigest string                              `json:"observationDigest"`
			Checkpoint        participation.OfflineHoldCheckpoint `json:"checkpoint"`
			ExecutionEvidence bool                                `json:"executionEvidence"`
		}{digest, checkpoint, false})
	}
	witnesses, err := os.OpenRoot(cfg.WitnessDirectory)
	if err != nil {
		return errors.New("independent witness directory is unavailable")
	}
	defer witnesses.Close()
	e, err := participation.NewOfflineHoldSettlement(j, kube, cluster, cfg.Node, directoryWitnesses{witnesses}, cfg.ObserverPublicKey)
	if err != nil {
		return err
	}
	scope := participation.HoldResource(cluster, cfg.Node)
	var created bool
	switch req.command {
	case "participation-anchor":
		created, err = e.AnchorResource(ctx, cfg.Actor, productioncontrol.Baseline{Scope: scope, Verified: req.version, EvidenceDigest: req.witness})
	case "participation-settle":
		claim, _, readErr := j.ReadExecution(ctx, scope, req.intent)
		if readErr != nil {
			return readErr
		}
		created, err = e.CompleteExecution(ctx, cfg.Actor, claim, productioncontrol.ExecutionResult{
			Outcome: req.outcome, Verified: req.version, EvidenceDigest: req.witness,
		})
	default:
		return errors.New("unsupported bootstrap participation mode")
	}
	if err != nil {
		return err
	}
	return emit(productioncontrol.Recorded{Digest: req.witness, Created: created})
}

func openRegistry(host, authFile string) (*releaseguardian.IndependentRegistry, error) {
	var auth authn.Authenticator
	if authFile != "" {
		var secret struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		raw, err := readFile(authFile, 8192)
		if err != nil || decode(raw, &secret) != nil || secret.Username == "" || secret.Password == "" {
			clear(raw)
			return nil, errors.New("bootstrap registry credential file is invalid")
		}
		clear(raw)
		auth = authn.FromConfig(authn.AuthConfig{Username: secret.Username, Password: secret.Password})
	}
	return releaseguardian.NewIndependentRegistry(host, auth, nil)
}

type directoryWitnesses struct{ root *os.Root }

type observeTransport struct{ next http.RoundTripper }

func (t observeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet || r.Header.Get("Upgrade") != "" {
		return nil, errors.New("bootstrap observation only permits plain Kubernetes GET requests")
	}
	return t.next.RoundTrip(r)
}

func (s directoryWitnesses) ReadExecutionWitness(ctx context.Context, digest string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !allDigests(digest) {
		return nil, errors.New("invalid provider witness digest")
	}
	file, err := s.root.OpenFile(digest[len("sha256:"):]+".json", os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("provider witness is unavailable")
	}
	defer file.Close()
	return boundedRead(file, productioncontrol.MaxWitnessBytes)
}

func readFile(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("bootstrap requires absolute input paths")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("bootstrap input file is unavailable")
	}
	defer file.Close()
	return boundedRead(file, limit)
}

func boundedRead(file *os.File, limit int64) ([]byte, error) {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > limit {
		return nil, errors.New("bootstrap input must be a bounded regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(raw) < 1 || int64(len(raw)) > limit {
		return nil, errors.New("could not read bounded bootstrap input")
	}
	return raw, nil
}

func decode(raw []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if d.Decode(new(json.RawMessage)) != io.EOF {
		return errors.New("unexpected content after bootstrap document")
	}
	return nil
}
