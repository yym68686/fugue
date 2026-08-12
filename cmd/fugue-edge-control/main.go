// Command fugue-edge-control runs the independently deployable, group-scoped
// Edge control boundary.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fugue/internal/edgecontrol"
)

const (
	defaultBindAddr            = "127.0.0.1:8092"
	defaultShutdownTimeout     = 10 * time.Second
	defaultAuthorityInterval   = 30 * time.Second
	defaultGroupBundleValidity = 30 * time.Minute
	minReconcileInterval       = 5 * time.Second
	maxReconcileInterval       = 5 * time.Minute
)

var edgeGroupIDPattern = regexp.MustCompile(`^edge-group-[a-z0-9]+(?:-[a-z0-9]+)*$`)

type config struct {
	Enabled                 bool
	BindAddr                string
	ShutdownTimeout         time.Duration
	RouteIntentURL          string
	RouteIntentIssuerFile   string
	RouteIntentIdentityNode string
	RouteIntentCAFile       string
	RouteIntentServerName   string
	InventoryKeyringDir     string
	AuthorityRuntimeEnabled bool
	AuthorityStateDir       string
	AuthorityGroupIDs       []string
	AuthorityPollInterval   time.Duration
	GroupSigningKeyringDir  string
	GroupReaderKeyringDir   string
	GroupRecoveryKeyringDir string
	GroupBundleValidity     time.Duration
	CandidatePublisher      bool
	CandidateIdentity       edgecontrol.CandidateReleaseIdentity
}

func main() {
	cfg, err := configFromEnv(os.Getenv)
	if err != nil {
		log.Fatalf("edge-control config: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log.Default()); err != nil {
		log.Fatalf("edge-control: %v", err)
	}
}

func run(ctx context.Context, cfg config, logger *log.Logger) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	if logger == nil {
		logger = log.Default()
	}
	authorityRuntime, handler, err := buildAuthorityProcess(cfg)
	if err != nil {
		return err
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	mode := "boundary-only"
	if handler == nil {
		handler = edgecontrol.NewBoundary(cfg.Enabled).Handler()
	} else {
		mode = "group-authority"
	}
	server := edgecontrol.Server(cfg.BindAddr, handler)
	defer server.Close()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe() }()
	authority := "none"
	if authorityRuntime != nil {
		authority = "edge-control"
	}
	logger.Printf("fugue-edge-control listening on %s mode=%s authority=%s enabled=%t", cfg.BindAddr, mode, authority, cfg.Enabled)
	var runtimeDone <-chan error
	if authorityRuntime != nil {
		done := make(chan error, 1)
		runtimeDone = done
		go func() {
			done <- authorityRuntime.Run(runCtx, cfg.AuthorityPollInterval, func(observation edgecontrol.AuthorityRuntimeObservation) {
				if observation.FailureCode != "" {
					logger.Printf("edge-control group authority reconcile status=failed failure_code=%s authority=edge-control publication=enabled", observation.FailureCode)
					return
				}
				if observation.CandidatePublished > 0 {
					logger.Printf("edge-control candidate reconcile status=observed generation=%s candidate_published=%d failed=%d authority=current-preserved", observation.RouteIntentGeneration, observation.CandidatePublished, observation.Failed)
					return
				}
				logger.Printf("edge-control group authority reconcile status=observed generation=%s published=%d failed=%d authority=edge-control publication=enabled", observation.RouteIntentGeneration, observation.Published, observation.Failed)
			})
		}()
	}

	select {
	case <-ctx.Done():
		cancelRun()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	case err := <-runtimeDone:
		if err != nil {
			return fmt.Errorf("edge-control runtime: %w", err)
		}
		return nil
	case err := <-serverDone:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	}
}

func configFromEnv(getenv func(string) string) (config, error) {
	if getenv == nil {
		return config{}, errors.New("environment reader is nil")
	}
	enabled, err := strictBool(getenv("FUGUE_EDGE_CONTROL_ENABLED"))
	if err != nil {
		return config{}, fmt.Errorf("FUGUE_EDGE_CONTROL_ENABLED: %w", err)
	}
	bindAddr := strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_BIND_ADDR"))
	if bindAddr == "" {
		bindAddr = defaultBindAddr
	}
	shutdownTimeout := defaultShutdownTimeout
	if raw := strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT")); raw != "" {
		shutdownTimeout, err = time.ParseDuration(raw)
		if err != nil || shutdownTimeout <= 0 || shutdownTimeout > 2*time.Minute {
			return config{}, errors.New("FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT must be between 1ns and 2m")
		}
	}
	authorityRuntimeEnabled, err := strictBool(getenv("FUGUE_EDGE_CONTROL_AUTHORITY_RUNTIME_ENABLED"))
	if err != nil {
		return config{}, fmt.Errorf("FUGUE_EDGE_CONTROL_AUTHORITY_RUNTIME_ENABLED: %w", err)
	}
	cfg := config{Enabled: enabled, BindAddr: bindAddr, ShutdownTimeout: shutdownTimeout, AuthorityRuntimeEnabled: authorityRuntimeEnabled}
	if authorityRuntimeEnabled {
		cfg.RouteIntentURL = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_ROUTE_INTENT_URL"))
		cfg.RouteIntentIssuerFile = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_ROUTE_INTENT_ISSUER_FILE"))
		cfg.RouteIntentIdentityNode = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_ROUTE_INTENT_IDENTITY_NODE_ID"))
		cfg.RouteIntentCAFile = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_ROUTE_INTENT_CA_FILE"))
		cfg.RouteIntentServerName = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_ROUTE_INTENT_SERVER_NAME"))
	}
	if authorityRuntimeEnabled {
		cfg.InventoryKeyringDir = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_INVENTORY_WRITER_KEYRING_DIR"))
		cfg.AuthorityStateDir = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_AUTHORITY_STATE_DIR"))
		cfg.AuthorityGroupIDs, err = parseEdgeGroupIDs(getenv("FUGUE_EDGE_CONTROL_AUTHORITY_GROUP_IDS"))
		if err != nil {
			return config{}, fmt.Errorf("FUGUE_EDGE_CONTROL_AUTHORITY_GROUP_IDS: %w", err)
		}
		cfg.AuthorityPollInterval = defaultAuthorityInterval
		if raw := strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_AUTHORITY_RECONCILE_INTERVAL")); raw != "" {
			cfg.AuthorityPollInterval, err = time.ParseDuration(raw)
			if err != nil {
				return config{}, errors.New("FUGUE_EDGE_CONTROL_AUTHORITY_RECONCILE_INTERVAL must be a duration")
			}
		}
		cfg.GroupSigningKeyringDir = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_GROUP_SIGNING_KEYRING_DIR"))
		cfg.GroupReaderKeyringDir = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_GROUP_READER_KEYRING_DIR"))
		cfg.GroupRecoveryKeyringDir = strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_GROUP_RECOVERY_KEYRING_DIR"))
		cfg.GroupBundleValidity = defaultGroupBundleValidity
		if raw := strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_GROUP_BUNDLE_VALIDITY")); raw != "" {
			cfg.GroupBundleValidity, err = time.ParseDuration(raw)
			if err != nil {
				return config{}, errors.New("FUGUE_EDGE_CONTROL_GROUP_BUNDLE_VALIDITY must be a duration")
			}
		}
		cfg.CandidatePublisher, err = strictBool(getenv("FUGUE_EDGE_CONTROL_CANDIDATE_PUBLISHER_ENABLED"))
		if err != nil {
			return config{}, fmt.Errorf("FUGUE_EDGE_CONTROL_CANDIDATE_PUBLISHER_ENABLED: %w", err)
		}
		if cfg.CandidatePublisher {
			imageRef := strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_SELF_IMAGE_REF"))
			separator := strings.LastIndex(imageRef, "@")
			if separator < 1 {
				return config{}, errors.New("FUGUE_EDGE_CONTROL_SELF_IMAGE_REF must be an immutable image reference")
			}
			cfg.CandidateIdentity = edgecontrol.CandidateReleaseIdentity{
				SourceSHA: strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_SOURCE_SHA")), ControlImageDigest: imageRef[separator+1:],
				ManifestDigest:       strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_MANIFEST_DIGEST")),
				HealthContractDigest: strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_HEALTH_CONTRACT_DIGEST")),
				ReleaseRecordDigest:  strings.TrimSpace(getenv("FUGUE_EDGE_CONTROL_RELEASE_RECORD_DIGEST")),
			}
		}
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg config) validate() error {
	host, port, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		return errors.New("bind address must be host:port")
	}
	if host == "" {
		return errors.New("bind address host is required")
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1 || parsedPort > 65535 {
		return errors.New("bind address port must be between 1 and 65535")
	}
	if cfg.ShutdownTimeout <= 0 || cfg.ShutdownTimeout > 2*time.Minute {
		return errors.New("shutdown timeout must be positive and no greater than 2m")
	}
	if !cfg.AuthorityRuntimeEnabled {
		return nil
	}
	if !cfg.Enabled {
		return errors.New("edge-control runtime requires FUGUE_EDGE_CONTROL_ENABLED=true")
	}
	if cfg.AuthorityRuntimeEnabled {
		if cfg.AuthorityStateDir == "" || !filepath.IsAbs(cfg.AuthorityStateDir) || filepath.Clean(cfg.AuthorityStateDir) != cfg.AuthorityStateDir {
			return errors.New("authority state directory must be an absolute normalized path")
		}
		if len(cfg.AuthorityGroupIDs) != 1 {
			return errors.New("authority runtime requires exactly one edge group per process fault domain")
		}
		if cfg.AuthorityPollInterval < minReconcileInterval || cfg.AuthorityPollInterval > maxReconcileInterval {
			return errors.New("authority reconcile interval must be between 5s and 5m")
		}
		paths := []string{cfg.InventoryKeyringDir, cfg.GroupSigningKeyringDir, cfg.GroupReaderKeyringDir, cfg.GroupRecoveryKeyringDir}
		seen := make(map[string]struct{}, len(paths))
		for _, path := range paths {
			if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
				return errors.New("authority keyring directories must be absolute normalized paths")
			}
			if _, duplicate := seen[path]; duplicate {
				return errors.New("authority keyring directories must be distinct")
			}
			seen[path] = struct{}{}
		}
		if cfg.GroupBundleValidity < 5*time.Minute || cfg.GroupBundleValidity > 24*time.Hour {
			return errors.New("group bundle validity must be between 5m and 24h")
		}
		if cfg.CandidatePublisher && cfg.CandidateIdentity.Validate() != nil {
			return errors.New("candidate publisher release identity is invalid")
		}
	}
	if err := edgecontrol.ValidateRouteIntentClientConfig(edgecontrol.RouteIntentClientConfig{
		Endpoint: cfg.RouteIntentURL, IssuerFile: cfg.RouteIntentIssuerFile, IdentityNodeID: cfg.RouteIntentIdentityNode, CAFile: cfg.RouteIntentCAFile, ServerName: cfg.RouteIntentServerName,
	}); err != nil {
		return err
	}
	return nil
}

func buildAuthorityProcess(cfg config) (*edgecontrol.AuthorityRuntime, http.Handler, error) {
	if err := cfg.validate(); err != nil {
		return nil, nil, err
	}
	if !cfg.AuthorityRuntimeEnabled {
		return nil, nil, nil
	}
	store, err := edgecontrol.OpenPersistentGroupStore(cfg.AuthorityStateDir)
	if err != nil {
		return nil, nil, err
	}
	client, err := edgecontrol.NewRouteIntentClient(edgecontrol.RouteIntentClientConfig{
		Endpoint: cfg.RouteIntentURL, IssuerFile: cfg.RouteIntentIssuerFile, IdentityNodeID: cfg.RouteIntentIdentityNode, CAFile: cfg.RouteIntentCAFile, ServerName: cfg.RouteIntentServerName,
	})
	if err != nil {
		return nil, nil, err
	}
	signer, err := edgecontrol.NewProjectedGroupBundleSigner(cfg.GroupSigningKeyringDir, cfg.GroupBundleValidity)
	if err != nil {
		return nil, nil, err
	}
	runtime := &edgecontrol.AuthorityRuntime{
		RouteIntents: client,
		Compiler:     edgecontrol.GroupShadowCompiler{Inventory: store, Ledger: store, InventoryMaxAge: edgecontrol.GroupInventoryHeartbeatMaxAge},
		Publisher:    edgecontrol.GroupAuthorityPublisher{Store: store, Signer: signer},
		GroupIDs:     append([]string(nil), cfg.AuthorityGroupIDs...),
		Status:       edgecontrol.NewAuthorityRuntimeState(nil),
	}
	if cfg.CandidatePublisher {
		candidate := edgecontrol.GroupCandidatePublisher{Store: store, Signer: signer, CurrentLKG: &runtime.Publisher, Identity: cfg.CandidateIdentity}
		runtime.Candidate = &candidate
	}
	heartbeat, err := edgecontrol.NewGroupInventoryHeartbeatHandler(edgecontrol.GroupInventoryHeartbeatHandlerConfig{
		Store: store, GroupIDs: cfg.AuthorityGroupIDs, KeyringDir: cfg.InventoryKeyringDir,
		Authority: "edge-control", PublicationEnabled: true, Path: edgecontrol.GroupAuthorityInventoryHeartbeatPathV1,
	})
	if err != nil {
		return nil, nil, err
	}
	status, err := edgecontrol.NewAuthorityStatusHandler(store, cfg.AuthorityGroupIDs, runtime.Status, nil)
	if err != nil {
		return nil, nil, err
	}
	bundles, err := edgecontrol.NewGroupBundleHandler(edgecontrol.GroupBundleHandlerConfig{
		Store: store, GroupIDs: cfg.AuthorityGroupIDs, KeyringDir: cfg.GroupReaderKeyringDir,
	})
	if err != nil {
		return nil, nil, err
	}
	recovery, err := edgecontrol.NewGroupRecoveryHandler(edgecontrol.GroupRecoveryHandlerConfig{
		Store: store, Signer: signer, GroupIDs: cfg.AuthorityGroupIDs, KeyringDir: cfg.GroupRecoveryKeyringDir,
	})
	if err != nil {
		return nil, nil, err
	}
	promotion, err := edgecontrol.NewGroupPromotionHandler(edgecontrol.GroupPromotionHandlerConfig{
		Store: store, Signer: signer, GroupIDs: cfg.AuthorityGroupIDs, KeyringDir: cfg.GroupRecoveryKeyringDir,
	})
	if err != nil {
		return nil, nil, err
	}
	handler, err := edgecontrol.NewAuthorityControlHandler(edgecontrol.NewAuthorityBoundary(cfg.Enabled).Handler(), heartbeat, status, bundles, recovery, promotion)
	if err != nil {
		return nil, nil, err
	}
	return runtime, handler, nil
}

func parseEdgeGroupIDs(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	groups := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		groupID := strings.TrimSpace(part)
		if groupID == "" || groupID != part || groupID != strings.ToLower(groupID) || !edgeGroupIDPattern.MatchString(groupID) {
			return nil, errors.New("edge group ids must be canonical lowercase values")
		}
		if _, duplicate := seen[groupID]; duplicate {
			return nil, errors.New("edge group ids must be unique")
		}
		seen[groupID] = struct{}{}
		groups = append(groups, groupID)
	}
	if len(groups) == 0 {
		return nil, errors.New("at least one edge group id is required")
	}
	sort.Strings(groups)
	return groups, nil
}

func strictBool(value string) (bool, error) {
	switch strings.TrimSpace(value) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, errors.New("must be exactly true or false")
	}
}
