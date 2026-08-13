package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	if err := run(); err != nil {
		log.Printf("release-guardian: %v", err)
		os.Exit(1)
	}
}

func run() error {
	role := strings.TrimSpace(os.Getenv("FUGUE_RELEASE_GUARDIAN_ROLE"))
	if role != "guardian" && role != "canary-prober" {
		return errors.New("FUGUE_RELEASE_GUARDIAN_ROLE must be guardian or canary-prober")
	}
	targets, err := parseTargets(os.Getenv("FUGUE_RELEASE_GUARDIAN_TARGETS"))
	if err != nil {
		return err
	}
	config, err := loadKubeConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	store, err := releaseguardian.NewKubeStore(client, targets)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if role == "canary-prober" {
		probes, err := parseProbes(os.Getenv("FUGUE_RELEASE_GUARDIAN_CANARY"))
		if err != nil {
			return err
		}
		candidateProbes, err := parseCandidateCanaryProbes(os.Getenv("FUGUE_RELEASE_GUARDIAN_CANDIDATE_CANARY"))
		if err != nil {
			return err
		}
		if len(candidateProbes) > 0 {
			authorityStore, err := releaseguardian.NewAuthorityStore(client, targets[0].Namespace)
			if err != nil {
				return err
			}
			startCandidateCanaryProbers(ctx, authorityStore, client, targets[0].Namespace, candidateProbes)
		}
		return runCanaryProbers(ctx, store, probes)
	}
	return runGuardian(ctx, config, client, store, targets)
}

func runGuardian(ctx context.Context, kubeConfig *rest.Config, client kubernetes.Interface, store *releaseguardian.KubeStore, targets []releaseguardian.TargetConfig) error {
	mode := releaseguardian.Mode(strings.TrimSpace(os.Getenv("FUGUE_RELEASE_GUARDIAN_MODE")))
	if mode == "" {
		mode = releaseguardian.ModeShadow
	}
	var executor releaseguardian.Executor
	if mode == releaseguardian.ModeWrite {
		value, err := releaseguardian.NewProcessExecutor("/usr/local/bin/fugue-declarative-release", os.Getenv("FUGUE_RELEASE_GUARDIAN_POD_UID"))
		if err != nil {
			return err
		}
		executor = value
	} else if mode != releaseguardian.ModeShadow {
		return errors.New("release Guardian mode is invalid")
	}
	controller, err := releaseguardian.NewController(mode, store, executor)
	if err != nil {
		return err
	}
	authorityStore, err := releaseguardian.NewAuthorityStore(client, targets[0].Namespace)
	if err != nil {
		return err
	}
	imports, err := parseCandidateImports(os.Getenv("FUGUE_RELEASE_GUARDIAN_CANDIDATE_IMPORTS"))
	if err != nil {
		return err
	}
	startCandidateImporters(ctx, authorityStore, client, imports)
	baselines, err := parseAuthorityBaselines(os.Getenv("FUGUE_RELEASE_GUARDIAN_AUTHORITY_BASELINES"))
	if err != nil {
		return err
	}
	startAuthorityBaselineAdopters(ctx, authorityStore, client, targets[0].Namespace, baselines)
	authority, err := newAuthorityRuntimeWithActivators(authorityStore, client, kubeConfig, targets[0].Namespace,
		os.Getenv("FUGUE_RELEASE_GUARDIAN_AUTHORITY_GROUPS"), os.Getenv("FUGUE_RELEASE_GUARDIAN_AUTHORITY_ACTIVATORS"))
	if err != nil {
		return err
	}
	authority.Start(ctx)
	namespace := targets[0].Namespace
	for _, target := range targets[1:] {
		if target.Namespace != namespace {
			return errors.New("one Guardian process cannot span namespaces")
		}
	}
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace(namespace))
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(value any) { enqueueEvent(controller, authority, store.Keys(), value) },
		UpdateFunc: func(_, value any) { enqueueEvent(controller, authority, store.Keys(), value) },
		DeleteFunc: func(value any) { enqueueEvent(controller, authority, store.Keys(), value) },
	}
	informersToWatch := []cache.SharedIndexInformer{
		factory.Core().V1().ConfigMaps().Informer(),
		factory.Core().V1().Pods().Informer(),
		factory.Core().V1().Events().Informer(),
		factory.Apps().V1().Deployments().Informer(),
		factory.Apps().V1().DaemonSets().Informer(),
		factory.Discovery().V1().EndpointSlices().Informer(),
		factory.Coordination().V1().Leases().Informer(),
	}
	for _, informer := range informersToWatch {
		if _, err := informer.AddEventHandler(handler); err != nil {
			return err
		}
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), func() bool {
		for _, informer := range informersToWatch {
			if !informer.HasSynced() {
				return false
			}
		}
		return true
	}) {
		return errors.New("release Guardian informer cache did not synchronize")
	}
	for _, key := range store.Keys() {
		if err := controller.Enqueue(key); err != nil {
			return err
		}
	}
	go enqueueFreshness(ctx, controller, store.Keys(), 20*time.Second)
	return controller.Run(ctx, len(store.Keys()))
}

func enqueueEvent(controller *releaseguardian.Controller, authority *authorityRuntime, keys []releaseguardian.Key, value any) {
	if metadata, ok := value.(metav1.Object); ok && strings.HasPrefix(metadata.GetName(), "fugue-release-status-") {
		return
	}
	for _, key := range keys {
		_ = controller.Enqueue(key)
	}
	authority.EnqueueAll()
}

func enqueueFreshness(ctx context.Context, controller *releaseguardian.Controller, keys []releaseguardian.Key, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, key := range keys {
				_ = controller.Enqueue(key)
			}
		}
	}
}

type canaryProbe struct {
	Key      releaseguardian.Key
	Address  string
	Host     string
	Path     string
	Expected string
	Interval time.Duration
}

func runCanaryProber(ctx context.Context, store *releaseguardian.KubeStore, probe canaryProbe) error {
	ticker := time.NewTicker(probe.Interval)
	defer ticker.Stop()
	for {
		if err := probeOnce(ctx, store, probe); err != nil {
			log.Printf("canary %s: %v", probe.Key.String(), err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func runCanaryProbers(ctx context.Context, store *releaseguardian.KubeStore, probes []canaryProbe) error {
	if len(probes) == 0 {
		return errors.New("release Guardian has no canary probe")
	}
	done := make(chan struct{}, len(probes))
	for _, probe := range probes {
		probe := probe
		go func() {
			_ = runCanaryProber(ctx, store, probe)
			done <- struct{}{}
		}()
	}
	<-ctx.Done()
	for range probes {
		<-done
	}
	return nil
}

func probeOnce(ctx context.Context, store *releaseguardian.KubeStore, probe canaryProbe) error {
	record, err := store.LoadRecord(ctx, probe.Key)
	if err != nil {
		return err
	}
	observedAt := time.Now().UTC()
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	status, body, probeErr := requestPublicRoute(probeCtx, probe)
	state := releaseguardian.HealthHealthy
	if probeErr != nil || status < 200 || status >= 300 || (probe.Expected != "" && !strings.Contains(string(body), probe.Expected)) {
		state = releaseguardian.HealthDegraded
	}
	evidenceRaw, err := declarativerelease.CanonicalJSON(map[string]any{
		"address": probe.Address, "host": probe.Host, "path": probe.Path,
		"status": status, "bodyDigest": shaDigest(body), "transportError": errorClass(probeErr),
	})
	if err != nil {
		return err
	}
	result, err := releaseguardian.NewCanaryResult(record, state, shaDigest(evidenceRaw), observedAt, observedAt.Add(3*probe.Interval))
	if err != nil {
		return err
	}
	return store.PutCanaryResult(ctx, result)
}

func requestPublicRoute(ctx context.Context, probe canaryProbe) (int, []byte, error) {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, probe.Address)
		},
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS12, ServerName: probe.Host},
		DisableKeepAlives: true,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+probe.Host+probe.Path, nil)
	if err != nil {
		return 0, nil, err
	}
	request.Host = probe.Host
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	return response.StatusCode, body, err
}

func parseTargets(value string) ([]releaseguardian.TargetConfig, error) {
	var targets []releaseguardian.TargetConfig
	for _, raw := range strings.Split(strings.TrimSpace(value), ";") {
		fields := strings.Split(raw, ",")
		if len(fields) != 5 {
			return nil, errors.New("guardian target must be component,group,namespace,monitor-component,dependency-service")
		}
		target := releaseguardian.TargetConfig{Key: releaseguardian.Key{Component: strings.TrimSpace(fields[0]), Group: strings.TrimSpace(fields[1])}, Namespace: strings.TrimSpace(fields[2]), MonitorComponent: strings.TrimSpace(fields[3]), DependencyService: strings.TrimSpace(fields[4])}
		if err := target.Validate(); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return nil, errors.New("release Guardian has no target")
	}
	return targets, nil
}

func parseProbe(value string) (canaryProbe, error) {
	fields := strings.Split(strings.TrimSpace(value), ",")
	if len(fields) != 7 {
		return canaryProbe{}, errors.New("canary must be component,group,address,host,path,expected,interval-seconds")
	}
	intervalSeconds, err := strconv.Atoi(strings.TrimSpace(fields[6]))
	probe := canaryProbe{
		Key:     releaseguardian.Key{Component: strings.TrimSpace(fields[0]), Group: strings.TrimSpace(fields[1])},
		Address: strings.TrimSpace(fields[2]), Host: strings.TrimSpace(fields[3]), Path: strings.TrimSpace(fields[4]), Expected: strings.TrimSpace(fields[5]),
		Interval: time.Duration(intervalSeconds) * time.Second,
	}
	if err != nil || probe.Key.Validate() != nil || probe.Interval < 5*time.Second || probe.Interval > time.Minute ||
		probe.Address == "" || probe.Host == "" || !strings.HasPrefix(probe.Path, "/") || strings.ContainsAny(probe.Address+probe.Host+probe.Path+probe.Expected, "\r\n\x00") {
		return canaryProbe{}, errors.New("canary configuration is invalid")
	}
	return probe, nil
}

func parseProbes(value string) ([]canaryProbe, error) {
	var probes []canaryProbe
	seen := map[releaseguardian.Key]bool{}
	for _, raw := range strings.Split(strings.TrimSpace(value), ";") {
		probe, err := parseProbe(raw)
		if err != nil {
			return nil, err
		}
		if seen[probe.Key] {
			return nil, errors.New("canary target is duplicated")
		}
		seen[probe.Key] = true
		probes = append(probes, probe)
	}
	if len(probes) == 0 {
		return nil, errors.New("release Guardian has no canary probe")
	}
	return probes, nil
}

func loadKubeConfig() (*rest.Config, error) {
	if config, err := rest.InClusterConfig(); err == nil {
		return config, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil || config == nil || strings.TrimSpace(config.Host) == "" {
		return nil, errors.New("Kubernetes client configuration is unavailable")
	}
	return config, nil
}

func shaDigest(value []byte) string {
	sum := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", sum)
}

func errorClass(err error) string {
	if err == nil {
		return ""
	}
	value := strings.TrimSpace(err.Error())
	if len(value) > 256 {
		value = value[:256]
	}
	return value
}

// Marshal-only compile guard: the executable never accepts arbitrary JSON
// command payloads, and its emitted records remain strict canonical objects.
var _ = json.Valid
