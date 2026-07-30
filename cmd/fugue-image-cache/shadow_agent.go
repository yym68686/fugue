package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	imageCacheProcessModePlatformPlanShadow = "platform-plan-shadow"
	imageCachePlatformPlanAgentShutdownTime = 25 * time.Second
)

func imageCacheProcessMode(args []string) (string, error) {
	if len(args) == 0 {
		return "", nil
	}
	if len(args) == 1 && args[0] == imageCacheProcessModePlatformPlanShadow {
		return imageCacheProcessModePlatformPlanShadow, nil
	}
	return "", errors.New("usage: fugue-image-cache [platform-plan-shadow]")
}

func runImageCachePlatformPlanAgent(lifecycle context.Context) error {
	if lifecycle == nil {
		return errors.New("image-plane shadow lifecycle is nil")
	}
	if err := lifecycle.Err(); err != nil {
		return fmt.Errorf("image-plane shadow lifecycle is already stopped: %w", err)
	}
	listenAddr := env("FUGUE_IMAGE_CACHE_LISTEN_ADDR", "127.0.0.1:5001")
	if err := validateImageCachePlatformPlanAgentListenAddress(listenAddr); err != nil {
		return err
	}
	apiBase := strings.TrimRight(env("FUGUE_API_BASE", os.Getenv("FUGUE_API_URL")), "/")
	clusterNode := env("FUGUE_IMAGE_CACHE_CLUSTER_NODE_NAME", os.Getenv("NODE_NAME"))
	owner := &imageCachePlatformPlanAgent{clusterNode: clusterNode, lifecycle: lifecycle}
	platformPlan, platformPlanErr := newImageCachePlatformPlanConsumerFromEnvironment(apiBase, clusterNode)
	if platformPlanErr != nil {
		owner.platformPlanErr = boundedImageCachePlatformError(platformPlanErr)
		log.Printf("image-plane shadow agent configuration is invalid: %v", platformPlanErr)
	} else if platformPlan != nil {
		owner.platformPlan = platformPlan
		if !owner.startBackground(platformPlan.Run) {
			return errors.New("image-plane shadow consumer could not start because its lifecycle is stopping")
		}
		log.Printf("image-plane shadow agent enabled node=%s credential_file=%s observation_file=%s", clusterNode, platformPlan.config.CredentialPath, platformPlan.config.ObservationPath)
	} else {
		log.Print("image-plane shadow agent is explicitly disabled and will remain unready")
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen on isolated image-plane shadow address %s: %w", listenAddr, err)
	}
	server := &http.Server{
		Addr:              listenAddr,
		Handler:           &imageCachePlatformPlanAgentHandler{owner: owner},
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	log.Printf("image-plane shadow agent listening on Pod loopback %s", listener.Addr().String())
	return runImageCacheHTTPServer(
		lifecycle,
		server,
		listener,
		imageCachePlatformPlanAgentShutdownTime,
		owner.waitForBackground,
	)
}

func validateImageCachePlatformPlanAgentListenAddress(address string) error {
	host, portRaw, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return errors.New("image-plane shadow listen address must be a numeric loopback host and port")
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil || !ip.IsLoopback() {
		return errors.New("image-plane shadow listen address must stay on numeric Pod loopback")
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port < 1024 || port > 65535 {
		return errors.New("image-plane shadow listen port must be between 1024 and 65535")
	}
	return nil
}

type imageCachePlatformPlanAgentHandler struct {
	owner *imageCachePlatformPlanAgent
}

func (h *imageCachePlatformPlanAgentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store, max-age=0")
	if r == nil || r.URL == nil {
		http.NotFound(w, r)
		return
	}
	path := strings.TrimRight(r.URL.Path, "/")
	if path == "" {
		path = "/"
	}
	known := path == "/healthz" || path == "/readyz" ||
		path == "/fugue/cache/v1/health" || path == "/fugue/cache/v1/platform-plan/readyz"
	if known && r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	switch path {
	case "/healthz":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	case "/readyz", "/fugue/cache/v1/platform-plan/readyz":
		h.serveReadiness(w, time.Now().UTC())
	case "/fugue/cache/v1/health":
		h.serveHealth(w)
	default:
		http.NotFound(w, r)
	}
}

func (h *imageCachePlatformPlanAgentHandler) serveReadiness(w http.ResponseWriter, now time.Time) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if h == nil || h.owner == nil || !h.owner.platformPlanReady(now) {
		http.Error(w, "platform plan shadow is not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

type imageCachePlatformPlanAgent struct {
	clusterNode     string
	platformPlan    *imageCachePlatformPlanConsumer
	platformPlanErr string
	lifecycle       context.Context
	background      sync.WaitGroup
}

func (a *imageCachePlatformPlanAgent) backgroundContext() context.Context {
	if a != nil && a.lifecycle != nil {
		return a.lifecycle
	}
	return context.Background()
}

func (a *imageCachePlatformPlanAgent) startBackground(work func(context.Context)) bool {
	if a == nil || work == nil {
		return false
	}
	ctx := a.backgroundContext()
	if ctx.Err() != nil {
		return false
	}
	a.background.Add(1)
	go func() {
		defer a.background.Done()
		work(ctx)
	}()
	return true
}

func (a *imageCachePlatformPlanAgent) waitForBackground(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("image-plane shadow background drain context is nil")
	}
	done := make(chan struct{})
	go func() {
		a.background.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *imageCachePlatformPlanAgent) platformPlanReady(now time.Time) bool {
	if a == nil || a.platformPlan == nil || a.platformPlanErr != "" {
		return false
	}
	status := a.platformPlan.Status()
	if !status.Enabled || !status.ObservationOnly || status.State != "observed" || status.LastObservationAt == nil {
		return false
	}
	observedAt := status.LastObservationAt.UTC()
	now = now.UTC()
	return !observedAt.After(now.Add(30*time.Second)) && observedAt.After(now.Add(-imageCachePlatformPlanReadinessMaxAge))
}

func (h *imageCachePlatformPlanAgentHandler) serveHealth(w http.ResponseWriter) {
	status := imageCachePlatformPlanStatus{
		Enabled:         false,
		ObservationOnly: true,
		State:           "disabled",
	}
	clusterNode := ""
	if h != nil && h.owner != nil {
		clusterNode = h.owner.clusterNode
		if h.owner.platformPlan != nil {
			status = h.owner.platformPlan.Status()
		} else if h.owner.platformPlanErr != "" {
			status.State = "configuration_error"
			status.LastError = h.owner.platformPlanErr
		}
	}
	writeManagementJSON(w, http.StatusOK, map[string]any{
		"status":               "ok",
		"mode":                 imageCacheProcessModePlatformPlanShadow,
		"cluster_node":         clusterNode,
		"platform_plan_shadow": status,
	})
}
