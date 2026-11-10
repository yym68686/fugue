package api

import (
	"context"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/inputarchive"
	"fugue/internal/store"
)

const pendingRootAudience = inputarchive.PendingRootAudience
const pendingRootPath = inputarchive.PendingRootHTTPPath

var pendingRootGroup = regexp.MustCompile(`^edge-control/(?:edge-group-|cell-)[a-z0-9]+(?:-[a-z0-9]+)*$`)
var pendingRootSourceSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)
var pendingRootImage = regexp.MustCompile(`^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$`)

// Trusted process configuration, never decoded from an HTTP request or a Pod
// annotation. A new policy must be explicitly installed; there is no default
// credential, verifier, reservation provider or production enablement.
type PendingRootAuthority struct {
	Owner, Namespace, ServiceAccount, ServiceAccountUID string
	Deployment, DeploymentUID, SourceSHA, ImageRef      string
	PolicyRevision                                      string
}

type PendingRootTransportConfig struct {
	Authorities     []PendingRootAuthority
	SourceVerifiers map[string]inputarchive.PendingRootSourceVerifier
	Admission       inputarchive.PendingRootAdmission
}

type pendingRootTransport struct {
	authorities map[string]PendingRootAuthority // exact Deployment UID -> owner
	verifiers   map[string]inputarchive.PendingRootSourceVerifier
	retired     atomic.Bool
}

type pendingRootPrincipal struct {
	transport *pendingRootTransport
	authority PendingRootAuthority
	podUID    string
	deadline  time.Time
}
type pendingRootPrincipalKey struct{}

// ConfigurePendingRootTransport also invalidates prepared Store witnesses.
// Configuration selects transport verification only. The Store additionally
// requires a matching live native authorization lease and a complete pending
// registry witness. Neither establishes complete external custody or a real
// whole-operation allocation budget. No command invokes this implicitly.
func (s *Server) ConfigurePendingRootTransport(config *PendingRootTransportConfig) error {
	if s == nil || s.store == nil {
		return store.ErrConflict
	}
	s.pendingRootConfigureMu.Lock()
	defer s.pendingRootConfigureMu.Unlock()
	if config == nil {
		s.store.ConfigurePendingRootAdmission(nil, nil)
		if previous := s.pendingRootTransport.Swap(nil); previous != nil {
			previous.retired.Store(true)
		}
		return nil
	}
	if config.Admission == nil || len(config.Authorities) == 0 || len(config.Authorities) > 32 || len(config.SourceVerifiers) != len(config.Authorities) {
		return store.ErrInvalidInput
	}
	candidate := &pendingRootTransport{authorities: map[string]PendingRootAuthority{}, verifiers: map[string]inputarchive.PendingRootSourceVerifier{}}
	for _, a := range config.Authorities {
		if !pendingRootGroup.MatchString(a.Owner) || a.Namespace != s.controlPlaneNamespace || a.Namespace == "" ||
			!pendingRootSourceSHA.MatchString(a.SourceSHA) || !pendingRootImage.MatchString(a.ImageRef) {
			return store.ErrInvalidInput
		}
		for _, value := range []string{a.ServiceAccount, a.ServiceAccountUID, a.Deployment, a.DeploymentUID, a.PolicyRevision} {
			if value == "" || len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n\t/") {
				return store.ErrInvalidInput
			}
		}
		if _, exists := candidate.authorities[a.DeploymentUID]; exists || candidate.verifiers[a.Owner] != nil || config.SourceVerifiers[a.Owner] == nil {
			return store.ErrInvalidInput
		}
		candidate.authorities[a.DeploymentUID], candidate.verifiers[a.Owner] = a, config.SourceVerifiers[a.Owner]
	}
	// Store's configuration lock serializes this boundary with final commits.
	// Publishing the new auth snapshot afterwards can only cause old in-flight
	// requests to be refused; it cannot authorize one under the wrong verifier.
	s.store.ConfigurePendingRootAdmission(candidate, config.Admission)
	if previous := s.pendingRootTransport.Swap(candidate); previous != nil {
		previous.retired.Store(true)
	}
	return nil
}

func (p *pendingRootPrincipal) check(ctx context.Context) error {
	if p == nil || p.transport == nil || p.transport.retired.Load() || ctx.Err() != nil || !time.Now().Before(p.deadline) || p.podUID == "" || p.transport.authorities[p.authority.DeploymentUID] != p.authority {
		return store.ErrConflict
	}
	return nil
}

type pendingRootAuthenticatedWitness struct {
	principal *pendingRootPrincipal
	source    inputarchive.PendingRootSourceWitness
}

func (w *pendingRootAuthenticatedWitness) PendingRootAuthority() inputarchive.PendingRootAuthority {
	if w == nil || w.principal == nil {
		return inputarchive.PendingRootAuthority{}
	}
	p, a := w.principal, w.principal.authority
	return inputarchive.PendingRootAuthority{Owner: a.Owner, Namespace: a.Namespace, ServiceAccount: a.ServiceAccount, ServiceAccountUID: a.ServiceAccountUID,
		Deployment: a.Deployment, DeploymentUID: a.DeploymentUID, PodUID: p.podUID, SourceSHA: a.SourceSHA, ImageRef: a.ImageRef, PolicyRevision: a.PolicyRevision}
}

func (t *pendingRootTransport) VerifyPendingRootSource(ctx context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
	p, _ := ctx.Value(pendingRootPrincipalKey{}).(*pendingRootPrincipal)
	if p == nil || p.transport != t || p.check(ctx) != nil || r.Registration.Owner != p.authority.Owner {
		return nil, store.ErrConflict
	}
	verifier := t.verifiers[p.authority.Owner] // chosen by authenticated authority, not request owner
	if verifier == nil {
		return nil, store.ErrConflict
	}
	witness, err := verifier.VerifyPendingRootSource(ctx, r)
	if err != nil || witness == nil {
		return nil, store.ErrConflict
	}
	return &pendingRootAuthenticatedWitness{p, witness}, nil
}

func (w *pendingRootAuthenticatedWitness) CheckPendingRootSource(ctx context.Context, r inputarchive.PendingRootRequest) error {
	if w == nil || w.principal.check(ctx) != nil || r.Registration.Owner != w.principal.authority.Owner {
		return store.ErrConflict
	}
	return w.source.CheckPendingRootSource(ctx, r)
}

func (s *Server) requirePendingRootPod(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Public ingress, query credentials and aliases never reach Kubernetes,
		// the request body or the Store. Exact private TLS remains mandatory even
		// when a caller invokes the generated API mux through another listener.
		host := r.Host
		if parsed, port, err := net.SplitHostPort(host); err == nil {
			if port != "8443" {
				http.NotFound(w, r)
				return
			}
			host = parsed
		}
		expectedHost := "fugue-api-tls." + s.controlPlaneNamespace + ".svc"
		if s.controlPlaneNamespace == "" || r.TLS == nil || r.TLS.ServerName != expectedHost || host != expectedHost || r.Method != http.MethodPost || r.URL.EscapedPath() != pendingRootPath || r.URL.RawQuery != "" || r.URL.ForceQuery {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		transport := s.pendingRootTransport.Load()
		if transport == nil || transport.retired.Load() {
			httpx.WriteError(w, http.StatusServiceUnavailable, "pending-root admission is disabled")
			return
		}
		values := r.Header.Values("Authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || len(values[0]) > 32768+7 || len(strings.Fields(values[0])) != 2 {
			httpx.WriteError(w, http.StatusUnauthorized, "dedicated bound Pod credential required")
			return
		}
		deadline := time.Now().Add(time.Minute)
		if parent, ok := r.Context().Deadline(); ok && parent.Before(deadline) {
			deadline = parent
		}
		// A single deadline, never reset after body reads, Kube calls or SQL.
		controller := http.NewResponseController(w)
		if controller.SetReadDeadline(deadline) != nil || controller.SetWriteDeadline(deadline) != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "bounded private transport unavailable")
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), deadline)
		defer cancel()
		p, status := s.verifyPendingRootPod(ctx, strings.TrimPrefix(values[0], "Bearer "), transport)
		if status != http.StatusOK {
			httpx.WriteError(w, status, "pending-root Pod authority could not be verified")
			return
		}
		p.deadline = deadline
		ctx = context.WithValue(ctx, pendingRootPrincipalKey{}, p)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) handleRetainPendingCompilerInputRoot(w http.ResponseWriter, r *http.Request) {
	p, _ := r.Context().Value(pendingRootPrincipalKey{}).(*pendingRootPrincipal)
	if p == nil || p.check(r.Context()) != nil {
		httpx.WriteError(w, http.StatusUnauthorized, "verified pending-root authority required")
		return
	}
	contentType, parameters, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || len(parameters) > 1 || len(parameters) == 1 && parameters["charset"] != "utf-8" {
		httpx.WriteError(w, http.StatusUnsupportedMediaType, "application/json required")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, inputarchive.PendingRootHTTPBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		status := http.StatusBadRequest
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		httpx.WriteError(w, status, "bounded pending-root request unreadable")
		return
	}
	request, err := inputarchive.DecodePendingRootHTTP(raw)
	if err != nil || request.SourceSchema != "fugue.edge-control.pending-root-source.v1" || request.Registration.Owner != p.authority.Owner {
		if errors.Is(err, inputarchive.ErrPendingRootTooLarge) {
			httpx.WriteError(w, http.StatusRequestEntityTooLarge, "pending-root source exceeds bound")
			return
		}
		httpx.WriteError(w, http.StatusBadRequest, "pending-root request invalid")
		return
	}
	receipt, err := s.store.RetainPendingCompilerInputExternalRoot(r.Context(), request)
	if err != nil || !receipt.Matches(request.Registration) {
		httpx.WriteError(w, http.StatusConflict, "pending-root registration not confirmed")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, receipt)
}
