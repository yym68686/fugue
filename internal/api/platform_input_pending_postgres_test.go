package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/edgecontrol"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/schemamigrate"
	"fugue/internal/store"
	"fugue/internal/trafficbinding"
)

// This covers authentic native source -> private TLS/Pod authentication ->
// formal Store -> PostgreSQL -> local publication. Kubernetes is an explicit
// TokenReview/metadata fixture and allocation is an explicit synthetic provider;
// neither is a production authority, real resource budget or cold-business E2E.
func TestPendingRootPostgresAuthenticatedSourceLostACKRestartAndRevocation(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("requires disposable loopback PostgreSQL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires disposable loopback fugue_test database")
	}
	base, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	schema := model.NewID("pending_http_test")
	if _, err := base.Exec(`CREATE SCHEMA "` + schema + `"`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := base.Exec(`DROP SCHEMA "` + schema + `" CASCADE`); err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	address = u.String()
	db, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	keys := bundleauth.NewKeyring("neutral-native-artifact-secret", "neutral-platform-key", "", "", nil)
	state := store.New("", address)
	if err := state.Init(); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateEdgeInstanceFencing(t.Context(), address); err != nil {
		t.Fatal(err)
	}
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	state.ConfigurePlatformArtifactSigning(keys)
	for _, ddl := range []string{schemamigrate.BoundedContentSQL, schemamigrate.CompilerInputArchiveSQL, schemamigrate.CompilerInputZeroTempSQL,
		schemamigrate.CompilerInputExternalRootsSQL, schemamigrate.CompilerInputPendingAuthoritiesSQL} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED", "true")
	input := []byte(`{"neutral":"retained original"}`)
	sum := sha256.Sum256(input)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,storage_location,created_at)
 VALUES($1,'runtime-snapshot-json-v1',$2,262144,1,'hot',now())`, digest, len(input)); err != nil {
		t.Fatal(err)
	}
	// No request in this test can consult the original old-chunks table or R2.
	if _, err := db.Exec(`DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
	artifact := func(id, kind string, content map[string]any) model.PlatformArtifact {
		t.Helper()
		a, err := state.CreatePlatformArtifact(model.PlatformArtifact{ID: id, ArtifactKind: kind, Generation: id, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	route := artifact("neutral-route", model.PlatformArtifactKindEdgeRouteBundle, map[string]any{"lineage": map[string]any{"input_snapshot_digest": digest}})
	set := artifact("neutral-release-set", model.PlatformArtifactKindReleaseSet, map[string]any{"artifact_ids": []string{route.ID}})
	if _, err := db.Exec(`INSERT INTO fugue_platform_artifact_releases(id,artifact_id,artifact_kind,scope_key,generation,release_channel,status,released_at,created_at,updated_at)
 VALUES('neutral-release',$1,$2,$3,$4,'full','superseded',now(),now(),now())`, set.ID, set.ArtifactKind, set.ScopeKey, set.Generation); err != nil {
		t.Fatal(err)
	}
	f := newPendingIdentityFixture()
	group := strings.TrimPrefix(f.a.Owner, "edge-control/")
	keyDir := filepath.Join(t.TempDir(), "keys")
	if err := os.Mkdir(keyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	keyPolicy := map[string]any{"schema": edgecontrol.GroupBundleSigningKeyringSchemaV1, "generation": 1,
		"group": map[string]any{"edge_group_id": group, "primary_key_id": "neutral-group-key", "primary_key": base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x73}, 32))}}
	rawKeys, err := json.Marshal(keyPolicy)
	if err != nil || os.WriteFile(filepath.Join(keyDir, group+".json"), rawKeys, 0o600) != nil {
		t.Fatal("private native key fixture", err)
	}
	verifier, err := edgecontrol.NewGroupPendingRootSourceVerifier(keyDir, group)
	if err != nil {
		t.Fatal(err)
	}
	a := f.a
	policy := inputarchive.PendingRootAuthorityPolicy{Schema: inputarchive.PendingRootAuthorityPolicySchema,
		Leases: []inputarchive.PendingRootAuthorityLease{{Authority: inputarchive.PendingRootAuthority{
			Owner: a.Owner, Namespace: a.Namespace, ServiceAccount: a.ServiceAccount, ServiceAccountUID: a.ServiceAccountUID,
			Deployment: a.Deployment, DeploymentUID: a.DeploymentUID, PodUID: string(f.pod.UID), SourceSHA: a.SourceSHA,
			ImageRef: a.ImageRef, PolicyRevision: a.PolicyRevision}, LeaseID: strings.Repeat("c", 64), ExpiresUnix: time.Now().Add(time.Minute).Unix()}}}
	policyRaw, err := policy.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_pending_authorities(singleton,revision,policy_json) VALUES(TRUE,1,$1)`, string(policyRaw)); err != nil {
		t.Fatal(err)
	}
	var reservations atomic.Int32
	configure := func(state *store.Store) *Server {
		s := pendingTestServer(t, f, verifier, nil)
		s.store = state
		if err := s.ConfigurePendingRootTransport(&PendingRootTransportConfig{Authorities: []PendingRootAuthority{a},
			SourceVerifiers: map[string]inputarchive.PendingRootSourceVerifier{a.Owner: verifier},
			Admission: pendingAdmissionFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
				reservations.Add(1)
				return pendingHTTPSyntheticReservation{}, nil
			})}); err != nil {
			t.Fatal(err)
		}
		return s
	}
	var current atomic.Pointer[Server]
	current.Store(configure(state))
	var requests atomic.Int32
	var loseReply atomic.Bool
	loseReply.Store(true)
	private := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		buffer := &pendingHTTPBufferedResponse{ResponseRecorder: httptest.NewRecorder(), underlying: w}
		s := current.Load()
		s.requirePendingRootPod(http.HandlerFunc(s.handleRetainPendingCompilerInputRoot)).ServeHTTP(buffer, r)
		if buffer.Code == http.StatusOK && loseReply.Swap(false) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close() // native commit succeeded, but no receipt reaches caller
			return
		}
		for k, v := range buffer.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(buffer.Code)
		_, _ = io.Copy(w, buffer.Body)
	}))
	host := "fugue-api-tls." + a.Namespace + ".svc"
	serverTLS, roots := pendingHTTPPrivatePKI(t, host)
	private.TLS = serverTLS
	private.StartTLS()
	t.Cleanup(private.Close)
	newRegistrar := func() *pendingHTTPNativeRegistrar {
		transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: host},
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", private.Listener.Addr().String())
			}}
		t.Cleanup(transport.CloseIdleConnections)
		return &pendingHTTPNativeRegistrar{client: &http.Client{Transport: transport, Timeout: time.Minute}, endpoint: "https://" + host + ":8443" + pendingRootPath}
	}
	registrar := newRegistrar()
	localDir := filepath.Join(t.TempDir(), "native-state")
	if err := os.Mkdir(localDir, 0o700); err != nil {
		t.Fatal(err)
	}
	openLocal := func(registrar edgecontrol.GroupPendingRootRegistrar) *edgecontrol.PersistentGroupStore {
		local, err := edgecontrol.OpenPersistentGroupStoreWithOptions(localDir, edgecontrol.PersistentGroupStoreOptions{
			RetainCompileInputs: true, CompileInputKeyringDir: keyDir, PendingRootRegistrar: registrar})
		if err != nil {
			t.Fatal(err)
		}
		return local
	}
	local := openLocal(registrar)
	now := time.Now().UTC()
	inventory := edgecontrol.GroupInventorySnapshot{Schema: edgecontrol.GroupInventorySchemaV1, GroupID: group, FaultDomainID: "domain-neutral", EdgePoolID: "pool-neutral",
		Sequence: 1, Generation: "inventory-neutral", ObservedAt: now,
		ActiveEpoch: edgecontrol.GroupActiveEpoch{GroupID: group, FaultDomainID: "domain-neutral", EdgePoolID: "pool-neutral", Slot: "a", ReleaseEpoch: "epoch-neutral", FenceSequence: 1, MinHealthyInstances: 1},
		Instances:   []edgecontrol.GroupInstance{{EdgeID: "edge-neutral", GroupID: group, FaultDomainID: "domain-neutral", EdgePoolID: "pool-neutral", Slot: "a", InstanceUID: "instance-neutral", ReleaseEpoch: "epoch-neutral", EffectiveHealthy: true, NodeHealthy: true, NodeStatus: model.EdgeHealthHealthy}}}
	if err := local.StoreGroupInventoryCAS(t.Context(), group, 0, inventory); err != nil {
		t.Fatal(err)
	}
	intent := model.EdgeRouteIntentSnapshot{SchemaVersion: model.EdgeRouteIntentSchemaVersionV1, Generation: "intent-neutral", GeneratedAt: now,
		Routes: []model.EdgeRouteIntent{{Generation: "route-neutral", Hostname: "neutral.example.test", PathPrefix: "/", RouteKind: model.EdgeRouteKindPlatform,
			TargetGroupMode: model.EdgeRouteIntentGroupModeAllGroups, MinHealthyEdgeNodes: 1, RoutePolicy: model.EdgeRoutePolicyEnabled,
			UpstreamKind: model.EdgeRouteUpstreamKindMesh, UpstreamScope: model.EdgeRouteUpstreamScopeMesh, UpstreamURL: "http://origin.mesh:8080", ServicePort: 8080,
			TLSPolicy: model.EdgeRouteTLSPolicyPlatform, OriginStatus: model.EdgeRouteStatusActive, CreatedAt: now, UpdatedAt: now}},
		TLSAllowlist: []model.EdgeTLSAllowlistEntry{{Hostname: "neutral.example.test", Status: model.EdgeRouteStatusActive, TLSStatus: model.EdgeTLSStatusReady}}}
	intent.TrafficRelease = &model.TrafficReleaseBinding{Schema: trafficbinding.Schema, ReleaseSetID: set.ID, ReleaseSetDigest: set.ContentHash, ReleaseSetGeneration: set.Generation,
		RouteArtifactID: route.ID, RouteArtifactDigest: route.ContentHash, RouteArtifactGeneration: intent.Generation, RouteArtifactSequence: 1,
		ReleaseID: "neutral-release", ReleaseChannel: "full", FencingToken: 1, ScopeKey: "global", IntentDigest: digest, PolicyDigest: digest, InputSnapshotDigest: digest,
		CompilerVersion: "neutral-compiler", ProjectionDigest: trafficbinding.ProjectionDigest(intent)}
	batch, err := (edgecontrol.GroupShadowCompiler{Inventory: local, Ledger: local, Now: func() time.Time { return now }, InventoryMaxAge: time.Minute}).Reconcile(t.Context(), intent, []string{group})
	if err != nil || batch.Succeeded != 1 {
		t.Fatal(batch, err)
	}
	signer, err := edgecontrol.NewProjectedGroupBundleSigner(keyDir, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	publish := func(local *edgecontrol.PersistentGroupStore) edgecontrol.GroupAuthorityBatch {
		t.Helper()
		result, err := (edgecontrol.GroupAuthorityPublisher{Store: local, Signer: signer, Now: func() time.Time { return now }}).Publish(t.Context(), batch)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	countRoots := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM fugue_platform_input_external_roots`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	first := publish(local)
	afterLoss, err := local.ReadGroupAuthority(t.Context(), group)
	if err != nil || first.Failed != 1 || afterLoss.Published.Bundle.Generation != "" || afterLoss.LedgerHead.FailureCode != edgecontrol.GroupAuthorityFailurePendingRetention || countRoots() != 1 || requests.Load() != 1 || reservations.Load() != 1 {
		t.Fatal("lost ACK changed publication or lost native protection", first, afterLoss, err, countRoots(), requests.Load(), reservations.Load())
	}
	var firstReceipt inputarchive.PendingRootReceipt
	if err := db.QueryRow(`SELECT registration_sha256,registry_revision,source_epoch FROM fugue_platform_input_external_roots`).Scan(&firstReceipt.RegistrationSHA256, &firstReceipt.RegistryRevision, &firstReceipt.SourceEpoch); err != nil {
		t.Fatal(err)
	}
	// Reopen both application sides: no in-memory receipt, auth witness or
	// connection-pool cache may stand in for current native policy/registry.
	fresh := store.New("", address)
	if err := fresh.Init(); err != nil {
		t.Fatal(err)
	}
	fresh.ConfigurePlatformArtifactSigning(keys)
	current.Store(configure(fresh))
	registrar = newRegistrar()
	local = openLocal(registrar)
	second := publish(local)
	beforeRevoke, err := local.ReadGroupAuthority(t.Context(), group)
	if err != nil || second.Published != 1 || beforeRevoke.Published.Bundle.Generation == "" || countRoots() != 1 || registrar.receipt != firstReceipt || requests.Load() != 2 {
		t.Fatal("fresh exact retry did not recover the original receipt", second, err, registrar.receipt, firstReceipt, countRoots(), requests.Load())
	}
	policy.Leases = []inputarchive.PendingRootAuthorityLease{}
	policyRaw, err = policy.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_input_pending_authorities SET revision=revision+1,policy_json=$1`, string(policyRaw)); err != nil {
		t.Fatal(err)
	}
	if receipt, err := registrar.RetainPendingCompilerInputExternalRoot(t.Context(), registrar.request); err == nil || receipt.RegistryRevision != 0 || countRoots() != 1 || requests.Load() != 3 {
		t.Fatal("stale transport bypassed native revocation", receipt, err)
	}
	// Persisted receipt is a retention record, not activation or deletion power.
	if _, err := db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest); err == nil {
		t.Fatal("revocation removed the orphan's concrete FK protection")
	}
	withoutRegistrar, err := edgecontrol.OpenPersistentGroupStore(localDir)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := (edgecontrol.GroupAuthorityPublisher{Store: withoutRegistrar, Signer: signer}).RefreshPublishedLKG(t.Context(), group, beforeRevoke.Published.Bundle.ValidUntil)
	afterRevoke, readErr := withoutRegistrar.ReadGroupAuthority(t.Context(), group)
	if err != nil || readErr != nil || failed.Status != edgecontrol.GroupAuthorityStatusFailed || failed.FailureCode != edgecontrol.GroupAuthorityFailurePendingRetention ||
		!reflect.DeepEqual(beforeRevoke.Published, afterRevoke.Published) || afterRevoke.LedgerHead.Sequence != beforeRevoke.LedgerHead.Sequence+1 || countRoots() != 1 || requests.Load() != 3 {
		t.Fatal("restart downgraded durable requirement or destroyed LKG", failed, err, readErr)
	}
}

type pendingHTTPSyntheticReservation struct{}

func (pendingHTTPSyntheticReservation) CheckPendingRootReservation(ctx context.Context) error {
	return ctx.Err()
}
func (pendingHTTPSyntheticReservation) RecordPendingRootOutcome(inputarchive.PendingRootOutcome) {}

type pendingHTTPBufferedResponse struct {
	*httptest.ResponseRecorder
	underlying http.ResponseWriter
}

func (w *pendingHTTPBufferedResponse) Unwrap() http.ResponseWriter { return w.underlying }

type pendingHTTPNativeRegistrar struct {
	client   *http.Client
	endpoint string
	request  inputarchive.PendingRootRequest
	receipt  inputarchive.PendingRootReceipt
}

func (c *pendingHTTPNativeRegistrar) RetainPendingCompilerInputExternalRoot(ctx context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootReceipt, error) {
	c.request = r
	raw, err := inputarchive.MarshalPendingRootHTTP(r)
	if err != nil {
		return inputarchive.PendingRootReceipt{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw))
	if err != nil {
		return inputarchive.PendingRootReceipt{}, err
	}
	request.Header.Set("Authorization", "Bearer "+pendingDedicatedTestToken())
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return inputarchive.PendingRootReceipt{}, err
	}
	defer response.Body.Close()
	var receipt inputarchive.PendingRootReceipt
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" || json.NewDecoder(io.LimitReader(response.Body, 16385)).Decode(&receipt) != nil || !receipt.Matches(r.Registration) {
		return inputarchive.PendingRootReceipt{}, inputarchive.ErrPendingRoot
	}
	c.receipt = receipt
	return receipt, nil
}

func pendingHTTPPrivatePKI(t *testing.T, host string) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: host}, DNSNames: []string{host},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"http/1.1"}}, roots
}
