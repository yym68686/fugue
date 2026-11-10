package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

const captureControlPath = "/v1/admin/platform/producer-capture-deferrals/control"

func captureControlFixture(t *testing.T) (platformproducer.CaptureDeferralControlRequest, platformproducer.CaptureDeferralRecord) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Second)
	policy := model.PlatformPublicationPrecondition{ArtifactID: "synthetic-policy", ContentHash: "sha256:" + strings.Repeat("a", 64), ReleaseID: "synthetic-release", FencingToken: 1}
	original := platformproducer.CaptureDeferralRequest{SchemaVersion: platformproducer.CaptureDeferralSchema, OperationID: "control-http-alpha", RequestedAt: at, ExpiresAt: at.Add(time.Minute), Authorities: []platformproducer.CaptureDeferralAuthority{{Scope: platformproducer.Scope, Policy: policy}}}
	record, err := platformproducer.PrepareCaptureDeferral(original, nil, platformproducer.CaptureDeferralObservation{At: at, Authorities: map[string]model.PlatformPublicationPrecondition{platformproducer.Scope: policy}, InFlight: map[string]uint64{platformproducer.Scope: 0}})
	if err != nil {
		t.Fatal(err)
	}
	return platformproducer.CaptureDeferralControlRequest{SchemaVersion: platformproducer.CaptureDeferralControlSchema, OperationID: original.OperationID, RequestDigest: record.RequestDigest, Action: "release"}, record
}

func captureControlAuthFixture(t *testing.T) (*store.Store, *auth.Authenticator, map[string]string) {
	t.Helper()
	db := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := db.Init(); err != nil {
		t.Fatal(err)
	}
	keys := map[string]string{"bootstrap": "synthetic-capture-admin", "unauthenticated": ""}
	for label, scopes := range map[string][]string{"tenant": {"artifact.read"}, "admin-only": {"platform.admin"}, "control-only": {"producer_capture.control"}, "explicit-admin": {"platform.admin", "producer_capture.control"}} {
		_, key, err := db.CreateAPIKey("", label, scopes)
		if err != nil {
			t.Fatal(err)
		}
		keys[label] = key
	}
	return db, auth.New(db, keys["bootstrap"]), keys
}

func captureControlHTTP(t *testing.T, handler http.Handler, key, path, media, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	if media != "" {
		r.Header.Set("Content-Type", media)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestCaptureControlHTTPAuthenticatesExactCallerAndTerminalReceipt(t *testing.T) {
	db, authn, keys := captureControlAuthFixture(t)
	control, original := captureControlFixture(t)
	body, _ := json.Marshal(control)
	calls := 0
	handler := authn.RequireAPI(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveProducerCaptureControl(w, r, func(ctx context.Context, got platformproducer.CaptureDeferralControlRequest, p model.Principal) (store.ProducerCaptureDeferralReceipt, error) {
			calls++
			if got != control || p.ActorType == "" || p.ActorID == "" || ctx != r.Context() {
				t.Fatal("transport changed the authenticated caller or original target")
			}
			record, err := platformproducer.ReleaseCaptureDeferral(original, time.Now().UTC())
			return store.ProducerCaptureDeferralReceipt{Control: got, Record: record, Version: 2, Changed: true, ActorType: p.ActorType, ActorID: p.ActorID}, err
		})
	}))
	for label, status := range map[string]int{"unauthenticated": 401, "tenant": 403, "admin-only": 403, "control-only": 403, "explicit-admin": 200, "bootstrap": 200} {
		t.Run(label, func(t *testing.T) {
			before := calls
			w := captureControlHTTP(t, handler, keys[label], captureControlPath, "application/json", string(body))
			if w.Code != status || calls-before != map[bool]int{true: 1, false: 0}[status == 200] {
				t.Fatal("authentication did not isolate the native adapter", status, w.Code, calls-before, w.Body.String())
			}
			if status == 200 {
				var receipt store.ProducerCaptureDeferralReceipt
				if json.Unmarshal(w.Body.Bytes(), &receipt) != nil || receipt.Control != control || receipt.Record.Phase != "released" || !receipt.Record.Request.ExpiresAt.Equal(original.Request.ExpiresAt) || receipt.Version != 2 || w.Header().Get("Cache-Control") != "private, no-store" {
					t.Fatal("HTTP receipt lost the original terminal binding", w.Body.String())
				}
			}
		})
	}
	// The generated production route also uses real authentication, but an
	// ordinary file store cannot install schema or enable controls as a side effect.
	s := NewServer(db, authn, nil, ServerConfig{})
	w := performJSONRequest(t, s, http.MethodPost, captureControlPath, keys["bootstrap"], control)
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), `"record"`) || s.producerCaptureAdmission != nil {
		t.Fatal("generated route bypassed the uninstalled-storage gate", w.Code, w.Body.String())
	}
}

func TestCaptureControlHTTPRefusesAmbiguousTargetsBeforeNativeCall(t *testing.T) {
	_, authn, keys := captureControlAuthFixture(t)
	control, _ := captureControlFixture(t)
	encoded, _ := json.Marshal(control)
	body := string(encoded)
	handler := authn.RequireAPI(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveProducerCaptureControl(w, r, func(context.Context, platformproducer.CaptureDeferralControlRequest, model.Principal) (store.ProducerCaptureDeferralReceipt, error) {
			t.Fatal("invalid request reached control storage")
			return store.ProducerCaptureDeferralReceipt{}, nil
		})
	}))
	for _, tc := range []struct {
		name, path, media, body string
		status                  int
	}{
		{"missing media", captureControlPath, "", body, 415},
		{"wrong media", captureControlPath, "text/plain", body, 415},
		{"query", captureControlPath + "?enable=true", "application/json", body, 400},
		{"duplicate action", captureControlPath, "application/json", strings.TrimSuffix(body, "}") + `,"action":"observe"}`, 400},
		{"escaped duplicate", captureControlPath, "application/json", strings.TrimSuffix(body, "}") + `,"act\u0069on":"observe"}`, 400},
		{"alternate case", captureControlPath, "application/json", strings.Replace(body, `"action"`, `"Action"`, 1), 400},
		{"unknown actor", captureControlPath, "application/json", strings.TrimSuffix(body, "}") + `,"actor_id":"forged"}`, 400},
		{"null", captureControlPath, "application/json", strings.Replace(body, `"release"`, `null`, 1), 400},
		{"array", captureControlPath, "application/json", "[" + body + "]", 400},
		{"trailing object", captureControlPath, "application/json", body + "{}", 400},
		{"trailing invalid", captureControlPath, "application/json", body + "garbage", 400},
		{"enable", captureControlPath, "application/json", strings.Replace(body, `"release"`, `"enable"`, 1), 400},
		{"extend", captureControlPath, "application/json", strings.Replace(body, `"release"`, `"extend"`, 1), 400},
		{"oversized whitespace", captureControlPath, "application/json", body + strings.Repeat(" ", maxProducerCaptureControlBytes), 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := captureControlHTTP(t, handler, keys["bootstrap"], tc.path, tc.media, tc.body)
			if w.Code != tc.status || strings.Contains(w.Body.String(), `"record"`) {
				t.Fatal("ambiguous request produced a receipt", w.Code, w.Body.String())
			}
		})
	}
}

func TestCaptureControlHTTPNeverPublishesPartialOrUnboundReceipts(t *testing.T) {
	_, authn, keys := captureControlAuthFixture(t)
	control, original := captureControlFixture(t)
	body, _ := json.Marshal(control)
	for _, tc := range []struct {
		name   string
		err    error
		mutate func(*store.ProducerCaptureDeferralReceipt)
		status int
	}{
		{name: "missing", err: sql.ErrNoRows, status: 404},
		{name: "digest conflict", err: store.ErrConflict, status: 409},
		{name: "invalid", err: store.ErrInvalidInput, status: 400},
		{name: "uninstalled", err: store.ErrProducerCaptureDeferralUnavailable, status: 503},
		{name: "commit response lost", err: errors.New("synthetic ambiguous commit with private details"), status: 503},
		{name: "caller drift", mutate: func(r *store.ProducerCaptureDeferralReceipt) { r.ActorID += "-other" }, status: 503},
		{name: "target drift", mutate: func(r *store.ProducerCaptureDeferralReceipt) { r.Control.OperationID += "-other" }, status: 503},
		{name: "record drift", mutate: func(r *store.ProducerCaptureDeferralReceipt) {
			r.Record.RequestDigest = "sha256:" + strings.Repeat("f", 64)
		}, status: 503},
		{name: "unpersisted", mutate: func(r *store.ProducerCaptureDeferralReceipt) { r.Version = 0 }, status: 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := authn.RequireAPI(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				serveProducerCaptureControl(w, r, func(_ context.Context, c platformproducer.CaptureDeferralControlRequest, p model.Principal) (store.ProducerCaptureDeferralReceipt, error) {
					receipt := store.ProducerCaptureDeferralReceipt{Control: c, Record: original, Version: 1, ActorType: p.ActorType, ActorID: p.ActorID}
					if tc.mutate != nil {
						tc.mutate(&receipt)
					}
					return receipt, tc.err
				})
			}))
			w := captureControlHTTP(t, handler, keys["bootstrap"], captureControlPath, "application/json; charset=utf-8", string(body))
			if w.Code != tc.status || strings.Contains(w.Body.String(), `"record"`) || strings.Contains(w.Body.String(), "private details") {
				t.Fatal("failed or unbound transaction leaked a success receipt", w.Code, w.Body.String())
			}
		})
	}
}
