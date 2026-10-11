package api

import (
	"bytes"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
	"fugue/internal/store"

	"golang.org/x/crypto/ssh"
)

func TestSSHObservationUsesRealHandshakeAndDoesNotChangeWorkload(test *testing.T) {
	for _, matches := range []bool{true, false} {
		test.Run(strconv.FormatBool(matches), func(test *testing.T) {
			stateStore, server, apiKey, app := setupAppSSHTestServer(test)
			keyring, err := sshidentity.NewKeyring("storage", map[string][]byte{"storage": bytes.Repeat([]byte{31}, 32)})
			if err != nil {
				test.Fatal(err)
			}
			stateStore.ConfigureSSHIdentityKeyring(keyring)
			identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
			if err != nil {
				test.Fatal(err)
			}
			privateKey, err := stateStore.AppSSHIdentityPrivateKey(app.TenantID, app.ID, identity.ID)
			if err != nil {
				test.Fatal(err)
			}
			signer, err := ssh.ParsePrivateKey(privateKey)
			if err != nil {
				test.Fatal(err)
			}
			if !matches {
				other, err := keyring.Create("tenant_other", "app_other", time.Now())
				if err != nil {
					test.Fatal(err)
				}
				signer, err = keyring.Signer(other)
				if err != nil {
					test.Fatal(err)
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				test.Fatal(err)
			}
			defer listener.Close()
			configuration := &ssh.ServerConfig{NoClientAuth: true}
			configuration.AddHostKey(signer)
			done := make(chan struct{})
			go func() {
				defer close(done)
				connection, err := listener.Accept()
				if err != nil {
					return
				}
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
				_, _, _, _ = ssh.NewServerConn(connection, configuration)
			}()
			port := listener.Addr().(*net.TCPAddr).Port
			before, _, err := stateStore.UpsertAppSSHConfig(app.ID, store.AppSSHUpdate{Enabled: true, Hostname: "127.0.0.1", PublicPortStart: port, PublicPortEnd: port, AuthorizedKeys: []string{testAPISSHPublicKey}})
			if err != nil {
				test.Fatal(err)
			}
			path := "/v1/apps/" + app.ID + "/ssh"
			beforeResponse := performJSONRequest(test, server, http.MethodGet, path, apiKey, nil)
			var previous struct {
				SSH model.AppSSHStatus `json:"ssh"`
			}
			mustDecodeJSON(test, beforeResponse, &previous)
			if previous.SSH.Ready || !previous.SSH.RouteConfigured || previous.SSH.IdentityState != "unobserved" {
				test.Fatal("configuration falsely reported verified readiness", previous.SSH)
			}
			wrong := performJSONRequest(test, server, http.MethodPost, path+"/identity/observe", apiKey, map[string]string{"identity_id": "wrong-identity"})
			if wrong.Code != http.StatusConflict {
				test.Fatal("identity precondition was not enforced", wrong.Code)
			}
			response := performJSONRequest(test, server, http.MethodPost, path+"/identity/observe", apiKey, map[string]string{"identity_id": identity.ID})
			if response.Code != http.StatusOK {
				test.Fatal(response.Code, response.Body.String())
			}
			var observed struct {
				Observation model.AppSSHObservation `json:"observation"`
			}
			mustDecodeJSON(test, response, &observed)
			if !observed.Observation.KeyExchangeVerified || observed.Observation.IdentityMatched != matches {
				test.Fatal("incorrect handshake evidence", observed)
			}
			<-done
			statusResponse := performJSONRequest(test, server, http.MethodGet, path, apiKey, nil)
			var status struct {
				SSH model.AppSSHStatus `json:"ssh"`
			}
			mustDecodeJSON(test, statusResponse, &status)
			if status.SSH.Ready != matches || status.SSH.IdentityVerified != matches || status.SSH.Observation == nil {
				test.Fatal("status ignored handshake evidence", status)
			}
			repeated := performJSONRequest(test, server, http.MethodPost, path+"/identity/observe", apiKey, map[string]string{"identity_id": identity.ID})
			if repeated.Code != http.StatusTooManyRequests {
				test.Fatal("probe rate limit not enforced", repeated.Code)
			}
			after, err := stateStore.GetApp(app.ID)
			if err != nil || !reflect.DeepEqual(before.Spec, after.Spec) {
				test.Fatal("observation changed workload", err)
			}
		})
	}
}

func TestSSHObservationReadinessRejectsStaleOrUnboundEvidence(test *testing.T) {
	now := time.Now().UTC()
	endpoint := model.AppSSHEndpoint{ID: "endpoint", TenantID: "tenant", AppID: "app", Hostname: "ssh.example", PublicPort: 23456, TargetHost: "workload.example", TargetPort: 22, Status: model.AppSSHEndpointStatusPending}
	identity := model.AppSSHIdentity{ID: "identity", TenantID: "tenant", AppID: "app", Algorithm: ssh.KeyAlgoED25519, Fingerprint: "SHA256:expected"}
	observation := model.AppSSHObservation{ID: "observation", TenantID: "tenant", AppID: "app", IdentityID: "identity", EndpointID: "endpoint", RouteDigest: model.AppSSHRouteDigest(endpoint), Scope: "public_endpoint", Target: model.AppSSHPublicAddress(endpoint), Algorithm: identity.Algorithm, Fingerprint: identity.Fingerprint, KeyExchangeVerified: true, IdentityMatched: true, ObservedAt: now, ExpiresAt: now.Add(time.Minute), Reason: "identity_verified"}
	status := model.AppSSHStatus{Supported: true, RouteConfigured: true}
	if !applyAppSSHObservation(status, endpoint, identity, observation, now).Ready {
		test.Fatal("valid evidence not accepted")
	}
	for name, mutate := range map[string]func(*model.AppSSHObservation){
		"tenant":      func(value *model.AppSSHObservation) { value.TenantID = "another" },
		"app":         func(value *model.AppSSHObservation) { value.AppID = "another" },
		"identity":    func(value *model.AppSSHObservation) { value.IdentityID = "another" },
		"route":       func(value *model.AppSSHObservation) { value.RouteDigest = "another" },
		"target":      func(value *model.AppSSHObservation) { value.Target = "another:22" },
		"scope":       func(value *model.AppSSHObservation) { value.Scope = "internal_only" },
		"expired":     func(value *model.AppSSHObservation) { value.ExpiresAt = now.Add(-time.Second) },
		"unbounded":   func(value *model.AppSSHObservation) { value.ExpiresAt = now.Add(time.Hour) },
		"future":      func(value *model.AppSSHObservation) { value.ObservedAt = now.Add(time.Hour) },
		"fingerprint": func(value *model.AppSSHObservation) { value.Fingerprint = "SHA256:other" },
		"algorithm":   func(value *model.AppSSHObservation) { value.Algorithm = ssh.KeyAlgoRSA },
		"handshake":   func(value *model.AppSSHObservation) { value.KeyExchangeVerified = false },
	} {
		test.Run(name, func(test *testing.T) {
			candidate := observation
			mutate(&candidate)
			if got := applyAppSSHObservation(status, endpoint, identity, candidate, now); got.Ready || got.IdentityVerified {
				test.Fatal("invalid evidence was accepted", got)
			}
		})
	}
	status.RouteConfigured = false
	if applyAppSSHObservation(status, endpoint, identity, observation, now).Ready {
		test.Fatal("disabled route appears ready")
	}
}
