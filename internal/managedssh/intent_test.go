package managedssh

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
	"golang.org/x/crypto/ssh"
)

func TestManagedSSHCompileUsesCurrentInventoryAndBoundedGrants(test *testing.T) {
	configuration, signer, user := testConfiguration(test)
	intent := model.ManagedSSHIntent{TenantID: configuration.Identity.TenantID, AppID: configuration.Identity.AppID, Generation: 1, Spec: model.ManagedSSHIntentSpec{Enabled: true, IdentityID: configuration.Identity.ID, Container: configuration.Target.Container, User: configuration.Target.User, WorkingDirectory: configuration.Target.WorkingDirectory, AuthorizedKeyIDs: []string{"current", "deleted", "foreign"}, AllowPTY: true, AllowSFTP: true, MaxConnections: 8, MaxConnectionsPerIP: 4, MaxChannels: 4}}
	key := model.SSHKey{ID: "current", TenantID: intent.TenantID, Status: model.SSHKeyStatusActive, PublicKey: string(ssh.MarshalAuthorizedKey(user.PublicKey())), Fingerprint: ssh.FingerprintSHA256(user.PublicKey())}
	foreign := key
	foreign.ID, foreign.TenantID = "foreign", "another-tenant"
	now := time.Now().UTC()
	compiled, err := CompileConfiguration(intent, configuration.Identity, configuration.Target, []model.SSHKey{key, foreign}, 5, now)
	if err != nil {
		test.Fatal(err)
	}
	if len(compiled.Keys) != 1 || compiled.Keys[0].ID != key.ID || !compiled.ValidUntil.Equal(now.Add(5*time.Minute)) || !compiled.Keys[0].ExpiresAt.Equal(compiled.ValidUntil) {
		test.Fatal("grant compilation used stale keys or unbounded expiry")
	}
	if _, err := prepareSnapshot(context.Background(), compiled, signer, &testExecutor{}, now, true); err != nil {
		test.Fatal(err)
	}
	compiled, err = CompileConfiguration(intent, configuration.Identity, configuration.Target, nil, 6, now)
	if err != nil || len(compiled.Keys) != 0 {
		test.Fatal("deleted keys resurrected from intent", err)
	}
	intent.Spec.Enabled = false
	compiled, err = CompileConfiguration(intent, configuration.Identity, configuration.Target, []model.SSHKey{key}, 7, now)
	if err != nil || len(compiled.Keys) != 0 {
		test.Fatal("disabled intent retained grants", err)
	}
	intent.Spec.ForwardRules = []ForwardRule{{Host: "10.0.0.1", Port: 443}}
	if ValidateIntent(intent.Spec) == nil {
		test.Fatal("cluster address admitted for forwarding")
	}
	intent.Spec.ForwardRules = nil
	intent.Spec.AllowedEnvironment = []string{"LD_PRELOAD"}
	if ValidateIntent(intent.Spec) == nil {
		test.Fatal("unsafe environment admitted")
	}
}
