package localissuer

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	"fugue/internal/backupmaterializer/httpapi"
)

var _ httpapi.Issuer = (*Issuer)(nil)

func TestLocalIssuerClonesKeyringAndIssuesSelfValidatingBundle(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 30, 15, 0, 0, 123, time.UTC)
	configured := testLocalIssuerKeyring()
	verification := testLocalIssuerKeyring()
	issuer, err := New(configured)
	if err != nil {
		t.Fatalf("construct local issuer: %v", err)
	}
	configured.ActiveKeyID = "changed"
	for keyID := range configured.Keys {
		configured.Keys[keyID] = "external mutation"
		delete(configured.Keys, keyID)
		configured.RevokedKeyIDs[keyID] = struct{}{}
	}
	spec := testLocalIssuerSpec(t)
	bundle, err := issuer.IssueObserverInputBundle(context.Background(), spec, "tenant-1", now)
	if err != nil {
		t.Fatalf("issue observer input bundle: %v", err)
	}
	if err := backupmaterializer.ValidateObserverInputBundle(bundle, verification, now.Add(time.Minute)); err != nil {
		t.Fatalf("validate locally issued bundle: %v", err)
	}
	if bundle.DesiredSpec != spec || bundle.CellKey != spec.CellKey || bundle.RunID != spec.RunID ||
		bundle.IssuedAt != now.UTC().Truncate(time.Second) {
		t.Fatalf("locally issued bundle drifted: %#v", bundle)
	}
}

func TestLocalIssuerIsConcurrentAndGeneratesUniqueCredentialGenerations(t *testing.T) {
	t.Parallel()
	issuer, err := New(testLocalIssuerKeyring())
	if err != nil {
		t.Fatalf("construct local issuer: %v", err)
	}
	spec := testLocalIssuerSpec(t)
	now := time.Date(2026, 7, 30, 15, 5, 0, 0, time.UTC)
	const workers = 64
	tokenIDs := make(chan string, workers)
	errorsFound := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			bundle, issueErr := issuer.IssueObserverInputBundle(context.Background(), spec, "tenant-1", now)
			if issueErr != nil {
				errorsFound <- issueErr
				return
			}
			tokenIDs <- bundle.TokenID
		}()
	}
	group.Wait()
	close(tokenIDs)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("concurrent issue failed: %v", err)
	}
	seen := map[string]struct{}{}
	for tokenID := range tokenIDs {
		if _, duplicate := seen[tokenID]; duplicate {
			t.Fatalf("concurrent issue reused token id %q", tokenID)
		}
		seen[tokenID] = struct{}{}
	}
	if len(seen) != workers {
		t.Fatalf("issued %d unique credential generations, want %d", len(seen), workers)
	}
}

func TestLocalIssuerConfigurationFailsClosed(t *testing.T) {
	t.Parallel()
	valid := testLocalIssuerKeyring()
	activeSecret := valid.Keys[valid.ActiveKeyID]
	tests := map[string]backupidentity.Keyring{
		"empty":                     {},
		"missing active":            {ActiveKeyID: "backup-key-1", Keys: map[string]string{}, RevokedKeyIDs: map[string]struct{}{}},
		"active whitespace":         {ActiveKeyID: " backup-key-1", Keys: map[string]string{"backup-key-1": activeSecret}, RevokedKeyIDs: map[string]struct{}{}},
		"weak secret":               {ActiveKeyID: "backup-key-1", Keys: map[string]string{"backup-key-1": "weak"}, RevokedKeyIDs: map[string]struct{}{}},
		"secret whitespace":         {ActiveKeyID: "backup-key-1", Keys: map[string]string{"backup-key-1": activeSecret + " "}, RevokedKeyIDs: map[string]struct{}{}},
		"malformed key id":          {ActiveKeyID: "backup-key-1", Keys: map[string]string{"backup-key-1": activeSecret, "BAD": activeSecret}, RevokedKeyIDs: map[string]struct{}{}},
		"revoked active":            {ActiveKeyID: "backup-key-1", Keys: map[string]string{"backup-key-1": activeSecret}, RevokedKeyIDs: map[string]struct{}{"backup-key-1": {}}},
		"retained revoked previous": {ActiveKeyID: "backup-key-1", Keys: map[string]string{"backup-key-1": activeSecret, "backup-key-0": activeSecret}, RevokedKeyIDs: map[string]struct{}{"backup-key-0": {}}},
		"malformed revocation":      {ActiveKeyID: "backup-key-1", Keys: map[string]string{"backup-key-1": activeSecret}, RevokedKeyIDs: map[string]struct{}{"BAD": {}}},
	}
	for name, keyring := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := New(keyring); !errors.Is(err, ErrConfig) {
				t.Fatalf("invalid keyring error=%v, want ErrConfig", err)
			}
		})
	}
}

func TestLocalIssuerFailuresAreDetailFreeAndFormattingIsRedacted(t *testing.T) {
	t.Parallel()
	keyring := testLocalIssuerKeyring()
	secret := keyring.Keys[keyring.ActiveKeyID]
	issuer, _ := New(keyring)
	spec := testLocalIssuerSpec(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	invalidSpec := spec
	invalidSpec.Digest = "sha256:" + strings.Repeat("f", 64)
	for name, issue := range map[string]func() error{
		"nil context": func() error {
			_, err := issuer.IssueObserverInputBundle(nil, spec, "tenant-1", time.Now())
			return err
		},
		"canceled": func() error {
			_, err := issuer.IssueObserverInputBundle(canceled, spec, "tenant-1", time.Now())
			return err
		},
		"invalid spec": func() error {
			_, err := issuer.IssueObserverInputBundle(context.Background(), invalidSpec, "tenant-1", time.Now())
			return err
		},
		"invalid tenant": func() error {
			_, err := issuer.IssueObserverInputBundle(context.Background(), spec, "tenant/1", time.Now())
			return err
		},
		"zero time": func() error {
			_, err := issuer.IssueObserverInputBundle(context.Background(), spec, "tenant-1", time.Time{})
			return err
		},
		"nil issuer": func() error {
			_, err := (*Issuer)(nil).IssueObserverInputBundle(context.Background(), spec, "tenant-1", time.Now())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := issue(); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), secret) {
				t.Fatalf("issuer failure=%v, want detail-free unavailable", err)
			}
		})
	}
	for _, rendered := range []string{issuer.String(), issuer.GoString(), fmt.Sprint(issuer), fmt.Sprintf("%+v", issuer), fmt.Sprintf("%#v", issuer)} {
		if strings.Contains(rendered, secret) || !strings.Contains(rendered, "[REDACTED]") {
			t.Fatalf("issuer formatting exposed keyring: %q", rendered)
		}
	}
}

func TestLocalIssuerDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list local issuer dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupidentity",
		"fugue/internal/backupmaterializer",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("local issuer dependency boundary widened: got=%v want=%v", local, want)
	}
	for _, forbidden := range []string{"fugue/internal/api", "fugue/internal/store", "fugue/internal/model", "database/sql", "k8s.io/", "net/http", "os", "os/exec"} {
		if strings.Contains(string(output), forbidden) {
			t.Fatalf("local issuer gained forbidden dependency %q", forbidden)
		}
	}
}

func testLocalIssuerKeyring() backupidentity.Keyring {
	return backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", strings.Repeat("p", 32), "backup-key-0", nil)
}

func testLocalIssuerSpec(t *testing.T) backupcontrol.BackupRunSpec {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		"run-local-issuer-1",
		"run-local-issuer-1",
		backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"},
		"backend-1",
		"sha256:"+strings.Repeat("a", 64),
		4,
		120,
		1800,
	)
	if err != nil {
		t.Fatalf("build local issuer spec: %v", err)
	}
	return spec
}
