// Package localissuer owns an immutable in-process snapshot of the dedicated
// backup observer signing keyring. It has no datastore, network, filesystem,
// Kubernetes, route, or mutation capability.
package localissuer

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
)

var (
	ErrConfig      = errors.New("backup materializer local issuer configuration invalid")
	ErrUnavailable = errors.New("backup materializer local issuer unavailable")

	canonicalKeyID = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
)

type issueFunc func(backupcontrol.BackupRunSpec, string, time.Time) (backupmaterializer.ObserverInputBundle, error)

// Issuer keeps signing material inside a closure so ordinary struct formatting
// cannot traverse or disclose the captured keyring.
type Issuer struct {
	issue issueFunc
}

func New(keyring backupidentity.Keyring) (*Issuer, error) {
	cloned, ok := cloneValidKeyring(keyring)
	if !ok {
		return nil, ErrConfig
	}
	return &Issuer{
		issue: func(spec backupcontrol.BackupRunSpec, tenantID string, now time.Time) (backupmaterializer.ObserverInputBundle, error) {
			return backupmaterializer.IssueObserverInputBundle(cloned, spec, tenantID, now)
		},
	}, nil
}

func (issuer *Issuer) IssueObserverInputBundle(
	ctx context.Context,
	spec backupcontrol.BackupRunSpec,
	tenantID string,
	now time.Time,
) (backupmaterializer.ObserverInputBundle, error) {
	if ctx == nil || issuer == nil || issuer.issue == nil || ctx.Err() != nil {
		return backupmaterializer.ObserverInputBundle{}, ErrUnavailable
	}
	bundle, err := issuer.issue(spec, tenantID, now)
	if err != nil || ctx.Err() != nil {
		return backupmaterializer.ObserverInputBundle{}, ErrUnavailable
	}
	return bundle, nil
}

func (issuer *Issuer) String() string {
	return "backup materializer local issuer [REDACTED]"
}

func (issuer *Issuer) GoString() string {
	return issuer.String()
}

func cloneValidKeyring(keyring backupidentity.Keyring) (backupidentity.Keyring, bool) {
	activeKeyID := strings.TrimSpace(keyring.ActiveKeyID)
	if activeKeyID != keyring.ActiveKeyID || !canonicalKeyID.MatchString(activeKeyID) || len(keyring.Keys) == 0 {
		return backupidentity.Keyring{}, false
	}
	cloned := backupidentity.Keyring{
		ActiveKeyID:   activeKeyID,
		Keys:          make(map[string]string, len(keyring.Keys)),
		RevokedKeyIDs: make(map[string]struct{}, len(keyring.RevokedKeyIDs)),
	}
	for keyID, secret := range keyring.Keys {
		if strings.TrimSpace(keyID) != keyID || !canonicalKeyID.MatchString(keyID) ||
			strings.TrimSpace(secret) != secret || len(secret) < 32 {
			return backupidentity.Keyring{}, false
		}
		cloned.Keys[keyID] = secret
	}
	for keyID := range keyring.RevokedKeyIDs {
		if strings.TrimSpace(keyID) != keyID || !canonicalKeyID.MatchString(keyID) {
			return backupidentity.Keyring{}, false
		}
		if _, retained := cloned.Keys[keyID]; retained {
			return backupidentity.Keyring{}, false
		}
		cloned.RevokedKeyIDs[keyID] = struct{}{}
	}
	if _, ok := cloned.Keys[activeKeyID]; !ok {
		return backupidentity.Keyring{}, false
	}
	return cloned, true
}
