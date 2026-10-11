package managedssh

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"path"
	"strings"
	"time"

	"fugue/internal/sshidentity"

	"golang.org/x/crypto/ssh"
)

var (
	ErrConfiguration  = errors.New("managed SSH configuration is invalid")
	ErrCapabilities   = errors.New("runtime cannot provide the requested managed SSH capabilities")
	ErrGeneration     = errors.New("managed SSH configuration generation is stale or conflicts")
	ErrIdentityChange = errors.New("host identity change requires an explicit serving migration")
	ErrUnauthorized   = errors.New("managed SSH access is not authorized")
	ErrUnavailable    = errors.New("managed SSH serving state is unavailable")
)

type snapshot struct {
	configuration Configuration
	signer        ssh.Signer
	digest        string
	grants        map[string]KeyGrant
}

func prepareSnapshot(ctx context.Context, configuration Configuration, signer ssh.Signer, executor Executor, now time.Time, preflight bool) (*snapshot, error) {
	if configuration.Schema != "fugue.managed-ssh/v1" || configuration.Generation == 0 || signer == nil || executor == nil || !configuration.ValidUntil.After(now) || configuration.ValidUntil.After(now.Add(24*time.Hour)) {
		return nil, ErrConfiguration
	}
	if sshidentity.ValidatePublic(configuration.Identity) != nil || configuration.Identity.Fingerprint != ssh.FingerprintSHA256(signer.PublicKey()) || strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))) != configuration.Identity.PublicKey {
		return nil, ErrConfiguration
	}
	target := configuration.Target
	if target.TenantID != configuration.Identity.TenantID || target.AppID != configuration.Identity.AppID || !safeToken(target.Namespace) || !safeToken(target.PodName) || !safeToken(target.PodUID) || !safeToken(target.Container) || !safeToken(target.ContainerID) || !safeToken(target.User) || !path.IsAbs(target.WorkingDirectory) || path.Clean(target.WorkingDirectory) != target.WorkingDirectory || strings.ContainsAny(target.WorkingDirectory, "\x00\r\n") {
		return nil, ErrConfiguration
	}
	if configuration.MaxConnections < 1 || configuration.MaxConnections > 1024 || configuration.MaxConnectionsPerIP < 1 || configuration.MaxConnectionsPerIP > configuration.MaxConnections || configuration.MaxChannels < 1 || configuration.MaxChannels > 64 || len(configuration.Keys) > 256 || len(configuration.ForwardRules) > 128 || len(configuration.AllowedEnvironment) > 64 {
		return nil, ErrConfiguration
	}
	grants := make(map[string]KeyGrant, len(configuration.Keys))
	identifiers := map[string]struct{}{}
	for _, grant := range configuration.Keys {
		key, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(grant.PublicKey))
		if err != nil || len(options) != 0 || len(bytes.TrimSpace(rest)) != 0 || !safeToken(grant.ID) || grant.ExpiresAt.IsZero() || grant.ExpiresAt.After(configuration.ValidUntil) {
			return nil, ErrConfiguration
		}
		switch key.Type() {
		case ssh.KeyAlgoED25519, ssh.KeyAlgoRSA, ssh.KeyAlgoECDSA256, ssh.KeyAlgoECDSA384, ssh.KeyAlgoECDSA521:
		default:
			return nil, ErrConfiguration
		}
		fingerprint := ssh.FingerprintSHA256(key)
		if _, exists := grants[fingerprint]; exists {
			return nil, ErrConfiguration
		}
		if _, exists := identifiers[grant.ID]; exists {
			return nil, ErrConfiguration
		}
		identifiers[grant.ID] = struct{}{}
		grants[fingerprint] = grant
	}
	for _, rule := range configuration.ForwardRules {
		address := net.ParseIP(rule.Host)
		if address == nil || !address.IsLoopback() || rule.Port == 0 || rule.Port > 65535 || (rule.EndPort != 0 && (rule.EndPort < rule.Port || rule.EndPort > 65535)) {
			return nil, ErrConfiguration
		}
	}
	for _, name := range configuration.AllowedEnvironment {
		if !allowedEnvironmentName(name) {
			return nil, ErrConfiguration
		}
	}
	if preflight {
		capabilities, err := executor.Validate(ctx, target)
		if err != nil {
			return nil, err
		}
		if !capabilities.Exec || !capabilities.Shell || !capabilities.Signals || (configuration.AllowPTY && !capabilities.PTY) || (configuration.AllowSFTP && !capabilities.SFTP) || (len(configuration.ForwardRules) > 0 && !capabilities.Forward) {
			return nil, ErrCapabilities
		}
	}
	raw, err := json.Marshal(configuration)
	if err != nil {
		return nil, err
	}
	var frozen Configuration
	if err := json.Unmarshal(raw, &frozen); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	return &snapshot{configuration: frozen, signer: signer, digest: "sha256:" + hex.EncodeToString(digest[:]), grants: grants}, nil
}

func (state *snapshot) authorize(user, fingerprint string, now time.Time) bool {
	if state == nil || user != state.configuration.Target.User || !state.configuration.ValidUntil.After(now) {
		return false
	}
	grant, found := state.grants[fingerprint]
	return found && grant.ExpiresAt.After(now)
}

func safeToken(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || strings.ContainsRune("_-.:/", character) {
			continue
		}
		return false
	}
	return true
}

func safeEnvironmentName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index, character := range value {
		if character == '_' || (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (index > 0 && character >= '0' && character <= '9') {
			continue
		}
		return false
	}
	return true
}
