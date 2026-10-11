package managedssh

import (
	"math"
	"net"
	"path"
	"regexp"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/sshidentity"
	"golang.org/x/crypto/ssh"
)

const GrantLifetime = 5 * time.Minute

var containerNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
var userNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]*\$?$`)

func ValidateIntent(spec model.ManagedSSHIntentSpec) error {
	if !safeToken(spec.IdentityID) || len(spec.Container) > 63 || !containerNamePattern.MatchString(spec.Container) || len(spec.User) > 256 || !userNamePattern.MatchString(spec.User) || !path.IsAbs(spec.WorkingDirectory) || path.Clean(spec.WorkingDirectory) != spec.WorkingDirectory || strings.ContainsAny(spec.WorkingDirectory, "\x00\r\n") || len(spec.WorkingDirectory) > 4096 {
		return ErrConfiguration
	}
	if spec.MaxConnections < 1 || spec.MaxConnections > 1024 || spec.MaxConnectionsPerIP < 1 || spec.MaxConnectionsPerIP > spec.MaxConnections || spec.MaxChannels < 1 || spec.MaxChannels > 64 || len(spec.AuthorizedKeyIDs) > 256 || len(spec.ForwardRules) > 128 || len(spec.AllowedEnvironment) > 64 {
		return ErrConfiguration
	}
	identifiers := map[string]bool{}
	for _, identifier := range spec.AuthorizedKeyIDs {
		if !safeToken(identifier) || identifiers[identifier] {
			return ErrConfiguration
		}
		identifiers[identifier] = true
	}
	for _, rule := range spec.ForwardRules {
		address := net.ParseIP(rule.Host)
		if address == nil || !address.IsLoopback() || rule.Port == 0 || rule.Port > 65535 || (rule.EndPort != 0 && (rule.EndPort < rule.Port || rule.EndPort > 65535)) {
			return ErrConfiguration
		}
	}
	environment := map[string]bool{}
	for _, name := range spec.AllowedEnvironment {
		if !allowedEnvironmentName(name) || environment[name] {
			return ErrConfiguration
		}
		environment[name] = true
	}
	return nil
}

func allowedEnvironmentName(name string) bool {
	if !safeEnvironmentName(name) {
		return false
	}
	switch name {
	case "LD_PRELOAD", "LD_LIBRARY_PATH", "BASH_ENV", "ENV", "SHELLOPTS", "NODE_OPTIONS", "PYTHONPATH":
		return false
	}
	return true
}

func CompileConfiguration(intent model.ManagedSSHIntent, identity model.AppSSHIdentity, target Target, inventory []model.SSHKey, generation uint64, now time.Time) (Configuration, error) {
	if err := ValidateIntent(intent.Spec); err != nil {
		return Configuration{}, err
	}
	if generation == 0 || generation > math.MaxInt64 || intent.Generation == 0 || now.IsZero() || sshidentity.ValidatePublic(identity) != nil || intent.Spec.IdentityID != identity.ID || intent.TenantID != identity.TenantID || intent.AppID != identity.AppID || target.TenantID != intent.TenantID || target.AppID != intent.AppID || target.Container != intent.Spec.Container || target.User != intent.Spec.User || target.WorkingDirectory != intent.Spec.WorkingDirectory {
		return Configuration{}, ErrConfiguration
	}
	spec := intent.Spec
	configuration := Configuration{Schema: "fugue.managed-ssh/v1", Generation: generation, Identity: identity, Target: target, Keys: []KeyGrant{}, ValidUntil: now.UTC().Add(GrantLifetime), AllowPTY: spec.AllowPTY, AllowSFTP: spec.AllowSFTP, ForwardRules: append([]ForwardRule(nil), spec.ForwardRules...), AllowedEnvironment: append([]string(nil), spec.AllowedEnvironment...), MaxConnections: spec.MaxConnections, MaxConnectionsPerIP: spec.MaxConnectionsPerIP, MaxChannels: spec.MaxChannels}
	if !spec.Enabled {
		return configuration, nil
	}
	available := map[string]model.SSHKey{}
	for _, key := range inventory {
		if key.TenantID != intent.TenantID || key.Status != model.SSHKeyStatusActive {
			continue
		}
		if _, exists := available[key.ID]; exists {
			return Configuration{}, ErrConfiguration
		}
		available[key.ID] = key
	}
	fingerprints := map[string]bool{}
	for _, identifier := range spec.AuthorizedKeyIDs {
		key, found := available[identifier]
		if !found {
			continue
		}
		parsed, _, options, remaining, err := ssh.ParseAuthorizedKey([]byte(key.PublicKey))
		if err != nil || len(options) != 0 || len(strings.TrimSpace(string(remaining))) != 0 || ssh.FingerprintSHA256(parsed) != key.Fingerprint {
			return Configuration{}, ErrConfiguration
		}
		if fingerprints[key.Fingerprint] {
			continue
		}
		fingerprints[key.Fingerprint] = true
		configuration.Keys = append(configuration.Keys, KeyGrant{ID: identifier, PublicKey: key.PublicKey, ExpiresAt: configuration.ValidUntil})
	}
	return configuration, nil
}
