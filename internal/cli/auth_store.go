package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

const (
	fugueCredentialService = "fugue-cli"
	authConfigFileMode     = 0o600
	authConfigDirMode      = 0o700
)

type authTokenSource string

const (
	authTokenSourceNone           authTokenSource = ""
	authTokenSourceFlag           authTokenSource = "flag"
	authTokenSourceEnvFugueToken  authTokenSource = "env:FUGUE_TOKEN"
	authTokenSourceEnvAPIKey      authTokenSource = "env:FUGUE_API_KEY"
	authTokenSourceEnvBootstrap   authTokenSource = "env:FUGUE_BOOTSTRAP_KEY"
	authTokenSourceSavedKeychain  authTokenSource = "saved:keychain"
	authTokenSourceSavedSecretSvc authTokenSource = "saved:secret-service"
	authTokenSourceSavedFile      authTokenSource = "saved:file"
)

type authStoredCredential struct {
	BaseURL   string `json:"base_url" yaml:"base_url"`
	Token     string `json:"token" yaml:"token"`
	Source    string `json:"source,omitempty" yaml:"source,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty" yaml:"updated_at,omitempty"`
}

type authConfigFile struct {
	Credentials []authStoredCredential `json:"credentials" yaml:"credentials"`
}

type authTokenMetadata struct {
	BaseURL   string          `json:"base_url"`
	Source    authTokenSource `json:"source"`
	Location  string          `json:"location,omitempty"`
	UpdatedAt string          `json:"updated_at,omitempty"`
}

func (c *CLI) effectiveTokenWithSource() (string, authTokenSource) {
	if token, source := c.transientTokenWithSource(); token != "" {
		return token, source
	}
	cred, ok, _ := loadSavedAuthCredential(c.effectiveBaseURL())
	if ok {
		return cred.Token, authTokenSource(cred.Source)
	}
	return "", authTokenSourceNone
}

func (c *CLI) transientTokenWithSource() (string, authTokenSource) {
	if token := strings.TrimSpace(c.root.Token); token != "" {
		return token, authTokenSourceFlag
	}
	if token := strings.TrimSpace(os.Getenv("FUGUE_TOKEN")); token != "" {
		return token, authTokenSourceEnvFugueToken
	}
	if token := strings.TrimSpace(os.Getenv("FUGUE_API_KEY")); token != "" {
		return token, authTokenSourceEnvAPIKey
	}
	if token := strings.TrimSpace(os.Getenv("FUGUE_BOOTSTRAP_KEY")); token != "" {
		return token, authTokenSourceEnvBootstrap
	}
	return "", authTokenSourceNone
}

func (c *CLI) saveRootTokenIfRequested(cmd *cobra.Command) error {
	if c == nil || !c.root.SaveToken {
		return nil
	}
	if strings.TrimSpace(c.root.Token) == "" {
		return fmt.Errorf("--save-token requires --token")
	}
	if cmd != nil && cmd.CommandPath() == "fugue auth login" {
		return nil
	}
	cred, err := c.verifyAndSaveAuthToken(c.root.Token, c.effectiveBaseURL())
	if err != nil {
		return err
	}
	if !c.wantsJSON() {
		c.progressf("Saved Fugue API key for %s in %s.", cred.BaseURL, authTokenLocation(authTokenSource(cred.Source)))
	}
	return nil
}

func (c *CLI) verifyAndSaveAuthToken(token, baseURL string) (authStoredCredential, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return authStoredCredential{}, fmt.Errorf("token is required")
	}
	baseURL = canonicalAuthBaseURL(baseURL)
	client, err := newClientWithOptions(baseURL, token, clientOptions{RequireToken: true})
	if err != nil {
		return authStoredCredential{}, err
	}
	if _, err := client.GetAuthContext(); err != nil {
		return authStoredCredential{}, fmt.Errorf("verify token: %w", err)
	}
	cred, err := saveAuthCredential(baseURL, token)
	if err != nil {
		return authStoredCredential{}, err
	}
	return cred, nil
}

func loadSavedAuthCredential(baseURL string) (authStoredCredential, bool, error) {
	baseURL = canonicalAuthBaseURL(baseURL)
	if token, ok := loadSystemAuthToken(baseURL); ok {
		return authStoredCredential{BaseURL: baseURL, Token: token, Source: string(systemAuthTokenSource()), UpdatedAt: savedAuthFileCredentialUpdatedAt(baseURL)}, true, nil
	}
	cred, ok, err := loadFileAuthCredential(baseURL)
	if err != nil {
		return authStoredCredential{}, false, err
	}
	return cred, ok, nil
}

func saveAuthCredential(baseURL, token string) (authStoredCredential, error) {
	baseURL = canonicalAuthBaseURL(baseURL)
	token = strings.TrimSpace(token)
	source := authTokenSourceSavedFile
	if saveSystemAuthToken(baseURL, token) {
		source = systemAuthTokenSource()
	}
	cred := authStoredCredential{
		BaseURL:   baseURL,
		Token:     token,
		Source:    string(source),
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	fileCred := cred
	if source != authTokenSourceSavedFile {
		fileCred.Token = ""
	}
	if err := saveFileAuthCredential(fileCred); err != nil {
		return authStoredCredential{}, err
	}
	cred.Source = string(source)
	if source != authTokenSourceSavedFile {
		cred.Token = ""
	}
	return cred, nil
}

func deleteSavedAuthCredential(baseURL string) (bool, error) {
	baseURL = canonicalAuthBaseURL(baseURL)
	systemDeleted := deleteSystemAuthToken(baseURL)
	fileDeleted, err := deleteFileAuthCredential(baseURL)
	if err != nil {
		return false, err
	}
	return systemDeleted || fileDeleted, nil
}

func savedAuthTokenMetadata(baseURL string) (authTokenMetadata, bool, error) {
	baseURL = canonicalAuthBaseURL(baseURL)
	if _, ok := loadSystemAuthToken(baseURL); ok {
		return authTokenMetadata{
			BaseURL:   baseURL,
			Source:    systemAuthTokenSource(),
			Location:  systemAuthLocation(),
			UpdatedAt: savedAuthFileCredentialUpdatedAt(baseURL),
		}, true, nil
	}
	cred, ok, err := loadFileAuthCredential(baseURL)
	if err != nil || !ok {
		return authTokenMetadata{}, ok, err
	}
	return authTokenMetadata{
		BaseURL:   baseURL,
		Source:    authTokenSourceSavedFile,
		Location:  authConfigPath(),
		UpdatedAt: cred.UpdatedAt,
	}, true, nil
}

func canonicalAuthBaseURL(baseURL string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		baseURL = defaultCloudBaseURL
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return strings.TrimRight(baseURL, "/")
	}
	if parsed.Scheme != "" {
		parsed.Scheme = strings.ToLower(parsed.Scheme)
	}
	if parsed.Host != "" {
		parsed.Host = strings.ToLower(parsed.Host)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

func authConfigPath() string {
	if path := strings.TrimSpace(os.Getenv("FUGUE_CONFIG_FILE")); path != "" {
		return path
	}
	if dir := strings.TrimSpace(os.Getenv("FUGUE_CONFIG_DIR")); dir != "" {
		return filepath.Join(dir, "config.yaml")
	}
	if dir, err := os.UserConfigDir(); err == nil && strings.TrimSpace(dir) != "" {
		return filepath.Join(dir, "fugue", "config.yaml")
	}
	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		return filepath.Join(home, ".config", "fugue", "config.yaml")
	}
	return filepath.Join(".fugue", "config.yaml")
}

func loadAuthConfigFile() (authConfigFile, error) {
	path := authConfigPath()
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return authConfigFile{}, nil
		}
		return authConfigFile{}, err
	}
	var cfg authConfigFile
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return authConfigFile{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, nil
}

func saveAuthConfigFile(cfg authConfigFile) error {
	path := authConfigPath()
	if err := os.MkdirAll(filepath.Dir(path), authConfigDirMode); err != nil {
		return err
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, authConfigFileMode)
}

func loadFileAuthCredential(baseURL string) (authStoredCredential, bool, error) {
	cfg, err := loadAuthConfigFile()
	if err != nil {
		return authStoredCredential{}, false, err
	}
	baseURL = canonicalAuthBaseURL(baseURL)
	for _, cred := range cfg.Credentials {
		if canonicalAuthBaseURL(cred.BaseURL) != baseURL {
			continue
		}
		cred.BaseURL = baseURL
		if cred.Source == "" {
			cred.Source = string(authTokenSourceSavedFile)
		}
		if strings.TrimSpace(cred.Token) == "" {
			return authStoredCredential{}, false, nil
		}
		return cred, true, nil
	}
	return authStoredCredential{}, false, nil
}

func saveFileAuthCredential(cred authStoredCredential) error {
	cfg, err := loadAuthConfigFile()
	if err != nil {
		return err
	}
	cred.BaseURL = canonicalAuthBaseURL(cred.BaseURL)
	updated := false
	for idx := range cfg.Credentials {
		if canonicalAuthBaseURL(cfg.Credentials[idx].BaseURL) != cred.BaseURL {
			continue
		}
		cfg.Credentials[idx] = cred
		updated = true
		break
	}
	if !updated {
		cfg.Credentials = append(cfg.Credentials, cred)
	}
	return saveAuthConfigFile(cfg)
}

func deleteFileAuthCredential(baseURL string) (bool, error) {
	cfg, err := loadAuthConfigFile()
	if err != nil {
		return false, err
	}
	baseURL = canonicalAuthBaseURL(baseURL)
	next := cfg.Credentials[:0]
	deleted := false
	for _, cred := range cfg.Credentials {
		if canonicalAuthBaseURL(cred.BaseURL) == baseURL {
			deleted = true
			continue
		}
		next = append(next, cred)
	}
	cfg.Credentials = next
	if !deleted {
		return false, nil
	}
	return true, saveAuthConfigFile(cfg)
}

func savedAuthFileCredentialUpdatedAt(baseURL string) string {
	cfg, err := loadAuthConfigFile()
	if err != nil {
		return ""
	}
	baseURL = canonicalAuthBaseURL(baseURL)
	for _, cred := range cfg.Credentials {
		if canonicalAuthBaseURL(cred.BaseURL) == baseURL {
			return cred.UpdatedAt
		}
	}
	return ""
}

func authTokenFingerprint(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])[:12]
}

func displayAuthTokenSource(source authTokenSource) string {
	switch source {
	case authTokenSourceFlag:
		return "--token"
	case authTokenSourceEnvFugueToken:
		return "FUGUE_TOKEN"
	case authTokenSourceEnvAPIKey:
		return "FUGUE_API_KEY"
	case authTokenSourceEnvBootstrap:
		return "FUGUE_BOOTSTRAP_KEY"
	case authTokenSourceSavedKeychain:
		return "saved credential: macOS Keychain"
	case authTokenSourceSavedSecretSvc:
		return "saved credential: Secret Service"
	case authTokenSourceSavedFile:
		return "saved credential file"
	default:
		return "none"
	}
}

func authTokenLocation(source authTokenSource) string {
	switch source {
	case authTokenSourceSavedKeychain:
		return "macOS Keychain"
	case authTokenSourceSavedSecretSvc:
		return "Secret Service"
	case authTokenSourceSavedFile:
		return authConfigPath()
	default:
		return displayAuthTokenSource(source)
	}
}

func systemAuthTokenSource() authTokenSource {
	switch runtime.GOOS {
	case "darwin":
		return authTokenSourceSavedKeychain
	case "linux":
		return authTokenSourceSavedSecretSvc
	default:
		return authTokenSourceSavedFile
	}
}

func systemAuthLocation() string {
	switch runtime.GOOS {
	case "darwin":
		return "macOS Keychain"
	case "linux":
		return "Secret Service"
	default:
		return ""
	}
}

func authCredentialAccount(baseURL string) string {
	return canonicalAuthBaseURL(baseURL)
}

func loadSystemAuthToken(baseURL string) (string, bool) {
	if systemAuthStoreDisabled() {
		return "", false
	}
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("security", "find-generic-password", "-s", fugueCredentialService, "-a", authCredentialAccount(baseURL), "-w").Output()
		if err != nil {
			return "", false
		}
		token := strings.TrimSpace(string(out))
		return token, token != ""
	case "linux":
		if _, err := exec.LookPath("secret-tool"); err != nil {
			return "", false
		}
		out, err := exec.Command("secret-tool", "lookup", "service", fugueCredentialService, "base_url", canonicalAuthBaseURL(baseURL)).Output()
		if err != nil {
			return "", false
		}
		token := strings.TrimSpace(string(out))
		return token, token != ""
	default:
		return "", false
	}
}

func saveSystemAuthToken(baseURL, token string) bool {
	if systemAuthStoreDisabled() {
		return false
	}
	switch runtime.GOOS {
	case "darwin":
		cmd := exec.Command("security", "add-generic-password", "-U", "-s", fugueCredentialService, "-a", authCredentialAccount(baseURL), "-w", token)
		return cmd.Run() == nil
	case "linux":
		if _, err := exec.LookPath("secret-tool"); err != nil {
			return false
		}
		cmd := exec.Command("secret-tool", "store", "--label", "Fugue CLI API key", "service", fugueCredentialService, "base_url", canonicalAuthBaseURL(baseURL))
		cmd.Stdin = strings.NewReader(token)
		return cmd.Run() == nil
	default:
		return false
	}
}

func deleteSystemAuthToken(baseURL string) bool {
	if systemAuthStoreDisabled() {
		return false
	}
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("security", "delete-generic-password", "-s", fugueCredentialService, "-a", authCredentialAccount(baseURL)).Run() == nil
	case "linux":
		if _, err := exec.LookPath("secret-tool"); err != nil {
			return false
		}
		return exec.Command("secret-tool", "clear", "service", fugueCredentialService, "base_url", canonicalAuthBaseURL(baseURL)).Run() == nil
	default:
		return false
	}
}

func systemAuthStoreDisabled() bool {
	value := strings.TrimSpace(os.Getenv("FUGUE_AUTH_STORAGE"))
	return strings.EqualFold(value, "file") || strings.EqualFold(strings.TrimSpace(os.Getenv("FUGUE_AUTH_DISABLE_SYSTEM_STORE")), "1") || strings.EqualFold(strings.TrimSpace(os.Getenv("FUGUE_AUTH_DISABLE_SYSTEM_STORE")), "true")
}
