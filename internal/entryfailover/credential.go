package entryfailover

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	c "fugue/internal/staticedgecontract"
)

type credentialEnvelope struct {
	Schema     string    `json:"schema"`
	TenantID   string    `json:"tenant_id"`
	Zone       string    `json:"zone"`
	Version    uint64    `json:"version"`
	Nonce      string    `json:"nonce"`
	Ciphertext string    `json:"ciphertext"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type CredentialVault struct {
	StateDir string
	Key      []byte
}

func LoadVaultKey(path string) ([]byte, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("vault key must be private regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(key) != 32 {
		return nil, errors.New("vault key must contain exactly 32 random bytes in hex")
	}
	return key, nil
}

func NewVaultKey(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("absolute vault key path required")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return errors.New("vault key already exists or path is inaccessible")
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(hex.EncodeToString(key) + "\n"); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (v CredentialVault) path() string { return filepath.Join(v.StateDir, "cloudflare-token.json") }
func (v CredentialVault) aead() (cipher.AEAD, error) {
	if len(v.Key) != 32 || !filepath.IsAbs(v.StateDir) {
		return nil, errors.New("vault key and absolute state directory required")
	}
	block, err := aes.NewCipher(v.Key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func credentialAAD(tenant, zone string, version uint64) []byte {
	return []byte(PolicySchema + "\n" + tenant + "\n" + zone + "\n" + strconv.FormatUint(version, 10))
}

func (v CredentialVault) Put(policy Policy, token string) (uint64, error) {
	if err := policy.Validate(); err != nil {
		return 0, err
	}
	if token == "" || len(token) > 512 || strings.ContainsAny(token, "\r\n \t") {
		return 0, errors.New("invalid Cloudflare token")
	}
	aead, err := v.aead()
	if err != nil {
		return 0, err
	}
	version := uint64(1)
	if old, err := v.metadata(); err == nil {
		if old.TenantID != policy.TenantID || old.Zone != policy.Zone {
			return 0, errors.New("credential ownership differs")
		}
		version = old.Version + 1
	} else if !os.IsNotExist(err) {
		return 0, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return 0, err
	}
	ciphertext := aead.Seal(nil, nonce, []byte(token), credentialAAD(policy.TenantID, policy.Zone, version))
	envelope := credentialEnvelope{Schema: PolicySchema, TenantID: policy.TenantID, Zone: policy.Zone, Version: version,
		Nonce: hex.EncodeToString(nonce), Ciphertext: hex.EncodeToString(ciphertext), UpdatedAt: time.Now().UTC()}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return 0, err
	}
	if err = writeDurable(v.path(), append(raw, '\n')); err != nil {
		return 0, err
	}
	return version, nil
}

func (v CredentialVault) metadata() (credentialEnvelope, error) {
	var envelope credentialEnvelope
	raw, err := os.ReadFile(v.path())
	if err != nil {
		return envelope, err
	}
	if err = c.StrictJSON(raw, &envelope); err != nil {
		return envelope, err
	}
	if envelope.Schema != PolicySchema || !identifier.MatchString(envelope.TenantID) || !validName(envelope.Zone) || envelope.Version == 0 {
		return envelope, errors.New("invalid credential metadata")
	}
	return envelope, nil
}

func (v CredentialVault) Present(policy Policy) (bool, uint64, error) {
	envelope, err := v.metadata()
	if os.IsNotExist(err) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	if envelope.TenantID != policy.TenantID || envelope.Zone != policy.Zone {
		return false, 0, errors.New("credential ownership differs")
	}
	return true, envelope.Version, nil
}

func (v CredentialVault) Token(policy Policy) (string, error) {
	envelope, err := v.metadata()
	if err != nil {
		return "", err
	}
	if envelope.TenantID != policy.TenantID || envelope.Zone != policy.Zone {
		return "", errors.New("credential ownership differs")
	}
	aead, err := v.aead()
	if err != nil {
		return "", err
	}
	nonce, err := hex.DecodeString(envelope.Nonce)
	if err != nil {
		return "", err
	}
	ciphertext, err := hex.DecodeString(envelope.Ciphertext)
	if err != nil {
		return "", err
	}
	if len(nonce) != aead.NonceSize() {
		return "", errors.New("invalid credential nonce")
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, credentialAAD(envelope.TenantID, envelope.Zone, envelope.Version))
	if err != nil {
		return "", errors.New("credential authentication failed")
	}
	return string(plaintext), nil
}
