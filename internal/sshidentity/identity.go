package sshidentity

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"

	"golang.org/x/crypto/ssh"
)

var (
	ErrKeyUnavailable  = errors.New("SSH identity encryption key is unavailable")
	ErrInvalidKeyring  = errors.New("SSH identity keyring requires named 32-byte encryption keys")
	ErrInvalidIdentity = errors.New("SSH identity metadata is invalid")
	ErrIntegrity       = errors.New("SSH identity integrity verification failed")
)

type Keyring struct {
	activeKeyID string
	keys        map[string][]byte
}

func NewKeyring(activeKeyID string, keys map[string][]byte) (Keyring, error) {
	if !validIdentifier(activeKeyID) || len(keys[activeKeyID]) != 32 {
		return Keyring{}, ErrInvalidKeyring
	}
	keyring := Keyring{activeKeyID: activeKeyID, keys: make(map[string][]byte, len(keys))}
	for keyID, key := range keys {
		if !validIdentifier(keyID) || len(key) != 32 {
			return Keyring{}, ErrInvalidKeyring
		}
		keyring.keys[keyID] = append([]byte(nil), key...)
	}
	return keyring, nil
}

func (keyring Keyring) Configured() bool {
	return len(keyring.keys[keyring.activeKeyID]) == 32
}

func (keyring Keyring) Create(tenantID, appID string, now time.Time) (model.AppSSHIdentityRecord, error) {
	if !keyring.Configured() {
		return model.AppSSHIdentityRecord{}, ErrKeyUnavailable
	}
	if !validIdentifier(tenantID) || !validIdentifier(appID) || now.IsZero() {
		return model.AppSSHIdentityRecord{}, ErrInvalidIdentity
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	sshPublicKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	identity := model.AppSSHIdentity{
		ID:          model.NewID("sshidentity"),
		TenantID:    tenantID,
		AppID:       appID,
		Generation:  1,
		Algorithm:   sshPublicKey.Type(),
		PublicKey:   strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPublicKey))),
		Fingerprint: ssh.FingerprintSHA256(sshPublicKey),
		CreatedAt:   now.UTC(),
	}
	return keyring.seal(identity, privateKey.Seed())
}

func ValidatePublic(identity model.AppSSHIdentity) error {
	if !validIdentifier(identity.ID) || !validIdentifier(identity.TenantID) || !validIdentifier(identity.AppID) || identity.Generation == 0 || identity.CreatedAt.IsZero() || identity.Algorithm != ssh.KeyAlgoED25519 {
		return ErrInvalidIdentity
	}
	publicKey, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(identity.PublicKey))
	if err != nil || len(options) != 0 || len(strings.TrimSpace(string(rest))) != 0 {
		return ErrInvalidIdentity
	}
	if publicKey.Type() != identity.Algorithm || ssh.FingerprintSHA256(publicKey) != identity.Fingerprint || strings.TrimSpace(string(ssh.MarshalAuthorizedKey(publicKey))) != identity.PublicKey {
		return ErrInvalidIdentity
	}
	return nil
}

func (keyring Keyring) Signer(record model.AppSSHIdentityRecord) (ssh.Signer, error) {
	seed, err := keyring.openSeed(record)
	if err != nil {
		return nil, err
	}
	return ssh.NewSignerFromKey(ed25519.NewKeyFromSeed(seed))
}

func (keyring Keyring) PrivateKeyPEM(record model.AppSSHIdentityRecord) ([]byte, error) {
	seed, err := keyring.openSeed(record)
	if err != nil {
		return nil, err
	}
	block, err := ssh.MarshalPrivateKey(ed25519.NewKeyFromSeed(seed), record.Identity.ID)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(block), nil
}

func (keyring Keyring) Rewrap(record model.AppSSHIdentityRecord) (model.AppSSHIdentityRecord, error) {
	seed, err := keyring.openSeed(record)
	if err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	return keyring.seal(record.Identity, seed)
}

func (keyring Keyring) seal(identity model.AppSSHIdentity, seed []byte) (model.AppSSHIdentityRecord, error) {
	if err := ValidatePublic(identity); err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	if len(seed) != ed25519.SeedSize {
		return model.AppSSHIdentityRecord{}, ErrIntegrity
	}
	box, err := keyring.box(keyring.activeKeyID)
	if err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	record := model.AppSSHIdentityRecord{Identity: identity, EncryptionKeyID: keyring.activeKeyID}
	nonce := make([]byte, box.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	associatedData, err := identityAssociatedData(record)
	if err != nil {
		return model.AppSSHIdentityRecord{}, err
	}
	record.Ciphertext = base64.StdEncoding.EncodeToString(box.Seal(nonce, nonce, seed, associatedData))
	return record, nil
}

func (keyring Keyring) openSeed(record model.AppSSHIdentityRecord) ([]byte, error) {
	if err := ValidatePublic(record.Identity); err != nil {
		return nil, ErrIntegrity
	}
	box, err := keyring.box(record.EncryptionKeyID)
	if err != nil {
		return nil, err
	}
	encoded, err := base64.StdEncoding.DecodeString(record.Ciphertext)
	if err != nil || len(encoded) != box.NonceSize()+ed25519.SeedSize+box.Overhead() {
		return nil, ErrIntegrity
	}
	associatedData, err := identityAssociatedData(record)
	if err != nil {
		return nil, ErrIntegrity
	}
	seed, err := box.Open(nil, encoded[:box.NonceSize()], encoded[box.NonceSize():], associatedData)
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, ErrIntegrity
	}
	signer, err := ssh.NewSignerFromKey(ed25519.NewKeyFromSeed(seed))
	if err != nil || ssh.FingerprintSHA256(signer.PublicKey()) != record.Identity.Fingerprint {
		return nil, ErrIntegrity
	}
	return seed, nil
}

func (keyring Keyring) box(keyID string) (cipher.AEAD, error) {
	key := keyring.keys[keyID]
	if len(key) != 32 {
		return nil, ErrKeyUnavailable
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func identityAssociatedData(record model.AppSSHIdentityRecord) ([]byte, error) {
	return json.Marshal(struct {
		Schema          string               `json:"schema"`
		Identity        model.AppSSHIdentity `json:"identity"`
		EncryptionKeyID string               `json:"encryption_key_id"`
	}{Schema: "fugue.ssh-identity/v1", Identity: record.Identity, EncryptionKeyID: record.EncryptionKeyID})
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, char := range value {
		if char < 33 || char > 126 {
			return false
		}
	}
	return true
}
