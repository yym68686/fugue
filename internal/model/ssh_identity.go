package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"time"
)

type AppSSHIdentity struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	AppID       string    `json:"app_id"`
	Generation  uint64    `json:"generation"`
	Algorithm   string    `json:"algorithm"`
	PublicKey   string    `json:"public_key"`
	Fingerprint string    `json:"fingerprint"`
	CreatedAt   time.Time `json:"created_at"`
}

type AppSSHIdentityRecord struct {
	Identity        AppSSHIdentity `json:"identity"`
	EncryptionKeyID string         `json:"encryption_key_id"`
	Ciphertext      string         `json:"ciphertext"`
}

type AppSSHObservation struct {
	ID                  string    `json:"id"`
	TenantID            string    `json:"tenant_id"`
	AppID               string    `json:"app_id"`
	IdentityID          string    `json:"identity_id"`
	EndpointID          string    `json:"endpoint_id"`
	RouteDigest         string    `json:"route_digest"`
	Scope               string    `json:"scope"`
	Observer            string    `json:"observer"`
	Target              string    `json:"target"`
	Algorithm           string    `json:"algorithm,omitempty"`
	Fingerprint         string    `json:"fingerprint,omitempty"`
	KeyExchangeVerified bool      `json:"key_exchange_verified"`
	IdentityMatched     bool      `json:"identity_matched"`
	ObservedAt          time.Time `json:"observed_at"`
	ExpiresAt           time.Time `json:"expires_at"`
	Reason              string    `json:"reason"`
}

func AppSSHRouteDigest(endpoint AppSSHEndpoint) string {
	payload, _ := json.Marshal(struct {
		ID, TenantID, AppID, Hostname, TargetHost, RuntimeID, Status string
		PublicPort, TargetPort                                       int
	}{endpoint.ID, endpoint.TenantID, endpoint.AppID, endpoint.Hostname, endpoint.TargetHost, endpoint.RuntimeID, endpoint.Status, endpoint.PublicPort, endpoint.TargetPort})
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func AppSSHPublicAddress(endpoint AppSSHEndpoint) string {
	return net.JoinHostPort(strings.TrimSpace(endpoint.Hostname), strconv.Itoa(endpoint.PublicPort))
}
