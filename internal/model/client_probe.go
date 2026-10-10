package model

import "time"

type EdgeClientProbeRequest struct {
	Hostname      string `json:"hostname"`
	Path          string `json:"path"`
	TrafficClass  string `json:"traffic_class"`
	ObserverLabel string `json:"observer_label"`
}

type EdgeClientProbePermit struct {
	Schema        string    `json:"schema"`
	RoundID       string    `json:"round_id"`
	AttemptID     string    `json:"attempt_id"`
	ObserverID    string    `json:"observer_id"`
	Hostname      string    `json:"hostname"`
	Path          string    `json:"path"`
	TrafficClass  string    `json:"traffic_class"`
	EdgeID        string    `json:"edge_id"`
	EdgeGroupID   string    `json:"edge_group_id"`
	Address       string    `json:"address"`
	RouteDigest   string    `json:"route_digest"`
	BundleVersion string    `json:"bundle_version"`
	IssuedAt      time.Time `json:"issued_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	BodyBytes     int       `json:"body_bytes"`
	BodySeed      string    `json:"body_seed"`
	BodySHA256    string    `json:"body_sha256"`
	TargetEdgeIDs []string  `json:"target_edge_ids"`
	KeyID         string    `json:"key_id"`
	Signature     string    `json:"signature"`
}

type EdgeClientProbePlan struct {
	Schema        string                  `json:"schema"`
	RoundID       string                  `json:"round_id"`
	ObserverLabel string                  `json:"observer_label"`
	Permits       []EdgeClientProbePermit `json:"permits"`
}

type EdgeClientProbeAttestation struct {
	Schema        string                  `json:"schema"`
	AttemptID     string                  `json:"attempt_id"`
	EdgeID        string                  `json:"edge_id"`
	EdgeGroupID   string                  `json:"edge_group_id"`
	RouteDigest   string                  `json:"route_digest"`
	BundleVersion string                  `json:"bundle_version"`
	ObservedAt    time.Time               `json:"observed_at"`
	ClientNetwork EdgeClientNetworkSample `json:"client_network"`
	KeyID         string                  `json:"key_id"`
	Signature     string                  `json:"signature"`
}

type EdgeClientProbeOutcome struct {
	AttemptID     string                      `json:"attempt_id"`
	StartedAt     time.Time                   `json:"started_at"`
	CompletedAt   time.Time                   `json:"completed_at"`
	BytesReceived int                         `json:"bytes_received"`
	BodySHA256    string                      `json:"body_sha256"`
	BodySeconds   float64                     `json:"body_seconds"`
	Failure       string                      `json:"failure"`
	HTTPStatus    int                         `json:"http_status,omitempty"`
	Attestation   *EdgeClientProbeAttestation `json:"attestation,omitempty"`
}

type EdgeClientProbeReport struct {
	Schema   string                   `json:"schema"`
	Plan     EdgeClientProbePlan      `json:"plan"`
	Outcomes []EdgeClientProbeOutcome `json:"outcomes"`
}
