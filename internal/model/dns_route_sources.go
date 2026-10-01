package model

import "time"

type PlatformConsumerDNSRouteSourcesResponse struct {
	Assignment PlatformConsumerAssignment     `json:"assignment"`
	Release    PlatformArtifactRelease        `json:"release"`
	Snapshot   PlatformDNSRouteSourceSnapshot `json:"snapshot"`
}

type PlatformDNSRouteSourceSnapshot struct {
	DNSArtifactID     string                        `json:"dns_artifact_id"`
	DNSArtifactDigest string                        `json:"dns_artifact_digest"`
	SelectionDigest   string                        `json:"selection_digest"`
	ObservedAt        time.Time                     `json:"observed_at"`
	Scopes            []PlatformDNSRouteSourceScope `json:"scopes"`
}

type PlatformDNSRouteSourceScope struct {
	ScopeKey     string                              `json:"scope_key"`
	Lanes        []PlatformDNSRouteSourceLane        `json:"lanes"`
	Publications []PlatformDNSRouteSourcePublication `json:"publications"`
}

type PlatformDNSRouteSourceLane struct {
	ReleaseChannel  string `json:"release_channel"`
	FencingToken    int64  `json:"fencing_token"`
	Version         int64  `json:"version"`
	ActiveReleaseID string `json:"active_release_id"`
}

type PlatformDNSRouteSourcePublication struct {
	Selections      []string                `json:"selections"`
	Parent          PlatformArtifact        `json:"parent"`
	Route           PlatformArtifact        `json:"route"`
	TLS             PlatformArtifact        `json:"tls"`
	Release         PlatformArtifactRelease `json:"release"`
	ProducerPolicy  PlatformArtifact        `json:"producer_policy"`
	ProducerRelease PlatformArtifactRelease `json:"producer_release"`
	LKG             *PlatformLKGSnapshot    `json:"lkg,omitempty"`
}
