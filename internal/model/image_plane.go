package model

import "time"

const (
	ImagePlaneAPIVersionV1             = "image-plane.fugue.dev/v1"
	ImageReplicationPlanKind           = "ImageReplicationPlan"
	ImageReplicationPlanStateKind      = "ImageReplicationPlanState"
	ImageReplicationPlanReleaseChannel = PlatformArtifactReleaseChannelShadow
)

// ImageReplicationPlanStateResponse is the least-privilege desired-state
// contract returned to one node-bound image-cache component. The control plane
// derives Component, NodeID, ScopeKey, ArtifactKind, and ReleaseChannel from
// the verified component identity; none of those trust boundaries are caller
// selectable.
//
// LKGArtifact carries the signed content referenced by LKG. A fresh component
// therefore has a usable recovery input even when there is no locally cached
// generation yet. Existing components still keep their own lane-local copy.
type ImageReplicationPlanStateResponse struct {
	APIVersion            string                   `json:"api_version"`
	Kind                  string                   `json:"kind"`
	Component             string                   `json:"component"`
	NodeID                string                   `json:"node_id"`
	ScopeKey              string                   `json:"scope_key"`
	ArtifactKind          string                   `json:"artifact_kind"`
	ReleaseChannel        string                   `json:"release_channel"`
	Artifact              *PlatformArtifact        `json:"artifact,omitempty"`
	Release               *PlatformArtifactRelease `json:"release,omitempty"`
	LKG                   *PlatformLKGSnapshot     `json:"lkg,omitempty"`
	LKGArtifact           *PlatformArtifact        `json:"lkg_artifact,omitempty"`
	ExpectedConsumerSetID string                   `json:"expected_consumer_set_id,omitempty"`
	Generation            string                   `json:"generation"`
	Waited                bool                     `json:"waited"`
	ServerTime            time.Time                `json:"server_time"`
}
