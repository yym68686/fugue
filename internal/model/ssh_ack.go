package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"
)

type EdgeSSHAcknowledgementRequest struct {
	EdgeID        string                    `json:"edge_id"`
	EdgeGroupID   string                    `json:"edge_group_id"`
	InstanceUID   string                    `json:"instance_uid"`
	BundleVersion string                    `json:"bundle_version"`
	RouteDigest   string                    `json:"route_digest"`
	Persisted     bool                      `json:"persisted"`
	LoadedAt      time.Time                 `json:"loaded_at"`
	Connections   []EdgeSSHRouteConnections `json:"connections"`
}

type EdgeSSHRouteConnections struct {
	AppID      string `json:"app_id"`
	PublicPort int    `json:"public_port"`
	TargetHost string `json:"target_host"`
	TargetPort int    `json:"target_port"`
	Count      int    `json:"count"`
}

type EdgeSSHAcknowledgement struct {
	EdgeSSHAcknowledgementRequest
	Routes     []EdgeSSHRoute `json:"routes"`
	ObservedAt time.Time      `json:"observed_at"`
	ExpiresAt  time.Time      `json:"expires_at"`
}

func SSHRoutesDigest(routes []EdgeSSHRoute) string {
	ordered := append([]EdgeSSHRoute{}, routes...)
	sort.Slice(ordered, func(first, second int) bool {
		if ordered[first].PublicPort == ordered[second].PublicPort {
			return ordered[first].AppID < ordered[second].AppID
		}
		return ordered[first].PublicPort < ordered[second].PublicPort
	})
	raw, _ := json.Marshal(ordered)
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:])
}
