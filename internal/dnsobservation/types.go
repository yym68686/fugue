// Package dnsobservation describes bounded, read-only public DNS expectations.
// These receipts are evidence of candidate membership, never traffic authority.
package dnsobservation

import (
	"time"

	"fugue/internal/model"
)

const (
	PolicySchema        = "fugue.dns-observation-policy/v1"
	ExpectationsSchema  = "fugue.dns-observation-expectations/v1"
	ServingPolicySource = "verified_serving_policy"
)

type Query struct {
	Hostname string `json:"hostname"`
	Type     string `json:"type"`
}

type Record struct {
	Query
	Allowed    []string `json:"allowed_answers"`
	MaximumTTL uint32   `json:"maximum_ttl"`
}

type Policy struct {
	Schema       string                           `json:"schema"`
	NodeID       string                           `json:"node_id"`
	Assignment   model.PlatformConsumerAssignment `json:"assignment"`
	ParentDigest string                           `json:"parent_digest"`
	CapturedAt   time.Time                        `json:"captured_at"`
	ValidUntil   time.Time                        `json:"valid_until"`
	Records      []Record                         `json:"records"`
}

type Server struct {
	Server     string `json:"server"`
	ServiceUID string `json:"service_uid"`
	PodUID     string `json:"pod_uid"`
	Policy     Policy `json:"policy"`
}

type Expectations struct {
	Schema            string   `json:"schema"`
	DeclarationDigest string   `json:"declaration_digest"`
	Servers           []Server `json:"servers"`
}
