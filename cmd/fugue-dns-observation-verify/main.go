package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"fugue/internal/config"
	"fugue/internal/dnsfacts"
	"fugue/internal/dnsserver"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func verify(input io.Reader) error {
	var request struct {
		HandoffPrevious   *model.PlatformArtifact              `json:"handoff_previous,omitempty"`
		AvailabilityRoute *platformconfig.DNSAvailabilityRoute `json:"availability_route,omitempty"`
		NodeID            string                               `json:"node_id"`
		GroupID           string                               `json:"group_id"`
		Zone              string                               `json:"zone"`
		Scope             string                               `json:"scope"`
		SigningKey        string                               `json:"signing_key"`
		SigningKeyID      string                               `json:"signing_key_id"`
		PreviousKey       string                               `json:"previous_key"`
		PreviousKeyID     string                               `json:"previous_key_id"`
		RevokedKeyIDs     []string                             `json:"revoked_key_ids"`
		Checkpoint        json.RawMessage                      `json:"checkpoint"`
		Snapshot          dnsfacts.Snapshot                    `json:"snapshot"`
	}
	decoder := json.NewDecoder(io.LimitReader(input, 64<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid bounded DNS verification input")
	}
	configuration := config.DNSConfig{DNSNodeID: request.NodeID, EdgeGroupID: request.GroupID, Zone: request.Zone, PlatformScopeKey: request.Scope,
		BundleSigningKey: request.SigningKey, BundleSigningKeyID: request.SigningKeyID,
		BundleSigningPreviousKey: request.PreviousKey, BundleSigningPreviousKeyID: request.PreviousKeyID, BundleRevokedKeyIDs: request.RevokedKeyIDs}
	if request.HandoffPrevious != nil {
		return dnsserver.VerifyServingAuthorityHandoff(configuration, request.Checkpoint, request.Snapshot, *request.HandoffPrevious, request.AvailabilityRoute, time.Now().UTC())
	}
	if request.AvailabilityRoute != nil {
		return errors.New("availability change requires explicit handoff predecessor")
	}
	return dnsserver.VerifyServingObservation(configuration, request.Checkpoint, request.Snapshot, time.Now().UTC())
}

func main() {
	if err := verify(os.Stdin); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	fmt.Println(`{"verified":true}`)
}
