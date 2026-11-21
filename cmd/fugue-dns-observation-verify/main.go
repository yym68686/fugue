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
	"fugue/internal/dnsobservation"
	"fugue/internal/dnsserver"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

const (
	exportPolicyMode   = "--export-policy"
	unreadyBackendMode = "--verify-unready-predecessor"
)

func verify(input io.Reader, output io.Writer, mode string) error {
	var request struct {
		ObservationQueries []dnsobservation.Query               `json:"observation_queries,omitempty"`
		HandoffPrevious    *model.PlatformArtifact              `json:"handoff_previous,omitempty"`
		AvailabilityRoute  *platformconfig.DNSAvailabilityRoute `json:"availability_route,omitempty"`
		NodeID             string                               `json:"node_id"`
		GroupID            string                               `json:"group_id"`
		Zone               string                               `json:"zone"`
		Scope              string                               `json:"scope"`
		SigningKey         string                               `json:"signing_key"`
		SigningKeyID       string                               `json:"signing_key_id"`
		PreviousKey        string                               `json:"previous_key"`
		PreviousKeyID      string                               `json:"previous_key_id"`
		RevokedKeyIDs      []string                             `json:"revoked_key_ids"`
		Checkpoint         json.RawMessage                      `json:"checkpoint"`
		Snapshot           dnsfacts.Snapshot                    `json:"snapshot"`
	}
	decoder := json.NewDecoder(io.LimitReader(input, 64<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid bounded DNS verification input")
	}
	configuration := config.DNSConfig{DNSNodeID: request.NodeID, EdgeGroupID: request.GroupID, Zone: request.Zone, PlatformScopeKey: request.Scope,
		BundleSigningKey: request.SigningKey, BundleSigningKeyID: request.SigningKeyID,
		BundleSigningPreviousKey: request.PreviousKey, BundleSigningPreviousKeyID: request.PreviousKeyID, BundleRevokedKeyIDs: request.RevokedKeyIDs}
	if mode == exportPolicyMode {
		if request.HandoffPrevious != nil || request.AvailabilityRoute != nil {
			return errors.New("observation policy export cannot authorize a handoff")
		}
		policy, err := dnsserver.VerifiedObservationPolicy(configuration, request.Checkpoint, request.Snapshot, request.ObservationQueries, time.Now().UTC())
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(policy)
	}
	if len(request.ObservationQueries) != 0 {
		return errors.New("observation queries require explicit policy export")
	}
	if mode == unreadyBackendMode {
		if request.HandoffPrevious != nil || request.AvailabilityRoute != nil {
			return errors.New("unready predecessor verification cannot authorize a handoff")
		}
		if err := dnsserver.VerifyUnreadyPredecessorObservation(configuration, request.Checkpoint, request.Snapshot, time.Now().UTC()); err != nil {
			return err
		}
		_, err := fmt.Fprintln(output, `{"verified_unready_predecessor":true,"authorizes_traffic":false}`)
		return err
	}
	var err error
	if request.HandoffPrevious != nil {
		err = dnsserver.VerifyServingAuthorityHandoff(configuration, request.Checkpoint, request.Snapshot, *request.HandoffPrevious, request.AvailabilityRoute, time.Now().UTC())
	} else if request.AvailabilityRoute != nil {
		return errors.New("availability change requires explicit handoff predecessor")
	} else {
		err = dnsserver.VerifyServingObservation(configuration, request.Checkpoint, request.Snapshot, time.Now().UTC())
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, `{"verified":true}`)
	return err
}

func main() {
	var mode string
	if len(os.Args) == 2 && (os.Args[1] == exportPolicyMode || os.Args[1] == unreadyBackendMode) {
		mode = os.Args[1]
	} else if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "unsupported DNS verification mode")
		os.Exit(1)
	}
	if err := verify(os.Stdin, os.Stdout, mode); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
