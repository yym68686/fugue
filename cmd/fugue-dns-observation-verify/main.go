package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
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
	expiredBackendMode = "--verify-expired-predecessor"
	servingPairMode    = "--verify-serving-pair"
)

type verificationInput struct {
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

func verify(input io.Reader, output io.Writer, mode string) error {
	if mode == servingPairMode {
		// Read both independent inputs before sampling one local comparison
		// time. Callers cannot choose the clock or a subset of either plan.
		var pair struct{ Candidate, Fallback *verificationInput }
		limited := &io.LimitedReader{R: input, N: (128 << 20) + 1}
		decoder := json.NewDecoder(limited)
		decoder.DisallowUnknownFields()
		if decoder.Decode(&pair) != nil || decoder.Decode(&struct{}{}) != io.EOF || limited.N == 0 || pair.Candidate == nil || pair.Fallback == nil {
			return errors.New("invalid bounded DNS pair")
		}
		now := time.Now().UTC()
		a, b := pair.Candidate.Snapshot, pair.Fallback.Snapshot
		if a.NodeID != b.NodeID || a.EdgeGroupID != b.EdgeGroupID || a.ParentDigest != b.ParentDigest ||
			a.PlanDigest != b.PlanDigest || a.RouteArtifactID != b.RouteArtifactID || !reflect.DeepEqual(a.Assignment, b.Assignment) {
			return errors.New("DNS serving pair assignment mismatch")
		}
		for _, member := range []struct {
			role string
			in   *verificationInput
		}{{"candidate", pair.Candidate}, {"fallback", pair.Fallback}} {
			if member.in.HandoffPrevious != nil || member.in.AvailabilityRoute != nil || len(member.in.ObservationQueries) != 0 {
				return errors.New("serving pair requires only exact serving inputs")
			}
			if err := verifyRequest(*member.in, io.Discard, "", now); err != nil {
				return fmt.Errorf("%s: %w", member.role, err)
			}
		}
		_, err := fmt.Fprintln(output, `{"verified_serving_pair":true}`)
		return err
	}
	var request verificationInput
	decoder := json.NewDecoder(io.LimitReader(input, 64<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid bounded DNS verification input")
	}
	return verifyRequest(request, output, mode, time.Now().UTC())
}

func verifyRequest(request verificationInput, output io.Writer, mode string, now time.Time) error {
	configuration := config.DNSConfig{DNSNodeID: request.NodeID, EdgeGroupID: request.GroupID, Zone: request.Zone, PlatformScopeKey: request.Scope,
		BundleSigningKey: request.SigningKey, BundleSigningKeyID: request.SigningKeyID,
		BundleSigningPreviousKey: request.PreviousKey, BundleSigningPreviousKeyID: request.PreviousKeyID, BundleRevokedKeyIDs: request.RevokedKeyIDs}
	if mode == exportPolicyMode {
		if request.HandoffPrevious != nil || request.AvailabilityRoute != nil {
			return errors.New("observation policy export cannot authorize a handoff")
		}
		policy, err := dnsserver.VerifiedObservationPolicy(configuration, request.Checkpoint, request.Snapshot, request.ObservationQueries, now)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(policy)
	}
	if len(request.ObservationQueries) != 0 {
		return errors.New("observation queries require explicit policy export")
	}
	if mode == unreadyBackendMode || mode == expiredBackendMode {
		if request.HandoffPrevious != nil || request.AvailabilityRoute != nil {
			return errors.New("unready predecessor verification cannot authorize a handoff")
		}
		if mode == expiredBackendMode {
			if err := dnsserver.VerifyExpiredPredecessorObservation(configuration, request.Checkpoint, request.Snapshot, now); err != nil {
				return err
			}
			_, err := fmt.Fprintln(output, `{"verified_expired_predecessor":true,"authorizes_traffic":false}`)
			return err
		}
		if err := dnsserver.VerifyUnreadyPredecessorObservation(configuration, request.Checkpoint, request.Snapshot, now); err != nil {
			return err
		}
		_, err := fmt.Fprintln(output, `{"verified_unready_predecessor":true,"authorizes_traffic":false}`)
		return err
	}
	var err error
	if request.HandoffPrevious != nil {
		err = dnsserver.VerifyServingAuthorityHandoff(configuration, request.Checkpoint, request.Snapshot, *request.HandoffPrevious, request.AvailabilityRoute, now)
	} else if request.AvailabilityRoute != nil {
		return errors.New("availability change requires explicit handoff predecessor")
	} else {
		err = dnsserver.VerifyServingObservation(configuration, request.Checkpoint, request.Snapshot, now)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, `{"verified":true}`)
	return err
}

func main() {
	var mode string
	if len(os.Args) == 2 && (os.Args[1] == exportPolicyMode || os.Args[1] == unreadyBackendMode || os.Args[1] == expiredBackendMode || os.Args[1] == servingPairMode) {
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
