package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
)

var errHostedDNSPathTopology = errors.New("hosted DNS paths and DNS address authority must change in separate publications")

// Keep old routing intent intact while a DNS authority is upgrading. No
// timestamp, process version, or merely staged consumer counts as readiness.
func (s *Server) checkProjectHostDNSContinuity(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := s.checkGlobalHostedPathTransition(ctx); err != nil {
		return err
	}
	requirement := projectHostConsumerCapability{model.PlatformArtifactKindDNSAnswerBundle, platformconfig.DNSManagedHostContinuityCapabilityV1}
	scopes, err := s.store.ListPlatformProducerScopes()
	if err != nil || ctx.Err() != nil {
		return errProjectHostDNSContinuity
	}
	for _, scope := range scopes {
		artifact, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, scope, model.PlatformArtifactReleaseChannelShadow)
		if err != nil || !found || s.store.VerifyPlatformArtifactIntegrity(artifact) != nil {
			return errProjectHostDNSContinuity
		}
		policy, err := platformproducer.Decode(artifact)
		if err != nil {
			return errProjectHostDNSContinuity
		}
		if policy.PublicationRole != platformconfig.PublicationRoleCellDNS || policy.Mode != "serving" {
			continue
		}
		if !s.projectHostAuthoritySupports(ctx, policy.TargetScope, platformconfig.PublicationRoleCellDNS, requirement) {
			return errProjectHostDNSContinuity
		}
	}
	if ctx.Err() != nil {
		return errProjectHostDNSContinuity
	}
	return nil
}

func (s *Server) checkGlobalHostedPathTransition(ctx context.Context) error {
	if !s.projectHostAuthoritySupports(ctx, platformconfig.GlobalScopeKey, "",
		projectHostConsumerCapability{model.PlatformArtifactKindDNSAnswerBundle, platformconfig.DNSHostedPathTransitionCapabilityV1}) {
		return fmt.Errorf("%w: selected global DNS backends must report %s on the current verified release",
			errProjectHostDNSContinuity, platformconfig.DNSHostedPathTransitionCapabilityV1)
	}
	return nil
}

// The producer can recapture saved intent without another CLI write, even
// after a failed gray publication. The selected global DNS capability alone
// is not sufficient: restoring owner-only routing would also require every
// serving DNS authority to pass the normal managed-host continuity preflight.
func (s *Server) recheckProducedHostedPathTransition(ctx context.Context, full, next model.PlatformArtifact) error {
	if next.ScopeKey != platformconfig.GlobalScopeKey || full.ScopeKey != next.ScopeKey ||
		next.Content["publication_role"] == platformconfig.PublicationRoleCellDNS {
		return nil
	}
	before, err := s.consumerAssignmentChild(full, model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		return err
	}
	after, err := s.consumerAssignmentChild(next, model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		return err
	}
	return guardProducedHostedDNSPaths(ctx, full, next, before, after, s.checkProjectHostDNSContinuity)
}

func guardProducedHostedDNSPaths(ctx context.Context, full, next, before, after model.PlatformArtifact, rollbackDNS func(context.Context) error) error {
	ref, digest, srcErr := platformconfig.HostedAddressAuthority(after)
	if srcErr != nil {
		return srcErr
	}
	restore, err := platformconfig.HostedDeclarationRestoreAuthority(after)
	if err != nil {
		return err
	}
	parentRestore, err := platformconfig.HostedDeclarationRestoreAuthority(next)
	if err != nil {
		return err
	}
	if restore != nil || parentRestore != nil {
		// This precheck classifies the candidate. Gray admission still
		// verifies signed history, holds and the exact declaration-only
		// delta against the current full and LKG inside the store transaction.
		parentRef, parentDigest, err := platformconfig.HostedAddressAuthority(next)
		if err != nil || restore == nil || parentRestore == nil || *restore != *parentRestore ||
			ref == nil || parentRef == nil || *ref != *parentRef || digest != parentDigest ||
			ref.ReleaseSetID != full.ID || ref.ReleaseSetDigest != full.ContentHash ||
			ref.DNSArtifactID != before.ID || ref.DNSArtifactDigest != before.ContentHash {
			return errors.New("shared DNS restore does not bind the selected parent and DNS child")
		}
	} else if ref != nil {
		if err := platformconfig.ValidateScopedHostedPublicationTransition(before, after); err != nil {
			return err
		}
	}
	changed, err := hostedDNSProbePathsChanged(before, after)
	if err != nil || !changed {
		return err
	}
	if restore != nil {
		return errHostedDNSPathTopology
	}
	return rollbackDNS(ctx)
}

// Match existing hosted address records by owner, not just by hostname.
// New and removed domains, external records and deployment-only digest
// changes must not block unrelated publications during a binary rollout.
func hostedDNSProbePathsChanged(before, after model.PlatformArtifact) (bool, error) {
	type owner struct{ host, app, tenant string }
	type evidence struct {
		owners  map[owner]bool
		records map[string][]platformconfig.DNSIntent
		targets map[string]platformconfig.DNSReadinessRecord
		paths   map[string]map[string]bool
	}
	read := func(artifact model.PlatformArtifact) (evidence, error) {
		data, err := json.Marshal(struct {
			Records any `json:"records"`
			Plan    any `json:"readiness_plan"`
		}{artifact.Content["records"], artifact.Content["readiness_plan"]})
		var payload struct {
			Records []platformconfig.DNSIntent       `json:"records"`
			Plan    *platformconfig.DNSReadinessPlan `json:"readiness_plan"`
		}
		if err != nil || json.Unmarshal(data, &payload) != nil {
			return evidence{}, errors.New("hosted DNS transition input is invalid")
		}
		out := evidence{
			owners:  map[owner]bool{},
			records: map[string][]platformconfig.DNSIntent{},
			targets: map[string]platformconfig.DNSReadinessRecord{},
			paths:   map[string]map[string]bool{},
		}
		for _, record := range payload.Records {
			if record.Type == "A" || record.Type == "AAAA" || record.Type == "CNAME" {
				out.records[record.Hostname] = append(out.records[record.Hostname], record)
			}
			if record.RecordKind == "hosted" && (record.Type == "A" || record.Type == "AAAA") &&
				record.Hostname != "" && record.AppID != "" && record.TenantID != "" {
				out.owners[owner{record.Hostname, record.AppID, record.TenantID}] = true
			}
		}
		if payload.Plan != nil {
			for _, record := range payload.Plan.Records {
				if _, found := out.targets[record.Hostname]; found {
					return evidence{}, errors.New("hosted DNS transition has duplicate readiness records")
				}
				for i := range record.Targets {
					record.Targets[i].ProbeIDs = nil
				}
				out.targets[record.Hostname] = record
			}
			for _, probe := range payload.Plan.Probes {
				if out.paths[probe.Hostname] == nil {
					out.paths[probe.Hostname] = map[string]bool{}
				}
				out.paths[probe.Hostname][probe.Path] = true
			}
		}
		return out, nil
	}
	old, err := read(before)
	if err != nil {
		return false, err
	}
	current, err := read(after)
	if err != nil {
		return false, err
	}
	changed := false
	for key := range old.owners {
		previousPaths, nextPaths := old.paths[key.host], current.paths[key.host]
		// A complete route withdrawal is covered by its independent live
		// safety checks. It has no native root on one side to transition.
		if current.owners[key] && len(previousPaths) != 0 && len(nextPaths) != 0 && !maps.Equal(previousPaths, nextPaths) {
			// The global record transition cannot combine different signed
			// address authorities. Hold the path edit if a recapture also
			// adds, drops, or changes endpoints, TTL, or quorum.
			if !previousPaths["/"] || !nextPaths["/"] || !reflect.DeepEqual(old.records[key.host], current.records[key.host]) ||
				!reflect.DeepEqual(old.targets[key.host], current.targets[key.host]) {
				return false, errHostedDNSPathTopology
			}
			changed = true
		}
	}
	return changed, nil
}

func (s *Server) checkProjectHostWorkerTLS(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	tls := platformcontrol.SharedCustomHostTLSCapabilityV1
	if !s.projectHostAuthoritySupports(ctx, platformconfig.GlobalScopeKey, "",
		projectHostConsumerCapability{model.PlatformArtifactKindEdgeRouteBundle, tls},
		projectHostConsumerCapability{model.PlatformArtifactKindCaddyRouteConfig, tls}) {
		return errProjectHostWorkerTLS
	}
	return nil
}

type projectHostConsumerCapability struct {
	kind, capability string
}

func (s *Server) projectHostAuthoritySupports(ctx context.Context, scope, expectedRole string, requirements ...projectHostConsumerCapability) bool {
	parent, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, model.PlatformArtifactReleaseChannelFull)
	role, _ := parent.Content["publication_role"].(string)
	if err != nil || !found || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || role != expectedRole ||
		release.VerificationState != model.PlatformArtifactVerificationStateVerified {
		return false
	}
	for _, requirement := range requirements {
		if !s.projectHostPublishedConsumerCapability(ctx, parent, release, requirement) {
			return false
		}
	}
	// Recheck the fence after reading the selected consumers and transports.
	_, current, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, model.PlatformArtifactReleaseChannelFull)
	return ctx.Err() == nil && err == nil && found && current.ID == release.ID && current.FencingToken == release.FencingToken && current.VerificationState == release.VerificationState
}

func (s *Server) projectHostPublishedConsumerCapability(ctx context.Context, parent model.PlatformArtifact, release model.PlatformArtifactRelease, requirement projectHostConsumerCapability) bool {
	sets, err := s.store.ListPlatformExpectedConsumerSetsContext(ctx, model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: requirement.kind, ScopeKey: parent.ScopeKey})
	if err != nil {
		return false
	}
	var latest model.PlatformExpectedConsumerSet
	for _, set := range sets {
		if set.Revision == latest.Revision && set.ID != latest.ID {
			return false
		}
		if set.Revision > latest.Revision {
			latest = set
		}
	}
	if !latest.RequiresConsumers || platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, latest) != nil {
		return false
	}
	binding := s.platformConvergenceBinding(latest)
	consumers, err := s.store.ListPlatformConsumers(latest.ArtifactKind, parent.ScopeKey)
	if err != nil || binding == nil || binding.ArtifactReleaseID != release.ID {
		return false
	}
	status := s.evaluateLiveConsumerConvergence(ctx, latest, consumers, binding)
	return projectHostConsumersReady(status, requirement.capability)
}

func projectHostConsumersReady(status model.PlatformConsumerConvergenceStatus, capability string) bool {
	if !status.Pass || status.RequiredPassing == 0 || status.RequiredPassing != status.RequiredExpected || len(status.UnexpectedConsumers) != 0 {
		return false
	}
	count := 0
	for _, item := range status.Assessments {
		if !item.Required {
			continue
		}
		count++
		if item.State != model.InvariantEvidenceStatePass || item.Observed == nil ||
			!slices.Contains(item.Observed.CompatibilityCapabilities, capability) {
			return false
		}
	}
	return count == status.RequiredExpected
}
