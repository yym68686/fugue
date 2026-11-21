package main

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	"k8s.io/apimachinery/pkg/util/validation"
)

type frontRepairNode struct {
	Name     string
	UID      string
	Admitted bool
}

type frontRepairMember struct {
	Node                  frontRepairNode
	FrontPodUID           string
	TargetWorkerPodUID    string
	Activation            edgegroupfront.ActivationState
	TargetWorkerSlot      releaseguardian.AuthoritySlot
	TargetWorkerSourceSHA string
	TargetWorkerImage     string
}

// The runtime adapter must read these facts from the same physical node and
// pin the Pod UIDs before using them. These are observations, not nominations
// of arbitrary Pods by the declarative repair intent.
type frontRepairWitness struct {
	frontRepairMember
	LiveFront          observedFront
	TargetWorkerHealth authorityWorkerHealth
}

type frontRepairPlan struct {
	CurrentUID             string
	CurrentResourceVersion string
	Before                 releaseguardian.CurrentAuthority
	TargetGeneration       uint64
	Members                []frontRepairMember
	Digest                 string
}

// planPartialFrontRollbackRepair admits only the exact one-step partial
// rollback of a committed group. It is read-only, and does not authorize a
// write. An executor must separately verify the declared roster, hold both
// locks, re-read witnesses and persist a journal before applying this plan.
func planPartialFrontRollbackRepair(before releaseguardian.CurrentAuthority, currentUID, currentRV string,
	expected []frontRepairNode, observed []frontRepairWitness) (frontRepairPlan, error) {
	members := make([]frontRepairMember, 0, len(observed))
	for _, witness := range observed {
		if !frontRepairLiveMatches(witness.LiveFront, witness.Activation) {
			return frontRepairPlan{}, fmt.Errorf("node %s live Front does not match its durable activation", witness.Node.Name)
		}
		members = append(members, witness.frontRepairMember)
	}
	return bindFrontRepairPredecessors(before, currentUID, currentRV, expected, members)
}

func bindFrontRepairPredecessors(before releaseguardian.CurrentAuthority, currentUID, currentRV string,
	expected []frontRepairNode, observed []frontRepairMember) (frontRepairPlan, error) {
	if before.Validate() != nil || before.CurrentFrontGeneration <= before.PreviousFrontGeneration ||
		before.PreviousFrontGeneration == 0 || before.CurrentFrontGeneration > math.MaxUint64-3 ||
		before.AuthorityEpoch == math.MaxInt64 || !frontRepairIdentity(currentUID) || !frontRepairIdentity(currentRV) {
		return frontRepairPlan{}, errors.New("partial Front repair requires an exact committed authority and predecessor")
	}
	if len(expected) < 2 || len(expected) > 100 || len(observed) != len(expected) {
		return frontRepairPlan{}, errors.New("partial Front repair cohort is incomplete")
	}
	want := make(map[string]frontRepairNode, len(expected))
	uids := map[string]bool{currentUID: true}
	for _, node := range expected {
		if len(validation.IsDNS1123Subdomain(node.Name)) != 0 || !frontRepairIdentity(node.UID) ||
			uids[node.UID] || want[node.Name].Name != "" {
			return frontRepairPlan{}, errors.New("partial Front repair cohort has a duplicate or invalid node identity")
		}
		want[node.Name], uids[node.UID] = node, true
	}
	var kept, rolledBack int
	members := make([]frontRepairMember, 0, len(observed))
	for _, member := range observed {
		identity, exists := want[member.Node.Name]
		if !exists || identity != member.Node || !frontRepairIdentity(member.FrontPodUID) || !frontRepairIdentity(member.TargetWorkerPodUID) ||
			member.FrontPodUID == member.TargetWorkerPodUID || uids[member.FrontPodUID] || uids[member.TargetWorkerPodUID] {
			return frontRepairPlan{}, errors.New("partial Front repair observed a changed or duplicate Node/Pod identity")
		}
		delete(want, member.Node.Name)
		uids[member.FrontPodUID], uids[member.TargetWorkerPodUID] = true, true
		if member.TargetWorkerSlot != before.CurrentWorkerSlot || member.TargetWorkerSourceSHA != before.CurrentWorkerSourceSHA || member.TargetWorkerImage != before.CurrentWorkerImageDigest {
			return frontRepairPlan{}, fmt.Errorf("node %s lacks the exact committed Worker identity", member.Node.Name)
		}
		state := member.Activation
		if state.GroupID != before.GroupID || state.Schema != edgegroupfront.ActivationStateSchemaV1 ||
			state.Authority != edgegroupfront.ActivationAuthority || state.UpdatedAt.IsZero() {
			return frontRepairPlan{}, fmt.Errorf("node %s durable activation is invalid", member.Node.Name)
		}
		switch {
		case frontRepairMatchesCommitted(before, member):
			kept++
		case frontRepairMatchesRollback(before, state):
			rolledBack++
		default:
			return frontRepairPlan{}, fmt.Errorf("node %s is neither committed nor an exact one-step rollback", member.Node.Name)
		}
		members = append(members, member)
	}
	if kept == 0 || rolledBack == 0 {
		return frontRepairPlan{}, errors.New("partial Front repair requires both committed and rolled-back members")
	}
	plan := frontRepairPlan{
		CurrentUID: currentUID, CurrentResourceVersion: currentRV, Before: before,
		TargetGeneration: before.CurrentFrontGeneration + 2, Members: members,
	}
	sort.Slice(plan.Members, func(i, j int) bool { return plan.Members[i].Node.Name < plan.Members[j].Node.Name })
	result, err := frontRepairPlanDigest(plan)
	if err != nil {
		return frontRepairPlan{}, err
	}
	plan.Digest = result
	return plan, nil
}

func frontRepairPlanDigest(plan frontRepairPlan) (string, error) {
	plan.Digest = ""
	raw, err := declarativerelease.CanonicalJSON(plan)
	if err != nil {
		return "", err
	}
	return shaDigest(raw), nil
}

type frontRepairPreparation struct {
	request edgegroupfront.ActivationCASRequest
}

// frontRepairPublicationRequests describes expiring, non-serving preparation
// only on the diverged members. Use with PreparePublication, never ApplyActivationCAS.
func frontRepairPublicationRequests(plan frontRepairPlan) (map[string]frontRepairPreparation, error) {
	digest, err := frontRepairPlanDigest(plan)
	if err != nil || digest != plan.Digest {
		return nil, errors.New("partial Front repair plan digest is invalid")
	}
	before := plan.Before
	requests := make(map[string]frontRepairPreparation)
	for _, member := range plan.Members {
		state := member.Activation
		if state.ActiveSlot == string(before.CurrentWorkerSlot) {
			continue
		}
		requests[member.Node.Name] = frontRepairPreparation{request: edgegroupfront.ActivationCASRequest{
			GroupID: before.GroupID, ExpectedGeneration: state.Generation, ExpectedSlot: state.ActiveSlot,
			TargetSlot: string(before.CurrentWorkerSlot), WorkerSourceCommit: before.CurrentWorkerSourceSHA,
			WorkerImageDigest: before.CurrentWorkerImageDigest, BundleGeneration: before.CurrentBundleGeneration,
			Operation: edgegroupfront.ActivationOperationPromote, RequirePublicationPreparation: true,
			Reason: "prepare exact committed Worker for group authority repair",
		}}
	}
	return requests, nil
}

// Verification after non-serving publication preparation is separate from
// sealing the predecessor. An inactive Worker may be holding an old LKG at
// planning time; it must freshly load and apply the current publication
// before any Front traffic move. A refresh cannot authorize another Pod.
func verifyFrontRepairPrepared(plan frontRepairPlan, current releaseguardian.CurrentAuthority, currentUID, currentRV string,
	expected []frontRepairNode, refreshed []frontRepairWitness, preparations map[string]edgegroupfront.PublicationPreparation, now time.Time) error {
	digest, err := frontRepairPlanDigest(plan)
	if err != nil || digest != plan.Digest || now.IsZero() {
		return errors.New("partial Front repair plan digest is invalid")
	}
	observed, err := planPartialFrontRollbackRepair(current, currentUID, currentRV, expected, refreshed)
	if err != nil || observed.Digest != plan.Digest {
		return errors.Join(errors.New("partial Front repair predecessor changed during publication preparation"), err)
	}
	target := releaseguardian.FrontAuthorityTarget{GroupID: current.GroupID, FrontBundleGeneration: current.CurrentBundleGeneration}
	var appliedBundle string
	checked := 0
	for _, member := range refreshed {
		if member.Activation.ActiveSlot != string(current.CurrentWorkerSlot) {
			p, exists := preparations[member.Node.Name]
			if !exists || !frontRepairPreparationReady(p, member.Activation, current, now) {
				return fmt.Errorf("node %s lacks fresh publication preparation for its exact predecessor", member.Node.Name)
			}
			checked++
		}
		if !preparedWorkerPublicationReady(member.TargetWorkerHealth, target) {
			return fmt.Errorf("node %s has not applied a healthy current publication", member.Node.Name)
		}
		if appliedBundle == "" {
			appliedBundle = member.TargetWorkerHealth.BundleVersion
		} else if appliedBundle != member.TargetWorkerHealth.BundleVersion {
			return errors.New("partial Front repair target Workers disagree on the applied publication")
		}
	}
	if checked != len(preparations) {
		return errors.New("partial Front repair has unexpected publication preparations")
	}
	return nil
}

func frontRepairPreparationReady(p edgegroupfront.PublicationPreparation, predecessor edgegroupfront.ActivationState, target releaseguardian.CurrentAuthority, now time.Time) bool {
	return p.Matches(predecessor, string(target.CurrentWorkerSlot), target.CurrentWorkerSourceSHA, now) &&
		p.WorkerImageDigest == target.CurrentWorkerImageDigest && p.BundleGeneration == target.CurrentBundleGeneration &&
		now.Add(5*time.Second).Before(p.ExpiresAt)
}

func frontRepairMatchesCommitted(before releaseguardian.CurrentAuthority, member frontRepairMember) bool {
	state := member.Activation
	if state.Generation != before.CurrentFrontGeneration || state.ActiveSlot != string(before.CurrentWorkerSlot) ||
		state.WorkerSourceCommit != before.CurrentWorkerSourceSHA || state.WorkerImageDigest != before.CurrentWorkerImageDigest ||
		state.BundleGeneration != before.CurrentBundleGeneration {
		return false
	}
	switch state.Operation {
	case edgegroupfront.ActivationOperationInit:
		return member.Node.Admitted && state.PreviousSlot == "" && state.RollbackOfGeneration == 0
	case edgegroupfront.ActivationOperationPromote:
		return state.PreviousSlot == string(before.PreviousWorkerSlot) && state.RollbackOfGeneration == 0
	case edgegroupfront.ActivationOperationRollback:
		return state.PreviousSlot == string(before.PreviousWorkerSlot) && state.RollbackOfGeneration == state.Generation-1
	default:
		return false
	}
}

func frontRepairMatchesRollback(before releaseguardian.CurrentAuthority, state edgegroupfront.ActivationState) bool {
	// Code and route publication advance independently. Legacy rollback could
	// select the predecessor binary after it applied the currently committed
	// publication. Anything other than these two exact versions is unproven.
	return state.Operation == edgegroupfront.ActivationOperationRollback &&
		state.Generation == before.CurrentFrontGeneration+1 && state.RollbackOfGeneration == before.CurrentFrontGeneration &&
		state.PreviousSlot == string(before.CurrentWorkerSlot) && state.ActiveSlot == string(before.PreviousWorkerSlot) &&
		state.WorkerSourceCommit == before.PreviousWorkerSourceSHA && state.WorkerImageDigest == before.PreviousWorkerImageDigest &&
		(state.BundleGeneration == before.PreviousBundleGeneration || state.BundleGeneration == before.CurrentBundleGeneration)
}

func frontRepairIdentity(value string) bool {
	return value != "" && strings.TrimSpace(value) == value
}

func frontRepairLiveMatches(live observedFront, state edgegroupfront.ActivationState) bool {
	return state.Schema == edgegroupfront.ActivationStateSchemaV1 && state.Authority == edgegroupfront.ActivationAuthority &&
		!state.UpdatedAt.IsZero() && live.Status == "ok" && live.Generation == state.Generation &&
		live.ActiveSlot == state.ActiveSlot && live.RouteAuthority == state.Authority && live.BundleGeneration == state.BundleGeneration &&
		live.WorkerSourceCommit == state.WorkerSourceCommit && live.WorkerImageDigest == state.WorkerImageDigest
}
