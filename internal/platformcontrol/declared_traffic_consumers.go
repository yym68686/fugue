package platformcontrol

import (
	"fmt"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// DeclaredTrafficConsumerTopology is configuration membership only. The
// synthetic inventory rows carry no health, addresses or observed generations.
func DeclaredTrafficConsumerTopology(parent model.PlatformArtifact) (ExpectedConsumerTopology, bool, error) {
	declared, err := platformconfig.TrafficConsumersFromRelease(parent)
	if err != nil || declared == nil {
		return ExpectedConsumerTopology{}, false, err
	}
	out := ExpectedConsumerTopology{}
	for _, node := range declared.EdgeNodeIDs {
		out.EdgeNodes = append(out.EdgeNodes, model.EdgeNode{ID: node, EdgeGroupID: declared.AuthorityCellID})
	}
	for _, node := range declared.DNSNodeIDs {
		out.DNSNodes = append(out.DNSNodes, model.DNSNode{ID: node, PhysicalNodeID: node, EdgeGroupID: declared.AuthorityCellID})
	}
	return out, true, nil
}

// A prepared set is an immutable receipt expectation, not a second source of
// membership policy. This check is shared by API reads and store publication.
func ValidateDeclaredTrafficConsumerSet(parent model.PlatformArtifact, set model.PlatformExpectedConsumerSet) error {
	topology, declared, err := DeclaredTrafficConsumerTopology(parent)
	if err != nil {
		return err
	}
	if !declared {
		return nil
	}
	if set.ReleaseSetID != parent.ID || set.ScopeKey != parent.ScopeKey {
		return fmt.Errorf("consumer set belongs to another declared release")
	}
	switch set.ArtifactKind {
	case model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig:
	default:
		return fmt.Errorf("undeclared traffic consumer component")
	}
	want, err := BuildExpectedConsumerSet(ExpectedConsumerSetBuildRequest{ReleaseSetID: set.ReleaseSetID, ArtifactReleaseID: set.ArtifactReleaseID, ArtifactKind: set.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, Generation: set.ExpectedGeneration, Revision: set.Revision, PreparedAt: set.CreatedAt, Topology: topology})
	if err != nil {
		return err
	}
	// Database timestamps may have microsecond precision while member JSON
	// retains nanoseconds. Absolute preparation deadlines are not membership;
	// original receipt freshness is still checked independently at assessment.
	actual := append([]model.PlatformExpectedConsumer(nil), set.Consumers...)
	for i := range actual {
		actual[i].HeartbeatDeadline, actual[i].ConvergenceDeadline = time.Time{}, time.Time{}
	}
	for i := range want.Consumers {
		want.Consumers[i].HeartbeatDeadline, want.Consumers[i].ConvergenceDeadline = time.Time{}, time.Time{}
	}
	if set.ID != want.ID || set.TopologyRevision != want.TopologyRevision || set.RequiresConsumers != want.RequiresConsumers || set.RequiredCardinality != want.RequiredCardinality || set.OptionalCardinality != want.OptionalCardinality || !reflect.DeepEqual(actual, want.Consumers) {
		return fmt.Errorf("prepared consumer set differs from signed execution membership")
	}
	return nil
}
