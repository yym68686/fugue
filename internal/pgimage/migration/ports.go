// Package migration owns the execution and settlement policy of the managed
// PostgreSQL image migration. Its ports are scoped to a single operation.
package migration

import (
	"context"
	"errors"

	"fugue/internal/model"
	"fugue/internal/pgimage"
)

var ErrUnsettled = errors.New("database image migration awaits verified settlement")

// Ledger is already bound to the active operation and its Store transaction
// guards. Complete is the only port that can change the persisted image.
type Ledger interface {
	ReadIntent() (intent *model.OperationEvidence, abortRecorded bool, err error)
	WriteWitness(kind string, witness pgimage.Witness) error
	CheckOwnership() error
	IsInactive(error) bool
	Progress(message string)
	Complete() error
}

type Target struct {
	OperationID string
	Namespace   string
	Cluster     string
	Image       string
	Budget      int
	Resources   *model.ResourceSpec
}

// Observation exposes the exact observed Cluster for Kubernetes compare-and-
// swap, with bounded checks of the corresponding Pod and its volume identity.
type Observation struct {
	Cluster           map[string]any
	CurrentImage      string
	PodUID            string
	PodImage          string
	Verify            func(pgimage.Witness) error
	VerifyVolumes     func(context.Context, pgimage.Witness) error
	VerifyReplacement func(string) error
	Probe             func(context.Context) error
}

type DrainState struct {
	Cluster  map[string]any
	OnSource bool
	Staged   bool
}

type EndpointTiming interface {
	MarkCutover()
	StopAndRecord()
}

// Platform controls the operation's one authorized cluster and primary. It
// retains the concrete SQL and Kubernetes adapters, including UID/version
// compare-and-swap and drain restoration guards.
type Platform interface {
	Capture(context.Context) (pgimage.Witness, error)
	Observe(context.Context) (Observation, error)
	RecoverBootstrap(context.Context, pgimage.Witness) (bool, error)
	ConfigureDrain(*pgimage.Witness, map[string]any) error
	ObserveDrain(context.Context, pgimage.Witness) (DrainState, error)
	DrainStaged(pgimage.Witness, map[string]any) (bool, error)
	ReplacementExists(context.Context, pgimage.Witness) bool
	SourceStillPresent(context.Context, pgimage.Witness) bool
	SampleDrain(context.Context, pgimage.Witness) pgimage.DrainObservation
	RecordDrain(pgimage.DrainObservation)
	StageDrain(context.Context, pgimage.Witness, map[string]any) error
	AbortDrain(context.Context, pgimage.Witness, map[string]any, bool, string) error
	RestoreDrain(context.Context, pgimage.Witness, map[string]any) error
	BeginEndpointTiming(context.Context, pgimage.Witness, bool) EndpointTiming
	PatchImage(context.Context, pgimage.Witness, map[string]any) error
}

type Prepare func(context.Context) (Target, Platform, error)
