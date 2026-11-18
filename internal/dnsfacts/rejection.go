package dnsfacts

// These internal HTTP diagnostics are emitted only for a rejected observation.
// Neither a header nor its presence is part of serving authorization.
const (
	ReplacementBaseHeader      = "X-Fugue-DNS-Replacement-Base"
	ReplacementSelectionHeader = "X-Fugue-DNS-Replacement-Selection"
	ReplacementRenewalHeader   = "X-Fugue-DNS-Replacement-Renewal"
)

const (
	ReplacementCheckpoint            = "checkpoint_restore"
	ReplacementRetainedSource        = "retained_source_refresh"
	ReplacementRetainedObservation   = "retained_observation_refresh"
	ReplacementRetainedFallback      = "retained_fallback_refresh"
	ReplacementRetainedBeforePending = "retained_before_candidate"
	ReplacementClearFallback         = "clear_fallback"
	ReplacementReadinessIncomplete   = "readiness_incomplete"
	ReplacementRecordTransition      = "record_transition"
	ReplacementCandidate             = "candidate_install"
	ReplacementRollback              = "candidate_rollback"
	ReplacementUnattributed          = "unattributed"
	ReplacementUnchanged             = "unchanged"
	ReplacementChanged               = "changed"
	ReplacementBaseToBase            = "base_to_base"
	ReplacementBaseToRenewal         = "base_to_renewal"
	ReplacementRenewalToBase         = "renewal_to_base"
	ReplacementRenewalToRenewal      = "renewal_to_renewal"
)
