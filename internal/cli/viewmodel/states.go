package viewmodel

import "fmt"

func EmptyAppHealth(message string) AppHealthView {
	return AppHealthView{State: EmptyState(message)}
}

func ErrorAppHealth(err error) AppHealthView {
	return AppHealthView{State: ErrorState(err)}
}

func PermissionAppHealth(message string) AppHealthView {
	return AppHealthView{State: PermissionState(message)}
}

func EmptyProjectWorkbench(message string) ProjectWorkbenchView {
	return ProjectWorkbenchView{State: EmptyState(message)}
}

func ErrorProjectWorkbench(err error) ProjectWorkbenchView {
	return ProjectWorkbenchView{State: ErrorState(err)}
}

func PermissionProjectWorkbench(message string) ProjectWorkbenchView {
	return ProjectWorkbenchView{State: PermissionState(message)}
}

func EmptyRoutePath(message string) RoutePathView {
	return RoutePathView{State: EmptyState(message), Tone: ToneMuted}
}

func ErrorRoutePath(err error) RoutePathView {
	return RoutePathView{State: ErrorState(err), Tone: ToneDanger}
}

func PermissionRoutePath(message string) RoutePathView {
	return RoutePathView{State: PermissionState(message), Tone: ToneWarning}
}

func EmptyOperationTimeline(message string) OperationTimelineView {
	return OperationTimelineView{State: EmptyState(message)}
}

func ErrorOperationTimeline(err error) OperationTimelineView {
	return OperationTimelineView{State: ErrorState(err)}
}

func PermissionOperationTimeline(message string) OperationTimelineView {
	return OperationTimelineView{State: PermissionState(message)}
}

func EmptyRuntimeCapacity(message string) RuntimeCapacityView {
	return RuntimeCapacityView{State: EmptyState(message), Tone: ToneMuted}
}

func ErrorRuntimeCapacity(err error) RuntimeCapacityView {
	return RuntimeCapacityView{State: ErrorState(err), Tone: ToneDanger}
}

func PermissionRuntimeCapacity(message string) RuntimeCapacityView {
	return RuntimeCapacityView{State: PermissionState(message), Tone: ToneWarning}
}

func EmptyDiagnosisEvidence(message string) DiagnosisEvidenceView {
	return DiagnosisEvidenceView{State: EmptyState(message), Tone: ToneMuted}
}

func ErrorDiagnosisEvidence(err error) DiagnosisEvidenceView {
	return DiagnosisEvidenceView{State: ErrorState(err), Tone: ToneDanger}
}

func PermissionDiagnosisEvidence(message string) DiagnosisEvidenceView {
	return DiagnosisEvidenceView{State: PermissionState(message), Tone: ToneWarning}
}

func EmptyActionPlan(message string) ActionPlanView {
	return ActionPlanView{State: EmptyState(message)}
}

func ErrorActionPlan(err error) ActionPlanView {
	return ActionPlanView{State: ErrorState(err)}
}

func PermissionActionPlan(message string) ActionPlanView {
	return ActionPlanView{State: PermissionState(message)}
}

func PermissionDeniedError(scope string) error {
	return fmt.Errorf("%w: %s", ErrPermissionDenied, scope)
}
