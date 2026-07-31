package imagecacheevidence

import "strings"

const (
	GraphStatusComplete   = "complete"
	GraphStatusIncomplete = "incomplete"

	ReasonMissingChildManifest = "missing_child_manifest"
	ReasonMissingBlob          = "missing_blob"
)

func NormalizeGraphStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", GraphStatusComplete:
		return GraphStatusComplete
	case GraphStatusIncomplete:
		return GraphStatusIncomplete
	default:
		// Unknown values are not legacy omissions. Fail closed so an unrecognized
		// producer cannot make graph bytes count as complete evidence.
		return GraphStatusIncomplete
	}
}

func NormalizeGraphFailureReason(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case ReasonMissingChildManifest:
		return ReasonMissingChildManifest
	case ReasonMissingBlob:
		return ReasonMissingBlob
	default:
		return ""
	}
}

func GraphIsIncomplete(status string) bool {
	return NormalizeGraphStatus(status) == GraphStatusIncomplete
}
