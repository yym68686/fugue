// Package storagerecovery names the optional storage-recovery protocol shared
// by the API, controller, store, and CLI. Keeping this protocol separate avoids
// coupling unrelated serving components to host-maintenance capabilities.
package storagerecovery

const (
	OperationType  = "database-recover"
	ExpandPoolTask = "expand-lvm-localpv"
	// Only a node taking part in storage recovery needs this capability. Existing
	// nodes keep the normal model.NodeUpdaterCurrentVersion baseline until needed.
	NodeUpdaterVersion = "v39"
)
