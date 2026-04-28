package runtime

const (
	FugueLabelName               = "app.kubernetes.io/name"
	FugueLabelManagedBy          = "app.kubernetes.io/managed-by"
	FugueLabelManagedByValue     = "fugue"
	FugueLabelComponent          = "app.kubernetes.io/component"
	FugueLabelAppID              = "fugue.pro/app-id"
	FugueLabelTenantID           = "fugue.pro/tenant-id"
	FugueLabelProjectID          = "fugue.pro/project-id"
	FugueLabelManagedApp         = "fugue.pro/managed-app"
	FugueLabelBackingServiceID   = "fugue.pro/backing-service-id"
	FugueLabelBackingServiceType = "fugue.pro/backing-service-type"
	FugueLabelOwnerAppID         = "fugue.pro/owner-app-id"
	FugueLabelFenceEpoch         = "fugue.pro/fence-epoch"

	FugueAnnotationReleaseKey = "fugue.pro/release-key"
)
