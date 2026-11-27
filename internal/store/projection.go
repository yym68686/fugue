package store

import (
	"time"

	"fugue/internal/model"
)

// storeProjection holds rows loaded from PostgreSQL for shared validation and
// calculations. It is not a persisted store or a public wire representation.
type storeProjection struct {
	AppRecoveries             []model.AppRecoveryRecord
	MailPlatformConfig        *model.MailPlatformConfig
	AppMailBindings           []model.AppMailBinding
	ImageOrphanPolicy         *model.ImageOrphanPolicy
	Version                   string
	Tenants                   []model.Tenant
	Projects                  []model.Project
	ProjectDeleteRequests     map[string]time.Time
	StaticEdgeRegistrations   []model.StaticEdgeRegistration
	APIKeys                   []model.APIKey
	SSHKeys                   []model.SSHKey
	EnrollmentTokens          []model.EnrollmentToken
	NodeKeys                  []model.NodeKey
	Machines                  []model.Machine
	NodeUpdaters              []model.NodeUpdater
	ImageLocations            []model.ImageLocation
	Images                    []model.Image
	AppImageTrackings         []model.AppImageTracking
	AppImageTrackingChecks    []model.AppImageTrackingCheck
	AppReleases               []model.AppRelease
	ReleaseAttempts           []model.ReleaseAttempt
	AppTrafficPolicies        []model.AppTrafficPolicy
	AutomationPolicies        []model.AutomationPolicy
	AutomationActionFencing   []model.AutomationActionFencing
	Runtimes                  []model.Runtime
	Apps                      []model.App
	EdgeGroups                []model.EdgeGroup
	EdgeNodes                 []model.EdgeNode
	EdgeNodeInstances         []model.EdgeNodeInstance
	EdgeActiveEpochs          []model.EdgeActiveEpoch
	EdgeInstanceFencingSchema string
	EdgeActivation            *model.EdgeActivationState
	DNSNodes                  []model.DNSNode
	HostedZones               []model.HostedZone
	DNSRecords                []model.DNSRecord
	EdgeRoutePolicies         []model.EdgeRoutePolicy
	PlatformArtifacts         []model.PlatformArtifact
	PlatformQualityEvidence   []model.PlatformQualityEvidence
	PlatformArtifactReleases  []model.PlatformArtifactRelease
	PlatformReleaseLanes      []model.PlatformReleaseLane
	PlatformReleaseMessages   []model.PlatformReleaseMessage
	PlatformShadowConsumers   []model.PlatformConsumerInstance
	PlatformConsumerInstances []model.PlatformConsumerInstance
	ExpectedConsumerSets      []model.ExpectedConsumerSet
	PlatformLKGSnapshots      []model.PlatformLKGSnapshot
	BackingServices           []model.BackingService
	ServiceBindings           []model.ServiceBinding
	Operations                []model.Operation
	OperationEvidence         []model.OperationEvidence
	AuditEvents               []model.AuditEvent
	Idempotency               []model.IdempotencyRecord
	TenantBilling             []model.TenantBilling
	BillingEvents             []model.TenantBillingEvent
}
