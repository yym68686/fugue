// Package contract contains the query data shared by operation projections and
// persistence. The consumer's read interface lives in the projection package.
package contract

type Filter struct {
	TenantID  string
	AppID     string
	ProjectID string
	ServiceID string
	ActorID   string
	Types     []string
	Statuses  []string
	Limit     int
}
