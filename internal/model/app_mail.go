package model

import "time"

type MailPlatformConfig struct {
	MailflareAppID string             `json:"mailflare_app_id"`
	Enabled        bool               `json:"enabled"`
	DNSRecords     []AppMailDNSRecord `json:"dns_records,omitempty"`
}

// Public DNS only. Private relay/signing credentials stay in the managed
// provider's secret environment and never enter a DNS artifact.
type AppMailDNSRecord struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	Values []string `json:"values"`
	TTL    int      `json:"ttl"`
}

// Intent survives provider and application releases. A revoked row is a
// tombstone, retained after application deletion to fence stale enables.
type AppMailBinding struct {
	AppID      string    `json:"app_id"`
	TenantID   string    `json:"tenant_id"`
	ProjectID  string    `json:"project_id"`
	Hostname   string    `json:"hostname"`
	LocalPart  string    `json:"local_part"`
	Generation uint64    `json:"generation"`
	State      string    `json:"state"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
