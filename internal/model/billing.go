package model

import "time"

const (
	BillingStatusInactive   = "inactive"
	BillingStatusActive     = "active"
	BillingStatusRestricted = "restricted"
	BillingStatusOverCap    = "over-cap"

	BillingEventTypeTopUp         = "top-up"
	BillingEventTypeConfigUpdated = "config-updated"

	DefaultBillingCurrency            = "USD"
	DefaultBillingHoursPerMonth int64 = 730
	// Calibrated so a managed envelope of 2 vCPU / 4 GiB displays as roughly $4.00/month.
	DefaultBillingCPUMicroCentsPerMilliCoreHour int64 = 161
	DefaultBillingMemoryMicroCentsPerMiBHour    int64 = 55

	DefaultTenantFreeManagedCPUMilliCores   int64 = 500
	DefaultTenantFreeManagedMemoryMebibytes int64 = 512

	DefaultManagedAppCPUMilliCores        int64 = 250
	DefaultManagedAppMemoryMebibytes      int64 = 512
	DefaultManagedPostgresCPUMilliCores   int64 = 500
	DefaultManagedPostgresMemoryMebibytes int64 = 1024
)

type ResourceSpec struct {
	CPUMilliCores   int64 `json:"cpu_millicores,omitempty"`
	MemoryMebibytes int64 `json:"memory_mebibytes,omitempty"`
}

type BillingPriceBook struct {
	Currency                      string `json:"currency"`
	HoursPerMonth                 int64  `json:"hours_per_month"`
	CPUMicroCentsPerMilliCoreHour int64  `json:"cpu_microcents_per_millicore_hour"`
	MemoryMicroCentsPerMiBHour    int64  `json:"memory_microcents_per_mib_hour"`
}

type TenantBilling struct {
	TenantID          string           `json:"tenant_id"`
	ManagedCap        ResourceSpec     `json:"managed_cap"`
	BalanceMicroCents int64            `json:"balance_microcents"`
	PriceBook         BillingPriceBook `json:"price_book"`
	LastAccruedAt     time.Time        `json:"last_accrued_at"`
	CreatedAt         time.Time        `json:"created_at"`
	UpdatedAt         time.Time        `json:"updated_at"`
}

type TenantBillingEvent struct {
	ID                     string            `json:"id"`
	TenantID               string            `json:"tenant_id"`
	Type                   string            `json:"type"`
	AmountMicroCents       int64             `json:"amount_microcents"`
	BalanceAfterMicroCents int64             `json:"balance_after_microcents"`
	Metadata               map[string]string `json:"metadata,omitempty"`
	CreatedAt              time.Time         `json:"created_at"`
}

type TenantBillingSummary struct {
	TenantID                  string               `json:"tenant_id"`
	Status                    string               `json:"status"`
	StatusReason              string               `json:"status_reason,omitempty"`
	BYOVPSFree                bool                 `json:"byo_vps_free"`
	OverCap                   bool                 `json:"over_cap"`
	BalanceRestricted         bool                 `json:"balance_restricted"`
	ManagedCap                ResourceSpec         `json:"managed_cap"`
	ManagedCommitted          ResourceSpec         `json:"managed_committed"`
	ManagedAvailable          ResourceSpec         `json:"managed_available"`
	CurrentUsage              *ResourceUsage       `json:"current_usage,omitempty"`
	DefaultAppResources       ResourceSpec         `json:"default_app_resources"`
	DefaultPostgresResources  ResourceSpec         `json:"default_postgres_resources"`
	PriceBook                 BillingPriceBook     `json:"price_book"`
	HourlyRateMicroCents      int64                `json:"hourly_rate_microcents"`
	MonthlyEstimateMicroCents int64                `json:"monthly_estimate_microcents"`
	BalanceMicroCents         int64                `json:"balance_microcents"`
	RunwayHours               *float64             `json:"runway_hours,omitempty"`
	LastAccruedAt             time.Time            `json:"last_accrued_at"`
	UpdatedAt                 time.Time            `json:"updated_at"`
	Events                    []TenantBillingEvent `json:"events"`
}

func DefaultManagedAppResources() ResourceSpec {
	return ResourceSpec{
		CPUMilliCores:   DefaultManagedAppCPUMilliCores,
		MemoryMebibytes: DefaultManagedAppMemoryMebibytes,
	}
}

func DefaultManagedPostgresResources() ResourceSpec {
	return ResourceSpec{
		CPUMilliCores:   DefaultManagedPostgresCPUMilliCores,
		MemoryMebibytes: DefaultManagedPostgresMemoryMebibytes,
	}
}

func DefaultTenantFreeManagedCap() ResourceSpec {
	return ResourceSpec{
		CPUMilliCores:   DefaultTenantFreeManagedCPUMilliCores,
		MemoryMebibytes: DefaultTenantFreeManagedMemoryMebibytes,
	}
}

func DefaultBillingPriceBook() BillingPriceBook {
	return BillingPriceBook{
		Currency:                      DefaultBillingCurrency,
		HoursPerMonth:                 DefaultBillingHoursPerMonth,
		CPUMicroCentsPerMilliCoreHour: DefaultBillingCPUMicroCentsPerMilliCoreHour,
		MemoryMicroCentsPerMiBHour:    DefaultBillingMemoryMicroCentsPerMiBHour,
	}
}
