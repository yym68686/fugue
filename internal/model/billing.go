package model

import "time"

const (
	BillingStatusInactive   = "inactive"
	BillingStatusActive     = "active"
	BillingStatusRestricted = "restricted"
	BillingStatusOverCap    = "over-cap"

	BillingEventTypeTopUp               = "top-up"
	BillingEventTypeConfigUpdated       = "config-updated"
	BillingEventTypeBalanceAdjusted     = "balance-adjusted"
	BillingEventTypePublicRuntimeDebit  = "public-runtime-debit"
	BillingEventTypePublicRuntimeCredit = "public-runtime-credit"

	DefaultBillingCurrency            = "USD"
	DefaultBillingHoursPerMonth int64 = 730
	// Calibrated so a managed envelope of 2 vCPU / 4 GiB / 30 GiB displays as roughly $4.00/month.
	DefaultBillingCPUMicroCentsPerMilliCoreHour int64 = 154
	DefaultBillingMemoryMicroCentsPerMiBHour    int64 = 53
	DefaultBillingStorageMicroCentsPerGiBHour   int64 = 760

	DefaultTenantFreeManagedCPUMilliCores    int64 = 500
	DefaultTenantFreeManagedMemoryMebibytes  int64 = 512
	DefaultTenantFreeManagedStorageGibibytes int64 = 5

	DefaultManagedAppCPUMilliCores         int64 = 250
	DefaultManagedAppMemoryMebibytes       int64 = 512
	DefaultManagedAppStorageGibibytes      int64 = 0
	DefaultManagedPostgresCPUMilliCores    int64 = 250
	DefaultManagedPostgresMemoryMebibytes  int64 = 512
	DefaultManagedPostgresStorageGibibytes int64 = 1

	DefaultManagedWorkspaceStorageSize = "10Gi"
	DefaultManagedPostgresStorageSize  = "1Gi"
)

type ResourceSpec struct {
	CPUMilliCores   int64 `json:"cpu_millicores,omitempty"`
	MemoryMebibytes int64 `json:"memory_mebibytes,omitempty"`
}

type BillingResourceSpec struct {
	CPUMilliCores    int64 `json:"cpu_millicores,omitempty"`
	MemoryMebibytes  int64 `json:"memory_mebibytes,omitempty"`
	StorageGibibytes int64 `json:"storage_gibibytes,omitempty"`
}

type BillingPriceBook struct {
	Currency                      string `json:"currency"`
	HoursPerMonth                 int64  `json:"hours_per_month"`
	CPUMicroCentsPerMilliCoreHour int64  `json:"cpu_microcents_per_millicore_hour"`
	MemoryMicroCentsPerMiBHour    int64  `json:"memory_microcents_per_mib_hour"`
	StorageMicroCentsPerGiBHour   int64  `json:"storage_microcents_per_gib_hour"`
}

type TenantBilling struct {
	TenantID                     string              `json:"tenant_id"`
	ManagedCap                   BillingResourceSpec `json:"managed_cap"`
	ManagedImageStorageGibibytes int64               `json:"managed_image_storage_gibibytes,omitempty"`
	BalanceMicroCents            int64               `json:"balance_microcents"`
	PriceBook                    BillingPriceBook    `json:"price_book"`
	LastAccruedAt                time.Time           `json:"last_accrued_at"`
	CreatedAt                    time.Time           `json:"created_at"`
	UpdatedAt                    time.Time           `json:"updated_at"`
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
	ManagedCap                BillingResourceSpec  `json:"managed_cap"`
	ManagedCommitted          BillingResourceSpec  `json:"managed_committed"`
	ManagedAvailable          BillingResourceSpec  `json:"managed_available"`
	CurrentUsage              *ResourceUsage       `json:"current_usage,omitempty"`
	DefaultAppResources       BillingResourceSpec  `json:"default_app_resources"`
	DefaultPostgresResources  BillingResourceSpec  `json:"default_postgres_resources"`
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

func DefaultManagedAppBillingResources() BillingResourceSpec {
	spec := BillingResourceSpecFromResourceSpec(DefaultManagedAppResources())
	spec.StorageGibibytes = DefaultManagedAppStorageGibibytes
	return spec
}

func DefaultManagedPostgresResources() ResourceSpec {
	return ResourceSpec{
		CPUMilliCores:   DefaultManagedPostgresCPUMilliCores,
		MemoryMebibytes: DefaultManagedPostgresMemoryMebibytes,
	}
}

func DefaultManagedPostgresBillingResources() BillingResourceSpec {
	spec := BillingResourceSpecFromResourceSpec(DefaultManagedPostgresResources())
	spec.StorageGibibytes = DefaultManagedPostgresStorageGibibytes
	return spec
}

func DefaultTenantFreeManagedCap() BillingResourceSpec {
	return BillingResourceSpec{
		CPUMilliCores:    DefaultTenantFreeManagedCPUMilliCores,
		MemoryMebibytes:  DefaultTenantFreeManagedMemoryMebibytes,
		StorageGibibytes: DefaultTenantFreeManagedStorageGibibytes,
	}
}

func BillingResourceSpecFromResourceSpec(spec ResourceSpec) BillingResourceSpec {
	return BillingResourceSpec{
		CPUMilliCores:   spec.CPUMilliCores,
		MemoryMebibytes: spec.MemoryMebibytes,
	}
}

func DefaultBillingPriceBook() BillingPriceBook {
	return BillingPriceBook{
		Currency:                      DefaultBillingCurrency,
		HoursPerMonth:                 DefaultBillingHoursPerMonth,
		CPUMicroCentsPerMilliCoreHour: DefaultBillingCPUMicroCentsPerMilliCoreHour,
		MemoryMicroCentsPerMiBHour:    DefaultBillingMemoryMicroCentsPerMiBHour,
		StorageMicroCentsPerGiBHour:   DefaultBillingStorageMicroCentsPerGiBHour,
	}
}
