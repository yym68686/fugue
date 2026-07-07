package store

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

var (
	ErrBillingCapExceeded     = errors.New("billing cap exceeded")
	ErrBillingBalanceDepleted = errors.New("billing balance depleted")

	billingQuantityPattern = regexp.MustCompile(`^([+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?)([a-zA-Z]{0,2})$`)
)

const (
	billingHistoryLimit       = 12
	microCentsPerCent   int64 = 1_000_000
	bytesPerGiB         int64 = 1 << 30
)

func (s *Store) GetTenantBillingSummary(tenantID string) (model.TenantBillingSummary, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return model.TenantBillingSummary{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetTenantBillingSummary(tenantID)
	}

	var summary model.TenantBillingSummary
	err := s.withLockedState(true, func(state *model.State) error {
		index := newBillingStateIndex(state)
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := accrueTenantBillingLedgerWithIndex(state, index, tenantID, now)
		if billing == nil {
			return ErrNotFound
		}
		summary = buildTenantBillingSummaryWithIndex(state, index, *billing)
		return nil
	})
	return summary, err
}

func (s *Store) UpdateTenantBilling(tenantID string, managedCap model.BillingResourceSpec) (model.TenantBillingSummary, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return model.TenantBillingSummary{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpdateTenantBilling(tenantID, managedCap)
	}

	normalizedCap, err := normalizeBillingCap(managedCap)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}

	var summary model.TenantBillingSummary
	err = s.withLockedState(true, func(state *model.State) error {
		index := newBillingStateIndex(state)
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := accrueTenantBillingLedgerWithIndex(state, index, tenantID, now)
		if billing == nil {
			return ErrNotFound
		}
		committed := tenantManagedCommittedResourcesForBillingWithIndex(state, index, *billing)
		if err := validateCommittedManagedCapacity(normalizedCap, committed); err != nil {
			return err
		}
		billing.ManagedCap = normalizedCap
		billing.UpdatedAt = now
		appendTenantBillingEvent(state, newTenantBillingConfigUpdatedEvent(
			tenantID,
			normalizedCap,
			billing.BalanceMicroCents,
			now,
			nil,
		))
		summary = buildTenantBillingSummaryWithIndex(state, index, *billing)
		return nil
	})
	return summary, err
}

func (s *Store) TopUpTenantBilling(tenantID string, amountCents int64, note string) (model.TenantBillingSummary, error) {
	tenantID = strings.TrimSpace(tenantID)
	note = strings.TrimSpace(note)
	if tenantID == "" || amountCents <= 0 {
		return model.TenantBillingSummary{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgTopUpTenantBilling(tenantID, amountCents, note)
	}

	var summary model.TenantBillingSummary
	err := s.withLockedState(true, func(state *model.State) error {
		index := newBillingStateIndex(state)
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := accrueTenantBillingLedgerWithIndex(state, index, tenantID, now)
		if billing == nil {
			return ErrNotFound
		}
		billing.BalanceMicroCents += amountCents * microCentsPerCent
		billing.UpdatedAt = now

		metadata := map[string]string{}
		if note != "" {
			metadata["note"] = note
		}
		appendTenantBillingEvent(state, model.TenantBillingEvent{
			ID:                     model.NewID("billingevt"),
			TenantID:               tenantID,
			Type:                   model.BillingEventTypeTopUp,
			AmountMicroCents:       amountCents * microCentsPerCent,
			BalanceAfterMicroCents: billing.BalanceMicroCents,
			Metadata:               metadata,
			CreatedAt:              now,
		})
		summary = buildTenantBillingSummaryWithIndex(state, index, *billing)
		return nil
	})
	return summary, err
}

func (s *Store) SetTenantBillingBalance(tenantID string, balanceCents int64, metadata map[string]string) (model.TenantBillingSummary, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || balanceCents < 0 {
		return model.TenantBillingSummary{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSetTenantBillingBalance(tenantID, balanceCents, metadata)
	}

	targetBalanceMicroCents := balanceCents * microCentsPerCent
	var summary model.TenantBillingSummary
	err := s.withLockedState(true, func(state *model.State) error {
		index := newBillingStateIndex(state)
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := accrueTenantBillingLedgerWithIndex(state, index, tenantID, now)
		if billing == nil {
			return ErrNotFound
		}
		previousBalanceMicroCents := billing.BalanceMicroCents
		if previousBalanceMicroCents != targetBalanceMicroCents {
			billing.BalanceMicroCents = targetBalanceMicroCents
			billing.UpdatedAt = now
			appendTenantBillingEvent(state, newTenantBillingBalanceAdjustedEvent(
				tenantID,
				targetBalanceMicroCents-previousBalanceMicroCents,
				billing.BalanceMicroCents,
				now,
				metadata,
			))
		}
		summary = buildTenantBillingSummaryWithIndex(state, index, *billing)
		return nil
	})
	return summary, err
}

func (s *Store) SyncTenantBillingImageStorage(tenantID string, storageGibibytes int64) (model.TenantBillingSummary, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || storageGibibytes < 0 {
		return model.TenantBillingSummary{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSyncTenantBillingImageStorage(tenantID, storageGibibytes)
	}

	var summary model.TenantBillingSummary
	err := s.withLockedState(true, func(state *model.State) error {
		index := newBillingStateIndex(state)
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := accrueTenantBillingLedgerWithIndex(state, index, tenantID, now)
		if billing == nil {
			return ErrNotFound
		}
		if billing.ManagedImageStorageGibibytes != storageGibibytes {
			billing.ManagedImageStorageGibibytes = storageGibibytes
			billing.UpdatedAt = now
		}
		summary = buildTenantBillingSummaryWithIndex(state, index, *billing)
		return nil
	})
	return summary, err
}

func normalizeAppSpecResources(spec *model.AppSpec) error {
	if spec == nil {
		return ErrInvalidInput
	}
	if spec.ImageMirrorLimit < 0 {
		return ErrInvalidInput
	}
	if strings.TrimSpace(spec.WorkloadClass) != "" && model.NormalizeWorkloadClass(spec.WorkloadClass) == "" {
		return ErrInvalidInput
	}
	if spec.RightSizing != nil && strings.TrimSpace(spec.RightSizing.Mode) != "" && model.NormalizeAppRightSizingMode(spec.RightSizing.Mode) == "" {
		return ErrInvalidInput
	}
	if spec.Continuity != nil {
		if err := model.ValidateAppZeroDowntimePolicy(spec.Continuity.ZeroDowntime); err != nil {
			return err
		}
	}
	model.ApplyAppSpecDefaults(spec)
	if model.NormalizeWorkloadClass(spec.WorkloadClass) == "" {
		return ErrInvalidInput
	}
	if spec.RightSizing != nil && model.NormalizeAppRightSizingMode(spec.RightSizing.Mode) == "" {
		return ErrInvalidInput
	}
	resources, err := normalizeOptionalWorkloadResources(spec.Resources)
	if err != nil {
		return err
	}
	spec.Resources = resources
	if spec.Postgres != nil {
		if err := normalizePostgresSpecResources(spec.Postgres); err != nil {
			return err
		}
	}
	return nil
}

func normalizeOptionalWorkloadResources(spec *model.ResourceSpec) (*model.ResourceSpec, error) {
	if spec == nil {
		return nil, nil
	}
	out := *spec
	if out.CPUMilliCores < 0 || out.MemoryMebibytes < 0 || out.CPULimitMilliCores < 0 || out.MemoryLimitMebibytes < 0 {
		return nil, ErrInvalidInput
	}
	if out.CPUMilliCores == 0 && out.MemoryMebibytes == 0 && out.CPULimitMilliCores == 0 && out.MemoryLimitMebibytes == 0 {
		return nil, nil
	}
	if out.CPULimitMilliCores > 0 && out.CPUMilliCores == 0 {
		return nil, ErrInvalidInput
	}
	if out.MemoryLimitMebibytes > 0 && out.MemoryMebibytes == 0 {
		return nil, ErrInvalidInput
	}
	if out.CPULimitMilliCores > 0 && out.CPUMilliCores > 0 && out.CPULimitMilliCores < out.CPUMilliCores {
		return nil, ErrInvalidInput
	}
	if out.MemoryLimitMebibytes > 0 && out.MemoryMebibytes > 0 && out.MemoryLimitMebibytes < out.MemoryMebibytes {
		return nil, ErrInvalidInput
	}
	return &out, nil
}

func normalizePostgresSpecResources(spec *model.AppPostgresSpec) error {
	if spec == nil {
		return nil
	}
	resources, err := normalizeWorkloadResources(spec.Resources, model.DefaultManagedPostgresResources())
	if err != nil {
		return err
	}
	if resources.MemoryLimitMebibytes == 0 {
		resources.MemoryLimitMebibytes = model.DefaultPostgresMemoryLimitMebibytes(resources.MemoryMebibytes)
	}
	spec.Resources = resources
	return nil
}

func normalizeWorkloadResources(spec *model.ResourceSpec, defaults model.ResourceSpec) (*model.ResourceSpec, error) {
	if spec == nil {
		value := defaults
		return &value, nil
	}
	out := *spec
	if out.CPUMilliCores < 0 || out.MemoryMebibytes < 0 || out.CPULimitMilliCores < 0 || out.MemoryLimitMebibytes < 0 {
		return nil, ErrInvalidInput
	}
	if out.CPUMilliCores == 0 {
		out.CPUMilliCores = defaults.CPUMilliCores
	}
	if out.MemoryMebibytes == 0 {
		out.MemoryMebibytes = defaults.MemoryMebibytes
	}
	if out.CPULimitMilliCores > 0 && out.CPULimitMilliCores < out.CPUMilliCores {
		return nil, ErrInvalidInput
	}
	if out.MemoryLimitMebibytes > 0 && out.MemoryLimitMebibytes < out.MemoryMebibytes {
		return nil, ErrInvalidInput
	}
	if out.CPUMilliCores <= 0 || out.MemoryMebibytes <= 0 {
		return nil, ErrInvalidInput
	}
	return &out, nil
}

func normalizeBillingCap(cap model.BillingResourceSpec) (model.BillingResourceSpec, error) {
	if cap.CPUMilliCores < 0 || cap.MemoryMebibytes < 0 || cap.StorageGibibytes < 0 {
		return model.BillingResourceSpec{}, ErrInvalidInput
	}
	return cap, nil
}

func ensureTenantBillingDefaults(state *model.State) {
	if state == nil {
		return
	}
	if state.TenantBilling == nil {
		state.TenantBilling = []model.TenantBilling{}
	}
	if state.BillingEvents == nil {
		state.BillingEvents = []model.TenantBillingEvent{}
	}
	now := time.Now().UTC()
	for _, tenant := range state.Tenants {
		ensureTenantBillingRecord(state, tenant.ID, now)
	}
}

func defaultTenantBilling(tenantID string, now time.Time) model.TenantBilling {
	record := model.TenantBilling{
		TenantID:      tenantID,
		ManagedCap:    model.DefaultTenantFreeManagedCap(),
		PriceBook:     model.DefaultBillingPriceBook(),
		LastAccruedAt: now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	normalizeTenantBillingRecord(&record, now)
	record.BalanceMicroCents = billingMonthlyEstimateMicroCents(record)
	return record
}

func ensureTenantBillingRecord(state *model.State, tenantID string, now time.Time) *model.TenantBilling {
	index := findTenantBillingRecord(state, tenantID)
	if index >= 0 {
		record := &state.TenantBilling[index]
		normalizeTenantBillingRecord(record, now)
		if shouldRecalibrateTenantBillingPriceBook(*record) {
			recalibrateTenantBillingPriceBook(record, now)
		}
		if shouldBackfillLegacyTenantBillingRecord(*record) && !tenantHasBillingEvents(state, tenantID) {
			backfillLegacyTenantBillingRecord(record, now)
		}
		return record
	}
	record := defaultTenantBilling(tenantID, now)
	state.TenantBilling = append(state.TenantBilling, record)
	return &state.TenantBilling[len(state.TenantBilling)-1]
}

func normalizeTenantBillingRecord(record *model.TenantBilling, now time.Time) {
	if record == nil {
		return
	}
	record.TenantID = strings.TrimSpace(record.TenantID)
	if record.ManagedImageStorageGibibytes < 0 {
		record.ManagedImageStorageGibibytes = 0
	}
	record.PriceBook = normalizeBillingPriceBook(record.PriceBook)
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.LastAccruedAt.IsZero() {
		record.LastAccruedAt = now
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = record.CreatedAt
	}
}

func shouldBackfillLegacyTenantBillingRecord(record model.TenantBilling) bool {
	return record.ManagedCap.CPUMilliCores == 0 &&
		record.ManagedCap.MemoryMebibytes == 0 &&
		record.ManagedCap.StorageGibibytes == 0 &&
		record.ManagedImageStorageGibibytes == 0 &&
		record.BalanceMicroCents == 0
}

func shouldRecalibrateTenantBillingPriceBook(record model.TenantBilling) bool {
	return normalizeBillingPriceBook(record.PriceBook) != model.DefaultBillingPriceBook()
}

func recalibrateTenantBillingPriceBook(record *model.TenantBilling, now time.Time) {
	if record == nil {
		return
	}
	accrueTenantBilling(record, now)
	record.PriceBook = model.DefaultBillingPriceBook()
	record.UpdatedAt = now
}

func backfillLegacyTenantBillingRecord(record *model.TenantBilling, now time.Time) {
	if record == nil {
		return
	}
	record.PriceBook = model.DefaultBillingPriceBook()
	record.ManagedCap = model.DefaultTenantFreeManagedCap()
	record.BalanceMicroCents = billingMonthlyEstimateMicroCents(*record)
	record.LastAccruedAt = now
	record.UpdatedAt = now
}

func normalizeBillingPriceBook(priceBook model.BillingPriceBook) model.BillingPriceBook {
	defaults := model.DefaultBillingPriceBook()
	if strings.TrimSpace(priceBook.Currency) == "" {
		priceBook.Currency = defaults.Currency
	}
	if priceBook.HoursPerMonth <= 0 {
		priceBook.HoursPerMonth = defaults.HoursPerMonth
	}
	if priceBook.CPUMicroCentsPerMilliCoreHour <= 0 {
		priceBook.CPUMicroCentsPerMilliCoreHour = defaults.CPUMicroCentsPerMilliCoreHour
	}
	if priceBook.MemoryMicroCentsPerMiBHour <= 0 {
		priceBook.MemoryMicroCentsPerMiBHour = defaults.MemoryMicroCentsPerMiBHour
	}
	if priceBook.StorageMicroCentsPerGiBHour <= 0 {
		priceBook.StorageMicroCentsPerGiBHour = defaults.StorageMicroCentsPerGiBHour
	}
	return priceBook
}

func cloneRuntimePublicOffer(offer *model.RuntimePublicOffer) *model.RuntimePublicOffer {
	if offer == nil {
		return nil
	}
	out := *offer
	return &out
}

func normalizeRuntimePublicOfferPriceBook(priceBook model.BillingPriceBook) model.BillingPriceBook {
	defaults := model.DefaultBillingPriceBook()
	if strings.TrimSpace(priceBook.Currency) == "" {
		priceBook.Currency = defaults.Currency
	}
	if priceBook.HoursPerMonth <= 0 {
		priceBook.HoursPerMonth = defaults.HoursPerMonth
	}
	if priceBook.CPUMicroCentsPerMilliCoreHour < 0 {
		priceBook.CPUMicroCentsPerMilliCoreHour = 0
	}
	if priceBook.MemoryMicroCentsPerMiBHour < 0 {
		priceBook.MemoryMicroCentsPerMiBHour = 0
	}
	if priceBook.StorageMicroCentsPerGiBHour < 0 {
		priceBook.StorageMicroCentsPerGiBHour = 0
	}
	return priceBook
}

func normalizeRuntimePublicOffer(offer model.RuntimePublicOffer) (model.RuntimePublicOffer, error) {
	if offer.ReferenceBundle.CPUMilliCores < 0 ||
		offer.ReferenceBundle.MemoryMebibytes < 0 ||
		offer.ReferenceBundle.StorageGibibytes < 0 ||
		offer.ReferenceMonthlyPriceMicroCents < 0 {
		return model.RuntimePublicOffer{}, ErrInvalidInput
	}

	offer.PriceBook = normalizeRuntimePublicOfferPriceBook(offer.PriceBook)
	offer.ReferenceBundle = model.BillingResourceSpec{
		CPUMilliCores:    maxInt64(0, offer.ReferenceBundle.CPUMilliCores),
		MemoryMebibytes:  maxInt64(0, offer.ReferenceBundle.MemoryMebibytes),
		StorageGibibytes: maxInt64(0, offer.ReferenceBundle.StorageGibibytes),
	}

	chargeCPU := !offer.Free && !offer.FreeCPU
	chargeMemory := !offer.Free && !offer.FreeMemory
	chargeStorage := !offer.Free && !offer.FreeStorage
	if chargeCPU && offer.ReferenceBundle.CPUMilliCores <= 0 {
		return model.RuntimePublicOffer{}, ErrInvalidInput
	}
	if chargeMemory && offer.ReferenceBundle.MemoryMebibytes <= 0 {
		return model.RuntimePublicOffer{}, ErrInvalidInput
	}
	if chargeStorage && offer.ReferenceBundle.StorageGibibytes <= 0 {
		return model.RuntimePublicOffer{}, ErrInvalidInput
	}

	offer.PriceBook = deriveRuntimePublicOfferPriceBook(offer)
	if offer.UpdatedAt.IsZero() {
		offer.UpdatedAt = time.Now().UTC()
	}
	return offer, nil
}

func deriveRuntimePublicOfferPriceBook(offer model.RuntimePublicOffer) model.BillingPriceBook {
	priceBook := normalizeRuntimePublicOfferPriceBook(offer.PriceBook)
	if offer.Free || offer.ReferenceMonthlyPriceMicroCents <= 0 {
		priceBook.CPUMicroCentsPerMilliCoreHour = 0
		priceBook.MemoryMicroCentsPerMiBHour = 0
		priceBook.StorageMicroCentsPerGiBHour = 0
		return priceBook
	}

	defaults := model.DefaultBillingPriceBook()
	cpuWeight := int64(0)
	if !offer.FreeCPU {
		cpuWeight = offer.ReferenceBundle.CPUMilliCores * defaults.CPUMicroCentsPerMilliCoreHour
	}
	memoryWeight := int64(0)
	if !offer.FreeMemory {
		memoryWeight = offer.ReferenceBundle.MemoryMebibytes * defaults.MemoryMicroCentsPerMiBHour
	}
	storageWeight := int64(0)
	if !offer.FreeStorage {
		storageWeight = offer.ReferenceBundle.StorageGibibytes * defaults.StorageMicroCentsPerGiBHour
	}
	totalWeight := cpuWeight + memoryWeight + storageWeight
	if totalWeight <= 0 || priceBook.HoursPerMonth <= 0 {
		priceBook.CPUMicroCentsPerMilliCoreHour = 0
		priceBook.MemoryMicroCentsPerMiBHour = 0
		priceBook.StorageMicroCentsPerGiBHour = 0
		return priceBook
	}

	scaleNumerator := offer.ReferenceMonthlyPriceMicroCents
	scaleDenominator := totalWeight * priceBook.HoursPerMonth
	priceBook.CPUMicroCentsPerMilliCoreHour = 0
	priceBook.MemoryMicroCentsPerMiBHour = 0
	priceBook.StorageMicroCentsPerGiBHour = 0
	if !offer.FreeCPU {
		priceBook.CPUMicroCentsPerMilliCoreHour = int64(math.Round(
			float64(defaults.CPUMicroCentsPerMilliCoreHour*scaleNumerator) / float64(scaleDenominator),
		))
	}
	if !offer.FreeMemory {
		priceBook.MemoryMicroCentsPerMiBHour = int64(math.Round(
			float64(defaults.MemoryMicroCentsPerMiBHour*scaleNumerator) / float64(scaleDenominator),
		))
	}
	if !offer.FreeStorage {
		priceBook.StorageMicroCentsPerGiBHour = int64(math.Round(
			float64(defaults.StorageMicroCentsPerGiBHour*scaleNumerator) / float64(scaleDenominator),
		))
	}
	return priceBook
}

type publicRuntimeChargeComponent struct {
	OwnerTenantID        string
	RuntimeID            string
	RuntimeName          string
	HourlyRateMicroCents int64
}

func findTenantBillingRecord(state *model.State, tenantID string) int {
	if state == nil {
		return -1
	}
	for index, billing := range state.TenantBilling {
		if billing.TenantID == tenantID {
			return index
		}
	}
	return -1
}

func appendTenantBillingEvent(state *model.State, event model.TenantBillingEvent) {
	if state == nil {
		return
	}
	if state.BillingEvents == nil {
		state.BillingEvents = []model.TenantBillingEvent{}
	}
	state.BillingEvents = append(state.BillingEvents, event)
}

func newTenantBillingConfigUpdatedEvent(
	tenantID string,
	managedCap model.BillingResourceSpec,
	balanceMicroCents int64,
	now time.Time,
	metadata map[string]string,
) model.TenantBillingEvent {
	eventMetadata := map[string]string{
		"cpu_millicores":    strconv.FormatInt(managedCap.CPUMilliCores, 10),
		"memory_mebibytes":  strconv.FormatInt(managedCap.MemoryMebibytes, 10),
		"storage_gibibytes": strconv.FormatInt(managedCap.StorageGibibytes, 10),
	}
	for key, value := range metadata {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		eventMetadata[key] = value
	}
	return model.TenantBillingEvent{
		ID:                     model.NewID("billingevt"),
		TenantID:               tenantID,
		Type:                   model.BillingEventTypeConfigUpdated,
		AmountMicroCents:       0,
		BalanceAfterMicroCents: balanceMicroCents,
		Metadata:               eventMetadata,
		CreatedAt:              now,
	}
}

func newTenantBillingBalanceAdjustedEvent(
	tenantID string,
	amountMicroCents int64,
	balanceAfterMicroCents int64,
	now time.Time,
	metadata map[string]string,
) model.TenantBillingEvent {
	eventMetadata := map[string]string{}
	for key, value := range metadata {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		eventMetadata[key] = value
	}
	return model.TenantBillingEvent{
		ID:                     model.NewID("billingevt"),
		TenantID:               tenantID,
		Type:                   model.BillingEventTypeBalanceAdjusted,
		AmountMicroCents:       amountMicroCents,
		BalanceAfterMicroCents: balanceAfterMicroCents,
		Metadata:               eventMetadata,
		CreatedAt:              now,
	}
}

func newTenantBillingPublicRuntimeEvent(
	tenantID string,
	eventType string,
	amountMicroCents int64,
	balanceAfterMicroCents int64,
	now time.Time,
	metadata map[string]string,
) model.TenantBillingEvent {
	eventMetadata := map[string]string{}
	for key, value := range metadata {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		eventMetadata[key] = value
	}
	return model.TenantBillingEvent{
		ID:                     model.NewID("billingevt"),
		TenantID:               tenantID,
		Type:                   eventType,
		AmountMicroCents:       amountMicroCents,
		BalanceAfterMicroCents: balanceAfterMicroCents,
		Metadata:               eventMetadata,
		CreatedAt:              now,
	}
}

func tenantHasBillingEvents(state *model.State, tenantID string) bool {
	if state == nil {
		return false
	}
	for _, event := range state.BillingEvents {
		if event.TenantID == tenantID {
			return true
		}
	}
	return false
}

func deleteTenantBillingRecords(records []model.TenantBilling, tenantID string) []model.TenantBilling {
	filtered := records[:0]
	for _, record := range records {
		if record.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, record)
	}
	return filtered
}

func deleteTenantBillingEvents(events []model.TenantBillingEvent, tenantID string) []model.TenantBillingEvent {
	filtered := events[:0]
	for _, event := range events {
		if event.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered
}

func accrueTenantBilling(record *model.TenantBilling, now time.Time) {
	accrueTenantBillingWithCommittedResources(record, record.ManagedCap, now)
}

func accrueTenantBillingWithCommittedResources(
	record *model.TenantBilling,
	committed model.BillingResourceSpec,
	now time.Time,
) {
	if record == nil {
		return
	}
	normalizeTenantBillingRecord(record, now)
	if !now.After(record.LastAccruedAt) {
		return
	}
	hourlyRate := activatedManagedHourlyRateMicroCents(*record, committed)
	if hourlyRate > 0 {
		record.BalanceMicroCents -= hourlyRate * now.Sub(record.LastAccruedAt).Nanoseconds() / int64(time.Hour)
	}
	record.LastAccruedAt = now
	record.UpdatedAt = now
}

func accrueTenantBillingLedger(state *model.State, tenantID string, now time.Time) *model.TenantBilling {
	return accrueTenantBillingLedgerWithIndex(state, newBillingStateIndex(state), tenantID, now)
}

func accrueTenantBillingLedgerWithIndex(state *model.State, index billingStateIndex, tenantID string, now time.Time) *model.TenantBilling {
	if state == nil || strings.TrimSpace(tenantID) == "" {
		return nil
	}
	record := ensureTenantBillingRecord(state, tenantID, now)
	if record == nil {
		return nil
	}

	lastAccruedAt := record.LastAccruedAt
	committed := tenantManagedCommittedResourcesForBillingWithIndex(state, index, *record)
	accrueTenantBillingWithCommittedResources(record, committed, now)
	if !now.After(lastAccruedAt) {
		return record
	}

	elapsedNanos := now.Sub(lastAccruedAt).Nanoseconds()
	if elapsedNanos <= 0 {
		return record
	}

	for _, component := range tenantPublicRuntimeChargeComponentsWithIndex(state, index, tenantID) {
		if component.HourlyRateMicroCents <= 0 {
			continue
		}
		amountMicroCents := component.HourlyRateMicroCents * elapsedNanos / int64(time.Hour)
		if amountMicroCents <= 0 {
			continue
		}

		consumerIndex := findTenantBillingRecord(state, tenantID)
		if consumerIndex < 0 {
			continue
		}
		state.TenantBilling[consumerIndex].BalanceMicroCents -= amountMicroCents
		state.TenantBilling[consumerIndex].UpdatedAt = now
		appendTenantBillingEvent(state, newTenantBillingPublicRuntimeEvent(
			tenantID,
			model.BillingEventTypePublicRuntimeDebit,
			-amountMicroCents,
			state.TenantBilling[consumerIndex].BalanceMicroCents,
			now,
			map[string]string{
				"counterparty_tenant_id": component.OwnerTenantID,
				"runtime_id":             component.RuntimeID,
				"runtime_name":           component.RuntimeName,
			},
		))

		ownerRecord := ensureTenantBillingRecord(state, component.OwnerTenantID, now)
		ownerRecord.BalanceMicroCents += amountMicroCents
		ownerRecord.UpdatedAt = now
		appendTenantBillingEvent(state, newTenantBillingPublicRuntimeEvent(
			component.OwnerTenantID,
			model.BillingEventTypePublicRuntimeCredit,
			amountMicroCents,
			ownerRecord.BalanceMicroCents,
			now,
			map[string]string{
				"counterparty_tenant_id": tenantID,
				"runtime_id":             component.RuntimeID,
				"runtime_name":           component.RuntimeName,
			},
		))
	}

	recordIndex := findTenantBillingRecord(state, tenantID)
	if recordIndex < 0 {
		return nil
	}
	return &state.TenantBilling[recordIndex]
}

func publicRuntimeOfferHourlyRateMicroCents(offer model.RuntimePublicOffer, resources model.BillingResourceSpec) int64 {
	priceBook := normalizeRuntimePublicOfferPriceBook(offer.PriceBook)
	if offer.Free {
		return 0
	}
	return resources.CPUMilliCores*priceBook.CPUMicroCentsPerMilliCoreHour +
		resources.MemoryMebibytes*priceBook.MemoryMicroCentsPerMiBHour +
		resources.StorageGibibytes*priceBook.StorageMicroCentsPerGiBHour
}

func publicRuntimeChargeComponentForResources(state *model.State, consumerTenantID, runtimeID string, resources model.BillingResourceSpec) (publicRuntimeChargeComponent, bool) {
	return publicRuntimeChargeComponentForResourcesWithIndex(newBillingStateIndex(state), consumerTenantID, runtimeID, resources)
}

func mergePublicRuntimeChargeComponent(components map[string]publicRuntimeChargeComponent, component publicRuntimeChargeComponent) {
	if components == nil || component.HourlyRateMicroCents <= 0 {
		return
	}
	key := component.OwnerTenantID + ":" + component.RuntimeID
	if existing, ok := components[key]; ok {
		existing.HourlyRateMicroCents += component.HourlyRateMicroCents
		components[key] = existing
		return
	}
	components[key] = component
}

func tenantPublicRuntimeChargeComponents(state *model.State, tenantID string) []publicRuntimeChargeComponent {
	return tenantPublicRuntimeChargeComponentsWithIndex(state, newBillingStateIndex(state), tenantID)
}

func tenantPublicRuntimeOutgoingHourlyRateMicroCents(state *model.State, tenantID string) int64 {
	return tenantPublicRuntimeOutgoingHourlyRateMicroCentsWithIndex(state, newBillingStateIndex(state), tenantID)
}

func hasBillableManagedEnvelope(spec model.BillingResourceSpec) bool {
	return spec.CPUMilliCores > 0 || spec.MemoryMebibytes > 0 || spec.StorageGibibytes > 0
}

func billingHourlyRateMicroCents(record model.TenantBilling) int64 {
	return billingHourlyRateMicroCentsWithCommittedStorage(record, record.ManagedCap.StorageGibibytes)
}

func billingHourlyRateMicroCentsWithCommittedStorage(record model.TenantBilling, committedStorageGibibytes int64) int64 {
	_ = committedStorageGibibytes
	if !hasBillableManagedEnvelope(record.ManagedCap) {
		return 0
	}
	priceBook := normalizeBillingPriceBook(record.PriceBook)
	return record.ManagedCap.CPUMilliCores*priceBook.CPUMicroCentsPerMilliCoreHour +
		record.ManagedCap.MemoryMebibytes*priceBook.MemoryMicroCentsPerMiBHour +
		record.ManagedCap.StorageGibibytes*priceBook.StorageMicroCentsPerGiBHour
}

func billingMonthlyEstimateMicroCents(record model.TenantBilling) int64 {
	return billingMonthlyEstimateMicroCentsWithCommittedStorage(record, record.ManagedCap.StorageGibibytes)
}

func billingMonthlyEstimateMicroCentsWithCommittedStorage(record model.TenantBilling, committedStorageGibibytes int64) int64 {
	priceBook := normalizeBillingPriceBook(record.PriceBook)
	return billingHourlyRateMicroCentsWithCommittedStorage(record, committedStorageGibibytes) * priceBook.HoursPerMonth
}

func activatedManagedHourlyRateMicroCents(
	record model.TenantBilling,
	committed model.BillingResourceSpec,
) int64 {
	if !hasBillableManagedEnvelope(committed) {
		return 0
	}
	return billingHourlyRateMicroCents(record)
}

func activatedOutgoingHourlyRateMicroCents(
	state *model.State,
	record model.TenantBilling,
	committed model.BillingResourceSpec,
) int64 {
	return activatedManagedHourlyRateMicroCents(record, committed) +
		tenantPublicRuntimeOutgoingHourlyRateMicroCents(state, record.TenantID)
}

func activatedOutgoingMonthlyEstimateMicroCents(
	state *model.State,
	record model.TenantBilling,
	committed model.BillingResourceSpec,
) int64 {
	priceBook := normalizeBillingPriceBook(record.PriceBook)
	return activatedOutgoingHourlyRateMicroCents(state, record, committed) * priceBook.HoursPerMonth
}

func billingBalanceRestricted(record model.TenantBilling) bool {
	return billingBalanceRestrictedWithCommittedStorage(record, record.ManagedCap.StorageGibibytes)
}

func billingRunwayHours(record model.TenantBilling) *float64 {
	return billingRunwayHoursWithCommittedStorage(record, record.ManagedCap.StorageGibibytes)
}

func billingBalanceRestrictedWithCommittedStorage(record model.TenantBilling, committedStorageGibibytes int64) bool {
	return billingHourlyRateMicroCentsWithCommittedStorage(record, committedStorageGibibytes) > 0 &&
		record.BalanceMicroCents <= 0
}

func billingRunwayHoursWithCommittedStorage(record model.TenantBilling, committedStorageGibibytes int64) *float64 {
	hourlyRate := billingHourlyRateMicroCentsWithCommittedStorage(record, committedStorageGibibytes)
	return billingRunwayHoursForHourlyRate(record, hourlyRate)
}

func billingRunwayHoursForHourlyRate(record model.TenantBilling, hourlyRate int64) *float64 {
	if hourlyRate <= 0 || record.BalanceMicroCents <= 0 {
		return nil
	}
	hours := float64(record.BalanceMicroCents) / float64(hourlyRate)
	return &hours
}

func activatedOutgoingBalanceRestricted(
	state *model.State,
	record model.TenantBilling,
	committed model.BillingResourceSpec,
) bool {
	return activatedOutgoingHourlyRateMicroCents(state, record, committed) > 0 &&
		record.BalanceMicroCents <= 0
}

func activatedOutgoingRunwayHours(
	state *model.State,
	record model.TenantBilling,
	committed model.BillingResourceSpec,
) *float64 {
	hourlyRate := activatedOutgoingHourlyRateMicroCents(state, record, committed)
	if hourlyRate <= 0 || record.BalanceMicroCents <= 0 {
		return nil
	}
	hours := float64(record.BalanceMicroCents) / float64(hourlyRate)
	return &hours
}

func buildTenantBillingSummary(state *model.State, record model.TenantBilling) model.TenantBillingSummary {
	return buildTenantBillingSummaryWithIndex(state, newBillingStateIndex(state), record)
}

func buildTenantBillingSummaryWithIndex(state *model.State, index billingStateIndex, record model.TenantBilling) model.TenantBillingSummary {
	committed := tenantManagedCommittedResourcesForBillingWithIndex(state, index, record)
	available := clampResourceSpecSub(record.ManagedCap, committed)
	managedHourlyRate := activatedManagedHourlyRateMicroCents(record, committed)
	publicHourlyRate := tenantPublicRuntimeOutgoingHourlyRateMicroCentsWithIndex(state, index, record.TenantID)
	outgoingHourlyRate := managedHourlyRate + publicHourlyRate
	billingActive := outgoingHourlyRate > 0
	overCap := billingActive && resourceSpecExceeds(committed, record.ManagedCap)
	balanceRestricted := outgoingHourlyRate > 0 && record.BalanceMicroCents <= 0
	status, reason := tenantBillingStatus(record, committed, managedHourlyRate, publicHourlyRate)
	events := recentTenantBillingEvents(state, record.TenantID)
	priceBook := normalizeBillingPriceBook(record.PriceBook)

	return model.TenantBillingSummary{
		TenantID:                  record.TenantID,
		Status:                    status,
		StatusReason:              reason,
		BYOVPSFree:                true,
		OverCap:                   overCap,
		BalanceRestricted:         balanceRestricted,
		ManagedCap:                record.ManagedCap,
		ManagedCommitted:          committed,
		ManagedAvailable:          available,
		DefaultAppResources:       model.BillingResourceSpec{},
		DefaultPostgresResources:  model.DefaultManagedPostgresBillingResources(),
		PriceBook:                 priceBook,
		HourlyRateMicroCents:      outgoingHourlyRate,
		MonthlyEstimateMicroCents: outgoingHourlyRate * priceBook.HoursPerMonth,
		BalanceMicroCents:         record.BalanceMicroCents,
		RunwayHours:               billingRunwayHoursForHourlyRate(record, outgoingHourlyRate),
		LastAccruedAt:             record.LastAccruedAt,
		UpdatedAt:                 record.UpdatedAt,
		Events:                    events,
	}
}

func tenantBillingStatus(record model.TenantBilling, committed model.BillingResourceSpec, managedHourlyRate, publicHourlyRate int64) (string, string) {
	totalHourlyRate := managedHourlyRate + publicHourlyRate
	switch {
	case totalHourlyRate <= 0:
		return model.BillingStatusInactive, "Billing is inactive until any managed resource, retained managed image inventory, or paid public server usage becomes active. Your own attached servers remain free unless you publish them for others."
	case resourceSpecExceeds(committed, record.ManagedCap):
		return model.BillingStatusOverCap, "Current live managed capacity is above the saved envelope. Save a higher cap to match what is already committed before adding more managed capacity."
	case totalHourlyRate > 0 && record.BalanceMicroCents <= 0:
		return model.BillingStatusRestricted, "Balance is depleted. Top up before increasing managed capacity or deploying onto paid public servers."
	case publicHourlyRate > 0 && managedHourlyRate <= 0:
		return model.BillingStatusActive, "Deployments placed on paid public servers are metered hourly from your balance. Your own attached servers remain free unless you publish them for others."
	default:
		return model.BillingStatusActive, "Once any managed resource or retained managed image inventory is active, the saved managed envelope is metered hourly from your balance. Paid public-server deployments also deduct credits hourly, while your own attached servers stay free unless you publish them for others."
	}
}

func recentTenantBillingEvents(state *model.State, tenantID string) []model.TenantBillingEvent {
	if state == nil || len(state.BillingEvents) == 0 {
		return []model.TenantBillingEvent{}
	}
	events := make([]model.TenantBillingEvent, 0, billingHistoryLimit)
	for _, event := range state.BillingEvents {
		if event.TenantID != tenantID {
			continue
		}
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].CreatedAt.Equal(events[j].CreatedAt) {
			return events[i].ID > events[j].ID
		}
		return events[i].CreatedAt.After(events[j].CreatedAt)
	})
	if len(events) > billingHistoryLimit {
		events = events[:billingHistoryLimit]
	}
	return events
}

func tenantManagedCommittedResources(state *model.State, tenantID string) model.BillingResourceSpec {
	return tenantManagedCommittedResourcesWithIndex(state, newBillingStateIndex(state), tenantID)
}

func tenantManagedCommittedResourcesForBilling(state *model.State, record model.TenantBilling) model.BillingResourceSpec {
	return tenantManagedCommittedResourcesForBillingWithIndex(state, newBillingStateIndex(state), record)
}

func addManagedImageStorageCommitment(spec model.BillingResourceSpec, imageStorageGibibytes int64) model.BillingResourceSpec {
	spec.StorageGibibytes += maxInt64(0, imageStorageGibibytes)
	return spec
}

func appManagedBundleCommitment(state *model.State, app model.App, runtimeID string, replicas int) model.BillingResourceSpec {
	return appManagedBundleCommitmentWithIndex(newBillingStateIndex(state), app, runtimeID, replicas)
}

func boundManagedServicesForApp(state *model.State, appID string) []model.BackingService {
	return newBillingStateIndex(state).managedServicesForApp(appID)
}

func isBillableManagedBackingService(service model.BackingService) bool {
	if isDeletedBackingService(service) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
		return false
	}
	provisioner := strings.TrimSpace(strings.ToLower(service.Provisioner))
	return provisioner == "" || provisioner == model.BackingServiceProvisionerManaged
}

func backingServiceResources(service model.BackingService) model.BillingResourceSpec {
	if service.Spec.Postgres == nil {
		return model.BillingResourceSpec{}
	}
	return postgresEffectiveResources(*service.Spec.Postgres)
}

func backingServiceRuntimeID(service model.BackingService, fallbackRuntimeID string) string {
	if service.Spec.Postgres != nil {
		if runtimeID := strings.TrimSpace(service.Spec.Postgres.RuntimeID); runtimeID != "" {
			return runtimeID
		}
	}
	return strings.TrimSpace(fallbackRuntimeID)
}

func appEffectiveResources(spec model.AppSpec) model.BillingResourceSpec {
	compute := model.ResourceSpec{}
	if spec.Resources != nil {
		compute = *spec.Resources
	}
	return model.BillingResourceSpec{
		CPUMilliCores:    compute.CPUMilliCores,
		MemoryMebibytes:  compute.MemoryMebibytes,
		StorageGibibytes: appStorageGibibytes(spec),
	}
}

func postgresEffectiveResources(spec model.AppPostgresSpec) model.BillingResourceSpec {
	compute := model.DefaultManagedPostgresResources()
	if spec.Resources != nil {
		compute = *spec.Resources
	}
	return model.BillingResourceSpec{
		CPUMilliCores:    compute.CPUMilliCores,
		MemoryMebibytes:  compute.MemoryMebibytes,
		StorageGibibytes: postgresStorageGibibytes(&spec),
	}
}

func runtimeTypeForState(state *model.State, runtimeID string) string {
	return newBillingStateIndex(state).runtimeType(runtimeID)
}

func isBillableManagedRuntimeType(runtimeType string) bool {
	switch strings.TrimSpace(runtimeType) {
	case model.RuntimeTypeManagedShared:
		return true
	default:
		return false
	}
}

func validateTenantOperationBilling(
	record model.TenantBilling,
	currentTotal model.BillingResourceSpec,
	nextTotal model.BillingResourceSpec,
	currentPublicHourlyRateMicroCents int64,
	nextPublicHourlyRateMicroCents int64,
) error {
	if err := validateTenantManagedCapacityIncrease(record, currentTotal, nextTotal); err != nil {
		return err
	}
	totalOutgoingNext := activatedManagedHourlyRateMicroCents(record, nextTotal) + nextPublicHourlyRateMicroCents
	if record.BalanceMicroCents <= 0 &&
		(resourceSpecExceeds(nextTotal, currentTotal) || nextPublicHourlyRateMicroCents > currentPublicHourlyRateMicroCents) &&
		totalOutgoingNext > 0 {
		return fmt.Errorf(
			"%w: balance=%d hourly_rate=%d microcents",
			ErrBillingBalanceDepleted,
			record.BalanceMicroCents,
			totalOutgoingNext,
		)
	}
	return nil
}

func projectedAppManagedBundleCommitment(state *model.State, app model.App, op model.Operation) (model.BillingResourceSpec, model.BillingResourceSpec, error) {
	index := newBillingStateIndex(state)
	current := appManagedBundleCommitmentWithIndex(index, app, app.Status.CurrentRuntimeID, app.Status.CurrentReplicas)
	projection := cloneBillingProjectionState(state, app)
	opCopy := op
	opCopy.DesiredSpec = cloneAppSpec(op.DesiredSpec)
	opCopy.DesiredSource = cloneAppSource(op.DesiredSource)
	if strings.TrimSpace(opCopy.ID) == "" {
		opCopy.ID = "billing-projection"
	}
	if err := applyOperationToApp(&projection, &opCopy); err != nil {
		return model.BillingResourceSpec{}, model.BillingResourceSpec{}, err
	}
	if len(projection.Apps) == 0 {
		return current, model.BillingResourceSpec{}, nil
	}
	projectedApp := projection.Apps[0]
	next := appManagedBundleCommitmentWithIndex(newBillingStateIndex(&projection), projectedApp, projectedApp.Status.CurrentRuntimeID, projectedApp.Status.CurrentReplicas)
	return current, next, nil
}

func cloneBillingProjectionState(state *model.State, app model.App) model.State {
	index := newBillingStateIndex(state)
	projection := model.State{
		Apps:            []model.App{cloneAppForBilling(app)},
		BackingServices: []model.BackingService{},
		ServiceBindings: []model.ServiceBinding{},
		Runtimes:        []model.Runtime{},
	}
	for _, runtime := range state.Runtimes {
		projection.Runtimes = append(projection.Runtimes, runtime)
	}
	for _, binding := range state.ServiceBindings {
		if binding.AppID != app.ID {
			continue
		}
		projection.ServiceBindings = append(projection.ServiceBindings, cloneServiceBinding(binding))
		service, found := index.service(binding.ServiceID)
		if found {
			projection.BackingServices = append(projection.BackingServices, cloneBackingService(service))
		}
	}
	copiedServiceIDs := make(map[string]struct{}, len(projection.BackingServices))
	for _, service := range projection.BackingServices {
		copiedServiceIDs[service.ID] = struct{}{}
	}
	for _, service := range state.BackingServices {
		if service.OwnerAppID != app.ID {
			continue
		}
		if _, exists := copiedServiceIDs[service.ID]; exists {
			continue
		}
		projection.BackingServices = append(projection.BackingServices, cloneBackingService(service))
		copiedServiceIDs[service.ID] = struct{}{}
	}
	return projection
}

func cloneAppForBilling(app model.App) model.App {
	out := app
	out.Source = model.CloneAppSource(app.Source)
	out.OriginSource = model.CloneAppSource(app.OriginSource)
	out.BuildSource = model.CloneAppSource(app.BuildSource)
	model.NormalizeAppSourceState(&out)
	if app.Route != nil {
		route := *app.Route
		out.Route = &route
	}
	if cloned := cloneAppSpec(&app.Spec); cloned != nil {
		out.Spec = *cloned
	}
	out.Bindings = nil
	out.BackingServices = nil
	return out
}

func addResourceSpec(left, right model.BillingResourceSpec) model.BillingResourceSpec {
	return model.BillingResourceSpec{
		CPUMilliCores:    left.CPUMilliCores + right.CPUMilliCores,
		MemoryMebibytes:  left.MemoryMebibytes + right.MemoryMebibytes,
		StorageGibibytes: left.StorageGibibytes + right.StorageGibibytes,
	}
}

func maxResourceSpec(left, right model.BillingResourceSpec) model.BillingResourceSpec {
	return model.BillingResourceSpec{
		CPUMilliCores:    maxInt64(left.CPUMilliCores, right.CPUMilliCores),
		MemoryMebibytes:  maxInt64(left.MemoryMebibytes, right.MemoryMebibytes),
		StorageGibibytes: maxInt64(left.StorageGibibytes, right.StorageGibibytes),
	}
}

func subtractResourceSpec(left, right model.BillingResourceSpec) model.BillingResourceSpec {
	return model.BillingResourceSpec{
		CPUMilliCores:    maxInt64(0, left.CPUMilliCores-right.CPUMilliCores),
		MemoryMebibytes:  maxInt64(0, left.MemoryMebibytes-right.MemoryMebibytes),
		StorageGibibytes: maxInt64(0, left.StorageGibibytes-right.StorageGibibytes),
	}
}

func clampResourceSpecSub(left, right model.BillingResourceSpec) model.BillingResourceSpec {
	return subtractResourceSpec(left, right)
}

func multiplyResourceSpec(spec model.BillingResourceSpec, factor int64) model.BillingResourceSpec {
	if factor <= 0 {
		return model.BillingResourceSpec{}
	}
	return model.BillingResourceSpec{
		CPUMilliCores:    spec.CPUMilliCores * factor,
		MemoryMebibytes:  spec.MemoryMebibytes * factor,
		StorageGibibytes: spec.StorageGibibytes * factor,
	}
}

func resourceSpecExceeds(left, right model.BillingResourceSpec) bool {
	return left.CPUMilliCores > right.CPUMilliCores ||
		left.MemoryMebibytes > right.MemoryMebibytes ||
		left.StorageGibibytes > right.StorageGibibytes
}

func validateTenantManagedCapacityIncrease(
	record model.TenantBilling,
	currentTotal model.BillingResourceSpec,
	nextTotal model.BillingResourceSpec,
) error {
	if !resourceSpecExceeds(nextTotal, record.ManagedCap) {
		return nil
	}
	if !resourceSpecExceeds(nextTotal, currentTotal) {
		return nil
	}
	return describeBillingCapExceeded(record, nextTotal)
}

func validateCommittedManagedCapacity(managedCap, committed model.BillingResourceSpec) error {
	if !resourceSpecExceeds(committed, managedCap) {
		return nil
	}
	return fmt.Errorf(
		"%w: committed managed capacity cpu=%dm/%dm memory=%dMi/%dMi storage=%dGi/%dGi",
		ErrBillingCapExceeded,
		committed.CPUMilliCores,
		managedCap.CPUMilliCores,
		committed.MemoryMebibytes,
		managedCap.MemoryMebibytes,
		committed.StorageGibibytes,
		managedCap.StorageGibibytes,
	)
}

func projectedTenantManagedTotals(state *model.State, app model.App, op model.Operation) (model.BillingResourceSpec, model.BillingResourceSpec, error) {
	index := newBillingStateIndex(state)
	currentTotal := tenantManagedCommittedResourcesWithIndex(state, index, app.TenantID)
	currentBundle, nextBundle, err := projectedAppManagedBundleCommitment(state, app, op)
	if err != nil {
		return model.BillingResourceSpec{}, model.BillingResourceSpec{}, err
	}
	nextTotal := addResourceSpec(subtractResourceSpec(currentTotal, currentBundle), nextBundle)
	return currentTotal, nextTotal, nil
}

func projectedTenantManagedTotalsWithBilling(state *model.State, app model.App, op model.Operation, record model.TenantBilling) (model.BillingResourceSpec, model.BillingResourceSpec, error) {
	currentTotal, nextTotal, err := projectedTenantManagedTotals(state, app, op)
	if err != nil {
		return model.BillingResourceSpec{}, model.BillingResourceSpec{}, err
	}
	return addManagedImageStorageCommitment(currentTotal, record.ManagedImageStorageGibibytes),
		addManagedImageStorageCommitment(nextTotal, record.ManagedImageStorageGibibytes),
		nil
}

func cloneTenantBillingState(state *model.State) model.State {
	if state == nil {
		return model.State{}
	}
	projection := model.State{
		Apps:            make([]model.App, 0, len(state.Apps)),
		BackingServices: make([]model.BackingService, 0, len(state.BackingServices)),
		ServiceBindings: make([]model.ServiceBinding, 0, len(state.ServiceBindings)),
		Runtimes:        make([]model.Runtime, 0, len(state.Runtimes)),
	}
	for _, app := range state.Apps {
		projection.Apps = append(projection.Apps, cloneAppForBilling(app))
	}
	for _, service := range state.BackingServices {
		projection.BackingServices = append(projection.BackingServices, cloneBackingService(service))
	}
	for _, binding := range state.ServiceBindings {
		projection.ServiceBindings = append(projection.ServiceBindings, cloneServiceBinding(binding))
	}
	for _, runtime := range state.Runtimes {
		runtimeCopy := runtime
		runtimeCopy.Labels = cloneMap(runtime.Labels)
		runtimeCopy.PublicOffer = cloneRuntimePublicOffer(runtime.PublicOffer)
		projection.Runtimes = append(projection.Runtimes, runtimeCopy)
	}
	return projection
}

func validateTenantManagedCapacityProjection(
	state *model.State,
	record model.TenantBilling,
	apply func(*model.State),
) error {
	currentTotal := tenantManagedCommittedResourcesForBillingWithIndex(state, newBillingStateIndex(state), record)
	projection := cloneTenantBillingState(state)
	apply(&projection)
	nextTotal := tenantManagedCommittedResourcesForBillingWithIndex(&projection, newBillingStateIndex(&projection), record)
	return validateTenantManagedCapacityIncrease(record, currentTotal, nextTotal)
}

func projectedTenantPublicRuntimeHourlyRates(state *model.State, app model.App, op model.Operation) (int64, int64, error) {
	currentRate := tenantPublicRuntimeOutgoingHourlyRateMicroCentsWithIndex(state, newBillingStateIndex(state), app.TenantID)
	projection := cloneTenantBillingState(state)
	opCopy := op
	opCopy.DesiredSpec = cloneAppSpec(op.DesiredSpec)
	opCopy.DesiredSource = cloneAppSource(op.DesiredSource)
	if strings.TrimSpace(opCopy.ID) == "" {
		opCopy.ID = "billing-public-runtime-projection"
	}
	if err := applyOperationToApp(&projection, &opCopy); err != nil {
		return 0, 0, err
	}
	nextRate := tenantPublicRuntimeOutgoingHourlyRateMicroCentsWithIndex(&projection, newBillingStateIndex(&projection), app.TenantID)
	return currentRate, nextRate, nil
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func describeBillingCapExceeded(record model.TenantBilling, nextTotal model.BillingResourceSpec) error {
	return fmt.Errorf(
		"%w: requested managed capacity cpu=%dm/%dm memory=%dMi/%dMi storage=%dGi/%dGi",
		ErrBillingCapExceeded,
		nextTotal.CPUMilliCores,
		record.ManagedCap.CPUMilliCores,
		nextTotal.MemoryMebibytes,
		record.ManagedCap.MemoryMebibytes,
		nextTotal.StorageGibibytes,
		record.ManagedCap.StorageGibibytes,
	)
}

func describeBillingBalanceDepleted(record model.TenantBilling) error {
	return describeBillingBalanceDepletedWithCommittedStorage(record, record.ManagedCap.StorageGibibytes)
}

func describeBillingBalanceDepletedWithCommittedStorage(record model.TenantBilling, committedStorageGibibytes int64) error {
	hourlyRateMicroCents := billingHourlyRateMicroCentsWithCommittedStorage(record, committedStorageGibibytes)
	if hourlyRateMicroCents <= 0 {
		return ErrBillingBalanceDepleted
	}
	return fmt.Errorf(
		"%w: balance=%d hourly_rate=%d microcents",
		ErrBillingBalanceDepleted,
		record.BalanceMicroCents,
		hourlyRateMicroCents,
	)
}

func appStorageGibibytes(spec model.AppSpec) int64 {
	if spec.PersistentStorage != nil {
		return persistentStorageGibibytes(spec.PersistentStorage)
	}
	return workspaceStorageGibibytes(spec.Workspace)
}

func persistentStorageGibibytes(spec *model.AppPersistentStorageSpec) int64 {
	if spec == nil {
		return 0
	}
	if model.AppPersistentStorageSpecUsesSharedProjectRWX(spec) {
		return 0
	}
	size := strings.TrimSpace(spec.StorageSize)
	if size == "" {
		size = model.DefaultManagedWorkspaceStorageSize
	}
	return storageQuantityGibibytes(size)
}

func workspaceStorageGibibytes(spec *model.AppWorkspaceSpec) int64 {
	if spec == nil {
		return 0
	}
	size := strings.TrimSpace(spec.StorageSize)
	if size == "" {
		size = model.DefaultManagedWorkspaceStorageSize
	}
	return storageQuantityGibibytes(size)
}

func postgresStorageGibibytes(spec *model.AppPostgresSpec) int64 {
	if spec == nil {
		return 0
	}
	size := strings.TrimSpace(spec.StorageSize)
	if size == "" {
		size = model.DefaultManagedPostgresStorageSize
	}
	return storageQuantityGibibytes(size)
}

func storageQuantityGibibytes(value string) int64 {
	bytes, ok := parseStorageQuantityBytes(value)
	if !ok || bytes <= 0 {
		return 0
	}
	return int64(math.Ceil(float64(bytes) / float64(bytesPerGiB)))
}

func parseStorageQuantityBytes(value string) (int64, bool) {
	number, suffix, ok := splitBillingQuantity(value)
	if !ok {
		return 0, false
	}

	multiplier := 0.0
	switch suffix {
	case "":
		multiplier = 1
	case "K":
		multiplier = 1_000
	case "M":
		multiplier = 1_000_000
	case "G":
		multiplier = 1_000_000_000
	case "T":
		multiplier = 1_000_000_000_000
	case "P":
		multiplier = 1_000_000_000_000_000
	case "E":
		multiplier = 1_000_000_000_000_000_000
	case "Ki":
		multiplier = 1 << 10
	case "Mi":
		multiplier = 1 << 20
	case "Gi":
		multiplier = 1 << 30
	case "Ti":
		multiplier = 1 << 40
	case "Pi":
		multiplier = 1 << 50
	case "Ei":
		multiplier = 1 << 60
	default:
		return 0, false
	}

	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, false
	}
	return int64(math.Round(parsed * multiplier)), true
}

func splitBillingQuantity(value string) (string, string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", false
	}
	matches := billingQuantityPattern.FindStringSubmatch(value)
	if len(matches) != 3 {
		return "", "", false
	}
	return matches[1], matches[2], true
}
