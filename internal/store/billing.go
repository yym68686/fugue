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
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := ensureTenantBillingRecord(state, tenantID, now)
		committed := tenantManagedCommittedResourcesForBilling(state, *billing)
		accrueTenantBillingWithCommittedStorage(billing, committed.StorageGibibytes, now)
		summary = buildTenantBillingSummary(state, *billing)
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
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := ensureTenantBillingRecord(state, tenantID, now)
		committed := tenantManagedCommittedResourcesForBilling(state, *billing)
		accrueTenantBillingWithCommittedStorage(billing, committed.StorageGibibytes, now)
		billing.ManagedCap = normalizedCap
		billing.UpdatedAt = now
		appendTenantBillingEvent(state, newTenantBillingConfigUpdatedEvent(
			tenantID,
			normalizedCap,
			billing.BalanceMicroCents,
			now,
			nil,
		))
		summary = buildTenantBillingSummary(state, *billing)
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
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := ensureTenantBillingRecord(state, tenantID, now)
		committed := tenantManagedCommittedResourcesForBilling(state, *billing)
		accrueTenantBillingWithCommittedStorage(billing, committed.StorageGibibytes, now)
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
		summary = buildTenantBillingSummary(state, *billing)
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
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := ensureTenantBillingRecord(state, tenantID, now)
		committed := tenantManagedCommittedResourcesForBilling(state, *billing)
		accrueTenantBillingWithCommittedStorage(billing, committed.StorageGibibytes, now)
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
		summary = buildTenantBillingSummary(state, *billing)
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
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		billing := ensureTenantBillingRecord(state, tenantID, now)
		committed := tenantManagedCommittedResourcesForBilling(state, *billing)
		accrueTenantBillingWithCommittedStorage(billing, committed.StorageGibibytes, now)
		if billing.ManagedImageStorageGibibytes != storageGibibytes {
			billing.ManagedImageStorageGibibytes = storageGibibytes
			billing.UpdatedAt = now
		}
		summary = buildTenantBillingSummary(state, *billing)
		return nil
	})
	return summary, err
}

func normalizeAppSpecResources(spec *model.AppSpec) error {
	if spec == nil {
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
	if out.CPUMilliCores < 0 || out.MemoryMebibytes < 0 {
		return nil, ErrInvalidInput
	}
	if out.CPUMilliCores == 0 && out.MemoryMebibytes == 0 {
		return nil, nil
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
	spec.Resources = resources
	return nil
}

func normalizeWorkloadResources(spec *model.ResourceSpec, defaults model.ResourceSpec) (*model.ResourceSpec, error) {
	if spec == nil {
		value := defaults
		return &value, nil
	}
	out := *spec
	if out.CPUMilliCores < 0 || out.MemoryMebibytes < 0 {
		return nil, ErrInvalidInput
	}
	if out.CPUMilliCores == 0 {
		out.CPUMilliCores = defaults.CPUMilliCores
	}
	if out.MemoryMebibytes == 0 {
		out.MemoryMebibytes = defaults.MemoryMebibytes
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
	accrueTenantBillingWithCommittedStorage(record, record.ManagedCap.StorageGibibytes, now)
}

func accrueTenantBillingWithCommittedStorage(record *model.TenantBilling, committedStorageGibibytes int64, now time.Time) {
	if record == nil {
		return
	}
	normalizeTenantBillingRecord(record, now)
	if !now.After(record.LastAccruedAt) {
		return
	}
	hourlyRate := billingHourlyRateMicroCentsWithCommittedStorage(*record, committedStorageGibibytes)
	if hourlyRate > 0 {
		record.BalanceMicroCents -= hourlyRate * now.Sub(record.LastAccruedAt).Nanoseconds() / int64(time.Hour)
	}
	record.LastAccruedAt = now
	record.UpdatedAt = now
}

func hasBillableManagedEnvelope(spec model.BillingResourceSpec) bool {
	return spec.CPUMilliCores > 0 && spec.MemoryMebibytes > 0
}

func billingHourlyRateMicroCents(record model.TenantBilling) int64 {
	return billingHourlyRateMicroCentsWithCommittedStorage(record, record.ManagedCap.StorageGibibytes)
}

func billingHourlyRateMicroCentsWithCommittedStorage(record model.TenantBilling, committedStorageGibibytes int64) int64 {
	if !hasBillableManagedEnvelope(record.ManagedCap) {
		return 0
	}
	priceBook := normalizeBillingPriceBook(record.PriceBook)
	return record.ManagedCap.CPUMilliCores*priceBook.CPUMicroCentsPerMilliCoreHour +
		record.ManagedCap.MemoryMebibytes*priceBook.MemoryMicroCentsPerMiBHour +
		billingChargeableStorageGibibytes(record, committedStorageGibibytes)*priceBook.StorageMicroCentsPerGiBHour
}

func billingMonthlyEstimateMicroCents(record model.TenantBilling) int64 {
	return billingMonthlyEstimateMicroCentsWithCommittedStorage(record, record.ManagedCap.StorageGibibytes)
}

func billingMonthlyEstimateMicroCentsWithCommittedStorage(record model.TenantBilling, committedStorageGibibytes int64) int64 {
	priceBook := normalizeBillingPriceBook(record.PriceBook)
	return billingHourlyRateMicroCentsWithCommittedStorage(record, committedStorageGibibytes) * priceBook.HoursPerMonth
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
	if hourlyRate <= 0 || record.BalanceMicroCents <= 0 {
		return nil
	}
	hours := float64(record.BalanceMicroCents) / float64(hourlyRate)
	return &hours
}

func buildTenantBillingSummary(state *model.State, record model.TenantBilling) model.TenantBillingSummary {
	committed := tenantManagedCommittedResourcesForBilling(state, record)
	available := clampResourceSpecSub(record.ManagedCap, committed)
	billingActive := billingHourlyRateMicroCentsWithCommittedStorage(record, committed.StorageGibibytes) > 0
	overCap := billingActive && resourceSpecExceeds(committed, record.ManagedCap)
	balanceRestricted := billingBalanceRestrictedWithCommittedStorage(record, committed.StorageGibibytes)
	status, reason := tenantBillingStatus(record, committed)
	events := recentTenantBillingEvents(state, record.TenantID)

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
		PriceBook:                 normalizeBillingPriceBook(record.PriceBook),
		HourlyRateMicroCents:      billingHourlyRateMicroCentsWithCommittedStorage(record, committed.StorageGibibytes),
		MonthlyEstimateMicroCents: billingMonthlyEstimateMicroCentsWithCommittedStorage(record, committed.StorageGibibytes),
		BalanceMicroCents:         record.BalanceMicroCents,
		RunwayHours:               billingRunwayHoursWithCommittedStorage(record, committed.StorageGibibytes),
		LastAccruedAt:             record.LastAccruedAt,
		UpdatedAt:                 record.UpdatedAt,
		Events:                    events,
	}
}

func tenantBillingStatus(record model.TenantBilling, committed model.BillingResourceSpec) (string, string) {
	switch {
	case billingHourlyRateMicroCentsWithCommittedStorage(record, committed.StorageGibibytes) <= 0:
		return model.BillingStatusInactive, "Managed billing is inactive until both CPU and memory are above zero. Storage, including retained managed image inventory, is billed inside an active managed envelope. External-owned runtimes remain free."
	case resourceSpecExceeds(committed, record.ManagedCap):
		return model.BillingStatusOverCap, "Current live managed capacity is above the saved envelope. This includes retained managed image inventory. Managed expansion still works while balance stays positive, and Fugue will lift the envelope automatically on the next managed capacity increase."
	case billingBalanceRestrictedWithCommittedStorage(record, committed.StorageGibibytes):
		return model.BillingStatusRestricted, "Balance is depleted. Top up before increasing managed capacity."
	default:
		return model.BillingStatusActive, "Managed capacity is metered hourly from the saved envelope. Storage billing uses the higher of the saved storage envelope or current managed storage, including retained managed image inventory. BYO VPS stays free."
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
	total := model.BillingResourceSpec{}
	if state == nil {
		return total
	}
	for _, app := range state.Apps {
		if app.TenantID != tenantID || isDeletedApp(app) {
			continue
		}
		total = addResourceSpec(total, appManagedBundleCommitment(state, app, app.Status.CurrentRuntimeID, app.Status.CurrentReplicas))
	}
	return total
}

func tenantManagedCommittedResourcesForBilling(state *model.State, record model.TenantBilling) model.BillingResourceSpec {
	return addManagedImageStorageCommitment(tenantManagedCommittedResources(state, record.TenantID), record.ManagedImageStorageGibibytes)
}

func addManagedImageStorageCommitment(spec model.BillingResourceSpec, imageStorageGibibytes int64) model.BillingResourceSpec {
	spec.StorageGibibytes += maxInt64(0, imageStorageGibibytes)
	return spec
}

func appManagedBundleCommitment(state *model.State, app model.App, runtimeID string, replicas int) model.BillingResourceSpec {
	if replicas <= 0 || !isBillableManagedRuntimeType(runtimeTypeForState(state, runtimeID)) {
		return model.BillingResourceSpec{}
	}
	total := multiplyResourceSpec(appEffectiveResources(app.Spec), int64(replicas))
	services := boundManagedServicesForApp(state, app.ID)
	for _, service := range services {
		total = addResourceSpec(total, backingServiceResources(service))
	}
	return total
}

func boundManagedServicesForApp(state *model.State, appID string) []model.BackingService {
	if state == nil {
		return nil
	}
	seen := map[string]struct{}{}
	services := make([]model.BackingService, 0)
	for _, binding := range state.ServiceBindings {
		if binding.AppID != appID {
			continue
		}
		index := findBackingService(state, binding.ServiceID)
		if index < 0 {
			continue
		}
		service := state.BackingServices[index]
		if !isBillableManagedBackingService(service) {
			continue
		}
		if _, ok := seen[service.ID]; ok {
			continue
		}
		seen[service.ID] = struct{}{}
		services = append(services, service)
	}
	return services
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

func appEffectiveResources(spec model.AppSpec) model.BillingResourceSpec {
	compute := model.ResourceSpec{}
	if spec.Resources != nil {
		compute = *spec.Resources
	}
	return model.BillingResourceSpec{
		CPUMilliCores:    compute.CPUMilliCores,
		MemoryMebibytes:  compute.MemoryMebibytes,
		StorageGibibytes: workspaceStorageGibibytes(spec.Workspace),
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
	if state == nil || strings.TrimSpace(runtimeID) == "" {
		return ""
	}
	index := findRuntime(state, runtimeID)
	if index < 0 {
		return ""
	}
	return state.Runtimes[index].Type
}

func isBillableManagedRuntimeType(runtimeType string) bool {
	switch strings.TrimSpace(runtimeType) {
	case model.RuntimeTypeManagedShared, model.RuntimeTypeManagedOwned:
		return true
	default:
		return false
	}
}

func validateManagedOperationBilling(record model.TenantBilling, currentTotal, nextTotal model.BillingResourceSpec) error {
	if billingBalanceRestrictedWithCommittedStorage(record, currentTotal.StorageGibibytes) &&
		resourceSpecExceeds(nextTotal, currentTotal) {
		return describeBillingBalanceDepletedWithCommittedStorage(record, currentTotal.StorageGibibytes)
	}
	return nil
}

func projectedAppManagedBundleCommitment(state *model.State, app model.App, op model.Operation) (model.BillingResourceSpec, model.BillingResourceSpec, error) {
	current := appManagedBundleCommitment(state, app, app.Status.CurrentRuntimeID, app.Status.CurrentReplicas)
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
	next := appManagedBundleCommitment(&projection, projectedApp, projectedApp.Status.CurrentRuntimeID, projectedApp.Status.CurrentReplicas)
	return current, next, nil
}

func cloneBillingProjectionState(state *model.State, app model.App) model.State {
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
		index := findBackingService(state, binding.ServiceID)
		if index >= 0 {
			projection.BackingServices = append(projection.BackingServices, cloneBackingService(state.BackingServices[index]))
		}
	}
	for _, service := range state.BackingServices {
		if service.OwnerAppID != app.ID {
			continue
		}
		if findBackingService(&projection, service.ID) >= 0 {
			continue
		}
		projection.BackingServices = append(projection.BackingServices, cloneBackingService(service))
	}
	return projection
}

func cloneAppForBilling(app model.App) model.App {
	out := app
	if app.Source != nil {
		source := *app.Source
		out.Source = &source
	}
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

func projectedTenantManagedTotals(state *model.State, app model.App, op model.Operation) (model.BillingResourceSpec, model.BillingResourceSpec, error) {
	currentTotal := tenantManagedCommittedResources(state, app.TenantID)
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

func nextManagedEnvelope(record model.TenantBilling, currentTotal, nextTotal model.BillingResourceSpec) (model.BillingResourceSpec, bool) {
	if !resourceSpecExceeds(nextTotal, currentTotal) {
		return record.ManagedCap, false
	}
	expanded := maxResourceSpec(record.ManagedCap, nextTotal)
	return expanded, expanded != record.ManagedCap
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

func billingChargeableStorageGibibytes(record model.TenantBilling, committedStorageGibibytes int64) int64 {
	return maxInt64(record.ManagedCap.StorageGibibytes, committedStorageGibibytes)
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
