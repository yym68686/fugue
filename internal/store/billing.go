package store

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

var (
	ErrBillingCapExceeded     = errors.New("billing cap exceeded")
	ErrBillingBalanceDepleted = errors.New("billing balance depleted")
)

const (
	billingHistoryLimit       = 12
	microCentsPerCent   int64 = 1_000_000
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
		accrueTenantBilling(billing, now)
		summary = buildTenantBillingSummary(state, *billing)
		return nil
	})
	return summary, err
}

func (s *Store) UpdateTenantBilling(tenantID string, managedCap model.ResourceSpec) (model.TenantBillingSummary, error) {
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
		accrueTenantBilling(billing, now)
		billing.ManagedCap = normalizedCap
		billing.UpdatedAt = now
		appendTenantBillingEvent(state, model.TenantBillingEvent{
			ID:                     model.NewID("billingevt"),
			TenantID:               tenantID,
			Type:                   model.BillingEventTypeConfigUpdated,
			AmountMicroCents:       0,
			BalanceAfterMicroCents: billing.BalanceMicroCents,
			Metadata: map[string]string{
				"cpu_millicores":   strconv.FormatInt(normalizedCap.CPUMilliCores, 10),
				"memory_mebibytes": strconv.FormatInt(normalizedCap.MemoryMebibytes, 10),
			},
			CreatedAt: now,
		})
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
		accrueTenantBilling(billing, now)
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

func normalizeAppSpecResources(spec *model.AppSpec) error {
	if spec == nil {
		return ErrInvalidInput
	}
	resources, err := normalizeWorkloadResources(spec.Resources, model.DefaultManagedAppResources())
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

func normalizeBillingCap(cap model.ResourceSpec) (model.ResourceSpec, error) {
	if cap.CPUMilliCores < 0 || cap.MemoryMebibytes < 0 {
		return model.ResourceSpec{}, ErrInvalidInput
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
		record.BalanceMicroCents == 0
}

func backfillLegacyTenantBillingRecord(record *model.TenantBilling, now time.Time) {
	if record == nil {
		return
	}
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
	if record == nil {
		return
	}
	normalizeTenantBillingRecord(record, now)
	if !now.After(record.LastAccruedAt) {
		return
	}
	hourlyRate := billingHourlyRateMicroCents(*record)
	if hourlyRate > 0 {
		record.BalanceMicroCents -= hourlyRate * now.Sub(record.LastAccruedAt).Nanoseconds() / int64(time.Hour)
	}
	record.LastAccruedAt = now
	record.UpdatedAt = now
}

func billingHourlyRateMicroCents(record model.TenantBilling) int64 {
	priceBook := normalizeBillingPriceBook(record.PriceBook)
	return record.ManagedCap.CPUMilliCores*priceBook.CPUMicroCentsPerMilliCoreHour +
		record.ManagedCap.MemoryMebibytes*priceBook.MemoryMicroCentsPerMiBHour
}

func billingMonthlyEstimateMicroCents(record model.TenantBilling) int64 {
	priceBook := normalizeBillingPriceBook(record.PriceBook)
	return billingHourlyRateMicroCents(record) * priceBook.HoursPerMonth
}

func billingBalanceRestricted(record model.TenantBilling) bool {
	return billingHourlyRateMicroCents(record) > 0 && record.BalanceMicroCents <= 0
}

func billingRunwayHours(record model.TenantBilling) *float64 {
	hourlyRate := billingHourlyRateMicroCents(record)
	if hourlyRate <= 0 || record.BalanceMicroCents <= 0 {
		return nil
	}
	hours := float64(record.BalanceMicroCents) / float64(hourlyRate)
	return &hours
}

func buildTenantBillingSummary(state *model.State, record model.TenantBilling) model.TenantBillingSummary {
	committed := tenantManagedCommittedResources(state, record.TenantID)
	available := clampResourceSpecSub(record.ManagedCap, committed)
	billingActive := billingHourlyRateMicroCents(record) > 0
	overCap := billingActive && resourceSpecExceeds(committed, record.ManagedCap)
	balanceRestricted := billingBalanceRestricted(record)
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
		DefaultAppResources:       model.DefaultManagedAppResources(),
		DefaultPostgresResources:  model.DefaultManagedPostgresResources(),
		PriceBook:                 normalizeBillingPriceBook(record.PriceBook),
		HourlyRateMicroCents:      billingHourlyRateMicroCents(record),
		MonthlyEstimateMicroCents: billingMonthlyEstimateMicroCents(record),
		BalanceMicroCents:         record.BalanceMicroCents,
		RunwayHours:               billingRunwayHours(record),
		LastAccruedAt:             record.LastAccruedAt,
		UpdatedAt:                 record.UpdatedAt,
		Events:                    events,
	}
}

func tenantBillingStatus(record model.TenantBilling, committed model.ResourceSpec) (string, string) {
	switch {
	case billingHourlyRateMicroCents(record) <= 0:
		return model.BillingStatusInactive, "Managed billing is inactive. External-owned runtimes remain free."
	case resourceSpecExceeds(committed, record.ManagedCap):
		return model.BillingStatusOverCap, "Current live managed capacity exceeds the configured envelope. Scale down, migrate to BYO, or raise the cap."
	case billingBalanceRestricted(record):
		return model.BillingStatusRestricted, "Balance is depleted. Top up before increasing managed capacity."
	default:
		return model.BillingStatusActive, "Managed capacity is metered hourly from the configured envelope. BYO VPS stays free."
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

func tenantManagedCommittedResources(state *model.State, tenantID string) model.ResourceSpec {
	total := model.ResourceSpec{}
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

func appManagedBundleCommitment(state *model.State, app model.App, runtimeID string, replicas int) model.ResourceSpec {
	if replicas <= 0 || !isBillableManagedRuntimeType(runtimeTypeForState(state, runtimeID)) {
		return model.ResourceSpec{}
	}
	total := multiplyResourceSpec(appEffectiveResources(app.Spec), int64(replicas))
	services := boundManagedServicesForApp(state, app.ID)
	for _, service := range services {
		total = addResourceSpec(total, backingServiceResources(service))
	}
	if len(services) == 0 && app.Spec.Postgres != nil {
		total = addResourceSpec(total, postgresEffectiveResources(*app.Spec.Postgres))
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

func backingServiceResources(service model.BackingService) model.ResourceSpec {
	if service.Spec.Postgres == nil {
		return model.ResourceSpec{}
	}
	return postgresEffectiveResources(*service.Spec.Postgres)
}

func appEffectiveResources(spec model.AppSpec) model.ResourceSpec {
	if spec.Resources != nil {
		return *spec.Resources
	}
	return model.DefaultManagedAppResources()
}

func postgresEffectiveResources(spec model.AppPostgresSpec) model.ResourceSpec {
	if spec.Resources != nil {
		return *spec.Resources
	}
	return model.DefaultManagedPostgresResources()
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

func validateManagedOperationBilling(state *model.State, record model.TenantBilling, app model.App, op model.Operation) error {
	if billingHourlyRateMicroCents(record) <= 0 {
		return nil
	}
	currentTotal := tenantManagedCommittedResources(state, app.TenantID)
	currentBundle, nextBundle, err := projectedAppManagedBundleCommitment(state, app, op)
	if err != nil {
		return err
	}
	nextTotal := addResourceSpec(subtractResourceSpec(currentTotal, currentBundle), nextBundle)
	currentlyOverCap := resourceSpecExceeds(currentTotal, record.ManagedCap)
	nextOverCap := resourceSpecExceeds(nextTotal, record.ManagedCap)
	if nextOverCap && (!currentlyOverCap || resourceSpecExceeds(nextTotal, currentTotal)) {
		return describeBillingCapExceeded(record, nextTotal)
	}
	if billingBalanceRestricted(record) && resourceSpecExceeds(nextTotal, currentTotal) {
		return describeBillingBalanceDepleted(record)
	}
	return nil
}

func projectedAppManagedBundleCommitment(state *model.State, app model.App, op model.Operation) (model.ResourceSpec, model.ResourceSpec, error) {
	current := appManagedBundleCommitment(state, app, app.Status.CurrentRuntimeID, app.Status.CurrentReplicas)
	projection := cloneBillingProjectionState(state, app)
	opCopy := op
	opCopy.DesiredSpec = cloneAppSpec(op.DesiredSpec)
	opCopy.DesiredSource = cloneAppSource(op.DesiredSource)
	if strings.TrimSpace(opCopy.ID) == "" {
		opCopy.ID = "billing-projection"
	}
	if err := applyOperationToApp(&projection, &opCopy); err != nil {
		return model.ResourceSpec{}, model.ResourceSpec{}, err
	}
	if len(projection.Apps) == 0 {
		return current, model.ResourceSpec{}, nil
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

func addResourceSpec(left, right model.ResourceSpec) model.ResourceSpec {
	return model.ResourceSpec{
		CPUMilliCores:   left.CPUMilliCores + right.CPUMilliCores,
		MemoryMebibytes: left.MemoryMebibytes + right.MemoryMebibytes,
	}
}

func subtractResourceSpec(left, right model.ResourceSpec) model.ResourceSpec {
	return model.ResourceSpec{
		CPUMilliCores:   maxInt64(0, left.CPUMilliCores-right.CPUMilliCores),
		MemoryMebibytes: maxInt64(0, left.MemoryMebibytes-right.MemoryMebibytes),
	}
}

func clampResourceSpecSub(left, right model.ResourceSpec) model.ResourceSpec {
	return subtractResourceSpec(left, right)
}

func multiplyResourceSpec(spec model.ResourceSpec, factor int64) model.ResourceSpec {
	if factor <= 0 {
		return model.ResourceSpec{}
	}
	return model.ResourceSpec{
		CPUMilliCores:   spec.CPUMilliCores * factor,
		MemoryMebibytes: spec.MemoryMebibytes * factor,
	}
}

func resourceSpecExceeds(left, right model.ResourceSpec) bool {
	return left.CPUMilliCores > right.CPUMilliCores || left.MemoryMebibytes > right.MemoryMebibytes
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func describeBillingCapExceeded(record model.TenantBilling, nextTotal model.ResourceSpec) error {
	return fmt.Errorf(
		"%w: requested managed capacity cpu=%dm/%dm memory=%dMi/%dMi",
		ErrBillingCapExceeded,
		nextTotal.CPUMilliCores,
		record.ManagedCap.CPUMilliCores,
		nextTotal.MemoryMebibytes,
		record.ManagedCap.MemoryMebibytes,
	)
}

func describeBillingBalanceDepleted(record model.TenantBilling) error {
	hourlyRateMicroCents := billingHourlyRateMicroCents(record)
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
