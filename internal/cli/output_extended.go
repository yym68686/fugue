package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"
)

func formatBytes(value int64) string {
	if value <= 0 {
		return "0 B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	current := float64(value)
	index := 0
	for current >= 1024 && index < len(units)-1 {
		current /= 1024
		index++
	}
	if index == 0 {
		return fmt.Sprintf("%d %s", value, units[index])
	}
	return fmt.Sprintf("%.1f %s", current, units[index])
}

func formatOptionalTimePtr(value *time.Time) string {
	if value == nil {
		return ""
	}
	return formatTime(*value)
}

func formatScopes(scopes []string) string {
	if len(scopes) == 0 {
		return ""
	}
	joined := strings.Join(scopes, ",")
	if len(joined) > 48 {
		return joined[:45] + "..."
	}
	return joined
}

func formatResourceUsageSummary(usage *model.ResourceUsage) string {
	if usage == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if usage.CPUMilliCores != nil {
		parts = append(parts, fmt.Sprintf("%dm CPU", *usage.CPUMilliCores))
	}
	if usage.MemoryBytes != nil {
		parts = append(parts, fmt.Sprintf("%s RAM", formatBytes(*usage.MemoryBytes)))
	}
	if usage.EphemeralStorageBytes != nil {
		parts = append(parts, fmt.Sprintf("%s eph", formatBytes(*usage.EphemeralStorageBytes)))
	}
	return strings.Join(parts, " / ")
}

func formatBillingResourceSpec(spec model.BillingResourceSpec) string {
	parts := make([]string, 0, 3)
	if spec.CPUMilliCores > 0 {
		parts = append(parts, fmt.Sprintf("%dm CPU", spec.CPUMilliCores))
	}
	if spec.MemoryMebibytes > 0 {
		parts = append(parts, fmt.Sprintf("%d MiB", spec.MemoryMebibytes))
	}
	if spec.StorageGibibytes > 0 {
		parts = append(parts, fmt.Sprintf("%d GiB", spec.StorageGibibytes))
	}
	return strings.Join(parts, " / ")
}

func formatCurrencyMicroCents(value int64, currency string) string {
	if strings.TrimSpace(currency) == "" {
		currency = "USD"
	}
	return fmt.Sprintf("%s %.2f", currency, float64(value)/1_000_000)
}

func writeProjectTable(w io.Writer, projects []model.Project) error {
	return writeProjectTableWithContext(w, projects, nil, false)
}

func writeProjectTableWithContext(w io.Writer, projects []model.Project, tenantNames map[string]string, showIDs bool) error {
	sorted := append([]model.Project(nil), projects...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PROJECT\tTENANT\tDESCRIPTION\tUPDATED"); err != nil {
		return err
	}
	for _, project := range sorted {
		tenantName := firstNonEmptyTrimmed(tenantNames[project.TenantID], project.TenantID)
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			formatDisplayName(project.Name, project.ID, showIDs),
			formatDisplayName(tenantName, project.TenantID, showIDs),
			project.Description,
			formatTime(project.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeProjectRuntimeReservationTable(w io.Writer, reservations []model.ProjectRuntimeReservation, runtimeNames map[string]string, showIDs bool) error {
	sorted := append([]model.ProjectRuntimeReservation(nil), reservations...)
	sortProjectRuntimeReservationsForOutput(sorted)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RUNTIME\tMODE\tUPDATED"); err != nil {
		return err
	}
	for _, reservation := range sorted {
		runtimeName := firstNonEmptyTrimmed(runtimeNames[reservation.RuntimeID], reservation.RuntimeID)
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\n",
			formatDisplayName(runtimeName, reservation.RuntimeID, showIDs),
			firstNonEmptyTrimmed(reservation.Mode, model.ProjectRuntimeReservationModeExclusive),
			formatTime(reservation.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func sortProjectRuntimeReservationsForOutput(reservations []model.ProjectRuntimeReservation) {
	sort.Slice(reservations, func(i, j int) bool {
		if reservations[i].CreatedAt.Equal(reservations[j].CreatedAt) {
			return reservations[i].RuntimeID < reservations[j].RuntimeID
		}
		return reservations[i].CreatedAt.Before(reservations[j].CreatedAt)
	})
}

func writeServiceTable(w io.Writer, services []model.BackingService) error {
	return writeServiceTableWithContext(w, services, nil, nil, nil, false)
}

func writeServiceTableWithContext(w io.Writer, services []model.BackingService, projectNames, appNames, runtimeNames map[string]string, showIDs bool) error {
	sorted := append([]model.BackingService(nil), services...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SERVICE\tTYPE\tSTATUS\tPROJECT\tOWNER\tRUNTIME\tUSAGE"); err != nil {
		return err
	}
	for _, service := range sorted {
		runtimeID := ""
		if service.Spec.Postgres != nil {
			runtimeID = strings.TrimSpace(service.Spec.Postgres.RuntimeID)
		}
		projectName := firstNonEmptyTrimmed(projectNames[service.ProjectID], service.ProjectID)
		ownerName := firstNonEmptyTrimmed(appNames[service.OwnerAppID], service.OwnerAppID)
		runtimeName := firstNonEmptyTrimmed(runtimeNames[runtimeID], runtimeID)
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			formatDisplayName(service.Name, service.ID, showIDs),
			service.Type,
			service.Status,
			formatDisplayName(projectName, service.ProjectID, showIDs),
			formatDisplayName(ownerName, service.OwnerAppID, showIDs),
			formatDisplayName(runtimeName, runtimeID, showIDs),
			formatResourceUsageSummary(service.CurrentResourceUsage),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeBindingTable(w io.Writer, bindings []model.ServiceBinding, services []model.BackingService) error {
	serviceNames := make(map[string]string, len(services))
	serviceTypes := make(map[string]string, len(services))
	for _, service := range services {
		serviceNames[service.ID] = service.Name
		serviceTypes[service.ID] = service.Type
	}
	sorted := append([]model.ServiceBinding(nil), bindings...)
	sort.Slice(sorted, func(i, j int) bool {
		leftName := firstNonEmpty(serviceNames[sorted[i].ServiceID], sorted[i].ServiceID)
		rightName := firstNonEmpty(serviceNames[sorted[j].ServiceID], sorted[j].ServiceID)
		if compare := strings.Compare(leftName, rightName); compare != 0 {
			return compare < 0
		}
		if compare := strings.Compare(sorted[i].Alias, sorted[j].Alias); compare != 0 {
			return compare < 0
		}
		return strings.Compare(sorted[i].ID, sorted[j].ID) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "BINDING\tSERVICE\tTYPE\tALIAS\tENV\tUPDATED"); err != nil {
		return err
	}
	for _, binding := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%d\t%s\n",
			binding.ID,
			firstNonEmpty(serviceNames[binding.ServiceID], binding.ServiceID),
			serviceTypes[binding.ServiceID],
			binding.Alias,
			len(binding.Env),
			formatTime(binding.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeRuntimeAccessGrantTable(w io.Writer, grants []model.RuntimeAccessGrant, tenantNames map[string]string) error {
	sorted := append([]model.RuntimeAccessGrant(nil), grants...)
	sort.Slice(sorted, func(i, j int) bool {
		leftTenant := firstNonEmpty(tenantNames[sorted[i].TenantID], sorted[i].TenantID)
		rightTenant := firstNonEmpty(tenantNames[sorted[j].TenantID], sorted[j].TenantID)
		if compare := strings.Compare(leftTenant, rightTenant); compare != 0 {
			return compare < 0
		}
		return strings.Compare(sorted[i].TenantID, sorted[j].TenantID) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TENANT\tTENANT_ID\tUPDATED"); err != nil {
		return err
	}
	for _, grant := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\n",
			firstNonEmpty(tenantNames[grant.TenantID], grant.TenantID),
			grant.TenantID,
			formatTime(grant.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAppImageTable(w io.Writer, versions []appImageVersion) error {
	sorted := append([]appImageVersion(nil), versions...)
	sort.Slice(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		if left.Current != right.Current {
			return left.Current
		}
		leftTime := formatOptionalTimePtr(left.LastDeployedAt)
		rightTime := formatOptionalTimePtr(right.LastDeployedAt)
		return strings.Compare(rightTime, leftTime) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "IMAGE\tCURRENT\tSTATUS\tSIZE\tRECLAIMABLE\tLAST_DEPLOYED"); err != nil {
		return err
	}
	for _, version := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%t\t%s\t%s\t%s\t%s\n",
			version.ImageRef,
			version.Current,
			version.Status,
			formatBytes(version.SizeBytes),
			formatBytes(version.ReclaimableSizeBytes),
			formatOptionalTimePtr(version.LastDeployedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeProjectUsageTable(w io.Writer, projects []projectImageUsageSummary) error {
	return writeProjectUsageTableWithContext(w, projects, nil, false)
}

func writeProjectUsageTableWithContext(w io.Writer, projects []projectImageUsageSummary, projectNames map[string]string, showIDs bool) error {
	sorted := append([]projectImageUsageSummary(nil), projects...)
	sort.Slice(sorted, func(i, j int) bool {
		left := firstNonEmptyTrimmed(projectNames[sorted[i].ProjectID], sorted[i].ProjectID)
		right := firstNonEmptyTrimmed(projectNames[sorted[j].ProjectID], sorted[j].ProjectID)
		if compare := strings.Compare(left, right); compare != 0 {
			return compare < 0
		}
		return strings.Compare(sorted[i].ProjectID, sorted[j].ProjectID) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PROJECT\tVERSIONS\tCURRENT\tSTALE\tRECLAIMABLE"); err != nil {
		return err
	}
	for _, project := range sorted {
		projectName := firstNonEmptyTrimmed(projectNames[project.ProjectID], project.ProjectID)
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%d\t%d\t%d\t%s\n",
			formatDisplayName(projectName, project.ProjectID, showIDs),
			project.VersionCount,
			project.CurrentVersionCount,
			project.StaleVersionCount,
			formatBytes(project.ReclaimableSizeBytes),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeProjectUsageAppsTable(w io.Writer, apps []projectImageUsageAppSummary) error {
	sorted := append([]projectImageUsageAppSummary(nil), apps...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].AppName, sorted[j].AppName) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "APP\tVERSIONS\tCURRENT\tSTALE\tRECLAIMABLE"); err != nil {
		return err
	}
	for _, app := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%d\t%d\t%d\t%s\n",
			app.AppName,
			app.VersionCount,
			app.CurrentVersionCount,
			app.StaleVersionCount,
			formatBytes(app.ReclaimableSizeBytes),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeOperationTable(w io.Writer, operations []model.Operation) error {
	sorted := append([]model.Operation(nil), operations...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "OPERATION\tSTATUS\tTYPE\tAPP\tTARGET\tUPDATED"); err != nil {
		return err
	}
	for _, op := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			op.ID,
			op.Status,
			op.Type,
			op.AppID,
			firstNonEmpty(op.TargetRuntimeID, op.AssignedRuntimeID),
			formatTime(op.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAuditEventTable(w io.Writer, events []model.AuditEvent) error {
	sorted := append([]model.AuditEvent(nil), events...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TIME\tACTION\tTARGET\tACTOR"); err != nil {
		return err
	}
	for _, event := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s:%s\t%s:%s\n",
			formatTime(event.CreatedAt),
			event.Action,
			event.TargetType,
			event.TargetID,
			event.ActorType,
			event.ActorID,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAPIKeyTable(w io.Writer, keys []model.APIKey) error {
	sorted := append([]model.APIKey(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Label, sorted[j].Label) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "KEY\tLABEL\tSTATUS\tSCOPES\tLAST_USED"); err != nil {
		return err
	}
	for _, key := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			key.Prefix,
			key.Label,
			key.Status,
			formatScopes(key.Scopes),
			formatOptionalTimePtr(key.LastUsedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeNodeKeyTable(w io.Writer, keys []model.NodeKey) error {
	sorted := append([]model.NodeKey(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Label, sorted[j].Label) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "KEY\tLABEL\tSTATUS\tLAST_USED"); err != nil {
		return err
	}
	for _, key := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			key.Prefix,
			key.Label,
			key.Status,
			formatOptionalTimePtr(key.LastUsedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeRuntimeTable(w io.Writer, runtimes []model.Runtime) error {
	return writeRuntimeTableWithContext(w, runtimes, nil, false)
}

func writeRuntimeTableWithContext(w io.Writer, runtimes []model.Runtime, tenantNames map[string]string, showIDs bool) error {
	sorted := append([]model.Runtime(nil), runtimes...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RUNTIME\tTYPE\tACCESS\tPOOL\tSTATUS\tTENANT\tENDPOINT"); err != nil {
		return err
	}
	for _, runtime := range sorted {
		tenantName := firstNonEmptyTrimmed(tenantNames[runtime.TenantID], runtime.TenantID)
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			formatDisplayName(runtime.Name, runtime.ID, showIDs),
			runtime.Type,
			runtime.AccessMode,
			runtime.PoolMode,
			runtime.Status,
			formatDisplayName(tenantName, runtime.TenantID, showIDs),
			runtime.Endpoint,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeRuntimeGrantTable(w io.Writer, grants []model.RuntimeAccessGrant) error {
	sorted := append([]model.RuntimeAccessGrant(nil), grants...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].TenantID, sorted[j].TenantID) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TENANT\tCREATED\tUPDATED"); err != nil {
		return err
	}
	for _, grant := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", grant.TenantID, formatTime(grant.CreatedAt), formatTime(grant.UpdatedAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeEnrollmentTokenTable(w io.Writer, tokens []model.EnrollmentToken) error {
	sorted := append([]model.EnrollmentToken(nil), tokens...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TOKEN\tLABEL\tEXPIRES\tUSED"); err != nil {
		return err
	}
	for _, token := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			token.Prefix,
			token.Label,
			formatTime(token.ExpiresAt),
			formatOptionalTimePtr(token.UsedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeClusterNodeTable(w io.Writer, nodes []model.ClusterNode) error {
	sorted := append([]model.ClusterNode(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NODE\tSTATUS\tRUNTIME\tROLES\tREGION\tBUILD\tSHARED\tMODE\tCP\tCPU\tCPU_REQ\tMEMORY\tMEM_REQ"); err != nil {
		return err
	}
	for _, node := range sorted {
		cpu := ""
		if node.CPU != nil && node.CPU.UsagePercent != nil {
			cpu = fmt.Sprintf("%.0f%%", *node.CPU.UsagePercent)
		}
		cpuRequest := ""
		if node.CPU != nil && node.CPU.RequestPercent != nil {
			cpuRequest = fmt.Sprintf("%.0f%%", *node.CPU.RequestPercent)
		}
		memory := ""
		if node.Memory != nil && node.Memory.UsagePercent != nil {
			memory = fmt.Sprintf("%.0f%%", *node.Memory.UsagePercent)
		}
		memoryRequest := ""
		if node.Memory != nil && node.Memory.RequestPercent != nil {
			memoryRequest = fmt.Sprintf("%.0f%%", *node.Memory.RequestPercent)
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			node.Name,
			firstNonEmpty(node.Status, "-"),
			firstNonEmpty(node.RuntimeID, "-"),
			firstNonEmpty(strings.Join(node.Roles, ","), "-"),
			firstNonEmpty(node.Region, node.Zone, "-"),
			firstNonEmpty(formatClusterNodePolicyToggle(node.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowBuilds }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveBuilds }), "-"),
			firstNonEmpty(formatClusterNodePolicyToggle(node.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowSharedPool }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveSharedPool }), "-"),
			firstNonEmpty(strings.TrimSpace(clusterNodePolicyMode(node.Policy)), "-"),
			firstNonEmpty(strings.TrimSpace(clusterNodePolicyControlPlane(node.Policy)), "-"),
			firstNonEmpty(cpu, "-"),
			firstNonEmpty(cpuRequest, "-"),
			firstNonEmpty(memory, "-"),
			firstNonEmpty(memoryRequest, "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeClusterNodePolicyStatusTable(w io.Writer, statuses []model.ClusterNodePolicyStatus) error {
	sorted := append([]model.ClusterNodePolicyStatus(nil), statuses...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].NodeName, sorted[j].NodeName) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NODE\tRUNTIME\tAPP\tBUILD\tSHARED\tEDGE\tDNS\tINTERNAL\tCP\tREADY\tDISK\tHEALTH\tRECONCILED\tBLOCK\tGATE\tREASONS"); err != nil {
		return err
	}
	for _, status := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			status.NodeName,
			firstNonEmpty(status.RuntimeID, "-"),
			firstNonEmpty(formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowAppRuntime }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveAppRuntime }), "-"),
			firstNonEmpty(formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowBuilds }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveBuilds }), "-"),
			firstNonEmpty(formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowSharedPool }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveSharedPool }), "-"),
			firstNonEmpty(formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowEdge }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveEdge }), "-"),
			firstNonEmpty(formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowDNS }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveDNS }), "-"),
			firstNonEmpty(formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowInternalMaintenance }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveInternalMaintenance }), "-"),
			firstNonEmpty(clusterNodePolicyControlPlane(status.Policy), "-"),
			clusterNodeBoolLabel(status.Ready),
			clusterNodeBoolLabel(status.DiskPressure),
			firstNonEmpty(clusterNodePolicyHealth(status), "-"),
			clusterNodeBoolLabel(status.Reconciled),
			clusterNodeBoolLabel(status.BlockRollout),
			firstNonEmpty(status.GateReason, "-"),
			firstNonEmpty(strings.Join(status.ReconcileReasons, "; "), "-"),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeClusterNodePolicyDetails(w io.Writer, status model.ClusterNodePolicyStatus) error {
	if err := writeKeyValues(w,
		kvPair{Key: "node", Value: status.NodeName},
		kvPair{Key: "runtime", Value: firstNonEmpty(status.RuntimeID, "-")},
		kvPair{Key: "tenant", Value: firstNonEmpty(status.TenantID, "-")},
		kvPair{Key: "machine", Value: firstNonEmpty(status.MachineID, "-")},
		kvPair{Key: "ready", Value: clusterNodeBoolLabel(status.Ready)},
		kvPair{Key: "disk_pressure", Value: clusterNodeBoolLabel(status.DiskPressure)},
		kvPair{Key: "node_schedulable", Value: clusterNodeBoolLabel(status.NodeSchedulable)},
		kvPair{Key: "reconciled", Value: clusterNodeBoolLabel(status.Reconciled)},
		kvPair{Key: "block_rollout", Value: clusterNodeBoolLabel(status.BlockRollout)},
		kvPair{Key: "gate_reason", Value: firstNonEmpty(status.GateReason, "-")},
		kvPair{Key: "suggested_fix_command", Value: firstNonEmpty(status.SuggestedFixCommand, "-")},
		kvPair{Key: "reconcile_reasons", Value: firstNonEmpty(strings.Join(status.ReconcileReasons, "; "), "-")},
	); err != nil {
		return err
	}
	if status.Policy != nil {
		if _, err := fmt.Fprintln(w, "\n[policy]"); err != nil {
			return err
		}
		if err := writeKeyValues(w,
			kvPair{Key: "app_runtime", Value: formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowAppRuntime }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveAppRuntime })},
			kvPair{Key: "builds", Value: formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowBuilds }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveBuilds })},
			kvPair{Key: "shared_pool", Value: formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowSharedPool }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveSharedPool })},
			kvPair{Key: "edge", Value: formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowEdge }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveEdge })},
			kvPair{Key: "dns", Value: formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowDNS }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveDNS })},
			kvPair{Key: "internal_maintenance", Value: formatClusterNodePolicyToggle(status.Policy, func(policy *model.ClusterNodePolicy) bool { return policy.AllowInternalMaintenance }, func(policy *model.ClusterNodePolicy) bool { return policy.EffectiveInternalMaintenance })},
			kvPair{Key: "control_plane", Value: clusterNodePolicyControlPlane(status.Policy)},
			kvPair{Key: "node_mode", Value: firstNonEmpty(status.Policy.NodeMode, "-")},
			kvPair{Key: "node_health", Value: firstNonEmpty(status.Policy.NodeHealth, "-")},
		); err != nil {
			return err
		}
	}
	if len(status.Labels) > 0 {
		if _, err := fmt.Fprintln(w, "\n[labels]"); err != nil {
			return err
		}
		if err := writeStringMap(w, status.Labels); err != nil {
			return err
		}
	}
	if len(status.Taints) > 0 {
		if _, err := fmt.Fprintln(w, "\n[taints]"); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(tw, "KEY\tVALUE\tEFFECT"); err != nil {
			return err
		}
		for _, taint := range status.Taints {
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", taint.Key, firstNonEmpty(taint.Value, "-"), firstNonEmpty(taint.Effect, "-")); err != nil {
				return err
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func formatClusterNodePolicyToggle(policy *model.ClusterNodePolicy, desired func(*model.ClusterNodePolicy) bool, effective func(*model.ClusterNodePolicy) bool) string {
	if policy == nil || desired == nil || effective == nil {
		return ""
	}
	return clusterNodeToggleLabel(desired(policy)) + "/" + clusterNodeToggleLabel(effective(policy))
}

func clusterNodeToggleLabel(value bool) string {
	if value {
		return "on"
	}
	return "off"
}

func clusterNodePolicyMode(policy *model.ClusterNodePolicy) string {
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.NodeMode)
}

func clusterNodePolicyControlPlane(policy *model.ClusterNodePolicy) string {
	if policy == nil {
		return ""
	}
	desired := firstNonEmpty(strings.TrimSpace(policy.DesiredControlPlaneRole), "none")
	effective := firstNonEmpty(strings.TrimSpace(policy.EffectiveControlPlaneRole), "none")
	if desired == effective {
		return desired
	}
	return desired + "/" + effective
}

func clusterNodeBoolLabel(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func clusterNodePolicyHealth(status model.ClusterNodePolicyStatus) string {
	if status.Policy != nil && strings.TrimSpace(status.Policy.NodeHealth) != "" {
		return strings.TrimSpace(status.Policy.NodeHealth)
	}
	if status.NodeSchedulable {
		return "ready"
	}
	return "blocked"
}

func writeControlPlaneComponentTable(w io.Writer, components []model.ControlPlaneComponent) error {
	sorted := append([]model.ControlPlaneComponent(nil), components...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Component, sorted[j].Component) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "COMPONENT\tSTATUS\tDESIRED_IMAGE\tLIVE_TAGS\tREADY\tUPDATED"); err != nil {
		return err
	}
	for _, component := range sorted {
		liveTags := "-"
		if len(component.ObservedImageTags) > 0 {
			liveTags = strings.Join(component.ObservedImageTags, ",")
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%d/%d\t%d\n",
			component.Component,
			component.Status,
			component.Image,
			liveTags,
			component.ReadyReplicas,
			component.DesiredReplicas,
			component.UpdatedReplicas,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeTenantTable(w io.Writer, tenants []model.Tenant) error {
	sorted := append([]model.Tenant(nil), tenants...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TENANT\tSLUG\tSTATUS\tUPDATED"); err != nil {
		return err
	}
	for _, tenant := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", tenant.Name, tenant.Slug, tenant.Status, formatTime(tenant.UpdatedAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeTemplateServiceTable(w io.Writer, services []inspectGitHubTemplateManifestService) error {
	sorted := append([]inspectGitHubTemplateManifestService(nil), services...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Service, sorted[j].Service) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SERVICE\tTYPE\tKIND\tBACKING\tBUILD\tPORT\tPUBLISHED\tSOURCE\tBINDINGS\tSEED_FILES"); err != nil {
		return err
	}
	for _, service := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%t\t%s\t%d\t%t\t%s\t%s\t%d\n",
			service.Service,
			firstNonEmpty(service.ServiceType, service.Kind),
			service.Kind,
			service.BackingService,
			service.BuildStrategy,
			service.InternalPort,
			service.Published,
			firstNonEmpty(service.SourceDir, service.BuildContextDir),
			strings.Join(service.BindingTargets, ","),
			len(service.PersistentStorageSeedFiles),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeTemplateVariableTable(w io.Writer, variables []templateVariable) error {
	sorted := append([]templateVariable(nil), variables...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Key, sorted[j].Key) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "KEY\tREQUIRED\tSECRET\tDEFAULT\tLABEL"); err != nil {
		return err
	}
	for _, variable := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%t\t%t\t%s\t%s\n",
			variable.Key,
			variable.Required,
			variable.Secret,
			variable.DefaultValue,
			firstNonEmpty(variable.Label, variable.Description),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func hasTemplateSeedFiles(services []inspectGitHubTemplateManifestService) bool {
	for _, service := range services {
		if len(service.PersistentStorageSeedFiles) > 0 {
			return true
		}
	}
	return false
}

func writeTemplateSeedFileTable(w io.Writer, services []inspectGitHubTemplateManifestService) error {
	type row struct {
		Service string
		Path    string
		Mode    int32
		Seeded  bool
	}
	rows := make([]row, 0)
	for _, service := range services {
		for _, file := range service.PersistentStorageSeedFiles {
			rows = append(rows, row{
				Service: service.Service,
				Path:    file.Path,
				Mode:    file.Mode,
				Seeded:  strings.TrimSpace(file.SeedContent) != "",
			})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Service == rows[j].Service {
			return strings.Compare(rows[i].Path, rows[j].Path) < 0
		}
		return strings.Compare(rows[i].Service, rows[j].Service) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SERVICE\tPATH\tMODE\tSEEDED"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%t\n", row.Service, row.Path, formatFileMode(row.Mode), row.Seeded); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeTemplateInferenceTable(w io.Writer, report []templateTopologyInference) error {
	sorted := append([]templateTopologyInference(nil), report...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Service == sorted[j].Service {
			if sorted[i].Category == sorted[j].Category {
				return strings.Compare(sorted[i].Message, sorted[j].Message) < 0
			}
			return strings.Compare(sorted[i].Category, sorted[j].Category) < 0
		}
		return strings.Compare(sorted[i].Service, sorted[j].Service) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "LEVEL\tCATEGORY\tSERVICE\tMESSAGE"); err != nil {
		return err
	}
	for _, inference := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			inference.Level,
			inference.Category,
			inference.Service,
			inference.Message,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeBillingEventsTable(w io.Writer, events []model.TenantBillingEvent, currency string) error {
	sorted := append([]model.TenantBillingEvent(nil), events...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.After(sorted[j].CreatedAt)
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TIME\tTYPE\tAMOUNT\tBALANCE"); err != nil {
		return err
	}
	for _, event := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			formatTime(event.CreatedAt),
			event.Type,
			formatCurrencyMicroCents(event.AmountMicroCents, currency),
			formatCurrencyMicroCents(event.BalanceAfterMicroCents, currency),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}
