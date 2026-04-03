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
	sorted := append([]model.Project(nil), projects...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PROJECT\tDESCRIPTION\tUPDATED"); err != nil {
		return err
	}
	for _, project := range sorted {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", project.Name, project.Description, formatTime(project.UpdatedAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeServiceTable(w io.Writer, services []model.BackingService) error {
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
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			service.Name,
			service.Type,
			service.Status,
			service.ProjectID,
			service.OwnerAppID,
			runtimeID,
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
	sorted := append([]projectImageUsageSummary(nil), projects...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].ProjectID, sorted[j].ProjectID) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PROJECT\tVERSIONS\tCURRENT\tSTALE\tRECLAIMABLE"); err != nil {
		return err
	}
	for _, project := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%d\t%d\t%d\t%s\n",
			project.ProjectID,
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
	sorted := append([]model.Runtime(nil), runtimes...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Name, sorted[j].Name) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "RUNTIME\tTYPE\tACCESS\tPOOL\tSTATUS\tTENANT\tENDPOINT"); err != nil {
		return err
	}
	for _, runtime := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			runtime.Name,
			runtime.Type,
			runtime.AccessMode,
			runtime.PoolMode,
			runtime.Status,
			runtime.TenantID,
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
	if _, err := fmt.Fprintln(tw, "NODE\tSTATUS\tRUNTIME\tROLES\tREGION\tCPU\tMEMORY"); err != nil {
		return err
	}
	for _, node := range sorted {
		cpu := ""
		if node.CPU != nil && node.CPU.UsagePercent != nil {
			cpu = fmt.Sprintf("%.0f%%", *node.CPU.UsagePercent)
		}
		memory := ""
		if node.Memory != nil && node.Memory.UsagePercent != nil {
			memory = fmt.Sprintf("%.0f%%", *node.Memory.UsagePercent)
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			node.Name,
			node.Status,
			node.RuntimeID,
			strings.Join(node.Roles, ","),
			firstNonEmpty(node.Region, node.Zone),
			cpu,
			memory,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeControlPlaneComponentTable(w io.Writer, components []model.ControlPlaneComponent) error {
	sorted := append([]model.ControlPlaneComponent(nil), components...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Component, sorted[j].Component) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "COMPONENT\tSTATUS\tIMAGE\tREADY\tUPDATED"); err != nil {
		return err
	}
	for _, component := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%d/%d\t%d\n",
			component.Component,
			component.Status,
			component.Image,
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
	if _, err := fmt.Fprintln(tw, "SERVICE\tKIND\tBUILD\tPORT\tPUBLISHED\tSOURCE"); err != nil {
		return err
	}
	for _, service := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%d\t%t\t%s\n",
			service.Service,
			service.Kind,
			service.BuildStrategy,
			service.InternalPort,
			service.Published,
			firstNonEmpty(service.SourceDir, service.BuildContextDir),
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
