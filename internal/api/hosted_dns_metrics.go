package api

import (
	"io"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
)

func (s *Server) writeHostedDNSMetrics(w io.Writer) {
	now := time.Now().UTC()
	zones, err := s.store.ListHostedZones("", true)
	if err != nil {
		observability.WriteGaugeMetric(w, "fugue_hosted_dns_metrics_error", "Whether hosted DNS metrics collection failed.", map[string]string{"stage": "zones", "error": truncateMetricLabel(err.Error(), 160)}, 1)
		return
	}

	zoneCounts := map[string]int{
		model.HostedZoneStatusActive:            0,
		model.HostedZoneStatusDegraded:          0,
		model.HostedZoneStatusPendingDelegation: 0,
		model.HostedZoneStatusSuspended:         0,
	}
	recordCounts := map[string]int{}
	recordSourceCounts := map[string]int{}
	maxPublishLagSeconds := 0.0

	for _, zone := range zones {
		if zone.Status == model.HostedZoneStatusDeleted {
			continue
		}
		zoneCounts[model.NormalizeHostedZoneStatus(zone.Status)]++

		records, err := s.store.ListDNSRecords(zone.ID)
		if err != nil {
			observability.WriteGaugeMetric(w, "fugue_hosted_dns_metrics_error", "Whether hosted DNS metrics collection failed.", map[string]string{"stage": "records", "zone": zone.ZoneName, "error": truncateMetricLabel(err.Error(), 160)}, 1)
			continue
		}
		for _, record := range records {
			status := model.NormalizeDNSRecordStatus(record.Status)
			source := model.NormalizeDNSRecordSource(record.Source)
			recordCounts[status]++
			recordSourceCounts[source]++

			publishedAt := record.UpdatedAt
			if record.LastPublishedAt != nil {
				publishedAt = *record.LastPublishedAt
			}
			if !publishedAt.IsZero() {
				if lag := now.Sub(publishedAt).Seconds(); lag > maxPublishLagSeconds {
					maxPublishLagSeconds = lag
				}
			}
		}
	}

	for status, count := range zoneCounts {
		observability.WriteGaugeMetric(w, "fugue_hosted_dns_zones", "Hosted DNS zones by status.", map[string]string{"status": status}, float64(count))
	}
	for status, count := range recordCounts {
		observability.WriteGaugeMetric(w, "fugue_hosted_dns_records", "Hosted DNS records by status.", map[string]string{"status": status}, float64(count))
	}
	for source, count := range recordSourceCounts {
		observability.WriteGaugeMetric(w, "fugue_hosted_dns_records_by_source", "Hosted DNS records by owner source.", map[string]string{"source": source}, float64(count))
	}
	observability.WriteGaugeMetric(w, "fugue_hosted_dns_record_publish_lag_seconds", "Maximum observed hosted DNS record publish lag based on last_published_at or updated_at.", nil, maxPublishLagSeconds)

	managedPending := s.countManagedAppDomainDNSPendingMetric(w)
	observability.WriteGaugeMetric(w, "fugue_app_domain_managed_dns_pending", "Managed app custom domains waiting for hosted DNS verification.", nil, float64(managedPending))
	observability.WriteGaugeMetric(w, "fugue_hosted_dns_metrics_error", "Whether hosted DNS metrics collection failed.", map[string]string{"stage": "all", "error": ""}, 0)
}

func (s *Server) countManagedAppDomainDNSPendingMetric(w io.Writer) int {
	apps, err := s.store.ListApps("", true)
	if err != nil {
		observability.WriteGaugeMetric(w, "fugue_hosted_dns_metrics_error", "Whether hosted DNS metrics collection failed.", map[string]string{"stage": "app_domains", "error": truncateMetricLabel(err.Error(), 160)}, 1)
		return 0
	}

	pending := 0
	for _, app := range apps {
		domains, err := s.store.ListAppDomains(app.ID)
		if err != nil {
			observability.WriteGaugeMetric(w, "fugue_hosted_dns_metrics_error", "Whether hosted DNS metrics collection failed.", map[string]string{"stage": "app_domains", "app_id": app.ID, "error": truncateMetricLabel(err.Error(), 160)}, 1)
			continue
		}
		for _, domain := range domains {
			if domain.DNSMode != model.AppDomainDNSModeManaged {
				continue
			}
			if domain.DNSStatus != model.AppDomainDNSStatusReady || domain.Status != model.AppDomainStatusVerified {
				pending++
			}
		}
	}
	return pending
}
