package model

import "time"

// ClusterResourceReport covers every nonterminal pod assigned to one node.
// Totals always cover the complete report, even when the CLI limits its rows.
type ClusterResourceReport struct {
	Node                 string                    `json:"node"`
	ObservedAt           time.Time                 `json:"observed_at"`
	WindowHours          int                       `json:"window_hours"`
	MinSamples           int                       `json:"min_samples"`
	TotalServices        int                       `json:"total_services"`
	TotalPods            int                       `json:"total_pods"`
	CPURequestMilliCores int64                     `json:"cpu_request_millicores"`
	MemoryRequestBytes   int64                     `json:"memory_request_bytes"`
	Warnings             []string                  `json:"warnings,omitempty"`
	Services             []ClusterServiceResources `json:"services"`
}

type ClusterServiceResources struct {
	Kind                     string                             `json:"kind"`
	ID                       string                             `json:"id"`
	Name                     string                             `json:"name"`
	Namespace                string                             `json:"namespace"`
	PodCount                 int                                `json:"pod_count"`
	MeasuredPods             int                                `json:"measured_pods"`
	CPURequestMilliCores     int64                              `json:"cpu_request_millicores"`
	MemoryRequestBytes       int64                              `json:"memory_request_bytes"`
	CPUUsageMilliCores       *float64                           `json:"cpu_usage_millicores"`
	MemoryUsageBytes         *int64                             `json:"memory_usage_bytes"`
	CPURecommendedMilliCores *int64                             `json:"cpu_recommended_millicores"`
	CPUCapacityMilliCores    *int64                             `json:"cpu_capacity_millicores"`
	MemoryRecommendedBytes   *int64                             `json:"memory_recommended_bytes"`
	RecommendationReason     string                             `json:"recommendation_reason"`
	Recommendation           *ResourceRightSizingRecommendation `json:"recommendation,omitempty"`
}
