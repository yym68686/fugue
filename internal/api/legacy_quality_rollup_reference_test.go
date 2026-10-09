package api

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

func scoreEdgeQualityRollup(rollup model.EdgeQualityRollup) (float64, map[string]float64) {
	profile := edgeDNSLatencyCandidateProfile{
		EdgeGroupID:               rollup.EdgeGroupID,
		EdgeID:                    rollup.EdgeID,
		ScoreBreakdown:            map[string]float64{},
		TrafficClass:              rollup.TrafficClass,
		TTFBMS:                    firstPositiveFloat(rollup.P95TTFBMS, rollup.P50TTFBMS),
		UpstreamMS:                rollup.AvgUpstreamMS,
		TotalMS:                   rollup.AvgTotalMS,
		HitRatio:                  rollup.CacheHitRate,
		ErrorRate:                 rollup.ErrorRate,
		UploadBPS:                 firstPositiveFloat(rollup.P10UploadEffectiveBPS, rollup.AvgUploadEffectiveBPS),
		BodyReadMS:                rollup.AvgBodyReadBlockMS,
		MaxReadGapMS:              rollup.P95MaxReadGapMS,
		BodyIncompleteRate:        rollup.BodyIncompleteRate,
		BodyReadErrorRate:         rollup.BodyReadErrorRate,
		ResponseEgressBPS:         firstPositiveFloat(rollup.P10ResponseEgressBPS, rollup.AvgResponseEgressBPS),
		ResponseWriteMS:           rollup.P95ResponseWriteMS,
		OriginConnectMS:           rollup.AvgOriginConnectMS,
		OriginWriteMS:             rollup.AvgOriginRequestWriteMS,
		OriginWaitMS:              rollup.AvgOriginResponseWaitMS,
		OriginTTFBMS:              rollup.AvgOriginTTFBMS,
		OriginTotalMS:             rollup.AvgOriginTotalMS,
		ActiveRequests:            rollup.AvgActiveRequests,
		ActiveBodyBuffers:         rollup.AvgActiveBodyBuffers,
		ClientTCPRTTMS:            rollup.AvgClientTCPRTTMS,
		ClientTCPMinRTTMS:         rollup.AvgClientTCPMinRTTMS,
		ClientTCPRTTVarMS:         rollup.AvgClientTCPRTTVarMS,
		ClientTCPRetransRate:      rollup.ClientTCPRetransRate,
		ClientTCPBytesRetransRate: rollup.ClientTCPBytesRetransRate,
		ClientTCPRTORate:          rollup.ClientTCPRTORate,
		ClientTCPDeliveryBPS:      rollup.AvgClientTCPDeliveryBPS,
		Confidence:                rollup.Confidence,
		ConfidencePenalty:         edgeDNSLatencyConfidencePenalty(rollup.Confidence),
		SampleCount:               rollup.RequestCount,
		BodySampleCount:           rollup.RequestCount,
	}
	score := edgeDNSLatencyScore(profile)
	breakdown := cloneFloat64Map(profile.ScoreBreakdown)
	if penalty, _ := edgeQualitySevereDegradePenalty(rollup); penalty > 0 {
		breakdown["severe_degrade"] = penalty
		score += penalty
	}
	return score, breakdown
}

func edgeQualitySevereDegradePenalty(rollup model.EdgeQualityRollup) (float64, string) {
	if strings.TrimSpace(rollup.Window) != "5m" {
		return 0, ""
	}
	switch {
	case rollup.RequestCount >= 10 && rollup.ErrorRate >= 0.20:
		return 1200, "5m_error_rate"
	case rollup.RequestCount >= 10 && rollup.BodyReadErrorRate+rollup.BodyIncompleteRate >= 0.08:
		return 1100, "5m_body_read_failures"
	case rollup.RequestCount >= 5 && firstPositiveFloat(rollup.P10UploadEffectiveBPS, rollup.AvgUploadEffectiveBPS) > 0 && firstPositiveFloat(rollup.P10UploadEffectiveBPS, rollup.AvgUploadEffectiveBPS) < 32*1024:
		return 900, "5m_upload_collapse"
	case rollup.RequestCount >= 10 && (rollup.ClientTCPRetransRate >= 0.12 || rollup.ClientTCPRTORate >= 0.08):
		return 900, "5m_tcp_loss"
	case rollup.AvgActiveRequests >= 250 || rollup.AvgActiveBodyBuffers >= 100:
		return 700, "5m_saturation"
	default:
		return 0, ""
	}
}

func formatEdgeQualityRollupReason(rollup model.EdgeQualityRollup) string {
	if penalty, reason := edgeQualitySevereDegradePenalty(rollup); penalty > 0 {
		return fmt.Sprintf("scoped_quality_rollup_%s_penalty_%.0f_confidence_%d_pct", reason, penalty, int(rollup.Confidence*100+0.5))
	}
	return fmt.Sprintf("scoped_quality_rollup_confidence_%d_pct", int(rollup.Confidence*100+0.5))
}
