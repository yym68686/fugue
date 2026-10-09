package api

import (
	"fugue/internal/model"
	"strings"
)

func edgeQualitySampleMatchesScope(sample model.EdgePerformanceSample, scope edgeQualityRankScope) bool {
	switch scope.Kind {
	case "asn":
		return strings.EqualFold(strings.TrimSpace(sample.ClientASN), scope.ASN)
	case "region":
		return strings.EqualFold(strings.TrimSpace(sample.ClientCountry), scope.Country) &&
			strings.EqualFold(strings.TrimSpace(sample.ClientRegion), scope.Region)
	case "country":
		return strings.EqualFold(strings.TrimSpace(sample.ClientCountry), scope.Country)
	default:
		return true
	}
}
