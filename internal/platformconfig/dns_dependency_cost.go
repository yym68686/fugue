package platformconfig

import "time"

// DNSDependencyDecodeCost is aggregate wall time for fixed deterministic decode
// stages. It contains no identities, content, errors, trust or serving proof.
// A caller owns one value; observations are never global or merged implicitly.
type DNSDependencyDecodeCost struct {
	Attempts             int64 `json:"attempts"`
	Rejected             int64 `json:"rejected"`
	EnvelopeEncodeUS     int64 `json:"envelope_encode_us"`
	EnvelopeDecodeUS     int64 `json:"envelope_decode_us"`
	CellPublicationsUS   int64 `json:"cell_publications_us"`
	PreviousDecodeUS     int64 `json:"previous_decode_us"`
	PreviousValidationUS int64 `json:"previous_validation_us"`
	SourceDependenciesUS int64 `json:"source_dependencies_us"`
	AliasFallbacks       int64 `json:"alias_fallbacks"`
	AliasFallbackUS      int64 `json:"alias_fallback_us"`
}

type dnsDependencyStage uint8

const (
	dnsDependencyEnvelopeEncode dnsDependencyStage = iota
	dnsDependencyEnvelopeDecode
	dnsDependencyCells
	dnsDependencyPreviousDecode
	dnsDependencyPreviousValidation
	dnsDependencySource
	dnsDependencyAliasFallback
)

func (c *DNSDependencyDecodeCost) start() time.Time {
	if c == nil {
		return time.Time{}
	}
	return time.Now()
}

func (c *DNSDependencyDecodeCost) record(stage dnsDependencyStage, start time.Time) {
	if c == nil {
		return
	}
	elapsed := time.Since(start).Microseconds()
	switch stage {
	case dnsDependencyEnvelopeEncode:
		c.EnvelopeEncodeUS += elapsed
	case dnsDependencyEnvelopeDecode:
		c.EnvelopeDecodeUS += elapsed
	case dnsDependencyCells:
		c.CellPublicationsUS += elapsed
	case dnsDependencyPreviousDecode:
		c.PreviousDecodeUS += elapsed
	case dnsDependencyPreviousValidation:
		c.PreviousValidationUS += elapsed
	case dnsDependencySource:
		c.SourceDependenciesUS += elapsed
	case dnsDependencyAliasFallback:
		c.AliasFallbacks++
		c.AliasFallbackUS += elapsed
	}
}
