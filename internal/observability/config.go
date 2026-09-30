package observability

import "fugue/internal/observability/configschema"

// Preserve the public configuration types while execution stays in this package.
type Config = configschema.Config
type Identity = configschema.Identity
type Status = configschema.Status

const (
	DefaultRetention                      = configschema.DefaultRetention
	DefaultExportTimeout                  = configschema.DefaultExportTimeout
	DefaultQueueSize                      = configschema.DefaultQueueSize
	DefaultSampleRate                     = configschema.DefaultSampleRate
	DefaultScrapeInterval                 = configschema.DefaultScrapeInterval
	DefaultBatchSize                      = configschema.DefaultBatchSize
	DefaultMaxPayloadBytes                = configschema.DefaultMaxPayloadBytes
	DefaultClickHouseQueryMaxPayloadBytes = configschema.DefaultClickHouseQueryMaxPayloadBytes
	DefaultMemoryLimit                    = configschema.DefaultMemoryLimit
	DefaultRetryAttempts                  = configschema.DefaultRetryAttempts
	DefaultKubernetesLogPollInterval      = configschema.DefaultKubernetesLogPollInterval
	DefaultKubernetesLogQPS               = configschema.DefaultKubernetesLogQPS
	DefaultKubernetesLogBurst             = configschema.DefaultKubernetesLogBurst
	DefaultKubernetesLogTailLines         = configschema.DefaultKubernetesLogTailLines
	DefaultKubernetesLogMaxLineBytes      = configschema.DefaultKubernetesLogMaxLineBytes
	DefaultKubernetesLogMaxPods           = configschema.DefaultKubernetesLogMaxPods
	DefaultKubernetesLogMaxLinesPerCycle  = configschema.DefaultKubernetesLogMaxLinesPerCycle
)
