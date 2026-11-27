package schemamigrate

import _ "embed"

// ObservationSampleRetentionFenceSQL prepares the legacy-DML guard without
// attaching it, changing ownership, or granting maintenance credentials.
//
//go:embed observation_sample_retention_fence.sql
var ObservationSampleRetentionFenceSQL string

// ObservationSampleRetentionSQL installs bounded deletion and a durable
// progress projection without selecting the worker or granting privileges.
//
//go:embed observation_sample_retention.sql
var ObservationSampleRetentionSQL string

// ObservationSampleRetentionActivateSQL atomically fences legacy deletion
// and selects the worker. It is reserved for the bound release action.
//
//go:embed observation_sample_retention_activate.sql
var ObservationSampleRetentionActivateSQL string
