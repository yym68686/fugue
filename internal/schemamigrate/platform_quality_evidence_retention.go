package schemamigrate

import _ "embed"

// PlatformQualityEvidenceRetentionSQL installs the narrowly bound maintenance
// capability. It neither changes the timer owner nor grants tenant access.
//
//go:embed platform_quality_evidence_retention.sql
var PlatformQualityEvidenceRetentionSQL string

// PlatformQualityEvidenceRetentionActivateSQL installs the write fence and
// transfers ownership together. The production release must prove the settled
// compatible API and standby worker before applying this administrator SQL.
//
//go:embed platform_quality_evidence_retention_activate.sql
var PlatformQualityEvidenceRetentionActivateSQL string
