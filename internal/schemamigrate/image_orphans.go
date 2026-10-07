package schemamigrate

const ImageOrphanSQL = `
ALTER TABLE fugue_image_cache_nodes ADD COLUMN IF NOT EXISTS snapshot_complete BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE IF NOT EXISTS fugue_image_orphan_policies (generation BIGINT PRIMARY KEY, body JSONB NOT NULL);
CREATE TABLE IF NOT EXISTS fugue_image_orphan_decisions (id TEXT PRIMARY KEY, body JSONB NOT NULL);
CREATE TABLE IF NOT EXISTS fugue_image_orphan_events (id BIGSERIAL PRIMARY KEY, decision_id TEXT NOT NULL, body JSONB NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS fugue_build_artifacts (id TEXT PRIMARY KEY, body JSONB NOT NULL);
`
