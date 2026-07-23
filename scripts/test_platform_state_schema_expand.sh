#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rendered="$(mktemp)"
trap 'rm -f "${rendered}"' EXIT

helm template fugue "${REPO_ROOT}/deploy/helm/fugue" \
  --namespace fugue-system \
  --set-string api.databaseURL='postgres://fugue:fixture@postgres.example:5432/fugue?sslmode=disable' \
  --set-string api.image.repository='registry.example.test/fugue-api' \
  --set-string api.image.tag='schema-checkpoint' \
  --show-only templates/deployment.yaml >"${rendered}"

ruby -ryaml - "${rendered}" <<'RUBY'
path = ARGV.fetch(0)
deployment = YAML.load_stream(File.read(path)).compact.find do |document|
  document["kind"] == "Deployment" && document.dig("metadata", "name") == "fugue-fugue-api"
end
raise "API Deployment was not rendered" unless deployment

containers = deployment.dig("spec", "template", "spec", "containers")
raise "API containers are missing" unless containers.is_a?(Array)
api = containers.find { |container| container["name"] == "api" }
raise "API container is missing" unless api
raise "schema expand must not add an untracked image consumer" unless containers.length == 1
expected_image = "registry.example.test/fugue-api:schema-checkpoint"
raise "API image fixture drifted" unless api["image"] == expected_image
mode_env = Array(api["env"]).find { |entry| entry["name"] == "FUGUE_PLATFORM_STATE_SCHEMA_EXPAND_V3" }
raise "schema expand activation must be owned by the API binary, not chart wiring" if mode_env
database_env = Array(api["env"]).find { |entry| entry["name"] == "FUGUE_DATABASE_URL" }
secret_ref = database_env&.dig("valueFrom", "secretKeyRef")
unless secret_ref == {"name" => "fugue-fugue-config", "key" => "FUGUE_DATABASE_URL"}
  raise "API must consume only the chart-managed database URL secret"
end
strategy = deployment.dig("spec", "strategy", "rollingUpdate")
unless strategy == {"maxUnavailable" => 0, "maxSurge" => 1}
  raise "schema expand must retain every old ready API replica until the migrated process starts serving"
end
RUBY

helm lint "${REPO_ROOT}/deploy/helm/fugue" \
  --set-string api.databaseURL='postgres://fugue:fixture@postgres.example:5432/fugue?sslmode=disable' \
  --set-string api.image.repository='registry.example.test/fugue-api' \
  --set-string api.image.tag='schema-checkpoint' >/dev/null

printf 'platform-state schema expand render checks passed\n'
