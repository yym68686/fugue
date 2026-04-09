# Agent Notes

## Control Plane Updates

- Do not use `./scripts/install_fugue_ha.sh` to update the existing remote Fugue control plane for normal code changes or verification.
- For this repository, remote control plane updates must go through `git push` to `main` and the GitHub Actions workflow in `.github/workflows/deploy-control-plane.yml`.
- Do not bypass the normal deployment path with manual reinstall or upgrade commands unless the user explicitly asks for that recovery path.
- Treat the GitHub Actions deployment workflow and its self-hosted runner as the only normal control-plane release path.
- Do not build images locally and then transfer them to control-plane or cluster nodes over `ssh`, `scp`, `rsync`, `ctr`, `docker save/load`, or similar direct image copy methods.
- Do not patch live control-plane Deployments by hand with ad-hoc image tags when the intended change can be released through the normal workflow.
- If an emergency investigation requires SSH access, keep it read-only by default; only perform manual remote recovery when the user explicitly asks for that exception.
- If an emergency incident requires a temporary manual hotfix on a live control-plane node, backport the same change into this repository immediately and treat the GitHub Actions deployment workflow as the required durable follow-up before calling the rollout complete.

## General Design Constraints

- Do not add project-specific code paths, heuristics, adapters, conditionals, or migration logic keyed to a particular project name, repository name, slug, hostname, or demo fixture.
- Do not hardcode any user/project/repository-specific names to make one import path, template, manifest, or deployment succeed.
- Do not hardcode specific startup commands, entrypoint strings, filenames, ports, dependency names, or directory names just to make one uploaded project or one failing app deploy successfully.
- Zero-config detection and auto-repair behavior must be derived from generic runtime metadata, explicit configuration, or reusable language/framework rules; never from a named project, a copied incident sample, or a single user's repository layout.
- When behavior needs to vary, express it through explicit schema, configuration, generic metadata, or reusable adapters rather than matching a specific project.
- If an existing implementation appears to require a one-off workaround for a named project, stop and redesign it into a general mechanism before merging.
- The same rule applies to tests and fixtures: avoid embedding real user identifiers, app names, repository names, upload folder names, or one-off startup files from production incidents unless the test is explicitly documenting a stable public compatibility contract. Prefer neutral synthetic fixtures that exercise the generalized rule instead of replaying a single customer's project shape.

## API Contract Rules

- Fugue now follows an OpenAPI-first workflow. The single source of truth for the HTTP API is `openapi/openapi.yaml`.
- Do not treat `internal/api/server.go`, README tables, frontend fetch shapes, or tests as the authoritative API definition.
- Generated files are derived artifacts and must not be edited by hand:
  - `internal/api/routes_gen.go`
  - `internal/apispec/spec_gen.go`
- Route registration is generated from the OpenAPI contract. Do not re-introduce manual route registration blocks in `internal/api/server.go` or elsewhere.
- The runtime-served API contract endpoints are:
  - `GET /openapi.yaml`
  - `GET /openapi.json`
  - `GET /docs`
- The generator entrypoint is `cmd/fugue-openapi-gen`.
- `internal/apispec/spec.go` is the `go generate` entrypoint for the contract pipeline.

## API Change Workflow

- Every API addition, removal, rename, auth change, request shape change, response shape change, content-type change, or path/method change must start in `openapi/openapi.yaml`.
- After editing the OpenAPI contract, regenerate artifacts before touching handler wiring:
  - `make generate-openapi`
  - or `go generate ./internal/apispec`
- If the contract changes, ensure the generated route file and embedded spec are updated in the same change.
- Never hand-edit generated route patterns to “quick-fix” a mismatch. Fix the contract, then regenerate.
- If a new operation is added to the contract, it must include:
  - a unique `operationId`
  - `x-fugue-handler`
  - the correct security scheme
  - request/response metadata that matches the actual handler behavior
- If a handler behavior changes but the contract is not updated, that is a bug.
- If the contract changes but tests still only assert old README text or ad-hoc JSON bodies, update the tests to validate the contract-aligned behavior.

## Frontend Consumer Sync

- `fugue-web` now vendors this contract into:
  - `/Users/yanyuming/Downloads/GitHub/fugue-web/openapi/fugue.yaml`
  - `/Users/yanyuming/Downloads/GitHub/fugue-web/lib/fugue/openapi.generated.ts`
- When an API change affects frontend-consumed endpoints, the cross-repo follow-up is part of the same task, not optional cleanup.
- After `make generate-openapi` and `make test` pass here, update the sibling frontend repo with:
  - `npm run openapi:sync`
  - `npm run openapi:generate`
  - `npm run contract:check`
- Do not tell the frontend to hand-maintain duplicated request/response types when the contract can be generated.
- Do not hand-edit `fugue-web/lib/fugue/openapi.generated.ts`; regenerate it from `openapi/openapi.yaml`.

## API Review Expectations

- Review API changes in this order:
  1. `openapi/openapi.yaml`
  2. generated artifacts
  3. handler implementation
  4. tests
  5. README / docs summaries
- Prefer tightening OpenAPI schemas over leaving generic `object` placeholders when the request/response shape is known.
- Do not add new undocumented auth modes, query params, or body fields “temporarily”.
- When changing an endpoint used by `fugue-web`, update the consumer in the same overall task or call out the cross-repo follow-up explicitly.
- `fugue-web` CI now checks contract drift against this repo's `main` branch contract through `.github/workflows/contract-drift.yml`; if a backend contract change would break that check, treat it as a real compatibility issue that must be resolved, not bypassed.

## Verification

- `make test` must pass for API changes.
- `make test` already includes the generated-artifact drift check; if it fails, regenerate instead of bypassing the check.
