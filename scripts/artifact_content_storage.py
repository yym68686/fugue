"""Read-only current/automatic-LKG gate for single-copy artifact activation."""
import argparse
import json
from pathlib import Path
import re

from scripts.compiler_input_storage import command, readers


def reader_registry():
    """Use the planner's complete inventory, including data-defined groups.

    A group component is not necessarily a database reader. Determine that from
    its source roots, exactly as for a static component; never bypass the gate
    based on a component name or family.
    """
    registry = json.loads(Path('deploy/releases/components.json').read_text())
    components = list(registry['components'])
    group_path = registry.get('edgeGroupRegistryPath', '')
    if group_path:
        path = Path(group_path)
        if path.is_absolute() or '..' in path.parts or str(path) != group_path:
            raise ValueError('edge group registry path is not repository-relative')
        groups = json.loads(path.read_text())
        if (groups.get('apiVersion') != 'release.fugue.dev/v1' or
                groups.get('kind') != 'EdgeGroupRegistry' or not groups.get('groups')):
            raise ValueError('edge group registry identity is invalid')
        for group in groups['groups']:
            components.extend(group[key] for key in ('client', 'control', 'worker'))
    ids = [component['id'] for component in components]
    if len(ids) != len(set(ids)):
        raise ValueError('artifact reader component registration is ambiguous')
    return {**registry, 'components': components}


def compatible_revision(revision):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('artifact reader source revision is missing')
    # Require the complete reviewed reader implementation, not just a schema
    # marker or the writer toggle. Fail closed if the source is unavailable.
    requirements = {
        'internal/store/platform_content_read_pg.go': ('fugue_platform_artifact_body', 'resolved_content'),
        'internal/store/platform_state_pg.go': ('fugue_platform_artifact_body', 'content_ref_hash'),
        'internal/store/platform_input_hot_batch.go': ('fugue_platform_artifact_body', 'hot_body.body'),
        'internal/store/platform_input_source_verification.go': ('fugue_platform_artifact_body',),
        'internal/store/platform_input_activation.go': ('fugue_platform_artifact_body',),
        'internal/store/platform_hostname_lineage.go': ('fugue_platform_artifact_body',),
        'internal/store/platform_producer_input_deferral.go': ('fugue_platform_artifact_body',),
        'internal/store/platform_producer_capture_deferral.go': ('fugue_platform_artifact_body',),
    }
    for path, markers in requirements.items():
        body = command(['git', 'show', revision+':'+path])
        if any(marker not in body for marker in markers):
            raise ValueError('artifact current/rollback reader lacks reference support')
    # Existing signed runtime inputs remain necessary when writers are off.
    from scripts.configure_runtime_input import compatible_revision as runtime_compatible
    runtime_compatible(revision)


def check_readers():
    population = readers(reader_registry(), archive_required=True)
    for revision in sorted({r[key] for r in population for key in ('revision', 'rollback_revision')}):
        compatible_revision(revision)
    return population


def check_candidate(component_id, revision):
    registry = reader_registry()
    matches = [c for c in registry['components'] if c['id'] == component_id]
    if len(matches) != 1:
        raise ValueError('artifact reader component is not registered')
    if not {'internal/store', 'internal/schemamigrate'}.intersection(matches[0].get('sourceRoots', [])):
        return {'component': component_id, 'artifact_store_reader': False}
    # Remain required even when new reference writes have been disabled:
    # disabling the writer cannot undo existing single-copy representations.
    compatible_revision(revision)
    return {'component': component_id, 'revision': revision, 'artifact_store_reader': True}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--component')
    parser.add_argument('--candidate-revision')
    args = parser.parse_args()
    if bool(args.component) != bool(args.candidate_revision):
        parser.error('component and candidate revision must be supplied together')
    if args.component:
        print(json.dumps(check_candidate(args.component, args.candidate_revision)))
    else:
        print(json.dumps({'artifact_reference_compatible_readers_and_rollback': check_readers()}))
