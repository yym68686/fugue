"""Fence a DNS configuration against relevant workflow code, not unrelated jobs."""
import re
import subprocess
import sys


def workflow_projection(raw, names):
    # This repository uses canonical two-space top-level job indentation. Fail
    # closed if any requested job is missing or duplicated; retain the complete
    # global workflow prefix and every byte of the selected jobs.
    matches = list(re.finditer(r'^  ([a-zA-Z_][a-zA-Z0-9_-]*):\s*$', raw, re.MULTILINE))
    jobs_start = re.search(r'^jobs:\s*$', raw, re.MULTILINE)
    if jobs_start is None:
        raise ValueError('workflow jobs block missing')
    matches = [match for match in matches if match.start() > jobs_start.start()]
    if not matches:
        raise ValueError('workflow job inventory missing')
    projection = [raw[:matches[0].start()]]
    for name in names:
        indices = [i for i, match in enumerate(matches) if match.group(1) == name]
        if len(indices) != 1:
            raise ValueError('DNS workflow job missing or duplicated: ' + name)
        i = indices[0]
        projection.append(raw[matches[i].start():matches[i + 1].start() if i + 1 < len(matches) else len(raw)])
    return projection


def main():
    before, after, lane = sys.argv[1:]
    names = ['dns_configuration_plan', 'dns_transport']
    if lane == 'slots':
        names.append('dns_slot_retirement')
    elif lane != 'transport':
        raise ValueError('unknown DNS configuration lane')
    workflow = '.github/workflows/ci.yml'
    sources = [subprocess.check_output(['git', 'show', revision + ':' + workflow], text=True) for revision in [before, after]]
    if workflow_projection(sources[0], names) != workflow_projection(sources[1], names):
        raise ValueError('DNS workflow execution or global authority changed')


if __name__ == '__main__':
    main()
