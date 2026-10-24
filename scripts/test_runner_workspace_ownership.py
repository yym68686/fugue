from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

WORKFLOW = Path(__file__).resolve().parent.parent / '.github/workflows/ci.yml'


def repair_source():
    lines = WORKFLOW.read_text().splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == 'import os, pathlib, stat, sys')
    end = next(i for i in range(start, len(lines)) if lines[i].strip() == 'jobs:')
    return '\n'.join(line[4:] for line in lines[start:end])


class RunnerWorkspaceOwnershipTests(unittest.TestCase):
    def execute(self, scenario):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            temporary = root / '_temp'
            temporary.mkdir()
            workspace = root / 'repository' / 'repository'
            workspace.mkdir(parents=True)
            for rel in ['scripts/__pycache__', '.git/worktrees/fixture']:
                target = workspace / rel
                target.mkdir(parents=True)
                (target / 'generated').write_text('retained content')
            untouched = workspace / 'business-input'
            untouched.write_text('preserve')
            if scenario == 'symlink':
                (workspace / 'scripts/__pycache__/link').symlink_to(untouched)
            if scenario == 'hardlink':
                os.link(untouched, workspace / 'scripts/__pycache__/linked')
            actual_stat = Path.stat
            uid, gid = max(1001, os.getuid()), max(1001, os.getgid())

            def observed(path, *, follow_symlinks=True):
                info = actual_stat(path, follow_symlinks=follow_symlinks)
                row = list(info)
                row[4], row[5] = uid, gid
                if ('__pycache__' in path.parts or 'worktrees' in path.parts) and scenario != 'owned':
                    row[4], row[5] = 0, 0
                if path.name == 'generated' and scenario == 'foreign':
                    row[4] = uid + 1
                return os.stat_result(row)

            args = ['inspect', str(workspace), str(temporary), str(uid), str(gid)]
            if scenario == 'wrong path':
                args[1] = str(root)
            with patch.object(sys, 'argv', args), patch.object(Path, 'stat', observed):
                if scenario in ['symlink', 'hardlink', 'foreign', 'wrong path']:
                    with self.assertRaises(SystemExit):
                        exec(compile(repair_source(), str(WORKFLOW), 'exec'), {})
                else:
                    exec(compile(repair_source(), str(WORKFLOW), 'exec'), {})
            self.assertEqual('preserve', untouched.read_text())
            self.assertTrue(all((workspace / rel / 'generated').read_text() == 'retained content'
                                for rel in ['scripts/__pycache__', '.git/worktrees/fixture']))

    def test_bounded_metadata_audit(self):
        for scenario in ['root', 'owned', 'symlink', 'hardlink', 'foreign', 'wrong path']:
            with self.subTest(scenario=scenario):
                self.execute(scenario)

    def test_every_self_hosted_checkout_audits_without_cleanup(self):
        text = WORKFLOW.read_text()
        import re
        blocks = re.split(r'(?m)(?=^  [a-zA-Z0-9_-]+:$)', text)
        for block in blocks:
            if 'runs-on: [self-hosted,' not in block:
                continue
            step = block.split('    steps:', 1)[1].lstrip()
            self.assertTrue(step.startswith('- name: Inspect generated metadata before checkout'))
            self.assertIn('working-directory: ${{ runner.temp }}', step)
            self.assertIn('python3 -B -c', step)
            checkout = step.index('actions/checkout')
            self.assertIn('clean: false', step[checkout:step.find('\n', checkout) + 200])
            self.assertLess(step.index('FUGUE_RUNNER_WORKSPACE_REPAIR'), checkout)


if __name__ == '__main__':
    unittest.main()
