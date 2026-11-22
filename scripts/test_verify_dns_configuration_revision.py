import unittest
from pathlib import Path
import subprocess
import tempfile

from scripts.verify_dns_configuration_revision import workflow_projection


class RevisionTests(unittest.TestCase):
    source = "name: ci\non: push\npermissions: {contents: read}\njobs:\n  unrelated:\n    run: old\n  dns_configuration_plan:\n    run: select\n  dns_transport:\n    run: verify\n  dns_slot_retirement:\n    run: retire\n"

    def test_unrelated_job_changes_do_not_supersede_configuration(self):
        names = ['dns_configuration_plan', 'dns_transport']
        self.assertEqual(workflow_projection(self.source, names), workflow_projection(self.source.replace('run: old', 'run: new'), names))

    def test_global_authority_and_selected_job_changes_are_fenced(self):
        names = ['dns_configuration_plan', 'dns_transport', 'dns_slot_retirement']
        for old, new in [('contents: read', 'contents: write'), ('on: push', 'on: schedule'), ('run: select', 'run: other'), ('run: verify', 'run: bypass'), ('run: retire', 'run: other')]:
            self.assertNotEqual(workflow_projection(self.source, names), workflow_projection(self.source.replace(old, new), names))

    def test_missing_and_duplicate_jobs_fail_closed(self):
        for source in [self.source.replace('dns_transport:', 'other:'), self.source + '  dns_transport:\n    run: duplicate\n', 'name: no-jobs\n']:
            with self.assertRaises(ValueError):
                workflow_projection(source, ['dns_transport'])

    def test_dns_transport_tool_change_tests_but_does_not_replay_live_declaration(self):
        source = (Path(__file__).resolve().parents[1] / '.github/workflows/ci.yml').read_text()
        job = workflow_projection(source, ['dns_transport'])[1]
        step = job.split('id: dns_transport\n', 1)[1].split('      - name:', 1)[0]
        bash = '\n'.join(line[10:] for line in step.split('        run: |\n', 1)[1].splitlines())
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'github-output'
            # Run the actual code-only branch. Any prewrite, verifier build or
            # ref access is an error; none is needed when transport.json is unchanged.
            setup = f'export BASE_SHA={"1"*40} GITHUB_OUTPUT={output}\n' + """
git() {
  [[ "$1 $2" == 'diff --name-only' && "$#" == 6 &&
     ( "$6" == 'deploy/environments/production/dns-transport/transport.json' ||
       "$6" == 'deploy/environments/production/dns-transport/ledger-inspection.json' ||
       "$6" == 'deploy/environments/production/dns-transport/refusal-preview.json' ) ]] || return 71
}
python3() {
  [[ "$1 $2 $3" == '-m unittest scripts.test_reconcile_dns_transport' ]] || return 72
  echo 'transport tests ran'
}
kubectl() { return 73; }
go() { return 74; }
"""
            result = subprocess.run(['bash', '-c', setup + bash], text=True, capture_output=True, check=True)
            self.assertIn('transport tests ran', result.stdout)
            self.assertIn('no new listener declaration to apply', result.stdout)
            self.assertFalse(output.exists())

    def test_refusal_preview_never_sets_writer_output_and_rejects_combined_intent(self):
        workflow=(Path(__file__).resolve().parents[1]/'.github/workflows/ci.yml').read_text()
        step=workflow_projection(workflow,['dns_transport'])[1].split('id: dns_transport\n',1)[1].split('      - name:',1)[0]
        bash='\n'.join(line[10:] for line in step.split('        run: |\n',1)[1].splitlines())
        for mixed in ('none','transport.json','ledger-inspection.json'):
            with tempfile.TemporaryDirectory() as root:
                folder=Path(root)/'deploy/environments/production/dns-transport'
                folder.mkdir(parents=True)
                (folder/'refusal-preview.json').touch()
                setup=f'export BASE_SHA={"1"*40} GITHUB_OUTPUT="{root}/outputs" RUNNER_TEMP="{root}" MIXED={mixed}\n'+"""
git() {
  if [[ "$1 $2" == 'diff --name-only' ]]; then
    if [[ "$6" == */refusal-preview.json || "$6" == */"$MIXED" ]]; then echo "$6"; fi
  elif [[ "$*" != 'fetch origin main' && "$1 $2" != 'diff --quiet' ]]; then return 75; fi
}
go() { [[ "$1" == build ]]; }
python3() {
 case "$1 $2" in
 '-m unittest'|'-m scripts.verify_dns_configuration_revision') return 0 ;;
 '-m scripts.preview_dns_refused_stage') echo preview_only ;;
 *) echo unexpected_writer >&2; return 76 ;;
 esac
}
"""
                run=subprocess.run(['bash','-c',setup+bash],cwd=root,text=True,capture_output=True)
                self.assertEqual(run.returncode==0,mixed=='none',run.stderr)
                self.assertEqual('preview_only' in run.stdout,mixed=='none')
                self.assertNotIn('unexpected_writer',run.stderr)
                self.assertFalse((Path(root)/'outputs').exists())

    def test_ledger_inspection_never_sets_writer_output_or_runs_reconciler(self):
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/ci.yml').read_text()
        job = workflow_projection(workflow, ['dns_transport'])[1]
        step = job.split('id: dns_transport\n', 1)[1].split('      - name:', 1)[0]
        bash = '\n'.join(line[10:] for line in step.split('        run: |\n', 1)[1].splitlines())
        for dual in (False,True):
            with self.subTest(dual=dual),tempfile.TemporaryDirectory() as root:
                output=Path(root)/'outputs'
                declaration=Path(root)/'deploy/environments/production/dns-transport/ledger-inspection.json'
                declaration.parent.mkdir(parents=True)
                declaration.touch()
                setup=f'export BASE_SHA={"1"*40} GITHUB_OUTPUT="{output}" RUNNER_TEMP="{root}" DUAL={int(dual)}\n'+"""
git() {
  if [[ "$1 $2" == "diff --name-only" ]]; then
    if [[ "$6" == */ledger-inspection.json || "$DUAL" == 1 ]]; then echo "$6"; fi
  elif [[ "$*" != "fetch origin main" && "$1 $2" != "diff --quiet" ]]; then return 78; fi
}
python3() {
  case "$1 $2" in
    '-m unittest'|'-m scripts.verify_dns_configuration_revision') return 0 ;;
    '-m scripts.inspect_dns_rollback_ledger') echo '{"authorizes_traffic":false}' ;;
    *) echo unexpected_writer >&2; return 79 ;;
  esac
}
"""
                result=subprocess.run(['bash','-c',setup+bash],cwd=root,text=True,capture_output=True)
                self.assertNotIn('unexpected_writer',result.stderr)
                if dual:
                    self.assertNotEqual(result.returncode,0)
                    self.assertFalse(output.exists())
                else:
                    self.assertEqual(result.returncode,0,result.stderr)
                    self.assertEqual(output.read_text(),'inspected=true\n')
                    self.assertIn('authorizes_traffic',result.stdout)
