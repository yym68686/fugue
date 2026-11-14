import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid
import urllib.request


CALICO_MANIFEST_URL = "https://raw.githubusercontent.com/projectcalico/calico/v3.33.0/manifests/calico.yaml"
CALICO_MANIFEST_SHA256 = "2de8f47595fb9c41b3f47d7b767a1f8e72ecf84057af834738ff12689a234da5"


def policy_manifest(raw):
    if hashlib.sha256(raw).hexdigest() != CALICO_MANIFEST_SHA256:
        raise ValueError("network-policy test manifest differs from its pinned upstream digest")
    source = raw.decode()
    old = '            # - name: CALICO_IPV4POOL_CIDR\n            #   value: "192.168.0.0/16"'
    if source.count(old) != 1:
        raise ValueError("network-policy test manifest lost its explicit address-pool configuration")
    return source.replace(old, '            - name: CALICO_IPV4POOL_CIDR\n              value: "10.244.0.0/16"')


def run(command, timeout=180, env=None):
    stage=' '.join(command[:2])
    started=time.monotonic()
    print('managed SSH verification starting: '+stage+'; timeout='+str(timeout)+'s',file=sys.stderr,flush=True)
    process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,start_new_session=True)
    try:
        stdout,stderr=process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            stdout,stderr=process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.stdout.close()
            process.stderr.close()
            raise RuntimeError('managed SSH verification timeout cleanup did not complete: '+stage) from None
        sys.stderr.buffer.write(stdout[-12000:]+stderr[-12000:])
        raise RuntimeError('managed SSH verification timed out: '+stage) from None
    print('managed SSH verification finished: '+stage+'; exit='+str(process.returncode)+'; elapsed='+str(round(time.monotonic()-started,2))+'s',file=sys.stderr,flush=True)
    if process.returncode:
        sys.stderr.buffer.write(stdout[-12000:] + stderr[-12000:])
        raise RuntimeError("managed SSH artifact verification command failed")
    return stdout


def validate_metadata(metadata, source):
    if metadata.get("Os") != "linux" or metadata.get("Architecture") not in {"amd64", "arm64"} or metadata.get("Config", {}).get("Labels", {}).get("org.opencontainers.image.revision") != source:
        raise ValueError("immutable artifact provenance or platform differs from the release")
    if metadata.get("Config", {}).get("Entrypoint") != ["/usr/local/bin/fugue-managed-ssh"]:
        raise ValueError("managed SSH artifact entrypoint differs from its contract")
    return metadata["Architecture"]


def canonical_repository_digest(value):
    match = re.fullmatch(r"([^@\s]+)@(sha256:[a-f0-9]{64})", value)
    if not match:
        raise ValueError("repository digest is missing")
    repository = match[1]
    if '/' not in repository:
        repository = 'docker.io/library/' + repository
    elif not any(character in repository.split('/')[0] for character in '.:') and not repository.startswith('localhost/'):
        repository = 'docker.io/' + repository
    return repository + '@' + match[2]


def verify_cri_identity(observed, docker_metadata, expected_digests):
    status = observed.get('status', {})
    spec = observed.get('info', {}).get('imageSpec', {})
    actual = {canonical_repository_digest(value) for value in status.get('repoDigests', [])}
    # Docker's containerd store may report an index as Id; CRI reports a
    # platform config as id. Bind the immutable repository AND native platform,
    # not two implementation-dependent local identifiers.
    if (not expected_digests or not actual.intersection(expected_digests) or
            not re.fullmatch(r'sha256:[a-f0-9]{64}', status.get('id', '')) or
            docker_metadata.get('Os') != 'linux' or docker_metadata.get('Architecture') not in {'amd64', 'arm64'} or
            spec.get('os') != docker_metadata['Os'] or spec.get('architecture') != docker_metadata['Architecture']):
        raise ValueError('CRI image repository digest or native platform differs from Docker')
    return status['id']


def pull_candidate_into_test_node(node, image, docker_metadata):
    if not re.fullmatch(r"fugue-ssh-artifact-[a-f0-9]{10}-control-plane",node):
        raise ValueError("candidate pull is restricted to the task-owned kind node")
    run(["docker","exec",node,"crictl","pull",image],timeout=180)
    observed=json.loads(run(["docker","exec",node,"crictl","inspecti",image]))
    return verify_cri_identity(observed, docker_metadata, {canonical_repository_digest(image)})


def pull_fixture_into_test_node(node, image):
    """Resolve native fixture layers with CRI, not Docker index export/import.

    Docker's containerd store can retain a multi-platform index without every
    platform's content. Exporting that index through kind then fails before the
    actual acceptance tests run. Compare config identity after an independent
    native pull so a moved fixture tag cannot silently change the test image.
    """
    if not re.fullmatch(r"fugue-ssh-(?:artifact|transport)-[a-f0-9]{10}-control-plane", node):
        raise ValueError("fixture pull is restricted to the task-owned kind node")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9./:_-]*", image):
        raise ValueError("explicit fixture image required")
    run(["docker", "pull", image], timeout=180)
    metadata = json.loads(run(["docker", "image", "inspect", image]))[0]
    expected = {canonical_repository_digest(value) for value in metadata.get('RepoDigests', [])}
    if not expected:
        raise ValueError('fixture repository digest is missing')
    run(["docker", "exec", node, "crictl", "pull", image], timeout=180)
    observed = json.loads(run(["docker", "exec", node, "crictl", "inspecti", image]))
    return verify_cri_identity(observed, metadata, expected)


def validate_native_acceptance(output):
    required = [b"--- PASS: TestManagedSSHIsolatedKubernetes", b"native detached bootstrap and cross-session Unix socket verified"]
    if any(marker not in output for marker in required):
        raise ValueError("native SSH/SCP or detached Unix socket acceptance did not verify")


def verify(image, source):
    if not re.fullmatch(r"[^\s]+@sha256:[a-f0-9]{64}", image) or not re.fullmatch(r"[a-f0-9]{40}", source):
        raise ValueError("digest-addressed image and exact source commit required")
    run(["docker", "pull", image], timeout=300)
    metadata = json.loads(run(["docker", "image", "inspect", image]))[0]
    architecture = validate_metadata(metadata, source)
    host = run(["docker", "info", "--format", "{{.Architecture}}"]).decode().strip()
    if host not in {"amd64": {"amd64", "x86_64"}, "arm64": {"arm64", "aarch64"}}[architecture]:
        raise ValueError("image acceptance requires a native same-architecture Docker host")
    cluster = "fugue-ssh-artifact-"+uuid.uuid4().hex[:10]
    node = cluster+"-control-plane"
    kind = ["go", "run", "sigs.k8s.io/kind@v0.30.0"]
    started = time.time()
    with tempfile.TemporaryDirectory(prefix="fugue-ssh-artifact-") as directory:
        work = Path(directory)
        helper = work/"fugue-ssh-helper"
        helper.write_bytes(run(["docker", "run", "--rm", "--entrypoint", "cat", image, "/usr/local/bin/fugue-ssh-helper"]))
        helper.chmod(0o700)
        helper_digest = "sha256:"+hashlib.sha256(helper.read_bytes()).hexdigest()
        declared = run(["docker", "run", "--rm", "--entrypoint", "cat", image, "/usr/local/share/fugue-ssh-helper.sha256"]).decode().split()[0]
        if helper_digest != "sha256:"+declared:
            raise ValueError("helper digest receipt differs from installed helper")
        kubeconfig = work/"kubeconfig"
        environment = dict(os.environ, GOOS="linux", GOARCH=architecture, CGO_ENABLED="0")
        binary = work/"sshcontrol.test"
        run(["go", "test", "-c", "-o", str(binary), "./internal/sshcontrol"], timeout=300, env=environment)
        kind_config = work/"kind.json"
        kind_config.write_text(json.dumps({"kind":"Cluster", "apiVersion":"kind.x-k8s.io/v1alpha4", "nodes":[{"role":"control-plane"}], "networking":{"disableDefaultCNI":True, "podSubnet":"10.244.0.0/16"}}))
        with urllib.request.urlopen(CALICO_MANIFEST_URL, timeout=60) as response:
            manifest = policy_manifest(response.read(1024*1024))
        calico = work/"calico.yaml"
        calico.write_text(manifest)
        try:
            run(["ssh", "-V"], timeout=10)
            run(["kubectl", "version", "--client"], timeout=15)
            run(kind+["create", "cluster", "--name", cluster, "--image", "kindest/node:v1.34.0", "--config", str(kind_config), "--kubeconfig", str(kubeconfig)], timeout=180)
            kube = ["kubectl", "--kubeconfig", str(kubeconfig)]
            run(kube+["create", "-f", str(calico)], timeout=120)
            run(kube+["rollout", "status", "daemonset/calico-node", "-n", "kube-system", "--timeout=300s"], timeout=310)
            run(kube+["wait", "--for=condition=Ready", "node", "--all", "--timeout=60s"], timeout=70)
            pull_fixture_into_test_node(node, "alpine:3.22")
            pull_candidate_into_test_node(node,image,metadata)
            internal = json.loads(run(["kubectl", "--kubeconfig", str(kubeconfig), "config", "view", "--raw", "-o", "json"]))
            for configured in internal["clusters"]:
                configured["cluster"]["server"] = "https://127.0.0.1:6443"
            internal_path = work/"internal-kubeconfig.json"
            internal_path.write_text(json.dumps(internal))
            internal_path.chmod(0o600)
            run(["docker", "cp", str(internal_path), node+":/root/ssh-kubeconfig.json"])
            run(["docker", "cp", str(binary), node+":/root/sshcontrol.test"])
            result = run(["docker", "exec", "-e", "FUGUE_SSH_TEST_KUBECONFIG=/root/ssh-kubeconfig.json", "-e", "FUGUE_SSH_TEST_IMAGE="+image, "-e", "FUGUE_SSH_TEST_HELPER_DIGEST="+helper_digest, "-e", "FUGUE_SSH_TEST_NETWORK_POLICY=required", node, "/root/sshcontrol.test", "-test.run", "^TestCandidatePublicationIsolatedKubernetes$", "-test.v", "-test.timeout=4m"], timeout=260)
            if b"--- PASS: TestCandidatePublicationIsolatedKubernetes" not in result or b"enforcing network policy verified" not in result:
                raise ValueError("candidate integration was skipped or did not verify")
            native_env = dict(os.environ, FUGUE_SSH_TEST_KUBECONFIG=str(kubeconfig), FUGUE_SSH_TEST_HELPER=str(helper))
            native = run(["go", "test", "./internal/sshkube", "-run", "^TestManagedSSHIsolatedKubernetes$", "-count=1", "-v"], timeout=180, env=native_env)
            validate_native_acceptance(native)
            return {"schema": "fugue.managed-ssh-image-receipt/v1", "image": image, "source_commit": source, "architecture": architecture, "helper_digest": helper_digest, "verified": True, "checks": ["candidate-publication", "pod-and-service-host-identity", "target-execution", "authorization-refresh", "graceful-retirement", "unchanged-workload", "native-ssh", "native-detached-bootstrap", "cross-session-unix-socket", "native-interactive-account-login", "native-scp", "pty", "sftp", "loopback-forward", "network-policy-allow-and-deny"], "network_policy_enforced": True, "network_policy_manifest_sha256": CALICO_MANIFEST_SHA256, "elapsed_seconds": round(time.time()-started, 2)}
        except Exception:
            for command in [["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "list"], ["kubectl", "--kubeconfig", str(kubeconfig), "get", "pods", "-A", "-o", "wide"]]:
                try:
                    sys.stderr.buffer.write(run(command, timeout=15)[-16000:])
                except Exception:
                    pass
            raise
        finally:
            run(kind+["delete", "cluster", "--name", cluster, "--kubeconfig", str(kubeconfig)], timeout=120)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("source")
    arguments = parser.parse_args()
    print(json.dumps(verify(arguments.image, arguments.source), sort_keys=True, indent=2))
