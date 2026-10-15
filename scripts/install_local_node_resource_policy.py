#!/usr/bin/env python3
"""CI publishes host capacity policy through a declared Kubernetes executor."""
import json
from pathlib import Path
import subprocess
import sys

HOST_INSTALL = r'''
import json,os,socket,subprocess,sys
from pathlib import Path
packet=json.load(sys.stdin)
os.environ['PATH']='/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
assert os.geteuid()==0 and socket.gethostname()==packet['node'], 'host identity differs'
executor={'__name__':'fugue_node_resource_executor'}
exec(compile(packet['source'],'node-resource-policy.py','exec'),executor)
policy=executor['validate'](packet['policy'])
print(json.dumps({'node':socket.gethostname(),'plan':executor['reconcile'](policy,dry_run=True)}))
if packet['apply']:
    result=executor['reconcile'](policy,persist=True)
    write=executor['atomic_write']
    write(Path('/usr/local/lib/fugue/node-resource-policy.py'),packet['source'])
    write(Path('/etc/systemd/system/fugue-node-resource-policy.service'), '[Unit]\nDescription=Reconcile persisted Fugue node resource policy\nAfter=local-fs.target\n[Service]\nType=oneshot\nExecStart=/usr/bin/python3 /usr/local/lib/fugue/node-resource-policy.py reconcile\nTimeoutStartSec=45\n')
    write(Path('/etc/systemd/system/fugue-node-resource-policy.timer'), '[Unit]\nDescription=Reconcile node resource policy without control-plane connectivity\n[Timer]\nOnBootSec=60s\nOnUnitActiveSec=5min\n[Install]\nWantedBy=timers.target\n')
    subprocess.run(['systemctl','daemon-reload'],check=True,timeout=30)
    subprocess.run(['systemctl','enable','--now','fugue-node-resource-policy.timer'],check=True,timeout=30)
    print(json.dumps({'node':socket.gethostname(),'result':result,'observation':executor['observation']()}))
'''


def verified_executor(config, listing):
    executor = config["local_executor"]
    matches = [pod for pod in listing.get("items", []) if pod["spec"].get("nodeName") == config["local_node"] and not pod["metadata"].get("deletionTimestamp")]
    if len(matches) != 1 or listing.get("metadata", {}).get("continue"):
        raise ValueError("local node executor absent or ambiguous")
    pod = matches[0]
    containers = [c for c in pod["spec"]["containers"] if c["name"] == executor["container"]]
    states = [c for c in pod.get("status", {}).get("containerStatuses", []) if c["name"] == executor["container"]]
    if pod["spec"].get("hostPID") is not True or len(containers) != 1 or len(states) != 1 or not states[0].get("ready") or states[0].get("imageID", "").removeprefix("docker-pullable://") != executor["image"]:
        raise ValueError("local executor host namespace, readiness or immutable image differs")
    if not any(owner.get("kind") == "DaemonSet" and owner.get("name") == executor["daemonset"] and owner.get("controller") for owner in pod["metadata"].get("ownerReferences", [])):
        raise ValueError("local executor owner differs")
    container = containers[0]
    security = container.get("securityContext", {})
    mounts = [m for m in container.get("volumeMounts", []) if m.get("mountPath") == "/host" and not m.get("readOnly") and not m.get("subPath") and not m.get("subPathExpr")]
    if security.get("privileged") is not True or security.get("runAsUser") != 0 or len(mounts) != 1 or not any(v.get("name") == mounts[0]["name"] and v.get("hostPath", {}).get("path") == "/" for v in pod["spec"].get("volumes", [])):
        raise ValueError("declared executor lacks explicit host policy access")
    return pod["metadata"]["name"]


def main():
    config_path = Path(sys.argv[1]).resolve()
    config = json.loads(config_path.read_text())
    executor = config["local_executor"]
    selector = ",".join(k + "=" + v for k, v in sorted(executor["selector"].items()))
    raw = subprocess.check_output(["kubectl", "-n", executor["namespace"], "get", "pods", "-l", selector, "--field-selector", "spec.nodeName=" + config["local_node"], "-o", "json"], text=True, timeout=30)
    name = verified_executor(config, json.loads(raw))
    packet = {"node": config["local_node"], "apply": "--apply" in sys.argv,
              "source": (Path(__file__).resolve().parents[1] / "internal/api/node_resource_policy.py").read_text(),
              "policy": json.loads((config_path.parent / config["policy_file"]).read_text())}
    subprocess.run(["kubectl", "-n", executor["namespace"], "exec", "-i", name, "-c", executor["container"], "--", "nsenter", "-t", "1", "-n", "-u", "--", "chroot", "/host", "/usr/bin/python3", "-I", "-B", "-c", HOST_INSTALL], input=json.dumps(packet), text=True, check=True, timeout=150)


if __name__ == "__main__":
    main()
