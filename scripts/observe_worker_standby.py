#!/usr/bin/env python3
"""Observe an unused Worker without confusing idle capacity with retired LKG.

This tool performs only Kubernetes reads and read-only Pod observations. Its
receipt never authorizes deletion, placement changes, or traffic changes.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

from scripts import observe_front_candidate as front


def validate(c):
    fields = {"schema", "generation", "namespace", "node", "authority", "release", "worker", "front_selectors", "samples", "interval_seconds"}
    if set(c) != fields or c["schema"] != "fugue.worker-standby-observation/v1" or type(c["generation"]) is not int or c["generation"] < 1:
        raise ValueError("explicit standby observation declaration required")
    for value in [c["namespace"], c["node"]]:
        if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9]+(?:[-.][a-z0-9]+)*", value):
            raise ValueError("canonical observation identity required")
    if set(c["authority"]) != {"config_map", "group_id", "active_slot", "record_digest", "epoch"} or c["authority"]["active_slot"] not in ["a", "b"] or type(c["authority"]["epoch"]) is not int or c["authority"]["epoch"] < 1 or not re.fullmatch(r"sha256:[a-f0-9]{64}", c["authority"]["record_digest"]):
        raise ValueError("exact current authority required")
    if set(c["release"]) != {"status_map", "desired_map", "lease", "record_digest"} or not re.fullmatch(r"sha256:[a-f0-9]{64}", c["release"]["record_digest"]):
        raise ValueError("exact stable code release and mutation lease required")
    w = c["worker"]
    if set(w) != {"daemonset", "slot", "source_sha", "image_digest", "container", "ports"} or w["slot"] not in ["a", "b"] or w["slot"] == c["authority"]["active_slot"] or not re.fullmatch(r"[a-f0-9]{40}", w["source_sha"]) or not re.fullmatch(r"sha256:[a-f0-9]{64}", w["image_digest"]) or not isinstance(w["ports"], list) or not w["ports"] or w["ports"] != sorted(set(w["ports"])) or any(type(x) is not int or not 1 <= x <= 65535 for x in w["ports"]):
        raise ValueError("exact inactive executable and listener ports required")
    for value in [*c["release"].values(), c["authority"]["config_map"], c["authority"]["group_id"], w["daemonset"], w["container"]]:
        if not isinstance(value, str) or len(value) > 253 or not re.fullmatch(r"[a-z0-9][a-z0-9.:-]*", value):
            raise ValueError("invalid observation resource identity")
    if not isinstance(c["front_selectors"], list) or not 1 <= len(c["front_selectors"]) <= 16 or any(not isinstance(s, dict) or not s or any(not isinstance(k,str) or not isinstance(v,str) or not k or not v or any(x in k+v for x in "\r\n ,=") for k,v in s.items()) for s in c["front_selectors"]):
        raise ValueError("complete explicit Front selectors required")
    if type(c["samples"]) is not int or not 3 <= c["samples"] <= 12 or type(c["interval_seconds"]) is not int or not 10 <= c["interval_seconds"] <= 30:
        raise ValueError("bounded repeated observation required")
    return c


def resource(c, kind, name):
    return front.read("get", kind, name, "-n", c["namespace"], "-o", "json")


def document(c, name, key):
    obj = resource(c, "configmap", name)
    if obj["metadata"].get("deletionTimestamp") or not obj["metadata"].get("uid") or not obj["metadata"].get("resourceVersion"):
        raise ValueError("authority document identity unavailable")
    return obj, json.loads(obj["data"][key])


def authority(c):
    pins = c["authority"]
    obj, value = document(c, pins["config_map"], "authority.json")
    if value.get("kind") != "CurrentAuthority" or value.get("groupId") != pins["group_id"] or value.get("currentRecordDigest") != pins["record_digest"] or value.get("currentWorkerSlot") != pins["active_slot"] or value.get("authorityEpoch") != pins["epoch"]:
        raise ValueError("current traffic authority differs from declaration")
    if value.get("previousWorkerSlot") == c["worker"]["slot"] and (value.get("previousWorkerSourceSha") != c["worker"]["source_sha"] or value.get("previousWorkerImageDigest") != c["worker"]["image_digest"]):
        raise ValueError("previous authority does not bind the observed Worker")
    status_obj, status = document(c, c["release"]["status_map"], "status.json")
    _, desired = document(c, c["release"]["desired_map"], "desired.json")
    expected = c["release"]["record_digest"]
    if status.get("state") != "stable" or any(status.get(k) != expected for k in ["currentRecordDigest", "targetRecordDigest", "lastSuccessfulLkg"]) or desired.get("recordDigest") != expected:
        raise ValueError("code release is not the exact stable LKG")
    if not datetime.timedelta(0) <= front.now() - front.timestamp(status["observedAt"]) < datetime.timedelta(seconds=90) or any(status.get("health",{}).get(k,{}).get("state") != "healthy" for k in ["local", "dependency", "route"]):
        raise ValueError("stable release health is stale or incomplete")
    lease = resource(c, "lease", c["release"]["lease"])
    if lease.get("spec",{}).get("holderIdentity"):
        raise ValueError("a component mutation owns the release lease")
    return {"uid":obj["metadata"]["uid"],"resource_version":obj["metadata"]["resourceVersion"],"authority":value,"status_digest":status["statusDigest"],"desired":desired,"lease_uid":lease["metadata"]["uid"],"lease_version":lease["metadata"]["resourceVersion"]}


def socket_counts(raw, ports):
    if len(raw) > 4 << 20:
        raise ValueError("socket observation exceeds bound")
    states = {}
    headers = 0
    for line in raw.splitlines():
        parts = line.split()
        if parts and parts[0] == "sl":
            headers += 1
            continue
        if not parts:
            continue
        if len(parts) < 10 or not re.fullmatch(r"\d+:",parts[0]) or not re.fullmatch(r"[A-Fa-f0-9]+:[A-Fa-f0-9]{4}",parts[1]) or not re.fullmatch(r"[0-9A-Fa-f]{2}",parts[3]):
            raise ValueError("socket table is incomplete or invalid")
        if int(parts[1].split(":")[-1],16) in ports:
            state=parts[3].upper()
            states[state]=states.get(state,0)+1
    if headers != 2:
        raise ValueError("both IPv4 and IPv6 socket tables are required")
    # A live or closing connection still prevents retirement. TIME_WAIT has
    # no owning application socket; LISTEN is expected for an idle standby.
    return {"states":states,"active":sum(n for state,n in states.items() if state not in ["0A","06"])}


def worker(c):
    w=c["worker"]
    ds=resource(c,"daemonset",w["daemonset"])
    expected_labels={"fugue.io/edge-group-id":c["authority"]["group_id"],"fugue.io/edge-slot":w["slot"]}
    if ds["metadata"].get("deletionTimestamp") or ds["metadata"].get("annotations",{}).get("fugue.pro/production-config-sha")!=w["source_sha"] or any(ds["metadata"].get("labels",{}).get(k)!=v for k,v in expected_labels.items()):
        raise ValueError("Worker declaration code identity changed")
    selector=ds["spec"]["selector"]["matchLabels"]
    query=",".join(k+"="+v for k,v in sorted(selector.items()))
    pods=front.read("get","pods","-n",c["namespace"],"-l",query,"-o","json")
    if pods.get("metadata",{}).get("continue"):
        raise ValueError("Worker inventory is incomplete")
    matches=[p for p in pods["items"] if p["spec"].get("nodeName")==c["node"] and p["status"].get("phase") not in ["Succeeded","Failed"]]
    if len(matches)!=1:
        raise ValueError("Worker identity is ambiguous")
    pod=matches[0];metadata=pod["metadata"]
    if metadata.get("deletionTimestamp") or any(metadata.get("labels",{}).get(k)!=v for k,v in expected_labels.items()) or not any(o.get("kind")=="DaemonSet" and o.get("uid")==ds["metadata"]["uid"] and o.get("controller") is True for o in metadata.get("ownerReferences",[])) or not any(x.get("type")=="Ready" and x.get("status")=="True" for x in pod["status"].get("conditions",[])):
        raise ValueError("Worker is not a Ready owned instance")
    container=next(x for x in pod["spec"]["containers"] if x["name"]==w["container"])
    runtime=next(x for x in pod["status"]["containerStatuses"] if x["name"]==w["container"])
    if not container["image"].endswith("@"+w["image_digest"]) or not runtime.get("imageID","").endswith("@"+w["image_digest"]) or not runtime.get("ready") or runtime.get("restartCount")!=0:
        raise ValueError("Worker executable or process changed")
    command=["kubectl","-n",c["namespace"],"exec",metadata["name"],"-c",w["container"],"--","cat","/proc/net/tcp","/proc/net/tcp6"]
    result=subprocess.run(command,capture_output=True,text=True,timeout=20)
    if result.returncode:
        raise ValueError("Worker socket observation unavailable")
    return {"daemonset_uid":ds["metadata"]["uid"],"daemonset_version":ds["metadata"]["resourceVersion"],"pod_uid":metadata["uid"],"pod":metadata["name"],"sockets":socket_counts(result.stdout,w["ports"]),"resources":{x["name"]:x.get("resources",{}) for x in pod["spec"]["containers"]}}


def fronts(c, current):
    inventory=front.read("get","pods","-n",c["namespace"],"--field-selector","spec.nodeName="+c["node"],"-o","json")
    if inventory.get("metadata",{}).get("continue"):
        raise ValueError("Front inventory is incomplete")
    pods=[p for p in inventory["items"] if p["status"].get("phase") not in ["Succeeded","Failed"] and any(x.get("name")=="edge-front" for x in p["spec"].get("containers",[]))]
    selected=[]
    for selector in c["front_selectors"]:
        matches=[p for p in pods if all(p["metadata"].get("labels",{}).get(k)==v for k,v in selector.items())]
        if len(matches)!=1 or matches[0] in selected:
            raise ValueError("declared Front set is ambiguous")
        selected+=matches
    if len(selected)!=len(pods):
        raise ValueError("an undeclared Front can still target the old Worker")
    out=[]
    for pod in selected:
        container=next(x for x in pod["spec"]["containers"] if x["name"]=="edge-front")
        env={e["name"]:e.get("value") for e in container.get("env",[])}
        if pod["metadata"].get("deletionTimestamp") or not any(x.get("type")=="Ready" and x.get("status")=="True" for x in pod["status"].get("conditions",[])) or env.get("FUGUE_EDGE_FRONT_EDGE_GROUP_ID")!=c["authority"]["group_id"] or env.get("FUGUE_EDGE_FRONT_REQUIRE_ACTIVATION_STATE")!="true":
            raise ValueError("Front is not Ready and bound to the observed authority")
        base="/api/v1/namespaces/"+c["namespace"]+"/pods/"+pod["metadata"]["name"]+":7831/proxy/"
        health=front.read("get","--raw",base+"readyz")
        connections=front.read("get","--raw",base+"edge/tcp-connections")
        expected={"activation_generation":current["currentFrontGeneration"],"worker_source_commit":current["currentWorkerSourceSha"],"worker_image_digest":current["currentWorkerImageDigest"],"bundle_generation":current["currentBundleGeneration"]}
        if any(health.get(k)!=v for k,v in expected.items()):
            raise ValueError("Front activation differs from current authority")
        if health.get("status")!="ok" or health.get("route_authority")!="edge-control" or health.get("active_slot")!=c["authority"]["active_slot"] or type(connections.get("count")) is not int or not isinstance(connections.get("active"),list) or connections["count"]!=len(connections["active"]):
            raise ValueError("Front readiness or connection inventory is incomplete")
        # Connection IDs, source addresses and hostnames need not leave the node.
        if any(x.get("slot") not in ["a","b"] for x in connections["active"]):
            raise ValueError("Front connection slot is unknown")
        out.append({"pod":pod["metadata"]["name"],"uid":pod["metadata"]["uid"],"activation_generation":health.get("activation_generation"),"active_slot":health["active_slot"],"inactive_connections":sum(x["slot"]==c["worker"]["slot"] for x in connections["active"])})
    return sorted(out,key=lambda x:x["pod"])


def summarize(c, rows):
    if len(rows)!=c["samples"]:
        raise ValueError("standby observation window incomplete")
    first=rows[0]
    for row in rows:
        if row["authority"]["authority"]!=first["authority"]["authority"] or row["authority"]["uid"]!=first["authority"]["uid"] or row["authority"]["desired"]!=first["authority"]["desired"] or row["authority"]["lease_uid"]!=first["authority"]["lease_uid"] or row["authority"]["lease_version"]!=first["authority"]["lease_version"] or any(row["worker"][key]!=first["worker"][key] for key in ["daemonset_uid","pod_uid"]):
            raise ValueError("observed authority or executor changed")
        if [(x["uid"],x["active_slot"],x["activation_generation"]) for x in row["fronts"]]!=[(x["uid"],x["active_slot"],x["activation_generation"]) for x in first["fronts"]]:
            raise ValueError("Front activation changed during observation")
    idle=all(row["worker"]["sockets"]["active"]==0 and all(f["inactive_connections"]==0 for f in row["fronts"]) for row in rows)
    previous=first["authority"]["authority"].get("previousWorkerSlot")==c["worker"]["slot"]
    return {"schema":"fugue.worker-standby-observation-result/v1","declaration_digest":"sha256:"+hashlib.sha256(front.canonical(c).encode()).hexdigest(),"authorizes_mutation":False,"idle":idle,"required_for_immediate_rollback":previous,"retirement_ready":False,"blocking_reasons":(["live_connections"] if not idle else [])+(["referenced_positive_rollback_requires_replacement_or_reconstruction"] if previous else ["independent_retirement_authorization_required"]),"observations":rows}


def observe(c):
    validate(c);rows=[]
    for index in range(c["samples"]):
        before=authority(c);w=worker(c);f=fronts(c,before["authority"]);after=authority(c)
        if before["uid"]!=after["uid"] or before["authority"]!=after["authority"] or before["desired"]!=after["desired"] or before["lease_uid"]!=after["lease_uid"] or before["lease_version"]!=after["lease_version"]:
            raise ValueError("authority changed around runtime observations")
        rows.append({"at":front.now().isoformat(),"authority":after,"worker":w,"fronts":f})
        if index+1<c["samples"]:time.sleep(c["interval_seconds"])
    return summarize(c,rows)


def main():
    p=argparse.ArgumentParser();p.add_argument("declaration");p.add_argument("--evidence");p.add_argument("--validate-only",action="store_true");args=p.parse_args()
    c=validate(json.loads(Path(args.declaration).read_text()))
    if args.validate_only:print(front.canonical({"valid":True}));return
    if not args.evidence:raise ValueError("retained observation evidence required")
    result=observe(c);Path(args.evidence).write_text(front.canonical(result)+"\n");print(front.canonical({k:result[k] for k in ["idle","required_for_immediate_rollback","retirement_ready","blocking_reasons","authorizes_mutation"]}))


if __name__=="__main__":
    try:main()
    except Exception as error:raise SystemExit("Worker standby observation stopped: "+str(error)) from None
