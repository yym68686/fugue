"""Direct, bounded DNS samples from declared existing host-network Pods.

No shell, copied credentials, file writes or caller-supplied command. A refused
transport is distinct from timeout, SERVFAIL, an exec failure, or empty output.
Each invocation takes its own fresh observations through the Kubernetes API.
"""
import datetime as dt
import ipaddress
import re
import secrets
import struct
import subprocess
from concurrent.futures import ThreadPoolExecutor

from scripts import reconcile_dns_transport as transport

UID = r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"
NAME = r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?"


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def validate_sources(spec):
    require(isinstance(spec,dict) and set(spec)=={"control","sources"} and
            isinstance(spec["sources"],list) and len(spec["sources"])==2,"two DNS observer identities required")
    for src in spec["sources"]:
        require(isinstance(src,dict) and set(src)=={"node","node_uid","country","pod","pod_uid","ip","container","image"} and
                all(isinstance(v,str) for v in src.values()) and
                all(re.fullmatch(NAME,src[k]) for k in ("node","pod","container")) and
                all(re.fullmatch(UID,src[k]) for k in ("node_uid","pod_uid")) and
                re.fullmatch("[a-z]{2}",src["country"]) and
                re.fullmatch(r"[a-z0-9][a-z0-9./:_-]{1,245}@sha256:[a-f0-9]{64}",src["image"]),
                "invalid DNS observer identity")
    for key in ("country","ip","node","node_uid","pod","pod_uid"):
        require(len({s[key] for s in spec["sources"]})==2,"DNS observers are not distinct")
    for address in [spec["control"],*(s["ip"] for s in spec["sources"])]:
        ip=ipaddress.IPv4Address(address)
        require(ip.is_global and str(ip)==address,"non-public observer or control address")


def source_identity(ns,src):
    node=transport.read_json("get","node",src["node"],"-o","json")
    pod=transport.read_json("get","pod",src["pod"],"-n",ns,"-o","json")
    pm, nm, ps = pod["metadata"],node["metadata"],pod["spec"]
    ips=[x["address"] for x in node["status"]["addresses"] if x["type"] in ("ExternalIP","InternalIP")]
    require(nm["name"]==src["node"] and pm["name"]==src["pod"] and nm["uid"]==src["node_uid"] and not nm.get("deletionTimestamp") and
            nm["labels"].get("fugue.io/location-country-code")==src["country"] and src["ip"] in ips and
            [c["status"] for c in node["status"]["conditions"] if c["type"]=="Ready"]==["True"] and
            pm["namespace"]==ns and pm["uid"]==src["pod_uid"] and ps["nodeName"]==src["node"] and
            transport.ready_pod(pod) and ps.get("hostNetwork") is True and
            pod["status"]["hostIP"]==pod["status"]["podIP"]==src["ip"] and
            [c["image"] for c in ps["containers"] if c["name"]==src["container"]]==[src["image"]],
            "DNS observer source changed")
    return (nm["uid"],nm["resourceVersion"],pm["uid"],pm["resourceVersion"])


def invoke(ns,src,args,data):
    result=subprocess.run(["kubectl","--request-timeout=10s","exec","-i","-n",ns,src["pod"],"-c",src["container"],
                           "--","/bin/busybox",*args],input=data,capture_output=True,timeout=14)
    require(len(result.stdout)+len(result.stderr)<=8192,"oversized DNS observation")
    return result


def query_packet(host,typ,ident):
    name=b"".join(bytes([len(label)])+label.encode("ascii") for label in host.split("."))+b"\0"
    return struct.pack("!6H",ident,0,1,0,0,0)+name+struct.pack("!HH",{"A":1,"AAAA":28}[typ],1)


def positive(raw,wire,tcp):
    if tcp:
        require(len(raw)>2 and struct.unpack("!H",raw[:2])[0]==len(raw)-2,"bad control TCP frame")
        raw=raw[2:]
    require(len(raw)>=len(wire)+12,"missing control answer")
    ident,flags,qd,an,_,_=struct.unpack("!6H",raw[:12])
    require(raw[:2]==wire[:2] and flags & 0xfe0f == 0x8400 and qd==1 and an>0 and
            raw[12:len(wire)]==wire[12:],"control is not an exact authoritative positive response")
    rest=raw[len(wire):]
    name=wire[12:-4]
    start=2 if rest.startswith(b"\xc0\x0c") else len(name) if rest.startswith(name) else 0
    require(start and len(rest)>=start+10 and rest[start:start+4]==wire[-4:],"bad control answer identity")
    size=struct.unpack("!H",rest[start+8:start+10])[0]
    require(size==(4 if wire[-4:-2]==b"\0\x01" else 16) and len(rest)>=start+10+size and
            ipaddress.ip_address(rest[start+10:start+10+size]).is_global,"bad control address")


def sample(ns,src,server,query,mode,control,challenge):
    tcp=mode=="tcp"
    wire=query_packet(query["hostname"],query["type"],int.from_bytes(challenge[:2],"big"))
    if not control and not tcp:
        proc=invoke(ns,src,["nslookup","-type="+query["type"],query["hostname"]+".",server],b"")
        lines=[x for x in (proc.stdout+b"\n"+proc.stderr).decode("ascii","strict").splitlines() if x]
        explicit="nslookup: read: Connection refused"
        require(proc.returncode==1 and explicit in lines and
                all(x in {explicit,";; connection timed out; no servers could be reached","command terminated with exit code 1"} for x in lines),
                "UDP probe did not explicitly observe refusal")
    else:
        args=["nc","-n","-w","2",*([] if control else ["-vv"]),*([] if tcp else ["-u"]),server,"53"]
        proc=invoke(ns,src,args,(struct.pack("!H",len(wire)) if tcp else b"")+wire)
        if control:
            require(proc.returncode==0 and not proc.stderr,"control query failed")
            positive(proc.stdout,wire,tcp)
        else:
            expected=f"nc: {server} ({server}:53): Connection refused"
            lines=proc.stderr.decode("ascii","strict").splitlines()
            require(proc.returncode==1 and not proc.stdout and expected in lines and
                    all(x in {expected,"sent 0, rcvd 0","command terminated with exit code 1"} for x in lines),
                    "TCP probe did not explicitly observe refusal")
    return {"pod_uid":src["pod_uid"],"country":src["country"],"server":server,"transport":mode,
            "outcome":"authoritative_positive" if control else "connection_refused"}


def observe(config,stage):
    spec=stage["failed_predecessor"]["refused_listener"]
    validate_sources(spec)
    target, control = stage["address"],spec["control"]
    require(target!=control and all(s["ip"] not in (target,control) for s in spec["sources"]) and
            any(x["port"]==53 and x["address"]==control for x in config["listeners"]),"invalid authority control or observer locality")
    ns, query=config["namespace"],stage["failed_predecessor"]["query"]
    start=dt.datetime.now(dt.timezone.utc)
    with ThreadPoolExecutor(max_workers=8) as pool:
        def identities(): return list(pool.map(lambda s:source_identity(ns,s),spec["sources"]))
        initial=identities()
        challenge=secrets.token_bytes(16)
        jobs=[pool.submit(sample,ns,s,dst,query,kind,dst==control,challenge) for s in spec["sources"]
              for dst in (target,control) for kind in ("udp","tcp")]
        samples=[job.result() for job in jobs]
        require(initial==identities(),"DNS observer changed during the wire challenge")
    return {"challenge":challenge.hex(),"started_at":start.isoformat(),"samples":samples}
