"""An explicitly unready selected predecessor, never a ready backend."""
import datetime as dt
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

from scripts import reconcile_dns_transport as transport
from scripts import dns_refused_wire as wire
from scripts.dns_no_endpoint_observation import explicit_unready_snapshot


def inventory(ns,segment,selector):
    expr=",".join(k+"="+v for k,v in sorted(selector.items()))
    path=segment+"/namespaces/"+ns+("/pods" if segment=="/api/v1" else "/endpointslices")
    return transport.read_json("get","--raw",path+"?"+urllib.parse.urlencode({"labelSelector":expr,"limit":2}))


def empty(config,stage,listener,current):
    from scripts.dns_transport_failed_predecessor import require_listener_identity
    ns, name=config["namespace"],listener["name"]
    selector=current["spec"]["selector"]
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs=[pool.submit(transport.read_json,"get","service",name,"-n",ns,"-o","json"),
              pool.submit(inventory,ns,"/apis/discovery.k8s.io/v1",{"kubernetes.io/service-name":name}),
              pool.submit(inventory,ns,"/api/v1",selector),pool.submit(transport.listener_node,listener)]
        svc,slices,pods,node=[f.result() for f in jobs]
    wire.require(all(svc["metadata"].get(k)==current["metadata"].get(k) for k in ("uid","resourceVersion")),
                 "failed DNS Service changed")
    require_listener_identity(listener,svc,node["metadata"],stage["failed_predecessor"])
    topology=explicit_unready_snapshot(svc,slices,pods,node,dict(listener,namespace=ns,selector=selector))
    pod=pods["items"][0]
    failure=stage["failed_predecessor"]
    wire.require(pod["metadata"]["uid"]==failure["pod_uid"] and
                 pod["metadata"].get("annotations",{}).get("fugue.pro/oci-revision")==stage["previous_revision"],
                 "unexpected explicitly unready DNS process")
    return topology, pod


def backend(config,stage,listener,current):
    topology,pod=empty(config,stage,listener,current)
    result=transport.bound_backend(config["namespace"],pod,listener["targetPort"],read_runtime=False)
    return dict(result,empty_topology=topology)


def handoff(config,stage,listener,current,owner):
    from scripts.dns_transport_failed_predecessor import require_listener_identity
    from scripts.dns_transport_rollback_authority import require_current_rollback
    from scripts.inspect_dns_predecessor import verify_unready_backend
    from scripts.recover_dns_transport import DNSObservationRejected, candidate_request, observation_verifier, verify_observation_input
    from scripts.capture_dns_observation_policy import unchanged_backend
    ns,node,port=config["namespace"],owner["metadata"]["name"],listener["targetPort"]
    failed,rollback=stage["failed_predecessor"],stage["recovery_fallback"]
    require_listener_identity(listener,current,owner["metadata"],failed)
    verifier=observation_verifier(None)
    def read(role):
        request=None
        try:
            if role=="predecessor":
                item=backend(config,stage,listener,current)
                signed=verify_unready_backend(config,listener,item,expired=True)
                item["negative"]=dict(signed,empty_topology=item["empty_topology"])
            else:
                fallback=role=="fallback"
                selector=rollback["selector"] if fallback else listener["selector"]
                item=transport.handoff_backend(ns,selector,node,port,read_runtime=False)
                m=item["pod"]["metadata"]
                wire.require(m["uid"]==(rollback["pod_uid"] if fallback else failed["candidate_pod_uid"]) and
                             m.get("annotations",{}).get("fugue.pro/oci-revision")==
                             (rollback["revision"] if fallback else stage["candidate_revision"]),
                             "recovery recipient changed")
                request=candidate_request(item,transport.staged_backend_binding(item,ns,port))
                verify_observation_input(request,verifier,[], '{"verified":true}')
            # Private inputs stay in this stack frame and its caller only;
            # neither the witness nor a failure report can serialize them.
            return item,request
        except DNSObservationRejected as exc:
            exc.backend_role=role
            exc.add_note("refused listener signed pair: "+role)
            raise
    with ThreadPoolExecutor(max_workers=3) as pool:
        (old,_),(candidate,c_input),(fallback,f_input)=pool.map(read,("predecessor","candidate","fallback"))
    negative=old["negative"]
    wire.require(negative["parent_digest"]==failed["parent_digest"] and
                 negative["assignment"]["artifact_release_id"]==failed["release_id"] and
                 negative["assignment"]["fencing_token"]==failed["fencing_token"],"expired publication changed")
    items=[x["pod"] for x in (old,candidate,fallback)]
    for values in ((p["metadata"]["uid"] for p in items),(p["spec"]["serviceAccountName"] for p in items),
                   (p["status"]["podIP"] for p in items)):
        wire.require(len(set(values))==3,"DNS recovery executors are not isolated")
    snap=candidate["snapshot"]
    require_current_rollback(snap,failed)
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda p:unchanged_backend(ns,current,p),items))
    transport.validate_handoff_snapshots(snap,fallback["snapshot"],node,
                                         serving_pair_inputs={"candidate":c_input,"fallback":f_input})
    return {"node_uid":owner["metadata"]["uid"],"old_uid":failed["pod_uid"],"candidate_uid":failed["candidate_pod_uid"],
            "fallback_uid":rollback["pod_uid"],"previous_revision":stage["previous_revision"],"candidate_revision":stage["candidate_revision"],
            "assignment":snap["assignment"],"parent_digest":snap["parent_digest"],"plan_digest":snap["plan_digest"],"failed_predecessor":negative}


def node_revision_advanced(a,b):
    return (isinstance(a,(tuple,list)) and isinstance(b,(tuple,list)) and len(a)==len(b)==3 and
            a[:2]==b[:2] and all(isinstance(x,str) and re.fullmatch(r"[1-9][0-9]{0,30}",x) for x in (a[2],b[2])) and
            int(b[2])>int(a[2]))


def advanced_node_only(before, after):
    """Compare two independently validated topologies, never a wire receipt."""
    return (isinstance(before,dict) and isinstance(after,dict) and
            before.get("schema")=="fugue.dns.explicit-unready-diagnostic/v1" and
            node_revision_advanced(before.get("node"),after.get("node")) and dict(before,node=after["node"])==after)


def same_plan_after_node_update(before,after):
    a,b=before.get("failed_predecessor",{}),after.get("failed_predecessor",{})
    x,y=a.get("empty_topology"),b.get("empty_topology")
    return (advanced_node_only(x,y) and dict(a,empty_topology=y)==b and
            dict(before,failed_predecessor=b)==after)


class NodeObservationChanged(ValueError):
    """Never accept the interrupted sample; a new bracket may start."""


def observe(config,stage,listener,current,policy_path):
    for attempt in range(2):
        try:
            return _observe_once(config,stage,listener,current,policy_path)
        except NodeObservationChanged:
            if attempt: raise


def _observe_once(config,stage,listener,current,policy_path):
    failure=stage["failed_predecessor"]
    pub,= [x for x in config["listeners"] if x["address"]==stage["address"] and x["port"]==53]
    policy=transport.read_observation_declaration(policy_path)
    query=failure["query"]
    wire.require(len([q for q in policy["queries"] if all(q.get(k)==v for k,v in query.items()) and
                      q.get("answer_source")=="verified_serving_policy" and q.get("minimum_answers",0)>0])==1 and
                 {stage["address"],failure["refused_listener"]["control"]}.issubset(policy["servers"]),
                 "DNS refusal query or public listeners not declared")
    def snapshot():
        public=transport.read_json("get","service",pub["name"],"-n",config["namespace"],"-o","json")
        first,_=empty(config,stage,pub,public)
        other=first if listener["port"]==53 else empty(config,stage,listener,current)[0]
        if node_revision_advanced(first["node"],other["node"]):
            if all(first[k]==other[k] for k in ("selector","pod","pod_ip","target_port")):
                raise NodeObservationChanged("node revision advanced between reads; discard DNS observation")
        wire.require(first["selector"]==other["selector"] and
                     all(first[k]==other[k] for k in ("pod","node","pod_ip","target_port")) and
                     other["service"][1:]==(current["metadata"]["uid"],current["metadata"]["resourceVersion"]),
                     "public and staged DNS predecessors are not coherent")
        return first,other
    before=snapshot()
    sample=wire.observe(config,stage)
    after=snapshot()
    if before != after and all(a==b or advanced_node_only(a,b) for a,b in zip(before,after)):
        raise NodeObservationChanged("node revision advanced around DNS challenge; discard both sites")
    wire.require(before==after,"DNS topology changed around wire challenge")
    result={"schema":"fugue.dns.explicit-refusal/v1","authorizes_traffic":False,"query":query,**sample,
            "topology":before[1],"public_topology":before[0],"finished_at":dt.datetime.now(dt.timezone.utc).isoformat()}
    return result


def validate(report,stage,current,witness,now):
    from scripts.dns_transport_rollback_authority import utc_timestamp
    failed=stage["failed_predecessor"]
    spec=failed["refused_listener"]
    topology=report.get("topology") or {}
    expected=witness["failed_predecessor"]["empty_topology"]
    public=report.get("public_topology") or {}
    wire.require(report.get("schema")=="fugue.dns.explicit-refusal/v1" and report.get("authorizes_traffic") is False and
                 report.get("query")==failed["query"] and re.fullmatch("[a-f0-9]{32}",report.get("challenge","")) and
                 now-dt.timedelta(seconds=60)<=utc_timestamp(report["started_at"])<=utc_timestamp(report["finished_at"])<=now and
                 topology==expected and tuple(topology["service"][1:])==(current["metadata"]["uid"],current["metadata"]["resourceVersion"]) and
                 public["port"]==53 and public["service"][1]==failed["public_service_uid"] and
                 public["pod"]==topology["pod"] and public["node"]==topology["node"] and
                 public["pod"][1]==witness["old_uid"] and public["node"][1]==witness["node_uid"],
                 "DNS refusal is stale or not bound to this predecessor")
    rows=report["samples"]
    expected_rows={(s["pod_uid"],s["country"],ip,kind,result) for s in spec["sources"]
                   for ip,result in ((stage["address"],"connection_refused"),(spec["control"],"authoritative_positive"))
                   for kind in ("tcp","udp")}
    wire.require(len(rows)==8 and {(r["pod_uid"],r["country"],r["server"],r["transport"],r["outcome"]) for r in rows}==expected_rows,
                 "DNS refusal lacks both protocols and controls from each distinct source")
