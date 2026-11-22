"""Controlled preview: observe the actual refusal and both private receivers. No apply option."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import json
from pathlib import Path

from scripts import dns_refused_listener as refused
from scripts import reconcile_dns_transport as transport


def preview(config,policy,observer,verifier):
    stage,=config["handoffs"]
    if "refused_listener" not in stage.get("failed_predecessor",{}):
        raise ValueError("explicit refusal preview required")
    diag=transport.stage_diagnostic(config,stage["address"])
    current=transport.read_json("get","service",diag["name"],"-n",config["namespace"],"-o","json")
    wire=refused.observe(config,stage,diag,current,policy)
    print(json.dumps({"stage":"public_refusal","authorizes_traffic":False,"observation":wire},sort_keys=True),flush=True)
    witness=transport.handoff_witness(config,diag,current)
    print(json.dumps({"stage":"signed_pair","candidate_uid":witness["candidate_uid"],"fallback_uid":witness["fallback_uid"],
                      "parent":witness["parent_digest"],"authorizes_traffic":False}),flush=True)
    # A long signature/readiness check cannot refresh the external sample.
    wire=refused.observe(config,stage,diag,current,policy)
    refused.validate(wire,stage,current,witness,dt.datetime.now(dt.timezone.utc))
    def direct(role):
        try:
            observed=transport.observe_recovery_fallback(config,stage,diag,current,witness,policy_path=policy,observer=observer,
                                                         verifier=verifier,role=role)
            transport.validate_witness_policy(observed,witness,role+"_uid")
        except Exception as exc:
            exc.add_note("private DNS receiver: "+role)
            raise
        receipt={"role":role,"uid":observed["candidate_uid"],"parent":observed["parent_digest"],"passed":True}
        print(json.dumps({"stage":"private_receiver","authorizes_traffic":False,**receipt}),flush=True)
        return receipt
    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts=list(pool.map(direct,("candidate","fallback")))
    print(json.dumps({"stage":"private_recipients","authorizes_traffic":False,"signed_udp_tcp":receipts},sort_keys=True),flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for name in ("declaration","policy","observer","verifier"):
        p.add_argument(name,type=Path)
    args=p.parse_args()
    preview(transport.load_config(args.declaration,preview=True),args.policy,str(args.observer.resolve()),str(args.verifier.resolve()))
