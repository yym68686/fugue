import copy
import datetime
import json
import unittest
from unittest.mock import patch
from scripts import observe_worker_standby as s


def config():
    return {"schema":"fugue.worker-standby-observation/v1","generation":1,"namespace":"test-system","node":"node-two","authority":{"config_map":"current-authority","group_id":"cell-one","active_slot":"b","record_digest":"sha256:"+"a"*64,"epoch":7},"release":{"status_map":"release-status","desired_map":"desired-release","lease":"release-lease","record_digest":"sha256:"+"b"*64},"worker":{"daemonset":"worker-a","slot":"a","source_sha":"c"*40,"image_digest":"sha256:"+"d"*64,"container":"edge","ports":[18080,18443]},"front_selectors":[{"component":"front"}],"samples":3,"interval_seconds":10}


def row():
    return {"authority":{"uid":"authority-one","authority":{"currentWorkerSlot":"b","previousWorkerSlot":"a","previousRecordDigest":"sha256:"+"e"*64},"desired":{"recordDigest":"sha256:"+"b"*64},"lease_uid":"lease-one","lease_version":"10"},"worker":{"daemonset_uid":"ds-one","pod_uid":"pod-one","sockets":{"active":0}},"fronts":[{"uid":"front-one","active_slot":"b","activation_generation":7,"inactive_connections":0}]}


class WorkerStandbyObservationTests(unittest.TestCase):
    def test_authority_rejects_busy_stale_or_unhealthy_release(self):
        c=config();a={"kind":"CurrentAuthority","groupId":"cell-one","currentWorkerSlot":"b","authorityEpoch":7,"currentRecordDigest":c["authority"]["record_digest"],"previousWorkerSlot":"a","previousWorkerSourceSha":c["worker"]["source_sha"],"previousWorkerImageDigest":c["worker"]["image_digest"]}
        status={"state":"stable","currentRecordDigest":c["release"]["record_digest"],"targetRecordDigest":c["release"]["record_digest"],"lastSuccessfulLkg":c["release"]["record_digest"],"observedAt":s.front.now().isoformat(),"statusDigest":"sha256:"+"f"*64,"health":{k:{"state":"healthy"} for k in ["local","dependency","route"]}}
        def cm(key,value):return {"metadata":{"uid":key,"resourceVersion":"1"},"data":{key:json.dumps(value)}}
        values=[cm("authority.json",a),cm("status.json",status),cm("desired.json",{"recordDigest":c["release"]["record_digest"]}),{"metadata":{"uid":"lease-one","resourceVersion":"1"},"spec":{}}]
        with patch.object(s,"resource",side_effect=values):self.assertEqual(s.authority(c)["authority"],a)
        for change in [lambda v:v[3]["spec"].update(holderIdentity="writer"),lambda v:v[0]["metadata"].update(deletionTimestamp="now")]:
            v=copy.deepcopy(values);change(v)
            with patch.object(s,"resource",side_effect=v),self.assertRaises(ValueError):s.authority(c)
        for change in [lambda v:v.update(observedAt=(s.front.now()-datetime.timedelta(minutes=2)).isoformat()),lambda v:v.update(state="applying"),lambda v:v["health"].pop("dependency"),lambda v:v.update(lastSuccessfulLkg="sha256:"+"0"*64)]:
            bad=copy.deepcopy(status);change(bad);v=copy.deepcopy(values);v[1]=cm("status.json",bad)
            with patch.object(s,"resource",side_effect=v),self.assertRaises(ValueError):s.authority(c)

    def test_all_fronts_must_be_bound_to_exact_authority(self):
        c=config();current={"currentFrontGeneration":7,"currentWorkerSourceSha":"c"*40,"currentWorkerImageDigest":"sha256:"+"d"*64,"currentBundleGeneration":"bundle-one"}
        pod={"metadata":{"name":"front-one","uid":"front-id","labels":{"component":"front"}},"spec":{"containers":[{"name":"edge-front","env":[{"name":"FUGUE_EDGE_FRONT_EDGE_GROUP_ID","value":"cell-one"},{"name":"FUGUE_EDGE_FRONT_REQUIRE_ACTIVATION_STATE","value":"true"}]}]},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}
        health={"status":"ok","route_authority":"edge-control","active_slot":"b","activation_generation":7,"worker_source_commit":"c"*40,"worker_image_digest":"sha256:"+"d"*64,"bundle_generation":"bundle-one"}
        values=[{"items":[pod]},health,{"count":0,"active":[]}]
        with patch.object(s.front,"read",side_effect=values):self.assertEqual(s.fronts(c,current)[0]["inactive_connections"],0)
        for change in [lambda v:v[0]["items"].append({**pod,"metadata":{"name":"unknown","uid":"unknown","labels":{"component":"other"}}}),lambda v:v[1].update(activation_generation=8),lambda v:v[1].update(worker_source_commit="f"*40),lambda v:v[2].update(count=1,active=[{"slot":"unknown"}]),lambda v:v[2].update(count=1)]:
            v=copy.deepcopy(values);change(v)
            with patch.object(s.front,"read",side_effect=v),self.assertRaises(ValueError):s.fronts(c,current)

    def test_idle_previous_lkg_never_authorizes_retirement(self):
        result=s.summarize(config(),[row(),row(),row()])
        self.assertTrue(result["idle"])
        self.assertTrue(result["required_for_immediate_rollback"])
        self.assertFalse(result["retirement_ready"])
        self.assertFalse(result["authorizes_mutation"])
        self.assertIn("referenced_positive_rollback_requires_replacement_or_reconstruction",result["blocking_reasons"])
        rows=[row(),row(),row()]
        for x in rows:x["authority"]["authority"]["previousWorkerSlot"]=""
        result=s.summarize(config(),rows)
        self.assertFalse(result["required_for_immediate_rollback"])
        self.assertFalse(result["retirement_ready"])

    def test_any_connections_or_authority_changes_fail_closed(self):
        for mutation in [lambda r:r["worker"]["sockets"].update(active=1),lambda r:r["fronts"][0].update(inactive_connections=1)]:
            rows=[row(),row(),row()];mutation(rows[1]);result=s.summarize(config(),rows)
            self.assertFalse(result["idle"])
            self.assertIn("live_connections",result["blocking_reasons"])
        for mutation in [lambda r:r["authority"]["authority"].update(currentWorkerSlot="a"),lambda r:r["authority"].update(uid="other"),lambda r:r["authority"].update(lease_version="11"),lambda r:r["worker"].update(pod_uid="other"),lambda r:r["fronts"][0].update(activation_generation=8)]:
            rows=[row(),row(),row()];mutation(rows[1])
            with self.assertRaises(ValueError):s.summarize(config(),rows)

    def test_socket_evidence_requires_both_tables_and_counts_closing_connections(self):
        header="  sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"
        line=" 0: 00000000:480B 00000000:0000 {state} 00000000:00000000 00:00000000 00000000 0 0 999\n"
        self.assertEqual(s.socket_counts(header+line.format(state="0A")+header,[18443])["active"],0)
        for state in ["01","03","04","05","08","09"]:
            self.assertEqual(s.socket_counts(header+line.format(state=state)+header,[18443])["active"],1)
        for raw in ["",header,header+"malformed\n"+header]:
            with self.assertRaises(ValueError):s.socket_counts(raw,[18443])

    def test_required_identity_and_observation_window(self):
        s.validate(config())
        for change in [lambda c:c["worker"].update(slot="b"),lambda c:c.update(samples=1),lambda c:c.update(front_selectors=[]),lambda c:c["worker"].update(ports=[18443,18080]),lambda c:c["release"].update(lease="bad/name")]:
            c=config();change(c)
            with self.assertRaises(ValueError):s.validate(c)

    def test_complete_window_is_readonly_and_rechecks_lease(self):
        r=row();a=r["authority"]
        with patch.object(s,"authority",return_value=a) as authority,patch.object(s,"worker",return_value=r["worker"]),patch.object(s,"fronts",return_value=r["fronts"]),patch.object(s.time,"sleep"):
            result=s.observe(config());self.assertEqual(authority.call_count,6);self.assertTrue(result["idle"]);self.assertFalse(result["authorizes_mutation"])
        changed=copy.deepcopy(a);changed["lease_version"]="11"
        with patch.object(s,"authority",side_effect=[a,changed]),patch.object(s,"worker",return_value=r["worker"]),patch.object(s,"fronts",return_value=r["fronts"]):
            with self.assertRaisesRegex(ValueError,"authority changed"):s.observe(config())


if __name__=="__main__":unittest.main()
