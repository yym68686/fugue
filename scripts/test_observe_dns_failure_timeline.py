import json
import unittest

from scripts import observe_dns_failure_timeline as timeline


class FailureTimelineTests(unittest.TestCase):
    def test_counter_reset_and_failure_evidence_are_explicit(self):
        observed = {'count': 7, 'uid': 'instance-a', 'reads': [], 'receipts': []}
        def kube(arguments):
            self.assertEqual(arguments[0], 'get')
            observed['reads'].append(arguments)
            if arguments[1] == 'service':
                return json.dumps({'spec': {'selector': {'app': 'dns'}}})
            if arguments[1] == 'pods':
                return json.dumps({'items': [{'metadata': {'name': 'dns', 'uid': observed['uid']}, 'spec': {'containers': [{'readinessProbe': {'httpGet': {'path': '/readyz', 'port': 7834}}}]}, 'status': {'containerStatuses': [{'containerID': 'process-a'}]}}]})
            if arguments[-1].endswith('metrics'):
                return 'fugue_dns_query_total{qtype="A",rcode="SERVFAIL"} ' + str(observed['count'])
            return json.dumps({'receipts': observed['receipts'], 'dropped': 0, 'evicted': 100})
        state = {}
        first = timeline.sample('system', ['public'], state, kube)[0]
        self.assertTrue(first['baseline_reset'])
        self.assertEqual(first['servfail_delta'], 0)
        self.assertFalse(any('decisions' in str(read) for read in observed['reads']))
        observed['count'] = 10
        observed['receipts'] = [{'rcode': 2, 'decision_id': 'failed-1'}, {'rcode': 0, 'decision_id': 'success'}]
        second = timeline.sample('system', ['public'], state, kube)[0]
        self.assertEqual(second['servfail_delta'], 3)
        self.assertEqual(second['unattributed_increment'], 2)
        self.assertEqual(second['new_failures'], [{'rcode': 2, 'decision_id': 'failed-1'}])
        observed['count'] = 11
        third = timeline.sample('system', ['public'], state, kube)[0]
        self.assertEqual(third['new_failures'], [])
        self.assertEqual(third['unattributed_increment'], 1)
        observed['uid'], observed['count'] = 'instance-b', 0
        self.assertTrue(timeline.sample('system', ['public'], state, kube)[0]['baseline_reset'])

    def test_metric_counts_only_servfail(self):
        self.assertEqual(timeline.failure_count('fugue_dns_query_total{qtype="A",rcode="NOERROR"} 100\nfugue_dns_query_total{qtype="A",rcode="SERVFAIL"} 2\nfugue_dns_query_total{qtype="AAAA",rcode="SERVFAIL"} 3'), 5)
