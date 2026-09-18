package api

import (
	"encoding/json"
	"fugue/internal/platformconfig"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"reflect"
	"testing"
)

func dnsClientWorkload() appsv1.DaemonSet {
	return appsv1.DaemonSet{Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "dns", Env: []corev1.EnvVar{{Name: "FUGUE_DNS_ZONE", Value: "example.test"}, {Name: "FUGUE_EDGE_GROUP_ID", Value: "group-a"}, {Name: "FUGUE_DNS_GEOIP_OVERRIDES_JSON", Value: `[{"cidr":"192.0.2.99/24","country":" AA "},{"cidr":"192.0.2.128/25","country":"bb"}]`}}}}}}}}
}

func TestDNSClientMigrationCapturesExplicitRulesAndEmptyMapping(t *testing.T) {
	workload := dnsClientWorkload()
	values, err := dnsClientRulesFromWorkloads([]appsv1.DaemonSet{workload})
	if err != nil {
		t.Fatal(err)
	}
	if len(values["group-a"]) != 2 || values["group-a"][0].CIDR != "192.0.2.0/24" || values["group-a"][0].Country != "aa" {
		t.Fatal("precedence or normalization lost", values)
	}
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "intent", DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-b", EdgeGroupID: "group-a"}, {NodeID: "dns-a", EdgeGroupID: "group-a"}}}, Policy: platformconfig.PolicySnapshot{Generation: "old"}}
	intent := result.Intent
	if err := projectDNSClientPolicies(&result, values); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Intent, intent) || result.Policy.Generation == "old" || result.RuntimeSnapshot.PolicyGeneration != result.Policy.Generation || len(result.Policy.DNSClientPolicies) != 2 {
		t.Fatal("policy capture altered intent or lost lineage")
	}
	values["group-a"][0].Country = "zz"
	if result.Policy.DNSClientPolicies[0].Rules[0].Country != "aa" {
		t.Fatal("capture aliases mutable input")
	}
	workload.Spec.Template.Spec.Containers[0].Env = workload.Spec.Template.Spec.Containers[0].Env[:2]
	values, err = dnsClientRulesFromWorkloads([]appsv1.DaemonSet{workload})
	if err != nil {
		t.Fatal(err)
	}
	if rules, exists := values["group-a"]; !exists || rules == nil || len(rules) != 0 {
		t.Fatal("missing override is not explicit empty mapping")
	}
}

func TestDNSClientMigrationRejectsUnresolvedConflictingOrInvalidDeclarations(t *testing.T) {
	for name, mutate := range map[string]func(*appsv1.DaemonSet){
		"unknown field": func(w *appsv1.DaemonSet) {
			w.Spec.Template.Spec.Containers[0].Env[2].Value = `[{"cidr":"192.0.2.0/24","script":"x"}]`
		},
		"trailing JSON": func(w *appsv1.DaemonSet) { w.Spec.Template.Spec.Containers[0].Env[2].Value = `[] {}` },
		"null":          func(w *appsv1.DaemonSet) { w.Spec.Template.Spec.Containers[0].Env[2].Value = `null` },
		"syntax":        func(w *appsv1.DaemonSet) { w.Spec.Template.Spec.Containers[0].Env[2].Value = `invalid` },
		"CIDR":          func(w *appsv1.DaemonSet) { w.Spec.Template.Spec.Containers[0].Env[2].Value = `[{"cidr":"invalid"}]` },
		"valueFrom": func(w *appsv1.DaemonSet) {
			w.Spec.Template.Spec.Containers[0].Env[2].ValueFrom = &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{Key: "rules"}}
		},
		"envFrom": func(w *appsv1.DaemonSet) {
			w.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "config"}}}}
		},
		"duplicate": func(w *appsv1.DaemonSet) { c := &w.Spec.Template.Spec.Containers[0]; c.Env = append(c.Env, c.Env[2]) },
	} {
		t.Run(name, func(t *testing.T) {
			w := dnsClientWorkload()
			mutate(&w)
			if _, err := dnsClientRulesFromWorkloads([]appsv1.DaemonSet{w}); err == nil {
				t.Fatal("unresolved client declaration accepted")
			}
		})
	}
	w := dnsClientWorkload()
	if _, err := dnsClientRulesFromWorkloads([]appsv1.DaemonSet{w, w}); err == nil {
		t.Fatal("ambiguous group accepted")
	}
	result := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Generation: "intent", DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: "group-a"}}}, Policy: platformconfig.PolicySnapshot{Generation: "old"}}
	before, _ := json.Marshal(result)
	if err := projectDNSClientPolicies(&result, nil); err == nil {
		t.Fatal("missing consumer mapping accepted")
	}
	after, _ := json.Marshal(result)
	if string(before) != string(after) {
		t.Fatal("failed capture partially changed configuration")
	}
}
