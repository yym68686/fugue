package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/proxyproto"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

type candidateCanaryProbe struct {
	GroupID             string
	SlotAddresses       map[releaseguardian.AuthoritySlot]string
	Host                string
	Path                string
	ExpectedBodyDigest  string
	Interval            time.Duration
	KeyID               string
	SigningMaterialFile string
	SigningMaterial     []byte
}

func parseCandidateCanaryProbes(value string) ([]candidateCanaryProbe, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	var probes []candidateCanaryProbe
	seen := map[string]bool{}
	for _, raw := range strings.Split(value, ";") {
		fields := strings.Split(raw, ",")
		if len(fields) != 9 {
			return nil, errors.New("candidate canary must be group,address-a,address-b,host,path,expected-body-digest,interval-seconds,key-id,signing-key-file")
		}
		seconds, err := strconv.Atoi(strings.TrimSpace(fields[6]))
		probe := candidateCanaryProbe{
			GroupID: strings.TrimSpace(fields[0]), Host: strings.TrimSpace(fields[3]), Path: strings.TrimSpace(fields[4]),
			ExpectedBodyDigest: strings.TrimSpace(fields[5]), Interval: time.Duration(seconds) * time.Second,
			KeyID: strings.TrimSpace(fields[7]), SigningMaterialFile: strings.TrimSpace(fields[8]),
			SlotAddresses: map[releaseguardian.AuthoritySlot]string{
				releaseguardian.AuthoritySlotA: strings.TrimSpace(fields[1]),
				releaseguardian.AuthoritySlotB: strings.TrimSpace(fields[2]),
			},
		}
		if err != nil || !validCandidateProbe(probe) || seen[probe.GroupID] {
			return nil, errors.New("candidate canary configuration is invalid")
		}
		seen[probe.GroupID] = true
		material, err := os.ReadFile(probe.SigningMaterialFile)
		if err != nil || len(material) < 32 || len(material) > 4096 || strings.ContainsAny(string(material), "\r\n\t ") {
			return nil, errors.New("candidate canary signing key is unavailable")
		}
		probe.SigningMaterial = append([]byte(nil), material...)
		probes = append(probes, probe)
	}
	return probes, nil
}

func validCandidateProbe(probe candidateCanaryProbe) bool {
	if (releaseguardian.Key{Component: "edge", Group: probe.GroupID}).Validate() != nil ||
		probe.Interval < 5*time.Second || probe.Interval > 20*time.Second ||
		!exactSHA256Digest(probe.ExpectedBodyDigest) ||
		(releaseguardian.Key{Component: probe.KeyID}).Validate() != nil || strings.ContainsAny(probe.GroupID+probe.Host+probe.Path+probe.KeyID, "\r\n\x00") ||
		!filepath.IsAbs(probe.SigningMaterialFile) || filepath.Clean(probe.SigningMaterialFile) != probe.SigningMaterialFile {
		return false
	}
	parsed, err := url.Parse(probe.Path)
	if err != nil || parsed.Path != probe.Path || !strings.HasPrefix(parsed.Path, "/") || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Host != "" || parsed.Scheme != "" {
		return false
	}
	hostURL, hostErr := url.Parse("https://" + probe.Host)
	if hostErr != nil || hostURL.Hostname() != probe.Host || hostURL.Port() != "" || hostURL.Path != "" || strings.TrimSpace(probe.Host) == "" {
		return false
	}
	for _, slot := range []releaseguardian.AuthoritySlot{releaseguardian.AuthoritySlotA, releaseguardian.AuthoritySlotB} {
		host, port, err := net.SplitHostPort(probe.SlotAddresses[slot])
		parsedPort, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || strings.TrimSpace(host) == "" || parsedPort < 1 || parsedPort > 65535 {
			return false
		}
	}
	return probe.SlotAddresses[releaseguardian.AuthoritySlotA] != probe.SlotAddresses[releaseguardian.AuthoritySlotB]
}

func exactSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != 71 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && value == strings.ToLower(value)
}

func startCandidateCanaryProbers(ctx context.Context, store *releaseguardian.AuthorityStore, client kubernetes.Interface, namespace string, probes []candidateCanaryProbe) {
	for _, probe := range probes {
		probe := probe
		go func() {
			ticker := time.NewTicker(probe.Interval)
			defer ticker.Stop()
			for {
				if err := candidateCanaryOnce(ctx, store, client, namespace, probe); err != nil {
					logCandidateCanaryError(probe.GroupID, err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
}

func candidateCanaryOnce(ctx context.Context, store *releaseguardian.AuthorityStore, client kubernetes.Interface, namespace string, probe candidateCanaryProbe) error {
	if client == nil || strings.TrimSpace(namespace) == "" {
		return errors.New("candidate worker observer is unavailable")
	}
	now := time.Now().UTC()
	if err := store.PruneExpiredCandidateCanaryResults(ctx, probe.GroupID, now); err != nil {
		return err
	}
	candidate, candidateUID, candidateRV, err := store.LoadCandidate(ctx, probe.GroupID)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if candidate.State != releaseguardian.CandidateAuthorityLoaded {
		return nil
	}
	if _, err := store.LoadLatestCandidateCanaryResult(ctx, candidate, now); err == nil {
		return nil
	} else if !errors.Is(err, releaseguardian.ErrCandidateCanaryUnavailable) {
		return err
	}
	current, currentUID, currentRV, err := store.LoadCurrent(ctx, probe.GroupID)
	if err != nil {
		return err
	}
	cohort, err := observeCandidateWorkerCohort(ctx, client, namespace, candidate)
	if err != nil {
		return err
	}
	candidateSamples := make([]releaseguardian.CandidateRouteSample, 0, releaseguardian.CandidateCanaryRequiredSamples)
	previousSamples := make([]releaseguardian.CandidateRouteSample, 0, releaseguardian.CandidateCanaryRequiredSamples)
	for index := 0; index < releaseguardian.CandidateCanaryRequiredSamples; index++ {
		candidateSamples = append(candidateSamples, observeCandidateRoute(ctx, probe, candidate, candidate.WorkerSlot))
		previousBinding := candidate
		previousBinding.RecordDigest, previousBinding.WorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
		previousSamples = append(previousSamples, observeCandidateRoute(ctx, probe, previousBinding, current.CurrentWorkerSlot))
	}
	observedAt := time.Now().UTC()
	latestCandidate, latestCandidateUID, latestCandidateRV, err := store.LoadCandidate(ctx, probe.GroupID)
	if err != nil || latestCandidate != candidate || latestCandidateUID != candidateUID || latestCandidateRV != candidateRV {
		return errors.New("candidate authority changed during route canary")
	}
	latestCurrent, latestCurrentUID, latestCurrentRV, err := store.LoadCurrent(ctx, probe.GroupID)
	if err != nil || latestCurrent != current || latestCurrentUID != currentUID || latestCurrentRV != currentRV {
		return errors.New("current authority changed during route canary")
	}
	latestCohort, err := observeCandidateWorkerCohort(ctx, client, namespace, candidate)
	if err != nil || latestCohort.CohortDigest != cohort.CohortDigest {
		return errors.New("candidate worker cohort changed during route canary")
	}
	result, err := releaseguardian.EvaluateCandidateCanary(candidate, current, cohort, candidateSamples, previousSamples, observedAt, 3*probe.Interval, probe.KeyID, probe.SigningMaterial)
	if err != nil {
		return err
	}
	return store.CreateCandidateCanaryResult(ctx, result, observedAt)
}

func observeCandidateWorkerCohort(ctx context.Context, client kubernetes.Interface, namespace string, candidate releaseguardian.CandidateAuthority) (releaseguardian.CandidateWorkerCohort, error) {
	selector := labels.Set{
		"fugue.io/edge-group-id": candidate.GroupID,
		"fugue.io/edge-slot":     string(candidate.WorkerSlot),
	}.AsSelector().String()
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 101})
	if err != nil || pods.Continue != "" || len(pods.Items) < 1 || len(pods.Items) > 100 {
		return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker cohort is unavailable")
	}
	cohort := releaseguardian.CandidateWorkerCohort{
		GroupID: candidate.GroupID, WorkerSlot: candidate.WorkerSlot, BundleGeneration: candidate.BundleGeneration,
		Instances: make([]releaseguardian.CandidateWorkerInstance, 0, len(pods.Items)),
	}
	for index := range pods.Items {
		pod := &pods.Items[index]
		if pod.DeletionTimestamp != nil || pod.UID == "" || strings.TrimSpace(pod.Spec.NodeName) == "" ||
			pod.Labels["fugue.io/edge-control-client"] != "true" || !podReady(pod.Status.Conditions) {
			return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker cohort is not ready")
		}
		source := strings.TrimSpace(pod.Annotations["fugue.pro/source-commit"])
		imageRef := ""
		for _, container := range pod.Spec.Containers {
			if container.Name == "edge" {
				imageRef = strings.TrimSpace(container.Image)
			}
		}
		runtimeDigest := ""
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == "edge" {
				if !status.Ready || status.RestartCount != 0 {
					return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker process is not stable")
				}
				runtimeDigest = imageDigest(status.ImageID)
			}
		}
		declaredDigest := imageDigest(imageRef)
		if !exactSourceSHA(source) || declaredDigest == "" || runtimeDigest != declaredDigest {
			return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker immutable identity is invalid")
		}
		if cohort.WorkerSourceSHA == "" {
			cohort.WorkerSourceSHA, cohort.WorkerImageDigest = source, declaredDigest
		} else if cohort.WorkerSourceSHA != source || cohort.WorkerImageDigest != declaredDigest {
			return releaseguardian.CandidateWorkerCohort{}, errors.New("candidate worker cohort contains mixed releases")
		}
		cohort.Instances = append(cohort.Instances, releaseguardian.CandidateWorkerInstance{NodeName: strings.TrimSpace(pod.Spec.NodeName), PodUID: string(pod.UID)})
	}
	return cohort.Seal()
}

func podReady(conditions []corev1.PodCondition) bool {
	for _, condition := range conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func imageDigest(value string) string {
	index := strings.LastIndex(strings.TrimSpace(value), "@sha256:")
	if index < 0 {
		return ""
	}
	digest := strings.TrimSpace(value)[index+1:]
	if !exactSHA256Digest(digest) {
		return ""
	}
	return digest
}

func exactSourceSHA(value string) bool {
	if len(value) != 40 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func observeCandidateRoute(ctx context.Context, probe candidateCanaryProbe, binding releaseguardian.CandidateAuthority, slot releaseguardian.AuthoritySlot) releaseguardian.CandidateRouteSample {
	observedAt := time.Now().UTC()
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	status, body, headers, requestErr := requestCandidateRoute(requestCtx, probe.SlotAddresses[slot], probe.Host, probe.Path)
	bodyDigest := shaDigest(body)
	evidence, _ := declarativerelease.CanonicalJSON(map[string]any{
		"address": probe.SlotAddresses[slot], "host": probe.Host, "path": probe.Path,
		"status": status, "bodyDigest": bodyDigest, "transportError": errorClass(requestErr),
	})
	return releaseguardian.CandidateRouteSample{
		GroupID: binding.GroupID, AuthorityRecordDigest: binding.RecordDigest, WorkerSlot: slot,
		ReleaseRecordDigest: binding.ReleaseRecordDigest, StatusCode: status, BodyDigest: bodyDigest,
		ExpectedBodyDigest: probe.ExpectedBodyDigest, OriginEvidenceDigest: shaDigest(evidence),
		TransportErrorClass: errorClass(requestErr), ObservedAt: observedAt.Format(time.RFC3339Nano),
		Attested:              strings.TrimSpace(headers.Get("X-Fugue-Candidate-Record-Digest")) != "",
		ObservedRecordDigest:  strings.TrimSpace(headers.Get("X-Fugue-Candidate-Record-Digest")),
		ObservedReleaseDigest: strings.TrimSpace(headers.Get("X-Fugue-Release-Record-Digest")),
		ObservedWorkerSlot:    releaseguardian.AuthoritySlot(strings.TrimSpace(headers.Get("X-Fugue-Candidate-Worker-Slot"))),
	}
}

// requestCandidateRoute exercises the same public-TLS worker listener used by
// edge-front. The PROXY v1 preamble is part of that listener contract; there
// is no HTTP header or query parameter that lets ordinary port 443 traffic
// select a candidate slot.
func requestCandidateRoute(ctx context.Context, address, host, path string) (int, []byte, http.Header, error) {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return 0, nil, nil, err
	}
	defer connection.Close()
	if _, err := io.WriteString(connection, proxyproto.HeaderV1(connection.LocalAddr(), connection.RemoteAddr())); err != nil {
		return 0, nil, nil, err
	}
	tlsConnection := tls.Client(connection, &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host, RootCAs: candidateTLSRootCAs})
	if err := tlsConnection.HandshakeContext(ctx); err != nil {
		return 0, nil, nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+host+path, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	request.Host = host
	request.Header.Set("Connection", "close")
	if err := request.Write(tlsConnection); err != nil {
		return 0, nil, nil, err
	}
	response, err := http.ReadResponse(bufio.NewReader(tlsConnection), request)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	return response.StatusCode, body, response.Header.Clone(), err
}

var logCandidateCanaryError = func(group string, err error) {
	fmt.Fprintf(os.Stderr, "candidate canary %s: %v\n", strings.TrimSpace(group), err)
}

var candidateTLSRootCAs *x509.CertPool
