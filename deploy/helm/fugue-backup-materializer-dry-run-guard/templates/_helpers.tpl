{{- define "fugue-backup-materializer-dry-run-guard.cellID" -}}
{{- trimPrefix "backup/" (toString .Values.cell.key) | replace "/" "-" -}}
{{- end -}}

{{- define "fugue-backup-materializer-dry-run-guard.fullname" -}}
{{- printf "fugue-backup-dryrun-%s" (include "fugue-backup-materializer-dry-run-guard.cellID" .) -}}
{{- end -}}

{{- define "fugue-backup-materializer-dry-run-guard.secretName" -}}
{{- printf "fugue-backup-observer-%s-input" (include "fugue-backup-materializer-dry-run-guard.cellID" .) -}}
{{- end -}}

{{- define "fugue-backup-materializer-dry-run-guard.username" -}}
{{- printf "system:serviceaccount:fugue-system:%s" (include "fugue-backup-materializer-dry-run-guard.fullname" .) -}}
{{- end -}}

{{- define "fugue-backup-materializer-dry-run-guard.labels" -}}
app.kubernetes.io/name: fugue-backup-materializer-dry-run-guard
app.kubernetes.io/component: backup-materializer-dry-run-guard
app.kubernetes.io/part-of: fugue
app.kubernetes.io/managed-by: Helm
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
fugue.io/release-lane: backup
fugue.io/ownership-mode: shadow
fugue.io/production-mutation: forbidden
fugue.io/backup-cell-id: {{ include "fugue-backup-materializer-dry-run-guard.cellID" . }}
{{- end -}}

{{- define "fugue-backup-materializer-dry-run-guard.annotations" -}}
fugue.io/backup-cell-key: {{ .Values.cell.key | quote }}
fugue.io/backup-secret-name: {{ include "fugue-backup-materializer-dry-run-guard.secretName" . | quote }}
fugue.io/gateway-service-account: {{ include "fugue-backup-materializer-dry-run-guard.fullname" . | quote }}
fugue.io/guard-contract: "backup-materializer-dry-run-guard@v1"
fugue.io/minimum-kubernetes-version: "1.30.0"
fugue.io/production-mutation: "forbidden"
{{- end -}}

{{- define "fugue-backup-materializer-dry-run-guard.validate" -}}
{{- if ne .Release.Namespace "fugue-system" -}}
{{- fail "backup materializer dry-run guard may be rendered only in namespace fugue-system" -}}
{{- end -}}
{{- $cellKey := toString .Values.cell.key -}}
{{- if eq $cellKey "" -}}
{{- fail "cell.key is required when the dry-run guard is enabled" -}}
{{- end -}}
{{- if not (regexMatch "^backup/(control-plane-db|app-database|persistent-storage|data-workspace|registry|platform-component)/[0-9a-f]{16}$" $cellKey) -}}
{{- fail "cell.key must be a canonical backup cell key" -}}
{{- end -}}
{{- $name := include "fugue-backup-materializer-dry-run-guard.fullname" . -}}
{{- $secretName := include "fugue-backup-materializer-dry-run-guard.secretName" . -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$" $name) -}}
{{- fail "derived gateway identity must be a canonical lowercase DNS label of at most 63 characters" -}}
{{- end -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$" $secretName) -}}
{{- fail "derived Secret name must be a canonical lowercase DNS label of at most 63 characters" -}}
{{- end -}}
{{- if not (semverCompare ">=1.30.0-0" .Capabilities.KubeVersion.Version) -}}
{{- fail "enabled dry-run guard requires Kubernetes 1.30 or newer" -}}
{{- end -}}
{{- if not (.Capabilities.APIVersions.Has "admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy") -}}
{{- fail "enabled dry-run guard requires admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy discovery" -}}
{{- end -}}
{{- if not (.Capabilities.APIVersions.Has "admissionregistration.k8s.io/v1/ValidatingAdmissionPolicyBinding") -}}
{{- fail "enabled dry-run guard requires admissionregistration.k8s.io/v1/ValidatingAdmissionPolicyBinding discovery" -}}
{{- end -}}
{{- end -}}
