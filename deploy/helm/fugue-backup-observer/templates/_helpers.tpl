{{- define "fugue-backup-observer.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue-backup-observer.cellID" -}}
{{- trimPrefix "backup/" (toString .Values.cell.key) | replace "/" "-" -}}
{{- end -}}

{{- define "fugue-backup-observer.fullname" -}}
{{- printf "fugue-backup-observer-%s" (include "fugue-backup-observer.cellID" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue-backup-observer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fugue-backup-observer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: backup-observer
fugue.io/backup-cell-id: {{ include "fugue-backup-observer.cellID" . }}
{{- end -}}

{{- define "fugue-backup-observer.labels" -}}
{{ include "fugue-backup-observer.selectorLabels" . }}
app.kubernetes.io/part-of: fugue
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
fugue.io/release-lane: backup
fugue.io/ownership-mode: shadow
fugue.io/production-mutation: forbidden
{{- end -}}

{{- define "fugue-backup-observer.validateDuration" -}}
{{- $name := index . 0 -}}
{{- $value := toString (index . 1) -}}
{{- $minimum := int (index . 2) -}}
{{- $maximum := int (index . 3) -}}
{{- if not (regexMatch "^[1-9][0-9]{0,3}s$" $value) -}}
{{- fail (printf "%s must be a positive whole-second duration" $name) -}}
{{- end -}}
{{- $seconds := int (trimSuffix "s" $value) -}}
{{- if or (lt $seconds $minimum) (gt $seconds $maximum) -}}
{{- fail (printf "%s must be between %ds and %ds" $name $minimum $maximum) -}}
{{- end -}}
{{- $value -}}
{{- end -}}

{{- define "fugue-backup-observer.validateName" -}}
{{- $name := index . 0 -}}
{{- $raw := toString (index . 1) -}}
{{- $value := trim $raw -}}
{{- if or (eq $value "") (ne $value $raw) -}}
{{- fail (printf "%s must be a non-empty canonical lowercase DNS label" $name) -}}
{{- end -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$" $value) -}}
{{- fail (printf "%s must be a canonical lowercase DNS label of at most 63 characters" $name) -}}
{{- end -}}
{{- $value -}}
{{- end -}}

{{- define "fugue-backup-observer.validateKey" -}}
{{- $name := index . 0 -}}
{{- $raw := toString (index . 1) -}}
{{- $value := trim $raw -}}
{{- if or (eq $value "") (ne $value $raw) (gt (len $value) 253) (not (regexMatch "^[A-Za-z0-9._-]+$" $value)) -}}
{{- fail (printf "%s must be a canonical ConfigMap/Secret key without path separators" $name) -}}
{{- end -}}
{{- $value -}}
{{- end -}}
