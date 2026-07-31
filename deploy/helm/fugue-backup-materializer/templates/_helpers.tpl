{{- define "fugue-backup-materializer.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue-backup-materializer.cellID" -}}
{{- trimPrefix "backup/" (toString .Values.cell.key) | replace "/" "-" -}}
{{- end -}}

{{- define "fugue-backup-materializer.fullname" -}}
{{- printf "fugue-backup-materializer-%s" (include "fugue-backup-materializer.cellID" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue-backup-materializer.secretName" -}}
{{- printf "fugue-backup-observer-%s-input" (include "fugue-backup-materializer.cellID" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue-backup-materializer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fugue-backup-materializer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: backup-materializer
fugue.io/backup-cell-id: {{ include "fugue-backup-materializer.cellID" . }}
{{- end -}}

{{- define "fugue-backup-materializer.labels" -}}
{{ include "fugue-backup-materializer.selectorLabels" . }}
app.kubernetes.io/part-of: fugue
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
fugue.io/release-lane: backup
fugue.io/ownership-mode: shadow
fugue.io/production-mutation: forbidden
{{- end -}}

{{- define "fugue-backup-materializer.validateDuration" -}}
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

{{- define "fugue-backup-materializer.validateName" -}}
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

{{- define "fugue-backup-materializer.validateKey" -}}
{{- $name := index . 0 -}}
{{- $raw := toString (index . 1) -}}
{{- $value := trim $raw -}}
{{- if or (eq $value "") (ne $value $raw) (gt (len $value) 253) (not (regexMatch "^[A-Za-z0-9._-]+$" $value)) -}}
{{- fail (printf "%s must be a canonical ConfigMap key without path separators" $name) -}}
{{- end -}}
{{- $value -}}
{{- end -}}

{{- define "fugue-backup-materializer.validateInteger" -}}
{{- $name := index . 0 -}}
{{- $value := toString (index . 1) -}}
{{- $minimum := int64 (index . 2) -}}
{{- $maximum := int64 (index . 3) -}}
{{- if not (regexMatch "^[1-9][0-9]{0,6}$" $value) -}}
{{- fail (printf "%s must be a positive base-10 integer" $name) -}}
{{- end -}}
{{- $number := int64 $value -}}
{{- if or (lt $number $minimum) (gt $number $maximum) -}}
{{- fail (printf "%s must be between %d and %d" $name $minimum $maximum) -}}
{{- end -}}
{{- $value -}}
{{- end -}}
