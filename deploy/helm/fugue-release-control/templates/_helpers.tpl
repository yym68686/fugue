{{- define "fugue-release-control.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue-release-control.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "fugue-release-control.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "fugue-release-control.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fugue-release-control.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: release-control
{{- end -}}

{{- define "fugue-release-control.labels" -}}
{{ include "fugue-release-control.selectorLabels" . }}
app.kubernetes.io/part-of: fugue
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "fugue-release-control.validateDuration" -}}
{{- $name := index . 0 -}}
{{- $value := toString (index . 1) -}}
{{- $maximum := int (index . 2) -}}
{{- if not (regexMatch "^[1-9][0-9]{0,3}s$" $value) -}}
{{- fail (printf "%s must be a positive whole-second duration" $name) -}}
{{- end -}}
{{- $seconds := int (trimSuffix "s" $value) -}}
{{- if gt $seconds $maximum -}}
{{- fail (printf "%s must not exceed %ds" $name $maximum) -}}
{{- end -}}
{{- $value -}}
{{- end -}}
