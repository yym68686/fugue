{{- define "fugue-image-plane.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue-image-plane.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "fugue-image-plane.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "fugue-image-plane.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fugue-image-plane.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: image-plane-shadow
{{- end -}}

{{- define "fugue-image-plane.labels" -}}
{{ include "fugue-image-plane.selectorLabels" . }}
app.kubernetes.io/part-of: fugue
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
fugue.io/release-lane: image-plane
fugue.io/ownership-mode: shadow
fugue.io/production-mutation: forbidden
{{- end -}}

{{- define "fugue-image-plane.validateDuration" -}}
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

{{- define "fugue-image-plane.validateHostPath" -}}
{{- $name := index . 0 -}}
{{- $value := trim (toString (index . 1)) -}}
{{- if or (eq $value "") (eq $value "/") (not (hasPrefix "/" $value)) (contains "//" $value) (contains "/../" (printf "%s/" $value)) (contains "/./" (printf "%s/" $value)) (hasSuffix "/" $value) -}}
{{- fail (printf "%s must be a non-root absolute canonical host path" $name) -}}
{{- end -}}
{{- $value -}}
{{- end -}}
