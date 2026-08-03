{{- define "fugue-edge-control.name" -}}
fugue-edge-control
{{- end -}}

{{- define "fugue-edge-control.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "fugue-edge-control.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue-edge-control.labels" -}}
app.kubernetes.io/name: {{ include "fugue-edge-control.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: edge-control
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "fugue-edge-control.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fugue-edge-control.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: edge-control
{{- end -}}
