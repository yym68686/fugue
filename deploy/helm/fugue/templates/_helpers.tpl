{{- define "fugue.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "fugue.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "fugue.labels" -}}
app.kubernetes.io/name: {{ include "fugue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "fugue.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fugue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "fugue.componentLabels" -}}
{{- include "fugue.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "fugue.componentSelectorLabels" -}}
app.kubernetes.io/name: {{ include "fugue.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "fugue.serviceAccountName" -}}
{{- printf "%s-sa" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.apiDeploymentName" -}}
{{- include "fugue.fullname" . -}}
{{- end -}}

{{- define "fugue.controllerDeploymentName" -}}
{{- printf "%s-controller" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.configSecretName" -}}
{{- printf "%s-config" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.postgresServiceName" -}}
{{- printf "%s-postgres" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.headscaleServiceName" -}}
{{- printf "%s-headscale" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.headscaleConfigName" -}}
{{- printf "%s-headscale-config" (include "fugue.fullname" .) -}}
{{- end -}}
