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
{{- printf "%s-api" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.controllerDeploymentName" -}}
{{- printf "%s-controller" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.topologyLabelerName" -}}
{{- printf "%s-topology-labeler" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.nodeJanitorName" -}}
{{- printf "%s-node-janitor" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.imagePrePullName" -}}
{{- printf "%s-image-prepull" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.registryPushBase" -}}
{{- if .Values.api.registryPushBase -}}
{{- .Values.api.registryPushBase -}}
{{- else -}}
{{- printf "%s-registry.%s.svc.cluster.local:%v" (include "fugue.fullname" .) .Release.Namespace .Values.registry.service.port -}}
{{- end -}}
{{- end -}}

{{- define "fugue.sharedWorkspaceNFSName" -}}
{{- printf "%s-shared-workspace-nfs" (include "fugue.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue.sharedWorkspaceNFSServiceName" -}}
{{- printf "%s-shared-workspace-nfs" (include "fugue.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue.sharedWorkspaceProvisionerName" -}}
{{- printf "%s-shared-workspace-provisioner" (include "fugue.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue.registryJanitorName" -}}
{{- printf "%s-registry-janitor" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.configSecretName" -}}
{{- if .Values.configSecret.existingSecretName -}}
{{- .Values.configSecret.existingSecretName | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-config" (include "fugue.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "fugue.configSecretChecksum" -}}
{{- if .Values.configSecret.existingSecretName -}}
{{- printf "external:%s" .Values.configSecret.existingSecretName | sha256sum -}}
{{- else -}}
{{- $secretName := include "fugue.configSecretName" . -}}
{{- $existingSecret := lookup "v1" "Secret" .Release.Namespace $secretName -}}
{{- if $existingSecret -}}
{{- toJson $existingSecret.data | sha256sum -}}
{{- else -}}
{{- $bootstrapAdminKey := .Values.bootstrapAdminKey -}}
{{- if eq $bootstrapAdminKey "" -}}
{{- $bootstrapAdminKey = "generated" -}}
{{- end -}}
{{- $postgresPassword := .Values.postgres.password -}}
{{- if and .Values.postgres.enabled (eq $postgresPassword "") -}}
{{- $postgresPassword = "generated" -}}
{{- end -}}
{{- $databaseURL := .Values.api.databaseURL -}}
{{- if and (eq $databaseURL "") .Values.postgres.enabled -}}
{{- $databaseURL = printf "postgres://%s:%s@%s:%v/%s?sslmode=disable" .Values.postgres.username $postgresPassword (include "fugue.postgresServiceName" .) .Values.postgres.service.port .Values.postgres.database -}}
{{- else if eq $databaseURL "" -}}
{{- $databaseURL = "external" -}}
{{- end -}}
{{- $edgeTLSAskToken := .Values.api.edgeTLSAskToken -}}
{{- if eq $edgeTLSAskToken "" -}}
{{- $edgeTLSAskToken = "generated" -}}
{{- end -}}
{{- $payload := dict
  "FUGUE_BOOTSTRAP_ADMIN_KEY" $bootstrapAdminKey
  "FUGUE_DATABASE_URL" $databaseURL
  "FUGUE_CLUSTER_JOIN_SERVER" .Values.api.clusterJoinServer
  "FUGUE_CLUSTER_JOIN_CA_HASH" .Values.api.clusterJoinCAHash
  "FUGUE_CLUSTER_JOIN_MESH_PROVIDER" .Values.api.clusterJoinMeshProvider
  "FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER" .Values.api.clusterJoinMeshLoginServer
  "FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY" .Values.api.clusterJoinMeshAuthKey
  "FUGUE_EDGE_TLS_ASK_TOKEN" $edgeTLSAskToken
  "POSTGRES_PASSWORD" $postgresPassword
-}}
{{- toJson $payload | sha256sum -}}
{{- end -}}
{{- end -}}
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
