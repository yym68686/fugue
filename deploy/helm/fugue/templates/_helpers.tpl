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

{{- define "fugue.edgeDaemonSetName" -}}
{{- printf "%s-edge" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.dnsDaemonSetName" -}}
{{- printf "%s-dns" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.meshRecoveryDaemonSetName" -}}
{{- printf "%s-mesh-recovery" (include "fugue.fullname" .) -}}
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

{{- define "fugue.imageCacheName" -}}
{{- printf "%s-image-cache" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.imageRef" -}}
{{- $repository := required "image repository is required" .repository -}}
{{- $digest := default "" .digest -}}
{{- if $digest -}}
{{- printf "%s@%s" $repository $digest -}}
{{- else -}}
{{- printf "%s:%s" $repository (required "image tag is required when image digest is not set" .tag) -}}
{{- end -}}
{{- end -}}

{{- define "fugue.internalMaintenanceAffinity" -}}
nodeAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
      - matchExpressions:
          - key: node-role.kubernetes.io/control-plane
            operator: Exists
      - matchExpressions:
          - key: fugue.io/shared-pool
            operator: In
            values:
              - internal
{{- end -}}

{{- define "fugue.internalMaintenanceHealthyAffinity" -}}
nodeAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
      - matchExpressions:
          - key: node-role.kubernetes.io/control-plane
            operator: Exists
          - key: fugue.io/schedulable
            operator: In
            values:
              - "true"
      - matchExpressions:
          - key: fugue.io/shared-pool
            operator: In
            values:
              - internal
          - key: fugue.io/schedulable
            operator: In
            values:
              - "true"
{{- end -}}

{{- define "fugue.internalMaintenanceTolerations" -}}
tolerations:
  - key: node-role.kubernetes.io/control-plane
    operator: Equal
    effect: NoSchedule
  - key: node-role.kubernetes.io/master
    operator: Equal
    effect: NoSchedule
  - key: fugue.io/dedicated
    operator: Equal
    value: internal
    effect: NoSchedule
{{- end -}}

{{- define "fugue.registryPushBase" -}}
{{- if .Values.api.registryPushBase -}}
{{- .Values.api.registryPushBase -}}
{{- else -}}
{{- printf "%s-registry.%s.svc.cluster.local:%v" (include "fugue.fullname" .) .Release.Namespace .Values.registry.service.port -}}
{{- end -}}
{{- end -}}

{{- define "fugue.registryPullBase" -}}
{{- default (include "fugue.registryPushBase" .) .Values.api.registryPullBase -}}
{{- end -}}

{{- define "fugue.builderRegistryPushBase" -}}
{{- if and .Values.controller (get .Values.controller "builderRegistryPushBase") -}}
{{- trim (get .Values.controller "builderRegistryPushBase") -}}
{{- else if and .Values.imageCache.enabled .Values.imageCache.port -}}
{{- printf "127.0.0.1:%v" .Values.imageCache.port -}}
{{- else -}}
{{- include "fugue.registryPushBase" . -}}
{{- end -}}
{{- end -}}

{{- define "fugue.clusterJoinRegistryEndpoint" -}}
{{- if .Values.api.clusterJoinRegistryEndpoint -}}
{{- .Values.api.clusterJoinRegistryEndpoint -}}
{{- else if and .Values.imageCache.enabled .Values.imageCache.port -}}
{{- printf "http://127.0.0.1:%v" .Values.imageCache.port -}}
{{- else -}}
{{- include "fugue.registryPullBase" . -}}
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

{{- define "fugue.registryGCName" -}}
{{- printf "%s-registry-gc" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.registryDataClaimName" -}}
{{- if eq .Values.registry.persistence.mode "existingClaim" -}}
{{- .Values.registry.persistence.existingClaim -}}
{{- else -}}
{{- printf "%s-registry-data" (include "fugue.fullname" .) -}}
{{- end -}}
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
{{- $payload := dict -}}
{{- range $key, $value := $existingSecret.data -}}
{{- $_ := set $payload $key $value -}}
{{- end -}}
{{- $_ := set $payload "FUGUE_CLUSTER_JOIN_SERVER" (.Values.api.clusterJoinServer | b64enc) -}}
{{- $_ := set $payload "FUGUE_CLUSTER_JOIN_SERVER_FALLBACKS" (.Values.api.clusterJoinServerFallbacks | b64enc) -}}
{{- $_ := set $payload "FUGUE_CLUSTER_JOIN_CA_HASH" (.Values.api.clusterJoinCAHash | b64enc) -}}
{{- $_ := set $payload "FUGUE_CLUSTER_JOIN_MESH_PROVIDER" (.Values.api.clusterJoinMeshProvider | b64enc) -}}
{{- $_ := set $payload "FUGUE_CLUSTER_JOIN_MESH_LOGIN_SERVER" (.Values.api.clusterJoinMeshLoginServer | b64enc) -}}
{{- if ne .Values.api.clusterJoinMeshAuthKey "" -}}
{{- $_ := set $payload "FUGUE_CLUSTER_JOIN_MESH_AUTH_KEY" (.Values.api.clusterJoinMeshAuthKey | b64enc) -}}
{{- end -}}
{{- if ne .Values.bootstrapAdminKey "" -}}
{{- $_ := set $payload "FUGUE_BOOTSTRAP_ADMIN_KEY" (.Values.bootstrapAdminKey | b64enc) -}}
{{- end -}}
{{- if ne .Values.workloadIdentity.signingKey "" -}}
{{- $_ := set $payload "FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY" (.Values.workloadIdentity.signingKey | b64enc) -}}
{{- end -}}
{{- if ne .Values.bundle.signingKey "" -}}
{{- $_ := set $payload "FUGUE_BUNDLE_SIGNING_KEY" (.Values.bundle.signingKey | b64enc) -}}
{{- end -}}
{{- if ne .Values.bundle.previousSigningKey "" -}}
{{- $_ := set $payload "FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY" (.Values.bundle.previousSigningKey | b64enc) -}}
{{- end -}}
{{- if ne .Values.api.edgeTLSAskToken "" -}}
{{- $_ := set $payload "FUGUE_EDGE_TLS_ASK_TOKEN" (.Values.api.edgeTLSAskToken | b64enc) -}}
{{- end -}}
{{- if ne .Values.api.dataBackend.accessKeyID "" -}}
{{- $_ := set $payload "FUGUE_DATA_BACKEND_ACCESS_KEY_ID" (.Values.api.dataBackend.accessKeyID | b64enc) -}}
{{- end -}}
{{- if ne .Values.api.dataBackend.secretAccessKey "" -}}
{{- $_ := set $payload "FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY" (.Values.api.dataBackend.secretAccessKey | b64enc) -}}
{{- end -}}
{{- if ne .Values.api.dataBackend.sessionToken "" -}}
{{- $_ := set $payload "FUGUE_DATA_BACKEND_SESSION_TOKEN" (.Values.api.dataBackend.sessionToken | b64enc) -}}
{{- end -}}
{{- if ne .Values.api.dataBackend.credentialEncryptionKey "" -}}
{{- $_ := set $payload "FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY" (.Values.api.dataBackend.credentialEncryptionKey | b64enc) -}}
{{- end -}}
{{- if ne .Values.api.databaseURL "" -}}
{{- $_ := set $payload "FUGUE_DATABASE_URL" (.Values.api.databaseURL | b64enc) -}}
{{- end -}}
{{- if ne .Values.postgres.password "" -}}
{{- $_ := set $payload "POSTGRES_PASSWORD" (.Values.postgres.password | b64enc) -}}
{{- end -}}
{{- toJson $payload | sha256sum -}}
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

{{- define "fugue.controlPlanePostgresName" -}}
{{- default (printf "%s-control-plane-postgres" (include "fugue.fullname" .)) .Values.controlPlanePostgres.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue.controlPlanePostgresSecretName" -}}
{{- if .Values.controlPlanePostgres.existingSecretName -}}
{{- .Values.controlPlanePostgres.existingSecretName -}}
{{- else -}}
{{- printf "%s-app" (include "fugue.controlPlanePostgresName" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "fugue.controlPlanePostgresRWServiceName" -}}
{{- printf "%s-rw" (include "fugue.controlPlanePostgresName" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fugue.headscaleServiceName" -}}
{{- printf "%s-headscale" (include "fugue.fullname" .) -}}
{{- end -}}

{{- define "fugue.headscaleConfigName" -}}
{{- printf "%s-headscale-config" (include "fugue.fullname" .) -}}
{{- end -}}
