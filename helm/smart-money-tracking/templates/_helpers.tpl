{{/*
Expand the name of the chart.
*/}}
{{- define "smart-money-tracking.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "smart-money-tracking.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "smart-money-tracking.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "smart-money-tracking.labels" -}}
helm.sh/chart: {{ include "smart-money-tracking.chart" . }}
{{ include "smart-money-tracking.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "smart-money-tracking.selectorLabels" -}}
app.kubernetes.io/name: {{ include "smart-money-tracking.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "smart-money-tracking.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "smart-money-tracking.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the proper image name for PostgreSQL
*/}}
{{- define "postgresql.image" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.postgresql.image "global" .Values.global) }}
{{- end }}

{{/*
Return the proper image name for MinIO
*/}}
{{- define "minio.image" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.minio.image "global" .Values.global) }}
{{- end }}

{{/*
Return the proper image name for Kafka
*/}}
{{- define "kafka.image" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.kafka.image "global" .Values.global) }}
{{- end }}