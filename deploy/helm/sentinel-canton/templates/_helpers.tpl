{{- define "sentinel-canton.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sentinel-canton.fullname" -}}
{{- printf "%s-%s" .Release.Name .Values.detector | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sentinel-canton.labels" -}}
app.kubernetes.io/name: {{ include "sentinel-canton.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .Values.detector }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "sentinel-canton.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sentinel-canton.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .Values.detector }}
{{- end -}}
