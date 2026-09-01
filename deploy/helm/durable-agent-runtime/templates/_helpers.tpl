{{- define "dar.labels" -}}
app.kubernetes.io/name: durable-agent-runtime
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
{{- define "dar.securityContext" -}}
allowPrivilegeEscalation: false
capabilities: { drop: ["ALL"] }
readOnlyRootFilesystem: true
runAsNonRoot: true
seccompProfile: { type: RuntimeDefault }
{{- end }}
