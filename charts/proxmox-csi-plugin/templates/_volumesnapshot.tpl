{{/*
volumeSnapshotClass parameters uses to merge the default parameters with the user provided parameters.
*/}}
{{- define "volumeSnapshotClass.parameters" -}}
{{- with .uuidNamespace }}
uuidNamespace: {{ . | quote }}
{{- end }}
{{- with .snapshotNamePrefix }}
snapshotNamePrefix: {{ . | quote }}
{{- end }}
{{- with .snapshotNameSuffix }}
snapshotNameSuffix: {{ . | quote }}
{{- end }}
{{- with .snapshotNameTemplate }}
snapshotNameTemplate: {{ . | quote }}
{{- end }}
{{- with .snapshotNameTimestampFormat }}
snapshotNameTimestampFormat: {{ . | quote }}
{{- end }}
{{- end }}
