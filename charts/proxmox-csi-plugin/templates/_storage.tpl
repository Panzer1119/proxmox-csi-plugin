{{/*
storageClass parameters uses to merge the default parameters with the user provided parameters.
*/}}
{{- define "storageClass.parameters" -}}
csi.storage.k8s.io/fstype: {{ default "ext4" .fstype }}
storage: {{ .storage | required "Proxmox Storage name must be provided." }}
{{- with .cache }}
cache: {{ . }}
{{- end }}
{{- if .ssd }}
ssd: {{ .ssd | quote }}
{{- end }}
{{- with .vmID }}
vmID: {{ . | quote }}
{{- end }}
{{- with .uuidNamespace }}
uuidNamespace: {{ . | quote }}
{{- end }}
{{- with .volumeNamePrefix }}
volumeNamePrefix: {{ . | quote }}
{{- end }}
{{- with .volumeNameSuffix }}
volumeNameSuffix: {{ . | quote }}
{{- end }}
{{- with .volumeNameTemplate }}
volumeNameTemplate: {{ . | quote }}
{{- end }}
{{- end }}
