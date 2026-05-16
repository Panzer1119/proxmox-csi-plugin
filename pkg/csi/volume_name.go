package csi

import (
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"k8s.io/klog/v2"
)

const (
	// Provided by external-provisioner when started with --extra-create-metadata
	// https://kubernetes-csi.github.io/docs/external-provisioner.html#persistentvolumeclaim-and-persistentvolume-parameters
	pvcNamespaceParamKey = "csi.storage.k8s.io/pvc/namespace"
	pvcNameParamKey      = "csi.storage.k8s.io/pvc/name"
	pvNameParamKey       = "csi.storage.k8s.io/pv/name"
	maxVolumeNameLength  = 128
)

var invalidVolumeChars = regexp.MustCompile(`[^\w-.]+`)

type volumeNameTemplateContext struct {
	VMID           int
	PVCNamespace   string
	PVCName        string
	PVName         string
	RequestedName  string
	VolumeNameUUID string
}

// resolveProvisionedVolumeName determines the final volume name for a CSI
// CreateVolume request. It prefers PVC-derived names when available and
// falls back to the provisioner-generated PV name otherwise.
func resolveProvisionedVolumeName(req *csi.CreateVolumeRequest, storageParameters StorageParameters, vmID int) (string, error) {
	requestedName := req.GetName()
	klog.V(5).InfoS("resolveProvisionedVolumeName: provisioner persistent volume name", "requestedName", requestedName)

	requestParameters := req.GetParameters()
	klog.V(5).InfoS("resolveProvisionedVolumeName: request parameters", "requestParameters", requestParameters)
	klog.V(5).InfoS("resolveProvisionedVolumeName: storage parameters", "storageParameters", storageParameters)
	klog.V(5).InfoS("resolveProvisionedVolumeName: vmID", "vmID", vmID)

	prefix := strings.TrimSpace(storageParameters.VolumeNamePrefix)
	suffix := strings.TrimSpace(storageParameters.VolumeNameSuffix)
	klog.V(5).InfoS(
		"resolveProvisionedVolumeName: name prefix/suffix",
		"prefix", prefix,
		"suffix", suffix,
	)

	rawName, ok, err := volumeNameFromParameters(requestedName, requestParameters, &storageParameters, vmID)
	if err == nil && ok {
		klog.V(5).InfoS("resolveProvisionedVolumeName: derived raw volume name", "name", rawName)

		finalName, err := normalizeVolumeName(rawName)
		if err != nil {
			return "", err
		}

		klog.V(5).InfoS("resolveProvisionedVolumeName: final volume name", "name", finalName)
		return prefix + finalName + suffix, nil
	}

	// Fallback to provisioner-generated PV name
	return prefix + requestedName + suffix, nil
}

func volumeNameFromParameters(requestedName string, requestParameters map[string]string, storageParameters *StorageParameters, vmID int) (string, bool, error) {
	if requestParameters == nil {
		klog.V(5).InfoS("volumeNameFromParameters: no request parameters provided")
		return "", false, nil
	}
	if storageParameters == nil {
		klog.V(5).InfoS("volumeNameFromParameters: no storage parameters provided")
		return "", false, nil
	}

	pvcNamespace := strings.TrimSpace(requestParameters[pvcNamespaceParamKey])
	pvcName := strings.TrimSpace(requestParameters[pvcNameParamKey])
	pvName := strings.TrimSpace(requestParameters[pvNameParamKey])
	klog.V(5).InfoS(
		"volumeNameFromParameters: pvc/pv metadata",
		"pvcNamespace", pvcNamespace,
		"pvcName", pvcName,
		"pvName", pvName,
	)

	if pvcNamespace == "" || pvcName == "" {
		klog.V(5).InfoS("volumeNameFromParameters: missing PVC metadata")
		return "", false, nil
	}

	volumeNameUUID, err := volumeNameUUIDFromParameters(pvcNamespace, pvcName, storageParameters.NamespaceUUID)
	if err != nil {
		return "", false, err
	}

	if templateText := strings.TrimSpace(storageParameters.VolumeNameTemplate); templateText != "" {
		ctx := volumeNameTemplateContext{
			VMID:           vmID,
			PVCNamespace:   pvcNamespace,
			PVCName:        pvcName,
			PVName:         pvName,
			RequestedName:  requestedName,
			VolumeNameUUID: volumeNameUUID,
		}
		klog.V(5).InfoS("volumeNameFromParameters: rendering volume name from template", "template", templateText, "context", ctx)

		rendered, err := renderVolumeNameTemplate(templateText, ctx)
		if err == nil {
			klog.V(5).InfoS("volumeNameFromParameters: rendered volume name from template", "name", rendered)
			return rendered, true, nil
		}
		klog.ErrorS(err, "volumeNameFromParameters: failed to render volume name from template", "template", templateText, "context", ctx)
	}

	if volumeNameUUID != "" {
		klog.V(5).InfoS("volumeNameFromParameters: using volume name derived from UUID", "name", volumeNameUUID)
		return volumeNameUUID, true, nil
	}

	klog.V(5).InfoS("volumeNameFromParameters: using default PVC-based volume name")
	return fmt.Sprintf("ns-%s-pvc-%s", pvcNamespace, pvcName), true, nil
}

func volumeNameUUIDFromParameters(pvcNamespace, pvcName, namespaceUUID string) (string, error) {
	if strings.TrimSpace(namespaceUUID) == "" {
		return "", nil
	}

	nsUUID, err := uuid.Parse(strings.TrimSpace(namespaceUUID))
	if err != nil {
		return "", fmt.Errorf("parameter namespace UUID is invalid: %w", err)
	}

	identity := fmt.Sprintf("%d:%s|%d:%s", len(pvcNamespace), pvcNamespace, len(pvcName), pvcName)
	return uuid.NewSHA1(nsUUID, []byte(identity)).String(), nil
}

func renderVolumeNameTemplate(templateText string, ctx volumeNameTemplateContext) (string, error) {
	tpl, err := template.New("volume-name").Option("missingkey=error").Parse(templateText)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	if err := tpl.Execute(&b, ctx); err != nil {
		return "", err
	}

	return strings.TrimSpace(b.String()), nil
}

func normalizeVolumeName(name string) (string, error) {
	sanitized := sanitize(name)
	klog.V(5).InfoS(
		"normalizeVolumeName: sanitized name",
		"original", name,
		"sanitized", sanitized,
	)

	if sanitized == "" {
		return "", fmt.Errorf("volume name is empty after sanitization")
	}

	if len(sanitized) > maxVolumeNameLength {
		truncated := strings.TrimRight(sanitized[:maxVolumeNameLength], "-")
		klog.V(5).InfoS(
			"normalizeVolumeName: truncated name",
			"originalLength", len(sanitized),
			"finalLength", len(truncated),
		)
		return truncated, nil
	}

	return sanitized, nil
}

func sanitize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.NewReplacer("_", "-", ".", "-").Replace(s)
	s = invalidVolumeChars.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")

	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}

	return s
}
