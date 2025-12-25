package csi

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"
)

const (
	// Provided by external-provisioner when started with --extra-create-metadata
	pvcNameParamKey      = "csi.storage.k8s.io/pvc/name"
	pvcNamespaceParamKey = "csi.storage.k8s.io/pvc/namespace"
	maxVolumeNameLength  = 128
)

var invalidVolumeChars = regexp.MustCompile(`[^a-z0-9-]+`)

// resolveProvisionedVolumeName determines the final volume name for a CSI
// CreateVolume request. It prefers PVC-derived names when available and
// falls back to the provisioner-generated PV name otherwise.
func resolveProvisionedVolumeName(req *csi.CreateVolumeRequest) (string, error) {
	pvName := req.GetName()
	klog.V(5).InfoS("resolveProvisionedVolumeName: provisioner PV name", "name", pvName)

	if rawName, ok := volumeNameFromParameters(req); ok {
		klog.V(5).InfoS("resolveProvisionedVolumeName: derived raw volume name", "name", rawName)

		finalName, err := normalizeVolumeName(rawName)
		if err != nil {
			return "", err
		}

		klog.V(5).InfoS("resolveProvisionedVolumeName: final volume name", "name", finalName)
		return finalName, nil
	}

	// Fallback to provisioner-generated PV name
	return pvName, nil
}

func volumeNameFromParameters(req *csi.CreateVolumeRequest) (string, bool) {
	params := req.GetParameters()
	if params == nil {
		klog.V(5).InfoS("volumeNameFromParameters: no parameters provided")
		return "", false
	}

	ns := strings.TrimSpace(params[pvcNamespaceParamKey])
	pvc := strings.TrimSpace(params[pvcNameParamKey])
	klog.V(5).InfoS(
		"volumeNameFromParameters: pvc metadata",
		"namespace", ns,
		"pvcName", pvc,
	)

	if ns == "" || pvc == "" {
		klog.V(5).InfoS("volumeNameFromParameters: missing PVC metadata")
		return "", false
	}

	prefix := strings.TrimSpace(params["volumeNamePrefix"])
	suffix := strings.TrimSpace(params["volumeNameSuffix"])

	klog.V(5).InfoS(
		"volumeNameFromParameters: name components",
		"prefix", prefix,
		"suffix", suffix,
	)

	// Namespace included to reduce cross-namespace collisions.
	return fmt.Sprintf("%sns-%s--pvc-%s%s", prefix, ns, pvc, suffix), true
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
