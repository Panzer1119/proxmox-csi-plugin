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
)

var volumeNameAllowed = regexp.MustCompile(`[^a-z0-9-]+`)

func resolveProvisionedVolumeName(request *csi.CreateVolumeRequest) (string, error) {
	// This is usually "pvc-<uuid>" and is always available.
	pvName := request.GetName()
	klog.V(5).InfoS("resolveProvisionedVolumeName: pvName", pvName)

	// Prefer extra-create-metadata parameters (no API lookup required)
	if name, ok := volumeNameFromRequestParameters(request); ok {
		klog.V(5).InfoS("resolveProvisionedVolumeName: name", name)
		return buildProvisionedName(name)
	}

	// Fallback to provisioner-generated PV name
	return pvName, nil
}

func volumeNameFromRequestParameters(request *csi.CreateVolumeRequest) (string, bool) {
	params := request.GetParameters()
	if params == nil {
		return "", false
	}
	namePrefix := strings.TrimSpace(params["volumeNamePrefix"])
	nameSuffix := strings.TrimSpace(params["volumeNameSuffix"])
	klog.V(5).InfoS("volumeNameFromRequestParameters: namePrefix", namePrefix, "nameSuffix", nameSuffix)

	ns := strings.TrimSpace(params[pvcNamespaceParamKey])
	name := strings.TrimSpace(params[pvcNameParamKey])
	klog.V(5).InfoS("volumeNameFromRequestParameters: ns", ns, "name", name)
	if ns == "" || name == "" {
		return "", false
	}

	// Include namespace to reduce collisions across namespaces.
	return fmt.Sprintf("%sns-%s--pvc-%s%s", namePrefix, ns, name, nameSuffix), true
}

func buildProvisionedName(name string) (string, error) {
	name = sanitizeVolumeName(name)
	if name == "" {
		return "", fmt.Errorf("volume name results in an empty name after sanitization")
	}

	if len(name) > 128 {
		name = name[:128]
		name = strings.TrimRight(name, "-")
	}

	return name, nil
}

func sanitizeVolumeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.ReplaceAll(s, ".", "-")
	s = volumeNameAllowed.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	return s
}
