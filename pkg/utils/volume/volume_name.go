package volume

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	nameutil "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/name"
	"k8s.io/klog/v2"
)

const (
	// Provided by external-provisioner when started with --extra-create-metadata
	// https://kubernetes-csi.github.io/docs/external-provisioner.html#persistentvolumeclaim-and-persistentvolume-parameters
	PVCNamespaceParamKey = "csi.storage.k8s.io/pvc/namespace"
	PVCNameParamKey      = "csi.storage.k8s.io/pvc/name"
	PVNameParamKey       = "csi.storage.k8s.io/pv/name"
	MaxVolumeNameLength  = 128
)

type NameTemplateContext struct {
	VMID          int
	UUID          string
	PVCNamespace  string
	PVCName       string
	PVName        string
	RequestedName string
}

// NameOptions contains the subset of storage settings used for provisioning volume names.
type NameOptions struct {
	Prefix        string
	Suffix        string
	Template      string
	UUIDNamespace string
}

// ResolveProvisionedVolumeName determines the final volume name for a CSI
// CreateVolume request. It prefers PVC-derived names when available and
// falls back to the provisioner-generated PV name otherwise.
func ResolveProvisionedVolumeName(requestedName string, requestParameters map[string]string, opts NameOptions, vmID int) (string, error) {
	klog.V(5).InfoS("ResolveProvisionedVolumeName: provisioner persistent volume name", "requestedName", requestedName)

	klog.V(5).InfoS("ResolveProvisionedVolumeName: request parameters", "requestParameters", requestParameters)
	klog.V(5).InfoS("ResolveProvisionedVolumeName: storage parameters", "storageParameters", opts)
	klog.V(5).InfoS("ResolveProvisionedVolumeName: vmID", "vmID", vmID)

	prefix := strings.TrimSpace(opts.Prefix)
	suffix := strings.TrimSpace(opts.Suffix)
	klog.V(5).InfoS(
		"ResolveProvisionedVolumeName: name prefix/suffix",
		"prefix", prefix,
		"suffix", suffix,
	)

	rawName, ok, err := NameFromParameters(requestedName, requestParameters, opts, vmID)
	if err == nil && ok {
		klog.V(5).InfoS("ResolveProvisionedVolumeName: derived raw volume name", "name", rawName)

		finalName, err := nameutil.NormalizeName(rawName, MaxVolumeNameLength)
		if err != nil {
			return "", err
		}

		klog.V(5).InfoS("ResolveProvisionedVolumeName: final volume name", "name", finalName)
		return prefix + finalName + suffix, nil
	}

	// Fallback to provisioner-generated PV name
	return prefix + requestedName + suffix, nil
}

func NameFromParameters(requestedName string, requestParameters map[string]string, opts NameOptions, vmID int) (string, bool, error) {
	if requestParameters == nil {
		klog.V(5).InfoS("NameFromParameters: no request parameters provided")
		return "", false, nil
	}
	pvcNamespace := strings.TrimSpace(requestParameters[PVCNamespaceParamKey])
	pvcName := strings.TrimSpace(requestParameters[PVCNameParamKey])
	pvName := strings.TrimSpace(requestParameters[PVNameParamKey])
	klog.V(5).InfoS(
		"NameFromParameters: PVC/PV metadata",
		"pvcNamespace", pvcNamespace,
		"pvcName", pvcName,
		"pvName", pvName,
	)

	if pvcNamespace == "" || pvcName == "" {
		klog.V(5).InfoS("NameFromParameters: missing PVC metadata")
		return "", false, nil
	}

	nameUUID, err := nameutil.DeriveUUID(opts.UUIDNamespace, pvcNamespace, pvcName)
	if err != nil {
		return "", false, err
	}

	if templateText := strings.TrimSpace(opts.Template); templateText != "" {
		ctx := NameTemplateContext{
			VMID:          vmID,
			UUID:          nameUUID.String(),
			PVCNamespace:  pvcNamespace,
			PVCName:       pvcName,
			PVName:        pvName,
			RequestedName: requestedName,
		}
		klog.V(5).InfoS("NameFromParameters: rendering volume name from template", "template", templateText, "context", ctx)

		rendered, err := nameutil.RenderTemplate(templateText, ctx)
		if err == nil {
			klog.V(5).InfoS("NameFromParameters: rendered volume name from template", "name", rendered)
			return rendered, true, nil
		}
		klog.ErrorS(err, "NameFromParameters: failed to render volume name from template", "template", templateText, "context", ctx)
	}

	if nameUUID != uuid.Nil {
		klog.V(5).InfoS("NameFromParameters: using volume name derived from UUID", "name", nameUUID)
		return nameUUID.String(), true, nil
	}

	klog.V(5).InfoS("NameFromParameters: using default PVC-based volume name")
	return fmt.Sprintf("ns-%s-pvc-%s", pvcNamespace, pvcName), true, nil
}
