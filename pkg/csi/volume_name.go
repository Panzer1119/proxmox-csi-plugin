package csi

import (
	"fmt"
	"regexp"
	"strings"

	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	PVCVolumeNameAnnotationKeys = DriverName + "/volumeName"
)

var volumeNameAllowed = regexp.MustCompile(`[^a-z0-9-]+`)

func resolveProvisionedVolumeName(ctx context.Context, kclient kubernetes.Interface, pvName string) (string, error) {
	// pvName is request.GetName() (usually "pvc-<uuid>") and is always available.
	if kclient == nil {
		return pvName, nil
	}

	// Try PVC annotation (requires Kubernetes API lookup)
	if name, err := volumeNameFromPVCAnnotation(ctx, kclient, pvName); err != nil {
		return "", err
	} else if strings.TrimSpace(name) != "" {
		return buildProvisionedName(name)
	}
	return pvName, nil
}

func volumeNameFromPVCAnnotation(ctx context.Context, kclient kubernetes.Interface, pvName string) (string, error) {
	pv, err := kclient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		// If we cannot read the PV, just ignore and fall back.
		return "", nil
	}

	if pv.Spec.ClaimRef == nil {
		return "", nil
	}

	ns := pv.Spec.ClaimRef.Namespace
	name := pv.Spec.ClaimRef.Name
	if ns == "" || name == "" {
		return "", nil
	}

	pvc, err := kclient.CoreV1().PersistentVolumeClaims(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", nil
	}

	ann := pvc.GetAnnotations()
	if ann == nil {
		return "", nil
	}

	base := strings.TrimSpace(ann[PVCVolumeNameAnnotationKeys])
	if base == "" {
		return "", nil
	}

	// Include namespace to reduce collisions across namespaces.
	return fmt.Sprintf("pvc-%s-%s", ns, base), nil
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
