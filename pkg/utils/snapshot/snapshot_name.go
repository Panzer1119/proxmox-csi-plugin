package snapshot

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	nameutil "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/name"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

const (
	// Provided by external-snapshotter when started with --extra-create-metadata
	// https://kubernetes-csi.github.io/docs/external-snapshotter.html#volumesnapshot-volumesnapshotcontent-volumegroupsnapshot-and-volumegroupsnapshotcontent-parameters
	VSNameParamKey        = "csi.storage.k8s.io/volumesnapshot/name"
	VSNamespaceParamKey   = "csi.storage.k8s.io/volumesnapshot/namespace"
	VSCNameParamKey       = "csi.storage.k8s.io/volumesnapshotcontent/name"
	MaxSnapshotNameLength = 128
)

type NameTemplateContext struct {
	Timestamp     string
	UUID          string
	VSNamespace   string
	VSName        string
	VSCName       string
	RequestedName string
}

// NameOptions contains the subset of storage settings used for generating snapshot names.
type NameOptions struct {
	Prefix          string
	Suffix          string
	Template        string
	UUIDNamespace   string
	TimestampFormat string
}

// GenerateSnapshotName renders the template with provided fields and returns the snapshot name.
func GenerateSnapshotName(requestedName string, requestParameters map[string]string, opts NameOptions) (string, string, error) {
	klog.V(5).InfoS("GenerateSnapshotName: provisioner volume snapshot content name", "requestedName", requestedName)

	klog.V(5).InfoS("GenerateSnapshotName: request parameters", "requestParameters", requestParameters)
	klog.V(5).InfoS("GenerateSnapshotName: storage parameters", "storageParameters", opts)

	prefix := strings.TrimSpace(opts.Prefix)
	suffix := strings.TrimSpace(opts.Suffix)
	klog.V(5).InfoS(
		"GenerateSnapshotName: name prefix/suffix",
		"prefix", prefix,
		"suffix", suffix,
	)

	ts := time.Now().UTC().Format(opts.TimestampFormat)

	rawName, rawHoldTag, ok, err := NameFromParameters(requestedName, requestParameters, opts, ts)
	if err == nil && ok {
		klog.V(5).InfoS("GenerateSnapshotName: derived raw snapshot name", "name", rawName)
		klog.V(5).InfoS("GenerateSnapshotName: derived raw hold tag", "holdTag", rawHoldTag)

		finalName, err := nameutil.NormalizeName(rawName, MaxSnapshotNameLength)
		if err != nil {
			return "", "", err
		}
		finalHoldTag, err := nameutil.NormalizeName(rawHoldTag, MaxSnapshotNameLength)
		if err != nil {
			return "", "", err
		}

		klog.V(5).InfoS("GenerateSnapshotName: final snapshot name", "name", finalName)
		klog.V(5).InfoS("GenerateSnapshotName: final hold tag", "holdTag", finalHoldTag)
		return prefix + finalName + suffix, finalHoldTag, nil
	}

	// Fallback to provisioner-generated VSC name
	return prefix + requestedName + suffix, "pvecsi", nil
}

func NameFromParameters(requestedName string, requestParameters map[string]string, opts NameOptions, timestamp string) (string, string, bool, error) {
	if requestParameters == nil {
		klog.V(5).InfoS("NameFromParameters: no request parameters provided")
		return "", "", false, nil
	}
	vsNamespace := strings.TrimSpace(requestParameters[VSNamespaceParamKey])
	vsName := strings.TrimSpace(requestParameters[VSNameParamKey])
	vscName := strings.TrimSpace(requestParameters[VSCNameParamKey])
	klog.V(5).InfoS(
		"NameFromParameters: VS/VSC metadata",
		"vsNamespace", vsNamespace,
		"vsName", vsName,
		"vscName", vscName,
	)

	if vsNamespace == "" || vsName == "" {
		klog.V(5).InfoS("NameFromParameters: missing VS metadata")
		return "", "", false, nil
	}

	holdTag := ComputeHoldTag(opts.UUIDNamespace, vsNamespace, vsName)

	nameUUID, err := nameutil.DeriveUUID(opts.UUIDNamespace, vsNamespace, vsName)
	if err != nil {
		return "", "", false, err
	}

	if templateText := strings.TrimSpace(opts.Template); templateText != "" {
		ctx := NameTemplateContext{
			Timestamp:     timestamp,
			UUID:          nameUUID.String(),
			VSNamespace:   vsNamespace,
			VSName:        vsName,
			VSCName:       vscName,
			RequestedName: requestedName,
		}
		klog.V(5).InfoS("NameFromParameters: rendering volume name from template", "template", templateText, "context", ctx)

		rendered, err := nameutil.RenderTemplate(templateText, ctx)
		if err == nil {
			klog.V(5).InfoS("NameFromParameters: rendered volume name from template", "name", rendered)
			return rendered, holdTag, true, nil
		}
		klog.ErrorS(err, "NameFromParameters: failed to render volume name from template", "template", templateText, "context", ctx)
	}

	if nameUUID != uuid.Nil {
		klog.V(5).InfoS("NameFromParameters: using volume name derived from UUID", "name", nameUUID)
		return nameUUID.String(), holdTag, true, nil
	}

	klog.V(5).InfoS("NameFromParameters: using default VS-based snapshot name")
	return fmt.Sprintf("vs-%s-%s", timestamp, nameUUID.String()), holdTag, true, nil
}

// ComputeHoldTag creates a deterministic full-length hold tag from namespace/name.
func ComputeHoldTag(uuidNamespace, namespace, name string) string {
	u, err := nameutil.DeriveUUID(uuidNamespace, namespace, name)
	if err != nil {
		h := sha1.Sum([]byte(namespace + "/" + name))
		return "pvecsi-" + hex.EncodeToString(h[:])
	}
	return "pvecsi-" + u.String()
}

// NowTimestamp returns the current timestamp in protobuf form.
func NowTimestamp() *timestamppb.Timestamp {
	return &timestamppb.Timestamp{Seconds: time.Now().Unix(), Nanos: int32(time.Now().Nanosecond())}
}
