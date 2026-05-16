package csi

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"text/template"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	goproxmox "github.com/sergelogvinov/go-proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	sshclient "github.com/sergelogvinov/proxmox-csi-plugin/pkg/ssh"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

type nativeSnapshotID struct {
	Region   string            `json:"region"`
	Zone     string            `json:"zone"`
	Storage  string            `json:"storage"`
	Dataset  string            `json:"dataset"`
	Snapshot string            `json:"snapshot"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (n nativeSnapshotID) String() (string, error) {
	data, err := json.Marshal(n)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func parseNativeSnapshotID(id string) (*nativeSnapshotID, error) {
	ref := &nativeSnapshotID{}
	if err := json.Unmarshal([]byte(id), ref); err != nil {
		return nil, err
	}
	if ref.Region == "" || ref.Storage == "" || ref.Dataset == "" || ref.Snapshot == "" {
		return nil, fmt.Errorf("invalid native snapshot id")
	}
	return ref, nil
}

func nativeUUIDInput(namespace, name string) string {
	return fmt.Sprintf("%d:%s|%d:%s", len(namespace), namespace, len(name), name)
}

func nativeUUID(namespace, name string) (uuid.UUID, error) {
	var ns uuid.UUID
	var err error
	if namespace != "" {
		ns, err = uuid.Parse(namespace)
		if err != nil {
			return uuid.Nil, fmt.Errorf("invalid uuid namespace: %w", err)
		}
	} else {
		ns = uuid.NameSpaceURL
	}

	return uuid.NewSHA1(ns, []byte(name)), nil
}

// generateSnapshotName renders the template with provided fields and returns the snapshot name.
func generateSnapshotName(tmplStr, timestampFormat, namespace, name, uuidNamespace string) (string, error) {
	if tmplStr == "" {
		tmplStr = "vs-{{ .Timestamp }}-{{ .UUID }}"
	}
	if timestampFormat == "" {
		timestampFormat = "20060102T150405Z"
	}

	now := time.Now().UTC()
	ts := now.Format(timestampFormat)

	u, err := nativeUUID(uuidNamespace, nativeUUIDInput(namespace, name))
	if err != nil {
		return "", err
	}
	data := map[string]string{
		"Timestamp": ts,
		"UUID":      u.String(),
		"Namespace": namespace,
		"Name":      name,
	}

	tmpl, err := template.New("snapname").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed render template: %w", err)
	}

	res := buf.String()
	// basic sanitize: no whitespace
	if strings.ContainsAny(res, " \t\n") {
		return "", fmt.Errorf("snapshot name contains whitespace")
	}
	return res, nil
}

// computeHoldTag creates a deterministic full-length hold tag from namespace/name.
func computeHoldTag(namespace, name string) string {
	u, err := nativeUUID(uuid.NameSpaceOID.String(), nativeUUIDInput(namespace, name))
	if err != nil {
		h := sha1.Sum([]byte(namespace + "/" + name))
		return "pvecsi-" + hex.EncodeToString(h[:])
	}
	return "pvecsi-" + u.String()
}

func resolveSecretString(ctx context.Context, kclient kubernetes.Interface, ref *pxpool.SecretKeyRef) (string, error) {
	if ref == nil || ref.Name == "" {
		return "", nil
	}
	if kclient == nil {
		return "", fmt.Errorf("kubernetes client is required to resolve secret reference %s", ref.Name)
	}

	namespace := ref.Namespace
	if namespace == "" {
		namespace = "default"
	}

	secret, err := kclient.CoreV1().Secrets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if len(secret.Data) == 0 {
		return "", fmt.Errorf("secret %s/%s has no data", namespace, ref.Name)
	}

	if ref.Key != "" {
		if val, ok := secret.Data[ref.Key]; ok {
			return strings.TrimSpace(string(val)), nil
		}
		return "", fmt.Errorf("secret %s/%s does not contain key %q", namespace, ref.Name, ref.Key)
	}

	if val, ok := secret.Data["ssh-privatekey"]; ok {
		return strings.TrimSpace(string(val)), nil
	}
	if val, ok := secret.Data["password"]; ok {
		return strings.TrimSpace(string(val)), nil
	}
	for _, val := range secret.Data {
		return strings.TrimSpace(string(val)), nil
	}

	return "", fmt.Errorf("secret %s/%s does not contain usable data", namespace, ref.Name)
}

func buildSSHClientConfig(ctx context.Context, d *ControllerService, pxCfg *pxpool.ProxmoxCluster) (sshclient.SSHClientConfig, error) {
	sshCfg := sshclient.SSHClientConfig{}
	if pxCfg != nil {
		sshCfg.User = "root"
		return sshCfg, nil
	}

	sshCfg.User = pxCfg.SSHUser
	if sshCfg.User == "" {
		sshCfg.User = "root"
	}
	sshCfg.Port = pxCfg.SSHPort
	sshCfg.UseSudo = pxCfg.SSHUseSudo

	if pxCfg.SSHPrivateKeySecretRef != nil {
		val, err := resolveSecretString(ctx, d.kclient, pxCfg.SSHPrivateKeySecretRef)
		if err != nil {
			return sshclient.SSHClientConfig{}, err
		}
		sshCfg.PrivateKey = val
	} else {
		sshCfg.PrivateKeyFile = pxCfg.SSHPrivateKeyFile
	}

	if pxCfg.SSHPasswordSecretRef != nil {
		val, err := resolveSecretString(ctx, d.kclient, pxCfg.SSHPasswordSecretRef)
		if err != nil {
			return sshclient.SSHClientConfig{}, err
		}
		sshCfg.Password = val
	} else {
		sshCfg.PasswordFile = pxCfg.SSHPasswordFile
	}

	return sshCfg, nil
}

// createSnapshotNative implements the native ZFS snapshot creation path.
func createSnapshotNative(ctx context.Context, d *ControllerService, cl *goproxmox.APIClient, pxCfg *pxpool.ProxmoxCluster, vol *volume.Volume, reqName string, params map[string]string) (*csi.CreateSnapshotResponse, error) {
	// get storage config
	storageConfig, err := cl.Client.ClusterStorage(ctx, vol.Storage())
	if err != nil {
		klog.ErrorS(err, "createSnapshotNative: failed to get proxmox storage config", "cluster", vol.Cluster(), "storageID", vol.Storage())
		return nil, status.Error(codes.Internal, err.Error())
	}
	// detect storage type and pool via reflection to be resilient against client struct differences
	var storageType string
	var rootPool string
	sv := reflect.ValueOf(storageConfig)
	if sv.IsValid() {
		if sv.Kind() == reflect.Struct {
			f := sv.FieldByName("Type")
			if f.IsValid() && f.Kind() == reflect.String {
				storageType = f.String()
			}
			p := sv.FieldByName("Pool")
			if p.IsValid() && p.Kind() == reflect.String {
				rootPool = p.String()
			}
		} else if sv.Kind() == reflect.Map {
			key := reflect.ValueOf("type")
			v := sv.MapIndex(key)
			if v.IsValid() {
				storageType = fmt.Sprintf("%v", v.Interface())
			}
			key2 := reflect.ValueOf("pool")
			v2 := sv.MapIndex(key2)
			if v2.IsValid() {
				rootPool = fmt.Sprintf("%v", v2.Interface())
			}
		}
	}

	if storageType != "zfspool" && storageType != "zfs" {
		return nil, status.Error(codes.Internal, "storage is not zfs type")
	}
	if rootPool == "" {
		return nil, status.Error(codes.Internal, "storage pool (root dataset) not found in storage config")
	}

	// determine node hosting the volume
	node := vol.Node()
	if node == "" {
		nodes, err := cl.GetNodesForStorage(ctx, vol.Storage())
		if err != nil || len(nodes) == 0 {
			return nil, status.Error(codes.Internal, "failed to determine node for storage")
		}
		node = nodes[0]
	}

	// resolve host for SSH
	host := node
	if pxCfg != nil && pxCfg.NodeHostMap != nil {
		if h, ok := pxCfg.NodeHostMap[node]; ok && h != "" {
			host = h
		}
	}

	sshCfg, err := buildSSHClientConfig(ctx, d, pxCfg)
	if err != nil {
		klog.ErrorS(err, "createSnapshotNative: failed to build ssh config")
		return nil, status.Error(codes.Internal, err.Error())
	}

	client, err := sshclient.NewSSHClient(host, sshCfg)
	if err != nil {
		klog.ErrorS(err, "createSnapshotNative: failed to create ssh client", "host", host)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// find dataset for the volume
	disk := vol.Disk()
	// strip extension if present
	diskNoFmt := disk
	if idx := strings.LastIndex(disk, "."); idx != -1 {
		diskNoFmt = disk[:idx]
	}

	candidate := fmt.Sprintf("%s/%s", rootPool, diskNoFmt)
	// check existence
	cmd := fmt.Sprintf("zfs list -H -t volume '%s' >/dev/null 2>&1 && echo ok || echo no", candidate)
	out, _, err := client.Run(ctx, cmd)
	if err != nil || strings.TrimSpace(out) != "ok" {
		// fallback: search under rootPool
		search := fmt.Sprintf("zfs list -H -t volume -r '%s' -o name | grep -F '%s' || true", rootPool, diskNoFmt)
		out2, _, err2 := client.Run(ctx, search)
		if err2 != nil || strings.TrimSpace(out2) == "" {
			klog.InfoS("createSnapshotNative: dataset not found via ssh", "candidate", candidate, "searchOut", out2, "err", err2)
			return nil, status.Error(codes.NotFound, "zfs dataset for volume not found")
		}
		// take first match
		lines := strings.Split(strings.TrimSpace(out2), "\n")
		candidate = strings.TrimSpace(lines[0])
	}

	snapshotTemplate := params["zfsSnapshotNameTemplate"]
	timestampFormat := params["zfsTimestampFormat"]
	if timestampFormat == "" && pxCfg != nil && pxCfg.DefaultTimestampFormat != "" {
		timestampFormat = pxCfg.DefaultTimestampFormat
	}

	vsNamespace := params["vsNamespace"]
	vsName := reqName
	if params["vsName"] != "" {
		vsName = params["vsName"]
	}

	uuidNs := ""
	if pxCfg != nil {
		uuidNs = pxCfg.UUIDNamespace
	}

	snapName, err := generateSnapshotName(snapshotTemplate, timestampFormat, vsNamespace, vsName, uuidNs)
	if err != nil {
		klog.ErrorS(err, "createSnapshotNative: failed to generate snapshot name")
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	fullSnap := fmt.Sprintf("%s@%s", candidate, snapName)
	createCmd := fmt.Sprintf("zfs snapshot '%s'", fullSnap)
	if _, stderr, err := client.Run(ctx, createCmd); err != nil {
		klog.ErrorS(err, "createSnapshotNative: zfs snapshot failed", "cmd", createCmd, "stderr", stderr)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create zfs snapshot: %v", err))
	}

	policy := params["zfsSnapshotDeletePolicy"]
	if policy == "" {
		policy = "delete"
	}

	holdTag := ""
	if policy == "release" || policy == "release-destroy" {
		// compute hold tag
		holdTag = computeHoldTag(vsNamespace, vsName)
		holdCmd := fmt.Sprintf("zfs hold %s '%s'", holdTag, fullSnap)
		if _, stderr, err := client.Run(ctx, holdCmd); err != nil {
			klog.ErrorS(err, "createSnapshotNative: zfs hold failed", "cmd", holdCmd, "stderr", stderr)
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to hold zfs snapshot: %v", err))
		}
	}

	zone := vol.Zone()
	if zone == "" {
		zone = node
	}

	id, err := (nativeSnapshotID{
		Region:   vol.Cluster(),
		Zone:     zone,
		Storage:  vol.Storage(),
		Dataset:  candidate,
		Snapshot: snapName,
		Metadata: map[string]string{"policy": policy, "hold": holdTag},
	}).String()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.V(3).InfoS("createSnapshotNative: snapshot created", "snapshotID", id, "dataset@snapshot", fullSnap)

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			CreationTime:   timestamppbNow(),
			SnapshotId:     id,
			SourceVolumeId: vol.VolumeID(),
			SizeBytes:      0,
			ReadyToUse:     true,
		},
	}, nil
}

// deleteSnapshotNative implements native deletion honoring policy/hold metadata in the SnapshotId payload.
func deleteSnapshotNative(ctx context.Context, d *ControllerService, snapshotID string) (*csi.DeleteSnapshotResponse, error) {
	ref, err := parseNativeSnapshotID(snapshotID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid native snapshot id")
	}

	cl, err := d.pxpool.GetProxmoxCluster(ref.Region)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	pxCfg, _ := d.pxpool.GetProxmoxClusterConfig(ref.Region)

	// determine node
	node := ref.Zone
	if node == "" {
		// try to derive node from storage membership
		nodes, err := cl.GetNodesForStorage(ctx, ref.Storage)
		if err == nil && len(nodes) > 0 {
			node = nodes[0]
		}
	}
	if node == "" {
		return nil, status.Error(codes.Internal, "cannot determine node for dataset")
	}

	host := node
	if pxCfg != nil && pxCfg.NodeHostMap != nil {
		if h, ok := pxCfg.NodeHostMap[node]; ok && h != "" {
			host = h
		}
	}

	sshCfg, err := buildSSHClientConfig(ctx, d, pxCfg)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	client, err := sshclient.NewSSHClient(host, sshCfg)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	policy := "delete"
	holdTag := ""
	if ref.Metadata != nil {
		if v := ref.Metadata["policy"]; v != "" {
			policy = v
		}
		holdTag = ref.Metadata["hold"]
	}

	// perform actions based on policy
	snapshotRef := fmt.Sprintf("%s@%s", ref.Dataset, ref.Snapshot)

	switch policy {
	case "delete":
		cmd := fmt.Sprintf("zfs destroy '%s'", snapshotRef)
		if _, stderr, err := client.Run(ctx, cmd); err != nil {
			// if not found, return success
			if strings.Contains(stderr, "dataset does not exist") || strings.Contains(stderr, "cannot open") {
				return &csi.DeleteSnapshotResponse{}, nil
			}
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to destroy snapshot: %v", err))
		}
	case "release":
		if holdTag == "" {
			return nil, status.Error(codes.InvalidArgument, "hold tag missing for release policy")
		}
		cmd := fmt.Sprintf("zfs release %s '%s'", holdTag, snapshotRef)
		if _, stderr, err := client.Run(ctx, cmd); err != nil {
			if strings.Contains(stderr, "not found") {
				return &csi.DeleteSnapshotResponse{}, nil
			}
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to release hold: %v", err))
		}
	case "release-destroy":
		if holdTag == "" {
			return nil, status.Error(codes.InvalidArgument, "hold tag missing for release-destroy policy")
		}
		cmd := fmt.Sprintf("zfs release %s '%s'", holdTag, snapshotRef)
		if _, stderr, err := client.Run(ctx, cmd); err != nil {
			klog.InfoS("deleteSnapshotNative: release failed, continuing to destroy if possible", "err", err, "stderr", stderr)
		}
		cmd = fmt.Sprintf("zfs destroy '%s'", snapshotRef)
		if _, stderr, err := client.Run(ctx, cmd); err != nil {
			if strings.Contains(stderr, "dataset does not exist") {
				return &csi.DeleteSnapshotResponse{}, nil
			}
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to destroy snapshot: %v", err))
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown policy")
	}

	klog.V(3).InfoS("deleteSnapshotNative: snapshot deleted or released", "snapshot", snapshotID, "policy", policy)
	return &csi.DeleteSnapshotResponse{}, nil
}

// listSnapshotsNative lists snapshots under the storage root and returns JSON snapshot IDs.
func listSnapshotsNative(ctx context.Context, d *ControllerService, cl *goproxmox.APIClient, pxCfg *pxpool.ProxmoxCluster, vol *volume.Volume) (*csi.ListSnapshotsResponse, error) {
	storageConfig, err := cl.Client.ClusterStorage(ctx, vol.Storage())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// get rootPool via reflection
	var rootPool string
	sv := reflect.ValueOf(storageConfig)
	if sv.IsValid() {
		if sv.Kind() == reflect.Struct {
			p := sv.FieldByName("Pool")
			if p.IsValid() && p.Kind() == reflect.String {
				rootPool = p.String()
			}
		} else if sv.Kind() == reflect.Map {
			v2 := sv.MapIndex(reflect.ValueOf("pool"))
			if v2.IsValid() {
				rootPool = fmt.Sprintf("%v", v2.Interface())
			}
		}
	}
	if rootPool == "" {
		return nil, status.Error(codes.Internal, "storage pool (root dataset) not found in storage config")
	}

	// pick first node for listing
	nodes, err := cl.GetNodesForStorage(ctx, vol.Storage())
	if err != nil || len(nodes) == 0 {
		return nil, status.Error(codes.Internal, "failed to determine node for storage")
	}
	node := nodes[0]
	host := node
	if pxCfg != nil && pxCfg.NodeHostMap != nil {
		if h, ok := pxCfg.NodeHostMap[node]; ok && h != "" {
			host = h
		}
	}

	sshCfg, err := buildSSHClientConfig(ctx, d, pxCfg)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	client, err := sshclient.NewSSHClient(host, sshCfg)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	cmd := fmt.Sprintf("zfs list -H -t snapshot -o name,creation -r '%s'", rootPool)
	out, _, err := client.Run(ctx, cmd)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to list zfs snapshots: %v", err))
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	entries := []*csi.ListSnapshotsResponse_Entry{}
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			continue
		}
		// each line: dataset@snap\tcreation
		parts := strings.Fields(l)
		if len(parts) < 1 {
			continue
		}
		datasetSnap := parts[0]
		pair := strings.SplitN(datasetSnap, "@", 2)
		if len(pair) != 2 || pair[0] == "" || pair[1] == "" {
			continue
		}
		zone := vol.Zone()
		if zone == "" {
			zone = node
		}
		ref := nativeSnapshotID{
			Region:   vol.Cluster(),
			Zone:     zone,
			Storage:  vol.Storage(),
			Dataset:  pair[0],
			Snapshot: pair[1],
		}
		id, err := ref.String()
		if err != nil {
			continue
		}
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     id,
				SourceVolumeId: "",
				CreationTime:   timestamppbNow(),
				ReadyToUse:     true,
			},
		})
	}

	return &csi.ListSnapshotsResponse{Entries: entries}, nil
}

// helper to return current timestamppb
func timestamppbNow() *timestamppb.Timestamp {
	return &timestamppb.Timestamp{Seconds: time.Now().Unix(), Nanos: int32(time.Now().Nanosecond())}
}

// createVolumeFromNativeSnapshot performs zfs clone on the storage to create a new dataset for the volume
func createVolumeFromNativeSnapshot(ctx context.Context, d *ControllerService, cl *goproxmox.APIClient, pxCfg *pxpool.ProxmoxCluster, srcSnapshotID string, dest *volume.Volume) error {
	ref, err := parseNativeSnapshotID(srcSnapshotID)
	if err != nil {
		return err
	}
	if ref.Region != dest.Region() {
		return fmt.Errorf("snapshot region does not match destination region")
	}
	if ref.Storage != dest.Storage() {
		return fmt.Errorf("snapshot storage does not match destination storage")
	}

	// get root pool for destination
	storageConfig, err := cl.Client.ClusterStorage(ctx, dest.Storage())
	if err != nil {
		return err
	}
	var rootPool string
	sv := reflect.ValueOf(storageConfig)
	if sv.IsValid() {
		if sv.Kind() == reflect.Struct {
			p := sv.FieldByName("Pool")
			if p.IsValid() && p.Kind() == reflect.String {
				rootPool = p.String()
			}
		} else if sv.Kind() == reflect.Map {
			v2 := sv.MapIndex(reflect.ValueOf("pool"))
			if v2.IsValid() {
				rootPool = fmt.Sprintf("%v", v2.Interface())
			}
		}
	}
	if rootPool == "" {
		return fmt.Errorf("destination storage root pool not found")
	}

	// determine dest dataset
	disk := dest.Disk()
	diskNoFmt := disk
	if idx := strings.LastIndex(disk, "."); idx != -1 {
		diskNoFmt = disk[:idx]
	}
	destDataset := fmt.Sprintf("%s/%s", rootPool, diskNoFmt)

	// determine node and host
	node := dest.Node()
	if node == "" {
		nodes, err := cl.GetNodesForStorage(ctx, dest.Storage())
		if err != nil || len(nodes) == 0 {
			return fmt.Errorf("failed to determine node for storage")
		}
		node = nodes[0]
	}
	host := node
	if pxCfg != nil && pxCfg.NodeHostMap != nil {
		if h, ok := pxCfg.NodeHostMap[node]; ok && h != "" {
			host = h
		}
	}

	sshCfg, err := buildSSHClientConfig(ctx, d, pxCfg)
	if err != nil {
		return err
	}
	client, err := sshclient.NewSSHClient(host, sshCfg)
	if err != nil {
		return err
	}

	src := fmt.Sprintf("%s@%s", ref.Dataset, ref.Snapshot)
	// perform zfs clone
	cmd := fmt.Sprintf("zfs clone '%s' '%s'", src, destDataset)
	if _, stderr, err := client.Run(ctx, cmd); err != nil {
		klog.InfoS("createVolumeFromNativeSnapshot: clone failed, stderr", "stderr", stderr, "err", err)
		// fallback: return error to let caller fall back to copyVolume
		return fmt.Errorf("zfs clone failed: %v", err)
	}

	return nil
}
