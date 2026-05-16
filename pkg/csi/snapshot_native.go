package csi

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
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
	"k8s.io/klog/v2"
)

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

	var ns uuid.UUID
	var err error
	if uuidNamespace != "" {
		ns, err = uuid.Parse(uuidNamespace)
		if err != nil {
			return "", fmt.Errorf("invalid uuid namespace: %w", err)
		}
	} else {
		ns = uuid.NameSpaceURL
	}

	input := namespace + "/" + name
	u := uuid.NewSHA1(ns, []byte(input))
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

// computeHoldTag creates a deterministic short hold tag from namespace/name
func computeHoldTag(namespace, name string) string {
	h := sha1.Sum([]byte(namespace + "/" + name))
	hexs := hex.EncodeToString(h[:])
	return "pvecsi-" + hexs[:12]
}

// createSnapshotNative implements the native ZFS snapshot creation path.
func createSnapshotNative(ctx context.Context, d *ControllerService, cl *goproxmox.APIClient, pxCfg *pxpool.ProxmoxCluster, vol *volume.Volume, reqName string, params map[string]string) (*csi.CreateSnapshotResponse, error) {
	// get storage config
	storageConfig, err := cl.Client.ClusterStorage(ctx, vol.Storage())
	if err != nil {
		klog.ErrorS(err, "createSnapshotNative: failed to get proxmox storage config", "cluster", vol.Cluster(), "storageID", vol.Storage())
		return nil, status.Error(codes.Internal, err.Error())
	}

	if storageConfig.Type != "zfspool" && storageConfig.Type != "zfs" {
		return nil, status.Error(codes.Internal, "storage is not zfs type")
	}

	rootPool := storageConfig.Pool
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

	// build ssh config (defensive if pxCfg is nil)
	var sshUser, sshPasswordFile, sshPrivateKeyFile string
	var sshPort int
	var sshUseSudo bool
	if pxCfg != nil {
		sshUser = pxCfg.SSHUser
		sshPasswordFile = pxCfg.SSHPasswordFile
		sshPrivateKeyFile = pxCfg.SSHPrivateKeyFile
		sshPort = pxCfg.SSHPort
		sshUseSudo = pxCfg.SSHUseSudo
	}
	sshCfg := sshclient.SSHClientConfig{
		User:           sshUser,
		PasswordFile:   sshPasswordFile,
		PrivateKeyFile: sshPrivateKeyFile,
		Port:           sshPort,
		UseSudo:        sshUseSudo,
	}
	if sshCfg.User == "" {
		sshCfg.User = "root"
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
	if timestampFormat == "" && pxCfg.DefaultTimestampFormat != "" {
		timestampFormat = pxCfg.DefaultTimestampFormat
	}

	vsNamespace := params["vsNamespace"]
	vsName := reqName
	if params["vsName"] != "" {
		vsName = params["vsName"]
	}

	uuidNs := pxCfg.UUIDNamespace

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

	// build SnapshotId encoding policy and hold tag as suffix to the volume-style ID
	// format: region/zone/storage/<dataset>@<snapName>|policy=...|hold=...
	baseID := fmt.Sprintf("%s/%s/%s/%s", vol.Cluster(), vol.Zone(), vol.Storage(), fullSnap)
	metaParts := []string{}
	if policy != "" {
		metaParts = append(metaParts, fmt.Sprintf("policy=%s", policy))
	}
	if holdTag != "" {
		metaParts = append(metaParts, fmt.Sprintf("hold=%s", holdTag))
	}
	if len(metaParts) > 0 {
		baseID = baseID + "|" + strings.Join(metaParts, "|")
	}

	klog.V(3).InfoS("createSnapshotNative: snapshot created", "snapshotID", baseID, "dataset@snapshot", fullSnap)

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			CreationTime:   timestamppbNow(),
			SnapshotId:     baseID,
			SourceVolumeId: vol.VolumeID(),
			SizeBytes:      0,
			ReadyToUse:     true,
		},
	}, nil
}

// deleteSnapshotNative implements native deletion honoring policies encoded into the SnapshotId
func deleteSnapshotNative(ctx context.Context, d *ControllerService, snapshotID string) (*csi.DeleteSnapshotResponse, error) {
	// parse snapshotID: region/zone/storage/<dataset>@<snapName>[|meta]
	parts := strings.SplitN(snapshotID, "/", 4)
	if len(parts) != 4 {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot id")
	}
	region := parts[0]
	storage := parts[2]
	datasetAndMeta := parts[3]

	// split meta
	mainAndMeta := strings.SplitN(datasetAndMeta, "|", 2)
	main := mainAndMeta[0]
	meta := ""
	if len(mainAndMeta) == 2 {
		meta = mainAndMeta[1]
	}

	// main is dataset@snap
	datasetSnap := main

	cl, err := d.pxpool.GetProxmoxCluster(region)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	pxCfg, _ := d.pxpool.GetProxmoxClusterConfig(region)

	// determine node
	node := ""
	// try to derive node from dataset path by asking nodes for storage
	nodes, err := cl.GetNodesForStorage(ctx, storage)
	if err == nil && len(nodes) > 0 {
		node = nodes[0]
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

	var sshUser, sshPasswordFile, sshPrivateKeyFile string
	var sshPort int
	var sshUseSudo bool
	if pxCfg != nil {
		sshUser = pxCfg.SSHUser
		sshPasswordFile = pxCfg.SSHPasswordFile
		sshPrivateKeyFile = pxCfg.SSHPrivateKeyFile
		sshPort = pxCfg.SSHPort
		sshUseSudo = pxCfg.SSHUseSudo
	}
	sshCfg := sshclient.SSHClientConfig{
		User:           sshUser,
		PasswordFile:   sshPasswordFile,
		PrivateKeyFile: sshPrivateKeyFile,
		Port:           sshPort,
		UseSudo:        sshUseSudo,
	}
	if sshCfg.User == "" {
		sshCfg.User = "root"
	}

	client, err := sshclient.NewSSHClient(host, sshCfg)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// parse meta
	policy := "delete"
	holdTag := ""
	if meta != "" {
		for _, kv := range strings.Split(meta, "|") {
			if strings.HasPrefix(kv, "policy=") {
				policy = strings.TrimPrefix(kv, "policy=")
			} else if strings.HasPrefix(kv, "hold=") {
				holdTag = strings.TrimPrefix(kv, "hold=")
			}
		}
	}

	// perform actions based on policy
	snapshotRef := datasetSnap

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

// listSnapshotsNative lists snapshots under root pool. For simplicity this lists snapshots under provided storage root.
func listSnapshotsNative(ctx context.Context, d *ControllerService, cl *goproxmox.APIClient, pxCfg *pxpool.ProxmoxCluster, storage string) (*csi.ListSnapshotsResponse, error) {
	storageConfig, err := cl.Client.ClusterStorage(ctx, storage)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	rootPool := storageConfig.Pool
	if rootPool == "" {
		return nil, status.Error(codes.Internal, "storage pool (root dataset) not found in storage config")
	}

	// pick first node for listing
	nodes, err := cl.GetNodesForStorage(ctx, storage)
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

	var sshUser, sshPasswordFile, sshPrivateKeyFile string
	var sshPort int
	var sshUseSudo bool
	if pxCfg != nil {
		sshUser = pxCfg.SSHUser
		sshPasswordFile = pxCfg.SSHPasswordFile
		sshPrivateKeyFile = pxCfg.SSHPrivateKeyFile
		sshPort = pxCfg.SSHPort
		sshUseSudo = pxCfg.SSHUseSudo
	}
	sshCfg := sshclient.SSHClientConfig{
		User:           sshUser,
		PasswordFile:   sshPasswordFile,
		PrivateKeyFile: sshPrivateKeyFile,
		Port:           sshPort,
		UseSudo:        sshUseSudo,
	}
	if sshCfg.User == "" {
		sshCfg.User = "root"
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
		// build snapshot id in driver format: region unknown here so leave empty region placeholder
		id := fmt.Sprintf("%s/%s/%s/%s", "", "", storage, datasetSnap)
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
