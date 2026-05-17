package csi

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	goproxmox "github.com/sergelogvinov/go-proxmox"
	pxpool "github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	sshclient "github.com/sergelogvinov/proxmox-csi-plugin/pkg/ssh"
	snapshot "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/snapshot"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// CreateSnapshotNative implements the native ZFS snapshot creation path.
func CreateSnapshotNative(ctx context.Context, request *csi.CreateSnapshotRequest, d *ControllerService, cl *goproxmox.APIClient, pxCfg *pxpool.ProxmoxCluster, vol *volume.Volume) (*csi.CreateSnapshotResponse, error) {
	// get storage config
	storageConfig, err := cl.Client.ClusterStorage(ctx, vol.Storage())
	if err != nil {
		klog.ErrorS(err, "CreateSnapshotNative: failed to get proxmox storage config", "cluster", vol.Cluster(), "storageID", vol.Storage())
		return nil, status.Error(codes.Internal, err.Error())
	}

	params, err := ExtractVolumeSnapshotParameters(request.GetParameters())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
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

	host, sshCfg, err := buildSSHClientConfig(ctx, d, pxCfg, node)
	if err != nil {
		klog.ErrorS(err, "CreateSnapshotNative: failed to build ssh config")
		return nil, status.Error(codes.Internal, err.Error())
	}

	client, err := sshclient.NewSSHClient(host, sshCfg)
	if err != nil {
		klog.ErrorS(err, "CreateSnapshotNative: failed to create ssh client", "host", host)
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
			klog.InfoS("CreateSnapshotNative: dataset not found via ssh", "candidate", candidate, "searchOut", out2, "err", err2)
			return nil, status.Error(codes.NotFound, "zfs dataset for volume not found")
		}
		// take first match
		lines := strings.Split(strings.TrimSpace(out2), "\n")
		candidate = strings.TrimSpace(lines[0])
	}

	timestampFormat := params.SnapshotNameTimestampFormat
	if timestampFormat == "" {
		if pxCfg != nil && pxCfg.DefaultTimestampFormat != "" {
			timestampFormat = pxCfg.DefaultTimestampFormat
		} else {
			timestampFormat = "20060102T150405Z"
		}
	}

	snapshotName, holdTag, err := snapshot.GenerateSnapshotName(request.GetName(), request.GetParameters(), snapshot.NameOptions{
		Prefix:          params.SnapshotNamePrefix,
		Suffix:          params.SnapshotNameSuffix,
		Template:        params.SnapshotNameTemplate,
		UUIDNamespace:   params.UUIDNamespace,
		TimestampFormat: timestampFormat,
	})
	if err != nil {
		klog.ErrorS(err, "CreateSnapshotNative: failed to generate snapshot name")
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	fullSnapshotName := fmt.Sprintf("%s@%s", candidate, snapshotName)
	createCmd := fmt.Sprintf("zfs snapshot '%s'", fullSnapshotName)
	if _, stderr, err := client.Run(ctx, createCmd); err != nil {
		klog.ErrorS(err, "CreateSnapshotNative: zfs snapshot failed", "cmd", createCmd, "stderr", stderr)
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create zfs snapshot: %v", err))
	}

	policy := params.ZFSSnapshotDeletePolicy

	if policy == "Release" || policy == "ReleaseDelete" {
		holdCmd := fmt.Sprintf("zfs hold %s '%s'", holdTag, fullSnapshotName)
		if _, stderr, err := client.Run(ctx, holdCmd); err != nil {
			klog.ErrorS(err, "CreateSnapshotNative: zfs hold failed", "cmd", holdCmd, "stderr", stderr)
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to hold zfs snapshot: %v", err))
		}
	} else {
		holdTag = ""
	}

	zone := vol.Zone()
	if zone == "" {
		zone = node
	}

	id, err := (snapshot.NativeSnapshotID{
		Region:   vol.Cluster(),
		Zone:     zone,
		Storage:  vol.Storage(),
		Dataset:  candidate,
		Snapshot: snapshotName,
		Metadata: map[string]string{"policy": policy, "hold": holdTag},
	}).String()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.V(3).InfoS("CreateSnapshotNative: snapshot created", "snapshotID", id, "dataset@snapshot", fullSnapshotName)

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			CreationTime:   snapshot.NowTimestamp(),
			SnapshotId:     id,
			SourceVolumeId: vol.VolumeID(),
			SizeBytes:      0,
			ReadyToUse:     true,
		},
	}, nil
}

// deleteSnapshotNative implements native deletion honoring policy/hold metadata in the SnapshotId payload.
func deleteSnapshotNative(ctx context.Context, d *ControllerService, snapshotID string) (*csi.DeleteSnapshotResponse, error) {
	ref, err := snapshot.ParseNativeSnapshotID(snapshotID)
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

	host, sshCfg, err := buildSSHClientConfig(ctx, d, pxCfg, node)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	client, err := sshclient.NewSSHClient(host, sshCfg)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	policy := "Delete"
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
	case "Delete":
		cmd := fmt.Sprintf("zfs destroy '%s'", snapshotRef)
		if _, stderr, err := client.Run(ctx, cmd); err != nil {
			// if not found, return success
			if strings.Contains(stderr, "dataset does not exist") || strings.Contains(stderr, "cannot open") {
				return &csi.DeleteSnapshotResponse{}, nil
			}
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to destroy snapshot: %v", err))
		}
	case "Release":
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
	case "ReleaseDelete":
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
	host, sshCfg, err := buildSSHClientConfig(ctx, d, pxCfg, node)
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
		ref := snapshot.NativeSnapshotID{
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
				CreationTime:   snapshot.NowTimestamp(),
				ReadyToUse:     true,
			},
		})
	}

	return &csi.ListSnapshotsResponse{Entries: entries}, nil
}

// createVolumeFromNativeSnapshot performs zfs clone on the storage to create a new dataset for the volume
func createVolumeFromNativeSnapshot(ctx context.Context, d *ControllerService, cl *goproxmox.APIClient, pxCfg *pxpool.ProxmoxCluster, srcSnapshotID string, dest *volume.Volume) error {
	ref, err := snapshot.ParseNativeSnapshotID(srcSnapshotID)
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
	host, sshCfg, err := buildSSHClientConfig(ctx, d, pxCfg, node)
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
