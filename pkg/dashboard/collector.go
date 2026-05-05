package dashboard

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goproxmox "github.com/luthermonson/go-proxmox"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	volutil "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// Collector gathers Kubernetes and Proxmox resources
type Collector struct {
	kube    kubernetes.Interface
	proxmox *proxmoxpool.ProxmoxPool
}

type proxmoxDiskUsage struct {
	AttachedVMIDs []string
	AttachedNode  string
}

type proxmoxStorageMeta struct {
	Shared bool
	Nodes  []string
}

// Collect gathers all Kubernetes and Proxmox resources and returns a snapshot
func (c *Collector) Collect(ctx context.Context, store *Store) error {
	ss := Snapshot{GeneratedAt: time.Now().UTC()}
	ss.Kubernetes = c.collectKubernetes(ctx)
	ss.Proxmox = c.collectProxmox(ctx, ss.Kubernetes)
	store.Set(ss)
	return nil
}

// collectKubernetes gathers all Kubernetes resources
func (c *Collector) collectKubernetes(ctx context.Context) Kubernetes {
	k8s := Kubernetes{}

	// Collect nodes
	k8s.Nodes = c.collectKubernetesNodes(ctx)

	// Collect namespaces
	k8s.Namespaces = c.collectNamespaces(ctx)

	// Collect storage classes
	k8s.StorageClasses = c.collectStorageClasses(ctx)

	// Collect persistent volumes
	k8s.PersistentVolumes = c.collectPersistentVolumes(ctx)

	// Collect persistent volume claims
	k8s.PersistentVolumeClaims = c.collectPersistentVolumeClaims(ctx, k8s.StorageClasses)

	// Collect pods
	k8s.Pods = c.collectPods(ctx, k8s.PersistentVolumeClaims)

	return k8s
}

// collectKubernetesNodes gathers Kubernetes nodes
func (c *Collector) collectKubernetesNodes(ctx context.Context) []KubernetesNode {
	nodes, err := c.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "failed to list nodes")
		return nil
	}

	var result []KubernetesNode
	for _, node := range nodes.Items {
		kn := KubernetesNode{
			KubernetesBase: KubernetesBase{
				KubernetesReference: KubernetesReference{
					Kind: "Node",
					Name: node.Name,
					UID:  string(node.UID),
				},
				CreatedAt:   node.CreationTimestamp.Time,
				Annotations: node.Annotations,
				Labels:      node.Labels,
			},
		}
		result = append(result, kn)
	}
	return result
}

// collectNamespaces gathers Kubernetes namespaces
func (c *Collector) collectNamespaces(ctx context.Context) []KubernetesNamespace {
	namespaces, err := c.kube.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "failed to list namespaces")
		return nil
	}

	var result []KubernetesNamespace
	for _, ns := range namespaces.Items {
		isPrivileged := false
		if _, ok := ns.Labels["pod-security.kubernetes.io/enforce"]; ok {
			val := ns.Labels["pod-security.kubernetes.io/enforce"]
			isPrivileged = val == "privileged"
		}

		kns := KubernetesNamespace{
			KubernetesBase: KubernetesBase{
				KubernetesReference: KubernetesReference{
					Kind: "Namespace",
					Name: ns.Name,
					UID:  string(ns.UID),
				},
				CreatedAt:   ns.CreationTimestamp.Time,
				Annotations: ns.Annotations,
				Labels:      ns.Labels,
			},
			IsPrivileged: isPrivileged,
		}
		result = append(result, kns)
	}
	return result
}

// collectStorageClasses gathers Kubernetes storage classes
func (c *Collector) collectStorageClasses(ctx context.Context) []KubernetesStorageClass {
	scs, err := c.kube.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "failed to list storage classes")
		return nil
	}

	var result []KubernetesStorageClass
	for _, sc := range scs.Items {
		isDefault := false
		if val, ok := sc.Annotations["storageclass.kubernetes.io/is-default-class"]; ok {
			isDefault = val == "true"
		}

		reclaimPolicy := "Delete"
		if sc.ReclaimPolicy != nil {
			reclaimPolicy = string(*sc.ReclaimPolicy)
		}

		volumeBindingMode := "Immediate"
		if sc.VolumeBindingMode != nil {
			volumeBindingMode = string(*sc.VolumeBindingMode)
		}

		ksc := KubernetesStorageClass{
			KubernetesBase: KubernetesBase{
				KubernetesReference: KubernetesReference{
					Kind: "StorageClass",
					Name: sc.Name,
					UID:  string(sc.UID),
				},
				CreatedAt:   sc.CreationTimestamp.Time,
				Annotations: sc.Annotations,
				Labels:      sc.Labels,
			},
			Provisioner:          sc.Provisioner,
			ReclaimPolicy:        reclaimPolicy,
			AllowVolumeExpansion: *sc.AllowVolumeExpansion,
			VolumeBindingMode:    volumeBindingMode,
			IsDefault:            isDefault,
		}
		result = append(result, ksc)
	}
	return result
}

// collectPersistentVolumes gathers Kubernetes persistent volumes
func (c *Collector) collectPersistentVolumes(ctx context.Context) []KubernetesPersistentVolume {
	pvs, err := c.kube.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "failed to list persistent volumes")
		return nil
	}

	var result []KubernetesPersistentVolume
	for _, pv := range pvs.Items {
		// Only include volumes from our CSI driver
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csi.DriverName {
			continue
		}

		accessModes := make([]string, len(pv.Spec.AccessModes))
		for i, mode := range pv.Spec.AccessModes {
			accessModes[i] = string(mode)
		}

		storageClassName := ""
		if pv.Spec.StorageClassName != "" {
			storageClassName = pv.Spec.StorageClassName
		}

		mode := "Filesystem"
		if pv.Spec.VolumeMode != nil {
			mode = string(*pv.Spec.VolumeMode)
		}

		var claimRef *KubernetesReference
		if pv.Spec.ClaimRef != nil {
			claimRef = &KubernetesReference{
				Kind:      pv.Spec.ClaimRef.Kind,
				Namespace: pv.Spec.ClaimRef.Namespace,
				Name:      pv.Spec.ClaimRef.Name,
				UID:       string(pv.Spec.ClaimRef.UID),
			}
		}

		var volRef *VolumeReference
		if vol, err := volutil.NewVolumeFromVolumeID(pv.Spec.CSI.VolumeHandle); err == nil {
			volRef = &VolumeReference{
				Region:  vol.Region(),
				Zone:    vol.Zone(),
				Node:    vol.Node(),
				Storage: vol.Storage(),
				Disk:    vol.Disk(),
			}
		}

		bound := pv.Spec.ClaimRef != nil

		kpv := KubernetesPersistentVolume{
			KubernetesBase: KubernetesBase{
				KubernetesReference: KubernetesReference{
					Kind: "PersistentVolume",
					Name: pv.Name,
					UID:  string(pv.UID),
				},
				CreatedAt:   pv.CreationTimestamp.Time,
				Annotations: pv.Annotations,
				Labels:      pv.Labels,
			},
			StorageClassName: storageClassName,
			Bound:            bound,
			AccessMode:       accessModes,
			Capacity:         pv.Spec.Capacity.Storage().String(),
			Mode:             mode,
			ClaimReference:   claimRef,
			VolumeHandle:     pv.Spec.CSI.VolumeHandle,
			VolumeReference:  volRef,
		}
		result = append(result, kpv)
	}
	return result
}

// collectPersistentVolumeClaims gathers Kubernetes persistent volume claims
func (c *Collector) collectPersistentVolumeClaims(ctx context.Context, storageClasses []KubernetesStorageClass) []KubernetesPersistentVolumeClaim {
	pvcs, err := c.kube.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "failed to list persistent volume claims")
		return nil
	}

	// Create a map of storage class names for quick lookup
	scMap := make(map[string]bool)
	for _, sc := range storageClasses {
		scMap[sc.Name] = true
	}

	var result []KubernetesPersistentVolumeClaim
	for _, pvc := range pvcs.Items {
		// Filter to only relevant PVCs
		scName := ""
		if pvc.Spec.StorageClassName != nil {
			scName = *pvc.Spec.StorageClassName
		}

		// Skip if not using our CSI driver
		if scName != "" && !scMap[scName] {
			continue
		}

		accessModes := make([]string, len(pvc.Spec.AccessModes))
		for i, mode := range pvc.Spec.AccessModes {
			accessModes[i] = string(mode)
		}

		volumeMode := "Filesystem"
		if pvc.Spec.VolumeMode != nil {
			volumeMode = string(*pvc.Spec.VolumeMode)
		}

		bound := pvc.Spec.VolumeName != ""

		kpvc := KubernetesPersistentVolumeClaim{
			KubernetesBase: KubernetesBase{
				KubernetesReference: KubernetesReference{
					Kind:      "PersistentVolumeClaim",
					Namespace: pvc.Namespace,
					Name:      pvc.Name,
					UID:       string(pvc.UID),
				},
				CreatedAt:   pvc.CreationTimestamp.Time,
				Annotations: pvc.Annotations,
				Labels:      pvc.Labels,
			},
			StorageClassName: scName,
			Bound:            bound,
			AccessMode:       accessModes,
			CapacityRequest:  pvc.Spec.Resources.Requests.Storage().String(),
			VolumeMode:       volumeMode,
			VolumeName:       pvc.Spec.VolumeName,
		}
		result = append(result, kpvc)
	}
	return result
}

// collectPods gathers Kubernetes pods that use PVCs
func (c *Collector) collectPods(ctx context.Context, pvcs []KubernetesPersistentVolumeClaim) []KubernetesPod {
	pods, err := c.kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "failed to list pods")
		return nil
	}

	// Create a set of PVC names for quick lookup
	pvcSet := make(map[string]bool)
	for _, pvc := range pvcs {
		pvcSet[pvc.Namespace+"/"+pvc.Name] = true
	}

	var result []KubernetesPod
	for _, pod := range pods.Items {
		// Check if pod uses any PVCs
		volumes := make(map[string]string)
		usesPVC := false

		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				pvcKey := pod.Namespace + "/" + vol.PersistentVolumeClaim.ClaimName
				if pvcSet[pvcKey] {
					usesPVC = true
				}
				volumes[vol.Name] = vol.PersistentVolumeClaim.ClaimName
			}
		}

		if !usesPVC {
			continue
		}

		kpod := KubernetesPod{
			KubernetesBase: KubernetesBase{
				KubernetesReference: KubernetesReference{
					Kind:      "Pod",
					Namespace: pod.Namespace,
					Name:      pod.Name,
					UID:       string(pod.UID),
				},
				CreatedAt:   pod.CreationTimestamp.Time,
				Annotations: pod.Annotations,
				Labels:      pod.Labels,
			},
			Hostname: pod.Spec.Hostname,
			NodeName: pod.Spec.NodeName,
			Volumes:  volumes,
		}
		result = append(result, kpod)
	}
	return result
}

// collectProxmox gathers all Proxmox resources
func (c *Collector) collectProxmox(ctx context.Context, k8s Kubernetes) Proxmox {
	proxmox := Proxmox{}
	storageMeta := c.collectProxmoxStorageMeta(ctx, k8s.PersistentVolumes)

	// Determine which regions and zones we need to monitor based on used volumes
	regionsNeeded := c.getRegionsFromVolumes(k8s.PersistentVolumes)

	// Collect Proxmox clusters
	proxmox.Clusters = c.collectProxmoxClusters(ctx, regionsNeeded)

	// Collect Proxmox nodes and disks
	proxmox.Nodes, proxmox.SharedDisks, proxmox.LocalDisks = c.collectProxmoxResources(ctx, k8s, regionsNeeded, storageMeta)

	return proxmox
}

func (c *Collector) collectProxmoxStorageMeta(ctx context.Context, pvs []KubernetesPersistentVolume) map[string]map[string]proxmoxStorageMeta {
	byRegion := map[string]map[string]struct{}{}
	for _, pv := range pvs {
		if pv.VolumeReference == nil {
			continue
		}
		region := pv.VolumeReference.Region
		storage := pv.VolumeReference.Storage
		if byRegion[region] == nil {
			byRegion[region] = map[string]struct{}{}
		}
		byRegion[region][storage] = struct{}{}
	}

	result := map[string]map[string]proxmoxStorageMeta{}
	for region, storages := range byRegion {
		px, err := c.proxmox.GetProxmoxCluster(region)
		if err != nil {
			continue
		}

		regionMeta := map[string]proxmoxStorageMeta{}
		for storage := range storages {
			storageConfig, err := px.GetClusterStorage(ctx, storage)
			if err != nil {
				klog.ErrorS(err, "failed to get proxmox storage config", "region", region, "storage", storage)
				continue
			}

			regionMeta[storage] = proxmoxStorageMeta{
				Shared: storageConfig.Shared == 1,
				Nodes:  splitCSV(storageConfig.Nodes),
			}
		}
		if len(regionMeta) > 0 {
			result[region] = regionMeta
		}
	}

	return result
}

// getRegionsFromVolumes extracts regions and zones from volumes
func (c *Collector) getRegionsFromVolumes(pvs []KubernetesPersistentVolume) map[string]map[string]bool {
	regionsNeeded := make(map[string]map[string]bool)

	for _, pv := range pvs {
		if pv.VolumeReference == nil {
			continue
		}

		region := pv.VolumeReference.Region
		if regionsNeeded[region] == nil {
			regionsNeeded[region] = make(map[string]bool)
		}

		if pv.VolumeReference.Zone != "" {
			regionsNeeded[region][pv.VolumeReference.Zone] = true
		}
	}

	return regionsNeeded
}

// collectProxmoxClusters gathers Proxmox clusters from the pool
func (c *Collector) collectProxmoxClusters(ctx context.Context, regionsNeeded map[string]map[string]bool) []ProxmoxCluster {
	var result []ProxmoxCluster

	for region := range regionsNeeded {
		px, err := c.proxmox.GetProxmoxCluster(region)
		if err != nil {
			klog.ErrorS(err, "failed to get proxmox cluster", "region", region)
			continue
		}

		cl, err := px.Cluster(ctx)
		if err != nil {
			klog.ErrorS(err, "failed to get cluster info", "region", region)
			continue
		}

		info, err := cl.ClusterInfo(ctx)
		if err != nil {
			klog.ErrorS(err, "failed to get cluster info", "region", region)
			continue
		}

		result = append(result, ProxmoxCluster{
			Name:   info.Cluster,
			Region: region,
		})
	}

	return result
}

// collectProxmoxResources gathers Proxmox VMs and disks
func (c *Collector) collectProxmoxResources(ctx context.Context, k8s Kubernetes, regionsNeeded map[string]map[string]bool, storageMeta map[string]map[string]proxmoxStorageMeta) (
	[]ProxmoxNode, []ProxmoxSharedDisk, []ProxmoxLocalDisk) {

	var nodes []ProxmoxNode
	var sharedDisks []ProxmoxSharedDisk
	var localDisks []ProxmoxLocalDisk

	// Pre-fetch resources per region so we only call the cluster API once per region.
	resourcesByRegion := map[string][]goproxmox.ClusterResource{}
	pxClientMap := map[string]*goproxmox.APIClient{}

	for region := range regionsNeeded {
		px, err := c.proxmox.GetProxmoxCluster(region)
		if err != nil {
			klog.ErrorS(err, "failed to get proxmox cluster", "region", region)
			continue
		}

		cl, err := px.Cluster(ctx)
		if err != nil {
			klog.ErrorS(err, "failed to get cluster client", "region", region)
			continue
		}

		resources, err := cl.Resources(ctx, "")
		if err != nil {
			klog.ErrorS(err, "failed to get cluster resources", "region", region)
			continue
		}

		resourcesByRegion[region] = resources
		pxClientMap[region] = px

		wantedNodes := regionsNeeded[region]
		// Collect nodes (VMs and LXCs) that are in zones we care about.
		// If no zone is specified for the region, include all VM/LXC resources.
		for _, r := range resources {
			if r.Type != "qemu" && r.Type != "lxc" {
				continue
			}
			if len(wantedNodes) > 0 && !wantedNodes[r.Node] {
				continue
			}

			pnode := ProxmoxNode{
				ClusterName: region,
				ID:          fmt.Sprintf("%d", r.VMID),
				Name:        r.Name,
				NodeID:      r.Node,
				Type:        r.Type,
				Status:      r.Status,
			}
			nodes = append(nodes, pnode)
		}
	}

	// Track which disks are used and attached to which VMs. Pass the pre-fetched resources
	// to avoid duplicate API calls to list cluster resources.
	diskUsage := c.analyzeDiskUsage(ctx, k8s.PersistentVolumes, regionsNeeded, storageMeta, resourcesByRegion, pxClientMap)

	// Analyze disks from volumes
	for _, pv := range k8s.PersistentVolumes {
		if pv.VolumeReference == nil {
			continue
		}

		region := pv.VolumeReference.Region
		node := pv.VolumeReference.Node
		storage := pv.VolumeReference.Storage
		diskName := pv.VolumeReference.Disk
		// Ensure we have storage metadata (shared/nodes) from Proxmox. If not present,
		// try to fetch it once using GetClusterStorage and cache it locally to avoid repeated API calls.
		var meta proxmoxStorageMeta
		var ok bool
		if storageMeta[region] != nil {
			meta, ok = storageMeta[region][storage]
		}

		if !ok {
			// try to fetch and cache
			if px, has := pxClientMap[region]; has && px != nil {
				if storageConfig, err := px.GetClusterStorage(ctx, storage); err == nil {
					if storageMeta[region] == nil {
						storageMeta[region] = map[string]proxmoxStorageMeta{}
					}
					meta = proxmoxStorageMeta{
						Shared: storageConfig.Shared == 1,
						Nodes:  splitCSV(storageConfig.Nodes),
					}
					storageMeta[region][storage] = meta
				} else {
					klog.ErrorS(err, "failed to get proxmox storage config while building disk list", "region", region, "storage", storage)
				}
			}
		}
		shared := meta.Shared
		key := diskUsageKey(region, node, storage, diskName, shared)
		usage := diskUsage[key]

		volumeRef := &VolumeReference{
			Region:  region,
			Zone:    pv.VolumeReference.Zone,
			Node:    node,
			Storage: storage,
			Disk:    diskName,
		}
		if shared {
			// shared disks are identified by the storage/volume only.
			volumeRef.Node = ""
		} else {
			if usage.AttachedNode != "" {
				volumeRef.Node = usage.AttachedNode
			} else if len(meta.Nodes) > 0 {
				volumeRef.Node = meta.Nodes[0]
			}
		}

		// Determine if it's a shared disk using cluster storage metadata.
		if shared {
			// Shared disk
			disk := ProxmoxSharedDisk{
				ProxmoxDisk: ProxmoxDisk{
					ClusterName:     region,
					StorageID:       storage,
					Name:            diskName,
					SizeBytes:       0, // Would need to query Proxmox to get actual size
					VolumeReference: volumeRef,
				},
				AttachedVMIds: usage.AttachedVMIDs,
			}
			sharedDisks = append(sharedDisks, disk)
		} else {
			// Local disk
			attachedVMID := firstString(usage.AttachedVMIDs)
			attachedNode := usage.AttachedNode
			if attachedNode == "" {
				if len(meta.Nodes) > 0 {
					attachedNode = meta.Nodes[0]
				} else {
					attachedNode = node
				}
			}

			var vmIDPtr *string
			if attachedVMID != "" {
				vmIDPtr = &attachedVMID
			}

			disk := ProxmoxLocalDisk{
				ProxmoxDisk: ProxmoxDisk{
					ClusterName:     region,
					StorageID:       storage,
					Name:            diskName,
					SizeBytes:       0, // Would need to query Proxmox to get actual size
					VolumeReference: volumeRef,
				},
				NodeID:       attachedNode,
				AttachedVMID: vmIDPtr,
			}
			localDisks = append(localDisks, disk)
		}
	}

	return nodes, sharedDisks, localDisks
}

// analyzeDiskUsage maps disks to the actual Proxmox VM IDs that have them attached.
func (c *Collector) analyzeDiskUsage(ctx context.Context, pvs []KubernetesPersistentVolume, regionsNeeded map[string]map[string]bool, storageMeta map[string]map[string]proxmoxStorageMeta, resourcesByRegion map[string][]goproxmox.ClusterResource, pxClientMap map[string]*goproxmox.APIClient) map[string]proxmoxDiskUsage {
	diskToVMs := make(map[string]proxmoxDiskUsage)

	for region := range regionsNeeded {
		resources, ok := resourcesByRegion[region]
		if !ok {
			// no resources prefetched for this region
			continue
		}

		// prefer the prefetched API client, fall back to pool lookup
		px := pxClientMap[region]
		if px == nil {
			var err error
			px, err = c.proxmox.GetProxmoxCluster(region)
			if err != nil {
				klog.ErrorS(err, "failed to get proxmox cluster for disk analysis", "region", region)
				continue
			}
		}

		vmConfigCache := map[uint64]*goproxmox.VirtualMachine{}

		for _, pv := range pvs {
			if pv.VolumeReference == nil || pv.VolumeReference.Region != region {
				continue
			}

			vol, err := volutil.NewVolumeFromVolumeID(pv.VolumeHandle)
			if err != nil {
				continue
			}

			meta, ok := storageMeta[region][vol.Storage()]
			if !ok {
				// attempt on-demand fetch using prefetched client
				if px2, has := pxClientMap[region]; has && px2 != nil {
					if storageConfig, err := px2.GetClusterStorage(ctx, vol.Storage()); err == nil {
						if storageMeta[region] == nil {
							storageMeta[region] = map[string]proxmoxStorageMeta{}
						}
						meta = proxmoxStorageMeta{
							Shared: storageConfig.Shared == 1,
							Nodes:  splitCSV(storageConfig.Nodes),
						}
						storageMeta[region][vol.Storage()] = meta
					}
				}
			}

			key := diskUsageKey(region, vol.Node(), vol.Storage(), vol.Disk(), meta.Shared)
			if _, ok := diskToVMs[key]; ok {
				continue
			}

			candidateNodes := meta.Nodes
			if len(candidateNodes) == 0 && vol.Node() != "" {
				candidateNodes = []string{vol.Node()}
			}

			usage := proxmoxDiskUsage{}
			for _, r := range resources {
				if r.Type != "qemu" && r.Type != "lxc" {
					continue
				}
				if len(candidateNodes) > 0 && !containsString(candidateNodes, r.Node) {
					continue
				}

				vm, ok := vmConfigCache[r.VMID]
				if !ok {
					var err error
					vm, err = px.GetVMConfig(ctx, int(r.VMID))
					if err != nil {
						continue
					}
					vmConfigCache[r.VMID] = vm
				}

				if !diskConfigContains(vm.VirtualMachineConfig.MergeDisks(), vol.Disk()) {
					continue
				}

				vmID := strconv.FormatUint(r.VMID, 10)
				if !containsString(usage.AttachedVMIDs, vmID) {
					usage.AttachedVMIDs = append(usage.AttachedVMIDs, vmID)
				}
				if usage.AttachedNode == "" {
					usage.AttachedNode = r.Node
				}
			}

			diskToVMs[key] = usage
		}
	}

	return diskToVMs
}

func diskUsageKey(region, node, storage, disk string, shared bool) string {
	if shared {
		return region + "/" + storage + "/" + disk
	}

	if node == "" {
		return region + "/" + storage + "/" + disk
	}

	return region + "/" + node + "/" + storage + "/" + disk
}

func diskConfigContains(disks map[string]string, disk string) bool {
	for _, value := range disks {
		if matchesDiskValue(value, disk) {
			return true
		}
	}

	return false
}

func matchesDiskValue(value, disk string) bool {
	if value == "" || disk == "" {
		return false
	}

	if idx := strings.Index(value, ","); idx >= 0 {
		value = value[:idx]
	}
	if idx := strings.Index(value, ":"); idx >= 0 {
		value = value[idx+1:]
	}

	return value == disk || strings.Contains(value, disk)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
