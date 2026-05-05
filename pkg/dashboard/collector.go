package dashboard

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	proxmox "github.com/luthermonson/go-proxmox"
	goproxmox "github.com/sergelogvinov/go-proxmox"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	volume "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Collector gathers Kubernetes and Proxmox resources
type Collector struct {
	kube    kubernetes.Interface
	proxmox *proxmoxpool.ProxmoxPool
	// includeMetadata controls whether to include annotations and labels in the response
	includeMetadata bool
}

// Collect gathers all Kubernetes and Proxmox resources and returns a snapshot
func (c *Collector) Collect(ctx context.Context, store *Store) error {
	ss := Snapshot{GeneratedAt: time.Now().UTC(), Regions: map[string]Region{}, Kubernetes: Kubernetes{}}

	// 1. StorageClasses
	scList, err := c.kube.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list storageclasses: %w", err)
	}
	scNames := map[string]struct{}{}
	scs := make([]KubernetesStorageClass, 0, len(scList.Items))
	for _, sc := range scList.Items {
		ksc := KubernetesStorageClass{
			KubernetesBase:       KubernetesBase{KubernetesReference: KubernetesReference{Kind: "StorageClass", Name: sc.Name, UID: string(sc.UID)}, CreatedAt: sc.CreationTimestamp.Time},
			Provisioner:          sc.Provisioner,
			ReclaimPolicy:        "",
			AllowVolumeExpansion: false,
			VolumeBindingMode:    "",
			IsDefault:            false,
		}
		if sc.Provisioner == csi.DriverName {
			scNames[sc.Name] = struct{}{}
		}
		if sc.Annotations != nil {
			ksc.Annotations = sc.Annotations
		}
		if sc.Labels != nil {
			ksc.Labels = sc.Labels
		}
		if sc.ReclaimPolicy != nil {
			ksc.ReclaimPolicy = string(*sc.ReclaimPolicy)
		}
		if sc.AllowVolumeExpansion != nil {
			ksc.AllowVolumeExpansion = *sc.AllowVolumeExpansion
		}
		if sc.VolumeBindingMode != nil {
			ksc.VolumeBindingMode = string(*sc.VolumeBindingMode)
		}
		// default annotation
		if sc.Annotations != nil {
			if _, ok := sc.Annotations["storageclass.kubernetes.io/is-default-class"]; ok {
				ksc.IsDefault = sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true"
			}
		}
		scs = append(scs, ksc)
	}
	// sort for deterministic output
	sort.Slice(scs, func(i, j int) bool { return scs[i].Name < scs[j].Name })
	ss.Kubernetes.StorageClasses = scs

	// 2. PVs and PVCs
	pvList, err := c.kube.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list persistentvolumes: %w", err)
	}
	pvcList, err := c.kube.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list persistentvolumeclaims: %w", err)
	}

	// build PVC map and track namespaces
	pvcMap := map[string]corev1.PersistentVolumeClaim{}
	relevantPVCs := make([]KubernetesPersistentVolumeClaim, 0)
	nsNeeded := map[string]struct{}{}
	for _, pvc := range pvcList.Items {
		if pvc.Spec.StorageClassName == nil {
			continue
		}
		if _, ok := scNames[*pvc.Spec.StorageClassName]; !ok {
			continue
		}
		key := pvc.Namespace + "/" + pvc.Name
		pvcMap[key] = pvc
		nsNeeded[pvc.Namespace] = struct{}{}
		kpvc := KubernetesPersistentVolumeClaim{
			KubernetesBase:   KubernetesBase{KubernetesReference: KubernetesReference{Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: string(pvc.UID)}, CreatedAt: pvc.CreationTimestamp.Time},
			StorageClassName: *pvc.Spec.StorageClassName,
			Bound:            pvc.Status.Phase == corev1.ClaimBound,
			AccessMode:       []string{},
			CapacityRequest:  "",
			VolumeMode:       stringOrEmpty(pvc.Spec.VolumeMode),
			VolumeName:       pvc.Spec.VolumeName,
		}
		if pvc.Annotations != nil {
			kpvc.Annotations = pvc.Annotations
		}
		if pvc.Labels != nil {
			kpvc.Labels = pvc.Labels
		}
		for _, m := range pvc.Spec.AccessModes {
			kpvc.AccessMode = append(kpvc.AccessMode, string(m))
		}
		if q := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; q.Value() > 0 {
			kpvc.CapacityRequest = q.String()
		}
		relevantPVCs = append(relevantPVCs, kpvc)
	}
	sort.Slice(relevantPVCs, func(i, j int) bool { return relevantPVCs[i].Name < relevantPVCs[j].Name })
	ss.Kubernetes.PersistentVolumeClaims = relevantPVCs

	// PVs: select PVs that use matching storageclasses (by name) or CSI driver
	relevantPVs := make([]KubernetesPersistentVolume, 0)
	pvByName := map[string]corev1.PersistentVolume{}
	for _, pv := range pvList.Items {
		use := false
		if pv.Spec.StorageClassName != "" {
			if _, ok := scNames[pv.Spec.StorageClassName]; ok {
				use = true
			}
		}
		if !use && pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csi.DriverName {
			use = true
		}
		if !use {
			continue
		}
		pvByName[pv.Name] = pv
		kpv := KubernetesPersistentVolume{
			KubernetesBase:   KubernetesBase{KubernetesReference: KubernetesReference{Kind: "PersistentVolume", Name: pv.Name, UID: string(pv.UID)}, CreatedAt: pv.CreationTimestamp.Time},
			StorageClassName: pv.Spec.StorageClassName,
			Bound:            pv.Status.Phase == corev1.VolumeBound,
			AccessMode:       []string{},
			Capacity:         "",
			Mode:             stringOrEmpty(pv.Spec.VolumeMode),
			Status:           pv.Status,
			VolumeHandle:     "",
		}
		if pv.Annotations != nil {
			kpv.Annotations = pv.Annotations
		}
		if pv.Labels != nil {
			kpv.Labels = pv.Labels
		}
		for _, m := range pv.Spec.AccessModes {
			kpv.AccessMode = append(kpv.AccessMode, string(m))
		}
		if q := pv.Spec.Capacity[corev1.ResourceStorage]; q.Value() > 0 {
			kpv.Capacity = q.String()
		}
		if pv.Spec.ClaimRef != nil {
			kpv.ClaimReference = &KubernetesReference{Kind: "PersistentVolumeClaim", Namespace: pv.Spec.ClaimRef.Namespace, Name: pv.Spec.ClaimRef.Name, UID: string(pv.Spec.ClaimRef.UID)}
			nsNeeded[pv.Spec.ClaimRef.Namespace] = struct{}{}
		}
		if pv.Spec.CSI != nil {
			kpv.VolumeHandle = pv.Spec.CSI.VolumeHandle
		}
		relevantPVs = append(relevantPVs, kpv)
	}
	sort.Slice(relevantPVs, func(i, j int) bool { return relevantPVs[i].Name < relevantPVs[j].Name })
	ss.Kubernetes.PersistentVolumes = relevantPVs

	// 3. Pods using relevant volumes
	podList, err := c.kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list pods: %w", err)
	}
	// maps
	claimToPods := map[string][]corev1.Pod{}
	podsByNode := map[string][]corev1.Pod{}
	relevantPods := map[string]corev1.Pod{}
	for _, p := range podList.Items {
		used := false
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				key := p.Namespace + "/" + v.PersistentVolumeClaim.ClaimName
				if _, ok := pvcMap[key]; ok {
					used = true
					claimToPods[key] = append(claimToPods[key], p)
					nsNeeded[p.Namespace] = struct{}{}
				}
			}
		}
		if used {
			relevantPods[p.Namespace+"/"+p.Name] = p
			if p.Spec.NodeName != "" {
				podsByNode[p.Spec.NodeName] = append(podsByNode[p.Spec.NodeName], p)
			}
		}
	}

	// 4. Collect relevant Kubernetes Nodes
	nodeList, err := c.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	// find nodes that have relevant pods
	relevantNodeNames := map[string]struct{}{}
	for n := range podsByNode {
		relevantNodeNames[n] = struct{}{}
	}
	relevantNodes := make([]*corev1.Node, 0)
	nodeByName := map[string]*corev1.Node{}
	for i := range nodeList.Items {
		n := &nodeList.Items[i]
		nodeByName[n.Name] = n
		if _, ok := relevantNodeNames[n.Name]; ok {
			relevantNodes = append(relevantNodes, n)
		}
	}

	// 5. Relevant Namespaces
	nsList, err := c.kube.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list namespaces: %w", err)
	}
	namespaces := make([]KubernetesNamespace, 0)
	for _, ns := range nsList.Items {
		if _, ok := nsNeeded[ns.Name]; !ok {
			continue
		}
		kn := KubernetesNamespace{KubernetesBase: KubernetesBase{KubernetesReference: KubernetesReference{Kind: "Namespace", Name: ns.Name, UID: string(ns.UID)}, CreatedAt: ns.CreationTimestamp.Time}, IsPrivileged: false}
		if ns.Annotations != nil {
			kn.Annotations = ns.Annotations
		}
		if ns.Labels != nil {
			kn.Labels = ns.Labels
		}
		// privileged check best-effort
		if ns.Labels != nil {
			if ns.Labels["pod-security.kubernetes.io/enforce"] == "privileged" || ns.Labels["kubernetes.io/cluster-service"] == "true" {
				kn.IsPrivileged = true
			}
		}
		namespaces = append(namespaces, kn)
	}
	sort.Slice(namespaces, func(i, j int) bool { return namespaces[i].Name < namespaces[j].Name })
	ss.Kubernetes.Namespaces = namespaces

	// 6. Resolve Kubernetes Nodes to Proxmox VMs
	// Use proxmoxpool.FindVMsByNodes for relevant nodes
	vmsByRegion := map[string][]proxmoxpool.VMAndConfig{}
	if len(relevantNodes) > 0 {
		vmsByRegion, err = c.proxmox.FindVMsByNodes(ctx, relevantNodes)
		if err != nil && err != goproxmox.ErrVirtualMachineNotFound {
			return fmt.Errorf("find vms by nodes: %w", err)
		}
	}

	// build map vmid->k8s node name for quick lookup
	vmidToNode := map[string]string{}
	for _, vms := range vmsByRegion {
		for _, v := range vms {
			if v.RS != nil {
				vmid := fmt.Sprintf("%d", v.RS.VMID)
				// try to match by VM UUID to node UUID via helper
				if uuid := goproxmox.GetVMUUID(v.VM); uuid != "" {
					for _, n := range relevantNodes {
						if n.Status.NodeInfo.SystemUUID != "" && strings.EqualFold(uuid, n.Status.NodeInfo.SystemUUID) {
							vmidToNode[vmid] = n.Name
						}
					}
				}
			}
		}
	}

	// 7-9. Proxmox collection: per-region caches
	for region, vms := range vmsByRegion {
		// ensure region entry
		r := Region{Name: region, Zones: map[string]Zone{}}

		px, err := c.proxmox.GetProxmoxCluster(region)
		if err != nil {
			// skip region if cluster client unavailable
			ss.Regions[region] = r
			continue
		}
		cl, err := px.Cluster(ctx)
		if err != nil {
			ss.Regions[region] = r
			continue
		}

		// gather storage resources once
		storResources, _ := cl.Resources(ctx, "storage")
		storageInfo := map[string]*proxmox.ClusterResource{}
		for _, s := range storResources {
			storageInfo[s.Storage] = s
		}

		// Build disk->attachedVMs mapping by scanning all VMs in region
		diskToVMs := map[string][]string{}
		// use GetVMsByFilter to iterate VMs and fetch configs
		_, _ = px.GetVMsByFilter(ctx, func(rs *proxmox.ClusterResource) (bool, error) {
			if rs.Type != "qemu" {
				return false, nil
			}
			vmcfg, err := px.GetVMConfig(ctx, int(rs.VMID))
			if err != nil {
				return false, err
			}
			// MergeSCSIs returns map[string]string-like; reflect as map
			disks := vmcfg.VirtualMachineConfig.MergeSCSIs()
			for _, d := range disks {
				// disk string like "storage:disk,..."
				first := strings.SplitN(d, ",", 2)[0]
				diskToVMs[first] = append(diskToVMs[first], fmt.Sprintf("%d", rs.VMID))
			}
			return false, nil
		})

		// per-storage content cache
		storageContents := map[string]map[string]*proxmox.StorageContent{}

		// iterate relevant PVs and populate region/zone/disk info
		for _, pv := range ss.Kubernetes.PersistentVolumes {
			if pv.VolumeHandle == "" {
				continue
			}
			vol, err := volume.NewVolumeFromVolumeID(pv.VolumeHandle)
			if err != nil {
				continue
			}
			if vol.Region() != region {
				continue
			}

			zoneName := vol.Zone()
			z := r.Zones[zoneName]
			if z.Name == "" {
				z.Name = zoneName
			}

			// determine storage metadata once per storage
			if _, ok := storageContents[vol.Storage()]; !ok {
				storageContents[vol.Storage()] = map[string]*proxmox.StorageContent{}
				// attempt to get storage content for the node
				if vol.Node() != "" {
					if _, err := px.GetStorageStatus(ctx, vol.Node(), vol.Storage()); err == nil {
						contents, err := px.GetStorageContent(ctx, vol.Node(), vol.Storage())
						if err == nil {
							for i := range contents {
								c := contents[i]
								storageContents[vol.Storage()][c.Volid] = c
							}
						}
					}
				}
			}

			// build disk entry
			disk := ProxmoxDisk{StorageID: vol.Storage(), Name: vol.Disk()}
			if sc, ok := storageInfo[vol.Storage()]; ok {
				_ = sc // keep for future use
			}
			// size
			if scmap, ok := storageContents[vol.Storage()]; ok {
				if st, ok := scmap[vol.VolID()]; ok {
					size := int64(st.Size)
					disk.SizeBytes = &size
				}
			}
			// attached VMs from diskToVMs
			if vmsAttached, ok := diskToVMs[vol.VolID()]; ok {
				disk.AttachedVMIDs = append(disk.AttachedVMIDs, vmsAttached...)
			}

			// categorize as local (zone) or shared (region)
			// heuristic: if storageInfo plugin indicates file-based, consider shared
			shared := false
			if sc, ok := storageInfo[vol.Storage()]; ok {
				switch sc.PluginType {
				case "dir", "nfs", "cifs", "cephfs", "btrfs":
					shared = true
				}
			}
			if shared {
				r.Disks = append(r.Disks, disk)
			} else {
				z.Disks = append(z.Disks, disk)
			}
			r.Zones[zoneName] = z
		}

		// add VMs discovered for nodes in this region
		for _, vm := range vms {
			if vm.RS == nil {
				continue
			}
			zoneName := vm.RS.Node
			z := r.Zones[zoneName]
			if z.Name == "" {
				z.Name = zoneName
			}
			pvm := ProxmoxVM{ID: fmt.Sprintf("%d", vm.RS.VMID), Name: vm.VM.Name}
			// attach node/k8s info if available
			if k8sNode, ok := vmidToNode[fmt.Sprintf("%d", vm.RS.VMID)]; ok {
				// populate KubernetesNode
				kn := KubernetesNode{KubernetesBase: KubernetesBase{KubernetesReference: KubernetesReference{Kind: "Node", Name: k8sNode}}, Pods: []KubernetesPod{}}
				// attach pods scheduled to this node
				for _, p := range podsByNode[k8sNode] {
					kp := KubernetesPod{KubernetesBase: KubernetesBase{KubernetesReference: KubernetesReference{Kind: "Pod", Namespace: p.Namespace, Name: p.Name, UID: string(p.UID)}, CreatedAt: p.CreationTimestamp.Time}, Hostname: p.Spec.Hostname, Volumes: map[string]string{}}
					if p.Annotations != nil {
						kp.Annotations = p.Annotations
					}
					if p.Labels != nil {
						kp.Labels = p.Labels
					}
					for _, v := range p.Spec.Volumes {
						if v.PersistentVolumeClaim != nil {
							key := p.Namespace + "/" + v.PersistentVolumeClaim.ClaimName
							kp.Volumes[v.Name] = pvcMap[key].Spec.VolumeName
						}
					}
					kn.Pods = append(kn.Pods, kp)
				}
				pvm.Node = kn
			}
			z.VMs = append(z.VMs, pvm)
			r.Zones[zoneName] = z
		}

		// deterministic ordering for zones, disks and vms
		for zn, z := range r.Zones {
			sort.Slice(z.VMs, func(i, j int) bool { return z.VMs[i].Name < z.VMs[j].Name })
			sort.Slice(z.Disks, func(i, j int) bool { return z.Disks[i].Name < z.Disks[j].Name })
			r.Zones[zn] = z
		}
		sort.Slice(r.Disks, func(i, j int) bool { return r.Disks[i].Name < r.Disks[j].Name })
		ss.Regions[region] = r
	}

	store.Set(ss)
	return nil
}

func stringOrEmpty(s *corev1.PersistentVolumeMode) string {
	if s == nil {
		return ""
	}
	return string(*s)
}
