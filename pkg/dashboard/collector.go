package dashboard

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Collector struct {
	kube    kubernetes.Interface
	proxmox *proxmoxpool.ProxmoxPool
}

func (c *Collector) Collect(ctx context.Context, store *Store) error {
	ss := Snapshot{GeneratedAt: time.Now().UTC()}

	pvs, _ := c.kube.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	relevantPVs := map[string]string{} // pvName -> volumeHandle
	relevantVM := map[string]map[int]bool{}
	for _, pv := range pvs.Items {
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csi.DriverName || pv.Spec.CSI.VolumeHandle == "" {
			continue
		}
		relevantPVs[pv.Name] = pv.Spec.CSI.VolumeHandle
		parts := strings.Split(pv.Spec.CSI.VolumeHandle, "/")
		if len(parts) >= 2 {
			if vmid, err := strconv.Atoi(parts[1]); err == nil {
				if relevantVM[parts[0]] == nil {
					relevantVM[parts[0]] = map[int]bool{}
				}
				relevantVM[parts[0]][vmid] = true
			}
		}
	}

	if len(relevantPVs) == 0 {
		store.Set(ss)
		return nil
	}

	nodes, _ := c.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	k8sNodeToRegion := map[string]string{}
	k8sNodeToZone := map[string]string{}
	for _, n := range nodes.Items {
		k8sNodeToRegion[n.Name] = n.Labels[csi.ProxmoxRegion]
		k8sNodeToZone[n.Name] = n.Labels[csi.ProxmoxNode]
	}

	pvcs, _ := c.kube.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	for _, pvc := range pvcs.Items {
		if _, ok := relevantPVs[pvc.Spec.VolumeName]; !ok {
			continue
		}
		pvcID := "pvc:" + pvc.Namespace + ":" + pvc.Name
		ss.Nodes = append(ss.Nodes, Node{ID: pvcID, Kind: "pvc", Name: pvc.Name, Group: "k8s/ns:" + pvc.Namespace, Status: string(pvc.Status.Phase)})
		ss.Edges = append(ss.Edges, Edge{From: pvcID, To: "pv:" + pvc.Spec.VolumeName, Kind: "binds"})
	}

	for _, pv := range pvs.Items {
		volHandle, ok := relevantPVs[pv.Name]
		if !ok {
			continue
		}
		pvID := "pv:" + pv.Name
		volID := "volume:" + pv.Name
		ss.Nodes = append(ss.Nodes, Node{ID: pvID, Kind: "pv", Name: pv.Name, Group: "k8s/volumes", Status: string(pv.Status.Phase)})
		ss.Nodes = append(ss.Nodes, Node{ID: volID, Kind: "volume", Name: volHandle, Group: "proxmox/disks", Status: "known"})
		ss.Edges = append(ss.Edges, Edge{From: pvID, To: volID, Kind: "backed-by"})
	}

	pods, _ := c.kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	for _, p := range pods.Items {
		linked := false
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}
			for _, pvc := range pvcs.Items {
				if pvc.Namespace == p.Namespace && pvc.Name == v.PersistentVolumeClaim.ClaimName {
					if _, ok := relevantPVs[pvc.Spec.VolumeName]; ok {
						linked = true
					}
				}
			}
		}
		if !linked {
			continue
		}
		nodeGroup := "k8s/node:" + p.Spec.NodeName
		pid := "pod:" + p.Namespace + ":" + p.Name
		ss.Nodes = append(ss.Nodes, Node{ID: "k8s-node:" + p.Spec.NodeName, Kind: "k8s-node", Name: p.Spec.NodeName, Group: "k8s/cluster", Status: "known", Metadata: map[string]string{"region": k8sNodeToRegion[p.Spec.NodeName], "zone": k8sNodeToZone[p.Spec.NodeName]}})
		ss.Nodes = append(ss.Nodes, Node{ID: pid, Kind: "pod", Name: p.Name, Group: nodeGroup, Status: string(p.Status.Phase)})
		ss.Edges = append(ss.Edges, Edge{From: "k8s-node:" + p.Spec.NodeName, To: pid, Kind: "schedules"})
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				ss.Edges = append(ss.Edges, Edge{From: pid, To: "pvc:" + p.Namespace + ":" + v.PersistentVolumeClaim.ClaimName, Kind: "mounts"})
			}
		}
	}

	attachments, _ := c.kube.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	for _, a := range attachments.Items {
		if a.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		if _, ok := relevantPVs[*a.Spec.Source.PersistentVolumeName]; !ok {
			continue
		}
		aid := "va:" + a.Name
		ss.Nodes = append(ss.Nodes, Node{ID: aid, Kind: "volumeattachment", Name: a.Name, Group: "k8s/attachments", Status: map[bool]string{true: "attached", false: "pending"}[a.Status.Attached]})
		ss.Edges = append(ss.Edges, Edge{From: aid, To: "pv:" + *a.Spec.Source.PersistentVolumeName, Kind: "attaches"})
		ss.Edges = append(ss.Edges, Edge{From: aid, To: "k8s-node:" + a.Spec.NodeName, Kind: "targets"})
	}

	regions := c.proxmox.GetRegions()
	sort.Strings(regions)
	for _, region := range regions {
		regionWanted := len(relevantVM[region]) > 0
		if !regionWanted {
			continue
		}
		regionID := "region:" + region
		ss.Nodes = append(ss.Nodes, Node{ID: regionID, Kind: "proxmox-region", Name: region, Group: "proxmox", Status: "ready"})
		px, err := c.proxmox.GetProxmoxCluster(region)
		if err != nil {
			continue
		}
		cl, err := px.Cluster(ctx)
		if err != nil {
			continue
		}
		resources, err := cl.Resources(ctx, "")
		if err != nil {
			continue
		}
		zoneSeen := map[string]bool{}
		for _, r := range resources {
			if r.Type == "qemu" || r.Type == "lxc" {
				if !relevantVM[region][int(r.VMID)] {
					continue
				}
				zoneID := "pve-node:" + region + ":" + r.Node
				if !zoneSeen[zoneID] {
					ss.Nodes = append(ss.Nodes, Node{ID: zoneID, Kind: "proxmox-node", Name: r.Node, Group: "region:" + region, Status: "ready"})
					ss.Edges = append(ss.Edges, Edge{From: regionID, To: zoneID, Kind: "contains"})
					zoneSeen[zoneID] = true
				}
				vmID := fmt.Sprintf("vm:%s:%d", region, r.VMID)
				kind := "vm"
				if r.Type == "lxc" {
					kind = "container"
				}
				ss.Nodes = append(ss.Nodes, Node{ID: vmID, Kind: kind, Name: r.Name, Group: zoneID, Status: r.Status})
				ss.Edges = append(ss.Edges, Edge{From: zoneID, To: vmID, Kind: "runs"})
			}
			if r.Type == "storage" {
				for vmid := range relevantVM[region] {
					vmID := fmt.Sprintf("vm:%s:%d", region, vmid)
					diskID := "volume:" + strconv.Itoa(vmid)
					_ = vmID
					_ = diskID
				}
			}
		}
	}

	// bind proxmox volume handles back to VM nodes for hierarchy region->zone->vm->disk.
	for pvName, handle := range relevantPVs {
		parts := strings.Split(handle, "/")
		if len(parts) < 2 {
			continue
		}
		vmid, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		ss.Edges = append(ss.Edges, Edge{From: "volume:" + pvName, To: fmt.Sprintf("vm:%s:%d", parts[0], vmid), Kind: "attached-to"})
	}

	store.Set(dedup(ss))
	return nil
}

func dedup(in Snapshot) Snapshot {
	nodes := make([]Node, 0, len(in.Nodes))
	nseen := map[string]bool{}
	for _, n := range in.Nodes {
		if n.ID == "" || nseen[n.ID] {
			continue
		}
		nseen[n.ID] = true
		nodes = append(nodes, n)
	}
	edges := make([]Edge, 0, len(in.Edges))
	eseen := map[string]bool{}
	for _, e := range in.Edges {
		k := e.From + "|" + e.To + "|" + e.Kind
		if e.From == "" || e.To == "" || eseen[k] {
			continue
		}
		eseen[k] = true
		edges = append(edges, e)
	}
	in.Nodes = nodes
	in.Edges = edges
	return in
}
