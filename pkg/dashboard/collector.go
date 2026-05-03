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
	regions := c.proxmox.GetRegions()
	sort.Strings(regions)
	for _, region := range regions {
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
		for _, r := range resources {
			if r.Type == "node" {
				nid := "pve-node:" + region + ":" + r.Node
				ss.Nodes = append(ss.Nodes, Node{ID: nid, Kind: "proxmox-node", Name: r.Node, Group: region, Status: r.Status})
				ss.Edges = append(ss.Edges, Edge{From: regionID, To: nid, Kind: "contains"})
			}
			if r.Type == "qemu" || r.Type == "lxc" {
				vid := fmt.Sprintf("vm:%s:%d", region, r.VMID)
				kind := "vm"
				if r.Type == "lxc" {
					kind = "container"
				}
				ss.Nodes = append(ss.Nodes, Node{ID: vid, Kind: kind, Name: r.Name, Group: region, Status: r.Status, Metadata: map[string]string{"node": r.Node}})
				ss.Edges = append(ss.Edges, Edge{From: "pve-node:" + region + ":" + r.Node, To: vid, Kind: "runs"})
			}
			if r.Type == "storage" {
				sid := "storage:" + region + ":" + r.Storage
				ss.Nodes = append(ss.Nodes, Node{ID: sid, Kind: "storage", Name: r.Storage, Group: region, Status: r.Status, Metadata: map[string]string{"node": r.Node}})
				ss.Edges = append(ss.Edges, Edge{From: "pve-node:" + region + ":" + r.Node, To: sid, Kind: "hosts"})
			}
		}
	}

	nodes, _ := c.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	for _, n := range nodes.Items {
		id := "k8s-node:" + n.Name
		ss.Nodes = append(ss.Nodes, Node{ID: id, Kind: "k8s-node", Name: n.Name, Group: "kubernetes", Status: string(n.Status.Phase), Metadata: map[string]string{"region": n.Labels[csi.ProxmoxRegion], "proxmoxNode": n.Labels[csi.ProxmoxNode]}})
		if r := n.Labels[csi.ProxmoxRegion]; r != "" {
			ss.Edges = append(ss.Edges, Edge{From: "region:" + r, To: id, Kind: "mapped"})
		}
	}
	pods, _ := c.kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	for _, p := range pods.Items {
		pid := "pod:" + p.Namespace + ":" + p.Name
		ss.Nodes = append(ss.Nodes, Node{ID: pid, Kind: "pod", Name: p.Name, Group: p.Namespace, Status: string(p.Status.Phase)})
		if p.Spec.NodeName != "" {
			ss.Edges = append(ss.Edges, Edge{From: "k8s-node:" + p.Spec.NodeName, To: pid, Kind: "schedules"})
		}
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				ss.Edges = append(ss.Edges, Edge{From: pid, To: "pvc:" + p.Namespace + ":" + v.PersistentVolumeClaim.ClaimName, Kind: "mounts"})
			}
		}
	}
	pvcs, _ := c.kube.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	for _, pvc := range pvcs.Items {
		id := "pvc:" + pvc.Namespace + ":" + pvc.Name
		ss.Nodes = append(ss.Nodes, Node{ID: id, Kind: "pvc", Name: pvc.Name, Group: pvc.Namespace, Status: string(pvc.Status.Phase)})
		if pvc.Spec.VolumeName != "" {
			ss.Edges = append(ss.Edges, Edge{From: id, To: "pv:" + pvc.Spec.VolumeName, Kind: "binds"})
		}
	}
	pvs, _ := c.kube.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	for _, pv := range pvs.Items {
		id := "pv:" + pv.Name
		ss.Nodes = append(ss.Nodes, Node{ID: id, Kind: "pv", Name: pv.Name, Group: "kubernetes", Status: string(pv.Status.Phase)})
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csi.DriverName {
			ss.Edges = append(ss.Edges, Edge{From: id, To: "storage-link:" + pv.Name, Kind: "csi"})
		}
		if pv.Spec.CSI != nil {
			vol := pv.Spec.CSI.VolumeHandle
			ss.Nodes = append(ss.Nodes, Node{ID: "storage-link:" + pv.Name, Kind: "volume", Name: vol, Group: "proxmox", Status: "known"})
			parts := strings.Split(vol, "/")
			if len(parts) >= 2 {
				vmid, _ := strconv.Atoi(parts[1])
				ss.Edges = append(ss.Edges, Edge{From: "storage-link:" + pv.Name, To: fmt.Sprintf("vm:%s:%d", parts[0], vmid), Kind: "backed-by"})
			}
		}
	}
	attachments, _ := c.kube.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	for _, a := range attachments.Items {
		aid := "va:" + a.Name
		ss.Nodes = append(ss.Nodes, Node{ID: aid, Kind: "volumeattachment", Name: a.Name, Group: "kubernetes", Status: map[bool]string{true: "attached", false: "pending"}[a.Status.Attached]})
		if a.Spec.Source.PersistentVolumeName != nil {
			ss.Edges = append(ss.Edges, Edge{From: aid, To: "pv:" + *a.Spec.Source.PersistentVolumeName, Kind: "attaches"})
		}
		ss.Edges = append(ss.Edges, Edge{From: aid, To: "k8s-node:" + a.Spec.NodeName, Kind: "targets"})
	}
	store.Set(ss)
	return nil
}
