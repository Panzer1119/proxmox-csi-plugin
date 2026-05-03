package dashboard

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	volutil "github.com/sergelogvinov/proxmox-csi-plugin/pkg/utils/volume"
	log "github.com/sirupsen/logrus"
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
	pvByName := map[string]string{}
	volByPV := map[string]*volutil.Volume{}
	regionsWanted := map[string]bool{}
	zonesWanted := map[string]map[string]bool{}
	for _, pv := range pvs.Items {
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csi.DriverName {
			continue
		}
		v, err := volutil.NewVolumeFromVolumeID(pv.Spec.CSI.VolumeHandle)
		if err != nil {
			continue
		}
		pvByName[pv.Name] = pv.Spec.CSI.VolumeHandle
		volByPV[pv.Name] = v
		regionsWanted[v.Region()] = true
		if zonesWanted[v.Region()] == nil {
			zonesWanted[v.Region()] = map[string]bool{}
		}
		if v.Zone() != "" {
			zonesWanted[v.Region()][v.Zone()] = true
		}
	}
	if len(pvByName) == 0 {
		store.Set(ss)
		return nil
	}

	// Kubernetes storage classes for this driver.
	scs, _ := c.kube.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	for _, sc := range scs.Items {
		if sc.Provisioner == csi.DriverName {
			ss.Nodes = append(ss.Nodes, Node{ID: "sc:" + sc.Name, Kind: "storageclass", Shape: "diamond", Name: sc.Name, Group: "kubernetes", Status: "active"})
		}
	}

	pvcs, _ := c.kube.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	relPVC := map[string]bool{}
	pvcInfo := map[string]struct {
		namespace string
		name      string
		pvName    string
		phase     string
	}{}
	for _, pvc := range pvcs.Items {
		id := pvc.Namespace + "/" + pvc.Name
		pvName := pvc.Spec.VolumeName
		if pvName == "" { // unbound but class belongs to our driver
			if pvc.Spec.StorageClassName != nil && hasSC(ss.Nodes, *pvc.Spec.StorageClassName) {
				relPVC[id] = true
			}
		} else if _, ok := pvByName[pvName]; ok {
			relPVC[id] = true
		}
		if relPVC[id] {
			pvcInfo[id] = struct {
				namespace string
				name      string
				pvName    string
				phase     string
			}{namespace: pvc.Namespace, name: pvc.Name, pvName: pvName, phase: string(pvc.Status.Phase)}
		}
	}

	pvcParents := map[string]map[string]bool{}

	for pvName, handle := range pvByName {
		v := volByPV[pvName]
		pvID := "pv:" + pvName
		diskID := "disk:" + v.Region() + "/" + v.Zone() + "/" + v.Storage() + "/" + v.Disk()
		kind := "local-disk"
		parent := "zone:" + v.Region() + "/" + v.Zone()
		if v.Zone() == "" {
			kind = "shared-disk"
			parent = "region:" + v.Region()
		}
		ss.Nodes = append(ss.Nodes, Node{ID: pvID, Kind: "pv", Shape: "circle", Name: pvName, Group: "kubernetes", Status: "bound", Metadata: map[string]string{"volumeHandle": handle}})
		ss.Nodes = append(ss.Nodes, Node{ID: diskID, ParentID: parent, Kind: kind, Shape: "cylinder", Name: v.VolID(), Group: "proxmox", Status: "known"})
		ss.Edges = append(ss.Edges, Edge{From: pvID, To: diskID, Kind: "backs"})
	}

	pods, _ := c.kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	log.Debugf("collector: pods returned: %d", len(pods.Items))
	nodeSeen := map[string]bool{}
	for _, p := range pods.Items {
		podHas := false
		for _, vol := range p.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && relPVC[p.Namespace+"/"+vol.PersistentVolumeClaim.ClaimName] {
				podHas = true
				if p.Spec.NodeName != "" {
					claimID := p.Namespace + "/" + vol.PersistentVolumeClaim.ClaimName
					if pvcParents[claimID] == nil {
						pvcParents[claimID] = map[string]bool{}
					}
					pvcParents[claimID]["k8s-node:"+p.Spec.NodeName] = true
				}
			}
		}
		if !podHas {
			continue
		}
		nodeID := "k8s-node:" + p.Spec.NodeName
		if !nodeSeen[nodeID] {
			nodeSeen[nodeID] = true
			ss.Nodes = append(ss.Nodes, Node{ID: nodeID, Kind: "k8s-node", Shape: "square", Name: p.Spec.NodeName, Group: "kubernetes", Status: "ready"})
		}
		vmID := "vm:" + p.Spec.NodeName
		ss.Nodes = append(ss.Nodes, Node{ID: vmID, Kind: "vm-workload", ParentID: nodeID, Shape: "square", Name: p.Spec.NodeName, Group: "kubernetes", Status: "active"})
		podID := "pod:" + p.Namespace + "/" + p.Name
		ss.Nodes = append(ss.Nodes, Node{ID: podID, Kind: "pod", ParentID: vmID, Shape: "square", Name: p.Name, Group: "kubernetes", Status: string(p.Status.Phase)})
		ss.Edges = append(ss.Edges, Edge{From: nodeID, To: podID, Kind: "runs"})
		for _, vol := range p.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				ss.Edges = append(ss.Edges, Edge{From: podID, To: "pvc:" + p.Namespace + "/" + vol.PersistentVolumeClaim.ClaimName, Kind: "mounts"})
			}
		}
	}

	for id, info := range pvcInfo {
		parentID := ""
		if parents := pvcParents[id]; len(parents) == 1 {
			for parent := range parents {
				parentID = parent
			}
		}
		nid := "pvc:" + id
		ss.Nodes = append(ss.Nodes, Node{ID: nid, ParentID: parentID, Kind: "pvc", Shape: "hex", Name: info.name, Group: "kubernetes", Status: info.phase, Metadata: map[string]string{"namespace": info.namespace}})
		if info.pvName != "" {
			ss.Edges = append(ss.Edges, Edge{From: nid, To: "pv:" + info.pvName, Kind: "binds"})
		}
	}

	for region := range regionsWanted {
		rid := "region:" + region
		ss.Nodes = append(ss.Nodes, Node{ID: rid, Kind: "region", Shape: "square", Name: region, Group: "proxmox", Status: "active"})
		if zonesWanted[region] == nil {
			continue
		}
		for zone := range zonesWanted[region] {
			zid := "zone:" + region + "/" + zone
			ss.Nodes = append(ss.Nodes, Node{ID: zid, ParentID: rid, Kind: "zone", Shape: "square", Name: zone, Group: "proxmox", Status: "active"})
			ss.Edges = append(ss.Edges, Edge{From: rid, To: zid, Kind: "contains"})
		}
	}

	// Match proxmox VMs/LXCs by zone and include only resources tied to relevant zones.
	for region := range regionsWanted {
		px, err := c.proxmox.GetProxmoxCluster(region)
		if err != nil {
			continue
		}
		cl, err := px.Cluster(ctx)
		if err != nil {
			continue
		}
		res, err := cl.Resources(ctx, "")
		if err != nil {
			continue
		}
		for _, r := range res {
			if (r.Type != "qemu" && r.Type != "lxc") || !zonesWanted[region][r.Node] {
				continue
			}
			id := fmt.Sprintf("pve-vm:%s/%d", region, r.VMID)
			ss.Nodes = append(ss.Nodes, Node{ID: id, ParentID: "zone:" + region + "/" + r.Node, Kind: r.Type, Shape: "square", Name: r.Name, Group: "proxmox", Status: r.Status})
		}
	}

	store.Set(dedup(ss))
	return nil
}

func hasSC(nodes []Node, name string) bool {
	for _, n := range nodes {
		if n.Kind == "storageclass" && n.Name == name {
			return true
		}
	}
	return false
}
func dedup(in Snapshot) Snapshot {
	nm := map[string]Node{}
	for _, n := range in.Nodes {
		if n.ID != "" {
			nm[n.ID] = n
		}
	}
	em := map[string]Edge{}
	for _, e := range in.Edges {
		if e.From == "" || e.To == "" {
			continue
		}
		k := e.From + "|" + e.To + "|" + e.Kind
		em[k] = e
	}
	in.Nodes = make([]Node, 0, len(nm))
	for _, n := range nm {
		in.Nodes = append(in.Nodes, n)
	}
	sort.Slice(in.Nodes, func(i, j int) bool { return strings.Compare(in.Nodes[i].ID, in.Nodes[j].ID) < 0 })
	in.Edges = make([]Edge, 0, len(em))
	for _, e := range em {
		in.Edges = append(in.Edges, e)
	}
	return in
}
