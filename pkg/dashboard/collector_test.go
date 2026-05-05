package dashboard

import (
	"context"
	"testing"

	proxmox "github.com/luthermonson/go-proxmox"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCollectorCollectsPersistentVolumes(t *testing.T) {
	kube := fake.NewSimpleClientset(
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       csi.DriverName,
						VolumeHandle: "region-1/zone-1/storage/vm-100-pv-1",
					},
				},
				StorageClassName: "sc-1",
			},
		},
	)

	store := NewStore()
	collector := &Collector{kube: kube, proxmox: &proxmoxpool.ProxmoxPool{}}
	if err := collector.Collect(context.Background(), store); err != nil {
		t.Fatalf("collect: %v", err)
	}

	snapshot := store.Get()
	if len(snapshot.Kubernetes.PersistentVolumes) == 0 {
		t.Fatalf("expected at least one persistent volume")
	}

	pv := snapshot.Kubernetes.PersistentVolumes[0]
	if got, want := pv.Name, "pv-1"; got != want {
		t.Fatalf("unexpected pv name: got %q want %q", got, want)
	}
	if got, want := pv.VolumeHandle, "region-1/zone-1/storage/vm-100-pv-1"; got != want {
		t.Fatalf("unexpected volume handle: got %q want %q", got, want)
	}
	if pv.VolumeReference == nil {
		t.Fatalf("expected volume reference to be set")
	}
	if got, want := pv.VolumeReference.Region, "region-1"; got != want {
		t.Fatalf("unexpected region: got %q want %q", got, want)
	}
}

func TestCollectorCollectsPersistentVolumeClaims(t *testing.T) {
	kube := fake.NewSimpleClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "sc-1"},
			Provisioner: csi.DriverName,
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: "ns1"},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-1",
				StorageClassName: ptr("sc-1"),
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
	)

	store := NewStore()
	collector := &Collector{kube: kube, proxmox: &proxmoxpool.ProxmoxPool{}}
	if err := collector.Collect(context.Background(), store); err != nil {
		t.Fatalf("collect: %v", err)
	}

	snapshot := store.Get()
	if len(snapshot.Kubernetes.PersistentVolumeClaims) == 0 {
		t.Fatalf("expected at least one persistent volume claim")
	}

	pvc := snapshot.Kubernetes.PersistentVolumeClaims[0]
	if got, want := pvc.Name, "pvc-1"; got != want {
		t.Fatalf("unexpected pvc name: got %q want %q", got, want)
	}
	if got, want := pvc.Namespace, "ns1"; got != want {
		t.Fatalf("unexpected pvc namespace: got %q want %q", got, want)
	}
}

func TestCollectorCollectsPods(t *testing.T) {
	kube := fake.NewSimpleClientset(
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "sc-1"},
			Provisioner: csi.DriverName,
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: "ns1"},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-1",
				StorageClassName: ptr("sc-1"),
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns1"},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc-1"},
					},
				}},
			},
		},
	)

	store := NewStore()
	collector := &Collector{kube: kube, proxmox: &proxmoxpool.ProxmoxPool{}}
	if err := collector.Collect(context.Background(), store); err != nil {
		t.Fatalf("collect: %v", err)
	}

	snapshot := store.Get()
	if len(snapshot.Kubernetes.Pods) == 0 {
		t.Fatalf("expected at least one pod")
	}

	pod := snapshot.Kubernetes.Pods[0]
	if got, want := pod.Name, "pod-1"; got != want {
		t.Fatalf("unexpected pod name: got %q want %q", got, want)
	}
	if got, want := pod.Namespace, "ns1"; got != want {
		t.Fatalf("unexpected pod namespace: got %q want %q", got, want)
	}
	if got, want := pod.NodeName, "node-1"; got != want {
		t.Fatalf("unexpected pod node name: got %q want %q", got, want)
	}
	if len(pod.Volumes) == 0 {
		t.Fatalf("expected at least one volume in pod")
	}
}

func TestDiskUsageKeyAndMatching(t *testing.T) {
	if got, want := diskUsageKey("region-1", "", "storage", "vm-100-disk-0", true), "region-1/storage/vm-100-disk-0"; got != want {
		t.Fatalf("unexpected shared key: got %q want %q", got, want)
	}
	if got, want := diskUsageKey("region-1", "node-1", "storage", "vm-100-disk-0", false), "region-1/node-1/storage/vm-100-disk-0"; got != want {
		t.Fatalf("unexpected local key: got %q want %q", got, want)
	}
	if !matchesDiskValue("local-lvm:vm-100-disk-0,backup=1", "vm-100-disk-0") {
		t.Fatalf("expected disk value to match")
	}
	if matchesDiskValue("local-lvm:vm-101-disk-0,backup=1", "vm-100-disk-0") {
		t.Fatalf("did not expect disk value to match")
	}
}

func TestProxmoxNodeFromStatus(t *testing.T) {
	got := proxmoxNodeFromStatus("region-1", &proxmox.NodeStatus{
		Status: "online",
		Node:   "pve-01",
		Name:   "pve-01",
		ID:     "node/pve-01",
	})

	if got.ClusterName != "region-1" {
		t.Fatalf("unexpected cluster name: got %q", got.ClusterName)
	}
	if got.ID != "node/pve-01" {
		t.Fatalf("unexpected id: got %q", got.ID)
	}
	if got.Name != "pve-01" {
		t.Fatalf("unexpected name: got %q", got.Name)
	}
	if got.NodeID != "pve-01" {
		t.Fatalf("unexpected node id: got %q", got.NodeID)
	}
	if got.Type != "node" {
		t.Fatalf("unexpected type: got %q", got.Type)
	}
	if got.Status != "online" {
		t.Fatalf("unexpected status: got %q", got.Status)
	}
}

func TestProxmoxNodeFromResource(t *testing.T) {
	got := proxmoxNodeFromResource("region-1", &proxmox.Node{
		ID:   "pve-01",
		Name: "pve-01",
	})

	if got.ID != "pve-01" {
		t.Fatalf("unexpected id: got %q", got.ID)
	}
	if got.Name != "pve-01" {
		t.Fatalf("unexpected name: got %q", got.Name)
	}
}

func TestProxmoxVMFromResource(t *testing.T) {
	got := proxmoxVMFromResource("region-1", &proxmox.ClusterResource{
		VMID:   100,
		Name:   "vm-100",
		Node:   "pve-01",
		Type:   "qemu",
		Status: "running",
	})

	if got.ClusterName != "region-1" {
		t.Fatalf("unexpected cluster name: got %q", got.ClusterName)
	}
	if got.ID != "100" {
		t.Fatalf("unexpected id: got %q", got.ID)
	}
	if got.Name != "vm-100" {
		t.Fatalf("unexpected name: got %q", got.Name)
	}
	if got.NodeName != "pve-01" {
		t.Fatalf("unexpected node id: got %q", got.NodeName)
	}
	if got.Type != "qemu" {
		t.Fatalf("unexpected type: got %q", got.Type)
	}
	if got.Status != "running" {
		t.Fatalf("unexpected status: got %q", got.Status)
	}
}

func ptr(s string) *string {
	return &s
}
