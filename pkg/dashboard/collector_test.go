package dashboard

import (
	"context"
	"testing"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/csi"
	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCollectorAssignsPVCParentFromBoundPodNode(t *testing.T) {
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
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: "ns1"},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName: "pv-1",
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
	pvcNode := findNode(snapshot.Nodes, "pvc:ns1/pvc-1")
	if pvcNode == nil {
		t.Fatalf("expected pvc node to exist")
	}
	if got, want := pvcNode.ParentID, "k8s-node:node-1"; got != want {
		t.Fatalf("unexpected pvc parent: got %q want %q", got, want)
	}
}

func TestCollectorOmitsPVCParentWhenPodsAreOnDifferentNodes(t *testing.T) {
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
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1", Namespace: "ns1"},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-1"},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
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
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "ns1"},
			Spec: corev1.PodSpec{
				NodeName: "node-2",
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
	pvcNode := findNode(snapshot.Nodes, "pvc:ns1/pvc-1")
	if pvcNode == nil {
		t.Fatalf("expected pvc node to exist")
	}
	if got := pvcNode.ParentID; got != "" {
		t.Fatalf("expected no pvc parent, got %q", got)
	}
}

func findNode(nodes []Node, id string) *Node {
	for i := range nodes {
		if nodes[i].ID == id {
			return &nodes[i]
		}
	}
	return nil
}
