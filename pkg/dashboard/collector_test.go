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
		&corev1.StorageClass{
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
		&corev1.StorageClass{
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

func ptr(s string) *string {
	return &s
}
