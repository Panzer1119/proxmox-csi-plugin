package dashboard

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Snapshot represents the current state of Kubernetes and Proxmox resources
type Snapshot struct {
	GeneratedAt time.Time         `json:"generatedAt"`
	Regions     map[string]Region `json:"regions"`
	Kubernetes  Kubernetes        `json:"kubernetes"`
}

// Region represents a Proxmox cluster
type Region struct {
	Name  string          `json:"name"`
	Zones map[string]Zone `json:"zones"`
	Disks []ProxmoxDisk   `json:"disks,omitempty"` // Shared Disks
}

// Zone represents a Proxmox host
type Zone struct {
	Name  string        `json:"name"`
	VMs   []ProxmoxVM   `json:"vms,omitempty"`
	Disks []ProxmoxDisk `json:"disks,omitempty"` // Local Disks
}

// ProxmoxDisk represents a Proxmox disk
type ProxmoxDisk struct {
	StorageID     string   `json:"storageId"`
	Name          string   `json:"name"`
	SizeBytes     *int64   `json:"sizeBytes"`
	AttachedVMIDs []string `json:"attachedVMIds,omitempty"`
}

// ProxmoxVM represents a Proxmox vm and Kubernetes node
type ProxmoxVM struct {
	ID   string         `json:"id"`
	Name string         `json:"name"`
	Node KubernetesNode `json:"node"`
}

// KubernetesNode represents a Kubernetes node
type KubernetesNode struct {
	KubernetesBase
	Pods []KubernetesPod `json:"pods,omitempty"`
}

// KubernetesPod represents a Kubernetes Pod
type KubernetesPod struct {
	KubernetesBase
	Hostname string            `json:"hostname"`
	Volumes  map[string]string `json:"volumes"`
}

// KubernetesBase contains common Kubernetes resource metadata
type KubernetesBase struct {
	KubernetesReference
	CreatedAt   time.Time         `json:"createdAt"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// KubernetesReference identifies a Kubernetes resource
type KubernetesReference struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// Kubernetes contains global Kubernetes resources
type Kubernetes struct {
	Namespaces             []KubernetesNamespace             `json:"namespaces"`
	StorageClasses         []KubernetesStorageClass          `json:"storageClasses"`
	PersistentVolumeClaims []KubernetesPersistentVolumeClaim `json:"persistentVolumeClaims"`
	PersistentVolumes      []KubernetesPersistentVolume      `json:"persistentVolumes"`
}

// KubernetesNamespace represents a Kubernetes namespace
type KubernetesNamespace struct {
	KubernetesBase
	IsPrivileged bool `json:"isPrivileged"`
}

// KubernetesStorageClass represents a Kubernetes storage class
type KubernetesStorageClass struct {
	KubernetesBase
	Provisioner          string `json:"provisioner"`
	ReclaimPolicy        string `json:"reclaimPolicy"`
	AllowVolumeExpansion bool   `json:"allowVolumeExpansion"`
	VolumeBindingMode    string `json:"volumeBindingMode"`
	IsDefault            bool   `json:"isDefault"`
}

// KubernetesPersistentVolumeClaim represents a Kubernetes persistent volume claim
type KubernetesPersistentVolumeClaim struct {
	KubernetesBase
	StorageClassName string   `json:"storageClassName"`
	Bound            bool     `json:"bound"`
	AccessMode       []string `json:"accessMode"`
	CapacityRequest  string   `json:"capacityRequest"`
	VolumeMode       string   `json:"volumeMode"`
	VolumeName       string   `json:"volumeName"`
}

// KubernetesPersistentVolume represents a Kubernetes persistent volume
type KubernetesPersistentVolume struct {
	KubernetesBase
	StorageClassName string                        `json:"storageClassName"`
	Bound            bool                          `json:"bound"`
	AccessMode       []string                      `json:"accessMode"`
	Capacity         string                        `json:"capacity"`
	Mode             string                        `json:"mode"`
	Status           corev1.PersistentVolumeStatus `json:"status"`
	ClaimReference   *KubernetesReference          `json:"claimReference,omitempty"`
	VolumeHandle     string                        `json:"volumeHandle"`
	VolumeReference  *VolumeReference              `json:"volumeReference,omitempty"`
}

// VolumeReference contains parsed volume handle information
type VolumeReference struct {
	Region  string `json:"region"`
	Zone    string `json:"zone"`
	Node    string `json:"node"`
	Storage string `json:"storage"`
	Disk    string `json:"disk"`
}
