package dashboard

import "time"

// Snapshot represents the current state of Kubernetes and Proxmox resources
type Snapshot struct {
	GeneratedAt time.Time  `json:"generatedAt"`
	Kubernetes  Kubernetes `json:"kubernetes"`
	Proxmox     Proxmox    `json:"proxmox"`
}

// KubernetesReference identifies a Kubernetes resource
type KubernetesReference struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// KubernetesBase contains common Kubernetes resource metadata
type KubernetesBase struct {
	KubernetesReference
	CreatedAt   time.Time         `json:"createdAt"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// KubernetesNode represents a Kubernetes node
type KubernetesNode struct {
	KubernetesBase
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

// KubernetesPersistentVolumeClaim represents a PVC
type KubernetesPersistentVolumeClaim struct {
	KubernetesBase
	StorageClassName string   `json:"storageClassName"`
	Bound            bool     `json:"bound"`
	AccessMode       []string `json:"accessMode"`
	CapacityRequest  string   `json:"capacityRequest"`
	VolumeMode       string   `json:"volumeMode"`
	VolumeName       string   `json:"volumeName"`
}

// KubernetesPersistentVolume represents a PV
type KubernetesPersistentVolume struct {
	KubernetesBase
	StorageClassName string               `json:"storageClassName"`
	Bound            bool                 `json:"bound"`
	AccessMode       []string             `json:"accessMode"`
	Capacity         string               `json:"capacity"`
	Mode             string               `json:"mode"`
	ClaimReference   *KubernetesReference `json:"claimReference,omitempty"`
	VolumeHandle     string               `json:"volumeHandle"`
	VolumeReference  *VolumeReference     `json:"volumeReference,omitempty"`
}

// VolumeReference contains parsed volume handle information
type VolumeReference struct {
	Region  string `json:"region"`
	Zone    string `json:"zone"`
	Node    string `json:"node"`
	Storage string `json:"storage"`
	Disk    string `json:"disk"`
}

// KubernetesPod represents a Pod
type KubernetesPod struct {
	KubernetesBase
	Hostname string            `json:"hostname"`
	NodeName string            `json:"nodeName"`
	Volumes  map[string]string `json:"volumes"`
}

// Kubernetes contains all Kubernetes resources
type Kubernetes struct {
	Nodes                  []KubernetesNode                  `json:"nodes"`
	Namespaces             []KubernetesNamespace             `json:"namespaces"`
	StorageClasses         []KubernetesStorageClass          `json:"storageClasses"`
	PersistentVolumes      []KubernetesPersistentVolume      `json:"persistentVolumes"`
	PersistentVolumeClaims []KubernetesPersistentVolumeClaim `json:"persistentVolumeClaims"`
	Pods                   []KubernetesPod                   `json:"pods"`
}

// ProxmoxCluster represents a Proxmox cluster
type ProxmoxCluster struct {
	Name   string `json:"name"`
	Region string `json:"region"`
}

// ProxmoxNode represents a Proxmox host
type ProxmoxNode struct {
	ClusterName string `json:"clusterName"`
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
}

// ProxmoxVM represents a Proxmox VM
type ProxmoxVM struct {
	ClusterName string `json:"clusterName"`
	NodeName    string `json:"nodeName"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Status      string `json:"status"`
}

// ProxmoxDisk represents a Proxmox disk
type ProxmoxDisk struct {
	ClusterName     string           `json:"clusterName"`
	StorageID       string           `json:"storageId"`
	Name            string           `json:"name"`
	SizeBytes       int64            `json:"sizeBytes"`
	VolumeReference *VolumeReference `json:"volumeReference"`
}

// ProxmoxSharedDisk represents a shared Proxmox disk
type ProxmoxSharedDisk struct {
	ProxmoxDisk
	AttachedVMIds []string `json:"attachedVMIds"`
}

// ProxmoxLocalDisk represents a local Proxmox disk
type ProxmoxLocalDisk struct {
	ProxmoxDisk
	NodeID       string  `json:"nodeId"`
	AttachedVMID *string `json:"attachedVMId,omitempty"`
}

// Proxmox contains all Proxmox resources
type Proxmox struct {
	Clusters    []ProxmoxCluster    `json:"clusters"`
	Nodes       []ProxmoxNode       `json:"nodes"`
	VMs         []ProxmoxVM         `json:"vms"`
	SharedDisks []ProxmoxSharedDisk `json:"sharedDisks"`
	LocalDisks  []ProxmoxLocalDisk  `json:"localDisks"`
}
