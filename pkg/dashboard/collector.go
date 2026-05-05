package dashboard

import (
	"context"
	"time"

	"github.com/sergelogvinov/proxmox-csi-plugin/pkg/proxmoxpool"
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
	ss := Snapshot{GeneratedAt: time.Now().UTC()}
	//TODO
	store.Set(ss)
	return nil
}
