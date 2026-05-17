# Volume Snapshot

The Proxmox CSI Driver supports two snapshot modes:

1. **Legacy snapshot mode** - the default. This creates a copy of the volume and works without SSH access to the Proxmox nodes.
2. **Native ZFS snapshot mode** - opt-in per cluster. This runs `zfs` commands on the target Proxmox node over SSH and creates real ZFS snapshots.

Native mode is intended for ZFS-backed storage only. If the storage config does not expose a ZFS root dataset via Proxmox storage metadata, snapshot creation fails.

## Native ZFS snapshot requirements

Enable native mode per cluster in the driver configuration and provide SSH access for the target Proxmox nodes.

```yaml
clusters:
  - url: https://cluster-api-1.example.com:8006/api2/json
    region: cluster-1
    token_id: kubernetes-csi@pve!csi
    token_secret: secret
    enable_zfs_snapshots: true
    ssh_user: root
    ssh_private_key_secret_ref:
      namespace: proxmox-csi
      name: proxmox-node-ssh
      key: id_rsa
    node_ssh_options:
      pve-node-1:
        host: 10.10.0.11
        ssh_user: ubuntu
        ssh_port: 2222
      pve-node-2:
        host: pve-node-2.example.com
```

SSH authentication can also be configured with `ssh_private_key_file`, `ssh_password_file`, or the matching secret references. The default SSH user is `root` and the default SSH port is `22`.

`node_ssh_options` is optional, but recommended when a Proxmox node needs a different SSH host, user, port, sudo setting, or auth configuration than the cluster-wide defaults.

## VolumeSnapshotClass

Create a `VolumeSnapshotClass` and point it at the Proxmox CSI driver.

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: proxmox-zfs-snapshots
driver: csi.proxmox.sinextra.dev
deletionPolicy: Delete
parameters:
  # Optional. Disable native mode for this class even if the cluster enables it.
  nativeZfs: "true"

  # Optional: UUID namespace for deterministic UUIDv5 snapshot names
  uuidNamespace: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

  # Notes on namespace resolution
  # The UUID namespace used to derive deterministic UUIDv5 values for volume
  # and snapshot names can be configured in multiple places. The driver chooses
  # the namespace using the following precedence (highest-first):
  # 1. The `uuidNamespace` parameter on the VolumeSnapshotClass (shown above).
  # 2. The storage-level `uuidNamespace` parameter (e.g. in a StorageClass or
  #    storage configuration used by the source volume).
  # 3. The cluster-wide `uuidNamespace` configured in the driver's cluster
  #    configuration (a global fallback).
  #
  # This lets you override the namespace for a specific snapshot class while
  # retaining existing per-storage or global defaults when the parameter is
  # omitted.

  # Native ZFS deletion policy:
  # - delete: destroy the ZFS snapshot when the Kubernetes VolumeSnapshot is deleted (default)
  # - release: only remove the hold tag
  # - release-destroy: create a hold tag on snapshot creation, release it on delete, then destroy the snapshot
  zfsSnapshotDeletePolicy: delete

  # Optional. Snapshot name template for native mode.
  # Available template fields: .Timestamp, .UUID, .Namespace, .Name
  zfsSnapshotNameTemplate: vs-{{ .Timestamp }}-{{ .UUID }}

  # Optional. Go time format used for .Timestamp.
  zfsTimestampFormat: 20060102T150405Z
```

Notes:

- `deletionPolicy` is the Kubernetes snapshot-controller policy for the `VolumeSnapshotContent` object.
- `zfsSnapshotDeletePolicy` is the driver-side ZFS cleanup policy. These two settings are separate and both matter.
- The driver stores native snapshot handles as JSON, so dataset names can contain slashes safely.
- In native mode, the ZFS snapshot name is generated from the template and includes a deterministic UUIDv5 derived from the `VolumeSnapshot` namespace and name.

## Creating a VolumeSnapshot

Create a `VolumeSnapshot` that references the `VolumeSnapshotClass` above.

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: snapshot-name
  namespace: default
spec:
  volumeSnapshotClassName: proxmox-zfs-snapshots
  source:
    persistentVolumeClaimName: pvc-source-name
```

Check the snapshot status:

```bash
kubectl -n default get volumesnapshot snapshot-name
```

Once `READYTOUSE` becomes `true`, you can restore from the snapshot:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: restored-pvc
  namespace: default
spec:
  storageClassName: proxmox-zfs
  dataSource:
    apiGroup: snapshot.storage.k8s.io
    kind: VolumeSnapshot
    name: snapshot-name
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
```

The restore size must be equal to or larger than the original volume size.

## Legacy snapshot mode

If native ZFS snapshots are not enabled for the cluster, the driver keeps using the legacy copy-based snapshot implementation. This remains the default behavior.
