# Plugin configuration file

This file is used to configure the Proxmox CSI driver plugin.

```yaml
features:
  # Provider type
  provider: default|capmox

clusters:
  # List of Proxmox clusters
  - url: https://cluster-api-1.example.com:8006/api2/json
    # Skip the certificate verification, if needed
    insecure: false
    # Proxmox api token
    token_id: "kubernetes-csi@pve!csi"
    token_id_file: "/etc/proxmox/token_id"          # Optional, alternative to token_id
    token_secret: "secret"
    token_secret_file: "/etc/proxmox/token_secret"  # Optional, alternative to token_secret
    # Region name, which is cluster name
    region: Region-1

    # Native ZFS snapshot mode (opt-in per cluster)
    enable_zfs_snapshots: true

    # SSH access for native ZFS snapshots
    ssh_user: root
    ssh_port: 22
    ssh_use_sudo: false
    ssh_private_key_file: "/etc/proxmox/ssh/id_rsa"  # Optional, alternative to ssh_private_key_secret_ref
    ssh_password_file: "/etc/proxmox/ssh/password"    # Optional, alternative to ssh_password_secret_ref
    ssh_private_key_secret_ref:
      namespace: proxmox-csi
      name: proxmox-node-ssh
      key: id_rsa
    ssh_password_secret_ref:
      namespace: proxmox-csi
      name: proxmox-node-ssh
      key: password

    # Optional per-node SSH overrides. Unset fields fall back to the cluster-wide SSH settings.
    node_ssh_options:
      pve-node-1:
        host: 10.10.0.11
        ssh_user: ubuntu
        ssh_port: 2222
        ssh_use_sudo: true
      pve-node-2:
        host: pve-node-2.example.com

    # Optional UUID namespace for deterministic native snapshot names
    uuid_namespace: "123e4567-e89b-12d3-a456-426614174000"

    # Optional timestamp format used in native snapshot templates
    timestamp_format: "20060102T150405Z"

  # Add more clusters if needed
  - url: https://cluster-api-2.example.com:8006/api2/json
    insecure: false
    token_id: "kubernetes-csi@pve!csi"
    token_secret: "secret"
    region: Region-2
```

## Cluster list

You can define multiple clusters in the `clusters` section.

* `url` - The URL of the Proxmox cluster API.
* `insecure` - Set to `true` to skip TLS certificate verification.
* `token_id` - The Proxmox API token ID.
* `token_id_file` - The path to a file containing the Proxmox API token ID. This is an alternative to `token_id`.
* `token_secret` - The name of the Kubernetes Secret that contains the Proxmox API token.
* `token_secret_file` - The path to a file containing the Proxmox API token secret. This is an alternative to `token_secret`.
* `region` - The name of the region, which is also used as `topology.kubernetes.io/region` label.
* `enable_zfs_snapshots` - Enable native ZFS snapshot mode for this cluster. The default is `false`.
* `ssh_user` - SSH user used for native ZFS snapshot operations. The default is `root`.
* `ssh_port` - SSH port used for native ZFS snapshot operations. The default is `22`.
* `ssh_use_sudo` - Prefix remote ZFS commands with `sudo` when set to `true`.
* `ssh_private_key_file` - Path to a private key file used for SSH auth.
* `ssh_private_key_secret_ref` - Kubernetes Secret reference for the SSH private key. Supports `namespace`, `name`, and `key`.
* `ssh_password_file` - Path to a password file used for SSH auth.
* `ssh_password_secret_ref` - Kubernetes Secret reference for the SSH password. Supports `namespace`, `name`, and `key`.
* `node_ssh_options` - Optional mapping of Proxmox node name to SSH settings. Each entry can override `host`, `ssh_user`, `ssh_port`, `ssh_use_sudo`, and the SSH auth fields. Any omitted values fall back to the cluster-wide SSH settings.
* `uuid_namespace` - Optional UUID namespace used when generating deterministic native snapshot names.
* `timestamp_format` - Optional Go time format used for the timestamp portion of native snapshot names.

## Feature flags

* `provider` - Set the provider type. The default is `default`, which uses provider-id to define the Proxmox VM ID. The `capmox` value is used for working with the Cluster API for Proxmox (CAPMox).

## Native snapshot parameters

When `enable_zfs_snapshots` is enabled for a cluster, `VolumeSnapshotClass` parameters control native snapshot behavior:

* `nativeZfs` - Set to `"false"` to opt out of native snapshots for that class. The default is native if the cluster enables it.
* `zfsSnapshotDeletePolicy` - Controls ZFS cleanup. Supported values: `delete` (default), `release`, `release-destroy`.
* `zfsSnapshotNameTemplate` - Template used to build the ZFS snapshot name.
* `zfsTimestampFormat` - Go time format used by the template field `.Timestamp`.

