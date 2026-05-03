variable "TAG_NAME" {
  default = "edge"
}

variable "SHA" {
  default = "main"
}

group "default" {
  targets = [
    "proxmox-csi-controller",
    "proxmox-csi-node",
    "pvecsictl"
  ]
}

target "proxmox-csi-controller" {
  context    = "."
  dockerfile = "Dockerfile"
  target     = "proxmox-csi-controller"
  platforms  = ["linux/amd64", "linux/arm64"]
  args = {
    TAG = "${TAG_NAME}"
    SHA = "${SHA}"
  }
  cache-from = ["type=gha,scope=proxmox-csi-controller"]
  cache-to   = ["type=gha,scope=proxmox-csi-controller,mode=max"]
}

target "proxmox-csi-node" {
  context    = "."
  dockerfile = "Dockerfile"
  target     = "proxmox-csi-node"
  platforms  = ["linux/amd64", "linux/arm64"]
  args = {
    TAG = "${TAG_NAME}"
    SHA = "${SHA}"
  }
  cache-from = ["type=gha,scope=proxmox-csi-node"]
  cache-to   = ["type=gha,scope=proxmox-csi-node,mode=max"]
}

target "pvecsictl" {
  context    = "."
  dockerfile = "Dockerfile"
  target     = "pvecsictl"
  platforms  = ["linux/amd64", "linux/arm64"]
  args = {
    TAG = "${TAG_NAME}"
    SHA = "${SHA}"
  }
  cache-from = ["type=gha,scope=pvecsictl"]
  cache-to   = ["type=gha,scope=pvecsictl,mode=max"]
}

