import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';

const diagramEl = document.getElementById('diagram');
const detailsEl = document.getElementById('details');
const filterEl = document.getElementById('filter');
const copyBtn = document.getElementById('copy');
const copyFallbackBtn = document.getElementById('copyFallback');
const statusEl = document.getElementById('status');
const summaryEl = document.getElementById('summary');
const generatedAtEl = document.getElementById('generatedAt');

const emptySnapshot = {
    generatedAt: '',
    regions: {},
    kubernetes: {
        namespaces: [],
        storageClasses: [],
        persistentVolumeClaims: [],
        persistentVolumes: []
    }
};

let snapshot = emptySnapshot;
let mermaidSource = '';
let mermaidSourceFallback = '';
let renderToken = 0;
let statusResetTimer = null;
let baseStatusText = 'Waiting for snapshot…';

mermaid.initialize({startOnLoad: false, securityLevel: 'loose'});

function hashString(value) {
    let hash = 2166136261;
    for (let i = 0; i < value.length; i += 1) {
        hash ^= value.charCodeAt(i);
        hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(36);
}

function sanitizeId(value) {
    return String(value).replace(/[^a-zA-Z0-9_]/g, '_');
}

function mermaidId(prefix, key) {
    return `${prefix}_${sanitizeId(key)}_${hashString(key)}`;
}

function esc(value) {
    return String(value).replaceAll('"', '\\"');
}

function formatBytes(bytes) {
    if (!Number.isFinite(bytes)) {
        return '';
    }
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    let value = bytes;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
        value /= 1024;
        unit += 1;
    }
    return `${value.toFixed(value >= 10 || unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function formatGeneratedAt(value) {
    if (!value) {
        return 'No snapshot received yet';
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return `Generated at ${value}`;
    }
    return `Generated at ${date.toLocaleString()}`;
}

function label(text, ...badges) {
    return [text, ...badges.filter(Boolean)].join(' · ');
}

function formatDiskName(disk) {
    const parts = [disk.name || disk.storageId || 'disk'];
    if (disk.sizeBytes && Number.isFinite(disk.sizeBytes)) {
        parts.push(formatBytes(disk.sizeBytes));
    }
    if (disk.attachedVMIds?.length) {
        parts.push(`${disk.attachedVMIds.length} VM${disk.attachedVMIds.length === 1 ? '' : 's'}`);
    }
    return parts.join(' · ');
}

function formatVmName(vm) {
    return label(vm.name || vm.id || 'vm', vm.id ? `#${vm.id}` : '');
}

function formatNodeName(node) {
    return label(node.name || 'node', node.labels?.['kubernetes.io/hostname'] || '');
}

function formatPodName(pod) {
    return label([pod.namespace, pod.name].filter(Boolean).join('/'), pod.volumes && Object.keys(pod.volumes).length ? `${Object.keys(pod.volumes).length} volume${Object.keys(pod.volumes).length === 1 ? '' : 's'}` : '');
}

function formatNamespaceName(namespace) {
    return label(namespace.name || 'namespace', namespace.isPrivileged ? 'privileged' : '');
}

function formatStorageClassName(sc) {
    return label(sc.name || 'storageclass', sc.isDefault ? 'default' : '');
}

function formatPvcName(pvc) {
    return label(pvc.name || 'pvc', pvc.bound ? 'bound' : 'pending', pvc.capacityRequest || '', pvc.storageClassName || '');
}

function formatPvName(pv) {
    return label(pv.name || 'pv', pv.status?.phase || 'unknown', pv.bound ? 'bound' : 'unbound');
}

function createItem(items, index, item) {
    const mermaid = mermaidId(item.prefix, item.key);
    const labelText = item.label;
    const searchText = [
        item.kind,
        labelText,
        item.badgeText,
        item.searchText,
        item.key
    ].filter(Boolean).join(' ').toLowerCase();
    const node = {
        ...item,
        mermaid,
        searchText
    };
    items.push(node);
    index.set(item.key, node);
    return node;
}

function collectSnapshotModel(data) {
    const items = [];
    const index = new Map();
    const edges = [];
    const stats = {
        regions: 0,
        zones: 0,
        vms: 0,
        nodes: 0,
        pods: 0,
        namespaces: 0,
        privilegedNamespaces: 0,
        storageClasses: 0,
        defaultStorageClasses: 0,
        pvcs: 0,
        boundPvcs: 0,
        pvs: 0,
        boundPvs: 0,
        disks: 0,
        attachedDisks: 0
    };

    const regions = Object.entries(data?.regions || {}).sort(([a], [b]) => a.localeCompare(b));
    const proxmoxRootKey = 'root|proxmox';
    createItem(items, index, {
        key: proxmoxRootKey,
        prefix: 'root',
        kind: 'proxmox-root',
        type: 'group',
        label: 'Proxmox cluster',
        badgeText: '',
        searchText: 'proxmox cluster'
    });
    const kubernetesRootKey = 'root|kubernetes';
    createItem(items, index, {
        key: kubernetesRootKey,
        prefix: 'root',
        kind: 'kubernetes-root',
        type: 'group',
        label: 'Kubernetes resources',
        badgeText: '',
        searchText: 'kubernetes resources'
    });

    const vmKeyByRegionZoneId = new Map();
    const pvcKeyByNsName = new Map();
    const pvKeyByName = new Map();
    const scKeyByName = new Map();
    const diskKeyByExact = new Map();

    const k8s = data?.kubernetes || {};
    const namespaces = [...(k8s.namespaces || [])].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    const storageClasses = [...(k8s.storageClasses || [])].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    const pvcs = [...(k8s.persistentVolumeClaims || [])].sort((a, b) => (a.namespace || '').localeCompare(b.namespace || '') || (a.name || '').localeCompare(b.name || ''));
    const pvs = [...(k8s.persistentVolumes || [])].sort((a, b) => (a.name || '').localeCompare(b.name || ''));

    const storageRootKey = 'root|kubernetes|storage';
    createItem(items, index, {
        key: storageRootKey,
        prefix: 'storage',
        kind: 'storage',
        type: 'group',
        label: 'Storage inventory',
        badgeText: '',
        parentKey: kubernetesRootKey,
        searchText: 'storage inventory'
    });

    const namespaceRootKey = 'root|kubernetes|namespaces';
    createItem(items, index, {
        key: namespaceRootKey,
        prefix: 'namespace-root',
        kind: 'namespace',
        type: 'group',
        label: 'Namespaces',
        badgeText: '',
        parentKey: kubernetesRootKey,
        searchText: 'namespaces'
    });

    for (const namespace of namespaces) {
        stats.namespaces += 1;
        if (namespace.isPrivileged) {
            stats.privilegedNamespaces += 1;
        }
        const nsKey = `namespace|${namespace.name}`;
        createItem(items, index, {
            key: nsKey,
            prefix: 'namespace',
            kind: 'namespace',
            type: 'group',
            label: formatNamespaceName(namespace),
            badgeText: namespace.isPrivileged ? 'privileged' : '',
            parentKey: namespaceRootKey,
            searchText: [namespace.name, namespace.kind, namespace.namespace, namespace.isPrivileged ? 'privileged' : '', namespace.annotations ? Object.keys(namespace.annotations).join(' ') : '', namespace.labels ? Object.keys(namespace.labels).join(' ') : ''].filter(Boolean).join(' ')
        });
    }

    for (const sc of storageClasses) {
        stats.storageClasses += 1;
        if (sc.isDefault) {
            stats.defaultStorageClasses += 1;
        }
        const scKey = `storageclass|${sc.name}`;
        scKeyByName.set(sc.name, scKey);
        createItem(items, index, {
            key: scKey,
            prefix: 'storageclass',
            kind: 'storageclass',
            type: 'service',
            label: formatStorageClassName(sc),
            badgeText: sc.isDefault ? 'default' : '',
            parentKey: storageRootKey,
            searchText: [sc.name, sc.provisioner, sc.reclaimPolicy, sc.volumeBindingMode, sc.isDefault ? 'default' : '', sc.allowVolumeExpansion ? 'expandable' : ''].filter(Boolean).join(' ')
        });
    }

    for (const pv of pvs) {
        stats.pvs += 1;
        if (pv.bound) {
            stats.boundPvs += 1;
        }
        const pvKey = `pv|${pv.name}`;
        pvKeyByName.set(pv.name, pvKey);
        createItem(items, index, {
            key: pvKey,
            prefix: 'pv',
            kind: 'pv',
            type: 'service',
            label: formatPvName(pv),
            badgeText: pv.status?.phase || '',
            parentKey: storageRootKey,
            searchText: [pv.name, pv.storageClassName, pv.capacity, pv.mode, pv.status?.phase || '', pv.bound ? 'bound' : 'unbound', pv.volumeHandle, pv.volumeReference ? [pv.volumeReference.region, pv.volumeReference.zone, pv.volumeReference.node, pv.volumeReference.storage, pv.volumeReference.disk].filter(Boolean).join(' ') : '', pv.claimReference ? [pv.claimReference.namespace, pv.claimReference.name].filter(Boolean).join(' ') : ''].filter(Boolean).join(' ')
        });
    }

    for (const pvc of pvcs) {
        stats.pvcs += 1;
        if (pvc.bound) {
            stats.boundPvcs += 1;
        }
        const nsKey = `namespace|${pvc.namespace}`;
        const pvcKey = `pvc|${pvc.namespace}|${pvc.name}`;
        pvcKeyByNsName.set(`${pvc.namespace}/${pvc.name}`, pvcKey);
        createItem(items, index, {
            key: pvcKey,
            prefix: 'pvc',
            kind: 'pvc',
            type: 'service',
            label: formatPvcName(pvc),
            badgeText: pvc.bound ? 'bound' : 'pending',
            parentKey: nsKey,
            searchText: [pvc.namespace, pvc.name, pvc.storageClassName, pvc.bound ? 'bound' : 'pending', pvc.capacityRequest, pvc.volumeMode, pvc.volumeName, ...(pvc.accessMode || [])].filter(Boolean).join(' ')
        });
    }

    for (const region of regions) {
        const [regionName, regionValue] = region;
        stats.regions += 1;
        const regionKey = `region|${regionName}`;
        createItem(items, index, {
            key: regionKey,
            prefix: 'region',
            kind: 'region',
            type: 'group',
            label: regionValue?.name || regionName,
            badgeText: '',
            parentKey: proxmoxRootKey,
            searchText: [regionName, regionValue?.name || '', regionValue?.zones ? Object.keys(regionValue.zones).join(' ') : '', regionValue?.disks ? regionValue.disks.map(formatDiskName).join(' ') : ''].filter(Boolean).join(' ')
        });

        const zones = Object.entries(regionValue?.zones || {}).sort(([a], [b]) => a.localeCompare(b));
        for (const zone of zones) {
            const [zoneName, zoneValue] = zone;
            stats.zones += 1;
            const zoneKey = `zone|${regionName}|${zoneName}`;
            createItem(items, index, {
                key: zoneKey,
                prefix: 'zone',
                kind: 'zone',
                type: 'group',
                label: zoneValue?.name || zoneName,
                badgeText: '',
                parentKey: regionKey,
                searchText: [regionName, zoneName, zoneValue?.name || '', zoneValue?.vms ? zoneValue.vms.map(formatVmName).join(' ') : '', zoneValue?.disks ? zoneValue.disks.map(formatDiskName).join(' ') : ''].filter(Boolean).join(' ')
            });

            for (const disk of zoneValue?.disks || []) {
                stats.disks += 1;
                if (disk.attachedVMIds?.length) {
                    stats.attachedDisks += 1;
                }
                const diskKey = `disk|local|${regionName}|${zoneName}|${disk.storageId}|${disk.name}`;
                diskKeyByExact.set(`${regionName}|${zoneName}|${disk.storageId}|${disk.name}`, diskKey);
                createItem(items, index, {
                    key: diskKey,
                    prefix: 'local-disk',
                    kind: 'local-disk',
                    type: 'service',
                    label: formatDiskName(disk),
                    badgeText: disk.attachedVMIds?.length ? `${disk.attachedVMIds.length} attached` : '',
                    parentKey: zoneKey,
                    searchText: [regionName, zoneName, disk.storageId, disk.name, disk.sizeBytes ? String(disk.sizeBytes) : '', ...(disk.attachedVMIds || [])].filter(Boolean).join(' ')
                });
            }

            for (const vm of zoneValue?.vms || []) {
                stats.vms += 1;
                const vmKey = `vm|${regionName}|${zoneName}|${vm.id}`;
                vmKeyByRegionZoneId.set(`${regionName}|${zoneName}|${vm.id}`, vmKey);
                createItem(items, index, {
                    key: vmKey,
                    prefix: 'qemu',
                    kind: 'qemu',
                    type: 'group',
                    label: formatVmName(vm),
                    badgeText: vm.node?.kind ? 'node attached' : 'unmapped',
                    parentKey: zoneKey,
                    searchText: [regionName, zoneName, vm.id, vm.name, vm.node?.name || '', vm.node?.kind || '', vm.node?.hostname || ''].filter(Boolean).join(' ')
                });

                const node = vm.node || {};
                if (node.kind) {
                    stats.nodes += 1;
                    const nodeKey = `node|${node.uid || node.name || vmKey}`;
                    createItem(items, index, {
                        key: nodeKey,
                        prefix: 'k8s-node',
                        kind: 'k8s-node',
                        type: 'group',
                        label: formatNodeName(node),
                        badgeText: node.pods?.length ? `${node.pods.length} pod${node.pods.length === 1 ? '' : 's'}` : '',
                        parentKey: vmKey,
                        searchText: [node.kind, node.name, node.uid, node.namespace, node.createdAt, ...(node.labels ? Object.entries(node.labels).flat() : []), ...(node.annotations ? Object.entries(node.annotations).flat() : [])].filter(Boolean).join(' ')
                    });

                    for (const pod of node.pods || []) {
                        stats.pods += 1;
                        const podKey = `pod|${pod.namespace}|${pod.name}|${pod.uid}`;
                        createItem(items, index, {
                            key: podKey,
                            prefix: 'pod',
                            kind: 'pod',
                            type: 'service',
                            label: formatPodName(pod),
                            badgeText: pod.hostname || '',
                            parentKey: nodeKey,
                            searchText: [pod.namespace, pod.name, pod.uid, pod.hostname, ...(pod.volumes ? Object.entries(pod.volumes).flat() : []), pod.kind || '', pod.createdAt || '', ...(pod.labels ? Object.entries(pod.labels).flat() : []), ...(pod.annotations ? Object.entries(pod.annotations).flat() : [])].filter(Boolean).join(' ')
                        });

                        for (const [volumeName, pvName] of Object.entries(pod.volumes || {})) {
                            if (!pvName) {
                                continue;
                            }
                            const pvKey = pvKeyByName.get(pvName);
                            if (pvKey) {
                                edges.push({from: podKey, to: pvKey, kind: 'uses', label: volumeName});
                            }
                        }
                    }

                    edges.push({from: vmKey, to: nodeKey, kind: 'hosts', label: ''});
                }
            }
        }

        for (const disk of regionValue?.disks || []) {
            stats.disks += 1;
            if (disk.attachedVMIds?.length) {
                stats.attachedDisks += 1;
            }
            const diskKey = `disk|shared|${regionName}|${disk.storageId}|${disk.name}`;
            diskKeyByExact.set(`${regionName}||${disk.storageId}|${disk.name}`, diskKey);
            createItem(items, index, {
                key: diskKey,
                prefix: 'shared-disk',
                kind: 'shared-disk',
                type: 'service',
                label: formatDiskName(disk),
                badgeText: disk.attachedVMIds?.length ? `${disk.attachedVMIds.length} attached` : '',
                parentKey: regionKey,
                searchText: [regionName, disk.storageId, disk.name, disk.sizeBytes ? String(disk.sizeBytes) : '', ...(disk.attachedVMIds || [])].filter(Boolean).join(' ')
            });
        }
    }

    for (const region of regions) {
        const [regionName, regionValue] = region;
        for (const zone of Object.entries(regionValue?.zones || {})) {
            const [zoneName, zoneValue] = zone;
            for (const disk of zoneValue?.disks || []) {
                const diskKey = diskKeyByExact.get(`${regionName}|${zoneName}|${disk.storageId}|${disk.name}`);
                if (!diskKey) {
                    continue;
                }
                for (const vmId of disk.attachedVMIds || []) {
                    const vmKey = vmKeyByRegionZoneId.get(`${regionName}|${zoneName}|${vmId}`) || [...vmKeyByRegionZoneId.entries()].find(([key]) => key.endsWith(`|${vmId}`))?.[1];
                    if (vmKey) {
                        edges.push({from: diskKey, to: vmKey, kind: 'attached', label: ''});
                    }
                }
            }
        }
        for (const disk of regionValue?.disks || []) {
            const diskKey = diskKeyByExact.get(`${regionName}||${disk.storageId}|${disk.name}`);
            if (!diskKey) {
                continue;
            }
            for (const vmId of disk.attachedVMIds || []) {
                const vmKey = [...vmKeyByRegionZoneId.entries()].find(([key]) => key.endsWith(`|${vmId}`))?.[1];
                if (vmKey) {
                    edges.push({from: diskKey, to: vmKey, kind: 'attached', label: ''});
                }
            }
        }
    }

    for (const pvc of pvcs) {
        const pvcKey = pvcKeyByNsName.get(`${pvc.namespace}/${pvc.name}`);
        if (!pvcKey) {
            continue;
        }
        const scKey = scKeyByName.get(pvc.storageClassName);
        if (scKey) {
            edges.push({from: pvcKey, to: scKey, kind: 'provisioned-by', label: ''});
        }
        if (pvc.volumeName) {
            const pvKey = pvKeyByName.get(pvc.volumeName);
            if (pvKey) {
                edges.push({from: pvcKey, to: pvKey, kind: 'bound-to', label: ''});
            }
        }
    }

    for (const pv of pvs) {
        const pvKey = pvKeyByName.get(pv.name);
        if (!pvKey) {
            continue;
        }
        const ref = pv.volumeReference;
        if (ref) {
            const refKey = diskKeyByExact.get(`${ref.region}|${ref.zone || ''}|${ref.storage}|${ref.disk}`) || diskKeyByExact.get(`${ref.region}||${ref.storage}|${ref.disk}`);
            if (refKey) {
                edges.push({from: pvKey, to: refKey, kind: 'backed-by', label: ''});
            }
        }
    }

    return {items, edges, stats};
}

function visibleItems(items, query) {
    if (!query) {
        return new Set(items.map(item => item.key));
    }
    const byKey = new Map(items.map(item => [item.key, item]));
    const visible = new Set();
    const addDescendants = key => {
        for (const item of items) {
            if (item.parentKey === key && !visible.has(item.key)) {
                visible.add(item.key);
                if (item.type === 'group') {
                    addDescendants(item.key);
                }
            }
        }
    };

    const matches = items.filter(item => item.searchText.includes(query));
    for (const item of matches) {
        visible.add(item.key);
    }
    for (const item of matches) {
        if (item.type === 'group') {
            addDescendants(item.key);
        }
    }

    const addAncestors = key => {
        let current = byKey.get(key)?.parentKey ? byKey.get(byKey.get(key).parentKey) : null;
        while (current) {
            if (visible.has(current.key)) {
                current = current.parentKey ? byKey.get(current.parentKey) : null;
                continue;
            }
            visible.add(current.key);
            current = current.parentKey ? byKey.get(current.parentKey) : null;
        }

    };
    for (const item of [...visible]) {
        addAncestors(item);
    }

    return visible;
}

function buildMermaid(data) {
    const query = (filterEl?.value || '').trim().toLowerCase();
    const {items, edges} = collectSnapshotModel(data);
    const visible = visibleItems(items, query);
    const byKey = new Map(items.map(item => [item.key, item]));
    const groupKeys = new Set(items.filter(item => item.type === 'group').map(item => item.key));
    const childrenByParent = new Map();
    for (const item of items) {
        if (!item.parentKey) {
            continue;
        }
        if (!childrenByParent.has(item.parentKey)) {
            childrenByParent.set(item.parentKey, []);
        }
        childrenByParent.get(item.parentKey).push(item);
    }
    for (const children of childrenByParent.values()) {
        children.sort((a, b) => a.type.localeCompare(b.type) || a.label.localeCompare(b.label));
    }

    const indent = '    ';
    const lines = ['flowchart LR'];
    const emitted = new Set();
    let edgeSeq = 0;

    function emitLine(line, depth = 1) {
        lines.push(`${indent.repeat(depth)}${line}`);
    }

    function isVisibleGroup(key) {
        return visible.has(key) && groupKeys.has(key) && byKey.get(key)?.type === 'group';
    }

    function isValidParentGroup(item) {
        if (!item.parentKey) {
            return true;
        }
        const parent = byKey.get(item.parentKey);
        return Boolean(parent && parent.type === 'group' && visible.has(parent.key) && groupKeys.has(parent.key));
    }

    function emitNode(item, depth) {
        emitLine(`${item.mermaid}["${esc(item.label)}"]`, depth);
    }

    function emitService(key, depth) {
        const item = byKey.get(key);
        if (!item || item.type !== 'service' || !visible.has(key) || emitted.has(key) || !isValidParentGroup(item)) {
            return;
        }
        emitted.add(key);
        emitNode(item, depth);
    }

    function emitGroup(key, depth) {
        const item = byKey.get(key);
        if (!item || item.type !== 'group' || !isVisibleGroup(key) || emitted.has(key)) {
            return;
        }
        emitted.add(key);
        emitLine(`subgraph ${item.mermaid}["${esc(item.label)}"]`, depth);
        emitLine('direction TD', depth + 1);
        const children = childrenByParent.get(key) || [];
        for (const child of children) {
            if (child.type === 'group') {
                emitGroup(child.key, depth + 1);
            }
        }
        for (const child of children) {
            if (child.type === 'service') {
                emitService(child.key, depth + 1);
            }
        }
        emitLine('end', depth);
    }

    const roots = items
        .filter(item => !item.parentKey && item.type === 'group')
        .sort((a, b) => a.label.localeCompare(b.label));
    for (const root of roots) {
        emitGroup(root.key, 1);
    }

    for (const item of items) {
        if (item.type === 'group' && visible.has(item.key) && !emitted.has(item.key) && isValidParentGroup(item)) {
            emitGroup(item.key, 1);
        }
    }

    for (const item of items) {
        if (item.type === 'service' && visible.has(item.key) && !emitted.has(item.key)) {
            emitService(item.key, 1);
        }
    }

    function addEdge(fromKey, toKey, kind, label) {
        if (!visible.has(fromKey) || !visible.has(toKey)) {
            return;
        }
        const from = byKey.get(fromKey);
        const to = byKey.get(toKey);
        if (!from || !to) {
            return;
        }
        const edgeId = `e${edgeSeq += 1}`;
        const animate = kind === 'bound-to' || kind === 'backed-by';
        const operator = animate ? '==>' : kind === 'provisioned-by' ? '-.->' : '-->';
        void label;
        emitLine(`${from.mermaid} ${edgeId}@${operator} ${to.mermaid}`);
        if (animate) {
            emitLine(`${edgeId}@{ animate: true }`);
        }
    }

    const visibleEdges = edges
        .filter(edge => visible.has(edge.from) && visible.has(edge.to))
        .sort((a, b) => `${a.from}|${a.to}|${a.kind}|${a.label}`.localeCompare(`${b.from}|${b.to}|${b.kind}|${b.label}`));
    const seen = new Set();
    for (const edge of visibleEdges) {
        const key = `${edge.from}|${edge.to}|${edge.kind}|${edge.label}`;
        if (seen.has(key)) {
            continue;
        }
        seen.add(key);
        addEdge(edge.from, edge.to, edge.kind, edge.label);
    }


    return lines.join('\n');
}

function summarizeSnapshot(data) {
    const {stats} = collectSnapshotModel(data);
    return stats;
}

function updateSidebar(data) {
    const stats = summarizeSnapshot(data);
    const cards = [
        ['Regions', stats.regions],
        ['Zones', stats.zones],
        ['VMs', stats.vms],
        ['Nodes', stats.nodes],
        ['Pods', stats.pods],
        ['Namespaces', stats.namespaces],
        ['Privileged', stats.privilegedNamespaces],
        ['StorageClasses', stats.storageClasses],
        ['Default SCs', stats.defaultStorageClasses],
        ['PVCs', stats.pvcs],
        ['Bound PVCs', stats.boundPvcs],
        ['PVs', stats.pvs],
        ['Bound PVs', stats.boundPvs],
        ['Disks', stats.disks],
        ['Attached', stats.attachedDisks]
    ];
    if (summaryEl) {
        summaryEl.innerHTML = cards.map(([labelText, value]) => `
            <article class="summary-card">
                <span class="summary-card__label">${labelText}</span>
                <strong class="summary-card__value">${value}</strong>
            </article>
        `).join('');
    }
    if (generatedAtEl) {
        generatedAtEl.textContent = formatGeneratedAt(data?.generatedAt);
    }
    if (detailsEl) {
        detailsEl.textContent = JSON.stringify(data, null, 2);
    }
    baseStatusText = `${stats.regions} region${stats.regions === 1 ? '' : 's'} · ${stats.zones} zone${stats.zones === 1 ? '' : 's'} · ${stats.vms} VM${stats.vms === 1 ? '' : 's'} · ${stats.pods} pod${stats.pods === 1 ? '' : 's'}`;
    if (statusEl) {
        statusEl.textContent = baseStatusText;
    }
}

async function render() {
    if (!diagramEl) {
        return;
    }
    const token = ++renderToken;
    mermaidSource = buildMermaid(snapshot);
    mermaidSourceFallback = buildMermaid(snapshot);
    updateSidebar(snapshot);

    try {
        const {svg} = await mermaid.render(`topology_${token}`, mermaidSource);
        if (token === renderToken) {
            diagramEl.innerHTML = svg;
        }
    } catch (err) {
        try {
            const {svg} = await mermaid.render(`topology_fallback_${token}`, mermaidSourceFallback);
            if (token === renderToken) {
                diagramEl.innerHTML = svg;
            }
        } catch {
            if (token === renderToken) {
                diagramEl.innerHTML = '<p class="diagram-error">Unable to render the topology diagram.</p>';
                if (statusEl) {
                    statusEl.textContent = 'Diagram render failed';
                }
            }
            void err;
        }
    }
}

function setData(data) {
    snapshot = data || emptySnapshot;
    render();
}

function setTemporaryStatus(message) {
    if (!statusEl) {
        return;
    }
    statusEl.textContent = message;
    if (statusResetTimer) {
        clearTimeout(statusResetTimer);
    }
    statusResetTimer = setTimeout(() => {
        statusEl.textContent = baseStatusText;
    }, 1500);
}

async function copyMermaid(source, successText, failureText) {
    try {
        await navigator.clipboard.writeText(source);
        setTemporaryStatus(successText);
    } catch {
        setTemporaryStatus(failureText);
    }
}

if (copyBtn) {
    copyBtn.onclick = () => copyMermaid(mermaidSource, 'Copied Mermaid diagram', 'Copy failed');
}

if (copyFallbackBtn) {
    copyFallbackBtn.onclick = () => copyMermaid(mermaidSourceFallback, 'Copied fallback Mermaid diagram', 'Copy fallback failed');
}

if (filterEl) {
    filterEl.oninput = () => render();
}

if (diagramEl) {
    diagramEl.addEventListener('click', () => {
        if (detailsEl) {
            detailsEl.textContent = JSON.stringify(snapshot, null, 2);
        }
    });
}

fetch('/api/topology')
    .then(r => r.json())
    .then(setData)
    .catch(() => {
        snapshot = emptySnapshot;
        updateSidebar(snapshot);
        if (diagramEl) {
            diagramEl.innerHTML = '<p class="diagram-error">Failed to load the topology snapshot.</p>';
        }
        if (statusEl) {
            statusEl.textContent = 'Snapshot load failed';
        }
    });

const es = new EventSource('/api/stream');
es.onmessage = (event) => setData(JSON.parse(event.data));
es.onerror = () => {
    if (statusEl) {
        statusEl.textContent = 'Stream disconnected';
    }
};

window.getMermaidSource = () => mermaidSource;
