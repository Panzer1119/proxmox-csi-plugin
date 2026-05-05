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
    return String(value).replaceAll('"', '#quot;');
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

    const edgeSet = new Set();
    const addEdge = (from, to, kind, label, animate) => {
        if (!from || !to) {
            return;
        }
        const dedupe = `${from}|${to}|${kind}|${label}|${animate}`;
        if (edgeSet.has(dedupe)) {
            return;
        }
        edgeSet.add(dedupe);
        edges.push({from, to, kind, label, animate});
    };

    const addGroup = item => createItem(items, index, {...item, type: 'group'});
    const addService = item => createItem(items, index, {...item, type: 'service'});

    const rootKey = 'root|kubernetes';
    const namespacesRootKey = 'root|kubernetes|namespaces';
    const storageClassesRootKey = 'root|kubernetes|storageclasses';
    const regionsRootKey = 'root|kubernetes|regions';

    addGroup({
        key: rootKey,
        prefix: 'root',
        kind: 'kubernetes-root',
        label: 'Kubernetes',
        badgeText: '',
        searchText: 'kubernetes'
    });
    addGroup({
        key: namespacesRootKey,
        prefix: 'namespaces',
        kind: 'namespaces',
        label: 'Namespaces',
        badgeText: '',
        parentKey: rootKey,
        searchText: 'namespaces'
    });
    addGroup({
        key: storageClassesRootKey,
        prefix: 'storageclasses',
        kind: 'storageclasses',
        label: 'StorageClasses',
        badgeText: '',
        parentKey: rootKey,
        searchText: 'storageclasses'
    });
    addGroup({
        key: regionsRootKey,
        prefix: 'regions',
        kind: 'regions',
        label: 'Regions',
        badgeText: '',
        parentKey: rootKey,
        searchText: 'regions'
    });

    const k8s = data?.kubernetes || {};
    const namespaces = [...(k8s.namespaces || [])].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    const storageClasses = [...(k8s.storageClasses || [])].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    const pvcs = [...(k8s.persistentVolumeClaims || [])].sort((a, b) => (a.namespace || '').localeCompare(b.namespace || '') || (a.name || '').localeCompare(b.name || ''));
    const pvs = [...(k8s.persistentVolumes || [])].sort((a, b) => (a.name || '').localeCompare(b.name || ''));

    const namespaceKeyByName = new Map();
    const storageClassKeyByName = new Map();
    const pvcKeyByNsName = new Map();
    const pvByName = new Map();
    const pvKeyByName = new Map();
    const podKeyByPVName = new Map();
    const diskKeyByExact = new Map();
    const orphanPvGroupByZone = new Map();

    for (const ns of namespaces) {
        stats.namespaces += 1;
        if (ns.isPrivileged) {
            stats.privilegedNamespaces += 1;
        }
        const key = `namespace|${ns.name}`;
        namespaceKeyByName.set(ns.name, key);
        addService({
            key,
            prefix: 'namespace',
            kind: 'namespace',
            label: formatNamespaceName(ns),
            badgeText: ns.isPrivileged ? 'privileged' : '',
            parentKey: namespacesRootKey,
            searchText: [ns.name, ns.isPrivileged ? 'privileged' : '', ns.kind || ''].filter(Boolean).join(' ')
        });
    }

    for (const sc of storageClasses) {
        stats.storageClasses += 1;
        if (sc.isDefault) {
            stats.defaultStorageClasses += 1;
        }
        const scKey = `storageclass|${sc.name}`;
        storageClassKeyByName.set(sc.name, scKey);
        addGroup({
            key: scKey,
            prefix: 'storageclass',
            kind: 'storageclass',
            label: formatStorageClassName(sc),
            badgeText: sc.isDefault ? 'default' : '',
            parentKey: storageClassesRootKey,
            searchText: [sc.name, sc.provisioner, sc.isDefault ? 'default' : ''].filter(Boolean).join(' ')
        });
    }

    const unclassifiedStorageClassKey = 'storageclass|_unclassified';
    let hasUnclassifiedPVC = false;
    for (const pvc of pvcs) {
        stats.pvcs += 1;
        if (pvc.bound) {
            stats.boundPvcs += 1;
        }
        const parentSC = storageClassKeyByName.get(pvc.storageClassName) || unclassifiedStorageClassKey;
        if (parentSC === unclassifiedStorageClassKey) {
            hasUnclassifiedPVC = true;
        }
        const key = `pvc|${pvc.namespace}|${pvc.name}`;
        pvcKeyByNsName.set(`${pvc.namespace}/${pvc.name}`, key);
        addService({
            key,
            prefix: 'pvc',
            kind: 'pvc',
            label: formatPvcName(pvc),
            badgeText: pvc.bound ? 'bound' : 'pending',
            parentKey: parentSC,
            searchText: [pvc.namespace, pvc.name, pvc.storageClassName, pvc.bound ? 'bound' : 'pending', pvc.volumeName || ''].filter(Boolean).join(' ')
        });
    }
    if (hasUnclassifiedPVC) {
        addGroup({
            key: unclassifiedStorageClassKey,
            prefix: 'storageclass',
            kind: 'storageclass',
            label: 'StorageClass: <none>',
            badgeText: '',
            parentKey: storageClassesRootKey,
            searchText: 'storageclass none'
        });
    }

    for (const pv of pvs) {
        pvByName.set(pv.name, pv);
        stats.pvs += 1;
        if (pv.bound) {
            stats.boundPvs += 1;
        }
    }

    const ensurePVNode = (pvName, parentKey) => {
        if (pvKeyByName.has(pvName)) {
            return pvKeyByName.get(pvName);
        }
        const pv = pvByName.get(pvName);
        if (!pv) {
            return null;
        }
        const key = `pv|${pvName}`;
        pvKeyByName.set(pvName, key);
        addService({
            key,
            prefix: 'pv',
            kind: 'pv',
            label: formatPvName(pv),
            badgeText: pv.bound ? 'bound' : 'unbound',
            parentKey,
            searchText: [pv.name, pv.bound ? 'bound' : 'unbound', pv.storageClassName || '', pv.volumeHandle || ''].filter(Boolean).join(' ')
        });
        return key;
    };

    const regions = Object.entries(data?.regions || {}).sort(([a], [b]) => a.localeCompare(b));
    const nodeGroupKeys = new Set();
    for (const [regionName, regionValue] of regions) {
        stats.regions += 1;
        const regionKey = `region|${regionName}`;
        const sharedDisksKey = `region|${regionName}|shared-disks`;

        addGroup({
            key: regionKey,
            prefix: 'region',
            kind: 'region',
            label: regionValue?.name || regionName,
            badgeText: '',
            parentKey: regionsRootKey,
            searchText: [regionName, regionValue?.name || ''].filter(Boolean).join(' ')
        });
        addGroup({
            key: sharedDisksKey,
            prefix: 'shared-disks',
            kind: 'shared-disks',
            label: 'SharedDisks',
            badgeText: '',
            parentKey: regionKey,
            searchText: 'shared disks'
        });

        for (const disk of regionValue?.disks || []) {
            stats.disks += 1;
            if (disk.attachedVMIds?.length) {
                stats.attachedDisks += 1;
            }
            const diskKey = `disk|shared|${regionName}|${disk.storageId}|${disk.name}`;
            diskKeyByExact.set(`${regionName}||${disk.storageId}|${disk.name}`, diskKey);
            addService({
                key: diskKey,
                prefix: 'disk-shared',
                kind: 'disk',
                label: formatDiskName(disk),
                badgeText: 'shared',
                parentKey: sharedDisksKey,
                searchText: [regionName, disk.storageId, disk.name].filter(Boolean).join(' ')
            });
        }

        const zones = Object.entries(regionValue?.zones || {}).sort(([a], [b]) => a.localeCompare(b));
        for (const [zoneName, zoneValue] of zones) {
            stats.zones += 1;
            const zoneKey = `zone|${regionName}|${zoneName}`;
            const localDisksKey = `zone|${regionName}|${zoneName}|local-disks`;
            const nodesRootKey = `zone|${regionName}|${zoneName}|nodes`;

            addGroup({
                key: zoneKey,
                prefix: 'zone',
                kind: 'zone',
                label: zoneValue?.name || zoneName,
                badgeText: '',
                parentKey: regionKey,
                searchText: [regionName, zoneName, zoneValue?.name || ''].filter(Boolean).join(' ')
            });
            addGroup({
                key: localDisksKey,
                prefix: 'local-disks',
                kind: 'local-disks',
                label: 'LocalDisks',
                badgeText: '',
                parentKey: zoneKey,
                searchText: 'local disks'
            });
            addGroup({
                key: nodesRootKey,
                prefix: 'nodes',
                kind: 'nodes',
                label: 'Nodes',
                badgeText: '',
                parentKey: zoneKey,
                searchText: 'nodes'
            });

            for (const disk of zoneValue?.disks || []) {
                stats.disks += 1;
                if (disk.attachedVMIds?.length) {
                    stats.attachedDisks += 1;
                }
                const diskKey = `disk|local|${regionName}|${zoneName}|${disk.storageId}|${disk.name}`;
                diskKeyByExact.set(`${regionName}|${zoneName}|${disk.storageId}|${disk.name}`, diskKey);
                addService({
                    key: diskKey,
                    prefix: 'disk-local',
                    kind: 'disk',
                    label: formatDiskName(disk),
                    badgeText: 'local',
                    parentKey: localDisksKey,
                    searchText: [regionName, zoneName, disk.storageId, disk.name].filter(Boolean).join(' ')
                });
            }

            const vms = [...(zoneValue?.vms || [])].sort((a, b) => String(a.id || '').localeCompare(String(b.id || '')));
            for (const vm of vms) {
                stats.vms += 1;
                const node = vm.node || {};
                if (!node.kind) {
                    continue;
                }
                const nodeName = node.name || vm.name || String(vm.id || 'node');
                const nodeGroupKey = `node|${regionName}|${zoneName}|${node.uid || nodeName}`;
                if (!nodeGroupKeys.has(nodeGroupKey)) {
                    nodeGroupKeys.add(nodeGroupKey);
                    stats.nodes += 1;
                    addGroup({
                        key: nodeGroupKey,
                        prefix: 'node',
                        kind: 'node',
                        label: formatNodeName(node),
                        badgeText: '',
                        parentKey: nodesRootKey,
                        searchText: [nodeName, node.uid || '', regionName, zoneName].filter(Boolean).join(' ')
                    });
                }

                const pods = [...(node.pods || [])].sort((a, b) => `${a.namespace || ''}/${a.name || ''}`.localeCompare(`${b.namespace || ''}/${b.name || ''}`));
                for (const pod of pods) {
                    stats.pods += 1;
                    const podKey = `pod|${regionName}|${zoneName}|${node.uid || nodeName}|${pod.uid || `${pod.namespace}/${pod.name}`}`;
                    addGroup({
                        key: podKey,
                        prefix: 'pod',
                        kind: 'pod',
                        label: formatPodName(pod),
                        badgeText: '',
                        parentKey: nodeGroupKey,
                        searchText: [pod.namespace, pod.name, pod.uid || '', pod.hostname || ''].filter(Boolean).join(' ')
                    });

                    const volumeEntries = Object.entries(pod.volumes || {}).sort(([a], [b]) => a.localeCompare(b));
                    for (const [, pvName] of volumeEntries) {
                        if (!pvName) {
                            continue;
                        }
                        const pvKey = ensurePVNode(pvName, podKey);
                        if (!pvKey) {
                            continue;
                        }
                        if (!podKeyByPVName.has(pvName)) {
                            podKeyByPVName.set(pvName, new Set());
                        }
                        podKeyByPVName.get(pvName).add(podKey);
                    }
                }
            }
        }
    }

    for (const pv of pvs) {
        if (pvKeyByName.has(pv.name)) {
            continue;
        }
        const ref = pv.volumeReference;
        let parentKey = regionsRootKey;
        if (ref?.region && ref?.zone) {
            const zoneOrphanKey = `zone|${ref.region}|${ref.zone}|orphan-pvs`;
            if (!orphanPvGroupByZone.has(zoneOrphanKey)) {
                orphanPvGroupByZone.set(zoneOrphanKey, true);
                addGroup({
                    key: zoneOrphanKey,
                    prefix: 'orphan-pvs',
                    kind: 'pvs',
                    label: 'PVs',
                    badgeText: '',
                    parentKey: `zone|${ref.region}|${ref.zone}`,
                    searchText: 'pvs'
                });
            }
            parentKey = zoneOrphanKey;
        }
        ensurePVNode(pv.name, parentKey);
    }

    for (const pvc of pvcs) {
        const pvcKey = pvcKeyByNsName.get(`${pvc.namespace}/${pvc.name}`);
        if (!pvcKey) {
            continue;
        }
        const nsKey = namespaceKeyByName.get(pvc.namespace);
        if (nsKey) {
            addEdge(nsKey, pvcKey, 'namespace-pvc', 'contains', false);
        }
        if (pvc.volumeName) {
            const pvKey = pvKeyByName.get(pvc.volumeName);
            if (pvKey) {
                addEdge(pvcKey, pvKey, 'pvc-pv', 'binds', Boolean(pvc.bound));
            }
        }
    }

    for (const [pvName, podKeys] of podKeyByPVName.entries()) {
        const pvKey = pvKeyByName.get(pvName);
        const pv = pvByName.get(pvName);
        if (!pvKey || !pv) {
            continue;
        }
        for (const podKey of podKeys) {
            addEdge(pvKey, podKey, 'pv-pod', 'mounted', Boolean(pv.bound));
        }
    }

    for (const pv of pvs) {
        const pvKey = pvKeyByName.get(pv.name);
        if (!pvKey || !pv.volumeReference) {
            continue;
        }
        const ref = pv.volumeReference;
        const diskKey = diskKeyByExact.get(`${ref.region}|${ref.zone || ''}|${ref.storage}|${ref.disk}`) || diskKeyByExact.get(`${ref.region}||${ref.storage}|${ref.disk}`);
        if (diskKey) {
            addEdge(diskKey, pvKey, 'disk-pv', 'backs', true);
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
    const nodeClassRefs = [];
    let edgeSeq = 0;

    function emitLine(line, depth = 1) {
        lines.push(`${indent.repeat(depth)}${line}`);
    }

    function classForKind(kind) {
        if (kind === 'namespace') return 'cNamespace';
        if (kind === 'storageclass') return 'cStorageClass';
        if (kind === 'pvc') return 'cPVC';
        if (kind === 'pv') return 'cPV';
        if (kind === 'pod') return 'cPod';
        if (kind === 'node') return 'cNode';
        if (kind === 'region') return 'cRegion';
        if (kind === 'zone') return 'cZone';
        if (kind === 'disk') return 'cDisk';
        return 'cDefault';
    }

    function nodeShape(item) {
        if (item.kind === 'pvc') {
            return `{{${esc(item.label)}}}`;
        }
        if (item.kind === 'pv') {
            return `[(${esc(item.label)})]`;
        }
        if (item.kind === 'disk') {
            return `[/${esc(item.label)}/]`;
        }
        return `["${esc(item.label)}"]`;
    }

    function emitServiceNode(item, depth) {
        emitLine(`${item.mermaid}${nodeShape(item)}`, depth);
        nodeClassRefs.push([item.mermaid, classForKind(item.kind)]);
    }

    function emitGroup(key, depth) {
        const item = byKey.get(key);
        if (!item || item.type !== 'group' || !visible.has(key) || emitted.has(key)) {
            return;
        }
        emitted.add(key);

        emitLine(`subgraph ${item.mermaid}["${esc(item.label)}"]`, depth);
        emitLine(depth === 1 ? 'direction LR' : 'direction TD', depth + 1);

        const children = childrenByParent.get(key) || [];
        for (const child of children) {
            if (child.type === 'group') {
                emitGroup(child.key, depth + 1);
            }
        }
        for (const child of children) {
            if (child.type === 'service' && visible.has(child.key) && !emitted.has(child.key)) {
                emitted.add(child.key);
                emitServiceNode(child, depth + 1);
            }
        }

        emitLine('end', depth);
    }

    emitGroup('root|kubernetes', 1);

    for (const item of items) {
        if (!visible.has(item.key) || emitted.has(item.key)) {
            continue;
        }
        if (item.type === 'group') {
            emitGroup(item.key, 1);
        } else {
            emitted.add(item.key);
            emitServiceNode(item, 1);
        }
    }

    emitLine('classDef cDefault fill:#f8fafc,stroke:#334155,stroke-width:1px;');
    emitLine('classDef cNamespace fill:#dbeafe,stroke:#1d4ed8,stroke-width:1.5px;');
    emitLine('classDef cStorageClass fill:#fef3c7,stroke:#b45309,stroke-width:1.5px;');
    emitLine('classDef cPVC fill:#e0f2fe,stroke:#0369a1,stroke-width:1.5px;');
    emitLine('classDef cPV fill:#e2e8f0,stroke:#475569,stroke-width:1.5px;');
    emitLine('classDef cPod fill:#dcfce7,stroke:#15803d,stroke-width:1.5px;');
    emitLine('classDef cNode fill:#ede9fe,stroke:#6d28d9,stroke-width:1.5px;');
    emitLine('classDef cRegion fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px;');
    emitLine('classDef cZone fill:#fff7ed,stroke:#c2410c,stroke-width:1.5px;');
    emitLine('classDef cDisk fill:#ecfccb,stroke:#4d7c0f,stroke-width:1.5px;');

    for (const [nodeId, cls] of nodeClassRefs) {
        emitLine(`class ${nodeId} ${cls}`);
    }

    const visibleEdges = edges
        .filter(edge => visible.has(edge.from) && visible.has(edge.to))
        .sort((a, b) => `${a.from}|${a.to}|${a.kind}|${a.label}`.localeCompare(`${b.from}|${b.to}|${b.kind}|${b.label}`));

    for (const edge of visibleEdges) {
        const from = byKey.get(edge.from);
        const to = byKey.get(edge.to);
        if (!from || !to) {
            continue;
        }
        const edgeId = `e${edgeSeq += 1}`;
        const operator = edge.animate ? '==>' : '-->';
        const label = edge.label ? `|${esc(edge.label)}|` : '';
        emitLine(`${from.mermaid} ${edgeId}@${operator}${label} ${to.mermaid}`);
        if (edge.animate) {
            emitLine(`${edgeId}@{ animate: true }`);
        }
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
