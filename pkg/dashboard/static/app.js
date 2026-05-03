import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';

const diagramEl=document.getElementById('diagram');
const details=document.getElementById('details');
const filterEl=document.getElementById('filter');
const copyBtn=document.getElementById('copy');
const statusEl=document.getElementById('status');
let snapshot={nodes:[],edges:[]},mermaidSource='';

async function registerIcons(){
  try {
    mermaid.registerIconPacks([{name:'mdi',loader:()=>fetch('https://cdn.jsdelivr.net/npm/@iconify-json/mdi/icons.json').then(r=>r.json())}]);
    return true;
  } catch { return false; }
}

function icon(kind,loaded){
  if(!loaded) return 'cloud';
  const map={region:'mdi:cloud-outline',zone:'mdi:server-outline',qemu:'mdi:monitor',lxc:'mdi:docker',pod:'mdi:cube-outline',pvc:'mdi:database-outline','shared-disk':'mdi:nas','local-disk':'mdi:harddisk','storageclass':'mdi:layers-outline',pv:'mdi:harddisk','vm-workload':'mdi:monitor-dashboard','k8s-node':'mdi:kubernetes'};
  return map[kind]||'mdi:shape-outline';
}
const fallbackIcon={region:'cloud',zone:'server',qemu:'server',lxc:'server',pod:'disk',pvc:'database','shared-disk':'disk','local-disk':'disk','storageclass':'database',pv:'disk','vm-workload':'server','k8s-node':'server'};
function sanitize(id){return id.replace(/[^a-zA-Z0-9_]/g,'_');}
function esc(t){return String(t).replaceAll('"','\\"');}

function buildMermaid(data, iconLoaded){
  const q=filterEl.value.toLowerCase();
  const nodes=[...data.nodes].filter(n=>!q||(`${n.name} ${n.kind}`.toLowerCase().includes(q))).sort((a,b)=>a.id.localeCompare(b.id));
  const ids=new Set(nodes.map(n=>n.id));
  const declared=new Set();
  const lines=['architecture-beta'];

  for(const n of nodes){
    const id=sanitize(n.id); const parent=n.parentId&&ids.has(n.parentId)?sanitize(n.parentId):'';
    const ic=iconLoaded?icon(n.kind,true):(fallbackIcon[n.kind]||'cloud');
    if(['region','zone','qemu','lxc','vm-workload','k8s-node','pod'].includes(n.kind)){
      lines.push(`group ${id}(${ic})["${esc(n.name)}"]${parent?` in ${parent}`:''}`);
    } else {
      lines.push(`service ${id}(${ic})["${esc(n.name)}"]${parent?` in ${parent}`:''}`);
    }
    declared.add(n.id);
  }

  const pvcToPV=new Map(), pvToDisk=new Map();
  for(const e of data.edges){ if(e.kind==='binds') pvcToPV.set(e.from,e.to); if(e.kind==='backs') pvToDisk.set(e.from,e.to); }
  const pairs=[];
  for(const [pvc,pv] of pvcToPV.entries()){ const disk=pvToDisk.get(pv); if(disk) pairs.push([pvc,disk]); }
  pairs.sort((a,b)=>(a[0]+a[1]).localeCompare(b[0]+b[1]));
  const seen=new Set();
  for(const [a,b] of pairs){ if(!declared.has(a)||!declared.has(b)) continue; const k=a+'|'+b; if(seen.has(k)) continue; seen.add(k); lines.push(`${sanitize(a)}:R -- L:${sanitize(b)}`); }
  return lines.join('\n');
}

async function render(){
  const iconLoaded = await registerIcons();
  mermaid.initialize({startOnLoad:false,securityLevel:'loose'});
  mermaidSource=buildMermaid(snapshot, iconLoaded);
  try { const {svg}=await mermaid.render('topology',mermaidSource); diagramEl.innerHTML=svg; }
  catch { mermaidSource=buildMermaid(snapshot, false); const {svg}=await mermaid.render('topology_fallback',mermaidSource); diagramEl.innerHTML=svg; }
}

function setData(data){snapshot=data;render();}
copyBtn.onclick=async()=>{try{await navigator.clipboard.writeText(mermaidSource);statusEl.textContent='Copied!';}catch{statusEl.textContent='Copy failed';}setTimeout(()=>statusEl.textContent='',1500)};
filterEl.oninput=()=>render();
fetch('/dashboard/api/topology').then(r=>r.json()).then(setData);
const es=new EventSource('/dashboard/api/stream');es.onmessage=(e)=>setData(JSON.parse(e.data));
diagramEl.addEventListener('click',()=>{details.textContent=JSON.stringify(snapshot,null,2)});
window.getMermaidSource=()=>mermaidSource;
