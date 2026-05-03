const canvas=document.getElementById('graph'),ctx=canvas.getContext('2d'),details=document.getElementById('details'),filter=document.getElementById('filter');
let state={nodes:[],edges:[]},selected=null,pan={x:40,y:40,scale:1};
function resize(){canvas.width=canvas.clientWidth;canvas.height=canvas.clientHeight;draw()}window.addEventListener('resize',resize);resize();
function layout(){const groups={};state.nodes.forEach(n=>(groups[n.group]??=[]).push(n));let y=30;for(const g of Object.keys(groups)){let x=30;for(const n of groups[g]){n.x=x;n.y=y;n.r=14;x+=120;}y+=100;}}
function color(k){if(k.startsWith('k8s'))return '#38bdf8';if(k.includes('vm')||k==='container')return '#22c55e';if(k.includes('storage')||k==='pv'||k==='pvc')return '#f59e0b';return '#cbd5e1'}
function draw(){ctx.setTransform(pan.scale,0,0,pan.scale,pan.x,pan.y);ctx.clearRect(-pan.x,-pan.y,canvas.width/pan.scale,canvas.height/pan.scale);const q=filter.value.toLowerCase();for(const e of state.edges){const a=state.nodes.find(n=>n.id===e.from),b=state.nodes.find(n=>n.id===e.to);if(!a||!b)continue;ctx.strokeStyle='#334155';ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke();}
for(const n of state.nodes){if(q&&!(n.name.toLowerCase().includes(q)||n.kind.includes(q)))continue;ctx.fillStyle=color(n.kind);ctx.beginPath();ctx.arc(n.x,n.y,n.r,0,Math.PI*2);ctx.fill();ctx.fillStyle='#fff';ctx.font='11px sans-serif';ctx.fillText(n.name,n.x+18,n.y+4);if(selected?.id===n.id){ctx.strokeStyle='#fff';ctx.strokeRect(n.x-18,n.y-18,36,36)}}}
canvas.onclick=(e)=>{const x=(e.offsetX-pan.x)/pan.scale,y=(e.offsetY-pan.y)/pan.scale;selected=state.nodes.find(n=>Math.hypot(n.x-x,n.y-y)<n.r);details.textContent=selected?JSON.stringify(selected,null,2):'No node selected';draw()};
canvas.onwheel=(e)=>{e.preventDefault();pan.scale=Math.max(.4,Math.min(2.5,pan.scale+(e.deltaY>0?-0.1:0.1)));draw()};
let drag=false,last={x:0,y:0};canvas.onmousedown=(e)=>{drag=true;last={x:e.clientX,y:e.clientY}};window.onmouseup=()=>drag=false;window.onmousemove=(e)=>{if(!drag)return;pan.x+=e.clientX-last.x;pan.y+=e.clientY-last.y;last={x:e.clientX,y:e.clientY};draw()};filter.oninput=draw;
fetch('/dashboard/api/topology').then(r=>r.json()).then(d=>{state=d;layout();draw()});
const es=new EventSource('/dashboard/api/stream');es.onmessage=(e)=>{state=JSON.parse(e.data);layout();draw()};
