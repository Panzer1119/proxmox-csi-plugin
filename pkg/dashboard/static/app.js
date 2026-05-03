const canvas=document.getElementById('graph'),ctx=canvas.getContext('2d'),details=document.getElementById('details'),filter=document.getElementById('filter');
let state={nodes:[],edges:[]},selected=null,pan={x:20,y:20,scale:1},layoutMap={};
function resize(){canvas.width=canvas.clientWidth;canvas.height=canvas.clientHeight;draw()}window.addEventListener('resize',resize);resize();
function children(id){return state.nodes.filter(n=>n.parentId===id)}
function roots(){return state.nodes.filter(n=>!n.parentId)}
function doLayout(){layoutMap={};let y=20;for(const r of roots().filter(n=>n.kind==='region')){layoutNode(r,20,y,440,280);y+=300;}let ky=20;for(const r of roots().filter(n=>n.group==='kubernetes')){layoutNode(r,500,ky,420,120);ky+=140;}}
function layoutNode(n,x,y,w,h){layoutMap[n.id]={x,y,w,h};const kids=children(n.id);if(!kids.length)return;const cols=Math.max(1,Math.floor(w/140));kids.forEach((k,i)=>{const cx=x+10+(i%cols)*130, cy=y+40+Math.floor(i/cols)*90;layoutNode(k,cx,cy,120,70);});}
function drawShape(n,b){ctx.strokeStyle='#94a3b8';ctx.fillStyle=n.kind.includes('disk')?'#f59e0b':n.kind==='pod'?'#38bdf8':'#1e293b';if(n.shape==='hex'){hex(b.x+60,b.y+35,28);ctx.fill();ctx.stroke();}
else{ctx.fillRect(b.x,b.y,b.w,b.h);ctx.strokeRect(b.x,b.y,b.w,b.h);}ctx.fillStyle='#fff';ctx.font='11px sans-serif';ctx.fillText(n.name,b.x+6,b.y+16);}
function hex(x,y,r){ctx.beginPath();for(let i=0;i<6;i++){const a=Math.PI/3*i;const px=x+r*Math.cos(a),py=y+r*Math.sin(a);if(i===0)ctx.moveTo(px,py);else ctx.lineTo(px,py)}ctx.closePath()}
function draw(){ctx.setTransform(pan.scale,0,0,pan.scale,pan.x,pan.y);ctx.clearRect(-pan.x,-pan.y,canvas.width/pan.scale,canvas.height/pan.scale);const q=filter.value.toLowerCase();
for(const e of state.edges){const a=layoutMap[e.from],b=layoutMap[e.to];if(!a||!b)continue;ctx.strokeStyle='#475569';ctx.beginPath();ctx.moveTo(a.x+a.w,a.y+a.h/2);ctx.lineTo(b.x,b.y+b.h/2);ctx.stroke()}
for(const n of state.nodes){if(q && !(`${n.name} ${n.kind}`.toLowerCase().includes(q)))continue;const b=layoutMap[n.id];if(!b)continue;drawShape(n,b);if(selected?.id===n.id){ctx.strokeStyle='#fff';ctx.strokeRect(b.x-2,b.y-2,b.w+4,b.h+4)}}}
canvas.onclick=(e)=>{const x=(e.offsetX-pan.x)/pan.scale,y=(e.offsetY-pan.y)/pan.scale;selected=state.nodes.find(n=>{const b=layoutMap[n.id];return b&&x>=b.x&&x<=b.x+b.w&&y>=b.y&&y<=b.y+b.h});details.textContent=selected?JSON.stringify(selected,null,2):'No node selected';draw()};
canvas.onwheel=(e)=>{e.preventDefault();pan.scale=Math.max(.5,Math.min(2.2,pan.scale+(e.deltaY>0?-0.1:0.1)));draw()};
let drag=false,last={x:0,y:0};canvas.onmousedown=(e)=>{drag=true;last={x:e.clientX,y:e.clientY}};window.onmouseup=()=>drag=false;window.onmousemove=(e)=>{if(!drag)return;pan.x+=e.clientX-last.x;pan.y+=e.clientY-last.y;last={x:e.clientX,y:e.clientY};draw()};filter.oninput=draw;
function ingest(d){state=d;doLayout();draw()}
fetch('/dashboard/api/topology').then(r=>r.json()).then(ingest);const es=new EventSource('/dashboard/api/stream');es.onmessage=(e)=>ingest(JSON.parse(e.data));
