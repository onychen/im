/* Repository reader: real source lines, reading checkpoints, and editor tabs. */
window.createWorkbench=({chapters,sources,escape,syntax,copy,download})=>{

  const $=id=>document.getElementById(id);
  const playback=window.createDevelopmentPlayer({chapters,sources,escape,syntax,copy});
  const icon=(name)=>'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'+({file:'<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6M9 13l-2 2 2 2m6-4 2 2-2 2"/>',folder:'<path d="M3 7V5a2 2 0 0 1 2-2h5l2 3h7a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>',lock:'<rect x="4" y="10" width="16" height="11" rx="2"/><path d="M8 10V6a4 4 0 0 1 8 0v4"/>',unlock:'<rect x="4" y="10" width="16" height="11" rx="2"/><path d="M8 10V6a4 4 0 0 1 7.5-2"/>',close:'<path d="m6 6 12 12M6 18 18 6"/>'}[name])+'</svg>';
  // Each entry pairs a prose section with an actual symbol in its source file.
  const plans=[
    [[2,') pushAfterConfirm('],[0,'type SendMessage'],[0,'type ServerAck'],[1,'func EncodeMsg'],[0,'type MessagePersistEvent']],
    [[0,'func main'],[0,'mux.HandleFunc'],[1,'module'],[3,'func main'],[0,'ListenAndServe']],
    [[0,'func main'],[0,'type SendMessage'],[1,'func ServiceError'],[0,'router.Use'],[2,') RegisterRoutes']],
    [[0,'type AuthService'],[0,') Register('],[2,'func run'],[1,'type MySQLRepo interface'],[0,'func NewAuthService']],
    [[0,'CREATE TABLE'],[4,') CreateUser('],[3,'func NewMySQLPool'],[4,') CreateUser('],[4,'BeginTx'],[2,'INDEX'],[1,'CREATE TABLE']],
    [[0,'func main'],[1,'bcrypt.Generate'],[2,'type Claims'],[1,') Refresh('],[3,'func TestJWTAuthMiddleware_ValidToken'],[4,'func setupRouter']],
    [[0,'server:'],[3,'services:'],[2,'func run'],[2,'/health'],[1,'func LoadConfig'],[1,'func LoadConfig'],[3,'services:'],[0,'server:'],[1,'type Config']],
    [[0,') AcceptFriendRequest('],[2,') SendFriendRequest('],[0,') createPrivateConversation('],[1,') isOwnerOrAdmin('],[4,'group_members']],
    [[0,'func main'],[1,'var upgrader'],[2,') HandleMessage('],[3,') WritePump('],[4,') KickOld('],[3,') Close(']],
    [[0,'type RedisRepo interface'],[0,') WriteInbox('],[4,'func CleanupExpiredData'],[0,') ReadInbox('],[1,'type PersistStreamEntry'],[0,') TrimInbox(']],
    [[3,') handlePrivateMsg('],[0,'local'],[0,'redis.call'],[0,'msgId'],[1,'func TestAtomicPrivateMessageWrite'],[0,'Duplicate']],
    [[0,'type MessageRelay'],[0,') handle('],[0,') run('],[3,') NewWriter('],[3,') commitInterval('],[1,') ReadPersistEvents(']],
    [[0,') HandleSendMessage('],[2,') pushAfterConfirm('],[1,'type ServerAck'],[3,') handle('],[3,') retryOrDeadLetter('],[3,') handleRetry(']],
    [[0,') HandleSyncReq('],[1,') readIndexedMessages('],[1,') fillGroupUnread('],[0,') handleGroupMsg('],[1,') SetGroupReadPos(']],
    [[1,') process('],[0,') PublishMoment('],[3,'const luaMomentLike'],[0,') ensureLikesLoaded('],[2,') flush('],[4,'type MomentConfig']],
    [[0,') UploadAvatar('],[0,'maxSize'],[1,') RevokeMessage('],[3,') UpdateSettings('],[4,'type UserSettings']],
    [[0,'func Test'],[1,'func TestRelayPublishesThenAcknowledgesAndPushes'],[2,'func TestPrivateMessageInsert'],[4,'func TestE2E_PrivateMessaging'],[0,'func Test'],[3,'func TestAtomicGroupMessageWrite'],[5,'server:']],
    [[0,'#'],[1,'func main'],[2,'func CleanupExpiredData'],[4,'services:'],[3,'signal.Notify'],[5,'FROM'],[0,'#']],
    [[0,'func main'],[0,'type updateProfileRequest'],[1,'server:'],[0,'func requestTrace'],[0,') Get('],[2,'async function request'],[0,') Update('],[5,'const defaultOrigin'],[3,') UpdateSettings('],[6,'async request'],[12,'func setupRouter']]
  ];
  let active=0,selected='',tabs=[],locked=false,full=false,step=-1,anchors=[],raf=0,preview='';
  const pinned=new Set();
  let revealed=new Map(),collapsed=new Set(),hot=null,available=[];
  const scrolls=new Map();
  const units=new Map();
  for(const [name,source] of Object.entries(sources)){
    const lines=source.code.split('\n');
    // Only Go top-level declarations are folded. Other formats remain intact.
    const starts=name.endsWith('.go')?lines.flatMap((line,i)=>/^(func |type |const |var )/.test(line)?[i]:[]):[];
    units.set(name,{lines,blocks:starts.map((start,i)=>({start,end:(starts[i+1]??lines.length)-1})),preamble:starts[0]??lines.length});
  }
  function locate(name,needle){
    const data=units.get(name);let at=needle?data.lines.findIndex(line=>line.includes(needle)):-1;
    if(at<0)at=data.blocks[0]?.start??0;
    const block=data.blocks.find(b=>at>=b.start&&at<=b.end);
    return {file:name,start:block?.start??0,end:block?.end??data.lines.length-1,at};
  }
  const audits=chapters.flatMap((chapter,c)=>(plans[chapter.id-1]??[]).map(([f,needle],s)=>({chapter:c+1,section:s+1,file:chapter.files[f],needle}))).filter(p=>!sources[p.file]?.code.includes(p.needle));
  // Exposed metadata is useful for checking the tutorial's code links.
  window.WORKBENCH_AUDIT=audits;
  function focusFor(index){const entry=plans[chapters[active].id-1]?.[index]??[0,''];return locate(chapters[active].files[entry[0]],entry[1]);}
  function reveal(focus){let set=revealed.get(focus.file);if(!set){set=new Set();revealed.set(focus.file,set)}set.add(focus.start);}
  function openParents(name){const parts=name.split('/');for(let i=1;i<parts.length;i++)collapsed.delete(parts.slice(0,i).join('/'));}
  function tree(){
    const root={children:new Map()};
    for(const file of available){let node=root;const parts=file.split('/');for(let i=0;i<parts.length;i++){const path=parts.slice(0,i+1).join('/');if(!node.children.has(parts[i]))node.children.set(parts[i],{path,label:parts[i],children:new Map(),file:i===parts.length-1?file:null});node=node.children.get(parts[i]);}}
    function html(node,depth){return [...node.children.values()].map(child=>child.file?'<button class="tree-file '+(child.file===selected?'is-active':'')+'" style="--depth:'+depth+'" data-file="'+escape(child.file)+'" title="'+escape(child.file)+'" '+(child.file===selected?'aria-current="true"':'')+'>'+icon('file')+'<span>'+escape(child.label)+'</span></button>':'<details class="tree-folder" data-folder="'+escape(child.path)+'" '+(!collapsed.has(child.path)?'open':'')+'><summary style="--depth:'+depth+'" title="'+escape(child.path)+'">'+icon('folder')+'<span>'+escape(child.label)+'</span></summary>'+html(child,depth+1)+'</details>').join('');}
    $('file-tree').innerHTML=html(root,0);
  }
  function tabbar(){
    $('file-tabs').innerHTML=tabs.map((name,i)=>'<div class="editor-tab '+(name===selected?'is-active':'')+'"><button role="tab" id="source-tab-'+i+'" aria-controls="source-code" aria-selected="'+(name===selected)+'" tabindex="'+(name===selected?'0':'-1')+'" data-tab="'+escape(name)+'" title="'+escape(name)+'">'+icon('file')+'<span>'+escape(name.split('/').pop())+'</span></button><button class="tab-close" data-close="'+escape(name)+'" aria-label="关闭 '+escape(name)+'">'+icon('close')+'</button></div>').join('');
    const index=tabs.indexOf(selected);if(index>=0)$('source-code').setAttribute('aria-labelledby','source-tab-'+index);else $('source-code').removeAttribute('aria-labelledby');
    const current=$('file-tabs').querySelector('.is-active');if(current){const rail=$('file-tabs');if(current.offsetLeft<rail.scrollLeft)rail.scrollLeft=current.offsetLeft;else if(current.offsetLeft+current.offsetWidth>rail.scrollLeft+rail.clientWidth)rail.scrollLeft=current.offsetLeft+current.offsetWidth-rail.clientWidth;}
  }
  function code(navigate=false){
    const empty=!selected;$('editor-empty').hidden=!empty;$('source-code').hidden=empty;$('editor-menu').hidden=empty;
    if(empty){$('source-code').innerHTML='';return;}
    const source=sources[selected],data=units.get(selected),set=revealed.get(selected)??new Set();
    $('source-info').textContent=selected+'\n'+source.kind+' · SHA-256 '+source.sha256.slice(0,12)+'\n'+(full?'完整文件':'按阅读展开，保留原始行号');
    $('full-file').textContent=full?'返回阅读展开':'查看完整文件';$('full-file').setAttribute('aria-pressed',String(full));
    $('source-code').setAttribute('aria-label',selected+' · '+(full?'完整源码':'阅读展开'));
    const visible=i=>full||i<data.preamble||!data.blocks.length||data.blocks.some(b=>set.has(b.start)&&i>=b.start&&i<=b.end);
    let html='';for(let i=0;i<data.lines.length;i++){
      if(!visible(i)){let end=i;while(end+1<data.lines.length&&!visible(end+1))end++;html+='<button class="code-fold" data-unfold="'+i+'" data-end="'+end+'" title="展开完整源码中的这段代码"><span>···</span><span>第 '+(i+1)+'–'+(end+1)+' 行 · 展开</span></button>';i=end;continue;}
      const isHot=hot?.file===selected&&i>=hot.start&&i<=hot.end;
      html+='<div class="code-line '+(isHot?'is-new':'')+'" data-line="'+(i+1)+'"><span class="line-number" aria-hidden="true">'+(i+1)+'</span><code>'+syntax(data.lines[i])+'</code></div>';
    }
    $('source-code').innerHTML=html;
    if(navigate&&hot?.file===selected){const row=$('source-code').querySelector('[data-line="'+(hot.at+1)+'"]');$('source-code').scrollTop=row?Math.max(0,row.offsetTop-$('source-code').offsetTop-52):0;$('source-code').scrollLeft=0;}
    else{const position=scrolls.get(selected)??{top:0,left:0};$('source-code').scrollTop=position.top;$('source-code').scrollLeft=position.left;}
  }
  function remember(){if(selected)scrolls.set(selected,{top:$('source-code').scrollTop,left:$('source-code').scrollLeft});}
  function open(name,navigate=false,automatic=false){
    if(!sources[name])return;remember();
    if(automatic){if(preview&&preview!==name&&!pinned.has(preview))tabs=tabs.filter(file=>file!==preview);preview=pinned.has(name)?'':name;}
    else{pinned.add(name);if(preview===name)preview='';}
    selected=name;if(!tabs.includes(name))tabs.push(name);
    if(!automatic&&!revealed.has(name))reveal(locate(name,''));
    openParents(name);tree();tabbar();code(navigate);
  }
  function go(index,explicit=false){if(playback.active){if(!explicit)return;playback.stop();}if(index<0)return;const focus=focusFor(index);reveal(focus);hot=focus;document.querySelectorAll('[data-reading-step]').forEach(el=>el.classList.toggle('reading-active',Number(el.dataset.readingStep)===index));if(!locked||explicit)open(focus.file,true,true);}
  function read(){
    raf=0;if(playback.active||!anchors.length)return;
    const threshold=Math.min(window.innerHeight*.38,340);let current=-1;
    for(let i=0;i<anchors.length;i++){if(anchors[i].getBoundingClientRect().top<=threshold)current=i;else break;}
    if(current!==step){step=current;go(step);}
    const main=document.getElementById('main'),distance=Math.max(1,main.offsetHeight-window.innerHeight+52);const percent=Math.min(100,Math.max(0,Math.round(window.scrollY/distance*100)));$('open-code').textContent='查看代码 '+percent+'%';
  }
  function schedule(){if(!raf)raf=requestAnimationFrame(read);}
  function lockUI(){const button=$('editor-lock');button.innerHTML=icon(locked?'lock':'unlock');button.setAttribute('aria-pressed',String(locked));button.setAttribute('aria-label',locked?'解锁编辑器，恢复自动导航':'锁定编辑器，停止自动导航');button.title=locked?'恢复跟随阅读':'停止跟随阅读';}
  $('editor-lock').onclick=()=>{locked=!locked;lockUI();if(!locked)go(step);};lockUI();
  $('file-tree').addEventListener('click',event=>{const button=event.target.closest('[data-file]');if(button)open(button.dataset.file);});
  $('file-tree').addEventListener('toggle',event=>{const node=event.target;if(!node.dataset.folder||!node.isConnected)return;if(node.open)collapsed.delete(node.dataset.folder);else collapsed.add(node.dataset.folder);},true);
  $('file-tabs').onclick=event=>{
    const close=event.target.closest('[data-close]');if(close){const name=close.dataset.close,index=tabs.indexOf(name);pinned.delete(name);if(preview===name)preview='';remember();tabs=tabs.filter(f=>f!==name);if(selected===name)selected=tabs[Math.min(index,tabs.length-1)]??'';tree();tabbar();code();const tab=$('file-tabs').querySelector('[aria-selected="true"]');tab?.focus();return;}
    const tab=event.target.closest('[data-tab]');if(tab)open(tab.dataset.tab);
  };
  $('file-tabs').onkeydown=event=>{if(!['ArrowLeft','ArrowRight','Home','End','Delete'].includes(event.key)||!event.target.matches('[role=tab]'))return;event.preventDefault();const i=tabs.indexOf(selected);if(event.key==='Delete'){$('file-tabs').querySelector('.is-active .tab-close')?.click();return;}const next=event.key==='Home'?0:event.key==='End'?tabs.length-1:(i+(event.key==='ArrowRight'?1:-1)+tabs.length)%tabs.length;open(tabs[next]);$('file-tabs').querySelector('[aria-selected="true"]')?.focus();};
  $('source-code').onclick=event=>{const fold=event.target.closest('[data-unfold]');if(!fold)return;remember();const start=Number(fold.dataset.unfold),end=Number(fold.dataset.end),data=units.get(selected);for(const b of data.blocks)if(b.start>=start&&b.start<=end)reveal({file:selected,start:b.start});code();};
  $('full-file').onclick=()=>{remember();full=!full;code();$('editor-menu').open=false;};
  $('copy-file').onclick=()=>{if(selected)copy(sources[selected].code);$('editor-menu').open=false;};
  $('download-file').onclick=()=>{if(selected)download(sources[selected].code,selected.split('/').pop());$('editor-menu').open=false;};
  document.addEventListener('click',event=>{if(!event.target.closest('#editor-menu'))$('editor-menu').open=false;});
  document.addEventListener('keydown',event=>{if(event.key==='Escape')$('editor-menu').open=false;});
  window.addEventListener('scroll',schedule,{passive:true});window.addEventListener('resize',schedule);
  return {startChapter(index){
    playback.setChapter(index);
    const previous=selected;remember();active=index;step=-1;if(!locked){hot=null;full=false;revealed=new Map();}
    available=[...new Set(chapters.slice(0,active+1).flatMap(c=>c.files))];
    tabs=tabs.filter(file=>available.includes(file));selected='';
    collapsed=new Set(available.flatMap(name=>{const parts=name.split('/');return parts.slice(0,-1).map((_,i)=>parts.slice(0,i+1).join('/'));}));
    anchors=[...$('chapter-body').querySelectorAll('h2')];
    anchors.forEach((heading,i)=>{heading.dataset.readingStep=i;const button=document.createElement('button');button.className='code-anchor-link';button.innerHTML='↗';button.setAttribute('aria-label','在工作台查看：'+heading.textContent);button.title='定位本节代码';button.onclick=()=>go(i,true);heading.append(button);});
    // Retain reviewed code from earlier sections when scrolling back to them.
    if(!locked||!tabs.length)open(chapters[active].files[0],false,true);else open(available.includes(previous)?previous:tabs[tabs.length-1]);
    $('editor-menu').open=false;schedule();
  }};
};
