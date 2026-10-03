(() => {
  'use strict';
  const {chapters,sources,meta}=window.GUIDE;
  const chapterKey=index=>chapters[index].id-1;
  const $=id=>document.getElementById(id);
  const escape=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const load=(key,fallback)=>{try{return JSON.parse(localStorage.getItem(key))??fallback}catch{return fallback}};
  const save=(key,value)=>{try{localStorage.setItem(key,JSON.stringify(value))}catch{}};
  let done=load('goim-guide-done-v1',[]), checked=load('goim-guide-checks-v1',{}), unlocked=load('goim-guide-unlocked-v1',0), active=0, selectedFile='';
  if(!Array.isArray(done))done=[];
  if(!checked || typeof checked!=='object')checked={};
  unlocked=Math.max(0,Math.min(chapters.length-1,Number(unlocked)||0));
  document.documentElement.dataset.theme=load('goim-guide-theme-v1','light')==='dark'?'dark':'light';
  let toastTimeout;
  const toast=text=>{$('toast').textContent=text;$('toast').classList.add('show');clearTimeout(toastTimeout);toastTimeout=setTimeout(()=>$('toast').classList.remove('show'),2200)};
  const inline=text=>escape(text).replace(/`([^`]+)`/g,'<code>$1</code>').replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+|[a-zA-Z0-9_.-]+\.html|#chapter\d+)\)/g,(_,label,url)=>'<a href="'+url+'"'+(url.startsWith('#')?'':' target="_blank" rel="noreferrer"')+'>'+label+'</a>');
  function markdown(text){
    const lines=text.split('\n');let html='',paragraph=[],list=null;
    const flush=()=>{if(paragraph.length){html+='<p>'+inline(paragraph.join(' '))+'</p>';paragraph=[]}if(list){html+='</'+list+'>';list=null}};
    for(let i=0;i<lines.length;i++){
      const line=lines[i];
      if(line.startsWith(':::details ')){flush();html+='<details class="reading-details"><summary>'+inline(line.slice(11))+'</summary><div class="details-content">';continue}
      if(line===':::'){flush();html+='</div></details>';continue}
      if(line.startsWith('```')){flush();const lang=line.slice(3)||'code';const code=[];while(++i<lines.length&&!lines[i].startsWith('```'))code.push(lines[i]);html+='<div class="block-code"><div class="block-code-header"><span>'+escape(lang)+'</span><button class="copy-snippet">复制</button></div><pre><code>'+escape(code.join('\n'))+'</code></pre></div>';continue}
      if(!line.trim()){flush();continue}
      if(line.startsWith('## ')){flush();html+='<h2>'+inline(line.slice(3))+'</h2>';continue}
      if(line.startsWith('### ')){flush();html+='<h3>'+inline(line.slice(4))+'</h3>';continue}
      if(line.startsWith('|')&&/^\|(?:\s*:?-+:?\s*\|)+$/.test(lines[i+1]??'')){
        flush();const cells=row=>row.trim().replace(/^\||\|$/g,'').split('|').map(cell=>cell.trim());
        const headers=cells(line);i++;const rows=[];
        while(i+1<lines.length&&lines[i+1].startsWith('|'))rows.push(cells(lines[++i]));
        html+='<div class="table-scroll" role="region" aria-label="参数与接口对照" tabindex="0"><table><thead><tr>'+headers.map(cell=>'<th scope="col">'+inline(cell)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(row=>'<tr>'+headers.map((_,n)=>'<td>'+inline(row[n]??'')+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';continue;
      }
      const item=line.match(/^(- |\d+\. )(.*)$/);
      if(item){if(paragraph.length){html+='<p>'+inline(paragraph.join(' '))+'</p>';paragraph=[]}const type=item[1].startsWith('-')?'ul':'ol';if(list!==type){if(list)html+='</'+list+'>';html+='<'+type+'>';list=type}html+='<li>'+inline(item[2])+'</li>';continue}
      if(list){html+='</'+list+'>';list=null}paragraph.push(line);
    }
    flush();return html;
  }
  function syntax(line){
    const parts=line.split(/("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|\/\/.*$)/g);
    return parts.map(part=>{
      if(/^\/\//.test(part))return '<span class="tok-comment">'+escape(part)+'</span>';
      if(/^['"]/.test(part))return '<span class="tok-string">'+escape(part)+'</span>';
      return escape(part).replace(/\b(package|import|func|type|struct|interface|return|if|else|for|range|switch|case|default|const|var|defer|go|select|make|nil|true|false|CREATE|TABLE|INDEX|SELECT|FROM|WHERE|local|then|end)\b/g,'<span class="tok-keyword">$1</span>');
    }).join('');
  }
  async function copy(text){
    try{if(navigator.clipboard&&window.isSecureContext){await navigator.clipboard.writeText(text)}else{const area=document.createElement('textarea');area.value=text;area.style.position='fixed';area.style.opacity='0';document.body.append(area);area.select();if(!document.execCommand('copy'))throw Error();area.remove()}toast('已复制到剪贴板')}catch{toast('复制受浏览器限制，请选择文本复制')}
  }
  function download(text,name){const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([text],{type:'text/plain;charset=utf-8'}));a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),500)}
  function progress(){const count=done.filter(n=>n>=0&&n<chapters.length).length;$('progress-label').textContent=count+' / '+chapters.length;$('progress-bar').style.width=count/chapters.length*100+'%';document.querySelectorAll('.chapter-link').forEach((el,i)=>{el.classList.toggle('completed',done.includes(chapterKey(i)));el.querySelector('.num').textContent=done.includes(chapterKey(i))?'✓':String(i+1).padStart(2,'0')})}
  function drawer(panel){$('sidebar').classList.toggle('open',panel==='sidebar');$('code-panel').classList.toggle('open',panel==='code');$('overlay').classList.toggle('open',!!panel);document.body.classList.toggle('drawer-open',!!panel);$('menu').setAttribute('aria-expanded',String(panel==='sidebar'));$('open-code').setAttribute('aria-expanded',String(panel==='code'))}
  function checkpoint(){const key=chapterKey(active),current=chapters[active],values=Array.isArray(checked[key])?checked[key]:[];const all=current.criteria.every((_,i)=>values.includes(i));$('complete').disabled=!all&&!done.includes(key);$('complete').textContent=done.includes(key)?'已完成 · 撤销记录':'记录本章完成';$('completion-note').textContent=done.includes(key)?'这是你的自评记录。':all?'验收通过后，可以记录完成。':'按实际结果勾选验收项。'}
  function render(){
    const match=location.hash.match(/^#chapter(\d+)$/);active=match?Math.max(0,chapters.findIndex(c=>c.id===Number(match[1]))):0;
    if(location.hash!=='#chapter'+chapters[active].id)history.replaceState(null,'','#chapter'+chapters[active].id);
    unlocked=Math.max(unlocked,active);save('goim-guide-unlocked-v1',unlocked);const chapter=chapters[active];
    document.title=String(active+1).padStart(2,'0')+' · '+chapter.title+' | GoIM 从零到一';
    $('chapter-phase').textContent=chapter.phase;$('chapter-time').textContent='阅读 + 实验约 '+chapter.time;$('chapter-number').textContent=String(active+1).padStart(2,'0');$('chapter-title').textContent=chapter.title;$('chapter-lead').textContent=chapter.lead;$('chapter-body').innerHTML=markdown(chapter.body);$('task').textContent=chapter.task;$('question').textContent=chapter.question;$('answer').textContent=chapter.answer;$('checkpoint-number').textContent=String(active+1).padStart(2,'0');
    $('criteria').innerHTML=chapter.criteria.map((item,i)=>'<label class="criteria-item"><input type="checkbox" data-criterion="'+i+'" '+((checked[chapterKey(active)]||[]).includes(i)?'checked':'')+'><span>'+escape(item)+'</span></label>').join('');
    document.querySelectorAll('.chapter-link').forEach((el,i)=>{el.classList.toggle('active',i===active);if(i===active)el.setAttribute('aria-current','step');else el.removeAttribute('aria-current')});
    $('previous').disabled=active===0;$('previous').textContent=active===0?'从这里开始':'上一章 · '+chapters[active-1].title;$('next').disabled=active===chapters.length-1;$('next').textContent=active===chapters.length-1?'完成综合验收':'下一章 · '+chapters[active+1].title;
    document.querySelectorAll('.route-map span').forEach((el,i)=>el.classList.toggle('current',i===(active<4?0:active<9?1:active<16?2:3)));
    progress();checkpoint();renderDemo(chapter.demo);drawer(null);window.scrollTo({top:0,behavior:'instant'});workbench.startChapter(active);
  }
  let currentPhase='';$('chapters').innerHTML=chapters.map((c,i)=>{let heading='';if(c.phase!==currentPhase){currentPhase=c.phase;heading='<div class="nav-group">'+escape(c.phase)+'</div>'}return heading+'<button class="chapter-link" data-chapter="'+i+'"><span class="num">'+String(i+1).padStart(2,'0')+'</span><span class="nav-title">'+escape(c.title)+'</span></button>'}).join('');
  $('chapters').addEventListener('click',event=>{const el=event.target.closest('[data-chapter]');if(el)location.hash='chapter'+chapters[Number(el.dataset.chapter)].id});
  $('previous').onclick=()=>{if(active>0)location.hash='chapter'+chapters[active-1].id};$('next').onclick=()=>{if(active<chapters.length-1)location.hash='chapter'+chapters[active+1].id};
  $('criteria').onchange=()=>{const key=chapterKey(active);checked[key]=[...$('criteria').querySelectorAll('input:checked')].map(el=>Number(el.dataset.criterion));if(done.includes(key)&&checked[key].length!==chapters[active].criteria.length)done=done.filter(n=>n!==key);save('goim-guide-checks-v1',checked);save('goim-guide-done-v1',done);progress();checkpoint()};
  $('complete').onclick=()=>{const key=chapterKey(active);if(done.includes(key))done=done.filter(n=>n!==key);else done.push(key);save('goim-guide-done-v1',done);progress();checkpoint();toast(done.includes(key)?'已记录本章完成':'已撤销完成记录')};
  $('chapter-body').onclick=event=>{const button=event.target.closest('.copy-snippet');if(button)copy(button.closest('.block-code').querySelector('code').textContent)};
  $('theme').onclick=()=>{const theme=document.documentElement.dataset.theme==='light'?'dark':'light';document.documentElement.dataset.theme=theme;save('goim-guide-theme-v1',theme);$('theme').setAttribute('aria-label',theme==='light'?'切换深色主题':'切换浅色主题')};
  $('menu').onclick=()=>drawer($('sidebar').classList.contains('open')?null:'sidebar');$('open-code').onclick=()=>drawer('code');$('close-code').onclick=()=>drawer(null);$('overlay').onclick=()=>drawer(null);
  document.addEventListener('keydown',event=>{if(event.key==='Escape')drawer(null)});
  $('about-button').onclick=()=>$('about').showModal();$('close-about').onclick=()=>$('about').close();$('base-commit').textContent=meta.baseCommit.slice(0,12);$('verification').textContent=meta.verification;
  window.addEventListener('hashchange',render);
  window.addEventListener('resize',()=>{if(window.innerWidth>=1200)drawer(null)});
  function renderDemo(type){
    const mount=$('demo');mount.innerHTML='';if(!type)return;
    const titles={pipeline:'故障发生后，消息在哪？',dedup:'同一个逻辑发送，只生成一个 ID',watermark:'把“已读”变成一个向前的游标'};
    mount.innerHTML='<section class="demo"><span class="section-label">'+(type==='watermark'?'改进目标 · 当前代码尚未保证单调更新':'可操作的状态模型')+'</span><h2>'+titles[type]+'</h2><div id="demo-inner"></div><p class="simulation-note">模型演示 · 不连接真实后端 · 不计入验收结果</p></section>';
    if(type==='pipeline')pipeline();if(type==='dedup')dedup();if(type==='watermark')watermark();
  }
  function pipeline(){
    const scenarios={normal:[['Redis','Lua 写入读模型和 Stream：有可恢复待办。'],['Relay','Relay 领取待办，事件进入 pending。'],['Kafka','Kafka 接受事件并按 ISR 策略确认。'],['ACK','Relay 删除已确认待办，发送 serverAck 和实时消息。'],['MySQL','消费者异步归档。归档和接收端展示是不同结果。']],kafka:[['Redis','Redis 写入成功，待办还在 Stream。'],['Relay','Kafka 不可用，投递失败，不删除待办，不发送首次 ACK。'],['Relay','10 秒以上 idle pending 可被接管；超龄事件可移到失败待办。'],['Kafka','Kafka 恢复后重新投递，成功后才移交；客户端可能已经重试。']],mysql:[['Redis','消息读模型和待办写入成功。'],['Kafka','Kafka 已确认；首次 serverAck 可以返回。'],['MySQL','MySQL 暂时不可用，归档进入 retry，实时通道不等于归档成功。'],['MySQL','5 秒后重试；达到次数上限转 DLQ，需另外处置。']],crash:[['Kafka','Kafka 已接收，relay 在删除待办之前崩溃。'],['Relay','重启接管 pending；同一服务端 ID 再次投递。'],['MySQL','同 ID、同内容的重复归档应幂等成功；冲突内容应拒绝。'],['ACK','若在删待办后、推 WS 前崩溃，实时推送可能缺失，由补同步恢复。']]};
    $('demo-inner').innerHTML='<p>换一种故障，再一步步观察移交边界。</p><div class="demo-controls"><label>场景<select id="scenario"><option value="normal">正常发送</option><option value="kafka">Kafka 暂时不可用</option><option value="mysql">MySQL 暂时不可用</option><option value="crash">确认附近进程崩溃</option></select></label><button id="step" class="primary">推进一步</button><button id="reset-demo">重置</button></div><div class="pipeline-nodes">'+['Redis','Relay','Kafka','ACK','MySQL'].map(name=>'<span class="pipeline-node" data-node="'+name+'">'+name+'</span>').join('')+'</div><div class="demo-log" id="pipeline-log" aria-live="polite">还没有发送。先选择场景，再推进一步。</div>';
    let index=-1;const reset=()=>{index=-1;$('step').disabled=false;$('pipeline-log').textContent='还没有发送。先选择场景，再推进一步。';document.querySelectorAll('.pipeline-node').forEach(el=>el.classList.remove('active'))};
    $('scenario').onchange=reset;$('reset-demo').onclick=reset;$('step').onclick=()=>{const steps=scenarios[$('scenario').value];index++;const [node,text]=steps[index];document.querySelectorAll('.pipeline-node').forEach(el=>el.classList.toggle('active',el.dataset.node===node));$('pipeline-log').textContent=(index+1)+' / '+steps.length+' · '+text;$('step').disabled=index===steps.length-1};
  }
  function dedup(){
    $('demo-inner').innerHTML='<p>固定发送者 Alice。去重 key 按发送者 + 客户端 ID，当前窗口是 300 秒。</p><div class="demo-controls"><label>客户端 msgId<input id="client-id" value="alice-001" maxlength="128"></label><button id="send-demo" class="primary">发送</button><button id="expire-demo">前进 301 秒</button><button id="reset-demo">重置</button></div><div class="demo-log" id="dedup-log" aria-live="polite" style="margin-top:20px">输入同一个 ID 发送两次，观察服务端 ID。</div>';
    let time=0,next=1001;const records=new Map();$('send-demo').onclick=()=>{const id=$('client-id').value.trim();if(!id){toast('请填写客户端 ID');return}const old=records.get(id);if(old&&time-old.time<300){$('dedup-log').textContent='重复：保留原 serverMsgId='+old.id+'，不新增 Stream 事件。当前私聊与群聊 service 返回重复错误；重发原 ACK 是待实现改进。'}else{const record={time,id:next++};records.set(id,record);$('dedup-log').textContent='首次：生成 serverMsgId='+record.id+'，写入读模型与待办。模型时钟 '+time+' 秒。'}};
    $('expire-demo').onclick=()=>{time+=301;$('dedup-log').textContent='模型时间前进到 '+time+' 秒。再次发送原 ID 时，Redis 去重窗口已到期，可能生成另一个服务端 ID。'};$('reset-demo').onclick=()=>{time=0;next=1001;records.clear();$('dedup-log').textContent='已清空模型状态。真实后端未受影响。'};
  }
  function watermark(){
    $('demo-inner').innerHTML='<p>已读只向前，且不能超过群最新序号。试着提交一个迟到的 ACK。</p><div class="watermark-stats"><div><strong id="latest"></strong><span>群最新序号</span></div><div><strong id="read"></strong><span>我的已读位置</span></div><div><strong id="unread"></strong><span>未读序号差</span></div></div><div class="watermark-track"><span id="read-bar"></span></div><div class="demo-controls"><label>上报 lastReadGroupSeq<input id="ack-seq" type="number" value="35" min="1" step="1"></label><button id="ack-demo" class="primary">上报已读</button><button id="new-message">新来一条消息</button><button id="reset-demo">重置</button></div><div class="demo-log" id="watermark-log" aria-live="polite" style="margin-top:20px"></div>';
    let latest=42,read=40;const update=text=>{$('latest').textContent=latest;$('read').textContent=read;$('unread').textContent=Math.max(0,latest-read);$('read-bar').style.width=read/latest*100+'%';$('watermark-log').textContent=text};update('已经读到 40，还有 41、42 两条未读。');
    $('ack-demo').onclick=()=>{const seq=Number($('ack-seq').value);if(!Number.isInteger(seq)||seq<1){toast('请填一个正整数序号');return}const previous=read;read=Math.max(read,Math.min(seq,latest));update('旧位置 '+previous+'，上报 '+seq+' → 保存 '+read+'。迟到 ACK 不倒退，过大 ACK 被限制到最新。')};$('new-message').onclick=()=>{latest++;update('群最新序号推进到 '+latest+'，已读位置保持 '+read+'。')};$('reset-demo').onclick=()=>{latest=42;read=40;$('ack-seq').value=35;update('恢复初始模型：最新 42、已读 40、未读 2。')};
  }
  const workbench=window.createWorkbench({chapters,sources,escape,syntax,copy,download});
  $('course-size').textContent=chapters.length+' 章 · Go 后端开发带路';
  render();
})();
