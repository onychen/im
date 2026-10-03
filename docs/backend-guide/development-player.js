/* A disposable editor simulation. Reader DOM and source snapshots stay intact. */
window.createDevelopmentPlayer=({chapters,sources,escape,syntax,copy})=>{
  const $=id=>document.getElementById(id);
  const panel=$('code-panel'),reader=panel.querySelector('.repo-workspace');
  const root=document.createElement('section');root.id='development-player';root.hidden=true;
  root.setAttribute('aria-label','开发过程播放器');
  root.innerHTML=`
    <div class="development-controls">
      <div class="development-buttons">
        <button id="dev-toggle" class="dev-toggle" aria-label="暂停开发播放">Ⅱ 暂停</button>
        <button id="dev-prev" title="回退一步" aria-label="回退一个开发步骤">←</button>
        <button id="dev-next" title="完成当前步骤，前进到下一步并暂停" aria-label="前进一个开发步骤">→</button>
        <button id="dev-restart" title="从头播放" aria-label="从头播放开发过程">↺</button>
        <label class="dev-speed-label"><span class="sr-only">播放速度</span><select id="dev-speed" aria-label="播放速度"><option value="0.5">0.5×</option><option value="1">1×</option><option value="2" selected>2×</option><option value="4">4×</option><option value="8">8×</option></select></label>
        <button id="dev-exit">返回阅读</button>
      </div>
      <label class="development-stage"><span id="dev-counter">步骤</span><select id="dev-stage" aria-label="选择开发步骤"></select></label>
      <p id="dev-why" aria-live="polite" aria-atomic="true"></p>
      <div id="dev-progress" class="dev-progress" role="progressbar" aria-label="开发演示进度" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"><span></span></div>
    </div>
    <div class="repo-workspace development-workspace">
      <nav id="dev-tree" class="file-tree" aria-label="演示中已创建的文件"></nav>
      <div class="editor-column">
        <div class="editor-topline"><div id="dev-tabs" class="file-tabs" role="tablist" aria-label="演示文件标签"></div><button id="dev-copy" class="dev-copy" title="复制已敲出的内容">复制片段</button></div>
        <div id="dev-code" class="source-code" role="tabpanel" tabindex="0" aria-label="正在输入的演示代码"></div>
      </div>
    </div>
    <div class="development-footer"><span id="dev-state" role="status">教学推演</span><span id="dev-scope"></span></div>`;
  reader.after(root);
  const reduced=window.matchMedia('(prefers-reduced-motion: reduce)');
  let lesson=null,chapterIndex=0,active=false,running=false,index=0,characters=0,elapsed=0,last=0,timer=0;
  let buffers=new Map(),tabs=[],selected='',typingOffset=-1;
  const current=()=>lesson?.events[index];
  const ordered=file=>[...(buffers.get(file)??new Map())].sort((a,b)=>a[0]-b[0]);
  const textFor=file=>ordered(file).map(([,text])=>text).join('');
  const clearTimer=()=>{clearTimeout(timer);timer=0;};
  const speed=()=>Number($('dev-speed').value);
  function fileShell(){
    const tree={children:new Map()};
    for(const file of buffers.keys()){
      let node=tree;const parts=file.split('/');
      for(let i=0;i<parts.length;i++){
        const path=parts.slice(0,i+1).join('/');
        if(!node.children.has(parts[i]))node.children.set(parts[i],{path,label:parts[i],children:new Map(),file:i===parts.length-1?file:null});
        node=node.children.get(parts[i]);
      }
    }
    function html(node,depth){return [...node.children.values()].map(n=>n.file?'<button class="tree-file '+(n.file===selected?'is-active':'')+'" data-dev-file="'+escape(n.file)+'" style="--depth:'+depth+'" title="'+escape(n.file)+'" '+(n.file===selected?'aria-current="true"':'')+'><span aria-hidden="true">◇</span><span>'+escape(n.label)+'</span></button>':'<details class="tree-folder" open><summary style="--depth:'+depth+'"><span aria-hidden="true">▱</span><span>'+escape(n.label)+'</span></summary>'+html(n,depth+1)+'</details>').join('');}
    $('dev-tree').innerHTML=html(tree,0)||'<p class="dev-tree-empty">文件会按开发顺序出现</p>';
    $('dev-tabs').innerHTML=tabs.map((file,i)=>'<div class="editor-tab '+(file===selected?'is-active':'')+'"><button role="tab" id="dev-tab-'+i+'" aria-controls="dev-code" aria-selected="'+(file===selected)+'" tabindex="'+(file===selected?'0':'-1')+'" data-dev-tab="'+escape(file)+'" title="'+escape(file)+'"><span>'+escape(file.split('/').pop())+'</span></button></div>').join('');
    const selectedTab=$('dev-tabs').querySelector('[aria-selected="true"]');
    if(selectedTab){$('dev-code').setAttribute('aria-labelledby',selectedTab.id);selectedTab.scrollIntoView({block:'nearest',inline:'nearest',behavior:'instant'});}
    else $('dev-code').removeAttribute('aria-labelledby');
    $('dev-copy').disabled=!selected;
  }
  function renderCode(follow=true){
    const text=selected?textFor(selected):'';
    const event=current();const at=event?.kind==='type'&&selected===event.file?typingOffset:-1;
    const before=at>=0?text.slice(0,at):'';
    const row=at>=0?before.split('\n').length:0;
    const col=at>=0?before.length-before.lastIndexOf('\n')-1:0;
    let position=0;
    $('dev-code').innerHTML=text.split('\n').map((line,i)=>{
      const cursor=i+1===row||(event?.kind==='create'&&!text&&i===0);
      const code=cursor&&at>=0?syntax(line.slice(0,col))+'<span class="typing-cursor" aria-hidden="true"></span>'+syntax(line.slice(col)):syntax(line)+(cursor?'<span class="typing-cursor" aria-hidden="true"></span>':'');
      const hot=event?.kind==='type'&&event.file===selected&&position<=at&&position+line.length>=at;
      position+=line.length+1;
      return '<div class="code-line '+(hot?'is-new':'')+'" data-dev-line="'+(i+1)+'"><span class="line-number" aria-hidden="true">'+(i+1)+'</span><code>'+code+'</code></div>';
    }).join('');
    if(follow){
      const cursor=$('dev-code').querySelector('.typing-cursor');
      if(cursor){const r=cursor.parentElement.parentElement,box=$('dev-code');const top=r.offsetTop;
        if(top<box.scrollTop+20||top+24>box.scrollTop+box.clientHeight-30)box.scrollTop=Math.max(0,top-box.clientHeight*.55);
        if(cursor.offsetLeft<box.scrollLeft+50||cursor.offsetLeft>box.scrollLeft+box.clientWidth-40)box.scrollLeft=Math.max(0,cursor.offsetLeft-box.clientWidth*.65);
      }
    }
    $('dev-code').setAttribute('aria-label',(selected||'尚未创建文件')+' · 演示缓冲区');
  }
  function controls(){
    if(!lesson)return;
    const event=current(),done=index===lesson.events.length-1;
    const fraction=event.kind==='type'?characters/Math.max(1,event.text.length):elapsed>=700?1:0;
    const progress=Math.round((index+fraction)/lesson.events.length*100);
    $('dev-counter').textContent=(index+1)+' / '+lesson.events.length;
    $('dev-stage').value=String(index);
    if($('dev-why').textContent!==event.why)$('dev-why').textContent=event.why;
    $('dev-toggle').textContent=running?'Ⅱ 暂停':done?'↺ 重播':'▶ 继续';
    $('dev-toggle').setAttribute('aria-label',running?'暂停开发播放':done?'重新播放开发过程':'继续开发播放');
    $('dev-toggle').setAttribute('aria-pressed',String(running));
    $('dev-prev').disabled=index===0;
    $('dev-next').disabled=done;
    const status=done?'演示结束 · 请实际验收':running?(event.kind==='type'?'正在敲代码':event.kind==='create'?'创建 / 打开文件':'教学推演'):'已暂停 · 可查看文件';
    if($('dev-state').textContent!==status)$('dev-state').textContent=status;
    $('dev-progress').setAttribute('aria-valuenow',String(done?100:progress));
    $('dev-progress').firstElementChild.style.width=(done?100:progress)+'%';
    root.classList.toggle('is-paused',!running);
  }
  function ensureFile(file){if(!buffers.has(file))buffers.set(file,new Map());if(!tabs.includes(file))tabs.push(file);selected=file;}
  function apply(event,count=Infinity){
    if(!event.file)return;
    ensureFile(event.file);
    if(event.kind==='type')buffers.get(event.file).set(event.start,event.text.slice(0,count));
  }
  function enter(next){
    index=next;characters=0;elapsed=0;typingOffset=-1;
    const event=current();apply(event,0);
    if(event.kind==='type')typingOffset=ordered(event.file).filter(([start])=>start<event.start).reduce((n,[,text])=>n+text.length,0);
    if(event.kind==='check'){running=false;clearTimer();}
    fileShell();renderCode();controls();
  }
  function rebuild(next,complete){
    clearTimer();buffers=new Map();tabs=[];selected='';
    for(let i=0;i<next;i++)apply(lesson.events[i]);
    running=false;enter(next);
    if(complete){const event=current();if(event.kind==='type'){characters=event.text.length;apply(event);typingOffset+=characters;elapsed=event.text.length/.065+550;}else elapsed=900;renderCode();controls();}
  }
  function tick(){
    timer=0;if(!active||!running)return;
    const now=performance.now(),delta=Math.min(160,now-last)*speed();last=now;
    const event=current();elapsed+=delta;
    if(event.kind==='type'){
      const previous=characters;characters=reduced.matches?event.text.length:Math.max(previous,Math.min(event.text.length,Math.floor(elapsed*.065)));
      if(characters!==previous){apply(event,characters);typingOffset+=characters-previous;renderCode();}
    }
    const finished=event.kind==='type'?characters===event.text.length&&elapsed>=(reduced.matches?550:event.text.length/.065+550):elapsed>=900;
    if(finished&&index<lesson.events.length-1)enter(index+1);
    controls();if(running)timer=setTimeout(tick,40);
  }
  function resume(){
    if(!active)return;
    if(current().kind==='check')rebuild(0,false);
    if(current().file&&selected!==current().file){selected=current().file;fileShell();renderCode();}
    running=true;last=performance.now();controls();clearTimer();timer=setTimeout(tick,40);
  }
  function pause(){running=false;clearTimer();controls();}
  function inspect(file){pause();selected=file;fileShell();renderCode(false);}
  function stop(){
    clearTimer();active=false;running=false;root.hidden=true;reader.hidden=false;
    $('editor-lock').hidden=false;$('play-development').setAttribute('aria-pressed','false');
    $('play-development').textContent='▶ 播放开发';panel.classList.remove('is-developing');
  }
  function start(c){
    chapterIndex=c;lesson=window.buildDevelopmentLesson(chapters[c],sources,chapters.slice(0,c).flatMap(ch=>ch.files));
    if(lesson.errors.length)return;
    active=true;root.hidden=false;reader.hidden=true;$('editor-menu').open=false;$('editor-lock').hidden=true;
    $('play-development').textContent='退出播放';$('play-development').setAttribute('aria-pressed','true');panel.classList.add('is-developing');
    $('dev-scope').textContent=lesson.scope;
    $('dev-stage').innerHTML=lesson.events.map((event,i)=>'<option value="'+i+'">'+(i+1)+'. '+escape(event.title)+'</option>').join('');
    rebuild(0,false);resume();
  }
  $('play-development').onclick=()=>active?stop():start(chapterIndex);
  $('dev-toggle').onclick=()=>running?pause():resume();
  $('dev-prev').onclick=()=>rebuild(Math.max(0,index-1),true);
  // A single step completes the current action and the next action, then waits.
  $('dev-next').onclick=()=>rebuild(Math.min(lesson.events.length-1,index+1),true);
  $('dev-restart').onclick=()=>{rebuild(0,false);resume();};
  $('dev-stage').onchange=()=>rebuild(Number($('dev-stage').value),true);
  $('dev-exit').onclick=()=>{stop();$('play-development').focus();};
  $('dev-copy').onclick=()=>{pause();copy(textFor(selected));};
  $('dev-tree').onclick=event=>{const button=event.target.closest('[data-dev-file]');if(button)inspect(button.dataset.devFile);};
  $('dev-tabs').onclick=event=>{const button=event.target.closest('[data-dev-tab]');if(button)inspect(button.dataset.devTab);};
  $('dev-tabs').onkeydown=event=>{
    if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key)||!event.target.matches('[role=tab]'))return;
    event.preventDefault();const i=tabs.indexOf(selected),next=event.key==='Home'?0:event.key==='End'?tabs.length-1:(i+(event.key==='ArrowRight'?1:-1)+tabs.length)%tabs.length;
    inspect(tabs[next]);$('dev-tabs').querySelector('[aria-selected="true"]')?.focus();
  };
  root.onkeydown=event=>{
    if(event.key==='Escape'){stop();$('play-development').focus();}
    else if(event.code==='Space'&&event.target===$('dev-code')){event.preventDefault();running?pause():resume();}
  };
  document.addEventListener('visibilitychange',()=>{if(document.hidden&&active)pause();});
  // Mobile drawer dismissal also suspends a lesson that is no longer visible.
  $('close-code').addEventListener('click',()=>{if(active)pause();});
  $('overlay').addEventListener('click',()=>{if(active)pause();});
  window.DEVELOPMENT_AUDIT=chapters.flatMap(ch=>window.buildDevelopmentLesson(ch,sources).errors.map(error=>({chapter:ch.id,error})));
  $('play-development').disabled=window.DEVELOPMENT_AUDIT.length>0;
  return {get active(){return active;},setChapter(c){stop();chapterIndex=c;},stop};
};
