'use strict';
const $=selector=>document.querySelector(selector);
const node=(tag,className,text)=>{const el=document.createElement(tag);if(className)el.className=className;if(text!==undefined)el.textContent=text;return el;};
const state={me:null,csrf:'',instances:[],connectors:{sources:[],sinks:[]},users:[]};
function toast(message){const el=$('#toast');el.textContent=message;el.classList.add('show');clearTimeout(toast.timer);toast.timer=setTimeout(()=>el.classList.remove('show'),2400);}
async function api(path,options={}){
  const headers=new Headers(options.headers||{});if(options.body)headers.set('content-type','application/json');
  if(!['GET','HEAD'].includes(options.method||'GET'))headers.set('x-csrf-token',state.csrf);
  const response=await fetch(path,{...options,headers});
  if(response.status===401){location.replace('/login?next='+encodeURIComponent(location.pathname));throw new Error('登录已失效');}
  const body=response.status===204?null:await response.json().catch(()=>null);
  if(!response.ok)throw new Error(body?.error?.message||'请求失败');return body;
}
function status(value,label){const span=node('span','status '+value);span.append(node('i','ph ph-'+(value==='good'?'check-circle':'warning-circle')),document.createTextNode(label));return span;}
function title(name,description){const row=node('div','title-row'),left=node('div');left.append(node('h1','',name),node('p','',description));row.append(left);return row;}
function empty(message){const box=node('div','panel empty');box.append(node('h2','',message));return box;}
function instanceAddLink(label){const link=node('a','btn primary',label);link.href='/instances/new';return link;}
function setTitle(name){$('#top-title').textContent=name;document.title=name+' · CDC';}
function roleName(role){return role==='admin'?'管理员':'普通账号';}
function databaseType(instance){return instance.kind==='postgresql'?'PostgreSQL':'MySQL';}
function metadata(instance) {
  const d=node('dl','metadata'),m=instance.metadata,pg=instance.kind==='postgresql';
  const fields=[['版本',databaseType(instance)+' '+(m?.server_version||instance.version)]];
  if(pg)fields.push(['连接数据库',(instance.databases||[instance.database]).join('、')||'未配置'],['wal_level',m?.wal_level||'未探测'],['字符编码',m?.server_encoding||'-'],
    ['复制权限',m?(m.can_replicate?'已具备':'未具备'):'-'],['当前库复制槽',m?String(m.replication_slots):'-'],
    ['max_replication_slots',m?String(m.max_replication_slots):'-'],['max_wal_senders',m?String(m.max_wal_senders):'-'],
    ['实例角色',m?(m.in_recovery?'备库':'主库'):'-']);
  else fields.push(['log_bin',m?String(m.log_bin).toUpperCase():'未探测'],['binlog_format',m?.binlog_format||'-'],['binlog_row_image',m?.binlog_row_image||'-'],['GTID',m?.gtid_mode||'-']);
  fields.forEach(([key,value])=>{const row=node('div');row.append(node('dt','',key),node('dd','',value));d.append(row);});return d;
}
function captureConfiguration(item){
  if(!item.metadata)return ['尚未探测',item.probe_error||'点击探测'];
  const m=item.metadata;
  return item.kind==='postgresql'?['WAL '+m.wal_level,(m.can_replicate?'具备复制权限':'缺少复制权限')+' · '+((item.databases||[item.database]).length)+' 个数据库']:[m.binlog_format+' / '+m.binlog_row_image,'GTID '+m.gtid_mode];
}
function renderHome(){
  setTitle('数据库总览');const main=$('#content'),pageTitle=title('数据库总览','SQLite 中保存的实例连接与 CDC 元信息');if(state.me.user.role==='admin')pageTitle.append(instanceAddLink('添加实例'));main.replaceChildren(pageTitle);
  const online=state.instances.filter(item=>item.metadata&&!item.probe_error).length;const summary=node('div','overview-summary');summary.append(node('span','',state.instances.length+' 个实例'),node('span','good',online+' 个已探测'),node('span','muted','账号密码不会返回浏览器'));main.append(summary);
  if(!state.instances.length){const box=empty('还没有数据库实例');box.append(instanceAddLink('添加第一个实例'));main.append(box);return;}
  const layout=node('div','inventory-layout'),left=node('div','inventory-main'),heading=node('div','section-heading');heading.append(node('h2','','已管理实例 / '+String(state.instances.length).padStart(2,'0')),node('span','muted','点击实例查看配置'));left.append(heading);
  const wrap=node('div','table-wrap'),table=node('table','inventory-table'),thead=node('thead'),head=node('tr');['实例 / 连接地址','版本','状态','采集配置','凭据','操作'].forEach(text=>head.append(node('th','',text)));thead.append(head);const tbody=node('tbody');
  state.instances.forEach((item,index)=>{const tr=node('tr',index===0?'selected':'');tr.dataset.instance=item.id;const identity=node('td');identity.append(node('b','',item.name),node('small','mono',item.host+':'+item.port));const config=node('td'),configuration=captureConfiguration(item);config.append(node('span','config-value',configuration[0]),node('small','muted',configuration[1]));const credentials=node('td');credentials.append(node('span','',item.reader_username?'读取账号已配置':'无读取账号'),node('small','muted',item.writer_username?'写入账号已配置':'无写入账号'));const actions=node('td');if(state.me.user.role==='admin'){const probe=node('button','text-button','探测');probe.dataset.probe=item.id;const edit=node('a','text-button','编辑');edit.href='/instances/'+encodeURIComponent(item.id)+'/edit';actions.append(probe,document.createTextNode(' · '),edit);}else actions.append(node('span','muted','只读'));tr.append(identity,node('td','mono',databaseType(item)+' '+item.version),node('td','',item.probe_error?'异常':item.metadata?'正常':'待探测'),config,credentials,actions);tbody.append(tr);});
  table.append(thead,tbody);wrap.append(table);left.append(wrap);const inspector=node('aside','inspector');layout.append(left,inspector);main.append(layout);
  function select(id){const item=state.instances.find(value=>value.id===id);if(!item)return;tbody.querySelectorAll('tr').forEach(row=>row.classList.toggle('selected',row.dataset.instance===id));inspector.replaceChildren(node('h2','','实例详情'),node('h3','',item.name),node('p','mono',item.host+':'+item.port),item.probe_error?status('danger',item.probe_error):status(item.metadata?'good':'warn',item.metadata?'元信息已更新':'尚未探测'),metadata(item));}
  select(state.instances[0].id);tbody.addEventListener('click',event=>{const row=event.target.closest('tr[data-instance]');if(row&&!event.target.closest('a,button'))select(row.dataset.instance);});
}
function instanceFields(item={}) {
  return `<div class="form-grid">
    <label>实例名称<input name="name" class="input" maxlength="128" required value="${attribute(item.name||'')}"></label>
    <label>地址<input name="host" class="input" maxlength="253" required value="${attribute(item.host||'')}"></label>
    <label>数据库类型<select name="kind" class="select"><option value="mysql">MySQL</option><option value="postgresql">PostgreSQL</option></select></label>
    <label>数据库版本<select name="version" class="select"></select></label>
    <label>端口<input name="port" class="input" type="number" min="1" max="65535" required value="${item.port||3306}"></label>
    <div class="pg-database-picker" data-pg-database><div class="pg-database-heading"><span>连接数据库</span><button type="button" class="btn" data-discover-databases>自动探测数据库</button></div><div class="pg-database-options" data-pg-database-options></div><p class="muted pg-database-hint">先填写读取账号和密码，点击自动探测后选择一个或多个数据库。</p></div>
    <label>读取账号<input name="reader_username" class="input" maxlength="96" value="${attribute(item.reader_username||'')}"></label>
    <label>读取密码<input name="reader_password" class="input" type="password" maxlength="1024" autocomplete="new-password" placeholder="${item.has_reader_password?'留空保持原密码':'新账号需要填写'}"></label>
    <label>写入账号<input name="writer_username" class="input" maxlength="96" value="${attribute(item.writer_username||'')}"></label>
    <label>写入密码<input name="writer_password" class="input" type="password" maxlength="1024" autocomplete="new-password" placeholder="${item.has_writer_password?'留空保持原密码':'按需填写'}"></label>
  </div><p class="muted" data-pg-note hidden>PostgreSQL 按选中的数据库分别建立连接。PostgreSQL 15 可作为 Source 或 Sink。</p>`;
}
function bindInstanceKind(form,item={}) {
  const kind=form.elements.kind,version=form.elements.version,port=form.elements.port;
  kind.value=item.kind||'mysql';
  let previous=kind.value;
  function update(initial=false) {
    const pg=kind.value==='postgresql';
    version.replaceChildren(...(pg?['15']:['5.7','8.0','8.4']).map(value=>{const option=node('option','',value);option.value=value;return option;}));
    if(initial)version.value=item.version||(pg?'15':'5.7');
    if(!initial&&Number(port.value)===(previous==='postgresql'?5432:3306))port.value=pg?5432:3306;
    form.querySelector('[data-pg-database]').hidden=!pg;
    form.querySelector('[data-pg-note]').hidden=!pg;
    form.elements.reader_username.placeholder=pg?'postgresql_reader':'mysql_reader';
    form.elements.writer_username.placeholder=pg?'postgresql_writer':'mysql_writer';
    previous=kind.value;
  }
  update(true);kind.addEventListener('change',()=>update());
}
function attribute(value){return String(value).replace(/[&"<>]/g,c=>({'&':'&amp;','"':'&quot;','<':'&lt;','>':'&gt;'}[c]));}
async function renderAdd(){
  setTitle('添加实例');const parts=location.pathname.split('/'),id=parts.length===4?decodeURIComponent(parts[2]):null,item=id?state.instances.find(value=>value.id===id):null;const main=$('#content');main.replaceChildren(title(item?'编辑数据库实例':'添加数据库实例','读取账号用于元信息与变更日志；写入账号用于目的端执行'));
  if(state.me.user.role!=='admin'){main.append(empty('当前账号只有查看权限'));return;}
  if(id&&!item){main.append(empty('实例不存在'));return;}const form=node('form','panel editor-form');form.innerHTML=instanceFields(item||{});bindInstanceKind(form,item||{});const actions=node('div','editor-actions'),save=node('button','btn primary',item?'保存修改':'保存实例');save.type='submit';actions.append(save);if(item){const remove=node('button','btn danger','删除实例');remove.type='button';remove.dataset.delete=item.id;actions.append(remove);}form.append(actions);main.append(form);
  const databaseOptions=form.querySelector('[data-pg-database-options]');
  const selectedDatabases=new Set((item?.databases||((item?.database&&[item.database])||[])));
  function renderDatabaseOptions(values) {
    databaseOptions.replaceChildren();
    values.forEach(value=>{const label=node('label','pg-database-option');const checkbox=node('input');checkbox.type='checkbox';checkbox.name='databases';checkbox.value=value;checkbox.checked=selectedDatabases.has(value);label.append(checkbox,document.createTextNode(value));databaseOptions.append(label);});
    if(!values.length)databaseOptions.append(node('span','muted','尚未探测数据库'));
  }
  renderDatabaseOptions([...selectedDatabases]);
  const discover=form.querySelector('[data-discover-databases]');
  discover.addEventListener('click',async()=>{const data={instance_id:item?.id||null,host:form.elements.host.value,port:Number(form.elements.port.value),kind:form.elements.kind.value,version:form.elements.version.value,reader_username:form.elements.reader_username.value,reader_password:form.elements.reader_password.value};try{discover.disabled=true;discover.textContent='探测中…';const databases=await api('/api/postgresql/databases',{method:'POST',body:JSON.stringify(data)});selectedDatabases.clear();databases.forEach(value=>selectedDatabases.add(value));renderDatabaseOptions(databases);toast('已探测 '+databases.length+' 个可连接数据库');}catch(reason){toast(reason.message);}finally{discover.disabled=false;discover.textContent='自动探测数据库';}});
  form.addEventListener('submit',async event=>{event.preventDefault();const data=Object.fromEntries(new FormData(form));data.port=Number(data.port);data.databases=[...form.querySelectorAll('input[name=databases]:checked')].map(input=>input.value);data.database=data.databases[0]||'';if(data.kind==='mysql'){data.database='';data.databases=[];}for(const key of ['reader_password','writer_password'])if(!data[key])data[key]=null;try{await api(item?'/api/instances/'+encodeURIComponent(item.id):'/api/instances',{method:item?'PUT':'POST',body:JSON.stringify(data)});await loadInstances();toast('实例配置已保存');location.assign('/');}catch(reason){toast(reason.message);}});
}
async function renderSettings(){
  setTitle('设置');const main=$('#content');main.replaceChildren(title('设置','当前账号、显示偏好和账号管理'));
  const grid=node('div','settings'),dialogs=[],account=node('article','setting panel');account.append(node('h2','','当前账号'),node('b','',state.me.user.username),node('p','muted',roleName(state.me.user.role)));const appearance=node('article','setting panel');appearance.append(node('h2','','外观'));['light','dark'].forEach(value=>{const button=node('button','theme '+(state.me.user.theme===value?'active':''),value==='dark'?'深色':'浅色');button.dataset.theme=value;appearance.append(button);});grid.append(account,appearance);
  if(state.me.user.role==='admin'){
    const users=node('article','setting panel wide account-management'),heading=node('div','setting-card-heading'),add=node('button','btn primary','添加账号');add.type='button';add.dataset.userAdd='';heading.append(node('h2','','账号管理'),add);state.users=await api('/api/users');
    const table=node('div','account-table'),head=node('div','account-head');['所有者','账号','角色','备注','操作'].forEach(label=>head.append(node('span','',label)));table.append(head);
    state.users.forEach(user=>{const row=node('div','account-row'),note=node('span','account-note',user.note||'—');note.title=user.note||'';row.append(node('span','',user.owner),node('b','',user.username),node('span','',roleName(user.role)),note);const actions=node('span','account-actions'),edit=node('button','btn','修改'),remove=node('button','btn danger','删除');edit.dataset.userEdit=user.id;remove.dataset.userDelete=user.id;actions.append(edit,remove);row.append(actions);table.append(row);});users.append(heading,table);grid.append(users);
    const createDialog=node('dialog','account-dialog account-create-dialog');createDialog.innerHTML='<form class="account-create-form"><div class="dialog-heading"><div><h2>添加账号</h2><p class="muted">填写登录账号的信息和权限</p></div><button type="button" class="icon" data-dialog-close aria-label="关闭"><i class="ph ph-x"></i></button></div><label>所有者<input name="owner" class="input" maxlength="128" required></label><label>账号<input name="username" class="input" minlength="3" maxlength="64" pattern="[A-Za-z0-9_.-]{3,64}" autocomplete="username" required></label><label>密码<input name="password" class="input" type="password" minlength="12" maxlength="128" autocomplete="new-password" required></label><label>角色<select name="role" class="select"><option value="viewer">普通账号</option><option value="admin">管理员</option></select></label><label>备注（可留空）<input name="note" class="input" maxlength="500"></label><div class="dialog-actions"><button type="button" class="btn" data-dialog-close>取消</button><button class="btn primary">确定</button></div></form>';dialogs.push(createDialog);
    const editDialog=node('dialog','account-dialog account-edit-dialog');editDialog.innerHTML='<form class="user-edit-form"><div class="dialog-heading"><div><h2>修改账号</h2><p class="muted">账号名称创建后不能修改</p></div><button type="button" class="icon" data-dialog-close aria-label="关闭"><i class="ph ph-x"></i></button></div><label>所有者<input name="owner" class="input" maxlength="128" required></label><label>账号<input name="username" class="input" disabled></label><label>新密码<input name="password" class="input" type="password" minlength="12" maxlength="128" autocomplete="new-password" placeholder="留空表示不修改密码"></label><label>备注（可留空）<input name="note" class="input" maxlength="500"></label><div class="dialog-actions"><button type="button" class="btn" data-dialog-close>取消</button><button class="btn primary">保存修改</button></div></form>';dialogs.push(editDialog);
  }
  const version=node('article','setting panel wide');version.append(node('h2','','平台'),node('p','mono','v'+state.me.version+' · SQLite 控制数据'),node('p','muted','任务运行服务：'+(state.me.task_service_connected?'已连接':'尚未接入')));grid.append(version);main.append(grid,...dialogs);
}
async function loadInstances(){[state.instances,state.connectors]=await Promise.all([api('/api/instances'),api('/api/connectors')]);$('#top-status').textContent=state.instances.length+' 个实例';}
async function route(){await loadInstances();const path=location.pathname;document.querySelectorAll('.nav[data-path]').forEach(link=>link.classList.toggle('active',link.dataset.path===path||(path.startsWith('/tasks')&&link.dataset.path==='/tasks')));if(path==='/')renderHome();else if(path.startsWith('/instances/'))await renderAdd();else if(path==='/add')await renderTaskAdd();else if(path==='/settings')await renderSettings();else if(path.startsWith('/tasks/'))await renderTaskDetail(decodeURIComponent(path.slice(7)));else await renderTasks();}
document.addEventListener('click',async event=>{
  const probe=event.target.closest('[data-probe]');if(probe){event.preventDefault();probe.disabled=true;try{const result=await api('/api/instances/'+encodeURIComponent(probe.dataset.probe)+'/probe',{method:'POST'});await loadInstances();renderHome();toast(result.probe_error||'实例元信息已更新');}catch(reason){await loadInstances();renderHome();toast(reason.message);}return;}
  const remove=event.target.closest('[data-delete]');if(remove){if(confirm('确定删除这个实例配置？'))try{await api('/api/instances/'+encodeURIComponent(remove.dataset.delete),{method:'DELETE'});await loadInstances();location.assign('/');}catch(reason){toast(reason.message);}return;}
  const userAdd=event.target.closest('[data-user-add]');if(userAdd){const dialog=$('.account-create-dialog'),form=dialog?.querySelector('form');if(!dialog||!form)return;form.reset();form.querySelector('[name=owner]').value=state.me.user.username;dialog.showModal();form.querySelector('[name=owner]').select();return;}
  const userEdit=event.target.closest('[data-user-edit]');if(userEdit){const user=state.users.find(item=>item.id===Number(userEdit.dataset.userEdit)),dialog=$('.account-edit-dialog');if(!user||!dialog)return;dialog.dataset.userId=user.id;dialog.querySelector('[name=owner]').value=user.owner;dialog.querySelector('[name=username]').value=user.username;dialog.querySelector('[name=password]').value='';dialog.querySelector('[name=note]').value=user.note||'';dialog.showModal();return;}
  const dialogClose=event.target.closest('[data-dialog-close]');if(dialogClose){dialogClose.closest('dialog').close();return;}
  const userDelete=event.target.closest('[data-user-delete]');if(userDelete){try{await api('/api/users/'+userDelete.dataset.userDelete,{method:'DELETE'});toast('账号已删除');await renderSettings();}catch(reason){toast(reason.message);}return;}
  const theme=event.target.closest('[data-theme]');if(theme){try{state.me.user=await api('/api/me/theme',{method:'POST',body:JSON.stringify({theme:theme.dataset.theme})});document.body.classList.toggle('dark',theme.dataset.theme==='dark');await renderSettings();}catch(reason){toast(reason.message);}return;}
});
document.addEventListener('submit',async event=>{if(event.target.matches('.account-create-form')){event.preventDefault();const dialog=event.target.closest('dialog'),data=Object.fromEntries(new FormData(event.target));try{await api('/api/users',{method:'POST',body:JSON.stringify(data)});dialog.close();toast('账号已添加');await renderSettings();}catch(reason){toast(reason.message);}return;}if(!event.target.matches('.user-edit-form'))return;event.preventDefault();const dialog=event.target.closest('dialog'),data=Object.fromEntries(new FormData(event.target));if(!data.password)data.password=null;const id=Number(dialog.dataset.userId),changedOwnPassword=id===state.me.user.id&&data.password;try{await api('/api/users/'+id,{method:'PUT',body:JSON.stringify({owner:data.owner,password:data.password,note:data.note})});if(changedOwnPassword){location.replace('/login?password_changed=1');return;}dialog.close();toast('账号已修改');await renderSettings();}catch(reason){toast(reason.message);}});
$('#logout').addEventListener('click',async()=>{try{await api('/api/auth/logout',{method:'POST'});}finally{location.replace('/login');}});
$('#theme-toggle').addEventListener('click',async()=>{const value=document.body.classList.contains('dark')?'light':'dark';try{state.me.user=await api('/api/me/theme',{method:'POST',body:JSON.stringify({theme:value})});document.body.classList.toggle('dark',value==='dark');}catch(reason){toast(reason.message);}});
$('#menu').addEventListener('click',()=>$('#sidebar').classList.toggle('open'));
(async()=>{state.me=await api('/api/me');state.csrf=state.me.csrf_token;$('#current-user').textContent=state.me.user.username;document.body.classList.toggle('dark',state.me.user.theme==='dark');await route();})().catch(reason=>toast(reason.message));
