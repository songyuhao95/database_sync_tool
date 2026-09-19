'use strict';
const $ = selector => document.querySelector(selector);
const $$ = selector => [...document.querySelectorAll(selector)];
const icon = name => '<i class="ph ph-' + name + '" aria-hidden="true"></i>';
const instances = {
  source: {name:'mysql-source-57', version:'5.7.44', host:'192.168.0.10:33061', state:'online', logBin:'ON', format:'ROW', rowImage:'FULL', gtid:'ON'},
  target80: {name:'mysql-target-80', version:'8.0', host:'192.168.0.10:33062', state:'online', logBin:'ON', format:'ROW', rowImage:'FULL', gtid:'ON'},
  target84: {name:'mysql-target-84', version:'8.4', host:'192.168.0.10:33063', state:'online', logBin:'ON', format:'ROW', rowImage:'FULL', gtid:'ON'}
};
// All records are local examples. Database roles belong to routes, not instances.
const routes = [
  {id:'task_orders_prod', name:'订单实时同步', source:'source', sink:'target84', state:'running', lag:320, mode:'GTID', cursor:'mysql-bin.000003:5914', tables:['orders','order_items','payments','refunds'], logs:[
    {time:'14:28:30.318', id:'430c326c:18294', kind:'transaction', sql:12, result:'已提交'},
    {time:'14:28:21.807', id:'430c326c:18293', kind:'transaction', sql:4, result:'已提交'},
    {time:'14:27:58.042', id:'430c326c:18291', kind:'error', sql:2, result:'已回滚', message:'目的端连接中断，已回滚当前事务'},
    {time:'14:27:59.100', id:'连接恢复', kind:'runtime', sql:0, result:'已恢复'}]},
  {id:'task_users', name:'用户中心同步', source:'source', sink:'target80', state:'running', lag:86, mode:'GTID', cursor:'mysql-bin.000003:6208', tables:['users','user_profiles'], logs:[
    {time:'14:28:27.211', id:'430c326c:19006', kind:'transaction', sql:8, result:'已提交'},
    {time:'14:28:18.099', id:'430c326c:19005', kind:'transaction', sql:3, result:'已提交'},
    {time:'14:26:20.013', id:'采集连接就绪', kind:'runtime', sql:0, result:'运行中'}]},
  {id:'task_inventory', name:'库存归档', source:'target80', sink:'target84', state:'paused', lag:null, mode:'文件 + position', cursor:'binlog.000009:8421', tables:['inventory','stock_movements'], logs:[
    {time:'12:16:40.076', id:'binlog.000009:8421', kind:'transaction', sql:6, result:'已提交'},
    {time:'12:16:41.000', id:'管理员暂停任务', kind:'runtime', sql:0, result:'已暂停'}]},
  {id:'task_finance', name:'财务数据同步', source:'source', sink:null, state:'draft', lag:null, mode:'自动', cursor:null, tables:[], logs:[]}
];
const columns = {
  orders:[['id','BIGINT PK'],['status','VARCHAR(32)'],['amount','DECIMAL(12,2)']],
  order_items:[['id','BIGINT PK'],['order_id','BIGINT'],['quantity','INT']],
  payments:[['id','BIGINT PK'],['order_id','BIGINT'],['amount','DECIMAL(12,2)']],
  refunds:[['id','BIGINT PK'],['payment_id','BIGINT'],['amount','DECIMAL(12,2)']],
  users:[['id','BIGINT PK'],['name','VARCHAR(100)'],['email','VARCHAR(255)']],
  user_profiles:[['id','BIGINT PK'],['user_id','BIGINT'],['bio','TEXT']],
  inventory:[['id','BIGINT PK'],['sku','VARCHAR(64)'],['quantity','INT']],
  stock_movements:[['id','BIGINT PK'],['sku','VARCHAR(64)'],['delta','INT']]
};
const names = {home:'数据库总览',tasks:'同步任务',detail:'任务详情',add:'添加任务',settings:'设置'};
let currentRoute = null;
let selectedInstance = 'source';
let selectedEndpoint = 'source';
let selectedTable = null;
let logKind = 'transaction';
let instanceTaskFilter = '';
let auth = sessionStorage.getItem('cdc-auth') === 'yes';
const associated = id => routes.filter(route => route.state !== 'draft' && (route.source === id || route.sink === id));
const status = state => {
  const labels = {online:['good','check-circle','在线'],running:['good','play-circle','运行中'],paused:['warn','pause-circle','已暂停'],draft:['muted','pencil-simple','待配置'],offline:['danger','warning-circle','离线']};
  const [color,glyph,label] = labels[state];
  return '<span class="status '+color+'">'+icon(glyph)+label+'</span>';
};
function toast(message) {
  $('#toast').textContent = message;
  $('#toast').classList.add('show');
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => $('#toast').classList.remove('show'), 2400);
}
function refreshSummary() {
  const active = routes.filter(route=>route.state==='running').length;
  const paused = routes.filter(route=>route.state==='paused').length;
  const online = Object.values(instances).filter(instance=>instance.state==='online').length;
  $('#global-status').innerHTML = '<span class="good">'+icon('check-circle')+online+'/3 实例在线</span><span>'+icon('link')+active+' 个任务运行</span>';
  $('#overview-summary').innerHTML = '<span><b>'+online+'</b> / 3 个实例在线</span><span><b>'+active+'</b> 运行中</span><span><b>'+paused+'</b> 已暂停</span><span class="muted">数据范围：全部已管理实例</span>';
}
function roleSummary(id) {
  const own = associated(id);
  return [own.some(route=>route.source===id)?'源端':'',own.some(route=>route.sink===id)?'目的端':''].filter(Boolean).join(' / ') || '未分配';
}
function renderInstances() {
  const q = $('#instance-search').value.trim().toLowerCase();
  const state = $('#instance-filter').value;
  const visible = Object.entries(instances).filter(([id,d])=>(state==='all'||d.state===state) && [d.name,d.host,d.version,roleSummary(id)].join(' ').toLowerCase().includes(q));
  $('#instance-rows').innerHTML = visible.map(([id,d])=>'<tr data-instance="'+id+'" class="'+(id===selectedInstance?'selected':'')+'"><td><button class="instance-title" data-instance="'+id+'" aria-pressed="'+(id===selectedInstance)+'">'+icon('database')+'<span><b>'+d.name+'</b><small class="mono">'+d.host+'</small></span></button></td><td class="mono">'+d.version+'</td><td>'+status(d.state)+'</td><td><span class="config-value">'+d.format+' / '+d.rowImage+'</span><small class="muted">GTID '+d.gtid+'</small></td><td>'+roleSummary(id)+'</td><td><button class="text-button" data-instance-tasks="'+id+'">'+associated(id).length+' 个'+icon('arrow-up-right')+'</button></td></tr>').join('') || '<tr><td colspan="6" class="empty">没有匹配的实例，请调整搜索条件。</td></tr>';
  renderInstanceInspector();
  $('#route-overview').innerHTML = routes.filter(route=>route.state!=='draft').map(route=>'<button class="route-summary-row" data-route="'+route.id+'"><span><b>'+route.name+'</b><small>'+route.tables.length+' 张表 / CDC_test</small></span><span class="route-path">'+instances[route.source].name+icon('arrow-right')+instances[route.sink].name+'</span>'+status(route.state)+icon('caret-right')+'</button>').join('');
}
function metadata(d) {
  return '<dl class="metadata"><div><dt>版本</dt><dd>MySQL '+d.version+'</dd></div><div><dt>log_bin</dt><dd>'+d.logBin+'</dd></div><div><dt>binlog_format</dt><dd>'+d.format+'</dd></div><div><dt>binlog_row_image</dt><dd>'+d.rowImage+'</dd></div><div><dt>GTID</dt><dd>'+d.gtid+'</dd></div></dl>';
}
function renderInstanceInspector() {
  const d=instances[selectedInstance], own=associated(selectedInstance);
  $('#instance-inspector').innerHTML = '<h2>实例详情</h2><div class="inspector-identity">'+icon('database')+'<div><h3>'+d.name+'</h3><span class="mono">'+d.host+'</span>'+status(d.state)+'</div></div>'+metadata(d)+'<div class="section-heading"><h3>关联任务</h3><span class="muted">'+own.length+' 个</span></div><div class="associated-routes">'+own.map(route=>'<button data-route="'+route.id+'"><span><b>'+route.name+'</b><small>作为'+(route.source===selectedInstance?'源端':'目的端')+'</small></span>'+icon('arrow-up-right')+'</button>').join('')+'</div>';
}
function renderTasks() {
  const q=$('#search').value.toLowerCase(), filter=$('#filter').value;
  const visible=routes.filter(route=> (filter==='all'||route.state===filter) && (!instanceTaskFilter || (route.state!=='draft'&&(route.source===instanceTaskFilter||route.sink===instanceTaskFilter))) &&
    [route.id,route.name,instances[route.source].name,instances[route.source].host,route.sink?instances[route.sink].name:'',route.sink?instances[route.sink].host:'','CDC_test',...route.tables].join(' ').toLowerCase().includes(q));
  $('#clear-task-filter').classList.toggle('hidden',!instanceTaskFilter);
  $('#task-filter-context').classList.toggle('hidden',!instanceTaskFilter);
  $('#task-filter-context').textContent=instanceTaskFilter?'关联实例：'+instances[instanceTaskFilter].name:'';
  $('#task-rows').innerHTML=visible.map(route=>'<tr data-route="'+route.id+'"><td><button class="text-button task-title" data-route="'+route.id+'">'+route.name+'</button><small class="mono">'+route.id+'</small></td><td>'+instances[route.source].name+'<small class="mono">'+instances[route.source].host+'</small></td><td>'+(route.sink?instances[route.sink].name+'<small class="mono">'+instances[route.sink].host+'</small>':'未选择')+'</td><td>'+(route.tables.length?'CDC_test<small>'+route.tables.length+' 张表</small>':'待配置')+'</td><td>'+status(route.state)+'</td><td class="mono">'+(route.state==='running'?route.lag+' ms':'-')+'</td></tr>').join('')||'<tr><td colspan="6" class="empty">没有匹配的任务，请调整筛选条件。</td></tr>';
}
function renderEndpoint(role) {
  const id=currentRoute[role],d=instances[id];
  return '<button class="endpoint '+(selectedEndpoint===role?'selected':'')+'" data-endpoint="'+role+'" aria-pressed="'+(selectedEndpoint===role)+'"><span class="endpoint-role">'+(role==='source'?'源端':'目的端')+'</span><div class="endpoint-identity">'+icon('database')+'<span><b>'+d.name+'</b><small>MySQL '+d.version+'</small></span></div><span class="mono endpoint-address">'+d.host+'</span><span class="endpoint-bottom"><span>CDC_test</span>'+status(d.state)+'</span></button>';
}
function renderDetail() {
  if (!currentRoute) return;
  const r=currentRoute;
  $('#detail-content').innerHTML='<div class="detail-toolbar"><button class="text-button" data-page="tasks">'+icon('arrow-left')+'返回任务列表</button><label for="task-switch">切换任务</label><select id="task-switch" class="select">'+routes.map(route=>'<option value="'+route.id+'" '+(route.id===r.id?'selected':'')+'>'+route.name+'</option>').join('')+'</select></div>'+
    '<div class="title-row detail-title"><div><div class="heading-with-status"><h1>'+r.name+'</h1>'+status(r.state)+'</div><p class="mono">'+r.id+'</p></div><div class="actions">'+(r.state!=='draft'?'<button id="pause-route" class="btn">'+icon(r.state==='running'?'pause':'play')+(r.state==='running'?'暂停任务':'恢复任务')+'</button>':'')+'<button class="btn" data-refresh>'+icon('arrows-clockwise')+'刷新</button></div></div>'+
    (r.sink?'<div class="detail-layout"><div class="detail-main"><section class="route-topology panel" aria-label="当前任务同步拓扑"><div class="section-heading"><h2>任务拓扑</h2><span class="muted">仅当前任务</span></div><div class="endpoint-layout" id="endpoint-layout">'+renderEndpoint('source')+'<div class="route-connector '+(r.state==='paused'?'paused':'')+'"><strong>'+(r.state==='running'?r.lag+' ms':'已暂停')+'</strong><span class="flow-direction">'+icon('arrow-right')+'</span><small>'+r.mode+'</small></div>'+renderEndpoint('sink')+'</div></section><section class="mapping-section"><div class="section-heading"><h2>库表映射</h2><span class="muted">'+r.tables.length+' 张表 / 同名映射</span></div><div id="mapping-trees" class="mapping-layout"></div></section></div><aside id="detail-inspector" class="inspector" aria-label="任务中的实例详情"></aside></div><section class="panel write-log"><div class="section-heading"><h2>写入日志</h2><span class="muted">仅当前任务，默认隐藏行值</span></div><div class="tabs" role="tablist" aria-label="日志类型">'+[['transaction','事务日志'],['runtime','运行事件'],['error','错误']].map(([id,label])=>'<button id="tab-'+id+'" role="tab" aria-controls="log-content" aria-selected="'+(logKind===id)+'" data-log-kind="'+id+'" class="tab '+(logKind===id?'active':'')+'">'+label+'</button>').join('')+'</div><div id="log-content" role="tabpanel"></div></section>':
    '<div class="panel empty"><h2>这条任务尚未配置完成</h2><p>请选择目的实例和同步表后再创建链路。</p><button class="btn" data-page="add">继续配置</button></div>');
  if(r.sink){renderDetailInspector();renderMappings();renderLogs();}
}
function renderDetailInspector(){
  const r=currentRoute,d=instances[r[selectedEndpoint]];
  $('#detail-inspector').innerHTML='<h2>'+(selectedEndpoint==='source'?'源端':'目的端')+'实例</h2><h3>'+d.name+'</h3><p class="mono">'+d.host+'</p>'+metadata(d)+'<div class="section-heading"><h3>当前任务</h3></div><dl class="metadata"><div><dt>起点模式</dt><dd>'+r.mode+'</dd></div><div><dt>同步表</dt><dd>'+r.tables.length+'</dd></div></dl><p class="muted cursor-label">最近提交点位</p><code class="cursor">'+r.cursor+'</code>';
}
function renderMappings(){
  $('#mapping-trees').innerHTML=['source','sink'].map(role=>'<div class="panel"><div class="section-heading"><h3>'+(role==='source'?'源端':'目的端')+'库表</h3><span class="muted">MySQL '+instances[currentRoute[role]].version+'</span></div><div class="tree"><h3>'+icon('database')+'CDC_test</h3>'+currentRoute.tables.map(table=>'<button class="tree-row '+(selectedTable===table?'selected':'')+'" data-table="'+table+'" aria-pressed="'+(selectedTable===table)+'">'+icon('table')+table+'<small>'+(selectedTable===table?'已选中':'同名映射')+'</small></button>'+(selectedTable===table?'<div class="columns">'+columns[table].map(([name,type])=>'<div><span>'+name+'</span><code>'+type+'</code></div>').join('')+'</div>':'')).join('')+'</div></div>').join('');
}
function renderLogs(){
  const rows=currentRoute.logs.filter(log=>log.kind===logKind);
  $$('[data-log-kind]').forEach(el=>el.tabIndex=el.dataset.logKind===logKind?0:-1);
  $('#log-content').setAttribute('aria-labelledby','tab-'+logKind);
  $('#log-content').innerHTML=rows.length?'<div class="table-wrap"><table class="log-table"><thead><tr><th>时间</th><th>事务 / 事件</th><th>目的端版本</th><th>SQL 数</th><th>结果</th></tr></thead><tbody>'+rows.map(log=>'<tr><td class="mono">'+log.time+'</td><td><span class="mono">'+log.id+'</span>'+(log.message?'<small>'+log.message+'</small>':'')+'</td><td>MySQL '+instances[currentRoute.sink].version+'</td><td class="mono">'+log.sql+'</td><td><span class="'+(log.kind==='error'?'danger':'good')+'">'+log.result+'</span></td></tr>').join('')+'</tbody></table></div>':'<div class="empty">当前任务没有'+({transaction:'事务日志',runtime:'运行事件',error:'错误日志'}[logKind])+'。</div>';
}
function navigate(path){
  if(location.hash.slice(1)===path) renderLocation(); else location.hash=path;
}
function renderLocation(){
  if(!auth){$('#login').classList.remove('hidden');$('#app').classList.add('hidden');return;}
  $('#login').classList.add('hidden');$('#app').classList.remove('hidden');
  const [part,id]=location.hash.slice(1).split('/'),page=names[part]?part:'home';
  $$('.page').forEach(el=>el.classList.toggle('active',el.id==='page-'+page));
  $$('.nav[data-page]').forEach(el=>{const active=el.dataset.page===page||(page==='detail'&&el.dataset.page==='tasks');el.classList.toggle('active',active);if(active)el.setAttribute('aria-current','page');else el.removeAttribute('aria-current');});
  $('#top-title').textContent=names[page];
  $('#sidebar').classList.remove('open');$('#menu').setAttribute('aria-expanded','false');
  if(page==='detail'){
    currentRoute=routes.find(route=>route.id===id)||null;
    if(currentRoute){selectedEndpoint='source';selectedTable=currentRoute.tables[0]||null;logKind='transaction';renderDetail();}
    else $('#detail-content').innerHTML='<div class="empty panel"><h1>未找到任务</h1><p>请从任务列表选择需要查看的任务。</p><button class="btn" data-page="tasks">返回任务列表</button></div>';
  }
  if(page==='tasks')renderTasks();
  if(page==='home')renderInstances();
}
function theme(value){
  const dark=value==='dark';document.body.classList.toggle('dark',dark);localStorage.setItem('cdc-theme',value);
  $$('.theme').forEach(el=>{el.classList.toggle('active',el.dataset.theme===value);el.setAttribute('aria-pressed',String(el.dataset.theme===value));});
  $('#theme-toggle').setAttribute('aria-label',dark?'切换浅色主题':'切换深色主题');
}
function updateMappingPreview(){
  const tables=$$('.map-check:checked').map(el=>el.value);
  $('#count').textContent=tables.length;
  $('#add-target-tables').innerHTML=tables.map(table=>'<div class="tree-row selected">'+icon('table')+table+'<small>类型兼容</small></div>').join('')||'<p class="empty">请先选择源端表。</p>';
  $('#add-summary').textContent='MySQL 5.7 → MySQL '+instances[$('#add-sink').value].version+' / GTID 自动起点';
}
$('#login-form').addEventListener('submit',event=>{
  event.preventDefault();
  if($('#user').value==='admin'&&$('#password').value==='cdc-demo'){
    auth=true;sessionStorage.setItem('cdc-auth','yes');$('#login-error').textContent='';$('#password').value='';renderLocation();
  }else $('#login-error').textContent='账号或密码错误，请重试。';
});
$('#logout').onclick=()=>{auth=false;sessionStorage.removeItem('cdc-auth');$('#password').value='';renderLocation();};
$('#theme-toggle').onclick=()=>theme(document.body.classList.contains('dark')?'light':'dark');
$('#menu').onclick=()=>{const open=$('#sidebar').classList.toggle('open');$('#menu').setAttribute('aria-expanded',String(open));};
$('#instance-search').oninput=renderInstances;$('#instance-filter').onchange=renderInstances;
$('#search').oninput=renderTasks;$('#filter').onchange=renderTasks;
$('#clear-task-filter').onclick=()=>{instanceTaskFilter='';renderTasks();};
$('#add-sink').onchange=updateMappingPreview;
$$('.map-check').forEach(el=>el.onchange=updateMappingPreview);
document.addEventListener('change',event=>{if(event.target.id==='task-switch')navigate('detail/'+event.target.value);});
document.addEventListener('keydown',event=>{
  if(!event.target.matches('[data-log-kind]'))return;
  const tabs=$$('[data-log-kind]'),index=tabs.indexOf(event.target);
  const next={ArrowRight:(index+1)%tabs.length,ArrowLeft:(index+tabs.length-1)%tabs.length,Home:0,End:tabs.length-1}[event.key];
  if(next===undefined)return;
  event.preventDefault();tabs[next].click();tabs[next].focus();
});
document.addEventListener('click',event=>{
  const target=event.target.closest('button,[data-instance],[data-route]'); if(!target)return;
  if(target.dataset.instanceTasks){instanceTaskFilter=target.dataset.instanceTasks;$('#filter').value='all';$('#search').value='';navigate('tasks');return;}
  if(target.dataset.route){navigate('detail/'+target.dataset.route);return;}
  if(target.dataset.instance){selectedInstance=target.dataset.instance;renderInstances();return;}
  if(target.dataset.page){if(target.dataset.page==='tasks')instanceTaskFilter='';navigate(target.dataset.page);return;}
  if(target.dataset.theme){theme(target.dataset.theme);return;}
  if(target.dataset.endpoint&&currentRoute){
    selectedEndpoint=target.dataset.endpoint;
    $$('[data-endpoint]').forEach(el=>{const selected=el.dataset.endpoint===selectedEndpoint;el.classList.toggle('selected',selected);el.setAttribute('aria-pressed',String(selected));});
    renderDetailInspector();return;
  }
  if(target.dataset.table){selectedTable=target.dataset.table;renderMappings();return;}
  if(target.dataset.logKind){logKind=target.dataset.logKind;$$('[data-log-kind]').forEach(el=>{el.classList.toggle('active',el.dataset.logKind===logKind);el.setAttribute('aria-selected',String(el.dataset.logKind===logKind));});renderLogs();return;}
  if(target.id==='pause-route'){
    currentRoute.state=currentRoute.state==='running'?'paused':'running';
    if(currentRoute.state==='running'&&currentRoute.lag===null)currentRoute.lag=0;
    currentRoute.logs.push({time:new Date().toLocaleTimeString('en-GB'),id:'样稿操作',kind:'runtime',sql:0,result:currentRoute.state==='running'?'已恢复':'已暂停'});
    refreshSummary();renderDetail();toast('已在样稿中'+(currentRoute.state==='running'?'恢复':'暂停')+'此任务');return;
  }
  if(target.hasAttribute('data-refresh')){refreshSummary();if(currentRoute&&$('#page-detail').classList.contains('active'))renderDetail();else renderInstances();toast('已刷新当前模拟数据');return;}
  if(target.hasAttribute('data-placeholder'))toast('此操作尚未接入，本次仅预览页面样式');
});
window.addEventListener('hashchange',renderLocation);
theme(localStorage.getItem('cdc-theme')||'dark');
refreshSummary();renderInstances();renderTasks();updateMappingPreview();renderLocation();
