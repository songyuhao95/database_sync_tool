'use strict';

let taskPollTimer;
function pollTasks(refresh) {
  clearInterval(taskPollTimer);
  const path=location.pathname;let busy=false;
  taskPollTimer=setInterval(async()=>{
    if(location.pathname!==path){clearInterval(taskPollTimer);return;}
    if(busy||document.hidden)return;
    busy=true;try{await refresh();}catch(reason){clearInterval(taskPollTimer);toast(reason.message);}finally{busy=false;}
  },2000);
}
window.addEventListener('pagehide',()=>clearInterval(taskPollTimer));
function taskAction(task,refresh) {
  if(state.me.user.role!=='admin')return null;
  const active=['starting','running','stopping'].includes(task.status);
  const button=node('button',active?'btn':'btn primary',active?'停止':'开始');
  button.type='button';button.disabled=task.status==='stopping'||(!active&&(task.configuration_changed||task.plan_needs_requalification));
  button.addEventListener('click',async()=>{
    button.disabled=true;
    try{await api('/api/tasks/'+encodeURIComponent(task.id)+(active?'/stop':'/start'),{method:'POST'});await refresh();}
    catch(reason){toast(reason.message);button.disabled=false;}
  });return button;
}
function taskRequalifyAction(task,refresh) {
  if(state.me.user.role!=='admin'||['starting','running','stopping'].includes(task.status)||!task.plan_needs_requalification)return null;
  const button=node('button','btn','重新预检');button.type='button';button.title='重新读取源/目的元数据并生成完整 ColumnConversionPlan';
  button.addEventListener('click',async()=>{
    button.disabled=true;
    try{await api('/api/tasks/'+encodeURIComponent(task.id)+'/requalify',{method:'POST',body:JSON.stringify({confirmations:task.risk_confirmations||[]})});await refresh();}
    catch(reason){toast(reason.message);button.disabled=false;}
  });return button;
}

function taskDeleteAction(task,refresh) {
  if(state.me.user.role!=='admin')return null;
  const button=node('button','btn task-delete','删除');button.type='button';
  button.disabled=['starting','running','stopping'].includes(task.status);
  button.title=button.disabled?'请先停止任务，待停止完成后再删除':'删除任务配置，已同步的数据保留';
  button.setAttribute('aria-label','删除任务 '+task.name);
  button.addEventListener('click',async()=>{
    button.disabled=true;
    try{await api('/api/tasks/'+encodeURIComponent(task.id),{method:'DELETE'});await refresh();}
    catch(reason){toast(reason.message);button.disabled=false;}
  });return button;
}

function taskModeLabel(mode) {
  return {auto:'自动（优先 GTID）',gtid:'GTID',binlog:'文件 + position'}[mode] || mode;
}
function conversionKind(parameters) {
  return parameters.range_kind||parameters.conversion_kind||parameters.value_strategy||(parameters.json_strategy?'json':undefined);
}
function conversionExample(kind,parameters) {
  if(kind==='binary')return '示例：仅按 raw bytes 写入；长度、固定填充和字节边界不满足即拒绝，禁止降级为文本';
  if(kind==='bit_string')return '示例：按声明的 bit 长度、MSB/LSB 顺序和末字节 padding 写入；长度或格式不符即拒绝';
  if(kind==='spatial')return '示例：必须同时匹配 WKB/EWKB、几何类型、维度、SRID 和 CRS；任一元数据不明即阻断';
  if(kind==='recursive')return '示例：仅执行已保存的显式递归结构映射；Array/Struct/Map/Range 结构不符即拒绝';
  if(kind==='integer')return '示例：目标范围内通过；越界值拒绝';
  if(kind==='decimal')return '示例：仅零小数可安全缩放；非零舍入和越界拒绝';
  if(kind==='float')return '示例：仅可精确表示且有限的值通过；溢出或舍入拒绝';
  if(kind==='text')return '示例：严格按目标字符集编码；超长、不可编码字符和排序语义变化拒绝';
  if(kind==='temporal')return '示例：按已保存精度与时区策略转换；会丢失精度时拒绝';
  if(kind==='json'&&parameters.json_strategy==='normalized_text')return '示例转换：结构化 JSON → 规范化文本；保留数组顺序和数字类型，键排序且不保留原始空白；解析失败拒绝';
  if(kind==='json'&&parameters.json_strategy==='raw_text')return '示例转换：原始 JSON 文本不可用；禁止选择或启动';
  if(kind==='json')return '示例转换：结构化 JSON → 目标结构化 JSON；保留数组顺序和数字表示，键按规范顺序输出';
  if(kind==='enum_label')return '示例转换：ENUM 标签 “Ready” → 目标同标签；按 label 转换，禁止按 ordinal';
  if(kind==='set_members')return '示例转换：SET {a,b} → 目标成员集合；顺序归一，未知成员和重复成员拒绝';
  return '示例：按已保存的类型规则执行；格式、能力或结构失败时回滚事务且不推进 checkpoint';
}
function conversionTarget(kind,parameters,nativeType) {
  if(kind==='binary')return '目标结果：raw bytes（不转文本）'+(parameters.target_length?`，目标长度 ${parameters.target_length} bytes`:'' );
  if(kind==='bit_string')return '目标结果：bit string（长度 '+(parameters.target_bit_length||parameters.target_length||'未知')+' bits，'+(parameters.target_bit_order||'未声明')+'，padding '+(parameters.target_padding||'未声明')+'）';
  if(kind==='spatial')return '目标结果：'+(parameters.target_spatial_format||'未声明')+' '+(parameters.target_geometry_type||'未声明')+'，'+(parameters.target_dimensions||'未声明')+'D，SRID '+(parameters.target_srid||'未声明')+'，CRS '+(parameters.target_crs||'未声明');
  if(kind==='recursive')return '目标结果：显式 '+(parameters.recursive_kind||'未声明')+' 结构映射';
  if(kind==='json'&&parameters.json_strategy==='normalized_text')return '目标结果：规范化 UTF-8 文本（原始键顺序、空白和重复键不保留）';
  if(kind==='json'&&parameters.json_strategy==='raw_text')return '目标结果：不可用，必须阻止启动';
  if(kind==='json')return '目标结果：结构化 JSON/JSONB';
  if(kind==='enum_label')return '目标结果：目标 ENUM 的同名标签';
  if(kind==='set_members')return '目标结果：目标 SET 的成员集合（按目标声明顺序编码）';
  return '目标结果：'+(nativeType||'按目标类型');
}
function compatibilityFieldType(field) {
  return field?.column_type||field?.native_type||'未识别类型';
}
function compatibilityFieldCollation(field) {
  return field?.collation||field?.logical_type?.collation||'';
}
function compatibilityNormalizedNativeType(value) {
  const normalized=String(value||'').trim().toLowerCase().replace(/\s+/g,' ');
  return normalized
    .replace(/\b(tinyint|smallint|mediumint|int|integer|bigint)\(\d+\)/g,'$1')
    .replace(/\binteger\b/g,'int');
}
function compatibilityRiskLabel(value) {
  return {NONE:'无',LOW:'低',MEDIUM:'中',HIGH:'高',CRITICAL:'严重'}[String(value||'').toUpperCase()]||value||'未评估';
}
function compatibilityRuleKind(parameters) {
  const kind=conversionKind(parameters||{});
  return {enum_label:'ENUM 按标签',set_members:'SET 按成员集合',bit_string:'BIT 按位串',binary:'二进制按原始字节',text:'文本编码转换',json:'JSON 结构转换',integer:'整数范围转换',decimal:'小数精度转换',float:'浮点转换',temporal:'时间精度/时区转换'}[kind]||'按目标字段规则';
}
function compatibilityHumanSummary(source,sink,result,plan,targetOverride) {
  const target=plan?.target||targetOverride||{};
  const parameters=plan?.target?.parameters||targetOverride?.parameters||{};
  const sourceType=compatibilityFieldType(source),targetType=target.native_type||compatibilityFieldType(sink);
  const sourceCollation=parameters.source_collation||compatibilityFieldCollation(source),targetCollation=parameters.target_collation||compatibilityFieldCollation(sink);
  const sourceNormalized=compatibilityNormalizedNativeType(sourceType),targetNormalized=compatibilityNormalizedNativeType(targetType);
  const difference=[];
  if(sourceNormalized!==targetNormalized)difference.push('类型从 '+sourceType+' 变为 '+targetType);
  if(sourceCollation&&targetCollation&&sourceCollation!==targetCollation)difference.push('排序规则从 '+sourceCollation+' 变为 '+targetCollation);
  if(!difference.length)difference.push('源端和目的端字段定义相同，按目标字段直接写入');
  const kind=conversionKind(parameters);
  let conversion='按目标端的 '+compatibilityRuleKind(parameters)+' 规则写入。';
  let risk='不会静默丢弃数据；无法满足目标字段约束时拒绝本次写入。';
  let loss='当前计划没有声明值语义损失。';
  if(kind==='text'){
    const sourceCharset=parameters.source_charset||'源端字符集',targetCharset=parameters.target_charset||'目标字符集';
    conversion='将源端文本按 '+targetCharset+' 编码后写入 '+targetType+'；排序和比较以目标端规则为准。';
    risk='不可编码字符或超出目标长度时拒绝，不截断；排序规则变化可能改变大小写、重音和排序结果。';
    loss=sourceCharset!==targetCharset||sourceCollation!==targetCollation?'字符集或排序比较语义可能变化，原始文本本身不主动截断。':'没有声明字符集或排序语义变化。';
  } else if(kind==='binary'){
    conversion='保留原始字节写入目标二进制字段，不把二进制转成文本。';
    risk='字节长度超过目标上限或固定长度规则不匹配时拒绝。';
    loss='不丢失字节；长度不兼容时整笔事务失败。';
  } else if(kind==='bit_string'){
    conversion='按声明的位数、位顺序和末字节填充写入目标 BIT 字段。';
    risk='位数、填充或格式不符合目标声明时拒绝。';
    loss='不丢失有效位；不会把 BIT 静默转换成整数或文本。';
  } else if(kind==='enum_label'){
    conversion='按 ENUM 标签名称匹配写入，不按内部 ordinal 数字转换。';
    risk='源端出现目的端没有的标签时拒绝；标签顺序变化不会改变标签含义。';
    loss='不丢失已知标签；未知标签不会被改写成其他标签。';
  } else if(kind==='set_members'){
    conversion='按 SET 成员名称组成目标成员集合，写入时按目标声明顺序编码。';
    risk='未知成员、重复成员或格式错误时拒绝。';
    loss='成员集合语义保留，显示顺序可能按目标字段顺序变化。';
  } else if(kind==='integer'){
    conversion='按目标整数的有符号性和取值范围写入。';
    risk='越过目标最小值或最大值时拒绝，不回绕、不截断。';
    loss='计划没有声明数值损失；超范围值会使事务失败。';
  } else if(kind==='decimal'){
    conversion='按目标 DECIMAL 的精度和小数位写入。';
    risk='需要舍入或超出精度时拒绝，不静默四舍五入。';
    loss='计划没有声明小数损失；不满足精度的值会使事务失败。';
  } else if(kind==='float'){
    conversion='按目标浮点宽度写入可精确表示的有限值。';
    risk='溢出、非有限值或不可避免的舍入时拒绝。';
    loss='不把特殊值或不可精确值静默写入目标。';
  } else if(kind==='json'){
    conversion=parameters.json_strategy==='normalized_text'?'把结构化 JSON 转为规范化 UTF-8 文本。':'保持为目标结构化 JSON/JSONB。';
    risk=parameters.json_strategy==='normalized_text'?'解析失败时拒绝，原始空白、键顺序和重复键不保留。':'结构化值不符合目标 JSON 能力时拒绝。';
    loss=parameters.json_strategy==='normalized_text'?'JSON 数据值语义保留，但原始文本格式可能变化。':'当前计划没有声明 JSON 值语义损失。';
  } else if(kind==='temporal'){
    conversion='按已保存的时间精度和时区策略转换后写入。';
    risk='精度无法保留或时区策略缺失时拒绝；显示的本地时间可能变化。';
    loss='时间点或本地时间语义按计划保留，超出目标精度的值不静默舍入。';
  }
  const status=String(result?.status||'').toUpperCase();
  return {difference:difference.join('；'),conversion,risk,loss,status,riskLabel:compatibilityRiskLabel(plan?.risk||result?.risk)};
}
function appendCompatibilitySummary(parent,summary) {
  const section=node('section','compatibility-human-summary');
  section.append(node('h3','','这次转换会发生什么'));
  [['字段差异',summary.difference],['转换方式',summary.conversion],['可能损失或语义变化',summary.loss],['风险和失败条件',summary.risk],['事务行为','失败时整笔事务回滚，checkpoint 不推进。']].forEach(([label,value])=>{
    const row=node('div','compatibility-human-row');row.append(node('b','',label),node('p','',value));section.append(row);
  });
  if(summary.riskLabel)section.append(node('p','compatibility-risk-badge','风险等级：'+summary.riskLabel));
  parent.append(section);
}
function compatibilityOptionLabel(name) {
  return {target_charset:'目标字符集',target_length:'目标长度',target_length_unit:'长度单位',target_collation:'目标排序规则',encoding_policy:'字符编码策略',length_policy:'超长处理',collation_policy:'排序规则策略',target_precision:'目标精度',precision_policy:'精度处理',temporal_strategy:'时间转换策略',time_zone:'时区',json_strategy:'JSON 处理方式'}[name]||name;
}
function compatibilityOptionHelp(name) {
  return {target_charset:'决定文本如何编码写入目标端。',target_length:'必须与预先创建的目标字段长度一致。',target_length_unit:'目标长度按字节还是字符计算。',target_collation:'决定目标端文本比较和排序方式。',encoding_policy:'当前只允许严格编码，无法编码时拒绝。',length_policy:'当前只允许拒绝超长值，禁止静默截断。',collation_policy:'当前按目标端排序规则执行。',target_precision:'目标数值或时间字段保留的精度。',precision_policy:'当前只允许拒绝精度损失。',temporal_strategy:'决定本地时间和绝对时间如何转换。',time_zone:'绝对时间转换使用的时区。',json_strategy:'选择结构化 JSON 或规范化文本。'}[name]||'';
}
function compatibilityOptionAdvanced(name) {
  return ['encoding_policy','length_policy','collation_policy','precision_policy'].includes(name);
}
function registeredConnector(instance,role) {
  if(!instance)return null;
  const list=role==='source'?state.connectors.sources:state.connectors.sinks;
  return list.find(connector=>connector.identity.kind===(instance.kind||'mysql')&&connector.identity.version===instance.version);
}
function taskLink(path,label,className='btn') {
  const link=node('a',className,label);link.href=path;return link;
}
function taskTableReason(table) {
  if(!['innodb','postgresql'].includes(table.engine.toLowerCase()))return '不支持该表类型';
  if(!table.primary_key.length)return '缺少主键';
  if(!table.columns.length)return '无法读取列定义';
  return '';
}
function taskMetadataText(catalog) {
  const m=catalog.metadata;
  if(m.wal_level!==undefined)return 'PostgreSQL '+m.server_version+' · 数据库 '+m.database+' · WAL '+m.wal_level;
  return 'MySQL '+m.server_version+' · '+m.binlog_format+' / '+m.binlog_row_image+' · GTID '+m.gtid_mode;
}
function taskStatus(task) {
  if(task.status==='running')return {key:'running',label:task.runtime?.checkpoint?.phase==='snapshot'?'全量同步中':'增量同步中',tone:'good'};
  const statuses={starting:['启动中','neutral'],running:['同步中','good'],stopping:['停止中','warning'],
    stopped:['已停止','neutral'],failed:['失败','warning']};
  if(statuses[task.status])return {key:task.status,label:statuses[task.status][0],tone:statuses[task.status][1]};
  if(task.configuration_changed||task.plan_needs_requalification)return {key:'changed',label:task.plan_status==='legacy'?'需要重新预检':'需要重新检查',tone:'warning'};
  return {key:'configured',label:'待启动',tone:'neutral'};
}
function taskScope(task) {
  const mappings=Array.isArray(task.mappings)?task.mappings:[];
  const schemas=[...new Set(mappings.map(item=>item.source_schema))];
  const explicitColumns=mappings.every(item=>Array.isArray(item.columns)&&item.columns.length);
  const fieldCount=mappings.reduce((sum,item)=>sum+(item.columns?.length||0),0);
  const database=task.source_database||task.sink_database;
  return {schemas:(database?database+' / ':'')+(schemas.join('、')||'未选择库'),tableCount:mappings.length,fieldCount:explicitColumns?fieldCount:null};
}
function taskSearchText(task) {
  const mappings=Array.isArray(task.mappings)?task.mappings:[];
  return [task.name,task.id,task.source_name,task.sink_name,task.source_database,task.sink_database,task.start_mode,taskModeLabel(task.start_mode),
    ...mappings.flatMap(item=>[item.source_schema,item.source_table,item.sink_schema,item.sink_table,...(item.columns||[])])]
    .filter(Boolean).join(' ').toLocaleLowerCase();
}
function taskListTable(tasks,refresh) {
  const wrap=node('div','table-wrap task-list-table-wrap'),table=node('table','task-list-table');
  const head=node('thead'),header=node('tr');
  ['任务','数据流向','同步范围','读取起点','状态','创建时间','操作'].forEach(label=>header.append(node('th','',label)));head.append(header);
  const body=node('tbody');
  tasks.forEach(task=>{
    const row=node('tr','task-list-row');
    const name=node('td','task-name-cell');
    name.append(taskLink('/tasks/'+encodeURIComponent(task.id),task.name,'text-button'),node('small','muted task-id',task.id.slice(0,12)));

    const route=node('td'),routeLine=node('div','task-route');
    routeLine.append(node('span','task-endpoint',task.source_name),node('span','task-route-arrow','→'),node('span','task-endpoint',task.sink_name));
    route.append(routeLine);

    const scopeInfo=taskScope(task),scope=node('td');
    scope.append(node('span','',scopeInfo.schemas),node('small','muted',scopeInfo.tableCount+' 张表'+(scopeInfo.fieldCount===null?'':' · '+scopeInfo.fieldCount+' 字段')));

    const statusInfo=taskStatus(task),statusCell=node('td');
    statusCell.append(node('span','task-status task-status-'+statusInfo.tone,statusInfo.label));

    const action=node('td','task-list-action');
    action.append(taskLink('/tasks/'+encodeURIComponent(task.id),'查看详情','text-button'));
    const control=taskAction(task,refresh);if(control)action.append(control);
    const remove=taskDeleteAction(task,refresh);if(remove)action.append(remove);

    row.append(name,route,scope,node('td','task-start-mode',taskModeLabel(task.start_mode)),statusCell,
      node('td','task-created-at',new Date(task.created_at*1000).toLocaleString()),action);
    body.append(row);
  });
  table.append(head,body);wrap.append(table);return wrap;
}
async function renderTasks() {
  clearInterval(taskPollTimer);
  setTitle('同步任务');
  const main=$('#content');
  main.replaceChildren(title('同步任务','检索任务配置，查看同步范围和实例流向'));
  let tasks=await api('/api/tasks');
  if(!tasks.length){main.append(empty('还没有同步任务，可从左侧“添加任务”创建'));return;}

  const changedCount=tasks.filter(task=>task.configuration_changed||task.plan_needs_requalification).length;
  const summary=node('div','task-list-summary');
  const total=node('span','task-summary-item');total.append(node('strong','',String(tasks.length)),document.createTextNode(' 个任务'));
  const ready=node('span','task-summary-item');
  const changed=node('span','task-summary-item'+(changedCount?' task-summary-warning':''));changed.append(node('strong','',String(changedCount)),document.createTextNode(' 个需要重新检查'));
  const serviceConnected=state.me.task_service_connected;
  const runtime=node('span','task-runtime-state'+(serviceConnected?' connected':''),serviceConnected?'运行服务已连接':'运行服务尚未接入');
  summary.append(total,ready,changed,runtime);main.append(summary);

  const shell=node('section','panel task-list-shell'),toolbar=node('div','task-list-toolbar');
  const search=node('input','task-search');search.type='search';search.placeholder='搜索任务、实例、库或表';search.setAttribute('aria-label','搜索同步任务');
  const status=node('select','task-status-filter');status.setAttribute('aria-label','按任务状态筛选');
  [['all','全部状态'],['configured','待启动'],['starting','启动中'],['running','同步中'],['stopping','停止中'],['stopped','已停止'],['failed','失败'],['changed','需要重新检查']].forEach(([value,label])=>{
    const option=node('option','',label);option.value=value;status.append(option);
  });
  const result=node('span','muted task-filter-result');
  toolbar.append(search,status,result);shell.append(toolbar);
  const tableHost=node('div','task-table-host');shell.append(tableHost);main.append(shell);

  const params=new URLSearchParams(location.search);
  search.value=params.get('q')||'';
  status.value=['configured','changed','starting','running','stopping','stopped','failed'].includes(params.get('status'))?params.get('status'):'all';

  const render=()=>{
    total.replaceChildren(node('strong','',String(tasks.length)),document.createTextNode(' 个任务'));
    const changedNow=tasks.filter(task=>task.configuration_changed||task.plan_needs_requalification).length;
    changed.textContent=changedNow+' 个需要重新检查';changed.classList.toggle('task-summary-warning',Boolean(changedNow));
    ready.textContent=tasks.filter(task=>task.status==='running').length+' 个同步中';
    const query=search.value.trim().toLocaleLowerCase();
    const filtered=tasks.filter(task=>{
      const matchesQuery=!query||taskSearchText(task).includes(query);
      return matchesQuery&&(status.value==='all'||taskStatus(task).key===status.value);
    });
    result.textContent='显示 '+filtered.length+' / '+tasks.length;
    if(filtered.length)tableHost.replaceChildren(taskListTable(filtered,refresh));
    else {
      const noResults=empty(tasks.length?'没有匹配的同步任务':'还没有同步任务，可从左侧“添加任务”创建');
      const clear=node('button','text-button','清除筛选');clear.type='button';
      clear.addEventListener('click',()=>{search.value='';status.value='all';render();search.focus();});
      noResults.append(clear);tableHost.replaceChildren(noResults);
    }
    const next=new URLSearchParams();
    if(search.value.trim())next.set('q',search.value.trim());
    if(status.value!=='all')next.set('status',status.value);
    const queryString=next.toString();
    history.replaceState(null,'','/tasks'+(queryString?'?'+queryString:''));
  };
  const refresh=async()=>{tasks=await api('/api/tasks');render();};
  search.addEventListener('input',render);status.addEventListener('change',render);render();pollTasks(refresh);
}
// Keep the viewport and its existing lines alive across refreshes. Each tab owns
// its cursor and scroll position; a reader above the bottom is never forced down.
function taskLogViewer(id) {
  const root=node('section','panel task-state task-logs'),head=node('div','task-log-heading');
  const tabs=node('div','task-log-tabs');tabs.setAttribute('role','tablist');tabs.setAttribute('aria-label','任务日志');
  const latest=node('button','btn task-log-latest','查看最新');latest.type='button';
  const hint=node('span','muted task-log-hint');
  const error=node('p','warn');error.hidden=true;
  const output=node('pre','task-write-log');output.id='task-log-output';output.setAttribute('role','tabpanel');output.tabIndex=0;
  const emptyText=node('p','muted task-log-empty');
  const views={source:{label:'源端日志'},sink:{label:'写入日志'}};
  let active='sink',switching=false;
  const limit=2000;
  for(const [key,view] of Object.entries(views)) {
    Object.assign(view,{entries:[],cursor:0,generation:null,following:true,paused:false,top:0,left:0,pending:null,loaded:false,error:''});
    const button=node('button','task-log-tab',view.label);button.type='button';button.id='task-log-tab-'+key;
    button.setAttribute('role','tab');button.setAttribute('aria-controls',output.id);
    button.addEventListener('click',()=>select(key));
    button.addEventListener('keydown',event=>{
      if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key))return;
      event.preventDefault();const next=event.key==='Home'?'source':event.key==='End'?'sink':key==='source'?'sink':'source';
      select(next);views[next].button.focus();
    });view.button=button;tabs.append(button);
  }
  function decorate() {
    const view=views[active];
    for(const [key,item] of Object.entries(views)) {
      item.button.setAttribute('aria-selected',String(key===active));item.button.tabIndex=key===active?0:-1;
    }
    output.setAttribute('aria-labelledby',view.button.id);
    latest.hidden=view.following&&!view.paused;
    hint.textContent=view.paused?'已暂停加载，点击“查看最新”继续':view.following?'跟随最新日志':'正在查看历史日志';
    emptyText.textContent=view.loaded?'暂无'+view.label+'。':'正在加载'+view.label+'…';emptyText.hidden=view.entries.length>0;
    error.textContent=view.error;error.hidden=!view.error;
  }
  function line(item,key) {
    const text=key==='source'?item.message:new Date(item.timestamp*1000).toLocaleString()+' ['+item.level+'] '+item.message;
    return node('span','task-log-line',text+'\n');
  }
  function show() {
    const view=views[active];switching=true;
    output.replaceChildren(...view.entries.map(item=>line(item,active)));
    output.scrollTop=view.following?output.scrollHeight:view.top;output.scrollLeft=view.left;
    switching=false;decorate();
  }
  function select(key) {
    if(key===active)return;
    const old=views[active];old.top=output.scrollTop;old.left=output.scrollLeft;
    active=key;show();void load(key);
  }
  output.addEventListener('scroll',()=>{
    if(switching)return;
    const view=views[active];view.top=output.scrollTop;view.left=output.scrollLeft;
    view.following=output.scrollHeight-output.clientHeight-output.scrollTop<=3;
    decorate();
  });
  async function load(key) {
    const view=views[key];if(view.pending)return view.pending;if(view.paused)return;
    view.pending=(async()=>{
      const params=new URLSearchParams({after:String(view.cursor)});
      if(key==='source'&&view.generation!==null)params.set('generation',view.generation);
      try {
        const response=await api('/api/tasks/'+encodeURIComponent(id)+(key==='source'?'/source-logs':'/logs')+'?'+params);
        const reset=key==='source'&&response.reset;
        const incoming=key==='source'?response.entries:response;
        // Read the actual position now: the user may have scrolled during fetch.
        const visible=active===key;
        const top=visible?output.scrollTop:view.top,left=visible?output.scrollLeft:view.left;
        const following=visible?output.scrollHeight-output.clientHeight-output.scrollTop<=3:view.following;
        if(reset){view.entries=[];view.cursor=0;}
        const fresh=incoming.filter(item=>item.id>view.cursor);
        const room=limit-view.entries.length;
        const accepted=following?fresh:fresh.slice(0,Math.max(0,room));
        view.entries.push(...accepted);
        if(accepted.length)view.cursor=accepted[accepted.length-1].id;
        if(key==='source'){
          view.generation=response.generation;
          if(accepted.length===fresh.length)view.cursor=response.next;
        }
        view.following=following;view.loaded=true;view.error='';
        view.paused=!following&&(view.entries.length>=limit);
        const excess=Math.max(0,view.entries.length-limit);
        if(excess)view.entries.splice(0,excess);
        if(visible){
          switching=true;
          if(reset||excess)output.replaceChildren(...view.entries.map(item=>line(item,key)));
          else if(accepted.length)output.append(...accepted.map(item=>line(item,key)));
          output.scrollTop=following?output.scrollHeight:top;output.scrollLeft=left;
          view.top=output.scrollTop;switching=false;decorate();
        }
      }catch(reason){view.error=reason.message;if(active===key)decorate();}
    })().finally(()=>{view.pending=null;});
    return view.pending;
  }
  latest.addEventListener('click',async()=>{
    const key=active,view=views[key];
    if(view.pending)await view.pending;
    if(view.paused){view.entries=[];view.cursor=0;view.generation=null;view.paused=false;}
    view.following=true;show();await load(key);
  });
  head.append(tabs,hint,latest);root.append(head,error,emptyText,output);decorate();
  return {element:root,refresh:()=>load(active)};
}

function taskAutoStart(task,onSaved) {
  const element=node('div','task-auto-start'),label=node('label','task-auto-start-label');
  const checkbox=node('input');checkbox.type='checkbox';
  const help=node('small','muted','勾选后，CDC Web 服务启动时自动运行此任务。');
  const feedback=node('span','muted task-auto-start-feedback');feedback.setAttribute('role','status');
  label.append(checkbox,node('span','','自动开始任务'));element.append(label,feedback,help);
  let saving=false,confirmed=Boolean(task.auto_start);
  function update(current) {
    if(saving)return;
    confirmed=Boolean(current.auto_start);checkbox.checked=confirmed;
    checkbox.disabled=state.me.user.role!=='admin';
  }
  checkbox.addEventListener('change',async()=>{
    const enabled=checkbox.checked;saving=true;checkbox.disabled=true;feedback.textContent='正在保存…';
    try {
      const saved=await api('/api/tasks/'+encodeURIComponent(task.id)+'/auto-start',{method:'PUT',body:JSON.stringify({enabled})});
      confirmed=Boolean(saved.auto_start);feedback.textContent='已保存';onSaved(saved);
    }catch(reason){feedback.textContent='保存失败';toast(reason.message);}
    finally{saving=false;checkbox.checked=confirmed;checkbox.disabled=state.me.user.role!=='admin';}
  });
  update(task);return {element,update};
}

async function renderTaskDetail(id) {
  clearInterval(taskPollTimer);
  setTitle('任务详情');
  const main=$('#content');main.replaceChildren(title('任务详情','正在加载任务'));
  let task;
  try {task=await api('/api/tasks/'+encodeURIComponent(id));}
  catch(reason){main.replaceChildren(title('任务详情',''),empty(reason.message),taskLink('/tasks','返回任务列表'));return;}
  const heading=title(task.name,'创建时间：'+new Date(task.created_at*1000).toLocaleString());
  heading.append(taskLink('/tasks','返回任务列表'));
  const info=node('div','panel task-state'),statusView=node('b'),controlHost=node('div','task-control'),details=node('div');
  const automatic=taskAutoStart(task,saved=>{task=saved;draw();});
  info.append(statusView,automatic.element,controlHost,details);
  const mapping=node('div','mapping-layout task-endpoints');
  for(const side of ['source','sink']) {
    const panel=node('section','panel'),top=node('div','section-heading'),list=node('div','task-saved-tables');
    top.append(node('h2','',side==='source'?'源端库表':'目的端库表'),node('span','muted',task[side+'_name']+(task[side+'_database']?' · '+task[side+'_database']:'')));
    task.mappings.forEach(m=>{
      const row=node('div','tree-row'),fieldText=Array.isArray(m.columns)&&m.columns.length?m.columns.length+' 个字段':'全部字段';
      row.append(node('span','',m[side+'_schema']+'.'+m[side+'_table']),node('small','muted',fieldText));list.append(row);
    });
    panel.append(top,list);mapping.append(panel);
  }
  const log=taskLogViewer(id);
  main.replaceChildren(heading,info,mapping,log.element);
  let lastInfo='';
  const refresh=async()=>{
    const [current]=await Promise.all([api('/api/tasks/'+encodeURIComponent(id)),log.refresh()]);
    task=current;draw();
  };
  function draw() {
    automatic.update(task);
    const signature=JSON.stringify([task.status,task.runtime,task.configuration_changed,task.plan_status,task.plan_set_digest,task.start_mode,task.auto_start]);
    if(signature===lastInfo)return;lastInfo=signature;
    const statusInfo=taskStatus(task),runtime=task.runtime||{},cp=runtime.checkpoint;
    statusView.className='task-status task-status-'+statusInfo.tone;statusView.textContent=statusInfo.label;
    const control=taskAction(task,refresh),requalify=taskRequalifyAction(task,refresh);controlHost.replaceChildren(...([control,requalify].filter(Boolean)));
    details.replaceChildren(node('p','muted',cp?.phase==='snapshot'?'全量数据尚未提交；中断后整体回滚，再次启动重新全量。':cp?'再次启动从目的端已提交位点继续增量。':'首次启动先全量同步，再从快照位点持续同步增量。目的端表须提前建好且为空。'),
      node('p','muted','起点模式：'+taskModeLabel(cp?.mode||task.start_mode)),
      node('p','','已提交 '+(runtime.applied_transactions||0)+' 个事务 / '+(runtime.applied_rows||0)+' 行'));
    if(runtime.pending_transaction)details.append(node('p','muted',runtime.pending_transaction));
    if(cp) {
      details.append(node('p','','全量已提交 '+(cp.snapshot_rows||0)+' 行'));
      details.append(node('p','mono','位点：'+cp.file+':'+cp.position));
      if(cp.gtid_set!==null)details.append(node('p','mono task-gtid','GTID：'+(cp.gtid_set||'空集合')));
    }
    if(runtime.last_error)details.append(node('p','warn',runtime.last_error));
    if(task.configuration_changed)details.append(node('p','warn','关联实例配置已变化，请重新检查配置。'));
    if(task.plan_needs_requalification)details.append(node('p','warn','ColumnConversionPlan：'+(task.plan_invalid_reason||({legacy:'旧任务尚未迁移计划',stale:'元数据或能力输入已变化',missing_confirmation:'缺少风险确认',capability_insufficient:'目的端能力不足'}[task.plan_status]||'需要重新预检'))));
    else details.append(node('p','muted','ColumnConversionPlan：有效 · '+(task.plans?.length||0)+' 个字段计划 · revision '+task.configuration_revision));
    const preview=node('section','panel task-plan-preview');
    preview.append(node('h2','','兼容计划预览'));
    if(!Array.isArray(task.plans)||!task.plans.length) {
      preview.append(node('p','muted','尚未保存字段兼容计划；缺少计划或风险确认时不能启动任务。'));
    } else {
      task.plans.forEach(plan=>{
        const parameters=plan.target?.parameters||{}, parameterText=Object.entries(parameters).map(([key,value])=>key+'='+value).join(' · ');
        const card=node('article','task-plan-card');
        card.append(node('b','',plan.source_field?.lineage_id+' → '+plan.target_field?.lineage_id));
        card.append(node('p','muted','资格：'+(plan.qualification||'未配置')+' · 风险等级：'+compatibilityRiskLabel(plan.risk)));
        appendCompatibilitySummary(card,compatibilityHumanSummary(
          {native_type:plan.source_field?.native_type,collation:parameters.source_collation},
          {native_type:plan.target?.native_type,collation:parameters.target_collation},
          {status:'COMPATIBLE',risk:plan.risk},plan
        ));
        const technical=node('details','compatibility-technical'),technicalTitle=node('summary','','查看技术细节');technical.append(technicalTitle);
        if(parameterText)technical.append(node('p','mono','转换参数：'+parameterText));
        if(plan.risk_code)technical.append(node('p','mono','风险代码：'+plan.risk_code));
        technical.append(node('p','mono','计划摘要：'+(plan.plan_digest||'未知')));
        card.append(technical);
        preview.append(card);
      });
    }
    details.append(preview);

  }
  await refresh();pollTasks(refresh);
}
async function renderTaskAdd() {
  setTitle('添加任务');
  const main=$('#content');
  main.replaceChildren(title('添加同步任务','选择多个源数据库、表和字段，并核对目的端对应关系'));
  if(state.me.user.role!=='admin'){main.append(empty('只有管理员可以添加任务'));return;}
  if(state.instances.length<2){
    const box=empty('请先添加源端和目的端数据库实例');
    box.append(node('p','muted','源实例需要读取账号，目的实例需要写入账号。'),instanceAddLink('添加实例'));main.append(box);return;
  }

  const form=node('form','task-create-form'),fields=node('fieldset','task-fields');
  const config=node('section','panel task-config'),nameLabel=node('label','','任务名称'),name=node('input','input');
  name.name='name';name.required=true;name.maxLength=128;name.placeholder='例如：业务库同步';nameLabel.append(name);
  const modeLabel=node('label','','起点模式'),mode=node('select','select');
  mode.name='start_mode';['auto','gtid','binlog'].forEach(value=>{const option=node('option','',taskModeLabel(value));option.value=value;mode.append(option);});
  modeLabel.append(mode);config.append(nameLabel,modeLabel,
    node('p','muted task-config-note','源端与目的端使用同库同名表映射。展开任一侧时两边会同步展开；点击名称可高亮对应节点。'));

  const treeLayout=node('div','task-tree-layout');
  const connector=document.createElementNS('http://www.w3.org/2000/svg','svg');
  connector.classList.add('task-tree-connectors');connector.setAttribute('aria-hidden','true');
  const endpoints={
    source:{role:'source',title:'源端库表',account:'读取账号',base:null,tables:new Map(),loadingSchemas:new Set(),seq:0,database:''},
    sink:{role:'sink',title:'目的端库表',account:'写入账号',base:null,tables:new Map(),loadingSchemas:new Set(),seq:0,database:''}
  };
  const selected=new Set(),expandedSchemas=new Set(),expandedTables=new Set();
  const compatibility=new Map();
  const draftId=(window.crypto&&typeof window.crypto.randomUUID==='function')
    ?window.crypto.randomUUID().replaceAll('-','')
    :Math.random().toString(36).slice(2)+Date.now().toString(36);
  let focusedKey='',submitting=false,scrolling=false;
  function syncControlHeights() {
    const controls=Object.values(endpoints).map(side=>side.controls).filter(Boolean);
    controls.forEach(control=>control.style.height='auto');
    const height=Math.max(...controls.map(control=>control.scrollHeight),0);
    controls.forEach(control=>{control.style.height=height+'px';});
  }

  const key=(kind,schema,table='',column='')=>JSON.stringify([kind,schema,table,column]);
  const schemaKey=schema=>key('schema',schema);
  const tableKey=(schema,table)=>key('table',schema,table);
  const columnKey=(schema,table,column)=>key('column',schema,table,column);
  const sorted=values=>[...new Set(values)].sort((a,b)=>a.localeCompare(b,'zh-CN'));

  function schemaExists(side,schema) {
    return Boolean(side.base&&side.base.schemas.includes(schema));
  }
  function tableFor(side,schema,table) {
    return (side.tables.get(schema)||[]).find(item=>item.name===table);
  }
  function columnFor(table,column) {
    return table?.columns.find(item=>item.name===column);
  }
  function tablePairReason(schema,table) {
    const source=tableFor(endpoints.source,schema,table),sink=tableFor(endpoints.sink,schema,table);
    if(!source)return '源端缺少同名表';
    if(!sink)return '目的端缺少同名表';
    const sourceReason=taskTableReason(source),sinkReason=taskTableReason(sink);
    if(sourceReason)return sourceReason;
    if(sinkReason)return '目的端'+sinkReason;
    if(JSON.stringify(source.primary_key)!==JSON.stringify(sink.primary_key))return '主键不一致';
    return '';
  }
  function normalizedNativeType(value) {
    const normalized=String(value||'').trim().toLowerCase().replace(/\s+/g,' ');
    // MySQL display widths are metadata spelling differences, not storage
    // semantics. Keep real parameters (decimal/char/varchar) so only an
    // actual conversion opens the compatibility editor.
    return normalized
      .replace(/\b(tinyint|smallint|mediumint|int|integer|bigint)\(\d+\)/g,'$1')
      .replace(/\binteger\b/g,'int');
  }
  function compatibilityStatus(result) { return String(result?.status||'').toUpperCase(); }
  function nativeLength(value) {
    const match=normalizedNativeType(value).match(/\((\d+)\)/);return match?match[1]:'';
  }
  function nativeTextLength(value) {
    const type=normalizedNativeType(value),declared=nativeLength(type);
    if(declared)return declared;
    return ({tinytext:'255',text:'65535',mediumtext:'16777215',longtext:'4294967295'})[type]||'unbounded';
  }
  function needsCompatibilityOptions(schema,table,column) {
    const sourceTable=tableFor(endpoints.source,schema,table),sinkTable=tableFor(endpoints.sink,schema,table);
    const source=columnFor(sourceTable,column),sink=columnFor(sinkTable,column);
    if(!source||!sink)return false;
    const saved=compatibility.get(columnKey(schema,table,column));
    if(compatibilityStatus(saved?.result)==='COMPATIBLE')return false;
    return normalizedNativeType(source.column_type)!==normalizedNativeType(sink.column_type)
      ||String(source.collation||'')!==String(sink.collation||'');
  }
  function columnPairReason(schema,table,column) {
    const sourceTable=tableFor(endpoints.source,schema,table),sinkTable=tableFor(endpoints.sink,schema,table);
    const tableReason=tablePairReason(schema,table);
    if(tableReason)return tableReason;
    const source=columnFor(sourceTable,column),sink=columnFor(sinkTable,column);
    if(!source)return '源端缺少字段';
    if(!sink)return '目的端缺少字段';
    if(!needsCompatibilityOptions(schema,table,column))return '';
    const saved=compatibility.get(columnKey(schema,table,column));
    if(!saved)return '源端和目的端字段定义不一致，请先配置兼容选项';
    if(saved.error)return saved.error.message;
    if(compatibilityStatus(saved.result)!=='COMPATIBLE')return saved.result?.explanation||'兼容选项尚未完成';
    return '';
  }
  function eligibleColumnKeys(schema,table) {
    const source=tableFor(endpoints.source,schema,table);
    if(!source||tablePairReason(schema,table))return [];
    return source.columns.filter(column=>!columnPairReason(schema,table,column.name))
      .map(column=>columnKey(schema,table,column.name));
  }
  function tableNames(schema) {
    return sorted([
      ...(endpoints.source.tables.get(schema)||[]).map(table=>table.name),
      ...(endpoints.sink.tables.get(schema)||[]).map(table=>table.name)
    ]);
  }
  function schemaColumnKeys(schema) {
    return tableNames(schema).flatMap(table=>eligibleColumnKeys(schema,table));
  }
  function rowKeys(row) {
    if(row.kind==='schema')return schemaColumnKeys(row.schema);
    if(row.kind==='table')return eligibleColumnKeys(row.schema,row.table);
    if(row.kind==='column'&&!columnPairReason(row.schema,row.table,row.column))return [row.key];
    return [];
  }
  function selectionState(row) {
    const keys=rowKeys(row),count=keys.filter(item=>selected.has(item)).length;
    return {checked:keys.length>0&&count===keys.length,indeterminate:count>0&&count<keys.length,count,total:keys.length};
  }
  function buildRows() {
    const rows=[];
    const schemas=sorted([
      ...(endpoints.source.base?.schemas||[]),
      ...(endpoints.sink.base?.schemas||[])
    ]);
    for(const schema of schemas) {
      rows.push({kind:'schema',schema,key:schemaKey(schema)});
      if(!expandedSchemas.has(schemaKey(schema)))continue;
      if(endpoints.source.loadingSchemas.has(schema)||endpoints.sink.loadingSchemas.has(schema)) {
        rows.push({kind:'loading',schema,key:key('loading',schema)});
        continue;
      }
      for(const table of tableNames(schema)) {
        rows.push({kind:'table',schema,table,key:tableKey(schema,table)});
        if(!expandedTables.has(tableKey(schema,table)))continue;
        const source=tableFor(endpoints.source,schema,table),sink=tableFor(endpoints.sink,schema,table);
        const columns=sorted([
          ...(source?.columns||[]).map(column=>column.name),
          ...(sink?.columns||[]).map(column=>column.name)
        ]);
        columns.forEach(column=>rows.push({kind:'column',schema,table,column,key:columnKey(schema,table,column)}));
      }
    }
    return rows;
  }

  function endpointUrl(side,schema) {
    let url='/api/instances/'+encodeURIComponent(side.select.value)+'/catalog?role='+side.role;
    if(side.database)url+='&database='+encodeURIComponent(side.database);
    if(schema)url+='&schema='+encodeURIComponent(schema);
    return url;
  }
  async function loadSchema(side,schema) {
    if(!schemaExists(side,schema)||side.tables.has(schema)||side.loadingSchemas.has(schema))return;
    const seq=side.seq;side.loadingSchemas.add(schema);renderTrees();
    try {
      const catalog=await api(endpointUrl(side,schema));
      if(seq!==side.seq)return;
      side.tables.set(schema,catalog.tables);
    } catch(reason) {
      if(seq===side.seq){side.error.textContent=reason.message;side.error.hidden=false;}
    } finally {
      side.loadingSchemas.delete(schema);
    }
  }
  async function loadSchemaPair(schema) {
    await Promise.all([loadSchema(endpoints.source,schema),loadSchema(endpoints.sink,schema)]);
  }
  async function toggleExpand(row) {
    if(row.kind==='schema') {
      if(expandedSchemas.has(row.key))expandedSchemas.delete(row.key);
      else {expandedSchemas.add(row.key);renderTrees();await loadSchemaPair(row.schema);}
    } else if(row.kind==='table') {
      expandedTables.has(row.key)?expandedTables.delete(row.key):expandedTables.add(row.key);
    }
    renderTrees();
  }
  function addPrimaryKeys(schema,table) {
    const source=tableFor(endpoints.source,schema,table);
    for(const primary of source?.primary_key||[]) {
      const fieldKey=columnKey(schema,table,primary);
      if(!columnPairReason(schema,table,primary))selected.add(fieldKey);
    }
  }
  async function changeSelection(row,checked) {
    if(row.kind==='schema'&&!endpoints.source.tables.has(row.schema)) {
      expandedSchemas.add(row.key);renderTrees();await loadSchemaPair(row.schema);
    }
    const refreshed=buildRows().find(item=>item.key===row.key)||row;
    const keys=rowKeys(refreshed);
    if(checked) {
      keys.forEach(item=>selected.add(item));
      if(row.kind==='column')addPrimaryKeys(row.schema,row.table);
      if(row.kind==='schema')expandedSchemas.add(row.key);
      if(row.kind==='table')expandedTables.add(row.key);
    } else {
      keys.forEach(item=>selected.delete(item));
    }
    renderTrees();
  }
  function rowEntity(side,row) {
    if(row.kind==='schema')return schemaExists(side,row.schema)?{name:row.schema}:null;
    if(row.kind==='table')return tableFor(side,row.schema,row.table);
    if(row.kind==='column')return columnFor(tableFor(side,row.schema,row.table),row.column);
    return null;
  }
  function rowMeta(side,row,entity) {
    if(!entity)return side.role==='source'?'源端缺少':'目的端缺少';
    if(row.kind==='schema')return '业务库';
    if(row.kind==='table')return taskTableReason(entity)||entity.engine;
    const table=tableFor(side,row.schema,row.table);
    return entity.column_type+(table?.primary_key.includes(entity.name)?' · 主键 · 必选':'');
  }
  function compatibilityDefault(spec,source,sink) {
    const sourceType=normalizedNativeType(source.column_type),targetType=normalizedNativeType(sink.column_type);
    const allowedValues=Array.isArray(spec.allowed_values)?spec.allowed_values:[];
    const targetCharset=String(sink.collation||'').split('_')[0].toLowerCase();
    if(spec.name==='target_charset') {
      if(allowedValues.includes(targetCharset))return targetCharset;
      if(allowedValues.includes('UTF8')&&['postgresql','utf8'].includes(targetCharset))return 'UTF8';
      return allowedValues[0]||'';
    }
    if(spec.name==='target_length')return nativeTextLength(sink.column_type);
    if(spec.name==='target_length_unit')return /^(char|varchar)\b/.test(targetType)?'characters':(allowedValues[0]||'bytes');
    if(spec.name==='target_collation')return sink.collation||'none';
    if(spec.name==='target_precision')return (targetType.match(/\((\d+)\)/)||[])[1]||'0';
    if(spec.name==='temporal_strategy') {
      if(/timestamp/.test(sourceType)&&/datetime/.test(targetType))return 'absolute_to_local';
      if(/datetime/.test(sourceType)&&/timestamp/.test(targetType))return 'local_to_absolute';
      if(/time/.test(sourceType)&&/time/.test(targetType))return 'preserve_duration';
      if(/timestamp/.test(sourceType))return 'preserve_absolute';
      return 'preserve_local';
    }
    if(spec.name==='json_strategy')return 'normalized_text';
    return spec.default||allowedValues[0]||'';
  }
  async function openCompatibility(row) {
    const source=columnFor(tableFor(endpoints.source,row.schema,row.table),row.column);
    const sink=columnFor(tableFor(endpoints.sink,row.schema,row.table),row.column);
    if(!source||!sink)return;
    const saved=compatibility.get(row.key);
    let parameters={...(saved?.parameters||{})},confirmations=[...(saved?.confirmations||[])],response=null;
    const dialog=node('dialog','compatibility-dialog'),form=node('form','compatibility-form');
    const heading=node('div','dialog-heading'),headingText=node('div');
    headingText.append(node('h2','','兼容选项'),node('p','muted',row.schema+'.'+row.table+'.'+row.column));
    const close=node('button','icon','×');close.type='button';close.setAttribute('aria-label','关闭');close.addEventListener('click',()=>dialog.close());
    heading.append(headingText,close);
    const content=node('div','compatibility-content'),actions=node('div','dialog-actions');
    const cancel=node('button','btn','取消');cancel.type='button';cancel.addEventListener('click',()=>dialog.close());
    const verify=node('button','btn primary','验证并保存');verify.type='submit';actions.append(cancel,verify);
    form.append(heading,content,actions);dialog.append(form);document.body.append(dialog);
    dialog.addEventListener('close',()=>dialog.remove(),{once:true});dialog.showModal();
    function payload(nextParameters,nextConfirmations) {
      return {draft_id:draftId,source_id:endpoints.source.select.value,sink_id:endpoints.sink.select.value,
        source_database:endpoints.source.database,sink_database:endpoints.sink.database,
        source_revision:endpoints.source.base.revision,sink_revision:endpoints.sink.base.revision,
        schema:row.schema,table:row.table,column:row.column,parameters:nextParameters,confirmations:nextConfirmations};
    }
    async function request(nextParameters,nextConfirmations) {
      return api('/api/compatibility/preview',{method:'POST',body:JSON.stringify(payload(nextParameters,nextConfirmations))});
    }
    function renderPreview() {
      content.replaceChildren();
      const sourceLabel='源端：'+compatibilityFieldType(source)+(source.collation?' · '+source.collation:'');
      const sinkLabel='目的端：'+compatibilityFieldType(sink)+(sink.collation?' · '+sink.collation:'');
      content.append(node('p','compatibility-field-pair',sourceLabel+' → '+sinkLabel));
      if(response?.error){content.append(node('p','task-error',response.error.message));verify.disabled=true;return;}
      const result=response?.result;
      if(!result){content.append(node('p','task-error','兼容性预览没有返回结果'));verify.disabled=true;return;}
      const candidates=result.candidates||[],rules=response.candidates||[];
      if(candidates.length>1){
        const label=node('label','compatibility-option-label','选择目标字段表示方式'),select=node('select','select');
        candidates.forEach(candidate=>{const option=node('option','',candidate.target.native_type+' · '+candidate.qualification+' · 风险 '+candidate.risk);option.value=candidate.rule.id;option.selected=parameters.__rule_id===candidate.rule.id;select.append(option);});
        if(!select.value&&candidates[0])select.value=candidates[0].rule.id;
        select.addEventListener('change',()=>{const candidate=candidates.find(item=>item.rule.id===select.value);if(candidate){parameters.__rule_id=candidate.rule.id;parameters.__rule_version=candidate.rule.version;renderPreview();}});
        content.append(label);label.append(select);
      } else if(candidates[0]) {
        parameters.__rule_id=candidates[0].rule.id;parameters.__rule_version=candidates[0].rule.version;
      }
      const ruleId=parameters.__rule_id||(candidates[0]?.rule.id||'');
      const rule=rules.find(item=>item.rule.id===ruleId)||rules[0];
      const selectedCandidate=candidates.find(candidate=>candidate.rule.id===ruleId)||candidates[0];
      appendCompatibilitySummary(content,compatibilityHumanSummary(source,sink,result,result.plan,selectedCandidate?.target));
      if(rule?.options?.length){
        const fieldset=node('fieldset','compatibility-options');fieldset.append(node('legend','','需要时调整转换设置'));
        const advanced=node('details','compatibility-advanced'),advancedTitle=node('summary','','高级设置（通常无需修改）'),advancedFields=node('div','compatibility-advanced-fields');advanced.append(advancedTitle,advancedFields);
        let visibleCount=0,advancedCount=0;
        rule.options.forEach(spec=>{const label=node('label','compatibility-option-label');label.append(node('span','',compatibilityOptionLabel(spec.name)));const help=compatibilityOptionHelp(spec.name);if(help)label.append(node('small','muted',help));const allowedValues=Array.isArray(spec.allowed_values)?spec.allowed_values:[];const input=allowedValues.length?node('select','select'):node('input','input');
          input.name='compat-option';input.dataset.option=spec.name;input.required=spec.required;
          if(input.tagName==='SELECT'){allowedValues.forEach(value=>{const option=node('option','',value);option.value=value;input.append(option);});}
          input.value=parameters[spec.name]||compatibilityDefault(spec,source,sink);label.append(input);
          if(compatibilityOptionAdvanced(spec.name)){advancedFields.append(label);advancedCount++;}else{fieldset.append(label);visibleCount++;}
        });
        if(visibleCount)content.append(fieldset);
        if(advancedCount)content.append(advanced);
      }
      const status=compatibilityStatus(result);
      const statusLabel={COMPATIBLE:'可同步',NEEDS_CONFIRMATION:'需要风险确认',NEEDS_CONFIGURATION:'需要配置参数',UNSUPPORTED:'不支持',BLOCKED:'已阻断',STALE:'已过期'}[status]||result.status;
      content.append(node('p',status==='COMPATIBLE'?'good':'warn','状态：'+statusLabel+' · '+result.explanation));
      const needsConfirm=status==='NEEDS_CONFIRMATION'||result.requires_confirmation;
      if(needsConfirm&&result.plan){
        const label=node('label','compatibility-confirm-label'),check=node('input');check.type='checkbox';check.name='compat-confirm';
        label.append(check,document.createTextNode('我已了解该字段的转换风险，同意按上述规则执行'));content.append(label);
      }
      verify.disabled=['UNSUPPORTED','BLOCKED','STALE'].includes(status)||Boolean(response.error);
    }
    try {response=await request(parameters,confirmations);renderPreview();}
    catch(reason){response={error:{message:reason.message}};renderPreview();}
    form.addEventListener('submit',async event=>{
      event.preventDefault();if(verify.disabled)return;verify.disabled=true;
      const values=form.querySelectorAll('[data-option]');values.forEach(input=>{parameters[input.dataset.option]=input.value;});
      const selected=form.querySelector('[data-option]');
      if(response?.candidates?.length){const selectedRule=parameters.__rule_id||response.result?.candidates?.[0]?.rule.id;if(selectedRule){const candidate=response.result.candidates.find(item=>item.rule.id===selectedRule)||response.result.candidates[0];parameters.__rule_id=candidate.rule.id;parameters.__rule_version=candidate.rule.version;}}
      try {
        response=await request(parameters,[]);
        if(compatibilityStatus(response.result)==='NEEDS_CONFIRMATION'&&response.result.plan){
          const check=form.querySelector('[name=compat-confirm]');
          if(!check?.checked){renderPreview();return;}
          const plan=response.result.plan;
          confirmations=[{source_field_lineage:plan.source_field.lineage_id,target_field_lineage:plan.target_field.lineage_id,rule:plan.rule,plan_digest:plan.plan_digest,actor:state.me.user.username,confirmed_at:new Date().toISOString(),reason:'添加任务页确认兼容转换风险'}];
          response=await request(parameters,confirmations);
        }
        if(compatibilityStatus(response.result)!=='COMPATIBLE'||!response.result.plan){renderPreview();return;}
        compatibility.set(row.key,{parameters:{...parameters},confirmations:[...confirmations],result:response.result});
        dialog.close();renderTrees();updateSummary();
      } catch(reason){response={error:{message:reason.message}};renderPreview();}
      finally {if(dialog.open)verify.disabled=false;}
    });
  }
  function focusRow(rowKey) {
    focusedKey=rowKey;
    treeLayout.querySelectorAll('.task-tree-row').forEach(element=>element.classList.toggle('focused',element.dataset.nodeKey===rowKey));
    drawLines();
  }
  function renderRow(side,row) {
    const line=node('div','task-tree-row task-tree-'+row.kind);
    line.dataset.nodeKey=row.key;
    if(row.kind==='loading') {
      line.append(node('span','task-tree-loading','正在读取 '+row.schema+' 的库表…'));return line;
    }
    const entity=rowEntity(side,row),present=Boolean(entity);
    line.classList.toggle('is-present',present);
    if(!present)line.classList.add('is-missing');
    if(focusedKey===row.key)line.classList.add('focused');
    if(selectionState(row).count>0)line.classList.add('selected');

    const expand=node('button','task-tree-toggle');
    expand.type='button';
    if(row.kind==='schema'||row.kind==='table') {
      const opened=row.kind==='schema'?expandedSchemas.has(row.key):expandedTables.has(row.key);
      expand.textContent=opened?'−':'+';
      expand.setAttribute('aria-label',(opened?'收起 ':'展开 ')+(row.table||row.schema));
      expand.addEventListener('click',event=>{event.stopPropagation();toggleExpand(row);});
    } else {
      expand.disabled=true;expand.setAttribute('aria-hidden','true');
    }

    const checkbox=node('input','task-tree-check');checkbox.type='checkbox';
    const stateInfo=selectionState(row);
    checkbox.checked=stateInfo.checked;checkbox.indeterminate=stateInfo.indeterminate;
    const pairAvailable=row.kind==='schema'
      ?schemaExists(endpoints.source,row.schema)&&schemaExists(endpoints.sink,row.schema)
      :stateInfo.total>0;
    const sourceTable=row.kind==='column'?tableFor(endpoints.source,row.schema,row.table):null;
    const primaryLocked=row.kind==='column'&&sourceTable?.primary_key.includes(row.column)&&stateInfo.count>0;
    checkbox.disabled=!pairAvailable||endpoints.source.loadingSchemas.has(row.schema)||endpoints.sink.loadingSchemas.has(row.schema);
    const incompatibility=row.kind==='column'?columnPairReason(row.schema,row.table,row.column):'';
    if(incompatibility){checkbox.title=incompatibility;line.title=incompatibility;line.classList.add('is-incompatible');}
    if(primaryLocked&&!checkbox.disabled) {
      checkbox.classList.add('task-tree-check-locked');
      checkbox.setAttribute('aria-disabled','true');
      checkbox.title='主键是增量同步的必选字段；取消整张表可以一并取消';
      checkbox.addEventListener('click',event=>{event.preventDefault();event.stopPropagation();});
    }
    checkbox.setAttribute('aria-label','选择 '+(row.column||row.table||row.schema));
    checkbox.addEventListener('change',event=>{event.stopPropagation();changeSelection(row,checkbox.checked);});

    const label=node('button','task-tree-name',row.column||row.table||row.schema);label.type='button';
    label.addEventListener('click',()=>focusRow(row.key));
    const meta=node('small',present?'muted':'warn',rowMeta(side,row,entity));
    line.append(expand,checkbox,label,meta);
    if(row.kind==='column'&&needsCompatibilityOptions(row.schema,row.table,row.column)) {
      const compatibilityButton=node('button','task-compatibility-button',compatibility.has(row.key)?'调整配置':'兼容选项');
      compatibilityButton.type='button';compatibilityButton.addEventListener('click',event=>{event.stopPropagation();openCompatibility(row);});
      line.append(compatibilityButton);
    } else line.append(node('span','task-tree-row-action'));
    return line;
  }
  function drawLines() {
    connector.replaceChildren();
    if(!endpoints.source.viewport||getComputedStyle(connector).display==='none')return;
    const layoutRect=treeLayout.getBoundingClientRect(),sourceView=endpoints.source.viewport.getBoundingClientRect(),sinkView=endpoints.sink.viewport.getBoundingClientRect();
    connector.setAttribute('viewBox','0 0 '+treeLayout.clientWidth+' '+treeLayout.clientHeight);
    const sourceRows=new Map([...endpoints.source.list.querySelectorAll('.task-tree-row.is-present')].map(item=>[item.dataset.nodeKey,item]));
    const sinkRows=new Map([...endpoints.sink.list.querySelectorAll('.task-tree-row.is-present')].map(item=>[item.dataset.nodeKey,item]));
    for(const [nodeKeyValue,sourceRow] of sourceRows) {
      const sinkRow=sinkRows.get(nodeKeyValue);if(!sinkRow)continue;
      const a=sourceRow.getBoundingClientRect(),b=sinkRow.getBoundingClientRect();
      const ay=a.top+a.height/2,by=b.top+b.height/2;
      if(ay<sourceView.top||ay>sourceView.bottom||by<sinkView.top||by>sinkView.bottom)continue;
      const line=document.createElementNS('http://www.w3.org/2000/svg','line');
      line.setAttribute('x1',String(a.right-layoutRect.left));
      line.setAttribute('x2',String(b.left-layoutRect.left));
      line.setAttribute('y1',String(ay-layoutRect.top));
      line.setAttribute('y2',String(by-layoutRect.top));
      line.classList.add('task-tree-connector');
      if(nodeKeyValue===focusedKey)line.classList.add('focused');
      connector.append(line);
    }
  }
  function syncScroll(from,to) {
    if(scrolling)return;
    scrolling=true;to.scrollTop=from.scrollTop;scrolling=false;drawLines();
  }
  function renderTrees() {
    syncControlHeights();
    const rows=buildRows(),top=Math.max(endpoints.source.viewport?.scrollTop||0,endpoints.sink.viewport?.scrollTop||0);
    for(const side of Object.values(endpoints)) {
      if(!side.list)continue;
      side.list.replaceChildren();
      if(side.loadingBase)side.list.append(node('p','muted task-tree-empty','正在读取业务库…'));
      else if(!side.base)side.list.append(node('p','muted task-tree-empty','选择实例后自动显示全部业务库'));
      else if(!rows.length)side.list.append(node('p','muted task-tree-empty','当前账号没有可见的业务库'));
      else rows.forEach(row=>side.list.append(renderRow(side,row)));
      side.viewport.scrollTop=top;
    }
    updateSummary();
    requestAnimationFrame(drawLines);
  }

  for(const side of Object.values(endpoints)) {
    const panel=node('section','panel task-tree-panel task-tree-panel-'+side.role);
    const heading=node('div','section-heading'),reload=node('button','text-button','重新加载');reload.type='button';reload.disabled=true;
    heading.append(node('h2','',side.title),reload);
    const controls=node('div','task-tree-controls'),label=node('label','',side.role==='source'?'源实例（读取账号）':'目的实例（写入账号）');
    side.select=node('select','select');side.select.required=true;
    const placeholder=node('option','','选择数据库实例');placeholder.value='';side.select.append(placeholder);
    state.instances.forEach(instance=>{
     const supported=Boolean(registeredConnector(instance,side.role));
      const eligible=supported&&(side.role==='source'?instance.has_reader_password:instance.has_writer_password);
      const option=node('option','',instance.name+' · '+instance.host+':'+instance.port+(eligible?'':supported?'（未配置对应账号）':'（当前角色尚未接入）'));
      option.value=instance.id;option.disabled=!eligible;side.select.append(option);
    });
    label.append(side.select);side.meta=node('p','muted task-endpoint-meta');side.error=node('p','task-error');side.error.hidden=true;side.error.setAttribute('role','alert');
    side.databaseLabel=node('label','task-database-label','连接数据库');
    side.databaseSelect=node('select','select');side.databaseSelect.setAttribute('aria-label',side.title+'连接数据库');
    side.databaseLabel.append(side.databaseSelect);side.databaseLabel.hidden=true;controls.append(label,side.databaseLabel,side.meta,side.error);
    side.controls=controls;
    side.viewport=node('div','task-tree-viewport');side.list=node('div','task-tree-list');side.viewport.append(side.list);
    side.viewport.addEventListener('scroll',()=>syncScroll(side.viewport,side===endpoints.source?endpoints.sink.viewport:endpoints.source.viewport));
    reload.addEventListener('click',()=>loadEndpoint(side));
    side.reload=reload;
    side.databaseSelect.addEventListener('change',()=>{side.database=side.databaseSelect.value;loadEndpoint(side);});
    side.select.addEventListener('change',()=>{updateDatabaseOptions(side);loadEndpoint(side);if(side.role==='source')updateStartModes();});
    panel.append(heading,controls,side.viewport);side.panel=panel;treeLayout.append(panel);
  }
  treeLayout.append(connector);
  window.addEventListener('resize',()=>{syncControlHeights();drawLines();},{passive:true});

  function updateStartModes() {
    const instance=state.instances.find(item=>item.id===endpoints.source.select.value);
    const connector=instance&&registeredConnector(instance,'source');
    for(const option of mode.options) {
      option.disabled=Boolean(connector&&((option.value==='gtid'&&!connector.capabilities.supports_gtid)||(option.value==='binlog'&&!connector.capabilities.supports_file_position)));
    }
    if(mode.selectedOptions[0]?.disabled)mode.value='auto';
    updateSummary();
  }
  function updateDatabaseOptions(side) {
    const instance=state.instances.find(item=>item.id===side.select.value),isPostgresql=instance?.kind==='postgresql';
    side.databaseSelect.replaceChildren();
    if(isPostgresql) {
      const values=instance.databases||((instance.database&&[instance.database])||[]);
      values.forEach(value=>{const option=node('option','',value);option.value=value;side.databaseSelect.append(option);});
      side.database=values.includes(side.database)?side.database:values[0]||'';
      side.databaseSelect.value=side.database;side.databaseLabel.hidden=false;side.databaseSelect.disabled=!values.length;
    } else {
      side.database='';side.databaseLabel.hidden=true;side.databaseSelect.disabled=true;
    }
  }

  const summary=node('section','panel task-summary'),count=node('b','','未选择库表'),description=node('span','muted');
  const submit=node('button','btn primary','检查并创建');submit.type='submit';submit.disabled=true;
  summary.append(count,description,submit);
  const error=node('p','task-error');error.setAttribute('role','alert');error.hidden=true;
  fields.append(config,treeLayout,summary,error);form.append(fields);main.append(form);
  updateStartModes();

  function selectedMappings() {
    const grouped=new Map();
    for(const item of selected) {
      const [kind,schema,table,column]=JSON.parse(item);if(kind!=='column')continue;
      const groupKey=tableKey(schema,table);
      if(!grouped.has(groupKey))grouped.set(groupKey,{source_schema:schema,source_table:table,sink_schema:schema,sink_table:table,columns:[]});
      const mapping=grouped.get(groupKey);mapping.columns.push(column);
      const configured=compatibility.get(item);
      if(configured?.parameters&&Object.keys(configured.parameters).length) {
        if(!mapping.conversion_options)mapping.conversion_options={};
        mapping.conversion_options[column]={...configured.parameters};
      }
    }
    for(const mapping of grouped.values()) {
      const source=tableFor(endpoints.source,mapping.source_schema,mapping.source_table);
      const order=new Map((source?.columns||[]).map((column,index)=>[column.name,index]));
      mapping.columns.sort((a,b)=>(order.get(a)??Number.MAX_SAFE_INTEGER)-(order.get(b)??Number.MAX_SAFE_INTEGER));
    }
    return [...grouped.values()].sort((a,b)=>(a.source_schema+'.'+a.source_table).localeCompare(b.source_schema+'.'+b.source_table,'zh-CN'));
  }
  function selectionIssue(mappings) {
    for(const mapping of mappings) {
      const source=tableFor(endpoints.source,mapping.source_schema,mapping.source_table);
      const sink=tableFor(endpoints.sink,mapping.sink_schema,mapping.sink_table);
      const selectedNames=new Set(mapping.columns);
      const pairReason=tablePairReason(mapping.source_schema,mapping.source_table);
      if(pairReason)return mapping.source_schema+'.'+mapping.source_table+'：'+pairReason;
      for(const primary of source.primary_key)if(!selectedNames.has(primary))return mapping.source_schema+'.'+mapping.source_table+'：必须选择主键 '+primary;
      for(const column of mapping.columns) {
        const reason=columnPairReason(mapping.source_schema,mapping.source_table,column);
        if(reason)return mapping.source_schema+'.'+mapping.source_table+'.'+column+'：'+reason;
      }
      for(const column of sink.columns) {
        const extra=String(column.extra||'').toLowerCase();
        if(!selectedNames.has(column.name)&&!column.nullable&&column.default_value==null&&!extra.includes('auto_increment')&&!extra.includes('generated'))
          return mapping.sink_schema+'.'+mapping.sink_table+'.'+column.name+' 未选择且没有默认值';
      }
    }
    return '';
  }
  function sourceWarning() {
    const source=endpoints.source.base,sink=endpoints.sink.base;
    const sourceInstance=state.instances.find(item=>item.id===endpoints.source.select.value),connector=sourceInstance&&registeredConnector(sourceInstance,'source');
    if(source&&sourceInstance?.kind==='mysql'&&(!source.metadata.log_bin||source.metadata.binlog_format!=='ROW'||source.metadata.binlog_row_image!=='FULL'))return '源端必须开启 binlog，并使用 ROW / FULL';
    if(source&&sourceInstance?.kind==='postgresql'&&source.metadata.can_replicate===false)return '源端需要 PostgreSQL Logical Replication 权限';
    if(connector&&mode.value==='gtid'&&!connector.capabilities.supports_gtid)return '所选源连接器不支持 GTID，请使用自动模式';
    if(connector&&mode.value==='binlog'&&!connector.capabilities.supports_file_position)return '所选源连接器不支持文件位点，请使用自动模式';
    if(source&&sourceInstance?.kind==='mysql'&&mode.value==='gtid'&&source.metadata.gtid_mode!=='ON')return '源端未启用 GTID，请选择自动模式或文件 + position';
    if(source&&sink&&source.server_uuid===sink.server_uuid)return '源端与目的端不能指向同一个 MySQL 实例';
    return '';
  }
  function updateSummary() {
    if(!submit)return;
    const mappings=selectedMappings(),schemas=new Set(mappings.map(item=>item.source_schema));
    const columns=mappings.reduce((sum,item)=>sum+item.columns.length,0);
    const warning=sourceWarning()||selectionIssue(mappings);
    count.textContent=mappings.length?schemas.size+' 个库 / '+mappings.length+' 张表 / '+columns+' 个字段':'未选择库表';
    description.textContent=warning||(mappings.length>100?'单个任务最多选择 100 张表':'创建时会重新连接两端并校验全部映射');
    description.className=warning||mappings.length>100?'warn':'muted';
    submit.disabled=submitting||!endpoints.source.base||!endpoints.sink.base||!mappings.length||mappings.length>100||Boolean(warning)
      ||endpoints.source.loadingBase||endpoints.sink.loadingBase||endpoints.source.loadingSchemas.size>0||endpoints.sink.loadingSchemas.size>0;
  }
  async function loadEndpoint(side) {
    const seq=++side.seq;side.base=null;side.tables.clear();side.loadingSchemas.clear();side.loadingBase=true;
    side.error.hidden=true;side.meta.textContent=side.select.value?'正在读取实例和业务库…':'';side.reload.disabled=true;
    selected.clear();compatibility.clear();expandedSchemas.clear();expandedTables.clear();focusedKey='';renderTrees();
    try {
      if(!side.select.value)return;
      const catalog=await api(endpointUrl(side));
      if(seq!==side.seq)return;
      side.base=catalog;side.meta.textContent=taskMetadataText(catalog)+' · '+catalog.schemas.length+' 个业务库';side.reload.disabled=false;
    } catch(reason) {
      if(seq===side.seq){side.error.textContent=reason.message;side.error.hidden=false;side.meta.textContent='加载失败，请检查实例配置';}
    } finally {
      if(seq===side.seq){side.loadingBase=false;renderTrees();}
    }
  }

  mode.addEventListener('change',updateSummary);
  renderTrees();
  form.addEventListener('submit',async event=>{
    event.preventDefault();if(submit.disabled||submitting)return;
    const source=endpoints.source,sink=endpoints.sink;
    const payload={draft_id:draftId,name:name.value.trim(),source_id:source.select.value,sink_id:sink.select.value,source_database:source.database,sink_database:sink.database,source_revision:source.base.revision,sink_revision:sink.base.revision,
      start_mode:mode.value,mappings:selectedMappings(),confirmations:[...new Map([...compatibility.values()].flatMap(item=>item.confirmations||[]).map(item=>[item.plan_digest,item])).values()]};
    submitting=true;fields.disabled=true;submit.textContent='正在检查全部库表字段…';error.hidden=true;
    try {const task=await api('/api/tasks',{method:'POST',body:JSON.stringify(payload)});location.assign('/tasks/'+encodeURIComponent(task.id));}
    catch(reason){error.textContent=reason.message;error.hidden=false;error.scrollIntoView({block:'nearest'});}
    finally{submitting=false;fields.disabled=false;submit.textContent='检查并创建';updateSummary();}
  });
}



