const http=require('node:http'),fs=require('node:fs'),path=require('node:path');
const root=path.resolve(__dirname,'../..');
http.createServer((req,res)=>{const url=new URL(req.url,'http://localhost');let file;
if(['/', '/tasks', '/tasks/qa-task'].includes(url.pathname))file=path.join(__dirname,'tasks.html');
else if(/^\/assets\/(tasks\.js|style\.css)$/.test(url.pathname))file=path.join(root,'crates/web_ui',url.pathname);
else{res.writeHead(404);res.end();return;}
res.setHeader('Content-Type',file.endsWith('.js')?'text/javascript':file.endsWith('.css')?'text/css':'text/html; charset=utf-8');res.end(fs.readFileSync(file));
}).listen(18082,'127.0.0.1',()=>process.stdout.write('Task UI regression: http://127.0.0.1:18082\n'));
