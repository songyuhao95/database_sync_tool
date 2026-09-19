'use strict';
const form=document.querySelector('#login-form');
const error=document.querySelector('#login-error');
if(new URLSearchParams(location.search).has('password_changed')){error.className='good';error.textContent='密码已修改，请使用新密码登录。';}
form.addEventListener('submit',async event=>{
  event.preventDefault();error.textContent='';
  const button=form.querySelector('button');button.disabled=true;
  try{
    const response=await fetch('/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({username:form.username.value,password:form.password.value})});
    const body=await response.json().catch(()=>({}));
    if(!response.ok)throw new Error(body.error?.message||'登录失败');
    const requested=new URLSearchParams(location.search).get('next')||'/';
    location.replace(requested.startsWith('/')&&!requested.startsWith('//')?requested:'/');
  }catch(reason){error.textContent=reason.message||'登录失败';form.password.value='';form.password.focus();}
  finally{button.disabled=false;}
});
