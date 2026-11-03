/* Shared, framework-free interactions. Works from file:// and HTTP. */
(()=>{'use strict';
const $=(s,r=document)=>r.querySelector(s), $$=(s,r=document)=>[...r.querySelectorAll(s)];
window.$u=id=>document.getElementById(id);
window.uiIcons=(id,cls='')=>`<svg class="rd-ico ${cls}" aria-hidden="true"><use href="#i-${id}"></use></svg>`;
window.uiEscape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
window.uiToast=msg=>{
    if(typeof window.toast==='function'){window.toast(msg);return;}
    document.querySelector('.ux-toast')?.remove();
    const n=document.createElement('div');n.className='ux-toast';n.setAttribute('role','status');n.textContent=msg;
    document.body.append(n);setTimeout(()=>n.remove(),3600);
};
window.uiCopy=async s=>{
    try{await navigator.clipboard.writeText(String(s??''));window.uiToast('Copiado para a área de transferência.');}
    catch(e){window.uiToast('Não foi possível copiar neste contexto.');}
};
window.uiDownload=(name,content,mime='text/plain;charset=utf-8')=>{
    const a=document.createElement('a');
    a.href=URL.createObjectURL(new Blob([content],{type:mime}));
    a.download=name;document.body.append(a);a.click();
    setTimeout(()=>{URL.revokeObjectURL(a.href);a.remove()},600);
};
const storage={get(k,f=null){try{return JSON.parse(localStorage.getItem(k))??f}catch{return f}},set(k,v){try{localStorage.setItem(k,JSON.stringify(v))}catch{}}};
const escape=window.uiEscape;
let toastTimer,dragged,sortNewest=true,filter='All';
function toast(message){const t=$('#toast');if(!t)return;t.textContent=message;t.classList.add('visible');clearTimeout(toastTimer);toastTimer=setTimeout(()=>t.classList.remove('visible'),3500)}
function setTheme(value){document.documentElement.dataset.theme=value;document.documentElement.style.colorScheme=value;try{localStorage.setItem('aero-theme',value);localStorage.setItem('ux-theme',value);}catch{}$$('[data-theme]').filter(x=>x.tagName==='BUTTON').forEach(x=>x.setAttribute('aria-pressed',String(x.dataset.theme===value)))}
setTheme(document.documentElement.dataset.theme||'dark');
const dialog=$('#dialog'), body=$('#dialog-body');
function openDialog(title,html){if(!dialog||!body)return;$('#dialog-title').textContent=title;body.innerHTML=html;dialog.showModal();setTimeout(()=>body.querySelector('input,textarea,button')?.focus(),0)}
function closeDialog(){dialog?.close()}
document.addEventListener('click',async event=>{
    const target=event.target.closest('button,a');if(!target)return;
    if(target.dataset.theme)setTheme(target.dataset.theme);
    if(target.hasAttribute('data-close'))closeDialog();
    if(target.dataset.toast)toast(target.dataset.toast);
    if(target.hasAttribute('data-expand'))$('.app-shell')?.classList.toggle('expanded-nav');
});
document.addEventListener('click',e=>{const pop=$('#popover');if(pop&&!e.target.closest('#popover,[data-menu],[data-column-menu]'))pop.hidden=true});
document.addEventListener('keydown',e=>{const pop=$('#popover');if(pop&&e.key==='Escape')pop.hidden=true});
dialog?.addEventListener('click',e=>{if(e.target===dialog){const r=dialog.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)closeDialog()}});
})();
