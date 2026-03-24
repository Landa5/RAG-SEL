registerPage('scopes', async (el) => {
  el.innerHTML = `<div class="section-card"><div class="section-header"><span class="section-title">Gestión de Scopes</span></div>
    <div class="section-body" style="padding:20px">
      <div class="form-group"><label>App ID</label><input class="form-input" id="scope_app_id" placeholder="UUID de la app">
        <button class="btn btn-sm btn-secondary mt-16" onclick="loadScopes()">Cargar scopes</button>
      </div>
      <div id="scopeEditor"></div>
    </div></div>`;
});

async function loadScopes() {
  const appId = document.getElementById('scope_app_id').value;
  if (!appId) { Toast.error('Introduce un App ID'); return; }
  try {
    const data = await API.scopes.get(appId);
    const all = ['query:run','rag:query','documents:upload','documents:list','documents:delete',
      'analytics:query','predictions:run','executions:read',
      'admin:tenants','admin:apps','admin:credentials','admin:providers','admin:usage','admin:reviews'];
    const current = new Set(data.scopes || []);
    document.getElementById('scopeEditor').innerHTML = `
      <div class="form-group"><label>Scopes de la app</label>
        ${all.map(s => `<label style="display:flex;align-items:center;gap:8px;padding:4px 0;font-size:0.82rem;cursor:pointer">
          <input type="checkbox" class="scope-check" value="${s}" ${current.has(s)?'checked':''}>
          <code>${s}</code>
        </label>`).join('')}
      </div>
      <button class="btn btn-primary" onclick="saveScopes('${appId}')">Guardar scopes</button>`;
  } catch (e) { Toast.error(e.message); }
}

async function saveScopes(appId) {
  const checked = Array.from(document.querySelectorAll('.scope-check:checked')).map(c=>c.value);
  try {
    await API.scopes.set(appId, checked);
    Toast.success('Scopes actualizados');
  } catch (e) { Toast.error(e.message); }
}
