registerPage('apps', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const tid = getEffectiveTenantId() || APP_USER.tenant_id;
    if (!tid) { el.innerHTML = emptyHtml('🏢', 'Selecciona un tenant', 'Usa el selector superior.'); return; }
    const data = await API.apps.list(tid);
    const apps = data.apps || [];
    el.innerHTML = sectionCard('Apps Conectadas', renderDataTable(
      [
        { label: 'Nombre', render: r => `<strong>${r.name}</strong>` },
        { label: 'ID', render: r => shortId(r.id), class: 'mono' },
        { label: 'Estado', render: r => badge(r.active?'active':'inactive', r.active?'active':'inactive') },
        { label: 'Scopes', render: r => (r.scopes||[]).map(s=>`<span class="badge badge-info" style="margin:1px">${s}</span>`).join(' ') || '—' },
        { label: 'Creado', render: r => formatDate(r.created_at) },
        { label: 'Acciones', render: r => `
          <button class="btn btn-sm btn-secondary" onclick="editApp('${r.id}')">Editar</button>
          <button class="btn btn-sm btn-danger" onclick="deactivateApp('${r.id}')">Desactivar</button>` },
      ], apps), '',
      `<button class="btn btn-sm btn-primary" onclick="showCreateApp('${tid}')">+ Nueva App</button>`
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});

async function showCreateApp(tid) {
  Modal.open('Nueva App', `
    <div class="form-group"><label>Nombre</label><input class="form-input" id="app_name"></div>
    <div class="form-group"><label>Descripción</label><input class="form-input" id="app_desc"></div>
    <div class="form-group"><label>Scopes (separados por coma)</label><input class="form-input" id="app_scopes" placeholder="query:run,rag:query"></div>
  `, `<button class="btn btn-primary" onclick="doCreateApp('${tid}')">Crear</button>
      <button class="btn btn-secondary" onclick="Modal.close()">Cancelar</button>`);
}

async function doCreateApp(tid) {
  try {
    const scopes = document.getElementById('app_scopes').value.split(',').map(s=>s.trim()).filter(Boolean);
    const data = await API.apps.create(tid, {
      name: document.getElementById('app_name').value,
      description: document.getElementById('app_desc').value,
      scopes,
    });
    Modal.close();
    Modal.open('⚠️ API Key Generada', `
      <p style="margin-bottom:12px">Guarda esta key, <strong>no se mostrará de nuevo</strong>:</p>
      <div style="background:var(--bg-dark);padding:12px;border-radius:6px;font-family:monospace;word-break:break-all;font-size:0.85rem">${data.credentials.api_key}</div>
      <p class="text-xs text-muted" style="margin-top:8px">App ID: ${data.credentials.app_id}</p>
    `, `<button class="btn btn-primary" onclick="Modal.close();navigate()">Entendido</button>`);
  } catch (e) { Toast.error(e.message); }
}

async function editApp(id) {
  Modal.open('Editar App', `
    <div class="form-group"><label>Scopes (separados por coma)</label><input class="form-input" id="ea_scopes" placeholder="query:run,rag:query"></div>
    <div class="form-group"><label>Activa</label><select class="form-select" id="ea_active"><option value="true">Sí</option><option value="false">No</option></select></div>
  `, `<button class="btn btn-primary" onclick="doEditApp('${id}')">Guardar</button>
      <button class="btn btn-secondary" onclick="Modal.close()">Cancelar</button>`);
}

async function doEditApp(id) {
  try {
    const scopes = document.getElementById('ea_scopes').value.split(',').map(s=>s.trim()).filter(Boolean);
    await API.apps.update(id, { active: document.getElementById('ea_active').value === 'true', scopes: scopes.length ? scopes : undefined });
    Modal.close(); Toast.success('App actualizada'); navigate();
  } catch (e) { Toast.error(e.message); }
}

async function deactivateApp(id) {
  if (!confirm('¿Desactivar esta app?')) return;
  try { await API.apps.deactivate(id); Toast.success('App desactivada'); navigate(); }
  catch (e) { Toast.error(e.message); }
}
