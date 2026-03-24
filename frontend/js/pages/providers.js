registerPage('providers', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.providers.list();
    const provs = data.providers || [];
    el.innerHTML = `
      ${kpiCard('Gasto Mensual', money(data.monthly_spent_usd), 'Fuente: execution_logs', data.monthly_spent_usd > 50 ? 'warning' : 'success')}
      <div class="mt-16"></div>
    ` + sectionCard('Proveedores IA', renderDataTable(
      [
        { label: 'Proveedor', render: r => `<strong>${r.display_name || r.provider_name}</strong>` },
        { label: 'Nombre', key: 'provider_name', class: 'mono' },
        { label: 'Estado', render: r => badge(r.status, r.status) },
        { label: 'Default', render: r => r.is_default ? '⭐' : '' },
        { label: 'Prioridad', key: 'priority' },
        { label: 'API Key', render: r => r.has_api_key ? badge('Configurada', 'active') : badge('Pendiente', 'pending') },
        { label: 'Scope', render: r => r.tenant_id ? badge('Tenant', 'info') : badge('Global', 'warning') },
        { label: 'Acciones', render: r => `
          <button class="btn btn-sm btn-secondary" onclick="editProvider('${r.id}')">Editar</button>
          <button class="btn btn-sm btn-icon" onclick="checkProviderHealth('${r.id}')" title="Health check">💚</button>
          <button class="btn btn-sm btn-danger" onclick="deleteProvider('${r.id}')">✕</button>
        ` },
      ], provs), '',
      `<button class="btn btn-sm btn-primary" onclick="showCreateProvider()">+ Nuevo Proveedor</button>`
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});

async function showCreateProvider() {
  Modal.open('Nuevo Proveedor', `
    <div class="form-group"><label>Nombre del proveedor</label><select class="form-select" id="pv_name"><option>google</option><option>openai</option><option>anthropic</option><option>mistral</option></select></div>
    <div class="form-group"><label>Display Name</label><input class="form-input" id="pv_display"></div>
    <div class="form-group"><label>API Key</label><input class="form-input" id="pv_key" type="password" placeholder="sk-..."></div>
    <div class="form-group"><label>Config Name</label><input class="form-input" id="pv_config" value="default"></div>
    <div class="form-group"><label>Prioridad</label><input class="form-input" id="pv_priority" type="number" value="100"></div>
    <div class="form-group"><label>Default</label><select class="form-select" id="pv_default"><option value="false">No</option><option value="true">Sí</option></select></div>
    <div class="form-group"><label>Budget mensual (USD)</label><input class="form-input" id="pv_budget" type="number" step="0.01"></div>
    ${isSuperadmin() ? '<div class="form-group"><label>Global</label><select class="form-select" id="pv_global"><option value="false">Solo mi tenant</option><option value="true">Global</option></select></div>' : ''}
  `, `<button class="btn btn-primary" onclick="doCreateProvider()">Crear</button>
      <button class="btn btn-secondary" onclick="Modal.close()">Cancelar</button>`);
}

async function doCreateProvider() {
  try {
    await API.providers.create({
      provider_name: document.getElementById('pv_name').value,
      display_name: document.getElementById('pv_display').value,
      api_key: document.getElementById('pv_key').value || null,
      config_name: document.getElementById('pv_config').value || 'default',
      priority: parseInt(document.getElementById('pv_priority').value) || 100,
      is_default: document.getElementById('pv_default').value === 'true',
      monthly_budget_usd: parseFloat(document.getElementById('pv_budget').value) || null,
      is_global: document.getElementById('pv_global')?.value === 'true' || false,
    });
    Modal.close(); Toast.success('Proveedor creado'); navigate();
  } catch (e) { Toast.error(e.message); }
}

async function editProvider(id) {
  Modal.open('Editar Proveedor', `
    <div class="form-group"><label>API Key (dejar vacío para no cambiar)</label><input class="form-input" id="ep_key" type="password"></div>
    <div class="form-group"><label>Estado</label><select class="form-select" id="ep_status"><option>active</option><option>inactive</option><option>pending_setup</option></select></div>
    <div class="form-group"><label>Prioridad</label><input class="form-input" id="ep_priority" type="number" value="100"></div>
    <div class="form-group"><label>Default</label><select class="form-select" id="ep_default"><option value="">Sin cambio</option><option value="true">Sí</option><option value="false">No</option></select></div>
    <div class="form-group"><label>Budget mensual (USD)</label><input class="form-input" id="ep_budget" type="number" step="0.01"></div>
  `, `<button class="btn btn-primary" onclick="doUpdateProvider('${id}')">Guardar</button>
      <button class="btn btn-secondary" onclick="Modal.close()">Cancelar</button>`);
}

async function doUpdateProvider(id) {
  const body = {};
  const key = document.getElementById('ep_key').value;
  if (key) body.api_key = key;
  body.status = document.getElementById('ep_status').value;
  body.priority = parseInt(document.getElementById('ep_priority').value);
  const def = document.getElementById('ep_default').value;
  if (def !== '') body.is_default = def === 'true';
  const budget = document.getElementById('ep_budget').value;
  if (budget) body.monthly_budget_usd = parseFloat(budget);
  try {
    await API.providers.update(id, body);
    Modal.close(); Toast.success('Proveedor actualizado'); navigate();
  } catch (e) { Toast.error(e.message); }
}

async function checkProviderHealth(id) {
  Toast.info('Comprobando health...');
  try {
    const data = await API.providers.health(id);
    const h = data.health;
    if (h.ok) Toast.success(`${data.provider_name}: OK (${h.latency_ms}ms)`);
    else Toast.error(`${data.provider_name}: ${h.error}`);
  } catch (e) { Toast.error(e.message); }
}

async function deleteProvider(id) {
  if (!confirm('¿Eliminar este proveedor?')) return;
  try {
    await API.providers.del(id);
    Toast.success('Proveedor eliminado'); navigate();
  } catch (e) { Toast.error(e.message); }
}
