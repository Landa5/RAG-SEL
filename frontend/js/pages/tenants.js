registerPage('tenants', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.tenants.list(false);
    const tenants = data.tenants || [];
    el.innerHTML = sectionCard('Tenants', renderDataTable(
      [
        { label: 'Nombre', render: r => `<strong>${r.name}</strong>` },
        { label: 'Slug', key: 'slug', class: 'mono' },
        { label: 'Estado', render: r => badge(r.active ? 'active' : 'inactive', r.active ? 'active' : 'inactive') },
        { label: 'Docs Max', key: 'max_documents' },
        { label: 'Queries/día', key: 'max_queries_per_day' },
        { label: 'Timezone', key: 'timezone' },
        { label: 'Creado', render: r => formatDate(r.created_at) },
        { label: 'Acciones', render: r => `<button class="btn btn-sm btn-secondary" onclick="editTenant('${r.id}')">Editar</button>` },
      ], tenants), '',
      `<button class="btn btn-sm btn-primary" onclick="showCreateTenant()">+ Nuevo Tenant</button>`
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});

async function showCreateTenant() {
  Modal.open('Nuevo Tenant', `
    <div class="form-group"><label>Nombre</label><input class="form-input" id="tn_name"></div>
    <div class="form-group"><label>Slug</label><input class="form-input" id="tn_slug" placeholder="mi-empresa"></div>
    <div class="form-group"><label>Connection Ref</label><input class="form-input" id="tn_connref" placeholder="env:TENANT_DB_URL"></div>
    <div class="form-group"><label>Max documentos</label><input class="form-input" id="tn_maxdocs" type="number" value="500"></div>
    <div class="form-group"><label>Max queries/día</label><input class="form-input" id="tn_maxq" type="number" value="1000"></div>
  `, `<button class="btn btn-primary" onclick="doCreateTenant()">Crear</button>
      <button class="btn btn-secondary" onclick="Modal.close()">Cancelar</button>`);
}

async function doCreateTenant() {
  try {
    await API.tenants.create({
      name: document.getElementById('tn_name').value,
      slug: document.getElementById('tn_slug').value,
      connection_ref: document.getElementById('tn_connref').value || null,
      max_documents: parseInt(document.getElementById('tn_maxdocs').value) || 500,
      max_queries_per_day: parseInt(document.getElementById('tn_maxq').value) || 1000,
    });
    Modal.close(); Toast.success('Tenant creado'); navigate();
  } catch (e) { Toast.error(e.message); }
}

async function editTenant(id) {
  try {
    const data = await API.tenants.get(id);
    const t = data.tenant;
    Modal.open('Editar Tenant', `
      <div class="form-group"><label>Nombre</label><input class="form-input" id="et_name" value="${t.name}"></div>
      <div class="form-group"><label>Max documentos</label><input class="form-input" id="et_maxdocs" type="number" value="${t.max_documents}"></div>
      <div class="form-group"><label>Max queries/día</label><input class="form-input" id="et_maxq" type="number" value="${t.max_queries_per_day}"></div>
      <div class="form-group"><label>Activo</label><select class="form-select" id="et_active"><option value="true" ${t.active?'selected':''}>Sí</option><option value="false" ${!t.active?'selected':''}>No</option></select></div>
    `, `<button class="btn btn-primary" onclick="doUpdateTenant('${id}')">Guardar</button>
        <button class="btn btn-secondary" onclick="Modal.close()">Cancelar</button>`);
  } catch (e) { Toast.error(e.message); }
}

async function doUpdateTenant(id) {
  try {
    await API.tenants.update(id, {
      name: document.getElementById('et_name').value,
      max_documents: parseInt(document.getElementById('et_maxdocs').value),
      max_queries_per_day: parseInt(document.getElementById('et_maxq').value),
      active: document.getElementById('et_active').value === 'true',
    });
    Modal.close(); Toast.success('Tenant actualizado'); navigate();
  } catch (e) { Toast.error(e.message); }
}
