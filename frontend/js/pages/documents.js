registerPage('documents', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.panel.documents();
    const docs = data.documents || [];
    el.innerHTML = `
      <div class="section-card"><div class="section-header"><span class="section-title">Subir Documento</span></div>
        <div class="section-body" style="padding:20px">
          <input type="file" id="docFileInput" accept=".pdf" style="margin-right:12px">
          <button class="btn btn-primary btn-sm" onclick="uploadDoc()">Subir PDF</button>
          <span id="uploadStatus" class="text-sm text-muted" style="margin-left:12px"></span>
        </div></div>
    ` + sectionCard('Documentos Indexados', renderDataTable(
      [
        { label: 'Nombre', render: r => `<strong>${r.filename || r.name || '—'}</strong>` },
        { label: 'Tenant', render: r => r.tenant_name || shortId(r.tenant_id) },
        { label: 'Estado', render: r => badge(r.status || 'indexed', r.status || 'active') },
        { label: 'Tamaño', render: r => r.file_size_bytes ? (r.file_size_bytes/1024/1024).toFixed(1) + ' MB' : '—' },
        { label: 'Subido', render: r => formatDate(r.created_at) },
        { label: 'Acciones', render: r => `<button class="btn btn-sm btn-danger" onclick="deleteDoc('${r.id}')">Borrar</button>` },
      ], docs, { emptyIcon: '📄', emptyTitle: 'Sin documentos', emptyText: 'Sube un PDF para empezar.' })
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});

async function uploadDoc() {
  const input = document.getElementById('docFileInput');
  const status = document.getElementById('uploadStatus');
  if (!input.files.length) { Toast.error('Selecciona un archivo'); return; }
  status.textContent = 'Subiendo...';
  try {
    const fd = new FormData();
    fd.append('file', input.files[0]);
    const data = await API.upload.file(fd);
    status.textContent = data.message || 'OK';
    Toast.success('Documento subido'); setTimeout(navigate, 1500);
  } catch (e) { status.textContent = 'Error'; Toast.error(e.message); }
}

async function deleteDoc(id) {
  if (!confirm('¿Eliminar este documento?')) return;
  try { await API.panel.deleteDoc(id); Toast.success('Documento eliminado'); navigate(); }
  catch (e) { Toast.error(e.message); }
}
