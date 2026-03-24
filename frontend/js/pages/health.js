registerPage('health', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.panel.health();
    const h = data || {};
    el.innerHTML = `
      <div class="kpi-grid">
        ${kpiCard('API', h.api || '—', '', (h.api==='ok'?'success':'danger'))}
        ${kpiCard('Base de Datos', h.database || '—', h.db_latency_ms ? h.db_latency_ms + 'ms' : '', (h.database==='ok'?'success':'danger'))}
        ${kpiCard('Qdrant', h.qdrant || '—', h.qdrant_collections ? h.qdrant_collections + ' collections' : '', (h.qdrant==='ok'?'success':'danger'))}
        ${kpiCard('Proveedores IA', h.providers_status || '—', h.providers_active + ' activos, ' + (h.providers_error||0) + ' con error', h.providers_error ? 'warning' : 'success')}
      </div>
      ${h.providers_detail ? sectionCard('Detalle Proveedores', renderDataTable(
        [
          { label: 'Proveedor', key: 'provider_name' },
          { label: 'Estado', render: r => badge(r.status, r.status) },
          { label: 'Último Check', render: r => formatDate(r.last_health_check) },
          { label: 'Último Error', render: r => r.last_error ? `<span class="text-danger text-xs">${r.last_error}</span>` : '—' },
        ], h.providers_detail)
      ) : ''}
    `;
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});
