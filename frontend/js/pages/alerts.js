registerPage('alerts', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.panel.alerts();
    const alerts = data.alerts || [];
    el.innerHTML = sectionCard('Alertas del Sistema', renderDataTable(
      [
        { label: 'Tipo', render: r => badge(r.type || '—', r.severity === 'critical' ? 'error' : r.severity === 'warning' ? 'warning' : 'info') },
        { label: 'Severidad', render: r => badge(r.severity || '—', r.severity) },
        { label: 'Origen', key: 'source' },
        { label: 'Mensaje', render: r => `<span class="truncate">${r.message || '—'}</span>`, class: 'truncate' },
        { label: 'Tenant', render: r => r.tenant_name || shortId(r.tenant_id) || 'Global' },
        { label: 'Fecha', render: r => formatDate(r.created_at) },
      ], alerts, { emptyIcon: '🔔', emptyTitle: 'Sin alertas', emptyText: 'No hay alertas activas. ¡Todo en orden!' })
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});
