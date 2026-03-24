registerPage('dashboard', async (el) => {
  el.innerHTML = loadingHtml('Cargando dashboard...');
  try {
    const data = await API.dashboard.stats();
    const s = data || {};
    el.innerHTML = `
      <div class="kpi-grid">
        ${kpiCard('Tenants Activos', s.tenants_active ?? '—', '', 'accent')}
        ${kpiCard('Apps Conectadas', s.apps_active ?? '—', '', 'info')}
        ${kpiCard('Queries Hoy', s.queries_today ?? '—', '', 'success')}
        ${kpiCard('Queries Mes', s.queries_month ?? '—', '', '')}
        ${kpiCard('Coste Mensual', money(s.cost_month), '', s.cost_month > 50 ? 'warning' : '')}
        ${kpiCard('Proveedores', s.providers_active ?? '—', s.providers_error ? s.providers_error + ' con error' : '', s.providers_error ? 'danger' : 'success')}
        ${kpiCard('Documentos', s.total_docs ?? '—', '', '')}
        ${kpiCard('Reviews Pendientes', s.pending_reviews ?? '—', '', s.pending_reviews > 0 ? 'warning' : 'success')}
      </div>
      ${sectionCard('Últimas Ejecuciones', s.recent_executions ? renderDataTable(
        [
          { label: 'Fecha', render: r => formatDate(r.created_at) },
          { label: 'Pipeline', key: 'pipeline' },
          { label: 'Query', render: r => `<span class="truncate">${r.question || '—'}</span>`, class: 'truncate' },
          { label: 'Coste', render: r => money(r.cost_usd) },
          { label: 'Latencia', render: r => (r.latency_ms || '—') + 'ms' },
        ],
        s.recent_executions.slice(0, 10)
      ) : emptyHtml('⚡', 'Sin ejecuciones', 'Aún no hay queries registradas.'))}
      ${s.recent_alerts && s.recent_alerts.length ? sectionCard('Alertas Recientes', renderDataTable(
        [
          { label: 'Tipo', render: r => badge(r.type, r.type) },
          { label: 'Mensaje', key: 'message' },
          { label: 'Fecha', render: r => formatDate(r.created_at) },
        ], s.recent_alerts.slice(0, 5)
      )) : ''}
    `;
  } catch (e) {
    el.innerHTML = errorHtml(e.message);
  }
});
