registerPage('executions', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.panel.executions('limit=100');
    const logs = data.executions || [];
    el.innerHTML = sectionCard('Historial de Ejecuciones', renderDataTable(
      [
        { label: 'Fecha', render: r => formatDate(r.created_at) },
        { label: 'Pipeline', render: r => badge(r.pipeline || '—', 'info') },
        { label: 'Query', render: r => `<span class="truncate">${r.question || '—'}</span>`, class: 'truncate' },
        { label: 'Modelo', key: 'model_used', class: 'mono' },
        { label: 'Tokens', render: r => ((r.tokens_in||0) + (r.tokens_out||0)) || '—' },
        { label: 'Coste', render: r => money(r.cost_usd) },
        { label: 'Latencia', render: r => r.latency_ms ? r.latency_ms + 'ms' : '—' },
        { label: 'SQL', render: r => r.sql_executed ? '✅' : '' },
        { label: 'RAG', render: r => r.retrieval_executed ? '✅' : '' },
        { label: 'Forecast', render: r => r.forecast_engine_executed ? '✅' : '' },
      ], logs, { emptyIcon: '⚡', emptyTitle: 'Sin ejecuciones', emptyText: 'Aún no hay queries.' })
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});
