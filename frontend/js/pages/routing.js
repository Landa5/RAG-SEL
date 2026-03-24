registerPage('routing', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const [rulesData, logsData] = await Promise.all([
      API.routing.rules(),
      API.routing.logs(30),
    ]);
    const rules = rulesData.rules || [];
    const logs = logsData.logs || [];

    el.innerHTML = sectionCard('Reglas de Routing', renderDataTable(
      [
        { label: 'Nombre', render: r => `<strong>${r.name || r.rule_name || '—'}</strong>` },
        { label: 'Condición', render: r => `<span class="truncate">${r.condition || '—'}</span>`, class: 'truncate' },
        { label: 'Modelo', key: 'target_model', class: 'mono' },
        { label: 'Peso', key: 'weight' },
        { label: 'Fallback', key: 'fallback_model', class: 'mono' },
        { label: 'Activa', render: r => r.active !== false ? '✅' : '❌' },
      ], rules, { emptyIcon: '🔀', emptyTitle: 'Sin reglas', emptyText: 'No hay reglas de routing.' }),
      '', `<button class="btn btn-sm btn-secondary" onclick="API.routing.sync().then(d=>{Toast.success('Sync: '+JSON.stringify(d.results||{}));navigate()}).catch(e=>Toast.error(e.message))">🔄 Sync Leaderboard</button>`
    ) + sectionCard('Últimas Decisiones de Routing', renderDataTable(
      [
        { label: 'Fecha', render: r => formatDate(r.created_at || r.timestamp) },
        { label: 'Pipeline', key: 'pipeline_id', class: 'mono' },
        { label: 'Modelo Seleccionado', key: 'selected_model', class: 'mono' },
        { label: 'Regla', key: 'rule_matched' },
        { label: 'Score', render: r => r.score != null ? r.score.toFixed(2) : '—' },
      ], logs)
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});
