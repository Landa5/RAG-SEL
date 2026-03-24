registerPage('models', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.routing.models();
    const models = data.models || [];
    el.innerHTML = sectionCard('Modelos Disponibles', renderDataTable(
      [
        { label: 'Modelo', render: r => `<strong>${r.display_name || r.model_id}</strong>` },
        { label: 'ID', key: 'model_id', class: 'mono' },
        { label: 'Proveedor', key: 'provider', class: 'mono' },
        { label: 'Estado', render: r => badge(r.status || 'active', r.status || 'active') },
        { label: 'Avg Latencia', render: r => r.avg_latency_ms ? r.avg_latency_ms.toFixed(0) + 'ms' : '—' },
        { label: 'Req 7d', render: r => r.total_requests_7d ?? '—' },
        { label: 'Contexto', render: r => r.context_window ? (r.context_window / 1000).toFixed(0) + 'K' : '—' },
        { label: 'Precio In/Out', render: r => r.price_input != null ? `$${r.price_input}/$${r.price_output}` : '—' },
        { label: 'Arena', render: r => r.arena_scores?.text ? r.arena_scores.text : '—' },
      ], models, { emptyIcon: '🧩', emptyTitle: 'Sin modelos', emptyText: 'No hay modelos registrados en el motor de routing.' }),
      '', `<button class="btn btn-sm btn-secondary" onclick="API.routing.recalculate().then(()=>{Toast.success('Métricas recalculadas');navigate()}).catch(e=>Toast.error(e.message))">Recalcular Métricas</button>
          <button class="btn btn-sm btn-secondary" onclick="API.routing.sync().then(d=>{Toast.success('Sync completado');navigate()}).catch(e=>Toast.error(e.message))">🔄 Sync Leaderboard</button>`
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});
