registerPage('pipelines', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.routing.pipelines();
    const pipes = data.pipelines || [];
    el.innerHTML = sectionCard('Pipelines Configurados', renderDataTable(
      [
        { label: 'Pipeline', render: r => `<strong>${r.display_name || r.id}</strong>` },
        { label: 'ID', key: 'id', class: 'mono' },
        { label: 'Modo', render: r => badge(r.execution_mode || '—', 'info') },
        { label: 'SQL', render: r => r.requires_sql ? '✅' : '—' },
        { label: 'RAG', render: r => r.requires_retrieval ? '✅' : '—' },
        { label: 'Tools', render: r => r.requires_tools ? '✅' : '—' },
        { label: 'Contexto Mín', render: r => r.min_context_window ? (r.min_context_window / 1000).toFixed(0) + 'K' : '—' },
        { label: 'Arena Mín', key: 'min_arena_score' },
        { label: 'Activo', render: r => r.enabled !== false ? '✅' : '❌' },
      ], pipes, { emptyIcon: '🔧', emptyTitle: 'Sin pipelines', emptyText: 'No hay pipelines configurados en el motor de routing.' })
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});
