registerPage('predictions', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const data = await API.routing.predictions(50);
    const runs = data.predictions || [];
    el.innerHTML = sectionCard('Predicciones', renderDataTable(
      [
        { label: 'Fecha', render: r => formatDate(r.created_at) },
        { label: 'Método', render: r => badge(r.method || '—', 'info') },
        { label: 'Confianza', render: r => r.confidence != null ? `${(r.confidence*100).toFixed(0)}%` : '—' },
        { label: 'Query', render: r => `<span class="truncate">${r.question || '—'}</span>`, class: 'truncate' },
        { label: 'Resultado', render: r => `<span class="truncate">${r.prediction || '—'}</span>`, class: 'truncate' },
        { label: 'Warnings', render: r => (r.warnings||[]).length ? badge(r.warnings.length + ' warn', 'warning') : '' },
      ], runs, { emptyIcon: '📈', emptyTitle: 'Sin predicciones', emptyText: 'No hay prediction runs.' })
    );
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});
