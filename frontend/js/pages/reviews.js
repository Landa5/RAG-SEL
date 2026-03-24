registerPage('reviews', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const [revData, statsData, pendingData] = await Promise.all([
      API.reviews.list('limit=50'),
      API.reviews.stats(),
      API.reviews.pending(10),
    ]);
    const reviews = revData.reviews || [];
    const stats = statsData.stats || {};
    const pending = pendingData.pending || [];

    el.innerHTML = `
      <div class="kpi-grid">
        ${kpiCard('Total Reviews', stats.total_reviews ?? '—', '')}
        ${kpiCard('Pass', stats.passed ?? '—', '', 'success')}
        ${kpiCard('Warning', stats.warnings ?? '—', '', 'warning')}
        ${kpiCard('Fail', stats.failed ?? '—', '', 'danger')}
        ${kpiCard('Pendiente Humana', stats.pending_human ?? pending.length, '', (stats.pending_human || pending.length) > 0 ? 'warning' : 'success')}
        ${kpiCard('Riesgo Crítico', stats.critical_risks ?? '—', '', (stats.critical_risks > 0) ? 'danger' : 'success')}
      </div>
      ${pending.length ? sectionCard('⚠️ Pendientes de Revisión Humana', renderDataTable(
        [
          { label: 'Fecha', render: r => formatDate(r.created_at) },
          { label: 'Riesgo', render: r => badge(r.risk_level || '—', r.risk_level === 'critical' ? 'error' : 'warning') },
          { label: 'Pipeline', key: 'pipeline_reviewed' },
          { label: 'Veredicto IA', render: r => badge(r.verdict || '—', r.verdict) },
          { label: 'Razón', key: 'human_review_reason', class: 'truncate' },
          { label: 'Acción', render: r => `
            <button class="btn btn-sm btn-primary" onclick="approveReview('${r.id}','confirmed_pass')">✓ Pass</button>
            <button class="btn btn-sm btn-danger" onclick="approveReview('${r.id}','confirmed_fail')">✕ Fail</button>` },
        ], pending)
      ) : ''}
      ${sectionCard('Últimas Reviews', renderDataTable(
        [
          { label: 'Fecha', render: r => formatDate(r.created_at) },
          { label: 'Veredicto', render: r => badge(r.verdict || '—', r.verdict) },
          { label: 'Riesgo', render: r => badge(r.risk_level || '—', r.risk_level === 'critical' ? 'error' : r.risk_level === 'high' ? 'warning' : 'info') },
          { label: 'Pipeline', key: 'pipeline_reviewed' },
          { label: 'Grounding', render: r => r.grounding_score != null ? r.grounding_score.toFixed(2) : '—' },
          { label: 'Issues', render: r => (r.issues||[]).join(', ') || '—', class: 'truncate' },
          { label: 'Status', render: r => badge(r.review_status||'raw', r.review_status === 'approved_training_candidate' ? 'active' : 'info') },
        ], reviews)
      )}
    `;
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});

async function approveReview(id, verdict) {
  try {
    await API.reviews.humanReview(id, verdict);
    Toast.success('Review procesada'); navigate();
  } catch (e) { Toast.error(e.message); }
}
