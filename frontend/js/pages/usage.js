registerPage('usage', async (el) => {
  el.innerHTML = loadingHtml();
  try {
    const tid = getEffectiveTenantId() || APP_USER.tenant_id;
    if (!tid) { el.innerHTML = emptyHtml('📉', 'Selecciona un tenant', 'Usa el selector.'); return; }
    const data = await API.tenants.usage(tid);
    const u = data.usage || {};
    el.innerHTML = `
      <div class="kpi-grid">
        ${kpiCard('Queries Hoy', u.queries_today ?? '—', `Límite: ${u.max_queries_per_day ?? '∞'}`, u.queries_today >= (u.max_queries_per_day||Infinity) ? 'danger' : 'accent')}
        ${kpiCard('Queries Mes', u.queries_month ?? '—', '', '')}
        ${kpiCard('Coste Este Mes', money(u.cost_month), `Budget: ${money(u.max_monthly_cost_usd)}`, u.cost_month >= (u.max_monthly_cost_usd||Infinity) ? 'danger' : 'success')}
        ${kpiCard('Documentos', u.total_docs ?? '—', `Límite: ${u.max_documents ?? '∞'}`, '')}
        ${kpiCard('Apps Activas', u.active_apps ?? '—', '', 'info')}
      </div>
    `;
  } catch (e) { el.innerHTML = errorHtml(e.message); }
});
