/**
 * components.js — Componentes reutilizables del panel admin
 * DataTable, Toast, Modal, StatusBadge, FilterBar, EmptyState, Loading
 */

/* ═══ Toast ═══ */
const Toast = {
  _container() { return document.getElementById('toastContainer'); },
  show(msg, type = 'info', duration = 4000) {
    const c = this._container(); if (!c) return;
    const t = document.createElement('div');
    t.className = `toast toast-${type}`;
    t.textContent = msg;
    c.appendChild(t);
    setTimeout(() => { t.style.opacity = '0'; setTimeout(() => t.remove(), 300); }, duration);
  },
  success(msg) { this.show(msg, 'success'); },
  error(msg) { this.show(msg, 'error', 6000); },
  info(msg) { this.show(msg, 'info'); },
};

/* ═══ Modal ═══ */
const Modal = {
  open(title, bodyHtml, footerHtml = '') {
    this.close();
    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.id = 'modalOverlay';
    overlay.onclick = (e) => { if (e.target === overlay) this.close(); };
    overlay.innerHTML = `
      <div class="modal">
        <div class="modal-header">
          <h3>${title}</h3>
          <button class="close-btn" onclick="Modal.close()">✕</button>
        </div>
        <div class="modal-body">${bodyHtml}</div>
        ${footerHtml ? `<div class="modal-footer">${footerHtml}</div>` : ''}
      </div>`;
    document.body.appendChild(overlay);
  },
  close() {
    const o = document.getElementById('modalOverlay');
    if (o) o.remove();
  },
};

/* ═══ DataTable ═══ */
function renderDataTable(columns, rows, opts = {}) {
  if (!rows || rows.length === 0) {
    return `<div class="empty-state">
      <div class="icon">${opts.emptyIcon || '📭'}</div>
      <h4>${opts.emptyTitle || 'Sin datos'}</h4>
      <p>${opts.emptyText || 'No hay registros que mostrar.'}</p>
    </div>`;
  }
  const ths = columns.map(c => `<th>${c.label}</th>`).join('');
  const trs = rows.map(row => {
    const tds = columns.map(c => {
      let val = typeof c.render === 'function' ? c.render(row) : (row[c.key] ?? '—');
      const cls = c.class || '';
      return `<td class="${cls}">${val}</td>`;
    }).join('');
    return `<tr>${tds}</tr>`;
  }).join('');
  return `<table class="data-table"><thead><tr>${ths}</tr></thead><tbody>${trs}</tbody></table>`;
}

/* ═══ Status Badge ═══ */
function badge(text, type = 'info') {
  const map = {
    active: 'badge-active', pass: 'badge-pass', ok: 'badge-active',
    inactive: 'badge-inactive', error: 'badge-error', fail: 'badge-error',
    warning: 'badge-warning', pending: 'badge-pending', pending_setup: 'badge-pending',
    info: 'badge-info',
  };
  return `<span class="badge ${map[type] || map[text] || 'badge-info'}">${text}</span>`;
}

/* ═══ Loading / Empty / Error states ═══ */
function loadingHtml(msg = 'Cargando...') {
  return `<div class="loading-state"><div class="spinner"></div><p>${msg}</p></div>`;
}
function emptyHtml(icon, title, text) {
  return `<div class="empty-state"><div class="icon">${icon}</div><h4>${title}</h4><p>${text}</p></div>`;
}
function errorHtml(msg) {
  return `<div class="error-state"><div class="icon">⚠️</div><h4>Error</h4><p>${msg}</p></div>`;
}

/* ═══ Section Card ═══ */
function sectionCard(title, bodyHtml, toolbarHtml = '', actionHtml = '') {
  return `<div class="section-card">
    <div class="section-header">
      <span class="section-title">${title}</span>
      ${actionHtml}
    </div>
    ${toolbarHtml ? `<div class="toolbar">${toolbarHtml}</div>` : ''}
    <div class="section-body">${bodyHtml}</div>
  </div>`;
}

/* ═══ KPI Card ═══ */
function kpiCard(label, value, sub = '', type = '') {
  return `<div class="kpi-card ${type}">
    <div class="kpi-label">${label}</div>
    <div class="kpi-value">${value}</div>
    ${sub ? `<div class="kpi-sub">${sub}</div>` : ''}
  </div>`;
}

/* ═══ Toolbar ═══ */
function searchInput(placeholder = 'Buscar...', id = 'searchInput') {
  return `<input type="text" class="search-input" id="${id}" placeholder="${placeholder}">`;
}

/* ═══ Helpers ═══ */
function formatDate(d) {
  if (!d) return '—';
  const dt = new Date(d);
  return dt.toLocaleDateString('es-ES', { day:'2-digit', month:'short', year:'numeric' })
    + ' ' + dt.toLocaleTimeString('es-ES', { hour:'2-digit', minute:'2-digit' });
}
function shortId(id) {
  if (!id) return '—';
  return String(id).substring(0, 8) + '…';
}
function money(v) {
  if (v == null) return '—';
  return '$' + Number(v).toFixed(2);
}
