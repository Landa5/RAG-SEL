/**
 * app.js — Router SPA, sidebar, inicialización del panel admin
 * Hash-based routing: #dashboard, #chat, #tenants, etc.
 */

let APP_USER = null; // usuario autenticado actual
let APP_TENANT_FILTER = null; // tenant seleccionado por superadmin

const ROUTES = [
  // { id, label, icon, page (render fn), section, superadminOnly }
  { id: 'dashboard',    label: 'Dashboard',       icon: '📊', section: 'Principal' },
  { id: 'chat',         label: 'Asistente',       icon: '💬', section: 'Principal' },
  { id: 'documents',    label: 'Documentos',      icon: '📄', section: 'Datos' },
  { id: 'executions',   label: 'Ejecuciones',     icon: '⚡', section: 'Datos' },
  { id: 'reviews',      label: 'Reviews IA',      icon: '🔍', section: 'Datos' },
  { id: 'predictions',  label: 'Predicciones',    icon: '📈', section: 'Datos' },
  { id: 'providers',    label: 'Proveedores IA',  icon: '🤖', section: 'Configuración' },
  { id: 'models',       label: 'Modelos',         icon: '🧩', section: 'Configuración' },
  { id: 'routing',      label: 'Routing',         icon: '🔀', section: 'Configuración' },
  { id: 'pipelines',    label: 'Pipelines',       icon: '🔧', section: 'Configuración' },
  { id: 'tenants',      label: 'Tenants',         icon: '🏢', section: 'Administración', superadminOnly: true },
  { id: 'apps',         label: 'Apps conectadas', icon: '🔗', section: 'Administración' },
  { id: 'credentials',  label: 'Credenciales',    icon: '🔑', section: 'Administración' },
  { id: 'scopes',       label: 'Scopes',          icon: '🛡️', section: 'Administración' },
  { id: 'usage',        label: 'Uso y límites',   icon: '📉', section: 'Operaciones' },
  { id: 'alerts',       label: 'Alertas',         icon: '🔔', section: 'Operaciones' },
  { id: 'health',       label: 'Health',          icon: '💚', section: 'Operaciones' },
];

// Mapa de funciones de renderizado por página
const PAGE_RENDERERS = {};
function registerPage(id, renderFn) { PAGE_RENDERERS[id] = renderFn; }

// ── Init ──
document.addEventListener('DOMContentLoaded', async () => {
  try {
    const data = await API.auth.me();
    APP_USER = data;
    buildSidebar();
    buildFooter();
    setupTenantSelector();
    window.addEventListener('hashchange', navigate);
    navigate();
  } catch (e) {
    window.location.href = '/app/login';
  }
});

// ── Build Sidebar ──
function buildSidebar() {
  const nav = document.getElementById('sidebarNav');
  const sections = {};
  ROUTES.forEach(r => {
    if (r.superadminOnly && APP_USER.role !== 'superadmin') return;
    if (!sections[r.section]) sections[r.section] = [];
    sections[r.section].push(r);
  });

  let html = '';
  Object.entries(sections).forEach(([section, items]) => {
    html += `<div class="nav-section">
      <div class="nav-section-title">${section}</div>
      ${items.map(r => `
        <div class="nav-item" data-route="${r.id}" onclick="goTo('${r.id}')">
          <span class="icon">${r.icon}</span>
          <span>${r.label}</span>
        </div>
      `).join('')}
    </div>`;
  });
  nav.innerHTML = html;
}

// ── Sidebar Footer ──
function buildFooter() {
  const footer = document.getElementById('sidebarFooter');
  const initials = (APP_USER.display_name || APP_USER.username || '?').substring(0, 2).toUpperCase();
  const roleLabel = APP_USER.role === 'superadmin' ? 'Super Admin' : 'Admin';
  footer.innerHTML = `
    <div class="avatar">${initials}</div>
    <div class="user-info">
      <div class="user-name">${APP_USER.display_name || APP_USER.username}</div>
      <div class="user-role">${roleLabel}${APP_USER.tenant_name ? ' · ' + APP_USER.tenant_name : ''}</div>
    </div>
    <button class="logout-btn" title="Cerrar sesión" onclick="doLogout()">⏻</button>
  `;
}

// ── Tenant Selector (solo superadmin) ──
async function setupTenantSelector() {
  if (APP_USER.role !== 'superadmin') return;
  const sel = document.getElementById('tenantSelector');
  sel.classList.remove('hidden');
  try {
    const data = await API.tenants.list();
    const select = document.getElementById('tenantSelect');
    select.innerHTML = '<option value="">Todos los tenants</option>';
    (data.tenants || []).forEach(t => {
      select.innerHTML += `<option value="${t.id}">${t.name}</option>`;
    });
    select.onchange = () => {
      APP_TENANT_FILTER = select.value || null;
      navigate(); // re-render current page
    };
  } catch (e) { /* ignore */ }
}

// ── Navigation ──
function goTo(route) {
  window.location.hash = '#' + route;
}

function navigate() {
  const hash = (window.location.hash || '#dashboard').replace('#', '');
  const route = ROUTES.find(r => r.id === hash) || ROUTES[0];

  // Access check
  if (route.superadminOnly && APP_USER.role !== 'superadmin') {
    goTo('dashboard');
    return;
  }

  // Update sidebar active
  document.querySelectorAll('.nav-item').forEach(el => {
    el.classList.toggle('active', el.dataset.route === route.id);
  });

  // Update header
  document.getElementById('pageTitle').textContent = route.label;
  document.getElementById('breadcrumb').textContent = `${route.section} / ${route.label}`;

  // Render page
  const content = document.getElementById('pageContent');
  content.innerHTML = loadingHtml();

  const renderer = PAGE_RENDERERS[route.id];
  if (renderer) {
    try { renderer(content); } catch (e) {
      content.innerHTML = errorHtml(e.message);
    }
  } else {
    content.innerHTML = emptyHtml('🚧', 'En desarrollo', `El módulo "${route.label}" se está implementando.`);
  }
}

// ── Helpers ──
function getEffectiveTenantId() {
  if (APP_USER.role === 'superadmin') return APP_TENANT_FILTER;
  return APP_USER.tenant_id;
}

function isSuperadmin() {
  return APP_USER && APP_USER.role === 'superadmin';
}

async function doLogout() {
  try {
    await API.auth.logout();
  } catch (e) { /* ignore */ }
  window.location.href = '/app/login';
}
