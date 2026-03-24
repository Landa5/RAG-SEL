/**
 * api.js — Cliente API centralizado para el panel admin
 * Todas las llamadas a /admin/v1, /api/v1 y /app/auth pasan por aquí.
 * Usa cookies de sesión automáticas (no headers manuales).
 */
const API = {
  /** Fetch genérico con manejo de errores */
  async request(url, opts = {}) {
    const defaults = {
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', ...opts.headers },
    };
    if (opts.body && typeof opts.body === 'object' && !(opts.body instanceof FormData)) {
      opts.body = JSON.stringify(opts.body);
    }
    if (opts.body instanceof FormData) {
      delete defaults.headers['Content-Type'];
    }
    try {
      const res = await fetch(url, { ...defaults, ...opts });
      if (res.status === 401) {
        window.location.href = '/app/login';
        return null;
      }
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.detail || `Error ${res.status}`);
      }
      return data;
    } catch (e) {
      if (e.message === 'Failed to fetch') {
        Toast.error('Error de conexión con el servidor');
      }
      throw e;
    }
  },

  get(url) { return this.request(url); },
  post(url, body) { return this.request(url, { method: 'POST', body }); },
  patch(url, body) { return this.request(url, { method: 'PATCH', body }); },
  put(url, body) { return this.request(url, { method: 'PUT', body }); },
  del(url) { return this.request(url, { method: 'DELETE' }); },

  // ── Auth ──
  auth: {
    me() { return API.get('/app/auth/me'); },
    logout() { return API.post('/app/auth/logout'); },
  },

  // ── Dashboard ──
  dashboard: {
    stats() { return API.get('/app/panel/dashboard'); },
  },

  // ── Tenants ──
  tenants: {
    list(active = true) { return API.get(`/admin/v1/tenants?active_only=${active}`); },
    get(id) { return API.get(`/admin/v1/tenants/${id}`); },
    create(data) { return API.post('/admin/v1/tenants', data); },
    update(id, data) { return API.patch(`/admin/v1/tenants/${id}`, data); },
    usage(id) { return API.get(`/admin/v1/tenants/${id}/usage`); },
  },

  // ── Apps ──
  apps: {
    list(tenantId) { return API.get(`/admin/v1/tenants/${tenantId}/apps`); },
    create(tenantId, data) { return API.post(`/admin/v1/tenants/${tenantId}/apps`, data); },
    update(id, data) { return API.patch(`/admin/v1/apps/${id}`, data); },
    deactivate(id) { return API.del(`/admin/v1/apps/${id}`); },
  },

  // ── Credentials ──
  credentials: {
    generate(appId) { return API.post(`/admin/v1/apps/${appId}/credentials`); },
    rotate(appId) { return API.post(`/admin/v1/apps/${appId}/rotate-key`); },
  },

  // ── Scopes ──
  scopes: {
    get(appId) { return API.get(`/admin/v1/apps/${appId}/scopes`); },
    set(appId, scopes) { return API.put(`/admin/v1/apps/${appId}/scopes`, { scopes }); },
  },

  // ── Providers ──
  providers: {
    list() { return API.get('/admin/v1/providers'); },
    create(data) { return API.post('/admin/v1/providers', data); },
    update(id, data) { return API.patch(`/admin/v1/providers/${id}`, data); },
    del(id) { return API.del(`/admin/v1/providers/${id}`); },
    health(id) { return API.get(`/admin/v1/providers/${id}/health`); },
  },

  // ── Reviews ──
  reviews: {
    list(params = '') { return API.get(`/admin/v1/reviews?${params}`); },
    stats(days = 30) { return API.get(`/admin/v1/reviews/stats?days=${days}`); },
    pending(limit = 50) { return API.get(`/admin/v1/reviews/pending?limit=${limit}`); },
    humanReview(id, verdict, notes = '', promote = false) {
      return API.post(`/admin/v1/reviews/${id}/human-review?human_verdict=${verdict}&human_notes=${encodeURIComponent(notes)}&promote=${promote}`);
    },
    approveTraining(id) { return API.post(`/admin/v1/reviews/${id}/approve-training`); },
  },

  // ── Routing / Models / Pipelines ──
  routing: {
    logs(limit = 50) { return API.get(`/admin/api/routing/logs?limit=${limit}`); },
    models() { return API.get('/admin/api/routing/models'); },
    pipelines() { return API.get('/admin/api/routing/pipelines'); },
    rules() { return API.get('/admin/api/routing/rules'); },
    predictions(limit = 20) { return API.get(`/admin/api/routing/predictions?limit=${limit}`); },
    sync() { return API.post('/admin/api/routing/sync'); },
    recalculate() { return API.post('/admin/api/routing/recalculate-metrics'); },
  },

  // ── Panel endpoints ──
  panel: {
    executions(params = '') { return API.get(`/app/panel/executions?${params}`); },
    documents(params = '') { return API.get(`/app/panel/documents?${params}`); },
    deleteDoc(id) { return API.del(`/app/panel/documents/${id}`); },
    alerts() { return API.get('/app/panel/alerts'); },
    health() { return API.get('/app/panel/health'); },
    audit(params = '') { return API.get(`/app/panel/audit?${params}`); },
  },

  // ── Upload ──
  upload: {
    file(formData) {
      return API.request('/upload', { method: 'POST', body: formData });
    },
  },
};
