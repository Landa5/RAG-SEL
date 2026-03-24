registerPage('credentials', async (el) => {
  el.innerHTML = `<div class="section-card"><div class="section-header"><span class="section-title">Credenciales</span></div>
    <div class="section-body" style="padding:20px">
      <p class="text-sm text-muted">Selecciona una app para gestionar sus credenciales.</p>
      <div class="form-group mt-16"><label>App ID</label><input class="form-input" id="cred_app_id" placeholder="UUID de la app"></div>
      <div class="flex gap-8">
        <button class="btn btn-primary" onclick="genCredential()">Generar nueva key</button>
        <button class="btn btn-danger" onclick="rotateCredential()">Rotar key</button>
      </div>
      <div id="credResult" class="mt-16"></div>
    </div></div>`;
});

async function genCredential() {
  const appId = document.getElementById('cred_app_id').value;
  if (!appId) { Toast.error('Introduce un App ID'); return; }
  try {
    const data = await API.credentials.generate(appId);
    document.getElementById('credResult').innerHTML = `
      <div style="background:var(--bg-dark);padding:16px;border-radius:8px">
        <p class="text-sm" style="margin-bottom:8px">⚠️ <strong>Guarda esta key ahora</strong>. No se mostrará de nuevo.</p>
        <code class="font-mono" style="word-break:break-all;font-size:0.85rem">${data.api_key}</code>
        <p class="text-xs text-muted" style="margin-top:8px">Prefijo: ${data.prefix}</p>
      </div>`;
    Toast.success('Key generada');
  } catch (e) { Toast.error(e.message); }
}

async function rotateCredential() {
  const appId = document.getElementById('cred_app_id').value;
  if (!appId) { Toast.error('Introduce un App ID'); return; }
  if (!confirm('¿Rotar key? La anterior se desactivará.')) return;
  try {
    const data = await API.credentials.rotate(appId);
    document.getElementById('credResult').innerHTML = `
      <div style="background:var(--bg-dark);padding:16px;border-radius:8px">
        <p class="text-sm" style="margin-bottom:8px">⚠️ Key rotada. <strong>Guarda la nueva ahora</strong>.</p>
        <code class="font-mono" style="word-break:break-all;font-size:0.85rem">${data.api_key}</code>
      </div>`;
    Toast.success('Key rotada');
  } catch (e) { Toast.error(e.message); }
}
