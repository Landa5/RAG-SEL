registerPage('chat', async (el) => {
  el.innerHTML = `
    <div class="chat-container">
      <div class="chat-messages" id="chatMessages">
        <div class="chat-msg bot">¡Hola! Soy el asistente RAG-SEL. Pregúntame lo que necesites sobre tus datos.</div>
      </div>
      <div class="chat-input-area">
        <input type="text" id="chatInput" placeholder="Escribe tu pregunta..." autofocus
               onkeydown="if(event.key==='Enter')sendChat()">
        <button onclick="sendChat()" id="chatSendBtn">Enviar</button>
      </div>
    </div>
  `;
});

const _chatHistory = [];

async function sendChat() {
  const input = document.getElementById('chatInput');
  const msgs = document.getElementById('chatMessages');
  const btn = document.getElementById('chatSendBtn');
  const q = input.value.trim();
  if (!q) return;

  // User message
  msgs.innerHTML += `<div class="chat-msg user">${escapeHtml(q)}</div>`;
  msgs.innerHTML += `<div class="chat-msg status" id="chatStatus">Procesando...</div>`;
  input.value = ''; btn.disabled = true;
  msgs.scrollTop = msgs.scrollHeight;

  _chatHistory.push({ role: 'user', content: q });

  try {
    const res = await fetch('/chat/stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: q, chat_history: _chatHistory.slice(-6) }),
    });

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let botMsg = '';
    const statusEl = document.getElementById('chatStatus');
    if (statusEl) statusEl.remove();

    const botEl = document.createElement('div');
    botEl.className = 'chat-msg bot';
    msgs.appendChild(botEl);

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value);
      for (const line of chunk.split('\n')) {
        if (!line.startsWith('data: ')) continue;
        const payload = line.slice(6).trim();
        if (payload === '[DONE]') break;
        try {
          const d = JSON.parse(payload);
          if (d.token) { botMsg += d.token; botEl.textContent = botMsg; }
          else if (d.answer) { botMsg = d.answer; botEl.textContent = botMsg; }
          else if (d.status) { botEl.textContent = botMsg || d.status; }
        } catch (e) { /* skip */ }
      }
      msgs.scrollTop = msgs.scrollHeight;
    }
    _chatHistory.push({ role: 'assistant', content: botMsg });
  } catch (e) {
    const statusEl = document.getElementById('chatStatus');
    if (statusEl) statusEl.remove();
    msgs.innerHTML += `<div class="chat-msg bot" style="color:var(--danger)">Error: ${e.message}</div>`;
  }
  btn.disabled = false;
  input.focus();
}

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
