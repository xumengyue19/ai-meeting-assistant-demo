/**
 * AI 会议助手安全代理（Cloudflare Workers 示例）
 * 环境变量：DEEPSEEK_API_KEY、ARK_API_KEY、ALLOWED_ORIGIN
 * 不要把任何 API Key 写进前端 HTML 或本文件。
 */
export default {
  async fetch(request, env) {
    const origin = request.headers.get('Origin') || '';
    const allowedOrigin = env.ALLOWED_ORIGIN || 'https://xumengyue19.github.io';
    const cors = {
      'Access-Control-Allow-Origin': allowedOrigin,
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'POST,OPTIONS',
      'Vary': 'Origin',
    };

    const url = new URL(request.url);
    if (url.pathname === '/asr-stream') {
      if (origin && origin !== allowedOrigin) return new Response('Origin not allowed', { status: 403 });
      return bridgeDoubaoAsr(request, env);
    }

    if (request.method === 'OPTIONS') {
      if (origin && origin !== allowedOrigin) return new Response('Origin not allowed', { status: 403, headers: cors });
      return new Response(null, { status: 204, headers: cors });
    }
    if (request.method === 'GET' && url.pathname === '/health') return json({ ok: true, asrConfigured: !!(env.VOLC_ASR_APP_ID && env.VOLC_ASR_ACCESS_TOKEN) }, 200, cors);
    if (request.method !== 'POST') return json({ error: 'Method not allowed' }, 405, cors);
    if (origin && origin !== allowedOrigin) return json({ error: 'Origin not allowed' }, 403, cors);

    try {
      const payload = await request.json();
      if (!['asr_polish', 'transcript_revise', 'minutes_generate'].includes(payload.task)) return json({ error: 'Unsupported task' }, 400, cors);

      const prompt = buildPrompt(payload);
      const provider = payload.provider === 'doubao' ? 'doubao' : 'deepseek';
      let upstream;

      if (provider === 'doubao') {
        if (!env.ARK_API_KEY) return json({ error: 'ARK_API_KEY is not configured' }, 500, cors);
        upstream = await fetch('https://ark.cn-beijing.volces.com/api/v3/responses', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${env.ARK_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ model: payload.model, input: prompt, thinking: { type: 'disabled' } }),
        });
      } else {
        if (!env.DEEPSEEK_API_KEY) return json({ error: 'DEEPSEEK_API_KEY is not configured' }, 500, cors);
        upstream = await fetch('https://api.deepseek.com/v1/chat/completions', {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${env.DEEPSEEK_API_KEY}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: payload.model || 'deepseek-v4-flash',
            messages: [
              { role: 'system', content: '你是严谨的中文会议智能助手。只输出合法 JSON，不得虚构转写中不存在的事实。' },
              { role: 'user', content: prompt },
            ],
            response_format: { type: 'json_object' },
            thinking: { type: 'disabled' },
            stream: false,
          }),
        });
      }

      const body = await upstream.text();
      return new Response(body, {
        status: upstream.status,
        headers: { ...cors, 'Content-Type': upstream.headers.get('Content-Type') || 'application/json' },
      });
    } catch (error) {
      return json({ error: error.message || 'Proxy request failed' }, 500, cors);
    }
  },
};

const MSG = { CLIENT_FULL: 1, CLIENT_AUDIO: 2, SERVER_FULL: 9, SERVER_ERROR: 15 };
const FLAG = { POS: 1, NEG_WITH_SEQ: 3 };
const SER = { NONE: 0, JSON: 1 };
const COMP = { NONE: 0, GZIP: 1 };

async function bridgeDoubaoAsr(request, env) {
  if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return new Response('WebSocket upgrade required', { status: 426 });
  if (!env.VOLC_ASR_APP_ID || !env.VOLC_ASR_ACCESS_TOKEN) return new Response('ASR credentials are not configured', { status: 503 });

  const pair = new WebSocketPair();
  const client = pair[0], browser = pair[1];
  browser.accept();
  let upstream = null, seq = 1, started = false, closing = false;

  const close = (code = 1000, reason = '') => {
    if (closing) return;
    closing = true;
    try { browser.close(code, reason); } catch {}
    try { upstream?.close(code, reason); } catch {}
  };
  const send = value => { try { browser.send(JSON.stringify(value)); } catch {} };

  const start = async (config = {}) => {
    if (started) return;
    started = true;
    try {
      const connectId = crypto.randomUUID();
      const response = await fetch('https://openspeech.bytedance.com/api/v3/sauc/bigmodel_async', {
        headers: {
          Upgrade: 'websocket',
          'X-Api-App-Key': env.VOLC_ASR_APP_ID,
          'X-Api-Access-Key': env.VOLC_ASR_ACCESS_TOKEN,
          'X-Api-Resource-Id': env.VOLC_ASR_RESOURCE_ID || 'volc.seedasr.sauc.duration',
          'X-Api-Connect-Id': connectId,
          'X-Api-Sequence': '-1',
        },
      });
      if (!response.webSocket) throw new Error(`豆包 ASR 建连失败（${response.status}）`);
      upstream = response.webSocket;
      upstream.accept();
      upstream.addEventListener('message', async event => {
        try {
          const result = await decodeServerFrame(event.data);
          if (result.error) { send({ type: 'error', message: result.error, code: result.code }); return; }
          const data = result.payload?.result || {};
          const text = data.text || '';
          if (text) send({ type: result.last ? 'final' : 'partial', text, utterances: data.utterances || [] });
          if (result.last) close();
        } catch (error) { send({ type: 'error', message: '无法解析豆包 ASR 返回结果' }); close(1011); }
      });
      upstream.addEventListener('close', () => { if (!closing) close(); });
      upstream.addEventListener('error', () => { send({ type: 'error', message: '豆包 ASR 连接异常' }); close(1011); });
      const requestPayload = {
        user: { uid: config.uid || 'meeting-demo' },
        audio: { format: 'pcm', codec: 'raw', rate: 16000, bits: 16, channel: 1 },
        request: { model_name: 'bigmodel', enable_itn: true, enable_punc: true, show_utterances: true, enable_nonstream: true, corpus: config.glossary?.length ? { context: config.glossary.join('，') } : undefined },
      };
      upstream.send(await fullFrame(seq++, requestPayload));
      send({ type: 'ready' });
    } catch (error) { send({ type: 'error', message: error.message || '无法连接豆包 ASR' }); close(1011); }
  };

  browser.addEventListener('message', async event => {
    try {
      if (typeof event.data === 'string') {
        const message = JSON.parse(event.data);
        if (message.type === 'start') return start(message);
        if (message.type === 'end') {
          if (upstream) upstream.send(await audioFrame(seq++, new Uint8Array(), true));
          return;
        }
        return;
      }
      if (!started) return;
      if (!upstream) return;
      const raw = event.data instanceof ArrayBuffer ? event.data : await event.data.arrayBuffer();
      const pcm = new Uint8Array(raw);
      upstream.send(await audioFrame(seq++, pcm, false));
    } catch (error) { send({ type: 'error', message: '音频发送失败' }); close(1011); }
  });
  browser.addEventListener('close', () => close());
  browser.addEventListener('error', () => close(1011));
  return new Response(null, { status: 101, webSocket: client });
}

function header(type, flags, serialization, compression) { return new Uint8Array([(1 << 4) | 1, (type << 4) | flags, (serialization << 4) | compression, 0]); }
function int32(value) { const data = new DataView(new ArrayBuffer(4)); data.setInt32(0, value, false); return new Uint8Array(data.buffer); }
function uint32(value) { const data = new DataView(new ArrayBuffer(4)); data.setUint32(0, value, false); return new Uint8Array(data.buffer); }
function concat(...items) { const length = items.reduce((n, item) => n + item.length, 0); const out = new Uint8Array(length); let offset = 0; for (const item of items) { out.set(item, offset); offset += item.length; } return out; }
async function gzip(data) { const stream = new CompressionStream('gzip'); const writer = stream.writable.getWriter(); await writer.write(data); await writer.close(); return new Uint8Array(await new Response(stream.readable).arrayBuffer()); }
async function gunzip(data) { const stream = new DecompressionStream('gzip'); const writer = stream.writable.getWriter(); await writer.write(data); await writer.close(); return new Uint8Array(await new Response(stream.readable).arrayBuffer()); }
async function fullFrame(sequence, body) { const packed = await gzip(new TextEncoder().encode(JSON.stringify(body))); return concat(header(MSG.CLIENT_FULL, FLAG.POS, SER.JSON, COMP.GZIP), int32(sequence), uint32(packed.length), packed); }
async function audioFrame(sequence, audio, last) { const packed = await gzip(audio); return concat(header(MSG.CLIENT_AUDIO, last ? FLAG.NEG_WITH_SEQ : FLAG.POS, SER.NONE, COMP.GZIP), int32(last ? -sequence : sequence), uint32(packed.length), packed); }
async function decodeServerFrame(data) {
  const bytes = new Uint8Array(data instanceof ArrayBuffer ? data : await data.arrayBuffer());
  if (bytes.length < 8) throw new Error('Invalid ASR frame');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength), type = bytes[1] >> 4, flags = bytes[1] & 15, serialization = bytes[2] >> 4, compression = bytes[2] & 15;
  let offset = (bytes[0] & 15) * 4;
  if (flags & 1) offset += 4;
  if (type === MSG.SERVER_ERROR) { const code = view.getInt32(offset, false); offset += 4; const len = view.getUint32(offset, false); let payload = bytes.slice(offset + 4, offset + 4 + len); if (compression === COMP.GZIP) payload = await gunzip(payload); return { error: new TextDecoder().decode(payload) || '豆包 ASR 返回错误', code }; }
  if (type !== MSG.SERVER_FULL) return {};
  const len = view.getUint32(offset, false); let payload = bytes.slice(offset + 4, offset + 4 + len); if (compression === COMP.GZIP) payload = await gunzip(payload); return { payload: serialization === SER.JSON ? JSON.parse(new TextDecoder().decode(payload)) : {}, last: !!(flags & 2) };
}

function buildPrompt(payload) {
  const schema = JSON.stringify(payload.responseSchema || {});
  if (payload.task === 'asr_polish') {
    return `${payload.instructions}\n术语词表：${JSON.stringify(payload.glossary || [])}\n返回结构示例：${schema}\n最近转写：${JSON.stringify(payload.recentTranscripts || [])}\n最新片段：${JSON.stringify(payload.latest || {})}`;
  }
  if (payload.task === 'transcript_revise') return `${payload.instructions}\n术语词表：${JSON.stringify(payload.glossary || [])}\n返回结构示例：${schema}\n全场转写：${JSON.stringify(payload.segments || [])}`;
  return `${payload.instructions}\n返回结构示例：${schema}\n会议数据：${JSON.stringify(payload.meeting || {})}`;
}

function json(value, status, headers) {
  return new Response(JSON.stringify(value), { status, headers: { ...headers, 'Content-Type': 'application/json' } });
}
