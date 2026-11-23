// Мелстрой Рулетка — сервер комнат для кооператива «Отбейся от Интерпола» (до 4 игроков).
// Отдельный воркер: старый воркер с рейтингом и ботом не трогается.
// Каждая комната — Durable Object. Сервер ничего не считает: он только пересылает сообщения.
// Игру считает телефон хоста, остальные шлют ему ввод и получают состояние мира.
//
//   GET  /health                 — проверка: {"ok":true}
//   POST /room/new               — новая комната: {"code":"K7QXM"}
//   GET  /room/CODE/info         — кто в комнате: {"n":2,"state":"lobby","players":[…]}
//   WS   /room/CODE              — подключение к комнате (WebSocket)
//
// Кадры WebSocket (текст):
//   клиент → сервер:  "C{json}"  — служебное (hello, meta, state)
//                     "H…"       — переслать хосту
//                     "A…"       — переслать всем остальным
//                     "1…".."4…" — переслать игроку с этим номером
//   сервер → клиент:  "C{json}"  — служебное (welcome, roster, full, busy, …)
//                     "R<id>…"   — пересланное сообщение от игрока <id> (одна цифра)
//
// Секрет BOT_TOKEN (необязательно): если задан, имя и Telegram-id игрока берутся из проверенных initData.

import { DurableObject } from 'cloudflare:workers';

const VERSION = 1;
const MAX_PLAYERS = 4;
const MAX_FRAME = 64 * 1024;
const CODE_ABC = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';     // без 0/O и 1/I — чтобы код легко продиктовать
const CODE_RE = /^[A-HJ-NP-Z2-9]{5}$/;
const ALLOWED_ORIGINS = ['https://makstit42.github.io'];
const INIT_DATA_MAX_AGE_S = 7 * 24 * 3600;

function newCode() {
  const b = crypto.getRandomValues(new Uint8Array(5));
  return [...b].map(x => CODE_ABC[x % CODE_ABC.length]).join('');
}
const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n);

async function checkInitData(initData, botToken) {
  if (!initData || !botToken) return null;
  const params = new URLSearchParams(initData);
  const hash = params.get('hash');
  if (!hash) return null;
  params.delete('hash');
  const dataCheckString = [...params.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([k, v]) => `${k}=${v}`).join('\n');
  const enc = new TextEncoder();
  const webAppKey = await crypto.subtle.importKey('raw', enc.encode('WebAppData'), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const secret = await crypto.subtle.sign('HMAC', webAppKey, enc.encode(botToken));
  const key = await crypto.subtle.importKey('raw', secret, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(dataCheckString)));
  const hex = [...sig].map(b => b.toString(16).padStart(2, '0')).join('');
  if (hex !== hash) return null;
  if (Date.now() / 1000 - (Number(params.get('auth_date')) || 0) > INIT_DATA_MAX_AGE_S) return null;
  try { const user = JSON.parse(params.get('user') || 'null'); return user && user.id ? user : null; } catch (e) { return null; }
}

// ===================== комната =====================
export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // «ping» → «pong» отвечает сама платформа, комната при этом не просыпается
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }

  // все подключённые игроки (у кого уже есть номер)
  members(except) {
    const out = [];
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === except) continue;
      const a = ws.deserializeAttachment();
      if (a && a.id) out.push({ ws, a });
    }
    return out;
  }
  async meta() {
    const [host, state] = await Promise.all([this.ctx.storage.get('host'), this.ctx.storage.get('state')]);
    return { host: host || 0, state: state || 'lobby' };
  }
  send(ws, obj) { try { ws.send('C' + JSON.stringify(obj)); } catch (e) {} }
  async roster(except) {
    const { host, state } = await this.meta();
    const players = this.members(except).map(({ a }) => ({ id: a.id, name: a.name, mid: a.mid, wpn: a.wpn, ready: !!a.ready, tg: !!a.tg })).sort((x, y) => x.id - y.id);
    return { t: 'roster', host, state, players };
  }
  async broadcastRoster(except) {
    const r = await this.roster(except);
    for (const { ws } of this.members(except)) this.send(ws, r);
  }

  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname.endsWith('/info')) {
      const r = await this.roster();
      return Response.json({ n: r.players.length, state: r.state, players: r.players.map(p => ({ id: p.id, name: p.name, mid: p.mid })) });
    }
    if (req.headers.get('Upgrade') !== 'websocket') return new Response('Нужен WebSocket', { status: 426 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ id: 0, code: url.searchParams.get('code') || '' });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    if (typeof msg !== 'string' || !msg.length || msg.length > MAX_FRAME) return;
    const a = ws.deserializeAttachment() || {};
    const k = msg[0];
    if (k === 'C') { await this.control(ws, a, msg.slice(1)); return; }
    if (!a.id) return;
    // пересылка: сервер не разбирает содержимое, только добавляет номер отправителя
    const out = 'R' + a.id + msg.slice(1);
    if (k === 'H') {
      const { host } = await this.meta();
      for (const m of this.members()) if (m.a.id === host && m.ws !== ws) { try { m.ws.send(out); } catch (e) {} }
    } else if (k === 'A') {
      for (const m of this.members(ws)) { try { m.ws.send(out); } catch (e) {} }
    } else if (k >= '1' && k <= '4') {
      const to = +k;
      for (const m of this.members(ws)) if (m.a.id === to) { try { m.ws.send(out); } catch (e) {} }
    }
  }

  async control(ws, a, body) {
    let m; try { m = JSON.parse(body); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'hello') {
      if (a.id) return;
      const tok = clean(m.tok, 40);
      const { host, state } = await this.meta();
      const others = this.members(ws);
      // переподключение с тем же токеном — тот же номер, старое соединение закрываем
      const old = tok ? others.find(o => o.a.tok === tok) : null;
      if (!old && others.length >= MAX_PLAYERS) { this.send(ws, { t: 'full' }); ws.close(4001, 'full'); return; }
      // идёт забег: пускаем только тех, кто в нём был (вылетел и вернулся) — на их прежний номер
      const runToks = state === 'run' ? (await this.ctx.storage.get('runToks')) || {} : null;
      const used = new Set(others.map(o => o.a.id));
      const back = runToks && tok && runToks[tok] && !used.has(runToks[tok]) ? runToks[tok] : 0;
      if (!old && state === 'run' && !back) { this.send(ws, { t: 'busy' }); ws.close(4002, 'busy'); return; }
      let id;
      if (old) { id = old.a.id; old.a.id = 0; old.ws.serializeAttachment(old.a); try { old.ws.close(4003, 'replaced'); } catch (e) {} }
      else if (back) id = back;
      else { id = 1; while (used.has(id)) id++; }
      const user = await checkInitData(m.initData, this.env.BOT_TOKEN);
      const name = user ? clean([user.first_name, user.last_name].filter(Boolean).join(' '), 32) : clean(m.name, 32);
      Object.assign(a, { id, tok, name: name || 'Игрок ' + id, mid: clean(m.mid, 40), wpn: clean(m.wpn, 4), ready: false, tg: user ? user.id : 0, at: Date.now() });
      ws.serializeAttachment(a);
      let h = host;
      if (!h || !others.some(o => o.a.id === h)) { h = id; await this.ctx.storage.put('host', h); }
      this.send(ws, { t: 'welcome', id, host: h, state, ver: VERSION, code: a.code });
      await this.broadcastRoster();
      return;
    }
    if (!a.id) return;
    if (m.t === 'meta') {
      if (m.name !== undefined && !a.tg) a.name = clean(m.name, 32) || a.name;
      if (m.mid !== undefined) a.mid = clean(m.mid, 40);
      if (m.wpn !== undefined) a.wpn = clean(m.wpn, 4);
      if (m.ready !== undefined) a.ready = !!m.ready;
      ws.serializeAttachment(a);
      await this.broadcastRoster();
      return;
    }
    if (m.t === 'state') {
      const { host } = await this.meta();
      if (a.id !== host) return;
      const s = m.s === 'run' ? 'run' : 'lobby';
      await this.ctx.storage.put('state', s);
      if (s === 'run') { const toks = {}; for (const o of this.members()) if (o.a.tok) toks[o.a.tok] = o.a.id; await this.ctx.storage.put('runToks', toks); }
      else await this.ctx.storage.delete('runToks');
      if (s === 'lobby') for (const o of this.members()) { o.a.ready = false; o.ws.serializeAttachment(o.a); }
      await this.broadcastRoster();
      return;
    }
    if (m.t === 'kick') {
      const { host } = await this.meta();
      if (a.id !== host || m.id === a.id) return;
      for (const o of this.members()) if (o.a.id === m.id) { this.send(o.ws, { t: 'kicked' }); try { o.ws.close(4004, 'kicked'); } catch (e) {} }
      return;
    }
  }

  async gone(ws) {
    const a = ws.deserializeAttachment() || {};
    const rest = this.members(ws);
    if (!rest.length) { await this.ctx.storage.deleteAll(); return; }      // пустая комната — забываем всё
    if (!a.id) return;
    const { host, state } = await this.meta();
    for (const o of rest) this.send(o.ws, { t: 'left', id: a.id });
    if (a.id === host) {
      // хост ушёл: хостом становится игрок с меньшим номером; идущий забег заканчивается
      const nh = rest.map(o => o.a.id).sort((x, y) => x - y)[0];
      await this.ctx.storage.put('host', nh);
      if (state === 'run') { await this.ctx.storage.put('state', 'lobby'); await this.ctx.storage.delete('runToks'); for (const o of rest) this.send(o.ws, { t: 'hostleft' }); }
    }
    await this.broadcastRoster(ws);
  }
  async webSocketClose(ws, code, reason) {
    try { ws.close(code === 1005 ? 1000 : code, reason); } catch (e) {}
    await this.gone(ws);
  }
  async webSocketError(ws) { await this.gone(ws); }
}

// ===================== вход =====================
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const origin = req.headers.get('Origin') || '';
    const allowed = env.EXTRA_ORIGIN ? [...ALLOWED_ORIGINS, env.EXTRA_ORIGIN] : ALLOWED_ORIGINS;
    const cors = { 'Access-Control-Allow-Origin': allowed.includes(origin) ? origin : ALLOWED_ORIGINS[0], 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', 'Vary': 'Origin' };
    const json = (obj, status = 200) => new Response(JSON.stringify(obj), { status, headers: { ...cors, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' } });
    if (req.method === 'OPTIONS') return new Response(null, { headers: cors });
    if (!env.ROOM) return json({ error: 'Нет привязки Durable Object с именем ROOM' }, 500);

    if (url.pathname === '/health') return json({ ok: true, ver: VERSION });

    if (url.pathname === '/room/new') {
      for (let k = 0; k < 6; k++) {
        const code = newCode(), stub = env.ROOM.get(env.ROOM.idFromName(code));
        const info = await (await stub.fetch('https://room/info')).json();
        if (!info.n) return json({ code });
      }
      return json({ error: 'Не удалось создать комнату, попробуй ещё раз' }, 503);
    }

    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{5})(\/info)?$/);
    if (m) {
      const code = m[1].toUpperCase();
      if (!CODE_RE.test(code)) return json({ error: 'Неверный код комнаты' }, 400);
      const stub = env.ROOM.get(env.ROOM.idFromName(code));
      if (m[2]) { const r = await stub.fetch('https://room/info'); return json(await r.json()); }
      if (req.headers.get('Upgrade') !== 'websocket') return json({ error: 'Нужен WebSocket' }, 426);
      if (origin && !allowed.includes(origin)) return new Response('Чужой сайт', { status: 403 });
      return stub.fetch(new Request('https://room/ws?code=' + code, req));
    }
    return json({ error: 'Не найдено' }, 404);
  },
};
