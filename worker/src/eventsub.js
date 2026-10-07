/* Twitch EventSub по webhook: Twitch сам сообщает, что эфир начался
   или закончился, и Worker сразу запускает бота.

   Документация: https://dev.twitch.tv/docs/eventsub/handling-webhook-events/
   Подписки — stream.online и stream.offline на каждого нескрытого
   стримера из streamers.json (тот же список, что собирает collect.py).
   Подписки ставит и чинит сам Worker, на каждом тике cron
   (syncSubscriptions), — так все секреты живут только в Cloudflare. */

import { dispatchCollect } from "./github.js";

export const TYPES = ["stream.online", "stream.offline"];

/** Сообщения старше этого — повтор или подделка, не обрабатываются. */
const MAX_AGE_MS = 10 * 60 * 1000;

/** Сколько подписок создаём или удаляем за один тик: бесплатный план
    Workers даёт 50 внешних запросов на вызов, ~5 уходят на остальное. */
export const MAX_CHANGES_PER_RUN = 40;

const REGISTRY_URL =
  "https://raw.githubusercontent.com/mashninski/streaminfo-sitebot/main/streamers.json";

/* ---------- приём уведомлений ---------- */

function hex(buffer) {
  return [...new Uint8Array(buffer)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** Сравнение строк за одно и то же время, чтобы подпись нельзя было
    подобрать по времени ответа. */
function safeEqual(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

/** Подпись Twitch: HMAC-SHA256(секрет, id + timestamp + тело), "sha256=<hex>". */
export async function verifySignature(secret, id, timestamp, body, signature) {
  if (!secret || !id || !timestamp || !signature) return false;
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const mac = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(id + timestamp + body));
  return safeEqual(`sha256=${hex(mac)}`, signature);
}

/** Пауза перед сбором после stream.offline, секунд (вход delay_sec воркфлоу). */
function offlineDelay(env) {
  const n = Number(env.OFFLINE_DELAY_SEC ?? 0);
  return Number.isFinite(n) && n > 0 ? Math.min(Math.round(n), 300) : 0;
}

/**
 * POST от Twitch. Ответ — быстро: Twitch ждёт несколько секунд и иначе
 * повторяет сообщение, а при частых сбоях отключает подписку. Запуск
 * бота идёт в ctx.waitUntil, уже после ответа.
 */
export async function handleWebhook(request, env, ctx, now = Date.now()) {
  const h = request.headers;
  const id = h.get("Twitch-Eventsub-Message-Id");
  const timestamp = h.get("Twitch-Eventsub-Message-Timestamp");
  const signature = h.get("Twitch-Eventsub-Message-Signature");
  const type = h.get("Twitch-Eventsub-Message-Type");
  const body = await request.text();

  if (!(await verifySignature(env.EVENTSUB_SECRET, id, timestamp, body, signature))) {
    console.warn("[eventsub] подпись не сошлась — отклонено");
    return new Response("bad signature", { status: 403 });
  }

  const sentAt = Date.parse(timestamp);
  if (!Number.isFinite(sentAt) || Math.abs(now - sentAt) > MAX_AGE_MS) {
    console.warn(`[eventsub] ${id}: сообщение от ${timestamp}, старше 10 минут — пропущено`);
    return new Response(null, { status: 204 });
  }

  // Повтор: Twitch шлёт сообщение ещё раз, если не дождался ответа.
  // KV хранит id 10 минут — дольше сообщение и так не примется.
  const seenKey = `msg:${id}`;
  if (env.KV && (await env.KV.get(seenKey))) {
    console.log(`[eventsub] ${id}: повтор — пропущен`);
    return new Response(null, { status: 204 });
  }
  if (env.KV) ctx.waitUntil(env.KV.put(seenKey, "1", { expirationTtl: 600 }));

  let payload;
  try {
    payload = JSON.parse(body);
  } catch {
    return new Response("bad json", { status: 400 });
  }

  if (type === "webhook_callback_verification") {
    // Подтверждение новой подписки: вернуть challenge как есть, текстом.
    console.log(`[eventsub] подтверждена подписка ${payload.subscription?.type} ` +
      `${payload.subscription?.condition?.broadcaster_user_id}`);
    return new Response(payload.challenge, {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }

  if (type === "revocation") {
    // Twitch отозвал подписку (бан, удаление канала, сбои доставки).
    // Следующий тик cron её пересоздаст, если стример ещё в списке.
    const s = payload.subscription ?? {};
    console.warn(`[eventsub] ОТЗЫВ подписки ${s.type} ${s.condition?.broadcaster_user_id}: ${s.status}`);
    return new Response(null, { status: 204 });
  }

  if (type === "notification") {
    const subType = payload.subscription?.type;
    const login = payload.event?.broadcaster_user_login;
    if (TYPES.includes(subType)) {
      const delay = subType === "stream.offline" ? offlineDelay(env) : 0;
      ctx.waitUntil(dispatchCollect(env, `${subType} ${login}`, delay));
    }
    return new Response(null, { status: 204 });
  }

  return new Response(null, { status: 204 });
}

/* ---------- Twitch Helix ---------- */

/** App access token; держится в KV до истечения, чтобы не брать
    новый на каждом тике. */
async function appToken(env, forceNew = false) {
  if (!forceNew && env.KV) {
    const cached = await env.KV.get("twitch_app_token");
    if (cached) return cached;
  }
  // Секрет — в теле запроса, а не в адресе: адреса попадают в логи.
  const res = await fetch("https://id.twitch.tv/oauth2/token", {
    method: "POST",
    body: new URLSearchParams({
      client_id: env.TWITCH_CLIENT_ID,
      client_secret: env.TWITCH_CLIENT_SECRET,
      grant_type: "client_credentials",
    }),
  });
  if (!res.ok) throw new Error(`Twitch token: HTTP ${res.status}`);
  const { access_token, expires_in } = await res.json();
  if (env.KV) {
    // Час запаса, но не меньше минуты — минимум TTL в KV.
    const ttl = Math.max(60, expires_in - 3600);
    await env.KV.put("twitch_app_token", access_token, { expirationTtl: ttl });
  }
  return access_token;
}

function helix(env) {
  let token = null;
  return async function call(path, init = {}) {
    token ??= await appToken(env);
    const send = (t) =>
      fetch(`https://api.twitch.tv/helix/${path}`, {
        ...init,
        headers: {
          "Client-Id": env.TWITCH_CLIENT_ID,
          Authorization: `Bearer ${t}`,
          "Content-Type": "application/json",
          ...init.headers,
        },
      });
    let res = await send(token);
    // Токен отозван или истёк раньше срока — новый и ещё раз.
    if (res.status === 401) {
      token = await appToken(env, true);
      res = await send(token);
    }
    return res;
  };
}

/* ---------- сверка подписок ---------- */

/**
 * Что поменять, чтобы подписки совпали со списком. Чистая функция.
 * `wantedIds` — user_id нужных стримеров; `subs` — подписки этого
 * приложения из Get EventSub Subscriptions. Чужие (другой callback или
 * тип) не трогаются. Живые — enabled и ждущие подтверждения; остальные
 * (отказ подтверждения, сбои доставки, отзыв) удаляются и создаются заново.
 */
export function planSubscriptions(wantedIds, subs, callback) {
  const wanted = new Set(wantedIds);
  const ours = subs.filter(
    (s) => TYPES.includes(s.type) && s.transport?.method === "webhook" && s.transport?.callback === callback,
  );
  const remove = [];
  const have = new Set();
  for (const s of ours) {
    const uid = s.condition?.broadcaster_user_id;
    const alive = s.status === "enabled" || s.status === "webhook_callback_verification_pending";
    const key = `${s.type}:${uid}`;
    if (!alive || !wanted.has(uid) || have.has(key)) remove.push(s.id);
    else have.add(key);
  }
  const create = [];
  for (const uid of wanted) {
    for (const type of TYPES) {
      if (!have.has(`${type}:${uid}`)) create.push({ type, uid });
    }
  }
  return { create, remove };
}

/** Нескрытые логины реестра — тот же отбор, что в collect.py. */
export function registryLogins(registry) {
  return (registry.streamers ?? []).filter((s) => !s.hidden).map((s) => s.login.toLowerCase());
}

/**
 * Сверка подписок с streamers.json. Новый стример подписывается
 * на ближайшем тике, отозванная подписка пересоздаётся так же.
 * За тик — не больше MAX_CHANGES_PER_RUN правок, остальное — на следующем.
 */
export async function syncSubscriptions(env) {
  if (!env.TWITCH_CLIENT_ID || !env.TWITCH_CLIENT_SECRET || !env.EVENTSUB_SECRET || !env.CALLBACK_URL) {
    console.error("[sync] не заданы TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, EVENTSUB_SECRET или CALLBACK_URL");
    return;
  }
  const regRes = await fetch(REGISTRY_URL, { cf: { cacheTtl: 60 } });
  if (!regRes.ok) throw new Error(`streamers.json: HTTP ${regRes.status}`);
  const logins = registryLogins(await regRes.json());

  const call = helix(env);

  const wantedIds = [];
  for (let i = 0; i < logins.length; i += 100) {
    const q = logins.slice(i, i + 100).map((l) => `login=${encodeURIComponent(l)}`).join("&");
    const res = await call(`users?${q}`);
    if (!res.ok) throw new Error(`Get Users: HTTP ${res.status}`);
    for (const u of (await res.json()).data) wantedIds.push(u.id);
  }

  const subs = [];
  let cursor = null;
  do {
    const res = await call(`eventsub/subscriptions${cursor ? `?after=${cursor}` : ""}`);
    if (!res.ok) throw new Error(`Get EventSub Subscriptions: HTTP ${res.status}`);
    const page = await res.json();
    subs.push(...page.data);
    cursor = page.pagination?.cursor ?? null;
  } while (cursor);

  const { create, remove } = planSubscriptions(wantedIds, subs, env.CALLBACK_URL);
  if (!create.length && !remove.length) return;

  let budget = MAX_CHANGES_PER_RUN;
  let removed = 0;
  let created = 0;
  for (const id of remove) {
    if (budget-- <= 0) break;
    const res = await call(`eventsub/subscriptions?id=${id}`, { method: "DELETE" });
    if (res.ok || res.status === 404) removed++;
    else console.error(`[sync] удаление ${id}: HTTP ${res.status}`);
  }
  for (const { type, uid } of create) {
    if (budget-- <= 0) break;
    const res = await call("eventsub/subscriptions", {
      method: "POST",
      body: JSON.stringify({
        type,
        version: "1",
        condition: { broadcaster_user_id: uid },
        transport: { method: "webhook", callback: env.CALLBACK_URL, secret: env.EVENTSUB_SECRET },
      }),
    });
    // 409 — такая подписка уже есть: повторный запуск дублей не создаёт.
    if (res.ok || res.status === 409) created++;
    else console.error(`[sync] подписка ${type} ${uid}: HTTP ${res.status} ${(await res.text()).slice(0, 300)}`);
  }
  console.log(`[sync] стримеров ${wantedIds.length}: создано ${created}, удалено ${removed}, ` +
    `осталось на следующий тик ${Math.max(0, create.length + remove.length - created - removed)}`);
}
