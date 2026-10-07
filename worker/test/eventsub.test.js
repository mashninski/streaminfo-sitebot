// Тесты Worker: node --test (встроенный в Node, без зависимостей).
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";

import { handleWebhook, planSubscriptions, registryLogins, verifySignature } from "../src/eventsub.js";

const SECRET = "test-secret-0123456789";
const CALLBACK = "https://strymy-bot.example.workers.dev/twitch/eventsub";

function sign(id, ts, body, secret = SECRET) {
  return "sha256=" + createHmac("sha256", secret).update(id + ts + body).digest("hex");
}

function request(type, payload, { id = "m1", ts = new Date().toISOString(), secret = SECRET } = {}) {
  const body = JSON.stringify(payload);
  return new Request("https://w/twitch/eventsub", {
    method: "POST",
    headers: {
      "Twitch-Eventsub-Message-Id": id,
      "Twitch-Eventsub-Message-Timestamp": ts,
      "Twitch-Eventsub-Message-Signature": sign(id, ts, body, secret),
      "Twitch-Eventsub-Message-Type": type,
    },
    body,
  });
}

/** env с KV в памяти и подменённым запуском бота через fetch. */
function setup() {
  const kv = new Map();
  const dispatched = [];
  const env = {
    EVENTSUB_SECRET: SECRET,
    GITHUB_TOKEN: "gh-test",
    OFFLINE_DELAY_SEC: "60",
    KV: {
      get: async (k) => kv.get(k) ?? null,
      put: async (k, v) => void kv.set(k, v),
    },
  };
  const waits = [];
  const ctx = { waitUntil: (p) => waits.push(p) };
  globalThis.fetch = async (url, init) => {
    dispatched.push({ url: String(url), body: JSON.parse(init.body) });
    return new Response(null, { status: 204 });
  };
  return { env, ctx, dispatched, settle: () => Promise.all(waits) };
}

test("подпись: верная проходит, чужой секрет и правка тела — нет", async () => {
  const ts = "2026-10-04T14:06:53Z";
  assert.equal(await verifySignature(SECRET, "a", ts, "{}", sign("a", ts, "{}")), true);
  assert.equal(await verifySignature(SECRET, "a", ts, "{}", sign("a", ts, "{}", "other")), false);
  assert.equal(await verifySignature(SECRET, "a", ts, '{"x":1}', sign("a", ts, "{}")), false);
  assert.equal(await verifySignature(SECRET, "a", ts, "{}", null), false);
});

test("подтверждение подписки возвращает challenge текстом", async () => {
  const { env, ctx } = setup();
  const res = await handleWebhook(
    request("webhook_callback_verification", { challenge: "abc-123", subscription: { type: "stream.online" } }),
    env,
    ctx,
  );
  assert.equal(res.status, 200);
  assert.equal(await res.text(), "abc-123");
  assert.match(res.headers.get("Content-Type"), /text\/plain/);
});

test("stream.offline запускает бота с паузой, stream.online — без", async () => {
  const { env, ctx, dispatched, settle } = setup();
  const offline = { subscription: { type: "stream.offline" }, event: { broadcaster_user_login: "mashninski" } };
  const online = { subscription: { type: "stream.online" }, event: { broadcaster_user_login: "mashninski" } };
  assert.equal((await handleWebhook(request("notification", offline, { id: "o1" }), env, ctx)).status, 204);
  assert.equal((await handleWebhook(request("notification", online, { id: "o2" }), env, ctx)).status, 204);
  await settle();
  assert.equal(dispatched.length, 2);
  assert.match(dispatched[0].url, /collect\.yml\/dispatches$/);
  assert.deepEqual(dispatched[0].body, { ref: "main", inputs: { delay_sec: "60" } });
  assert.deepEqual(dispatched[1].body, { ref: "main" });
});

test("повтор по Message-Id и старое сообщение не запускают бота; плохая подпись — 403", async () => {
  const { env, ctx, dispatched, settle } = setup();
  const p = { subscription: { type: "stream.offline" }, event: { broadcaster_user_login: "x" } };
  await handleWebhook(request("notification", p, { id: "same" }), env, ctx);
  await settle();
  await handleWebhook(request("notification", p, { id: "same" }), env, ctx);
  const old = new Date(Date.now() - 11 * 60 * 1000).toISOString();
  await handleWebhook(request("notification", p, { id: "old", ts: old }), env, ctx);
  const bad = await handleWebhook(request("notification", p, { id: "bad", secret: "wrong" }), env, ctx);
  await settle();
  assert.equal(dispatched.length, 1);
  assert.equal(bad.status, 403);
});

test("отзыв подписки только логируется", async () => {
  const { env, ctx, dispatched, settle } = setup();
  const res = await handleWebhook(
    request("revocation", { subscription: { type: "stream.online", status: "user_removed", condition: { broadcaster_user_id: "1" } } }),
    env,
    ctx,
  );
  await settle();
  assert.equal(res.status, 204);
  assert.equal(dispatched.length, 0);
});

test("сверка: недостающие создаются, лишние, сломанные и дубли удаляются, чужие не трогаются", () => {
  const sub = (id, type, uid, status = "enabled", callback = CALLBACK) => ({
    id, type, status, condition: { broadcaster_user_id: uid }, transport: { method: "webhook", callback },
  });
  const subs = [
    sub("a", "stream.online", "1"),
    sub("b", "stream.offline", "1"),
    sub("c", "stream.online", "2", "notification_failures_exceeded"),
    sub("d", "stream.offline", "2", "webhook_callback_verification_pending"),
    sub("e", "stream.online", "9"), // стример убран из реестра
    sub("f", "stream.online", "1"), // дубль
    sub("g", "stream.online", "3", "enabled", "https://other.example/cb"), // чужая
  ];
  const { create, remove } = planSubscriptions(["1", "2", "3"], subs, CALLBACK);
  assert.deepEqual(remove.sort(), ["c", "e", "f"]);
  assert.deepEqual(create, [
    { type: "stream.online", uid: "2" },
    { type: "stream.online", uid: "3" },
    { type: "stream.offline", uid: "3" },
  ]);
  // Повторная сверка того, что уже совпадает, ничего не меняет.
  const done = [sub("1", "stream.online", "1"), sub("2", "stream.offline", "1")];
  assert.deepEqual(planSubscriptions(["1"], done, CALLBACK), { create: [], remove: [] });
});

test("реестр: скрытые не подписываются, логины строчными", () => {
  const reg = { streamers: [{ login: "Mashninski" }, { login: "hid", hidden: true }, { login: "b", hidden: false }] };
  assert.deepEqual(registryLogins(reg), ["mashninski", "b"]);
});
