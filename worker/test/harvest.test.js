// Тесты запуска сборщика новостей: node --test.
import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import worker from "../src/index.js";
import { HARVEST_CRON, KIND_BY_HOUR, dispatchHarvest, harvestKind } from "../src/harvest.js";

function captureFetch() {
  const calls = [];
  globalThis.fetch = async (url, init) => {
    calls.push({ url: String(url), body: JSON.parse(init.body) });
    return new Response(null, { status: 204 });
  };
  return calls;
}

const at = (hour, minute = 17) => Date.UTC(2026, 9, 7, hour, minute);

test("вид прогона по часу: два цикла, сдвинутые на 12 часов", () => {
  const byKind = {};
  for (let h = 1; h < 24; h += 2) (byKind[harvestKind(h)] ??= []).push(h);
  assert.deepEqual(byKind, {
    fix: [1, 13],
    collect: [3, 7, 9, 15, 19, 21],
    publish: [5, 17],
    generate: [11, 23],
  });
  // Цепочка: генерация → +2 ч fix → +2 ч забор fix → +2 ч публикация
  for (const g of byKind.generate) {
    assert.equal(harvestKind((g + 2) % 24), "fix");
    assert.equal(harvestKind((g + 4) % 24), "collect");
    assert.equal(harvestKind((g + 6) % 24), "publish");
  }
});

test("dispatchHarvest шлёт workflow_dispatch harvest.yml с видом прогона", async () => {
  const calls = captureFetch();
  assert.equal(await dispatchHarvest({ GITHUB_TOKEN: "gh-test" }, at(23)), true);
  assert.equal(await dispatchHarvest({ GITHUB_TOKEN: "gh-test" }, at(9)), true);
  assert.match(calls[0].url, /ai-news-harvester\/actions\/workflows\/harvest\.yml\/dispatches$/);
  assert.deepEqual(calls[0].body, { ref: "main", inputs: { kind: "generate" } });
  assert.deepEqual(calls[1].body, { ref: "main", inputs: { kind: "collect" } });
});

test("scheduled: свой cron у harvest, бот и подписки на нём не трогаются", async () => {
  const calls = captureFetch();
  await worker.scheduled({ cron: HARVEST_CRON, scheduledTime: at(13) }, { GITHUB_TOKEN: "gh-test" }, {});
  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /harvest\.yml/);
  assert.deepEqual(calls[0].body.inputs, { kind: "fix" });
});

test("cron harvest в wrangler.toml — тот же, что HARVEST_CRON", () => {
  const toml = readFileSync(new URL("../wrangler.toml", import.meta.url), "utf-8");
  const crons = JSON.parse(toml.match(/^crons = (\[.*\])$/m)[1]);
  assert.deepEqual(crons, ["*/10 * * * *", HARVEST_CRON]);
});

// Запасной schedule харвестера — те же часы и виды, минута 47. Харвестер —
// отдельный репозиторий; лежит рядом (D:\production) — сверяем, нет — пропуск.
const HARVEST_YML = fileURLToPath(
  new URL("../../../ai-news-harvester/.github/workflows/harvest.yml", import.meta.url),
);

test("часы и виды совпадают с запасным schedule в harvest.yml", { skip: !existsSync(HARVEST_YML) }, () => {
  const yml = readFileSync(HARVEST_YML, "utf-8");
  const fromYml = {};
  for (const [, hours] of yml.matchAll(/- cron: "47 ([\d,]+) \* \* \*"/g)) {
    const line = `47 ${hours} * * *`;
    const m = yml.match(new RegExp(`"${line.replace(/\*/g, "\\*")}"\\)\\s+kind=(\\w+)`));
    const kind = m ? m[1] : "collect";
    for (const h of hours.split(",")) fromYml[Number(h)] = kind;
  }
  const fromWorker = {};
  for (let h = 1; h < 24; h += 2) fromWorker[h] = harvestKind(h);
  assert.deepEqual(fromYml, fromWorker);
  assert.deepEqual(Object.keys(KIND_BY_HOUR).map(Number).sort((a, b) => a - b), [1, 5, 11, 13, 17, 23]);
});
