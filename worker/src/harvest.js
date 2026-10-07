/* Запуск сборщика новостей ai-news-harvester (воркфлоу harvest.yml)
   по времени — вместо schedule GitHub, который выкидывает прогоны.

   Решение — claude/ai-news-plan.md сайта, «Этап 9»: новость на сайте
   не позже чем через сутки, два цикла в сутки. Прогон раз в 2 часа
   по нечётным часам UTC, в минуту 17; вид прогона (вход kind) — по часу:

     23 / 11  generate  генерация отправлена
      1 / 13  fix       генерация забрана, fix отправлен
      5 / 17  publish   PR с карточками, вливается сам
     прочие   collect   сбор и triage (в 3 / 15 — ещё и забор fix)

   Те же часы — в schedule воркфлоу, запасным путём в минуту 47: если
   прогон Worker'а уже есть, запасной ничего не делает. Меняешь часы
   здесь — меняй строки cron и case шага «Вид прогона» в harvest.yml. */

import { dispatchWorkflow } from "./github.js";

/** Cron-триггер Worker'а для harvest; тот же, что в wrangler.toml. */
export const HARVEST_CRON = "17 1-23/2 * * *";

const HARVEST_URL =
  "https://api.github.com/repos/mashninski/ai-news-harvester/actions/workflows/harvest.yml/dispatches";

export const KIND_BY_HOUR = { 23: "generate", 11: "generate", 1: "fix", 13: "fix", 5: "publish", 17: "publish" };

/** Вид прогона по часу UTC запланированного срабатывания. */
export function harvestKind(hourUtc) {
  return KIND_BY_HOUR[hourUtc] ?? "collect";
}

/** Запускает прогон harvest вида, положенного на час `scheduledTime` (мс, UTC). */
export async function dispatchHarvest(env, scheduledTime) {
  const kind = harvestKind(new Date(scheduledTime).getUTCHours());
  return dispatchWorkflow(env, HARVEST_URL, { kind }, `harvest ${kind}`);
}
