/* Запуск воркфлоу через workflow_dispatch: бот (collect.yml) и сборщик
   новостей ai-news-harvester (harvest.yml, src/harvest.js).

   Событие workflow_dispatch GitHub ставит в очередь сразу, в отличие
   от schedule, который часами пропускает прогоны. Параллельных прогонов
   не бывает: concurrency group в каждом воркфлоу держит один идущий
   и один ждущий, лишние ждущие GitHub сам заменяет последним. */

const COLLECT_URL =
  "https://api.github.com/repos/mashninski/streaminfo-sitebot/actions/workflows/collect.yml/dispatches";

/**
 * POST на адрес dispatches с телом { ref: "main", inputs }. Токен —
 * GITHUB_TOKEN, один на оба репозитория (Actions: Read and write).
 * Ответ не 204 — в лог Cloudflare. Возвращает true, если GitHub принял запуск.
 */
export async function dispatchWorkflow(env, url, inputs, reason) {
  if (!env.GITHUB_TOKEN) {
    console.error(`[dispatch] ${reason}: не задан секрет GITHUB_TOKEN`);
    return false;
  }
  const body = { ref: "main" };
  if (inputs && Object.keys(inputs).length > 0) body.inputs = inputs;

  let res;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "strymy-bot-worker",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
  } catch (err) {
    console.error(`[dispatch] ${reason}: сеть — ${err}`);
    return false;
  }
  if (res.status !== 204) {
    const text = await res.text().catch(() => "");
    console.error(`[dispatch] ${reason}: GitHub ответил ${res.status} ${text.slice(0, 500)}`);
    return res.ok;
  }
  const details = body.inputs ? `, ${JSON.stringify(body.inputs)}` : "";
  console.log(`[dispatch] ${reason}: запущен${details}`);
  return true;
}

/**
 * Запускает бота. `delaySec` — пауза перед сбором внутри прогона
 * (вход delay_sec воркфлоу): после конца эфира Get Streams ещё какое-то
 * время отдаёт канал как живой.
 */
export async function dispatchCollect(env, reason, delaySec = 0) {
  const inputs = delaySec > 0 ? { delay_sec: String(delaySec) } : null;
  return dispatchWorkflow(env, COLLECT_URL, inputs, reason);
}
