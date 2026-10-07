/* Запуск бота: workflow_dispatch воркфлоу collect.yml.

   Событие workflow_dispatch GitHub ставит в очередь сразу, в отличие
   от schedule, который часами пропускает прогоны. Параллельных прогонов
   не бывает: concurrency group collect в воркфлоу держит один идущий
   и один ждущий, лишние ждущие GitHub сам заменяет последним. */

const DISPATCH_URL =
  "https://api.github.com/repos/mashninski/streaminfo-sitebot/actions/workflows/collect.yml/dispatches";

/**
 * Запускает бота. `delaySec` — пауза перед сбором внутри прогона
 * (вход delay_sec воркфлоу): после конца эфира Get Streams ещё какое-то
 * время отдаёт канал как живой. Ответ не 204 — в лог Cloudflare.
 * Возвращает true, если GitHub принял запуск.
 */
export async function dispatchCollect(env, reason, delaySec = 0) {
  if (!env.GITHUB_TOKEN) {
    console.error(`[dispatch] ${reason}: не задан секрет GITHUB_TOKEN`);
    return false;
  }
  const body = { ref: "main" };
  if (delaySec > 0) body.inputs = { delay_sec: String(delaySec) };

  let res;
  try {
    res = await fetch(DISPATCH_URL, {
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
  console.log(`[dispatch] ${reason}: запущен${delaySec > 0 ? `, пауза ${delaySec} с` : ""}`);
  return true;
}
