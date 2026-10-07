/* Worker «будильник» бота streaminfo-sitebot.

   - cron каждые 10 минут: запуск collect.yml через workflow_dispatch
     (cron самого GitHub часами пропускает прогоны) и сверка подписок
     Twitch EventSub;
   - POST /twitch/eventsub: Twitch сообщает о начале или конце эфира,
     бот запускается сразу.

   Секреты — только в Cloudflare (wrangler secret put), в файлах их нет:
   GITHUB_TOKEN, TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET, EVENTSUB_SECRET.
   Устройство — CLAUDE.md репозитория, раздел «Расписание и EventSub». */

import { dispatchCollect } from "./github.js";
import { handleWebhook, syncSubscriptions } from "./eventsub.js";

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/twitch/eventsub" && request.method === "POST") {
      return handleWebhook(request, env, ctx);
    }
    return new Response("strymy-bot worker\n", { status: 200 });
  },

  async scheduled(event, env, ctx) {
    // Запуск бота — первым и независимо: сбой сверки подписок
    // не должен оставить бота без прогона.
    await dispatchCollect(env, `cron ${event.cron}`);
    try {
      await syncSubscriptions(env);
    } catch (err) {
      console.error(`[sync] ${err}`);
    }
  },
};
