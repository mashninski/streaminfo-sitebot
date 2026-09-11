#!/usr/bin/env python3
"""
Сборщик состояния беларуских Twitch-стримеров для mashninski.com/strymy.

Что делает за один прогон:
  1. берёт app access token (client credentials);
  2. Get Users   — аватар, описание, broadcaster_type, user_id;
  3. Get Streams — кто сейчас в эфире;
  4. ловит переходы онлайн→офлайн и закрывает сессию;
  5. раз в час — Get Videos: ищет VOD, проверяет, не исчезли ли старые;
  6. пишет data/twitch-state.json, только если состояние изменилось.

Ключи читаются из переменных окружения TWITCH_CLIENT_ID и TWITCH_CLIENT_SECRET.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
REGISTRY_PATH = ROOT / "streamers.json"
STATE_PATH = ROOT / "data" / "twitch-state.json"

HELIX = "https://api.twitch.tv/helix"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"

VOD_CHECK_WINDOW_MIN = 10     # Get Videos дёргаем только в первом прогоне каждого часа
VOD_LOOKBACK_DAYS = 65        # дольше максимального срока хранения VOD (60 дней)
HISTORY_LIMIT = 20            # сколько сессий храним на стримера
STALE_RUNS_TO_HIDE = 6        # после скольких прогонов без ответа считаем канал пропавшим

TIMEOUT = 20


# ---------- утилиты ----------

def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


DURATION_RE = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")


def parse_duration(s: str) -> int:
    """Twitch отдаёт длительность строкой вида '3h8m33s'. Возвращаем минуты."""
    m = DURATION_RE.fullmatch(s or "")
    if not m:
        return 0
    h, mi, sec = (int(x) if x else 0 for x in m.groups())
    return h * 60 + mi + round(sec / 60)


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as f:
        return json.load(f)


# ---------- Twitch API ----------

class Twitch:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.session = requests.Session()
        self.token = self._get_token(client_secret)

    def _get_token(self, client_secret: str) -> str:
        r = self.session.post(
            TOKEN_URL,
            data={
                "client_id": self.client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return r.json()["access_token"]

    def get(self, path: str, params: dict) -> list:
        """Один запрос к Helix с ретраями. Возвращает список из поля data."""
        headers = {"Client-Id": self.client_id, "Authorization": f"Bearer {self.token}"}
        for attempt in range(3):
            r = self.session.get(f"{HELIX}/{path}", params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                reset = int(r.headers.get("Ratelimit-Reset", "0"))
                wait = max(1, reset - int(time.time())) if reset else 5
                print(f"  429, ждём {wait} с", file=sys.stderr)
                time.sleep(min(wait, 60))
                continue
            if r.status_code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json().get("data", [])
        raise RuntimeError(f"Twitch не ответил на {path} после 3 попыток")

    def users(self, logins: list) -> dict:
        out = {}
        for batch in chunked(logins, 100):
            for u in self.get("users", [("login", x) for x in batch]):
                out[u["login"].lower()] = u
        return out

    def streams(self, user_ids: list) -> dict:
        out = {}
        for batch in chunked(user_ids, 100):
            for s in self.get("streams", [("user_id", x) for x in batch] + [("first", 100)]):
                out[s["user_id"]] = s
        return out

    def archives(self, user_id: str, limit: int = 5) -> list:
        # Get Videos принимает только один user_id за запрос — отсюда отдельный вызов на стримера.
        return self.get("videos", {"user_id": user_id, "type": "archive", "first": limit})


# ---------- логика ----------

def blank_streamer() -> dict:
    return {
        "user_id": None,
        "display_name": None,
        "profile_image_url": None,
        "description": None,
        "broadcaster_type": "",
        "live": {"since": None, "title": None, "game": None, "viewers": None, "stream_id": None},
        "last_stream": None,
        "history": [],
        "missing_runs": 0,
    }


def session_from_live(live: dict, ended_at: datetime) -> dict:
    started = parse_iso(live["since"])
    return {
        "started_at": live["since"],
        "ended_at": iso(ended_at),
        "duration_min": max(1, round((ended_at - started).total_seconds() / 60)),
        "title": live.get("title"),
        "game": live.get("game"),
        "stream_id": live.get("stream_id"),
        "source": "observed",
        "vod": None,
    }


def session_from_video(v: dict) -> dict:
    started = parse_iso(v["created_at"])
    minutes = parse_duration(v.get("duration", ""))
    return {
        "started_at": v["created_at"],
        "ended_at": iso(started + timedelta(minutes=minutes)),
        "duration_min": minutes,
        "title": v.get("title"),
        "game": None,
        "stream_id": v.get("stream_id"),
        "source": "vod",
        "vod": {"id": v["id"], "url": v["url"], "seen_at": iso(now()), "gone": False},
    }


def update_vods(tw: Twitch, state: dict, logins: list) -> None:
    """Привязывает VOD к последней сессии, подтягивает историю задним числом,
    помечает исчезнувшие записи."""
    cutoff = now() - timedelta(days=VOD_LOOKBACK_DAYS)
    for login in logins:
        st = state["streamers"][login]
        if not st.get("user_id"):
            continue
        last = st.get("last_stream")
        # Пропускаем тех, у кого последняя сессия старше срока хранения VOD
        # и запись уже помечена как исчезнувшая — там проверять нечего.
        if last and last.get("vod") and last["vod"].get("gone") and parse_iso(last["ended_at"]) < cutoff:
            continue
        try:
            videos = tw.archives(st["user_id"])
        except Exception as e:
            print(f"  {login}: не удалось получить видео — {e}", file=sys.stderr)
            continue

        if last is None:
            # Первый запуск: берём историю из VOD, если она есть.
            if videos:
                st["last_stream"] = session_from_video(videos[0])
            continue

        by_stream = {v.get("stream_id"): v for v in videos if v.get("stream_id")}
        match = by_stream.get(last.get("stream_id"))

        if match:
            # Нашли запись нашей сессии: уточняем длительность по VOD, она точная.
            prev = last.get("vod") or {}
            # seen_at проставляем один раз, при первой привязке. Иначе поле менялось бы
            # каждый час у всех подряд и порождало пустой коммит.
            seen_at = prev["seen_at"] if prev.get("id") == match["id"] else iso(now())
            last["vod"] = {"id": match["id"], "url": match["url"], "seen_at": seen_at, "gone": False}
            d = parse_duration(match.get("duration", ""))
            if d:
                last["duration_min"] = d
                last["ended_at"] = iso(parse_iso(last["started_at"]) + timedelta(minutes=d))
                last["source"] = "vod"
        elif last.get("vod") and not last["vod"].get("gone"):
            # Запись была, а теперь её нет — истекла или удалена.
            last["vod"]["gone"] = True
        # Если vod никогда не было — значит стример не сохраняет трансляции. Оставляем None.


def run() -> int:
    client_id = os.environ.get("TWITCH_CLIENT_ID")
    client_secret = os.environ.get("TWITCH_CLIENT_SECRET")
    if not client_id or not client_secret:
        print("Нет TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET", file=sys.stderr)
        return 2

    registry = load_json(REGISTRY_PATH, {"streamers": []})["streamers"]
    logins = [s["login"].lower() for s in registry if not s.get("hidden")]
    if not logins:
        print("Реестр пуст — нечего собирать")
        return 0

    state = load_json(STATE_PATH, {"updated_at": None, "streamers": {}})
    before = json.dumps(state.get("streamers", {}), sort_keys=True, ensure_ascii=False)

    for login in logins:
        state["streamers"].setdefault(login, blank_streamer())

    tw = Twitch(client_id, client_secret)

    # --- профили ---
    users = tw.users(logins)
    for login in logins:
        st = state["streamers"][login]
        u = users.get(login)
        if not u:
            # Считаем до порога и останавливаемся: иначе счётчик рос бы вечно
            # и каждый прогон порождал бы коммит из-за одного пропавшего канала.
            if st.get("missing_runs", 0) < STALE_RUNS_TO_HIDE:
                st["missing_runs"] = st.get("missing_runs", 0) + 1
                if st["missing_runs"] >= STALE_RUNS_TO_HIDE:
                    st["stale"] = True
                    print(f"  {login}: канал не отвечает {STALE_RUNS_TO_HIDE} прогонов, помечен stale",
                          file=sys.stderr)
            continue
        st["missing_runs"] = 0
        st.pop("stale", None)
        st["user_id"] = u["id"]
        st["display_name"] = u["display_name"]
        st["profile_image_url"] = u["profile_image_url"]
        st["description"] = u.get("description") or None
        st["broadcaster_type"] = u.get("broadcaster_type", "")
        if u["login"].lower() != login:
            print(f"  {login}: логин сменился на {u['login']}", file=sys.stderr)

    # --- кто в эфире ---
    ids = [state["streamers"][l]["user_id"] for l in logins if state["streamers"][l].get("user_id")]
    live_now = tw.streams(ids) if ids else {}

    ts = now()
    for login in logins:
        st = state["streamers"][login]
        uid = st.get("user_id")
        if not uid:
            continue
        was_live = st["live"]["since"] is not None
        s = live_now.get(uid)

        if s:
            st["live"] = {
                "since": s["started_at"],
                "title": s.get("title"),
                "game": s.get("game_name"),
                "viewers": s.get("viewer_count"),
                "stream_id": s.get("id"),
            }
        elif was_live:
            closed = session_from_live(st["live"], ts)
            st["last_stream"] = closed
            st["history"] = ([closed] + st.get("history", []))[:HISTORY_LIMIT]
            st["live"] = {"since": None, "title": None, "game": None, "viewers": None, "stream_id": None}
            print(f"  {login}: стрим закончился, {closed['duration_min']} мин")

    # --- VOD ---
    # Проверяем раз в час: только в первом прогоне часа. Расписание определяется
    # часами, а не файлом состояния, — иначе отметку о проверке пришлось бы
    # коммитить каждый час даже там, где ничего не поменялось.
    # Первый запуск (пустое состояние) — проверяем сразу, чтобы подтянуть историю из VOD.
    first_run = not any(state["streamers"][l].get("last_stream") for l in logins)
    if first_run or ts.minute < VOD_CHECK_WINDOW_MIN or os.environ.get("FORCE_VOD_CHECK"):
        update_vods(tw, state, logins)

    # --- запись только при изменениях ---
    after = json.dumps(state["streamers"], sort_keys=True, ensure_ascii=False)
    if after == before:
        print("Изменений нет, файл не трогаем")
        return 0

    state["updated_at"] = iso(ts)
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    print(f"Состояние обновлено: {STATE_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
