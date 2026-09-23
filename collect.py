#!/usr/bin/env python3
"""
Сборщик состояния беларуских Twitch-стримеров для mashninski.com/strymy.

Что делает за один прогон:
  1. берёт app access token (client credentials);
  2. Get Users   — аватар, описание, broadcaster_type, user_id;
  3. Get Streams — кто сейчас в эфире;
  4. ловит переходы онлайн→офлайн и закрывает сессию;
  5. раз в час — Get Videos: ищет VOD, проверяет, не исчезли ли старые;
  6. пишет data/twitch-state.json, только если состояние изменилось;
  7. копит статистику (категории, часы суток по Мінску, месячные счётчики,
     подписчики) и пишет data/twitch-stats.json — тоже только при изменениях.

Ключи читаются из переменных окружения TWITCH_CLIENT_ID и TWITCH_CLIENT_SECRET.
Устройство статистики — claude/twitch-stats-spec.md в репозитории сайта.
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
STATS_PATH = ROOT / "data" / "twitch-stats.json"

HELIX = "https://api.twitch.tv/helix"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"

# Внутренний GraphQL Twitch — им пользуется сама страница twitch.tv. Недокументированный,
# формат может поменяться без предупреждения (риск принят, спека статистики §8).
# Client-Id — публичный идентификатор веб-клиента twitch.tv, виден в коде любой страницы
# канала; это не наш секрет. Переменная окружения — на случай, если Twitch его сменит.
GQL_URL = "https://gql.twitch.tv/gql"
GQL_CLIENT_ID = os.environ.get("TWITCH_GQL_CLIENT_ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
FOLLOWERS_QUERY = "query($logins:[String!]){users(logins:$logins){login followers{totalCount}}}"
FOLLOWERS_ATTEMPTS = 3
FOLLOWERS_PAUSE_SEC = 2.5

# Беларусь весь год на UTC+3, без перехода на летнее время — смещение фиксированное.
MINSK = timezone(timedelta(hours=3))
RECENT_DAYS = 31              # окно recent_sessions: 30 дней карточек + день запаса

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


# Сообщения автору о событиях, которые требуют его рук: смена логина, пропажа канала.
# Пишутся в файл ALERTS_PATH (задаёт workflow), и последний шаг workflow, уже после
# коммита данных, завершается ошибкой — GitHub присылает владельцу письмо о сбое.
# Сообщение появляется один раз, в прогоне, где событие случилось впервые, иначе
# письмо приходило бы каждые 10 минут.
ALERTS: list = []


def alert(message: str) -> None:
    print(f"  !!! {message}", file=sys.stderr)
    ALERTS.append(message)


def write_alerts() -> None:
    path = os.environ.get("ALERTS_PATH")
    if ALERTS and path:
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(ALERTS) + "\n")


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

    def users_by_id(self, user_ids: list) -> dict:
        """Get Users по user_id: user_id → объект. Нужен, когда канал перестал
        находиться по логину, — отличить смену логина от бана или удаления."""
        out = {}
        for batch in chunked(user_ids, 100):
            for u in self.get("users", [("id", x) for x in batch]):
                out[u["id"]] = u
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
            # Первое знакомство: берём историю из VOD, если она есть.
            # Но свежайшая запись может быть архивом стрима, который идёт прямо сейчас —
            # такой архив ещё растёт и прошлой сессией не является.
            live_sid = st["live"].get("stream_id")
            candidates = [v for v in videos if not live_sid or v.get("stream_id") != live_sid]
            if candidates:
                st["last_stream"] = session_from_video(candidates[0])
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


# ---------- статистика ----------
#
# in_progress — открытая сессия, которая переживает между прогонами. Внутри копятся
# секунды, а не минуты: прогон идёт раз в ~10 минут, и округление на каждом шаге
# набегало бы. В минуты переводится один раз, при закрытии сессии.

def minsk_month(dt: datetime) -> str:
    return dt.astimezone(MINSK).strftime("%Y-%m")


def prev_month(month: str) -> str:
    y, m = map(int, month.split("-"))
    return f"{y - 1}-12" if m == 1 else f"{y}-{m - 1:02d}"


def blank_stats() -> dict:
    return {
        "monthly": {},
        "recent_sessions": [],
        "in_progress": None,
        "followers": {"count": None, "changed_at": None, "snapshots": {}, "fail_runs": 0},
    }


def blank_month() -> dict:
    return {"launches": 0, "minutes": 0, "hours": [0] * 24, "categories": {}}


def open_progress(stream_id: str, started_at: str, game) -> dict:
    # last_seen_at = начало эфира: время от старта до первого прогона, который эфир
    # увидел, тоже засчитывается — иначе сумма часов не сходилась бы с duration_min.
    return {
        "stream_id": stream_id,
        "started_at": started_at,
        "last_seen_at": started_at,
        "hours_sec": [0] * 24,
        "categories": [{"game": game or None, "sec": 0}],
    }


def advance_progress(prog: dict, until: datetime, game_now=None, switch: bool = False) -> None:
    """Засчитывает время с прошлого прогона до until: в текущий сегмент категории
    и по часам суток по Мінску. Интервал, перешедший границу часа, делится по минутам.
    switch=True — после этого открыть новый сегмент, если категория сменилась."""
    cur = parse_iso(prog["last_seen_at"])
    if until > cur:
        prog["categories"][-1]["sec"] += int((until - cur).total_seconds())
        while cur < until:
            local = cur.astimezone(MINSK)
            next_hour = local.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            piece_end = min(until, next_hour)
            prog["hours_sec"][local.hour] += int((piece_end - cur).total_seconds())
            cur = piece_end
        prog["last_seen_at"] = iso(until)
    # Смена категории: время с прошлого прогона ушло старой, новая начинается сейчас.
    if switch and (game_now or None) != prog["categories"][-1]["game"]:
        prog["categories"].append({"game": game_now or None, "sec": 0})


def track_live(login: str, ss: dict, stream: dict, ts: datetime) -> None:
    prog = ss.get("in_progress")
    if prog and prog.get("stream_id") != stream.get("id"):
        # Сюда попадать не должны: смену stream_id ловит run() и закрывает сессию раньше.
        print(f"  {login}: in_progress от чужого stream_id, начинаем заново", file=sys.stderr)
        prog = None
    if not prog:
        prog = open_progress(stream.get("id"), stream["started_at"], stream.get("game_name"))
        ss["in_progress"] = prog
    advance_progress(prog, ts, stream.get("game_name"), switch=True)


def close_stats(ss: dict, live: dict, closed: dict) -> None:
    """Закрытие сессии: in_progress → recent_sessions и в месяц её начала."""
    prog = ss.get("in_progress")
    if not prog or prog.get("stream_id") != live.get("stream_id"):
        # Статистика эту сессию не видела (файл статистики появился позже или потерялся) —
        # восстанавливаем из того, что знает состояние: всё время на последнюю категорию.
        prog = open_progress(live.get("stream_id"), live["since"], live.get("game"))
    advance_progress(prog, parse_iso(closed["ended_at"]))

    session = {
        "started_at": closed["started_at"],
        "ended_at": closed["ended_at"],
        "hours": [round(s / 60) for s in prog["hours_sec"]],
        "categories": [{"game": c["game"], "minutes": round(c["sec"] / 60)}
                       for c in prog["categories"] if c["sec"] > 0],
    }
    ss["recent_sessions"].insert(0, session)
    ss["in_progress"] = None

    # Сессия через полночь 1-го числа целиком относится к месяцу начала — так три числа
    # карточки (выходы, минуты, часы суток) не расходятся между собой.
    m = ss["monthly"].setdefault(minsk_month(parse_iso(closed["started_at"])), blank_month())
    m["launches"] += 1
    m["minutes"] += closed["duration_min"]
    m["hours"] = [a + b for a, b in zip(m["hours"], session["hours"])]
    for c in session["categories"]:
        if not c["game"]:
            continue
        mc = m["categories"].setdefault(c["game"], {"launches": 0, "minutes": 0})
        mc["launches"] += 1
        mc["minutes"] += c["minutes"]


def prune_stats(ss: dict, ts: datetime) -> None:
    """Держим только текущий и прошлый месяц и сессии за RECENT_DAYS дней.
    Зовётся каждый прогон для всех: у того, кто перестал стримить, старое тоже уходит."""
    keep_from = prev_month(minsk_month(ts))
    for k in [k for k in ss["monthly"] if k < keep_from]:
        del ss["monthly"][k]
    snaps = ss["followers"]["snapshots"]
    for k in [k for k in snaps if k[:7] < keep_from]:
        del snaps[k]
    cutoff = ts - timedelta(days=RECENT_DAYS)
    ss["recent_sessions"] = [s for s in ss["recent_sessions"] if parse_iso(s["ended_at"]) >= cutoff]


def fetch_followers(session: requests.Session, logins: list) -> dict:
    """Число подписчиков одним запросом на всех. Три попытки с паузой, как у Twitch.get().
    Логина, которого Twitch не нашёл, в ответе нет. Все попытки провалились — исключение."""
    last_err = None
    for attempt in range(FOLLOWERS_ATTEMPTS):
        if attempt:
            time.sleep(FOLLOWERS_PAUSE_SEC)
        try:
            r = session.post(
                GQL_URL,
                json={"query": FOLLOWERS_QUERY, "variables": {"logins": logins}},
                headers={"Client-Id": GQL_CLIENT_ID},
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            users = r.json()["data"]["users"]
            out = {}
            for u in users:
                if u and isinstance(u.get("followers", {}).get("totalCount"), int):
                    out[u["login"].lower()] = u["followers"]["totalCount"]
            if not out:
                raise ValueError("в ответе ни одного числа подписчиков")
            return out
        except Exception as e:
            last_err = e
            print(f"  подписчики: попытка {attempt + 1} не удалась — {e}", file=sys.stderr)
    raise RuntimeError(f"подписчики не получены после {FOLLOWERS_ATTEMPTS} попыток: {last_err}")


def update_followers(stats: dict, logins: list, counts, ts: datetime) -> None:
    """counts — ответ fetch_followers или None, если все попытки провалились.
    При сбое старое число не трогаем: ни нуля, ни пропуска."""
    month_key = ts.astimezone(MINSK).strftime("%Y-%m-01")
    for login in logins:
        f = stats["streamers"][login]["followers"]
        count = counts.get(login) if counts else None
        if count is None:
            # Считаем до порога и останавливаемся — как missing_runs: иначе счётчик
            # порождал бы коммит каждый прогон, пока путь сломан.
            if f["fail_runs"] < STALE_RUNS_TO_HIDE:
                f["fail_runs"] += 1
            if f["fail_runs"] >= STALE_RUNS_TO_HIDE:
                print(f"  !!! {login}: ПОДПІСЧЫКІ НЕ СОБІРАЮЦЦА ЎЖО ГАДЗІНУ "
                      f"({STALE_RUNS_TO_HIDE}+ прогонов подряд)", file=sys.stderr)
            else:
                print(f"  {login}: подписчики не получены, прогон {f['fail_runs']} подряд",
                      file=sys.stderr)
            continue
        f["fail_runs"] = 0
        # changed_at — когда число последний раз изменилось, а не «когда проверяли»:
        # отметка проверки менялась бы каждый прогон и порождала коммит каждые 10 минут.
        if count != f["count"]:
            f["count"] = count
            f["changed_at"] = iso(ts)
        # Снимок на 1-е число — первое успешное значение на или после 1-го по Мінску.
        f["snapshots"].setdefault(month_key, count)


def dump_json(path: Path, data: dict, compact_lists: bool = False) -> None:
    text = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
    if compact_lists:
        # Массивы чисел (hours) — в одну строку, иначе 24 строки на каждый.
        text = re.sub(r"\[\s*(-?\d+(?:,\s*-?\d+)*)\s*\]",
                      lambda m: "[" + re.sub(r"\s+", "", m.group(1)) + "]", text)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(text + "\n")


def run() -> int:
    client_id = os.environ.get("TWITCH_CLIENT_ID")
    client_secret = os.environ.get("TWITCH_CLIENT_SECRET")
    if not client_id or not client_secret:
        print("Нет TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET", file=sys.stderr)
        return 2

    registry = load_json(REGISTRY_PATH, {"streamers": []})["streamers"]
    known = {s["login"].lower() for s in registry}
    logins = [s["login"].lower() for s in registry if not s.get("hidden")]
    if not logins:
        print("Реестр пуст — нечего собирать")
        return 0

    state = load_json(STATE_PATH, {"updated_at": None, "streamers": {}})
    before = json.dumps(state.get("streamers", {}), sort_keys=True, ensure_ascii=False)
    stats = load_json(STATS_PATH, {"updated_at": None, "collecting_since": None, "streamers": {}})
    # updated_at в сравнение не входит — по той же причине, что у состояния.
    stats_before = json.dumps({k: v for k, v in stats.items() if k != "updated_at"},
                              sort_keys=True, ensure_ascii=False)

    # Записи, чьих логинов в реестре больше нет. Удаляются не сразу: сначала
    # проверяем, не сменил ли кто-то из них логин (ниже, после Get Users).
    orphans = [k for k in state["streamers"] if k not in known]
    # Логины, которых в состоянии ещё не было, — кандидаты в «новое имя» сироты.
    fresh = [l for l in logins if l not in state["streamers"]]

    for login in logins:
        state["streamers"].setdefault(login, blank_streamer())
        stats["streamers"].setdefault(login, blank_stats())

    tw = Twitch(client_id, client_secret)

    # --- профили ---
    users = tw.users(logins)

    # Смена логина: автор поправил login в той же записи реестра (added_at остался).
    # user_id на Twitch не меняется никогда — по нему старая запись и находится.
    # История и статистика переезжают на новый логин, а не обнуляются.
    orphan_by_uid = {state["streamers"][k]["user_id"]: k
                     for k in orphans if state["streamers"][k].get("user_id")}
    for login in fresh:
        u = users.get(login)
        old = orphan_by_uid.get(u["id"]) if u else None
        if not old:
            continue
        state["streamers"][login] = state["streamers"].pop(old)
        if old in stats["streamers"]:
            stats["streamers"][login] = stats["streamers"].pop(old)
        orphans.remove(old)
        print(f"  {old} → {login}: логин сменился, история перенесена")

    # Убранный из реестра стример уходит и из состояния, иначе его данные висят вечно.
    # У скрытого (hidden) запись сохраняется: скрытие — временное, история не теряется.
    for gone in orphans:
        del state["streamers"][gone]
        print(f"  {gone}: убран из реестра, запись удалена")
    for gone in [k for k in stats["streamers"] if k not in known]:
        del stats["streamers"][gone]

    # Кто не нашёлся по логину, но известен по user_id, — спрашиваем по user_id:
    # нашёлся под другим логином — это смена логина, а не бан.
    lost_ids = [state["streamers"][l]["user_id"] for l in logins
                if l not in users and state["streamers"][l].get("user_id")]
    try:
        by_id = tw.users_by_id(lost_ids) if lost_ids else {}
    except Exception as e:
        print(f"  поиск по user_id не удался — {e}", file=sys.stderr)
        by_id = {}

    for login in logins:
        st = state["streamers"][login]
        u = users.get(login)
        if not u:
            moved = by_id.get(st.get("user_id"))
            new_login = moved["login"].lower() if moved else None
            if new_login and st.get("renamed_to") != new_login:
                st["renamed_to"] = new_login
                alert(f"{login}: логин на Twitch сменился на {new_login}. В streamers.json "
                      f"поправь login в записи {login} на {new_login} (added_at не трогай) — "
                      f"история и статистика перенесутся сами. Пока не поправлено, через час "
                      f"карточка скроется с сайта.")
            # Считаем до порога и останавливаемся: иначе счётчик рос бы вечно
            # и каждый прогон порождал бы коммит из-за одного пропавшего канала.
            if st.get("missing_runs", 0) < STALE_RUNS_TO_HIDE:
                st["missing_runs"] = st.get("missing_runs", 0) + 1
                if st["missing_runs"] >= STALE_RUNS_TO_HIDE:
                    st["stale"] = True
                    print(f"  {login}: канал не отвечает {STALE_RUNS_TO_HIDE} прогонов, помечен stale",
                          file=sys.stderr)
                    if not st.get("renamed_to"):
                        alert(f"{login}: канала нет на Twitch уже {STALE_RUNS_TO_HIDE} прогонов "
                              f"(около часа) — бан или удаление. Карточка скрыта с сайта; "
                              f"вернётся сама, если канал вернётся.")
            continue
        st["missing_runs"] = 0
        st.pop("stale", None)
        st.pop("renamed_to", None)
        st["user_id"] = u["id"]
        st["display_name"] = u["display_name"]
        st["profile_image_url"] = u["profile_image_url"]
        st["description"] = u.get("description") or None
        st["broadcaster_type"] = u.get("broadcaster_type", "")

    # --- кто в эфире ---
    ids = [state["streamers"][l]["user_id"] for l in logins if state["streamers"][l].get("user_id")]
    live_now = tw.streams(ids) if ids else {}

    # Без микросекунд: отметки в in_progress пишутся с точностью до секунды,
    # и интервалы между прогонами должны считаться от тех же значений.
    ts = now().replace(microsecond=0)

    def close_session(login: str, st: dict, ended_at: datetime) -> None:
        live = st["live"]
        closed = session_from_live(live, ended_at)
        st["last_stream"] = closed
        st["history"] = ([closed] + st.get("history", []))[:HISTORY_LIMIT]
        st["live"] = {"since": None, "title": None, "game": None, "viewers": None, "stream_id": None}
        close_stats(stats["streamers"][login], live, closed)
        print(f"  {login}: стрим закончился, {closed['duration_min']} мин")

    for login in logins:
        st = state["streamers"][login]
        uid = st.get("user_id")
        if not uid:
            continue
        was_live = st["live"]["since"] is not None
        s = live_now.get(uid)

        if s:
            # Канал упал и поднялся внутри одного интервала опроса: офлайна мы не видели,
            # но stream_id сменился — это второй выход, а не продолжение первого.
            # Конец первой сессии — не позже начала второй, иначе часы задвоятся.
            old_sid = st["live"].get("stream_id")
            if was_live and old_sid and s.get("id") and s["id"] != old_sid:
                print(f"  {login}: stream_id сменился без офлайна — рестарт, закрываем прошлую сессию")
                ended = min(ts, parse_iso(s["started_at"]))
                close_session(login, st, max(ended, parse_iso(st["live"]["since"])))
            st["live"] = {
                "since": s["started_at"],
                "title": s.get("title"),
                "game": s.get("game_name"),
                "viewers": s.get("viewer_count"),
                "stream_id": s.get("id"),
            }
            track_live(login, stats["streamers"][login], s, ts)
        elif was_live:
            close_session(login, st, ts)

    # --- подписчики ---
    try:
        counts = fetch_followers(requests.Session(), logins)
    except Exception as e:
        print(f"  {e}", file=sys.stderr)
        counts = None
    update_followers(stats, logins, counts, ts)

    for login in logins:
        prune_stats(stats["streamers"][login], ts)
    if not stats.get("collecting_since"):
        # Пишется один раз: по нему сайт отличает «не стримил» от «бот ещё не собирал».
        stats["collecting_since"] = ts.astimezone(MINSK).strftime("%Y-%m-%d")

    # --- VOD ---
    # Проверяем раз в час: только в первом прогоне часа. Расписание определяется
    # часами, а не файлом состояния, — иначе отметку о проверке пришлось бы
    # коммитить каждый час даже там, где ничего не поменялось.
    # Первый запуск (пустое состояние) — проверяем сразу, чтобы подтянуть историю из VOD.
    first_run = not any(state["streamers"][l].get("last_stream") for l in logins)
    if first_run or ts.minute < VOD_CHECK_WINDOW_MIN or os.environ.get("FORCE_VOD_CHECK"):
        update_vods(tw, state, logins)

    # --- запись только при изменениях, каждый файл отдельно ---
    after = json.dumps(state["streamers"], sort_keys=True, ensure_ascii=False)
    if after == before:
        print("Изменений нет, файл не трогаем")
    else:
        state["updated_at"] = iso(ts)
        dump_json(STATE_PATH, state)
        print(f"Состояние обновлено: {STATE_PATH}")

    stats_after = json.dumps({k: v for k, v in stats.items() if k != "updated_at"},
                             sort_keys=True, ensure_ascii=False)
    if stats_after == stats_before:
        print("Статистика без изменений, файл не трогаем")
    else:
        stats["updated_at"] = iso(ts)
        dump_json(STATS_PATH, stats, compact_lists=True)
        print(f"Статистика обновлена: {STATS_PATH}")
    write_alerts()
    return 0


if __name__ == "__main__":
    sys.exit(run())
