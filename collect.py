#!/usr/bin/env python3
"""
Сборщик состояния беларуских Twitch-стримеров для mashninski.com/strymy.

Что делает за один прогон:
  1. берёт app access token (client credentials);
  2. Get Users   — аватар, описание, broadcaster_type, user_id;
  3. Get Streams — кто сейчас в эфире;
  4. ловит переходы онлайн→офлайн и закрывает сессию;
  5. Get Videos: ищет VOD к сессиям без записи (у кого такая есть за 7 дней —
     каждый прогон, остальным раз в час), проверяет, не исчезли ли старые;
     по найденной записи пересчитывает статистику сессии;
  6. пишет data/twitch-state.json, только если состояние изменилось;
  7. копит статистику (категории, часы суток по Мінску, месячные счётчики,
     подписчики) и пишет data/twitch-stats.json — тоже только при изменениях;
  8. пишет архив месяцев data/archive/YYYY-MM.json для текущего и прошлого
     месяца — тоже только при изменениях.

Ключи читаются из переменных окружения TWITCH_CLIENT_ID и TWITCH_CLIENT_SECRET.
Устройство статистики — claude/twitch-stats-spec.md в репозитории сайта,
архива месяцев — claude/twitch-chronicle-spec.md, §4, там же.
"""

import copy
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
ARCHIVE_DIR = ROOT / "data" / "archive"

HELIX = "https://api.twitch.tv/helix"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"

# Внутренний GraphQL Twitch — им пользуется сама страница twitch.tv. Недокументированный,
# формат может поменяться без предупреждения (риск принят, спека статистики §8).
# Client-Id — публичный идентификатор веб-клиента twitch.tv, виден в коде любой страницы
# канала; это не наш секрет. Переменная окружения — на случай, если Twitch его сменит.
GQL_URL = "https://gql.twitch.tv/gql"
GQL_CLIENT_ID = os.environ.get("TWITCH_GQL_CLIENT_ID", "kimne78kx3ncx6brgo4mv6wki5h1ko")
# Тем же запросом — начало последнего эфира (lastBroadcast) и идёт ли эфир сейчас (stream):
# дата прошлого эфира для «Вяртання» на сайте (спека статистики, §9, «Точное число»).
FOLLOWERS_QUERY = ("query($logins:[String!]){users(logins:$logins)"
                   "{login followers{totalCount} lastBroadcast{startedAt} stream{id}}}")
# Категории записи — главы смены игры, как их показывает плеер twitch.tv. Helix их
# не отдаёт. Нужны только для эфиров, которые бот не видел вовсе (missed_sessions).
VIDEO_QUERY = ("query($id:ID){video(id:$id){lengthSeconds game{name} "
               "moments(momentRequestType:VIDEO_CHAPTER_MARKERS){edges{node{"
               "positionMilliseconds durationMilliseconds "
               "details{... on GameChangeMomentDetails{game{name}}}}}}}}")
FOLLOWERS_ATTEMPTS = 3
FOLLOWERS_PAUSE_SEC = 2.5

# Беларусь весь год на UTC+3, без перехода на летнее время — смещение фиксированное.
MINSK = timezone(timedelta(hours=3))
RECENT_DAYS = 31              # окно recent_sessions: 30 дней карточек + день запаса

VOD_CHECK_WINDOW_MIN = 10     # всем Get Videos — в первом прогоне каждого часа
# У кого есть сессия без записи, закончившаяся за столько дней, — Get Videos
# на каждом прогоне: Actions пропускает часы, и окно minute < 10 может не наступить.
VOD_PENDING_DAYS = 7
VIDEOS_LIMIT = 20             # сколько записей брать: сессии ищутся по всей history
VOD_LOOKBACK_DAYS = 65        # дольше максимального срока хранения VOD (60 дней)
HISTORY_LIMIT = 20            # сколько сессий храним на стримера
STALE_RUNS_TO_HIDE = 6        # после скольких прогонов без ответа считаем канал пропавшим
# Насколько раньше первой сессии history должен начаться эфир из lastBroadcast, чтобы
# считаться прошлым, а не той же сессией: Twitch и Helix расходятся в начале на секунды.
PREV_BROADCAST_MARGIN = timedelta(hours=1)

TIMEOUT = 20


# ---------- утилиты ----------

def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


DURATION_RE = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")


def parse_duration_sec(s: str) -> int:
    """Twitch отдаёт длительность строкой вида '3h8m33s'. Возвращаем секунды."""
    m = DURATION_RE.fullmatch(s or "")
    if not m:
        return 0
    h, mi, sec = (int(x) if x else 0 for x in m.groups())
    return h * 3600 + mi * 60 + sec


def minutes_between(start: datetime, end: datetime) -> int:
    # Одна формула длительности на всё: состояние, monthly.minutes и архив месяца
    # считают одинаково, и суммы между файлами сходятся.
    return max(1, round((end - start).total_seconds() / 60))


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

    def archives(self, user_id: str, limit: int = VIDEOS_LIMIT) -> list:
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
        "duration_min": minutes_between(started, ended_at),
        "title": live.get("title"),
        "game": live.get("game"),
        "stream_id": live.get("stream_id"),
        "source": "observed",
        "vod": None,
    }


def session_from_video(v: dict) -> dict:
    started = parse_iso(v["created_at"])
    ended = started + timedelta(seconds=parse_duration_sec(v.get("duration", "")))
    return {
        "started_at": v["created_at"],
        "ended_at": iso(ended),
        "duration_min": minutes_between(started, ended),
        "title": v.get("title"),
        "game": None,
        "stream_id": v.get("stream_id"),
        "source": "vod",
        "vod": {"id": v["id"], "url": v["url"], "seen_at": iso(now()), "gone": False},
    }


def vod_pending(st: dict, ts: datetime) -> bool:
    """Есть наблюдённая сессия без записи, закончившаяся за VOD_PENDING_DAYS дней.
    Таким Get Videos — на каждом прогоне, остальным — раз в час. Решается по самим
    сессиям, без отметки «когда проверяли»: она порождала бы коммит каждый прогон."""
    cutoff = ts - timedelta(days=VOD_PENDING_DAYS)
    return any(h.get("source") == "observed" and not h.get("vod")
               and parse_iso(h["ended_at"]) >= cutoff
               for h in st.get("history", []))


def apply_vod(session: dict, v: dict, limit) -> None:
    """Запись нашлась: конец и длительность — по ней, они точные. Конец наблюдения —
    время первого прогона, увидевшего офлайн, а прогоны Actions пропускает часами.
    limit — начало следующей сессии: конец не позже него, иначе часы суток задвоятся."""
    session["vod"] = {"id": v["id"], "url": v["url"], "seen_at": iso(now()), "gone": False}
    sec = parse_duration_sec(v.get("duration", ""))
    if not sec:
        return
    started = parse_iso(session["started_at"])
    ended = started + timedelta(seconds=sec)
    if limit and ended > limit:
        ended = max(started, limit)
    session["ended_at"] = iso(ended)
    session["duration_min"] = minutes_between(started, ended)
    session["source"] = "vod"


def fetch_chapters(session: requests.Session, video_id: str) -> list:
    """Категории записи по главам: [(игра, секунды)] по порядку. Глав нет — вся запись
    одной игрой. Сбой — исключение: эфир не добавится, его возьмёт следующий прогон."""
    r = session.post(GQL_URL, json={"query": VIDEO_QUERY, "variables": {"id": video_id}},
                     headers={"Client-Id": GQL_CLIENT_ID}, timeout=TIMEOUT)
    r.raise_for_status()
    video = r.json()["data"]["video"]
    if video is None:
        raise ValueError(f"видео {video_id} нет в ответе")
    nodes = sorted((e["node"] for e in (video.get("moments") or {}).get("edges", [])),
                   key=lambda n: n["positionMilliseconds"])
    if nodes:
        return [(((n.get("details") or {}).get("game") or {}).get("name"),
                 n["durationMilliseconds"] // 1000) for n in nodes]
    return [((video.get("game") or {}).get("name"), video.get("lengthSeconds") or 0)]


def progress_from_chapters(started: datetime, ended: datetime, chapters: list) -> dict:
    """Те же секунды по часам суток и сегментам категорий, что копит живая сессия, —
    но по главам записи. Последняя глава тянется до конца записи."""
    prog = {"last_seen_at": iso(started), "hours_sec": [0] * 24, "categories": []}
    cur = started
    for i, (game, sec) in enumerate(chapters):
        until = ended if i == len(chapters) - 1 else min(ended, cur + timedelta(seconds=sec))
        prog["categories"].append({"game": game or None, "sec": 0})
        advance_progress(prog, until)
        cur = max(cur, until)
    return prog


def missed_sessions(login: str, st: dict, ss, videos: list, since, gql: requests.Session) -> None:
    """Эфиры, которых бот не видел вовсе: целиком попали в дыру между прогонами
    (03.10.2026 так пропали два). Запись есть — сессия добавляется по ней в history,
    а если её время учтено бы статистикой, то и в recent_sessions и месяц.
    Берутся только записи новее самой старой сессии history: раньше бот не наблюдал,
    там пропусков нет. Пересекается с известной сессией — не берётся: время задвоилось бы.
    since — collecting_since статистики, ss — статистика стримера."""
    history = st.get("history", [])
    if not history:
        return
    known = {h.get("stream_id") for h in history}
    oldest = parse_iso(history[-1]["started_at"])
    ts = now()
    for v in videos:
        sid = v.get("stream_id")
        sec = parse_duration_sec(v.get("duration", ""))
        started = parse_iso(v["created_at"])
        if not sid or sid in known or sid == st["live"].get("stream_id") or not sec or started <= oldest:
            continue
        ended = started + timedelta(seconds=sec)
        spans = [(parse_iso(h["started_at"]), parse_iso(h["ended_at"])) for h in history]
        if st["live"].get("since"):
            spans.append((parse_iso(st["live"]["since"]), ts))
        if any(a < ended and started < b for a, b in spans):
            print(f"  {login}: запись {v['id']} пересекается с известной сессией, не берём")
            continue
        try:
            chapters = fetch_chapters(gql, v["id"])
        except Exception as e:
            print(f"  {login}: главы записи {v['id']} не получены — {e}", file=sys.stderr)
            continue
        session = session_from_video(v)
        session["game"] = chapters[-1][0]
        pos = next((i for i, h in enumerate(history) if parse_iso(h["started_at"]) < started),
                   len(history))
        history.insert(pos, session)
        del history[HISTORY_LIMIT:]
        known.add(sid)
        print(f"  {login}: эфир {session['started_at']} бот не видел — добавлен по записи, "
              f"{session['duration_min']} мин")
        # В статистику — по тем же правилам, что закрытая сессия: только время, которое
        # бот собирал, и только пока месяц и окно recent_sessions ещё живы.
        month = minsk_month(started)
        if (ss is not None and since and started.astimezone(MINSK).strftime("%Y-%m-%d") >= since
                and month >= prev_month(minsk_month(ts)) and ended >= ts - timedelta(days=RECENT_DAYS)):
            add_session_stats(ss, progress_from_chapters(started, ended, chapters), session)


def update_vods(tw: Twitch, state: dict, stats: dict, logins: list) -> None:
    """Привязывает VOD ко всем сессиям history, у которых записи ещё нет, подтягивает
    историю задним числом, добавляет эфиры, которых бот не видел, помечает исчезнувшие
    записи. Уточнённые концы в статистику переносит sync_stats_with_vods."""
    cutoff = now() - timedelta(days=VOD_LOOKBACK_DAYS)
    gql = requests.Session()
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

        live_sid = st["live"].get("stream_id")
        if last is None:
            # Первое знакомство: берём историю из VOD, если она есть.
            # Но свежайшая запись может быть архивом стрима, который идёт прямо сейчас —
            # такой архив ещё растёт и прошлой сессией не является.
            candidates = [v for v in videos if not live_sid or v.get("stream_id") != live_sid]
            if candidates:
                st["last_stream"] = session_from_video(candidates[0])
            continue

        history = st.get("history", [])
        twin = bool(history) and history[0]["started_at"] == last["started_at"]
        by_stream = {v.get("stream_id"): v for v in videos if v.get("stream_id")}
        for i, h in enumerate(history):
            if h.get("source") != "observed" or h.get("vod"):
                continue
            match = by_stream.get(h.get("stream_id"))
            # Архив идущего эфира ещё растёт — его длительность не конец сессии.
            if not match or h.get("stream_id") == live_sid:
                continue
            # history — свежие сверху: следующая сессия стоит перед этой.
            if i:
                limit = parse_iso(history[i - 1]["started_at"])
            else:
                limit = parse_iso(st["live"]["since"]) if st["live"].get("since") else None
            apply_vod(h, match, limit)
            print(f"  {login}: запись к сессии {h['started_at']} — {h['duration_min']} мин")

        # До 04.10.2026 запись привязывалась только к last_stream, а history[0] оставалась
        # наблюдённой. Если запись с тех пор исчезла и выше не нашлась — берём найденное тогда.
        if twin and last.get("source") == "vod" and history[0].get("source") == "observed":
            history[0] = copy.deepcopy(last)

        # Исчезновение записи проверяем у последней сессии, как раньше.
        newest = history[0] if twin else last
        vod = newest.get("vod")
        if vod and not vod.get("gone") and newest.get("stream_id") not in by_stream:
            # Запись была, а теперь её нет — истекла или удалена.
            vod["gone"] = True
        # Если vod никогда не было — значит стример не сохраняет трансляции. Оставляем None.

        missed_sessions(login, st, stats["streamers"].get(login), videos,
                        stats.get("collecting_since"), gql)
        # last_stream — та же сессия, что history[0]: одна правда на двоих. Пропущенный
        # эфир мог встать в history первым — тогда последний стрим теперь он.
        if history and parse_iso(history[0]["started_at"]) >= parse_iso(last["started_at"]) \
                and last != history[0]:
            st["last_stream"] = copy.deepcopy(history[0])


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
    ss["in_progress"] = None
    add_session_stats(ss, prog, closed)


def add_session_stats(ss: dict, prog: dict, closed: dict) -> None:
    """Готовая сессия → recent_sessions и месяц её начала. prog — накопленные секунды
    по часам суток и сегментам категорий, closed — сессия из состояния."""
    session = {
        "started_at": closed["started_at"],
        "ended_at": closed["ended_at"],
        "hours": [round(s / 60) for s in prog["hours_sec"]],
        "categories": [{"game": c["game"], "minutes": round(c["sec"] / 60)}
                       for c in prog["categories"] if c["sec"] > 0],
    }
    # Свежие сверху. Закрытая сессия и так самая свежая, а пропущенная, добавленная
    # по записи задним числом, встаёт на своё место.
    recent = ss["recent_sessions"]
    pos = next((i for i, r in enumerate(recent)
                if parse_iso(r["started_at"]) < parse_iso(session["started_at"])), len(recent))
    recent.insert(pos, session)

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


def hours_minutes(start: datetime, end: datetime) -> list:
    """Минуты эфира по часам суток по Мінску — тем же advance_progress, которым их
    копит живая сессия: эфир непрерывен, и часы сессии — это покрытие [start, end]."""
    prog = {"last_seen_at": iso(start), "hours_sec": [0] * 24, "categories": [{"game": None, "sec": 0}]}
    advance_progress(prog, end)
    return [round(s / 60) for s in prog["hours_sec"]]


def retime_stats(ss: dict, started_at: str, ended_at: str) -> bool:
    """Конец сессии уточнился по записи — пересчитать её в recent_sessions и в месяце:
    минуты, часы суток, категории. Хвост после настоящего конца снимается с последнего
    сегмента категории: смену категории видно только на прогонах, а после последнего
    прогона, видевшего эфир, весь остаток шёл последнему сегменту.
    Конец уже совпадает — ничего не делает, повторный прогон файл не меняет.
    Возвращает, был ли пересчёт."""
    rec = next((r for r in ss["recent_sessions"] if r["started_at"] == started_at), None)
    if rec is None or rec["ended_at"] == ended_at:
        return False
    start, end = parse_iso(started_at), parse_iso(ended_at)
    new_hours = hours_minutes(start, end)
    # Ровно столько close_stats когда-то прибавил к monthly.minutes — столько и сдвигаем.
    delta = minutes_between(start, end) - session_minutes(rec)

    old_cats = rec["categories"]
    cats = [dict(c) for c in old_cats]
    if delta > 0 and cats:
        cats[-1]["minutes"] += delta
    rest = -delta
    for c in reversed(cats):
        if rest <= 0:
            break
        take = min(rest, c["minutes"])
        c["minutes"] -= take
        rest -= take
    # Сегмент, от которого ничего не осталось, — эфир кончился раньше, чем он начался:
    # это не запуск категории.
    dropped = [o["minutes"] > 0 and c["minutes"] == 0 for o, c in zip(old_cats, cats)]

    m = ss["monthly"].get(minsk_month(start))
    if m is not None:
        m["minutes"] = max(0, m["minutes"] + delta)
        m["hours"] = [max(0, a - b + c) for a, b, c in zip(m["hours"], rec["hours"], new_hours)]
        for o, c, gone in zip(old_cats, cats, dropped):
            mc = m["categories"].get(o["game"]) if o["game"] else None
            if mc is None:
                continue
            mc["minutes"] = max(0, mc["minutes"] + c["minutes"] - o["minutes"])
            if gone:
                mc["launches"] -= 1
                if mc["launches"] <= 0:
                    del m["categories"][o["game"]]

    rec["ended_at"] = ended_at
    rec["hours"] = new_hours
    rec["categories"] = [c for c, gone in zip(cats, dropped) if not gone]
    return True


def sync_stats_with_vods(login: str, st: dict, ss: dict) -> None:
    """Статистика идёт за состоянием: у сессии с записью конец в recent_sessions и месяце
    должен быть тот же, что в history. Пересчёт случается один раз — при переходе
    observed → vod; для сессий, записанных до 04.10.2026, — на первом прогоне нового кода.
    Архив месяца собирается из recent_sessions и monthly и подтягивается сам."""
    for h in st.get("history", []):
        if h.get("source") == "vod" and retime_stats(ss, h["started_at"], h["ended_at"]):
            print(f"  {login}: статистика сессии {h['started_at']} пересчитана по записи")


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


def fetch_followers(session: requests.Session, logins: list) -> tuple:
    """Число подписчиков одним запросом на всех. Три попытки с паузой, как у Twitch.get().
    Возвращает (подписчики, эфиры): эфиры — начало последнего эфира у тех, кто сейчас
    не в эфире. Логина, которого Twitch не нашёл, в ответе нет. Все попытки провалились —
    исключение."""
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
            out, broadcasts = {}, {}
            for u in users:
                if u and isinstance(u.get("followers", {}).get("totalCount"), int):
                    out[u["login"].lower()] = u["followers"]["totalCount"]
                # В эфире lastBroadcast — уже идущий эфир, а не прошлый: такой не берём.
                started = ((u or {}).get("lastBroadcast") or {}).get("startedAt")
                if started and u.get("stream") is None:
                    broadcasts[u["login"].lower()] = iso(parse_iso(started))
            if not out:
                raise ValueError("в ответе ни одного числа подписчиков")
            return out, broadcasts
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


def update_prev_broadcast(state: dict, logins: list, broadcasts) -> None:
    """prev_broadcast_at — начало последнего эфира до первой сессии в history: по нему
    сайт считает перерыв перед первой сессией точно, а не «больш за» от added_at
    (спека статистики, §9, «Точное число»). broadcasts — из fetch_followers или None.

    Не «последний эфир вообще»: после эфира-возвращения Twitch отдаёт уже его, и прошлая
    дата затёрлась бы. Поэтому берётся только эфир раньше первой сессии history — пока
    history пуста, любой, потом значение больше не меняется. Отсюда же «коммит только
    при изменениях»: поле меняется, только когда канал с пустой history стримил.
    Сбой запроса и канал в эфире старое значение не трогают."""
    if not broadcasts:
        return
    for login in logins:
        st = state["streamers"][login]
        started = broadcasts.get(login)
        if not started or st["live"]["since"] is not None:
            continue
        history = st.get("history", [])
        if history:
            first = min(parse_iso(h["started_at"]) for h in history)
            if parse_iso(started) > first - PREV_BROADCAST_MARGIN:
                continue
        if st.get("prev_broadcast_at") != started:
            st["prev_broadcast_at"] = started
            print(f"  {login}: прошлый эфир по Twitch — {started}")


def dump_json(path: Path, data: dict, compact_lists: bool = False) -> None:
    text = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True)
    if compact_lists:
        # Массивы чисел (hours) — в одну строку, иначе 24 строки на каждый.
        text = re.sub(r"\[\s*(-?\d+(?:,\s*-?\d+)*)\s*\]",
                      lambda m: "[" + re.sub(r"\s+", "", m.group(1)) + "]", text)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(text + "\n")


# ---------- архив месяцев ----------
#
# data/archive/YYYY-MM.json — всё, что сайту нужно, чтобы показать месяц, когда его
# в twitch-stats.json уже нет. Спека — claude/twitch-chronicle-spec.md, §4, в репозитории
# сайта. Файл пишется, пока месяц текущий или прошлый, потом не трогается: заморожен сам.
#
# Сессии копятся в самом файле: recent_sessions держит 31 день, history — 20 сессий,
# а месяцу нужны все. Поэтому файл собирается поверх своей прошлой версии, ключ
# сессии — started_at: одна и та же сессия не задваивается, ушедшая из recent_sessions
# не теряется.

def next_month(month: str) -> str:
    y, m = map(int, month.split("-"))
    return f"{y + 1}-01" if m == 12 else f"{y}-{m + 1:02d}"


def session_minutes(s: dict) -> int:
    # Та же формула, что в session_from_live: ровно столько close_stats прибавил
    # к monthly.minutes (а retime_stats потом сдвинул), и сумма по сессиям месяца
    # с minutes сходится. Источник — recent_sessions, не history: только они учтены в monthly.
    return minutes_between(parse_iso(s["started_at"]), parse_iso(s["ended_at"]))


def archive_entry(month: str, old: dict, ss: dict, st: dict, added_at) -> dict:
    """Запись стримера за месяц: old — его запись из прошлой версии файла (или {}),
    ss — его статистика, st — его состояние."""
    sessions = {s["started_at"]: s for s in old.get("sessions", [])}
    # Сессия через полночь 1-го числа — в месяце начала, как в monthly.
    for s in ss["recent_sessions"]:
        if minsk_month(parse_iso(s["started_at"])) == month:
            sessions[s["started_at"]] = {"started_at": s["started_at"], "ended_at": s["ended_at"],
                                         "duration_min": session_minutes(s)}
    sessions = sorted(sessions.values(), key=lambda s: parse_iso(s["started_at"]))

    # Конец последней сессии до первой сессии месяца — для «Вяртання» с первой же сессии.
    # history держит 20 сессий, и к концу месяца нужная может из неё уйти,
    # поэтому найденное раньше значение остаётся кандидатом.
    prev_ended_at = None
    if sessions:
        first = parse_iso(sessions[0]["started_at"])
        candidates = [h["ended_at"] for h in st.get("history", [])
                      if parse_iso(h["started_at"]) < first]
        if old.get("prev_ended_at") and parse_iso(old["prev_ended_at"]) <= first:
            candidates.append(old["prev_ended_at"])
        if candidates:
            prev_ended_at = max(candidates, key=parse_iso)

    # Месяц из monthly не уходит, пока файл пишется. Если его там нет, а в файле
    # счётчики есть (файл статистики потерялся), — старые не затираются нулями.
    counters = ss["monthly"].get(month)
    if counters is None:
        counters = {k: old[k] for k in blank_month() if k in old} or blank_month()

    # Бот не знал конца прошлой сессии — начало прошлого эфира по Twitch, отдельным полем:
    # это начало, а не конец. Только пока history короче предела: полная могла потерять
    # сессии после этого эфира, и он уже не «прошлый» для первой сессии месяца.
    prev_broadcast_at = None
    if sessions and prev_ended_at is None:
        limit = parse_iso(sessions[0]["started_at"]) - PREV_BROADCAST_MARGIN
        candidates = [old.get("prev_broadcast_at")]
        if len(st.get("history", [])) < HISTORY_LIMIT:
            candidates.append(st.get("prev_broadcast_at"))
        candidates = [c for c in candidates if c and parse_iso(c) <= limit]
        if candidates:
            prev_broadcast_at = max(candidates, key=parse_iso)

    snaps = ss["followers"]["snapshots"]
    return {
        "display_name": st.get("display_name") or old.get("display_name"),
        "added_at": added_at or old.get("added_at"),
        **{k: counters.get(k, v) for k, v in blank_month().items()},
        "sessions": sessions,
        "prev_ended_at": prev_ended_at,
        "prev_broadcast_at": prev_broadcast_at,
        "followers_start": snaps.get(f"{month}-01", old.get("followers_start")),
        "followers_end": snaps.get(f"{next_month(month)}-01", old.get("followers_end")),
    }


def update_archive(stats: dict, state: dict, registry: list, logins: list, ts: datetime,
                   renamed: dict, archive_dir: Path = ARCHIVE_DIR) -> list:
    """Пишет файлы текущего и прошлого месяца, только при изменениях.
    Запросов к Twitch нет — всё из stats и state. Возвращает записанные месяцы.
    renamed — {старый логин: новый}, логины, сменившиеся в этом прогоне."""
    since = stats.get("collecting_since")
    if not since:
        return []
    added = {s["login"].lower(): s.get("added_at") for s in registry}
    cur = minsk_month(ts)
    written = []
    # До месяца, в котором бот начал собирать, данных нет — такого файла не будет.
    for month in [m for m in (prev_month(cur), cur) if m >= since[:7]]:
        path = archive_dir / f"{month}.json"
        old = load_json(path, {})
        streamers = dict(old.get("streamers", {}))
        # Смена логина: запись месяца переезжает, а не появляется вторая — с теми же сессиями.
        for old_login, new_login in renamed.items():
            if old_login in streamers and new_login not in streamers:
                streamers[new_login] = streamers.pop(old_login)
        # Обновляются только те, кого бот собирает сейчас. Убранный из реестра или скрытый
        # остаётся в файле таким, каким был: в своём месяце человек не пропадает.
        for login in logins:
            streamers[login] = archive_entry(month, streamers.get(login, {}),
                                             stats["streamers"][login],
                                             state["streamers"].get(login, {}), added.get(login))
        data = {"month": month, "collecting_since": since, "streamers": streamers}
        # updated_at в сравнение не входит — по той же причине, что у состояния.
        if data == {k: v for k, v in old.items() if k != "updated_at"}:
            continue
        data["updated_at"] = iso(ts)
        dump_json(path, data, compact_lists=True)
        written.append(month)
    return written


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
    renamed = {}  # старый логин → новый, для архива месяцев
    for login in fresh:
        u = users.get(login)
        old = orphan_by_uid.get(u["id"]) if u else None
        if not old:
            continue
        state["streamers"][login] = state["streamers"].pop(old)
        if old in stats["streamers"]:
            stats["streamers"][login] = stats["streamers"].pop(old)
        orphans.remove(old)
        renamed[old] = login
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
        counts, broadcasts = fetch_followers(requests.Session(), logins)
    except Exception as e:
        print(f"  {e}", file=sys.stderr)
        counts, broadcasts = None, None
    update_followers(stats, logins, counts, ts)
    update_prev_broadcast(state, logins, broadcasts)

    for login in logins:
        prune_stats(stats["streamers"][login], ts)
    if not stats.get("collecting_since"):
        # Пишется один раз: по нему сайт отличает «не стримил» от «бот ещё не собирал».
        stats["collecting_since"] = ts.astimezone(MINSK).strftime("%Y-%m-%d")

    # --- VOD ---
    # Всем — раз в час, в первом прогоне часа; у кого есть сессия без записи за
    # VOD_PENDING_DAYS дней — на каждом прогоне. Расписание определяется часами
    # и самими сессиями, а не файлом состояния, — иначе отметку о проверке пришлось бы
    # коммитить каждый прогон даже там, где ничего не поменялось.
    # Первый запуск (пустое состояние) — проверяем сразу, чтобы подтянуть историю из VOD.
    first_run = not any(state["streamers"][l].get("last_stream") for l in logins)
    if first_run or ts.minute < VOD_CHECK_WINDOW_MIN or os.environ.get("FORCE_VOD_CHECK"):
        vod_logins = logins
    else:
        vod_logins = [l for l in logins if vod_pending(state["streamers"][l], ts)]
    if vod_logins:
        update_vods(tw, state, stats, vod_logins)
    for login in logins:
        sync_stats_with_vods(login, state["streamers"][login], stats["streamers"][login])

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

    # Архив — после записи состояния и статистики: его сбой не должен их задержать.
    # Но и молча пропадать не должен — сессии месяца живут в recent_sessions 31 день.
    try:
        written = update_archive(stats, state, registry, logins, ts, renamed)
        print(f"Архив месяцев обновлён: {', '.join(written)}" if written
              else "Архив месяцев без изменений")
    except Exception as e:
        alert(f"архив месяцев (data/archive) не записан — {e}")
    write_alerts()
    return 0


if __name__ == "__main__":
    sys.exit(run())
