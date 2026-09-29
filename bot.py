import os
import re
import json
import secrets
import time
import asyncio
import aiosqlite
import aiohttp
import hashlib
from collections import Counter
from aiohttp import web
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import logging

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl import functions as tg_functions

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    asyncpg = None
    HAS_ASYNCPG = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


# ============ ENV ============

def _require_env(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        raise ValueError(f"Переменная окружения {name} не задана")
    return val


BOT_TOKEN = _require_env("BOT_TOKEN")
TG_API_ID = int(_require_env("TG_API_ID"))
TG_API_HASH = _require_env("TG_API_HASH")
TG_SESSION_STR = os.getenv("TG_SESSION_STR", "").strip()
TG_SESSION_NAME = os.getenv("TG_SESSION_NAME", "gift_bot")

DB_PATH = os.getenv("DB_PATH", "dataseeker.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

FUNSTAT_TOKEN = os.getenv("FUNSTAT_TOKEN", "").strip()
FUNSTAT_BASE = os.getenv("FUNSTAT_BASE", "https://funstatbot.info").strip()

tg_client = None

SEARCH_PRICE_KOPEKS = 100
SIGNUP_BONUS_KOPEKS = 100


# ============ ТАРИФЫ И ПОДПИСКИ ============

TOPUP_PACKAGES = [
    {"id": "pack_start", "tier": "СТАРТ",    "rub": 50,   "searches": 50,   "bonus": 0},
    {"id": "pack_base",  "tier": "БАЗОВЫЙ",  "rub": 100,  "searches": 100,  "bonus": 0},
    {"id": "pack_plus",  "tier": "ВЫГОДНЫЙ", "rub": 500,  "searches": 500,  "bonus": 50},
    {"id": "pack_pro",   "tier": "ПРОФИ",    "rub": 1000, "searches": 1000, "bonus": 150},
    {"id": "pack_biz",   "tier": "БИЗНЕС",   "rub": 5000, "searches": 5000, "bonus": 1000},
]

SUBSCRIPTIONS = [
    {"id": "sub_min", "tier": "МИНИ",     "rub": 300,  "daily": 50},
    {"id": "sub_std", "tier": "СТАНДАРТ", "rub": 600,  "daily": 150},
    {"id": "sub_pro", "tier": "ПРО",      "rub": 1200, "daily": 400},
]

SUB_DURATION_DAYS = 30


# ============ HELPERS ============

def _esc(v):
    return str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _json(data, status: int = 200):
    return web.json_response(
        data, status=status,
        dumps=lambda o: json.dumps(o, ensure_ascii=False),
    )


def _json_error(msg: str, status: int = 400):
    return _json({"ok": False, "error": msg}, status=status)


def _pg_normalize(row):
    if row is None:
        return None
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(v, date):
            d[k] = v.strftime('%Y-%m-%d')
    return d


# ============ DB ============

class DBAdapter:
    def __init__(self, backend, conn=None, pool=None):
        self.backend = backend
        self.conn = conn
        self.pool = pool
        self._lock = asyncio.Lock()

    @staticmethod
    def _translate(sql: str) -> str:
        n = [0]
        def repl(_):
            n[0] += 1
            return f"${n[0]}"
        sql = re.sub(r'\?', repl, sql)
        sql = re.sub(r"date\('now'\)", "CURRENT_DATE", sql, flags=re.IGNORECASE)
        if re.search(r'INSERT\s+OR\s+IGNORE', sql, re.IGNORECASE):
            sql = re.sub(r'INSERT\s+OR\s+IGNORE', 'INSERT', sql, flags=re.IGNORECASE)
            sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
        if re.search(r'INSERT\s+OR\s+REPLACE\s+INTO\s+site_keys', sql, re.IGNORECASE):
            sql = re.sub(
                r'INSERT\s+OR\s+REPLACE\s+INTO\s+site_keys\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)',
                r'INSERT INTO site_keys (\1) VALUES (\2) ON CONFLICT (key) DO UPDATE SET balance_kopeks = excluded.balance_kopeks, total_searches = excluded.total_searches',
                sql, flags=re.IGNORECASE
            )
        return sql

    def _fix_params(self, params):
        if not params:
            return params
        out = []
        for p in params:
            if self.backend == "sqlite":
                if isinstance(p, datetime):
                    out.append(p.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(p, date):
                    out.append(p.strftime('%Y-%m-%d'))
                else:
                    out.append(p)
            else:
                if isinstance(p, bool):
                    out.append(p)
                elif isinstance(p, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', p):
                    try:
                        out.append(date.fromisoformat(p))
                    except ValueError:
                        out.append(p)
                else:
                    out.append(p)
        return tuple(out)

    async def execute(self, sql, params=()):
        params = self._fix_params(params)
        if self.backend == "sqlite":
            async with self._lock:
                cur = await self.conn.execute(sql, params)
                await self.conn.commit()
                return cur
        async with self.pool.acquire() as conn:
            await conn.execute(self._translate(sql), *params)
        return None

    async def fetchone(self, sql, params=()):
        params = self._fix_params(params)
        if self.backend == "sqlite":
            async with self._lock:
                async with self.conn.execute(sql, params) as cur:
                    return await cur.fetchone()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(self._translate(sql), *params)
        return _pg_normalize(row)

    async def fetchall(self, sql, params=()):
        params = self._fix_params(params)
        if self.backend == "sqlite":
            async with self._lock:
                async with self.conn.execute(sql, params) as cur:
                    return await cur.fetchall()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(self._translate(sql), *params)
        return [_pg_normalize(r) for r in rows]

    async def executescript(self, sql: str):
        if self.backend == "sqlite":
            async with self._lock:
                await self.conn.executescript(sql)
                await self.conn.commit()
            return
        async with self.pool.acquire() as conn:
            for stmt in sql.split(';'):
                s = stmt.strip()
                if s:
                    await conn.execute(s)


db_conn = None
http_session = None
_http_lock = None

cache = {}
CACHE_TTL = timedelta(hours=1)

_funstat_bot_cache = {}
_FUNSTAT_BOT_CACHE_TTL = 1800
_FUNSTAT_BOT_NAME = "@evenfa_bot"
_FUNSTAT_BOTS = [
    "@evenfa_bot",
    "@SangMataInfo_bot",
]
_funstat_bot_lock = None
_funstat_bot_blocked_until = 0.0
_last_bot_request_ts = 0.0
_BOT_MIN_INTERVAL = 8.0


def get_cache_key(func_name: str, query: str) -> str:
    return f"{func_name}:{hashlib.md5(query.encode()).hexdigest()}"


API_TIMEOUTS = {
    "funstat": 10.0,
    "tg_gifts": 30.0,
    "funstat_bot": 60.0,
    "tg_full_user": 15.0,
}


SCHEMA_SQLITE = '''
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS site_keys (
    key TEXT PRIMARY KEY,
    balance_kopeks INTEGER DEFAULT 100,
    total_searches INTEGER DEFAULT 0,
    today_searches INTEGER DEFAULT 0,
    today_date TEXT,
    active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_site_keys_key ON site_keys(key);
CREATE TABLE IF NOT EXISTS subscriptions (
    key TEXT PRIMARY KEY,
    plan_id TEXT,
    daily_quota INTEGER DEFAULT 0,
    started_at TEXT,
    expires_at TEXT,
    last_credited_date TEXT,
    active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    key TEXT,
    query TEXT,
    status TEXT DEFAULT 'pending',
    result TEXT,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tasks_key ON tasks(key);
'''

SCHEMA_PG = '''
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS site_keys (
    key TEXT PRIMARY KEY,
    balance_kopeks BIGINT DEFAULT 100,
    total_searches BIGINT DEFAULT 0,
    today_searches BIGINT DEFAULT 0,
    today_date DATE,
    active SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_site_keys_key ON site_keys(key);
CREATE TABLE IF NOT EXISTS subscriptions (
    key TEXT PRIMARY KEY,
    plan_id TEXT,
    daily_quota BIGINT DEFAULT 0,
    started_at TIMESTAMP,
    expires_at TIMESTAMP,
    last_credited_date DATE,
    active SMALLINT DEFAULT 1
);
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    key TEXT,
    query TEXT,
    status TEXT DEFAULT 'pending',
    result TEXT,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tasks_key ON tasks(key);
'''


async def init_db():
    global db_conn

    if DATABASE_URL:
        logger.info("🔌 DATABASE_URL найден, пробуем подключить PostgreSQL...")

        if not HAS_ASYNCPG:
            logger.warning("⚠️ asyncpg НЕ УСТАНОВЛЕН — падаю на SQLite")
        else:
            try:
                pool = await asyncpg.create_pool(
                    DATABASE_URL, min_size=1, max_size=10,
                    timeout=10.0, command_timeout=30.0,
                )
                async with pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                db_conn = DBAdapter("postgres", pool=pool)
                await db_conn.executescript(SCHEMA_PG)

                for stmt in [
                    "ALTER TABLE site_keys ADD COLUMN IF NOT EXISTS today_searches BIGINT DEFAULT 0",
                    "ALTER TABLE site_keys ADD COLUMN IF NOT EXISTS today_date DATE",
                ]:
                    try:
                        await db_conn.execute(stmt)
                    except Exception:
                        pass

                logger.info("✅ PostgreSQL подключён")
                return
            except asyncio.TimeoutError:
                logger.warning("⏱ PostgreSQL таймаут — падаю на SQLite")
            except Exception as e:
                logger.warning(f"❌ PostgreSQL не подключился ({type(e).__name__}: {e})")
    else:
        logger.warning("⚠️ DATABASE_URL НЕ ЗАДАН — используется SQLite")

    try:
        raw = await aiosqlite.connect(DB_PATH)
        raw.row_factory = aiosqlite.Row
        await raw.execute("PRAGMA journal_mode=WAL")
        await raw.execute("PRAGMA synchronous=NORMAL")
        db_conn = DBAdapter("sqlite", conn=raw)
        await db_conn.executescript(SCHEMA_SQLITE)

        for stmt in [
            "ALTER TABLE site_keys ADD COLUMN today_searches INTEGER DEFAULT 0",
            "ALTER TABLE site_keys ADD COLUMN today_date TEXT",
        ]:
            try:
                await db_conn.execute(stmt)
            except Exception:
                pass

        logger.info(f"✅ SQLite подключён: {DB_PATH}")
    except Exception as e:
        logger.error(f"❌ SQLite тоже не подключился: {e}")
        raise


async def get_http_session():
    global http_session, _http_lock
    if _http_lock is None:
        _http_lock = asyncio.Lock()
    if http_session is None or http_session.closed:
        async with _http_lock:
            if http_session is None or http_session.closed:
                http_session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False)
                )
    return http_session


# ============ FUNSTAT API ============

async def _funstat_get(path: str, params: dict = None, timeout: float = 10.0):
    if not FUNSTAT_TOKEN:
        return {}
    session = await get_http_session()
    url = f"{FUNSTAT_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {FUNSTAT_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    try:
        async with session.get(url, params=params or {}, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            text = await resp.text()
            logger.info(f"Funstat [{path}] status={resp.status} len={len(text)}")
            if resp.status != 200:
                return {}
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {}
    except Exception as e:
        logger.error(f"Funstat exception [{path}]: {e}")
        return {}


async def funstat_search(query: str, search_type: str = "tg_id"):
    if not FUNSTAT_TOKEN:
        return {}
    if search_type != "tg_id":
        return {}
    q = str(query).strip().lstrip("@")
    if not q.isdigit():
        return {}
    tg_id = int(q)

    out = {"tg_id": tg_id, "stats": {}, "gifts": [], "groups_count": None, "usernames_history": []}

    stats = await _funstat_get(f"/api/v1/users/{tg_id}/stats_min")
    if stats.get("id"):
        out["stats"] = stats

    gc = await _funstat_get(f"/api/v1/users/{tg_id}/groups_count")
    if isinstance(gc, (int, float)):
        out["groups_count"] = int(gc)

    gifts = await _funstat_get(f"/api/v1/users/{tg_id}/gifts_relation")
    if gifts.get("success") and gifts.get("data"):
        out["gifts"] = gifts["data"]

    unames = await _funstat_get(f"/api/v1/users/{tg_id}/usernames")
    if unames.get("success") and unames.get("data"):
        out["usernames_history"] = unames["data"]

    return out


async def funstat_common_groups(tg_id: str) -> list:
    """
    /api/v1/users/{id}/common_groups_stat — общие группы.
    Возвращает список юзеров, с которыми у {id} есть общие группы.
    """
    if not FUNSTAT_TOKEN:
        return []
    q = str(tg_id).strip()
    if not q.isdigit():
        return []
    data = await _funstat_get(f"/api/v1/users/{q}/common_groups_stat")
    if not data or not data.get("success"):
        return []
    return data.get("data") or []


def parse_funstat(data: dict):
    if not data:
        return []
    parsed = []
    my_id = data.get("tg_id")

    st = data.get("stats") or {}
    if st:
        f = {}
        if st.get("id"):
            f["Telegram ID"] = str(st.get("id"))
        name_parts = [st.get("first_name") or "", st.get("last_name") or ""]
        full_name = " ".join(p for p in name_parts if p).strip()
        if full_name:
            f["Имя"] = full_name
        if st.get("is_bot") is not None:
            f["Бот"] = "да" if st["is_bot"] else "нет"
        if st.get("is_active") is not None:
            f["Активен"] = "да" if st["is_active"] else "нет"
        if st.get("total_msg_count") is not None:
            f["Всего сообщений"] = st["total_msg_count"]
        if st.get("total_groups") is not None:
            f["Всего групп"] = st["total_groups"]
        if st.get("first_msg_date"):
            f["Первое сообщение"] = st["first_msg_date"].split("T")[0]
        if st.get("last_msg_date"):
            f["Последнее сообщение"] = st["last_msg_date"].split("T")[0]
        if f:
            parsed.append({"type": "result", "data": f, "source": "Funstat"})

    gifts = data.get("gifts") or []
    sent = []
    recv = []

    for g in gifts:
        from_id = g.get("from_user_id")
        to_id = g.get("to_user_id")
        date_str = (g.get("last_gift_date") or "").split("T")[0]

        if my_id and from_id == my_id:
            sent.append({
                "Кому": (f"@{g['to_mainUsername']}" if g.get("to_mainUsername")
                         else " ".join(p for p in [g.get("to_first_name"), g.get("to_last_name")] if p).strip()),
                "Имя": " ".join(p for p in [g.get("to_first_name"), g.get("to_last_name")] if p).strip(),
                "Username": ("@" + g["to_mainUsername"]) if g.get("to_mainUsername") else "",
                "Telegram ID": str(to_id) if to_id else "",
                "Дата подарка": date_str,
            })
        elif my_id and to_id == my_id:
            recv.append({
                "От": (f"@{g['from_mainUsername']}" if g.get("from_mainUsername")
                       else " ".join(p for p in [g.get("from_first_name"), g.get("from_last_name")] if p).strip()),
                "Имя": " ".join(p for p in [g.get("from_first_name"), g.get("from_last_name")] if p).strip(),
                "Username": ("@" + g["from_mainUsername"]) if g.get("from_mainUsername") else "",
                "Telegram ID": str(from_id) if from_id else "",
                "Дата подарка": date_str,
            })
        else:
            sent.append({
                "Кому": (f"@{g['to_mainUsername']}" if g.get("to_mainUsername")
                         else " ".join(p for p in [g.get("to_first_name"), g.get("to_last_name")] if p).strip()),
                "Имя": " ".join(p for p in [g.get("to_first_name"), g.get("to_last_name")] if p).strip(),
                "Username": ("@" + g["to_mainUsername"]) if g.get("to_mainUsername") else "",
                "Telegram ID": str(to_id) if to_id else "",
                "Дата подарка": date_str,
            })

    for item in sent:
        parsed.append({"type": "result", "data": item, "source": "Funstat · Кому дарил"})
    for item in recv:
        parsed.append({"type": "result", "data": item, "source": "Funstat · От кого получал"})

    unames_history = data.get("usernames_history") or []
    for u in unames_history[:30]:
        if not isinstance(u, dict):
            continue
        d = {}
        uname = u.get("username") or u.get("name") or u.get("value")
        date_str = u.get("date") or u.get("first_seen") or u.get("last_seen") or u.get("created_at") or ""
        if uname:
            d["Username"] = uname if str(uname).startswith("@") else f"@{uname}"
        if date_str:
            d["Дата"] = str(date_str).split("T")[0]
        if d:
            parsed.append({"type": "result", "data": d, "source": "Funstat · История имён"})

    return parsed


# ============ TELETHON / ПОДАРКИ ============

def tg_from_id_str(from_id):
    if from_id is None:
        return None
    if hasattr(from_id, "user_id"):
        return str(from_id.user_id)
    if hasattr(from_id, "channel_id"):
        return str(from_id.channel_id)
    return str(from_id)


def tg_get_username(user):
    if getattr(user, "username", None):
        return "@" + user.username
    name = " ".join(
        x for x in [
            getattr(user, "first_name", None),
            getattr(user, "last_name", None)
        ] if x
    )
    return name or str(user.id)


async def tg_get_profile_gifts(user):
    if tg_client is None:
        return []
    req_cls = getattr(tg_functions.payments, "GetUserStarGiftsRequest", None)
    if req_cls is None:
        logger.error("[!] GetUserStarGiftsRequest отсутствует — обнови Telethon")
        return []
    try:
        logger.info(f"tg_get_profile_gifts: user_id={user.id}")
        result = await tg_client(req_cls(user_id=user))
        gifts = getattr(result, "gifts", []) or []
        logger.info(f"tg_get_profile_gifts: получено {len(gifts)}")
        return gifts
    except Exception as e:
        logger.error(f"PROFILE GIFTS ERROR: {e!r}")
        return []


async def tg_get_saved_gifts(user):
    if tg_client is None:
        return []
    offset = ""
    all_gifts = []
    while True:
        try:
            result = await tg_client(
                tg_functions.payments.GetSavedStarGiftsRequest(
                    peer=user, offset=offset, limit=100
                )
            )
        except Exception as e:
            logger.error(f"SAVED GIFTS ERROR: {e!r}")
            break
        gifts = getattr(result, "gifts", []) or []
        all_gifts.extend(gifts)
        next_offset = getattr(result, "next_offset", None)
        if not next_offset:
            break
        offset = next_offset
        if len(all_gifts) >= 500:
            break
    return all_gifts


async def _resolve_tg_entity(target: str, username: str = None):
    if username:
        uname = username.lstrip("@").strip()
        if uname:
            try:
                logger.info(f"_resolve_tg_entity: по username={uname!r}")
                return await tg_client.get_entity(uname)
            except Exception as e:
                logger.warning(f"_resolve_tg_entity: username {uname!r} не сработал: {e}")

    target = (target or "").strip().lstrip("@")
    if not target.isdigit():
        raise ValueError("Ожидается числовой Telegram ID")

    uid = int(target)
    try:
        return await tg_client.get_entity(uid)
    except Exception:
        try:
            from telethon.tl.types import InputPeerUser
            return await tg_client.get_entity(InputPeerUser(uid, 0))
        except Exception:
            raise ValueError(
                "Не удалось получить профиль. Укажите username или используйте юзер-сессию."
            )


async def get_full_user_info(target: str, username: str = None) -> dict:
    if tg_client is None:
        return {}
    if not TG_SESSION_STR:
        return {}

    try:
        user = await asyncio.wait_for(
            _resolve_tg_entity(target, username),
            timeout=API_TIMEOUTS["tg_full_user"]
        )
    except Exception as e:
        logger.warning(f"get_full_user_info: resolve failed: {e}")
        return {}

    out = {}

    try:
        from telethon.tl.functions.users import GetFullUserRequest
        result = await asyncio.wait_for(
            tg_client(GetFullUserRequest(user.id)),
            timeout=API_TIMEOUTS["tg_full_user"]
        )
        full = getattr(result, "full_user", None) or result

        about = getattr(full, "about", None)
        if about:
            out["bio"] = about.strip()

        channel_id = getattr(full, "personal_channel_id", None)
        if channel_id:
            out["personal_channel_id"] = channel_id
            try:
                ch_entity = await asyncio.wait_for(
                    tg_client.get_entity(channel_id),
                    timeout=10.0
                )
                if ch_entity:
                    out["personal_channel_title"] = getattr(ch_entity, "title", None)
                    if getattr(ch_entity, "username", None):
                        out["personal_channel_username"] = "@" + ch_entity.username
                        out["personal_channel_link"] = f"https://t.me/{ch_entity.username}"
                    else:
                        out["personal_channel_link"] = f"https://t.me/c/{channel_id}"
            except Exception as e:
                logger.info(f"personal_channel {channel_id} resolve: {e}")
                out["personal_channel_link"] = f"https://t.me/c/{channel_id}"

        bday = getattr(full, "birthday", None)
        if bday:
            d = getattr(bday, "day", None)
            m = getattr(bday, "month", None)
            y = getattr(bday, "year", None)
            if d and m:
                out["birthday"] = f"{d:02d}.{m:02d}" + (f".{y}" if y else "")

        if getattr(user, "premium", None):
            out["premium"] = True
        if getattr(user, "verified", None):
            out["verified"] = True
        if getattr(user, "scam", None):
            out["scam"] = True
        if getattr(user, "fake", None):
            out["fake"] = True

        common = getattr(full, "common_chats_count", None)
        if common:
            out["common_chats_count"] = common

    except Exception as e:
        logger.error(f"get_full_user_info error: {e!r}")

    return out


async def fetch_gifts_data(target: str, username: str = None):
    if tg_client is None:
        return {"error": "MTProto-клиент не инициализирован"}

    target = (target or "").strip()

    try:
        user = await asyncio.wait_for(
            _resolve_tg_entity(target, username),
            timeout=API_TIMEOUTS["tg_gifts"]
        )
    except asyncio.TimeoutError:
        return {"error": "Таймаут получения профиля"}
    except Exception as e:
        return {"error": f"Не удалось получить профиль: {e}"}

    try:
        profile_gifts = await asyncio.wait_for(tg_get_profile_gifts(user),
                                               timeout=API_TIMEOUTS["tg_gifts"])
    except asyncio.TimeoutError:
        profile_gifts = []

    try:
        saved_gifts = await asyncio.wait_for(tg_get_saved_gifts(user),
                                             timeout=API_TIMEOUTS["tg_gifts"])
    except asyncio.TimeoutError:
        saved_gifts = []

    return {
        "user_id": user.id,
        "username": tg_get_username(user),
        "profile_gifts": profile_gifts,
        "saved_gifts": saved_gifts,
    }


def format_gifts_message(data: dict, target: str) -> str:
    if data.get("error"):
        return data["error"]

    profile_gifts = data.get("profile_gifts") or []
    saved_gifts = data.get("saved_gifts") or []

    all_ids = []
    for g in profile_gifts:
        fid = tg_from_id_str(getattr(g, "from_id", None))
        if fid and fid not in all_ids:
            all_ids.append(fid)
    for g in saved_gifts:
        fid = tg_from_id_str(getattr(g, "from_id", None))
        if fid and fid not in all_ids:
            all_ids.append(fid)

    if not all_ids:
        return "Подарков не найдено."

    header = f"<b>Подарки ({len(all_ids)})</b>"
    body = ", ".join(
        f'<a href="tg://user?id={_esc(i)}">{_esc(i)}</a>' for i in all_ids
    )
    return f"{header}\n<blockquote>{body}</blockquote>"


def _gifts_is_ok(text: str) -> bool:
    if not text:
        return False
    if "не найдено" in text.lower():
        return False
    if "ошибка" in text.lower():
        return False
    if "не удалось" in text.lower():
        return False
    if "не инициализирован" in text.lower():
        return False
    return True


# ============ FUNSTAT BOT (@evenfa_bot) ============

_BOT_OBF_MAP = {
    'α': 'a', 'β': 'b', 'γ': 'y', 'δ': 'd', 'ε': 'e', 'ζ': 'z', 'η': 'n',
    'θ': 'th', 'ι': 'i', 'κ': 'k', 'λ': 'l', 'μ': 'm', 'ν': 'v', 'ξ': 'x',
    'ο': 'o', 'π': 'p', 'ρ': 'p', 'σ': 's', 'ς': 's', 'τ': 't', 'υ': 'u',
    'φ': 'f', 'χ': 'x', 'ψ': 'ps', 'ω': 'o',
    'Α': 'A', 'Β': 'B', 'Γ': 'G', 'Δ': 'D', 'Ε': 'E', 'Ζ': 'Z', 'Η': 'H',
    'Θ': 'Th', 'Ι': 'I', 'Κ': 'K', 'Λ': 'L', 'Μ': 'M', 'Ν': 'N', 'Ξ': 'X',
    'Ο': 'O', 'Π': 'P', 'Ρ': 'P', 'Σ': 'S', 'Τ': 'T', 'Υ': 'Y', 'Φ': 'F',
    'Χ': 'X', 'Ψ': 'Ps', 'Ω': 'O',
    'ց': 'g',
    'ᴜ': 'u', 'ᴠ': 'v', 'ᴧ': 'L', 'ᴨ': 'n', 'ᴦ': 'r',
    'ᴋ': 'k', 'ᴍ': 'm', 'ᴏ': 'o', 'ᴘ': 'p', 'ᴛ': 't', 'ᴅ': 'd',
    'ᴢ': 'z', 'ᴊ': 'j', 'ᴡ': 'w', 'ʏ': 'y', 'ɪ': 'I', 'ᴎ': 'n', 'ᴓ': 'o',
    'Ⅼ': 'L', 'Ⅽ': 'C', 'Ⅾ': 'D', 'Ⅿ': 'M',
    'ⅰ': 'i', 'ⅼ': 'l', 'ⅽ': 'c', 'ⅾ': 'd', 'ⅿ': 'm',
    '℮': 'e', 'ƒ': 'f', 'ł': 'l', 'қ': 'q', 'ѕ': 's',
    'q': 'k',
}
_BOT_TRANS = str.maketrans(_BOT_OBF_MAP)


def _normalize_bot_text(s: str) -> str:
    if not s:
        return s
    return s.translate(_BOT_TRANS)


def _clean_markdown(s: str) -> str:
    if not s:
        return s
    s = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', s)
    s = re.sub(r'\*\*([^*]+)\*\*', r'\1', s)
    s = re.sub(r'`([^`]+)`', r'\1', s)
    return s


def parse_funstat_bot_response(text: str) -> dict:
    if not text:
        return {}

    low = text.lower()
    error_markers = [
        "слишком много", "подождите", "попробуйте позже",
        "too many", "flood", "rate limit", "wait",
        "premium", "купи", "подписка", "ошибка",
        "не найден", "not found",
    ]
    for marker in error_markers:
        if marker in low:
            logger.warning(f"bot returned error-like text: {text[:80]}")
            return {}

    norm = _normalize_bot_text(text)
    norm_search = norm.replace('*', '')

    result = {
        "tg_id": None,
        "usernames": [],
        "names": [],
        "stats": {},
        "favorite_chat": None,
        "admin_in_chats": 0,
        "searched_count": 0,
    }

    m = re.search(r'ID\s*[:：]\s*`?(\d{5,15})`?', norm_search, re.IGNORECASE)
    if m:
        result["tg_id"] = m.group(1)

    m = re.search(
        r'usernames\s*:?\s*\n([^\n]+(?:\n[^\n]+)*?)(?=\n\s*(?:first name|last name|names|$))',
        norm_search, re.IGNORECASE | re.DOTALL
    )
    if not m:
        m = re.search(r'usernames\s*:?\s*\n([^\n]+)', norm_search, re.IGNORECASE)
    if m:
        for um in re.finditer(r'@([a-zA-Z0-9_]{4,32})', m.group(1)):
            u = "@" + um.group(1)
            if u not in result["usernames"]:
                result["usernames"].append(u)

    for m in re.finditer(
        r'(\d{4}-\d{2}-\d{2})\s*[➜→▶►]\s*([^\n]+?)(?:\n|$)',
        norm
    ):
        date = m.group(1)
        name = _clean_markdown(m.group(2)).strip()
        if name and len(name) < 100:
            result["names"].append({"date": date, "name": name})

    m = re.search(r'(\d+)\s*messages?\s+in\s+(\d+)\s+groups?', norm_search, re.IGNORECASE)
    if m:
        result["stats"]["total_messages"] = int(m.group(1))
        result["stats"]["total_chats"] = int(m.group(2))
    else:
        m = re.search(r'(\d+)\s*messages?', norm_search, re.IGNORECASE)
        if m:
            result["stats"]["total_messages"] = int(m.group(1))
        m = re.search(r'in\s+(\d+)\s+groups?', norm_search, re.IGNORECASE)
        if m:
            result["stats"]["total_chats"] = int(m.group(1))

    m = re.search(r'diversity\s+([\d,.]+)\s*%', norm_search, re.IGNORECASE)
    if m:
        try:
            result["stats"]["diversity_percent"] = float(m.group(1).replace(',', '.'))
        except ValueError:
            pass

    m = re.search(r'([\d,.]+)\s*%\s*replies', norm_search, re.IGNORECASE)
    if m:
        try:
            result["stats"]["replies_percent"] = float(m.group(1).replace(',', '.'))
        except ValueError:
            pass

    m = re.search(r'([\d,.]+)\s*%\s*media', norm_search, re.IGNORECASE)
    if m:
        try:
            result["stats"]["media_percent"] = float(m.group(1).replace(',', '.'))
        except ValueError:
            pass

    m = re.search(r'circles?\s*:?\s*(\d+)', norm_search, re.IGNORECASE)
    if m:
        result["stats"]["circles"] = int(m.group(1))
    m = re.search(r'voice\s*:?\s*(\d+)', norm_search, re.IGNORECASE)
    if m:
        result["stats"]["voice"] = int(m.group(1))

    m = re.search(r'favorite\s+group\s*:?\s*([^\n]+)', norm_search, re.IGNORECASE)
    if m:
        result["favorite_chat"] = m.group(1).strip()

    m = re.search(r'admin\s+in\s+groups?\s*:?\s*(\d+)', norm_search, re.IGNORECASE)
    if m:
        result["admin_in_chats"] = int(m.group(1))

    m = re.search(r'were\s+looking\s+for\s*:?\s*(\d+)', norm_search, re.IGNORECASE)
    if m:
        result["searched_count"] = int(m.group(1))

    m = re.search(r'from\s+(\d{1,2}/\d{1,2}/\d{4})\s+to', norm_search, re.IGNORECASE)
    if m:
        result["stats"]["period_start"] = m.group(1)

    return result


async def _try_single_bot(bot_name: str, tg_id: str) -> dict:
    try:
        bot = await tg_client.get_entity(bot_name)
    except Exception as e:
        logger.warning(f"{bot_name}: не найден: {e}")
        return {}

    async with _funstat_bot_lock:
        try:
            logger.info(f"{bot_name}: отправляю {tg_id}")
            async with tg_client.conversation(bot, timeout=API_TIMEOUTS["funstat_bot"]) as conv:
                await conv.send_message(tg_id)
                response = await conv.get_response()
                text = response.text or ""
                logger.info(f"{bot_name}: получен ответ, длина {len(text)}")
        except asyncio.TimeoutError:
            logger.warning(f"{bot_name}: timeout для {tg_id}")
            return {}
        except Exception as e:
            err_str = str(e)
            if "FloodWait" in err_str or "420" in err_str:
                m = re.search(r'(\d+)', err_str)
                wait_s = int(m.group(1)) if m else 60
                global _funstat_bot_blocked_until
                _funstat_bot_blocked_until = time.time() + wait_s
                logger.warning(f"FloodWait {wait_s}s — блокирую всех ботов на {wait_s} сек")
            logger.error(f"{bot_name}: error: {e!r}")
            return {}

    if not text:
        return {}

    parsed = parse_funstat_bot_response(text)
    if parsed.get("usernames") or parsed.get("names") or parsed.get("stats"):
        logger.info(
            f"{bot_name}: {tg_id} → "
            f"{len(parsed.get('usernames', []))} usernames, "
            f"{len(parsed.get('names', []))} names"
        )
    return parsed


async def query_funstat_bot(tg_id: str) -> dict:
    global _funstat_bot_lock, _last_bot_request_ts, _funstat_bot_blocked_until

    if tg_client is None:
        logger.warning("query_funstat_bot: tg_client не инициализирован")
        return {}

    if not TG_SESSION_STR:
        logger.warning("query_funstat_bot: TG_SESSION_STR не задан — только бот-сессия")
        return {}

    if _funstat_bot_lock is None:
        _funstat_bot_lock = asyncio.Semaphore(1)

    if time.time() < _funstat_bot_blocked_until:
        remaining = int(_funstat_bot_blocked_until - time.time())
        logger.warning(f"query_funstat_bot: все боты в блокировке ещё {remaining} сек")
        return {}

    now = time.time()
    cached = _funstat_bot_cache.get(tg_id)
    if cached and now - cached[0] < _FUNSTAT_BOT_CACHE_TTL:
        logger.info(f"query_funstat_bot: cache hit для {tg_id}")
        return cached[1]

    delta = now - _last_bot_request_ts
    if delta < _BOT_MIN_INTERVAL:
        wait = _BOT_MIN_INTERVAL - delta
        logger.info(f"query_funstat_bot: пауза {wait:.1f} сек перед запросом")
        await asyncio.sleep(wait)
    _last_bot_request_ts = time.time()

    for bot_name in _FUNSTAT_BOTS:
        logger.info(f"query_funstat_bot: пробую {bot_name} для {tg_id}")
        result = await _try_single_bot(bot_name, tg_id)
        if result and (result.get("usernames") or result.get("names") or result.get("stats")):
            _funstat_bot_cache[tg_id] = (time.time(), result)
            return result

    logger.warning(f"query_funstat_bot: ни один бот не дал данных для {tg_id}")
    return {}


# ============ INTERESTS DETECTION ============

INTEREST_KEYWORDS = {
    "Криптовалюты": [
        "крипт", "crypto", "btc", "bitcoin", "eth", "ethereum", "токен", "coin",
        "web3", "defi", "nft", "блокчейн", "binance", "бинанс",
        "wallet", "кошелек", "кошелёк", "usdt", "ton", "solana",
    ],
    "Инвестиции": [
        "инвест", "invest", "брокер", "broker", "акци",
        "биржа", "дивиденд", "портфель", "portfolio",
    ],
    "Трейдинг": [
        "трейд", "trade", "форекс", "forex", "бинарн", "smart-money",
        "signal", "сигнал", "трейдер", "trader",
    ],
    "Программирование": [
        "python", "developer", "dev", "программ", "разраб", "code", "coder",
        "javascript", "backend", "frontend", "golang", "java", "kotlin",
        "swift", "php", "sql", "docker", "kubernetes", "прогер",
    ],
    "IT/Технологии": [
        "tech", "технолог", "ai", "ии", "ml", "данные", "data",
        "server", "cloud", "devops", "sysadmin",
    ],
    "Игры": [
        "gamer", "геймер", "csgo", "cs2", "dota", "gta", "minecraft",
        "steam", "valorant", "lol", "warcraft",
        "roblox", "fortnite", "pubg", "игр", "игровой",
    ],
    "Музыка": [
        "music", "музык", "битмейк", "beatmaker", "диджей", "dj",
        "sound", "звук", "fl studio", "ableton", "midi", "рэп",
    ],
    "Спорт": [
        "спорт", "sport", "фитнес", "fitness", "gym", "качалк", "бодибилд",
        "кроссфит", "бокс", "мма", "футбол", "хоккей", "баскетбол",
    ],
    "Бизнес": [
        "бизнес", "business", "стартап", "startup", "предприн", "founder",
        "ceo", "маркетинг", "marketing", "продаж", "sales", "smm",
    ],
    "Мода/Красота": [
        "мода", "fashion", "красот", "beauty", "макияж", "стиль", "style",
        "маникюр", "визаж", "парикмахер",
    ],
    "Путешествия": [
        "путеш", "travel", "вокруг света", "trip", "tourist", "tour",
        "отдых", "виза", "пляж",
    ],
    "Авто": [
        "авто", "auto", "car", "машин", "тачк", "bmw", "mers", "mercedes",
        "audi", "toyota", "тюнинг",
    ],
    "Фото/Видео": [
        "фото", "photo", "видео", "video", "фотограф", "photographer",
        "монтаж", "editor", "камера",
    ],
    "Кино/Сериалы": [
        "кино", "movie", "фильм", "сериал", "series", "netflix",
        "аниме", "anime", "manga", "манга",
    ],
    "Обучение": [
        "учус", "учит", "студент", "student", "school",
        "универ", "обучен", "education", "курс",
    ],
    "Медицина": [
        "медиц", "medic", "врач", "доктор", "doctor", "клиник",
        "здоров", "health", "фарм",
    ],
    "Недвижимость": [
        "недвиж", "риэлтор", "realtor", "квартир", "строит",
        "аренд", "rent", "ипотек",
    ],
}


def _normalize_for_match(s: str) -> str:
    if not s:
        return ""
    return str(s).lower().strip()


def detect_interests(
    bio: str = None,
    favorite_chat: str = None,
    name_history: list = None,
    username_history: list = None,
    channel_title: str = None,
    common_groups: list = None,
) -> list:
    """
    Классифицирует интересы по ключевым словам.
    Возвращает [{category, confidence, source}].
    """
    sources = {
        "bio": bio or "",
        "personal_channel": channel_title or "",
        "favorite_chat": favorite_chat or "",
        "username_history": " ".join(
            [u.get("username", "") for u in (username_history or []) if isinstance(u, dict)]
        ),
        "name_history": " ".join(
            [n.get("name", "") for n in (name_history or []) if isinstance(n, dict)]
        ),
        "common_groups": " ".join(
            [str(g) for g in (common_groups or [])[:50]]
        ),
    }

    source_weights = {
        "bio": 1.0,
        "personal_channel": 0.9,
        "common_groups": 0.8,
        "favorite_chat": 0.7,
        "username_history": 0.5,
        "name_history": 0.4,
    }

    found = {}

    for source, text in sources.items():
        if not text:
            continue
        norm = _normalize_for_match(text)
        for category, keywords in INTEREST_KEYWORDS.items():
            hits = 0
            for kw in keywords:
                if kw in norm:
                    hits += 1
            if hits == 0:
                continue

            confidence = min(1.0, hits / 5.0) * source_weights[source]

            if category not in found:
                found[category] = {
                    "category": category,
                    "confidence": confidence,
                    "source": source,
                }
            else:
                if confidence > found[category]["confidence"]:
                    found[category]["confidence"] = confidence
                    found[category]["source"] = source

    result = sorted(
        [
            {
                "category": v["category"],
                "confidence": round(v["confidence"], 2),
                "source": v["source"],
            }
            for v in found.values()
        ],
        key=lambda x: x["confidence"],
        reverse=True,
    )

    return [r for r in result if r["confidence"] >= 0.15]


# ============ COLLECT ============

async def collect_general_data(query: str, search_type: str = "tg_id"):
    cache_key = get_cache_key(search_type, query)
    if cache_key in cache:
        cached_time, data = cache[cache_key]
        if datetime.now() - cached_time < CACHE_TTL:
            return data

    original_query = query
    funstat_parsed = []

    if search_type == "tg_id":
        funstat_data = await funstat_search(query, "tg_id")
        logger.info(f"tg_id search: input={query!r} resolved_id={funstat_data.get('tg_id')}")
        funstat_parsed = parse_funstat(funstat_data)

    logger.info(f"Parsed: fun={len(funstat_parsed)}")

    result = {
        'query': original_query, 'type': search_type,
        'blocks': [], 'records_count': 0, 'sources': [],
    }

    sources_set = set()
    records_count = 0
    seen_records = set()
    blocks = []

    def _add_block(block):
        nonlocal records_count
        if block['type'] != 'result':
            return
        fields = dict(block['data'])
        if not fields:
            return
        source_name = block.get('source') or 'Funstat'
        record_key = f"{source_name}|" + "|".join(f"{k}={v}" for k, v in sorted(fields.items()))
        if record_key in seen_records:
            return
        seen_records.add(record_key)
        sources_set.add(source_name)
        records_count += 1
        existing = None
        for i, (name, rows) in enumerate(blocks):
            if name == source_name:
                existing = i
                break
        if existing is not None:
            blocks[existing][1].append(fields)
        else:
            blocks.append([source_name, [fields]])

    for block in funstat_parsed:
        _add_block(block)

    result['sources'] = list(sources_set)
    result['records_count'] = records_count
    result['blocks'] = blocks

    cache[cache_key] = (datetime.now(), result)
    return result


def _format_month_year(iso: str) -> str:
    if not iso:
        return ""
    s = str(iso).strip()
    dt = None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            if "T" in s:
                dt = datetime.strptime(s[:10], "%Y-%m-%d")
            else:
                dt = datetime.strptime(s, fmt)
            break
        except Exception:
            continue
    if dt is None:
        return s
    months = ["янв", "фев", "мар", "апр", "май", "июн",
              "июл", "авг", "сен", "окт", "ноя", "дек"]
    return f"{months[dt.month - 1]}, {dt.year}"


def _months_ago(iso: str) -> str:
    if not iso:
        return ""
    s = str(iso).strip()
    dt = None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            if "T" in s:
                dt = datetime.strptime(s[:10], "%Y-%m-%d")
            else:
                dt = datetime.strptime(s, fmt)
            break
        except Exception:
            continue
    if dt is None:
        return ""
    today = datetime.now()
    total_months = (today.year - dt.year) * 12 + (today.month - dt.month)
    if total_months < 0:
        total_months = 0
    if total_months == 0:
        return "сейчас"
    if total_months < 12:
        return f"{total_months} мес"
    years = total_months // 12
    if years % 10 == 1 and years % 100 != 11:
        word = "год"
    elif 2 <= years % 10 <= 4 and (years % 100 < 10 or years % 100 >= 20):
        word = "года"
    else:
        word = "лет"
    return f"{years} {word}"


def _format_group_date(date_str: str) -> str:
    if not date_str:
        return ""
    s = str(date_str).strip()
    d = s[:10]
    try:
        dt = datetime.strptime(d, "%Y-%m-%d")
        months = ["янв", "фев", "мар", "апр", "май", "июн",
                  "июл", "авг", "сен", "окт", "ноя", "дек"]
        return f"{dt.day} {months[dt.month - 1]} {str(dt.year)[-2:]}"
    except Exception:
        return ""


def build_funstat_preview(data: dict, query: str) -> str:
    tid = ""
    reg_date = ""
    name_history = []

    for source, rows in data.get('blocks', []):
        for row in rows:
            if not tid:
                for k in ("Telegram ID", "TG ID"):
                    if row.get(k):
                        tid = str(row[k])
                        break
            if not reg_date and row.get("Первое сообщение"):
                reg_date = str(row["Первое сообщение"])
            if "История имён" in source:
                uname = row.get("Username")
                date_str = row.get("Дата")
                if uname:
                    name_history.append((date_str or "", str(uname)))

    lines = []
    header = "<b>Telegram"
    if tid:
        header += f" · {_esc(tid)}"
    header += "</b>"
    lines.append(header)
    lines.append("")

    if reg_date:
        mon = _format_month_year(reg_date)
        ago = _months_ago(reg_date)
        suffix = f" ({_esc(ago)})" if ago else ""
        lines.append(f"<b>Регистрация:</b> ~{_esc(mon)}{suffix}")
        lines.append("")

    if name_history:
        with_date = sorted([x for x in name_history if x[0]], key=lambda t: t[0], reverse=True)
        without_date = [x for x in name_history if not x[0]]
        sorted_history = with_date + without_date
        lines.append("<b>История изменения имени:</b>")
        for date_str, uname in sorted_history[:15]:
            u_clean = uname.lstrip("@")
            link = f'<a href="https://t.me/{_esc(u_clean)}">{_esc(uname)}</a>'
            if date_str:
                d_short = _format_group_date(date_str)
                if d_short:
                    lines.append(f"{d_short} → {link}")
                else:
                    lines.append(f"{_esc(date_str)} → {link}")
            else:
                lines.append(f"• {link}")
        lines.append("")

    if not lines or (len(lines) == 2 and not tid):
        return "По этому Telegram ничего не найдено."

    return "\n".join(lines).strip()


# ============ SITE API ============

async def _get_key_row(key: str):
    if not key or not isinstance(key, str) or len(key) < 16:
        return None
    try:
        return await db_conn.fetchone(
            'SELECT * FROM site_keys WHERE key = ? AND active = 1', (key,)
        )
    except Exception as e:
        logger.error(f"_get_key_row error: {e}")
        return None


async def api_key_create_handler(request):
    try:
        new_key = "dsk_" + secrets.token_urlsafe(24)
        today_str = date.today().isoformat()
        await db_conn.execute(
            'INSERT INTO site_keys '
            '(key, balance_kopeks, total_searches, today_searches, today_date, active) '
            'VALUES (?, ?, 0, 0, ?, 1)',
            (new_key, SIGNUP_BONUS_KOPEKS, today_str)
        )
        return _json({
            "ok": True, "key": new_key,
            "balance_kopeks": SIGNUP_BONUS_KOPEKS,
            "balance_rub": SIGNUP_BONUS_KOPEKS / 100,
            "total_searches": 0, "today_searches": 0,
            "signup_bonus_kopeks": SIGNUP_BONUS_KOPEKS,
        })
    except Exception as e:
        logger.exception("api_key_create_handler")
        return _json_error(f"internal: {type(e).__name__}: {e}", 500)


async def api_key_info_handler(request):
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")
    key = (body.get("key") or "").strip()
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)

    today_str = date.today().isoformat()
    today_count = 0
    try:
        td = row["today_date"]
        ts = row["today_searches"]
        if td is not None and str(td)[:10] == today_str:
            today_count = int(ts or 0)
    except Exception:
        pass

    sub_info = None
    try:
        sub = await db_conn.fetchone(
            'SELECT plan_id, daily_quota, started_at, expires_at, last_credited_date '
            'FROM subscriptions WHERE key = ? AND active = 1', (key,)
        )
        if sub:
            expires_str = str(sub["expires_at"])[:10] if sub["expires_at"] else ""
            if expires_str and expires_str >= today_str:
                days_left = (date.fromisoformat(expires_str) - date.today()).days
                last_str = str(sub["last_credited_date"])[:10] if sub["last_credited_date"] else ""
                sub_info = {
                    "plan_id": sub["plan_id"],
                    "daily_quota": sub["daily_quota"],
                    "started_at": str(sub["started_at"])[:10] if sub["started_at"] else None,
                    "expires_at": expires_str,
                    "days_left": days_left,
                    "credited_today": last_str == today_str,
                }
            else:
                await db_conn.execute(
                    'UPDATE subscriptions SET active = 0 WHERE key = ?', (key,)
                )
    except Exception as e:
        logger.error(f"sub info error: {e}")

    return _json({
        "ok": True, "key": key,
        "balance_kopeks": row['balance_kopeks'],
        "balance_rub": row['balance_kopeks'] / 100,
        "total_searches": row['total_searches'],
        "today_searches": today_count,
        "price_per_search_kopeks": SEARCH_PRICE_KOPEKS,
        "searches_left": row['balance_kopeks'] // SEARCH_PRICE_KOPEKS,
        "subscription": sub_info,
    })


async def api_key_topup_handler(request):
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")
    key = (body.get("key") or "").strip()
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    package_stars = int(body.get("package_stars") or 0)
    pkg = next((p for p in TOPUP_PACKAGES if p.get("rub") == package_stars), None)
    if not pkg:
        return _json_error("unknown_package")
    total_searches = pkg["searches"] + pkg.get("bonus", 0)
    amount = total_searches * SEARCH_PRICE_KOPEKS
    await db_conn.execute(
        'UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?',
        (amount, key)
    )
    new_row = await db_conn.fetchone('SELECT balance_kopeks FROM site_keys WHERE key = ?', (key,))
    return _json({
        "ok": True, "added_kopeks": amount,
        "balance_kopeks": new_row['balance_kopeks'] if new_row else row['balance_kopeks'] + amount,
    })


async def api_topup_packages_handler(request):
    return _json({"ok": True, "packages": TOPUP_PACKAGES})


async def api_pricing_handler(request):
    return _json({"ok": True, "packs": TOPUP_PACKAGES, "subscriptions": SUBSCRIPTIONS})


async def api_buy_pack_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")
    key = (body.get("key") or "").strip()
    pack_id = (body.get("pack_id") or "").strip()
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    pkg = next((p for p in TOPUP_PACKAGES if p["id"] == pack_id), None)
    if not pkg:
        return _json_error("unknown_pack")
    total_searches = pkg["searches"] + pkg["bonus"]
    add_kopeks = total_searches * SEARCH_PRICE_KOPEKS
    await db_conn.execute(
        'UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?',
        (add_kopeks, key)
    )
    new_row = await db_conn.fetchone(
        'SELECT balance_kopeks FROM site_keys WHERE key = ?', (key,)
    )
    return _json({
        "ok": True, "pack_id": pack_id,
        "searches_added": total_searches, "kopeks_added": add_kopeks,
        "balance_kopeks": new_row["balance_kopeks"] if new_row else row["balance_kopeks"] + add_kopeks,
    })


async def api_buy_subscription_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")
    key = (body.get("key") or "").strip()
    sub_id = (body.get("sub_id") or "").strip()
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    sub = next((s for s in SUBSCRIPTIONS if s["id"] == sub_id), None)
    if not sub:
        return _json_error("unknown_subscription")
    today = date.today().isoformat()
    expires = (date.today() + timedelta(days=SUB_DURATION_DAYS)).isoformat()

    existing = await db_conn.fetchone(
        'SELECT key, plan_id, expires_at FROM subscriptions WHERE key = ? AND active = 1',
        (key,)
    )
    if existing and existing["expires_at"]:
        cur_exp = str(existing["expires_at"])[:10]
        if cur_exp >= today:
            return _json({
                "ok": False,
                "error": "subscription_already_active",
                "active_until": cur_exp,
                "active_plan": existing["plan_id"],
            }, status=409)

    if existing:
        await db_conn.execute(
            'UPDATE subscriptions SET plan_id = ?, daily_quota = ?, '
            'started_at = ?, expires_at = ?, last_credited_date = NULL, active = 1 '
            'WHERE key = ?',
            (sub_id, sub["daily"], today, expires, key)
        )
    else:
        await db_conn.execute(
            'INSERT INTO subscriptions '
            '(key, plan_id, daily_quota, started_at, expires_at, active) '
            'VALUES (?, ?, ?, ?, ?, 1)',
            (key, sub_id, sub["daily"], today, expires)
        )

    daily_kopeks = sub["daily"] * SEARCH_PRICE_KOPEKS
    await db_conn.execute(
        'UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?',
        (daily_kopeks, key)
    )
    await db_conn.execute(
        'UPDATE subscriptions SET last_credited_date = ? WHERE key = ?',
        (today, key)
    )
    return _json({
        "ok": True, "sub_id": sub_id,
        "daily_quota": sub["daily"],
        "started_at": today, "expires_at": expires,
        "credited_now": sub["daily"],
    })


async def api_public_search_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    if request.method == "POST":
        try:
            body = await request.json()
        except Exception:
            return _json_error("invalid_json")
    else:
        body = dict(request.query)

    key = (body.get("key") or "").strip()
    query = (body.get("query") or "").strip().lstrip("@")

    if not query:
        return _json_error("empty_query")
    if not query.isdigit():
        return _json_error("only_tg_id_supported")

    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)

    try:
        sub = await db_conn.fetchone(
            'SELECT plan_id, daily_quota, expires_at, last_credited_date '
            'FROM subscriptions WHERE key = ? AND active = 1', (key,)
        )
        if sub:
            today_str = date.today().isoformat()
            expires_str = str(sub["expires_at"])[:10] if sub["expires_at"] else ""
            last_str = str(sub["last_credited_date"])[:10] if sub["last_credited_date"] else ""
            if expires_str and expires_str >= today_str:
                if last_str != today_str:
                    daily_kopeks = int(sub["daily_quota"]) * SEARCH_PRICE_KOPEKS
                    await db_conn.execute(
                        'UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?',
                        (daily_kopeks, key)
                    )
                    await db_conn.execute(
                        'UPDATE subscriptions SET last_credited_date = ? WHERE key = ?',
                        (today_str, key)
                    )
                    row = await _get_key_row(key)
            else:
                await db_conn.execute(
                    'UPDATE subscriptions SET active = 0 WHERE key = ?', (key,)
                )
    except Exception as e:
        logger.error(f"subscription credit error: {e}")

    if row['balance_kopeks'] < SEARCH_PRICE_KOPEKS:
        return _json_error("insufficient_balance", 402)

    target = query

    try:
        data = await collect_general_data(target, "tg_id")
    except Exception as e:
        logger.exception("api_public_search_handler: search")
        return _json_error(f"search_error: {type(e).__name__}", 500)

    if not isinstance(data, dict):
        data = {}

    # === Бот @evenfa_bot ===
    try:
        bot_data = await query_funstat_bot(str(target))
        if bot_data:
            usernames_rows = []
            for u in bot_data.get("usernames", []):
                usernames_rows.append({"Username": u, "Дата": None})

            if usernames_rows:
                data.setdefault("blocks", []).append(
                    ("Funstat Bot · Username", usernames_rows)
                )
                data["records_count"] = data.get("records_count", 0) + len(usernames_rows)
                if "Funstat Bot · Username" not in data.get("sources", []):
                    data.setdefault("sources", []).append("Funstat Bot · Username")

            names_rows = []
            for n in bot_data.get("names", []):
                names_rows.append({"Имя": n.get("name"), "Дата": n.get("date")})

            if names_rows:
                data.setdefault("blocks", []).append(
                    ("Funstat Bot · Имена", names_rows)
                )
                data["records_count"] = data.get("records_count", 0) + len(names_rows)
                if "Funstat Bot · Имена" not in data.get("sources", []):
                    data.setdefault("sources", []).append("Funstat Bot · Имена")

            if bot_data.get("stats"):
                data["bot_stats"] = bot_data["stats"]
            if bot_data.get("favorite_chat"):
                data["favorite_chat"] = bot_data["favorite_chat"]
            if bot_data.get("admin_in_chats"):
                data["admin_in_chats"] = bot_data["admin_in_chats"]
    except Exception as e:
        logger.error(f"funstat bot merge error: {e}")

    # === Bio + личный канал ===
    username = (data.get("stats") or {}).get("username") or None
    try:
        full_info = await get_full_user_info(str(target), username)
        if full_info:
            if full_info.get("bio"):
                data.setdefault("blocks", []).append(
                    ("Telegram · Bio", [{"Bio": full_info["bio"]}])
                )
                data["records_count"] = data.get("records_count", 0) + 1
                if "Telegram · Bio" not in data.get("sources", []):
                    data.setdefault("sources", []).append("Telegram · Bio")

            if full_info.get("personal_channel_link"):
                ch_row = {
                    "Название": full_info.get("personal_channel_title"),
                    "Username": full_info.get("personal_channel_username"),
                    "Ссылка": full_info.get("personal_channel_link"),
                }
                ch_row = {k: v for k, v in ch_row.items() if v}
                if ch_row:
                    data.setdefault("blocks", []).append(
                        ("Telegram · Личный канал", [ch_row])
                    )
                    data["records_count"] = data.get("records_count", 0) + 1
                    if "Telegram · Личный канал" not in data.get("sources", []):
                        data.setdefault("sources", []).append("Telegram · Личный канал")

            data["birthday"] = full_info.get("birthday")
            data["premium"] = full_info.get("premium", False)
            data["verified"] = full_info.get("verified", False)
            data["common_chats_count"] = full_info.get("common_chats_count")
    except Exception as e:
        logger.error(f"full_user_info merge error: {e}")

    # === Интересы ===
    try:
        channel_title = None
        bio_val = None
        name_hist = []
        username_hist = []

        for src_name, rows in data.get("blocks", []):
            if src_name == "Telegram · Личный канал" and rows:
                channel_title = rows[0].get("Название")
            elif src_name == "Telegram · Bio" and rows:
                bio_val = rows[0].get("Bio")
            elif src_name == "Funstat Bot · Имена":
                for r in rows:
                    if r.get("Имя"):
                        name_hist.append({"name": r["Имя"], "date": r.get("Дата")})
            elif src_name == "Funstat Bot · Username":
                for r in rows:
                    if r.get("Username"):
                        username_hist.append({"username": r["Username"]})

        interests = detect_interests(
            bio=bio_val,
            favorite_chat=data.get("favorite_chat"),
            name_history=name_hist,
            username_history=username_hist,
            channel_title=channel_title,
        )
        if interests:
            interests_rows = [
                {
                    "Категория": i["category"],
                    "Точность": f"{int(i['confidence']*100)}%",
                    "Источник": i["source"],
                }
                for i in interests
            ]
            data.setdefault("blocks", []).append(("Интересы", interests_rows))
            data["records_count"] = data.get("records_count", 0) + len(interests_rows)
            if "Интересы" not in data.get("sources", []):
                data.setdefault("sources", []).append("Интересы")
        data["interests"] = interests
        logger.info(f"interests: {len(interests)} для {target}")
    except Exception as e:
        logger.error(f"detect_interests error: {e}")
        data["interests"] = []

    try:
        base_text = build_funstat_preview(data, target)
    except Exception:
        logger.exception("api_public_search_handler: preview")
        base_text = ""

    try:
        gifts_data = await fetch_gifts_data(target, username)
        gifts_text = format_gifts_message(gifts_data, target)
    except Exception:
        logger.exception("api_public_search_handler: gifts")
        gifts_data = None
        gifts_text = ""

    parts = []
    if base_text and "не найдено" not in base_text.lower():
        parts.append(base_text)
    if _gifts_is_ok(gifts_text):
        parts.append(gifts_text)

    if parts:
        full_text = "\n\n".join(parts)
    else:
        full_text = "По этому Telegram ничего не найдено."

    public_blocks = []
    for src, rows in data.get('blocks', []) or []:
        pub_rows = []
        for r in rows:
            clean = {k: v for k, v in r.items() if v not in (None, "", [], {})}
            if clean:
                pub_rows.append(clean)
        if pub_rows:
            public_blocks.append({"source": src, "rows": pub_rows})

    gift_ids = []
    if gifts_data and not gifts_data.get("error"):
        for g in (gifts_data.get("profile_gifts") or []):
            fid = tg_from_id_str(getattr(g, "from_id", None))
            if fid and fid not in gift_ids:
                gift_ids.append(fid)
        for g in (gifts_data.get("saved_gifts") or []):
            fid = tg_from_id_str(getattr(g, "from_id", None))
            if fid and fid not in gift_ids:
                gift_ids.append(fid)
        if gift_ids:
            gift_rows = [
                {"ID": gid, "Ссылка": f"tg://user?id={gid}"} for gid in gift_ids
            ]
            public_blocks.append({"source": "Telegram · Подарки", "rows": gift_rows})

    has_data = (data.get("records_count", 0) > 0) or (len(gift_ids) > 0)

    today_str = date.today().isoformat()
    row_today = await db_conn.fetchone(
        'SELECT today_searches, today_date FROM site_keys WHERE key = ?', (key,)
    )
    today_count = 0
    if row_today:
        try:
            td = row_today["today_date"]
            ts = row_today["today_searches"]
            if td is not None and str(td)[:10] == today_str:
                today_count = int(ts or 0)
        except Exception:
            today_count = 0

    charged_kopeks = 0
    if has_data:
        today_count += 1
        await db_conn.execute(
            'UPDATE site_keys SET balance_kopeks = balance_kopeks - ?, '
            'total_searches = total_searches + 1, '
            'today_searches = ?, today_date = ? WHERE key = ?',
            (SEARCH_PRICE_KOPEKS, today_count, today_str, key)
        )
        charged_kopeks = SEARCH_PRICE_KOPEKS

    new_row = await db_conn.fetchone(
        'SELECT balance_kopeks, total_searches FROM site_keys WHERE key = ?', (key,)
    )

    return _json({
        "ok": True,
        "query": data.get("query"),
        "type": data.get("type"),
        "sources": data.get("sources", []),
        "records_count": data.get("records_count", 0),
        "blocks": public_blocks,
        "text": full_text,
        "gifts_count": len(gift_ids),
        "charged_kopeks": charged_kopeks,
        "balance_kopeks": new_row['balance_kopeks'] if new_row else row['balance_kopeks'] - charged_kopeks,
        "total_searches": new_row['total_searches'] if new_row else row['total_searches'] + (1 if has_data else 0),
        "today_searches": today_count,
        "bot_stats": data.get("bot_stats", {}),
        "favorite_chat": data.get("favorite_chat"),
        "admin_in_chats": data.get("admin_in_chats", 0),
        "birthday": data.get("birthday"),
        "premium": data.get("premium", False),
        "verified": data.get("verified", False),
        "common_chats_count": data.get("common_chats_count"),
        "interests": data.get("interests", []),
    })


async def api_batch_search_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")

    key = (body.get("key") or "").strip()
    queries = body.get("queries") or []
    search_type = (body.get("type") or "tg_id").strip()

    if not key:
        return _json_error("empty_key")
    if not isinstance(queries, list) or not queries:
        return _json_error("empty_queries")
    if len(queries) > 50:
        return _json_error("too_many_queries_max_50", 400)
    if search_type != "tg_id":
        return _json_error("only_tg_id_supported")

    clean = []
    for q in queries:
        s = str(q).strip().lstrip("@")
        if s.isdigit() and s not in clean:
            clean.append(s)

    if not clean:
        return _json_error("no_valid_queries")

    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    if row['balance_kopeks'] < SEARCH_PRICE_KOPEKS:
        return _json_error("insufficient_balance", 402)

    results = []
    found_count = 0
    for q in clean:
        try:
            data = await collect_general_data(q, "tg_id")
            base_text = build_funstat_preview(data, q)
            username = (data.get("stats") or {}).get("username") or None
            gifts_data = await fetch_gifts_data(q, username)
            gifts_text = format_gifts_message(gifts_data, q)
            parts = []
            if base_text and "не найдено" not in base_text.lower():
                parts.append(base_text)
            if _gifts_is_ok(gifts_text):
                parts.append(gifts_text)
            full_text = "\n\n".join(parts) if parts else "По этому Telegram ничего не найдено."
            gift_ids = []
            if gifts_data and not gifts_data.get("error"):
                for g in (gifts_data.get("profile_gifts") or []):
                    fid = tg_from_id_str(getattr(g, "from_id", None))
                    if fid and fid not in gift_ids:
                        gift_ids.append(fid)
                for g in (gifts_data.get("saved_gifts") or []):
                    fid = tg_from_id_str(getattr(g, "from_id", None))
                    if fid and fid not in gift_ids:
                        gift_ids.append(fid)
            has_data = (data.get("records_count", 0) > 0) or (len(gift_ids) > 0)
            if has_data:
                found_count += 1
            results.append({
                "query": q, "ok": True, "text": full_text,
                "records_count": data.get("records_count", 0),
                "gifts_count": len(gift_ids),
                "sources": data.get("sources", []),
                "charged": has_data,
            })
        except Exception as e:
            logger.exception(f"batch item error [{q}]")
            results.append({
                "query": q, "ok": False,
                "error": f"{type(e).__name__}: {e}",
                "charged": False,
            })

    charged_kopeks = found_count * SEARCH_PRICE_KOPEKS

    today_str = date.today().isoformat()
    row_today = await db_conn.fetchone(
        'SELECT today_searches, today_date FROM site_keys WHERE key = ?', (key,)
    )
    today_count = 0
    if row_today:
        try:
            td = row_today["today_date"]
            ts = row_today["today_searches"]
            if td is not None and str(td)[:10] == today_str:
                today_count = int(ts or 0)
        except Exception:
            today_count = 0

    if found_count > 0:
        today_count += found_count
        await db_conn.execute(
            'UPDATE site_keys SET balance_kopeks = balance_kopeks - ?, '
            'total_searches = total_searches + ?, '
            'today_searches = ?, today_date = ? WHERE key = ?',
            (charged_kopeks, found_count, today_count, today_str, key)
        )

    new_row = await db_conn.fetchone(
        'SELECT balance_kopeks, total_searches FROM site_keys WHERE key = ?', (key,)
    )

    return _json({
        "ok": True, "count": len(clean), "found": found_count,
        "results": results,
        "charged_kopeks": charged_kopeks,
        "balance_kopeks": new_row['balance_kopeks'] if new_row else row['balance_kopeks'] - charged_kopeks,
        "total_searches": new_row['total_searches'] if new_row else row['total_searches'] + found_count,
        "today_searches": today_count,
    })


async def _run_task(task_id: str, key: str, query: str):
    try:
        await db_conn.execute(
            "UPDATE tasks SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (task_id,)
        )
        data = await collect_general_data(query, "tg_id")
        base_text = build_funstat_preview(data, query)
        username = (data.get("stats") or {}).get("username") or None
        gifts_data = await fetch_gifts_data(query, username)
        gifts_text = format_gifts_message(gifts_data, query)
        parts = []
        if base_text and "не найдено" not in base_text.lower():
            parts.append(base_text)
        if _gifts_is_ok(gifts_text):
            parts.append(gifts_text)
        full_text = "\n\n".join(parts) if parts else "По этому Telegram ничего не найдено."

        gift_ids = []
        if gifts_data and not gifts_data.get("error"):
            for g in (gifts_data.get("profile_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in gift_ids:
                    gift_ids.append(fid)
            for g in (gifts_data.get("saved_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in gift_ids:
                    gift_ids.append(fid)

        has_data = (data.get("records_count", 0) > 0) or (len(gift_ids) > 0)

        if not has_data:
            await db_conn.execute(
                "UPDATE site_keys SET balance_kopeks = balance_kopeks + ?, "
                "total_searches = total_searches - 1 WHERE key = ? AND total_searches > 0",
                (SEARCH_PRICE_KOPEKS, key)
            )
            logger.info(f"TASK not found, refunded: {task_id}")

        result = {
            "query": query, "text": full_text,
            "records_count": data.get("records_count", 0),
            "gifts_count": len(gift_ids),
            "sources": data.get("sources", []),
            "charged": has_data,
        }

        await db_conn.execute(
            "UPDATE tasks SET status = 'done', result = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (json.dumps(result, ensure_ascii=False), task_id)
        )
    except Exception as e:
        logger.exception(f"TASK failed: {task_id}")
        try:
            await db_conn.execute(
                "UPDATE site_keys SET balance_kopeks = balance_kopeks + ?, "
                "total_searches = total_searches - 1 WHERE key = ? AND total_searches > 0",
                (SEARCH_PRICE_KOPEKS, key)
            )
        except Exception:
            pass
        await db_conn.execute(
            "UPDATE tasks SET status = 'error', error = ?, "
            "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (f"{type(e).__name__}: {e}", task_id)
        )


async def api_task_create_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")

    key = (body.get("key") or "").strip()
    query = (body.get("query") or "").strip().lstrip("@")

    if not key:
        return _json_error("empty_key")
    if not query:
        return _json_error("empty_query")
    if not query.isdigit():
        return _json_error("only_tg_id_supported")

    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    if row['balance_kopeks'] < SEARCH_PRICE_KOPEKS:
        return _json_error("insufficient_balance", 402)

    task_id = secrets.token_urlsafe(12)
    await db_conn.execute(
        "INSERT INTO tasks (id, key, query, status) VALUES (?, ?, ?, 'pending')",
        (task_id, key, query)
    )
    await db_conn.execute(
        "UPDATE site_keys SET balance_kopeks = balance_kopeks - ?, "
        "total_searches = total_searches + 1 WHERE key = ?",
        (SEARCH_PRICE_KOPEKS, key)
    )
    asyncio.create_task(_run_task(task_id, key, query))
    return _json({
        "ok": True, "task_id": task_id, "status": "pending",
        "query": query, "check_url": f"/status?task={task_id}",
    })


async def api_task_status_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    task_id = (request.query.get("id") or request.query.get("task_id") or "").strip()
    if not task_id:
        return _json_error("empty_id")
    row = await db_conn.fetchone(
        "SELECT id, query, status, result, error, created_at, updated_at "
        "FROM tasks WHERE id = ?", (task_id,)
    )
    if not row:
        return _json_error("task_not_found", 404)
    result = None
    if row["result"]:
        try:
            result = json.loads(row["result"])
        except Exception:
            result = {"raw": row["result"]}
    return _json({
        "ok": True, "task_id": row["id"], "query": row["query"],
        "status": row["status"], "result": result, "error": row["error"],
        "created_at": str(row["created_at"]) if row["created_at"] else None,
        "updated_at": str(row["updated_at"]) if row["updated_at"] else None,
    })


async def api_public_tg_info_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    q = (request.query.get("q") or request.query.get("query") or "").strip().lstrip("@")
    if not q:
        return _json({"ok": False, "error": "empty_query"}, status=400)
    if not q.isdigit():
        return _json({"ok": False, "error": "invalid_id"}, status=400)

    ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote or "unknown"
    now = time.time()
    if not hasattr(api_public_tg_info_handler, "_rl"):
        api_public_tg_info_handler._rl = {}
    rl = api_public_tg_info_handler._rl
    bucket = rl.setdefault(ip, [])
    bucket[:] = [t for t in bucket if now - t < 3600]
    if len(bucket) >= 10:
        return _json({"ok": False, "error": "rate_limit"}, status=429)
    bucket.append(now)

    try:
        fs = await funstat_search(q, "tg_id")
    except Exception:
        logger.exception("public_tg_info funstat")
        return _json({"ok": False, "error": "funstat_error"}, status=502)

    if not fs or not fs.get("tg_id"):
        return _json({"ok": False, "error": "not_found"}, status=404)

    uid = fs["tg_id"]
    stats = fs.get("stats") or {}

    profile = {
        "tg_id": uid,
        "username": stats.get("username") or None,
        "name": " ".join(p for p in [stats.get("first_name") or "", stats.get("last_name") or ""] if p).strip() or None,
        "is_bot": stats.get("is_bot"),
        "is_active": stats.get("is_active"),
        "registered_at": (str(stats.get("first_msg_date") or "").split("T")[0] or None),
        "last_message_at": (str(stats.get("last_msg_date") or "").split("T")[0] or None),
        "messages_count": stats.get("total_msg_count"),
        "groups_count": stats.get("total_groups"),
        "username_history": [],
        "name_history": [],
        "gifts": [],
        "gifts_count": 0,
        "favorite_chat": None,
        "admin_in_chats": 0,
        "bot_stats": {},
        "bio": None,
        "personal_channel": None,
        "birthday": None,
        "premium": False,
        "verified": False,
        "common_chats_count": None,
        "interests": [],
    }

    for u in (fs.get("usernames_history") or [])[:30]:
        if isinstance(u, dict):
            uname = u.get("username") or u.get("name") or u.get("value")
            date = u.get("date") or u.get("first_seen") or u.get("last_seen")
            if uname:
                profile["username_history"].append({
                    "username": uname if str(uname).startswith("@") else f"@{uname}",
                    "date": str(date).split("T")[0] if date else None,
                })

    try:
        bot_data = await query_funstat_bot(str(uid))
        if bot_data:
            existing = {h["username"].lower() for h in profile["username_history"]}
            for u in bot_data.get("usernames", []):
                if u.lower() not in existing:
                    profile["username_history"].append({
                        "username": u,
                        "date": None,
                    })
                    existing.add(u.lower())

            if bot_data.get("names"):
                profile["name_history"] = bot_data["names"]

            if bot_data.get("stats"):
                profile["bot_stats"] = bot_data["stats"]

            if bot_data.get("favorite_chat"):
                profile["favorite_chat"] = bot_data["favorite_chat"]

            if bot_data.get("admin_in_chats"):
                profile["admin_in_chats"] = bot_data["admin_in_chats"]
    except Exception as e:
        logger.error(f"funstat bot merge error: {e}")

    username = stats.get("username") or None
    try:
        full_info = await get_full_user_info(str(uid), username)
        if full_info:
            profile["bio"] = full_info.get("bio")
            if full_info.get("personal_channel_id") or full_info.get("personal_channel_link"):
                profile["personal_channel"] = {
                    "id": full_info.get("personal_channel_id"),
                    "title": full_info.get("personal_channel_title"),
                    "username": full_info.get("personal_channel_username"),
                    "link": full_info.get("personal_channel_link"),
                }
            profile["birthday"] = full_info.get("birthday")
            profile["premium"] = full_info.get("premium", False)
            profile["verified"] = full_info.get("verified", False)
            profile["common_chats_count"] = full_info.get("common_chats_count")
    except Exception as e:
        logger.error(f"full_user_info merge error: {e}")

    try:
        gd = await fetch_gifts_data(str(uid), username)
        if gd and not gd.get("error"):
            ids = []
            for g in (gd.get("profile_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in ids:
                    ids.append(fid)
            for g in (gd.get("saved_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in ids:
                    ids.append(fid)
            profile["gifts"] = [{"id": i, "link": f"tg://user?id={i}"} for i in ids]
            profile["gifts_count"] = len(ids)
    except Exception:
        logger.exception("public_tg_info gifts")

    # === Интересы ===
    try:
        channel_title = None
        if profile.get("personal_channel"):
            channel_title = profile["personal_channel"].get("title")
        profile["interests"] = detect_interests(
            bio=profile.get("bio"),
            favorite_chat=profile.get("favorite_chat"),
            name_history=profile.get("name_history"),
            username_history=profile.get("username_history"),
            channel_title=channel_title,
        )
        logger.info(f"interests: {len(profile['interests'])} для {uid}")
    except Exception as e:
        logger.error(f"detect_interests error: {e}")
        profile["interests"] = []

    return _json({"ok": True, "query": q, "profile": profile})


# ============ TELETHON START ============

async def start_tg_client():
    global tg_client, _funstat_bot_lock, _funstat_bot_blocked_until, _last_bot_request_ts

    _funstat_bot_cache.clear()
    _funstat_bot_blocked_until = 0.0
    _last_bot_request_ts = 0.0
    logger.info("🧹 funstat_bot cache + блокировки сброшены")

    try:
        _funstat_bot_lock = asyncio.Semaphore(1)

        if TG_SESSION_STR:
            logger.info("🔐 Используется юзер-сессия (StringSession)")
            session = StringSession(TG_SESSION_STR)
            tg_client = TelegramClient(session, TG_API_ID, TG_API_HASH)
            await tg_client.start()
            me = await tg_client.get_me()
            logger.info(f"✅ Telethon (user) запущен: @{me.username if me.username else me.id}")
        else:
            logger.warning("⚠️ TG_SESSION_STR не задан — используется бот-сессия")
            tg_client = TelegramClient(TG_SESSION_NAME, TG_API_ID, TG_API_HASH)
            await tg_client.start(bot_token=BOT_TOKEN)
            me = await tg_client.get_me()
            logger.info(f"✅ Telethon (bot) запущен: @{me.username if me.username else me.id}")

        has_user_gifts = hasattr(tg_functions.payments, "GetUserStarGiftsRequest")
        has_saved_gifts = hasattr(tg_functions.payments, "GetSavedStarGiftsRequest")
        logger.info(f"GetUserStarGiftsRequest: {has_user_gifts}")
        logger.info(f"GetSavedStarGiftsRequest: {has_saved_gifts}")

    except Exception as e:
        logger.error(f"❌ Telethon старт ошибка: {e}")
        tg_client = None


# ============ MAIN ============

async def main():
    await asyncio.sleep(1)
    await init_db()

    asyncio.create_task(start_tg_client())

    @web.middleware
    async def cors_middleware(request, handler):
        if request.method == "OPTIONS":
            response = web.Response()
        else:
            try:
                response = await handler(request)
            except web.HTTPException as ex:
                response = ex
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response

    app = web.Application(middlewares=[cors_middleware])

    app.router.add_get("/", lambda r: web.Response(text="DataSeeker API"))
    app.router.add_get("/health", lambda r: web.Response(text="OK"))

    app.router.add_post("/api/v1/key/create", api_key_create_handler)
    app.router.add_post("/api/v1/key/info", api_key_info_handler)
    app.router.add_post("/api/v1/key/topup", api_key_topup_handler)
    app.router.add_get("/api/v1/topup_packages", api_topup_packages_handler)

    app.router.add_get("/api/v1/pricing", api_pricing_handler)
    app.router.add_route("OPTIONS", "/api/v1/buy/pack", api_buy_pack_handler)
    app.router.add_post("/api/v1/buy/pack", api_buy_pack_handler)
    app.router.add_route("OPTIONS", "/api/v1/buy/subscription", api_buy_subscription_handler)
    app.router.add_post("/api/v1/buy/subscription", api_buy_subscription_handler)

    app.router.add_route("OPTIONS", "/api/v1/search", api_public_search_handler)
    app.router.add_post("/api/v1/search", api_public_search_handler)
    app.router.add_get("/api/v1/search", api_public_search_handler)

    app.router.add_route("OPTIONS", "/api/v1/search/batch", api_batch_search_handler)
    app.router.add_post("/api/v1/search/batch", api_batch_search_handler)

    app.router.add_route("OPTIONS", "/api/v1/task/create", api_task_create_handler)
    app.router.add_post("/api/v1/task/create", api_task_create_handler)
    app.router.add_route("OPTIONS", "/api/v1/task/status", api_task_status_handler)
    app.router.add_get("/api/v1/task/status", api_task_status_handler)

    app.router.add_route("OPTIONS", "/api/tg/info", api_public_tg_info_handler)
    app.router.add_get("/api/tg/info", api_public_tg_info_handler)

    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"✅ API server started on port {port}")

    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
        if tg_client:
            try:
                await tg_client.disconnect()
            except Exception:
                pass
        if db_conn:
            if db_conn.backend == "sqlite" and db_conn.conn:
                await db_conn.conn.close()
            elif db_conn.backend == "postgres" and db_conn.pool:
                await db_conn.pool.close()
        if http_session:
            await http_session.close()


if __name__ == "__main__":
    asyncio.run(main())
