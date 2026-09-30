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

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()


def _require_env(name):
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

TOPUP_PACKAGES = [
    {"id": "pack_start", "tier": "СТАРТ", "rub": 50, "searches": 50, "bonus": 0},
    {"id": "pack_base", "tier": "БАЗОВЫЙ", "rub": 100, "searches": 100, "bonus": 0},
    {"id": "pack_plus", "tier": "ВЫГОДНЫЙ", "rub": 500, "searches": 500, "bonus": 50},
    {"id": "pack_pro", "tier": "ПРОФИ", "rub": 1000, "searches": 1000, "bonus": 150},
    {"id": "pack_biz", "tier": "БИЗНЕС", "rub": 5000, "searches": 5000, "bonus": 1000},
]
SUBSCRIPTIONS = [
    {"id": "sub_min", "tier": "МИНИ", "rub": 300, "daily": 50},
    {"id": "sub_std", "tier": "СТАНДАРТ", "rub": 600, "daily": 150},
    {"id": "sub_pro", "tier": "ПРО", "rub": 1200, "daily": 400},
]
SUB_DURATION_DAYS = 30


def _esc(v):
    return str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _json(data, status=200):
    return web.json_response(data, status=status,
        dumps=lambda o: json.dumps(o, ensure_ascii=False))


def _json_error(msg, status=400):
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


class DBAdapter:
    def __init__(self, backend, conn=None, pool=None):
        self.backend = backend
        self.conn = conn
        self.pool = pool
        self._lock = asyncio.Lock()

    @staticmethod
    def _translate(sql):
        n = [0]
        def repl(_):
            n[0] += 1
            return f"${n[0]}"
        sql = re.sub(r'\?', repl, sql)
        sql = re.sub(r"date\('now'\)", "CURRENT_DATE", sql, flags=re.IGNORECASE)
        if re.search(r'INSERT\s+OR\s+IGNORE', sql, re.IGNORECASE):
            sql = re.sub(r'INSERT\s+OR\s+IGNORE', 'INSERT', sql, flags=re.IGNORECASE)
            sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
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

    async def executescript(self, sql):
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
_FUNSTAT_BOTS = ["@evenfa_bot", "@SangMataInfo_bot"]
_funstat_bot_lock = None
_funstat_bot_blocked_until = 0.0
_last_bot_request_ts = 0.0
_BOT_MIN_INTERVAL = 8.0

_words_cache = {}
_WORDS_CACHE_TTL = 1800


def get_cache_key(fn, q):
    return f"{fn}:{hashlib.md5(q.encode()).hexdigest()}"


API_TIMEOUTS = {
    "funstat": 10.0, "tg_gifts": 30.0, "funstat_bot": 60.0,
    "tg_full_user": 15.0, "words_button": 90.0,
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
    key TEXT PRIMARY KEY, plan_id TEXT, daily_quota INTEGER DEFAULT 0,
    started_at TEXT, expires_at TEXT, last_credited_date TEXT, active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY, key TEXT, query TEXT, status TEXT DEFAULT 'pending',
    result TEXT, error TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tasks_key ON tasks(key);
'''

SCHEMA_PG = '''
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS site_keys (
    key TEXT PRIMARY KEY, balance_kopeks BIGINT DEFAULT 100,
    total_searches BIGINT DEFAULT 0, today_searches BIGINT DEFAULT 0,
    today_date DATE, active SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_site_keys_key ON site_keys(key);
CREATE TABLE IF NOT EXISTS subscriptions (
    key TEXT PRIMARY KEY, plan_id TEXT, daily_quota BIGINT DEFAULT 0,
    started_at TIMESTAMP, expires_at TIMESTAMP, last_credited_date DATE,
    active SMALLINT DEFAULT 1
);
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY, key TEXT, query TEXT, status TEXT DEFAULT 'pending',
    result TEXT, error TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tasks_key ON tasks(key);
'''


async def init_db():
    global db_conn
    if DATABASE_URL and HAS_ASYNCPG:
        logger.info("🔌 DATABASE_URL найден, пробуем PostgreSQL...")
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10,
                timeout=10.0, command_timeout=30.0)
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
        except Exception as e:
            logger.warning(f"❌ PostgreSQL: {e}")
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
        logger.error(f"❌ SQLite: {e}")
        raise


async def get_http_session():
    global http_session, _http_lock
    if _http_lock is None:
        _http_lock = asyncio.Lock()
    if http_session is None or http_session.closed:
        async with _http_lock:
            if http_session is None or http_session.closed:
                http_session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
    return http_session


async def _funstat_get(path, params=None, timeout=10.0):
    if not FUNSTAT_TOKEN:
        return {}
    session = await get_http_session()
    url = f"{FUNSTAT_BASE}{path}"
    headers = {"Authorization": f"Bearer {FUNSTAT_TOKEN}", "Accept": "application/json",
               "User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, params=params or {}, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            text = await resp.text()
            logger.info(f"Funstat [{path}] status={resp.status}")
            if resp.status != 200:
                return {}
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {}
    except Exception as e:
        logger.error(f"Funstat exception: {e}")
        return {}


async def funstat_search(query, search_type="tg_id"):
    if not FUNSTAT_TOKEN or search_type != "tg_id":
        return {}
    q = str(query).strip().lstrip("@")
    if not q.isdigit():
        return {}
    tid = int(q)
    out = {"tg_id": tid, "stats": {}, "gifts": [], "groups_count": None, "usernames_history": []}
    stats = await _funstat_get(f"/api/v1/users/{tid}/stats_min")
    if stats.get("id"):
        out["stats"] = stats
    gc = await _funstat_get(f"/api/v1/users/{tid}/groups_count")
    if isinstance(gc, (int, float)):
        out["groups_count"] = int(gc)
    gifts = await _funstat_get(f"/api/v1/users/{tid}/gifts_relation")
    if gifts.get("success") and gifts.get("data"):
        out["gifts"] = gifts["data"]
    unames = await _funstat_get(f"/api/v1/users/{tid}/usernames")
    if unames.get("success") and unames.get("data"):
        out["usernames_history"] = unames["data"]
    return out


def parse_funstat(data):
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
    for g in (data.get("gifts") or []):
        from_id = g.get("from_user_id")
        to_id = g.get("to_user_id")
        date_str = (g.get("last_gift_date") or "").split("T")[0]
        if my_id and from_id == my_id:
            parsed.append({"type": "result", "source": "Funstat · Кому дарил", "data": {
                "Кому": (f"@{g['to_mainUsername']}" if g.get("to_mainUsername") else ""),
                "Имя": " ".join(p for p in [g.get("to_first_name"), g.get("to_last_name")] if p).strip(),
                "Username": ("@" + g["to_mainUsername"]) if g.get("to_mainUsername") else "",
                "Telegram ID": str(to_id) if to_id else "",
                "Дата подарка": date_str}})
        elif my_id and to_id == my_id:
            parsed.append({"type": "result", "source": "Funstat · От кого получал", "data": {
                "От": (f"@{g['from_mainUsername']}" if g.get("from_mainUsername") else ""),
                "Имя": " ".join(p for p in [g.get("from_first_name"), g.get("from_last_name")] if p).strip(),
                "Username": ("@" + g["from_mainUsername"]) if g.get("from_mainUsername") else "",
                "Telegram ID": str(from_id) if from_id else "",
                "Дата подарка": date_str}})
    for u in (data.get("usernames_history") or [])[:30]:
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


# ============ TELETHON ============

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
    name = " ".join(x for x in [getattr(user, "first_name", None), getattr(user, "last_name", None)] if x)
    return name or str(user.id)


async def tg_get_profile_gifts(user):
    if tg_client is None:
        return []
    req_cls = getattr(tg_functions.payments, "GetUserStarGiftsRequest", None)
    if req_cls is None:
        return []
    try:
        result = await tg_client(req_cls(user_id=user))
        return getattr(result, "gifts", []) or []
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
            result = await tg_client(tg_functions.payments.GetSavedStarGiftsRequest(
                peer=user, offset=offset, limit=100))
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


async def _resolve_tg_entity(target, username=None):
    if username:
        uname = username.lstrip("@").strip()
        if uname:
            try:
                return await tg_client.get_entity(uname)
            except Exception:
                pass
    target = (target or "").strip().lstrip("@")
    if not target.isdigit():
        raise ValueError("Ожидается числовой ID")
    uid = int(target)
    try:
        return await tg_client.get_entity(uid)
    except Exception:
        try:
            from telethon.tl.types import InputPeerUser
            return await tg_client.get_entity(InputPeerUser(uid, 0))
        except Exception:
            raise ValueError("Не удалось получить профиль")


async def fetch_gifts_data(target, username=None):
    if tg_client is None:
        return {"error": "MTProto не инициализирован"}
    try:
        user = await asyncio.wait_for(_resolve_tg_entity(target, username),
                                        timeout=API_TIMEOUTS["tg_gifts"])
    except asyncio.TimeoutError:
        return {"error": "Таймаут"}
    except Exception as e:
        return {"error": f"Не удалось: {e}"}
    try:
        profile_gifts = await asyncio.wait_for(tg_get_profile_gifts(user),
                                                timeout=API_TIMEOUTS["tg_gifts"])
    except Exception:
        profile_gifts = []
    try:
        saved_gifts = await asyncio.wait_for(tg_get_saved_gifts(user),
                                                timeout=API_TIMEOUTS["tg_gifts"])
    except Exception:
        saved_gifts = []
    return {"user_id": user.id, "username": tg_get_username(user),
            "profile_gifts": profile_gifts, "saved_gifts": saved_gifts}


# ============ BOT OBFUSCATION (расширенная карта) ============

_BOT_OBF_MAP = {
    # ---- Греческий ----
    'α':'a','β':'b','γ':'y','δ':'d','ε':'e','ζ':'z','η':'n','θ':'th',
    'ι':'i','κ':'k','λ':'l','μ':'m','ν':'v','ξ':'x','ο':'o','π':'p',
    'ρ':'p','σ':'s','ς':'s','τ':'t','υ':'u','φ':'f','χ':'x','ψ':'ps','ω':'o',
    'Α':'A','Β':'B','Γ':'G','Δ':'D','Ε':'E','Ζ':'Z','Η':'H','Θ':'Th',
    'Ι':'I','Κ':'K','Λ':'L','Μ':'M','Ν':'N','Ξ':'X','Ο':'O','Π':'P',
    'Ρ':'P','Σ':'S','Τ':'T','Υ':'Y','Φ':'F','Χ':'X','Ψ':'Ps','Ω':'O',
    # ---- Small caps ----
    'ց':'g','ᴜ':'u','ᴠ':'v','ᴧ':'L','ᴨ':'n','ᴦ':'r','ᴋ':'k','ᴍ':'m',
    'ᴏ':'o','ᴘ':'p','ᴛ':'t','ᴅ':'d','ᴢ':'z','ᴊ':'j','ᴡ':'w','ʏ':'y',
    'ɪ':'I','ᴎ':'n','ᴓ':'o','Ⅼ':'L','Ⅽ':'C','Ⅾ':'D','Ⅿ':'M',
    'ⅰ':'i','ⅼ':'l','ⅽ':'c','ⅾ':'d','ⅿ':'m','℮':'e','ƒ':'f',
    'ł':'l','қ':'q','ѕ':'s','ħ':'h',
    # ---- Полноширинные латинские (ＡＢＣ / ａｂｃ) ----
    'Ａ':'A','Ｂ':'B','Ｃ':'C','Ｄ':'D','Ｅ':'E','Ｆ':'F','Ｇ':'G','Ｈ':'H',
    'Ｉ':'I','Ｊ':'J','Ｋ':'K','Ｌ':'L','Ｍ':'M','Ｎ':'N','Ｏ':'O','Ｐ':'P',
    'Ｑ':'Q','Ｒ':'R','Ｓ':'S','Ｔ':'T','Ｕ':'U','Ｖ':'V','Ｗ':'W','Ｘ':'X',
    'Ｙ':'Y','Ｚ':'Z',
    'ａ':'a','ｂ':'b','ｃ':'c','ｄ':'d','ｅ':'e','ｆ':'f','ｇ':'g','ｈ':'h',
    'ｉ':'i','ｊ':'j','ｋ':'k','ｌ':'l','ｍ':'m','ｎ':'n','ｏ':'o','ｐ':'p',
    'ｑ':'q','ｒ':'r','ｓ':'s','ｔ':'t','ｕ':'u','ｖ':'v','ｗ':'w','ｘ':'x',
    'ｙ':'y','ｚ':'z',
    # ---- Турецкие ----
    'İ':'I','ı':'i',
    # ---- Кириллица (гомоглифы для заголовков) ----
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'z','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'h','ц':'c','ч':'c','ш':'s','щ':'s',
    'ъ':'','ы':'y','ь':'','э':'e','ю':'u','я':'y',
    'А':'A','Б':'B','В':'V','Г':'G','Д':'D','Е':'E','Ё':'E','Ж':'Z','З':'Z',
    'И':'I','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O','П':'P','Р':'R',
    'С':'S','Т':'T','У':'U','Ф':'F','Х':'H','Ц':'C','Ч':'C','Ш':'S','Щ':'S',
    'Ъ':'','Ы':'Y','Ь':'','Э':'E','Ю':'U','Я':'Y',
    # ---- Украинские / белорусские / сербские ----
    'і':'i','І':'I','ї':'i','Ї':'I','є':'e','Є':'E','ґ':'g','Ґ':'G',
    'Ѕ':'S','ј':'j','Ј':'J','ԁ':'d',
    # ---- Невидимые ----
    'ᅠ':'',
}
_BOT_TRANS = str.maketrans(_BOT_OBF_MAP)


def _normalize_bot_text(s):
    return str(s or "").translate(_BOT_TRANS)


# ============ GIFTS VIA 🎁 ============

def _extract_gift_line(line):
    m = re.match(r'(\d{4}-\d{2}-\d{2})\.?\s*([⇇⇉←→⬅➡])\s*(.+?)$', line.strip())
    if not m:
        return None
    arrow = m.group(2)
    rest = m.group(3).strip()
    uid = None
    username = None
    hash_m = re.search(r'start=0102([A-Fa-f0-9]+)01000000', rest)
    if hash_m:
        hex_str = hash_m.group(1)
        try:
            if len(hex_str) >= 8:
                bytes_le = bytes.fromhex(hex_str[:8])
                uid = str(int.from_bytes(bytes_le, byteorder='little'))
        except Exception:
            pass
    um = re.search(r'@([a-zA-Z0-9_]{4,32})', rest)
    if um:
        username = um.group(1)
    if not uid:
        hm = re.search(r'#(\d{5,15})\b', rest)
        if hm:
            uid = hm.group(1)
    return arrow, uid, username


async def _resolve_uname(username):
    if not username or tg_client is None:
        return None
    try:
        entity = await asyncio.wait_for(tg_client.get_entity(username), timeout=10.0)
        return str(entity.id)
    except Exception:
        return None


async def query_gifts_via_button(tg_id):
    global _funstat_bot_lock, _last_bot_request_ts, _funstat_bot_blocked_until
    empty = {"sent": [], "received": [], "total": 0}
    if tg_client is None or not TG_SESSION_STR:
        return empty
    if _funstat_bot_lock is None:
        _funstat_bot_lock = asyncio.Semaphore(1)
    if time.time() < _funstat_bot_blocked_until:
        return empty
    cache_key = f"gifts:{tg_id}"
    now = time.time()
    cached = _words_cache.get(cache_key)
    if cached and now - cached[0] < _WORDS_CACHE_TTL:
        return cached[1]
    delta = now - _last_bot_request_ts
    if delta < _BOT_MIN_INTERVAL:
        await asyncio.sleep(_BOT_MIN_INTERVAL - delta)
    _last_bot_request_ts = time.time()
    try:
        bot = await tg_client.get_entity(_FUNSTAT_BOT_NAME)
    except Exception:
        return empty
    from telethon.tl.functions.messages import GetBotCallbackAnswerRequest

    async def _find_pages(msg):
        pages = []
        if not msg or not msg.reply_markup:
            return pages
        for row in msg.reply_markup.rows:
            for btn in row.buttons:
                txt = (btn.text or "").strip()
                m = re.search(r'(\d+)\s*$', txt)
                if m and len(txt) < 10:
                    try:
                        n = int(m.group(1))
                        if n >= 2:
                            bt = getattr(btn, "type", None)
                            cb = getattr(bt, "data", None) if bt else None
                            if cb is None:
                                cb = getattr(btn, "data", None)
                            if cb:
                                pages.append({"num": n, "data": cb})
                    except ValueError:
                        pass
        pages.sort(key=lambda x: x["num"])
        return pages

    async def _click(msg_id, cb_data):
        try:
            await tg_client(GetBotCallbackAnswerRequest(peer=bot, msg_id=msg_id, data=cb_data))
        except Exception:
            return None
        await asyncio.sleep(3)
        for i in range(15):
            try:
                fresh = await tg_client.get_messages(bot, ids=msg_id)
                if fresh and fresh.text:
                    return fresh
            except Exception:
                pass
            await asyncio.sleep(1)
        return None

    all_lines = []
    async with _funstat_bot_lock:
        try:
            async with tg_client.conversation(bot, timeout=API_TIMEOUTS["funstat_bot"]) as conv:
                await conv.send_message(tg_id)
                first_msg = await conv.get_response()
            if not first_msg.reply_markup:
                return empty
            target_btn = None
            for row in first_msg.reply_markup.rows:
                for btn in row.buttons:
                    if "🎁" in (btn.text or ""):
                        target_btn = btn
                        break
                if target_btn:
                    break
            if not target_btn:
                return empty
            cb_data = (getattr(getattr(target_btn, "type", None), "data", None)
                        or getattr(target_btn, "data", None))
            if cb_data is None:
                return empty
            try:
                await tg_client(GetBotCallbackAnswerRequest(peer=bot, msg_id=first_msg.id, data=cb_data))
            except Exception:
                return empty
            await asyncio.sleep(5)
            current = await tg_client.get_messages(bot, ids=first_msg.id)
            if not current or not current.text:
                return empty
            all_lines.append(current.text)
            pages = await _find_pages(current)
            for p in pages:
                fresh = await _click(first_msg.id, p["data"])
                if fresh:
                    all_lines.append(fresh.text)
                await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"query_gifts error: {e!r}")
            return empty
    raw_entries = []
    for text in all_lines:
        for line in _normalize_bot_text(text).split("\n"):
            r = _extract_gift_line(line)
            if r:
                raw_entries.append(r)
    usernames_to_resolve = set()
    for arrow, uid, uname in raw_entries:
        if not uid and uname:
            usernames_to_resolve.add(uname)
    username_to_id = {}
    for uname in usernames_to_resolve:
        uid = await _resolve_uname(uname)
        if uid:
            username_to_id[uname] = uid
        await asyncio.sleep(0.4)
    sent = []
    received = []
    for arrow, uid, uname in raw_entries:
        if not uid and uname and uname in username_to_id:
            uid = username_to_id[uname]
        if not uid or not str(uid).isdigit():
            continue
        if arrow in ("⇇", "←", "⬅"):
            if uid not in received:
                received.append(uid)
        else:
            if uid not in sent:
                sent.append(uid)
    total = 0
    if all_lines:
        tm = re.search(r'[Tt]o[tτ]al\s*\*?\*?(\d+)', _normalize_bot_text(all_lines[0]))
        if tm:
            total = int(tm.group(1))
    result = {"sent": sent, "received": received, "total": total}
    _words_cache[cache_key] = (time.time(), result)
    return result


# ============ WORDS VIA 🗣 ============

_TO_KIRILLIC = {
    'a':'а','b':'в','c':'с','d':'д','e':'е','f':'ф','g':'г','h':'н','i':'и',
    'j':'й','k':'к','l':'л','m':'м','n':'п','o':'о','p':'п','q':'к','r':'р',
    's':'с','t':'т','u':'и','v':'в','w':'в','x':'х','y':'у','z':'з',
    'α':'а','β':'в','γ':'у','δ':'д','ε':'е','ζ':'з','η':'н','θ':'т','ι':'и',
    'κ':'к','λ':'л','μ':'м','ν':'в','ξ':'х','ο':'о','π':'п','ρ':'р','σ':'с',
    'ς':'с','τ':'т','υ':'и','φ':'ф','χ':'х','ψ':'п','ω':'о',
    'ց':'г','ᴜ':'и','ᴠ':'в','ᴧ':'л','ᴨ':'п','ᴦ':'р','ᴋ':'к','ᴍ':'м',
    'ᴏ':'о','ᴘ':'п','ᴛ':'т','ᴅ':'д','ᴢ':'з','ᴊ':'й','ᴡ':'в','ʏ':'у',
    'ɪ':'и','ᴎ':'н','ᴓ':'о','Ⅼ':'л','Ⅽ':'с','Ⅾ':'д','Ⅿ':'м',
    'ⅰ':'и','ⅼ':'л','ⅽ':'с','ⅾ':'д','ⅿ':'м','℮':'е','ƒ':'ф',
    'ł':'л','қ':'к','ѕ':'с','і':'и','ї':'и','ў':'у','ј':'й',
}
_KIR_TRANS = str.maketrans(_TO_KIRILLIC)


def _full_normalize(s):
    return str(s or "").translate(_KIR_TRANS).lower()


WORDS_KEYWORD_MAP = {
    "Гейминг": ["килл","килаур","kill","чит","cheat","реп","репут","майн","csgo","cs2",
                "dota","дота","кс2","ксго","fortnite","фортнайт","pubg","пабг","gta","гта",
                "warcraft","wow","лол","lol","геншин","genshin","роблокс","roblox",
                "майнкрафт","minecraft","стим","steam","танки","аим","aim","спавн","spawn",
                "лут","loot","клан","clan","сервер","server","ранг","rank"],
    "Маты/Сленг": ["ебан","ебал","еблан","ебат","ебуч","пизд","хуй","хуя","бляд","блят",
                    "мраз","долбо","нахуй","нахуя","залуп","гондон","мудак","дебил","туп",
                    "кринж","кринге","база","жиза","рофл","кек","азаз","ахах","хех","нуб",
                    "читер","читор","краб","мамк","мат"],
    "Криптовалюты": ["битк","bitcoin","биткойн","eth","ethereum","эфир","токен","token",
                      "крипт","crypto","usdt","tether","ton","sol","solana","bnb","binance",
                      "бинанс","defi","nft","web3","блокчейн","blockchain","кошелек","wallet",
                      "airdrop","эирдроп","майнер","miner"],
    "AI/Нейросети": ["нейро","neuro","нейросет","нейронк","gpt","чатгпт","chatgpt",
                      "midjourney","midjorney","stable","diffusion","candy","openai",
                      "опенаи","claude","llm","промпт","prompt"],
    "Операторы/SIM": ["мегафон","megafon","мтс","mts","билайн","beeline","теле2","tele2",
                       "оператор","sim","симк","мобил","тариф","связь","мобильн"],
    "Подписки/Контент": ["подпис","подпиш","лайк","репост","канал","shorts","reels",
                          "тикток","tiktok","инста","insta","youtube","ютуб","блогер",
                          "стрим","стример","донат","буст","видео","video","клип","clip"],
    "Программирование": ["python","питон","код","code","программ","разраб","developer",
                          "javascript","backend","frontend","golang","java","php","sql",
                          "docker","github","гитхаб","баг","bug","фикс","fix","деплой","deploy"],
    "Инвестиции/Трейдинг": ["трейд","trade","форекс","forex","инвест","invest","брокер",
                            "broker","акци","биржа","дивиденд","портфель","portfolio",
                            "профит","profit","убыток","лосс","loss","скальп","scalp"],
    "Музыка": ["music","музык","битмейк","beatmaker","диджей","dj","рэп","rap","хип-хоп",
                "hip-hop","трек","track","бит","beat","soundcloud","спотифай","spotify"],
    "Спорт": ["спорт","sport","фитнес","fitness","gym","качалк","бодибилд","кроссфит",
              "бокс","мма","футбол","хоккей","баскетбол","спортзал","тренер","coach"],
    "Авто": ["авто","auto","машин","bmw","mers","mercedes","audi","toyota","тачк",
             "тюнинг","tuning","двигат","движок","колес","руль","wheel","car"],
    "Кино/Аниме": ["аниме","anime","манга","manga","сериал","series","фильм","movie",
                    "кино","netflix","нетфликс","дорам","dorama","сезон","season"],
    "Путешествия": ["путеш","travel","вокруг света","trip","tourist","tour","отдых",
                     "виза","пляж","beach","отпуск","vacation","аэропорт","airport"],
    "Обучение": ["учус","учит","студент","student","school","школа","универ","обучен",
                  "education","курс","course","экзамен","exam","сессия","диплом"],
    "Мода/Красота": ["мода","fashion","красот","beauty","макияж","стиль","style",
                      "маникюр","визаж","парикмахер","шмот","одежд"],
    "Бизнес": ["бизнес","business","стартап","startup","предприн","founder","ceo",
                "маркетинг","marketing","продаж","sales","smm","смм","клиент","client"],
    "Продажа физов": ["селл","sell","слив","прод","физ","физы","физов","физиков",
                       "пробив","база","дамп","паспорт","инн","снилс","дропы",
                       "карт","скам","scam","селлер","селлеры","продаж"],
    "OSINT/Darknet": ["osint","пробив","детектив","расслед","черн","даркнет","дарк","darknet"],
}


def _parse_words_response(text):
    if not text:
        return []
    words = []
    for line in str(text).split("\n"):
        line = line.strip()
        m = re.match(r'[├|└]\s*\*?\*?(\d+)\s*[-–]\s*(\d+)\*?\*?\s*[`\s]*([^`\n]+)', line)
        if not m:
            continue
        try:
            mn = int(m.group(1)); mx = int(m.group(2))
        except ValueError:
            continue
        for w in m.group(3).strip().rstrip('`').strip().split(","):
            wc = re.sub(r'[^\w\-]+', '', w, flags=re.UNICODE).strip()
            if wc and len(wc) >= 3:
                words.append({"word": wc, "avg": (mn + mx) // 2})
    words.sort(key=lambda x: x["avg"], reverse=True)
    return words


def _detect_interests_from_words(words, max_results=5):
    if not words:
        return []
    all_text = " ".join(_full_normalize(w.get("word", "")) for w in words)
    res = []
    for cat, kws in WORDS_KEYWORD_MAP.items():
        hits = sum(1 for kw in kws if _full_normalize(kw) in all_text)
        if hits > 0:
            res.append((cat, hits))
    res.sort(key=lambda x: x[1], reverse=True)
    return [r[0] for r in res[:max_results]]


async def query_words_via_button(tg_id):
    global _funstat_bot_lock, _last_bot_request_ts, _funstat_bot_blocked_until
    if tg_client is None or not TG_SESSION_STR:
        return {"categories": []}
    if _funstat_bot_lock is None:
        _funstat_bot_lock = asyncio.Semaphore(1)
    if time.time() < _funstat_bot_blocked_until:
        return {"categories": []}
    now = time.time()
    cache_key = f"words:{tg_id}"
    cached = _words_cache.get(cache_key)
    if cached and now - cached[0] < _WORDS_CACHE_TTL:
        return cached[1]
    delta = now - _last_bot_request_ts
    if delta < _BOT_MIN_INTERVAL:
        await asyncio.sleep(_BOT_MIN_INTERVAL - delta)
    _last_bot_request_ts = time.time()
    try:
        bot = await tg_client.get_entity(_FUNSTAT_BOT_NAME)
    except Exception:
        return {"categories": []}
    from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
    async with _funstat_bot_lock:
        try:
            async with tg_client.conversation(bot, timeout=API_TIMEOUTS["funstat_bot"]) as conv:
                await conv.send_message(tg_id)
                first_msg = await conv.get_response()
            if not first_msg.reply_markup:
                return {"categories": []}
            target_btn = None
            for row in first_msg.reply_markup.rows:
                for btn in row.buttons:
                    if "🗣" in (btn.text or ""):
                        target_btn = btn
                        break
                if target_btn:
                    break
            if not target_btn:
                return {"categories": []}
            cb_data = (getattr(getattr(target_btn, "type", None), "data", None)
                        or getattr(target_btn, "data", None))
            if cb_data is None:
                return {"categories": []}
            try:
                await tg_client(GetBotCallbackAnswerRequest(peer=bot, msg_id=first_msg.id, data=cb_data))
            except Exception:
                return {"categories": []}
            words_text = ""
            for i in range(30):
                await asyncio.sleep(1)
                try:
                    fresh = await tg_client.get_messages(bot, ids=first_msg.id)
                    if fresh and fresh.text and re.search(r'[├|└]\s*\*?\*?\d+\s*[-–]\s*\d+', fresh.text):
                        words_text = fresh.text
                        break
                except Exception:
                    pass
            if not words_text:
                return {"categories": []}
        except Exception:
            return {"categories": []}
    words = _parse_words_response(words_text)
    cats = _detect_interests_from_words(words, max_results=5)
    result = {"categories": cats, "words": words}
    if cats:
        _words_cache[cache_key] = (time.time(), result)
    return result


# ============ INTERESTS from bio ============

INTEREST_KEYWORDS = {
    "Криптовалюты": ["крипт","crypto","btc","bitcoin","eth","ethereum","токен","coin",
                      "web3","defi","nft","блокчейн","binance","бинанс","wallet","кошелек",
                      "usdt","ton","solana"],
    "Инвестиции": ["инвест","invest","брокер","broker","акци","биржа","дивиденд","портфель"],
    "Программирование": ["python","developer","dev","программ","разраб","code","coder",
                          "javascript","backend","frontend","golang","java","kotlin","swift","php","sql","docker"],
    "Игры": ["gamer","геймер","csgo","cs2","dota","gta","minecraft","steam","valorant",
             "lol","warcraft","roblox","fortnite","pubg"],
    "Музыка": ["music","музык","битмейк","beatmaker","диджей","dj","sound","звук",
               "fl studio","ableton","midi","рэп"],
    "Спорт": ["спорт","sport","фитнес","fitness","gym","качалк","бодибилд","кроссфит",
              "бокс","мма","футбол","хоккей","баскетбол"],
    "Бизнес": ["бизнес","business","стартап","startup","предприн","founder","ceo",
               "маркетинг","marketing","продаж","sales","smm"],
    "Кино/Сериалы": ["кино","movie","фильм","сериал","series","netflix","аниме","anime","manga"],
    "Авто": ["авто","auto","car","машин","тачк","bmw","mers","mercedes","audi","toyota","тюнинг"],
    "Путешествия": ["путеш","travel","вокруг света","trip","tourist","tour","отдых","виза","пляж"],
    "Продажа физов": ["селл","sell","физ","физы","пробив","слив"],
}


def detect_interests(bio=None, favorite_chat=None, name_history=None,
                      username_history=None, channel_title=None, channel_posts=None):
    sources = {
        "bio": bio or "",
        "personal_channel": channel_title or "",
        "channel_posts": " ".join(p.get("text", "")[:1000] for p in (channel_posts or []) if isinstance(p, dict))[:5000],
        "favorite_chat": favorite_chat or "",
        "username_history": " ".join(u.get("username", "") for u in (username_history or []) if isinstance(u, dict)),
        "name_history": " ".join(n.get("name", "") for n in (name_history or []) if isinstance(n, dict)),
    }
    weights = {"bio": 1.0, "personal_channel": 0.9, "channel_posts": 0.9,
               "favorite_chat": 0.7, "username_history": 0.5, "name_history": 0.4}
    found = {}
    for src, text in sources.items():
        if not text:
            continue
        norm = str(text).lower().strip()
        for cat, kws in INTEREST_KEYWORDS.items():
            hits = sum(1 for kw in kws if kw in norm)
            if hits == 0:
                continue
            conf = min(1.0, hits / 5.0) * weights[src]
            if cat not in found or conf > found[cat]["confidence"]:
                found[cat] = {"category": cat, "confidence": conf, "source": src}
    result = sorted(found.values(), key=lambda x: x["confidence"], reverse=True)
    return [{"category": r["category"], "confidence": round(r["confidence"], 2),
             "source": r["source"]} for r in result if r["confidence"] >= 0.15]


# ============ BOT PARSER (ФИКС) ============

def parse_funstat_bot_response(text):
    if not text:
        return {}
    low = text.lower()
    for marker in ["слишком много","подождите","too many","flood","rate limit","premium",
                    "купи","подписка","ошибка","не найден","not found"]:
        if marker in low:
            return {}

    norm = _normalize_bot_text(text)
    ns = norm.replace('*', '')

    result = {"tg_id": None, "usernames": [], "names": [], "stats": {},
              "favorite_chat": None, "admin_in_chats": 0,
              "channel_title": None, "channel_link": None, "channel_username": None}

    # ---------- ID ----------
    m_id = re.search(r'ID\s*[:：]\s*`?(\d{5,15})`?', ns, re.IGNORECASE)
    if m_id:
        result["tg_id"] = m_id.group(1)

    # ---------- Заголовок имён (маркер конца блока usernames) ----------
    names_match = re.search(
        r'(?:first\s*name|last\s*name|names?|imena?|imya|имя|имена)\s*[:：]',
        ns, re.IGNORECASE)

    # ---------- Usernames: всё между ID и заголовком имён ----------
    if m_id:
        start = m_id.end()
        end = names_match.start() if names_match else len(ns)
        block = ns[start:end]
        for um in re.finditer(r'@([a-zA-Z0-9_]{4,32})', block):
            u = "@" + um.group(1)
            if u not in result["usernames"]:
                result["usernames"].append(u)
        if result["usernames"]:
            logger.info(f"usernames: {result['usernames']}")

    # ---------- Имена с датами ----------
    for m in re.finditer(r'(\d{4}-\d{2}-\d{2})\s*[➜→▶►]\s*([^\n]+?)(?:\n|$)', norm):
        date = m.group(1)
        name = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', m.group(2)).strip()
        name = re.sub(r'\*\*([^*]+)\*\*', r'\1', name).strip()
        name = re.sub(r'`([^`]+)`', r'\1', name).strip()
        name = name.replace('ᅠ', '').strip()
        if name and len(name) < 100:
            result["names"].append({"date": date, "name": name})

    # ---------- Stats (EN + RU) ----------
    m = re.search(r'(\d+)\s*messages?\s+in\s+(\d+)\s+groups?', ns, re.IGNORECASE)
    if not m:
        m = re.search(r'(\d+)\s*[а-яё]*щ?[еи]ний\s+в\s+(\d+)\s*чатах?', ns, re.IGNORECASE)
    if m:
        result["stats"]["total_messages"] = int(m.group(1))
        result["stats"]["total_chats"] = int(m.group(2))

    for key, pat in [("diversity_percent", r'diversity\s+([\d,.]+)\s*%'),
                     ("diversity_percent", r'разнообраз\w*\s*([\d,.]+)\s*%'),
                     ("replies_percent", r'([\d,.]+)\s*%\s*replies'),
                     ("replies_percent", r'([\d,.]+)\s*%\s*реплаи'),
                     ("media_percent", r'([\d,.]+)\s*%\s*media'),
                     ("media_percent", r'([\d,.]+)\s*%\s*медиа')]:
        mm = re.search(pat, ns, re.IGNORECASE)
        if mm and key not in result["stats"]:
            try:
                result["stats"][key] = float(mm.group(1).replace(',', '.'))
            except ValueError:
                pass

    # ---------- Любимый чат / Админ ----------
    m = re.search(r'favorite\s+(?:group|chat)\s*:?\s*([^\n]+)', ns, re.IGNORECASE)
    if not m:
        m = re.search(r'любим\w*\s+чат\s*:?\s*([^\n]+)', ns, re.IGNORECASE)
    if m:
        result["favorite_chat"] = m.group(1).strip()

    m = re.search(r'admin\s+in\s+groups?\s*:?\s*(\d+)', ns, re.IGNORECASE)
    if not m:
        m = re.search(r'админ\s+в\s+чатах?\s*:?\s*(\d+)', ns, re.IGNORECASE)
    if m:
        result["admin_in_chats"] = int(m.group(1))

    # ---------- Канал ----------
    m = re.search(r'channel\s*:?\s*\[([^\]]+)\]\(([^)]+)\)', ns, re.IGNORECASE)
    if m:
        result["channel_title"] = m.group(1).strip()
        result["channel_link"] = m.group(2).strip()
        cm = re.search(r't\.me/([a-zA-Z0-9_]+)', result["channel_link"])
        if cm:
            result["channel_username"] = "@" + cm.group(1)

    logger.info(f"parse_bot: usernames={result['usernames']}, names={len(result['names'])}")
    return result


async def _try_single_bot(bot_name, tg_id):
    try:
        bot = await tg_client.get_entity(bot_name)
    except Exception:
        return {}
    async with _funstat_bot_lock:
        try:
            async with tg_client.conversation(bot, timeout=API_TIMEOUTS["funstat_bot"]) as conv:
                await conv.send_message(tg_id)
                response = await conv.get_response()
                text = response.text or ""
        except Exception:
            return {}
    if not text:
        return {}
    return parse_funstat_bot_response(text)


async def query_funstat_bot(tg_id):
    global _funstat_bot_lock, _last_bot_request_ts, _funstat_bot_blocked_until
    if tg_client is None or not TG_SESSION_STR:
        return {}
    if _funstat_bot_lock is None:
        _funstat_bot_lock = asyncio.Semaphore(1)
    if time.time() < _funstat_bot_blocked_until:
        return {}
    now = time.time()
    cached = _funstat_bot_cache.get(tg_id)
    if cached and now - cached[0] < _FUNSTAT_BOT_CACHE_TTL:
        return cached[1]
    delta = now - _last_bot_request_ts
    if delta < _BOT_MIN_INTERVAL:
        await asyncio.sleep(_BOT_MIN_INTERVAL - delta)
    _last_bot_request_ts = time.time()
    for bn in _FUNSTAT_BOTS:
        result = await _try_single_bot(bn, tg_id)
        if result and (result.get("usernames") or result.get("names") or result.get("stats")):
            _funstat_bot_cache[tg_id] = (time.time(), result)
            return result
    return {}


# ============ FULL USER ============

async def get_full_user_info(target, username=None):
    if tg_client is None or not TG_SESSION_STR:
        return {}
    try:
        user = await asyncio.wait_for(_resolve_tg_entity(target, username),
                                       timeout=API_TIMEOUTS["tg_full_user"])
    except Exception:
        return {}
    out = {}
    try:
        from telethon.tl.functions.users import GetFullUserRequest
        result = await asyncio.wait_for(tg_client(GetFullUserRequest(user.id)),
                                        timeout=API_TIMEOUTS["tg_full_user"])
        full = getattr(result, "full_user", None) or result
        about = getattr(full, "about", None)
        if about:
            out["bio"] = about.strip()
        cid = getattr(full, "personal_channel_id", None)
        if cid:
            out["personal_channel_id"] = cid
            try:
                ch = await asyncio.wait_for(tg_client.get_entity(cid), timeout=10.0)
                if ch:
                    out["personal_channel_title"] = getattr(ch, "title", None)
                    if getattr(ch, "username", None):
                        out["personal_channel_username"] = "@" + ch.username
                        out["personal_channel_link"] = f"https://t.me/{ch.username}"
                    else:
                        out["personal_channel_link"] = f"https://t.me/c/{cid}"
            except Exception:
                out["personal_channel_link"] = f"https://t.me/c/{cid}"
        bd = getattr(full, "birthday", None)
        if bd:
            d = getattr(bd, "day", None); mo = getattr(bd, "month", None); y = getattr(bd, "year", None)
            if d and mo:
                out["birthday"] = f"{d:02d}.{mo:02d}" + (f".{y}" if y else "")
        if getattr(user, "premium", None):
            out["premium"] = True
        if getattr(user, "verified", None):
            out["verified"] = True
        cc = getattr(full, "common_chats_count", None)
        if cc:
            out["common_chats_count"] = cc
    except Exception as e:
        logger.error(f"get_full_user_info error: {e!r}")
    return out


# ============ COLLECT ============

async def collect_general_data(query, search_type="tg_id"):
    ck = get_cache_key(search_type, query)
    if ck in cache:
        ct, d = cache[ck]
        if datetime.now() - ct < CACHE_TTL:
            return d
    parsed = []
    if search_type == "tg_id":
        fd = await funstat_search(query, "tg_id")
        parsed = parse_funstat(fd)
    result = {"query": query, "type": search_type, "blocks": [],
              "records_count": 0, "sources": []}
    sources_set = set(); records_count = 0
    seen = set(); blocks = []
    for block in parsed:
        if block["type"] != "result":
            continue
        fields = dict(block["data"])
        if not fields:
            continue
        sn = block.get("source") or "Funstat"
        rk = f"{sn}|" + "|".join(f"{k}={v}" for k, v in sorted(fields.items()))
        if rk in seen:
            continue
        seen.add(rk); sources_set.add(sn); records_count += 1
        ex = None
        for i, (name, rows) in enumerate(blocks):
            if name == sn:
                ex = i; break
        if ex is not None:
            blocks[ex][1].append(fields)
        else:
            blocks.append([sn, [fields]])
    result["sources"] = list(sources_set)
    result["records_count"] = records_count
    result["blocks"] = blocks
    cache[ck] = (datetime.now(), result)
    return result


def _format_month_year(iso):
    if not iso:
        return ""
    s = str(iso).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d") if "T" in s else datetime.strptime(s, fmt)
            months = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
            return f"{months[dt.month - 1]}, {dt.year}"
        except Exception:
            continue
    return s


def _months_ago(iso):
    if not iso:
        return ""
    s = str(iso).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d") if "T" in s else datetime.strptime(s, fmt)
            today = datetime.now()
            tm = (today.year - dt.year) * 12 + (today.month - dt.month)
            if tm < 0: tm = 0
            if tm == 0: return "сейчас"
            if tm < 12: return f"{tm} мес"
            y = tm // 12
            if y % 10 == 1 and y % 100 != 11: w = "год"
            elif 2 <= y % 10 <= 4 and (y % 100 < 10 or y % 100 >= 20): w = "года"
            else: w = "лет"
            return f"{y} {w}"
        except Exception:
            continue
    return ""


def _format_group_date(ds):
    if not ds:
        return ""
    try:
        dt = datetime.strptime(str(ds).strip()[:10], "%Y-%m-%d")
        months = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
        return f"{dt.day} {months[dt.month - 1]} {str(dt.year)[-2:]}"
    except Exception:
        return ""


def build_funstat_preview(data, query):
    tid = ""; reg = ""; names = []
    for src, rows in data.get("blocks", []):
        for row in rows:
            if not tid:
                for k in ("Telegram ID","TG ID"):
                    if row.get(k):
                        tid = str(row[k]); break
            if not reg and row.get("Первое сообщение"):
                reg = str(row["Первое сообщение"])
            if "История имён" in src:
                u = row.get("Username"); d = row.get("Дата")
                if u:
                    names.append((d or "", str(u)))
    lines = []
    h = "<b>Telegram"
    if tid:
        h += f" · {_esc(tid)}"
    h += "</b>"
    lines.append(h); lines.append("")
    if reg:
        mon = _format_month_year(reg); ago = _months_ago(reg)
        suf = f" ({_esc(ago)})" if ago else ""
        lines.append(f"<b>Регистрация:</b> ~{_esc(mon)}{suf}"); lines.append("")
    if names:
        wd = sorted([x for x in names if x[0]], key=lambda t: t[0], reverse=True)
        nod = [x for x in names if not x[0]]
        for ds, un in (wd + nod)[:15]:
            uc = un.lstrip("@")
            link = f'<a href="https://t.me/{_esc(uc)}">{_esc(un)}</a>'
            if ds:
                dsh = _format_group_date(ds)
                lines.append(f"{dsh or _esc(ds)} → {link}")
            else:
                lines.append(f"• {link}")
        lines.append("")
    if not lines or (len(lines) == 2 and not tid):
        return "По этому Telegram ничего не найдено."
    return "\n".join(lines).strip()


# ============ API ============

async def _get_key_row(key):
    if not key or not isinstance(key, str) or len(key) < 16:
        return None
    try:
        return await db_conn.fetchone('SELECT * FROM site_keys WHERE key = ? AND active = 1', (key,))
    except Exception:
        return None


async def api_key_create_handler(request):
    try:
        nk = "dsk_" + secrets.token_urlsafe(24)
        ts = date.today().isoformat()
        await db_conn.execute(
            'INSERT INTO site_keys (key, balance_kopeks, total_searches, today_searches, today_date, active) '
            'VALUES (?, ?, 0, 0, ?, 1)', (nk, SIGNUP_BONUS_KOPEKS, ts))
        return _json({"ok": True, "key": nk, "balance_kopeks": SIGNUP_BONUS_KOPEKS,
                      "balance_rub": SIGNUP_BONUS_KOPEKS/100, "total_searches": 0,
                      "today_searches": 0, "signup_bonus_kopeks": SIGNUP_BONUS_KOPEKS})
    except Exception as e:
        logger.exception("key_create")
        return _json_error(f"internal: {e}", 500)


async def api_key_info_handler(request):
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")
    key = (body.get("key") or "").strip()
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    ts = date.today().isoformat()
    tc = 0
    try:
        if row["today_date"] and str(row["today_date"])[:10] == ts:
            tc = int(row["today_searches"] or 0)
    except Exception:
        pass
    sub_info = None
    try:
        sub = await db_conn.fetchone(
            'SELECT plan_id, daily_quota, started_at, expires_at, last_credited_date '
            'FROM subscriptions WHERE key = ? AND active = 1', (key,))
        if sub:
            es = str(sub["expires_at"])[:10] if sub["expires_at"] else ""
            if es and es >= ts:
                dl = (date.fromisoformat(es) - date.today()).days
                ls = str(sub["last_credited_date"])[:10] if sub["last_credited_date"] else ""
                sub_info = {"plan_id": sub["plan_id"], "daily_quota": sub["daily_quota"],
                            "started_at": str(sub["started_at"])[:10] if sub["started_at"] else None,
                            "expires_at": es, "days_left": dl, "credited_today": ls == ts}
            else:
                await db_conn.execute('UPDATE subscriptions SET active = 0 WHERE key = ?', (key,))
    except Exception:
        pass
    return _json({"ok": True, "key": key, "balance_kopeks": row["balance_kopeks"],
                  "balance_rub": row["balance_kopeks"]/100, "total_searches": row["total_searches"],
                  "today_searches": tc, "price_per_search_kopeks": SEARCH_PRICE_KOPEKS,
                  "searches_left": row["balance_kopeks"]//SEARCH_PRICE_KOPEKS,
                  "subscription": sub_info})


async def api_key_topup_handler(request):
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")
    key = (body.get("key") or "").strip()
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    ps = int(body.get("package_stars") or 0)
    pkg = next((p for p in TOPUP_PACKAGES if p.get("rub") == ps), None)
    if not pkg:
        return _json_error("unknown_package")
    total = pkg["searches"] + pkg.get("bonus", 0)
    amt = total * SEARCH_PRICE_KOPEKS
    await db_conn.execute('UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?', (amt, key))
    nr = await db_conn.fetchone('SELECT balance_kopeks FROM site_keys WHERE key = ?', (key,))
    return _json({"ok": True, "added_kopeks": amt,
                  "balance_kopeks": nr["balance_kopeks"] if nr else row["balance_kopeks"] + amt})


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
    pid = (body.get("pack_id") or "").strip()
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    pkg = next((p for p in TOPUP_PACKAGES if p["id"] == pid), None)
    if not pkg:
        return _json_error("unknown_pack")
    total = pkg["searches"] + pkg["bonus"]
    amt = total * SEARCH_PRICE_KOPEKS
    await db_conn.execute('UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?', (amt, key))
    nr = await db_conn.fetchone('SELECT balance_kopeks FROM site_keys WHERE key = ?', (key,))
    return _json({"ok": True, "pack_id": pid, "searches_added": total, "kopeks_added": amt,
                  "balance_kopeks": nr["balance_kopeks"] if nr else row["balance_kopeks"] + amt})


async def api_buy_subscription_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")
    key = (body.get("key") or "").strip()
    sid = (body.get("sub_id") or "").strip()
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    sub = next((s for s in SUBSCRIPTIONS if s["id"] == sid), None)
    if not sub:
        return _json_error("unknown_subscription")
    today = date.today().isoformat()
    exp = (date.today() + timedelta(days=SUB_DURATION_DAYS)).isoformat()
    ex = await db_conn.fetchone(
        'SELECT key, plan_id, expires_at FROM subscriptions WHERE key = ? AND active = 1', (key,))
    if ex and ex["expires_at"]:
        ce = str(ex["expires_at"])[:10]
        if ce >= today:
            return _json({"ok": False, "error": "subscription_already_active",
                          "active_until": ce, "active_plan": ex["plan_id"]}, status=409)
    if ex:
        await db_conn.execute(
            'UPDATE subscriptions SET plan_id = ?, daily_quota = ?, started_at = ?, '
            'expires_at = ?, last_credited_date = NULL, active = 1 WHERE key = ?',
            (sid, sub["daily"], today, exp, key))
    else:
        await db_conn.execute(
            'INSERT INTO subscriptions (key, plan_id, daily_quota, started_at, expires_at, active) '
            'VALUES (?, ?, ?, ?, ?, 1)', (key, sid, sub["daily"], today, exp))
    dk = sub["daily"] * SEARCH_PRICE_KOPEKS
    await db_conn.execute('UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?', (dk, key))
    await db_conn.execute('UPDATE subscriptions SET last_credited_date = ? WHERE key = ?', (today, key))
    return _json({"ok": True, "sub_id": sid, "daily_quota": sub["daily"],
                  "started_at": today, "expires_at": exp, "credited_now": sub["daily"]})


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
            'FROM subscriptions WHERE key = ? AND active = 1', (key,))
        if sub:
            ts = date.today().isoformat()
            es = str(sub["expires_at"])[:10] if sub["expires_at"] else ""
            ls = str(sub["last_credited_date"])[:10] if sub["last_credited_date"] else ""
            if es and es >= ts:
                if ls != ts:
                    dk = int(sub["daily_quota"]) * SEARCH_PRICE_KOPEKS
                    await db_conn.execute('UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?', (dk, key))
                    await db_conn.execute('UPDATE subscriptions SET last_credited_date = ? WHERE key = ?', (ts, key))
                    row = await _get_key_row(key)
            else:
                await db_conn.execute('UPDATE subscriptions SET active = 0 WHERE key = ?', (key,))
    except Exception:
        pass
    if row["balance_kopeks"] < SEARCH_PRICE_KOPEKS:
        return _json_error("insufficient_balance", 402)
    target = query
    try:
        data = await collect_general_data(target, "tg_id")
    except Exception as e:
        logger.exception("search error")
        return _json_error(f"search_error: {e}", 500)
    if not isinstance(data, dict):
        data = {}

    try:
        bd = await query_funstat_bot(str(target))
        if bd:
            if bd.get("usernames"):
                rows = [{"Username": u, "Дата": None} for u in bd["usernames"]]
                data.setdefault("blocks", []).append(("Funstat Bot · Username", rows))
                data["records_count"] = data.get("records_count", 0) + len(rows)
                if "Funstat Bot · Username" not in data.get("sources", []):
                    data.setdefault("sources", []).append("Funstat Bot · Username")
            if bd.get("names"):
                rows = [{"Имя": n.get("name"), "Дата": n.get("date")} for n in bd["names"]]
                data.setdefault("blocks", []).append(("Funstat Bot · Имена", rows))
                data["records_count"] = data.get("records_count", 0) + len(rows)
                if "Funstat Bot · Имена" not in data.get("sources", []):
                    data.setdefault("sources", []).append("Funstat Bot · Имена")
            if bd.get("stats"):
                data["bot_stats"] = bd["stats"]
            if bd.get("favorite_chat"):
                data["favorite_chat"] = bd["favorite_chat"]
            if bd.get("admin_in_chats"):
                data["admin_in_chats"] = bd["admin_in_chats"]
            if bd.get("channel_title"):
                data["personal_channel"] = {
                    "title": bd.get("channel_title"),
                    "username": bd.get("channel_username"),
                    "link": bd.get("channel_link")}
    except Exception:
        pass

    username = (data.get("stats") or {}).get("username") or None
    if not username:
        bd_cached = _funstat_bot_cache.get(str(target))
        if bd_cached and bd_cached[1].get("usernames"):
            username = bd_cached[1]["usernames"][0]
            data.setdefault("stats", {})["username"] = username

    try:
        fi = await get_full_user_info(str(target), username)
        if fi:
            if fi.get("bio"):
                data.setdefault("blocks", []).append(("Telegram · Bio", [{"Bio": fi["bio"]}]))
                data["records_count"] = data.get("records_count", 0) + 1
                if "Telegram · Bio" not in data.get("sources", []):
                    data.setdefault("sources", []).append("Telegram · Bio")
            if fi.get("personal_channel_link") or fi.get("personal_channel_title"):
                ch_row = {"Название": fi.get("personal_channel_title"),
                          "Username": fi.get("personal_channel_username"),
                          "Ссылка": fi.get("personal_channel_link")}
                ch_row = {k: v for k, v in ch_row.items() if v}
                if ch_row:
                    data.setdefault("blocks", []).append(("Telegram · Личный канал", [ch_row]))
                    data["records_count"] = data.get("records_count", 0) + 1
                    if "Telegram · Личный канал" not in data.get("sources", []):
                        data.setdefault("sources", []).append("Telegram · Личный канал")
            data["birthday"] = fi.get("birthday")
            data["premium"] = fi.get("premium", False)
            data["verified"] = fi.get("verified", False)
            data["common_chats_count"] = fi.get("common_chats_count")
    except Exception:
        pass

    gift_ids = []
    try:
        gd = await fetch_gifts_data(str(target), username)
        if gd and not gd.get("error"):
            for g in (gd.get("profile_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in gift_ids:
                    gift_ids.append(fid)
            for g in (gd.get("saved_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in gift_ids:
                    gift_ids.append(fid)
    except Exception:
        pass

    bot_sent = []; bot_received = []
    if not gift_ids:
        try:
            gb = await query_gifts_via_button(str(target))
            if gb:
                bot_sent = gb.get("sent", [])
                bot_received = gb.get("received", [])
        except Exception:
            pass

    try:
        wd = await query_words_via_button(str(target))
        if wd and wd.get("categories"):
            data["interests_from_words"] = wd["categories"]
    except Exception:
        pass

    try:
        ct = None; bv = None; nh = []; uh = []
        for sn, rows in data.get("blocks", []):
            if sn == "Telegram · Личный канал" and rows:
                ct = rows[0].get("Название")
            elif sn == "Telegram · Bio" and rows:
                bv = rows[0].get("Bio")
            elif sn == "Funstat Bot · Имена":
                for r in rows:
                    if r.get("Имя"):
                        nh.append({"name": r["Имя"], "date": r.get("Дата")})
            elif sn == "Funstat Bot · Username":
                for r in rows:
                    if r.get("Username"):
                        uh.append({"username": r["Username"]})
        interests = detect_interests(bio=bv, favorite_chat=data.get("favorite_chat"),
                                      name_history=nh, username_history=uh, channel_title=ct)
        if data.get("interests_from_words"):
            existing = {i["category"] for i in interests} if interests else set()
            for cat in data["interests_from_words"]:
                if cat not in existing:
                    interests.append({"category": cat, "confidence": 0.7, "source": "words"})
        data["interests"] = interests
        if interests:
            rows = [{"Категория": i["category"], "Точность": f"{int(i['confidence']*100)}%",
                     "Источник": i["source"]} for i in interests]
            has = any(sn == "Интересы" for sn, _ in data.get("blocks", []))
            if not has:
                data.setdefault("blocks", []).append(("Интересы", rows))
                data["records_count"] = data.get("records_count", 0) + len(rows)
                if "Интересы" not in data.get("sources", []):
                    data.setdefault("sources", []).append("Интересы")
    except Exception:
        data["interests"] = []

    try:
        bt = build_funstat_preview(data, target)
    except Exception:
        bt = ""

    parts = []
    if bt and "не найдено" not in bt.lower():
        parts.append(bt)
    full_text = "\n\n".join(parts) if parts else "По этому Telegram ничего не найдено."

    all_public = []
    for src, rows in data.get("blocks", []) or []:
        pub_rows = [{k: v for k, v in r.items() if v not in (None, "", [], {})} for r in rows]
        pub_rows = [r for r in pub_rows if r]
        if pub_rows:
            all_public.append({"source": src, "rows": pub_rows})
    if gift_ids:
        all_public.append({"source": "Telegram · Подарки",
                           "rows": [{"ID": gid, "Ссылка": f"tg://user?id={gid}"} for gid in gift_ids]})
    elif bot_sent or bot_received:
        rows = []
        if bot_sent:
            rows.append({"⬆ Отправленные": ", ".join(f"#{x}" for x in bot_sent)})
        if bot_received:
            rows.append({"⬇ Полученные": ", ".join(f"#{x}" for x in bot_received)})
        all_public.append({"source": "Telegram · Подарки", "rows": rows})

    has_data = (data.get("records_count", 0) > 0) or gift_ids or bot_sent or bot_received
    ts = date.today().isoformat()
    rt = await db_conn.fetchone('SELECT today_searches, today_date FROM site_keys WHERE key = ?', (key,))
    tc = 0
    if rt:
        try:
            if rt["today_date"] and str(rt["today_date"])[:10] == ts:
                tc = int(rt["today_searches"] or 0)
        except Exception:
            pass
    charged = 0
    if has_data:
        tc += 1
        await db_conn.execute(
            'UPDATE site_keys SET balance_kopeks = balance_kopeks - ?, '
            'total_searches = total_searches + 1, today_searches = ?, today_date = ? WHERE key = ?',
            (SEARCH_PRICE_KOPEKS, tc, ts, key))
        charged = SEARCH_PRICE_KOPEKS
    nr = await db_conn.fetchone('SELECT balance_kopeks, total_searches FROM site_keys WHERE key = ?', (key,))

    return _json({
        "ok": True,
        "query": data.get("query"),
        "type": data.get("type"),
        "sources": data.get("sources", []),
        "records_count": data.get("records_count", 0),
        "blocks": all_public,
        "text": full_text,
        "gifts_count": len(gift_ids) or (len(bot_sent) + len(bot_received)),
        "charged_kopeks": charged,
        "balance_kopeks": nr['balance_kopeks'] if nr else row['balance_kopeks'] - charged,
        "total_searches": nr['total_searches'] if nr else row['total_searches'] + (1 if has_data else 0),
        "today_searches": tc,
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
    st = (body.get("type") or "tg_id").strip()
    if not key:
        return _json_error("empty_key")
    if not isinstance(queries, list) or not queries:
        return _json_error("empty_queries")
    if len(queries) > 50:
        return _json_error("too_many_queries_max_50", 400)
    if st != "tg_id":
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
    if row["balance_kopeks"] < SEARCH_PRICE_KOPEKS:
        return _json_error("insufficient_balance", 402)
    results = []
    found = 0
    for q in clean:
        try:
            d = await collect_general_data(q, "tg_id")
            bt = build_funstat_preview(d, q)
            un = (d.get("stats") or {}).get("username") or None
            gd = await fetch_gifts_data(q, un)
            gids = []
            if gd and not gd.get("error"):
                for g in (gd.get("profile_gifts") or []):
                    fid = tg_from_id_str(getattr(g, "from_id", None))
                    if fid and fid not in gids: gids.append(fid)
                for g in (gd.get("saved_gifts") or []):
                    fid = tg_from_id_str(getattr(g, "from_id", None))
                    if fid and fid not in gids: gids.append(fid)
            hd = (d.get("records_count", 0) > 0) or (len(gids) > 0)
            if hd: found += 1
            results.append({"query": q, "ok": True, "text": bt,
                            "records_count": d.get("records_count", 0),
                            "gifts_count": len(gids),
                            "sources": d.get("sources", []), "charged": hd})
        except Exception as e:
            results.append({"query": q, "ok": False, "error": str(e), "charged": False})
    charged = found * SEARCH_PRICE_KOPEKS
    ts = date.today().isoformat()
    rt = await db_conn.fetchone('SELECT today_searches, today_date FROM site_keys WHERE key = ?', (key,))
    tc = 0
    if rt:
        try:
            if rt["today_date"] and str(rt["today_date"])[:10] == ts:
                tc = int(rt["today_searches"] or 0)
        except Exception:
            pass
    if found > 0:
        tc += found
        await db_conn.execute(
            'UPDATE site_keys SET balance_kopeks = balance_kopeks - ?, total_searches = total_searches + ?, '
            'today_searches = ?, today_date = ? WHERE key = ?', (charged, found, tc, ts, key))
    nr = await db_conn.fetchone('SELECT balance_kopeks, total_searches FROM site_keys WHERE key = ?', (key,))
    return _json({"ok": True, "count": len(clean), "found": found, "results": results,
                  "charged_kopeks": charged,
                  "balance_kopeks": nr['balance_kopeks'] if nr else row['balance_kopeks'] - charged,
                  "total_searches": nr['total_searches'] if nr else row['total_searches'] + found,
                  "today_searches": tc})


async def _run_task(task_id, key, query):
    try:
        await db_conn.execute("UPDATE tasks SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = ?", (task_id,))
        d = await collect_general_data(query, "tg_id")
        bt = build_funstat_preview(d, query)
        un = (d.get("stats") or {}).get("username") or None
        gd = await fetch_gifts_data(query, un)
        gids = []
        if gd and not gd.get("error"):
            for g in (gd.get("profile_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in gids: gids.append(fid)
            for g in (gd.get("saved_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in gids: gids.append(fid)
        hd = (d.get("records_count", 0) > 0) or (len(gids) > 0)
        if not hd:
            await db_conn.execute("UPDATE site_keys SET balance_kopeks = balance_kopeks + ?, "
                                  "total_searches = total_searches - 1 WHERE key = ? AND total_searches > 0",
                                  (SEARCH_PRICE_KOPEKS, key))
        res = {"query": query, "text": bt, "records_count": d.get("records_count", 0),
               "gifts_count": len(gids), "sources": d.get("sources", []), "charged": hd}
        await db_conn.execute("UPDATE tasks SET status = 'done', result = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                              (json.dumps(res, ensure_ascii=False), task_id))
    except Exception:
        try:
            await db_conn.execute("UPDATE site_keys SET balance_kopeks = balance_kopeks + ?, "
                                  "total_searches = total_searches - 1 WHERE key = ? AND total_searches > 0",
                                  (SEARCH_PRICE_KOPEKS, key))
        except Exception:
            pass


async def api_task_create_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    try:
        body = await request.json()
    except Exception:
        return _json_error("invalid_json")
    key = (body.get("key") or "").strip()
    q = (body.get("query") or "").strip().lstrip("@")
    if not key or not q.isdigit():
        return _json_error("invalid")
    row = await _get_key_row(key)
    if not row:
        return _json_error("key_not_found", 404)
    if row["balance_kopeks"] < SEARCH_PRICE_KOPEKS:
        return _json_error("insufficient_balance", 402)
    tid = secrets.token_urlsafe(12)
    await db_conn.execute("INSERT INTO tasks (id, key, query, status) VALUES (?, ?, ?, 'pending')", (tid, key, q))
    await db_conn.execute("UPDATE site_keys SET balance_kopeks = balance_kopeks - ?, "
                          "total_searches = total_searches + 1 WHERE key = ?", (SEARCH_PRICE_KOPEKS, key))
    asyncio.create_task(_run_task(tid, key, q))
    return _json({"ok": True, "task_id": tid, "status": "pending", "query": q,
                  "check_url": f"/status?task={tid}"})


async def api_task_status_handler(request):
    if request.method == "OPTIONS":
        return web.Response()
    tid = (request.query.get("id") or request.query.get("task_id") or "").strip()
    if not tid:
        return _json_error("empty_id")
    row = await db_conn.fetchone("SELECT id, query, status, result, error, created_at, updated_at "
                                  "FROM tasks WHERE id = ?", (tid,))
    if not row:
        return _json_error("task_not_found", 404)
    res = None
    if row["result"]:
        try:
            res = json.loads(row["result"])
        except Exception:
            res = {"raw": row["result"]}
    return _json({"ok": True, "task_id": row["id"], "query": row["query"], "status": row["status"],
                  "result": res, "error": row["error"],
                  "created_at": str(row["created_at"]) if row["created_at"] else None,
                  "updated_at": str(row["updated_at"]) if row["updated_at"] else None})


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
    b = rl.setdefault(ip, [])
    b[:] = [t for t in b if now - t < 3600]
    if len(b) >= 10:
        return _json({"ok": False, "error": "rate_limit"}, status=429)
    b.append(now)

    try:
        fs = await funstat_search(q, "tg_id")
    except Exception:
        return _json({"ok": False, "error": "funstat_error"}, status=502)
    if not fs or not fs.get("tg_id"):
        return _json({"ok": False, "error": "not_found"}, status=404)
    uid = fs["tg_id"]
    stats = fs.get("stats") or {}
    profile = {"tg_id": uid, "username": stats.get("username") or None,
               "name": " ".join(p for p in [stats.get("first_name") or "", stats.get("last_name") or ""] if p).strip() or None,
               "is_bot": stats.get("is_bot"), "is_active": stats.get("is_active"),
               "registered_at": (str(stats.get("first_msg_date") or "").split("T")[0] or None),
               "last_message_at": (str(stats.get("last_msg_date") or "").split("T")[0] or None),
               "messages_count": stats.get("total_msg_count"), "groups_count": stats.get("total_groups"),
               "username_history": [], "name_history": [], "gifts": [], "gifts_count": 0,
               "gifts_sent": [], "gifts_received": [], "gifts_total": 0,
               "favorite_chat": None, "admin_in_chats": 0, "bot_stats": {}, "bio": None,
               "personal_channel": None, "birthday": None, "premium": False, "verified": False,
               "common_chats_count": None, "interests": []}

    for u in (fs.get("usernames_history") or [])[:30]:
        if isinstance(u, dict):
            uname = u.get("username") or u.get("name") or u.get("value")
            d = u.get("date") or u.get("first_seen") or u.get("last_seen")
            if uname:
                profile["username_history"].append({
                    "username": uname if str(uname).startswith("@") else f"@{uname}",
                    "date": str(d).split("T")[0] if d else None})

    try:
        bd = await query_funstat_bot(str(uid))
        if bd:
            existing = {h["username"].lower() for h in profile["username_history"]}
            for u in bd.get("usernames", []):
                if u.lower() not in existing:
                    profile["username_history"].append({"username": u, "date": None})
                    existing.add(u.lower())
            if not profile.get("username") and bd.get("usernames"):
                profile["username"] = bd["usernames"][0]
                logger.info(f"profile.username из бота: {bd['usernames'][0]}")
            if bd.get("names"):
                profile["name_history"] = bd["names"]
            if bd.get("stats"):
                profile["bot_stats"] = bd["stats"]
            if bd.get("favorite_chat"):
                profile["favorite_chat"] = bd["favorite_chat"]
            if bd.get("admin_in_chats"):
                profile["admin_in_chats"] = bd["admin_in_chats"]
            if bd.get("channel_title"):
                profile["personal_channel"] = {
                    "title": bd.get("channel_title"),
                    "username": bd.get("channel_username"),
                    "link": bd.get("channel_link")}
    except Exception:
        pass

    username = profile.get("username") or stats.get("username") or None
    try:
        fi = await get_full_user_info(str(uid), username)
        if fi:
            profile["bio"] = fi.get("bio")
            if fi.get("personal_channel_id") or fi.get("personal_channel_link"):
                ex = profile.get("personal_channel") or {}
                profile["personal_channel"] = {
                    "id": fi.get("personal_channel_id") or ex.get("id"),
                    "title": fi.get("personal_channel_title") or ex.get("title"),
                    "username": fi.get("personal_channel_username") or ex.get("username"),
                    "link": fi.get("personal_channel_link") or ex.get("link")}
            profile["birthday"] = fi.get("birthday")
            profile["premium"] = fi.get("premium", False)
            profile["verified"] = fi.get("verified", False)
            profile["common_chats_count"] = fi.get("common_chats_count")
    except Exception:
        pass

    gift_ids = []
    try:
        gd = await fetch_gifts_data(str(uid), username)
        if gd and not gd.get("error"):
            for g in (gd.get("profile_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in gift_ids: gift_ids.append(fid)
            for g in (gd.get("saved_gifts") or []):
                fid = tg_from_id_str(getattr(g, "from_id", None))
                if fid and fid not in gift_ids: gift_ids.append(fid)
    except Exception:
        pass

    bot_sent = []; bot_received = []
    if not gift_ids:
        try:
            gb = await query_gifts_via_button(str(uid))
            if gb:
                bot_sent = gb.get("sent", [])
                bot_received = gb.get("received", [])
                profile["gifts_total"] = gb.get("total", 0)
        except Exception:
            pass

    if gift_ids:
        profile["gifts"] = [{"id": i, "link": f"tg://user?id={i}"} for i in gift_ids]
        profile["gifts_count"] = len(gift_ids)
    elif bot_sent or bot_received:
        profile["gifts_sent"] = [{"id": i, "link": f"tg://user?id={i}"} for i in bot_sent]
        profile["gifts_received"] = [{"id": i, "link": f"tg://user?id={i}"} for i in bot_received]
        profile["gifts_count"] = len(bot_sent) + len(bot_received)

    try:
        wd = await query_words_via_button(str(uid))
        if wd and wd.get("categories"):
            profile["interests_from_words"] = wd["categories"]
    except Exception:
        pass

    try:
        ct = None
        if profile.get("personal_channel"):
            ct = profile["personal_channel"].get("title")
        interests = detect_interests(bio=profile.get("bio"), favorite_chat=profile.get("favorite_chat"),
                                      name_history=profile.get("name_history"),
                                      username_history=profile.get("username_history"),
                                      channel_title=ct)
        if profile.get("interests_from_words"):
            existing = {i["category"] for i in interests} if interests else set()
            for cat in profile["interests_from_words"]:
                if cat not in existing:
                    interests.append({"category": cat, "confidence": 0.7, "source": "words"})
        profile["interests"] = interests
    except Exception:
        pass

    return _json({"ok": True, "query": q, "profile": profile})


# ============ START ============

async def start_tg_client():
    global tg_client, _funstat_bot_lock, _funstat_bot_blocked_until, _last_bot_request_ts
    _funstat_bot_cache.clear()
    _words_cache.clear()
    _funstat_bot_blocked_until = 0.0
    _last_bot_request_ts = 0.0
    logger.info("🧹 caches cleared")
    try:
        _funstat_bot_lock = asyncio.Semaphore(1)
        if TG_SESSION_STR:
            logger.info("🔐 Юзер-сессия")
            session = StringSession(TG_SESSION_STR)
            tg_client = TelegramClient(session, TG_API_ID, TG_API_HASH)
            await tg_client.start()
            me = await tg_client.get_me()
            logger.info(f"✅ Telethon (user): @{me.username if me.username else me.id}")
        else:
            tg_client = TelegramClient(TG_SESSION_NAME, TG_API_ID, TG_API_HASH)
            await tg_client.start(bot_token=BOT_TOKEN)
            me = await tg_client.get_me()
            logger.info(f"✅ Telethon (bot): @{me.username if me.username else me.id}")
        logger.info(f"GetUserStarGiftsRequest: {hasattr(tg_functions.payments, 'GetUserStarGiftsRequest')}")
        logger.info(f"GetSavedStarGiftsRequest: {hasattr(tg_functions.payments, 'GetSavedStarGiftsRequest')}")
    except Exception as e:
        logger.error(f"❌ Telethon: {e}")
        tg_client = None


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
