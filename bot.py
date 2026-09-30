import os
import re
import json
import copy
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

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl import functions as tg_functions
from telethon.tl.types import InputPeerUser
from telethon.tl.functions.users import GetFullUserRequest

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
tg_bot_client = None


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
_FUNSTAT_BOTS = ["@evenfa_bot"]
_funstat_bot_lock = None
_last_bot_request_ts = 0.0
_BOT_MIN_INTERVAL = 2.0

_words_cache = {}
_WORDS_CACHE_TTL = 1800

_netlog_cache = {}
_USERS_CACHE = {}
_USERS_CACHE_TTL = 1800

_id_to_username_cache = {}

_LOADING_MARKERS = ("looking for data", "ищем", "поиск", "⏳", "loading",
                    "загрузка", "ожидайте", "wait", "found")


def _is_loading(t):
    if not t:
        return True
    low = _normalize_bot_text(t).lower()
    return any(m in low for m in _LOADING_MARKERS)


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
    out = {"tg_id": tid, "stats": {}, "groups_count": None, "usernames_history": []}
    stats = await _funstat_get(f"/api/v1/users/{tid}/stats_min")
    if stats.get("id"):
        out["stats"] = stats
    gc = await _funstat_get(f"/api/v1/users/{tid}/groups_count")
    if isinstance(gc, (int, float)):
        out["groups_count"] = int(gc)
    unames = await _funstat_get(f"/api/v1/users/{tid}/usernames")
    if unames.get("success") and unames.get("data"):
        out["usernames_history"] = unames["data"]
    return out


def parse_funstat(data):
    if not data:
        return []
    parsed = []
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


# ============ TELETHON HELPERS ============

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
            return await tg_client.get_entity(InputPeerUser(uid, 0))
        except Exception:
            raise ValueError("Не удалось получить профиль")


async def _resolve_id_to_username(uid):
    """числовой ID → @username через MTProto. Кэш в памяти."""
    if not uid or tg_client is None:
        return None
    uid = str(uid).strip()
    if not uid.isdigit():
        return None
    if uid in _id_to_username_cache:
        return _id_to_username_cache[uid]
    try:
        i = int(uid)
        try:
            ent = await asyncio.wait_for(tg_client.get_entity(i), timeout=8.0)
        except Exception:
            ent = await asyncio.wait_for(tg_client.get_entity(InputPeerUser(i, 0)), timeout=8.0)
        uname = getattr(ent, "username", None)
        if uname:
            val = "@" + uname
            _id_to_username_cache[uid] = val
            return val
    except Exception:
        pass
    _id_to_username_cache[uid] = None
    return None


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
        bd_ = getattr(full, "birthday", None)
        if bd_:
            d = getattr(bd_, "day", None); mo = getattr(bd_, "month", None); y = getattr(bd_, "year", None)
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


# ============ BOT OBFUSCATION ============

_BOT_OBF_MAP = {
    'α':'a','β':'b','γ':'y','δ':'d','ε':'e','ζ':'z','η':'n','θ':'th',
    'ι':'i','κ':'k','λ':'l','μ':'m','ν':'v','ξ':'x','ο':'o','π':'p',
    'ρ':'p','σ':'s','ς':'s','τ':'t','υ':'u','φ':'f','χ':'x','ψ':'ps','ω':'o',
    'Α':'A','Β':'B','Γ':'G','Δ':'D','Ε':'E','Ζ':'Z','Η':'H','Θ':'Th',
    'Ι':'I','Κ':'K','Λ':'L','Μ':'M','Ν':'N','Ξ':'X','Ο':'O','Π':'P',
    'Ρ':'P','Σ':'S','Τ':'T','Υ':'Y','Φ':'F','Χ':'X','Ψ':'Ps','Ω':'O',
    'ϲ':'c','Ϲ':'C',
    'ᴀ':'a','ᴃ':'b','ᴄ':'c','ᴅ':'d','ᴇ':'e','ꜰ':'f','ɢ':'g','ʜ':'h','ɪ':'i',
    'ᴊ':'j','ᴋ':'k','ʟ':'l','ᴍ':'m','ɴ':'n','ᴏ':'o','ᴘ':'p','ʀ':'r','ꜱ':'s',
    'ᴛ':'t','ᴜ':'u','ᴠ':'v','ᴡ':'w','ʏ':'y','ᴢ':'z',
    'ᴎ':'n','ᴓ':'o','ᴦ':'r','ᴧ':'L','ᴨ':'n','ց':'g',
    'Ⅰ':'I','Ⅱ':'II','Ⅲ':'III','Ⅳ':'IV','Ⅴ':'V','Ⅵ':'VI',
    'Ⅶ':'VII','Ⅷ':'VIII','Ⅸ':'IX','Ⅹ':'X',
    'Ⅼ':'L','Ⅽ':'C','Ⅾ':'D','Ⅿ':'M',
    'ⅰ':'i','ⅱ':'ii','ⅲ':'iii','ⅳ':'iv','ⅴ':'v','ⅵ':'vi',
    'ⅶ':'vii','ⅷ':'viii','ⅸ':'ix','ⅹ':'x',
    'ⅼ':'l','ⅽ':'c','ⅾ':'d','ⅿ':'m',
    '℮':'e','ƒ':'f','ɡ':'g',
    'ł':'l','ѕ':'s','ħ':'h',
    'ѵ':'v','Ѵ':'V',
    'Ａ':'A','Ｂ':'B','Ｃ':'C','Ｄ':'D','Ｅ':'E','Ｆ':'F','Ｇ':'G','Ｈ':'H',
    'Ｉ':'I','Ｊ':'J','Ｋ':'K','Ｌ':'L','Ｍ':'M','Ｎ':'N','Ｏ':'O','Ｐ':'P',
    'Ｑ':'Q','Ｒ':'R','Ｓ':'S','Ｔ':'T','Ｕ':'U','Ｖ':'V','Ｗ':'W','Ｘ':'X',
    'Ｙ':'Y','Ｚ':'Z',
    'ａ':'a','ｂ':'b','ｃ':'c','ｄ':'d','ｅ':'e','ｆ':'f','ｇ':'g','ｈ':'h',
    'ｉ':'i','ｊ':'j','ｋ':'k','ｌ':'l','ｍ':'m','ｎ':'n','ｏ':'o','ｐ':'p',
    'ｑ':'q','ｒ':'r','ｓ':'s','ｔ':'t','ｕ':'u','ｖ':'v','ｗ':'w','ｘ':'x',
    'ｙ':'y','ｚ':'z',
    'İ':'I','ı':'i',
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'z','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'x','ц':'c','ч':'c','ш':'s','щ':'s',
    'ъ':'','ы':'y','ь':'','э':'e','ю':'u','я':'y',
    'А':'A','Б':'B','В':'V','Г':'G','Д':'D','Е':'E','Ё':'E','Ж':'Z','З':'Z',
    'И':'I','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O','П':'P','Р':'R',
    'С':'S','Т':'T','У':'U','Ф':'F','Х':'X','Ц':'C','Ч':'C','Ш':'S','Щ':'S',
    'Ъ':'','Ы':'Y','Ь':'','Э':'E','Ю':'U','Я':'Y',
    'қ':'k','Қ':'K',
    'і':'i','І':'I','ї':'i','Ї':'I','є':'e','Є':'E','ґ':'g','Ґ':'G',
    'Ѕ':'S','ј':'j','Ј':'J','ԁ':'d',
    'ᅠ':'',
}
_BOT_TRANS = str.maketrans(_BOT_OBF_MAP)


def _normalize_bot_text(s):
    return str(s or "").translate(_BOT_TRANS)


_NAME_HOMOGLYPH_MAP = {
    'α':'a','β':'b','γ':'y','δ':'d','ε':'e','ζ':'z','η':'n','θ':'th',
    'ι':'i','κ':'k','λ':'l','μ':'m','ν':'v','ξ':'x','ο':'o','π':'p',
    'ρ':'p','σ':'s','ς':'s','τ':'t','υ':'u','φ':'f','χ':'x','ψ':'ps','ω':'o',
    'Α':'A','Β':'B','Γ':'G','Δ':'D','Ε':'E','Ζ':'Z','Η':'H','Θ':'Th',
    'Ι':'I','Κ':'K','Λ':'L','Μ':'M','Ν':'N','Ξ':'X','Ο':'O','Π':'P',
    'Ρ':'P','Σ':'S','Τ':'T','Υ':'Y','Φ':'F','Χ':'X','Ψ':'Ps','Ω':'O',
    'ϲ':'c','Ϲ':'C',
    'ᴀ':'a','ᴃ':'b','ᴄ':'c','ᴅ':'d','ᴇ':'e','ꜰ':'f','ɢ':'g','ʜ':'h','ɪ':'i',
    'ᴊ':'j','ᴋ':'k','ʟ':'l','ᴍ':'m','ɴ':'n','ᴏ':'o','ᴘ':'p','ʀ':'r','ꜱ':'s',
    'ᴛ':'t','ᴜ':'u','ᴠ':'v','ᴡ':'w','ʏ':'y','ᴢ':'z',
    'ᴎ':'n','ᴓ':'o','ᴦ':'r','ᴧ':'L','ᴨ':'n',
    'Ⅼ':'L','Ⅽ':'C','Ⅾ':'D','Ⅿ':'M',
    'ⅰ':'i','ⅼ':'l','ⅽ':'c','ⅾ':'d','ⅿ':'m',
    '℮':'e','ƒ':'f','ɡ':'g','ł':'l','ѕ':'s','ѵ':'v',
    'ᅠ':'',
}
_NAME_TRANS = str.maketrans(_NAME_HOMOGLYPH_MAP)


def _clean_name(name):
    if not name:
        return ""
    s = str(name).strip()
    s = s.translate(_NAME_TRANS)
    s = re.sub(r'\s+', ' ', s)
    s = s.strip(".,;:—-·•")
    return s


_CYR_TO_LAT_MAP = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'x','ц':'ts','ч':'ch','ш':'sh','щ':'shch',
    'ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
    'А':'A','Б':'B','В':'V','Г':'G','Д':'D','Е':'E','Ё':'E','Ж':'Zh','З':'Z',
    'И':'I','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O','П':'P','Р':'R',
    'С':'S','Т':'T','У':'U','Ф':'F','Х':'X','Ц':'Ts','Ч':'Ch','Ш':'Sh','Щ':'Shch',
    'Ъ':'','Ы':'Y','Ь':'','Э':'E','Ю':'Yu','Я':'Ya',
    'қ':'k','Қ':'K',
    'і':'i','І':'I','ї':'i','Ї':'I','є':'e','Є':'E','ґ':'g','Ґ':'G',
    'Ѕ':'S','ј':'j','Ј':'J',
    'ᅠ':'',
}
_CYR_TO_LAT = str.maketrans(_CYR_TO_LAT_MAP)


def _name_to_latin(name):
    if not name:
        return ""
    s = _clean_name(name)
    s = s.translate(_CYR_TO_LAT)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _extract_favorite_chat_ru(raw_text):
    if not raw_text:
        return None
    for raw in raw_text.split("\n"):
        line = raw.strip()
        if not line:
            continue
        if ":" not in line and "：" not in line:
            continue
        sep = ":" if ":" in line else "："
        head, _, tail = line.partition(sep)
        head_norm = _normalize_bot_text(head).lower()
        if any(k in head_norm for k in
               ("favorite group", "favorite chat", "lyubim chat", "lyubimyj chat")):
            value = tail.strip()
            value = re.sub(r'\*\*([^*]+)\*\*', r'\1', value)
            value = re.sub(r'`([^`]+)`', r'\1', value)
            value = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', value)
            return value.strip() or None
    return None


# ============ GIFTS ============

_DIRS = {
    "⇇": "received", "⇐": "received", "⇍": "received", "←": "received", "⬅": "received",
    "⇉": "sent",     "⇒": "sent",     "⇏": "sent",     "→": "sent",     "➡": "sent",
    "↕": "mutual",   "⇅": "mutual",
}


def _parse_gift_lines(text):
    out = []
    if not text:
        return out
    norm = _normalize_bot_text(text)
    lines = norm.split("\n")
    merged = []
    for raw in lines:
        s = raw.strip()
        if not s:
            continue
        if s.startswith("(@") and merged:
            merged[-1] = merged[-1] + " " + s
        else:
            merged.append(s)
    for line in merged:
        m = re.match(
            r'(\d{4}-\d{2}-\d{2})\.?\s+([⇇⇐⇍⇉⇒⇏←→⬅➡↕⇅])\s+(.+?)$',
            line)
        if not m:
            continue
        date = m.group(1)
        arrow = m.group(2)
        rest = m.group(3).strip()
        direction = _DIRS.get(arrow, "received")
        uid = None
        hash_m = re.search(r'start=0102([A-Fa-f0-9]+)01000000', rest)
        if hash_m:
            hex_str = hash_m.group(1)
            try:
                if len(hex_str) >= 8:
                    bytes_le = bytes.fromhex(hex_str[:8])
                    uid = str(int.from_bytes(bytes_le, byteorder='little'))
            except Exception:
                pass
        rest_clean = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', rest).strip()
        uname = None
        um = re.search(r'@([a-zA-Z0-9_]{4,32})', rest_clean)
        if um:
            uname = "@" + um.group(1)
        name = rest_clean
        if um:
            name = rest_clean[:um.start()].strip()
            name = re.sub(r'\(\s*$', '', name).strip()
            name = re.sub(r'\)\s*$', '', name).strip()
        name = name.strip().rstrip("—–-").strip()
        name = name.strip("[]").strip()
        if name:
            name = _name_to_latin(name)
        out.append({"date": date, "dir": direction, "name": name or None,
                    "username": uname, "id": uid})
    return out


def _find_button(msg, keywords):
    if not msg or not msg.reply_markup:
        return None
    for row in msg.reply_markup.rows:
        for btn in row.buttons:
            raw = (btn.text or "")
            norm = _normalize_bot_text(raw).lower()
            norm_clean = re.sub(r'[^\w\s]', '', norm, flags=re.UNICODE).strip()
            for kw in keywords:
                k_norm = _normalize_bot_text(kw).lower()
                k_clean = re.sub(r'[^\w\s]', '', k_norm, flags=re.UNICODE).strip()
                if (k_clean and k_clean in norm_clean) or (k_norm and k_norm in norm):
                    bt = getattr(btn, "type", None)
                    cb = getattr(bt, "data", None) if bt else None
                    if cb is None:
                        cb = getattr(btn, "data", None)
                    if cb:
                        return {"data": cb, "msg_id": msg.id, "label": raw}
    return None


def _find_next_page(msg, exclude_nums=None, min_num=2):
    if not msg or not msg.reply_markup:
        return None
    exclude_nums = exclude_nums or set()
    cands = []
    for row in msg.reply_markup.rows:
        for btn in row.buttons:
            txt = (btn.text or "").strip()
            if "▶▶" in txt or "⏭" in txt or "⏩" in txt:
                continue
            txt_clean = re.sub(r'[^\w]', '', txt, flags=re.UNICODE).strip()
            m = re.fullmatch(r'(\d+)', txt_clean)
            if not m:
                continue
            try:
                n = int(m.group(1))
            except ValueError:
                continue
            if n in exclude_nums or n < min_num:
                continue
            bt = getattr(btn, "type", None)
            cb = getattr(bt, "data", None) if bt else None
            if cb is None:
                cb = getattr(btn, "data", None)
            if cb:
                cands.append({"num": n, "data": cb, "msg_id": msg.id, "label": txt})
    if not cands:
        return None
    cands.sort(key=lambda x: x["num"])
    return cands[0]


def _dedup_key(e):
    if e.get("username"):
        return (e["dir"], e["username"].lower(), e["date"])
    return (e["dir"], e.get("name") or "", e["date"])


async def query_gift_list_full(tg_id):
    logger.info(f"[gift] START tg_id={tg_id}")
    empty = {"sent": [], "received": [], "mutual": [], "total": 0}
    if tg_client is None or not TG_SESSION_STR:
        return empty
    global _funstat_bot_lock, _last_bot_request_ts
    if _funstat_bot_lock is None:
        _funstat_bot_lock = asyncio.Semaphore(1)

    cache_key = f"giftlist:{tg_id}"
    now = time.time()
    cached = _netlog_cache.get(cache_key)
    if cached and now - cached[0] < _WORDS_CACHE_TTL:
        return cached[1]

    delta = now - _last_bot_request_ts
    if delta < _BOT_MIN_INTERVAL:
        await asyncio.sleep(_BOT_MIN_INTERVAL - delta)
    _last_bot_request_ts = time.time()

    try:
        bot = await tg_client.get_entity(_FUNSTAT_BOT_NAME)
    except Exception as e:
        logger.error(f"[gift] cannot get bot: {e!r}")
        return empty

    from telethon.tl.functions.messages import GetBotCallbackAnswerRequest

    async def _wait_edit(msg_id, timeout=25.0):
        t0 = time.time()
        last = None
        while time.time() - t0 < timeout:
            try:
                fresh = await tg_client.get_messages(bot, ids=msg_id)
                if fresh and fresh.text:
                    last = fresh
                    if not _is_loading(fresh.text):
                        return fresh
            except Exception:
                pass
            await asyncio.sleep(1.2)
        return last

    async def _click(cb, timeout=20.0):
        try:
            await tg_client(GetBotCallbackAnswerRequest(
                peer=bot, msg_id=cb["msg_id"], data=cb["data"]))
        except Exception as e:
            logger.error(f"[gift] click err: {e!r}")
            return None
        await asyncio.sleep(1.2)
        return await _wait_edit(cb["msg_id"], timeout=timeout)

    recv_e, sent_e, mut_e = [], [], []
    all_e = []
    total = 0

    async with _funstat_bot_lock:
        try:
            async with tg_client.conversation(bot, timeout=API_TIMEOUTS["funstat_bot"]) as conv:
                await conv.send_message(tg_id)
                first_msg = await conv.get_response()
            if not first_msg or not first_msg.text:
                return empty
            base_msg_id = first_msg.id
            if _is_loading(first_msg.text):
                edited = await _wait_edit(base_msg_id, timeout=30.0)
                if edited and edited.text:
                    first_msg = edited

            gift_btn = _find_button(first_msg, ["🎁", "gift", "подарки"])
            if not gift_btn:
                logger.info("[gift] 🎁 not found")
                return empty
            logger.info(f"[gift] click '{gift_btn['label']}'")
            gift_msg = await _click(gift_btn, timeout=25.0)
            if not gift_msg or not gift_msg.text:
                logger.info("[gift] gift_msg empty")
                return empty

            tm = re.search(r'Всего\s+(\d+)', _normalize_bot_text(gift_msg.text))
            if tm:
                try:
                    total = int(tm.group(1))
                except ValueError:
                    pass

            async def _collect_all_pages(start_msg, category_keywords=None):
                entries = []
                cur = start_msg
                if category_keywords:
                    cat_btn = _find_button(start_msg, category_keywords)
                    if not cat_btn:
                        logger.info(f"[gift] category not found: {category_keywords}")
                        return entries, start_msg
                    logger.info(f"[gift] click category '{cat_btn['label']}'")
                    page = await _click(cat_btn, timeout=20.0)
                    if not page or not page.text:
                        logger.info("[gift] page empty after category click")
                        return entries, start_msg
                    cur = page
                e_first = _parse_gift_lines(cur.text)
                logger.info(f"[gift] page#1: {len(e_first)} entries")
                entries.extend(e_first)
                visited_pages = set()
                for _ in range(15):
                    nxt = _find_next_page(cur, exclude_nums=visited_pages)
                    if not nxt:
                        await asyncio.sleep(2.0)
                        fresh = await tg_client.get_messages(bot, ids=base_msg_id)
                        if fresh and fresh.text:
                            cur = fresh
                            nxt = _find_next_page(cur, exclude_nums=visited_pages)
                            if not nxt:
                                break
                        else:
                            break
                    visited_pages.add(nxt["num"])
                    logger.info(f"[gift] next page '{nxt['label']}'")
                    fresh = await _click(nxt, timeout=20.0)
                    if not fresh or not fresh.text:
                        break
                    new_e = _parse_gift_lines(fresh.text)
                    logger.info(f"[gift] page: {len(new_e)} entries")
                    if not new_e:
                        break
                    entries.extend(new_e)
                    cur = fresh
                    await asyncio.sleep(1.5)
                if category_keywords:
                    back_btn = _find_button(cur, ["назад", "back", "◀", "⬅"])
                    if back_btn:
                        logger.info(f"[gift] back '{back_btn['label']}'")
                        refreshed = await _click(back_btn, timeout=20.0)
                        if refreshed and refreshed.text:
                            return entries, refreshed
                return entries, cur

            recv_e, gift_msg = await _collect_all_pages(gift_msg, ["получен", "rcv", "rсv", "received"])
            await asyncio.sleep(2)

            if not gift_msg or not _find_button(gift_msg, ["подарен", "sen", "sent"]):
                logger.info("[gift] no sent button, fallback to All")
                all_e, _ = await _collect_all_pages(gift_msg) if gift_msg else ([], None)
            else:
                sent_e, gift_msg = await _collect_all_pages(gift_msg, ["подарен", "sen", "sent"])
                await asyncio.sleep(2)
                if gift_msg and _find_button(gift_msg, ["взаимно", "bidirect", "mutual"]):
                    mut_e, gift_msg = await _collect_all_pages(gift_msg, ["взаимно", "bidirect", "mutual"])

        except Exception as e:
            logger.error(f"[gift] err: {e!r}")

    seen = set()
    sent, received, mutual = [], [], []
    for e in list(recv_e) + list(sent_e) + list(mut_e) + list(all_e):
        k = _dedup_key(e)
        if k in seen:
            continue
        seen.add(k)
        if e["dir"] == "sent":
            sent.append(e)
        elif e["dir"] == "mutual":
            mutual.append(e)
        else:
            received.append(e)

    # Если у записи нет username, но есть id — резолвим id → @username
    for e in sent + received + mutual:
        if not e.get("username") and e.get("id"):
            uname = await _resolve_id_to_username(e["id"])
            if uname:
                e["username"] = uname
            await asyncio.sleep(0.2)

    result = {"sent": sent, "received": received, "mutual": mutual,
              "total": total or (len(sent) + len(received) + len(mutual))}
    logger.info(f"[gift] DONE sent={len(sent)} recv={len(received)} mut={len(mutual)} total={total}")
    _netlog_cache[cache_key] = (time.time(), result)
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
    "Политика": ["политик","протест","митинг","оппозиция","антивоен","антирос","война","путин","кремл"],
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
    global _funstat_bot_lock, _last_bot_request_ts
    if tg_client is None or not TG_SESSION_STR:
        return {"categories": []}
    if _funstat_bot_lock is None:
        _funstat_bot_lock = asyncio.Semaphore(1)
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
            if _is_loading(first_msg.text):
                for _ in range(20):
                    await asyncio.sleep(1.5)
                    try:
                        fresh = await tg_client.get_messages(bot, ids=first_msg.id)
                        if fresh and fresh.text and not _is_loading(fresh.text):
                            first_msg = fresh
                            break
                    except Exception:
                        pass
            if not first_msg.reply_markup:
                return {"categories": []}
            target_btn = _find_button(first_msg, ["🗣", "частота слов", "частота"])
            if not target_btn:
                return {"categories": []}
            try:
                await tg_client(GetBotCallbackAnswerRequest(peer=bot, msg_id=first_msg.id, data=target_btn["data"]))
            except Exception:
                return {"categories": []}
            words_text = ""
            for i in range(30):
                await asyncio.sleep(1)
                try:
                    fresh = await tg_client.get_messages(bot, ids=first_msg.id)
                    if fresh and fresh.text and not _is_loading(fresh.text) \
                            and re.search(r'[├|└]\s*\*?\*?\d+\s*[-–]\s*\d+', fresh.text):
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
    "Политика": ["политик","протест","митинг","оппозиция","антивоен","антирос","война","путин","кремл"],
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


# ============ BOT PARSER ============

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
    m_id = re.search(r'ID\s*[:：]\s*`?(\d{5,15})`?', ns, re.IGNORECASE)
    if m_id:
        result["tg_id"] = m_id.group(1)
    un_hdr = re.search(r'\bu?usernames?\s*[:：]', ns, re.IGNORECASE)
    if un_hdr:
        body_start = un_hdr.end()
    else:
        body_start = m_id.end() if m_id else 0
    nm_hdr = re.search(
        r'(?<![a-z])(?:first\s*name|last\s*name|names?|imena?|imya|имя|имена)\s*[:：]',
        ns[body_start:], re.IGNORECASE)
    body_end = body_start + nm_hdr.start() if nm_hdr else len(ns)
    block = ns[body_start:body_end]
    for um in re.finditer(r'@([a-zA-Z0-9_]{4,32})', block):
        u = "@" + um.group(1)
        if u not in result["usernames"]:
            result["usernames"].append(u)

    for m in re.finditer(r'(\d{4}-\d{2}-\d{2})\s*[➜→▶►]\s*([^\n]+?)(?:\n|$)', text):
        date = m.group(1)
        name = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', m.group(2)).strip()
        name = re.sub(r'\*\*([^*]+)\*\*', r'\1', name).strip()
        name = re.sub(r'`([^`]+)`', r'\1', name).strip()
        name = name.replace('ᅠ', '').strip()
        name = _name_to_latin(name)
        if name and len(name) < 100:
            result["names"].append({"date": date, "name": name})

    m = re.search(r'(\d+)\s*messages?\s+in\s+(\d+)\s+groups?', ns, re.IGNORECASE)
    if m:
        result["stats"]["total_messages"] = int(m.group(1))
        result["stats"]["total_chats"] = int(m.group(2))
    for key, pat in [
        ("diversity_percent", r'(?:diversity|raznoobraz\w*)\s+([\d,.]+)\s*%'),
        ("replies_percent",   r'([\d,.]+)\s*%\s*(?:replies|replay|replai)'),
        ("media_percent",     r'([\d,.]+)\s*%\s*(?:media|medi[ao])'),
    ]:
        mm = re.search(pat, ns, re.IGNORECASE)
        if mm and key not in result["stats"]:
            try:
                result["stats"][key] = float(mm.group(1).replace(',', '.'))
            except ValueError:
                pass
    m = re.search(r'[KК]р[уγᴜ][жk][кқ][иi]\s*:\s*\*?\*?(\d+)', ns)
    if m:
        try:
            result["stats"]["circles"] = int(m.group(1))
        except ValueError:
            pass
    m = re.search(r'(?:голос|voi[cｃ]e|voise)\s*:\s*\*?\*?(\d+)', ns, re.IGNORECASE)
    if m:
        try:
            result["stats"]["voice"] = int(m.group(1))
        except ValueError:
            pass
    for pat in [r'admin\s+in\s+groups?\s*:?\s*(\d+)', r'админ\s+в\s+чатах?\s*:?\s*(\d+)']:
        m = re.search(pat, ns, re.IGNORECASE)
        if m:
            result["admin_in_chats"] = int(m.group(1))
            break
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
                first = await conv.get_response()
        except Exception:
            return {}
    if not first:
        return {}
    text = first.text or ""
    if _is_loading(text):
        for _ in range(20):
            await asyncio.sleep(1.5)
            try:
                fresh = await tg_client.get_messages(bot, ids=first.id)
                if fresh and fresh.text and not _is_loading(fresh.text):
                    text = fresh.text
                    break
            except Exception:
                pass
    if not text or _is_loading(text):
        return {}
    parsed = parse_funstat_bot_response(text)
    try:
        fav_ru = _extract_favorite_chat_ru(text)
        if fav_ru:
            parsed["favorite_chat"] = fav_ru
    except Exception:
        pass
    return parsed


async def query_funstat_bot(tg_id):
    global _funstat_bot_lock, _last_bot_request_ts
    if tg_client is None or not TG_SESSION_STR:
        return {}
    if _funstat_bot_lock is None:
        _funstat_bot_lock = asyncio.Semaphore(1)
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


# ============ COLLECT ============

async def collect_general_data(query, search_type="tg_id"):
    ck = get_cache_key(search_type, query)
    if ck in cache:
        ct, d = cache[ck]
        if datetime.now() - ct < CACHE_TTL:
            return copy.deepcopy(d)
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
    return copy.deepcopy(result)


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


def _fmt_short_iso(iso):
    if not iso:
        return ""
    try:
        dt = datetime.strptime(str(iso).strip()[:10], "%Y-%m-%d")
        months = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]
        return f"{dt.day:02d} {months[dt.month - 1]} {str(dt.year)[-2:]}"
    except Exception:
        return str(iso)[:10]


def build_funstat_preview(data, query):
    tid = ""
    first_date = ""
    for src, rows in data.get("blocks", []):
        for row in rows:
            if not tid:
                for k in ("Telegram ID", "TG ID"):
                    if row.get(k):
                        tid = str(row[k])
                        break
            if not first_date and row.get("Первое сообщение"):
                first_date = str(row["Первое сообщение"])

    bd = data.get("_bot_data") or {}
    bd_names = bd.get("names") or []
    bd_usernames = bd.get("usernames") or []
    interests = data.get("interests") or []

    lines = []
    lines.append(f"✈️ <b>Telegram · {_esc(tid) if tid else _esc(query)}</b>")
    lines.append("")

    if first_date:
        mon = _format_month_year(first_date)
        ago = _months_ago(first_date)
        suf = f" ({_esc(ago)})" if ago else ""
        lines.append(f"🕐 <b>Регистрация:</b> ~{_esc(mon)}{suf}")
        lines.append("")

    if bd_names or bd_usernames:
        total = max(len(bd_names), len(bd_usernames))
        lines.append(f"🌀 <b>История изменения имени ({total}):</b>")

        for i in range(total):
            n_row = bd_names[i] if i < len(bd_names) else None
            u = bd_usernames[i] if i < len(bd_usernames) else None

            d = (n_row or {}).get("date") or ""
            name = _name_to_latin((n_row or {}).get("name") or "")

            u_part = ""
            if u:
                uc = u.lstrip("@")
                u_part = f'<a href="https://t.me/{_esc(uc)}">{_esc(u)}</a>'

            ds = _fmt_short_iso(d) if d else ""

            if ds and u_part and name:
                lines.append(f"{_esc(ds)} → {u_part}, {_esc(name)}")
            elif ds and u_part:
                lines.append(f"{_esc(ds)} → {u_part}")
            elif ds and name:
                lines.append(f"{_esc(ds)} → {_esc(name)}")
            elif u_part and name:
                lines.append(f"→ {u_part}, {_esc(name)}")
            elif u_part:
                lines.append(f"→ {u_part}")
            elif name:
                lines.append(f"→ {_esc(name)}")

        lines.append("")

    sent_list = data.get("_gift_sent_list") or []
    if sent_list:
        parts = []
        for e in sent_list:
            if e.get("username"):
                parts.append(_esc(e["username"]))
            elif e.get("id"):
                parts.append(f"#{e['id']}")
            elif e.get("name"):
                parts.append(_esc(e["name"]))
        if parts:
            lines.append(f"⬆ <b>Кому отправлял(-а) подарки ({len(parts)}):</b>")
            lines.append(" · ".join(parts))
            lines.append("")

    recv_list = data.get("_gift_received_list") or []
    if recv_list:
        parts = []
        for e in recv_list:
            if e.get("username"):
                parts.append(_esc(e["username"]))
            elif e.get("id"):
                parts.append(f"#{e['id']}")
            elif e.get("name"):
                parts.append(_esc(e["name"]))
        if parts:
            lines.append(f"⬇ <b>От кого получал(-а) подарки ({len(parts)}):</b>")
            lines.append(" · ".join(parts))
            lines.append("")

    cats = [i.get("category") for i in interests if i.get("category")]
    if cats:
        lines.append(f"🧠 <b>Интересы [{len(cats)}]:</b>")
        inner = "\n".join(_esc(c) for c in cats)
        lines.append(f"<blockquote>{inner}</blockquote>")

    if not lines or (len(lines) == 2 and not tid):
        return "По этому Telegram ничего не найдено."
    return "\n".join(lines).strip()


# ============ SEARCH PIPELINE ============

async def run_full_search(target: str):
    try:
        data = await collect_general_data(target, "tg_id")
    except Exception as e:
        logger.exception("collect_general_data error")
        data = {"query": target, "type": "tg_id", "blocks": [], "records_count": 0, "sources": []}
    if not isinstance(data, dict):
        data = {}

    bd = {}
    try:
        bd = await query_funstat_bot(str(target))
        if bd:
            if bd.get("usernames"):
                rows = [{"Username": u, "Дата": None} for u in bd["usernames"]]
                data.setdefault("blocks", []).append(("Funstat Bot · Username", rows))
                data["records_count"] = data.get("records_count", 0) + len(rows)
            if bd.get("names"):
                rows = [{"Имя": n.get("name"), "Дата": n.get("date")} for n in bd["names"]]
                data.setdefault("blocks", []).append(("Funstat Bot · Имена", rows))
                data["records_count"] = data.get("records_count", 0) + len(rows)
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
    except Exception as e:
        logger.error(f"query_funstat_bot err: {e!r}")

    username = (data.get("stats") or {}).get("username") or None
    if not username and bd and bd.get("usernames"):
        username = bd["usernames"][0]

    try:
        fi = await get_full_user_info(str(target), username)
        if fi:
            if fi.get("bio"):
                data.setdefault("blocks", []).append(("Telegram · Bio", [{"Bio": fi["bio"]}]))
                data["records_count"] = data.get("records_count", 0) + 1
            if fi.get("personal_channel_link") or fi.get("personal_channel_title"):
                ch_row = {"Название": fi.get("personal_channel_title"),
                          "Username": fi.get("personal_channel_username"),
                          "Ссылка": fi.get("personal_channel_link")}
                ch_row = {k: v for k, v in ch_row.items() if v}
                if ch_row:
                    data.setdefault("blocks", []).append(("Telegram · Личный канал", [ch_row]))
                    data["records_count"] = data.get("records_count", 0) + 1
            data["birthday"] = fi.get("birthday")
            data["premium"] = fi.get("premium", False)
            data["verified"] = fi.get("verified", False)
            data["common_chats_count"] = fi.get("common_chats_count")
    except Exception:
        pass

    gift_list = {"sent": [], "received": [], "mutual": [], "total": 0}
    try:
        gl = await query_gift_list_full(str(target))
        if gl:
            gift_list = gl
    except Exception as e:
        logger.error(f"gift_list_full err: {e!r}")

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
    except Exception:
        data["interests"] = []

    data["_bot_data"] = bd
    data["_gift_sent_list"] = gift_list.get("sent", [])
    data["_gift_received_list"] = gift_list.get("received", [])
    data["_gift_mutual_list"] = gift_list.get("mutual", [])

    try:
        bt = build_funstat_preview(data, target)
    except Exception:
        bt = ""

    all_public = []
    for src, rows in data.get("blocks", []) or []:
        pub_rows = [{k: v for k, v in r.items() if v not in (None, "", [], {})} for r in rows]
        pub_rows = [r for r in pub_rows if r]
        if pub_rows:
            all_public.append({"rows": pub_rows})

    gifts_sent_out = [{"date": e["date"], "username": e["username"],
                       "name": e["name"], "id": e.get("id")} for e in gift_list.get("sent", [])]
    gifts_received_out = [{"date": e["date"], "username": e["username"],
                           "name": e["name"], "id": e.get("id")} for e in gift_list.get("received", [])]
    gifts_mutual_out = [{"date": e["date"], "username": e["username"],
                         "name": e["name"], "id": e.get("id")} for e in gift_list.get("mutual", [])]

    return {
        "query": data.get("query"),
        "type": data.get("type"),
        "records_count": data.get("records_count", 0),
        "blocks": all_public,
        "text": bt if bt else "По этому Telegram ничего не найдено.",
        "gifts_count": gift_list["total"] or (len(gifts_sent_out) + len(gifts_received_out) + len(gifts_mutual_out)),
        "gifts_sent": gifts_sent_out,
        "gifts_received": gifts_received_out,
        "gifts_mutual": gifts_mutual_out,
        "bot_stats": data.get("bot_stats", {}),
        "favorite_chat": data.get("favorite_chat"),
        "admin_in_chats": data.get("admin_in_chats", 0),
        "birthday": data.get("birthday"),
        "premium": data.get("premium", False),
        "verified": data.get("verified", False),
        "common_chats_count": data.get("common_chats_count"),
        "interests": data.get("interests", []),
    }


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
            'VALUES (?, ?, 0, 0, ?, 1)', (nk, 100, ts))
        return _json({"ok": True, "key": nk, "balance_kopeks": 100,
                      "balance_rub": 1.0, "total_searches": 0, "today_searches": 0})
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
    return _json({"ok": True, "key": key, "balance_kopeks": row["balance_kopeks"],
                  "balance_rub": row["balance_kopeks"]/100, "total_searches": row["total_searches"]})


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
    result = await run_full_search(query)
    result["ok"] = True
    return _json(result)


# ============ BOT HANDLERS ============

def register_bot_handlers():
    if tg_bot_client is None:
        logger.warning("register_bot_handlers: tg_bot_client is None")
        return

    @tg_bot_client.on(events.NewMessage(incoming=True))
    async def on_bot_message(event):
        try:
            text = (event.raw_text or "").strip()
            if not text:
                return
            if text == "/start":
                await event.reply(
                    "👋 Привет!\n\n"
                    "Отправь мне числовой <b>Telegram ID</b> — "
                    "и я соберу по нему досье.",
                    parse_mode="html"
                )
                return
            if not re.fullmatch(r'\d{5,15}', text):
                await event.reply("❌ Отправь числовой Telegram ID (только цифры).")
                return
            logger.info(f"[bot] запрос от {event.sender_id}: {text}")
            if tg_client is None:
                await event.reply("❌ Юзер-сессия не настроена.")
                return
            await event.reply("⏳ Собираю данные, подожди 20-40 сек…")
            result = await run_full_search(text)
            reply_text = result.get("text") or "Ничего не найдено."
            if len(reply_text) > 4000:
                reply_text = reply_text[:3990] + "…"
            await event.reply(reply_text, parse_mode="html", link_preview=False)
        except Exception as e:
            logger.exception(f"[bot] on_bot_message err: {e!r}")
            try:
                await event.reply(f"❌ Ошибка: {e}")
            except Exception:
                pass

    logger.info("[bot] handlers registered on tg_bot_client")


# ============ START ============

async def start_tg_clients():
    global tg_client, tg_bot_client, _funstat_bot_lock, _last_bot_request_ts
    _funstat_bot_cache.clear()
    _words_cache.clear()
    _netlog_cache.clear()
    _USERS_CACHE.clear()
    _id_to_username_cache.clear()
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
            logger.warning("⚠️ TG_SESSION_STR не задан")
            tg_client = None

        logger.info("🤖 Bot-сессия")
        tg_bot_client = TelegramClient("dataseeker_bot", TG_API_ID, TG_API_HASH)
        await tg_bot_client.start(bot_token=BOT_TOKEN)
        me_bot = await tg_bot_client.get_me()
        logger.info(f"✅ Telethon (bot): @{me_bot.username if me_bot.username else me_bot.id}")

        register_bot_handlers()
    except Exception as e:
        logger.error(f"❌ Telethon start error: {e!r}")


async def main():
    await asyncio.sleep(1)
    await init_db()
    asyncio.create_task(start_tg_clients())

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
    app.router.add_route("OPTIONS", "/api/v1/search", api_public_search_handler)
    app.router.add_post("/api/v1/search", api_public_search_handler)
    app.router.add_get("/api/v1/search", api_public_search_handler)

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
        if tg_bot_client:
            try:
                await tg_bot_client.disconnect()
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
