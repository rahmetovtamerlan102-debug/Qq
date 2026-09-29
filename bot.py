import os
import re
import json
import secrets
import time
import asyncio
import aiosqlite
import aiohttp
import hashlib
from aiohttp import web
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import logging

from telethon import TelegramClient
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

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан")

DB_PATH = os.getenv("DB_PATH", "dataseeker.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

FUNSTAT_TOKEN = os.getenv("FUNSTAT_TOKEN", "")
FUNSTAT_BASE = os.getenv("FUNSTAT_BASE", "https://funstat.info")

TG_API_ID = int(os.getenv("TG_API_ID", "22047819"))
TG_API_HASH = os.getenv("TG_API_HASH", "f7e6c7d3b4bab72925aab12513ca48b0")
TG_SESSION_NAME = os.getenv("TG_SESSION_NAME", "gift_bot")

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


def _esc(v):
    return str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _json(data, status: int = 200):
    """JSON-ответ с русскими буквами как есть (не \\uXXXX)."""
    return web.json_response(
        data,
        status=status,
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


def get_cache_key(func_name: str, query: str) -> str:
    return f"{func_name}:{hashlib.md5(query.encode()).hexdigest()}"


API_TIMEOUTS = {
    "funstat": 10.0,
    "tg_gifts": 20.0,
}


SCHEMA_SQLITE = '''
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
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
'''

SCHEMA_PG = '''
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
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
                    DATABASE_URL,
                    min_size=1,
                    max_size=10,
                    timeout=10.0,
                    command_timeout=30.0,
                )

                async with pool.acquire() as conn:
                    await conn.execute("SELECT 1")

                db_conn = DBAdapter("postgres", pool=pool)
                await db_conn.executescript(SCHEMA_PG)

                for stmt in [
                    "ALTER TABLE site_keys ADD COLUMN IF NOT EXISTS today_searches BIGINT DEFAULT 0",
                    "ALTER TABLE site_keys ADD COLUMN IF NOT EXISTS today_date DATE",
                    """CREATE TABLE IF NOT EXISTS subscriptions (
                        key TEXT PRIMARY KEY,
                        plan_id TEXT,
                        daily_quota BIGINT DEFAULT 0,
                        started_at TIMESTAMP,
                        expires_at TIMESTAMP,
                        last_credited_date DATE,
                        active SMALLINT DEFAULT 1
                    )""",
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
            """CREATE TABLE IF NOT EXISTS subscriptions (
                key TEXT PRIMARY KEY,
                plan_id TEXT,
                daily_quota INTEGER DEFAULT 0,
                started_at TEXT,
                expires_at TEXT,
                last_credited_date TEXT,
                active INTEGER DEFAULT 1
            )""",
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


# ============ FUNSTAT ============

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
    """Поиск только по числовому Telegram ID."""
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
            result = await tg_client(
                tg_functions.payments.GetSavedStarGiftsRequest(
                    peer=user,
                    offset=offset,
                    limit=100
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


async def _resolve_tg_entity(target: str):
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
                "Бот не может получить профиль по числовому ID. "
                "Возможно ID приватный или бот не в контактах."
            )


async def fetch_gifts_data(target: str):
    if tg_client is None:
        return {"error": "MTProto-клиент не инициализирован"}

    target = (target or "").strip()

    try:
        user = await asyncio.wait_for(_resolve_tg_entity(target),
                                      timeout=API_TIMEOUTS["tg_gifts"])
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
        return f"❌ {data['error']}"

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

    header = f"🎁 <b>Подарки ({len(all_ids)})</b>"
    body = ", ".join(
        f'<a href="tg://user?id={_esc(i)}">{_esc(i)}</a>'
        for i in all_ids
    )
    return f"{header}\n<blockquote>{body}</blockquote>"


def _gifts_is_ok(text: str) -> bool:
    if not text:
        return False
    if text.startswith("❌"):
        return False
    if text in ("Подарков не найдено.", ""):
        return False
    return True


# ============ COLLECT ============

async def collect_general_data(query: str, search_type: str = "tg_id"):
    cache_key = get_cache_key(search_type, query)
    if cache_key in cache:
        cached_time, data = cache[cache_key]
        if datetime.now() - cached_time < CACHE_TTL:
            return data

    original_query = query
    tasks = {}

    if search_type == "tg_id":
        funstat_data = await funstat_search(query, "tg_id")
        logger.info(f"tg_id search: input={query!r} resolved_id={funstat_data.get('tg_id')}")

        async def _return_funstat():
            return funstat_data
        tasks['funstat'] = asyncio.create_task(_return_funstat())

    results = {}
    for name, task in tasks.items():
        try:
            results[name] = await asyncio.wait_for(task, timeout=20.0)
        except asyncio.TimeoutError:
            results[name] = {}
            task.cancel()
        except Exception as e:
            logger.error(f"{name} exception: {e}")
            results[name] = {}

    funstat = results.get('funstat', {}) or {}
    funstat_parsed = parse_funstat(funstat)

    logger.info(f"Parsed: fun={len(funstat_parsed)}")

    result = {
        'query': original_query, 'type': search_type,
        'blocks': [],
        'records_count': 0,
        'sources': [],
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
    header = "✈️ <b>Telegram"
    if tid:
        header += f" · {_esc(tid)}"
    header += "</b>"
    lines.append(header)
    lines.append("")

    if reg_date:
        mon = _format_month_year(reg_date)
        ago = _months_ago(reg_date)
        suffix = f" ({_esc(ago)})" if ago else ""
        lines.append(f"🕐 <b>Регистрация:</b> ~{_esc(mon)}{suffix}")
        lines.append("")

    if name_history:
        with_date = sorted([x for x in name_history if x[0]], key=lambda t: t[0], reverse=True)
        without_date = [x for x in name_history if not x[0]]
        sorted_history = with_date + without_date
        lines.append(f"🌀 <b>История изменения имени:</b>")
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
        return "❌ По этому Telegram ничего не найдено."

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
            "ok": True,
            "key": new_key,
            "balance_kopeks": SIGNUP_BONUS_KOPEKS,
            "balance_rub": SIGNUP_BONUS_KOPEKS / 100,
            "total_searches": 0,
            "today_searches": 0,
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

    # Инфа о подписке
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
        "ok": True,
        "key": key,
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
        "ok": True,
        "added_kopeks": amount,
        "balance_kopeks": new_row['balance_kopeks'] if new_row else row['balance_kopeks'] + amount,
    })


async def api_topup_packages_handler(request):
    return _json({"ok": True, "packages": TOPUP_PACKAGES})


async def api_pricing_handler(request):
    """GET /api/v1/pricing — списки пакетов и подписок."""
    return _json({
        "ok": True,
        "packs": TOPUP_PACKAGES,
        "subscriptions": SUBSCRIPTIONS,
    })


async def api_buy_pack_handler(request):
    """POST /api/v1/buy/pack — покупка разового пакета запросов."""
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

    logger.info(f"PACK purchased: {pack_id} → +{total_searches} searches for {key[:12]}...")

    return _json({
        "ok": True,
        "pack_id": pack_id,
        "searches_added": total_searches,
        "kopeks_added": add_kopeks,
        "balance_kopeks": new_row["balance_kopeks"] if new_row else row["balance_kopeks"] + add_kopeks,
    })


async def api_buy_subscription_handler(request):
    """POST /api/v1/buy/subscription — покупка подписки на месяц.
    Повторная покупка при активной подписке — 409."""
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
        'SELECT key, plan_id, expires_at FROM subscriptions '
        'WHERE key = ? AND active = 1',
        (key,)
    )

    # Блокируем повторную покупку пока старая активна
    if existing and existing["expires_at"]:
        cur_exp = str(existing["expires_at"])[:10]
        if cur_exp >= today:
            return _json({
                "ok": False,
                "error": "subscription_already_active",
                "active_until": cur_exp,
                "active_plan": existing["plan_id"],
            }, status=409)

    # Если подписки нет или она истекла — создаём новую
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

    # Сразу начисляем первый день
    daily_kopeks = sub["daily"] * SEARCH_PRICE_KOPEKS
    await db_conn.execute(
        'UPDATE site_keys SET balance_kopeks = balance_kopeks + ? WHERE key = ?',
        (daily_kopeks, key)
    )
    await db_conn.execute(
        'UPDATE subscriptions SET last_credited_date = ? WHERE key = ?',
        (today, key)
    )

    logger.info(f"SUB purchased: {sub_id} → +{sub['daily']}/day for {key[:12]}...")

    return _json({
        "ok": True,
        "sub_id": sub_id,
        "daily_quota": sub["daily"],
        "started_at": today,
        "expires_at": expires,
        "credited_now": sub["daily"],
    })


async def api_public_search_handler(request):
    """Публичный поиск для сайта — только по числовому Telegram ID."""
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

    # === Подписка: начислить дневную квоту если новый день ===
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
                    logger.info(f"Sub credit: +{sub['daily_quota']} searches for {key[:12]}...")
                    row = await _get_key_row(key)
            else:
                await db_conn.execute(
                    'UPDATE subscriptions SET active = 0 WHERE key = ?', (key,)
                )
                logger.info(f"Sub expired for {key[:12]}...")
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

    try:
        base_text = build_funstat_preview(data, target)
    except Exception:
        logger.exception("api_public_search_handler: preview")
        base_text = ""

    try:
        gifts_data = await fetch_gifts_data(target)
        gifts_text = format_gifts_message(gifts_data, target)
    except Exception:
        logger.exception("api_public_search_handler: gifts")
        gifts_data = None
        gifts_text = ""

    parts = []
    if base_text and not base_text.startswith("❌"):
        parts.append(base_text)
    if _gifts_is_ok(gifts_text):
        parts.append(gifts_text)

    if parts:
        full_text = "\n\n".join(parts)
    else:
        full_text = "❌ По этому Telegram ничего не найдено."

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
    today_count += 1

    await db_conn.execute(
        'UPDATE site_keys SET balance_kopeks = balance_kopeks - ?, '
        'total_searches = total_searches + 1, '
        'today_searches = ?, today_date = ? '
        'WHERE key = ?',
        (SEARCH_PRICE_KOPEKS, today_count, today_str, key)
    )

    new_row = await db_conn.fetchone(
        'SELECT balance_kopeks, total_searches FROM site_keys WHERE key = ?', (key,)
    )

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
                {"ID": gid, "Ссылка": f"tg://user?id={gid}"}
                for gid in gift_ids
            ]
            public_blocks.append({"source": "Telegram · Подарки", "rows": gift_rows})

    return _json({
        "ok": True,
        "query": data.get("query"),
        "type": data.get("type"),
        "sources": data.get("sources", []),
        "records_count": data.get("records_count", 0),
        "blocks": public_blocks,
        "text": full_text,
        "gifts_count": len(gift_ids),
        "balance_kopeks": new_row['balance_kopeks'] if new_row else row['balance_kopeks'] - SEARCH_PRICE_KOPEKS,
        "total_searches": new_row['total_searches'] if new_row else row['total_searches'] + 1,
        "today_searches": today_count,
    })


async def api_public_tg_info_handler(request):
    """GET /api/tg/info?q=123456789 — публичный TG-инфо без ключа."""
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

    target = q

    try:
        fs = await funstat_search(target, "tg_id")
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
        "gifts": [],
        "gifts_count": 0,
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
        gd = await fetch_gifts_data(str(uid))
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

    return _json({"ok": True, "query": q, "profile": profile})


# ============ TELETHON START ============

async def start_tg_client():
    global tg_client
    try:
        tg_client = TelegramClient(TG_SESSION_NAME, TG_API_ID, TG_API_HASH)
        await tg_client.start(bot_token=BOT_TOKEN)
        me = await tg_client.get_me()
        logger.info(f"✅ Telethon (bot) запущен: @{me.username if me.username else me.id}")
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
    app.router.add_get("/", lambda r: web.Response(text="API is running"))
    app.router.add_get("/health", lambda r: web.Response(text="OK"))

    app.router.add_post("/api/v1/key/create", api_key_create_handler)
    app.router.add_post("/api/v1/key/info", api_key_info_handler)
    app.router.add_post("/api/v1/key/topup", api_key_topup_handler)
    app.router.add_get("/api/v1/topup_packages", api_topup_packages_handler)

    # Тарифы и подписки
    app.router.add_get("/api/v1/pricing", api_pricing_handler)

    app.router.add_route("OPTIONS", "/api/v1/buy/pack", api_buy_pack_handler)
    app.router.add_post("/api/v1/buy/pack", api_buy_pack_handler)

    app.router.add_route("OPTIONS", "/api/v1/buy/subscription", api_buy_subscription_handler)
    app.router.add_post("/api/v1/buy/subscription", api_buy_subscription_handler)

    app.router.add_route("OPTIONS", "/api/v1/search", api_public_search_handler)
    app.router.add_post("/api/v1/search", api_public_search_handler)
    app.router.add_get("/api/v1/search", api_public_search_handler)

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
