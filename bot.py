import os
import re
import json
import random
import string
import time
import asyncio
import aiosqlite
import aiohttp
from aiohttp import web
from datetime import datetime, date, timedelta
from urllib.parse import urlparse, urlunparse, quote
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile, PreCheckoutQuery, LabeledPrice, WebAppInfo
)
from bs4 import BeautifulSoup
import hashlib
import logging

from telethon import TelegramClient
from telethon.tl import functions as tg_functions

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    asyncpg = None
    HAS_ASYNCPG = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан")

DB_PATH = os.getenv("DB_PATH", "dataseeker.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

BIGBASE_TOKEN = os.getenv("BIGBASE_TOKEN", "")
BIGBASE_BASE = os.getenv("BIGBASE_BASE", "https://bigbase.top")

SEON_API_KEY = os.getenv("SEON_API_KEY")
SEON_BASE = os.getenv("SEON_BASE", "https://api.seon.io")

SNUSBASE_API_KEY = os.getenv("SNUSBASE_API_KEY")
SNUSBASE_BASE = os.getenv("SNUSBASE_BASE", "https://api.snusbase.com")

JITLER_TOKENS = [t.strip() for t in os.getenv(
    "JITLER_TOKENS",
    "sYNTmjuaZTb2vUSOCE6AMJfD,"
    "Kj94VjELcD5y9FHMWaP64rD0,"
    "bVaxQuNJQDwcFaowGOOWlHgr,"
    "q49N0me1xlMKyTuJEiMWGVR3,"
    "yXtPtLBncR245PVWVvTZFk2m,"
    "H3Up52WMKkS7pid20r2zXwd3,"
    "cV134WqlWuc1cC2ojnxjz0H9,"
    "dhGe2dBgXI6eoXphtfe3MmAi"
).split(",") if t.strip()]

JITLER_BASE = "https://salty-bee-4852.rahmetovtamerlan102-debug.deno.net"

FUNSTAT_TOKEN = os.getenv("FUNSTAT_TOKEN", "")
FUNSTAT_BASE = os.getenv("FUNSTAT_BASE", "https://funstat.info")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "5021557806").split(",") if x.strip()]

CHUNK_SIZE = 5
TG_GROUPS_LIMIT = 2
MAX_MIRRORS_PER_USER = 1
BONUS_PER_MIRROR = 1

TG_API_ID = int(os.getenv("TG_API_ID", "22047819"))
TG_API_HASH = os.getenv("TG_API_HASH", "f7e6c7d3b4bab72925aab12513ca48b0")
TG_SESSION_NAME = os.getenv("TG_SESSION_NAME", "gift_bot")

tg_client = None

HIDDEN_FIELDS = {
    'Источник', 'Описание базы', 'Актуальность базы', 'Актуальность', 'Внутренний источник',
    'О себе', 'Bio', 'About', 'about', 'bio', 'description', 'О СЕБЕ',
    'Сайт', 'сайт', 'site', 'website', 'url', 'URL', 'САЙТ',
    'Фото', 'фото', 'photo', 'avatar', 'Avatar', 'ФОТО',
}

DEDUPED_FIELDS = {
    'Телефонные книги',
    'ВКонтакте', 'VK', 'Одноклассники', 'OK',
    'Instagram', 'TikTok', 'Facebook', 'Twitter', 'WhatsApp',
}

MIRROR_TOKEN_RE = re.compile(r'^\d{7,12}:[A-Za-z0-9_\-]{30,}$')


def _esc(v):
    return str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


_DOMAIN_URLS = (
    "vk.com/", "vkontakte.ru/", "vk.me/", "ok.ru/", "instagram.com/",
    "tiktok.com/", "facebook.com/", "fb.com/", "t.me/", "telegram.me/",
    "twitter.com/", "x.com/", "youtube.com/", "wa.me/", "whatsapp.com/",
    "telegram.org/", "youtu.be/", "github.com/", "linkedin.com/", "max.ru/",
)

SOCIAL_KEYS = {
    'ВКонтакте', 'VK', 'vk', 'Одноклассники', 'OK', 'ok', 'MAX', 'Max', 'max',
    'Instagram', 'instagram', 'TikTok', 'tiktok', 'Telegram', 'telegram',
    'WhatsApp', 'whatsapp', 'Facebook', 'facebook', 'Twitter', 'twitter',
    'Никнейм', 'Имя пользователя', 'Логин', 'Username', 'username',
    'Ссылка', 'Название', 'Кому', 'От',
}

JUNK_VALUE_PATTERNS = [
    r'^покупатель\b', r'^уважаемый\b', r'^назначена\b', r'^назначен\b',
    r'^клиент\b', r'^абонент\b', r'^пользователь\b', r'^владелец\b',
    r'^не\s+назначен', r'^не\s+назначена', r'\bnull\b',
    r'^неизвестно$', r'^нет данных$', r'^не указано$', r'^не определ',
    r'^none$', r'^-$', r'^—$', r'^н/д$', r'^нд$',
]

_FIO_WORD_RE = re.compile(r"^[А-Яа-яЁёA-Za-z][А-Яа-яЁёA-Za-z\-']{1,}$")


def _is_junk_value(value) -> bool:
    if value is None:
        return True
    s = str(value).strip()
    if not s:
        return True
    low = s.lower()
    for pat in JUNK_VALUE_PATTERNS:
        if re.search(pat, low):
            return True
    return False


def _looks_like_fio(value) -> bool:
    if value is None:
        return False
    s = re.sub(r'\s*\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}\s*', ' ', str(value))
    s = re.sub(r'\s+', ' ', s).strip(" ,.-")
    if not s:
        return False
    if len(s) < 5 or len(s) > 120:
        return False
    words = [w for w in s.split() if w]
    if len(words) < 2 or len(words) > 5:
        return False
    return all(_FIO_WORD_RE.match(w) for w in words)


def _fio_key(s: str) -> str:
    s = str(s)
    s = re.sub(r'\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}', '', s)
    s = re.sub(r'\d+', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip().lower()


def _dedup_fio(raw: str) -> str:
    if not raw:
        return ""
    parts = [p.strip() for p in str(raw).split(",") if p.strip()]
    seen = {}
    order = []
    for p in parts:
        if _is_junk_value(p):
            continue
        cleaned = re.sub(r'\s*\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}\s*', ' ', p)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip(" ,.-")
        if not cleaned or not _looks_like_fio(cleaned):
            continue
        key = _fio_key(cleaned)
        if not key:
            continue
        if key not in seen:
            seen[key] = cleaned
            order.append(key)
        else:
            if len(cleaned) < len(seen[key]):
                seen[key] = cleaned
    return ", ".join(seen[k] for k in order)


def _encode_url(u: str) -> str:
    if not u:
        return u
    try:
        p = urlparse(u)
        path = quote(p.path, safe="/@:-._~")
        return urlunparse((p.scheme, p.netloc, path, p.params, p.query, p.fragment))
    except Exception:
        return u


def _clean_country(raw):
    if not raw:
        return ""
    s = str(raw).strip()
    s = re.sub(r'^[A-Z]{2}\s+', '', s)
    s = re.sub(r'\s*\([^)]*\)\s*$', '', s)
    s = re.sub(r'^[🇦-🇿\U0001F1E6-\U0001F1FF]+\s*', '', s)
    return s.strip()


def _country_flag(name):
    if not name:
        return "🌍"
    n = name.lower()
    table = [
        (('рос', 'russia'), '🇷🇺'), (('казах', 'kazakh'), '🇰🇿'),
        (('украин', 'ukrain'), '🇺🇦'), (('беларус', 'belarus'), '🇧🇾'),
        (('узбек', 'uzbek'), '🇺🇿'), (('киргиз', 'kyrgyz'), '🇰🇬'),
        (('таджик', 'tajik'), '🇹🇯'), (('армен', 'armen'), '🇦🇲'),
        (('азерб', 'azer'), '🇦🇿'), (('грузин', 'georgi'), '🇬🇪'),
        (('молдов', 'moldov'), '🇲🇩'), (('туркмен', 'turkmen'), '🇹🇲'),
        (('сша', 'usa', 'united states'), '🇺🇸'), (('герман', 'german'), '🇩🇪'),
        (('франц', 'france'), '🇫🇷'), (('турц', 'turkey'), '🇹🇷'),
        (('китай', 'china'), '🇨🇳'), (('британ', 'britain', 'uk'), '🇬🇧'),
        (('япон', 'japan'), '🇯🇵'), (('индия', 'india'), '🇮🇳'),
        (('бразил', 'brazil'), '🇧🇷'), (('польш', 'poland'), '🇵🇱'),
    ]
    for keys, flag in table:
        for k in keys:
            if k in n:
                return flag
    return "🌍"


def _make_link(value, key: str = ""):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    k_low = (key or "").lower()

    if s.startswith("http://") or s.startswith("https://"):
        return f'<a href="{_esc(_encode_url(s))}" target="_blank" class="val-link">{_esc(s)}</a>'

    lower = s.lower()
    for d in _DOMAIN_URLS:
        if d in lower:
            idx = lower.find(d)
            candidate = s[idx:].split()[0].rstrip('.,;')
            if not candidate.startswith("http"):
                candidate = "https://" + candidate
            return f'<a href="{_esc(_encode_url(candidate))}" target="_blank" class="val-link">{_esc(s)}</a>'

    if re.match(r'^@[a-zA-Z0-9_]{5,32}$', s):
        return f'<a href="https://t.me/{_esc(s[1:])}" target="_blank" class="val-link">{_esc(s)}</a>'

    if key in SOCIAL_KEYS and re.match(r'^[a-zA-Z0-9_.]{3,64}$', s) and not s.isdigit():
        if 'вконтакте' in k_low or k_low == 'vk':
            return f'<a href="https://vk.com/{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'
        if 'одноклассники' in k_low or k_low == 'ok':
            return f'<a href="https://ok.ru/{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'
        if 'instagram' in k_low:
            return f'<a href="https://instagram.com/{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'
        if 'tiktok' in k_low:
            return f'<a href="https://tiktok.com/@{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'
        if 'facebook' in k_low:
            return f'<a href="https://facebook.com/{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'
        if 'twitter' in k_low:
            return f'<a href="https://twitter.com/{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'
        if 'telegram' in k_low or k_low in ('кому', 'от'):
            return f'<a href="https://t.me/{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'
        if 'username' in k_low or 'никнейм' in k_low or 'логин' in k_low:
            return f'<a href="https://t.me/{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'

    return None


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
        if re.search(r'INSERT\s+OR\s+REPLACE\s+INTO\s+reports', sql, re.IGNORECASE):
            sql = re.sub(
                r'INSERT\s+OR\s+REPLACE\s+INTO\s+reports\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)',
                r'INSERT INTO reports (\1) VALUES (\2) ON CONFLICT (report_id) DO UPDATE SET data = excluded.data, phone = excluded.phone',
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
                if isinstance(p, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', p):
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
_last_bb_tg_call = 0.0

cache = {}
CACHE_TTL = timedelta(hours=1)

active_mirrors = {}


def _public_base() -> str:
    base = (
        os.getenv("MINI_APP_BASE")
        or os.getenv("RENDER_EXTERNAL_URL")
        or "https://qq-v1ay.onrender.com"
    )
    return base.rstrip("/")


def make_report_id() -> str:
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))


def get_cache_key(func_name: str, query: str) -> str:
    return f"{func_name}:{hashlib.md5(query.encode()).hexdigest()}"


API_TIMEOUTS = {
    "bigbase": 15.0, "seon": 6.0, "snusbase": 6.0,
    "ipapi": 3.0, "jitler": 20.0, "funstat": 10.0, "tg_gifts": 20.0,
}


class PromoCreation(StatesGroup):
    waiting_for_code = State()
    waiting_for_max_uses = State()
    waiting_for_requests = State()


class GiveRequests(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_amount = State()


class Broadcast(StatesGroup):
    waiting_for_text = State()


class EnterPromo(StatesGroup):
    waiting_for_code = State()


PACKAGES = [
    {"requests": 5, "usd": 1.50, "stars": 30},
    {"requests": 10, "usd": 2.50, "stars": 50},
    {"requests": 25, "usd": 5.00, "stars": 80},
    {"requests": 50, "usd": 9.00, "stars": 100},
    {"requests": 100, "usd": 15.00, "stars": 150},
    {"requests": 200, "usd": 20.00, "stars": 200},
    {"requests": 1000, "usd": 200.00, "stars": 400},
]


def plural_days(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return "день"
    elif 2 <= n % 10 <= 4 and (n % 100 < 10 or n % 100 >= 20):
        return "дня"
    return "дней"


SCHEMA_SQLITE = '''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    daily_requests INTEGER DEFAULT 0,
    last_request_date TEXT DEFAULT (date('now')),
    bonus_requests INTEGER DEFAULT 0,
    referral_code TEXT UNIQUE,
    referred_by INTEGER
);
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    phone TEXT,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_reports_phone ON reports(phone);
CREATE TABLE IF NOT EXISTS phone_views (
    phone TEXT PRIMARY KEY,
    user_ids TEXT DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS promo_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE,
    max_uses INTEGER NOT NULL,
    used_count INTEGER DEFAULT 0,
    requests_granted INTEGER NOT NULL,
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS referrals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer_id INTEGER,
    referred_id INTEGER,
    bonus_given INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(referrer_id, referred_id)
);
CREATE TABLE IF NOT EXISTS purchases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    invoice_id TEXT UNIQUE,
    amount REAL,
    currency TEXT,
    requests INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confirmed_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mirrors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER NOT NULL,
    bot_token TEXT UNIQUE NOT NULL,
    bot_username TEXT,
    active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''

SCHEMA_PG = '''
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    daily_requests INTEGER DEFAULT 0,
    last_request_date DATE DEFAULT CURRENT_DATE,
    bonus_requests INTEGER DEFAULT 0,
    referral_code TEXT UNIQUE,
    referred_by BIGINT
);
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    phone TEXT,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_reports_phone ON reports(phone);
CREATE TABLE IF NOT EXISTS phone_views (
    phone TEXT PRIMARY KEY,
    user_ids TEXT DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS promo_codes (
    id BIGSERIAL PRIMARY KEY,
    code TEXT UNIQUE,
    max_uses INTEGER NOT NULL,
    used_count INTEGER DEFAULT 0,
    requests_granted INTEGER NOT NULL,
    created_by BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS referrals (
    id BIGSERIAL PRIMARY KEY,
    referrer_id BIGINT,
    referred_id BIGINT,
    bonus_given INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(referrer_id, referred_id)
);
CREATE TABLE IF NOT EXISTS purchases (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT,
    invoice_id TEXT UNIQUE,
    amount REAL,
    currency TEXT,
    requests INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confirmed_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS mirrors (
    id BIGSERIAL PRIMARY KEY,
    owner_id BIGINT NOT NULL,
    bot_token TEXT UNIQUE NOT NULL,
    bot_username TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''


async def init_db():
    global db_conn
    if DATABASE_URL:
        if not HAS_ASYNCPG:
            raise RuntimeError("DATABASE_URL задан, но asyncpg не установлен")
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        db_conn = DBAdapter("postgres", pool=pool)
        try:
            async with db_conn.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'reports' AND column_name = 'report_id'
                """)
                if not row:
                    logger.warning("Старая схема reports без report_id — пересоздаю")
                    await conn.execute("DROP TABLE IF EXISTS reports CASCADE")
        except Exception as e:
            logger.error(f"Проверка схемы reports: {e}")
        await db_conn.executescript(SCHEMA_PG)
        logger.info("PostgreSQL подключён")
    else:
        raw = await aiosqlite.connect(DB_PATH)
        raw.row_factory = aiosqlite.Row
        await raw.execute("PRAGMA journal_mode=WAL")
        await raw.execute("PRAGMA synchronous=NORMAL")
        db_conn = DBAdapter("sqlite", conn=raw)
        await db_conn.executescript(SCHEMA_SQLITE)
        logger.info(f"SQLite подключён: {DB_PATH}")


async def save_report(report_id: str, phone: str, data: dict):
    try:
        await db_conn.execute(
            'INSERT OR REPLACE INTO reports (report_id, phone, data, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)',
            (report_id, phone, json.dumps(data, ensure_ascii=False))
        )
        logger.info(f"report saved: {report_id}")
    except Exception as e:
        logger.error(f"save_report failed: {e}")


async def load_report(report_id: str):
    try:
        row = await db_conn.fetchone('SELECT data FROM reports WHERE report_id = ?', (report_id,))
        if not row:
            return None
        return json.loads(row['data'])
    except Exception as e:
        logger.error(f"load_report failed: {e}")
        return None


async def get_http_session():
    global http_session, _http_lock
    if _http_lock is None:
        _http_lock = asyncio.Lock()
    if http_session is None or http_session.closed:
        async with _http_lock:
            if http_session is None or http_session.closed:
                http_session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
    return http_session


class JitlerBalancer:
    def __init__(self, tokens):
        self.tokens = list(tokens)
        self.current_index = 0
        self.lock = asyncio.Lock()
        self.failed_tokens = set()

    async def get_token(self):
        async with self.lock:
            if not self.tokens:
                return None
            active = [t for t in self.tokens if t not in self.failed_tokens]
            if not active:
                self.failed_tokens.clear()
                active = list(self.tokens)
            token = active[self.current_index % len(active)]
            self.current_index = (self.current_index + 1) % max(len(active), 1)
            return token

    def mark_failed(self, token):
        self.failed_tokens.add(token)

    def mark_success(self, token):
        self.failed_tokens.discard(token)


jitler_balancer = JitlerBalancer(JITLER_TOKENS)


def calculate_age_from_birthdate(birthdate_str):
    if not birthdate_str:
        return None
    try:
        for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
            try:
                bd = datetime.strptime(birthdate_str, fmt)
                today = datetime.now()
                return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
            except ValueError:
                continue
        cleaned = birthdate_str.replace('/', '.').replace('-', '.')
        bd = datetime.strptime(cleaned, '%d.%m.%Y')
        today = datetime.now()
        return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
    except Exception:
        return None


async def bigbase_search(query: str):
    if not BIGBASE_TOKEN:
        return {}
    session = await get_http_session()
    url = f"{BIGBASE_BASE}/api/search"
    headers = {
        "Authorization": BIGBASE_TOKEN,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    clean = query.strip()
    if re.match(r'^[+]?[\d\s\-()]+$', clean):
        clean = re.sub(r'\D', '', clean)
    t0 = time.monotonic()
    try:
        async with session.post(url, json={"search": clean, "page": 0}, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["bigbase"])) as resp:
            body = await resp.text()
            elapsed = time.monotonic() - t0
            logger.info(f"BigBase status={resp.status} t={elapsed:.2f}s len={len(body)}")
            if resp.status != 200:
                return {}
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return {}
    except asyncio.TimeoutError:
        return {}
    except Exception as e:
        logger.error(f"BigBase exception: {e}")
        return {}


async def bigbase_search_telegram(query: str, retry: int = 2):
    global _last_bb_tg_call
    if not BIGBASE_TOKEN:
        return {}
    now = time.monotonic()
    delta = now - _last_bb_tg_call
    if delta < 2.0:
        await asyncio.sleep(2.0 - delta)
    _last_bb_tg_call = time.monotonic()

    session = await get_http_session()
    url = f"{BIGBASE_BASE}/api/search_telegram"
    headers = {
        "Authorization": BIGBASE_TOKEN,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    q = query.strip().lstrip('@')
    payload = {"query": q}

    for attempt in range(retry + 1):
        try:
            async with session.post(url, json=payload, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["bigbase"])) as resp:
                body = await resp.text()
                logger.info(f"BigBase TG [{q}] status={resp.status} len={len(body)}")
                if resp.status != 200:
                    return {}
                try:
                    data = json.loads(body)
                except json.JSONDecodeError:
                    return {}
                err = data.get("error") or ""
                if "Слишком много поисков" in err or "Попробуйте немного позже" in err:
                    if attempt < retry:
                        wait = 3 + attempt * 3
                        await asyncio.sleep(wait)
                        continue
                    return {"error": err}
                return data
        except asyncio.TimeoutError:
            return {}
        except Exception as e:
            logger.error(f"BigBase TG exception: {e}")
            return {}
    return {}


BIGBASE_KEY_MAP = {
    'фио': 'ФИО', 'имя': 'Имя', 'фамилия': 'Фамилия', 'отчество': 'Отчество',
    'рабочее фио': 'Рабочее ФИО', 'наименование': 'ФИО', 'наименование (англ.)': 'ФИО (англ.)',
    'телефон': 'Телефон', 'телефоны': 'Телефон', 'номер телефона': 'Телефон',
    'email': 'Email', 'почта': 'Email', 'e-mail': 'Email',
    'дата рождения': 'Дата рождения', 'др': 'Дата рождения',
    'возраст': 'Возраст', 'пол': 'Пол',
    'адрес': 'Адрес', 'город': 'Город', 'регион': 'Регион', 'страна': 'Страна', 'индекс': 'Индекс',
    'паспорт': 'Паспорт', 'серия паспорта': 'Серия паспорта',
    'номер паспорта': 'Номер паспорта', 'тип паспорта': 'Тип паспорта',
    'кем выдан': 'Кем выдан', 'дата выдачи': 'Дата выдачи',
    'инн': 'ИНН', 'снилс': 'СНИЛС', 'огрн': 'ОГРН', 'огрнип': 'ОГРНИП',
    'автомобиль': 'Автомобиль', 'авто': 'Автомобиль', 'транспорт': 'Транспорт',
    'госномер': 'Госномер', 'модель': 'Модель', 'vin': 'VIN',
    'класс автомобиля': 'Класс автомобиля', 'цвет': 'Цвет',
    'текущая база': 'Текущая база', 'первая база': 'Первая база', 'база': 'База',
    'статус': 'Статус', 'статус (текст)': 'Статус', 'рейтинг': 'Рейтинг',
    'сегмент': 'Сегмент', 'тип': 'Тип',
    'источник': 'Источник', 'база данных': 'База данных',
    'дата первой активности': 'Дата первой активности',
    'дата начала': 'Дата начала', 'дата обновления': 'Дата обновления',
    'дата регистрации': 'Дата регистрации',
    'информация диспетчера': 'Информация диспетчера',
    'статус email (текст)': 'Статус email',
    'компания': 'Компания', 'организация': 'Организация', 'должность': 'Должность',
    'login': 'Логин', 'логин': 'Логин', 'nickname': 'Никнейм',
    'telegram': 'Telegram', 'vk': 'ВКонтакте', 'ok': 'Одноклассники',
    'instagram': 'Instagram', 'tiktok': 'TikTok', 'whatsapp': 'WhatsApp',
    'max': 'MAX', 'uuid': 'UUID',
    'код страны': 'Код страны', 'код города': 'Код города',
    'код главного города': 'Код главного города',
    'код подразделения': 'Код подразделения',
    'код валюты': 'Код валюты', 'код контрагента': 'Код контрагента',
    'валюта': 'Валюта', 'валюта пополнения': 'Валюта пополнения',
    'ек4 id': 'ЕК4 ID', 'ключ партнера': 'Ключ партнёра',
    'создан': 'Создан', 'обновлён': 'Обновлён',
    'удалён': 'Удалён', 'архивный': 'Архивный', 'ip': 'IP',
}

JUNK_KEYS = {'id', 'record_id', 'rec_id', 'base_id', 'internal_id',
             'id автомобиля', 'id класса автомобиля', 'id первой базы',
             'id звонка', 'id организации', 'id базы', 'id города',
             'изображения', 'изображения авто', 'нет активности',
             'актуальность', 'актуальность базы', 'описание базы'}


def _flatten_complex(value):
    if value in (None, "", [], {}):
        return None
    if isinstance(value, (str, int, float)):
        return str(value)
    if isinstance(value, dict):
        for k in ('value', 'Номер', 'номер', 'number', 'phone', 'Значение', 'text', 'title'):
            if k in value and value[k]:
                v = value[k]
                if isinstance(v, (str, int, float)):
                    return str(v)
                return _flatten_complex(v)
        return None
    if isinstance(value, list):
        if not value:
            return None
        base_val = None
        first = value[0]
        if isinstance(first, (str, int, float)):
            base_val = str(first)
        elif isinstance(first, list) and first:
            for item in first:
                if isinstance(item, (str, int, float)):
                    base_val = str(item)
                    break
        extras = {}
        def scan(obj):
            if isinstance(obj, list):
                if len(obj) == 2 and isinstance(obj[0], str) and isinstance(obj[1], (str, int, float)):
                    k = obj[0].strip()
                    if k in ('Тип', 'type', 'Тип номера', 'Оператор', 'operator'):
                        extras[k] = str(obj[1])
                    return
                for x in obj:
                    scan(x)
        scan(value)
        if base_val:
            if extras:
                return f"{base_val} ({', '.join(f'{k}: {v}' for k, v in extras.items())})"
            return base_val
        return None
    return None


def _clean_pair(key, value):
    if key is None:
        return None, None
    k_str = str(key).strip()
    k_low = k_str.lower()
    if k_low in JUNK_KEYS:
        return None, None
    if isinstance(value, (list, dict)):
        cleaned = _flatten_complex(value)
        if not cleaned:
            return None, None
        value = cleaned
    else:
        if value in (None, "", [], {}):
            return None, None
        if value == "0" and k_low not in ('нет активности', 'изображения', 'изображения авто', 'рейтинг'):
            return None, None
    if _is_junk_value(value):
        return None, None
    if k_low in ('фио', 'наименование', 'рабочее фио') and not _looks_like_fio(value):
        return None, None
    if isinstance(value, str):
        if 'T00:00:00' in value:
            value = value.split('T')[0]
        elif re.match(r'^\d{4}-\d{2}-\d{2}T', value):
            value = value.split('T')[0]
    ru = BIGBASE_KEY_MAP.get(k_low, k_str)
    return ru, value


def _parse_base_record(record):
    out = {}
    if not isinstance(record, list) or not record:
        return out
    if all(isinstance(x, list) and len(x) == 2 for x in record):
        for k, v in record:
            ru, val = _clean_pair(k, v)
            if ru and ru not in out:
                out[ru] = val
    elif all(not isinstance(x, (list, dict)) for x in record) and len(record) % 2 == 0:
        for i in range(0, len(record), 2):
            ru, val = _clean_pair(record[i], record[i + 1])
            if ru and ru not in out:
                out[ru] = val
    if 'Телефоны' in out and 'Телефон' not in out:
        out['Телефон'] = out.pop('Телефоны')
    if 'Почта' in out and 'Email' not in out:
        out['Email'] = out.pop('Почта')
    if out.get('Рабочее ФИО') and out.get('ФИО') and out['Рабочее ФИО'].strip() == out['ФИО'].strip():
        del out['Рабочее ФИО']
    return out


def _collect_connections(connections):
    result = {'ФИО': [], 'Телефон': [], 'Транспорт': [], 'Связи': []}
    if not isinstance(connections, list):
        return result
    for conn in connections:
        if not isinstance(conn, dict):
            continue
        ctype = conn.get('type', '')
        for fio in conn.get('fio', []) or []:
            if isinstance(fio, dict) and fio.get('value') and fio['value'] not in result['ФИО']:
                result['ФИО'].append(fio['value'])
        for ph in conn.get('phone', []) or []:
            if isinstance(ph, dict) and ph.get('value') and ph['value'] not in result['Телефон']:
                result['Телефон'].append(ph['value'])
        for tr in conn.get('transport', []) or []:
            if isinstance(tr, dict) and tr.get('model') and tr['model'] not in result['Транспорт']:
                result['Транспорт'].append(tr['model'])
        title = conn.get('title')
        if title and ctype == 'person' and title not in result['ФИО']:
            result['ФИО'].append(title)

        if ctype == 'person':
            for item in conn.get('fio', []) or []:
                if not isinstance(item, dict):
                    continue
                val = item.get('value')
                if not val or _is_junk_value(val):
                    continue
                if val in result['Связи']:
                    continue
                result['Связи'].append(val)
    return result


def parse_bigbase(data):
    if not isinstance(data, dict):
        return []
    parsed = []

    dossier = data.get('dossier') if isinstance(data.get('dossier'), dict) else {}
    head = dossier.get('head') if isinstance(dossier.get('head'), dict) else {}
    if head:
        hf = {}
        if head.get('phone_country_info'):
            hf['Страна'] = head['phone_country_info']
        if head.get('phone_operator'):
            hf['Оператор'] = head['phone_operator']
        if head.get('phone_region'):
            hf['Регион'] = head['phone_region']
        if head.get('phone_operator_inn'):
            hf['ИНН оператора'] = head['phone_operator_inn']
        if head.get('phone_code_country'):
            hf['Код страны'] = head['phone_code_country']
        if hf:
            parsed.append({"type": "result", "data": hf, "source": "Инфо о номере"})

    records = data.get('records')
    if isinstance(records, list):
        for rec in records:
            if not isinstance(rec, dict):
                continue
            fields = {}
            base_record = rec.get('base_record')
            if isinstance(base_record, list):
                fields.update(_parse_base_record(base_record))

            conn = _collect_connections(rec.get('connections'))
            if conn['ФИО'] and 'ФИО' not in fields:
                fio_clean = _dedup_fio(", ".join(conn['ФИО']))
                if fio_clean:
                    fields['ФИО'] = fio_clean
            if conn['Телефон'] and 'Телефон' not in fields:
                fields['Телефон'] = ', '.join(conn['Телефон'])
            if conn['Транспорт'] and 'Транспорт' not in fields and 'Автомобиль' not in fields:
                fields['Транспорт'] = ', '.join(conn['Транспорт'])

            owner_fio = fields.get('ФИО', '')
            conn_list = []
            for c in conn['Связи']:
                if c and c != owner_fio and c not in conn_list:
                    conn_list.append(c)
            if conn_list:
                fields['Связи'] = ', '.join(conn_list[:15])

            base_info = rec.get('base_info')
            base_name = 'BigBase'
            if isinstance(base_info, dict):
                if base_info.get('name'):
                    base_name_check = base_info['name']
                    if base_name_check != 'Инфо о номере':
                        fields['Источник'] = base_name_check
                        base_name = base_name_check
                if base_info.get('description'):
                    fields['Описание базы'] = base_info['description']
                if base_info.get('date_relevance'):
                    fields['Актуальность базы'] = base_info['date_relevance']

            conn_sources = set()
            if isinstance(rec.get('connections'), list):
                for c in rec['connections']:
                    if not isinstance(c, dict):
                        continue
                    for key in ('fio', 'phone', 'transport'):
                        for item in c.get(key, []) or []:
                            if isinstance(item, dict):
                                for src in item.get('source', []) or []:
                                    if isinstance(src, dict) and src.get('name'):
                                        conn_sources.add(src['name'])
            conn_sources.discard(base_name)
            if conn_sources:
                fields['Внутренний источник'] = ', '.join(sorted(conn_sources))

            if fields:
                parsed.append({"type": "result", "data": fields, "source": base_name})

    if not parsed and isinstance(data.get('connections'), dict):
        top_conn = _collect_connections(data['connections'].get('person'))
        if top_conn['ФИО'] or top_conn['Телефон'] or top_conn['Транспорт']:
            fields = {}
            if top_conn['ФИО']:
                fio_clean = _dedup_fio(", ".join(top_conn['ФИО']))
                if fio_clean:
                    fields['ФИО'] = fio_clean
            if top_conn['Телефон']:
                fields['Телефон'] = ', '.join(top_conn['Телефон'])
            if top_conn['Транспорт']:
                fields['Транспорт'] = ', '.join(top_conn['Транспорт'])
            parsed.append({"type": "result", "data": fields, "source": "BigBase"})

    return parsed


TG_ENTITY_TYPE = {
    "user": "Пользователь", "channel": "Канал", "group": "Группа",
    "bot": "Бот", "supergroup": "Супергруппа",
}


def parse_bigbase_telegram(data):
    if not data or not isinstance(data, dict):
        return []

    if data.get("error"):
        return [{"type": "result", "data": {"Ошибка": data["error"]}, "source": "BigBase · Telegram"}]

    if not data.get("found"):
        return [{
            "type": "result",
            "data": {
                "Запрос": data.get("query", ""),
                "Тип запроса": data.get("query_type", ""),
                "Результат": "не найден",
            },
            "source": "BigBase · Telegram"
        }]

    fields = {}
    if data.get("query"):
        fields["Запрос"] = data["query"]
    if data.get("query_type"):
        fields["Тип запроса"] = data["query_type"]

    tid = data.get("telegram_id")
    if tid is not None:
        fields["Telegram ID"] = str(tid)

    entity = data.get("entity") if isinstance(data.get("entity"), dict) else {}
    if entity:
        et = entity.get("type", "")
        if et:
            fields["Тип объекта"] = TG_ENTITY_TYPE.get(et, et)
        if entity.get("title"):
            fields["Название"] = entity["title"]
        if entity.get("participants_count") is not None:
            fields["Участников"] = entity["participants_count"]

    def _name_to_str(item):
        if isinstance(item, str):
            return item.strip()
        if isinstance(item, dict):
            parts = []
            for key in ("first_name", "name", "firstname"):
                v = item.get(key)
                if v:
                    parts.append(str(v).strip())
                    break
            for key in ("last_name", "surname", "lastname"):
                v = item.get(key)
                if v:
                    parts.append(str(v).strip())
                    break
            return " ".join(p for p in parts if p)
        return str(item).strip()

    profile = data.get("profile") if isinstance(data.get("profile"), dict) else {}
    if profile:
        if profile.get("last_username"):
            fields["Последний username"] = profile["last_username"]

        names = profile.get("names")
        if isinstance(names, list) and names:
            seen = []
            for n in names:
                s = _name_to_str(n)
                if s and s not in seen:
                    seen.append(s)
            if seen:
                fields["Известные имена"] = ", ".join(seen[:15])

        unames = profile.get("usernames")
        if isinstance(unames, list) and unames:
            seen = []
            for u in unames:
                if isinstance(u, str):
                    s = u.strip()
                elif isinstance(u, dict):
                    s = str(u.get("username") or u.get("name") or u.get("value") or "").strip()
                else:
                    s = str(u).strip()
                if s and s not in seen:
                    seen.append(s if s.startswith("@") else "@" + s)
            if seen:
                fields["Известные username"] = ", ".join(seen[:15])

    if data.get("groups_count") is not None:
        fields["Всего групп"] = data["groups_count"]
    if data.get("messages_count") is not None:
        fields["Всего сообщений"] = data["messages_count"]

    parsed = [{"type": "result", "data": fields, "source": "BigBase · Telegram"}]

    groups = data.get("groups")
    if isinstance(groups, list) and groups:
        for i, g in enumerate(groups[:20], 1):
            if not isinstance(g, dict):
                continue
            gfields = {"#": i}
            if g.get("title"):
                gfields["Название"] = g["title"]
            if g.get("id") is not None:
                gfields["ID"] = str(g["id"])
            if g.get("username"):
                gfields["Username"] = g["username"]
            if g.get("type"):
                gfields["Тип"] = TG_ENTITY_TYPE.get(g["type"], g["type"])
            if g.get("participants_count") is not None:
                gfields["Участников"] = g["participants_count"]
            if g.get("last_message_at") or g.get("last_seen"):
                gfields["Последняя активность"] = g.get("last_message_at") or g.get("last_seen")
            if len(gfields) > 1:
                parsed.append({"type": "result", "data": gfields, "source": "BigBase · Группа"})

    messages = data.get("messages")
    if isinstance(messages, list) and messages:
        for i, m in enumerate(messages[:20], 1):
            if not isinstance(m, dict):
                continue
            mfields = {"#": i}
            if m.get("chat_title") or m.get("chat"):
                mfields["Чат"] = m.get("chat_title") or m.get("chat")
            if m.get("chat_id") is not None:
                mfields["ID чата"] = str(m["chat_id"])
            if m.get("text") or m.get("message"):
                text = str(m.get("text") or m.get("message"))
                mfields["Текст"] = text[:300] + ("..." if len(text) > 300 else "")
            if m.get("date") or m.get("created_at"):
                mfields["Дата"] = m.get("date") or m.get("created_at")
            if len(mfields) > 1:
                parsed.append({"type": "result", "data": mfields, "source": "BigBase · Сообщение"})

    return parsed


async def seon_search(query: str):
    if not SEON_API_KEY:
        return {}
    session = await get_http_session()
    url = f"{SEON_BASE}/SeonRestService/phone-api/v2"
    headers = {"X-API-KEY": SEON_API_KEY, "Content-Type": "application/json"}
    try:
        async with session.post(url, json={"phone": query}, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["seon"])) as resp:
            text = await resp.text()
            logger.info(f"SEON status={resp.status} len={len(text)}")
            return json.loads(text) if resp.status == 200 else {}
    except Exception as e:
        logger.error(f"SEON error: {e}")
        return {}


def parse_seon(data):
    if not data or not isinstance(data, dict):
        return []
    parsed = []
    payload = data.get('data') if isinstance(data.get('data'), dict) else data
    phone_info = {}
    if isinstance(payload, dict):
        if payload.get('phone'):
            phone_info['Телефон'] = payload['phone']
        risk = payload.get('risk_scores')
        if isinstance(risk, dict):
            for k, v in risk.items():
                phone_info[k.replace('_', ' ').title()] = v
        agg = payload.get('account_aggregates')
        if isinstance(agg, dict):
            if agg.get('total_registration') is not None:
                phone_info['Всего регистраций'] = agg['total_registration']
            if isinstance(agg.get('business'), dict):
                biz = agg['business']
                if biz.get('total_registration') is not None:
                    phone_info['Бизнес-регистраций'] = biz['total_registration']
    if phone_info:
        parsed.append({"type": "phone_info", "data": phone_info})
    if data.get('email'):
        parsed.append({"type": "email", "data": {"Email": data['email']}})
    return parsed


async def snusbase_search(query: str):
    if not SNUSBASE_API_KEY:
        return {}
    session = await get_http_session()
    url = f"{SNUSBASE_BASE}/data/search"
    headers = {"Auth": SNUSBASE_API_KEY, "Content-Type": "application/json"}
    payload = {"terms": [query], "types": ["email", "username", "name", "lastip"], "wildcard": False}
    try:
        async with session.post(url, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["snusbase"])) as resp:
            text = await resp.text()
            logger.info(f"Snusbase status={resp.status} len={len(text)}")
            return json.loads(text) if resp.status == 200 else {}
    except Exception as e:
        logger.error(f"Snusbase error: {e}")
        return {}


def parse_snusbase(data):
    if not data or not isinstance(data, dict):
        return []
    parsed = []
    results_map = data.get('results')
    if isinstance(results_map, dict):
        for term, items in results_map.items():
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        fields = {k: v for k, v in item.items() if v not in (None, "", [], {})}
                        if fields:
                            parsed.append({"type": "result", "data": fields})
        return parsed
    results = data.get('data') or []
    if isinstance(results, dict):
        results = [results]
    key_mapping = {
        'email': 'Email', 'username': 'Имя пользователя', 'password': 'Пароль',
        'hash': 'Хеш', 'phone': 'Телефон', 'address': 'Адрес', 'name': 'Имя',
        'full_name': 'ФИО', 'city': 'Город', 'country': 'Страна',
        'ip': 'IP адрес', 'source': 'Источник', 'lastip': 'Последний IP',
        'login': 'Логин',
    }
    for item in results:
        if isinstance(item, dict):
            fields = {key_mapping.get(k, k): v for k, v in item.items() if v not in (None, "", [], {})}
            if fields:
                parsed.append({"type": "result", "data": fields})
    return parsed


async def jitler_search(query: str, search_type: str = "number"):
    if not jitler_balancer.tokens:
        return {}
    session = await get_http_session()

    refusal_count = 0

    for _ in range(len(jitler_balancer.tokens) * 2):
        token = await jitler_balancer.get_token()
        if not token:
            return {}
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        payload = {"type": search_type, "query": query, "page": 1}
        t0 = time.monotonic()
        try:
            async with session.post(
                f"{JITLER_BASE}/?path=/search",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["jitler"]),
            ) as resp:
                text = await resp.text()
                elapsed = time.monotonic() - t0
                logger.info(f"Jitler POST [{search_type}:{query}] status={resp.status} t={elapsed:.2f}s token=...{token[-6:]}")

                if resp.status == 401:
                    jitler_balancer.mark_failed(token)
                    continue

                if resp.status == 429:
                    logger.info(f"Jitler 429 rate-limit token=...{token[-6:]}")
                    continue

                if resp.status == 403:
                    body_low = text.lower()
                    if "неавторизован" in body_low:
                        jitler_balancer.mark_failed(token)
                        continue
                    if "подписаны" in body_low or "спонсор" in body_low:
                        jitler_balancer.mark_failed(token)
                        continue
                    refusal_count += 1
                    logger.info(f"Jitler 403 мягкий отказ token=...{token[-6:]}")
                    if refusal_count >= 6:
                        return {}
                    continue

                if resp.status != 200:
                    continue

                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    return {}

                if isinstance(data.get("response"), (dict, list)):
                    jitler_balancer.mark_success(token)
                    return data

                if data.get("result") is False and data.get("error"):
                    err_low = str(data.get("error", "")).lower()
                    if "неавторизован" in err_low or "подписаны" in err_low:
                        jitler_balancer.mark_failed(token)
                        continue
                    refusal_count += 1
                    if refusal_count >= 6:
                        return {}
                    continue

                task_id = data.get("id")
                if task_id:
                    for _ in range(12):
                        await asyncio.sleep(1.5)
                        try:
                            async with session.get(
                                f"{JITLER_BASE}/?path=/search/{task_id}",
                                headers=headers,
                                timeout=aiohttp.ClientTimeout(total=5),
                            ) as g:
                                if g.status == 200:
                                    r = await g.json()
                                    if isinstance(r.get("response"), (dict, list)):
                                        jitler_balancer.mark_success(token)
                                        return r
                                elif g.status == 501:
                                    continue
                                elif g.status in (404, 403, 500):
                                    break
                        except asyncio.TimeoutError:
                            continue
                        except Exception:
                            continue
                return {}
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"Jitler exception [{search_type}]: {e}")
            continue
    return {}


JITLER_FIELD_MAP = {
    "телефон": "Телефон", "оператор": "Оператор", "страна": "Страна",
    "регион": "Регион", "город": "Город",
    "телефонные книги": "Телефонные книги",
    "vk профили": "VK", "одноклассники": "Одноклассники",
    "facebook": "Facebook", "telegram": "Telegram",
    "whatsapp": "WhatsApp", "instagram": "Instagram",
    "фото": "Фото", "имя": "Имя", "возраст": "Возраст",
}


def _strip_tags_html(s: str) -> str:
    s = re.sub(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', r'\2 (\1)', s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r'<[^>]+>', '', s)
    s = s.replace('&nbsp;', ' ').replace('&amp;', '&')
    return s.strip()


def _parse_jitler_raw(raw: str):
    result = []
    main = {}
    SKIP_FROM_MAIN = {
        "vk профили", "одноклассники", "facebook", "telegram",
        "instagram", "whatsapp", "оператор", "страна",
    }

    for m in re.finditer(
        r'<strong>\s*([^<:]+?)\s*:?\s*</strong>\s*(?:<code>([^<]*)</code>|([^<]+?))(?=<|$)',
        raw, re.IGNORECASE
    ):
        label_raw = m.group(1).strip().lower()
        value = (m.group(2) or m.group(3) or "").strip()
        value = _strip_tags_html(value)
        if not value or label_raw in SKIP_FROM_MAIN:
            continue
        label = JITLER_FIELD_MAP.get(label_raw, m.group(1).strip())
        if label not in main:
            main[label] = value

    pb_match = re.search(
        r'Телефонные книги[^:]*:\s*</strong>\s*(.+?)(?=<strong>|<br|🧑|🔵|💬|📱|<b>|by jitler|$)',
        raw, re.IGNORECASE | re.DOTALL
    )
    if pb_match:
        pb_text = _strip_tags_html(pb_match.group(1))
        books = [x.strip() for x in pb_text.split(',') if x.strip()]
        if books:
            main['Телефонные книги'] = ", ".join(books)

    if main:
        result.append({"type": "result", "data": main, "source": "Jitler"})

    social_map = {
        "vk профили": "VK", "одноклассники": "Одноклассники",
        "facebook": "Facebook", "instagram": "Instagram",
    }

    for key, display in social_map.items():
        pat = re.compile(
            rf'<strong>\s*{re.escape(key)}[^<]*?</strong>(.*?)(?=<strong>|<b>|$)',
            re.IGNORECASE | re.DOTALL
        )
        m = pat.search(raw)
        if not m:
            continue
        block = m.group(1)
        items = re.findall(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', block, re.IGNORECASE | re.DOTALL)
        if not items:
            text_only = _strip_tags_html(block).strip(' •')
            if text_only:
                parts = [p.strip() for p in re.split(r'[•·]', text_only) if p.strip()]
                for i, p in enumerate(parts, 1):
                    result.append({"type": "result", "data": {"#": i, "Значение": p}, "source": f"Jitler · {display}"})
            continue
        total_items = len(items)
        for i, (url, text) in enumerate(items, 1):
            text_clean = _strip_tags_html(text)
            d = {}
            if total_items > 1:
                d["#"] = i
            name = text_clean
            bd_match = re.search(r'\((\d{2}\.\d{2}\.\d{4})\)', text_clean)
            if bd_match:
                d["Дата рождения"] = bd_match.group(1)
                name = text_clean.replace(f"({bd_match.group(1)})", "").strip()
            if name:
                d["Имя"] = name
            if url:
                d["Ссылка"] = url
            result.append({"type": "result", "data": d, "source": f"Jitler · {display}"})

    tg_pat = re.compile(
        r'<strong>\s*Telegram[^<]*?</strong>(.*?)(?=<strong>|<b>|by jitler|$)',
        re.IGNORECASE | re.DOTALL
    )
    tg_match = tg_pat.search(raw)
    if tg_match:
        block = tg_match.group(1)
        items = re.findall(r'<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', block, re.IGNORECASE | re.DOTALL)
        if items:
            total_items = len(items)
            for i, (url, text) in enumerate(items, 1):
                d = {}
                if total_items > 1:
                    d["#"] = i
                d["Название"] = _strip_tags_html(text)
                d["Ссылка"] = url
                result.append({"type": "result", "data": d, "source": "Jitler · Telegram"})
        else:
            text_only = _strip_tags_html(block).strip(' •')
            parts = [p.strip() for p in re.split(r'[•·]', text_only) if p.strip()]
            total_items = len(parts)
            for i, p in enumerate(parts, 1):
                data_item = {}
                if total_items > 1:
                    data_item["#"] = i
                m_username = re.search(r'@([a-zA-Z0-9_]+)\s*\(ID:\s*(\d+)\)', p)
                if m_username:
                    data_item["Username"] = "@" + m_username.group(1)
                    data_item["Telegram ID"] = m_username.group(2)
                else:
                    m_id = re.search(r'ID:\s*(\d+)', p)
                    if m_id:
                        data_item["Telegram ID"] = m_id.group(1)
                    else:
                        m_phone = re.search(r'\b(\d{10,15})\b', p)
                        if m_phone:
                            data_item["Телефон"] = m_phone.group(1)
                        else:
                            data_item["Значение"] = p
                result.append({"type": "result", "data": data_item, "source": "Jitler · Telegram"})

    return result


def parse_jitler(data):
    if not data or not isinstance(data, dict):
        return []
    response = data.get("response")
    if response is None:
        return []
    if isinstance(response, list):
        if not response:
            return []
        if isinstance(response[0], dict):
            response = response[0]
        else:
            return [{"type": "result", "data": {"Значение": str(x)}} for x in response if x]

    if not isinstance(response, dict):
        return []

    parsed = []

    if any(k in response for k in ("phonebooks", "telegram", "profiles", "cars", "counts", "mentions")):
        main = {}
        if response.get("phone"):
            main["Телефон"] = response["phone"]
        if response.get("operator"):
            main["Оператор"] = response["operator"]
        if response.get("region"):
            main["Регион"] = response["region"]
        if response.get("country"):
            main["Страна"] = response["country"]

        phonebooks = response.get("phonebooks") or []
        if phonebooks:
            cleaned = []
            for b in phonebooks:
                s = str(b).strip()
                if not s or s.lower() == "none":
                    continue
                if not re.search(r'[a-zA-Zа-яА-Я0-9]', s):
                    continue
                cleaned.append(s)
            if cleaned:
                main["Телефонные книги"] = ", ".join(cleaned[:50])

        if main:
            parsed.append({"type": "result", "data": main, "source": "Jitler"})

        telegram = response.get("telegram") or []
        valid_tg = [t for t in telegram if isinstance(t, dict)]
        total_tg = len(valid_tg)
        for i, tg in enumerate(valid_tg, 1):
            tfields = {}
            if total_tg > 1:
                tfields["#"] = i
            if tg.get("username"):
                tfields["Username"] = tg["username"]
            if tg.get("id"):
                tfields["Telegram ID"] = str(tg["id"])
            if tfields:
                parsed.append({"type": "result", "data": tfields, "source": "Jitler · Telegram"})

        profiles = response.get("profiles") if isinstance(response.get("profiles"), dict) else {}
        social_display = {
            "vk": "VK", "ok": "Одноклассники", "tiktok": "TikTok",
            "instagram": "Instagram", "facebook": "Facebook", "max": "MAX",
        }
        for key, display in social_display.items():
            items = profiles.get(key) or []
            valid_items = [x for x in items if x]
            total_items = len(valid_items)
            for i, item in enumerate(valid_items, 1):
                if isinstance(item, str):
                    s = item.strip()
                    if not s:
                        continue
                    url_match = re.search(r'https?://\S+', s)
                    if url_match:
                        url = url_match.group(0).rstrip('.,;')
                        name = s.replace(url, "").strip(" -—,:|•")
                        d = {}
                        if total_items > 1:
                            d["#"] = i
                        if name:
                            d["Имя"] = name
                        d["Ссылка"] = url
                    else:
                        d = {}
                        if total_items > 1:
                            d["#"] = i
                        d["Значение"] = s
                    if d:
                        parsed.append({"type": "result", "data": d, "source": f"Jitler · {display}"})
                elif isinstance(item, dict):
                    d = {}
                    if total_items > 1:
                        d["#"] = i
                    name = item.get("name") or item.get("title") or item.get("username") or ""
                    url = item.get("url") or item.get("link") or item.get("profile") or ""
                    if not url and isinstance(name, str):
                        m2 = re.search(r'https?://\S+', name)
                        if m2:
                            url = m2.group(0).rstrip('.,;')
                            name = name.replace(url, "").strip(" -—,:|•")
                    if name:
                        d["Имя"] = name
                    if url:
                        d["Ссылка"] = url
                    for k, v in item.items():
                        if k in ("name", "title", "username", "url", "link", "profile"):
                            continue
                        if v not in (None, "", [], {}):
                            d[str(k)] = v
                    if d:
                        parsed.append({"type": "result", "data": d, "source": f"Jitler · {display}"})

        cars = response.get("cars") or []
        valid_cars = [c for c in cars if c]
        total_cars = len(valid_cars)
        for i, car in enumerate(valid_cars, 1):
            d = {}
            if total_cars > 1:
                d["#"] = i
            if isinstance(car, str):
                d["Авто"] = car
            elif isinstance(car, dict):
                for k, v in car.items():
                    if v not in (None, "", [], {}):
                        d[str(k)] = v
            if d:
                parsed.append({"type": "result", "data": d, "source": "Jitler · Авто"})

        if parsed:
            return parsed

    raw = response.get("raw") or response.get("text")
    if raw:
        parsed_html = _parse_jitler_raw(str(raw))
        if parsed_html:
            return parsed_html

    flat = {}
    for k, v in response.items():
        if k == "raw" or v in (None, "", [], {}):
            continue
        if isinstance(v, list) and all(not isinstance(x, (dict, list)) for x in v):
            flat[str(k)] = ", ".join(str(x) for x in v)
        elif not isinstance(v, (dict, list)):
            flat[str(k)] = v
    if flat:
        return [{"type": "result", "data": flat, "source": "Jitler"}]
    return []


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


async def funstat_resolve_username(name: str):
    if not name:
        return None
    name = name.lstrip("@")
    r = await _funstat_get("/api/v1/users/resolve_username", {"name": name})
    for item in (r.get("data") or []):
        if item.get("id"):
            return int(item["id"])
    return None


async def funstat_search(query: str, search_type: str = "tg_id"):
    if not FUNSTAT_TOKEN:
        return {}
    tg_id = None
    if search_type == "tg_id":
        if query.isdigit():
            tg_id = int(query)
        else:
            tg_id = await funstat_resolve_username(query)
    if not tg_id:
        return {}

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


async def ip_info_search(query: str):
    session = await get_http_session()
    url = f"http://ip-api.com/json/{query}?fields=status,message,country,regionName,city,zip,lat,lon,timezone,isp,org,as,query"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["ipapi"])) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data if data.get('status') == 'success' else {}
    except Exception:
        pass
    return {}


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


def tg_from_id_str(from_id):
    if from_id is None:
        return None
    if hasattr(from_id, "user_id"):
        return str(from_id.user_id)
    if hasattr(from_id, "channel_id"):
        return str(from_id.channel_id)
    return str(from_id)


async def tg_get_profile_gifts(user):
    if tg_client is None:
        return []
    req_cls = getattr(tg_functions.payments, "GetUserStarGiftsRequest", None)
    if req_cls is None:
        print("[!] GetUserStarGiftsRequest отсутствует — обнови Telethon")
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
    target = (target or "").strip()
    if target.startswith("@"):
        target = target[1:]

    if not target.isdigit():
        return await tg_client.get_entity(target)

    uid = int(target)
    try:
        return await tg_client.get_entity(uid)
    except Exception:
        try:
            from telethon.tl.types import InputPeerUser
            return await tg_client.get_entity(InputPeerUser(uid, 0))
        except Exception:
            raise ValueError(
                "Бот не может получить профиль по числовому ID без username. "
                "Используй /id @username."
            )


async def fetch_gifts_data(target: str):
    if tg_client is None:
        return {"error": "MTProto-клиент не инициализирован. Проверь, что бот запущен."}

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


async def collect_general_data(query: str, search_type: str = "phone"):
    cache_key = get_cache_key(search_type, query)
    if cache_key in cache:
        cached_time, data = cache[cache_key]
        if datetime.now() - cached_time < CACHE_TTL:
            return data

    original_query = query
    if search_type == "phone":
        query = re.sub(r'[^0-9]', '', query)

    tasks = {}
    if search_type == "tg_id":
        funstat_data = await funstat_search(query, "tg_id")
        resolved_id = funstat_data.get("tg_id")
        target_for_jitler = str(resolved_id) if resolved_id else query
        logger.info(f"tg_id search: input={query!r} resolved_id={resolved_id} jitler_target={target_for_jitler!r}")

        tasks['bigbase'] = asyncio.create_task(bigbase_search_telegram(target_for_jitler))
        tasks['jitler'] = asyncio.create_task(jitler_search(target_for_jitler, "sherlock"))

        async def _return_funstat():
            return funstat_data
        tasks['funstat'] = asyncio.create_task(_return_funstat())
    else:
        tasks['bigbase'] = asyncio.create_task(bigbase_search(query))
        tasks['seon'] = asyncio.create_task(seon_search(query))
        if search_type == "email":
            tasks['snusbase'] = asyncio.create_task(snusbase_search(query))
        if search_type == "ip":
            tasks['ipapi'] = asyncio.create_task(ip_info_search(query))
        if search_type == "phone":
            tasks['jitler'] = asyncio.create_task(jitler_search(query, "number"))

    results = {}
    for name, task in tasks.items():
        try:
            timeout = 25.0 if name == "jitler" else 20.0 if name == "bigbase" else 15.0
            results[name] = await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            results[name] = {}
            task.cancel()
        except Exception as e:
            logger.error(f"{name} exception: {e}")
            results[name] = {}

    bigbase = results.get('bigbase', {}) or {}
    seon = results.get('seon', {}) or {}
    snusbase = results.get('snusbase', {}) or {}
    jitler = results.get('jitler', {}) or {}
    funstat = results.get('funstat', {}) or {}
    ipdata = results.get('ipapi', {}) if search_type == "ip" else {}

    if search_type == "tg_id":
        bigbase_parsed = parse_bigbase_telegram(bigbase)
        seon_parsed = []
        snusbase_parsed = []
        jitler_parsed = parse_jitler(jitler)
        funstat_parsed = parse_funstat(funstat)
    else:
        bigbase_parsed = parse_bigbase(bigbase)
        seon_parsed = parse_seon(seon)
        snusbase_parsed = parse_snusbase(snusbase)
        jitler_parsed = parse_jitler(jitler)
        funstat_parsed = []

    logger.info(f"Parsed: big={len(bigbase_parsed)}, seon={len(seon_parsed)}, snus={len(snusbase_parsed)}, jit={len(jitler_parsed)}, fun={len(funstat_parsed)}")

    result = {
        'query': original_query, 'type': search_type,
        'operator': None, 'region': None, 'country': None, 'city': None,
        'fio': None, 'birthdate': None, 'age': None, 'address': None,
        'emails': [], 'telegrams': [],
        'vk': None, 'instagram': None, 'tiktok': None, 'ok': None,
        'phone_books': [], 'contacts': [], 'extra': {}, 'sources': [], 'records_count': 0,
        'blocks': [],
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
        source_name = block.get('source') or fields.get('Источник') or 'BigBase'

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

    if ipdata:
        result['country'] = ipdata.get('country') or result['country']
        result['region'] = ipdata.get('regionName') or result['region']
        result['city'] = ipdata.get('city') or result['city']
        if ipdata.get('isp'):
            result['operator'] = ipdata['isp']
        extra_ip = {k: ipdata[k] for k in ['country', 'regionName', 'city', 'zip', 'lat', 'lon', 'timezone', 'isp', 'org', 'as'] if ipdata.get(k)}
        if extra_ip:
            records_count += 1
            blocks.append(['IP информация', [extra_ip]])
            sources_set.add("ip-api.com")

    for block in bigbase_parsed:
        _add_block(block)
        fields = block.get('data') or {}
        if fields.get('Телефон'):
            for ph in str(fields['Телефон']).split(','):
                ph = ph.strip()
                if ph and ph not in result['phone_books']:
                    result['phone_books'].append(ph)
        if fields.get('ФИО') and not result['fio']:
            fio_clean = _dedup_fio(fields['ФИО'])
            if fio_clean:
                result['fio'] = fio_clean
        if fields.get('Дата рождения') and not result['birthdate']:
            bd = str(fields['Дата рождения'])
            result['birthdate'] = bd
            age = calculate_age_from_birthdate(bd)
            if age is not None:
                result['age'] = age
        if fields.get('Возраст') and result.get('age') is None:
            try:
                result['age'] = int(str(fields['Возраст']).split()[0])
            except Exception:
                pass
        if fields.get('Адрес') and not result['address']:
            result['address'] = str(fields['Адрес'])
        if fields.get('Email'):
            for em in str(fields['Email']).split(','):
                em = em.strip()
                if em and em not in result['emails']:
                    result['emails'].append(em)
        if fields.get('Оператор') and not result['operator']:
            result['operator'] = str(fields['Оператор'])
        if fields.get('Город') and not result['city']:
            result['city'] = str(fields['Город'])
        if fields.get('Регион') and not result['region']:
            result['region'] = str(fields['Регион'])
        if fields.get('Страна') and not result['country']:
            result['country'] = str(fields['Страна'])

    for block in seon_parsed:
        if block['type'] == 'phone_info':
            info = block['data']
            if info.get('Оператор') and not result['operator']:
                result['operator'] = str(info['Оператор'])
            if info.get('Страна') and not result['country']:
                result['country'] = str(info['Страна'])
            sources_set.add("SEON")
        elif block['type'] == 'email':
            em = block['data'].get('Email')
            if em and str(em) not in result['emails']:
                result['emails'].append(str(em))
                sources_set.add("SEON")

    for block in snusbase_parsed:
        _add_block(block)
        fields = block.get('data') or {}
        em = fields.get('Email')
        if em and str(em) not in result['emails']:
            result['emails'].append(str(em))

    for block in jitler_parsed:
        _add_block(block)
        fields = block.get('data') or {}
        if fields.get('Телефон'):
            for ph in re.split(r'[,;]', str(fields['Телефон'])):
                ph = ph.strip()
                if ph and ph not in result['phone_books']:
                    result['phone_books'].append(ph)
        if fields.get('ФИО') and not result['fio']:
            fio_clean = _dedup_fio(fields['ФИО'])
            if fio_clean:
                result['fio'] = fio_clean
        if fields.get('Возраст') and result.get('age') is None:
            try:
                result['age'] = int(str(fields['Возраст']).split()[0])
            except Exception:
                pass
        if fields.get('Email'):
            for em in re.split(r'[,;]', str(fields['Email'])):
                em = em.strip()
                if em and em not in result['emails']:
                    result['emails'].append(em)

    for block in funstat_parsed:
        _add_block(block)

    result['sources'] = list(sources_set)
    result['records_count'] = records_count
    result['emails'] = list(dict.fromkeys([e for e in result['emails'] if e and '@' in str(e)]))
    result['blocks'] = blocks

    rid = make_report_id()
    result['report_id'] = rid
    try:
        await save_report(rid, str(original_query), result)
    except Exception as e:
        logger.error(f"save_report failed: {e}")

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


def _clean_group_title(title: str) -> str:
    if not title:
        return title
    s = str(title)
    s = re.sub(r'\s*[—–-]\s*[^,]+\(\d{1,2}\.\d{1,2}\.\d{4}\)\s*$', '', s).strip()
    s = re.sub(r'\s*[—–-]\s*[A-ZА-Я][a-zа-я]+\s+[A-ZА-Я][a-zа-я]+.*$', '', s).strip()
    return s


def build_funstat_preview(data: dict, query: str) -> str:
    tid = ""
    reg_date = ""
    name_history = []
    groups = []

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

            if "Группа" in source:
                title = row.get("Название") or row.get("Название чата")
                uname = row.get("Username")
                date_str = row.get("Последняя активность") or row.get("Дата") or ""
                if title or uname:
                    title = _clean_group_title(title) if title else title
                    label = ""
                    if uname:
                        u = str(uname).lstrip("@")
                        label += f"@{u}"
                    if title:
                        label += (", " if label else "") + str(title)
                    d_short = _format_group_date(date_str)
                    if d_short:
                        label = f"{d_short} → {label}"
                    if label and label not in groups:
                        groups.append(label)

    if not name_history:
        for source, rows in data.get('blocks', []):
            if source.startswith("BigBase"):
                for row in rows:
                    if row.get("Последний username"):
                        name_history.append(("", "@" + str(row["Последний username"]).lstrip("@")))
                    if row.get("Известные username"):
                        for u in str(row["Известные username"]).split(','):
                            u = u.strip()
                            if u and not any(u == h[1] for h in name_history):
                                name_history.append(("", u if u.startswith("@") else "@" + u))

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

    if groups:
        lines.append(f"👥 <b>Группы:</b>")
        joined = "\n".join(groups[:TG_GROUPS_LIMIT])
        lines.append(f"<blockquote>{joined}</blockquote>")
        lines.append("")

    if not lines or (len(lines) == 2 and not tid):
        return "❌ По этому Telegram ничего не найдено."

    return "\n".join(lines).strip()


def build_preview(data: dict) -> str:
    lines = []
    phone = data.get('query')
    if phone:
        lines.append(f"📱 <b>Телефон:</b> <code>{_esc(phone)}</code>")
    op = data.get('operator')
    if op:
        lines.append(f"📡 <b>Оператор:</b> {_esc(op)}")
    region = data.get('region')
    if region:
        lines.append(f"📍 <b>Регион:</b> {_esc(region)}")
    country = data.get('country')
    if country:
        c_clean = _clean_country(country)
        if c_clean:
            flag = _country_flag(c_clean)
            lines.append(f"🌍 <b>Страна:</b> {flag} {_esc(c_clean)}")

    fio = data.get('fio')
    bd = data.get('birthdate')
    age = data.get('age')

    if fio or bd or age:
        lines.append("")
        if fio:
            fio_clean = _dedup_fio(fio)
            if fio_clean:
                lines.append(f"👤 <b>ФИО:</b> {_esc(fio_clean)}")
        if bd:
            lines.append(f"🎂 <b>Дата рождения:</b> {_esc(bd)}")
        if age is not None:
            lines.append(f"🔷 <b>Возраст:</b> {_esc(age)} лет")

    phonebooks = []
    for src, rows in data.get('blocks', []):
        for row in rows:
            tb = row.get('Телефонные книги')
            if tb:
                phonebooks.extend([x.strip() for x in str(tb).split(',') if x.strip()])
    if phonebooks:
        seen = []
        for p in phonebooks:
            if p not in seen:
                seen.append(p)
        pb_str = ", ".join(seen[:30])
        if len(seen) > 30:
            pb_str += f" и ещё {len(seen) - 30}"
        lines.append(f"\n🔍 <b>Телефонные книги:</b> {_esc(pb_str)}")

    socials = {'VK': [], 'Одноклассники': [], 'MAX': [], 'Instagram': [], 'TikTok': [], 'Facebook': []}

    for src, rows in data.get('blocks', []):
        src_low = (src or '').lower()
        for row in rows:
            name = (row.get('Имя') or row.get('Название') or '').strip()
            url = (row.get('Ссылка') or '').strip()
            direct = ''
            if not name:
                for k in ('ВКонтакте', 'VK', 'Одноклассники', 'OK', 'MAX', 'Max', 'Instagram', 'TikTok', 'Facebook'):
                    v = row.get(k)
                    if v:
                        direct = str(v).strip()
                        break
                name = direct
            if not name and not url:
                continue

            target = None
            if 'vk' in src_low or 'вконтакте' in src_low:
                target = 'VK'
            elif 'одноклассники' in src_low or src_low.endswith(' ok') or '· ok' in src_low:
                target = 'Одноклассники'
            elif 'max' in src_low:
                target = 'MAX'
            elif 'instagram' in src_low:
                target = 'Instagram'
            elif 'tiktok' in src_low:
                target = 'TikTok'
            elif 'facebook' in src_low:
                target = 'Facebook'

            if target is None:
                if row.get('ВКонтакте'):
                    target = 'VK'
                elif row.get('Одноклассники') or row.get('OK'):
                    target = 'Одноклассники'
                elif row.get('MAX') or row.get('Max'):
                    target = 'MAX'
                elif row.get('Instagram'):
                    target = 'Instagram'
                elif row.get('TikTok'):
                    target = 'TikTok'
                elif row.get('Facebook'):
                    target = 'Facebook'

            if target is None:
                continue

            label = name or url
            href = _encode_url(url) if (url and url.startswith("http")) else ""
            entry = f'<a href="{_esc(href)}">{_esc(label)}</a>' if href else _esc(label)

            if socials[target]:
                existing = socials[target][0]
                has_link_old = 'href=' in existing
                has_link_new = 'href=' in entry
                if has_link_new and not has_link_old:
                    socials[target][0] = entry
                continue
            socials[target].append(entry)

    icons = {'VK': '🌐', 'Одноклассники': '🟠', 'MAX': '🟣',
             'Instagram': '📷', 'TikTok': '🎵', 'Facebook': '📘'}
    for net, entries in socials.items():
        if not entries:
            continue
        lines.append(f"\n{icons[net]} <b>{net}:</b> " + entries[0])

    tg = []
    for src, rows in data.get('blocks', []):
        for row in rows:
            uname = row.get('Username')
            if uname and uname not in tg:
                tg.append(uname)
            tid_val = row.get('Telegram ID')
            if tid_val and f"#{tid_val}" not in tg:
                tg.append(f"#{tid_val}")

    if tg:
        pretty = []
        for t in tg[:3]:
            if str(t).startswith('@'):
                pretty.append(f"<a href=\"https://t.me/{_esc(str(t)[1:])}\">{_esc(t)}</a>")
            elif str(t).startswith('#'):
                num = str(t)[1:]
                pretty.append(f"<a href=\"tg://user?id={_esc(num)}\">#{_esc(num)}</a>")
            else:
                pretty.append(_esc(t))
        lines.append(f"\n✈️ <b>Telegram:</b> " + " ".join(pretty))

    return "\n".join(lines) if lines else "❌ Пусто"


def generate_html_report(data: dict, views: int = 0) -> str:
    query = data.get('query', '')
    blocks = data.get('blocks', [])
    total_records = data.get('records_count', 0)

    blocks = [(s, r) for s, r in blocks if s != 'Инфо о номере']
    total_records = sum(len(rows) for _, rows in blocks)

    phonebooks = []
    socials = {
        'VK': [], 'Одноклассники': [], 'MAX': [],
        'Instagram': [], 'TikTok': [], 'Facebook': [],
        'Telegram': [], 'WhatsApp': [], 'Twitter': [],
    }
    for src, rows in blocks:
        src_low = (src or '').lower()
        for row in rows:
            tb = row.get('Телефонные книги')
            if tb:
                for p in re.split(r'[,;]', str(tb)):
                    p = p.strip()
                    if p and p not in phonebooks:
                        phonebooks.append(p)

            name = (row.get('Имя') or row.get('Название') or '').strip()
            url = (row.get('Ссылка') or '').strip()

            target = None
            if 'vk' in src_low or 'вконтакте' in src_low:
                target = 'VK'
            elif 'одноклассники' in src_low or src_low.endswith(' ok') or '· ok' in src_low:
                target = 'Одноклассники'
            elif 'max' in src_low:
                target = 'MAX'
            elif 'instagram' in src_low:
                target = 'Instagram'
            elif 'tiktok' in src_low:
                target = 'TikTok'
            elif 'facebook' in src_low:
                target = 'Facebook'
            elif 'telegram' in src_low:
                target = 'Telegram'
            elif 'whatsapp' in src_low:
                target = 'WhatsApp'
            elif 'twitter' in src_low:
                target = 'Twitter'

            if target is None:
                for k, t in [('ВКонтакте','VK'), ('Одноклассники','Одноклассники'),
                             ('OK','Одноклассники'), ('MAX','MAX'), ('Max','MAX'),
                             ('Instagram','Instagram'), ('TikTok','TikTok'),
                             ('Facebook','Facebook'), ('Telegram','Telegram'),
                             ('WhatsApp','WhatsApp'), ('Twitter','Twitter')]:
                    if row.get(k):
                        target = t
                        name = str(row[k]).strip()
                        break

            if target is None or (not name and not url):
                continue

            entry = {"label": name or url, "url": url}
            if entry not in socials[target]:
                socials[target].append(entry)

    phonebooks_html = ""
    if phonebooks:
        items = "".join(
            f'<span class="tag-pill">{_esc(p)}</span>'
            for p in phonebooks
        )
        phonebooks_html = f'''
        <div class="card open" id="sec-phonebooks">
            <div class="card-head" onclick="toggleCard(this)">
                <div class="card-title">
                    <svg class="chevron" viewBox="0 0 12 8" fill="none"><path d="M1 1.5L6 6.5L11 1.5" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>
                    Телефонные книги
                </div>
                <div class="badges"><span class="badge">{len(phonebooks)}</span></div>
            </div>
            <div class="card-body"><div class="card-content">
                <div class="tags-wrap">{items}</div>
            </div></div>
        </div>'''

    socials_html = ""
    social_blocks = []
    for net, entries in socials.items():
        if not entries:
            continue
        rows_html = ""
        for e in entries:
            label = e["label"] or e["url"]
            url = e["url"]
            if url and url.startswith("http"):
                link = f'<a href="{_esc(_encode_url(url))}" target="_blank">{_esc(label)}</a>'
            elif label.startswith("@"):
                link = f'<a href="https://t.me/{_esc(label[1:])}" target="_blank">{_esc(label)}</a>'
            elif net == "VK":
                link = f'<a href="https://vk.com/{_esc(label)}" target="_blank">{_esc(label)}</a>'
            elif net == "Instagram":
                link = f'<a href="https://instagram.com/{_esc(label)}" target="_blank">{_esc(label)}</a>'
            elif net == "TikTok":
                link = f'<a href="https://tiktok.com/@{_esc(label)}" target="_blank">{_esc(label)}</a>'
            else:
                link = _esc(label)
            rows_html += f'<div class="data-item"><div class="value" style="font-size:15px;">{link}</div></div>'
        social_blocks.append(f'''
            <div style="margin-bottom:14px;">
                <div class="label">{_esc(net)} ({len(entries)})</div>
                {rows_html}
            </div>''')

    if social_blocks:
        socials_html = f'''
        <div class="card open" id="sec-socials">
            <div class="card-head" onclick="toggleCard(this)">
                <div class="card-title">
                    <svg class="chevron" viewBox="0 0 12 8" fill="none"><path d="M1 1.5L6 6.5L11 1.5" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>
                    Соцсети
                </div>
                <div class="badges"><span class="badge">{sum(len(v) for v in socials.values())}</span></div>
            </div>
            <div class="card-body"><div class="card-content">{"".join(social_blocks)}</div></div>
        </div>'''

    cats = {
        "Документы": 0, "Банки": 0, "Соцсети": 0, "Работа": 0,
        "Связи": 0, "Адреса": 0, "Авто": 0, "Недвижимость": 0, "Нарушения": 0,
    }
    for source, rows in blocks:
        s_low = (source or "").lower()
        if "соцсет" in s_low or "vk" in s_low or "instagram" in s_low or "telegram" in s_low or "jitler" in s_low:
            cats["Соцсети"] += len(rows)
        if "паспорт" in s_low or "снилс" in s_low or "инн" in s_low:
            cats["Документы"] += len(rows)
        if "авто" in s_low or "транспорт" in s_low:
            cats["Авто"] += len(rows)
        if "адрес" in s_low or "город" in s_low:
            cats["Адреса"] += len(rows)
        if "кому дарил" in s_low or "от кого получал" in s_low:
            cats["Связи"] += len(rows)
    covered_cats = sum(1 for v in cats.values() if v > 0)
    total_cats = len(cats)
    percent = int(covered_cats / total_cats * 100) if total_cats else 0

    nav_items = [
        ("sec-info", "Информация о запросе", ""),
        ("sec-cover", "Покрытие отчёта", ""),
    ]
    if phonebooks:
        nav_items.append(("sec-phonebooks", "Телефонные книги", str(len(phonebooks))))
    if social_blocks:
        nav_items.append(("sec-socials", "Соцсети", str(sum(len(v) for v in socials.values()))))
    for idx, (source, rows) in enumerate(blocks, start=1):
        nav_items.append((f"sec-block-{idx}", source, str(len(rows))))

    nav_html = ""
    for i, (anchor, title, count) in enumerate(nav_items):
        active = " active" if i == 0 else ""
        cnt = f'<span class="nav-count">{_esc(count)}</span>' if count else ''
        nav_html += f'''
        <a href="#{anchor}" class="nav-item{active}" onclick="closeNav()">
          <span class="nav-dot"></span> {_esc(title)}
          {cnt}
        </a>'''

    cover_html = ""
    for cat, val in cats.items():
        if val > 0:
            cover_html += f'<div class="cov-item active"><span>✓</span> {_esc(cat)} <span style="margin-left:auto;font-size:12px;">{val}</span></div>'
        else:
            cover_html += f'<div class="cov-item">— {_esc(cat)}</div>'

    blocks_html = ""
    for idx, (source, rows) in enumerate(blocks, start=1):
        total = len(rows)

        if total == 1:
            fields = rows[0]
            visible = [(k, v) for k, v in fields.items()
                       if k not in HIDDEN_FIELDS
                       and k not in DEDUPED_FIELDS
                       and v is not None and str(v).strip()
                       and not _is_junk_value(v)]

            inner = ""
            for k, v in visible:
                link = _make_link(v, k)
                if link:
                    val_html = link
                elif k in ("Telegram ID", "TG ID") and str(v).isdigit():
                    val_html = f'<a href="tg://user?id={_esc(v)}">{_esc(v)}</a>'
                else:
                    val_html = _esc(v)
                inner += f'<div class="data-item"><div class="label">{_esc(k)}</div><div class="value">{val_html}</div></div>'

            body = inner
        else:
            cards = ""
            for r_i, fields in enumerate(rows, 1):
                visible = [(k, v) for k, v in fields.items()
                           if k not in HIDDEN_FIELDS
                           and k not in DEDUPED_FIELDS
                           and v is not None and str(v).strip()
                           and not _is_junk_value(v)]
                card_inner = ""
                for k, v in visible:
                    link = _make_link(v, k)
                    if link:
                        val_html = link
                    elif k in ("Telegram ID", "TG ID") and str(v).isdigit():
                        val_html = f'<a href="tg://user?id={_esc(v)}">{_esc(v)}</a>'
                    else:
                        val_html = _esc(v)
                    card_inner += f'<div class="data-item"><div class="label">{_esc(k)}</div><div class="value">{val_html}</div></div>'
                cards += f'''
                <div style="background:var(--card2);border:1px solid var(--border);border-radius:12px;padding:14px 16px;margin-bottom:10px;">
                    <div class="label" style="margin-bottom:10px;">Запись #{r_i}</div>
                    {card_inner}
                </div>'''
            body = cards

        blocks_html += f'''
        <div class="card open" id="sec-block-{idx}">
            <div class="card-head" onclick="toggleCard(this)">
                <div class="card-title">
                    <svg class="chevron" viewBox="0 0 12 8" fill="none"><path d="M1 1.5L6 6.5L11 1.5" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>
                    {_esc(source)}
                </div>
                <div class="badges"><span class="badge">{total}</span></div>
            </div>
            <div class="card-body">
                <div class="card-content">
                    {body}
                </div>
            </div>
        </div>'''

    return f'''<!DOCTYPE html>
<html lang="ru" data-theme="dark">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0" />
<title>Отчёт · {_esc(query)}</title>
<style>
  :root, [data-theme="dark"] {{
    --bg: #12100e;
    --card: #1c1916;
    --card2: #241f1b;
    --border: #2e2924;
    --text: #e8e2d9;
    --muted: #8a8075;
    --gold: #c9a227;
    --green: #3d9b6a;
    --green-bg: rgba(61, 155, 106, 0.18);
    --nav-bg: #1a1714;
    --nav-active: #3a322a;
    --overlay: rgba(0,0,0,0.55);
    --radius: 16px;
  }}
  [data-theme="light"] {{
    --bg: #f2f0ec;
    --card: #ffffff;
    --card2: #f7f5f1;
    --border: #e0dcd4;
    --text: #1a1714;
    --muted: #7a7368;
    --gold: #a8841a;
    --green: #2d8a5a;
    --green-bg: rgba(45, 138, 90, 0.12);
    --nav-bg: #ffffff;
    --nav-active: #f0ebe3;
    --overlay: rgba(0,0,0,0.35);
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  html {{ scroll-behavior: smooth; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", Roboto, sans-serif;
    background: var(--bg); color: var(--text); min-height: 100vh;
    line-height: 1.45; padding-bottom: 50px;
    transition: background 0.25s, color 0.25s;
  }}
  a {{ color: var(--gold); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}

  .topbar {{
    position: sticky; top: 0; z-index: 50;
    background: color-mix(in srgb, var(--bg) 92%, transparent);
    backdrop-filter: blur(12px);
    padding: 12px 16px;
    display: flex; align-items: center; gap: 12px;
    border-bottom: 1px solid var(--border);
  }}
  .icon-btn {{
    width: 36px; height: 36px; border-radius: 10px;
    background: transparent; border: none; color: var(--muted);
    display: flex; align-items: center; justify-content: center;
    cursor: pointer; flex-shrink: 0;
  }}
  .logo-eye {{
    width: 32px; height: 32px; border-radius: 50%;
    background: linear-gradient(135deg, #c9a227, #8b6914);
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0; font-size: 15px;
  }}
  .search {{
    flex: 1; background: var(--card); border: 1px solid var(--border);
    border-radius: 20px; padding: 10px 16px; color: var(--text);
    font-size: 14px; outline: none;
  }}
  .search::placeholder {{ color: var(--muted); }}

  .nav-overlay {{
    position: fixed; inset: 0; background: var(--overlay);
    z-index: 90; opacity: 0; pointer-events: none;
    transition: opacity 0.25s;
  }}
  .nav-overlay.open {{ opacity: 1; pointer-events: auto; }}

  .side-nav {{
    position: fixed; top: 0; left: 0; bottom: 0;
    width: min(320px, 85vw);
    background: var(--nav-bg); z-index: 100;
    transform: translateX(-105%);
    transition: transform 0.28s ease;
    display: flex; flex-direction: column;
    border-right: 1px solid var(--border);
    box-shadow: 4px 0 24px rgba(0,0,0,0.25);
  }}
  .side-nav.open {{ transform: translateX(0); }}

  .nav-header {{
    display: flex; align-items: center; justify-content: space-between;
    padding: 18px 18px 14px;
    border-bottom: 1px solid var(--border);
  }}
  .nav-header-title {{
    font-size: 13px; font-weight: 700; letter-spacing: 0.8px;
    text-transform: uppercase; color: var(--muted);
  }}
  .nav-close {{
    width: 32px; height: 32px; border-radius: 8px;
    background: transparent; border: none; color: var(--muted);
    display: flex; align-items: center; justify-content: center; cursor: pointer;
  }}
  .nav-search-wrap {{ padding: 14px 16px; position: relative; }}
  .nav-search {{
    width: 100%; background: var(--card2); border: 1px solid var(--border);
    border-radius: 12px; padding: 11px 14px 11px 38px; color: var(--text);
    font-size: 14px; outline: none;
  }}
  .nav-search-wrap svg {{
    position: absolute; left: 28px; top: 50%; transform: translateY(-50%);
    color: var(--muted); pointer-events: none;
  }}
  .nav-section-label {{
    font-size: 11px; font-weight: 600; letter-spacing: 0.6px;
    text-transform: uppercase; color: var(--muted);
    padding: 8px 18px 6px;
  }}
  .nav-list {{ flex: 1; overflow-y: auto; padding: 0 10px 20px; }}
  .nav-item {{
    display: flex; align-items: center; gap: 12px;
    padding: 12px 14px; border-radius: 12px;
    color: var(--text); font-size: 15px; font-weight: 500;
    cursor: pointer; text-decoration: none;
    transition: background 0.15s; margin-bottom: 2px;
  }}
  .nav-item:hover {{ background: var(--card2); text-decoration: none; color: var(--text); }}
  .nav-item.active {{ background: var(--nav-active); }}
  .nav-dot {{
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--gold); flex-shrink: 0;
  }}
  .nav-count {{
    margin-left: auto;
    min-width: 24px; height: 22px; padding: 0 7px;
    border-radius: 11px; background: var(--card2);
    border: 1px solid var(--border);
    font-size: 12px; font-weight: 700; color: var(--muted);
    display: flex; align-items: center; justify-content: center;
  }}

  .wrap {{ max-width: 480px; margin: 0 auto; padding: 16px; }}

  .card {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: var(--radius); margin-bottom: 14px; overflow: hidden;
    transition: background 0.25s, border-color 0.25s;
  }}
  .card-head {{
    display: flex; align-items: center; justify-content: space-between;
    padding: 16px 18px; cursor: pointer; user-select: none;
  }}
  .card-head:hover {{ background: color-mix(in srgb, var(--text) 3%, transparent); }}
  .card-title {{ display: flex; align-items: center; gap: 10px; font-size: 16px; font-weight: 600; }}
  .chevron {{ width: 18px; height: 18px; color: var(--muted); transition: transform 0.25s; }}
  .card.open .chevron {{ transform: rotate(180deg); }}
  .badges {{ display: flex; align-items: center; gap: 6px; }}
  .badge {{
    min-width: 24px; height: 24px; padding: 0 8px; border-radius: 12px;
    background: var(--card2); color: var(--gold); font-size: 12px; font-weight: 700;
    display: flex; align-items: center; justify-content: center;
    border: 1px solid var(--border);
  }}
  .card-body {{ max-height: 0; overflow: hidden; transition: max-height 0.35s ease; }}
  .card.open .card-body {{ max-height: 8000px; }}
  .card-content {{ padding: 0 18px 18px; }}

  .label {{
    font-size: 11px; font-weight: 600; letter-spacing: 0.6px;
    text-transform: uppercase; color: var(--muted); margin-bottom: 6px;
  }}
  .value {{ font-size: 17px; font-weight: 600; color: var(--text); margin-bottom: 16px; }}
  .value:last-child {{ margin-bottom: 0; }}
  .row-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }}
  .status {{
    display: inline-flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 600;
    color: var(--green); background: var(--green-bg); padding: 5px 12px; border-radius: 20px;
  }}
  .status::before {{ content: ""; width: 7px; height: 7px; border-radius: 50%; background: var(--green); }}
  .divider {{ height: 1px; background: var(--border); margin: 14px 0; }}
  .actions {{ display: flex; gap: 8px; flex-wrap: wrap; }}
  .action-btn {{
    flex: 1; min-width: 90px; padding: 11px 14px; border-radius: 12px;
    background: var(--card2); border: 1px solid var(--border); color: var(--text);
    font-size: 13px; font-weight: 600; text-align: center; cursor: pointer;
  }}
  .action-btn:hover {{ filter: brightness(1.08); }}

  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }}
  .cov-item {{
    padding: 12px 14px; border-radius: 12px; background: var(--card2);
    border: 1px solid var(--border); font-size: 13px; font-weight: 500;
    color: var(--muted); display: flex; align-items: center; gap: 8px;
  }}
  .cov-item.active {{
    background: var(--green-bg); border-color: color-mix(in srgb, var(--green) 40%, transparent); color: var(--green);
  }}

  .tags-wrap {{
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 4px;
  }}
  .tag-pill {{
    display: inline-flex;
    align-items: center;
    padding: 9px 14px;
    border-radius: 20px;
    background: var(--card2);
    border: 1px solid var(--border);
    font-size: 13px;
    font-weight: 500;
    color: var(--text);
    line-height: 1.2;
    white-space: nowrap;
  }}

  .section-label {{
    font-size: 11px; font-weight: 600; letter-spacing: 0.5px;
    text-transform: uppercase; color: var(--muted); margin: 14px 0 10px;
  }}
  .data-item {{ margin-bottom: 14px; }}
  .data-item:last-child {{ margin-bottom: 0; }}
  .data-item .label {{ margin-bottom: 4px; }}
  .data-item .value {{ font-size: 16px; margin-bottom: 0; }}
  .count-x {{ font-size: 12px; color: var(--muted); font-weight: 500; }}

  .fab {{
    position: fixed; bottom: 24px; right: 20px; width: 48px; height: 48px;
    border-radius: 50%; background: var(--card2); border: 1px solid var(--border);
    color: var(--muted); display: flex; align-items: center; justify-content: center;
    box-shadow: 0 4px 20px rgba(0,0,0,0.25); cursor: pointer; z-index: 40;
  }}

  @media (min-width: 520px) {{ .wrap {{ max-width: 440px; }} }}
  @media print {{
    .topbar, .side-nav, .nav-overlay, .fab {{ display: none !important; }}
    .card-body {{ max-height: none !important; }}
    body {{ background: #fff; color: #000; }}
  }}
</style>
</head>
<body>

<div class="nav-overlay" id="navOverlay" onclick="closeNav()"></div>
<aside class="side-nav" id="sideNav">
  <div class="nav-header">
    <div class="nav-header-title">Навигация</div>
    <button class="nav-close" onclick="closeNav()">
      <svg width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></svg>
    </button>
  </div>
  <div class="nav-search-wrap">
    <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg>
    <input class="nav-search" type="text" placeholder="Поиск по источникам" />
  </div>
  <div class="nav-section-label">Разделы</div>
  <nav class="nav-list">{nav_html}</nav>
</aside>

<div class="topbar">
  <button class="icon-btn" onclick="openNav()" aria-label="Меню">
    <svg width="20" height="20" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M4 6h16M4 12h16M4 18h16"/></svg>
  </button>
  <div class="logo-eye">👁</div>
  <input class="search" type="text" placeholder="Поиск по отчёту" />
  <button class="icon-btn" onclick="toggleTheme()" aria-label="Тема">
    <svg id="themeIcon" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z"/></svg>
  </button>
</div>

<div class="wrap">

  <div class="card open" id="sec-info">
    <div class="card-content" style="padding-top: 18px;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
        <div class="label" style="margin:0;">Информация о запросе</div>
        <span class="status">Завершён</span>
      </div>
      <div style="background:var(--card2); border:1px solid var(--border); border-radius:12px; padding:14px 16px; margin-bottom:16px; font-size:18px; font-weight:700;">{_esc(query)}</div>
      <div class="row-2">
        <div>
          <div class="label">Время запроса</div>
          <div class="value" style="font-size:15px;">{datetime.now().strftime('%d.%m.%Y %H:%M')}</div>
        </div>
        <div>
          <div class="label">Источников</div>
          <div class="value" style="font-size:20px;">{len(blocks)}</div>
        </div>
      </div>
      <div class="label">Записей</div>
      <div class="value" style="font-size:20px; margin-bottom:16px;">{total_records}</div>
      <div class="actions">
        <button class="action-btn" onclick="window.print()">Печать / PDF</button>
      </div>
    </div>
  </div>

  <div class="card open" id="sec-cover">
    <div class="card-head" onclick="toggleCard(this)">
      <div class="card-title">
        <svg class="chevron" viewBox="0 0 12 8" fill="none"><path d="M1 1.5L6 6.5L11 1.5" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>
        Покрытие отчёта
      </div>
      <div class="badges"><span class="badge">{covered_cats}/{total_cats}</span><span class="badge">{percent}%</span></div>
    </div>
    <div class="card-body">
      <div class="card-content">
        <div class="grid-2">{cover_html}</div>
      </div>
    </div>
  </div>

  {phonebooks_html}
  {socials_html}
  {blocks_html}

</div>

<button class="fab" onclick="window.scrollTo({{top:0,behavior:'smooth'}})">
  <svg width="18" height="18" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path d="M18 15l-6-6-6 6"/></svg>
</button>

<script>
function toggleCard(head) {{
  head.parentElement.classList.toggle('open');
}}
function openNav() {{
  document.getElementById('sideNav').classList.add('open');
  document.getElementById('navOverlay').classList.add('open');
  document.body.style.overflow = 'hidden';
}}
function closeNav() {{
  document.getElementById('sideNav').classList.remove('open');
  document.getElementById('navOverlay').classList.remove('open');
  document.body.style.overflow = '';
}}
function toggleTheme() {{
  const html = document.documentElement;
  const next = html.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  html.setAttribute('data-theme', next);
  const icon = document.getElementById('themeIcon');
  if (next === 'light') {{
    icon.innerHTML = '<circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/>';
  }} else {{
    icon.innerHTML = '<path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z"/>';
  }}
}}
</script>
</body>
</html>'''


async def api_get_bot_handler(request):
    """JSON API: основной бот."""
    try:
        me = await bot.get_me()
        return web.json_response({
            "ok": True,
            "bot_link": me.username or "",
        })
    except Exception as e:
        logger.error(f"api_get_bot error: {e}")
        return web.json_response({"ok": False, "bot_link": ""})


async def api_get_mirrors_handler(request):
    """JSON API: список зеркал из БД."""
    try:
        rows = await db_conn.fetchall(
            'SELECT bot_username, created_at FROM mirrors WHERE active = 1 ORDER BY created_at DESC LIMIT 500'
        )
    except Exception as e:
        logger.error(f"api_get_mirrors error: {e}")
        return web.json_response({"ok": False, "sites": []})

    sites = []
    for r in rows:
        uname = (r.get('bot_username') or "").strip()
        if not uname:
            continue
        sites.append({
            "url": f"https://t.me/{uname}",
            "host": f"@{uname}",
            "online": True,
            "latency_ms": 0,
        })

    primary = sites[0]["url"] if sites else None
    return web.json_response({
        "ok": True,
        "sites": sites,
        "primary": primary,
    })


async def generate_referral_code(user_id: int) -> str:
    while True:
        code = f"REF{user_id}{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"
        row = await db_conn.fetchone('SELECT user_id FROM users WHERE referral_code = ?', (code,))
        if not row:
            break
    await db_conn.execute('UPDATE users SET referral_code = ? WHERE user_id = ?', (code, user_id))
    return code


async def get_referral_stats(user_id: int):
    row = await db_conn.fetchone('SELECT COUNT(*) AS cnt FROM referrals WHERE referrer_id = ?', (user_id,))
    invited = row['cnt'] if row else 0
    return invited, invited


async def get_user_available_requests(user_id: int) -> int:
    row = await db_conn.fetchone(
        'SELECT daily_requests, bonus_requests, last_request_date FROM users WHERE user_id = ?', (user_id,)
    )
    if not row:
        return 0
    today = datetime.now().strftime('%Y-%m-%d')
    last = str(row['last_request_date'])[:10] if row['last_request_date'] else ''
    daily = row['daily_requests'] if last == today else 0
    limit = 5 + (row['bonus_requests'] or 0)
    available = limit - daily
    return available if available > 0 else 0


async def use_request(user_id: int) -> bool:
    if await get_user_available_requests(user_id) <= 0:
        return False
    today = datetime.now().strftime('%Y-%m-%d')
    await db_conn.execute(
        'UPDATE users SET daily_requests = daily_requests + 1, last_request_date = ? WHERE user_id = ?',
        (today, user_id)
    )
    return True


async def create_user(user_id: int, username: str = None, referred_by: int = None, mirror_owner_id: int = None):
    await db_conn.execute('INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)', (user_id, username))
    code = await generate_referral_code(user_id)
    actual_referrer = referred_by or mirror_owner_id
    if actual_referrer and actual_referrer != user_id:
        ref = await db_conn.fetchone('SELECT user_id FROM users WHERE user_id = ?', (actual_referrer,))
        if ref:
            try:
                await db_conn.execute(
                    'INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)',
                    (actual_referrer, user_id)
                )
                await db_conn.execute(
                    'UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = ?',
                    (actual_referrer,)
                )
            except Exception:
                pass
    return code


async def get_user(user_id: int):
    return await db_conn.fetchone('SELECT * FROM users WHERE user_id = ?', (user_id,))


async def get_referral_code(user_id: int):
    row = await db_conn.fetchone('SELECT referral_code FROM users WHERE user_id = ?', (user_id,))
    return row['referral_code'] if row else None


async def create_promo_code(code: str, max_uses: int, requests_granted: int, created_by: int) -> bool:
    try:
        await db_conn.execute(
            'INSERT INTO promo_codes (code, max_uses, requests_granted, created_by) VALUES (?, ?, ?, ?)',
            (code, max_uses, requests_granted, created_by)
        )
        return True
    except Exception:
        return False


async def get_promo_code(code: str):
    return await db_conn.fetchone('SELECT * FROM promo_codes WHERE code = ?', (code,))


async def activate_promo_code(user_id: int, code: str):
    row = await db_conn.fetchone('SELECT * FROM promo_codes WHERE code = ?', (code,))
    if not row:
        return False, "Промокод не найден."
    if row['used_count'] >= row['max_uses']:
        return False, "Промокод уже использован максимальное количество раз."
    await db_conn.execute('UPDATE promo_codes SET used_count = used_count + 1 WHERE code = ?', (code,))
    await db_conn.execute(
        'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
        (row['requests_granted'], user_id)
    )
    return True, f"Промокод активирован! Вы получили {row['requests_granted']} дополнительных запросов."


async def get_all_promo_codes():
    return await db_conn.fetchall('SELECT * FROM promo_codes ORDER BY created_at DESC')


async def delete_promo_code(code: str):
    await db_conn.execute('DELETE FROM promo_codes WHERE code = ?', (code,))


async def process_stars_payment(charge_id: str, user_id: int, payload: str):
    data = json.loads(payload)
    temp_invoice_id = data.get("temp_invoice_id")
    requests = data.get("requests", 0)
    purchase = await db_conn.fetchone(
        'SELECT * FROM purchases WHERE invoice_id = ? AND user_id = ?', (temp_invoice_id, user_id)
    )
    if not purchase or purchase['status'] != 'pending':
        return
    await db_conn.execute(
        'UPDATE purchases SET invoice_id = ?, status = ?, confirmed_at = CURRENT_TIMESTAMP WHERE invoice_id = ? AND user_id = ?',
        (charge_id, 'confirmed', temp_invoice_id, user_id)
    )
    await db_conn.execute(
        'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
        (requests, user_id)
    )


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


START_TEXT = (
    "🕵️ dataseeker — твой бесплатный цифровой детектив.\n\n"
    "Типы поиска:\n\n"
    "┌ Контакты:\n"
    "├ Телефон → +79999999999\n"
    "└ Email → ivanov@gmail.com\n\n"
    "┌ Соцсети:\n"
    "├ VK → vk.com/id1234567\n"
    "└ Telegram → @username\n\n"
    "┌ Онлайн-следы:\n"
    "└ IP → 185.85.219.243\n\n"
    "┌ Физ. лица:\n"
    "├ ИНН → /inn 123456789012\n"
    "└ ФИО → Иванов Иван Иванович\n\n"
    "Каждые 24 часа выдаётся по 5 бесплатных запросов."
)


def detect_type(text: str):
    t = text.strip()
    cleaned = re.sub(r'\s+', '', t)

    if re.match(r'^@[a-zA-Z0-9_]{5,32}$', t):
        return "tg_id"

    if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', t):
        return "email"

    if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', cleaned):
        return "ip"

    if re.search(r'vk\.com/', t, re.IGNORECASE):
        return "vk"

    if re.match(r'^\+?\d{10,15}$', cleaned):
        return "phone"

    words = t.split()
    if len(words) >= 2 and all(re.match(r'^[А-Яа-яЁё\-]+$', w) for w in words):
        return "fio"

    return None


def register_mirror_handlers(mirror_dp: Dispatcher, mirror_bot: Bot, owner_id: int):

    @mirror_dp.message(Command("start"))
    async def mirror_start(message: Message):
        user_id = message.from_user.id
        username = message.from_user.username
        user = await get_user(user_id)
        referrer_id = None
        args = message.text.split()
        if len(args) > 1:
            payload = args[1]
            ref_code = payload[4:] if payload.startswith("ref_") else (payload[3:] if payload.startswith("ref") else None)
            if ref_code:
                row = await db_conn.fetchone('SELECT user_id FROM users WHERE referral_code = ?', (ref_code,))
                if row and row['user_id'] != user_id:
                    referrer_id = row['user_id']
        if not user:
            await create_user(user_id, username, referrer_id, mirror_owner_id=owner_id)
            if referrer_id:
                try:
                    await mirror_bot.send_message(referrer_id, f"По вашей ссылке пришёл @{username or 'пользователь'}, +1 запрос.")
                except Exception:
                    pass

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Мой профиль", callback_data="m_my_profile")],
            [InlineKeyboardButton(text="Реферальная система", callback_data="m_referral_system")],
            [InlineKeyboardButton(text="Создать зеркало", callback_data="m_mirror_info")],
            [InlineKeyboardButton(text="Пополнить запросы", callback_data="m_buy_requests")],
            [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")],
        ])
        await message.reply(START_TEXT, reply_markup=keyboard)

    @mirror_dp.message(Command("add_mirror"))
    async def mirror_add_mirror_cmd(message: Message):
        user_id = message.from_user.id
        if not await get_user(user_id):
            await create_user(user_id, message.from_user.username, mirror_owner_id=owner_id)
        await message.reply(
            "Отправь токен бота от @BotFather.\n\n"
            "Формат: <code>1234567890:ABCdef...</code>\n\n"
            "Инструкция:\n"
            "1. @BotFather → /newbot\n"
            "2. Имя бота\n"
            "3. Username (заканчивается на bot)\n"
            "4. Скопируй токен → отправь сюда",
            parse_mode="HTML"
        )

    @mirror_dp.message(F.text.regexp(MIRROR_TOKEN_RE.pattern), StateFilter(None))
    async def mirror_catch_token(message: Message):
        token = message.text.strip()
        user_id = message.from_user.id

        if token in active_mirrors:
            await message.reply("❌ Это зеркало уже запущено.")
            return

        mirrors_row = await db_conn.fetchone(
            'SELECT COUNT(*) AS cnt FROM mirrors WHERE owner_id = ?', (user_id,)
        )
        mirrors_count = mirrors_row['cnt'] if mirrors_row else 0
        if mirrors_count >= MAX_MIRRORS_PER_USER:
            existing = await db_conn.fetchone(
                'SELECT bot_username FROM mirrors WHERE owner_id = ? LIMIT 1', (user_id,)
            )
            existing_username = existing['bot_username'] if existing else "твой бот"
            await message.reply(
                f"❌ У тебя уже есть зеркало: @{existing_username}\n\n"
                f"Одно зеркало на аккаунт."
            )
            return

        session = await get_http_session()
        try:
            async with session.get(f"https://api.telegram.org/bot{token}/getMe") as resp:
                if resp.status != 200:
                    await message.reply("❌ Токен недействителен.")
                    return
                data = await resp.json()
                if not data.get("ok"):
                    await message.reply("❌ Токен недействителен.")
                    return
                bot_username = data["result"]["username"]
        except Exception as e:
            logger.error(f"mirror token check failed: {e}")
            await message.reply("❌ Не удалось проверить токен.")
            return

        existing = await db_conn.fetchone('SELECT bot_token FROM mirrors WHERE bot_token = ?', (token,))
        if existing:
            await message.reply("❌ Этот токен уже добавлен.")
            return

        try:
            await db_conn.execute(
                'INSERT INTO mirrors (owner_id, bot_token, bot_username) VALUES (?, ?, ?)',
                (user_id, token, bot_username)
            )
        except Exception as e:
            logger.error(f"mirror insert failed: {e}")
            await message.reply("❌ Не удалось сохранить токен.")
            return

        ok, err = await start_mirror(token, user_id)
        if not ok:
            await message.reply(f"❌ {err}")
            return

        await db_conn.execute(
            'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
            (BONUS_PER_MIRROR, user_id)
        )

        await message.reply(
            f"✅ Зеркало @{bot_username} запущено!\n\n"
            f"Ссылка: https://t.me/{bot_username}\n"
            f"Все юзеры этого бота будут твоими рефералами (+1 запрос за каждого).\n\n"
            f"🎁 +{BONUS_PER_MIRROR} бонусный запрос начислен."
        )

    @mirror_dp.message(Command("my_mirrors"))
    async def mirror_my_mirrors_cmd(message: Message):
        rows = await db_conn.fetchall(
            'SELECT bot_username, active, created_at FROM mirrors WHERE owner_id = ? ORDER BY created_at DESC',
            (message.from_user.id,)
        )
        if not rows:
            await message.reply("У тебя пока нет зеркал. Создай через /add_mirror")
            return
        text = "🔗 <b>Твои зеркала:</b>\n\n"
        for r in rows:
            status = "✅" if r['active'] else "⏸"
            text += f"{status} @{r['bot_username']} — https://t.me/{r['bot_username']}\n"
        await message.reply(text, parse_mode="HTML", disable_web_page_preview=True)

    @mirror_dp.message(Command("inn"))
    async def mirror_inn_cmd(message: Message):
        args = message.text.split()
        if len(args) < 2:
            await message.reply("Укажите ИНН: `/inn 123456789012`", parse_mode="Markdown")
            return
        inn = args[1].strip()
        if not re.match(r'^\d{10,12}$', inn):
            await message.reply("Неверный формат ИНН.")
            return
        await mirror_process_query(message, inn, "inn")

    @mirror_dp.message(Command("id"))
    async def mirror_id_cmd(message: Message):
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply(
                "Укажите @username:\n"
                "`/id @username`",
                parse_mode="Markdown"
            )
            return

        target = args[1].strip().lstrip('@')

        if not re.match(r'^[a-zA-Z0-9_]{5,32}$', target):
            await message.reply("Ник: 5–32 латинских буквы, цифры или _. ID не поддерживается.")
            return

        await mirror_process_id_query(message, target)

    async def mirror_process_id_query(message: Message, target: str):
        user_id = message.from_user.id
        user = await get_user(user_id)
        if not user:
            await create_user(user_id, message.from_user.username, mirror_owner_id=owner_id)
        if await get_user_available_requests(user_id) <= 0:
            await message.reply("Лимит запросов исчерпан.")
            return

        status = await message.reply("🔍 Поиск по Telegram...")

        try:
            data = await collect_general_data(target, "tg_id")
            base_text = build_funstat_preview(data, target)
        except Exception:
            logger.exception("mirror collect_general_data")
            base_text = ""
            data = {}

        try:
            gifts_data = await fetch_gifts_data(target)
            gifts_text = format_gifts_message(gifts_data, target)
        except Exception:
            logger.exception("mirror fetch_gifts_data")
            gifts_text = ""

        parts = []
        if base_text and not base_text.startswith("❌"):
            parts.append(base_text)
        if gifts_text and gifts_text not in ("Подарков не найдено.", ""):
            parts.append(gifts_text)

        if not parts:
            try:
                await status.edit("❌ По этому Telegram ничего не найдено.")
            except Exception:
                await message.reply("❌ По этому Telegram ничего не найдено.")
            await use_request(user_id)
            return

        full_text = "\n\n".join(parts)

        rid = data.get("report_id") if isinstance(data, dict) else None
        kb = None
        if rid:
            base_url = _public_base()
            url = f"{base_url}/r/{rid}"
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"📄 Открыть полный отчёт ({data.get('records_count', 0)} шт)",
                    url=url
                )],
            ])

        try:
            await status.delete()
        except Exception:
            pass

        try:
            await message.reply(full_text, parse_mode="HTML",
                                reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            await message.reply(full_text)

        await use_request(user_id)

    async def mirror_process_query(message: Message, query: str, search_type: str):
        user_id = message.from_user.id
        user = await get_user(user_id)
        if not user:
            await create_user(user_id, message.from_user.username, mirror_owner_id=owner_id)
        if await get_user_available_requests(user_id) <= 0:
            await message.reply("Лимит запросов исчерпан.")
            return

        status = await message.reply(f"🔍 Поиск...")
        try:
            data = await collect_general_data(query, search_type)

            if data.get('records_count', 0) == 0 and not data.get('blocks'):
                try:
                    await status.edit_text(
                        f"❌ По `{query}` ничего не найдено.",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
                await use_request(user_id)
                return

            preview = build_preview(data)

            rid = data.get('report_id')
            base = _public_base()
            if rid:
                url = f"{base}/r/{rid}"
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(
                        text=f"📄 Открыть полный отчёт ({data.get('records_count', 0)} шт)",
                        url=url
                    )],
                ])
            else:
                kb = None

            try:
                await status.delete()
            except Exception:
                pass
            await message.reply(preview, parse_mode="HTML", reply_markup=kb,
                                disable_web_page_preview=True)
            await use_request(user_id)
        except Exception as e:
            logger.exception("mirror_process_query error")
            try:
                await status.edit_text(f"❌ Ошибка: {e}")
            except Exception:
                pass

    @mirror_dp.message(lambda m: m.text and not m.text.startswith('/'), StateFilter(None))
    async def mirror_universal(message: Message):
        text = message.text.strip()
        t = text.lstrip('@') if text.startswith('@') else text
        stype = detect_type(text)

        if stype is None:
            await message.reply(START_TEXT)
            return

        if stype == "tg_id":
            await mirror_process_id_query(message, t)
        else:
            await mirror_process_query(message, text, stype)

    @mirror_dp.callback_query(lambda c: c.data == "m_mirror_info")
    async def m_mirror_info_cb(cb: CallbackQuery):
        creator_id = cb.from_user.id
        mirrors_row = await db_conn.fetchone(
            'SELECT COUNT(*) AS cnt FROM mirrors WHERE owner_id = ?', (creator_id,)
        )
        mirrors_count = mirrors_row['cnt'] if mirrors_row else 0

        if mirrors_count >= MAX_MIRRORS_PER_USER:
            text = (
                "🔗 <b>Создание зеркала</b>\n\n"
                "❌ <b>У тебя уже есть зеркало. Одно зеркало на аккаунт.</b>\n\n"
                "Чтобы заменить — напиши админу, он удалит старое."
            )
        else:
            text = (
                "🔗 <b>Создание зеркала</b>\n\n"
                f"Зеркало — это твоя копия бота под своим @username.\n"
                f"Все юзеры зеркала становятся твоими рефералами.\n"
                f"За создание зеркала даётся +{BONUS_PER_MIRROR} бонусный запрос.\n\n"
                f"<b>Создано зеркал: {mirrors_count}/{MAX_MIRRORS_PER_USER}</b>\n\n"
                "<b>Как создать:</b>\n"
                "1. Открой @BotFather\n"
                "2. Отправь /newbot\n"
                "3. Придумай имя и username (заканчивается на bot)\n"
                "4. Скопируй токен вида <code>123456:ABC-DEF...</code>\n"
                "5. Отправь токен сюда в чат"
            )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Открыть @BotFather", url="https://t.me/BotFather")],
            [InlineKeyboardButton(text="Мои зеркала", callback_data="m_my_mirrors")],
            [InlineKeyboardButton(text="Назад", callback_data="m_back_to_menu")],
        ])
        try:
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            try:
                await cb.message.answer(text, reply_markup=kb, parse_mode="HTML")
            except Exception:
                pass
        await cb.answer()

    @mirror_dp.callback_query(lambda c: c.data == "m_my_mirrors")
    async def m_my_mirrors_cb(cb: CallbackQuery):
        rows = await db_conn.fetchall(
            'SELECT bot_username, active, created_at FROM mirrors WHERE owner_id = ? ORDER BY created_at DESC',
            (cb.from_user.id,)
        )
        if not rows:
            text = "У тебя пока нет зеркал."
        else:
            text = "🔗 <b>Твои зеркала:</b>\n\n"
            for r in rows:
                status = "✅" if r['active'] else "⏸"
                text += f"{status} @{r['bot_username']} — https://t.me/{r['bot_username']}\n"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="m_mirror_info")]
        ])
        try:
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML",
                                       disable_web_page_preview=True)
        except Exception:
            pass
        await cb.answer()

    @mirror_dp.callback_query(lambda c: c.data == "m_my_profile")
    async def m_my_profile_cb(cb: CallbackQuery):
        user_id = cb.from_user.id
        user = await get_user(user_id)
        if not user:
            await cb.answer("Не зарегистрированы.")
            return
        available = await get_user_available_requests(user_id)
        text = f"Ваш профиль\nID: {user_id}\nДоступно запросов: {available}\nРегистрация: {user['created_at']}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Ввести промокод", callback_data="m_enter_promo")],
            [InlineKeyboardButton(text="Назад", callback_data="m_back_to_menu")],
        ])
        try:
            await cb.message.edit_text(text, reply_markup=kb)
        except Exception:
            pass
        await cb.answer()

    @mirror_dp.callback_query(lambda c: c.data == "m_referral_system")
    async def m_referral_system_cb(cb: CallbackQuery):
        user_id = cb.from_user.id
        invited, bonuses = await get_referral_stats(user_id)
        code = await get_referral_code(user_id)
        bot_username = (await mirror_bot.get_me()).username
        text = (f"Реферальная система\n\n+1 запрос за каждого друга.\n"
                f"Ссылка:\nhttps://t.me/{bot_username}?start=ref{code}\n\n"
                f"Приглашено: {invited}\nБонусов: {bonuses}")
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="m_back_to_menu")]])
        try:
            await cb.message.edit_text(text, reply_markup=kb)
        except Exception:
            pass
        await cb.answer()

    @mirror_dp.callback_query(lambda c: c.data == "m_buy_requests")
    async def m_buy_requests_cb(cb: CallbackQuery):
        buttons = []
        for pkg in PACKAGES:
            if pkg["requests"] == 1000:
                buttons.append([InlineKeyboardButton(
                    text=f"Выгодный · {pkg['requests']} запр. · {pkg['stars']}⭐",
                    callback_data=f"m_pkg_{pkg['requests']}_{pkg['usd']}_{pkg['stars']}"
                )])
            else:
                buttons.append(InlineKeyboardButton(
                    text=f"{pkg['requests']} запр. · {pkg['stars']}⭐",
                    callback_data=f"m_pkg_{pkg['requests']}_{pkg['usd']}_{pkg['stars']}"
                ))
        rows, row = [], []
        for btn in buttons:
            if isinstance(btn, list):
                if row: rows.append(row); row = []
                rows.append(btn)
            else:
                row.append(btn)
                if len(row) == 2: rows.append(row); row = []
        if row: rows.append(row)
        rows.append([InlineKeyboardButton(text="Назад", callback_data="m_back_to_menu")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
        try:
            await cb.message.edit_text("Выбери тариф:\n\nЧем больше пакет — тем дешевле запрос.", reply_markup=kb)
        except Exception:
            pass
        await cb.answer()

    @mirror_dp.callback_query(lambda c: c.data and c.data.startswith("m_pkg_"))
    async def m_pkg_cb(cb: CallbackQuery):
        parts = cb.data.split("_")
        rq, usd, stars = int(parts[2]), float(parts[3]), int(parts[4])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Оплатить {stars}⭐", callback_data=f"m_pay_{rq}_{stars}")],
            [InlineKeyboardButton(text="Назад", callback_data="m_buy_requests")],
        ])
        try:
            await cb.message.edit_text(f"Пакет: {rq} запросов\nЦена: {stars}⭐\n\nОплата через Telegram Stars.", reply_markup=kb)
        except Exception:
            pass
        await cb.answer()

    @mirror_dp.callback_query(lambda c: c.data and c.data.startswith("m_pay_"))
    async def m_pay_cb(cb: CallbackQuery):
        parts = cb.data.split("_")
        rq, stars = int(parts[2]), int(parts[3])
        user_id = cb.from_user.id
        temp_invoice_id = f"stars_{user_id}_{int(datetime.now().timestamp())}"
        await db_conn.execute(
            'INSERT INTO purchases (user_id, invoice_id, amount, currency, requests, status) VALUES (?, ?, ?, ?, ?, ?)',
            (user_id, temp_invoice_id, stars, 'XTR', rq, 'pending')
        )
        prices = [LabeledPrice(label=f"{rq} запросов", amount=stars)]
        try:
            await mirror_bot.send_invoice(
                chat_id=user_id,
                title=f"Пополнение: {rq} запросов",
                description=f"Вы получаете {rq} дополнительных запросов.",
                provider_token="", currency="XTR", prices=prices,
                start_parameter=f"stars_{user_id}_{int(datetime.now().timestamp())}",
                payload=json.dumps({"user_id": user_id, "requests": rq, "temp_invoice_id": temp_invoice_id}),
            )
            await cb.answer("Счёт создан. Оплатите в Telegram.")
        except Exception as e:
            logger.error(f"mirror stars invoice error: {e}")
            await cb.answer("Ошибка создания счёта.", show_alert=True)
            await db_conn.execute('DELETE FROM purchases WHERE invoice_id = ?', (temp_invoice_id,))

    @mirror_dp.pre_checkout_query()
    async def m_pre_checkout(q: PreCheckoutQuery):
        await q.answer(ok=True)

    @mirror_dp.message(lambda m: m.successful_payment is not None)
    async def m_success_payment(message: Message):
        sp = message.successful_payment
        payload = sp.invoice_payload or ""
        try:
            data = json.loads(payload)
        except Exception:
            return
        temp_invoice_id = data.get("temp_invoice_id")
        requests = data.get("requests", 0)
        purchase = await db_conn.fetchone(
            'SELECT * FROM purchases WHERE invoice_id = ? AND user_id = ?',
            (temp_invoice_id, message.from_user.id)
        )
        if not purchase or purchase['status'] != 'pending':
            return
        await db_conn.execute(
            'UPDATE purchases SET invoice_id = ?, status = ?, confirmed_at = CURRENT_TIMESTAMP WHERE invoice_id = ? AND user_id = ?',
            (sp.telegram_payment_charge_id, 'confirmed', temp_invoice_id, message.from_user.id)
        )
        await db_conn.execute(
            'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
            (requests, message.from_user.id)
        )
        try:
            await mirror_bot.send_message(message.from_user.id, f"Оплата Stars подтверждена! Начислено {requests} запросов.")
        except Exception:
            pass

    @mirror_dp.callback_query(lambda c: c.data == "m_enter_promo")
    async def m_enter_promo_cb(cb: CallbackQuery, state: FSMContext):
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="m_promo_back")]])
        await cb.message.edit_text("Введите промокод:", reply_markup=kb)
        await state.set_state(EnterPromo.waiting_for_code)
        await cb.answer()

    @mirror_dp.callback_query(lambda c: c.data == "m_promo_back")
    async def m_promo_back_cb(cb: CallbackQuery, state: FSMContext):
        await state.clear()
        await m_my_profile_cb(cb)

    @mirror_dp.message(EnterPromo.waiting_for_code)
    async def m_enter_promo_process(message: Message, state: FSMContext):
        code = message.text.strip()
        ok, msg = await activate_promo_code(message.from_user.id, code)
        await message.reply(msg)
        await state.clear()

    @mirror_dp.callback_query(lambda c: c.data == "m_back_to_menu")
    async def m_back_to_menu_cb(cb: CallbackQuery):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Мой профиль", callback_data="m_my_profile")],
            [InlineKeyboardButton(text="Реферальная система", callback_data="m_referral_system")],
            [InlineKeyboardButton(text="Создать зеркало", callback_data="m_mirror_info")],
            [InlineKeyboardButton(text="Пополнить запросы", callback_data="m_buy_requests")],
            [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")],
        ])
        try:
            await cb.message.edit_text("Выберите действие:", reply_markup=keyboard)
        except Exception:
            pass
        await cb.answer()


async def start_mirror(token: str, owner_id: int):
    if token in active_mirrors:
        return False, "Уже запущено"

    mirror_bot = Bot(token=token)
    mirror_dp = Dispatcher(storage=MemoryStorage())
    register_mirror_handlers(mirror_dp, mirror_bot, owner_id)

    task = asyncio.create_task(mirror_dp.start_polling(mirror_bot, skip_updates=True))
    active_mirrors[token] = {"bot": mirror_bot, "dp": mirror_dp, "task": task}
    logger.info(f"mirror started: {token[:10]}... owner={owner_id}")
    return True, "OK"


async def restore_mirrors():
    try:
        if db_conn.backend == "sqlite":
            rows = await db_conn.fetchall('SELECT bot_token, owner_id FROM mirrors WHERE active = 1')
        else:
            rows = await db_conn.fetchall('SELECT bot_token, owner_id FROM mirrors WHERE active = TRUE')
    except Exception as e:
        logger.error(f"restore_mirrors db error: {e}")
        return
    for r in rows:
        try:
            await start_mirror(r['bot_token'], r['owner_id'])
        except Exception as e:
            logger.error(f"mirror restore failed: {e}")


@dp.message(Command("start"))
async def start_cmd(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    user = await get_user(user_id)
    referrer_id = None
    args = message.text.split()
    if len(args) > 1:
        payload = args[1]
        ref_code = payload[4:] if payload.startswith("ref_") else (payload[3:] if payload.startswith("ref") else None)
        if ref_code:
            row = await db_conn.fetchone('SELECT user_id FROM users WHERE referral_code = ?', (ref_code,))
            if row and row['user_id'] != user_id:
                referrer_id = row['user_id']
    if not user:
        await create_user(user_id, username, referrer_id)
        if referrer_id:
            try:
                await bot.send_message(referrer_id, f"По вашей ссылке пришёл @{username or 'пользователь'}, +1 запрос.")
            except Exception:
                pass

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Создать зеркало", callback_data="mirror_info")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")],
    ])
    await message.reply(START_TEXT, reply_markup=keyboard)


@dp.message(Command("inn"))
async def inn_cmd(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Укажите ИНН: `/inn 123456789012`", parse_mode="Markdown")
        return
    inn = args[1].strip()
    if not re.match(r'^\d{10,12}$', inn):
        await message.reply("Неверный формат ИНН.")
        return
    await process_general_query(message, inn, "inn")


@dp.message(Command("id"))
async def id_cmd(message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply(
            "Укажите @username:\n"
            "`/id @username`",
            parse_mode="Markdown"
        )
        return

    target = args[1].strip().lstrip('@')

    if not re.match(r'^[a-zA-Z0-9_]{5,32}$', target):
        await message.reply("Ник: 5–32 латинских буквы, цифры или _. ID не поддерживается.")
        return

    await process_id_query(message, target)


async def process_id_query(message: Message, target: str):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await create_user(user_id, message.from_user.username)
    if await get_user_available_requests(user_id) <= 0:
        await message.reply("Лимит запросов исчерпан.")
        return

    status = await message.reply("🔍 Поиск по Telegram...")

    try:
        data = await collect_general_data(target, "tg_id")
        base_text = build_funstat_preview(data, target)
    except Exception:
        logger.exception("collect_general_data in /id")
        base_text = ""
        data = {}

    try:
        gifts_data = await fetch_gifts_data(target)
        gifts_text = format_gifts_message(gifts_data, target)
    except Exception:
        logger.exception("fetch_gifts_data in /id")
        gifts_text = ""

    parts = []
    if base_text and not base_text.startswith("❌"):
        parts.append(base_text)
    if gifts_text and gifts_text not in ("Подарков не найдено.", ""):
        parts.append(gifts_text)

    if not parts:
        try:
            await status.edit("❌ По этому Telegram ничего не найдено.")
        except Exception:
            await message.reply("❌ По этому Telegram ничего не найдено.")
        await use_request(user_id)
        return

    full_text = "\n\n".join(parts)

    rid = data.get("report_id") if isinstance(data, dict) else None
    kb = None
    if rid:
        base_url = _public_base()
        url = f"{base_url}/r/{rid}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"📄 Открыть полный отчёт ({data.get('records_count', 0)} шт)",
                url=url
            )],
        ])

    try:
        await status.delete()
    except Exception:
        pass

    try:
        await message.reply(full_text, parse_mode="HTML",
                            reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        await message.reply(full_text)

    await use_request(user_id)


async def process_general_query(message: Message, query: str, search_type: str):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await create_user(user_id, message.from_user.username)
    if await get_user_available_requests(user_id) <= 0:
        await message.reply("Лимит запросов исчерпан.")
        return

    status = await message.reply(f"🔍 Поиск...")
    try:
        data = await collect_general_data(query, search_type)

        if data.get('records_count', 0) == 0 and not data.get('blocks'):
            try:
                await status.edit_text(
                    f"❌ По `{query}` ничего не найдено.",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
            await use_request(user_id)
            return

        preview = build_preview(data)

        rid = data.get('report_id')
        base = _public_base()
        if rid:
            url = f"{base}/r/{rid}"
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"📄 Открыть полный отчёт ({data.get('records_count', 0)} шт)",
                    url=url
                )],
            ])
        else:
            kb = None

        try:
            await status.delete()
        except Exception:
            pass
        await message.reply(preview, parse_mode="HTML", reply_markup=kb,
                            disable_web_page_preview=True)
        await use_request(user_id)
    except Exception as e:
        logger.exception("process_general_query error")
        try:
            await status.edit_text(f"❌ Ошибка: {e}")
        except Exception:
            pass


@dp.message(F.text.regexp(MIRROR_TOKEN_RE.pattern), StateFilter(None))
async def catch_mirror_token(message: Message):
    token = message.text.strip()
    owner_id = message.from_user.id

    if token in active_mirrors:
        await message.reply("❌ Это зеркало уже запущено.")
        return

    mirrors_row = await db_conn.fetchone(
        'SELECT COUNT(*) AS cnt FROM mirrors WHERE owner_id = ?', (owner_id,)
    )
    mirrors_count = mirrors_row['cnt'] if mirrors_row else 0
    if mirrors_count >= MAX_MIRRORS_PER_USER:
        existing = await db_conn.fetchone(
            'SELECT bot_username FROM mirrors WHERE owner_id = ? LIMIT 1', (owner_id,)
        )
        existing_username = existing['bot_username'] if existing else "твой бот"
        await message.reply(
            f"❌ У тебя уже есть зеркало: @{existing_username}\n\n"
            f"Одно зеркало на аккаунт. Удалить старое и создать новое — напиши админу."
        )
        return

    session = await get_http_session()
    try:
        async with session.get(f"https://api.telegram.org/bot{token}/getMe") as resp:
            if resp.status != 200:
                await message.reply("❌ Токен недействителен.")
                return
            data = await resp.json()
            if not data.get("ok"):
                await message.reply("❌ Токен недействителен.")
                return
            bot_username = data["result"]["username"]
    except Exception as e:
        logger.error(f"token check failed: {e}")
        await message.reply("❌ Не удалось проверить токен.")
        return

    existing = await db_conn.fetchone('SELECT bot_token FROM mirrors WHERE bot_token = ?', (token,))
    if existing:
        await message.reply("❌ Этот токен уже добавлен.")
        return

    try:
        await db_conn.execute(
            'INSERT INTO mirrors (owner_id, bot_token, bot_username) VALUES (?, ?, ?)',
            (owner_id, token, bot_username)
        )
    except Exception as e:
        logger.error(f"mirror insert failed: {e}")
        await message.reply("❌ Не удалось сохранить токен.")
        return

    ok, err = await start_mirror(token, owner_id)
    if not ok:
        await message.reply(f"❌ {err}")
        return

    await db_conn.execute(
        'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
        (BONUS_PER_MIRROR, owner_id)
    )

    await message.reply(
        f"✅ Зеркало @{bot_username} запущено!\n\n"
        f"Ссылка: https://t.me/{bot_username}\n"
        f"Все юзеры этого бота будут твоими рефералами (+1 запрос за каждого).\n\n"
        f"🎁 +{BONUS_PER_MIRROR} бонусный запрос начислен за создание зеркала."
    )


@dp.message(lambda msg: msg.text and not msg.text.startswith('/'), StateFilter(None))
async def universal_handler(message: Message):
    text = message.text.strip()
    t = text.lstrip('@') if text.startswith('@') else text
    stype = detect_type(text)

    if stype is None:
        await message.reply(START_TEXT)
        return

    if stype == "tg_id":
        await process_id_query(message, t)
    else:
        await process_general_query(message, text, stype)


@dp.pre_checkout_query()
async def pre_checkout_handler(q: PreCheckoutQuery):
    await q.answer(ok=True)


@dp.message(lambda m: m.successful_payment is not None)
async def success_payment_handler(message: Message):
    sp = message.successful_payment
    payload = sp.invoice_payload or ""
    try:
        json.loads(payload)
    except Exception:
        return
    await process_stars_payment(sp.telegram_payment_charge_id, message.from_user.id, payload)


@dp.callback_query(lambda c: c.data == "my_profile")
async def my_profile_cb(cb: CallbackQuery):
    user_id = cb.from_user.id
    user = await get_user(user_id)
    if not user:
        await cb.answer("Не зарегистрированы.")
        return
    available = await get_user_available_requests(user_id)
    mirrors_row = await db_conn.fetchone('SELECT COUNT(*) AS cnt FROM mirrors WHERE owner_id = ?', (user_id,))
    mirrors_count = mirrors_row['cnt'] if mirrors_row else 0
    text = (
        f"Ваш профиль\n"
        f"ID: {user_id}\n"
        f"Доступно запросов: {available}\n"
        f"Зеркал: {mirrors_count}\n"
        f"Регистрация: {user['created_at']}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ввести промокод", callback_data="enter_promo")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(lambda c: c.data == "referral_system")
async def referral_system_cb(cb: CallbackQuery):
    user_id = cb.from_user.id
    invited, bonuses = await get_referral_stats(user_id)
    code = await get_referral_code(user_id)
    bot_username = (await bot.get_me()).username
    text = (f"Реферальная система\n\n+1 запрос за каждого друга.\n"
            f"Ссылка:\nhttps://t.me/{bot_username}?start=ref{code}\n\n"
            f"Приглашено: {invited}\nБонусов: {bonuses}")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(lambda c: c.data == "buy_requests")
async def buy_requests_cb(cb: CallbackQuery):
    buttons = []
    for pkg in PACKAGES:
        if pkg["requests"] == 1000:
            buttons.append([InlineKeyboardButton(
                text=f"Выгодный · {pkg['requests']} запр. · {pkg['stars']}⭐",
                callback_data=f"pkg_{pkg['requests']}_{pkg['usd']}_{pkg['stars']}"
            )])
        else:
            buttons.append(InlineKeyboardButton(
                text=f"{pkg['requests']} запр. · {pkg['stars']}⭐",
                callback_data=f"pkg_{pkg['requests']}_{pkg['usd']}_{pkg['stars']}"
            ))
    rows, row = [], []
    for btn in buttons:
        if isinstance(btn, list):
            if row: rows.append(row); row = []
            rows.append(btn)
        else:
            row.append(btn)
            if len(row) == 2: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="Назад", callback_data="back_to_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await cb.message.edit_text("Выбери тариф:\n\nЧем больше пакет — тем дешевле запрос.", reply_markup=kb)
    await cb.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith("pkg_"))
async def pkg_selected_cb(cb: CallbackQuery):
    parts = cb.data.split("_")
    rq, usd, stars = int(parts[1]), float(parts[2]), int(parts[3])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Оплатить {stars}⭐", callback_data=f"pay_stars_{rq}_{stars}")],
        [InlineKeyboardButton(text="Назад", callback_data="buy_requests")],
    ])
    await cb.message.edit_text(f"Пакет: {rq} запросов\nЦена: {stars}⭐\n\nОплата через Telegram Stars.", reply_markup=kb)
    await cb.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith("pay_stars_"))
async def pay_stars_cb(cb: CallbackQuery):
    parts = cb.data.split("_")
    rq, stars = int(parts[2]), int(parts[3])
    user_id = cb.from_user.id
    temp_invoice_id = f"stars_{user_id}_{int(datetime.now().timestamp())}"
    await db_conn.execute(
        'INSERT INTO purchases (user_id, invoice_id, amount, currency, requests, status) VALUES (?, ?, ?, ?, ?, ?)',
        (user_id, temp_invoice_id, stars, 'XTR', rq, 'pending')
    )
    prices = [LabeledPrice(label=f"{rq} запросов", amount=stars)]
    try:
        await bot.send_invoice(
            chat_id=user_id,
            title=f"Пополнение: {rq} запросов",
            description=f"Вы получаете {rq} дополнительных запросов.",
            provider_token="", currency="XTR", prices=prices,
            start_parameter=f"stars_{user_id}_{int(datetime.now().timestamp())}",
            payload=json.dumps({"user_id": user_id, "requests": rq, "temp_invoice_id": temp_invoice_id}),
        )
        await cb.answer("Счёт создан. Оплатите в Telegram.")
    except Exception as e:
        logger.error(f"stars invoice error: {e}")
        await cb.answer("Ошибка создания счёта.", show_alert=True)
        await db_conn.execute('DELETE FROM purchases WHERE invoice_id = ?', (temp_invoice_id,))


@dp.callback_query(lambda c: c.data == "enter_promo")
async def enter_promo_cb(cb: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="promo_back")]])
    await cb.message.edit_text("Введите промокод:", reply_markup=kb)
    await state.set_state(EnterPromo.waiting_for_code)
    await cb.answer()


@dp.callback_query(lambda c: c.data == "promo_back")
async def promo_back_cb(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await my_profile_cb(cb)


@dp.message(EnterPromo.waiting_for_code)
async def enter_promo_process(message: Message, state: FSMContext):
    code = message.text.strip()
    ok, msg = await activate_promo_code(message.from_user.id, code)
    await message.reply(msg)
    await state.clear()


@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu_cb(cb: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Создать зеркало", callback_data="mirror_info")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")],
    ])
    await cb.message.edit_text("Выберите действие:", reply_markup=kb)
    await cb.answer()


@dp.callback_query(lambda c: c.data == "mirror_info")
async def mirror_info_cb(cb: CallbackQuery):
    mirrors_row = await db_conn.fetchone(
        'SELECT COUNT(*) AS cnt FROM mirrors WHERE owner_id = ?', (cb.from_user.id,)
    )
    mirrors_count = mirrors_row['cnt'] if mirrors_row else 0

    if mirrors_count >= MAX_MIRRORS_PER_USER:
        text = (
            "🔗 <b>Создание зеркала</b>\n\n"
            "❌ <b>У тебя уже есть зеркало. Одно зеркало на аккаунт.</b>\n\n"
            "Чтобы заменить — напиши админу, он удалит старое."
        )
    else:
        text = (
            "🔗 <b>Создание зеркала</b>\n\n"
            f"Зеркало — это твоя копия бота под своим @username.\n"
            f"Все юзеры зеркала становятся твоими рефералами.\n"
            f"За создание зеркала даётся +{BONUS_PER_MIRROR} бонусный запрос.\n\n"
            f"<b>Создано зеркал: {mirrors_count}/{MAX_MIRRORS_PER_USER}</b>\n\n"
            "<b>Как создать:</b>\n"
            "1. Открой @BotFather\n"
            "2. Отправь /newbot\n"
            "3. Придумай имя и username (заканчивается на bot)\n"
            "4. Скопируй токен вида <code>123456:ABC-DEF...</code>\n"
            "5. Отправь токен сюда в чат"
        )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть @BotFather", url="https://t.me/BotFather")],
        [InlineKeyboardButton(text="Мои зеркала", callback_data="my_mirrors")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")],
    ])
    try:
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await cb.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()


@dp.callback_query(lambda c: c.data == "my_mirrors")
async def my_mirrors_cb(cb: CallbackQuery):
    rows = await db_conn.fetchall(
        'SELECT bot_username, active, created_at FROM mirrors WHERE owner_id = ? ORDER BY created_at DESC',
        (cb.from_user.id,)
    )
    if not rows:
        text = "У тебя пока нет зеркал."
    else:
        text = "🔗 <b>Твои зеркала:</b>\n\n"
        for r in rows:
            status = "✅" if r['active'] else "⏸"
            text += f"{status} @{r['bot_username']} — https://t.me/{r['bot_username']}\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="mirror_info")]])
    try:
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    except Exception:
        await cb.message.answer(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    await cb.answer()


@dp.message(Command("add_mirror"))
async def add_mirror_cmd(message: Message):
    user_id = message.from_user.id
    if not await get_user(user_id):
        await create_user(user_id, message.from_user.username)
    await message.reply(
        "Отправь токен бота от @BotFather.\n\n"
        "Формат: <code>1234567890:ABCdef...</code>\n\n"
        "Инструкция:\n"
        "1. @BotFather → /newbot\n"
        "2. Имя бота\n"
        "3. Username (заканчивается на bot)\n"
        "4. Скопируй токен → отправь сюда",
        parse_mode="HTML"
    )


@dp.message(Command("my_mirrors"))
async def my_mirrors_cmd(message: Message):
    rows = await db_conn.fetchall(
        'SELECT bot_username, active, created_at FROM mirrors WHERE owner_id = ? ORDER BY created_at DESC',
        (message.from_user.id,)
    )
    if not rows:
        await message.reply("У тебя пока нет зеркал. Создай через /add_mirror")
        return
    text = "🔗 <b>Твои зеркала:</b>\n\n"
    for r in rows:
        status = "✅" if r['active'] else "⏸"
        text += f"{status} @{r['bot_username']} — https://t.me/{r['bot_username']}\n"
    await message.reply(text, parse_mode="HTML", disable_web_page_preview=True)


@dp.message(Command("del_mirror"))
async def del_mirror_cmd(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Доступ запрещён.")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("Формат: `/del_mirror <user_id>`", parse_mode="Markdown")
        return
    try:
        target_id = int(args[1].strip())
    except ValueError:
        await message.reply("Неверный user_id.")
        return

    rows = await db_conn.fetchall(
        'SELECT bot_token, bot_username FROM mirrors WHERE owner_id = ?', (target_id,)
    )
    if not rows:
        await message.reply("У этого юзера нет зеркал.")
        return

    for r in rows:
        token = r['bot_token']
        m = active_mirrors.get(token)
        if m:
            try:
                m['task'].cancel()
                await m['bot'].session.close()
            except Exception:
                pass
            del active_mirrors[token]
        await db_conn.execute('DELETE FROM mirrors WHERE bot_token = ?', (token,))

    await message.reply(f"✅ Удалено зеркал: {len(rows)} у юзера {target_id}")


def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="Создать промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Выдать запросы", callback_data="admin_give")],
        [InlineKeyboardButton(text="Список промокодов", callback_data="admin_list_promo")],
        [InlineKeyboardButton(text="Удалить промокод", callback_data="admin_delete_promo")],
        [InlineKeyboardButton(text="Платежи", callback_data="admin_payments")],
        [InlineKeyboardButton(text="Зеркала", callback_data="admin_mirrors")],
        [InlineKeyboardButton(text="Закрыть", callback_data="admin_close")],
    ])


@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Доступ запрещён.")
        return
    await message.reply("Админ-панель", reply_markup=get_admin_kb())


@dp.callback_query(lambda c: c.data and c.data.startswith("admin_"))
async def admin_cb(cb: CallbackQuery, state: FSMContext = None):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("Доступ запрещён.")
        return
    action = cb.data.replace("admin_", "")
    back_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]])
    if action == "stats":
        u = await db_conn.fetchone('SELECT COUNT(*) AS cnt FROM users')
        r = await db_conn.fetchone('SELECT COUNT(*) AS cnt FROM reports')
        p = await db_conn.fetchone('SELECT COUNT(*) AS cnt FROM promo_codes')
        m = await db_conn.fetchone('SELECT COUNT(*) AS cnt FROM mirrors')
        pay = await db_conn.fetchone("SELECT COUNT(*) AS cnt FROM purchases WHERE status='confirmed'")
        await cb.message.edit_text(
            f"Статистика\nПользователей: {u['cnt']}\nОтчётов: {r['cnt']}\n"
            f"Промокодов: {p['cnt']}\nПлатежей: {pay['cnt']}\nЗеркал: {m['cnt']}",
            reply_markup=back_kb
        )
    elif action == "back":
        await cb.message.edit_text("Админ-панель", reply_markup=get_admin_kb())
    elif action == "close":
        await cb.message.delete()
    elif action == "create_promo":
        await cb.message.edit_text("Введите промокод:", reply_markup=back_kb)
        if state: await state.set_state(PromoCreation.waiting_for_code)
    elif action == "broadcast":
        await cb.message.edit_text("Введите текст рассылки:", reply_markup=back_kb)
        if state: await state.set_state(Broadcast.waiting_for_text)
    elif action == "give":
        await cb.message.edit_text("Введите ID пользователя:", reply_markup=back_kb)
        if state: await state.set_state(GiveRequests.waiting_for_user_id)
    elif action == "list_promo":
        promos = await get_all_promo_codes()
        text = "Нет промокодов." if not promos else "Промокоды:\n" + "\n".join(
            [f"{p['code']} — {p['used_count']}/{p['max_uses']}" for p in promos]
        )
        await cb.message.edit_text(text, reply_markup=back_kb)
    elif action == "delete_promo":
        promos = await get_all_promo_codes()
        if not promos:
            await cb.message.edit_text("Нет промокодов.", reply_markup=back_kb)
        else:
            rows = [[InlineKeyboardButton(text=f"❌ {p['code']}", callback_data=f"delpromo_{p['code']}")] for p in promos]
            rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_back")])
            await cb.message.edit_text("Выберите промокод для удаления:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    elif action == "payments":
        rows_db = await db_conn.fetchall('SELECT * FROM purchases ORDER BY created_at DESC LIMIT 10')
        text = "Нет платежей." if not rows_db else "Последние платежи:\n" + "\n".join(
            [f"{p['user_id']} — {p['requests']} запр., {p['amount']} {p['currency']}, {p['status']}" for p in rows_db]
        )
        await cb.message.edit_text(text, reply_markup=back_kb)
    elif action == "mirrors":
        rows_db = await db_conn.fetchall('SELECT owner_id, bot_username, active FROM mirrors ORDER BY created_at DESC LIMIT 20')
        text = "Нет зеркал." if not rows_db else "Зеркала:\n" + "\n".join(
            [f"{'✅' if r['active'] else '⏸'} @{r['bot_username']} — owner={r['owner_id']}" for r in rows_db]
        )
        await cb.message.edit_text(text, reply_markup=back_kb)
    await cb.answer()


@dp.callback_query(lambda c: c.data and c.data.startswith("delpromo_"))
async def delpromo_cb(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("Доступ запрещён.")
        return
    code = cb.data.replace("delpromo_", "")
    await delete_promo_code(code)
    await cb.answer(f"Удалён: {code}")
    await cb.message.edit_text("Админ-панель", reply_markup=get_admin_kb())


@dp.message(PromoCreation.waiting_for_code)
async def promo_code_input(message: Message, state: FSMContext):
    code = message.text.strip()
    if not re.match(r'^[A-Za-z0-9_\-]+$', code):
        await message.reply("Некорректный промокод.")
        return
    if await get_promo_code(code):
        await message.reply("Такой промокод уже есть.")
        return
    await state.update_data(code=code)
    await state.set_state(PromoCreation.waiting_for_max_uses)
    await message.reply("Максимальное количество активаций:")


@dp.message(PromoCreation.waiting_for_max_uses)
async def promo_max_uses(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n <= 0:
            await message.reply("Число должно быть > 0.")
            return
        await state.update_data(max_uses=n)
        await state.set_state(PromoCreation.waiting_for_requests)
        await message.reply("Сколько запросов даёт промокод:")
    except ValueError:
        await message.reply("Введите целое число.")


@dp.message(PromoCreation.waiting_for_requests)
async def promo_requests(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n <= 0:
            await message.reply("Число должно быть > 0.")
            return
        data = await state.get_data()
        ok = await create_promo_code(data['code'], data['max_uses'], n, message.from_user.id)
        await message.reply(f"Промокод создан: {data['code']}" if ok else "Ошибка создания.")
        await state.clear()
    except ValueError:
        await message.reply("Введите целое число.")


@dp.message(GiveRequests.waiting_for_user_id)
async def give_uid(message: Message, state: FSMContext):
    try:
        uid = int(message.text.strip())
        await state.update_data(target_user_id=uid)
        await state.set_state(GiveRequests.waiting_for_amount)
        await message.reply("Количество запросов:")
    except ValueError:
        await message.reply("Введите ID.")


@dp.message(GiveRequests.waiting_for_amount)
async def give_amount(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n <= 0:
            await message.reply("Количество > 0.")
            return
        data = await state.get_data()
        uid = data['target_user_id']
        if not await get_user(uid):
            await message.reply(f"Пользователь {uid} не найден.")
            await state.clear()
            return
        await db_conn.execute('UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?', (n, uid))
        await message.reply(f"Выдано {n} запросов пользователю {uid}.")
        await state.clear()
    except ValueError:
        await message.reply("Введите число.")


@dp.message(Broadcast.waiting_for_text)
async def broadcast_input(message: Message, state: FSMContext):
    text = message.text
    users = await db_conn.fetchall('SELECT user_id FROM users')
    count = 0
    for u in users:
        try:
            await bot.send_message(u['user_id'], text)
            count += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await message.reply(f"Рассылка завершена. Отправлено {count}.")
    await state.clear()


async def report_page_handler(request):
    rid = request.match_info.get('rid', '')
    logger.info(f"report page request: {rid}")
    data = await load_report(rid)
    if not data:
        logger.warning(f"report not found in DB: {rid}")
        html_404 = '''<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head><body style="background:#12100e;color:#e8e2d9;font-family:-apple-system,sans-serif;text-align:center;padding:80px 20px">
<h2 style="margin:0 0 12px">Отчёт не найден</h2>
<p style="color:#8a8075;margin:0">Ссылка устарела или неверна</p>
<p style="color:#5a5045;margin:24px 0 0;font-size:12px">ID: ''' + _esc(rid) + '''</p>
</body></html>'''
        return web.Response(text=html_404, content_type='text/html', status=404)

    html = generate_html_report(data, 0)
    inject = (
        '<script src="https://telegram.org/js/telegram-web-app.js"></script>'
        '<script>window.addEventListener("load",function(){'
        'if(window.Telegram&&window.Telegram.WebApp){'
        'window.Telegram.WebApp.ready();'
        'window.Telegram.WebApp.expand();'
        'try{window.Telegram.WebApp.setHeaderColor("#12100e");}catch(e){}'
        'try{window.Telegram.WebApp.setBackgroundColor("#12100e");}catch(e){}'
        '}});</script>'
    )
    html = html.replace('</body>', inject + '</body>')
    return web.Response(text=html, content_type='text/html')


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


async def main():
    await asyncio.sleep(1)
    await init_db()
    await restore_mirrors()

    asyncio.create_task(start_tg_client())

    # ============ CORS MIDDLEWARE ============
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
    app.router.add_get("/", lambda r: web.Response(text="Bot is running"))
    app.router.add_get("/mirrors", mirrors_page_handler) if 'mirrors_page_handler' in globals() else None
    app.router.add_get("/health", lambda r: web.Response(text="OK"))
    app.router.add_get("/api/v1/get_bot", api_get_bot_handler)
    app.router.add_get("/api/v1/get_mirrors", api_get_mirrors_handler)
    app.router.add_get("/r/{rid}", report_page_handler)
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}, base={_public_base()}")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, skip_updates=True, allowed_updates=["message", "callback_query"])
    finally:
        await runner.cleanup()
        for token, m in active_mirrors.items():
            try:
                await m['bot'].session.close()
            except Exception:
                pass
        await bot.session.close()
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
