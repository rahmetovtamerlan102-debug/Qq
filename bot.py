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
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, StateFilter
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile, PreCheckoutQuery, LabeledPrice
)
from bs4 import BeautifulSoup
import hashlib
import logging

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
    "bVaxQuNJQDwcFaowGOOWlHgr,"
    "q49N0me1xlMKyTuJEiMWGVR3,"
    "yXtPtLBncR245PVWVvTZFk2m,"
    "sYNTmjuaZTb2vUSOCE6AMJfD,"
    "H3Up52WMKkS7pid20r2zXwd3,"
    "cV134WqlWuc1cC2ojnxjz0H9,"
    "Kj94VjELcD5y9FHMWaP64rD0,"
    "dhGe2dBgXI6eoXphtfe3MmAi"
).split(",") if t.strip()]
JITLER_BASE = os.getenv("JITLER_BASE", "https://api.jitler.top")

FUNSTAT_TOKEN = os.getenv("FUNSTAT_TOKEN", "")
FUNSTAT_BASE = os.getenv("FUNSTAT_BASE", "https://funstat.info")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "5021557806").split(",") if x.strip()]

CHUNK_SIZE = 5
TG_GROUPS_LIMIT = 2

HIDDEN_FIELDS = {
    'Источник', 'Описание базы', 'Актуальность базы', 'Актуальность', 'Внутренний источник',
    'О себе', 'Bio', 'About', 'about', 'bio', 'description', 'О СЕБЕ',
    'Сайт', 'сайт', 'site', 'website', 'url', 'URL', 'САЙТ',
    'Фото', 'фото', 'photo', 'avatar', 'Avatar', 'ФОТО',
}


def _esc(v):
    return str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


_DOMAIN_URLS = (
    "vk.com/", "vkontakte.ru/", "vk.me/", "ok.ru/", "instagram.com/",
    "tiktok.com/", "facebook.com/", "fb.com/", "t.me/", "telegram.me/",
    "twitter.com/", "x.com/", "youtube.com/", "wa.me/", "whatsapp.com/",
    "telegram.org/", "youtu.be/", "github.com/", "linkedin.com/",
)

SOCIAL_KEYS = {
    'ВКонтакте', 'VK', 'vk', 'Одноклассники', 'OK', 'ok',
    'Instagram', 'instagram', 'TikTok', 'tiktok', 'Telegram', 'telegram',
    'WhatsApp', 'whatsapp', 'Facebook', 'facebook', 'Twitter', 'twitter',
    'Никнейм', 'Имя пользователя', 'Логин', 'Username', 'username',
    'Ссылка', 'Название', 'Кому', 'От',
}


def _make_link(value, key: str = ""):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    k_low = (key or "").lower()

    if s.startswith("http://") or s.startswith("https://"):
        return f'<a href="{_esc(s)}" target="_blank" class="val-link">{_esc(s)}</a>'

    lower = s.lower()
    for d in _DOMAIN_URLS:
        if d in lower:
            idx = lower.find(d)
            candidate = s[idx:].split()[0].rstrip('.,;')
            if not candidate.startswith("http"):
                candidate = "https://" + candidate
            return f'<a href="{_esc(candidate)}" target="_blank" class="val-link">{_esc(s)}</a>'

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


def get_cache_key(func_name: str, query: str) -> str:
    return f"{func_name}:{hashlib.md5(query.encode()).hexdigest()}"


API_TIMEOUTS = {
    "bigbase": 15.0,
    "seon": 6.0,
    "snusbase": 6.0,
    "ipapi": 3.0,
    "jitler": 20.0,
    "funstat": 10.0,
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
    phone TEXT PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
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
    phone TEXT PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
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
'''


async def init_db():
    global db_conn
    if DATABASE_URL:
        if not HAS_ASYNCPG:
            raise RuntimeError("DATABASE_URL задан, но asyncpg не установлен")
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        db_conn = DBAdapter("postgres", pool=pool)
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


async def parse_html_page(url: str, query: str = None):
    session = await get_http_session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
            html = await response.text()
            if response.status != 200:
                return {'error': f'HTTP {response.status}'}
            if 'Just a moment' in html or 'cf-browser-verification' in html:
                return {'error': 'Cloudflare protection detected'}
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()
            result = {
                'url': url,
                'title': soup.title.string.strip() if soup.title else None,
                'emails': [], 'phones': [], 'addresses': [],
                'snils': [], 'inn': [], 'passport': [],
                'birthdates': [], 'socials': [], 'telegrams': [],
                'full_text': text[:5000]
            }
            result['emails'] = list(set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)))
            for pattern in [r'\+7\s?\(?\d{3}\)?\s?\d{3}[-\s]?\d{2}[-\s]?\d{2}',
                            r'8\s?\(?\d{3}\)?\s?\d{3}[-\s]?\d{2}[-\s]?\d{2}']:
                result['phones'].extend(re.findall(pattern, text))
            result['phones'] = list(set(result['phones']))[:10]
            for pattern in [r'(?:г\.?|город)\s*[А-Яа-я\-]+\s*(?:ул\.?|улица)\s*[А-Яа-я\-]+\s*(?:д\.?|дом)\s*\d+[А-Яа-я]?',
                            r'(?:ул\.?|улица)\s*[А-Яа-я\-]+\s*(?:д\.?|дом)\s*\d+[А-Яа-я]?']:
                result['addresses'].extend(re.findall(pattern, text, re.IGNORECASE))
            result['addresses'] = list(set(result['addresses']))[:10]
            result['snils'] = list(set(re.findall(r'\d{3}[-\s]?\d{3}[-\s]?\d{3}[-\s]?\d{2}', text)))[:5]
            for pattern in [r'\b\d{10}\b', r'\b\d{12}\b']:
                result['inn'].extend(re.findall(pattern, text))
            result['inn'] = [i for i in set(result['inn']) if 10 <= len(i) <= 12][:5]
            for pattern in [r'\d{4}\s?\d{6}', r'серия\s*\d{4}\s*номер\s*\d{6}']:
                result['passport'].extend(re.findall(pattern, text, re.IGNORECASE))
            result['passport'] = list(set(result['passport']))[:5]
            for pattern in [r'\d{2}[./-]\d{2}[./-]\d{4}', r'\d{4}[./-]\d{2}[./-]\d{2}']:
                result['birthdates'].extend(re.findall(pattern, text))
            result['birthdates'] = list(set(result['birthdates']))[:5]
            socials_map = {
                'vk': r'(?:https?://)?(?:www\.)?vk\.com/[^\s"\']+',
                'ok': r'(?:https?://)?(?:www\.)?ok\.ru/[^\s"\']+',
                'instagram': r'(?:https?://)?(?:www\.)?instagram\.com/[^\s"\']+',
                'tiktok': r'(?:https?://)?(?:www\.)?tiktok\.com/@[^\s"\']+',
            }
            for _, pattern in socials_map.items():
                result['socials'].extend(re.findall(pattern, text, re.IGNORECASE)[:3])
            for pattern in [r'@[a-zA-Z0-9_]{5,32}', r't\.me/[a-zA-Z0-9_]+']:
                result['telegrams'].extend(re.findall(pattern, text))
            result['telegrams'] = list(set(result['telegrams']))[:10]
            if query:
                found = []
                ql = query.lower()
                for p in soup.find_all(['p', 'div', 'span', 'li']):
                    t = p.get_text(strip=True)
                    if ql in t.lower():
                        found.append({'type': 'text', 'content': t[:200] + ('...' if len(t) > 200 else '')})
                        if len(found) >= 10:
                            break
                result['found_data'] = found
            return result
    except asyncio.TimeoutError:
        return {'error': 'Timeout'}
    except Exception as e:
        return {'error': str(e)}


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
        logger.warning("[bigbase] TIMEOUT")
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
                        logger.warning(f"[bigbase tg] rate-limit, ждём {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    return {"error": err}
                return data
        except asyncio.TimeoutError:
            logger.warning("[bigbase tg] TIMEOUT")
            return {}
        except Exception as e:
            logger.error(f"BigBase TG exception: {e}")
            return {}
    return {}


BIGBASE_KEY_MAP = {
    'фио': 'ФИО', 'имя': 'Имя', 'фамилия': 'Фамилия', 'отчество': 'Отчество',
    'рабочее фио': 'Рабочее ФИО',
    'наименование': 'ФИО', 'наименование (англ.)': 'ФИО (англ.)',
    'телефон': 'Телефон', 'телефоны': 'Телефон', 'номер телефона': 'Телефон',
    'email': 'Email', 'почта': 'Email', 'e-mail': 'Email',
    'дата рождения': 'Дата рождения', 'др': 'Дата рождения',
    'возраст': 'Возраст', 'пол': 'Пол',
    'адрес': 'Адрес', 'город': 'Город', 'регион': 'Регион', 'страна': 'Страна',
    'индекс': 'Индекс',
    'паспорт': 'Паспорт', 'серия паспорта': 'Серия паспорта',
    'номер паспорта': 'Номер паспорта', 'тип паспорта': 'Тип паспорта',
    'кем выдан': 'Кем выдан', 'дата выдачи': 'Дата выдачи',
    'инн': 'ИНН', 'снилс': 'СНИЛС', 'огрн': 'ОГРН', 'огрнип': 'ОГРНИП',
    'автомобиль': 'Автомобиль', 'авто': 'Автомобиль', 'транспорт': 'Транспорт',
    'госномер': 'Госномер', 'модель': 'Модель', 'vin': 'VIN',
    'класс автомобиля': 'Класс автомобиля', 'цвет': 'Цвет',
    'текущая база': 'Текущая база', 'первая база': 'Первая база',
    'база': 'База',
    'статус': 'Статус', 'статус (текст)': 'Статус',
    'рейтинг': 'Рейтинг', 'сегмент': 'Сегмент', 'тип': 'Тип',
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
    'uuid': 'UUID',
    'код страны': 'Код страны', 'код города': 'Код города',
    'код главного города': 'Код главного города',
    'код подразделения': 'Код подразделения',
    'код валюты': 'Код валюты', 'код контрагента': 'Код контрагента',
    'валюта': 'Валюта', 'валюта пополнения': 'Валюта пополнения',
    'ек4 id': 'ЕК4 ID', 'ключ партнера': 'Ключ партнёра',
    'создан': 'Создан', 'обновлён': 'Обновлён',
    'удалён': 'Удалён', 'архивный': 'Архивный',
    'ip': 'IP',
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
    result = {'ФИО': [], 'Телефон': [], 'Транспорт': []}
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
                fields['ФИО'] = ', '.join(conn['ФИО'])
            if conn['Телефон'] and 'Телефон' not in fields:
                fields['Телефон'] = ', '.join(conn['Телефон'])
            if conn['Транспорт'] and 'Транспорт' not in fields and 'Автомобиль' not in fields:
                fields['Транспорт'] = ', '.join(conn['Транспорт'])

            base_info = rec.get('base_info')
            base_name = 'BigBase'
            if isinstance(base_info, dict):
                if base_info.get('name'):
                    fields['Источник'] = base_info['name']
                    base_name = base_info['name']
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
                fields['ФИО'] = ', '.join(top_conn['ФИО'])
            if top_conn['Телефон']:
                fields['Телефон'] = ', '.join(top_conn['Телефон'])
            if top_conn['Транспорт']:
                fields['Транспорт'] = ', '.join(top_conn['Транспорт'])
            parsed.append({"type": "result", "data": fields, "source": "BigBase"})

    return parsed


TG_ENTITY_TYPE = {
    "user": "Пользователь",
    "channel": "Канал",
    "group": "Группа",
    "bot": "Бот",
    "supergroup": "Супергруппа",
}


def parse_bigbase_telegram(data):
    if not data or not isinstance(data, dict):
        return []

    if data.get("error"):
        return [{
            "type": "result",
            "data": {"Ошибка": data["error"]},
            "source": "BigBase · Telegram"
        }]

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

    if data.get("registration_date"):
        fields["Дата регистрации"] = data["registration_date"]

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

    parsed = [{
        "type": "result",
        "data": fields,
        "source": "BigBase · Telegram"
    }]

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
                parsed.append({
                    "type": "result",
                    "data": gfields,
                    "source": "BigBase · Группа"
                })

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
                parsed.append({
                    "type": "result",
                    "data": mfields,
                    "source": "BigBase · Сообщение"
                })

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

    from curl_cffi.requests import AsyncSession

    for _ in range(len(jitler_balancer.tokens) * 2):
        token = await jitler_balancer.get_token()
        if not token:
            return {}
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {"type": search_type, "query": query, "page": 1}
        t0 = time.monotonic()
        try:
            async with AsyncSession() as cs:
                resp = await cs.post(
                    f"{JITLER_BASE}/search",
                    json=payload,
                    headers=headers,
                    impersonate="chrome120",
                    timeout=API_TIMEOUTS["jitler"],
                )
                text = resp.text
                status = resp.status_code
                elapsed = time.monotonic() - t0
                logger.info(f"Jitler POST [{search_type}:{query}] status={status} t={elapsed:.2f}s token=...{token[-6:]}")

                if status in (401, 403, 429):
                    logger.warning(f"Jitler {status} body: {text[:200]}")
                    jitler_balancer.mark_failed(token)
                    continue
                if status != 200:
                    return {}

                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    return {}

                if isinstance(data.get("response"), (dict, list)):
                    jitler_balancer.mark_success(token)
                    return data

                task_id = data.get("id")
                if task_id:
                    for _ in range(12):
                        await asyncio.sleep(1.5)
                        try:
                            g = await cs.get(
                                f"{JITLER_BASE}/search/{task_id}",
                                headers=headers,
                                impersonate="chrome120",
                                timeout=5,
                            )
                            if g.status_code == 200:
                                try:
                                    r = json.loads(g.text)
                                except json.JSONDecodeError:
                                    continue
                                if isinstance(r.get("response"), (dict, list)):
                                    jitler_balancer.mark_success(token)
                                    return r
                            elif g.status_code == 501:
                                continue
                            elif g.status_code in (404, 403, 500):
                                break
                        except Exception:
                            continue
                return {}
        except Exception as e:
            logger.error(f"Jitler exception [{search_type}]: {e}")
            continue
    return {}


JITLER_FIELD_MAP = {
    "телефон": "Телефон",
    "оператор": "Оператор",
    "страна": "Страна",
    "регион": "Регион",
    "город": "Город",
    "телефонные книги": "Телефонные книги",
    "vk профили": "VK",
    "одноклассники": "Одноклассники",
    "facebook": "Facebook",
    "telegram": "Telegram",
    "whatsapp": "WhatsApp",
    "instagram": "Instagram",
    "фото": "Фото",
    "имя": "Имя",
    "возраст": "Возраст",
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
        "instagram", "whatsapp",
        "оператор", "страна",
    }

    for m in re.finditer(
        r'<strong>\s*([^<:]+?)\s*:?\s*</strong>\s*(?:<code>([^<]*)</code>|([^<]+?))(?=<|$)',
        raw, re.IGNORECASE
    ):
        label_raw = m.group(1).strip().lower()
        value = (m.group(2) or m.group(3) or "").strip()
        value = _strip_tags_html(value)
        if not value:
            continue
        if label_raw in SKIP_FROM_MAIN:
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
        "vk профили": "VK",
        "одноклассники": "Одноклассники",
        "facebook": "Facebook",
        "instagram": "Instagram",
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
                    result.append({
                        "type": "result",
                        "data": {"#": i, "Значение": p},
                        "source": f"Jitler · {display}"
                    })
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
            result.append({
                "type": "result",
                "data": d,
                "source": f"Jitler · {display}"
            })

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
                result.append({
                    "type": "result",
                    "data": data_item,
                    "source": "Jitler · Telegram"
                })

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
            "instagram": "Instagram", "facebook": "Facebook",
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
        tasks['bigbase'] = asyncio.create_task(bigbase_search_telegram(query))
        tasks['jitler'] = asyncio.create_task(jitler_search(query, "sherlock"))
        tasks['funstat'] = asyncio.create_task(funstat_search(query, "tg_id"))
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
            logger.warning(f"{name} TIMEOUT")
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
        'phone_books': [], 'extra': {}, 'sources': [], 'records_count': 0,
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
            result['fio'] = str(fields['ФИО'])
        if fields.get('Дата рождения') and not result['birthdate']:
            bd = str(fields['Дата рождения'])
            result['birthdate'] = bd
            age = calculate_age_from_birthdate(bd)
            if age is not None:
                result['age'] = age
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
            result['fio'] = str(fields['ФИО'])
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

    cache[cache_key] = (datetime.now(), result)
    return result


def _tg_link_from_username(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if u.startswith("@"):
        u = u[1:]
    return f'<a href="https://t.me/{_esc(u)}">{_esc("@" + u)}</a>'


def _tg_link_from_id(i) -> str:
    s = str(i).strip()
    return f'<a href="tg://user?id={_esc(s)}">#{_esc(s)}</a>'


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
    sent_ids = []
    recv_ids = []
    groups = []

    for source, rows in data.get('blocks', []):
        for row in rows:
            if not tid:
                for k in ("Telegram ID", "TG ID"):
                    if row.get(k):
                        tid = str(row[k])
                        break

            if not reg_date and source.startswith("BigBase") and row.get("Дата регистрации"):
                reg_date = str(row["Дата регистрации"])

            if "Кому дарил" in source:
                to_ = row.get("Username") or row.get("Кому") or row.get("Имя")
                if to_ and to_ not in sent_ids:
                    sent_ids.append(to_)
            if "От кого получал" in source:
                from_ = row.get("Username") or row.get("От") or row.get("Имя")
                if from_ and from_ not in recv_ids:
                    recv_ids.append(from_)

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

        lines.append(f"🌀 <b>История изменения имени ({len(sorted_history)}):</b>")
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

    if sent_ids:
        lines.append(f"⬆️ <b>Кому отправлял(-а) подарки ({len(sent_ids)}):</b>")
        links = []
        for i in sent_ids[:20]:
            s = str(i)
            if s.startswith("@"):
                links.append(_tg_link_from_username(s))
            else:
                links.append(_tg_link_from_id(s))
        lines.append(" ".join(links))
        lines.append("")

    if recv_ids:
        lines.append(f"⬇️ <b>От кого получал(-а) подарки ({len(recv_ids)}):</b>")
        links = []
        for i in recv_ids[:20]:
            s = str(i)
            if s.startswith("@"):
                links.append(_tg_link_from_username(s))
            else:
                links.append(_tg_link_from_id(s))
        lines.append(" ".join(links))
        lines.append("")

    if groups:
        total_groups = len(groups)
        lines.append(f"👥 <b>Группы ({total_groups}):</b>")
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
    country = data.get('country')
    if country:
        flag = "🇰🇿" if "казах" in str(country).lower() else "🇷🇺" if "рос" in str(country).lower() else "🌍"
        lines.append(f"🌐 <b>Страна:</b> {flag} {_esc(country)}")
    fio = data.get('fio')
    if fio:
        lines.append(f"\n👤 <b>ФИО:</b> {_esc(fio)}")

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

    vk = None
    tg = []
    for src, rows in data.get('blocks', []):
        for row in rows:
            if not vk and row.get('Имя') and 'ВКонтакте' in (src or ''):
                vk = row.get('Имя')
            if not vk and row.get('ВКонтакте'):
                vk = row.get('ВКонтакте')
            uname = row.get('Username')
            if uname and uname not in tg:
                tg.append(uname)
            tid_val = row.get('Telegram ID')
            if tid_val and f"#{tid_val}" not in tg:
                tg.append(f"#{tid_val}")

    if vk:
        lines.append(f"\n🌐 <b>VK:</b> <a href=\"https://vk.com/{_esc(str(vk).replace(' ', '_'))}\">{_esc(vk)}</a>")
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

    chunked = []
    for source, rows in blocks:
        if len(rows) <= CHUNK_SIZE:
            chunked.append((source, rows))
        else:
            for i in range(0, len(rows), CHUNK_SIZE):
                chunked.append((source, rows[i:i + CHUNK_SIZE]))

    sidebar_items = ""
    for idx, (source, rows) in enumerate(chunked, start=1):
        cnt = len(rows)
        cnt_label = f' ({cnt})' if cnt > 1 else ''
        sidebar_items += f'''
        <div class="client">
        <svg width="22.03" height="22.03" viewBox="0 0 22.03 22.03" xmlns="http://www.w3.org/2000/svg">
            <circle cx="11.015" cy="11.015" r="8.08" fill="none" stroke="#currentColor" stroke-width="5.86"/>
        </svg>
        <a href="#number{idx}" class="clients_name">🇷🇺 {_esc(source)}{cnt_label}</a>
    </div>
    <div class="stick"></div>
    '''

    accordions = ""
    for idx, (source, rows) in enumerate(chunked, start=1):
        total = len(rows)
        rows_html = ""
        for r_idx, fields in enumerate(rows, 1):
            visible = [(k, v) for k, v in fields.items()
                       if k not in HIDDEN_FIELDS
                       and v is not None and str(v).strip()]

            def render_field(k, v):
                link = _make_link(v, k)
                if link:
                    return link
                if k in ("Telegram ID", "TG ID") and str(v).isdigit():
                    return _tg_link_from_id(v)
                return _esc(v)

            inner = "".join(
                f'<div class="row"><strong>{_esc(k)}:</strong><span>{render_field(k, v)}</span></div>'
                for k, v in visible
            )
            if total == 1:
                rows_html += inner
            else:
                head_name = fields.get('Внутренний источник') or fields.get('Источник') or source
                rows_html += f'''
                <div class="record-card">
                    <div class="record-head">{_esc(head_name)}</div>
                    <div class="record-body">{inner}</div>
                </div>
                '''

        count_pill = f'<span class="count-pill">{total}</span>' if total > 1 else ''

        accordions += f'''
    <div id="number{idx}" class="accordion_inner">
        <div class="accordion open">
            <div class="accordion-header" onclick="toggleAccordion(this)">
                <span>🇷🇺 {_esc(source)} {count_pill}</span>
                <div class="accordion-arrow">
                    <svg width="13" height="9" viewBox="0 0 21 12" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M1 1L10.5 10L20 1" stroke="#A5AAB4" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round"/>
                    </svg>
                </div>
            </div>
            <div class="accordion-body"><div class="accordion-content">{rows_html}</div></div>
        </div>
    </div>
        '''

    if not accordions:
        accordions = '<div class="accordion_inner"><div class="accordion open"><div class="accordion-header"><span>❌ Данные не найдены</span><div class="accordion-arrow"></div></div></div></div>'

    return f'''<!DOCTYPE html><html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" /><title>{_esc(query)}</title>
<style>
html {{ scroll-behavior: smooth; }}
body {{ font-family: "Source Sans Pro", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background-color: #0b0d10; margin: 0; height: 100vh; margin-right: 0; }}
*, ::before, ::after {{ box-sizing: border-box; }}
h1, h2, h3, h4, h5, h6 {{ margin: 0; }}
p {{ margin: 0; }}
.container {{ max-width: 1645px; width: 100%; margin: 0 auto; }}
.header {{ margin: 60px 0; margin-bottom: 45px; }}
.header_inner {{ padding: 40px 30px 40px 75px; background-color: #13161b; border-radius: 30px; }}
.request {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 38px; }}
.request1 {{ display: flex; align-items: center; gap: 12px; }}
.request_text {{ font-weight: 700; font-size: 34px; line-height: 38px; color: #fff; }}
.request_number {{ padding: 16px 27px; font-weight: 600; font-size: 23px; line-height: 27px; text-align: center; color: #fff; background-color: #0b0d10; border-radius: 20px; }}
.result {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; background-color: #0b0d10; padding: 14px 20px; border-radius: 20px; }}
.result_text {{ font-weight: 600; font-size: 16px; line-height: 21px; text-align: center; color: #fff; }}
.result_number {{ background-color: #ff851f; border-radius: 10px; font-weight: 600; font-size: 16px; line-height: 21px; text-align: center; color: #fff; padding: 6px 22px; }}
.downloading {{ display: flex; align-items: center; gap: 22px; }}
.btn1 {{ padding: 18px 45px; display: flex; align-items: center; gap: 10px; border-radius: 20px; font-weight: 600; font-size: 16px; line-height: 21px; text-align: center; color: #fff; cursor: pointer; border: none; transition: opacity 0.3s ease; }}
.btn1:hover {{ opacity: 0.6; }}
.downloadPDF {{ background-color: #ff8119; }}
.print {{ background-color: #0b0d10; }}
.main_inner {{ display: flex; justify-content: space-between; gap: 33px; width: 100%; }}
.block1 {{ width: 25%; }}
.block1_inner {{ position: sticky; top: 45px; z-index: 100; }}
.block2 {{ width: 73%; }}
.block_title {{ font-weight: 600; font-size: 14px; line-height: 23px; color: #fff; margin-left: 35px; margin-bottom: 16px; }}
.bg_str {{ background-color: #13161b; padding-right: 28px; border-radius: 20px; }}
.structure {{ padding: 30px 16px 30px 38px; background-color: #13161b; border-radius: 20px; max-height: calc(100vh - 134px); overflow-y: auto; }}
.client {{ display: flex; align-items: center; gap: 10px; color: #fff; }}
.client svg {{ flex-shrink: 0; width: 22px; height: 22px; stroke: #222730; transition: stroke 0.3s ease; }}
.clients_name {{ font-weight: 500; font-size: 16px; line-height: 23px; color: #fff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; transition: color 0.3s ease; cursor: pointer; text-decoration: none; }}
.client:hover svg, .client:hover .clients_name {{ stroke: #ff8119; color: #ff8119; }}
.stick {{ width: 4px; height: 36px; background: #222730; margin-left: 9px; }}
.accordion_inner {{ padding: 20px 26px 20px 16px; background-color: #13161b; border-radius: 20px; margin-bottom: 30px; }}
.accordion_inner:last-of-type {{ margin-bottom: 0; }}
.accordion {{ border-radius: 8px; overflow: hidden; background: #13161b; }}
.accordion-header {{ padding: 14px 10px 14px 35px; background: #0b0d10; display: flex; align-items: center; justify-content: space-between; cursor: pointer; user-select: none; font-weight: 700; color: #fff; font-size: 18px; line-height: 23px; text-align: center; border-radius: 15px; }}
.accordion-arrow {{ transition: transform 0.3s; padding: 8px 14px; background-color: #13161b; border-radius: 10px; }}
.accordion-body {{ max-height: 0; overflow: hidden; transition: max-height 0.3s ease; }}
.accordion-content {{ padding: 30px 20px 10px 30px; }}
.accordion-content .row {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; margin-bottom: 20px; }}
.accordion-content .row:last-of-type {{ margin-bottom: 0; }}
.accordion-content .row strong {{ text-transform: uppercase; font-weight: 500; font-size: 16px; line-height: 20px; color: #fff; flex-shrink: 0; max-width: 45%; word-break: break-word; overflow-wrap: anywhere; }}
.accordion-content .row span {{ font-weight: 600; font-size: 16px; line-height: 20px; color: #fff; text-align: right; flex: 1; min-width: 0; word-break: break-word; overflow-wrap: anywhere; white-space: normal; }}
.accordion.open .accordion-body {{ max-height: 50000px; }}
.accordion.open .accordion-arrow {{ transform: rotate(180deg); }}
.count-pill {{ display: inline-block; background: #ff8119; color: #fff; font-size: 11px; font-weight: 700; padding: 2px 9px; border-radius: 8px; margin-left: 8px; vertical-align: middle; }}
.record-card {{ background: #0f1216; border-radius: 12px; padding: 16px 20px 8px; margin-bottom: 16px; border: 1px solid #1a1f27; }}
.record-card:last-child {{ margin-bottom: 0; }}
.record-head {{ font-weight: 700; font-size: 11px; color: #ff8119; text-transform: uppercase; letter-spacing: 1.2px; margin-bottom: 14px; padding-bottom: 10px; border-bottom: 1px solid #222730; }}
.record-body .row:last-of-type {{ margin-bottom: 0; }}
.val-link {{ color: #ff9c3f; text-decoration: none; word-break: break-all; overflow-wrap: anywhere; }}
.val-link:hover {{ text-decoration: underline; }}
.no-transform * {{ transform: none !important; }}
@media print {{ body * {{ visibility: hidden; }} #printArea, #printArea * {{ visibility: visible; }} #printArea {{ position: absolute; left: 0; top: 0; }} .show_print {{ display: block; }} .accordion-header .accordion-arrow {{ transform: none !important; transition: none !important; }} }}
@media screen and (max-width: 1640px) {{ .container {{ padding: 0 20px; }} }}
@media screen and (max-width: 990px) {{ .header {{ margin: 30px 0; }} .header_inner {{ padding: 20px 25px; }} .request {{ flex-direction: column; align-items: flex-start; gap: 18px; margin-bottom: 20px; }} .request1 {{ order: 2; }} .result {{ order: 1; }} .block1 {{ display: none; }} .block2 {{ width: 100%; }} .hide_mobile {{ display: none; }} .accordion-content .row span {{ width: auto; text-align: right; word-break: break-word; overflow-wrap: anywhere; }} }}
@media screen and (max-width: 640px) {{ .result_number {{ padding: 6px 12px; }} .request_text {{ font-size: 26px; }} .request_number {{ padding: 12px 24px; font-size: 20px; }} .accordion-header {{ font-size: 15px; text-align: left; border-radius: 12px; }} .btn1 {{ padding: 14px 35px; font-size: 10px; }} }}
@media screen and (max-width: 440px) {{ .block2 {{ padding-bottom: 30px; }} .header_inner {{ border-radius: 15px; }} .result {{ padding: 12px 13px; border-radius: 10px; }} .result_text, .result_number {{ font-size: 10px; }} .result_number {{ padding: 3px 9px; border-radius: 5px; }} .request {{ margin-bottom: 15px; }} .request_text {{ font-size: 16px; }} .request_number {{ padding: 8px 26px; font-size: 12px; border-radius: 8px; }} .downloading {{ gap: 15px; }} .btn1 {{ padding: 12px 30px; font-size: 10px; border-radius: 10px; gap: 6px; }} .accordion_inner {{ border-radius: 12px; padding: 10px; margin-bottom: 10px; }} .accordion-header {{ font-size: 12px; text-align: left; padding: 13px; line-height: 16px; border-radius: 9px; }} .accordion-arrow {{ padding: 10px 8px; line-height: 0; border-radius: 6px; }} .accordion-content {{ padding: 22px; }} .accordion-content .row {{ margin-bottom: 13px; }} .accordion-content .row strong, .accordion-content .row span {{ font-size: 10px; line-height: 11px; }} .record-card {{ padding: 12px 14px 6px; margin-bottom: 10px; }} }}
</style></head><body>
<header class="header"><div class="container"><div class="header_inner">
<div class="request">
<div class="request1"><h2 class="request_text">Запрос:</h2><div class="request_number">{_esc(query)}</div></div>
<div class="result"><h3 class="result_text">Результатов:</h3><div class="result_number">{total_records}</div><h3 class="result_text" style="margin-left: 20px;">Просмотров:</h3><div class="result_number">{views}</div></div>
</div>
<div class="downloading"><button onclick="downloadPDF()" class="downloadPDF btn1">Сохранить в PDF</button><button id="printButton" class="print btn1">Печатать</button></div>
</div></div></header>
<div class="main"><div class="container"><div class="main_inner">
<div class="block1"><div class="block1_inner"><h3 class="block_title">Структура отчёта</h3><div class="bg_str"><div class="structure">{sidebar_items}</div></div></div></div>
<div class="block2 no_transform" id="printArea"><h3 class="block_title hide_mobile show_print">Полный отчёт</h3>{accordions}</div>
</div></div></div>
<script>
function toggleAccordion(header) {{ header.parentElement.classList.toggle('open'); }}
document.addEventListener('DOMContentLoaded', function() {{ document.getElementById('printButton').addEventListener('click', function() {{ window.print(); }}); }});
</script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js"></script>
<script>
function downloadPDF() {{
    document.body.classList.add('no-transform');
    const element = document.getElementById('printArea');
    const options = {{ margin: 0, filename: 'document.pdf', image: {{ type: 'jpeg', quality: 1 }}, html2canvas: {{ scale: 1.5 }}, jsPDF: {{ unit: 'pt', format: 'a4', orientation: 'portrait' }} }};
    html2pdf().set(options).from(element).save().then(() => {{ document.body.classList.remove('no-transform'); }});
}}
</script></body></html>'''


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


async def create_user(user_id: int, username: str = None, referred_by: int = None):
    await db_conn.execute('INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)', (user_id, username))
    code = await generate_referral_code(user_id)
    if referred_by and referred_by != user_id:
        ref = await db_conn.fetchone('SELECT user_id FROM users WHERE user_id = ?', (referred_by,))
        if ref:
            try:
                await db_conn.execute(
                    'INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)',
                    (referred_by, user_id)
                )
                await db_conn.execute(
                    'UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = ?',
                    (referred_by,)
                )
            except Exception:
                pass
    return code


async def get_user(user_id: int):
    return await db_conn.fetchone('SELECT * FROM users WHERE user_id = ?', (user_id,))


async def get_referral_code(user_id: int):
    row = await db_conn.fetchone('SELECT referral_code FROM users WHERE user_id = ?', (user_id,))
    return row['referral_code'] if row else None


async def save_report(phone: str, data: dict):
    await db_conn.execute(
        'INSERT INTO reports (phone, data) VALUES (?, ?) ON CONFLICT(phone) DO UPDATE SET data = excluded.data, created_at = CURRENT_TIMESTAMP',
        (phone, json.dumps(data, ensure_ascii=False))
    )


async def get_unique_views_phone(phone: str, user_id: int) -> int:
    row = await db_conn.fetchone('SELECT user_ids FROM phone_views WHERE phone = ?', (phone,))
    user_ids = json.loads(row['user_ids']) if row and row['user_ids'] else []
    if user_id not in user_ids:
        user_ids.append(user_id)
    await db_conn.execute(
        'INSERT INTO phone_views (phone, user_ids) VALUES (?, ?) ON CONFLICT(phone) DO UPDATE SET user_ids = excluded.user_ids',
        (phone, json.dumps(user_ids))
    )
    return len(user_ids)


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


async def create_stars_invoice(user_id: int, stars_price: int, requests_count: int):
    temp_invoice_id = f"stars_{user_id}_{int(datetime.now().timestamp())}"
    await db_conn.execute(
        'INSERT INTO purchases (user_id, invoice_id, amount, currency, requests, status) VALUES (?, ?, ?, ?, ?, ?)',
        (user_id, temp_invoice_id, stars_price, 'XTR', requests_count, 'pending')
    )
    prices = [LabeledPrice(label=f"{requests_count} запросов", amount=stars_price)]
    try:
        await bot.send_invoice(
            chat_id=user_id,
            title=f"Пополнение: {requests_count} запросов",
            description=f"Вы получаете {requests_count} дополнительных запросов.",
            provider_token="", currency="XTR", prices=prices,
            start_parameter=f"stars_{user_id}_{int(datetime.now().timestamp())}",
            payload=json.dumps({"user_id": user_id, "requests": requests_count, "temp_invoice_id": temp_invoice_id}),
        )
        return True, temp_invoice_id
    except Exception as e:
        logger.error(f"Stars invoice error: {e}")
        await db_conn.execute('DELETE FROM purchases WHERE invoice_id = ?', (temp_invoice_id,))
        return False, None


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
    try:
        await bot.send_message(user_id, f"Оплата Stars подтверждена! Начислено {requests} запросов.")
    except Exception:
        pass


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

SEARCH_TYPE_LABELS = {
    "phone": "номеру телефона",
    "email": "Email",
    "ip": "IP-адресу",
    "vk": "VK",
    "fio": "ФИО",
    "inn": "ИНН",
    "tg_id": "Telegram",
}


def search_type_label(t: str) -> str:
    return SEARCH_TYPE_LABELS.get((t or "").strip().lower(), t)


def detect_type(text: str) -> str:
    cleaned = re.sub(r'\s+', '', text)
    if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', text):
        return "email"
    if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', cleaned):
        return "ip"
    if re.search(r'vk\.com/', text, re.IGNORECASE):
        return "vk"
    if re.match(r'^\+?\d{10,15}$', cleaned):
        return "phone"
    words = text.split()
    if len(words) >= 3 and all(re.match(r'^[А-Яа-я\-]+$', w) for w in words[:3]):
        return "fio"
    return "phone"


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
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")],
    ])
    text = (
        "🕵️ dataseeker — твой бесплатный цифровой детектив.\n\n"
        "Типы поиска:\n\n"
        "┌ Контакты:\n"
        "├ Телефон → +79999999999\n"
        "└ Email → ivanov@gmail.com\n\n"
        "┌ Соцсети:\n"
        "├ VK → vk.com/id1234567\n"
        "└ TG ID → /id 123456789\n\n"
        "┌ Онлайн-следы:\n"
        "└ IP → 185.85.219.243\n\n"
        "┌ Физ. лица:\n"
        "├ ИНН → /inn 123456789012\n"
        "└ ФИО → Иванов Иван Иванович\n\n"
        "Каждые 24 часа выдаётся по 5 бесплатных запросов."
    )
    await message.reply(text, reply_markup=keyboard)


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
        await message.reply("Укажите Telegram ID: `/id 123456789` или `/id @username`", parse_mode="Markdown")
        return
    tg_id = args[1].strip().lstrip('@')
    if tg_id.isdigit():
        if not (1 <= int(tg_id) <= 20000000000):
            await message.reply("Telegram ID должен быть от 1 до 20000000000.")
            return
    elif not re.match(r'^[a-zA-Z0-9_]{5,32}$', tg_id):
        await message.reply("Ник: 5–32 латинских буквы, цифры или _. ID: только цифры.")
        return
    await process_general_query(message, tg_id, "tg_id")


async def process_general_query(message: Message, query: str, search_type: str):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await create_user(user_id, message.from_user.username)
    if await get_user_available_requests(user_id) <= 0:
        await message.reply("Лимит запросов исчерпан.")
        return

    label = search_type_label(search_type)
    status = await message.reply(f"🔍 Поиск по {label}...")
    try:
        data = await collect_general_data(query, search_type)

        if data.get('records_count', 0) == 0 and not data.get('blocks'):
            try:
                await status.edit_text(
                    f"❌ По {label} `{query}` ничего не найдено.",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
            await use_request(user_id)
            return

        if search_type == "tg_id":
            preview = build_funstat_preview(data, query)
        else:
            preview = build_preview(data)

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"📄 Открыть полный отчёт ({data.get('records_count', 0)} шт)",
                callback_data=f"view_full_{search_type}|{query}"
            )],
        ])

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


@dp.message(lambda msg: msg.text and not msg.text.startswith('/'), StateFilter(None))
async def universal_handler(message: Message):
    text = message.text.strip()
    await process_general_query(message, text, detect_type(text))


@dp.callback_query(lambda c: c.data and c.data.startswith("view_full_"))
async def view_full_cb(cb: CallbackQuery):
    parts = cb.data.replace("view_full_", "", 1)
    if "|" not in parts:
        await cb.answer("Ошибка запроса.")
        return
    search_type, query = parts.split("|", 1)
    data_pair = cache.get(get_cache_key(search_type, query))
    if not data_pair:
        await cb.answer("Данные устарели, введи запрос снова.")
        return
    _, data = data_pair

    html = generate_html_report(data, 0)
    safe_query = re.sub(r'[^0-9a-zA-Z_\-]', '_', str(query))
    file = BufferedInputFile(html.encode('utf-8'),
                             filename=f"report_{search_type}_{safe_query}.html")
    try:
        await cb.message.reply_document(file, caption=f"📋 Полный отчёт по {query}")
    except Exception as e:
        logger.error(f"Send full report error: {e}")
        await cb.answer("Ошибка отправки файла.", show_alert=True)
        return
    await cb.answer("Отчёт отправлен.")


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
    text = f"Ваш профиль\nID: {user_id}\nДоступно запросов: {available}\nРегистрация: {user['created_at']}"
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
    ok, _ = await create_stars_invoice(cb.from_user.id, stars, rq)
    if not ok:
        await cb.message.edit_text("Ошибка создания счёта.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="buy_requests")]]))
    else:
        await cb.message.edit_text("Счёт создан. Оплатите в Telegram.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="buy_requests")]]))
    await cb.answer()


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
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")],
    ])
    await cb.message.edit_text("Выберите действие:", reply_markup=kb)
    await cb.answer()


def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="Создать промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Выдать запросы", callback_data="admin_give")],
        [InlineKeyboardButton(text="Список промокодов", callback_data="admin_list_promo")],
        [InlineKeyboardButton(text="Удалить промокод", callback_data="admin_delete_promo")],
        [InlineKeyboardButton(text="Платежи", callback_data="admin_payments")],
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
        pay = await db_conn.fetchone("SELECT COUNT(*) AS cnt FROM purchases WHERE status='confirmed'")
        await cb.message.edit_text(
            f"Статистика\nПользователей: {u['cnt']}\nОтчётов: {r['cnt']}\nПромокодов: {p['cnt']}\nПлатежей: {pay['cnt']}",
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


async def main():
    await asyncio.sleep(1)
    await init_db()
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="Bot is running"))
    app.router.add_get("/health", lambda r: web.Response(text="OK"))
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, skip_updates=True, allowed_updates=["message", "callback_query"])
    finally:
        await runner.cleanup()
        await bot.session.close()
        if db_conn:
            if db_conn.backend == "sqlite" and db_conn.conn:
                await db_conn.conn.close()
            elif db_conn.backend == "postgres" and db_conn.pool:
                await db_conn.pool.close()
        if http_session:
            await http_session.close()


if __name__ == "__main__":
    asyncio.run(main())
