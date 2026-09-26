import os
import re
import json
import random
import string
import asyncio
import aiosqlite
import aiohttp
from aiohttp import web
from datetime import datetime, timedelta
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# === ENV ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан")

DB_PATH = os.getenv("DB_PATH", "dataseeker.db")

DEPSEARCH_TOKEN = os.getenv("DEPSEARCH_TOKEN")

NIGHTSEARCH_API_KEY = os.getenv("NIGHTSEARCH_API_KEY")
NIGHTSEARCH_BASE = os.getenv("NIGHTSEARCH_BASE", "https://nightsearch.life")

SEON_API_KEY = os.getenv("SEON_API_KEY")
SEON_BASE = os.getenv("SEON_BASE", "https://api.seon.io")

SNUSBASE_API_KEY = os.getenv("SNUSBASE_API_KEY")
SNUSBASE_BASE = os.getenv("SNUSBASE_BASE", "https://api.snusbase.com")

CRYPTOPAY_TOKEN = os.getenv("CRYPTOPAY_TOKEN", "")
CRYPTOPAY_BASE = os.getenv("CRYPTOPAY_BASE", "https://pay.crypt.bot")

PROXY_URL = os.getenv("PROXY_URL")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

db_conn = None
db_lock = asyncio.Lock()
http_session = None

cache = {}
CACHE_TTL = timedelta(hours=1)

def get_cache_key(func_name: str, query: str) -> str:
    return f"{func_name}:{hashlib.md5(query.encode()).hexdigest()}"

API_TIMEOUTS = {
    "nightsearch": 12.0,
    "seon": 6.0,
    "snusbase": 6.0,
    "depsearch": 25.0,
    "ipapi": 3.0
}

# === FSM ===
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

# === БАЗА ДАННЫХ ===
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    db_conn.row_factory = aiosqlite.Row
    await db_conn.execute("PRAGMA journal_mode=WAL")
    await db_conn.execute("PRAGMA synchronous=NORMAL")
    async with db_lock:
        await db_conn.executescript('''
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
        ''')
        await db_conn.commit()

async def get_http_session():
    global http_session
    if http_session is None:
        http_session = aiohttp.ClientSession()
    return http_session

# === ВСПОМОГАТЕЛЬНЫЕ ===
def get_social_url(value):
    if not value:
        return None
    if isinstance(value, dict):
        for key in ("url", "link", "href", "profile_url", "profile"):
            if value.get(key):
                return str(value[key]).strip()
        return None
    if isinstance(value, list):
        for item in value:
            url = get_social_url(item)
            if url:
                return url
        return None
    value = str(value).strip()
    if re.match(r'^https?://', value):
        return value
    return None

def find_best_birthdate(birthdates):
    if not birthdates:
        return None
    best, best_score = None, 0
    for bd in birthdates:
        if not bd:
            continue
        parts = re.split(r'[./-]', str(bd))
        score = len(parts)
        if re.search(r'\d{4}', str(bd)):
            score += 10
        if re.match(r'\d{1,2}[./-]\d{1,2}[./-]\d{4}', str(bd)):
            score += 20
        if score > best_score:
            best_score, best = score, bd
    return best

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

# === HTML ПАРСЕР ===
async def parse_html_page(url: str, query: str = None):
    session = await get_http_session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    proxy_url = PROXY_URL or None
    try:
        async with session.get(url, headers=headers, proxy=proxy_url,
                               timeout=aiohttp.ClientTimeout(total=15)) as response:
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

# === API ===
async def depsearch_search(query: str):
    if not DEPSEARCH_TOKEN:
        return {}
    session = await get_http_session()
    clean = re.sub(r'\D', '', query)
    variants = [clean]
    if clean.startswith('8') and len(clean) == 11:
        variants += ['7' + clean[1:], '+7' + clean[1:]]
    elif clean.startswith('7') and len(clean) == 11:
        variants += ['8' + clean[1:], '+' + clean]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://depsearch.sbs/",
        "Origin": "https://depsearch.sbs",
    }
    proxy_url = PROXY_URL or None
    for variant in variants:
        params = {"quest": variant, "token": DEPSEARCH_TOKEN, "lang": "ru"}
        try:
            async with session.get("https://api.depsearch.sbs/quest", params=params, headers=headers,
                                   proxy=proxy_url,
                                   timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["depsearch"])) as resp:
                text = await resp.text()
                logger.info(f"DepSearch [{variant}] status={resp.status}: {text[:500]}")
                if resp.status != 200:
                    continue
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(data, dict) and data:
                    return data
        except Exception as e:
            logger.error(f"DepSearch exception [{variant}]: {e}")
            continue
    return {}

async def nightsearch_search(query: str):
    if not NIGHTSEARCH_API_KEY:
        return {}
    session = await get_http_session()
    clean = re.sub(r'\D', '', query)
    headers = {
        "X-API-Key": NIGHTSEARCH_API_KEY,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    urls = [
        f"{NIGHTSEARCH_BASE}/api/search",
        f"{NIGHTSEARCH_BASE}/api/v1/search",
        f"{NIGHTSEARCH_BASE}/search",
    ]
    payload = {"query": clean, "search_type": "phone", "type": "phone"}
    for url in urls:
        try:
            async with session.post(url, json=payload, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=8)) as resp:
                text = await resp.text()
                logger.info(f"NightSearch [{url}] status={resp.status}: {text[:500]}")
                if resp.status != 200:
                    continue
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if data.get('results') or data.get('data') or data.get('items'):
                    return data
                task_id = data.get('task_id') or data.get('id') or data.get('request_id')
                if task_id:
                    for _ in range(8):
                        await asyncio.sleep(1.2)
                        for purl in [f"{url}/{task_id}", f"{NIGHTSEARCH_BASE}/api/search/{task_id}",
                                     f"{NIGHTSEARCH_BASE}/api/v1/search/{task_id}", f"{NIGHTSEARCH_BASE}/search/{task_id}"]:
                            try:
                                async with session.get(purl, headers=headers,
                                                       timeout=aiohttp.ClientTimeout(total=5)) as presp:
                                    if presp.status != 200:
                                        continue
                                    pdata = json.loads(await presp.text())
                                    if pdata.get('results') or pdata.get('data') or pdata.get('items'):
                                        return pdata
                                    if pdata.get('status') in ('failed', 'error'):
                                        return {}
                            except Exception:
                                continue
                return data
        except Exception as e:
            logger.error(f"NightSearch exception [{url}]: {e}")
            continue
    return {}

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
            logger.info(f"SEON status={resp.status}: {text[:300]}")
            return json.loads(text) if resp.status == 200 else {}
    except Exception as e:
        logger.error(f"SEON error: {e}")
        return {}

async def snusbase_search(query: str):
    if not SNUSBASE_API_KEY:
        return {}
    session = await get_http_session()
    url = f"{SNUSBASE_BASE}/data/search"
    headers = {"Auth": SNUSBASE_API_KEY, "Content-Type": "application/json"}
    payload = {"terms": [query], "types": ["email", "username", "phone", "name"], "wildcard": False}
    try:
        async with session.post(url, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["snusbase"])) as resp:
            text = await resp.text()
            logger.info(f"Snusbase status={resp.status}: {text[:300]}")
            return json.loads(text) if resp.status == 200 else {}
    except Exception as e:
        logger.error(f"Snusbase error: {e}")
        return {}

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

# === ПАРСЕР DEPSEARCH ===
def depsearch_to_report(data: dict) -> dict:
    report = {"results": [], "phone_info": {}}
    if not isinstance(data, dict):
        return report

    def find_list(obj, depth=0):
        if depth > 5 or obj is None:
            return None
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            return obj
        if isinstance(obj, dict):
            for key in ("results", "result", "data", "items", "records", "list"):
                if key in obj:
                    r = find_list(obj[key], depth + 1)
                    if r:
                        return r
            for v in obj.values():
                r = find_list(v, depth + 1)
                if r:
                    return r
        return None

    def find_dict(obj, key, depth=0):
        if depth > 5 or obj is None:
            return None
        if isinstance(obj, dict):
            if key in obj and isinstance(obj[key], dict):
                return obj[key]
            for v in obj.values():
                r = find_dict(v, key, depth + 1)
                if r:
                    return r
        return None

    results = find_list(data) or []
    phone_info = find_dict(data, "phone_info")
    if isinstance(phone_info, dict):
        report["phone_info"] = phone_info
    for item in results:
        if not isinstance(item, dict):
            continue
        record = {str(k): v for k, v in item.items() if v is not None and not (isinstance(v, str) and not v.strip())}
        if record:
            report["results"].append(record)
    return report

def parse_nightsearch(data):
    if not data or not isinstance(data, dict):
        return []
    parsed = []
    items = data.get("results") or data.get("data") or data.get("items") or []
    if isinstance(items, dict):
        items = [items]
    key_mapping = {
        'full_name': 'ФИО', 'name': 'Имя', 'first_name': 'Имя', 'last_name': 'Фамилия',
        'phone': 'Телефон', 'phone_number': 'Телефон', 'email': 'Email',
        'address': 'Адрес', 'city': 'Город', 'region': 'Регион', 'country': 'Страна',
        'birthdate': 'Дата рождения', 'birth_date': 'Дата рождения',
        'age': 'Возраст', 'gender': 'Пол', 'operator': 'Оператор',
        'source': 'Источник', 'database': 'База данных',
        'username': 'Имя пользователя', 'nickname': 'Никнейм',
        'telegram': 'Telegram', 'vk': 'ВКонтакте',
    }
    for item in items:
        if isinstance(item, dict):
            fields = {key_mapping.get(k, k): v for k, v in item.items() if v not in (None, "", [], {})}
            if fields:
                parsed.append({"type": "result", "data": fields})
    return parsed

def parse_seon(data):
    if not data or not isinstance(data, dict):
        return []
    parsed = []
    phone_info = data.get('phone', {})
    if phone_info:
        key_mapping = {'phone': 'Телефон', 'country': 'Страна', 'carrier': 'Оператор',
                       'is_valid': 'Валидный', 'is_active': 'Активный'}
        info = {key_mapping.get(k, k): v for k, v in phone_info.items() if v}
        if info:
            parsed.append({"type": "phone_info", "data": info})
    if data.get('risk_score') is not None:
        parsed.append({"type": "risk_score", "data": {"Оценка риска": data['risk_score']}})
    if data.get('email'):
        parsed.append({"type": "email", "data": {"Email": data['email']}})
    return parsed

def parse_snusbase(data):
    if not data or not isinstance(data, dict):
        return []
    parsed = []
    results = data.get('data') or data.get('results') or []
    key_mapping = {
        'email': 'Email', 'username': 'Имя пользователя', 'password': 'Пароль',
        'hash': 'Хеш', 'phone': 'Телефон', 'address': 'Адрес', 'name': 'Имя',
        'full_name': 'ФИО', 'city': 'Город', 'country': 'Страна',
        'ip': 'IP адрес', 'source': 'Источник',
    }
    for item in results:
        if isinstance(item, dict):
            fields = {key_mapping.get(k, k): v for k, v in item.items() if v not in (None, "", [], {})}
            if fields:
                parsed.append({"type": "result", "data": fields})
    return parsed

# === СБОР ДАННЫХ ===
async def collect_general_data(query: str, search_type: str = "phone"):
    cache_key = get_cache_key(search_type, query)
    if cache_key in cache:
        cached_time, data = cache[cache_key]
        if datetime.now() - cached_time < CACHE_TTL:
            return data

    original_query = query
    if search_type == "phone":
        query = re.sub(r'[^0-9]', '', query)

    tasks = {
        'depsearch': asyncio.create_task(depsearch_search(query)),
        'nightsearch': asyncio.create_task(nightsearch_search(query)),
        'seon': asyncio.create_task(seon_search(query)),
        'snusbase': asyncio.create_task(snusbase_search(query)),
    }
    if search_type == "ip":
        tasks['ipapi'] = asyncio.create_task(ip_info_search(query))

    results = {}
    for name, task in tasks.items():
        try:
            timeout = 30.0 if name == "depsearch" else 15.0 if name == "nightsearch" else 12.0
            results[name] = await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"{name} TIMEOUT")
            results[name] = {}
            task.cancel()
        except Exception as e:
            logger.error(f"{name} exception: {e}")
            results[name] = {}

    depsearch = results.get('depsearch', {}) or {}
    nightsearch = results.get('nightsearch', {}) or {}
    seon = results.get('seon', {}) or {}
    snusbase = results.get('snusbase', {}) or {}
    ipdata = results.get('ipapi', {}) if search_type == "ip" else {}

    dep_report = depsearch_to_report(depsearch)
    dep_results = dep_report.get("results", [])
    dep_phone_info = dep_report.get("phone_info", {})

    night_parsed = parse_nightsearch(nightsearch)
    seon_parsed = parse_seon(seon)
    snusbase_parsed = parse_snusbase(snusbase)

    logger.info(f"Parsed: dep={len(dep_results)}, night={len(night_parsed)}, seon={len(seon_parsed)}, snus={len(snusbase_parsed)}")

    result = {
        'query': original_query, 'type': search_type,
        'operator': None, 'region': None, 'country': None, 'city': None,
        'fio': None, 'birthdate': None, 'age': None, 'address': None,
        'emails': [], 'telegrams': [],
        'vk': None, 'instagram': None, 'tiktok': None, 'ok': None,
        'phone_books': [], 'extra': {}, 'sources': [], 'records_count': 0
    }

    sources_set = set()
    records_count = 0
    seen_records = set()

    if ipdata:
        result['country'] = ipdata.get('country') or result['country']
        result['region'] = ipdata.get('regionName') or result['region']
        result['city'] = ipdata.get('city') or result['city']
        if ipdata.get('isp'):
            result['operator'] = ipdata['isp']
        extra_ip = {k: ipdata[k] for k in ['country', 'regionName', 'city', 'zip', 'lat', 'lon', 'timezone', 'isp', 'org', 'as'] if ipdata.get(k)}
        if extra_ip:
            records_count += 1
            result['extra']["IP информация"] = {'source': 'ip-api.com', 'data': extra_ip}
            sources_set.add("ip-api.com")

    if dep_phone_info:
        if dep_phone_info.get('operator'):
            result['operator'] = str(dep_phone_info['operator'])
        if dep_phone_info.get('region'):
            result['region'] = str(dep_phone_info['region'])
        if dep_phone_info.get('country'):
            result['country'] = str(dep_phone_info['country'])
        sources_set.add("DepSearch")

    for item in dep_results:
        record_key = "|".join(str(v) for v in item.values())
        if record_key in seen_records:
            continue
        seen_records.add(record_key)
        source_name = item.get('🏫Источник') or item.get('Источник') or 'DepSearch'
        sources_set.add(source_name)
        records_count += 1
        result['extra'][f"Запись #{records_count} (DepSearch)"] = {'source': source_name, 'data': item}

    for block in night_parsed:
        if block['type'] == 'result':
            fields = block['data']
            record_key = "|".join(str(v) for v in fields.values())
            if record_key in seen_records:
                continue
            seen_records.add(record_key)
            source_name = fields.get('Источник') or fields.get('База данных') or 'NightSearch'
            sources_set.add(source_name)
            records_count += 1
            result['extra'][f"Запись #{records_count} (NightSearch)"] = {'source': source_name, 'data': fields}

    for block in seon_parsed:
        if block['type'] == 'phone_info':
            info = block['data']
            if info.get('Оператор'):
                result['operator'] = str(info['Оператор'])
            if info.get('Страна'):
                result['country'] = str(info['Страна'])
            records_count += 1
            result['extra']["Информация о номере (SEON)"] = {'source': 'SEON', 'data': info}
            sources_set.add("SEON")
        elif block['type'] == 'email':
            if block['data'].get('Email'):
                result['emails'].append(str(block['data']['Email']))
                sources_set.add("SEON")

    for block in snusbase_parsed:
        if block['type'] == 'result':
            fields = block['data']
            record_key = "|".join(str(v) for v in fields.values())
            if record_key in seen_records:
                continue
            seen_records.add(record_key)
            sources_set.add("Snusbase")
            records_count += 1
            result['extra'][f"Запись #{records_count} (Snusbase)"] = {'source': 'Snusbase', 'data': fields}

    result['sources'] = list(sources_set)
    result['records_count'] = records_count
    result['emails'] = list(dict.fromkeys([e for e in result['emails'] if e and '@' in str(e)]))

    cache[cache_key] = (datetime.now(), result)
    return result

# === HTML ОТЧЁТ ===
def generate_html_report(data: dict, views: int = 0) -> str:
    query = data.get('query', '')
    records = data.get('extra', {})
    records_count = len(records)

    main_info = []
    if data.get('fio'): main_info.append(("ФИО", data['fio']))
    if data.get('birthdate'): main_info.append(("Дата рождения", data['birthdate']))
    if data.get('age') is not None: main_info.append(("Возраст", f"{data['age']} лет"))
    if data.get('address'): main_info.append(("Адрес", data['address']))
    if data.get('phone_books'): main_info.append(("Телефоны", ', '.join(data['phone_books'][:10])))
    if data.get('emails'): main_info.append(("Email", ', '.join(data['emails'][:5])))
    if data.get('vk'): main_info.append(("ВКонтакте", data['vk']))
    if data.get('ok'): main_info.append(("Одноклассники", data['ok']))
    if data.get('instagram'): main_info.append(("Instagram", data['instagram']))
    if data.get('tiktok'): main_info.append(("TikTok", data['tiktok']))
    if data.get('telegrams'): main_info.append(("Telegram", ', '.join(data['telegrams'])))
    if data.get('operator'): main_info.append(("Оператор", data['operator']))
    if data.get('region'): main_info.append(("Регион", data['region']))
    if data.get('country'): main_info.append(("Страна", data['country']))
    if data.get('city'): main_info.append(("Город", data['city']))

    structure_items = ""
    for idx, (_, rec) in enumerate(records.items(), start=1):
        source = rec.get('source', 'Без названия')
        structure_items += f'''
        <div class="client">
            <svg width="22" height="22" viewBox="0 0 22 22"><circle cx="11" cy="11" r="8" fill="none" stroke="#222730" stroke-width="5.86"/></svg>
            <a href="#record{idx}" class="clients_name">{source[:30]}</a>
        </div>
        <div class="stick"></div>
        '''

    accordions = ""
    for idx, (_, rec) in enumerate(records.items(), start=1):
        source = rec.get('source', 'Без названия')
        fields = rec.get('data', {})
        rows_html = "".join(
            f'<div class="row"><strong>{label}:</strong><span>{value}</span></div>'
            for label, value in fields.items() if value and str(value).strip()
        )
        accordions += f'''
        <div id="record{idx}" class="accordion_inner">
            <div class="accordion open">
                <div class="accordion-header" onclick="toggleAccordion(this)">
                    <span>{source}</span>
                    <div class="accordion-arrow">
                        <svg width="13" height="9" viewBox="0 0 21 12"><path d="M1 1L10.5 10L20 1" stroke="#A5AAB4" stroke-width="3" fill="none"/></svg>
                    </div>
                </div>
                <div class="accordion-body"><div class="accordion-content">{rows_html}</div></div>
            </div>
        </div>
        '''

    no_data = ""
    if not records and not main_info:
        no_data = '<div class="accordion_inner"><div class="accordion open"><div class="accordion-header"><span>❌ Данные не найдены</span></div></div></div>'

    return f'''<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Отчёт по запросу {query}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; background: #0b0d10; margin: 0; color: #fff; }}
.container {{ max-width: 1645px; margin: 0 auto; padding: 0 20px; }}
.header {{ margin: 60px 0 45px; }}
.header_inner {{ padding: 40px 30px 40px 75px; background: #13161b; border-radius: 30px; }}
.request {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 38px; flex-wrap: wrap; gap: 15px; }}
.request1 {{ display: flex; align-items: center; gap: 12px; }}
.request_text {{ font-weight: 700; font-size: 34px; color: #fff; margin: 0; }}
.request_number {{ padding: 16px 27px; font-weight: 600; font-size: 23px; background: #0b0d10; border-radius: 20px; }}
.result {{ display: flex; align-items: center; gap: 10px; background: #0b0d10; padding: 14px 20px; border-radius: 20px; }}
.result_text {{ font-weight: 600; font-size: 16px; margin: 0; }}
.result_number {{ background: #ff851f; border-radius: 10px; font-weight: 600; font-size: 16px; padding: 6px 22px; }}
.downloading {{ display: flex; gap: 22px; }}
.btn1 {{ padding: 18px 45px; border-radius: 20px; font-weight: 600; font-size: 16px; cursor: pointer; border: none; color: #fff; }}
.downloadPDF {{ background: #ff8119; }}
.print {{ background: #0b0d10; }}
.main_inner {{ display: flex; gap: 33px; }}
.block1 {{ width: 25%; }}
.block2 {{ width: 73%; padding-bottom: 40px; }}
.block_title {{ font-weight: 600; font-size: 14px; margin: 0 0 16px 35px; }}
.bg_str {{ background: #13161b; padding-right: 28px; border-radius: 20px; }}
.structure {{ padding: 30px 16px 30px 38px; background: #13161b; border-radius: 20px; max-height: calc(100vh - 134px); overflow-y: auto; }}
.client {{ display: flex; align-items: center; gap: 10px; }}
.clients_name {{ font-weight: 500; font-size: 16px; color: #fff; text-decoration: none; }}
.stick {{ width: 4px; height: 36px; background: #222730; margin-left: 9px; }}
.accordion_inner {{ padding: 20px 26px 20px 16px; background: #13161b; border-radius: 20px; margin-bottom: 30px; }}
.accordion-header {{ padding: 14px 10px 14px 35px; background: #0b0d10; display: flex; justify-content: space-between; cursor: pointer; font-weight: 700; font-size: 18px; border-radius: 15px; }}
.accordion-arrow {{ padding: 8px 14px; background: #13161b; border-radius: 10px; transition: transform 0.3s; }}
.accordion-body {{ max-height: 0; overflow: hidden; transition: max-height 0.3s; }}
.accordion.open .accordion-body {{ max-height: 4000px; }}
.accordion.open .accordion-arrow {{ transform: rotate(180deg); }}
.accordion-content {{ padding: 30px 20px 10px 30px; }}
.accordion-content .row {{ display: flex; justify-content: space-between; margin-bottom: 20px; gap: 10px; }}
.accordion-content .row strong {{ font-weight: 500; font-size: 16px; }}
.accordion-content .row span {{ font-weight: 600; font-size: 16px; color: #ff851f; text-align: right; width: 60%; word-break: break-word; }}
@media (max-width: 990px) {{ .block1 {{ display: none; }} .block2 {{ width: 100%; }} .header_inner {{ padding: 20px 25px; }} }}
</style></head><body>
<header class="header"><div class="container"><div class="header_inner">
<div class="request">
<div class="request1"><h2 class="request_text">Запрос:</h2><div class="request_number">{query}</div></div>
<div class="result"><h3 class="result_text">Результатов:</h3><div class="result_number">{records_count}</div>
<h3 class="result_text" style="margin-left:20px">Просмотров:</h3><div class="result_number">{views}</div></div>
</div>
<div class="downloading">
<button onclick="downloadPDF()" class="downloadPDF btn1">Сохранить в PDF</button>
<button onclick="window.print()" class="print btn1">Печатать</button>
</div></div></div></header>
<div class="main"><div class="container"><div class="main_inner">
<div class="block1"><div class="bg_str"><div class="structure">
<div class="client"><a href="#main" class="clients_name">Основная информация</a></div>
<div class="stick"></div>{structure_items}
</div></div></div>
<div class="block2" id="printArea">
<h3 class="block_title">Полный отчёт</h3>
<div id="main" class="accordion_inner"><div class="accordion open">
<div class="accordion-header" onclick="toggleAccordion(this)"><span>Основная информация</span>
<div class="accordion-arrow"><svg width="13" height="9" viewBox="0 0 21 12"><path d="M1 1L10.5 10L20 1" stroke="#A5AAB4" stroke-width="3" fill="none"/></svg></div></div>
<div class="accordion-body"><div class="accordion-content">
{''.join(f'<div class="row"><strong>{l.upper()}:</strong><span>{v}</span></div>' for l, v in main_info) if main_info else '<div class="row"><span style="width:100%;text-align:center">Нет данных</span></div>'}
</div></div></div></div>
{accordions if records else no_data}
</div></div></div></div>
<script>
function toggleAccordion(h) {{ h.parentElement.classList.toggle('open'); }}
function downloadPDF() {{
    const el = document.getElementById('printArea');
    html2pdf().set({{ margin:0, filename:'report.pdf', image:{{type:'jpeg',quality:1}}, html2canvas:{{scale:1.5}}, jsPDF:{{unit:'pt',format:'a4',orientation:'portrait'}} }}).from(el).save();
}}
</script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js"></script>
</body></html>'''

# === USERS ===
async def generate_referral_code(user_id: int) -> str:
    async with db_lock:
        while True:
            code = f"REF{user_id}{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"
            async with db_conn.execute('SELECT user_id FROM users WHERE referral_code = ?', (code,)) as cur:
                if not await cur.fetchone():
                    break
        await db_conn.execute('UPDATE users SET referral_code = ? WHERE user_id = ?', (code, user_id))
        await db_conn.commit()
    return code

async def get_referral_stats(user_id: int):
    async with db_lock:
        async with db_conn.execute('SELECT COUNT(*) FROM referrals WHERE referrer_id = ?', (user_id,)) as cur:
            row = await cur.fetchone()
        invited = row[0] if row else 0
    return invited, invited

async def get_user_available_requests(user_id: int) -> int:
    async with db_lock:
        async with db_conn.execute(
            'SELECT daily_requests, bonus_requests, last_request_date FROM users WHERE user_id = ?', (user_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return 0
    today = datetime.now().strftime('%Y-%m-%d')
    daily = row['daily_requests'] if row['last_request_date'] == today else 0
    limit = 5 + (row['bonus_requests'] or 0)
    available = limit - daily
    return available if available > 0 else 0

async def use_request(user_id: int) -> bool:
    if await get_user_available_requests(user_id) <= 0:
        return False
    today = datetime.now().strftime('%Y-%m-%d')
    async with db_lock:
        await db_conn.execute(
            'UPDATE users SET daily_requests = daily_requests + 1, last_request_date = ? WHERE user_id = ?',
            (today, user_id)
        )
        await db_conn.commit()
    return True

async def create_user(user_id: int, username: str = None, referred_by: int = None):
    async with db_lock:
        await db_conn.execute(
            'INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)', (user_id, username)
        )
        await db_conn.commit()
    code = await generate_referral_code(user_id)
    if referred_by and referred_by != user_id:
        async with db_lock:
            async with db_conn.execute('SELECT user_id FROM users WHERE user_id = ?', (referred_by,)) as cur:
                ref = await cur.fetchone()
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
                    await db_conn.commit()
                except Exception:
                    pass
    return code

async def get_user(user_id: int):
    async with db_lock:
        async with db_conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cur:
            return await cur.fetchone()

async def get_referral_code(user_id: int):
    async with db_lock:
        async with db_conn.execute('SELECT referral_code FROM users WHERE user_id = ?', (user_id,)) as cur:
            row = await cur.fetchone()
    return row['referral_code'] if row else None

async def save_report(phone: str, data: dict):
    async with db_lock:
        await db_conn.execute(
            'INSERT INTO reports (phone, data) VALUES (?, ?) ON CONFLICT(phone) DO UPDATE SET data = excluded.data, created_at = CURRENT_TIMESTAMP',
            (phone, json.dumps(data, ensure_ascii=False))
        )
        await db_conn.commit()

async def get_unique_views_phone(phone: str, user_id: int) -> int:
    async with db_lock:
        async with db_conn.execute('SELECT user_ids FROM phone_views WHERE phone = ?', (phone,)) as cur:
            row = await cur.fetchone()
        user_ids = json.loads(row['user_ids']) if row and row['user_ids'] else []
        if user_id not in user_ids:
            user_ids.append(user_id)
        await db_conn.execute(
            'INSERT INTO phone_views (phone, user_ids) VALUES (?, ?) ON CONFLICT(phone) DO UPDATE SET user_ids = excluded.user_ids',
            (phone, json.dumps(user_ids))
        )
        await db_conn.commit()
    return len(user_ids)

# === ПРОМОКОДЫ ===
async def create_promo_code(code: str, max_uses: int, requests_granted: int, created_by: int) -> bool:
    async with db_lock:
        try:
            await db_conn.execute(
                'INSERT INTO promo_codes (code, max_uses, requests_granted, created_by) VALUES (?, ?, ?, ?)',
                (code, max_uses, requests_granted, created_by)
            )
            await db_conn.commit()
            return True
        except Exception:
            return False

async def get_promo_code(code: str):
    async with db_lock:
        async with db_conn.execute('SELECT * FROM promo_codes WHERE code = ?', (code,)) as cur:
            return await cur.fetchone()

async def activate_promo_code(user_id: int, code: str):
    async with db_lock:
        async with db_conn.execute('SELECT * FROM promo_codes WHERE code = ?', (code,)) as cur:
            row = await cur.fetchone()
        if not row:
            return False, "Промокод не найден."
        if row['used_count'] >= row['max_uses']:
            return False, "Промокод уже использован максимальное количество раз."
        await db_conn.execute('UPDATE promo_codes SET used_count = used_count + 1 WHERE code = ?', (code,))
        await db_conn.execute(
            'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
            (row['requests_granted'], user_id)
        )
        await db_conn.commit()
    return True, f"Промокод активирован! Вы получили {row['requests_granted']} дополнительных запросов."

async def get_all_promo_codes():
    async with db_lock:
        async with db_conn.execute('SELECT * FROM promo_codes ORDER BY created_at DESC') as cur:
            return await cur.fetchall()

async def delete_promo_code(code: str):
    async with db_lock:
        await db_conn.execute('DELETE FROM promo_codes WHERE code = ?', (code,))
        await db_conn.commit()

# === ПЛАТЕЖИ ===
async def create_crypto_pay_invoice(user_id: int, amount_usd: float, requests_count: int):
    if not CRYPTOPAY_TOKEN:
        return None, None, "CRYPTOPAY_TOKEN не настроен."
    session = await get_http_session()
    url = f"{CRYPTOPAY_BASE}/api/createInvoice"
    headers = {"Crypto-Pay-API-Token": CRYPTOPAY_TOKEN, "Content-Type": "application/json"}
    payload = {
        "amount": str(amount_usd),
        "currency_type": "fiat",
        "fiat": "USD",
        "description": f"Пополнение запросов: {requests_count} шт.",
        "payload": json.dumps({"user_id": user_id, "requests": requests_count}),
        "allow_comments": False,
        "allow_anonymous": False,
    }
    try:
        async with session.post(url, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('ok'):
                    invoice = data['result']
                    invoice_id = str(invoice['invoice_id'])
                    pay_url = invoice['pay_url']
                    async with db_lock:
                        await db_conn.execute(
                            'INSERT INTO purchases (user_id, invoice_id, amount, currency, requests, status) VALUES (?, ?, ?, ?, ?, ?)',
                            (user_id, invoice_id, amount_usd, 'USD', requests_count, 'pending')
                        )
                        await db_conn.commit()
                    return pay_url, invoice_id, None
                return None, None, f"Ошибка CryptoPay: {data.get('error', {}).get('message', 'unknown')}"
            return None, None, f"Ошибка HTTP {resp.status}"
    except Exception as e:
        return None, None, f"Ошибка сети: {e}"

async def process_crypto_pay_payment(invoice_id: str):
    async with db_lock:
        async with db_conn.execute('SELECT * FROM purchases WHERE invoice_id = ?', (invoice_id,)) as cur:
            purchase = await cur.fetchone()
        if not purchase or purchase['status'] != 'pending':
            return
        await db_conn.execute(
            'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
            (purchase['requests'], purchase['user_id'])
        )
        await db_conn.execute(
            'UPDATE purchases SET status = ?, confirmed_at = CURRENT_TIMESTAMP WHERE invoice_id = ?',
            ('confirmed', invoice_id)
        )
        await db_conn.commit()
    try:
        await bot.send_message(
            purchase['user_id'],
            f"Оплата USDT подтверждена! Начислено {purchase['requests']} запросов."
        )
    except Exception:
        pass

async def check_payment_status(user_id: int, invoice_id: str):
    if not CRYPTOPAY_TOKEN:
        return {"status": "error", "message": "CRYPTOPAY_TOKEN не настроен."}
    session = await get_http_session()
    url = f"{CRYPTOPAY_BASE}/api/getInvoices?invoice_id={invoice_id}"
    headers = {"Crypto-Pay-API-Token": CRYPTOPAY_TOKEN}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('ok'):
                    invoices = data.get('result', {}).get('items', [])
                    if invoices:
                        status = invoices[0].get('status')
                        if status == 'paid': return {"status": "paid", "message": "Оплачено!"}
                        if status == 'active': return {"status": "pending", "message": "Счёт ещё не оплачен"}
                        if status == 'expired': return {"status": "expired", "message": "Счёт истёк."}
                        if status == 'cancelled': return {"status": "cancelled", "message": "Счёт отменён."}
                        return {"status": "pending", "message": f"Статус: {status}"}
                    return {"status": "not_found", "message": "Счёт не найден."}
                return {"status": "error", "message": f"Ошибка API"}
            return {"status": "error", "message": f"HTTP {resp.status}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

async def create_stars_invoice(user_id: int, stars_price: int, requests_count: int):
    temp_invoice_id = f"stars_{user_id}_{int(datetime.now().timestamp())}"
    async with db_lock:
        await db_conn.execute(
            'INSERT INTO purchases (user_id, invoice_id, amount, currency, requests, status) VALUES (?, ?, ?, ?, ?, ?)',
            (user_id, temp_invoice_id, stars_price, 'XTR', requests_count, 'pending')
        )
        await db_conn.commit()
    prices = [LabeledPrice(label=f"{requests_count} запросов", amount=stars_price)]
    try:
        await bot.send_invoice(
            chat_id=user_id,
            title=f"Пополнение: {requests_count} запросов",
            description=f"Вы получаете {requests_count} дополнительных запросов.",
            provider_token="",
            currency="XTR",
            prices=prices,
            start_parameter=f"stars_{user_id}_{int(datetime.now().timestamp())}",
            payload=json.dumps({"user_id": user_id, "requests": requests_count, "temp_invoice_id": temp_invoice_id}),
        )
        return True, temp_invoice_id
    except Exception as e:
        logger.error(f"Stars invoice error: {e}")
        async with db_lock:
            await db_conn.execute('DELETE FROM purchases WHERE invoice_id = ?', (temp_invoice_id,))
            await db_conn.commit()
        return False, None

async def process_stars_payment(charge_id: str, user_id: int, payload: str):
    data = json.loads(payload)
    temp_invoice_id = data.get("temp_invoice_id")
    requests = data.get("requests", 0)
    async with db_lock:
        async with db_conn.execute(
            'SELECT * FROM purchases WHERE invoice_id = ? AND user_id = ?', (temp_invoice_id, user_id)
        ) as cur:
            purchase = await cur.fetchone()
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
        await db_conn.commit()
    try:
        await bot.send_message(user_id, f"Оплата Stars подтверждена! Начислено {requests} запросов.")
    except Exception:
        pass

# === BOT ===
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("parse"))
async def parse_html_command(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Доступ запрещён.")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Укажите URL: `/parse https://example.com [запрос]`", parse_mode="Markdown")
        return
    url = args[1]
    query = ' '.join(args[2:]) if len(args) > 2 else None
    status = await message.reply("🔍 Парсим страницу...")
    try:
        result = await parse_html_page(url, query)
        if result.get('error'):
            await status.edit_text(f"❌ Ошибка: {result['error']}")
            return
        text = f"📄 **{result.get('title', 'Без заголовка')}**\n🔗 {url}\n\n"
        if result.get('emails'):
            text += f"📧 Email: {', '.join(result['emails'][:5])}\n"
        if result.get('phones'):
            text += f"📞 Телефоны: {', '.join(result['phones'][:5])}\n"
        if result.get('addresses'):
            text += f"📍 Адреса: {', '.join(result['addresses'][:3])}\n"
        if result.get('inn'):
            text += f"📄 ИНН: {', '.join(result['inn'][:3])}\n"
        if result.get('telegrams'):
            text += f"📱 Telegram: {', '.join(result['telegrams'][:3])}\n"
        await status.edit_text(text[:4000])
        file = BufferedInputFile(result.get('full_text', '').encode('utf-8'),
                                 filename=f"parse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        await message.reply_document(file, caption="📄 Полный текст")
    except Exception as e:
        await status.edit_text(f"❌ Ошибка: {e}")

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
            async with db_lock:
                async with db_conn.execute('SELECT user_id FROM users WHERE referral_code = ?', (ref_code,)) as cur:
                    row = await cur.fetchone()
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
        "┌ Контакты:\n├ Телефон → +79999999999\n└ Email → ivanov@gmail.com\n\n"
        "┌ Соцсети:\n└ VK → vk.com/id1234567\n\n"
        "┌ Онлайн-следы:\n└ IP → 185.85.219.243\n\n"
        "┌ Физ. лица:\n├ ИНН → /inn 123456789012\n└ ФИО → Иванов Иван Иванович\n\n"
        "Каждые 24 часа выдаётся по 5 бесплатных запросов.\n\n"
        "📎 /parse URL [запрос] — парсинг страницы"
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

async def process_general_query(message: Message, query: str, search_type: str):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await create_user(user_id, message.from_user.username)
    if await get_user_available_requests(user_id) <= 0:
        await message.reply("Лимит запросов исчерпан.")
        return
    status = await message.reply(f"🔍 Поиск по {search_type}...")
    try:
        data = await collect_general_data(query, search_type)
        if search_type == "phone":
            views = await get_unique_views_phone(query, user_id)
            await save_report(query, data)
        else:
            views = 0
        html = generate_html_report(data, views)
        file = BufferedInputFile(html.encode('utf-8'), filename=f"report_{search_type}_{query}.html")
        await status.delete()
        await message.reply_document(file, caption=f"📋 Отчёт по {search_type}: {query}")
        await use_request(user_id)
    except Exception as e:
        await status.edit_text(f"❌ Ошибка: {e}")

@dp.message(lambda msg: msg.text and not msg.text.startswith('/'), StateFilter(None))
async def universal_handler(message: Message):
    text = message.text.strip()
    await process_general_query(message, text, detect_type(text))

# === КОЛБЭКИ ===
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
                text=f"Выгодный · {pkg['requests']} запр. · ${pkg['usd']}",
                callback_data=f"pkg_{pkg['requests']}_{pkg['usd']}_{pkg['stars']}"
            )])
        else:
            buttons.append(InlineKeyboardButton(
                text=f"{pkg['requests']} запр. · ${pkg['usd']}",
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
        [InlineKeyboardButton(text=f"Оплатить звёздами ({stars}⭐)", callback_data=f"pay_stars_{rq}_{stars}")],
        [InlineKeyboardButton(text=f"Оплатить USDT (${usd})", callback_data=f"pay_usdt_{rq}_{usd}")],
        [InlineKeyboardButton(text="Назад", callback_data="buy_requests")],
    ])
    await cb.message.edit_text(
        f"Пакет: {rq} запросов\nЦена: ${usd} или {stars}⭐\n\nВыберите способ оплаты:",
        reply_markup=kb
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("pay_stars_"))
async def pay_stars_cb(cb: CallbackQuery):
    parts = cb.data.split("_")
    rq, stars = int(parts[2]), int(parts[3])
    ok, _ = await create_stars_invoice(cb.from_user.id, stars, rq)
    if not ok:
        await cb.message.edit_text(
            "Ошибка создания счёта.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="buy_requests")]])
        )
    else:
        await cb.message.edit_text(
            "Счёт создан. Оплатите в Telegram.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="buy_requests")]])
        )
    await cb.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("pay_usdt_"))
async def pay_usdt_cb(cb: CallbackQuery):
    parts = cb.data.split("_")
    rq, usd = int(parts[2]), float(parts[3])
    pay_url, invoice_id, err = await create_crypto_pay_invoice(cb.from_user.id, usd, rq)
    if not pay_url:
        await cb.message.edit_text(
            f"Ошибка: {err}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="buy_requests")]])
        )
        await cb.answer()
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Оплатить USDT", url=pay_url)],
        [InlineKeyboardButton(text="Я оплатил", callback_data=f"check_usdt_{invoice_id}")],
        [InlineKeyboardButton(text="Назад", callback_data="buy_requests")],
    ])
    await cb.message.edit_text(
        f"Счёт USDT:\nПакет: {rq} запросов\nСумма: ${usd}\n\nНажмите «Оплатить», затем «Я оплатил».",
        reply_markup=kb
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("check_usdt_"))
async def check_usdt_cb(cb: CallbackQuery):
    invoice_id = cb.data.replace("check_usdt_", "")
    result = await check_payment_status(cb.from_user.id, invoice_id)
    if result["status"] == "paid":
        await process_crypto_pay_payment(invoice_id)
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В меню", callback_data="back_to_menu")]])
        await cb.message.edit_text("Оплата подтверждена! Запросы начислены.", reply_markup=kb)
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Проверить снова", callback_data=f"check_usdt_{invoice_id}")],
            [InlineKeyboardButton(text="Назад", callback_data="buy_requests")],
        ])
        await cb.message.edit_text(result["message"], reply_markup=kb)
    await cb.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(q: PreCheckoutQuery):
    await q.answer(ok=True)

@dp.message(lambda m: m.successful_payment is not None)
async def success_payment_handler(message: Message):
    sp = message.successful_payment
    await process_stars_payment(sp.telegram_payment_charge_id, message.from_user.id, sp.invoice_payload)

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

# === АДМИН ===
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
        async with db_lock:
            async with db_conn.execute('SELECT COUNT(*) FROM users') as cur: users = (await cur.fetchone())[0]
            async with db_conn.execute('SELECT COUNT(*) FROM reports') as cur: reports = (await cur.fetchone())[0]
            async with db_conn.execute('SELECT COUNT(*) FROM promo_codes') as cur: promos = (await cur.fetchone())[0]
            async with db_conn.execute("SELECT COUNT(*) FROM purchases WHERE status='confirmed'") as cur: payments = (await cur.fetchone())[0]
        await cb.message.edit_text(
            f"Статистика\nПользователей: {users}\nОтчётов: {reports}\nПромокодов: {promos}\nПлатежей: {payments}",
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
            await cb.message.edit_text("Выберите промокод для удаления:",
                                       reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    elif action == "payments":
        async with db_lock:
            async with db_conn.execute('SELECT * FROM purchases ORDER BY created_at DESC LIMIT 10') as cur:
                rows_db = await cur.fetchall()
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
        async with db_lock:
            await db_conn.execute('UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?', (n, uid))
            await db_conn.commit()
        await message.reply(f"Выдано {n} запросов пользователю {uid}.")
        await state.clear()
    except ValueError:
        await message.reply("Введите число.")

@dp.message(Broadcast.waiting_for_text)
async def broadcast_input(message: Message, state: FSMContext):
    text = message.text
    async with db_lock:
        async with db_conn.execute('SELECT user_id FROM users') as cur:
            users = await cur.fetchall()
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

# === ЗАПУСК ===
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
            await db_conn.close()
        if http_session:
            await http_session.close()

if __name__ == "__main__":
    asyncio.run(main())
