import os
import asyncio
import re
import json
import random
import string
import asyncpg
import aiohttp
from aiohttp import web
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile, PreCheckoutQuery, LabeledPrice, SuccessfulPayment
)
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# === ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не задан")

DEPSEARCH_TOKEN = os.getenv("DEPSEARCH_TOKEN")
DEPSEARCH_BASE = os.getenv("DEPSEARCH_BASE", "https://api.depsearch.sbs")
NIGHTSEARCH_API_KEY = os.getenv("NIGHTSEARCH_API_KEY")
SEON_API_KEY = os.getenv("SEON_API_KEY")
SNUSBASE_API_KEY = os.getenv("SNUSBASE_API_KEY")
JITLER_TOKENS_STR = os.getenv("JITLER_TOKENS", "")
JITLER_TOKENS = [t.strip() for t in JITLER_TOKENS_STR.split(",") if t.strip()]

CRYPTOPAY_TOKEN = "626190:AAkRdsFEHPdfZc6yIACB52hh1mVKkOdT0qc"
logger.warning("CRYPTOPAY_TOKEN вшит в код. Для продакшена используйте .env и перевыпустите токен.")

CHANNEL_USERNAME = "@dataseekerinfo"
CHANNEL_LINK = "tg://resolve?domain=dataseekerinfo"
ADMIN_IDS = [8559629118]

db_pool = None
http_session = None

# === КЭШ ===
cache = {}
CACHE_TTL = timedelta(hours=1)

def get_cache_key(func_name: str, query: str) -> str:
    return f"{func_name}:{hashlib.md5(query.encode()).hexdigest()}"

# === ТАЙМАУТЫ ===
API_TIMEOUTS = {
    "nightsearch": 8.0,
    "seon": 6.0,
    "snusbase": 6.0,
    "depsearch": 10.0,
    "jitler": 8.0,
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

# === ПАКЕТЫ ОПЛАТЫ ===
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
    else:
        return "дней"

# === БАЗА ДАННЫХ ===
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                daily_requests INTEGER DEFAULT 0,
                last_request_date DATE DEFAULT CURRENT_DATE,
                bonus_requests INTEGER DEFAULT 0,
                referral_code TEXT UNIQUE,
                referred_by BIGINT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                phone TEXT PRIMARY KEY,
                data JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS phone_views (
                phone TEXT PRIMARY KEY,
                user_ids JSONB DEFAULT '[]'
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promo_codes (
                id SERIAL PRIMARY KEY,
                code VARCHAR(50) UNIQUE,
                max_uses INTEGER NOT NULL,
                used_count INTEGER DEFAULT 0,
                requests_granted INTEGER NOT NULL,
                created_by BIGINT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                referrer_id BIGINT REFERENCES users(user_id),
                referred_id BIGINT REFERENCES users(user_id),
                bonus_given BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                invoice_id VARCHAR(100) UNIQUE,
                amount DECIMAL(10,2),
                currency VARCHAR(10),
                requests INTEGER NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW(),
                confirmed_at TIMESTAMP
            )
        ''')

async def get_pool():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return db_pool

async def get_http_session():
    global http_session
    if http_session is None:
        http_session = aiohttp.ClientSession()
    return http_session

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
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

def deep_find(data, key, default=None):
    if data is None:
        return default
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for v in data.values():
            result = deep_find(v, key, default)
            if result != default:
                return result
    elif isinstance(data, list):
        for item in data:
            result = deep_find(item, key, default)
            if result != default:
                return result
    return default

def deep_find_all(data, key):
    results = []
    if data is None:
        return results
    if isinstance(data, dict):
        if key in data:
            results.append(data[key])
        for v in data.values():
            results.extend(deep_find_all(v, key))
    elif isinstance(data, list):
        for item in data:
            results.extend(deep_find_all(item, key))
    return results

def clean_value(value):
    if value is None:
        return None
    if isinstance(value, (str, int, float)):
        return str(value)
    if isinstance(value, dict):
        if 'url' in value and value['url']:
            return value['url']
        if 'name' in value and value['name']:
            return value['name']
        if 'username' in value and value['username']:
            return value['username']
        if 'id' in value and value['id']:
            return str(value['id'])
        if 'value' in value and value['value']:
            return str(value['value'])
        return None
    if isinstance(value, list) and not value:
        return None
    return value if value not in (None, "", [], {}, False) else None

def extract_telegram(tg_data):
    if isinstance(tg_data, dict):
        username = tg_data.get('username') or tg_data.get('user') or tg_data.get('nick')
        user_id = tg_data.get('id') or tg_data.get('tg_id') or tg_data.get('user_id')
        if username:
            if not username.startswith('@'):
                username = '@' + username
        if username and user_id:
            return f"{username} [{user_id}]"
        elif username:
            return username
        elif user_id:
            return f"ID: {user_id}"
    return None

def normalize_list(values):
    result = []
    for v in values:
        cleaned = clean_value(v)
        if cleaned:
            result.append(str(cleaned))
    return list(dict.fromkeys(result))

def find_best_birthdate(birthdates):
    if not birthdates:
        return None
    best = None
    best_score = 0
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
            best_score = score
            best = bd
    return best

def calculate_age_from_birthdate(birthdate_str):
    if not birthdate_str:
        return None
    try:
        for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
            try:
                bd = datetime.strptime(birthdate_str, fmt)
                today = datetime.now()
                age = today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
                return age
            except ValueError:
                continue
        cleaned = birthdate_str.replace('/', '.').replace('-', '.')
        bd = datetime.strptime(cleaned, '%d.%m.%Y')
        today = datetime.now()
        age = today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
        return age
    except Exception:
        return None

def normalize_birthdate(value):
    if not value:
        return None
    if isinstance(value, dict):
        value = (
            value.get("value")
            or value.get("date")
            or value.get("birthdate")
            or value.get("birth_date")
            or value.get("date_of_birth")
        )
    if isinstance(value, list):
        for item in value:
            result = normalize_birthdate(item)
            if result:
                return result
        return None
    if not isinstance(value, str):
        return None
    match = re.search(r'\b(\d{1,2}[./-]\d{1,2}[./-]\d{4})\b', value)
    if match:
        return match.group(1)
    match = re.search(r'\b(\d{4}[./-]\d{1,2}[./-]\d{1,2})\b', value)
    if match:
        return match.group(1)
    return None

def extract_socials(data):
    result = {'vk': None, 'ok': None, 'instagram': None, 'tiktok': None}
    def _search(obj):
        if obj is None:
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                key_lower = key.lower()
                if 'vk' in key_lower or 'vkontakte' in key_lower:
                    if value and not result['vk']:
                        result['vk'] = get_social_url(value)
                elif 'ok' in key_lower or 'odnoklassniki' in key_lower:
                    if value and not result['ok']:
                        result['ok'] = get_social_url(value)
                elif 'instagram' in key_lower:
                    if value and not result['instagram']:
                        result['instagram'] = get_social_url(value)
                elif 'tiktok' in key_lower:
                    if value and not result['tiktok']:
                        result['tiktok'] = get_social_url(value)
                if isinstance(value, str):
                    if 'vk.com' in value and not result['vk']:
                        result['vk'] = value
                    elif 'ok.ru' in value and not result['ok']:
                        result['ok'] = value
                    elif 'instagram.com' in value and not result['instagram']:
                        result['instagram'] = value
                    elif 'tiktok.com' in value and not result['tiktok']:
                        result['tiktok'] = value
                _search(value)
        elif isinstance(obj, list):
            for item in obj:
                _search(item)
    _search(data)
    return result

# === JITLER BALANCER ===
class JitlerBalancer:
    def __init__(self, tokens):
        self.tokens = tokens
        self.current_index = 0
        self.lock = asyncio.Lock()
        self.failed_tokens = set()

    async def get_token(self):
        async with self.lock:
            if not self.tokens:
                return None
            for _ in range(len(self.tokens)):
                idx = self.current_index % len(self.tokens)
                token = self.tokens[idx]
                if token not in self.failed_tokens:
                    self.current_index = (idx + 1) % len(self.tokens)
                    return token
                self.current_index = (idx + 1) % len(self.tokens)
            self.failed_tokens.clear()
            return self.tokens[self.current_index % len(self.tokens)]

    def mark_failed(self, token):
        self.failed_tokens.add(token)

    def mark_success(self, token):
        if token in self.failed_tokens:
            self.failed_tokens.remove(token)

balancer = JitlerBalancer(JITLER_TOKENS)

# === API ЗАПРОСЫ ===
async def nightsearch_search(query: str):
    if not NIGHTSEARCH_API_KEY:
        return {}
    session = await get_http_session()
    url = "https://nightsearch.life/api/search"
    headers = {"X-API-Key": NIGHTSEARCH_API_KEY, "Content-Type": "application/json"}
    payload = {"query": query, "search_type": "phone"}
    try:
        async with session.post(url, json=payload, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["nightsearch"])) as resp:
            if resp.status == 200:
                return await resp.json()
            return {}
    except Exception:
        return {}

async def seon_search(query: str):
    if not SEON_API_KEY:
        return {}
    session = await get_http_session()
    url = "https://api.seon.io/SeonRestService/phone-api/v2"
    headers = {"X-API-KEY": SEON_API_KEY, "Content-Type": "application/json"}
    payload = {"phone": query}
    try:
        async with session.post(url, json=payload, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["seon"])) as resp:
            if resp.status == 200:
                return await resp.json()
            return {}
    except Exception:
        return {}

async def snusbase_search(query: str):
    if not SNUSBASE_API_KEY:
        return {}
    session = await get_http_session()
    url = "https://api.snusbase.com/data/search"
    headers = {"Auth": SNUSBASE_API_KEY, "Content-Type": "application/json"}
    payload = {"terms": [query], "types": ["email", "username", "phone", "name"], "wildcard": False}
    try:
        async with session.post(url, json=payload, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["snusbase"])) as resp:
            if resp.status == 200:
                return await resp.json()
            return {}
    except Exception:
        return {}

async def depsearch_search(query: str):
    if not DEPSEARCH_TOKEN:
        return {}
    session = await get_http_session()
    
    # Сохраняем оригинальный запрос для логов
    original_query = query
    
    # Нормализация номера телефона
    if re.match(r'^\+?\d{10,15}$', query):
        query = re.sub(r'[^0-9]', '', query)
        logger.info(f"DepSearch: normalized phone from '{original_query}' to '{query}'")
    
    url = f"{DEPSEARCH_BASE}/quest={query}&token={DEPSEARCH_TOKEN}&lang=ru"
    logger.info(f"DepSearch URL: {url}")
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["depsearch"])) as resp:
            text = await resp.text()
            # Убираем BOM если есть
            text = text.lstrip('\ufeff')
            logger.info(f"DepSearch: HTTP {resp.status}, response length: {len(text)}")
            logger.info(f"DepSearch raw response (first 500 chars): {text[:500]}")
            
            if resp.status == 200:
                try:
                    data = json.loads(text)
                    logger.info(f"DepSearch parsed JSON, type: {type(data)}")
                    if isinstance(data, dict):
                        logger.info(f"DepSearch keys: {list(data.keys())}")
                        if 'results' in data:
                            logger.info(f"DepSearch results count: {len(data['results'])}")
                            if data['results']:
                                logger.info(f"First result: {data['results'][0]}")
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"DepSearch JSON decode error: {e}")
                    logger.error(f"Text that caused error: {text[:500]}")
                    return {}
            else:
                logger.warning(f"DepSearch HTTP error {resp.status}: {text[:200]}")
                return {}
    except asyncio.TimeoutError:
        logger.error(f"DepSearch timeout for query: {query}")
        return {}
    except Exception as e:
        logger.error(f"DepSearch error: {e}")
        return {}

async def jitler_search_with_balancer(query: str, search_type: str = "number"):
    session = await get_http_session()
    for attempt in range(2):
        token = await balancer.get_token()
        if not token:
            return {}
        url = "https://api.jitler.top/search"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"type": search_type, "query": query, "page": 1}
        try:
            async with session.post(url, json=payload, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["jitler"])) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('result'):
                        if 'id' in data:
                            task_id = data['id']
                            for _ in range(3):
                                await asyncio.sleep(0.5)
                                try:
                                    async with session.get(
                                        f"https://api.jitler.top/search/{task_id}",
                                        headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=3)
                                    ) as get_resp:
                                        if get_resp.status == 200:
                                            result_data = await get_resp.json()
                                            if result_data.get('result'):
                                                response = result_data.get('response')
                                                if response is not None:
                                                    balancer.mark_success(token)
                                                    return {'result': True, 'response': response}
                                                elif response == []:
                                                    balancer.mark_success(token)
                                                    return {'result': True, 'response': []}
                                        elif get_resp.status == 501:
                                            continue
                                        else:
                                            break
                                except asyncio.TimeoutError:
                                    continue
                            return {}
                        elif 'response' in data:
                            balancer.mark_success(token)
                            return data
                    return {}
                elif resp.status == 429:
                    balancer.mark_failed(token)
                    continue
                return {}
        except asyncio.TimeoutError:
            continue
        except Exception:
            continue
    return {}

async def ip_info_search(query: str):
    session = await get_http_session()
    url = f"http://ip-api.com/json/{query}?fields=status,message,country,regionName,city,zip,lat,lon,timezone,isp,org,as,query"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["ipapi"])) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'success':
                    return data
            return {}
    except Exception:
        return {}

# === УНИВЕРСАЛЬНЫЕ ПАРСЕРЫ ===
def parse_depsearch(data):
    logger.info(f"parse_depsearch received data: {type(data)}")
    
    if not data:
        logger.warning("parse_depsearch: data is empty")
        return []
    
    if not isinstance(data, dict):
        logger.warning(f"parse_depsearch: data is not dict, it's {type(data)}")
        return []
    
    logger.info(f"parse_depsearch keys: {list(data.keys())}")
    
    parsed = []
    
    # Основная информация
    phone_info = data.get("phone_info", {})
    if phone_info:
        parsed.append({
            "type": "phone_info",
            "data": phone_info
        })
        logger.info(f"Found phone_info: {phone_info}")
    
    # Все найденные записи
    results = data.get("results", [])
    logger.info(f"Found {len(results)} results in data")
    
    if not results:
        logger.warning("No results found in DepSearch response")
        # Проверяем, может данные в другом поле
        for key in ['data', 'response', 'items']:
            if key in data and isinstance(data[key], list):
                logger.info(f"Found list in '{key}' field, using it as results")
                results = data[key]
                break
    
    for idx, item in enumerate(results):
        logger.info(f"Processing result {idx}: type={type(item)}")
        if isinstance(item, dict):
            fields = {}
            for key, value in item.items():
                if value not in (None, "", [], {}):
                    fields[str(key)] = value
            
            if fields:
                parsed.append({
                    "type": "result",
                    "data": fields
                })
                logger.info(f"Added result {idx} with {len(fields)} fields: {list(fields.keys())[:5]}")
            else:
                logger.warning(f"Result {idx} has no fields after cleaning")
        else:
            logger.warning(f"Result {idx} is not a dict: {type(item)}")
    
    logger.info(f"DepSearch parsed: {len(parsed)} blocks total")
    return parsed

def parse_nightsearch(data):
    logger.info(f"parse_nightsearch received data: {type(data)}")
    
    if not data:
        return []
    
    if not isinstance(data, dict):
        return []
    
    parsed = []
    # Ищем результаты в разных возможных полях
    items = data.get("results", []) or data.get("data", []) or data.get("items", [])
    logger.info(f"NightSearch found {len(items)} items")
    
    for idx, item in enumerate(items):
        if isinstance(item, dict):
            fields = {}
            for key, value in item.items():
                if value not in (None, "", [], {}):
                    fields[str(key)] = value
            if fields:
                parsed.append({
                    "type": "result",
                    "data": fields
                })
    
    logger.info(f"NightSearch parsed: {len(parsed)} blocks")
    return parsed

# === СБОР ДАННЫХ ===
async def collect_general_data(query: str, search_type: str = "phone"):
    cache_key = get_cache_key(search_type, query)
    if cache_key in cache:
        cached_time, data = cache[cache_key]
        if datetime.now() - cached_time < CACHE_TTL:
            logger.info(f"Returning cached data for {query}")
            return data

    # Нормализация номера телефона для запроса
    original_query = query
    if search_type == "phone":
        query = re.sub(r'[^0-9]', '', query)
        logger.info(f"Normalized phone from '{original_query}' to '{query}'")

    tasks = {
        'depsearch': asyncio.create_task(depsearch_search(query)),
        'nightsearch': asyncio.create_task(nightsearch_search(query)),
        'seon': asyncio.create_task(seon_search(query)),
        'snusbase': asyncio.create_task(snusbase_search(query))
    }
    if search_type == "vk":
        tasks['jitler'] = asyncio.create_task(jitler_search_with_balancer(query, "vks"))
    if search_type == "ip":
        tasks['ipapi'] = asyncio.create_task(ip_info_search(query))

    results = {}
    for name, task in tasks.items():
        try:
            results[name] = await asyncio.wait_for(task, timeout=10.0)
        except asyncio.TimeoutError:
            results[name] = {}
            task.cancel()
        except Exception as e:
            logger.error(f"Ошибка {name}: {e}")
            results[name] = {}

    depsearch = results.get('depsearch', {}) or {}
    nightsearch = results.get('nightsearch', {}) or {}
    seon = results.get('seon', {}) or {}
    snusbase = results.get('snusbase', {}) or {}
    jitler = results.get('jitler', {}) if search_type == "vk" else {}
    ipdata = results.get('ipapi', {}) if search_type == "ip" else {}

    dep_parsed = parse_depsearch(depsearch)
    night_parsed = parse_nightsearch(nightsearch)

    result = {
        'query': original_query,
        'type': search_type,
        'operator': None,
        'region': None,
        'country': None,
        'city': None,
        'fio': None,
        'birthdate': None,
        'age': None,
        'address': None,
        'emails': [],
        'telegrams': [],
        'vk': None,
        'instagram': None,
        'tiktok': None,
        'ok': None,
        'phone_books': [],
        'cards': [],
        'banks': [],
        'inn': None,
        'snils': None,
        'passport': None,
        'extra': {},
        'sources': [],
        'records_count': 0
    }

    all_birthdates = []
    sources_set = set()
    records_count = 0
    seen_records = set()

    # ---- IP данные ----
    if ipdata:
        result['country'] = ipdata.get('country') or result['country']
        result['region'] = ipdata.get('regionName') or result['region']
        result['city'] = ipdata.get('city') or result['city']
        if ipdata.get('isp'):
            result['operator'] = ipdata['isp']
        extra_ip = {}
        for key in ['country', 'regionName', 'city', 'zip', 'lat', 'lon', 'timezone', 'isp', 'org', 'as']:
            if ipdata.get(key):
                extra_ip[key] = ipdata[key]
        if extra_ip:
            records_count += 1
            result['extra'][f"IP информация (ip-api.com)"] = {
                'source': 'ip-api.com',
                'data': extra_ip
            }
            sources_set.add("ip-api.com")

    # ---- DepSearch ----
    for block in dep_parsed:
        if block['type'] == 'phone_info':
            info = block['data']
            if info.get('operator'):
                result['operator'] = str(info['operator'])
                sources_set.add("DepSearch (оператор)")
            if info.get('region'):
                result['region'] = str(info['region'])
                sources_set.add("DepSearch (регион)")
            if info.get('country'):
                result['country'] = str(info['country'])
                sources_set.add("DepSearch (страна)")
        elif block['type'] == 'result':
            fields = block['data']
            record_key = "|".join(str(v) for v in fields.values())
            if record_key not in seen_records:
                seen_records.add(record_key)
                source_name = fields.get('🏫Источник', fields.get('Источник', 'DepSearch'))
                source_name = re.sub(r'[^\w\s\-\.]', '', source_name).strip()
                if not source_name:
                    source_name = 'DepSearch'
                sources_set.add(source_name)
                records_count += 1
                result['extra'][f"Запись #{records_count}"] = {
                    'source': source_name,
                    'data': fields
                }
                
                # Динамическое извлечение основных полей
                for key in ['👤ФИО', '👤Полное имя', 'full_name', 'fio', 'ФИО', 'Полное имя', '👤Имя', '🔸Никнейм', 'Имя', 'NAME']:
                    if key in fields and not result['fio']:
                        val = str(fields[key]).strip()
                        if val and val not in ['None', 'null', '']:
                            result['fio'] = val
                            break
                
                for key in ['🎂Дата рождения', 'birthdate', 'Дата рождения', 'BIRTHDATE', 'DATE_OF_BIRTH']:
                    if key in fields and not all_birthdates:
                        normalized = normalize_birthdate(str(fields[key]))
                        if normalized:
                            all_birthdates.append(normalized)
                        break
                
                for key in ['📍Адрес', 'Адрес', 'address', 'ADDRESS']:
                    if key in fields and not result['address']:
                        addr = str(fields[key]).strip()
                        if not re.match(r'^\d{4}-\d{2}-\d{2}', addr):
                            result['address'] = addr
                        break
                
                for key in ['✉️Почта', 'email', 'EMAIL', 'E-mail']:
                    if key in fields and '@' in str(fields[key]):
                        email = str(fields[key]).strip()
                        if email and email not in result['emails']:
                            result['emails'].append(email)
                        break
                
                for key in ['🪪 Паспорт', 'паспорт', 'passport_numbers', 'PASSPORT']:
                    if key in fields and not result['passport']:
                        val = str(fields[key]).strip()
                        if val and val not in ['None', 'null', '']:
                            result['passport'] = val
                            break
                
                for key in ['📄Инн', 'инн', 'inns', 'inn', 'INN']:
                    if key in fields and not result['inn']:
                        val = str(fields[key]).strip()
                        if val and val not in ['None', 'null', '']:
                            result['inn'] = val
                            break
                
                for key in ['📄Снилс', 'снилс', 'snils', 'SNILS']:
                    if key in fields and not result['snils']:
                        val = str(fields[key]).strip()
                        if val and val not in ['None', 'null', '']:
                            result['snils'] = val
                            break
                
                for key in ['📞Телефон', 'Телефон', 'phone', 'PHONE']:
                    if key in fields:
                        phone_val = str(fields[key]).strip()
                        if phone_val and phone_val not in result['phone_books']:
                            result['phone_books'].append(phone_val)
                        break
                
                if not result['vk']:
                    for key in ['🧑‍💻Вконтакте', 'vk', 'VK']:
                        if key in fields:
                            result['vk'] = get_social_url(fields[key])
                            break
                
                if not result['ok']:
                    for key in ['👨‍🦳Одноклассники', 'ok', 'OK']:
                        if key in fields:
                            result['ok'] = get_social_url(fields[key])
                            break
                
                if not result['instagram']:
                    for key in ['📷Instagram', 'instagram', 'INSTAGRAM']:
                        if key in fields:
                            result['instagram'] = get_social_url(fields[key])
                            break
                
                if not result['tiktok']:
                    for key in ['👩‍🦲TikTok', 'tiktok', 'TIKTOK']:
                        if key in fields:
                            result['tiktok'] = get_social_url(fields[key])
                            break

    # ---- NightSearch ----
    for block in night_parsed:
        if block['type'] == 'result':
            fields = block['data']
            record_key = "|".join(str(v) for v in fields.values())
            if record_key not in seen_records:
                seen_records.add(record_key)
                source_name = fields.get('database', fields.get('source', 'NightSearch'))
                source_name = re.sub(r'[^\w\s\-\.]', '', source_name).strip()
                if not source_name:
                    source_name = 'NightSearch'
                sources_set.add(source_name)
                records_count += 1
                result['extra'][f"Запись #{records_count}"] = {
                    'source': source_name,
                    'data': fields
                }
                
                if not result['fio']:
                    for key in ['ФИО', 'Имя', 'full_name', 'NAME']:
                        if key in fields:
                            val = str(fields[key]).strip()
                            if val and val not in ['None', 'null', '']:
                                result['fio'] = val
                                break
                
                if not all_birthdates:
                    for key in ['Дата рождения', 'birthdate', 'BIRTHDATE']:
                        if key in fields:
                            normalized = normalize_birthdate(str(fields[key]))
                            if normalized:
                                all_birthdates.append(normalized)
                            break
                
                if not result['address']:
                    for key in ['Адрес', 'address', 'ADDRESS']:
                        if key in fields:
                            addr = str(fields[key]).strip()
                            if not re.match(r'^\d{4}-\d{2}-\d{2}', addr):
                                result['address'] = addr
                            break
                
                for key in ['E-mail', 'email', 'EMAIL']:
                    if key in fields and '@' in str(fields[key]):
                        email = str(fields[key]).strip()
                        if email and email not in result['emails']:
                            result['emails'].append(email)
                        break
                
                for key in ['Телефон', 'phone', 'PHONE']:
                    if key in fields:
                        phone_val = str(fields[key]).strip()
                        if phone_val and phone_val not in result['phone_books']:
                            result['phone_books'].append(phone_val)
                        break

    # ---- Jitler (VK) ----
    if jitler:
        jitler_data = jitler.get('response', jitler)
        phonebooks = jitler_data.get('phonebooks', [])
        if phonebooks:
            result['phone_books'].extend(phonebooks)
            sources_set.add("Jitler (телефонная книга)")
        profiles = jitler_data.get('profiles', {})
        if profiles.get('vk') and not result['vk']:
            vk_urls = [p.get('url') for p in profiles['vk'] if p.get('url')]
            if vk_urls:
                result['vk'] = vk_urls[0]
                sources_set.add("Jitler (VK)")
        if profiles.get('ok') and not result['ok']:
            ok_urls = [p.get('url') for p in profiles['ok'] if p.get('url')]
            if ok_urls:
                result['ok'] = ok_urls[0]
                sources_set.add("Jitler (OK)")
        if profiles.get('instagram') and not result['instagram']:
            inst_urls = [p.get('url') for p in profiles['instagram'] if p.get('url')]
            if inst_urls:
                result['instagram'] = inst_urls[0]
                sources_set.add("Jitler (Instagram)")
        if profiles.get('tiktok') and not result['tiktok']:
            tt_urls = [p.get('url') for p in profiles['tiktok'] if p.get('url')]
            if tt_urls:
                result['tiktok'] = tt_urls[0]
                sources_set.add("Jitler (TikTok)")

    # ---- SEON / Snusbase ----
    for src, src_name in [(seon, 'SEON'), (snusbase, 'Snusbase')]:
        if not src:
            continue
        tg_data = deep_find_all(src, 'telegram') + deep_find_all(src, 'tg')
        for tg in tg_data:
            formatted = extract_telegram(tg)
            if formatted:
                result['telegrams'].append(formatted)
                sources_set.add(f"{src_name} (Telegram)")
        if not result['vk']:
            vk = deep_find(src, 'vk') or deep_find(src, 'vkontakte')
            if vk:
                result['vk'] = get_social_url(vk)
                sources_set.add(f"{src_name} (VK)")
        if not result['ok']:
            ok = deep_find(src, 'ok') or deep_find(src, 'odnoklassniki')
            if ok:
                result['ok'] = get_social_url(ok)
                sources_set.add(f"{src_name} (OK)")
        if not result['instagram']:
            inst = deep_find(src, 'instagram')
            if inst:
                result['instagram'] = get_social_url(inst)
                sources_set.add(f"{src_name} (Instagram)")
        if not result['tiktok']:
            tt = deep_find(src, 'tiktok')
            if tt:
                result['tiktok'] = get_social_url(tt)
                sources_set.add(f"{src_name} (TikTok)")
        pb = deep_find(src, 'phone_books')
        if pb:
            result['phone_books'].append(str(pb))
            sources_set.add(f"{src_name} (телефонная книга)")
        emails = deep_find_all(src, 'email') + deep_find_all(src, 'mail') + deep_find_all(src, 'e-mail')
        for email in emails:
            if email and '@' in str(email):
                result['emails'].append(str(email))
                sources_set.add(f"{src_name} (email)")

    result['sources'] = list(sources_set)
    result['records_count'] = records_count

    if all_birthdates:
        result['birthdate'] = find_best_birthdate(all_birthdates)
        if result['birthdate']:
            age = calculate_age_from_birthdate(result['birthdate'])
            if age is not None:
                result['age'] = age

    clean_emails = []
    for email in result['emails']:
        if isinstance(email, dict):
            val = email.get('value')
            if val and isinstance(val, str) and '@' in val:
                clean_emails.append(val)
        elif isinstance(email, str):
            match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', str(email))
            if match:
                clean_emails.append(match.group(0))
    result['emails'] = list(dict.fromkeys(clean_emails))
    result['telegrams'] = list(dict.fromkeys(result['telegrams']))
    result['phone_books'] = list(dict.fromkeys(result['phone_books']))

    logger.info(f"Final records_count: {records_count}, extra keys: {len(result['extra'])}")
    
    cache[cache_key] = (datetime.now(), result)
    return result

# === ГЕНЕРАТОР HTML-ОТЧЁТА ===
def generate_html_report(data: dict, views: int = 0) -> str:
    query = data.get('query', '')
    records = data.get('extra', {})
    records_count = len(records)

    main_info = []
    if data.get('operator'):
        main_info.append(("Оператор", data['operator']))
    if data.get('region'):
        main_info.append(("Регион", data['region']))
    if data.get('country'):
        main_info.append(("Страна", data['country']))
    if data.get('city'):
        main_info.append(("Город", data['city']))
    if data.get('fio'):
        main_info.append(("ФИО", data['fio']))
    if data.get('birthdate'):
        main_info.append(("Дата рождения", data['birthdate']))
    if data.get('age') is not None:
        main_info.append(("Возраст", f"{data['age']} лет"))
    if data.get('address'):
        main_info.append(("Адрес", data['address']))
    if data.get('inn'):
        main_info.append(("ИНН", data['inn']))
    if data.get('snils'):
        main_info.append(("СНИЛС", data['snils']))
    if data.get('passport'):
        main_info.append(("Паспорт", data['passport']))
    if data.get('emails'):
        main_info.append(("Email", ', '.join(data['emails'][:5])))
    if data.get('telegrams'):
        main_info.append(("Telegram", ', '.join(data['telegrams'])))
    if data.get('phone_books'):
        main_info.append(("Телефонные книги", ', '.join(data['phone_books'][:5])))
    if data.get('vk'):
        main_info.append(("VK", data['vk']))
    if data.get('ok'):
        main_info.append(("Одноклассники", data['ok']))
    if data.get('instagram'):
        main_info.append(("Instagram", data['instagram']))
    if data.get('tiktok'):
        main_info.append(("TikTok", data['tiktok']))

    structure_items = ""
    for idx, (key, rec) in enumerate(records.items(), start=1):
        source = rec.get('source', 'Без названия')
        structure_items += f"""
        <div class="client">
            <svg width="22.031171798706055" height="22.031171798706055" viewBox="0 0 22.031171798706055 22.031171798706055" xmlns="http://www.w3.org/2000/svg">
                <circle cx="11.015585899353027" cy="11.015585899353027" r="8.085585899353028" fill="none" stroke="#currentColor" stroke-width="5.86"/>
            </svg>
            <a href="#record{idx}" class="clients_name">{source[:30]}</a>
        </div>
        <div class="stick"></div>
        """

    accordions = ""
    for idx, (key, rec) in enumerate(records.items(), start=1):
        source = rec.get('source', 'Без названия')
        fields = rec.get('data', {})
        rows_html = ""
        for label, value in fields.items():
            if value and str(value).strip():
                rows_html += f'<div class="row"><strong>{label.upper()}:</strong><span>{value}</span></div>'
        accordions += f"""
        <div id="record{idx}" class="accordion_inner">
            <div class="accordion open">
                <div class="accordion-header" onclick="toggleAccordion(this)">
                    <span>{source}</span>
                    <div class="accordion-arrow">
                        <svg width="13" height="9" viewBox="0 0 21 12" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <path d="M1 1L10.5 10L20 1" stroke="#A5AAB4" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round"/>
                        </svg>
                    </div>
                </div>
                <div class="accordion-body">
                    <div class="accordion-content">
                        {rows_html}
                    </div>
                </div>
            </div>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Отчёт по запросу {query}</title>
    <style>
        html {{ scroll-behavior: smooth; }}
        body {{
            font-family: "Source Sans Pro", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background-color: #0b0d10;
            margin: 0;
            height: 100vh;
        }}
        *, ::before, ::after {{ box-sizing: border-box; }}
        h1, h2, h3, h4, h5, h6, p {{ margin: 0; }}
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
        .structure::-webkit-scrollbar {{ width: 16px; background: #0b0d10; }}
        .structure::-webkit-scrollbar-track {{ background: #0b0d10; }}
        .structure::-webkit-scrollbar-thumb {{ background: #fff; border: 4px solid #0b0d10; border-radius: 8px; background-clip: padding-box; }}
        .structure::-webkit-scrollbar-button:single-button:vertical:decrement {{ background: #0b0d10 url("data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 24 24%22 fill=%22white%22%3E%3Cpath d=%22M12 8l-6 6h12z%22/%3E%3C/svg%3E") center no-repeat; background-size: 20px; height: 28px; }}
        .structure::-webkit-scrollbar-button:single-button:vertical:increment {{ background: #0b0d10 url("data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 24 24%22 fill=%22white%22%3E%3Cpath d=%22M12 16l6-6H6z%22/%3E%3C/svg%3E") center no-repeat; background-size: 20px; height: 28px; }}
        .accordion_inner {{ padding: 20px 26px 20px 16px; background-color: #13161b; border-radius: 20px; margin-bottom: 30px; }}
        .accordion_inner:last-of-type {{ margin-bottom: 0; }}
        .accordion-header {{ padding: 14px 10px 14px 35px; background: #0b0d10; display: flex; align-items: center; justify-content: space-between; cursor: pointer; user-select: none; font-weight: 700; font-size: 18px; line-height: 23px; color: #fff; border-radius: 15px; }}
        .accordion-arrow {{ transition: transform 0.3s; padding: 8px 14px; background-color: #13161b; border-radius: 10px; }}
        .accordion-body {{ max-height: 0; overflow: hidden; transition: max-height 0.3s ease; }}
        .accordion-content {{ padding: 30px 20px 10px 30px; }}
        .accordion-content .row {{ display: flex; justify-content: space-between; margin-bottom: 20px; }}
        .accordion-content .row:last-of-type {{ margin-bottom: 0; }}
        .accordion-content .row strong {{ text-transform: uppercase; font-weight: 500; font-size: 16px; line-height: 20px; color: #fff; }}
        .accordion-content .row span {{ font-weight: 600; font-size: 16px; line-height: 20px; color: #fff; width: 60%; text-align: right; }}
        .accordion.open .accordion-body {{ max-height: 2000px; }}
        .accordion.open .accordion-arrow {{ transform: rotate(180deg); }}
        .no-transform * {{ transform: none !important; }}
        @media print {{
            body * {{ visibility: hidden; }}
            #printArea, #printArea * {{ visibility: visible; }}
            #printArea {{ position: absolute; left: 0; top: 0; }}
            .show_print {{ display: block; }}
            .accordion-header .accordion-arrow {{ transform: none !important; transition: none !important; }}
        }}
        @media screen and (max-width: 1640px) {{ .container {{ padding: 0 20px; }} }}
        @media screen and (max-width: 990px) {{
            .header {{ margin: 30px 0; }}
            .header_inner {{ padding: 20px 25px; }}
            .request {{ flex-direction: column; align-items: flex-start; gap: 18px; margin-bottom: 20px; }}
            .request1 {{ order: 2; }}
            .result {{ order: 1; }}
            .block1 {{ display: none; }}
            .block2 {{ width: 100%; }}
            .hide_mobile {{ display: none; }}
            .accordion-content .row span {{ width: 50%; text-align: right; white-space: wrap; }}
        }}
        @media screen and (max-width: 640px) {{
            .result_number {{ padding: 6px 12px; }}
            .request_text {{ font-size: 26px; }}
            .request_number {{ padding: 12px 24px; font-size: 20px; }}
            .accordion-header {{ font-size: 15px; text-align: left; border-radius: 12px; }}
            .btn1 {{ padding: 14px 35px; font-size: 10px; }}
        }}
        @media screen and (max-width: 440px) {{
            .block2 {{ padding-bottom: 30px; }}
            .header_inner {{ border-radius: 15px; }}
            .result {{ padding: 12px 13px; border-radius: 10px; }}
            .result_text, .result_number {{ font-size: 10px; }}
            .result_number {{ padding: 3px 9px; border-radius: 5px; }}
            .request {{ margin-bottom: 15px; }}
            .request_text {{ font-size: 16px; }}
            .request_number {{ padding: 8px 26px; font-size: 12px; border-radius: 8px; }}
            .downloading {{ gap: 15px; }}
            .btn1 {{ padding: 12px 30px; font-size: 10px; border-radius: 10px; gap: 6px; }}
            .accordion_inner {{ border-radius: 12px; padding: 10px; margin-bottom: 10px; }}
            .accordion-header {{ font-size: 12px; text-align: left; padding: 13px; line-height: 16px; border-radius: 9px; }}
            .accordion-arrow {{ padding: 10px 8px; line-height: 0; border-radius: 6px; }}
            .accordion-content {{ padding: 22px; }}
            .accordion-content .row {{ margin-bottom: 13px; }}
            .accordion-content .row strong, .accordion-content .row span {{ font-size: 10px; line-height: 11px; }}
        }}
    </style>
</head>
<body>
    <header class="header">
        <div class="container">
            <div class="header_inner">
                <div class="request">
                    <div class="request1">
                        <h2 class="request_text">Запрос:</h2>
                        <div class="request_number">{query}</div>
                    </div>
                    <div class="result">
                        <h3 class="result_text">Результатов:</h3>
                        <div class="result_number">{records_count}</div>
                        <h3 class="result_text" style="margin-left: 20px;">Просмотров:</h3>
                        <div class="result_number">{views}</div>
                    </div>
                </div>
                <div class="downloading">
                    <button onclick="downloadPDF()" class="downloadPDF btn1">Сохранить в PDF</button>
                    <button id="printButton" class="print btn1">Печатать</button>
                </div>
            </div>
        </div>
    </header>
    <div class="main">
        <div class="container">
            <div class="main_inner">
                <div class="block1">
                    <div class="block1_inner">
                        <h3 class="block_title">Структура отчёта</h3>
                        <div class="bg_str">
                            <div class="structure">
                                <div class="client">
                                    <svg width="22.031171798706055" height="22.031171798706055" viewBox="0 0 22.031171798706055 22.031171798706055" xmlns="http://www.w3.org/2000/svg">
                                        <circle cx="11.015585899353027" cy="11.015585899353027" r="8.085585899353028" fill="none" stroke="#currentColor" stroke-width="5.86"/>
                                    </svg>
                                    <a href="#main" class="clients_name">Основная информация</a>
                                </div>
                                <div class="stick"></div>
                                {structure_items}
                            </div>
                        </div>
                    </div>
                </div>
                <div class="block2 no_transform" id="printArea">
                    <h3 class="block_title hide_mobile show_print">Полный отчёт</h3>
                    <div id="main" class="accordion_inner">
                        <div class="accordion open">
                            <div class="accordion-header" onclick="toggleAccordion(this)">
                                <span>Основная информация</span>
                                <div class="accordion-arrow">
                                    <svg width="13" height="9" viewBox="0 0 21 12" fill="none" xmlns="http://www.w3.org/2000/svg">
                                        <path d="M1 1L10.5 10L20 1" stroke="#A5AAB4" stroke-width="3" fill="none" stroke-linecap="round" stroke-linejoin="round"/>
                                    </svg>
                                </div>
                            </div>
                            <div class="accordion-body">
                                <div class="accordion-content">
                                    {''.join(f'<div class="row"><strong>{label.upper()}:</strong><span>{value}</span></div>' for label, value in main_info)}
                                </div>
                            </div>
                        </div>
                    </div>
                    {accordions}
                </div>
            </div>
        </div>
    </div>
    <script>
        function toggleAccordion(header) {{
            const accordion = header.parentElement;
            accordion.classList.toggle('open');
        }}
        document.addEventListener('DOMContentLoaded', function() {{
            document.getElementById('printButton').addEventListener('click', function() {{
                window.print();
            }});
        }});
        function downloadPDF() {{
            document.body.classList.add('no-transform');
            const element = document.getElementById('printArea');
            const options = {{
                margin: 0,
                filename: 'document.pdf',
                image: {{ type: 'jpeg', quality: 1 }},
                html2canvas: {{ scale: 1.5 }},
                jsPDF: {{ unit: 'pt', format: 'a4', orientation: 'portrait' }}
            }};
            html2pdf().set(options).from(element).save().then(() => {{
                document.body.classList.remove('no-transform');
            }});
        }}
    </script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js"></script>
</body>
</html>"""
    return html

# === ФУНКЦИИ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ===
async def generate_referral_code(user_id: int) -> str:
    code = f"REF{user_id}{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"
    pool = await get_pool()
    async with pool.acquire() as conn:
        while True:
            existing = await conn.fetchrow('SELECT user_id FROM users WHERE referral_code = $1', code)
            if not existing:
                break
            code = f"REF{user_id}{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"
        await conn.execute('UPDATE users SET referral_code = $1 WHERE user_id = $2', code, user_id)
    return code

async def get_referral_stats(user_id: int) -> tuple:
    pool = await get_pool()
    async with pool.acquire() as conn:
        invited = await conn.fetchval('SELECT COUNT(*) FROM referrals WHERE referrer_id = $1', user_id) or 0
        bonuses = invited
        return invited, bonuses

async def get_user_available_requests(user_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT daily_requests, bonus_requests, last_request_date 
            FROM users WHERE user_id = $1
        ''', user_id)
        if not row:
            return 0
        if row['last_request_date'] != datetime.now().date():
            daily = 0
        else:
            daily = row['daily_requests']
        limit = 5 + (row['bonus_requests'] or 0)
        available = limit - daily
        return available if available > 0 else 0

async def use_request(user_id: int) -> bool:
    available = await get_user_available_requests(user_id)
    if available <= 0:
        return False
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            UPDATE users 
            SET daily_requests = daily_requests + 1,
                last_request_date = CURRENT_DATE
            WHERE user_id = $1
        ''', user_id)
    return True

async def create_user(user_id: int, username: str = None, referred_by: int = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO users (user_id, username, created_at, daily_requests, last_request_date)
            VALUES ($1, $2, NOW(), 0, CURRENT_DATE)
        ''', user_id, username)
        code = await generate_referral_code(user_id)
        if referred_by:
            referrer = await conn.fetchrow('SELECT user_id FROM users WHERE user_id = $1', referred_by)
            if referrer:
                await conn.execute('''
                    INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2)
                ''', referred_by, user_id)
                await conn.execute('''
                    UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = $1
                ''', referred_by)
        return code

async def get_user(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
    return row

async def get_referral_code(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT referral_code FROM users WHERE user_id = $1', user_id)
    return row['referral_code'] if row else None

async def save_report(phone: str, data: dict):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO reports (phone, data) VALUES ($1, $2)
            ON CONFLICT (phone) DO UPDATE SET data = $2, created_at = NOW()
        ''', phone, json.dumps(data))

async def get_report(phone: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT data FROM reports WHERE phone = $1', phone)
    if row:
        return json.loads(row['data'])
    return None

async def get_unique_views_phone(phone: str, user_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT user_ids FROM phone_views WHERE phone = $1', phone)
        if row:
            user_ids = json.loads(row['user_ids']) if row['user_ids'] else []
        else:
            user_ids = []
        if user_id not in user_ids:
            user_ids.append(user_id)
        await conn.execute('''
            INSERT INTO phone_views (phone, user_ids) VALUES ($1, $2)
            ON CONFLICT (phone) DO UPDATE SET user_ids = $2
        ''', phone, json.dumps(user_ids))
        return len(user_ids)

async def check_subscription(user_id: int) -> bool:
    try:
        chat = await bot.get_chat(CHANNEL_USERNAME)
        chat_id = chat.id
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception:
        return False

# === ПРОМОКОДЫ ===
async def create_promo_code(code: str, max_uses: int, requests_granted: int, created_by: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute('''
                INSERT INTO promo_codes (code, max_uses, requests_granted, created_by)
                VALUES ($1, $2, $3, $4)
            ''', code, max_uses, requests_granted, created_by)
            return True
        except asyncpg.UniqueViolationError:
            return False

async def get_promo_code(code: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM promo_codes WHERE code = $1', code)
    return row

async def activate_promo_code(user_id: int, code: str) -> tuple:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM promo_codes WHERE code = $1', code)
        if not row:
            return False, "Промокод не найден."
        if row['used_count'] >= row['max_uses']:
            return False, "Промокод уже использован максимальное количество раз."
        await conn.execute('''
            UPDATE promo_codes SET used_count = used_count + 1 WHERE code = $1
        ''', code)
        bonus = row['requests_granted']
        await conn.execute('''
            UPDATE users SET bonus_requests = bonus_requests + $1 WHERE user_id = $2
        ''', bonus, user_id)
        return True, f"Промокод активирован! Вы получили {bonus} дополнительных запросов."

async def get_all_promo_codes():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM promo_codes ORDER BY created_at DESC')
    return rows

async def delete_promo_code(code: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('DELETE FROM promo_codes WHERE code = $1', code)

# === ОПЛАТА ===
async def create_crypto_pay_invoice(user_id: int, amount_usd: float, requests_count: int):
    if not CRYPTOPAY_TOKEN:
        return None, None, "CRYPTOPAY_TOKEN не настроен."

    session = await get_http_session()
    url = "https://pay.crypt.bot/api/createInvoice"
    headers = {
        "Crypto-Pay-API-Token": CRYPTOPAY_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "amount": str(amount_usd),
        "currency_type": "fiat",
        "fiat": "USD",
        "paid_btn_name": "callback",
        "paid_btn_url": "https://t.me/Dataseekersearchbot",
        "description": f"Пополнение запросов: {requests_count} шт.",
        "payload": json.dumps({"user_id": user_id, "requests": requests_count}),
        "allow_comments": False,
        "allow_anonymous": False
    }
    try:
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('ok'):
                    invoice = data['result']
                    invoice_id = str(invoice['invoice_id'])
                    pay_url = invoice['pay_url']
                    pool = await get_pool()
                    async with pool.acquire() as conn:
                        await conn.execute('''
                            INSERT INTO purchases (user_id, invoice_id, amount, currency, requests, status)
                            VALUES ($1, $2, $3, 'USD', $4, 'pending')
                        ''', user_id, invoice_id, amount_usd, requests_count)
                    return pay_url, invoice_id, None
                else:
                    error_msg = data.get('error', {}).get('message', 'Неизвестная ошибка')
                    return None, None, f"Ошибка CryptoPay: {error_msg}"
            else:
                return None, None, f"Ошибка HTTP {resp.status}"
    except Exception as e:
        logger.error(f"CryptoPay error: {e}")
        return None, None, f"Ошибка сети: {str(e)}"

async def process_crypto_pay_payment(invoice_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        purchase = await conn.fetchrow('SELECT * FROM purchases WHERE invoice_id = $1', invoice_id)
        if not purchase or purchase['status'] != 'pending':
            return
        user_id = purchase['user_id']
        requests = purchase['requests']
        await conn.execute('''
            UPDATE users SET bonus_requests = bonus_requests + $1 WHERE user_id = $2
        ''', requests, user_id)
        await conn.execute('''
            UPDATE purchases SET status = 'confirmed', confirmed_at = NOW() WHERE invoice_id = $1
        ''', invoice_id)
        try:
            await bot.send_message(
                user_id,
                f"Оплата USDT подтверждена! Вам начислено {requests} дополнительных запросов.\n"
                f"Теперь у вас доступно {await get_user_available_requests(user_id)} запросов."
            )
        except Exception:
            pass

async def check_payment_status(user_id: int, invoice_id: str) -> dict:
    if not CRYPTOPAY_TOKEN:
        return {"status": "error", "message": "CRYPTOPAY_TOKEN не настроен."}
    session = await get_http_session()
    url = f"https://pay.crypt.bot/api/getInvoices?invoice_id={invoice_id}"
    headers = {"Crypto-Pay-API-Token": CRYPTOPAY_TOKEN}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('ok'):
                    invoices = data.get('result', {}).get('items', [])
                    if invoices:
                        status = invoices[0].get('status')
                        if status == 'paid':
                            return {"status": "paid", "message": "Оплачено!"}
                        elif status == 'active':
                            return {"status": "pending", "message": "Счёт ещё не оплачен"}
                        elif status == 'expired':
                            return {"status": "expired", "message": "Счёт истёк. Создайте новый."}
                        elif status == 'cancelled':
                            return {"status": "cancelled", "message": "Счёт отменён."}
                        else:
                            return {"status": "pending", "message": f"Статус: {status}. Подождите..."}
                    else:
                        return {"status": "not_found", "message": "Счёт не найден."}
                else:
                    error_msg = data.get('error', {}).get('message', 'Ошибка API')
                    return {"status": "error", "message": f"Ошибка CryptoPay: {error_msg}"}
            else:
                return {"status": "error", "message": f"Ошибка HTTP {resp.status}"}
    except Exception as e:
        return {"status": "error", "message": f"Ошибка сети: {str(e)}"}

async def create_stars_invoice(user_id: int, stars_price: int, requests_count: int):
    pool = await get_pool()
    temp_invoice_id = f"stars_{user_id}_{int(datetime.now().timestamp())}"
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO purchases (user_id, invoice_id, amount, currency, requests, status)
            VALUES ($1, $2, $3, 'XTR', $4, 'pending')
        ''', user_id, temp_invoice_id, stars_price, requests_count)

    prices = [LabeledPrice(label=f"{requests_count} запросов", amount=stars_price)]
    try:
        await bot.send_invoice(
            chat_id=user_id,
            title=f"Пополнение запросов: {requests_count} шт.",
            description=f"Вы получаете {requests_count} дополнительных запросов.",
            provider_token="",
            currency="XTR",
            prices=prices,
            start_parameter=f"stars_{user_id}_{int(datetime.now().timestamp())}",
            payload=json.dumps({"user_id": user_id, "requests": requests_count, "temp_invoice_id": temp_invoice_id})
        )
        return True, temp_invoice_id
    except Exception as e:
        logger.error(f"Stars invoice error: {e}")
        async with pool.acquire() as conn:
            await conn.execute('DELETE FROM purchases WHERE invoice_id = $1', temp_invoice_id)
        return False, None

async def process_stars_payment(charge_id: str, user_id: int, payload: str):
    data = json.loads(payload)
    temp_invoice_id = data.get("temp_invoice_id")
    requests = data.get("requests", 0)
    pool = await get_pool()
    async with pool.acquire() as conn:
        purchase = await conn.fetchrow('SELECT * FROM purchases WHERE invoice_id = $1 AND user_id = $2', temp_invoice_id, user_id)
        if not purchase or purchase['status'] != 'pending':
            return
        await conn.execute('''
            UPDATE purchases SET invoice_id = $1, status = 'confirmed', confirmed_at = NOW()
            WHERE invoice_id = $2 AND user_id = $3
        ''', charge_id, temp_invoice_id, user_id)
        await conn.execute('''
            UPDATE users SET bonus_requests = bonus_requests + $1 WHERE user_id = $2
        ''', requests, user_id)
        try:
            await bot.send_message(
                user_id,
                f"Оплата Stars подтверждена! Вам начислено {requests} дополнительных запросов.\n"
                f"Теперь у вас доступно {await get_user_available_requests(user_id)} запросов."
            )
        except Exception:
            pass

# === ОБРАБОТЧИКИ БОТА ===
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def start_cmd(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username

    if not await check_subscription(user_id):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подписаться на канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="Я подписался", callback_data="check_subscription")]
        ])
        await message.reply("Для использования бота подпишитесь на канал!", reply_markup=keyboard)
        return

    user = await get_user(user_id)
    if not user:
        args = message.text.split()
        referrer_id = None
        if len(args) > 1:
            payload = args[1]
            if payload.startswith("ref_"):
                ref_code = payload[4:]
            elif payload.startswith("ref"):
                ref_code = payload[3:]
            else:
                ref_code = None
            if ref_code:
                pool = await get_pool()
                async with pool.acquire() as conn:
                    row = await conn.fetchrow('SELECT user_id FROM users WHERE referral_code = $1', ref_code)
                    if row and row['user_id'] != user_id:
                        referrer_id = row['user_id']
        await create_user(user_id, username, referrer_id)
        if referrer_id:
            try:
                ref_user = await get_user(referrer_id)
                if ref_user:
                    await bot.send_message(referrer_id, f"По вашей ссылке пришёл @{username or 'пользователь'}, вы получили +1 запрос.")
            except:
                pass
    else:
        args = message.text.split()
        if len(args) > 1:
            payload = args[1]
            if payload.startswith("ref_"):
                ref_code = payload[4:]
            elif payload.startswith("ref"):
                ref_code = payload[3:]
            else:
                ref_code = None
            if ref_code:
                pool = await get_pool()
                async with pool.acquire() as conn:
                    row = await conn.fetchrow('SELECT user_id FROM users WHERE referral_code = $1', ref_code)
                    if row and row['user_id'] != user_id:
                        existing = await conn.fetchrow('SELECT * FROM referrals WHERE referrer_id=$1 AND referred_id=$2', row['user_id'], user_id)
                        if not existing:
                            await conn.execute('INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2)', row['user_id'], user_id)
                            await conn.execute('UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = $1', row['user_id'])
                            try:
                                await bot.send_message(row['user_id'], f"@{username or 'пользователь'} активировал вашу ссылку, вы получили +1 запрос.")
                            except:
                                pass

    info_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Информация", url=CHANNEL_LINK)]
    ])
    info_msg = await message.reply(
        "🔮 Вечная ссылка на информацию:\n"
        "Если удалят этого бота — то новую ссылку на него найдёте по кнопке ниже.",
        reply_markup=info_keyboard
    )
    try:
        await bot.pin_chat_message(message.chat.id, info_msg.message_id, disable_notification=True)
    except Exception as e:
        logger.warning(f"Не удалось закрепить: {e}")

    main_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")]
    ])
    main_text = (
        "🕵️ dataseeker — твой бесплатный цифровой детектив.\n\n"
        "Типы поиска:\n\n"
        "┌ Контакты:\n"
        "├ Телефон → +79999999999\n"
        "└ Email → ivanov@gmail.com\n\n"
        "┌ Соцсети:\n"
        "└ VK → vk.com/id1234567\n\n"
        "┌ Онлайн-следы:\n"
        "└ IP → 185.85.219.243\n\n"
        "┌ Физ. лица:\n"
        "├ ИНН → /inn 123456789012\n"
        "└ ФИО → Иванов Иван Иванович\n\n"
        "Каждые 24 часа выдаётся по 5 бесплатных запросов."
    )
    await message.reply(main_text, reply_markup=main_keyboard)

@dp.message(Command("inn"))
async def inn_cmd(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Укажите ИНН: `/inn 123456789012`", parse_mode="Markdown")
        return
    inn = args[1].strip()
    if not re.match(r'^\d{10,12}$', inn):
        await message.reply("Неверный формат ИНН. Должно быть 10 или 12 цифр.")
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

    if not await check_subscription(user_id):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="Я подписался", callback_data="check_subscription")]
        ])
        await message.reply("Подпишитесь на канал!", reply_markup=keyboard)
        return

    user = await get_user(user_id)
    if not user:
        await create_user(user_id, message.from_user.username)

    available = await get_user_available_requests(user_id)
    if available <= 0:
        await message.reply("Лимит запросов исчерпан.")
        return

    status_msg = await message.reply(f"Поиск по {search_type}...")

    try:
        data = await collect_general_data(query, search_type)
        if search_type == "phone":
            views = await get_unique_views_phone(query, user_id)
            await save_report(query, data)
        else:
            views = 0

        html_content = generate_html_report(data, views)
        html_bytes = html_content.encode('utf-8')
        html_file = BufferedInputFile(html_bytes, filename=f"report_{search_type}_{query}.html")

        await status_msg.delete()
        await message.reply_document(html_file, caption=f"Отчёт по {search_type}: {query}")

        await use_request(user_id)
    except Exception as e:
        logger.error(f"Error in process_general_query: {e}")
        await status_msg.edit_text(f"Ошибка: {str(e)}")

# Универсальный обработчик с фильтром StateFilter(None)
@dp.message(lambda msg: msg.text and not msg.text.startswith('/'), StateFilter(None))
async def universal_handler(message: Message):
    await process_general_query(message, message.text.strip(), detect_type(message.text.strip()))

# === КОЛБЭКИ ===
@dp.callback_query(lambda c: c.data == "my_profile")
async def my_profile_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("Не зарегистрированы.")
        return
    available = await get_user_available_requests(user_id)
    text = f"Ваш профиль\nID: {user_id}\nДоступно запросов: {available}\nДата регистрации: {user['created_at'].strftime('%d.%m.%Y %H:%M')}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ввести промокод", callback_data="enter_promo")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "referral_system")
async def referral_system_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    invited, bonuses = await get_referral_stats(user_id)
    ref_code = await get_referral_code(user_id)
    bot_username = (await bot.get_me()).username
    text = f"Реферальная система\n\nПриглашайте друзей, получайте +1 запрос за каждого.\nВаша ссылка:\nhttps://t.me/{bot_username}?start=ref{ref_code}\n\nПриглашено: {invited}\nБонусов: {bonuses}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "buy_requests")
async def buy_requests_callback(callback: CallbackQuery):
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
    keyboard_buttons = []
    row = []
    for btn in buttons:
        if isinstance(btn, list):
            if row:
                keyboard_buttons.append(row)
                row = []
            keyboard_buttons.append(btn)
        else:
            row.append(btn)
            if len(row) == 2:
                keyboard_buttons.append(row)
                row = []
    if row:
        keyboard_buttons.append(row)
    keyboard_buttons.append([InlineKeyboardButton(text="Назад", callback_data="back_to_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    text = "Выбери тариф:\n\nЧем больше пакет — тем дешевле каждый запрос."
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("pkg_"))
async def package_selected_callback(callback: CallbackQuery):
    parts = callback.data.split("_")
    requests_count = int(parts[1])
    amount_usd = float(parts[2])
    stars_price = int(parts[3])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Оплатить звёздами (${stars_price})", callback_data=f"pay_stars_{requests_count}_{stars_price}")],
        [InlineKeyboardButton(text="Оплатить USDT", callback_data=f"pay_usdt_{requests_count}_{amount_usd}")],
        [InlineKeyboardButton(text="Назад", callback_data="buy_requests")]
    ])
    await callback.message.edit_text(
        f"Пакет: {requests_count} запросов\n"
        f"Цена: {amount_usd} USD или {stars_price} звёзд\n\n"
        "Выберите способ оплаты:",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("pay_stars_"))
async def pay_stars_callback(callback: CallbackQuery):
    parts = callback.data.split("_")
    requests_count = int(parts[2])
    stars_price = int(parts[3])
    user_id = callback.from_user.id
    success, temp_invoice_id = await create_stars_invoice(user_id, stars_price, requests_count)
    if not success:
        await callback.message.edit_text(
            "Ошибка создания счёта. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="buy_requests")]
            ])
        )
        await callback.answer()
        return
    await callback.message.edit_text(
        "Счёт создан. Оплатите в Telegram (звёзды).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="buy_requests")]
        ])
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("pay_usdt_"))
async def pay_usdt_callback(callback: CallbackQuery):
    parts = callback.data.split("_")
    requests_count = int(parts[2])
    amount_usd = float(parts[3])
    user_id = callback.from_user.id
    pay_url, invoice_id, error = await create_crypto_pay_invoice(user_id, amount_usd, requests_count)
    if not pay_url:
        await callback.message.edit_text(
            f"Ошибка создания счёта USDT: {error}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="buy_requests")]
            ])
        )
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Оплатить USDT", url=pay_url)],
        [InlineKeyboardButton(text="Я оплатил", callback_data=f"check_usdt_{invoice_id}")],
        [InlineKeyboardButton(text="Назад", callback_data="buy_requests")]
    ])
    await callback.message.edit_text(
        f"Счёт на оплату USDT:\n"
        f"Пакет: {requests_count} запросов\n"
        f"Сумма: {amount_usd} USD\n\n"
        "Нажмите «Оплатить USDT», затем после оплаты — «Я оплатил».",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("check_usdt_"))
async def check_usdt_payment_callback(callback: CallbackQuery):
    invoice_id = callback.data.replace("check_usdt_", "")
    user_id = callback.from_user.id
    result = await check_payment_status(user_id, invoice_id)
    if result["status"] == "paid":
        await process_crypto_pay_payment(invoice_id)
        await callback.message.edit_text(
            "Оплата подтверждена! Запросы начислены.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="В меню", callback_data="back_to_menu")]
            ])
        )
        await callback.answer()
    elif result["status"] == "pending":
        await callback.message.edit_text(
            result["message"],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Проверить снова", callback_data=f"check_usdt_{invoice_id}")],
                [InlineKeyboardButton(text="Назад", callback_data="buy_requests")]
            ])
        )
        await callback.answer()
    else:
        await callback.message.edit_text(
            result["message"],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="buy_requests")]
            ])
        )
        await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await pre_checkout_query.answer(ok=True)

@dp.message(lambda message: message.successful_payment is not None)
async def successful_payment_handler(message: Message):
    successful_payment = message.successful_payment
    charge_id = successful_payment.telegram_payment_charge_id
    user_id = message.from_user.id
    payload = successful_payment.invoice_payload
    await process_stars_payment(charge_id, user_id, payload)

@dp.callback_query(lambda c: c.data == "enter_promo")
async def enter_promo_callback(callback: CallbackQuery, state: FSMContext):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="promo_back")]
    ])
    await callback.message.edit_text("Введите промокод:", reply_markup=keyboard)
    await state.set_state(EnterPromo.waiting_for_code)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "promo_back")
async def promo_back_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await my_profile_callback(callback)

@dp.message(EnterPromo.waiting_for_code)
async def process_enter_promo(message: Message, state: FSMContext):
    code = message.text.strip()
    user_id = message.from_user.id
    success, msg = await activate_promo_code(user_id, code)
    await message.reply(msg)
    await state.clear()
    await back_to_menu_message(message)

async def back_to_menu_message(message: Message):
    main_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")]
    ])
    text = "🕵️ dataseeker — твой бесплатный цифровой детектив.\n\nТипы поиска:\n\n┌ Контакты:\n├ Телефон → +79999999999\n└ Email → ivanov@gmail.com\n\n┌ Соцсети:\n└ VK → vk.com/id1234567\n\n┌ Онлайн-следы:\n└ IP → 185.85.219.243\n\n┌ Физ. лица:\n├ ИНН → /inn 123456789012\n└ ФИО → Иванов Иван Иванович\n\nКаждые 24 часа выдаётся по 5 бесплатных запросов."
    await message.answer(text, reply_markup=main_keyboard)

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu_callback(callback: CallbackQuery):
    main_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")]
    ])
    text = "🕵️ dataseeker — твой бесплатный цифровой детектив.\n\nТипы поиска:\n\n┌ Контакты:\n├ Телефон → +79999999999\n└ Email → ivanov@gmail.com\n\n┌ Соцсети:\n└ VK → vk.com/id1234567\n\n┌ Онлайн-следы:\n└ IP → 185.85.219.243\n\n┌ Физ. лица:\n├ ИНН → /inn 123456789012\n└ ФИО → Иванов Иван Иванович\n\nКаждые 24 часа выдаётся по 5 бесплатных запросов."
    await callback.message.edit_text(text, reply_markup=main_keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "check_subscription")
async def check_subscription_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    if await check_subscription(user_id):
        await callback.message.delete()
        await start_cmd(callback.message)
    else:
        await callback.answer("Вы ещё не подписались!", show_alert=True)

# === АДМИН-ПАНЕЛЬ ===
def get_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="Создать промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Выдать запросы", callback_data="admin_give")],
        [InlineKeyboardButton(text="Список промокодов", callback_data="admin_list_promo")],
        [InlineKeyboardButton(text="Удалить промокод", callback_data="admin_delete_promo")],
        [InlineKeyboardButton(text="Платежи", callback_data="admin_payments")],
        [InlineKeyboardButton(text="Закрыть", callback_data="admin_close")]
    ])

@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Доступ запрещён.")
        return
    await message.reply("Админ-панель", reply_markup=get_admin_keyboard())

@dp.callback_query(lambda c: c.data and c.data.startswith("admin_"))
async def admin_callback(callback: CallbackQuery, state: FSMContext = None):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Доступ запрещён.")
        return
    action = callback.data.replace("admin_", "")
    if action == "stats":
        pool = await get_pool()
        async with pool.acquire() as conn:
            users = await conn.fetchval('SELECT COUNT(*) FROM users')
            reports = await conn.fetchval('SELECT COUNT(*) FROM reports')
            promos = await conn.fetchval('SELECT COUNT(*) FROM promo_codes')
            payments = await conn.fetchval('SELECT COUNT(*) FROM purchases WHERE status=$1', 'confirmed')
        await callback.message.edit_text(
            f"Статистика\nПользователей: {users}\nОтчётов: {reports}\nПромокодов: {promos}\nПлатежей: {payments}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]])
        )
        await callback.answer()
    elif action == "back":
        await callback.message.edit_text("Админ-панель", reply_markup=get_admin_keyboard())
        await callback.answer()
    elif action == "close":
        await callback.message.delete()
        await callback.answer()
    elif action == "create_promo":
        await callback.message.edit_text("Введите промокод:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]]))
        await state.set_state(PromoCreation.waiting_for_code)
        await callback.answer()
    elif action == "broadcast":
        await callback.message.edit_text("Введите текст рассылки:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]]))
        await state.set_state(Broadcast.waiting_for_text)
        await callback.answer()
    elif action == "give":
        await callback.message.edit_text("Введите ID пользователя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]]))
        await state.set_state(GiveRequests.waiting_for_user_id)
        await callback.answer()
    elif action == "list_promo":
        promos = await get_all_promo_codes()
        if not promos:
            text = "Нет промокодов."
        else:
            text = "Промокоды:\n" + "\n".join([f"`{p['code']}` — {p['used_count']}/{p['max_uses']}" for p in promos])
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]]))
        await callback.answer()
    elif action == "delete_promo":
        promos = await get_all_promo_codes()
        if not promos:
            await callback.message.edit_text("Нет промокодов для удаления.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]]))
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[])
            for p in promos:
                keyboard.inline_keyboard.append([InlineKeyboardButton(text=f"❌ {p['code']}", callback_data=f"delpromo_{p['code']}")])
            keyboard.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="admin_back")])
            await callback.message.edit_text("Выберите промокод для удаления:", reply_markup=keyboard)
        await callback.answer()
    elif action == "payments":
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM purchases ORDER BY created_at DESC LIMIT 10')
        if not rows:
            text = "Нет платежей."
        else:
            text = "Последние платежи:\n" + "\n".join([f"{p['user_id']} — {p['requests']} запросов, {p['amount']} {p['currency']}, {p['status']}" for p in rows])
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]]))
        await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("delpromo_"))
async def delete_promo_callback(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Доступ запрещён.")
        return
    code = callback.data.replace("delpromo_", "")
    await delete_promo_code(code)
    await callback.answer(f"Промокод {code} удалён.")
    await admin_callback(callback)

@dp.message(PromoCreation.waiting_for_code)
async def process_promo_code(message: Message, state: FSMContext):
    code = message.text.strip()
    if not re.match(r'^[A-Za-z0-9_\-]+$', code):
        await message.reply("Некорректный промокод.")
        return
    existing = await get_promo_code(code)
    if existing:
        await message.reply("Такой промокод уже есть.")
        return
    await state.update_data(code=code)
    await state.set_state(PromoCreation.waiting_for_max_uses)
    await message.reply("Введите максимальное количество активаций:")

@dp.message(PromoCreation.waiting_for_max_uses)
async def process_max_uses(message: Message, state: FSMContext):
    try:
        max_uses = int(message.text.strip())
        if max_uses <= 0:
            await message.reply("Число должно быть > 0.")
            return
        await state.update_data(max_uses=max_uses)
        await state.set_state(PromoCreation.waiting_for_requests)
        await message.reply("Введите количество запросов, которое даёт промокод:")
    except ValueError:
        await message.reply("Введите целое число.")

@dp.message(PromoCreation.waiting_for_requests)
async def process_requests(message: Message, state: FSMContext):
    try:
        requests_granted = int(message.text.strip())
        if requests_granted <= 0:
            await message.reply("Число должно быть > 0.")
            return
        data = await state.get_data()
        code = data['code']
        max_uses = data['max_uses']
        success = await create_promo_code(code, max_uses, requests_granted, message.from_user.id)
        if success:
            await message.reply(f"Промокод создан: `{code}`")
        else:
            await message.reply("Ошибка создания.")
        await state.clear()
        await admin_cmd(message)
    except ValueError:
        await message.reply("Введите целое число.")

@dp.message(GiveRequests.waiting_for_user_id)
async def process_give_user_id(message: Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
        await state.update_data(target_user_id=user_id)
        await state.set_state(GiveRequests.waiting_for_amount)
        await message.reply("Введите количество запросов:")
    except ValueError:
        await message.reply("Введите ID.")

@dp.message(GiveRequests.waiting_for_amount)
async def process_give_amount(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount <= 0:
            await message.reply("Количество должно быть > 0.")
            return
        data = await state.get_data()
        user_id = data['target_user_id']
        user = await get_user(user_id)
        if not user:
            await message.reply(f"Пользователь {user_id} не найден.")
            await state.clear()
            return
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute('UPDATE users SET bonus_requests = bonus_requests + $1 WHERE user_id = $2', amount, user_id)
        await message.reply(f"Выдано {amount} запросов пользователю {user_id}.")
        await state.clear()
        await admin_cmd(message)
    except ValueError:
        await message.reply("Введите число.")

@dp.message(Broadcast.waiting_for_text)
async def process_broadcast(message: Message, state: FSMContext):
    text = message.text
    pool = await get_pool()
    async with pool.acquire() as conn:
        users = await conn.fetch('SELECT user_id FROM users')
    count = 0
    for user in users:
        try:
            await bot.send_message(user['user_id'], text)
            count += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await message.reply(f"Рассылка завершена. Отправлено {count} пользователям.")
    await state.clear()
    await admin_cmd(message)

# === ЗАПУСК ===
async def main():
    await asyncio.sleep(3)
    await init_db()
    app = web.Application()
    app.router.add_get("/", lambda req: web.Response(text="Bot is running"))
    app.router.add_get("/health", lambda req: web.Response(text="OK"))
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
        if db_pool:
            await db_pool.close()
        if http_session:
            await http_session.close()

if __name__ == "__main__":
    asyncio.run(main())
