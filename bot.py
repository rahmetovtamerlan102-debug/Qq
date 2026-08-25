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
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    WebAppInfo, PreCheckoutQuery, LabeledPrice, SuccessfulPayment
)
import hashlib
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не задан")

NIGHTSEARCH_API_KEY = os.getenv("NIGHTSEARCH_API_KEY")
SEON_API_KEY = os.getenv("SEON_API_KEY")
SNUSBASE_API_KEY = os.getenv("SNUSBASE_API_KEY")
DEPSEARCH_TOKEN = os.getenv("DEPSEARCH_TOKEN")
DEPSEARCH_BASE = os.getenv("DEPSEARCH_BASE", "https://api.depsearch.sbs")
JITLER_TOKENS_STR = os.getenv("JITLER_TOKENS", "")
JITLER_TOKENS = [t.strip() for t in JITLER_TOKENS_STR.split(",") if t.strip()]

# ===== БАЗОВЫЙ URL ДЛЯ MINI APP (ЗАМЕНИ НА СВОЙ ДОМЕН) =====
BASE_URL = "https://qq-rb6p.onrender.com"

CHANNEL_USERNAME = "@dataseekerinfo"
CHANNEL_LINK = "tg://resolve?domain=dataseekerinfo"
ADMIN_IDS = [8559629118]

db_pool = None
http_session = None

# ===== КЭШ =====
cache = {}
CACHE_TTL = timedelta(hours=1)

def get_cache_key(func_name: str, query: str) -> str:
    return f"{func_name}:{hashlib.md5(query.encode()).hexdigest()}"

# ===== API ТАЙМАУТЫ =====
API_TIMEOUTS = {
    "nightsearch": 3.0,
    "seon": 2.0,
    "snusbase": 2.0,
    "depsearch": 3.0,
    "jitler": 2.0
}

# ===== КЛАССЫ СОСТОЯНИЙ (для промокодов и админки) =====
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

# ===== ПАКЕТЫ ОПЛАТЫ =====
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

# ===== БАЗА ДАННЫХ =====
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

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
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

# ===== JITLER BALANCER =====
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

# ===== API ЗАПРОСЫ =====
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
    if not DEPSEARCH_TOKEN or not DEPSEARCH_BASE:
        return {}
    session = await get_http_session()
    url = f"{DEPSEARCH_BASE}/quest={query}&token={DEPSEARCH_TOKEN}&lang=ru"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["depsearch"])) as resp:
            if resp.status == 200:
                return await resp.json()
            return {}
    except Exception:
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
                            for _ in range(2):
                                await asyncio.sleep(0.2)
                                try:
                                    async with session.get(
                                        f"https://api.jitler.top/search/{task_id}",
                                        headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=1)
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

# ===== СБОР ДАННЫХ ПО ТЕЛЕФОНУ =====
async def collect_phone_data(query: str):
    cache_key = get_cache_key("phone", query)
    if cache_key in cache:
        cached_time, data = cache[cache_key]
        if datetime.now() - cached_time < CACHE_TTL:
            return data

    tasks = {
        'depsearch': asyncio.create_task(depsearch_search(query)),
        'nightsearch': asyncio.create_task(nightsearch_search(query)),
        'seon': asyncio.create_task(seon_search(query)),
        'snusbase': asyncio.create_task(snusbase_search(query)),
        'jitler': asyncio.create_task(jitler_search_with_balancer(query, "number"))
    }

    results = {}
    for name, task in tasks.items():
        try:
            results[name] = await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            results[name] = {}
            task.cancel()

    depsearch = results.get('depsearch', {}) if isinstance(results.get('depsearch', {}), dict) else {}
    nightsearch = results.get('nightsearch', {}) if isinstance(results.get('nightsearch', {}), dict) else {}
    seon = results.get('seon', {}) if isinstance(results.get('seon', {}), dict) else {}
    snusbase = results.get('snusbase', {}) if isinstance(results.get('snusbase', {}), dict) else {}
    jitler = results.get('jitler', {}) if isinstance(results.get('jitler', {}), dict) else {}

    result = {
        'query': query,
        'operator': None,
        'region': None,
        'country': None,
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

    # ---- DepSearch ----
    if depsearch:
        phone_info = depsearch.get('phone_info', {})
        if phone_info.get('operator'):
            result['operator'] = str(phone_info['operator'])
            sources_set.add("DepSearch (оператор)")
        if phone_info.get('region'):
            result['region'] = str(phone_info['region'])
            sources_set.add("DepSearch (регион)")
        if phone_info.get('country'):
            result['country'] = str(phone_info['country'])
            sources_set.add("DepSearch (страна)")

        results_list = depsearch.get('results', [])
        if isinstance(results_list, list):
            for item in results_list:
                if not isinstance(item, dict):
                    continue
                source_name = item.get('🏫Источник', item.get('Источник', 'Unknown'))
                source_name = re.sub(r'[^\w\s\-\.]', '', source_name).strip()
                if not source_name:
                    source_name = 'Unknown'

                record_data = {}
                record_key = ""
                for k, v in item.items():
                    if k not in ['🏫Источник', 'Источник'] and v and v not in (None, '', [], {}):
                        clean_key = re.sub(r'[^\w\s\-\.]', '', k).strip()
                        if clean_key:
                            record_data[clean_key] = v
                            if clean_key in ['ФИО', 'Адрес', 'Телефон']:
                                record_key += str(v)

                if record_data and record_key not in seen_records:
                    seen_records.add(record_key)
                    sources_set.add(source_name)
                    records_count += 1
                    result['extra'][f"Запись #{records_count}"] = {
                        'source': source_name,
                        'data': record_data
                    }

                if not result['fio']:
                    fio = (
                        item.get('👤ФИО') or
                        item.get('👤Имя') or
                        item.get('full_name') or
                        item.get('fio') or
                        item.get('ФИО')
                    )
                    if fio:
                        result['fio'] = str(fio)
                        sources_set.add("DepSearch (ФИО)")

                if not all_birthdates:
                    bdate = (
                        item.get('🎂Дата рождения') or
                        item.get('birthdate') or
                        item.get('birth_date') or
                        item.get('Дата рождения')
                    )
                    if bdate:
                        normalized = normalize_birthdate(str(bdate))
                        if normalized:
                            all_birthdates.append(normalized)

                if not result['address']:
                    address = (
                        item.get('📍Адрес') or
                        item.get('🏠Адрес') or
                        item.get('address') or
                        item.get('Адрес')
                    )
                    if address:
                        addr_str = str(address).strip()
                        if not re.match(r'^\d{4}-\d{2}-\d{2}', addr_str):
                            result['address'] = addr_str
                            sources_set.add("DepSearch (адрес)")

                email = item.get('✉️Почта') or item.get('email') or item.get('mail')
                if email and '@' in str(email):
                    result['emails'].append(str(email))

                vk = item.get('🧑‍💻Вконтакте') or item.get('vk') or item.get('vkontakte')
                if vk and not result['vk']:
                    result['vk'] = get_social_url(vk)
                    sources_set.add("DepSearch (VK)")

                ok = item.get('👨‍🦳Одноклассники') or item.get('ok') or item.get('odnoklassniki')
                if ok and not result['ok']:
                    result['ok'] = get_social_url(ok)
                    sources_set.add("DepSearch (OK)")

                inst = item.get('📷Instagram') or item.get('instagram')
                if inst and not result['instagram']:
                    result['instagram'] = get_social_url(inst)
                    sources_set.add("DepSearch (Instagram)")

                tt = item.get('👩‍🦲TikTok') or item.get('tiktok')
                if tt and not result['tiktok']:
                    result['tiktok'] = get_social_url(tt)
                    sources_set.add("DepSearch (TikTok)")

                for k, v in item.items():
                    if isinstance(v, str):
                        inn_match = re.search(r'\b(\d{12})\b', v)
                        if inn_match and not result['inn']:
                            result['inn'] = inn_match.group(1)
                            sources_set.add("DepSearch (ИНН)")
                        snils_clean = v.replace('-', '').replace(' ', '')
                        snils_match = re.search(r'\b(\d{11})\b', snils_clean)
                        if snils_match and not result['snils']:
                            result['snils'] = snils_match.group(1)
                            sources_set.add("DepSearch (СНИЛС)")
                        passport_match = re.search(r'\b(\d{4}\s?\d{6})\b', v)
                        if passport_match and not result['passport']:
                            result['passport'] = passport_match.group(1)
                            sources_set.add("DepSearch (паспорт)")

    # ---- NightSearch ----
    if nightsearch:
        if nightsearch.get('operator') and not result['operator']:
            result['operator'] = str(nightsearch['operator'])
            sources_set.add("NightSearch (оператор)")
        if nightsearch.get('region') and not result['region']:
            result['region'] = str(nightsearch['region'])
            sources_set.add("NightSearch (регион)")
        if nightsearch.get('country') and not result['country']:
            result['country'] = str(nightsearch['country'])
            sources_set.add("NightSearch (страна)")

        results_list = nightsearch.get('results', [])
        if isinstance(results_list, list):
            for item in results_list:
                if not isinstance(item, dict):
                    continue
                source_name = item.get('database', item.get('source', 'Unknown'))
                source_name = re.sub(r'[^\w\s\-\.]', '', source_name).strip()
                if not source_name:
                    source_name = 'Unknown'

                fields = item.get('fields', [])
                record_data = {}
                record_key = ""
                for field in fields:
                    if isinstance(field, dict):
                        key = field.get('key', '')
                        value = field.get('value', '')
                        if key and value:
                            clean_key = re.sub(r'[^\w\s\-\.]', '', key).strip()
                            if clean_key:
                                record_data[clean_key] = value
                                if clean_key in ['ФИО', 'Адрес', 'Телефон']:
                                    record_key += str(value)

                if record_data and record_key not in seen_records:
                    seen_records.add(record_key)
                    sources_set.add(source_name)
                    records_count += 1
                    result['extra'][f"Запись #{records_count}"] = {
                        'source': source_name,
                        'data': record_data
                    }

                if not result['fio']:
                    for field in fields:
                        if field.get('key') in ['ФИО', 'Имя', 'full_name']:
                            if field.get('value'):
                                result['fio'] = str(field['value'])
                                sources_set.add("NightSearch (ФИО)")
                                break

                if not all_birthdates:
                    for field in fields:
                        if field.get('key') in ['Дата рождения', 'birthdate']:
                            if field.get('value'):
                                normalized = normalize_birthdate(str(field['value']))
                                if normalized:
                                    all_birthdates.append(normalized)
                                    break

                if not result['address']:
                    for field in fields:
                        if field.get('key') in ['Адрес', 'address']:
                            if field.get('value'):
                                addr_str = str(field['value']).strip()
                                if not re.match(r'^\d{4}-\d{2}-\d{2}', addr_str):
                                    result['address'] = addr_str
                                    sources_set.add("NightSearch (адрес)")
                                    break

                for field in fields:
                    if isinstance(field, dict):
                        value = field.get('value', '')
                        if isinstance(value, str):
                            inn_match = re.search(r'\b(\d{12})\b', value)
                            if inn_match and not result['inn']:
                                result['inn'] = inn_match.group(1)
                                sources_set.add("NightSearch (ИНН)")
                            snils_clean = value.replace('-', '').replace(' ', '')
                            snils_match = re.search(r'\b(\d{11})\b', snils_clean)
                            if snils_match and not result['snils']:
                                result['snils'] = snils_match.group(1)
                                sources_set.add("NightSearch (СНИЛС)")
                            passport_match = re.search(r'\b(\d{4}\s?\d{6})\b', value)
                            if passport_match and not result['passport']:
                                result['passport'] = passport_match.group(1)
                                sources_set.add("NightSearch (паспорт)")

    # ---- Jitler (телефонные книги) ----
    if jitler:
        jitler_data = jitler.get('response', jitler)
        phonebooks = jitler_data.get('phonebooks', [])
        if phonebooks:
            result['phone_books'].extend(phonebooks)
            sources_set.add("Jitler (телефонная книга)")

        profiles = jitler_data.get('profiles', {})
        if profiles.get('vk'):
            vk_urls = [p.get('url') for p in profiles['vk'] if p.get('url')]
            if vk_urls and not result['vk']:
                result['vk'] = vk_urls[0]
                sources_set.add("Jitler (VK)")
        if profiles.get('ok'):
            ok_urls = [p.get('url') for p in profiles['ok'] if p.get('url')]
            if ok_urls and not result['ok']:
                result['ok'] = ok_urls[0]
                sources_set.add("Jitler (OK)")
        if profiles.get('instagram'):
            inst_urls = [p.get('url') for p in profiles['instagram'] if p.get('url')]
            if inst_urls and not result['instagram']:
                result['instagram'] = inst_urls[0]
                sources_set.add("Jitler (Instagram)")
        if profiles.get('tiktok'):
            tt_urls = [p.get('url') for p in profiles['tiktok'] if p.get('url')]
            if tt_urls and not result['tiktok']:
                result['tiktok'] = tt_urls[0]
                sources_set.add("Jitler (TikTok)")

    # ---- SEON / Snusbase (дополнительно) ----
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

    # Очистка дублей
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
    result['cards'] = list(dict.fromkeys(result['cards']))
    result['banks'] = list(dict.fromkeys(result['banks']))

    cache[cache_key] = (datetime.now(), result)
    return result

# ===== ФУНКЦИИ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ =====
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
    except:
        return False

# ===== MINI APP HTML =====
def generate_mini_app_html():
    return """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Fsociety Search</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body {
            background: #0b0d10;
            color: #e0e0e0;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            padding: 16px;
            min-height: 100vh;
        }
        .container { max-width: 600px; margin: 0 auto; }
        .header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 20px;
            padding-bottom: 12px;
            border-bottom: 1px solid #1e2329;
        }
        .header h1 { font-size: 24px; font-weight: 700; color: #ff851f; letter-spacing: -0.5px; }
        .search-box {
            background: #13161b;
            border-radius: 16px;
            padding: 16px 20px;
            margin-bottom: 20px;
        }
        .search-box input {
            width: 100%;
            padding: 12px 16px;
            border-radius: 10px;
            border: 1px solid #2a2f36;
            background: #0b0d10;
            color: #fff;
            font-size: 16px;
            outline: none;
        }
        .search-box input:focus { border-color: #ff851f; }
        .search-box button {
            width: 100%;
            margin-top: 12px;
            padding: 14px;
            border: none;
            border-radius: 10px;
            background: #ff851f;
            color: #fff;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
        }
        .search-box button:active { opacity: 0.7; }
        .result { margin-top: 16px; }
        .phone-card {
            background: #13161b;
            border-radius: 16px;
            padding: 16px 20px;
            margin-bottom: 16px;
            border-left: 4px solid #ff851f;
        }
        .phone-card .label { font-size: 12px; color: #a5aab4; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
        .phone-card .value { font-size: 20px; font-weight: 600; color: #fff; }
        .phone-card .sub { font-size: 14px; color: #a5aab4; margin-top: 4px; }
        .section {
            background: #13161b;
            border-radius: 16px;
            padding: 16px 20px;
            margin-bottom: 16px;
        }
        .section-title { font-size: 14px; font-weight: 600; color: #a5aab4; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px; }
        .row {
            display: flex;
            justify-content: space-between;
            padding: 6px 0;
            border-bottom: 1px solid #1e2329;
            font-size: 14px;
        }
        .row:last-child { border-bottom: none; }
        .row .label { color: #a5aab4; flex: 0 0 100px; }
        .row .value { color: #fff; text-align: right; flex: 1; word-break: break-word; }
        .row .value a { color: #ff851f; text-decoration: none; }
        .record-card {
            background: #0b0d10;
            border-radius: 12px;
            padding: 14px 16px;
            margin-bottom: 12px;
            border-left: 3px solid #ff851f;
        }
        .record-card .source { font-size: 12px; color: #ff851f; font-weight: 600; margin-bottom: 8px; }
        .record-card .field { display: flex; padding: 3px 0; font-size: 13px; }
        .record-card .field .key { color: #a5aab4; flex: 0 0 90px; }
        .record-card .field .val { color: #fff; flex: 1; word-break: break-word; }
        .loading { text-align: center; padding: 40px 0; color: #a5aab4; }
        .spinner {
            border: 3px solid #1e2329;
            border-top: 3px solid #ff851f;
            border-radius: 50%;
            width: 36px;
            height: 36px;
            animation: spin 1s linear infinite;
            margin: 0 auto 16px;
        }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        .footer { text-align: center; font-size: 11px; color: #2a2f36; padding-top: 20px; border-top: 1px solid #1e2329; margin-top: 20px; }
        .empty { text-align: center; padding: 30px 0; color: #a5aab4; }
        .empty .icon { font-size: 48px; margin-bottom: 12px; }
        .hidden { display: none; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🕵️ Fsociety</h1>
            <span style="color:#a5aab4;font-size:12px;" id="time"></span>
        </div>
        <div class="search-box">
            <input type="tel" id="phoneInput" placeholder="Введите номер телефона" autofocus>
            <button id="searchBtn">🔍 Найти</button>
        </div>
        <div id="resultContainer"></div>
        <div class="footer">Мониторинг uptimerobot.com</div>
    </div>
    <script>
        const tg = window.Telegram.WebApp;
        tg.expand();
        tg.ready();

        document.getElementById('time').textContent = new Date().toLocaleString('ru-RU', {
            day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit'
        });

        const phoneInput = document.getElementById('phoneInput');
        const searchBtn = document.getElementById('searchBtn');
        const resultContainer = document.getElementById('resultContainer');

        searchBtn.addEventListener('click', () => {
            const query = phoneInput.value.replace(/[^0-9]/g, '');
            if (query.length < 10) {
                resultContainer.innerHTML = '<div class="empty"><div class="icon">⚠️</div><div>Введите корректный номер</div></div>';
                return;
            }
            resultContainer.innerHTML = '<div class="loading"><div class="spinner"></div><div>Поиск...</div></div>';
            fetch(`/api/report?query=${encodeURIComponent(query)}`)
                .then(res => res.json())
                .then(data => {
                    if (data.error) {
                        resultContainer.innerHTML = `<div class="empty"><div class="icon">❌</div><div>${data.error}</div></div>`;
                        return;
                    }
                    renderReport(data);
                })
                .catch(err => {
                    resultContainer.innerHTML = `<div class="empty"><div class="icon">⚠️</div><div>Ошибка: ${err.message}</div></div>`;
                });
        });

        function renderReport(data) {
            let html = '';
            // Телефон
            html += `
                <div class="phone-card">
                    <div class="label">📱 Телефон</div>
                    <div class="value">${data.query}</div>
                    <div class="sub">
                        ${data.operator ? '📡 ' + data.operator : ''}
                        ${data.region ? '📍 ' + data.region : ''}
                        ${data.country ? '🌍 ' + data.country : ''}
                    </div>
                    <div class="sub" style="margin-top:6px;font-size:13px;color:#6b7280;">
                        👁 Просмотров: ${data.views || 0}
                    </div>
                </div>
            `;

            // Основные данные
            const basic = [];
            if (data.fio) basic.push({label:'ФИО', value:data.fio});
            if (data.birthdate) basic.push({label:'Дата рождения', value:data.birthdate});
            if (data.age !== null && data.age !== undefined) basic.push({label:'Возраст', value:data.age+' лет'});
            if (data.address) basic.push({label:'Адрес', value:data.address});
            if (data.inn) basic.push({label:'ИНН', value:data.inn});
            if (data.snils) basic.push({label:'СНИЛС', value:data.snils});
            if (data.passport) basic.push({label:'Паспорт', value:data.passport});
            if (basic.length) {
                html += `<div class="section"><div class="section-title">👤 Основные данные</div>`;
                basic.forEach(item => {
                    html += `<div class="row"><span class="label">${item.label}</span><span class="value">${item.value}</span></div>`;
                });
                html += `</div>`;
            }

            // Контакты
            const contacts = [];
            if (data.emails?.length) data.emails.forEach(e => contacts.push({label:'Email', value:e}));
            if (data.telegrams?.length) data.telegrams.forEach(t => contacts.push({label:'Telegram', value:t}));
            if (data.phone_books?.length) data.phone_books.slice(0,5).forEach(p => contacts.push({label:'Телефонная книга', value:p}));
            if (contacts.length) {
                html += `<div class="section"><div class="section-title">📞 Контакты</div>`;
                contacts.forEach(item => {
                    html += `<div class="row"><span class="label">${item.label}</span><span class="value">${item.value}</span></div>`;
                });
                html += `</div>`;
            }

            // Соцсети
            const social = [];
            if (data.vk) social.push({label:'VK', value:data.vk});
            if (data.ok) social.push({label:'Одноклассники', value:data.ok});
            if (data.instagram) social.push({label:'Instagram', value:data.instagram});
            if (data.tiktok) social.push({label:'TikTok', value:data.tiktok});
            if (social.length) {
                html += `<div class="section"><div class="section-title">🌐 Соцсети</div>`;
                social.forEach(item => {
                    html += `<div class="row"><span class="label">${item.label}</span><span class="value"><a href="${item.value}" target="_blank">${item.value}</a></span></div>`;
                });
                html += `</div>`;
            }

            // Все записи
            if (data.extra && Object.keys(data.extra).length) {
                html += `<div class="section"><div class="section-title">📂 Все записи</div>`;
                for (const [key, record] of Object.entries(data.extra)) {
                    if (typeof record === 'object' && record.source && record.data) {
                        const source = record.source;
                        const fields = record.data;
                        html += `<div class="record-card"><div class="source">${source}</div>`;
                        const important = ['ФИО','Имя','Фамилия','Дата рождения','Телефон','Email','Адрес','ИНН','СНИЛС','Паспорт'];
                        for (const k of important) {
                            if (fields[k]) {
                                html += `<div class="field"><span class="key">${k}</span><span class="val">${fields[k]}</span></div>`;
                            }
                        }
                        for (const [k, v] of Object.entries(fields)) {
                            if (!important.includes(k) && v) {
                                html += `<div class="field"><span class="key">${k}</span><span class="val">${v}</span></div>`;
                            }
                        }
                        html += `</div>`;
                    }
                }
                html += `</div>`;
            }

            // Итог
            html += `
                <div class="section" style="border-left:none;background:transparent;text-align:center;color:#6b7280;font-size:13px;">
                    📊 Найдено записей: ${data.records_count || 0}
                    ${data.sources?.length ? ' · 📁 ' + data.sources.join(', ') : ''}
                </div>
            `;

            resultContainer.innerHTML = html;
            tg.ready();
        }
    </script>
</body>
</html>
"""

# ===== ЭНДПОИНТЫ ВЕБ-СЕРВЕРА =====
async def mini_app(request):
    return web.Response(text=generate_mini_app_html(), content_type='text/html')

async def api_report(request):
    query = request.query.get('query', '')
    if not query:
        return web.json_response({'error': 'No query'}, status=400)

    data = await get_report(query)
    if not data:
        cache_key = get_cache_key("phone", query)
        if cache_key in cache:
            cached_time, data = cache[cache_key]
            if datetime.now() - cached_time > CACHE_TTL:
                data = None
    if not data:
        data = await collect_phone_data(query)
        await save_report(query, data)

    if not data:
        return web.json_response({'error': 'Данные не найдены'}, status=404)

    views = await get_unique_views_phone(query, 0)
    data['views'] = views

    if 'extra' in data:
        sorted_extra = {}
        for key in sorted(data['extra'].keys(), key=lambda x: int(x.split('#')[-1]) if '#' in x else 0):
            sorted_extra[key] = data['extra'][key]
        data['extra'] = sorted_extra

    return web.json_response(data)

# ===== ОБРАБОТЧИКИ БОТА =====

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def start_cmd(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username

    # Проверка подписки
    if not await check_subscription(user_id):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription")]
        ])
        await message.reply("❌ Для использования бота подпишитесь на канал!", reply_markup=keyboard)
        return

    # Проверка пользователя
    user = await get_user(user_id)
    if not user:
        # обработка рефералки
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
        # проверка рефералки для старых пользователей (если пришли по ссылке)
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

    # Получаем количество доступных запросов
    available = await get_user_available_requests(user_id)
    bonus = user['bonus_requests'] if user else 0
    invited, _ = await get_referral_stats(user_id)

    # Кнопка WebApp для поиска
    webapp_url = f"{BASE_URL}/app"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Открыть поиск", web_app=WebAppInfo(url=webapp_url))]
    ])

    # Приветственное сообщение как на скриншоте
    text = (
        f"<b>FSOCIETY SEARCH</b>\n\n"
        f"Привет, {message.from_user.first_name or 'пользователь'}!\n\n"
        f"• Поиск по базам данных.\n"
        f"• Доступно запросов: <b>{available}</b>\n"
        f"• Лимит обновляется каждый день по МСК\n\n"
        f"• Приглашай друзей — +1 запрос за каждого\n"
        f"• Покупка запросов: вкладка «Подписка» или @postgeny\n\n"
        f"Нажми кнопку ниже, чтобы открыть поиск."
    )

    await message.reply(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data == "check_subscription")
async def check_subscription_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    if await check_subscription(user_id):
        await callback.message.delete()
        await start_cmd(callback.message)
    else:
        await callback.answer("Вы ещё не подписались!", show_alert=True)

# ===== ЗАПУСК =====
async def main():
    await init_db()
    app = web.Application()
    app.router.add_get("/", lambda req: web.Response(text="Fsociety Search Bot is running"))
    app.router.add_get("/health", lambda req: web.Response(text="OK"))
    app.router.add_get("/app", mini_app)
    app.router.add_get("/api/report", api_report)

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
