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
    BufferedInputFile, PreCheckoutQuery, LabeledPrice, SuccessfulPayment
)
import hashlib

# Функция экранирования для MarkdownV2
def escape_md(text: str) -> str:
    """Экранирует специальные символы для MarkdownV2"""
    if not text:
        return ""
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text

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

# Funstat API
FUNSTAT_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiIyMDMzMDI5NDc1IiwianRpIjoiODJmMjlmNzQtYmJlMi00ZGUwLWEwZDQtN2EzMDJhMWE5MDViIiwiZXhwIjoxODAxMDA4MzM4fQ.Mba4aX85YAMcaMLfhUBzXtCoNmEujfMe-6sGBbp3kT-T2SiLM_Ho0BBAFAQ8_C6Gz06PH9mAYhfBvlLSjb4oVd1Fm_vmb8MC-wuObU3qgfGrYdGzVF3ntJHv-LdNELq-jsqvQOY3jq9meso9dUoyj5SviDQWL6cvnRQ03kpHWxA"
FUNSTAT_BASE_URL = "https://telelog.info/api/v1"

CRYPTOPAY_TOKEN = "626190:AAkRdsFEHPdfZc6yIACB52hh1mVKkOdT0qc"

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
    "nightsearch": 1.0,
    "seon": 1.0,
    "snusbase": 1.0,
    "depsearch": 2.0,
    "jitler": 1.5,
    "funstat": 5.0
}

# ===== КЛАССЫ СОСТОЯНИЙ =====
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
            CREATE TABLE IF NOT EXISTS id_reports (
                key TEXT PRIMARY KEY,
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
            CREATE TABLE IF NOT EXISTS id_views (
                id TEXT PRIMARY KEY,
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

# ===== FUNSTAT API =====
async def funstat_request(endpoint: str, method: str = "GET", params: dict = None) -> dict:
    """Универсальная функция для запросов к Funstat API"""
    if not FUNSTAT_TOKEN or not FUNSTAT_BASE_URL:
        return {}
    
    session = await get_http_session()
    url = f"{FUNSTAT_BASE_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {FUNSTAT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        async with session.request(
            method, url, params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["funstat"])
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                print(f"Funstat error: {resp.status} - {await resp.text()}")
                return {}
    except Exception as e:
        print(f"Funstat request error: {e}")
        return {}

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

# ===== СБОР ДАННЫХ ПО TELEGRAM ID =====
async def collect_telegram_data(user_id: str) -> dict:
    """Сбор информации по Telegram ID через Jitler + Funstat"""
    cache_key = get_cache_key("telegram", user_id)
    if cache_key in cache:
        cached_time, data = cache[cache_key]
        if datetime.now() - cached_time < CACHE_TTL:
            return data
    
    result = {
        'user_id': user_id,
        'username': None,
        'first_name': None,
        'last_name': None,
        'bio': None,
        'names_history': [],
        'usernames_history': [],
        'groups': [],
        'groups_count': 0,
        'gifts_received': [],
        'gifts_sent': [],
        'phone_books': [],
        'vk_profiles': [],
        'instagram_profiles': [],
        'tiktok_profiles': [],
        'ok_profiles': [],
        'telegram_contacts': [],
        'found': False
    }
    
    # 1. Запрос через Jitler с типом "sherlock"
    jitler_data = await jitler_search_with_balancer(user_id, "sherlock")
    
    if jitler_data and isinstance(jitler_data, dict):
        response = jitler_data.get('response', {})
        if response:
            result['found'] = True
            
            if response.get('username'):
                result['username'] = response['username']
            if response.get('first_name'):
                result['first_name'] = response['first_name']
            if response.get('last_name'):
                result['last_name'] = response['last_name']
            if response.get('bio'):
                result['bio'] = response['bio']
            if response.get('names'):
                result['names_history'] = response['names'][:10]
            if response.get('usernames'):
                result['usernames_history'] = response['usernames'][:10]
            if response.get('groups'):
                result['groups'] = response['groups'][:20]
                result['groups_count'] = len(response['groups'])
            if response.get('gifts_received'):
                result['gifts_received'] = response['gifts_received'][:20]
            if response.get('gifts_sent'):
                result['gifts_sent'] = response['gifts_sent'][:20]
            if response.get('phone_books'):
                result['phone_books'] = response['phone_books'][:50]
            if response.get('vk_profiles'):
                result['vk_profiles'] = response['vk_profiles'][:10]
            if response.get('instagram_profiles'):
                result['instagram_profiles'] = response['instagram_profiles'][:10]
            if response.get('tiktok_profiles'):
                result['tiktok_profiles'] = response['tiktok_profiles'][:10]
            if response.get('ok_profiles'):
                result['ok_profiles'] = response['ok_profiles'][:10]
            if response.get('telegram_contacts'):
                result['telegram_contacts'] = response['telegram_contacts'][:10]
    
    # 2. Дополняем из Funstat
    if FUNSTAT_TOKEN:
        try:
            # Базовая информация
            basic_info = await funstat_request("/api/v1/users/basic_info_by_id", params={"id": user_id})
            if basic_info and basic_info.get('result'):
                info = basic_info.get('result', {})
                if not result['username'] and info.get('username'):
                    result['username'] = info['username']
                if info.get('first_name') and not result['first_name']:
                    result['first_name'] = info['first_name']
                if info.get('last_name') and not result['last_name']:
                    result['last_name'] = info['last_name']
                if info.get('bio') and not result['bio']:
                    result['bio'] = info['bio']
                if not result['found']:
                    result['found'] = True
            
            # История имён
            names_data = await funstat_request(f"/api/v1/users/{user_id}/names")
            if names_data and names_data.get('result'):
                names = names_data.get('result', [])
                existing_names = set(result['names_history'])
                for name in names:
                    if name not in existing_names:
                        result['names_history'].append(name)
                result['names_history'] = result['names_history'][:20]
                if names:
                    result['found'] = True
            
            # История юзернеймов
            usernames_data = await funstat_request(f"/api/v1/users/{user_id}/usernames")
            if usernames_data and usernames_data.get('result'):
                usernames = usernames_data.get('result', [])
                existing_usernames = set(result['usernames_history'])
                for username in usernames:
                    if username not in existing_usernames:
                        result['usernames_history'].append(username)
                result['usernames_history'] = result['usernames_history'][:20]
                if usernames:
                    result['found'] = True
            
            # Подарки
            gifts_data = await funstat_request(f"/api/v1/users/{user_id}/gifts_relation")
            if gifts_data and gifts_data.get('result'):
                gifts = gifts_data.get('result', {})
                if gifts.get('received'):
                    existing_received = set(result['gifts_received'])
                    for gift in gifts['received']:
                        if gift not in existing_received:
                            result['gifts_received'].append(gift)
                    result['gifts_received'] = result['gifts_received'][:20]
                    result['found'] = True
                if gifts.get('sent'):
                    existing_sent = set(result['gifts_sent'])
                    for gift in gifts['sent']:
                        if gift not in existing_sent:
                            result['gifts_sent'].append(gift)
                    result['gifts_sent'] = result['gifts_sent'][:20]
                    result['found'] = True
        
        except Exception as e:
            print(f"Funstat ошибка: {e}")
    
    # Если хотя бы один источник дал данные
    if result['found'] or any([
        result['names_history'], result['usernames_history'],
        result['groups'], result['gifts_received'], result['gifts_sent'],
        result['phone_books'], result['vk_profiles']
    ]):
        result['found'] = True
    
    cache[cache_key] = (datetime.now(), result)
    return result

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
            results[name] = await asyncio.wait_for(task, timeout=3.0)
        except asyncio.TimeoutError:
            results[name] = {}
            task.cancel()
    
    nightsearch = results.get('nightsearch', {}) if isinstance(results.get('nightsearch', {}), dict) else {}
    seon = results.get('seon', {}) if isinstance(results.get('seon', {}), dict) else {}
    snusbase = results.get('snusbase', {}) if isinstance(results.get('snusbase', {}), dict) else {}
    depsearch = results.get('depsearch', {}) if isinstance(results.get('depsearch', {}), dict) else {}
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
        'extra': {},
        'sources': [],
        'records_count': 0,
        'raw_data': {
            'depsearch': depsearch,
            'jitler': jitler,
            'nightsearch': nightsearch,
            'seon': seon,
            'snusbase': snusbase
        }
    }

    all_birthdates = []
    sources_set = set()
    records_count = 0
    seen_records = set()

    # ===== ОСНОВНОЙ ИСТОЧНИК: DepSearch =====
    if depsearch:
        # Основная информация из phone_info
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

        # Результаты поиска (записи из утечек)
        results_list = depsearch.get('results', [])
        if isinstance(results_list, list):
            for item in results_list:
                if isinstance(item, dict):
                    source_name = item.get('🏫Источник', item.get('Источник', 'Unknown'))
                    source_name = re.sub(r'[^\w\s\-\.]', '', source_name).strip()
                    if not source_name or source_name == '':
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
                    
                    # Извлечение ФИО
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
                    
                    # Извлечение даты рождения
                    if not all_birthdates:
                        bdate = (
                            item.get('🎂Дата рождения') or
                            item.get('birthdate') or
                            item.get('birth_date') or
                            item.get('Дата рождения')
                        )
                        if bdate:
                            normalized = normalize_birthdate(bdate)
                            if normalized:
                                all_birthdates.append(normalized)
                    
                    # Извлечение адреса
                    if not result['address']:
                        address = item.get('📍Адрес') or item.get('🏠Адрес') or item.get('address')
                        if address:
                            addr_str = str(address).strip()
                            if not re.match(r'^\d{4}-\d{2}-\d{2}', addr_str):
                                result['address'] = addr_str
                                sources_set.add("DepSearch (адрес)")
                    
                    # Извлечение карт
                    if not result['cards']:
                        card = item.get('💳Карта') or item.get('card') or item.get('💳 Банковская карта')
                        if card:
                            result['cards'].append(str(card))
                    
                    # Извлечение email
                    email = item.get('✉️Почта') or item.get('email') or item.get('mail')
                    if email and '@' in str(email):
                        result['emails'].append(str(email))
                    
                    # Извлечение VK
                    vk = item.get('🧑‍💻Вконтакте') or item.get('vk') or item.get('vkontakte')
                    if vk and not result['vk']:
                        result['vk'] = get_social_url(vk)
                        sources_set.add("DepSearch (VK)")
                    
                    # Извлечение OK
                    ok = item.get('👨‍🦳Одноклассники') or item.get('ok') or item.get('odnoklassniki')
                    if ok and not result['ok']:
                        result['ok'] = get_social_url(ok)
                        sources_set.add("DepSearch (OK)")
                    
                    # Извлечение Instagram
                    inst = item.get('📷Instagram') or item.get('instagram')
                    if inst and not result['instagram']:
                        result['instagram'] = get_social_url(inst)
                        sources_set.add("DepSearch (Instagram)")
                    
                    # Извлечение TikTok
                    tt = item.get('👩‍🦲TikTok') or item.get('tiktok')
                    if tt and not result['tiktok']:
                        result['tiktok'] = get_social_url(tt)
                        sources_set.add("DepSearch (TikTok)")

    # ===== JITLER =====
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
        
        records_jitler = jitler_data.get('records', [])
        for rec in records_jitler:
            if isinstance(rec, dict):
                source_name = "Jitler"
                record_data = {}
                for k, v in rec.items():
                    if v and v not in (None, '', [], {}):
                        record_data[k] = v
                if record_data:
                    sources_set.add(source_name)
                    records_count += 1
                    result['extra'][f"Запись #{records_count}"] = {
                        'source': source_name,
                        'data': record_data
                    }

    # ===== ДОПОЛНИТЕЛЬНЫЕ ИСТОЧНИКИ =====
    for src, src_name in [(nightsearch, 'NightSearch'), (seon, 'SEON'), (snusbase, 'Snusbase')]:
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

    # Расчёт возраста
    if all_birthdates:
        result['birthdate'] = find_best_birthdate(all_birthdates)
        if result['birthdate']:
            age = calculate_age_from_birthdate(result['birthdate'])
            if age is not None:
                result['age'] = age

    # Очистка данных
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

# ===== ФОРМАТТЕРЫ =====
def format_value(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, (list, tuple)):
                if len(item) == 2 and isinstance(item[0], (str, bytes)) and isinstance(item[1], (str, int, float)):
                    parts.append(f"{item[0]}: {item[1]}")
                else:
                    parts.append(", ".join(str(x) for x in item))
            elif isinstance(item, dict):
                parts.append(", ".join(f"{k}: {v}" for k, v in item.items()))
            else:
                parts.append(str(item))
        return "; ".join(parts)
    if isinstance(value, dict):
        return ", ".join(f"{k}: {v}" for k, v in value.items())
    return str(value)

def format_phone_report(data: dict, views: int) -> str:
    if not data.get('sources') and not data.get('extra'):
        return f"📱\n└ Телефон: {data.get('query', '')}\n\n❌ Ничего не найдено по этому номеру.\n\n👁 Интересовались этим: {views}"
    
    lines = ["📱"]
    
    # Основная информация
    fields = [("Телефон", data['query'])]
    if data.get('operator'):
        fields.append(("Оператор", data['operator']))
    if data.get('region'):
        fields.append(("Регион", data['region']))
    if data.get('country'):
        fields.append(("Страна", data['country']))
    
    for i, (label, value) in enumerate(fields):
        if i == len(fields) - 1:
            lines.append(f"└ {label}: {value}")
        else:
            lines.append(f"├ {label}: {value}")

    # Основные данные (ФИО, дата рождения, возраст)
    personal_fields = []
    if data.get('fio'):
        personal_fields.append(("ФИО", data['fio']))
    if data.get('birthdate'):
        personal_fields.append(("Дата рождения", data['birthdate']))
    if data.get('age') is not None:
        personal_fields.append(("Возраст", str(data['age'])))
    
    if personal_fields:
        lines.append("")
        lines.append("👤 Основные данные")
        for i, (label, value) in enumerate(personal_fields):
            if i == len(personal_fields) - 1:
                lines.append(f"└ {label}: {value}")
            else:
                lines.append(f"├ {label}: {value}")

    # Телефонные книги
    if data.get('phone_books'):
        books = data['phone_books'][:15]
        lines.append("")
        if len(data['phone_books']) > 15:
            books_str = ', '.join(books) + f" и ещё {len(data['phone_books']) - 15}"
        else:
            books_str = ', '.join(books)
        lines.append(f"🔎 Телефонные книги: {books_str}")

    # Соцсети
    if data.get('vk'):
        lines.append("")
        lines.append(f"🧑‍💻 Вконтакте: {data['vk']}")
    if data.get('ok'):
        lines.append(f"👨‍🦳 Одноклассники: {data['ok']}")
    if data.get('tiktok'):
        lines.append(f"👩‍🦲 TikTok: {data['tiktok']}")
    if data.get('instagram'):
        lines.append(f"📷 Instagram: {data['instagram']}")

    # Email
    if data.get('emails'):
        lines.append("")
        emails_str = ', '.join(data['emails'][:5])
        if len(data['emails']) > 5:
            emails_str += f" ... и ещё {len(data['emails']) - 5}"
        lines.append(f"📧 E-mail: {emails_str}")

    # Telegram
    if data.get('telegrams'):
        lines.append("")
        lines.append(f"💬 Telegram: {', '.join(data['telegrams'])}")

    # Просмотры
    lines.append("")
    lines.append(f"👁 Интересовались этим: {views}")
    
    return "\n".join(lines)

def format_telegram_report(data: dict) -> str:
    """Форматирует отчёт по Telegram ID с символами ├ └"""
    if not data.get('found'):
        return f"👤\n└ Telegram ID: {data.get('user_id', '')}\n\n❌ Информация не найдена."
    
    lines = ["👤"]
    lines.append(f"├ Telegram ID: {data['user_id']}")
    
    if data.get('username'):
        lines.append(f"├ Username: @{data['username']}")
    if data.get('first_name'):
        lines.append(f"├ Имя: {data['first_name']}")
    if data.get('last_name'):
        lines.append(f"├ Фамилия: {data['last_name']}")
    if data.get('bio'):
        lines.append(f"├ Bio: {data['bio']}")
    
    if data.get('names_history'):
        lines.append("")
        lines.append("📝 История имён:")
        names = data['names_history'][:15]
        for i, name in enumerate(names):
            if i == len(names) - 1:
                lines.append(f"└ {name}")
            else:
                lines.append(f"├ {name}")
        if len(data['names_history']) > 15:
            lines.append(f"   ... и ещё {len(data['names_history']) - 15}")
    
    if data.get('usernames_history'):
        lines.append("")
        lines.append("📝 История юзернеймов:")
        usernames = data['usernames_history'][:15]
        for i, username in enumerate(usernames):
            if i == len(usernames) - 1:
                lines.append(f"└ @{username}")
            else:
                lines.append(f"├ @{username}")
        if len(data['usernames_history']) > 15:
            lines.append(f"   ... и ещё {len(data['usernames_history']) - 15}")
    
    if data.get('groups'):
        lines.append("")
        lines.append(f"👥 Группы ({data.get('groups_count', 0)}):")
        groups = data['groups'][:10]
        for i, group in enumerate(groups):
            if i == len(groups) - 1:
                lines.append(f"└ {group}")
            else:
                lines.append(f"├ {group}")
        if data.get('groups_count', 0) > 10:
            lines.append(f"   ... и ещё {data['groups_count'] - 10}")
    
    if data.get('phone_books'):
        books = data['phone_books'][:50]
        lines.append("")
        lines.append(f"🔎 Телефонные книги ({len(data['phone_books'])}): {', '.join(books)}")
    
    if data.get('vk_profiles'):
        lines.append("")
        lines.append(f"🧑‍💻 VK профили ({len(data['vk_profiles'])}):")
        vk_profiles = data['vk_profiles'][:5]
        for i, profile in enumerate(vk_profiles):
            if isinstance(profile, dict):
                profile_url = profile.get('url', '')
                profile_name = profile.get('name', '')
                if profile_url:
                    link = f"https://vk.com/{profile_url}" if not profile_url.startswith('http') else profile_url
                    display_name = profile_name if profile_name else profile_url
                    if i == len(vk_profiles) - 1:
                        lines.append(f"└ {display_name}: {link}")
                    else:
                        lines.append(f"├ {display_name}: {link}")
    
    if data.get('instagram_profiles'):
        lines.append("")
        lines.append(f"📷 Instagram ({len(data['instagram_profiles'])}):")
        insta_profiles = data['instagram_profiles'][:5]
        for i, profile in enumerate(insta_profiles):
            if isinstance(profile, dict):
                profile_url = profile.get('url', '')
                profile_name = profile.get('name', '')
                if profile_url:
                    link = f"https://instagram.com/{profile_url}" if not profile_url.startswith('http') else profile_url
                    display_name = profile_name if profile_name else profile_url
                    if i == len(insta_profiles) - 1:
                        lines.append(f"└ {display_name}: {link}")
                    else:
                        lines.append(f"├ {display_name}: {link}")
    
    if data.get('tiktok_profiles'):
        lines.append("")
        lines.append(f"🎵 TikTok ({len(data['tiktok_profiles'])}):")
        tiktok_profiles = data['tiktok_profiles'][:5]
        for i, profile in enumerate(tiktok_profiles):
            if isinstance(profile, dict):
                profile_url = profile.get('url', '')
                profile_name = profile.get('name', '')
                if profile_url:
                    link = f"https://tiktok.com/@{profile_url}" if not profile_url.startswith('http') else profile_url
                    display_name = profile_name if profile_name else profile_url
                    if i == len(tiktok_profiles) - 1:
                        lines.append(f"└ {display_name}: {link}")
                    else:
                        lines.append(f"├ {display_name}: {link}")
    
    if data.get('ok_profiles'):
        lines.append("")
        lines.append(f"👨‍🦳 Одноклассники ({len(data['ok_profiles'])}):")
        ok_profiles = data['ok_profiles'][:5]
        for i, profile in enumerate(ok_profiles):
            if isinstance(profile, dict):
                profile_url = profile.get('url', '')
                profile_name = profile.get('name', '')
                if profile_url:
                    link = f"https://ok.ru/{profile_url}" if not profile_url.startswith('http') else profile_url
                    display_name = profile_name if profile_name else profile_url
                    if i == len(ok_profiles) - 1:
                        lines.append(f"└ {display_name}: {link}")
                    else:
                        lines.append(f"├ {display_name}: {link}")
    
    if data.get('telegram_contacts'):
        lines.append("")
        lines.append(f"💬 Telegram контакты ({len(data['telegram_contacts'])}):")
        contacts = data['telegram_contacts'][:5]
        for i, contact in enumerate(contacts):
            if isinstance(contact, dict):
                contact_username = contact.get('username', '')
                contact_name = contact.get('name', '')
                if contact_username:
                    link = f"https://t.me/{contact_username}"
                    display_name = contact_name if contact_name else f"@{contact_username}"
                    if i == len(contacts) - 1:
                        lines.append(f"└ {display_name}: {link}")
                    else:
                        lines.append(f"├ {display_name}: {link}")
    
    gifts_list = []
    if data.get('gifts_received'):
        for gift in data['gifts_received'][:10]:
            gifts_list.append(f"📥 Получил от: {gift}")
    if data.get('gifts_sent'):
        for gift in data['gifts_sent'][:10]:
            gifts_list.append(f"📤 Отправил: {gift}")
    
    if gifts_list:
        lines.append("")
        lines.append("🎁 Подарки:")
        for i, gift in enumerate(gifts_list):
            if i == len(gifts_list) - 1:
                lines.append(f"└ {gift}")
            else:
                lines.append(f"├ {gift}")
        if len(gifts_list) > 10:
            lines.append(f"   ... и ещё {len(gifts_list) - 10}")
    
    return "\n".join(lines)

# ===== ГЕНЕРАТОР HTML-ОТЧЁТА =====
def generate_html_report(data: dict, views: int, title: str = "Отчёт по номеру телефона") -> str:
    extra = data.get('extra', {})
    records = []
    sources_set = set()
    records_count = 0
    seen_record_keys = set()
    
    for key, value in extra.items():
        if isinstance(value, dict) and 'source' in value and 'data' in value:
            source = value['source']
            record_key = source
            for k, v in value['data'].items():
                if k in ['ФИО', 'Адрес', 'ID', 'Телефон']:
                    record_key += str(v)
            if record_key in seen_record_keys:
                continue
            seen_record_keys.add(record_key)
            
            fields = []
            for k, v in value['data'].items():
                if v and v not in (None, '', [], {}):
                    clean_key = re.sub(r'[^\w\s\-\.]', '', str(k)).strip()
                    if clean_key:
                        if clean_key.upper() == 'БК' and isinstance(v, str):
                            fields.append(("БК", v))
                        else:
                            fields.append((clean_key.upper(), format_value(v)))
            if fields:
                records.append({
                    'title': source,
                    'fields': fields
                })
                sources_set.add(source)
                records_count += 1
    
    if not records:
        records = [{
            'title': "Данные не найдены",
            'fields': [("СООБЩЕНИЕ", "Ничего не найдено по этому запросу")]
        }]
        sources_set.add("Нет данных")
        records_count = 1
    
    total_bases = len(sources_set)
    
    structure_items = ""
    for idx, record in enumerate(records, 1):
        structure_items += f"""
        <div class="client">
            <svg width="22.031" height="22.031" viewBox="0 0 22.031 22.031" xmlns="http://www.w3.org/2000/svg">
                <circle cx="11.0156" cy="11.0156" r="8.0856" fill="none" stroke="#currentColor" stroke-width="5.86" />
            </svg>
            <a href="#record{idx}" class="clients_name">{record['title'][:30]}</a>
        </div>
        <div class="stick"></div>
        """
    
    accordions = ""
    for idx, record in enumerate(records, 1):
        rows_html = ""
        for label, value in record['fields']:
            if label and value:
                rows_html += f'<div class="row"><strong>{label.upper()}:</strong><span>{value}</span></div>'
        accordions += f"""
        <div id="record{idx}" class="accordion_inner">
            <div class="accordion open">
                <div class="accordion-header" onclick="toggleAccordion(this)">
                    <span>{record['title']}</span>
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
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{data.get('query', 'Отчёт')}</title>
    <style>
        html {{ scroll-behavior: smooth; }}
        body {{
            font-family: "Source Sans Pro", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background-color: #0b0d10;
            margin: 0;
            height: 100vh;
            margin-right: 0;
        }}
        *, ::before, ::after {{ box-sizing: border-box; }}
        h1, h2, h3, h4, h5, h6, p {{ margin: 0; }}
        .container {{ max-width: 1645px; width: 100%; margin: 0 auto; }}
        .header {{
            margin: 60px 0;
            margin-bottom: 45px;
        }}
        .header_inner {{
            padding: 40px 30px 40px 75px;
            background-color: #13161b;
            border-radius: 30px;
        }}
        .request {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 38px;
        }}
        .request1 {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .request_text {{
            font-weight: 700;
            font-size: 34px;
            line-height: 38px;
            color: #fff;
        }}
        .request_number {{
            padding: 16px 27px;
            font-weight: 600;
            font-size: 23px;
            line-height: 27px;
            text-align: center;
            color: #fff;
            background-color: #0b0d10;
            border-radius: 20px;
        }}
        .result {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 10px;
            background-color: #0b0d10;
            padding: 14px 20px;
            border-radius: 20px;
        }}
        .result_text {{
            font-weight: 600;
            font-size: 16px;
            line-height: 21px;
            text-align: center;
            color: #fff;
        }}
        .result_number {{
            background-color: #ff851f;
            border-radius: 10px;
            font-weight: 600;
            font-size: 16px;
            line-height: 21px;
            text-align: center;
            color: #fff;
            padding: 6px 22px;
        }}
        .downloading {{
            display: flex;
            align-items: center;
            gap: 22px;
        }}
        .btn1 {{
            padding: 18px 45px;
            display: flex;
            align-items: center;
            gap: 10px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 16px;
            line-height: 21px;
            text-align: center;
            color: #fff;
            cursor: pointer;
            border: none;
            transition: opacity 0.3s ease;
        }}
        .btn1:hover {{ opacity: 0.6; }}
        .downloadPDF {{ background-color: #ff8119; }}
        .print {{ background-color: #0b0d10; }}
        .main_inner {{
            display: flex;
            justify-content: space-between;
            gap: 33px;
            width: 100%;
        }}
        .block1 {{ width: 25%; }}
        .block1_inner {{ position: sticky; top: 45px; z-index: 100; }}
        .block2 {{ width: 73%; }}
        .block_title {{
            font-weight: 600;
            font-size: 14px;
            line-height: 23px;
            color: #fff;
            margin-left: 35px;
            margin-bottom: 16px;
        }}
        .bg_str {{
            background-color: #13161b;
            padding-right: 28px;
            border-radius: 20px;
        }}
        .structure {{
            padding: 30px 16px 30px 38px;
            background-color: #13161b;
            border-radius: 20px;
            max-height: calc(100vh - 134px);
            overflow-y: auto;
        }}
        .client {{
            display: flex;
            align-items: center;
            gap: 10px;
            color: #fff;
        }}
        .client svg {{
            flex-shrink: 0;
            width: 22px;
            height: 22px;
            stroke: #222730;
            transition: stroke 0.3s ease;
        }}
        .clients_name {{
            font-weight: 500;
            font-size: 16px;
            line-height: 23px;
            color: #fff;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            transition: color 0.3s ease;
            cursor: pointer;
            text-decoration: none;
        }}
        .client:hover svg,
        .client:hover .clients_name {{
            stroke: #ff8119;
            color: #ff8119;
        }}
        .stick {{
            width: 4px;
            height: 36px;
            background: #222730;
            margin-left: 9px;
        }}
        .structure::-webkit-scrollbar {{
            width: 16px;
            background: #0b0d10;
        }}
        .structure::-webkit-scrollbar-track {{ background: #0b0d10; }}
        .structure::-webkit-scrollbar-thumb {{
            background: #fff;
            border: 4px solid #0b0d10;
            border-radius: 8px;
            background-clip: padding-box;
        }}
        .structure::-webkit-scrollbar-button:single-button:vertical:decrement {{
            background: #0b0d10 url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='white'%3E%3Cpath d='M12 8l-6 6h12z'/%3E%3C/svg%3E") center no-repeat;
            background-size: 20px;
            height: 28px;
        }}
        .structure::-webkit-scrollbar-button:single-button:vertical:increment {{
            background: #0b0d10 url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='white'%3E%3Cpath d='M12 16l6-6H6z'/%3E%3C/svg%3E") center no-repeat;
            background-size: 20px;
            height: 28px;
        }}
        .accordion_inner {{
            padding: 20px 26px 20px 16px;
            background-color: #13161b;
            border-radius: 20px;
            margin-bottom: 30px;
        }}
        .accordion_inner:last-of-type {{ margin-bottom: 0; }}
        .accordion-header {{
            padding: 14px 10px 14px 35px;
            background: #0b0d10;
            display: flex;
            align-items: center;
            justify-content: space-between;
            cursor: pointer;
            user-select: none;
            font-weight: 700;
            font-size: 18px;
            line-height: 23px;
            color: #fff;
            border-radius: 15px;
        }}
        .accordion-arrow {{
            transition: transform 0.3s;
            padding: 8px 14px;
            background-color: #13161b;
            border-radius: 10px;
        }}
        .accordion-body {{
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease;
        }}
        .accordion-content {{
            padding: 30px 20px 10px 30px;
        }}
        .accordion-content .row {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 20px;
        }}
        .accordion-content .row:last-of-type {{ margin-bottom: 0; }}
        .accordion-content .row strong {{
            text-transform: uppercase;
            font-weight: 500;
            font-size: 16px;
            line-height: 20px;
            color: #fff;
        }}
        .accordion-content .row span {{
            font-weight: 600;
            font-size: 16px;
            line-height: 20px;
            color: #fff;
            width: 60%;
            text-align: right;
        }}
        .accordion.open .accordion-body {{
            max-height: 2000px;
        }}
        .accordion.open .accordion-arrow {{
            transform: rotate(180deg);
        }}
        .no-transform * {{ transform: none !important; }}
        @media print {{
            body * {{ visibility: hidden; }}
            #printArea, #printArea * {{ visibility: visible; }}
            #printArea {{ position: absolute; left: 0; top: 0; }}
            .show_print {{ display: block; }}
            .accordion-header .accordion-arrow {{
                transform: none !important;
                transition: none !important;
            }}
        }}
        @media screen and (max-width: 1640px) {{
            .container {{ padding: 0 20px; }}
        }}
        @media screen and (max-width: 990px) {{
            .header {{ margin: 30px 0; }}
            .header_inner {{ padding: 20px 25px; }}
            .request {{
                flex-direction: column;
                align-items: flex-start;
                gap: 18px;
                margin-bottom: 20px;
            }}
            .request1 {{ order: 2; }}
            .result {{ order: 1; }}
            .block1 {{ display: none; }}
            .block2 {{ width: 100%; }}
            .hide_mobile {{ display: none; }}
            .accordion-content .row span {{
                width: 50%;
                text-align: right;
                white-space: wrap;
            }}
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
            .accordion-arrow svg {{ width: 11px; height: 7px; }}
            .accordion-content {{ padding: 22px; }}
            .accordion-content .row {{ margin-bottom: 13px; }}
            .accordion-content .row strong,
            .accordion-content .row span {{ font-size: 10px; line-height: 11px; }}
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
                        <div class="request_number">{data.get('query', '')}</div>
                    </div>
                    <div class="result">
                        <div class="result_text">Количество баз: {total_bases}</div>
                        <div class="result_text">Количество записей: {records_count}</div>
                        <div class="result_number">{views} просмотров</div>
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
                                {structure_items}
                            </div>
                        </div>
                    </div>
                </div>
                <div class="block2 no_transform" id="printArea">
                    <h3 class="block_title hide_mobile show_print">Полный отчёт</h3>
                    {accordions}
                </div>
            </div>
        </div>
    </div>
    <div style="text-align: center; color: rgba(255,255,255,0.15); font-size: 12px; padding: 20px 0; border-top: 1px solid rgba(255,255,255,0.05); margin-top: 30px;">
        Мониторинг uptimerobot.com
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

async def save_id_report(key: str, data: dict):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO id_reports (key, data) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET data = $2, created_at = NOW()
        ''', key, json.dumps(data))

async def get_id_report(key: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT data FROM id_reports WHERE key = $1', key)
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

async def get_unique_views_id(id_str: str, user_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT user_ids FROM id_views WHERE id = $1', id_str)
        if row:
            user_ids = json.loads(row['user_ids']) if row['user_ids'] else []
        else:
            user_ids = []
        if user_id not in user_ids:
            user_ids.append(user_id)
            await conn.execute('''
                INSERT INTO id_views (id, user_ids) VALUES ($1, $2)
                ON CONFLICT (id) DO UPDATE SET user_ids = $2
            ''', id_str, json.dumps(user_ids))
        return len(user_ids)

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

async def check_subscription(user_id: int) -> bool:
    try:
        chat = await bot.get_chat(CHANNEL_USERNAME)
        chat_id = chat.id
        chat_member = await bot.get_chat_member(chat_id, user_id)
        return chat_member.status in ["member", "administrator", "creator"]
    except Exception as e:
        print(f"Ошибка проверки подписки: {e}")
        return False

def get_search_start_message(search_type: str, query: str) -> str:
    type_names = {
        "phone": "номеру телефона",
        "email": "Email",
        "ip": "IP-адресу",
        "vk": "VK",
        "telegram_id": "Telegram ID",
        "inn": "ИНН",
        "fio": "ФИО",
        "unknown": "запросу"
    }
    type_name = type_names.get(search_type, search_type)
    return (
        f"⠛ Поиск по {type_name}:\n\n"
        f"💬 Поиск: {query}\n"
        f"⏱️ Сбор информации...\n\n"
        f"Проверяю базы данных..."
    )

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ===== ФУНКЦИИ ДЛЯ ОПЛАТЫ =====
async def create_crypto_pay_invoice(user_id: int, amount_usd: float, requests_count: int):
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
            response_text = await resp.text()
            print(f"CryptoPay ответ: {response_text}")
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
                    return pay_url, invoice_id
                else:
                    error_msg = data.get('error', {}).get('message', 'Неизвестная ошибка')
                    return None, error_msg
            else:
                return None, f"HTTP {resp.status}: {response_text[:200]}"
    except Exception as e:
        return None, str(e)

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
                f"✅ Оплата USDT подтверждена! Вам начислено {requests} дополнительных запросов.\n"
                f"Теперь у вас доступно {await get_user_available_requests(user_id)} запросов."
            )
        except Exception:
            pass

async def check_payment_status(user_id: int, invoice_id: str) -> dict:
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
                            return {"status": "paid", "message": "✅ Оплачено!"}
                        elif status == 'active':
                            return {"status": "pending", "message": "Счёт ещё не оплачен"}
                        elif status == 'expired':
                            return {"status": "expired", "message": "❌ Счёт истёк. Создайте новый."}
                        elif status == 'cancelled':
                            return {"status": "cancelled", "message": "❌ Счёт отменён."}
                        else:
                            return {"status": "pending", "message": f"⏳ Статус: {status}. Подождите..."}
                    else:
                        return {"status": "not_found", "message": "❌ Счёт не найден."}
                else:
                    error_msg = data.get('error', {}).get('message', 'Ошибка API')
                    return {"status": "error", "message": f"❌ Ошибка CryptoPay: {error_msg}"}
            else:
                return {"status": "error", "message": f"❌ Ошибка HTTP {resp.status}"}
    except Exception as e:
        return {"status": "error", "message": f"❌ Ошибка сети: {str(e)}"}

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
        print(f"Ошибка отправки инвойса Stars: {e}")
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
                f"✅ Оплата Stars подтверждена! Вам начислено {requests} дополнительных запросов.\n"
                f"Теперь у вас доступно {await get_user_available_requests(user_id)} запросов."
            )
        except Exception:
            pass

# ===== ОБРАБОТЧИКИ =====

@dp.message(Command("start"))
async def start_cmd(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username

    args = message.text.split()
    referrer_id = None
    ref_code = None
    if len(args) > 1:
        payload = args[1]
        if payload.startswith("ref_"):
            ref_code = payload[4:]
        elif payload.startswith("ref"):
            ref_code = payload[3:]
        if ref_code:
            pool = await get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow('SELECT user_id FROM users WHERE referral_code = $1', ref_code)
                if row and row['user_id'] != user_id:
                    referrer_id = row['user_id']

    is_subscribed = await check_subscription(user_id)
    if not is_subscribed:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подписаться на канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="Я подписался", callback_data="check_subscription")]
        ])
        await message.reply("Подпишитесь на канал!", reply_markup=keyboard)
        return

    user = await get_user(user_id)
    referral_message = None

    if not user:
        await create_user(user_id, username, referrer_id)
        if referrer_id:
            try:
                ref_user = await get_user(referrer_id)
                if ref_user:
                    ref_username = f"@{username}" if username else f"ID {user_id}"
                    await bot.send_message(referrer_id, f"По вашей ссылке пришёл {ref_username}, вы получили +1 запрос.")
                    referral_message = f"Вы зарегистрировались по ссылке {ref_username}. Реферер получил +1 запрос."
            except Exception:
                pass
    else:
        if referrer_id:
            pool = await get_pool()
            async with pool.acquire() as conn:
                existing = await conn.fetchrow('SELECT * FROM referrals WHERE referrer_id=$1 AND referred_id=$2', referrer_id, user_id)
                if not existing:
                    await conn.execute('INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2)', referrer_id, user_id)
                    await conn.execute('UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = $1', referrer_id)
                    try:
                        ref_user = await get_user(referrer_id)
                        if ref_user:
                            ref_username = f"@{username}" if username else f"ID {user_id}"
                            await bot.send_message(referrer_id, f"{ref_username} активировал вашу ссылку, вы получили +1 запрос.")
                            referral_message = f"Вы активировали ссылку {ref_username}. Реферер получил +1 запрос."
                    except Exception:
                        pass
        ref_code_db = await get_referral_code(user_id)
        if not ref_code_db:
            await generate_referral_code(user_id)

    keyboard_info = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Информация", url=CHANNEL_LINK)]
    ])
    msg_link = await message.reply(
        "🔮 *Вечная ссылка на информацию:*\nЕсли удалят этого бота — то новую ссылку на него найдёте по кнопке ниже.",
        reply_markup=keyboard_info,
        parse_mode="Markdown"
    )
    try:
        await bot.pin_chat_message(message.chat.id, msg_link.message_id, disable_notification=True)
    except Exception:
        pass

    main_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")]
    ])
    text = """🕵️ dataseeker — твой бесплатный цифровой детектив.

Типы поиска:

┌ Соцсети:
├ Telegram → id 12345
└ VK → vk.com/id1234567

┌ Контакты:
├ Телефон → +79999999999
└ Email → ivanov@gmail.com

┌ Онлайн-следы:
└ IP → 185.85.219.243

┌ Физ. лица:
├ ИНН → /inn 123456789012
└ ФИО → Иванов Иван Иванович

Каждые 24 часа выдаётся по 5 бесплатных запросов."""
    await message.reply(text, parse_mode="Markdown", reply_markup=main_keyboard)
    if referral_message:
        await message.reply(referral_message)

@dp.callback_query(lambda c: c.data == "my_profile")
async def my_profile_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    if not user:
        await callback.answer("Не зарегистрированы.")
        return
    available = await get_user_available_requests(user_id)
    text = f"**Ваш профиль**\nID: {user_id}\nДоступно запросов: {available}\nДата регистрации: {user['created_at'].strftime('%d.%m.%Y %H:%M')}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ввести промокод", callback_data="enter_promo")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
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
    text = "Выбери тариф, как настоящий сшивчик:\n\nЧем больше пакет — тем дешевле каждый запрос. Экономия до 70%."
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("pkg_"))
async def package_selected_callback(callback: CallbackQuery):
    parts = callback.data.split("_")
    requests_count = int(parts[1])
    amount_usd = float(parts[2])
    stars_price = int(parts[3])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Оплатить звёздами [${stars_price}]", callback_data=f"pay_stars_{requests_count}_{stars_price}")],
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
        "Ссылка на оплату успешно сгенерирована:\n"
        "Оплатите счёт, нажав на кнопку ниже.",
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
    
    pay_url, invoice_id = await create_crypto_pay_invoice(user_id, amount_usd, requests_count)
    if not pay_url:
        await callback.message.edit_text(
            "Ошибка создания счёта USDT. Попробуйте позже.",
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
            "Счёт ещё не оплачен",
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
    text = """🕵️ dataseeker — твой бесплатный цифровой детектив.

Типы поиска:

┌ Соцсети:
├ Telegram → id 12345
└ VK → vk.com/id1234567

┌ Контакты:
├ Телефон → +79999999999
└ Email → ivanov@gmail.com

┌ Онлайн-следы:
└ IP → 185.85.219.243

┌ Физ. лица:
├ ИНН → /inn 123456789012
└ ФИО → Иванов Иван Иванович

Каждые 24 часа выдаётся по 5 бесплатных запросов."""
    await message.answer(text, parse_mode="Markdown", reply_markup=main_keyboard)

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu_callback(callback: CallbackQuery):
    main_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")]
    ])
    text = """🕵️ dataseeker — твой бесплатный цифровой детектив.

Типы поиска:

┌ Соцсети:
├ Telegram → id 12345
└ VK → vk.com/id1234567

┌ Контакты:
├ Телефон → +79999999999
└ Email → ivanov@gmail.com

┌ Онлайн-следы:
└ IP → 185.85.219.243

┌ Физ. лица:
├ ИНН → /inn 123456789012
└ ФИО → Иванов Иван Иванович

Каждые 24 часа выдаётся по 5 бесплатных запросов."""
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=main_keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "check_subscription")
async def check_subscription_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    is_subscribed = await check_subscription(user_id)
    if is_subscribed:
        await callback.message.delete()
        await start_cmd(callback.message)
    else:
        await callback.answer("Не подписались!", show_alert=True)

# ===== АДМИН-ПАНЕЛЬ =====
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
        await callback.message.edit_text(f"Статистика\nПользователей: {users}\nОтчётов: {reports}\nПромокодов: {promos}\nПлатежей: {payments}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_back")]
        ]))
    elif action == "back":
        await callback.message.edit_text("Админ-панель", reply_markup=get_admin_keyboard())
    elif action == "close":
        await callback.message.delete()
    elif action == "create_promo":
        await callback.message.edit_text("Введите промокод:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_back")]
        ]))
        if state:
            await state.set_state(PromoCreation.waiting_for_code)
    elif action == "broadcast":
        await callback.message.edit_text("Введите текст рассылки:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_back")]
        ]))
        if state:
            await state.set_state(Broadcast.waiting_for_text)
    elif action == "give":
        await callback.message.edit_text("Введите ID пользователя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_back")]
        ]))
        if state:
            await state.set_state(GiveRequests.waiting_for_user_id)
    elif action == "list_promo":
        promos = await get_all_promo_codes()
        if not promos:
            text = "Нет промокодов."
        else:
            text = "Промокоды:\n" + "\n".join([f"`{p['code']}` — {p['used_count']}/{p['max_uses']}" for p in promos])
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_back")]
        ]))
    elif action == "delete_promo":
        promos = await get_all_promo_codes()
        if not promos:
            await callback.message.edit_text("Нет промокодов для удаления.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад", callback_data="admin_back")]
            ]))
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[])
            for p in promos:
                keyboard.inline_keyboard.append([InlineKeyboardButton(text=f"❌ {p['code']}", callback_data=f"delpromo_{p['code']}")])
            keyboard.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="admin_back")])
            await callback.message.edit_text("Выберите промокод для удаления:", reply_markup=keyboard)
    elif action == "payments":
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM purchases ORDER BY created_at DESC LIMIT 10')
        if not rows:
            text = "Нет платежей."
        else:
            text = "Последние платежи:\n" + "\n".join([f"{p['user_id']} — {p['requests']} запросов, {p['amount']} {p['currency']}, {p['status']}" for p in rows])
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="admin_back")]
        ]))
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

# ===== УНИВЕРСАЛЬНЫЙ ПОИСК =====
@dp.message(lambda msg: msg.text and not msg.text.startswith('/'))
async def universal_handler(message: Message):
    text = message.text.strip()
    if not text:
        return
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
    search_type = "unknown"
    query = text
    cleaned = re.sub(r'\s+', '', text)
    
    # Проверка на Telegram ID (формат: "id 12345" или просто число от 5 до 10 цифр)
    id_match = re.match(r'^id\s+(\d{5,10})$', text, re.IGNORECASE)
    if id_match:
        search_type = "telegram_id"
        query = id_match.group(1)
    elif re.match(r'^\d{5,10}$', cleaned):
        search_type = "telegram_id"
        query = cleaned
    elif re.match(r'^\+?\d{10,15}$', cleaned):
        query = re.sub(r'\D', '', cleaned)
        search_type = "phone"
    elif re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', text):
        search_type = "email"
    elif re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', text):
        search_type = "ip"
    elif re.match(r'^https?://(www\.)?vk\.com/', text):
        search_type = "vk"
    elif re.search(r'[А-Яа-я]+\s+[А-Яа-я]+\s+[А-Яа-я]+', text):
        search_type = "fio"
    
    status_msg = await message.reply(get_search_start_message(search_type, query))
    
    if search_type == "phone":
        data = await collect_phone_data(query)
        views = await get_unique_views_phone(query, user_id)
        await save_report(query, data)
        report = format_phone_report(data, views)
        html_content = generate_html_report(data, views, "Отчёт по номеру телефона")
        html_bytes = html_content.encode('utf-8')
        html_file = BufferedInputFile(html_bytes, filename=f"phone_report_{query}.html")
        buttons = [
            [InlineKeyboardButton(text="Полный отчёт (HTML)", callback_data=f"html_phone_{query}")]
        ]
        if query:
            wa_url = f"https://wa.me/{query}"
            buttons.append([InlineKeyboardButton(text="WhatsApp", url=wa_url)])
        tg_usernames = data.get('telegrams', [])
        tg_link = None
        for tg in tg_usernames:
            if isinstance(tg, str) and tg.startswith('@'):
                tg_link = f"https://t.me/{tg[1:]}"
                break
        if tg_link:
            buttons.append([InlineKeyboardButton(text="Telegram", url=tg_link)])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        await status_msg.edit_text(escape_md(report), parse_mode="MarkdownV2", reply_markup=keyboard)
    
    elif search_type == "telegram_id":
        data = await collect_telegram_data(query)
        report = format_telegram_report(data)
        await status_msg.edit_text(escape_md(report), parse_mode="MarkdownV2")
    
    else:
        await status_msg.edit_text("Неизвестный тип запроса.")
        return
    await use_request(user_id)

@dp.callback_query(lambda c: c.data and c.data.startswith("html_phone_"))
async def html_phone_callback(callback: CallbackQuery):
    query = callback.data.replace("html_phone_", "")
    data = await get_report(query)
    if not data:
        await callback.answer("Отчёт не найден.")
        return
    views = await get_unique_views_phone(query, callback.from_user.id)
    html_content = generate_html_report(data, views, "Отчёт по номеру телефона")
    html_bytes = html_content.encode('utf-8')
    html_file = BufferedInputFile(html_bytes, filename=f"phone_report_{query}.html")
    await callback.message.reply_document(html_file, caption="Полный HTML-отчёт")
    await callback.answer()

# ===== ВЕБХУК =====
async def crypto_pay_webhook(request):
    try:
        data = await request.json()
        print(f"Webhook: {data}")
        if data.get('payload', {}).get('status') == 'paid':
            invoice_id = data['payload']['invoice_id']
            await process_crypto_pay_payment(invoice_id)
        return web.Response(text="OK")
    except Exception as e:
        print(f"Webhook error: {e}")
        return web.Response(text="OK", status=200)

async def health_check(request):
    return web.Response(text="OK")

# ===== MAIN =====
async def main():
    await asyncio.sleep(2)
    await init_db()
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    app.router.add_get("/webhook", lambda req: web.Response(text="Webhook OK"))
    app.router.add_post("/webhook", crypto_pay_webhook)
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Бот запущен на порту {port}")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await asyncio.sleep(1)
        await dp.start_polling(
            bot,
            skip_updates=True,
            allowed_updates=["message", "callback_query", "pre_checkout_query"]
        )
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await runner.cleanup()
        await bot.session.close()
        if db_pool:
            await db_pool.close()
        if http_session:
            await http_session.close()

if __name__ == "__main__":
    asyncio.run(main())
