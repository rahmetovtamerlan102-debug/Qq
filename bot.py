import os
import asyncio
import re
import json
import asyncpg
import aiohttp
from aiohttp import web
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не задан")

BIGBASE_TOKEN = os.getenv("BIGBASE_TOKEN")
NIGHTSEARCH_API_KEY = os.getenv("NIGHTSEARCH_API_KEY")
SEON_API_KEY = os.getenv("SEON_API_KEY")
SNUSBASE_API_KEY = os.getenv("SNUSBASE_API_KEY")
DEPSEARCH_TOKEN = os.getenv("DEPSEARCH_TOKEN")
DEPSEARCH_BASE = os.getenv("DEPSEARCH_BASE", "https://api.depsearch.sbs")
JITLER_TOKENS_STR = os.getenv("JITLER_TOKENS", "")
JITLER_TOKENS = [t.strip() for t in JITLER_TOKENS_STR.split(",") if t.strip()]

TELELOG_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiIyMDMzMDI5NDc1IiwianRpIjoiODJmMjlmNzQtYmJlMi00ZGUwLWEwZDQtN2EzMDJhMWE5MDViIiwiZXhwIjoxODAxMDA4MzM4fQ.Mba4aX85YAMcaMLfhUBzXtCoNmEujfMe-6sGBbp3kT-T2SiLM_Ho0BBAFAQ8_C6Gz06PH9mAYhfBvlLSjb4oVd1Fm_vmb8MC-wuObU3qgfGrYdGzVF3ntJHv-LdNELq-jsqvQOY3jq9meso9dUoyj5SviDQWL6cvnRQ03kpHWxA"

db_pool = None

async def get_pool():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return db_pool

# ===== БАЛАНСИРОВЩИК JITLER =====
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

# ===== ИНИЦИАЛИЗАЦИЯ БД =====
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                referral_code TEXT UNIQUE,
                referred_by BIGINT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bots (
                bot_id SERIAL PRIMARY KEY,
                owner_id BIGINT REFERENCES users(user_id),
                bot_token TEXT UNIQUE,
                bot_username TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
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
            CREATE TABLE IF NOT EXISTS referrals (
                referrer_id BIGINT REFERENCES users(user_id),
                referred_id BIGINT REFERENCES users(user_id),
                bonus_given BOOLEAN DEFAULT FALSE,
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

# ===== ОСНОВНЫЕ ФУНКЦИИ БОТА (CRUD) =====
async def get_user(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
    return row

async def create_user(user_id: int, username: str = None, referred_by: int = None):
    pool = await get_pool()
    ref_code = f"REF{user_id}{datetime.now().strftime('%m%d')}"
    async with pool.acquire() as conn:
        if referred_by:
            await conn.execute('INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2)', referred_by, user_id)
        await conn.execute('''
            INSERT INTO users (user_id, username, referral_code, referred_by)
            VALUES ($1, $2, $3, $4)
        ''', user_id, username, ref_code, referred_by)

async def get_referral_code(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT referral_code FROM users WHERE user_id = $1', user_id)
    return row['referral_code'] if row else None

async def get_referral_stats(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT referred_id FROM referrals WHERE referrer_id = $1', user_id)
    return len(rows)

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

async def register_bot(owner_id: int, bot_token: str, bot_username: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO bots (owner_id, bot_token, bot_username) VALUES ($1, $2, $3)
        ''', owner_id, bot_token, bot_username)

async def get_user_bots(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM bots WHERE owner_id = $1', user_id)
    return rows

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

# ===== API ЗАПРОСЫ =====
async def bigbase_search(query: str):
    url = "https://bigbase.top/api/search"
    headers = {"Authorization": BIGBASE_TOKEN, "Content-Type": "application/json"}
    payload = {"search": query, "page": 0}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
    except Exception as e:
        print(f"❌ BIGBASE ERROR: {repr(e)}")
        return {}

async def nightsearch_search(query: str):
    if not NIGHTSEARCH_API_KEY:
        return {}
    url = "https://nightsearch.life/api/search"
    headers = {"X-API-Key": NIGHTSEARCH_API_KEY, "Content-Type": "application/json"}
    payload = {"query": query, "search_type": "phone"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
    except Exception:
        return {}

async def seon_search(query: str):
    if not SEON_API_KEY:
        return {}
    url = "https://api.seon.io/SeonRestService/phone-api/v2"
    headers = {"X-API-KEY": SEON_API_KEY, "Content-Type": "application/json"}
    payload = {"phone": query}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
    except Exception:
        return {}

async def snusbase_search(query: str):
    if not SNUSBASE_API_KEY:
        return {}
    url = "https://api.snusbase.com/data/search"
    headers = {"Auth": SNUSBASE_API_KEY, "Content-Type": "application/json"}
    payload = {"terms": [query], "types": ["email", "username", "phone", "name"], "wildcard": False}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
    except Exception:
        return {}

async def depsearch_search(query: str):
    if not DEPSEARCH_TOKEN or not DEPSEARCH_BASE:
        return {}
    url = f"{DEPSEARCH_BASE}/quest={query}&token={DEPSEARCH_TOKEN}&lang=ru"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
    except Exception:
        return {}

async def jitler_search_with_balancer(query: str, search_type: str = "number"):
    create_timeout = aiohttp.ClientTimeout(total=5)
    poll_timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession() as session:
        for attempt in range(len(JITLER_TOKENS) * 2):
            token = await balancer.get_token()
            if not token:
                return {}
            url = "https://api.jitler.top/search"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            payload = {"type": search_type, "query": query, "page": 1}
            try:
                async with session.post(url, json=payload, headers=headers, timeout=create_timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('result'):
                            if 'id' in data:
                                task_id = data['id']
                                for _ in range(10):
                                    await asyncio.sleep(0.5)
                                    get_url = f"https://api.jitler.top/search/{task_id}"
                                    try:
                                        async with session.get(get_url, headers=headers, timeout=poll_timeout) as get_resp:
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
                            else:
                                return {}
                        else:
                            return {}
                    elif resp.status == 429:
                        balancer.mark_failed(token)
                        continue
                    else:
                        return {}
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue
    return {}

# ===== TELELOG API =====
async def telelog_get_names(user_id: str):
    if not TELELOG_TOKEN:
        return []
    url = f"https://telelog.info/api/v1/users/{user_id}/names"
    headers = {"accept": "text/plain", "Authorization": f"Bearer {TELELOG_TOKEN}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('success'):
                        return data.get('data', [])
                return []
    except Exception as e:
        print(f"❌ TeleLog names error: {repr(e)}")
        return []

async def telelog_get_usernames(user_id: str):
    if not TELELOG_TOKEN:
        return []
    url = f"https://telelog.info/api/v1/users/{user_id}/usernames"
    headers = {"accept": "text/plain", "Authorization": f"Bearer {TELELOG_TOKEN}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('success'):
                        return data.get('data', [])
                return []
    except Exception as e:
        print(f"❌ TeleLog usernames error: {repr(e)}")
        return []

async def telelog_get_gifts(user_id: str):
    if not TELELOG_TOKEN:
        return []
    url = f"https://telelog.info/api/v1/users/{user_id}/gifts_relation"
    headers = {"accept": "text/plain", "Authorization": f"Bearer {TELELOG_TOKEN}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('success'):
                        return data.get('data', [])
                return []
    except Exception as e:
        print(f"❌ TeleLog gifts error: {repr(e)}")
        return []

# ===== ОПРЕДЕЛЕНИЕ ТИПА ЗАПРОСА =====
def detect_query_type(text: str) -> str:
    text = text.strip()
    if not text:
        return "unknown"
    # Номер телефона (с + или без)
    if re.match(r'^\+?\d{10,15}$', re.sub(r'\s+', '', text)):
        return "phone"
    # Email
    if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', text):
        return "email"
    # VIN (17 символов, буквы и цифры)
    if re.match(r'^[A-HJ-NPR-Z0-9]{17}$', text.upper()):
        return "vin"
    # Госномер (буквы+цифры+буквы, 6-9 символов)
    if re.match(r'^[A-ZА-Я]\d{3}[A-ZА-Я]{2}\d{2,3}$', text.upper(), re.IGNORECASE):
        return "car_number"
    # IP-адрес
    if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', text):
        return "ip"
    # Домен (содержит точку, не IP)
    if '.' in text and not text.startswith(('http://', 'https://')):
        return "domain"
    # Кадастровый номер (цифры:цифры:цифры)
    if re.match(r'^\d+:\d+:\d+$', text):
        return "cadastre"
    # ОГРН (13 или 15 цифр)
    if re.match(r'^\d{13,15}$', text):
        return "ogrn"
    # СНИЛС (11 цифр с или без дефисов)
    if re.match(r'^\d{3}-\d{3}-\d{3} \d{2}$', text) or re.match(r'^\d{11}$', text):
        return "snils"
    # Водительские права (10 цифр)
    if re.match(r'^\d{10}$', text):
        return "driver_license"
    # Паспорт (серия+номер, 10 цифр)
    if re.match(r'^\d{10}$', text):
        return "passport"
    # ИНН (10 или 12 цифр)
    if re.match(r'^\d{10}$', text) or re.match(r'^\d{12}$', text):
        return "inn"
    # Если строка содержит @ - Telegram username
    if text.startswith('@') or 't.me/' in text or 'tg://' in text:
        return "telegram"
    # URL соцсети
    if re.match(r'^https?://(www\.)?(vk\.com|ok\.ru|instagram\.com|tiktok\.com)/', text):
        return "social"
    # Поиск по личности (ФИО и дата)
    if re.search(r'[А-Яа-я]+\s+[А-Яа-я]+\s+[А-Яа-я]+', text) or re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{4}', text):
        return "person"
    # Тег (слово)
    if text.isalpha():
        return "tag"
    return "unknown"

# ===== УНИВЕРСАЛЬНЫЙ СБОР ДАННЫХ =====
async def collect_universal_data(query: str, search_type: str):
    print(f"🔍 Универсальный поиск: {query}, тип: {search_type}")
    tasks = []
    # Всегда добавляем BigBase, DepSearch, NightSearch, SEON, Snusbase
    tasks.append(bigbase_search(query))
    tasks.append(depsearch_search(query))
    tasks.append(nightsearch_search(query))
    tasks.append(seon_search(query))
    tasks.append(snusbase_search(query))
    # Jitler добавляем для некоторых типов
    if search_type in ("phone", "telegram", "person", "tag", "social"):
        tasks.append(jitler_search_with_balancer(query, "sherlock"))
    else:
        tasks.append(asyncio.sleep(0, result={}))  # заглушка
    
    # TeleLog только для Telegram ID
    if search_type == "telegram" and query.isdigit():
        tasks.append(telelog_get_names(query))
        tasks.append(telelog_get_usernames(query))
        tasks.append(telelog_get_gifts(query))
    else:
        tasks.append(asyncio.sleep(0, result=[]))
        tasks.append(asyncio.sleep(0, result=[]))
        tasks.append(asyncio.sleep(0, result=[]))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    bigbase = results[0] if isinstance(results[0], dict) else {}
    depsearch = results[1] if isinstance(results[1], dict) else {}
    nightsearch = results[2] if isinstance(results[2], dict) else {}
    seon = results[3] if isinstance(results[3], dict) else {}
    snusbase = results[4] if isinstance(results[4], dict) else {}
    jitler = results[5] if isinstance(results[5], dict) else {}
    telelog_names = results[6] if isinstance(results[6], list) else []
    telelog_usernames = results[7] if isinstance(results[7], list) else []
    telelog_gifts = results[8] if isinstance(results[8], list) else []
    
    result = {
        'query': query,
        'telegrams': [],
        'groups': [],
        'name_history': [],
        'username_history': [],
        'phone_books': [],
        'gifts': [],
        'vk': None,
        'instagram': None,
        'tiktok': None,
        'ok': None,
        'raw_data': [],
        'emails': [],
        'phones': []
    }
    
    # Сбор сырых данных
    for src, name in [(bigbase, "BigBase"), (depsearch, "DepSearch"), (nightsearch, "NightSearch"), (seon, "SEON"), (snusbase, "Snusbase"), (jitler, "Jitler")]:
        if src:
            result['raw_data'].append(f"{name}: {json.dumps(src, ensure_ascii=False)[:300]}")
    
    # Обработка BigBase
    if bigbase:
        records = bigbase.get('records', [])
        for record in records:
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip().lower()
                    value = item[1]
                    if 'актуальный username' in key or 'username' in key:
                        if value:
                            result['telegrams'].append(str(value).strip())
                    elif 'группы' in key:
                        if isinstance(value, list):
                            for group_item in value:
                                if isinstance(group_item, list):
                                    group_name = None
                                    group_username = None
                                    for group_data in group_item:
                                        if isinstance(group_data, list) and len(group_data) >= 2:
                                            sub_key = str(group_data[0]).strip().lower()
                                            sub_value = group_data[1]
                                            if 'название' in sub_key:
                                                group_name = str(sub_value).strip()
                                            elif 'username' in sub_key:
                                                group_username = str(sub_value).strip()
                                    if group_name:
                                        if group_username:
                                            if not group_username.startswith('@'):
                                                group_username = '@' + group_username
                                            result['groups'].append(f"{group_name}: {group_username}")
                                        else:
                                            result['groups'].append(group_name)
                    elif 'почта' in key or 'email' in key:
                        if value:
                            result['emails'].append(str(value))
                    elif 'телефон' in key or 'phone' in key:
                        if value:
                            result['phones'].append(str(value))
                    elif 'история имён' in key:
                        if isinstance(value, list):
                            for name_list in value:
                                if isinstance(name_list, list):
                                    for name_item in name_list:
                                        if isinstance(name_item, list) and len(name_item) >= 2:
                                            name = str(name_item[1]).strip()
                                            if name:
                                                result['name_history'].append({'date': None, 'name': name})
    
    # Обработка Jitler
    if jitler:
        jitler_data = jitler.get('response', {})
        phonebooks = jitler_data.get('phonebooks', [])
        if phonebooks:
            result['phone_books'] = list(dict.fromkeys(phonebooks))
        profiles = jitler_data.get('profiles', {})
        if profiles.get('vk'):
            vk_urls = [p.get('url') for p in profiles['vk'] if p.get('url')]
            if vk_urls:
                result['vk'] = vk_urls[0]
        if profiles.get('ok'):
            ok_urls = [p.get('url') for p in profiles['ok'] if p.get('url')]
            if ok_urls:
                result['ok'] = ok_urls[0]
        if profiles.get('instagram'):
            inst_urls = [p.get('url') for p in profiles['instagram'] if p.get('url')]
            if inst_urls:
                result['instagram'] = inst_urls[0]
        if profiles.get('tiktok'):
            tt_urls = [p.get('url') for p in profiles['tiktok'] if p.get('url')]
            if tt_urls:
                result['tiktok'] = tt_urls[0]
        telegrams = jitler_data.get('telegram', [])
        for tg in telegrams:
            formatted = extract_telegram(tg)
            if formatted:
                result['telegrams'].append(formatted)
    
    # TeleLog
    try:
        target_id = int(query) if query.isdigit() else None
    except:
        target_id = None
    for name_entry in telelog_names:
        if isinstance(name_entry, dict):
            name = name_entry.get('name')
            if name:
                date = name_entry.get('date_time') or name_entry.get('date')
                date_str = None
                if date:
                    try:
                        dt = datetime.fromisoformat(str(date).replace('Z', '+00:00'))
                        date_str = dt.strftime('%d.%m.%Y %H:%M')
                    except:
                        date_str = str(date)[:10]
                result['name_history'].append({'date': date_str, 'name': str(name)})
    for username_entry in telelog_usernames:
        if isinstance(username_entry, dict):
            username = username_entry.get('name')
            if username:
                username = str(username).strip()
                if not username.startswith('@'):
                    username = '@' + username
                date = username_entry.get('date_time') or username_entry.get('date')
                date_str = None
                if date:
                    try:
                        dt = datetime.fromisoformat(str(date).replace('Z', '+00:00'))
                        date_str = dt.strftime('%d.%m.%Y %H:%M')
                    except:
                        date_str = str(date)[:10]
                result['username_history'].append({'date': date_str, 'username': username})
    for gift in telelog_gifts:
        if isinstance(gift, dict):
            from_user = gift.get('from_mainUsername') or gift.get('from_first_name') or f"ID {gift.get('from_user_id')}"
            to_user = gift.get('to_mainUsername') or gift.get('to_first_name') or f"ID {gift.get('to_user_id')}"
            date = gift.get('last_gift_date')
            date_str = "?"
            if date:
                try:
                    dt = datetime.fromisoformat(str(date).replace('Z', '+00:00'))
                    date_str = dt.strftime('%d.%m.%Y')
                except:
                    date_str = str(date)[:10]
            if target_id is not None and gift.get('from_user_id') == target_id:
                gift_str = f"📤 {date_str} → {to_user}"
            elif target_id is not None and gift.get('to_user_id') == target_id:
                gift_str = f"📥 {date_str} ← {from_user}"
            else:
                gift_str = f"{date_str}: {from_user} → {to_user}"
            result['gifts'].append(gift_str)
    
    # Нормализация
    result['telegrams'] = list(dict.fromkeys([x for x in result['telegrams'] if x]))
    result['groups'] = list(dict.fromkeys([x for x in result['groups'] if x]))
    result['phone_books'] = list(dict.fromkeys([x for x in result['phone_books'] if x]))
    result['gifts'] = list(dict.fromkeys([x for x in result['gifts'] if x]))
    result['emails'] = list(dict.fromkeys([x for x in result['emails'] if x]))
    result['phones'] = list(dict.fromkeys([x for x in result['phones'] if x]))
    
    # Удаляем дубликаты в истории
    seen_names = set()
    unique_name_history = []
    for entry in result['name_history']:
        key = entry.get('name', '')
        if key and key not in seen_names:
            seen_names.add(key)
            unique_name_history.append(entry)
    result['name_history'] = unique_name_history
    
    seen_usernames = set()
    unique_username_history = []
    for entry in result['username_history']:
        key = entry.get('username', '')
        if key and key not in seen_usernames:
            seen_usernames.add(key)
            unique_username_history.append(entry)
    result['username_history'] = unique_username_history
    
    print(f"📊 Универсальный результат: телеграм={len(result['telegrams'])}, группы={len(result['groups'])}, история={len(result['name_history'])}, книги={len(result['phone_books'])}")
    return result

# ===== ФОРМАТТЕР ОТЧЁТА =====
def format_universal_report(data: dict, views: int) -> str:
    lines = []
    if data.get('query'):
        lines.append(f"🔍 Запрос: {data['query']}")
        lines.append("")
    
    if data.get('phone_books'):
        lines.append(f"📱 Телефонные книги: {', '.join(data['phone_books'][:15])}")
        lines.append("")
    
    if data.get('vk'):
        lines.append(f"🧑‍💻 Вконтакте: {data['vk']}")
    if data.get('ok'):
        lines.append(f"👨‍🦳 Одноклассники: {data['ok']}")
    if data.get('instagram'):
        lines.append(f"📷 Instagram: {data['instagram']}")
    if data.get('tiktok'):
        lines.append(f"👩‍🦲 TikTok: {data['tiktok']}")
    
    if data.get('emails'):
        lines.append(f"📧 E-mail: {', '.join(data['emails'])}")
    if data.get('phones'):
        lines.append(f"📞 Телефоны: {', '.join(data['phones'])}")
    
    if data.get('telegrams'):
        lines.append(f"💬 Telegram: {', '.join(data['telegrams'])}")
        lines.append("")
    
    if data.get('groups'):
        lines.append(f"👥 Группы [{len(data['groups'])}]:")
        for group in data['groups'][:15]:
            lines.append(f"  {group}")
        lines.append("")
    
    if data.get('name_history'):
        lines.append("🕓 История изменения имени:")
        for entry in data['name_history'][:10]:
            if isinstance(entry, dict):
                date = entry.get('date')
                name = entry.get('name', '')
                if date:
                    lines.append(f"  {date} → {name}")
                else:
                    lines.append(f"  → {name}")
        lines.append("")
    
    if data.get('username_history'):
        lines.append("🕓 История изменения юзернейма:")
        for entry in data['username_history'][:10]:
            if isinstance(entry, dict):
                date = entry.get('date')
                username = entry.get('username', '')
                if date:
                    lines.append(f"  {date} → {username}")
                else:
                    lines.append(f"  → {username}")
        lines.append("")
    
    if data.get('gifts'):
        lines.append("🎁 Подарочные связи:")
        for gift in data['gifts'][:10]:
            lines.append(f"  {gift}")
        lines.append("")
    
    if data.get('raw_data'):
        lines.append("📄 Сырые данные API:")
        for raw in data['raw_data'][:3]:
            lines.append(f"  {raw[:200]}...")
        lines.append("")
    
    lines.append(f"👁 Интересовались этим: {views}")
    return "\n".join(lines)

# ===== ФУНКЦИИ ДЛЯ КОМАНД =====
async def process_command(message: types.Message, command: str, arg: str):
    # Определяем тип команды и запускаем соответствующий поиск
    search_type_map = {
        'inn': 'inn',
        'passport': 'passport',
        'snils': 'snils',
        'vu': 'driver_license',
        'car': 'car_number',
        'domain': 'domain',
        'adr': 'cadastre',
        'ogrn': 'ogrn',
        'person': 'person',
        'tag': 'tag',
        'social': 'social'
    }
    search_type = search_type_map.get(command, 'unknown')
    if not search_type or not arg:
        await message.reply("❌ Неправильный формат команды. Пример: /inn 123456789012")
        return
    
    status = await message.reply(f"🔍 Поиск по {command}...")
    data = await collect_universal_data(arg, search_type)
    views = await get_unique_views_id(arg, message.from_user.id) if arg.isdigit() else 0
    report = format_universal_report(data, views)
    await status.edit_text(report)

# ===== БОТ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    args = message.text.split()
    referrer_id = None
    if len(args) > 1:
        payload = args[1]
        if payload.startswith("ref_"):
            ref_code = payload.replace("ref_", "")
            pool = await get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow('SELECT user_id FROM users WHERE referral_code = $1', ref_code)
            if row:
                referrer_id = row['user_id']
    user = await get_user(message.from_user.id)
    if not user:
        await create_user(message.from_user.id, message.from_user.username, referrer_id)
        await message.reply("✅ Вы зарегистрированы!")
    else:
        await message.reply("👋 Добро пожаловать обратно!")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🤖 Мои боты", callback_data="my_bots")],
        [InlineKeyboardButton(text="🤝 Партнёрская программа", callback_data="referral")]
    ])
    await message.reply("Отправьте текст для поиска или используйте команды:\n"
                        "/inn <ИНН>\n/passport <серия+номер>\n/snils <СНИЛС>\n/vu <номер прав>\n"
                        "/car <госномер/VIN>\n/domain <домен/IP>\n/adr <адрес/кадастр>\n"
                        "/ogrn <ОГРН>\n/person <ФИО дата>\n/tag <тег>\n/social <ссылка>\n"
                        "Или просто введите номер телефона, email, ID Telegram, ссылку и т.д.", reply_markup=keyboard)

@dp.message(Command("inn"))
async def inn_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите ИНН: /inn 123456789012")
        return
    await process_command(message, "inn", args[1].strip())

@dp.message(Command("passport"))
async def passport_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите серию и номер паспорта (10 цифр): /passport 1234567890")
        return
    await process_command(message, "passport", args[1].strip())

@dp.message(Command("snils"))
async def snils_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите СНИЛС: /snils 12345678901")
        return
    await process_command(message, "snils", args[1].strip())

@dp.message(Command("vu"))
async def vu_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите номер водительских прав: /vu 1234567890")
        return
    await process_command(message, "vu", args[1].strip())

@dp.message(Command("car"))
async def car_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите госномер или VIN: /car A123BC77")
        return
    await process_command(message, "car", args[1].strip())

@dp.message(Command("domain"))
async def domain_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите домен или IP: /domain example.com")
        return
    await process_command(message, "domain", args[1].strip())

@dp.message(Command("adr"))
async def adr_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите адрес или кадастровый номер: /adr Москва, ул. Пушкина, 1")
        return
    await process_command(message, "adr", args[1].strip())

@dp.message(Command("ogrn"))
async def ogrn_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите ОГРН: /ogrn 1234567890123")
        return
    await process_command(message, "ogrn", args[1].strip())

@dp.message(Command("person"))
async def person_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите ФИО и дату рождения: /person Иванов Иван Иванович 01.01.1990")
        return
    await process_command(message, "person", args[1].strip())

@dp.message(Command("tag"))
async def tag_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите тег: /tag хирург")
        return
    await process_command(message, "tag", args[1].strip())

@dp.message(Command("social"))
async def social_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите ссылку на соцсеть: /social https://vk.com/durov")
        return
    await process_command(message, "social", args[1].strip())

@dp.message(lambda msg: msg.text and not msg.text.startswith('/'))
async def universal_handler(message: types.Message):
    text = message.text.strip()
    if not text:
        return
    # Определяем тип запроса
    query_type = detect_query_type(text)
    if query_type == "phone":
        # Обрабатываем как номер телефона
        digits = re.sub(r'\D', '', text)
        status = await message.reply("🔍 Поиск по телефону...")
        data = await collect_phone_data(digits)
        views = await get_unique_views_phone(digits, message.from_user.id)
        report = format_phone_report(data, views)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📲 Telegram", url=f"tg://resolve?phone={digits}"),
             InlineKeyboardButton(text="💬 WhatsApp", url=f"https://wa.me/{digits}")]
        ])
        await status.edit_text(report, reply_markup=keyboard)
        return
    elif query_type in ("email", "vin", "car_number", "ip", "domain", "cadastre", "ogrn", "snils", "driver_license", "passport", "inn", "person", "tag", "social", "telegram"):
        # Для всех остальных типов запускаем универсальный поиск
        status = await message.reply(f"🔍 Поиск по {query_type}...")
        data = await collect_universal_data(text, query_type)
        views = await get_unique_views_id(text, message.from_user.id) if text.isdigit() else 0
        report = format_universal_report(data, views)
        await status.edit_text(report)
    else:
        # Если тип не распознан, пробуем универсальный поиск
        status = await message.reply("🔍 Универсальный поиск...")
        data = await collect_universal_data(text, "unknown")
        views = await get_unique_views_id(text, message.from_user.id) if text.isdigit() else 0
        report = format_universal_report(data, views)
        await status.edit_text(report)

# ===== КЕШ И КНОПКИ =====
@dp.callback_query(lambda c: c.data == "profile")
async def profile_callback(callback: types.CallbackQuery):
    ref_code = await get_referral_code(callback.from_user.id)
    ref_count = await get_referral_stats(callback.from_user.id)
    text = (
        f"👤 **Мой профиль**\n"
        f"Баланс запросов: ♾️ Бесконечно\n"
        f"Доступно сегодня: ♾️\n"
        f"Реферальный код: `{ref_code}`\n"
        f"Приглашено друзей: {ref_count}"
    )
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "my_bots")
async def my_bots_callback(callback: types.CallbackQuery):
    bots = await get_user_bots(callback.from_user.id)
    if not bots:
        text = "У вас нет созданных зеркал.\nЧтобы создать зеркало, отправьте токен своего бота командой:\n`/addbot TOKEN`"
    else:
        text = "🤖 **Ваши боты-зеркала:**\n"
        for bot_row in bots:
            text += f"- @{bot_row['bot_username']} (токен: `{bot_row['bot_token'][:10]}...`)\n"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить бота", callback_data="add_bot")]
    ])
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "referral")
async def referral_callback(callback: types.CallbackQuery):
    ref_code = await get_referral_code(callback.from_user.id)
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{ref_code}"
    text = (
        "🤝 Партнёрская программа\n\n"
        "Приглашайте друзей и получайте бонусы!\n\n"
        f"Ваша реферальная ссылка:\n{link}"
    )
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "add_bot")
async def add_bot_callback(callback: types.CallbackQuery):
    await callback.message.reply("Отправьте токен вашего бота командой:\n`/addbot TOKEN`")
    await callback.answer()

@dp.message(Command("addbot"))
async def add_bot_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите токен бота: `/addbot TOKEN`")
        return
    token = args[1].strip()
    try:
        test_bot = Bot(token=token)
        me = await test_bot.get_me()
        username = me.username
        await register_bot(message.from_user.id, token, username)
        await message.reply(f"✅ Бот @{username} добавлен как зеркало!")
    except Exception as e:
        await message.reply(f"❌ Ошибка проверки токена: {e}")

# ===== HEALTH CHECK =====
async def health_check(request):
    return web.Response(text="OK", status=200)

# ===== MAIN =====
async def main():
    await init_db()
    print("🚀 Бот запущен (универсальный поиск)")

    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)

    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"🌐 Health check server running on port {port}")

    try:
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Ошибка в polling: {e}")
    finally:
        print("🛑 Остановка приложения...")
        await runner.cleanup()
        await bot.session.close()
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Бот остановлен вручную")
