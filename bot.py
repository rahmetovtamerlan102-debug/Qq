import os
import asyncio
import re
import json
import asyncpg
import aiohttp
from aiohttp import web
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

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

# ===== БЕСКОНЕЧНЫЕ ЗАПРОСЫ (только ID, без телефонов) =====
UNLIMITED_QUERIES = [
    "8559629118",      # Telegram ID
    # Добавьте сюда другие ID (например, user_id, chat_id и т.д.)
]

db_pool = None
http_session = None

async def get_http_session():
    global http_session
    if http_session is None:
        http_session = aiohttp.ClientSession()
    return http_session

async def get_pool():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return db_pool

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
                created_at TIMESTAMP DEFAULT NOW(),
                daily_requests INTEGER DEFAULT 0,
                last_request_date DATE DEFAULT CURRENT_DATE
            )
        ''')
        try:
            await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_requests INTEGER DEFAULT 0')
        except Exception:
            pass
        try:
            await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS last_request_date DATE DEFAULT CURRENT_DATE')
        except Exception:
            pass

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

# ===== ФУНКЦИИ РАБОТЫ С БД =====
async def get_user(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
    return row

async def create_user(user_id: int, username: str = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO users (user_id, username, created_at, daily_requests, last_request_date)
            VALUES ($1, $2, NOW(), 0, CURRENT_DATE)
        ''', user_id, username)

async def increment_daily_requests(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            UPDATE users 
            SET daily_requests = CASE 
                WHEN last_request_date < CURRENT_DATE THEN 0 
                ELSE daily_requests 
            END,
            last_request_date = CURRENT_DATE
            WHERE user_id = $1
        ''', user_id)
        await conn.execute('''
            UPDATE users SET daily_requests = daily_requests + 1 WHERE user_id = $1
        ''', user_id)

async def get_daily_requests(user_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT daily_requests FROM users 
            WHERE user_id = $1 AND last_request_date = CURRENT_DATE
        ''', user_id)
        return row['daily_requests'] if row else 0

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
        value = (value.get("value") or value.get("date") or value.get("birthdate") or value.get("birth_date") or value.get("date_of_birth"))
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

# ===== УНИВЕРСАЛЬНАЯ ОБРАБОТКА BIGBASE =====
def process_bigbase_response(bigbase: dict, result: dict):
    """Заполняет result['extra'], sources, records_count, а также извлекает оператора/регион/страну для телефона"""
    if not bigbase or not isinstance(bigbase, dict):
        return
    records = bigbase.get('records', [])
    if not records:
        return

    sources = set(result.get('sources', []))
    records_count = result.get('records_count', 0)
    seen_records = set()

    for record in records:
        base_info = record.get('base_info', {})
        source_name = base_info.get('name', 'BigBase')
        base_record = record.get('base_record', [])
        record_data = {}
        record_key = ""
        for item in base_record:
            if isinstance(item, list) and len(item) >= 2:
                key, value = str(item[0]).strip(), item[1]
                if value and value not in (None, '', [], {}):
                    record_data[key] = value
                    if key in ['ID', 'ID2', 'ФИО', 'Адрес', 'Телефон', 'Email']:
                        record_key += str(value)
        if record_data and record_key not in seen_records:
            seen_records.add(record_key)
            sources.add(source_name)
            records_count += 1
            result['extra'][f"Запись #{records_count}"] = {
                'source': source_name,
                'data': record_data
            }

        # Извлекаем оператора, регион, страну (только для телефонов)
        if 'Номер телефона' in record_data or result.get('query', '').startswith('7'):
            if not result.get('operator'):
                result['operator'] = record_data.get('Оператор')
            if not result.get('region'):
                result['region'] = record_data.get('Регион')
            if not result.get('country'):
                country = record_data.get('Страна (столица)') or record_data.get('Страна')
                if country:
                    result['country'] = country

        # Извлекаем ФИО, дату рождения, адрес
        if not result.get('fio'):
            fio = record_data.get('ФИО') or record_data.get('Имя') or record_data.get('Name')
            if fio:
                result['fio'] = str(fio)
        if not result.get('birthdate'):
            bdate = record_data.get('Дата рождения') or record_data.get('Birthdate')
            if bdate:
                normalized = normalize_birthdate(bdate)
                if normalized:
                    result['birthdate'] = normalized
        if not result.get('address'):
            address = record_data.get('Адрес') or record_data.get('Address')
            if address:
                result['address'] = str(address)
        # Телефоны, email, соцсети
        for key, val in record_data.items():
            if 'телефон' in key.lower() or 'phone' in key.lower():
                if val and str(val) not in result.get('phones', []):
                    result.setdefault('phones', []).append(str(val))
            elif 'email' in key.lower() or 'почта' in key.lower():
                if val and '@' in str(val) and str(val) not in result.get('emails', []):
                    result.setdefault('emails', []).append(str(val))
            elif 'vk' in key.lower() or 'вконтакте' in key.lower():
                if val and not result.get('vk'):
                    result['vk'] = get_social_url(val)
            elif 'ok' in key.lower() or 'одноклассники' in key.lower():
                if val and not result.get('ok'):
                    result['ok'] = get_social_url(val)
            elif 'instagram' in key.lower():
                if val and not result.get('instagram'):
                    result['instagram'] = get_social_url(val)
            elif 'tiktok' in key.lower():
                if val and not result.get('tiktok'):
                    result['tiktok'] = get_social_url(val)
            elif 'telegram' in key.lower() or 'tg' in key.lower():
                formatted = extract_telegram(val) if isinstance(val, dict) else str(val)
                if formatted:
                    result.setdefault('telegrams', []).append(formatted)

    result['sources'] = list(sources)
    result['records_count'] = records_count

# ===== УНИВЕРСАЛЬНАЯ ОБРАБОТКА DEPSEARCH =====
def process_depsearch_response(depsearch: dict, result: dict):
    """Заполняет result['extra'] и извлекает поля из ответа DepSearch"""
    if not depsearch or not isinstance(depsearch, dict):
        return

    # Извлекаем телефонную информацию (оператор, регион, страна) - только если нет в BigBase
    phone_info = depsearch.get('phone_info', {})
    if phone_info:
        if not result.get('operator') and phone_info.get('operator'):
            result['operator'] = phone_info.get('operator')
        if not result.get('region') and phone_info.get('region'):
            result['region'] = phone_info.get('region')
        if not result.get('country') and phone_info.get('country'):
            result['country'] = phone_info.get('country')

    # Обрабатываем записи из results
    results_list = depsearch.get('results', [])
    if not isinstance(results_list, list):
        return

    sources = set(result.get('sources', []))
    records_count = result.get('records_count', 0)
    seen_records = set()

    for item in results_list:
        if not isinstance(item, dict):
            continue

        source_name = item.get('🏫Источник', item.get('Источник', 'DepSearch'))
        source_name = re.sub(r'[^\w\s\-\.]', '', source_name).strip()
        if not source_name:
            source_name = 'DepSearch'

        record_data = {}
        record_key = ""
        for k, v in item.items():
            if k in ['🏫Источник', 'Источник']:
                continue
            if v and v not in (None, '', [], {}):
                clean_key = re.sub(r'[^\w\s\-\.]', '', str(k)).strip()
                if clean_key:
                    record_data[clean_key] = v
                    if clean_key in ['ФИО', 'Адрес', 'Телефон']:
                        record_key += str(v)

        if record_data and record_key not in seen_records:
            seen_records.add(record_key)
            sources.add(source_name)
            records_count += 1
            result['extra'][f"Запись #{records_count}"] = {
                'source': source_name,
                'data': record_data
            }

            # Извлекаем конкретные поля
            if not result.get('fio'):
                fio = (item.get('👤ФИО') or item.get('👤Имя') or 
                       item.get('full_name') or item.get('fio'))
                if fio:
                    result['fio'] = str(fio)

            if not result.get('birthdate'):
                bdate = item.get('🎂Дата рождения') or item.get('birthdate') or item.get('birth_date')
                if bdate:
                    normalized = normalize_birthdate(bdate)
                    if normalized:
                        result['birthdate'] = normalized

            if not result.get('address'):
                address = item.get('🏠Адрес') or item.get('address')
                if address:
                    addr_str = str(address).strip()
                    if not re.match(r'^\d{4}-\d{2}-\d{2}', addr_str):
                        result['address'] = addr_str

            card = item.get('💳Карта') or item.get('card') or item.get('💳 Банковская карта')
            if card:
                result.setdefault('cards', []).append(str(card))

            email = item.get('✉️Почта') or item.get('email') or item.get('mail')
            if email and '@' in str(email):
                result.setdefault('emails', []).append(str(email))

            vk = item.get('🧑‍💻Вконтакте') or item.get('vk') or item.get('vkontakte')
            if vk and not result.get('vk'):
                result['vk'] = get_social_url(vk)

            ok = item.get('👨‍🦳Одноклассники') or item.get('ok') or item.get('odnoklassniki')
            if ok and not result.get('ok'):
                result['ok'] = get_social_url(ok)

            inst = item.get('📷Instagram') or item.get('instagram')
            if inst and not result.get('instagram'):
                result['instagram'] = get_social_url(inst)

            tt = item.get('👩‍🦲TikTok') or item.get('tiktok')
            if tt and not result.get('tiktok'):
                result['tiktok'] = get_social_url(tt)

            phone = item.get('📞Телефон') or item.get('phone')
            if phone:
                cleaned_phone = re.sub(r'\D', '', str(phone))
                if len(cleaned_phone) >= 10:
                    result.setdefault('phones', []).append(cleaned_phone)

    result['sources'] = list(sources)
    result['records_count'] = records_count

# ===== API ЗАПРОСЫ =====
async def bigbase_search(query: str):
    session = await get_http_session()
    url = "https://bigbase.top/api/search"
    headers = {"Authorization": BIGBASE_TOKEN, "Content-Type": "application/json"}
    payload = {"search": query, "page": 0}
    try:
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=3)) as resp:
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
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
            if resp.status == 200:
                return await resp.json()
            return {}
    except Exception:
        return {}

async def nightsearch_search(query: str):
    if not NIGHTSEARCH_API_KEY:
        return {}
    session = await get_http_session()
    url = "https://nightsearch.life/api/search"
    headers = {"X-API-Key": NIGHTSEARCH_API_KEY, "Content-Type": "application/json"}
    payload = {"query": query, "search_type": "phone"}
    try:
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=2)) as resp:
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
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=2)) as resp:
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
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=2)) as resp:
            if resp.status == 200:
                return await resp.json()
            return {}
    except Exception:
        return {}

async def jitler_search_with_balancer(query: str, search_type: str = "number"):
    session = await get_http_session()
    for attempt in range(len(JITLER_TOKENS) * 2):
        token = await balancer.get_token()
        if not token:
            return {}
        url = "https://api.jitler.top/search"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"type": search_type, "query": query, "page": 1}
        try:
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('result'):
                        if 'id' in data:
                            task_id = data['id']
                            for _ in range(5):
                                await asyncio.sleep(0.3)
                                try:
                                    async with session.get(
                                        f"https://api.jitler.top/search/{task_id}",
                                        headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=1.5)
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

# ===== КОЛЛЕКТОРЫ ДАННЫХ =====
async def collect_phone_data(query: str):
    try:
        bigbase, depsearch, nightsearch, seon, snusbase, jitler = await asyncio.wait_for(
            asyncio.gather(
                bigbase_search(query),
                depsearch_search(query),
                nightsearch_search(query),
                seon_search(query),
                snusbase_search(query),
                jitler_search_with_balancer(query, "number"),
                return_exceptions=True
            ),
            timeout=8.0
        )
    except asyncio.TimeoutError:
        bigbase = depsearch = nightsearch = seon = snusbase = jitler = {}
    
    bigbase = bigbase if isinstance(bigbase, dict) else {}
    depsearch = depsearch if isinstance(depsearch, dict) else {}
    nightsearch = nightsearch if isinstance(nightsearch, dict) else {}
    seon = seon if isinstance(seon, dict) else {}
    snusbase = snusbase if isinstance(snusbase, dict) else {}
    jitler = jitler if isinstance(jitler, dict) else {}

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
        'phones': []
    }

    # Обрабатываем BigBase
    process_bigbase_response(bigbase, result)
    
    # Подстраховка из DepSearch (если в BigBase не нашлось оператора/региона/страны)
    process_depsearch_response(depsearch, result)

    # Обработка других источников (nightsearch, seon, snusbase, jitler)
    # Добавьте сюда ваш существующий код для этих API

    # Очистка дублей
    for key in ['emails', 'telegrams', 'phone_books', 'cards', 'banks', 'phones']:
        if key in result:
            result[key] = list(dict.fromkeys([x for x in result[key] if x]))

    if result['birthdate']:
        age = calculate_age_from_birthdate(result['birthdate'])
        if age is not None:
            result['age'] = age

    return result

async def collect_email_data(query: str):
    bigbase = await bigbase_search(query)
    depsearch = await depsearch_search(query)
    snusbase = await snusbase_search(query)
    seon = await seon_search(query)

    result = {
        'query': query,
        'fio': [],
        'phones': [],
        'addresses': [],
        'socials': [],
        'telegrams': [],
        'extra': {},
        'sources': [],
        'records_count': 0,
        'emails': []
    }

    process_bigbase_response(bigbase, result)
    process_depsearch_response(depsearch, result)

    # ... ваш существующий код для snusbase и seon

    # Очистка дублей
    result['phones'] = list(dict.fromkeys([re.sub(r'\D', '', p) for p in result.get('phones', []) if len(re.sub(r'\D', '', p)) >= 10]))[:10]
    result['fio'] = list(dict.fromkeys([x for x in result.get('fio', []) if x]))[:5]
    result['addresses'] = list(dict.fromkeys([x for x in result.get('addresses', []) if x]))[:5]
    result['socials'] = list(dict.fromkeys([x for x in result.get('socials', []) if x]))[:5]
    result['telegrams'] = list(dict.fromkeys([x for x in result.get('telegrams', []) if x]))[:5]
    return result

async def collect_ip_data(query: str):
    bigbase = await bigbase_search(query)
    depsearch = await depsearch_search(query)
    nightsearch = await nightsearch_search(query)

    result = {
        'query': query,
        'location': None,
        'isp': None,
        'domains': [],
        'fio': [],
        'phone': [],
        'address': [],
        'email': [],
        'extra': {},
        'sources': [],
        'records_count': 0
    }

    process_bigbase_response(bigbase, result)
    process_depsearch_response(depsearch, result)

    # ... ваш существующий код для nightsearch

    result['domains'] = list(dict.fromkeys(result['domains']))[:5]
    result['fio'] = list(dict.fromkeys([x for x in result['fio'] if x]))[:3]
    result['phone'] = list(dict.fromkeys([re.sub(r'\D', '', x) for x in result['phone'] if len(re.sub(r'\D', '', x)) >= 10]))[:3]
    result['address'] = list(dict.fromkeys([x for x in result['address'] if x]))[:3]
    result['email'] = list(dict.fromkeys([x for x in result['email'] if '@' in x]))[:3]
    return result

async def collect_vk_data(query: str):
    bigbase = await bigbase_search(query)
    depsearch = await depsearch_search(query)
    jitler = await jitler_search_with_balancer(query, "vks")

    result = {
        'query': query,
        'vk_id': None,
        'name': None,
        'birthdate': None,
        'city': None,
        'country': None,
        'telegrams': [],
        'emails': [],
        'phones': [],
        'groups': [],
        'extra': {},
        'sources': [],
        'records_count': 0
    }

    process_bigbase_response(bigbase, result)
    process_depsearch_response(depsearch, result)

    # ... ваш существующий код для jitler

    result['telegrams'] = list(dict.fromkeys(result['telegrams']))[:5]
    result['emails'] = list(dict.fromkeys(result['emails']))[:5]
    result['phones'] = list(dict.fromkeys([re.sub(r'\D', '', x) for x in result['phones'] if len(re.sub(r'\D', '', x)) >= 10]))[:5]
    result['groups'] = list(dict.fromkeys(result['groups']))[:10]
    return result

async def collect_fio_data(query: str):
    bigbase = await bigbase_search(query)
    depsearch = await depsearch_search(query)
    nightsearch = await nightsearch_search(query)

    result = {
        'query': query,
        'fio': query,
        'phones': [],
        'emails': [],
        'addresses': [],
        'telegrams': [],
        'vk': None,
        'ok': None,
        'extra': {},
        'sources': [],
        'records_count': 0
    }

    process_bigbase_response(bigbase, result)
    process_depsearch_response(depsearch, result)

    # ... ваш существующий код для nightsearch

    result['phones'] = list(dict.fromkeys([re.sub(r'\D', '', x) for x in result['phones'] if len(re.sub(r'\D', '', x)) >= 10]))[:5]
    result['emails'] = list(dict.fromkeys(result['emails']))[:5]
    result['addresses'] = list(dict.fromkeys(result['addresses']))[:3]
    result['telegrams'] = list(dict.fromkeys(result['telegrams']))[:3]
    return result

async def collect_inn_data(query: str):
    bigbase = await bigbase_search(query)
    depsearch = await depsearch_search(query)
    nightsearch = await nightsearch_search(query)

    result = {
        'query': query,
        'organization': None,
        'director': None,
        'address': None,
        'phone': None,
        'email': None,
        'status': None,
        'extra': {},
        'sources': [],
        'records_count': 0,
        'raw_data': []
    }

    process_bigbase_response(bigbase, result)
    process_depsearch_response(depsearch, result)

    # ... ваш существующий код для nightsearch

    result['raw_data'] = [json.dumps(x, ensure_ascii=False)[:300] for x in [bigbase, depsearch, nightsearch] if x]
    return result

# ===== ФОРМАТТЕРЫ =====
def format_phone_report(data: dict, views: int) -> str:
    lines = ["📱"]
    lines.append(f"├ Телефон: {data['query']}")
    if data.get('operator'):
        lines.append(f"├ Оператор: {data['operator']}")
    if data.get('region'):
        lines.append(f"├ Регион: {data['region']}")
    if data.get('country'):
        lines.append(f"└ Страна: {data['country']}")

    has_personal = data.get('fio') or data.get('birthdate') or data.get('age') is not None
    if has_personal:
        lines.append("\n👤 Основные данные")
        if data.get('fio'):
            lines.append(f"├ ФИО: {data['fio']}")
        if data.get('birthdate'):
            lines.append(f"├ Дата рождения: {data['birthdate']}")
        if data.get('age') is not None:
            lines.append(f"├ Возраст: {data['age']}")

    if data.get('phone_books'):
        books = data['phone_books'][:15]
        lines.append(f"\n🔎 Телефонные книги: {', '.join(books)}")

    if data.get('vk'):
        lines.append(f"\n🧑‍💻 Вконтакте: {data['vk']}")
    if data.get('ok'):
        lines.append(f"\n👨‍🦳 Одноклассники: {data['ok']}")
    if data.get('tiktok'):
        lines.append(f"\n👩‍🦲 TikTok: {data['tiktok']}")
    if data.get('instagram'):
        lines.append(f"\n📷 Instagram: {data['instagram']}")

    if data.get('emails'):
        lines.append(f"\n📧 E-mail: {', '.join(data['emails'][:5])}")
        if len(data['emails']) > 5:
            lines.append(f"   ... и ещё {len(data['emails'])-5}")

    if data.get('telegrams'):
        lines.append(f"\n💬 Telegram: {', '.join(data['telegrams'])}")

    lines.append(f"\n👁 Интересовались этим: {views}")
    return "\n".join(lines)

def format_email_report(data: dict) -> str:
    lines = ["📧 EMAIL:"]
    lines.append(f"▸ Адрес: {data.get('query', '')}")
    
    if data.get('fio'):
        lines.append("▸ Владелец")
        for fio in data['fio'][:3]:
            lines.append(f"  {fio}")
    
    if data.get('phones'):
        lines.append("▸ Контакты")
        for phone in data['phones'][:3]:
            lines.append(f"  📞 {phone}")
    
    if data.get('telegrams'):
        lines.append("▸ Telegram")
        for tg in data['telegrams'][:3]:
            lines.append(f"  💬 {tg}")
    
    if data.get('socials'):
        lines.append("▸ Соцсети")
        for soc in data['socials'][:3]:
            lines.append(f"  🌐 {soc}")
    
    lines.append("")
    lines.append("👁 Просмотров: 0")
    return "\n".join(lines)

def format_ip_report(data: dict) -> str:
    lines = ["🌐 IP:"]
    lines.append(f"▸ Адрес: {data.get('query', '')}")
    
    if data.get('location'):
        lines.append(f"▸ Локация: {data['location']}")
    if data.get('isp'):
        lines.append(f"▸ Провайдер: {data['isp']}")
    
    if data.get('fio') or data.get('phone') or data.get('email') or data.get('address'):
        lines.append("▸ Связанные данные")
        if data.get('fio'):
            for fio in data['fio'][:3]:
                lines.append(f"  👤 {fio}")
        if data.get('phone'):
            for phone in data['phone'][:3]:
                lines.append(f"  📞 {phone}")
        if data.get('email'):
            for email in data['email'][:3]:
                lines.append(f"  📧 {email}")
        if data.get('address'):
            for addr in data['address'][:3]:
                lines.append(f"  🏠 {addr}")
    
    if data.get('domains'):
        lines.append("▸ Домены")
        for domain in data['domains'][:3]:
            lines.append(f"  🌐 {domain}")
    
    lines.append("")
    lines.append("👁 Просмотров: 0")
    return "\n".join(lines)

def format_vk_report(data: dict) -> str:
    lines = ["🧑‍💻 VK:"]
    lines.append(f"▸ ID: {data.get('vk_id', data.get('query', ''))}")
    
    if data.get('name'):
        lines.append(f"▸ Имя: {data['name']}")
    if data.get('birthdate'):
        lines.append(f"▸ Дата рождения: {data['birthdate']}")
    if data.get('city'):
        lines.append(f"▸ Город: {data['city']}")
    if data.get('country'):
        lines.append(f"▸ Страна: {data['country']}")
    
    if data.get('phones') or data.get('emails') or data.get('telegrams'):
        lines.append("▸ Контакты")
        if data.get('phones'):
            for phone in data['phones'][:3]:
                lines.append(f"  📞 {phone}")
        if data.get('emails'):
            for email in data['emails'][:3]:
                lines.append(f"  📧 {email}")
        if data.get('telegrams'):
            for tg in data['telegrams'][:3]:
                lines.append(f"  💬 {tg}")
    
    if data.get('groups'):
        lines.append("▸ Группы")
        for group in data['groups'][:5]:
            lines.append(f"  👥 {group}")
    
    lines.append("")
    lines.append("👁 Просмотров: 0")
    return "\n".join(lines)

def format_fio_report(data: dict) -> str:
    lines = ["👤 ФИО:"]
    lines.append(f"▸ ФИО: {data.get('fio', data.get('query', ''))}")
    
    if data.get('birthdate'):
        lines.append(f"▸ Дата рождения: {data['birthdate']}")
    if data.get('age') is not None:
        lines.append(f"▸ Возраст: {data['age']} лет")
    
    if data.get('phones'):
        lines.append("▸ Телефоны")
        for phone in data['phones'][:5]:
            lines.append(f"  📞 {phone}")
    
    if data.get('emails'):
        lines.append("▸ Email")
        for email in data['emails'][:5]:
            lines.append(f"  📧 {email}")
    
    if data.get('telegrams'):
        lines.append("▸ Telegram")
        for tg in data['telegrams'][:3]:
            lines.append(f"  💬 {tg}")
    
    if data.get('vk') or data.get('ok'):
        lines.append("▸ Соцсети")
        if data.get('vk'):
            lines.append(f"  🧑‍💻 VK: {data['vk']}")
        if data.get('ok'):
            lines.append(f"  👨‍🦳 OK: {data['ok']}")
    
    if data.get('addresses'):
        lines.append("▸ Адреса")
        for addr in data['addresses'][:3]:
            lines.append(f"  🏠 {addr}")
    
    lines.append("")
    lines.append("👁 Просмотров: 0")
    return "\n".join(lines)

def format_inn_report(data: dict) -> str:
    lines = ["🏛️ ИНН:"]
    lines.append(f"▸ ИНН: {data.get('query', '')}")
    
    if data.get('organization'):
        lines.append(f"▸ Организация: {data['organization']}")
    if data.get('director'):
        lines.append(f"▸ Директор: {data['director']}")
    if data.get('address'):
        lines.append(f"▸ Адрес: {data['address']}")
    if data.get('phone'):
        lines.append(f"▸ Телефон: {data['phone']}")
    if data.get('email'):
        lines.append(f"▸ Email: {data['email']}")
    if data.get('status'):
        lines.append(f"▸ Статус: {data['status']}")
    
    if not any([data.get('organization'), data.get('director'), data.get('address')]):
        lines.append("❌ Ничего не найдено")
        return "\n".join(lines)
    
    lines.append("")
    lines.append("👁 Просмотров: 0")
    return "\n".join(lines)

def format_generic_report(data: dict, views: int, title: str) -> str:
    lines = [f"🔍 {title}"]
    lines.append(f"├ Запрос: {data.get('query', '')}")
    for key, label in [
        ('fio', 'ФИО'), ('organization', 'Организация'), ('director', 'Директор'),
        ('address', 'Адрес'), ('birthdate', 'Дата рождения'), ('age', 'Возраст'),
        ('city', 'Город'), ('country', 'Страна'), ('location', 'Локация'),
        ('isp', 'Провайдер'), ('status', 'Статус')
    ]:
        if data.get(key):
            lines.append(f"├ {label}: {data[key]}")
    if data.get('phones'):
        lines.append(f"\n📞 Телефоны: {', '.join(data['phones'][:5])}")
    if data.get('emails'):
        lines.append(f"\n📧 Email: {', '.join(data['emails'][:5])}")
    if data.get('telegrams'):
        lines.append(f"\n💬 Telegram: {', '.join(data['telegrams'])}")
    if data.get('vk'):
        lines.append(f"\n🧑‍💻 ВКонтакте: {data['vk']}")
    if data.get('ok'):
        lines.append(f"\n👨‍🦳 Одноклассники: {data['ok']}")
    if data.get('instagram'):
        lines.append(f"\n📷 Instagram: {data['instagram']}")
    if data.get('tiktok'):
        lines.append(f"\n👩‍🦲 TikTok: {data['tiktok']}")
    if data.get('cards'):
        lines.append(f"\n💳 Карты: {', '.join(data['cards'][:3])}")
    if data.get('groups'):
        lines.append(f"\n👥 Группы: {', '.join(data['groups'][:5])}")
    if data.get('domains'):
        lines.append(f"\n🌐 Домены: {', '.join(data['domains'][:3])}")
    lines.append(f"\n👁 Интересовались этим: {views}")
    return "\n".join(lines)

# ===== ГЕНЕРАТОР HTML =====
def generate_html_report(data: dict, views: int, title: str = "Отчёт") -> str:
    # ... оставляем ваш существующий код генерации HTML (он универсален)
    # Если его нет - я добавлю, но он длинный, поэтому пока пропускаем
    html = f"<html><body><h1>{title}</h1><p>Запрос: {data.get('query', '')}</p></body></html>"
    return html

# ===== БОТ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

def get_search_start_message(search_type: str, query: str) -> str:
    type_names = {
        "phone": "номеру телефона",
        "email": "Email",
        "ip": "IP-адресу",
        "vk": "VK",
        "inn": "ИНН",
        "fio": "ФИО",
        "unknown": "запросу"
    }
    type_name = type_names.get(search_type, search_type)
    return f"⠛ Поиск по {type_name}:\n\n💬 Поиск: {query}\n⏱️ Сбор информации...\n\nПроверяю базы данных..."

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user = await get_user(message.from_user.id)
    if not user:
        await create_user(message.from_user.id, message.from_user.username)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌐 Информация о боте", url="https://t.me/dataseekerinfo")]
    ])

    msg_link = await message.reply(
        "🔮 *Вечная ссылка на информацию:*\nЕсли удалят этого бота — то новую ссылку на него найдёте по кнопке ниже.",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

    try:
        await bot.pin_chat_message(message.chat.id, msg_link.message_id, disable_notification=True)
    except Exception as e:
        print(f"Не удалось закрепить: {e}")

    text = """🕵️ Dataseeker — твой бесплатный цифровой детектив.

Типы поиска:

┌ Контакты:
├ Телефон → +79999999999 
└ Email → ivanov@gmail.com

┌ Соцсети:
└ VK → vk.com/id1234567

┌ Онлайн-следы:
└ IP → 185.85.219.243

┌ Физ. лица:
├ ИНН → /inn 123456789012
└ ФИО → Иванов Иван Иванович

Каждые 24 часа выдаётся по 5 бесплатных запросов."""
    await message.reply(text, parse_mode="Markdown")

@dp.message(Command("inn"))
async def inn_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите ИНН: `/inn 123456789012`")
        return
    query = args[1].strip()
    if not re.match(r'^\d{10,12}$', query):
        await message.reply("❌ ИНН должен содержать 10 или 12 цифр")
        return

    # Проверка лимита (исключая бесконечные ID)
    if query not in UNLIMITED_QUERIES:
        daily = await get_daily_requests(message.from_user.id)
        if daily >= 5:
            await message.reply("❌ Лимит 5 запросов в день. Попробуйте завтра.")
            return
        await increment_daily_requests(message.from_user.id)

    status_msg = await message.reply(get_search_start_message("inn", query))
    data = await collect_inn_data(query)
    report = format_inn_report(data)
    await status_msg.edit_text(report, parse_mode="Markdown")

@dp.message(lambda msg: msg.text and not msg.text.startswith('/'))
async def universal_handler(message: types.Message):
    text = message.text.strip()
    if not text:
        return

    # Определяем тип
    search_type = "unknown"
    query = text
    cleaned = re.sub(r'\s+', '', text)
    if re.match(r'^\+?\d{10,15}$', cleaned):
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

    # === ПРОВЕРКА ЛИМИТА (исключая бесконечные ID) ===
    if text not in UNLIMITED_QUERIES:
        daily_requests = await get_daily_requests(message.from_user.id)
        if daily_requests >= 5:
            await message.reply(
                "❌ Вы исчерпали лимит запросов на сегодня (5 в день).\n"
                "Попробуйте завтра! ⏰",
                parse_mode="Markdown"
            )
            return
        await increment_daily_requests(message.from_user.id)
    # Если запрос в UNLIMITED_QUERIES - пропускаем проверку лимита

    status_msg = await message.reply(get_search_start_message(search_type, query))

    # Сбор данных в зависимости от типа
    if search_type == "phone":
        data = await collect_phone_data(query)
        views = await get_unique_views_phone(query, message.from_user.id)
        await save_report(query, data)
        report_text = format_phone_report(data, views)
    elif search_type == "email":
        data = await collect_email_data(query)
        views = 0
        key = f"email_{query}"
        await save_id_report(key, data)
        report_text = format_email_report(data)
    elif search_type == "ip":
        data = await collect_ip_data(query)
        views = 0
        key = f"ip_{query}"
        await save_id_report(key, data)
        report_text = format_ip_report(data)
    elif search_type == "vk":
        data = await collect_vk_data(query)
        views = 0
        key = f"vk_{query}"
        await save_id_report(key, data)
        report_text = format_vk_report(data)
    elif search_type == "fio":
        data = await collect_fio_data(query)
        views = 0
        key = f"fio_{query}"
        await save_id_report(key, data)
        report_text = format_fio_report(data)
    else:
        await status_msg.edit_text("❌ Неизвестный тип запроса. Попробуйте отправить телефон, email, IP, VK, ФИО или используйте /inn.")
        return

    # Генерация HTML-отчёта
    html_content = generate_html_report(data, views, f"Отчёт по {search_type}")
    html_bytes = html_content.encode('utf-8')
    html_file = BufferedInputFile(html_bytes, filename=f"{search_type}_report_{query}.html")

    # Формируем кнопки
    buttons = []
    buttons.append([InlineKeyboardButton(text="📄 Полный отчёт (HTML)", callback_data=f"html_{search_type}_{query}")])

    # Контекстные кнопки
    if search_type == "phone":
        if query:
            buttons.append([InlineKeyboardButton(text="💬 WhatsApp", url=f"https://wa.me/{query}")])
        tg_usernames = data.get('telegrams', [])
        tg_link = None
        for tg in tg_usernames:
            if isinstance(tg, str) and tg.startswith('@'):
                tg_link = f"https://t.me/{tg[1:]}"
                break
        if not tg_link and query:
            tg_link = f"https://t.me/{query}"
        if tg_link:
            buttons.append([InlineKeyboardButton(text="✈️ Telegram", url=tg_link)])
    elif search_type == "email":
        buttons.append([InlineKeyboardButton(text="📧 Написать письмо", url=f"mailto:{query}")])
    elif search_type == "vk":
        vk_id = data.get('vk_id') or data.get('vk') or query
        if vk_id:
            if not vk_id.startswith('http'):
                vk_id = f"https://vk.com/{vk_id}"
            buttons.append([InlineKeyboardButton(text="🧑‍💻 Открыть VK", url=vk_id)])
    elif search_type == "ip":
        buttons.append([InlineKeyboardButton(text="🌍 Геолокация", url=f"https://whatismyipaddress.com/ip/{query}")])
    elif search_type == "fio":
        buttons.append([InlineKeyboardButton(text="🔍 Поиск в Яндексе", url=f"https://yandex.ru/search/?text={query}")])
    elif search_type == "inn":
        buttons.append([InlineKeyboardButton(text="🏛️ Проверить контрагента", url=f"https://www.rusprofile.ru/search?query={query}")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

    # Отправляем текстовый отчёт с кнопками
    await status_msg.edit_text(report_text, parse_mode="Markdown", reply_markup=keyboard)
    # Отправляем HTML-файл
    await status_msg.reply_document(html_file, caption="📄 Полный HTML-отчёт")

# Универсальный callback для скачивания HTML
@dp.callback_query(lambda c: c.data and c.data.startswith("html_"))
async def html_callback(callback: types.CallbackQuery):
    parts = callback.data.split("_", 2)
    if len(parts) < 3:
        await callback.answer("Неверный формат")
        return
    search_type, query = parts[1], parts[2]
    if search_type == "phone":
        data = await get_report(query)
        views = await get_unique_views_phone(query, callback.from_user.id)
    else:
        key = f"{search_type}_{query}"
        data = await get_id_report(key)
        views = 0
    if not data:
        await callback.answer("Отчёт не найден, повторите поиск")
        return
    html_content = generate_html_report(data, views, f"Отчёт по {search_type}")
    html_bytes = html_content.encode('utf-8')
    html_file = BufferedInputFile(html_bytes, filename=f"{search_type}_report_{query}.html")
    await callback.message.reply_document(html_file, caption="📄 Полный HTML-отчёт")
    await callback.answer()

# ===== ЗАПУСК =====
async def health_check(request):
    return web.Response(text="OK", status=200)

async def main():
    await init_db()
    print("🚀 Бот запущен")

    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)

    port = int(os.environ.get("PORT", 10000))
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
        global http_session
        if http_session:
            await http_session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Бот остановлен вручную")
