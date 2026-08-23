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

TELELOG_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiIyMDMzMDI5NDc1IiwianRpIjoiODJmMjlmNzQtYmJlMi00ZGUwLWEwZDQtN2EzMDJhMWE5MDViIiwiZXhwIjoxODAxMDA4MzM4fQ.Mba4aX85YAMcaMLfhUBzXtCoNmEujfMe-6sGBbp3kT-T2SiLM_Ho0BBAFAQ8_C6Gz06PH9mAYhfBvlLSjb4oVd1Fm_vmb8MC-wuObU3qgfGrYdGzVF3ntJHv-LdNELq-jsqvQOY3jq9meso9dUoyj5SviDQWL6cvnRQ03kpHWxA"

db_pool = None

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

async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                referral_code TEXT UNIQUE,
                referred_by BIGINT,
                created_at TIMESTAMP DEFAULT NOW(),
                daily_requests INTEGER DEFAULT 0,
                last_request_date DATE DEFAULT CURRENT_DATE
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
            INSERT INTO users (user_id, username, referral_code, referred_by, created_at, daily_requests, last_request_date)
            VALUES ($1, $2, $3, $4, NOW(), 0, CURRENT_DATE)
        ''', user_id, username, ref_code, referred_by)

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

async def telelog_get_stats(user_id: str):
    if not TELELOG_TOKEN:
        return {}
    url = f"https://telelog.info/api/v1/users/{user_id}/stats_min"
    headers = {"accept": "text/plain", "Authorization": f"Bearer {TELELOG_TOKEN}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('success'):
                        return data.get('data', {})
                return {}
    except Exception as e:
        print(f"❌ TeleLog stats error: {repr(e)}")
        return []

# ===== СБОР ДАННЫХ =====
async def collect_phone_data(query: str):
    tasks = [
        bigbase_search(query),
        nightsearch_search(query),
        seon_search(query),
        snusbase_search(query),
        depsearch_search(query),
        jitler_search_with_balancer(query, "number")
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    bigbase = results[0] if isinstance(results[0], dict) else {}
    nightsearch = results[1] if isinstance(results[1], dict) else {}
    seon = results[2] if isinstance(results[2], dict) else {}
    snusbase = results[3] if isinstance(results[3], dict) else {}
    depsearch = results[4] if isinstance(results[4], dict) else {}
    jitler = results[5] if isinstance(results[5], dict) else {}

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
        'banks': []
    }

    all_birthdates = []

    if bigbase and isinstance(bigbase, dict):
        dossier = bigbase.get('dossier', {})
        head = dossier.get('head', {})
        result['operator'] = head.get('phone_operator')
        result['region'] = head.get('phone_region')
        result['country'] = head.get('phone_country_info')

        connections = bigbase.get('connections', {})
        persons = connections.get('person', [])
        for person in persons:
            if not result['fio']:
                fio_list = person.get('fio', [])
                for fio_item in fio_list:
                    if isinstance(fio_item, dict):
                        value = fio_item.get('value')
                    else:
                        value = fio_item
                    if value:
                        result['fio'] = str(value)
                        break
            if not all_birthdates:
                birthday_list = person.get('birthday', [])
                for bd_item in birthday_list:
                    if isinstance(bd_item, dict):
                        value = bd_item.get('value')
                    else:
                        value = bd_item
                    normalized = normalize_birthdate(value)
                    if normalized:
                        all_birthdates.append(normalized)
                        break
                if all_birthdates:
                    break
                person_head = person.get('head', {})
                head_birthday = person_head.get('head_birthday') or person_head.get('birthday')
                if head_birthday:
                    normalized = normalize_birthdate(head_birthday)
                    if normalized:
                        all_birthdates.append(normalized)
                        break
            if result['fio'] and all_birthdates:
                break

        if not all_birthdates:
            records = bigbase.get('records', [])
            for record in records:
                base_record = record.get('base_record', [])
                for item in base_record:
                    if isinstance(item, list) and len(item) >= 2:
                        key = str(item[0]).lower()
                        if 'дата рождения' in key or 'birthdate' in key or 'birth_date' in key:
                            value = item[1]
                            normalized = normalize_birthdate(value)
                            if normalized:
                                all_birthdates.append(normalized)
                                break
                if all_birthdates:
                    break

        for record in bigbase.get('records', []):
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).lower()
                    value = item[1]
                    if 'почта' in key or 'email' in key or 'mail' in key:
                        if value:
                            result['emails'].append(str(value))
                    if 'адрес' in key or 'address' in key or 'регистрация' in key:
                        if value:
                            addr = str(value).strip()
                            if not re.match(r'^\d{4}-\d{2}-\d{2}', addr):
                                result['address'] = addr
                    if 'карта' in key or 'card' in key:
                        if value:
                            result['cards'].append(str(value))
                    if 'банк' in key or 'bank' in key:
                        if value:
                            result['banks'].append(str(value))

        for conn in bigbase.get('connections', {}).get('person', []):
            email = deep_find(conn, 'email') or deep_find(conn, 'mail')
            if email:
                result['emails'].append(str(email))
            addr = deep_find(conn, 'address') or deep_find(conn, 'addr')
            if addr and not result['address']:
                addr_str = str(addr).strip()
                if not re.match(r'^\d{4}-\d{2}-\d{2}', addr_str):
                    result['address'] = addr_str

        graph = bigbase.get('graph', {})
        for node in graph.get('nodes', []):
            if node.get('type') == 'email' and node.get('title'):
                result['emails'].append(str(node['title']))

        socials = extract_socials(bigbase)
        for key in ['vk', 'ok', 'instagram', 'tiktok']:
            if socials.get(key) and not result.get(key):
                result[key] = socials[key]

        tg_data = deep_find_all(bigbase, 'telegram') + deep_find_all(bigbase, 'tg')
        for tg in tg_data:
            formatted = extract_telegram(tg)
            if formatted:
                result['telegrams'].append(formatted)

    if (not result['operator'] or not result['region'] or not result['country'] or
        not result['fio'] or not all_birthdates):
        if depsearch and isinstance(depsearch, dict):
            phone_info = depsearch.get('phone_info', {})
            if not result['operator']:
                result['operator'] = phone_info.get('operator')
            if not result['region']:
                result['region'] = phone_info.get('region')
            if not result['country']:
                result['country'] = phone_info.get('country')
            results_list = depsearch.get('results', [])
            if isinstance(results_list, list):
                for item in results_list:
                    if not isinstance(item, dict):
                        continue
                    if not result['fio']:
                        fio = (
                            item.get('👤ФИО') or
                            item.get('👤Имя') or
                            item.get('full_name') or
                            item.get('fio')
                        )
                        if fio:
                            result['fio'] = str(fio)
                    if not all_birthdates:
                        bdate = (
                            item.get('🎂Дата рождения') or
                            item.get('birthdate') or
                            item.get('birth_date')
                        )
                        if bdate:
                            normalized = normalize_birthdate(bdate)
                            if normalized:
                                all_birthdates.append(normalized)
                    if not result['address']:
                        address = item.get('🏠Адрес') or item.get('address')
                        if address:
                            addr_str = str(address).strip()
                            if not re.match(r'^\d{4}-\d{2}-\d{2}', addr_str):
                                result['address'] = addr_str
                    if not result['cards']:
                        card = item.get('💳Карта') or item.get('card')
                        if card:
                            result['cards'].append(str(card))
                    if not result['banks']:
                        bank = item.get('🏦Банк') or item.get('bank')
                        if bank:
                            result['banks'].append(str(bank))

    for src in [nightsearch, seon, snusbase, depsearch]:
        if not src:
            continue
        result['emails'].extend(
            deep_find_all(src, 'email') +
            deep_find_all(src, 'mail') +
            deep_find_all(src, 'e-mail')
        )
        if src == depsearch and isinstance(depsearch, dict):
            for item in depsearch.get('results', []):
                if isinstance(item, dict):
                    email = item.get('✉️Почта')
                    if email:
                        result['emails'].append(str(email))
                    address = item.get('🏠Адрес') or item.get('address')
                    if address and not result['address']:
                        addr_str = str(address).strip()
                        if not re.match(r'^\d{4}-\d{2}-\d{2}', addr_str):
                            result['address'] = addr_str
                    card = item.get('💳Карта') or item.get('card')
                    if card:
                        result['cards'].append(str(card))
                    bank = item.get('🏦Банк') or item.get('bank')
                    if bank:
                        result['banks'].append(str(bank))
        tg_data = deep_find_all(src, 'telegram') + deep_find_all(src, 'tg')
        for tg in tg_data:
            formatted = extract_telegram(tg)
            if formatted:
                result['telegrams'].append(formatted)
        if not result['vk']:
            vk = deep_find(src, 'vk') or deep_find(src, 'vkontakte')
            if vk:
                result['vk'] = get_social_url(vk)
        if not result['ok']:
            ok = deep_find(src, 'ok') or deep_find(src, 'odnoklassniki')
            if ok:
                result['ok'] = get_social_url(ok)
        if not result['instagram']:
            inst = deep_find(src, 'instagram')
            if inst:
                result['instagram'] = get_social_url(inst)
        if not result['tiktok']:
            tt = deep_find(src, 'tiktok')
            if tt:
                result['tiktok'] = get_social_url(tt)

    if jitler and isinstance(jitler, dict):
        jitler_data = jitler.get('response', jitler)
        phonebooks = jitler_data.get('phonebooks', [])
        if phonebooks:
            result['phone_books'] = list(dict.fromkeys(phonebooks))
        profiles = jitler_data.get('profiles', {})
        if profiles.get('vk'):
            vk_urls = [p.get('url') for p in profiles['vk'] if p.get('url')]
            if vk_urls and not result['vk']:
                result['vk'] = vk_urls[0]
        if profiles.get('ok'):
            ok_urls = [p.get('url') for p in profiles['ok'] if p.get('url')]
            if ok_urls and not result['ok']:
                result['ok'] = ok_urls[0]
        if profiles.get('instagram'):
            inst_urls = [p.get('url') for p in profiles['instagram'] if p.get('url')]
            if inst_urls and not result['instagram']:
                result['instagram'] = inst_urls[0]
        if profiles.get('tiktok'):
            tt_urls = [p.get('url') for p in profiles['tiktok'] if p.get('url')]
            if tt_urls and not result['tiktok']:
                result['tiktok'] = tt_urls[0]
        telegrams = jitler_data.get('telegram', [])
        for tg in telegrams:
            formatted = extract_telegram(tg)
            if formatted:
                result['telegrams'].append(formatted)

    if all_birthdates:
        result['birthdate'] = find_best_birthdate(all_birthdates)
        if result['birthdate']:
            age = calculate_age_from_birthdate(result['birthdate'])
            if age is not None:
                result['age'] = age

    clean_emails = []
    for email in result['emails']:
        if isinstance(email, dict):
            if email.get('value'):
                clean_emails.append(str(email['value']))
        elif isinstance(email, str) and '@' in email:
            clean_emails.append(email)
    result['emails'] = list(dict.fromkeys(clean_emails))
    result['telegrams'] = list(dict.fromkeys(result['telegrams']))
    result['phone_books'] = list(dict.fromkeys(result['phone_books']))
    result['cards'] = list(dict.fromkeys(result['cards']))
    result['banks'] = list(dict.fromkeys(result['banks']))

    return result

async def collect_telegram_data(query: str):
    bigbase = await bigbase_search(query)
    telelog_names = await telelog_get_names(query)
    telelog_usernames = await telelog_get_usernames(query)
    telelog_gifts = await telelog_get_gifts(query)
    telelog_stats = await telelog_get_stats(query)

    result = {
        'query': query,
        'telegrams': [],
        'groups': [],
        'name_history': [],
        'username_history': [],
        'gifts': [],
        'registration_date': None,
        'vk': None,
        'instagram': None,
        'tiktok': None,
        'ok': None
    }

    seen_names = set()
    for entry in telelog_names:
        if not isinstance(entry, dict):
            continue
        name = entry.get('name')
        if not name or name in seen_names:
            continue
        seen_names.add(name)
        date = entry.get('date_time') or entry.get('date')
        date_str = None
        if date:
            try:
                dt = datetime.fromisoformat(str(date).replace('Z', '+00:00'))
                date_str = dt.strftime('%d.%m.%Y %H:%M')
            except:
                date_str = str(date)[:10]
        result['name_history'].append({'date': date_str, 'name': str(name)})

    seen_usernames = set()
    for entry in telelog_usernames:
        if not isinstance(entry, dict):
            continue
        username = entry.get('name')
        if not username or username in seen_usernames:
            continue
        seen_usernames.add(username)
        username = str(username).strip()
        if not username.startswith('@'):
            username = '@' + username
        date = entry.get('date_time') or entry.get('date')
        date_str = None
        if date:
            try:
                dt = datetime.fromisoformat(str(date).replace('Z', '+00:00'))
                date_str = dt.strftime('%d.%m.%Y %H:%M')
            except:
                date_str = str(date)[:10]
        result['username_history'].append({'date': date_str, 'username': username})

    try:
        target_id = int(query)
    except:
        target_id = None
    for gift in telelog_gifts:
        if not isinstance(gift, dict):
            continue
        from_id = gift.get('from_user_id')
        to_id = gift.get('to_user_id')
        from_user = gift.get('from_mainUsername') or gift.get('from_first_name') or f"ID {from_id}"
        to_user = gift.get('to_mainUsername') or gift.get('to_first_name') or f"ID {to_id}"
        date = gift.get('last_gift_date')
        date_str = "?"
        if date:
            try:
                dt = datetime.fromisoformat(str(date).replace('Z', '+00:00'))
                date_str = dt.strftime('%d.%m.%Y')
            except:
                date_str = str(date)[:10]
        if target_id is not None and from_id == target_id:
            gift_str = f"📤 {date_str} → {to_user}"
        elif target_id is not None and to_id == target_id:
            gift_str = f"📥 {date_str} ← {from_user}"
        else:
            gift_str = f"{date_str}: {from_user} → {to_user}"
        result['gifts'].append(gift_str)

    if telelog_stats and isinstance(telelog_stats, dict):
        if telelog_stats.get('registration_date'):
            result['registration_date'] = telelog_stats['registration_date']
        if telelog_stats.get('username'):
            if not result['telegrams']:
                result['telegrams'].append('@' + telelog_stats['username'])

    if bigbase and isinstance(bigbase, dict):
        records = bigbase.get('records', [])
        for record in records:
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip().lower()
                    value = item[1]
                    if 'группы' in key:
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
                                            group_username = str(group_username).strip()
                                            if not group_username.startswith('@'):
                                                group_username = '@' + group_username
                                            result['groups'].append(f"{group_name}: {group_username}")
                                        else:
                                            result['groups'].append(group_name)
        socials = extract_socials(bigbase)
        for key in ['vk', 'ok', 'instagram', 'tiktok']:
            if socials.get(key) and not result.get(key):
                result[key] = socials[key]

    result['groups'] = list(dict.fromkeys(result['groups']))
    result['gifts'] = list(dict.fromkeys(result['gifts']))
    result['name_history'] = list(dict.fromkeys([x for x in result['name_history'] if x]))
    result['username_history'] = list(dict.fromkeys([x for x in result['username_history'] if x]))

    return result

async def collect_email_data(query: str):
    bigbase = await bigbase_search(query)
    snusbase = await snusbase_search(query)
    seon = await seon_search(query)

    result = {
        'query': query,
        'fio': [],
        'phones': [],
        'addresses': [],
        'socials': [],
        'telegrams': []
    }

    if bigbase and isinstance(bigbase, dict):
        records = bigbase.get('records', [])
        for record in records:
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip().lower()
                    value = item[1]
                    if 'телефон' in key or 'phone' in key:
                        if value:
                            result['phones'].append(str(value))
                    elif 'имя' in key or 'name' in key or 'фио' in key or 'fio' in key:
                        if value:
                            result['fio'].append(str(value))
                    elif 'адрес' in key or 'address' in key:
                        if value:
                            result['addresses'].append(str(value))
                    elif 'соцсеть' in key or 'social' in key or 'vk' in key or 'ok' in key:
                        if value:
                            result['socials'].append(str(value))
                    elif 'telegram' in key or 'tg' in key:
                        if value:
                            result['telegrams'].append(str(value))

    if snusbase and isinstance(snusbase, dict):
        data = snusbase.get('data', {})
        if isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, list):
                    for item in val:
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if 'phone' in k or 'телефон' in k:
                                    if v:
                                        result['phones'].append(str(v))
                                elif 'name' in k or 'имя' in k:
                                    if v:
                                        result['fio'].append(str(v))

    if seon and isinstance(seon, dict):
        data = seon.get('data', {})
        if data:
            if data.get('phone'):
                result['phones'].append(str(data['phone']))
            if data.get('name'):
                result['fio'].append(str(data['name']))
            if data.get('first_name'):
                result['fio'].append(str(data['first_name']))

    result['phones'] = list(dict.fromkeys([re.sub(r'\D', '', x) for x in result['phones'] if len(re.sub(r'\D', '', x)) >= 10]))[:10]
    result['fio'] = list(dict.fromkeys([x for x in result['fio'] if x]))[:5]
    result['addresses'] = list(dict.fromkeys([x for x in result['addresses'] if x]))[:5]
    result['socials'] = list(dict.fromkeys([x for x in result['socials'] if x]))[:5]
    result['telegrams'] = list(dict.fromkeys([x for x in result['telegrams'] if x]))[:5]

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
        'email': []
    }

    if bigbase and isinstance(bigbase, dict):
        records = bigbase.get('records', [])
        for record in records:
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip().lower()
                    value = item[1]
                    if 'локация' in key or 'location' in key:
                        result['location'] = str(value)
                    elif 'провайдер' in key or 'isp' in key:
                        result['isp'] = str(value)
                    elif 'домен' in key or 'domain' in key:
                        if value:
                            result['domains'].append(str(value))
                    elif 'имя' in key or 'name' in key or 'фио' in key:
                        if value:
                            result['fio'].append(str(value))
                    elif 'телефон' in key or 'phone' in key:
                        if value:
                            result['phone'].append(str(value))
                    elif 'адрес' in key or 'address' in key:
                        if value:
                            result['address'].append(str(value))
                    elif 'почта' in key or 'email' in key:
                        if value:
                            result['email'].append(str(value))

    if depsearch and isinstance(depsearch, dict):
        results_list = depsearch.get('results', [])
        if isinstance(results_list, list):
            for item in results_list:
                if not isinstance(item, dict):
                    continue
                loc = item.get('📍Локация') or item.get('location')
                if loc and not result['location']:
                    result['location'] = str(loc)
                isp = item.get('📡Провайдер') or item.get('isp')
                if isp and not result['isp']:
                    result['isp'] = str(isp)

    if nightsearch and isinstance(nightsearch, dict):
        data = nightsearch.get('data', {})
        if data:
            if data.get('location') and not result['location']:
                result['location'] = data['location']
            if data.get('isp') and not result['isp']:
                result['isp'] = data['isp']

    result['domains'] = list(dict.fromkeys(result['domains']))[:5]
    result['fio'] = list(dict.fromkeys([x for x in result['fio'] if x]))[:3]
    result['phone'] = list(dict.fromkeys([re.sub(r'\D', '', x) for x in result['phone'] if len(re.sub(r'\D', '', x)) >= 10]))[:3]
    result['address'] = list(dict.fromkeys([x for x in result['address'] if x]))[:3]
    result['email'] = list(dict.fromkeys([x for x in result['email'] if '@' in x]))[:3]

    return result

async def collect_vk_data(query: str):
    bigbase = await bigbase_search(query)
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
        'groups': []
    }

    if bigbase and isinstance(bigbase, dict):
        records = bigbase.get('records', [])
        for record in records:
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip().lower()
                    value = item[1]
                    if 'id' in key and not result['vk_id']:
                        if value:
                            result['vk_id'] = str(value)
                    elif 'имя' in key or 'name' in key:
                        if value and not result['name']:
                            result['name'] = str(value)
                    elif 'дата рождения' in key or 'birthdate' in key:
                        if value and not result['birthdate']:
                            result['birthdate'] = str(value)
                    elif 'город' in key or 'city' in key:
                        if value and not result['city']:
                            result['city'] = str(value)
                    elif 'страна' in key or 'country' in key:
                        if value and not result['country']:
                            result['country'] = str(value)
                    elif 'телефон' in key or 'phone' in key:
                        if value:
                            result['phones'].append(str(value))
                    elif 'почта' in key or 'email' in key:
                        if value:
                            result['emails'].append(str(value))
                    elif 'группы' in key:
                        if isinstance(value, list):
                            for group_item in value:
                                if isinstance(group_item, list):
                                    group_name = None
                                    for group_data in group_item:
                                        if isinstance(group_data, list) and len(group_data) >= 2:
                                            sub_key = str(group_data[0]).strip().lower()
                                            sub_value = group_data[1]
                                            if 'название' in sub_key:
                                                group_name = str(sub_value).strip()
                                    if group_name:
                                        result['groups'].append(group_name)

    if jitler and isinstance(jitler, dict):
        jitler_data = jitler.get('response', jitler)
        if jitler_data.get('vk_id'):
            result['vk_id'] = jitler_data['vk_id']
        profiles = jitler_data.get('profiles', {})
        vk_profiles = profiles.get('vk', [])
        if vk_profiles and isinstance(vk_profiles, list):
            for p in vk_profiles:
                if p.get('name') and not result['name']:
                    result['name'] = p['name']
                if p.get('url') and not result['vk_id']:
                    result['vk_id'] = p['url']
        telegrams = jitler_data.get('telegram', [])
        for tg in telegrams:
            if tg.get('username'):
                result['telegrams'].append('@' + tg['username'])

    result['telegrams'] = list(dict.fromkeys(result['telegrams']))[:5]
    result['emails'] = list(dict.fromkeys(result['emails']))[:5]
    result['phones'] = list(dict.fromkeys([re.sub(r'\D', '', x) for x in result['phones'] if len(re.sub(r'\D', '', x)) >= 10]))[:5]
    result['groups'] = list(dict.fromkeys(result['groups']))[:10]

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
        'raw_data': []
    }

    if bigbase and isinstance(bigbase, dict):
        records = bigbase.get('records', [])
        for record in records:
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip().lower()
                    value = item[1]
                    if 'организация' in key or 'company' in key or 'название' in key:
                        if value and not result['organization']:
                            result['organization'] = str(value)
                    elif 'директор' in key or 'director' in key:
                        if value and not result['director']:
                            result['director'] = str(value)
                    elif 'адрес' in key or 'address' in key:
                        if value and not result['address']:
                            result['address'] = str(value)
                    elif 'телефон' in key or 'phone' in key:
                        if value and not result['phone']:
                            result['phone'] = str(value)
                    elif 'почта' in key or 'email' in key:
                        if value and not result['email']:
                            result['email'] = str(value)
                    elif 'статус' in key or 'status' in key:
                        if value and not result['status']:
                            result['status'] = str(value)

    if depsearch and isinstance(depsearch, dict):
        results_list = depsearch.get('results', [])
        if isinstance(results_list, list):
            for item in results_list:
                if not isinstance(item, dict):
                    continue
                org = item.get('🏢Организация') or item.get('organization')
                if org and not result['organization']:
                    result['organization'] = str(org)
                dir = item.get('👤Директор') or item.get('director')
                if dir and not result['director']:
                    result['director'] = str(dir)
                addr = item.get('🏠Адрес') or item.get('address')
                if addr and not result['address']:
                    result['address'] = str(addr)

    if nightsearch and isinstance(nightsearch, dict):
        data = nightsearch.get('data', {})
        if data:
            if data.get('organization') and not result['organization']:
                result['organization'] = data['organization']
            if data.get('director') and not result['director']:
                result['director'] = data['director']

    result['raw_data'] = [json.dumps(x, ensure_ascii=False)[:300] for x in [bigbase, depsearch, nightsearch] if x]

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
        'ok': None
    }

    if bigbase and isinstance(bigbase, dict):
        records = bigbase.get('records', [])
        for record in records:
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip().lower()
                    value = item[1]
                    if 'телефон' in key or 'phone' in key:
                        if value:
                            result['phones'].append(str(value))
                    elif 'почта' in key or 'email' in key:
                        if value:
                            result['emails'].append(str(value))
                    elif 'адрес' in key or 'address' in key:
                        if value:
                            result['addresses'].append(str(value))
                    elif 'telegram' in key or 'tg' in key:
                        if value:
                            result['telegrams'].append(str(value))
                    elif 'vk' in key or 'vkontakte' in key:
                        if value and not result['vk']:
                            result['vk'] = get_social_url(value)
                    elif 'ok' in key or 'odnoklassniki' in key:
                        if value and not result['ok']:
                            result['ok'] = get_social_url(value)

    if depsearch and isinstance(depsearch, dict):
        results_list = depsearch.get('results', [])
        if isinstance(results_list, list):
            for item in results_list:
                if not isinstance(item, dict):
                    continue
                phone = item.get('📞Телефон') or item.get('phone')
                if phone:
                    result['phones'].append(str(phone))
                email = item.get('✉️Почта') or item.get('email')
                if email:
                    result['emails'].append(str(email))
                address = item.get('🏠Адрес') or item.get('address')
                if address:
                    result['addresses'].append(str(address))

    result['phones'] = list(dict.fromkeys([re.sub(r'\D', '', x) for x in result['phones'] if len(re.sub(r'\D', '', x)) >= 10]))[:5]
    result['emails'] = list(dict.fromkeys(result['emails']))[:5]
    result['addresses'] = list(dict.fromkeys(result['addresses']))[:3]
    result['telegrams'] = list(dict.fromkeys(result['telegrams']))[:3]

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

    has_personal = data.get('fio') or data.get('birthdate') or data.get('age') is not None or data.get('address')
    if has_personal:
        lines.append("\n👤 Основные данные")
        if data.get('fio'):
            lines.append(f"├ ФИО: {data['fio']}")
        if data.get('birthdate'):
            lines.append(f"├ Дата рождения: {data['birthdate']}")
        if data.get('age') is not None:
            lines.append(f"├ Возраст: {data['age']}")
        if data.get('address'):
            lines.append(f"└ Адрес: {data['address']}")

    if data.get('phone_books'):
        books = data['phone_books'][:15]
        lines.append(f"\n🔎 Телефонные книги: {', '.join(books)}")

    vk = data.get('vk')
    if vk:
        lines.append(f"\n🧑‍💻 Вконтакте: {vk}")
    ok = data.get('ok')
    if ok:
        lines.append(f"\n👨‍🦳 Одноклассники: {ok}")
    tiktok = data.get('tiktok')
    if tiktok:
        lines.append(f"\n👩‍🦲 TikTok: {tiktok}")
    instagram = data.get('instagram')
    if instagram:
        lines.append(f"\n📷 Instagram: {instagram}")

    if data.get('emails'):
        lines.append(f"\n📧 E-mail: {', '.join(data['emails'][:5])}")
        if len(data['emails']) > 5:
            lines.append(f"   ... и ещё {len(data['emails'])-5}")

    if data.get('telegrams'):
        lines.append(f"\n💬 Telegram: {', '.join(data['telegrams'])}")

    if data.get('cards'):
        lines.append(f"\n💳 Карты: {', '.join(data['cards'][:3])}")

    lines.append(f"\n👁 Интересовались этим: {views}")
    return "\n".join(lines)

def format_generic_report(data: dict, views: int, title: str) -> str:
    lines = [f"🔍 {title}"]
    lines.append(f"├ Запрос: {data.get('query', '')}")
    for key, label in [
        ('fio', 'ФИО'), ('organization', 'Организация'), ('director', 'Директор'),
        ('address', 'Адрес'), ('birthdate', 'Дата рождения'), ('age', 'Возраст'),
        ('city', 'Город'), ('country', 'Страна'), ('location', 'Локация'),
        ('isp', 'Провайдер'), ('registration_date', 'Дата регистрации'),
        ('status', 'Статус'), ('operator', 'Оператор'), ('region', 'Регион')
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
    if data.get('gifts'):
        lines.append(f"\n🎁 Подарки: {', '.join(data['gifts'][:3])}")
    if data.get('name_history'):
        lines.append(f"\n🕓 История имён: {', '.join([x['name'] if isinstance(x, dict) else str(x) for x in data['name_history'][:3]])}")
    if data.get('username_history'):
        lines.append(f"\n🕓 История юзернеймов: {', '.join([x['username'] if isinstance(x, dict) else str(x) for x in data['username_history'][:3]])}")
    if data.get('domains'):
        lines.append(f"\n🌐 Домены: {', '.join(data['domains'][:3])}")
    lines.append(f"\n👁 Интересовались этим: {views}")
    return "\n".join(lines)

def format_inn_report(data: dict) -> str:
    lines = ["🏛️ *Отчёт по ИНН*"]
    lines.append("")
    lines.append(f"📌 ИНН: `{data.get('query', '')}`")
    if data.get('organization'):
        lines.append(f"🏢 Организация: {data['organization']}")
    if data.get('director'):
        lines.append(f"👤 Директор: {data['director']}")
    if data.get('address'):
        lines.append(f"🏠 Адрес: {data['address']}")
    if data.get('phone'):
        lines.append(f"📞 Телефон: {data['phone']}")
    if data.get('email'):
        lines.append(f"📧 Email: {data['email']}")
    if data.get('status'):
        lines.append(f"📊 Статус: {data['status']}")
    if not any([data.get('organization'), data.get('director'), data.get('address')]):
        lines.append("")
        lines.append("❌ *Ничего не найдено по данному ИНН.*")
    lines.append("")
    lines.append("📌 *Источники:* BigBase, DepSearch, NightSearch")
    return "\n".join(lines)

def format_ip_report(data: dict) -> str:
    lines = ["🌐 *Отчёт по IP-адресу*"]
    lines.append("")
    lines.append(f"📌 IP: `{data.get('query', '')}`")
    if data.get('location'):
        lines.append(f"📍 Локация: {data['location']}")
    if data.get('isp'):
        lines.append(f"📡 Провайдер: {data['isp']}")
    if data.get('fio'):
        lines.append(f"👤 ФИО: {', '.join(data['fio'])}")
    if data.get('phone'):
        lines.append(f"📞 Телефоны: {', '.join(data['phone'])}")
    if data.get('address'):
        lines.append(f"🏠 Адреса: {', '.join(data['address'])}")
    if data.get('email'):
        lines.append(f"📧 Email: {', '.join(data['email'])}")
    if data.get('domains'):
        lines.append("")
        lines.append("🌐 Связанные домены:")
        for domain in data['domains'][:5]:
            lines.append(f"  • {domain}")
    if not any([data.get('location'), data.get('isp'), data.get('fio'), data.get('domains')]):
        lines.append("")
        lines.append("❌ *Ничего не найдено по данному IP.*")
    lines.append("")
    lines.append("📌 *Источники:* BigBase, DepSearch, NightSearch")
    return "\n".join(lines)

def format_email_report(data: dict) -> str:
    lines = ["📧 *Отчёт по Email*"]
    lines.append("")
    lines.append(f"📌 Email: `{data.get('query', '')}`")
    if data.get('fio'):
        lines.append(f"👤 ФИО: {', '.join(data['fio'][:5])}")
    if data.get('phones'):
        lines.append(f"📞 Телефоны: {', '.join(data['phones'][:5])}")
    if data.get('addresses'):
        lines.append(f"🏠 Адреса: {', '.join(data['addresses'][:3])}")
    if data.get('socials'):
        lines.append(f"🌐 Соцсети: {', '.join(data['socials'][:3])}")
    if data.get('telegrams'):
        lines.append(f"💬 Telegram: {', '.join(data['telegrams'][:3])}")
    if not any([data.get('fio'), data.get('phones'), data.get('addresses'), data.get('socials')]):
        lines.append("")
        lines.append("❌ *Ничего не найдено по данному Email.*")
    lines.append("")
    lines.append("📌 *Источники:* BigBase, Snusbase, SEON")
    return "\n".join(lines)

def format_vk_report(data: dict) -> str:
    lines = ["🧑‍💻 *Отчёт по VK*"]
    lines.append("")
    lines.append(f"📌 ID: `{data.get('vk_id', data.get('query', ''))}`")
    if data.get('name'):
        lines.append(f"👤 Имя: {data['name']}")
    if data.get('birthdate'):
        lines.append(f"📅 Дата рождения: {data['birthdate']}")
    if data.get('city'):
        lines.append(f"🌆 Город: {data['city']}")
    if data.get('country'):
        lines.append(f"🌍 Страна: {data['country']}")
    if data.get('phones'):
        lines.append(f"📞 Телефоны: {', '.join(data['phones'][:5])}")
    if data.get('emails'):
        lines.append(f"📧 Email: {', '.join(data['emails'][:5])}")
    if data.get('telegrams'):
        lines.append(f"💬 Telegram: {', '.join(data['telegrams'][:3])}")
    if data.get('groups'):
        lines.append(f"👥 Группы: {', '.join(data['groups'][:3])}")
    if not any([data.get('name'), data.get('birthdate'), data.get('city'), data.get('phones')]):
        lines.append("")
        lines.append("❌ *Ничего не найдено по данному VK.*")
    lines.append("")
    lines.append("📌 *Источники:* BigBase, Jitler (vks)")
    return "\n".join(lines)

def format_fio_report(data: dict) -> str:
    lines = ["👤 *Отчёт по ФИО*"]
    lines.append("")
    lines.append(f"📌 ФИО: `{data.get('fio', data.get('query', ''))}`")
    if data.get('phones'):
        lines.append(f"📞 Телефоны: {', '.join(data['phones'][:5])}")
    if data.get('emails'):
        lines.append(f"📧 Email: {', '.join(data['emails'][:5])}")
    if data.get('addresses'):
        lines.append(f"🏠 Адреса: {', '.join(data['addresses'][:3])}")
    if data.get('telegrams'):
        lines.append(f"💬 Telegram: {', '.join(data['telegrams'][:3])}")
    if data.get('vk'):
        lines.append(f"🧑‍💻 ВКонтакте: {data['vk']}")
    if data.get('ok'):
        lines.append(f"👨‍🦳 Одноклассники: {data['ok']}")
    if not any([data.get('phones'), data.get('emails'), data.get('addresses'), data.get('telegrams')]):
        lines.append("")
        lines.append("❌ *Ничего не найдено по данному ФИО.*")
    lines.append("")
    lines.append("📌 *Источники:* BigBase, DepSearch, NightSearch")
    return "\n".join(lines)

# ===== HTML-ОТЧЁТ ДЛЯ ТЕЛЕФОНА (КРАСИВЫЙ) =====
def generate_html_report(data: dict, views: int, title: str = "Отчёт по номеру телефона") -> str:
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title}</title>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{
                font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, Arial, sans-serif;
                background: linear-gradient(135deg, #0a0e1a 0%, #1a1a2e 100%);
                min-height: 100vh;
                display: flex;
                justify-content: center;
                padding: 40px 20px;
            }}
            .report {{
                max-width: 820px;
                width: 100%;
                background: rgba(255,255,255,0.03);
                backdrop-filter: blur(20px);
                border-radius: 32px;
                padding: 40px 35px;
                border: 1px solid rgba(255,255,255,0.06);
                box-shadow: 0 25px 60px rgba(0,0,0,0.7);
            }}
            .header {{
                display: flex;
                align-items: center;
                gap: 16px;
                margin-bottom: 32px;
                border-bottom: 1px solid rgba(255,255,255,0.06);
                padding-bottom: 24px;
            }}
            .header-icon {{
                font-size: 36px;
                background: linear-gradient(135deg, #00d4ff, #7b2ffc);
                width: 64px;
                height: 64px;
                border-radius: 18px;
                display: flex;
                align-items: center;
                justify-content: center;
                box-shadow: 0 8px 24px rgba(0, 212, 255, 0.2);
            }}
            .header h1 {{
                color: #fff;
                font-size: 26px;
                font-weight: 700;
                letter-spacing: -0.5px;
            }}
            .header .sub {{
                color: rgba(255,255,255,0.35);
                font-size: 14px;
                font-weight: 400;
                margin-top: 2px;
            }}
            .badge-osint {{
                background: rgba(0, 212, 255, 0.12);
                border: 1px solid rgba(0, 212, 255, 0.15);
                padding: 4px 14px;
                border-radius: 40px;
                font-size: 11px;
                font-weight: 600;
                color: #00d4ff;
                letter-spacing: 0.5px;
                text-transform: uppercase;
                margin-left: auto;
            }}
            .section {{
                background: rgba(255,255,255,0.04);
                border-radius: 20px;
                padding: 20px 24px;
                margin-bottom: 16px;
                border: 1px solid rgba(255,255,255,0.05);
                transition: all 0.2s ease;
            }}
            .section:hover {{
                background: rgba(255,255,255,0.07);
                border-color: rgba(255,255,255,0.1);
                transform: translateY(-1px);
            }}
            .section-title {{
                font-size: 13px;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 1px;
                color: rgba(255,255,255,0.25);
                margin-bottom: 14px;
                display: flex;
                align-items: center;
                gap: 8px;
            }}
            .section-title span {{ font-size: 18px; line-height: 1; }}
            .row {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 7px 0;
                border-bottom: 1px solid rgba(255,255,255,0.04);
            }}
            .row:last-child {{ border-bottom: none; }}
            .label {{
                color: rgba(255,255,255,0.45);
                font-size: 14px;
                font-weight: 400;
            }}
            .value {{
                color: #fff;
                font-size: 15px;
                font-weight: 500;
                text-align: right;
                word-break: break-word;
                max-width: 65%;
            }}
            .value a {{
                color: #7bb8ff;
                text-decoration: none;
                transition: color 0.2s;
            }}
            .value a:hover {{
                color: #a8d4ff;
                text-decoration: underline;
            }}
            .value .highlight {{
                background: linear-gradient(135deg, #00d4ff, #7b2ffc);
                padding: 3px 14px;
                border-radius: 30px;
                font-size: 14px;
                font-weight: 600;
                color: #fff;
                display: inline-block;
            }}
            .phone-books {{
                display: flex;
                flex-wrap: wrap;
                gap: 6px;
                justify-content: flex-end;
            }}
            .phone-book-tag {{
                background: rgba(255,255,255,0.07);
                padding: 4px 16px;
                border-radius: 30px;
                font-size: 13px;
                color: #ccc;
                border: 1px solid rgba(255,255,255,0.05);
            }}
            .email-tag {{
                background: rgba(123, 184, 255, 0.08);
                padding: 2px 12px;
                border-radius: 20px;
                font-size: 13px;
                color: #7bb8ff;
                border: 1px solid rgba(123, 184, 255, 0.1);
            }}
            .footer {{
                margin-top: 30px;
                text-align: center;
                color: rgba(255,255,255,0.15);
                font-size: 12px;
                border-top: 1px solid rgba(255,255,255,0.05);
                padding-top: 24px;
            }}
            .views {{
                color: rgba(255,255,255,0.4);
                font-size: 14px;
            }}
            .views span {{ color: #fff; font-weight: 600; }}
            @media (max-width: 600px) {{
                .report {{ padding: 24px 16px; }}
                .row {{ flex-wrap: wrap; gap: 4px; }}
                .value {{ text-align: left; max-width: 100%; width: 100%; }}
                .phone-books {{ justify-content: flex-start; }}
                .header {{ flex-wrap: wrap; }}
                .badge-osint {{ margin-left: 0; }}
            }}
        </style>
    </head>
    <body>
        <div class="report">
            <div class="header">
                <div class="header-icon">📱</div>
                <div>
                    <h1>{title}</h1>
                    <div class="sub">Данные из открытых источников</div>
                </div>
                <div class="badge-osint">🔍 OSINT</div>
            </div>
    """

    # Телефон
    html += f"""
            <div class="section">
                <div class="section-title"><span>📞</span> Номер телефона</div>
                <div class="row">
                    <span class="label">Номер</span>
                    <span class="value"><span class="highlight">{data.get('query', '')}</span></span>
                </div>
    """
    if data.get('operator'):
        html += f"""
                <div class="row">
                    <span class="label">📡 Оператор</span>
                    <span class="value">{data['operator']}</span>
                </div>
        """
    if data.get('region'):
        html += f"""
                <div class="row">
                    <span class="label">📍 Регион</span>
                    <span class="value">{data['region']}</span>
                </div>
        """
    if data.get('country'):
        html += f"""
                <div class="row">
                    <span class="label">🌍 Страна</span>
                    <span class="value">{data['country']}</span>
                </div>
        """
    html += "</div>"

    # Основные данные
    has_personal = data.get('fio') or data.get('birthdate') or data.get('age') is not None or data.get('address')
    if has_personal:
        html += """
            <div class="section">
                <div class="section-title"><span>👤</span> Основные данные</div>
        """
        if data.get('fio'):
            html += f"""
                <div class="row">
                    <span class="label">ФИО</span>
                    <span class="value">{data['fio']}</span>
                </div>
            """
        if data.get('birthdate'):
            html += f"""
                <div class="row">
                    <span class="label">📅 Дата рождения</span>
                    <span class="value">{data['birthdate']}</span>
                </div>
            """
        if data.get('age') is not None:
            html += f"""
                <div class="row">
                    <span class="label">🎂 Возраст</span>
                    <span class="value">{data['age']} лет</span>
                </div>
            """
        if data.get('address'):
            html += f"""
                <div class="row">
                    <span class="label">🏠 Адрес</span>
                    <span class="value">{data['address']}</span>
                </div>
            """
        html += "</div>"

    # Телефонные книги
    if data.get('phone_books'):
        books = data['phone_books'][:20]
        html += """
            <div class="section">
                <div class="section-title"><span>🔎</span> Телефонные книги</div>
                <div class="row">
                    <span class="label">Контакты</span>
                    <div class="value">
                        <div class="phone-books">
        """
        for book in books:
            html += f'<span class="phone-book-tag">{book}</span>'
        if len(data['phone_books']) > 20:
            html += f'<span class="phone-book-tag" style="background:rgba(255,255,255,0.03);color:rgba(255,255,255,0.3);">+{len(data["phone_books"])-20}</span>'
        html += """
                        </div>
                    </div>
                </div>
            </div>
        """

    # Соцсети
    has_social = data.get('vk') or data.get('ok') or data.get('instagram') or data.get('tiktok')
    if has_social:
        html += """
            <div class="section">
                <div class="section-title"><span>🌐</span> Социальные сети</div>
        """
        if data.get('vk'):
            html += f"""
                <div class="row">
                    <span class="label">🧑‍💻 ВКонтакте</span>
                    <span class="value"><a href="{data['vk']}" target="_blank">{data['vk']}</a></span>
                </div>
            """
        if data.get('ok'):
            html += f"""
                <div class="row">
                    <span class="label">👨‍🦳 Одноклассники</span>
                    <span class="value"><a href="{data['ok']}" target="_blank">{data['ok']}</a></span>
                </div>
            """
        if data.get('tiktok'):
            html += f"""
                <div class="row">
                    <span class="label">👩‍🦲 TikTok</span>
                    <span class="value"><a href="{data['tiktok']}" target="_blank">{data['tiktok']}</a></span>
                </div>
            """
        if data.get('instagram'):
            html += f"""
                <div class="row">
                    <span class="label">📷 Instagram</span>
                    <span class="value"><a href="{data['instagram']}" target="_blank">{data['instagram']}</a></span>
                </div>
            """
        html += "</div>"

    # Email
    if data.get('emails'):
        html += """
            <div class="section">
                <div class="section-title"><span>📧</span> E-mail</div>
                <div class="row">
                    <span class="label">Email</span>
                    <span class="value">
        """
        emails = data['emails'][:10]
        html += ', '.join([f'<a href="mailto:{e}" style="color:#7bb8ff;display:inline-block;margin:2px 0;">{e}</a>' for e in emails])
        if len(data['emails']) > 10:
            html += f' <span style="color:rgba(255,255,255,0.25);font-size:13px;">+{len(data["emails"])-10}</span>'
        html += """
                    </span>
                </div>
            </div>
        """

    # Telegram
    if data.get('telegrams'):
        html += """
            <div class="section">
                <div class="section-title"><span>💬</span> Telegram</div>
                <div class="row">
                    <span class="label">Аккаунты</span>
                    <span class="value">
        """
        tgs = []
        for tg in data['telegrams']:
            if isinstance(tg, str):
                if '[' in tg and ']' in tg:
                    parts = tg.split('[')
                    username = parts[0].strip()
                    user_id = parts[1].replace(']', '').strip()
                    tgs.append(f'<a href="tg://resolve?domain={username.replace("@","")}" style="color:#7bb8ff;">{username}</a> [{user_id}]')
                elif tg.startswith('@'):
                    tgs.append(f'<a href="tg://resolve?domain={tg.replace("@","")}" style="color:#7bb8ff;">{tg}</a>')
                else:
                    tgs.append(tg)
        html += ', '.join(tgs)
        html += """
                    </span>
                </div>
            </div>
        """

    # Карты и банки (если есть)
    if data.get('cards'):
        html += """
            <div class="section">
                <div class="section-title"><span>💳</span> Банковские карты</div>
        """
        for card in data['cards'][:5]:
            html += f"""
                <div class="row">
                    <span class="label">Карта</span>
                    <span class="value">{card}</span>
                </div>
            """
        html += "</div>"

    if data.get('banks'):
        html += """
            <div class="section">
                <div class="section-title"><span>🏦</span> Банки</div>
        """
        for bank in data['banks'][:5]:
            html += f"""
                <div class="row">
                    <span class="label">Банк</span>
                    <span class="value">{bank}</span>
                </div>
            """
        html += "</div>"

    # Просмотры
    html += f"""
            <div class="footer">
                <div class="views">👁 <span>{views}</span> { 'человек' if views%10!=1 or views%100==11 else 'человека' } интересовались этим</div>
                <div style="margin-top:8px;font-size:11px;color:rgba(255,255,255,0.1);">Данные получены из открытых источников</div>
            </div>
        </div>
    </body>
    </html>
    """
    return html

# ===== СТАРТОВОЕ СООБЩЕНИЕ =====
def get_search_start_message(search_type: str, query: str) -> str:
    type_names = {
        "phone": "номеру телефона",
        "telegram": "Telegram ID",
        "email": "Email",
        "ip": "IP-адресу",
        "vk": "VK",
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

# ===== БОТ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ===== КОМАНДЫ =====
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

    text = """🕵️ *Dataseeker* — твой бесплатный цифровой детектив.

*Типы поиска:*

┌ *Соцсети:*
├ Telegram →  `id1234567890`
└ VK → `vk.com/id1234567`

┌ *Контакты:*
├ Телефон → `+79999999999` 
└ Email → `ivanov@gmail.com`

┌ *Онлайн-следы:*
└ IP → `185.85.219.243`

┌ *Физ. лица:*
├ ИНН → `/inn 123456789012`
└ ФИО → `Иванов Иван Иванович`

Каждые 24 часа выдаётся по 5 бесплатных запросов.

🚀 Просто отправь данные для поиска!"""
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

    status_msg = await message.reply(get_search_start_message("inn", query))
    data = await collect_inn_data(query)
    views = 0
    report = format_inn_report(data)
    await status_msg.edit_text(report, parse_mode="Markdown")

@dp.message(lambda msg: msg.text and not msg.text.startswith('/'))
async def universal_handler(message: types.Message):
    text = message.text.strip()
    if not text:
        return

    if re.match(r'^\+?\d{10,15}$', re.sub(r'\s+', '', text)):
        query = re.sub(r'\D', '', text)
        search_type = "phone"
    elif re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', text):
        query = text
        search_type = "email"
    elif re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', text):
        query = text
        search_type = "ip"
    elif re.match(r'^https?://(www\.)?vk\.com/', text):
        query = text
        search_type = "vk"
    elif text.isdigit() and len(text) >= 8:
        query = text
        search_type = "telegram"
    elif text.startswith('@'):
        query = text
        search_type = "telegram"
    elif re.search(r'[А-Яа-я]+\s+[А-Яа-я]+\s+[А-Яа-я]+', text):
        query = text
        search_type = "fio"
    else:
        query = text
        search_type = "unknown"

    status_msg = await message.reply(get_search_start_message(search_type, query))

    if search_type == "phone":
        data = await collect_phone_data(query)
        views = await get_unique_views_phone(query, message.from_user.id)
        report = format_phone_report(data, views)
        # Для телефона — сохраняем отчёт для HTML-кнопки
        await save_report(query, data)
        # Генерируем HTML и кнопку
        html_content = generate_html_report(data, views, "Отчёт по номеру телефона")
        html_bytes = html_content.encode('utf-8')
        html_file = BufferedInputFile(html_bytes, filename=f"phone_report_{query}.html")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📄 Полный отчёт (HTML)", callback_data=f"html_phone_{query}")]
        ])
        await status_msg.edit_text(report, parse_mode="Markdown", reply_markup=keyboard)
        await message.reply_document(html_file, caption="📄 Красивый отчёт в HTML")
    elif search_type == "telegram":
        data = await collect_telegram_data(query)
        views = await get_unique_views_id(query, message.from_user.id)
        report = format_generic_report(data, views, "Telegram ID")
        await status_msg.edit_text(report, parse_mode="Markdown")
    elif search_type == "email":
        data = await collect_email_data(query)
        views = 0
        report = format_email_report(data)
        await status_msg.edit_text(report, parse_mode="Markdown")
    elif search_type == "ip":
        data = await collect_ip_data(query)
        views = 0
        report = format_ip_report(data)
        await status_msg.edit_text(report, parse_mode="Markdown")
    elif search_type == "vk":
        data = await collect_vk_data(query)
        views = 0
        report = format_vk_report(data)
        await status_msg.edit_text(report, parse_mode="Markdown")
    elif search_type == "fio":
        data = await collect_fio_data(query)
        views = 0
        report = format_fio_report(data)
        await status_msg.edit_text(report, parse_mode="Markdown")
    else:
        data = {'query': query, 'raw_data': ['Неизвестный тип запроса']}
        views = 0
        report = "❌ Неизвестный тип запроса"
        await status_msg.edit_text(report, parse_mode="Markdown")

@dp.callback_query(lambda c: c.data and c.data.startswith("html_phone_"))
async def html_phone_callback(callback: types.CallbackQuery):
    query = callback.data.replace("html_phone_", "")
    data = await get_report(query)
    if not data:
        await callback.answer("Отчёт не найден, повторите поиск")
        return
    views = await get_unique_views_phone(query, callback.from_user.id)
    html_content = generate_html_report(data, views, "Отчёт по номеру телефона")
    html_bytes = html_content.encode('utf-8')
    html_file = BufferedInputFile(html_bytes, filename=f"phone_report_{query}.html")
    await callback.message.reply_document(html_file, caption="📄 Красивый HTML-отчёт")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "profile")
async def profile_callback(callback: types.CallbackQuery):
    ref_code = await get_referral_code(callback.from_user.id)
    ref_count = await get_referral_stats(callback.from_user.id)
    user = await get_user(callback.from_user.id)
    daily = await get_daily_requests(callback.from_user.id)
    days_ago = (datetime.now() - user['created_at']).days if user else 0

    text = (
        f"👤 **Мой профиль**\n"
        f"Ваш ID: `{callback.from_user.id}`\n"
        f"Дата регистрации: `{user['created_at'].strftime('%d.%m.%Y %H:%M') if user else 'Неизвестно'}`\n"
        f"(Вы агент уже: {days_ago} дней)\n\n"
        f"📊 Доступно запросов сегодня: {5 - daily} из 5\n"
        f"👥 Приглашено друзей: {ref_count}\n"
        f"🔗 Реферальный код: `{ref_code}`"
    )
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "referral")
async def referral_callback(callback: types.CallbackQuery):
    ref_code = await get_referral_code(callback.from_user.id)
    ref_count = await get_referral_stats(callback.from_user.id)
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{ref_code}"

    text = (
        "🤝 **Партнёрская программа**\n\n"
        "Приглашайте друзей и получайте бонусы!\n\n"
        f"👥 Приглашено: {ref_count}\n"
        f"🔗 Ваша реферальная ссылка:\n{link}\n\n"
        "За каждого приглашённого друга вы получаете бонусный запрос!"
    )
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()

async def health_check(request):
    return web.Response(text="OK", status=200)

async def main():
    await init_db()
    print("🚀 Бот запущен")

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
