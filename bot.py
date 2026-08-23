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
TELELOG_TOKEN = os.getenv("TELELOG_TOKEN", "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiIyMDMzMDI5NDc1IiwianRpIjoiODJmMjlmNzQtYmJlMi00ZGUwLWEwZDQtN2EzMDJhMWE5MDViIiwiZXhwIjoxODAxMDA4MzM4fQ.Mba4aX85YAMcaMLfhUBzXtCoNmEujfMe-6sGBbp3kT-T2SiLM_Ho0BBAFAQ8_C6Gz06PH9mAYhfBvlLSjb4oVd1Fm_vmb8MC-wuObU3qgfGrYdGzVF3ntJHv-LdNELq-jsqvQOY3jq9meso9dUoyj5SviDQWL6cvnRQ03kpHWxA")

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
    
    headers = {
        "Authorization": BIGBASE_TOKEN,
        "Content-Type": "application/json"
    }
    
    payload = {
        "search": query,
        "page": 0
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                
                text = await resp.text()
                
                if resp.status != 200:
                    return {}
                
                return json.loads(text)
                
    except Exception as e:
        print(f"❌ BIGBASE ERROR: {repr(e)}")
        return {}

async def nightsearch_search(query: str):
    if not NIGHTSEARCH_API_KEY:
        return {}
    url = "https://nightsearch.life/api/search"
    headers = {"X-API-Key": NIGHTSEARCH_API_KEY, "Content-Type": "application/json"}
    payload = {"query": query, "search_type": "phone"}
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
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
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
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
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception:
            return {}

async def depsearch_search(query: str):
    if not DEPSEARCH_TOKEN or not DEPSEARCH_BASE:
        return {}
    url = f"{DEPSEARCH_BASE}/quest={query}&token={DEPSEARCH_TOKEN}&lang=ru"
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.get(url) as resp:
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
                                for poll_attempt in range(10):
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
    """Получает историю имён из TeleLog"""
    if not TELELOG_TOKEN:
        return []
    
    url = f"https://telelog.info/api/v1/users/{user_id}/names"
    headers = {
        "accept": "text/plain",
        "Authorization": f"Bearer {TELELOG_TOKEN}"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('success'):
                        result = data.get('data', [])
                        print(f"📛 TeleLog names: {len(result)} записей")
                        return result
                return []
    except Exception as e:
        print(f"❌ TeleLog names error: {repr(e)}")
        return []

async def telelog_get_usernames(user_id: str):
    """Получает историю юзернеймов из TeleLog"""
    if not TELELOG_TOKEN:
        return []
    
    url = f"https://telelog.info/api/v1/users/{user_id}/usernames"
    headers = {
        "accept": "text/plain",
        "Authorization": f"Bearer {TELELOG_TOKEN}"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('success'):
                        result = data.get('data', [])
                        print(f"👤 TeleLog usernames: {len(result)} записей")
                        return result
                return []
    except Exception as e:
        print(f"❌ TeleLog usernames error: {repr(e)}")
        return []

async def telelog_get_gifts(user_id: str):
    """Получает подарки из TeleLog"""
    if not TELELOG_TOKEN:
        return []
    
    url = f"https://telelog.info/api/v1/users/{user_id}/gifts_relation"
    headers = {
        "accept": "text/plain",
        "Authorization": f"Bearer {TELELOG_TOKEN}"
    }
    
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

# ===== СБОР ДАННЫХ ДЛЯ ТЕЛЕФОНА =====
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
        'emails': [],
        'telegrams': [],
        'vk': None,
        'instagram': None,
        'tiktok': None,
        'ok': None,
        'phone_books': []
    }
    
    all_birthdates = []
    
    # ---- BIGBASE ----
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
        
        # email
        for record in bigbase.get('records', []):
            base_record = record.get('base_record', [])
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).lower()
                    if 'почта' in key or 'email' in key or 'mail' in key:
                        value = item[1]
                        if value:
                            result['emails'].append(str(value))
        for conn in bigbase.get('connections', {}).get('person', []):
            email = deep_find(conn, 'email') or deep_find(conn, 'mail')
            if email:
                result['emails'].append(str(email))
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
    
    # ---- FALLBACK DEPSEARCH ----
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
    
    # ---- ДРУГИЕ API ----
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
    
    # ---- JITLER ----
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
    
    result['emails'] = normalize_list(result['emails'])
    result['telegrams'] = list(dict.fromkeys(result['telegrams']))
    result['phone_books'] = list(dict.fromkeys(result['phone_books']))
    
    return result

# ===== СБОР ДАННЫХ ДЛЯ ID (BIGBASE + JITLER + TELELOG) =====
async def collect_id_data(query: str, search_type: str):
    print(f"🔍 Поиск ID: {query}, тип: {search_type}")
    
    tasks = [
        bigbase_search(query),
        jitler_search_with_balancer(query, search_type),
        telelog_get_names(query),
        telelog_get_usernames(query),
        telelog_get_gifts(query)
    ]
    
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=16.0
        )
    except asyncio.TimeoutError:
        print(f"⏱️ Таймаут для {query}")
        results = [None, None, None, None, None]
    
    bigbase = results[0] if isinstance(results[0], dict) else {}
    jitler = results[1] if isinstance(results[1], dict) else {}
    telelog_names = results[2] if isinstance(results[2], list) else []
    telelog_usernames = results[3] if isinstance(results[3], list) else []
    telelog_gifts = results[4] if isinstance(results[4], list) else []
    
    print(f"✅ BigBase: {'есть' if bigbase else 'нет'}")
    print(f"✅ Jitler: {'есть' if jitler else 'нет'}")
    print(f"✅ TeleLog names: {len(telelog_names)}")
    print(f"✅ TeleLog usernames: {len(telelog_usernames)}")
    print(f"✅ TeleLog gifts: {len(telelog_gifts)}")
    
    result = {
        'query': query,
        'telegrams': [],
        'groups': [],
        'name_history': [],
        'phone_books': [],
        'gifts': [],
        'vk': None,
        'instagram': None,
        'tiktok': None,
        'ok': None
    }
    
    # ---- BIGBASE ----
    if bigbase and isinstance(bigbase, dict):
        records = bigbase.get('records', [])
        
        for record in records:
            base_record = record.get('base_record', [])
            
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip().lower()
                    value = item[1]
                    
                    if 'актуальный username' in key:
                        if value:
                            result['telegrams'].append(str(value).strip())
                    
                    elif 'известные username' in key:
                        if isinstance(value, list):
                            for username_list in value:
                                if isinstance(username_list, list):
                                    for username_item in username_list:
                                        if isinstance(username_item, list) and len(username_item) >= 2:
                                            username = str(username_item[1]).strip()
                                            if username and username not in result['telegrams']:
                                                result['telegrams'].append(username)
                    
                    elif 'история имён' in key or 'история имен' in key:
                        if isinstance(value, list):
                            for name_list in value:
                                if isinstance(name_list, list):
                                    for name_item in name_list:
                                        if isinstance(name_item, list) and len(name_item) >= 2:
                                            name = str(name_item[1]).strip()
                                            if name:
                                                result['name_history'].append({'date': None, 'name': name})
                    
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
                                            if group_username.startswith('@'):
                                                result['groups'].append(f"{group_name}: {group_username}")
                                            else:
                                                result['groups'].append(f"{group_name}: @{group_username}")
                                        else:
                                            result['groups'].append(group_name)
    
    # ---- JITLER ----
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
            if formatted and formatted not in result['telegrams']:
                result['telegrams'].append(formatted)
    
    # ---- TELELOG ----
    # История имён
    for name_entry in telelog_names:
        if isinstance(name_entry, dict):
            date = name_entry.get('date_time') or name_entry.get('date') or name_entry.get('created_at')
            name = name_entry.get('name') or name_entry.get('first_name') or name_entry.get('last_name')
            if name:
                if date:
                    try:
                        dt = datetime.fromisoformat(date.replace('Z', '+00:00'))
                        date_str = dt.strftime('%d.%m.%Y %H:%M')
                    except:
                        date_str = str(date)[:10]
                else:
                    date_str = None
                result['name_history'].append({'date': date_str, 'name': name})
        elif isinstance(name_entry, str):
            result['name_history'].append({'date': None, 'name': name_entry})
    
    # История юзернеймов
    for username_entry in telelog_usernames:
        if isinstance(username_entry, dict):
            username = username_entry.get('name') or username_entry.get('username')
            if username:
                if not username.startswith('@'):
                    username = '@' + username
                if username not in result['telegrams']:
                    result['telegrams'].append(username)
        elif isinstance(username_entry, str):
            username = username_entry
            if not username.startswith('@'):
                username = '@' + username
            if username not in result['telegrams']:
                result['telegrams'].append(username)
    
    # Подарки
    for gift in telelog_gifts:
        if isinstance(gift, dict):
            gift_id = gift.get('id') or gift.get('gift_id')
            if gift_id:
                result['gifts'].append(str(gift_id))
        elif isinstance(gift, str):
            result['gifts'].append(gift)
    
    # ---- НОРМАЛИЗАЦИЯ ----
    result['telegrams'] = list(dict.fromkeys([x for x in result['telegrams'] if x]))
    result['groups'] = list(dict.fromkeys([x for x in result['groups'] if x]))
    result['phone_books'] = list(dict.fromkeys([x for x in result['phone_books'] if x]))
    result['gifts'] = list(dict.fromkeys([x for x in result['gifts'] if x]))
    
    # История имён — убираем дубликаты
    seen_names = set()
    unique_name_history = []
    for entry in result['name_history']:
        key = entry.get('name', '')
        if key and key not in seen_names:
            seen_names.add(key)
            unique_name_history.append(entry)
    result['name_history'] = unique_name_history
    
    print(f"📊 Результат: телеграм={len(result['telegrams'])}, группы={len(result['groups'])}, история={len(result['name_history'])}, книги={len(result['phone_books'])}, подарки={len(result['gifts'])}")
    return result

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
            lines.append(f"└ Возраст: {data['age']}")
    
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
        lines.append(f"\n📧 E-mail: {', '.join(data['emails'])}")
    
    if data.get('telegrams'):
        lines.append(f"\n💬 Telegram: {', '.join(data['telegrams'])}")
    
    lines.append(f"\n👁 Интересовались этим: {views}")
    return "\n".join(lines)

def format_id_report(data: dict, views: int) -> str:
    lines = ["```"]
    if data.get('query'):
        lines.append(f"# ID: {data['query']}")
    
    # Телефонные книги
    if data.get('phone_books'):
        books = data['phone_books'][:15]
        lines.append(f"📱 Телефонные книги: {', '.join(books)}")
        lines.append("")
    
    # Соцсети
    if data.get('vk'):
        lines.append(f"# ВКонтакте: {data['vk']}")
    if data.get('ok'):
        lines.append(f"# Одноклассники: {data['ok']}")
    if data.get('instagram'):
        lines.append(f"# Instagram: {data['instagram']}")
    if data.get('tiktok'):
        lines.append(f"# TikTok: {data['tiktok']}")
    
    # Найденные аккаунты (Telegram)
    if data.get('telegrams'):
        lines.append(f"# Найденные аккаунты: {', '.join(data['telegrams'])}")
    
    # Группы
    if data.get('groups'):
        lines.append(f"\n👥 Группы [{len(data['groups'])}]:")
        for group in data['groups'][:15]:
            lines.append(str(group))
    
    # ИСТОРИЯ ИМЁН (ВСЯ)
    if data.get('name_history'):
        lines.append(f"\n🕓 История изменения имени:")
        for entry in data['name_history'][:15]:
            if isinstance(entry, dict):
                date = entry.get('date')
                name = entry.get('name', '')
                if date:
                    lines.append(f"  {date} → {name}")
                else:
                    lines.append(f"  → {name}")
            else:
                lines.append(f"  → {str(entry)}")
    
    # Подарки
    if data.get('gifts'):
        lines.append(f"\n🎁 Подарочные связи: {', '.join(data['gifts'][:20])}")
    
    lines.append(f"\n👁 Интересовались этим: {views}")
    lines.append("```")
    return "\n".join(lines)

async def show_direction_choice(message: types.Message, id_str: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="ИНН (юр. лицо)", callback_data=f"dir_inn_{id_str}")],
        [InlineKeyboardButton(text="Паспорт", callback_data=f"dir_passport_{id_str}")],
        [InlineKeyboardButton(text="Водительское удостоверение", callback_data=f"dir_driver_{id_str}")],
        [InlineKeyboardButton(text="ВКонтакте", callback_data=f"dir_vk_{id_str}")],
        [InlineKeyboardButton(text="Telegram", callback_data=f"dir_tg_{id_str}")]
    ])
    await message.reply(f"Обнаружен идентификатор: {id_str}\n\nВыберите направление для поиска:", reply_markup=keyboard)

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
    await message.reply("Отправьте номер телефона для поиска", reply_markup=keyboard)

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

@dp.callback_query(lambda c: c.data and c.data.startswith("dir_"))
async def direction_callback(callback: types.CallbackQuery):
    print(f"🔥 CALLBACK: {callback.data}")
    
    parts = callback.data.split("_", 2)
    if len(parts) < 3:
        await callback.answer("Ошибка формата")
        return
    direction = parts[1]
    id_str = parts[2]
    
    print(f"🔎 START SEARCH: id={id_str}, type={direction}")

    cache_key = f"{id_str}:{direction}"
    cached = await get_id_report(cache_key)
    if cached:
        data = cached.get('data', {})
        views = await get_unique_views_id(id_str, callback.from_user.id)
        detailed = format_id_report(data, views)
        await callback.message.edit_text(detailed, parse_mode="Markdown")
        await callback.answer()
        return

    search_type_map = {
        "inn": "sherlock",
        "passport": "sherlock",
        "driver": "sherlock",
        "vk": "vks",
        "tg": "sherlock"
    }
    search_type = search_type_map.get(direction)
    if not search_type:
        await callback.answer("Неизвестное направление")
        return

    await callback.message.edit_text(f"🔍 Поиск по {direction}...")

    try:
        data = await asyncio.wait_for(collect_id_data(id_str, search_type), timeout=16.0)
    except asyncio.TimeoutError:
        await callback.message.edit_text("⏱️ Поиск занял слишком много времени. Попробуйте позже.")
        await callback.answer()
        return

    views = await get_unique_views_id(id_str, callback.from_user.id)

    has_data = any([
        data.get('telegrams'),
        data.get('groups'),
        data.get('name_history'),
        data.get('phone_books'),
        data.get('gifts'),
        data.get('vk'),
        data.get('ok'),
        data.get('instagram'),
        data.get('tiktok')
    ])

    if not has_data:
        no_data_text = f"❌ По запросу `{id_str}` ничего не найдено.\n\n💡 Попробуйте другой идентификатор или проверьте правильность ввода."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data=f"dir_{direction}_{id_str}")]
        ])
        await callback.message.edit_text(no_data_text, parse_mode="Markdown", reply_markup=keyboard)
        await callback.answer()
        return

    detailed = format_id_report(data, views)

    if any([data.get('telegrams'), data.get('groups'), data.get('name_history'), data.get('gifts')]):
        await save_id_report(cache_key, {'data': data})

    await callback.message.edit_text(detailed, parse_mode="Markdown")
    await callback.answer()

@dp.message(lambda msg: msg.text and re.sub(r'\D', '', msg.text).isdigit())
async def number_handler(message: types.Message):
    raw = message.text.strip()
    digits = re.sub(r'\D', '', raw)
    
    is_phone = False
    if raw.startswith('+'):
        is_phone = True
    elif len(digits) == 11 and digits[0] in ('7', '8'):
        is_phone = True
    elif len(digits) == 10 and digits[0] == '7':
        is_phone = True
    
    if is_phone:
        status = await message.reply("🔍 Поиск...")
        data = await collect_phone_data(digits)
        views = await get_unique_views_phone(digits, message.from_user.id)
        report = format_phone_report(data, views)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📲 Telegram", url=f"tg://resolve?phone={digits}"),
             InlineKeyboardButton(text="💬 WhatsApp", url=f"https://wa.me/{digits}")]
        ])
        
        await status.edit_text(report, parse_mode="Markdown", reply_markup=keyboard)
    else:
        await show_direction_choice(message, digits)

async def health_check(request):
    return web.Response(text="OK", status=200)

async def main():
    await init_db()
    print("🚀 Бот запущен (BigBase + Jitler + TeleLog)")

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
