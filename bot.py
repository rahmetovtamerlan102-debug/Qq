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

# ===== КОНСТАНТЫ ДЛЯ КАНАЛА (ТОЛЬКО ЮЗЕРНЕЙМ) =====
CHANNEL_USERNAME = "@dataseekerinfo"  # Юзернейм канала
CHANNEL_LINK = "https://t.me/dataseekerinfo"

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
        await conn.execute('''
            UPDATE users 
            SET daily_requests = 0, last_request_date = CURRENT_DATE
            WHERE user_id = $1 AND last_request_date < CURRENT_DATE
        ''', user_id)
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

# ===== ПРОВЕРКА ПОДПИСКИ (ПО ЮЗЕРНЕЙМУ) =====
async def check_subscription(user_id: int) -> bool:
    try:
        # Получаем объект канала по юзернейму
        chat = await bot.get_chat(CHANNEL_USERNAME)
        chat_id = chat.id
        chat_member = await bot.get_chat_member(chat_id, user_id)
        return chat_member.status in ["member", "administrator", "creator"]
    except Exception as e:
        print(f"Ошибка проверки подписки: {e}")
        return False

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

# ===== СБОР ДАННЫХ =====
async def collect_phone_data(query: str):
    try:
        bigbase, nightsearch, seon, snusbase, depsearch, jitler = await asyncio.wait_for(
            asyncio.gather(
                bigbase_search(query),
                nightsearch_search(query),
                seon_search(query),
                snusbase_search(query),
                depsearch_search(query),
                jitler_search_with_balancer(query, "number"),
                return_exceptions=True
            ),
            timeout=8.0
        )
    except asyncio.TimeoutError:
        bigbase = nightsearch = seon = snusbase = depsearch = jitler = {}
    
    bigbase = bigbase if isinstance(bigbase, dict) else {}
    nightsearch = nightsearch if isinstance(nightsearch, dict) else {}
    seon = seon if isinstance(seon, dict) else {}
    snusbase = snusbase if isinstance(snusbase, dict) else {}
    depsearch = depsearch if isinstance(depsearch, dict) else {}
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
        'records_count': 0
    }

    all_birthdates = []
    sources_set = set()
    records_count = 0
    seen_records = set()

    if bigbase and isinstance(bigbase, dict):
        dossier = bigbase.get('dossier', {})
        records_list = dossier.get('records', []) or bigbase.get('records', [])
        connections_data = dossier.get('connections', {}) or bigbase.get('connections', {})
        head_data = dossier.get('head', {}) or bigbase.get('head', {})
        
        operator = head_data.get('phone_operator')
        region = head_data.get('phone_region')
        country_info = head_data.get('phone_country_info')
        
        if not operator or not region or not country_info:
            for record in records_list:
                base_record = record.get('base_record', [])
                for item in base_record:
                    if isinstance(item, list) and len(item) >= 2:
                        key = str(item[0]).strip()
                        value = item[1]
                        if key == 'Оператор' and not operator:
                            operator = str(value)
                        elif key == 'Регион' and not region:
                            region = str(value)
                        elif key == 'Страна (столица)' and not country_info:
                            country_info = str(value)
                        elif key == 'Страна' and not country_info:
                            country_info = str(value)
        
        if operator:
            result['operator'] = str(operator)
            sources_set.add("BigBase (оператор)")
        
        if region:
            result['region'] = str(region)
            sources_set.add("BigBase (регион)")
        
        if country_info:
            result['country'] = str(country_info)
            sources_set.add("BigBase (страна)")

        for record in records_list:
            source_name = record.get('base_info', {}).get('name', 'Unknown')
            base_record = record.get('base_record', [])
            record_data = {}
            record_key = ""
            
            for item in base_record:
                if isinstance(item, list) and len(item) >= 2:
                    key = str(item[0]).strip()
                    value = item[1]
                    if value and value not in (None, '', [], {}):
                        record_data[key] = value
                        if key in ['ID', 'ID2', 'ФИО', 'Адрес']:
                            record_key += str(value)
            
            if record_data and record_key not in seen_records:
                seen_records.add(record_key)
                sources_set.add(source_name)
                records_count += 1
                result['extra'][f"Запись #{records_count}"] = {
                    'source': source_name,
                    'data': record_data
                }

        persons = connections_data.get('person', [])
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
                        sources_set.add("BigBase (ФИО)")
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
                        sources_set.add("BigBase (дата рождения)")
                        break
            
            if not result['address']:
                address_list = person.get('address_place', [])
                if address_list:
                    for addr in address_list:
                        if isinstance(addr, dict):
                            full = addr.get('full')
                            if full:
                                result['address'] = str(full)
                                sources_set.add("BigBase (адрес)")
                                break
            
            if result['fio'] and all_birthdates and result['address']:
                break

        socials = extract_socials(bigbase)
        for key in ['vk', 'ok', 'instagram', 'tiktok']:
            if socials.get(key) and not result.get(key):
                result[key] = socials[key]
                if socials.get(key):
                    sources_set.add(f"BigBase ({key})")

        tg_data = deep_find_all(bigbase, 'telegram') + deep_find_all(bigbase, 'tg')
        for tg in tg_data:
            formatted = extract_telegram(tg)
            if formatted:
                result['telegrams'].append(formatted)
                sources_set.add("BigBase (Telegram)")

        phone_books = deep_find_all(bigbase, 'phone_books')
        for pb in phone_books:
            if pb:
                result['phone_books'].append(str(pb))
                sources_set.add("BigBase (телефонная книга)")
        
        emails = deep_find_all(bigbase, 'email') + deep_find_all(bigbase, 'mail') + deep_find_all(bigbase, 'e-mail')
        for email in emails:
            if email and '@' in str(email):
                result['emails'].append(str(email))
                sources_set.add("BigBase (email)")

    if depsearch and isinstance(depsearch, dict):
        phone_info = depsearch.get('phone_info', {})
        if not result['operator']:
            operator = phone_info.get('operator')
            if operator:
                result['operator'] = str(operator)
                sources_set.add("DepSearch (оператор)")
        
        if not result['region']:
            region = phone_info.get('region')
            if region:
                result['region'] = str(region)
                sources_set.add("DepSearch (регион)")
        
        if not result['country']:
            country = phone_info.get('country')
            if country:
                result['country'] = str(country)
                sources_set.add("DepSearch (страна)")

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
                                if clean_key in ['ФИО', 'Адрес']:
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
                            item.get('fio')
                        )
                        if fio:
                            result['fio'] = str(fio)
                            sources_set.add("DepSearch (ФИО)")
                    
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
                                sources_set.add("DepSearch (адрес)")
                    
                    if not result['cards']:
                        card = item.get('💳Карта') or item.get('card') or item.get('💳 Банковская карта')
                        if card:
                            result['cards'].append(str(card))
                    
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

    if jitler and isinstance(jitler, dict):
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
    result['cards'] = list(dict.fromkeys(result['cards']))
    result['banks'] = list(dict.fromkeys(result['banks']))

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

# ===== ТЕКСТОВЫЕ ФОРМАТТЕРЫ =====
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

# ===== HTML-ОТЧЁТ =====
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
                if k in ['ФИО', 'Адрес', 'ID']:
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
    
    html = f"""
    <!DOCTYPE html>
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
    </html>
    """
    return html

# ===== СТАРТОВОЕ СООБЩЕНИЕ =====
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
    return (
        f"⠛ Поиск по {type_name}:\n\n"
        f"💬 Поиск: {query}\n"
        f"⏱️ Сбор информации...\n\n"
        f"Проверяю базы данных..."
    )

# ===== БОТ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ===== СТАРТОВАЯ КОМАНДА =====
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = message.from_user.id
    
    # Проверяем подписку
    is_subscribed = await check_subscription(user_id)
    
    if not is_subscribed:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription")]
        ])
        
        await message.reply(
            "🔒 Для использования бота подпишитесь на наш канал!\n\n"
            "После подписки нажмите кнопку ниже.",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        return
    
    # Если подписан - создаём пользователя
    user = await get_user(user_id)
    if not user:
        await create_user(user_id, message.from_user.username)
    
    # ===== ЗАКРЕПЛЯЕМ ВЕЧНУЮ ССЫЛКУ =====
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

    # ===== ОСНОВНОЕ СООБЩЕНИЕ =====
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

# ===== КОЛБЭК ДЛЯ ПРОВЕРКИ ПОДПИСКИ =====
@dp.callback_query(lambda c: c.data == "check_subscription")
async def check_subscription_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    is_subscribed = await check_subscription(user_id)
    
    if is_subscribed:
        # Создаём пользователя если его нет
        user = await get_user(user_id)
        if not user:
            await create_user(user_id, callback.from_user.username)
        
        # Удаляем сообщение с кнопками
        await callback.message.delete()
        
        # ===== ЗАКРЕПЛЯЕМ ВЕЧНУЮ ССЫЛКУ =====
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌐 Информация о боте", url="https://t.me/dataseekerinfo")]
        ])

        msg_link = await callback.message.answer(
            "🔮 *Вечная ссылка на информацию:*\nЕсли удалят этого бота — то новую ссылку на него найдёте по кнопке ниже.",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )

        try:
            await bot.pin_chat_message(callback.message.chat.id, msg_link.message_id, disable_notification=True)
        except Exception as e:
            print(f"Не удалось закрепить: {e}")

        # ===== ОСНОВНОЕ СООБЩЕНИЕ =====
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
        await callback.message.answer(text, parse_mode="Markdown")
        
        await callback.answer("✅")
    else:
        await callback.answer("❌ Вы не подписались на канал!", show_alert=True)

# ===== КОМАНДА ИНН =====
@dp.message(Command("inn"))
async def inn_command(message: types.Message):
    user_id = message.from_user.id
    
    # Проверяем подписку
    is_subscribed = await check_subscription(user_id)
    if not is_subscribed:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription")]
        ])
        await message.reply(
            "🔒 Для использования бота подпишитесь на наш канал!\n\n"
            "После подписки нажмите кнопку ниже.",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите ИНН: `/inn 123456789012`")
        return
    query = args[1].strip()
    if not re.match(r'^\d{10,12}$', query):
        await message.reply("❌ ИНН должен содержать 10 или 12 цифр")
        return

    # Проверяем лимит
    daily_requests = await get_daily_requests(user_id)
    if daily_requests >= 5:
        await message.reply(
            "🚫 Лимит: 5/5 использовано\n\n"
            "Завтра будет новый день и новые 5 запросов."
        )
        return

    status_msg = await message.reply(get_search_start_message("inn", query))
    data = await collect_inn_data(query)
    report = format_inn_report(data)
    await status_msg.edit_text(report, parse_mode="Markdown")
    await increment_daily_requests(user_id)

# ===== УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК =====
@dp.message(lambda msg: msg.text and not msg.text.startswith('/'))
async def universal_handler(message: types.Message):
    text = message.text.strip()
    if not text:
        return

    user_id = message.from_user.id
    
    # ===== ПРОВЕРКА ПОДПИСКИ =====
    is_subscribed = await check_subscription(user_id)
    if not is_subscribed:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription")]
        ])
        await message.reply(
            "🔒 Для использования бота подпишитесь на наш канал!\n\n"
            "После подписки нажмите кнопку ниже.",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        return

    # Создаём пользователя если его нет
    user = await get_user(user_id)
    if not user:
        await create_user(user_id, message.from_user.username)

    # ===== ПРОВЕРКА ЛИМИТА ЗАПРОСОВ (5 В ДЕНЬ) =====
    daily_requests = await get_daily_requests(user_id)
    
    if daily_requests >= 5:
        await message.reply(
            "🚫 Лимит: 5/5 использовано\n\n"
            "Завтра будет новый день и новые 5 запросов."
        )
        return

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

    status_msg = await message.reply(get_search_start_message(search_type, query))

    if search_type == "phone":
        data = await collect_phone_data(query)
        views = await get_unique_views_phone(query, message.from_user.id)
        await save_report(query, data)
        report = format_phone_report(data, views)
        html_content = generate_html_report(data, views, "Отчёт по номеру телефона")
        html_bytes = html_content.encode('utf-8')
        html_file = BufferedInputFile(html_bytes, filename=f"phone_report_{query}.html")

        buttons = [
            [InlineKeyboardButton(text="📄 Полный отчёт (HTML)", callback_data=f"html_phone_{query}")]
        ]
        if query:
            wa_url = f"https://wa.me/{query}"
            buttons.append([InlineKeyboardButton(text="💬 WhatsApp", url=wa_url)])
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

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        await status_msg.edit_text(report, parse_mode="Markdown", reply_markup=keyboard)
    elif search_type == "email":
        data = await collect_email_data(query)
        report = format_email_report(data)
        await status_msg.edit_text(report, parse_mode="Markdown")
    elif search_type == "ip":
        data = await collect_ip_data(query)
        report = format_ip_report(data)
        await status_msg.edit_text(report, parse_mode="Markdown")
    elif search_type == "vk":
        data = await collect_vk_data(query)
        report = format_vk_report(data)
        await status_msg.edit_text(report, parse_mode="Markdown")
    elif search_type == "fio":
        data = await collect_fio_data(query)
        report = format_fio_report(data)
        await status_msg.edit_text(report, parse_mode="Markdown")
    else:
        await status_msg.edit_text("❌ Неизвестный тип запроса. Попробуйте отправить телефон, email, IP, VK, ФИО или используйте /inn.")
        return

    # ===== УВЕЛИЧИВАЕМ СЧЁТЧИК =====
    await increment_daily_requests(user_id)

# ===== КОЛБЭК ДЛЯ HTML ОТЧЁТА =====
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
    await callback.message.reply_document(html_file, caption="📄 Полный HTML-отчёт")
    await callback.answer()

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
