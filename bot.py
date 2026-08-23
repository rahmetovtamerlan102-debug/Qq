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
    
    # Очистка email от мусора (словарей)
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
    
    return result

# ===== ГЕНЕРАЦИЯ КРАСИВОГО HTML-ОТЧЁТА =====
def generate_html_report(data: dict, views: int) -> str:
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>OSINT-отчёт</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, Arial, sans-serif;
                background: linear-gradient(135deg, #0a0e1a 0%, #1a1a2e 100%);
                min-height: 100vh;
                display: flex;
                justify-content: center;
                padding: 40px 20px;
            }
            .report {
                max-width: 820px;
                width: 100%;
                background: rgba(255,255,255,0.03);
                backdrop-filter: blur(20px);
                border-radius: 32px;
                padding: 40px 35px;
                border: 1px solid rgba(255,255,255,0.06);
                box-shadow: 0 25px 60px rgba(0,0,0,0.7);
            }
            .header {
                display: flex;
                align-items: center;
                gap: 16px;
                margin-bottom: 32px;
                border-bottom: 1px solid rgba(255,255,255,0.06);
                padding-bottom: 24px;
            }
            .header-icon {
                font-size: 36px;
                background: linear-gradient(135deg, #00d4ff, #7b2ffc);
                width: 64px;
                height: 64px;
                border-radius: 18px;
                display: flex;
                align-items: center;
                justify-content: center;
                box-shadow: 0 8px 24px rgba(0, 212, 255, 0.2);
            }
            .header h1 {
                color: #fff;
                font-size: 26px;
                font-weight: 700;
                letter-spacing: -0.5px;
            }
            .header .sub {
                color: rgba(255,255,255,0.35);
                font-size: 14px;
                font-weight: 400;
                margin-top: 2px;
            }
            .badge-osint {
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
            }
            .section {
                background: rgba(255,255,255,0.04);
                border-radius: 20px;
                padding: 20px 24px;
                margin-bottom: 16px;
                border: 1px solid rgba(255,255,255,0.05);
                transition: all 0.2s ease;
            }
            .section:hover {
                background: rgba(255,255,255,0.07);
                border-color: rgba(255,255,255,0.1);
                transform: translateY(-1px);
            }
            .section-title {
                font-size: 13px;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 1px;
                color: rgba(255,255,255,0.25);
                margin-bottom: 14px;
                display: flex;
                align-items: center;
                gap: 8px;
            }
            .section-title span {
                font-size: 18px;
                line-height: 1;
            }
            .row {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 7px 0;
                border-bottom: 1px solid rgba(255,255,255,0.04);
            }
            .row:last-child { border-bottom: none; }
            .label {
                color: rgba(255,255,255,0.45);
                font-size: 14px;
                font-weight: 400;
            }
            .value {
                color: #fff;
                font-size: 15px;
                font-weight: 500;
                text-align: right;
                word-break: break-word;
                max-width: 65%;
            }
            .value a {
                color: #7bb8ff;
                text-decoration: none;
                transition: color 0.2s;
            }
            .value a:hover {
                color: #a8d4ff;
                text-decoration: underline;
            }
            .value .highlight {
                background: linear-gradient(135deg, #00d4ff, #7b2ffc);
                padding: 3px 14px;
                border-radius: 30px;
                font-size: 14px;
                font-weight: 600;
                color: #fff;
                display: inline-block;
            }
            .phone-books {
                display: flex;
                flex-wrap: wrap;
                gap: 6px;
                justify-content: flex-end;
            }
            .phone-book-tag {
                background: rgba(255,255,255,0.07);
                padding: 4px 16px;
                border-radius: 30px;
                font-size: 13px;
                color: #ccc;
                border: 1px solid rgba(255,255,255,0.05);
            }
            .email-tag {
                background: rgba(123, 184, 255, 0.08);
                padding: 2px 12px;
                border-radius: 20px;
                font-size: 13px;
                color: #7bb8ff;
                border: 1px solid rgba(123, 184, 255, 0.1);
            }
            .footer {
                margin-top: 30px;
                text-align: center;
                color: rgba(255,255,255,0.15);
                font-size: 12px;
                border-top: 1px solid rgba(255,255,255,0.05);
                padding-top: 24px;
            }
            .views {
                color: rgba(255,255,255,0.4);
                font-size: 14px;
            }
            .views span {
                color: #fff;
                font-weight: 600;
            }
            @media (max-width: 600px) {
                .report { padding: 24px 16px; }
                .row { flex-wrap: wrap; gap: 4px; }
                .value { text-align: left; max-width: 100%; width: 100%; }
                .phone-books { justify-content: flex-start; }
                .header { flex-wrap: wrap; }
                .badge-osint { margin-left: 0; }
            }
        </style>
    </head>
    <body>
        <div class="report">
            <div class="header">
                <div class="header-icon">📱</div>
                <div>
                    <h1>OSINT-отчёт</h1>
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
                    <span class="value"><span class="highlight">{data['query']}</span></span>
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
    has_personal = data.get('fio') or data.get('birthdate') or data.get('age') is not None
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
                # Ищем username и ID
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

# ===== ФОРМАТТЕР ТЕКСТОВОГО ОТЧЁТА =====
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
        lines.append(f"\n📧 E-mail: {', '.join(data['emails'][:5])}")
        if len(data['emails']) > 5:
            lines.append(f"   ... и ещё {len(data['emails'])-5}")
    
    if data.get('telegrams'):
        lines.append(f"\n💬 Telegram: {', '.join(data['telegrams'])}")
    
    lines.append(f"\n👁 Интересовались этим: {views}")
    return "\n".join(lines)

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
        [InlineKeyboardButton(text="🤝 Партнёрская программа", callback_data="referral")]
    ])
    await message.reply("Отправьте номер телефона для поиска", reply_markup=keyboard)

@dp.message(lambda msg: msg.text and re.search(r'\+?\d{10,15}', re.sub(r'\s+', '', msg.text)))
async def phone_handler(message: types.Message):
    text = message.text.strip()
    digits = re.sub(r'\D', '', text)
    
    # Проверка подписки
    try:
        chat_member = await bot.get_chat_member(chat_id="@Dataseekerboto", user_id=message.from_user.id)
        if chat_member.status in ["left", "kicked"]:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Подписаться на канал", url="https://t.me/Dataseekerboto")],
                [InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_sub")]
            ])
            await message.reply(
                "❌ Для использования бота необходимо подписаться на наш канал!\n\n"
                "Подпишитесь и нажмите кнопку «Проверить подписку»",
                reply_markup=keyboard
            )
            return
    except Exception as e:
        print(f"Ошибка проверки подписки: {e}")
    
    status = await message.reply("🔍 Поиск...")
    data = await collect_phone_data(digits)
    views = await get_unique_views_phone(digits, message.from_user.id)
    
    # Текстовый отчёт
    text_report = format_phone_report(data, views)
    
    # HTML-отчёт
    html_content = generate_html_report(data, views)
    html_bytes = html_content.encode('utf-8')
    html_file = BufferedInputFile(html_bytes, filename=f"osint_report_{digits}.html")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Telegram", url=f"tg://resolve?phone={digits}"),
         InlineKeyboardButton(text="💬 WhatsApp", url=f"https://wa.me/{digits}")]
    ])
    
    await status.edit_text(text_report, reply_markup=keyboard)
    await message.reply_document(html_file, caption="📄 Красивый OSINT-отчёт (HTML)")

@dp.callback_query(lambda c: c.data == "check_sub")
async def check_sub_callback(callback: types.CallbackQuery):
    try:
        chat_member = await bot.get_chat_member(chat_id="@Dataseekerboto", user_id=callback.from_user.id)
        if chat_member.status in ["left", "kicked"]:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Подписаться на канал", url="https://t.me/Dataseekerboto")],
                [InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_sub")]
            ])
            await callback.message.edit_text(
                "❌ Вы ещё не подписаны на канал!\n\n"
                "Подпишитесь и нажмите кнопку «Проверить подписку»",
                reply_markup=keyboard
            )
        else:
            await callback.message.edit_text("✅ Спасибо за подписку! Теперь вы можете использовать бота.")
            await callback.answer("Подписка подтверждена!")
    except Exception as e:
        await callback.answer("Ошибка проверки подписки", show_alert=True)

@dp.callback_query(lambda c: c.data == "profile")
async def profile_callback(callback: types.CallbackQuery):
    ref_code = await get_referral_code(callback.from_user.id)
    ref_count = await get_referral_stats(callback.from_user.id)
    user = await get_user(callback.from_user.id)
    
    text = (
        f"👤 **Мой профиль**\n"
        f"Ваш ID: `{callback.from_user.id}`\n"
        f"Дата регистрации: `{user['created_at'].strftime('%d.%m.%Y %H:%M') if user else 'Неизвестно'}`\n\n"
        f"👥 Приглашено друзей: {ref_count}\n"
        f"🔗 Реферальный код: `{ref_code}`"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Поделиться реферальной ссылкой", callback_data="share_ref")]
    ])
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "share_ref")
async def share_ref_callback(callback: types.CallbackQuery):
    ref_code = await get_referral_code(callback.from_user.id)
    if not ref_code:
        await callback.answer("Ошибка")
        return
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{ref_code}"
    await callback.message.edit_text(
        f"🔗 Ваша реферальная ссылка:\n\n{link}\n\n"
        "Приглашайте друзей и получайте бонусы!"
    )
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
        "За каждого приглашённого друга вы получаете бонусные запросы!"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Поделиться ссылкой", callback_data="share_ref")]
    ])
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
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
