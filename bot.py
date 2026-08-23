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

# ===== API ЗАПРОСЫ (таймаут 1.5 сек) =====
async def bigbase_search(query: str):
    url = "https://bigbase.top/api/search"
    headers = {"Authorization": BIGBASE_TOKEN, "Content-Type": "application/json"}
    payload = {"search": query, "page": 0}
    timeout = aiohttp.ClientTimeout(total=1.5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception:
            return {}

async def nightsearch_search(query: str):
    if not NIGHTSEARCH_API_KEY:
        return {}
    url = "https://nightsearch.life/api/search"
    headers = {"X-API-Key": NIGHTSEARCH_API_KEY, "Content-Type": "application/json"}
    payload = {"query": query, "search_type": "phone"}
    timeout = aiohttp.ClientTimeout(total=1.5)
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
    timeout = aiohttp.ClientTimeout(total=1.5)
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
    timeout = aiohttp.ClientTimeout(total=1.5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception:
            return {}

async def jitler_search_with_balancer(query: str, search_type: str = "number"):
    timeout = aiohttp.ClientTimeout(total=1.5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(len(JITLER_TOKENS) * 2):
            token = await balancer.get_token()
            if not token:
                return {}
            url = "https://api.jitler.top/search"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            payload = {"type": search_type, "query": query, "page": 1}
            try:
                async with session.post(url, json=payload, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        balancer.mark_success(token)
                        return data
                    elif resp.status == 429:
                        balancer.mark_failed(token)
                        continue
                    else:
                        return {}
            except Exception:
                continue
    return {}

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
        if 'url' in value:
            return value.get('url')
        if 'name' in value:
            return value.get('name')
        if 'username' in value:
            return value.get('username')
        if 'id' in value:
            return value.get('id')
        if 'value' in value:
            return value.get('value')
        return None
    if isinstance(value, list) and not value:
        return None
    return value if value not in (None, "", [], {}, False) else None

def extract_telegram(tg_data):
    if isinstance(tg_data, dict):
        username = tg_data.get('username') or tg_data.get('user') or tg_data.get('nick')
        user_id = tg_data.get('id') or tg_data.get('tg_id') or tg_data.get('user_id')
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
        if isinstance(v, (str, int, float)):
            cleaned = clean_value(v)
            if cleaned:
                result.append(str(cleaned))
        elif isinstance(v, dict):
            cleaned = clean_value(v)
            if cleaned:
                result.append(str(cleaned))
    return list(dict.fromkeys(result))

def extract_birthdate_from_text(text):
    if not text:
        return None
    match = re.search(r'\((\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\)', text)
    if match:
        return match.group(1)
    match = re.search(r'\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\b', text)
    if match:
        return match.group(1)
    return None

def extract_phone_books(data):
    books = []
    for src in [data.get('bigbase'), data.get('nightsearch'), data.get('seon'), data.get('jitler')]:
        if not src:
            continue
        found = deep_find_all(src, 'phone_books') + deep_find_all(src, 'contacts') + deep_find_all(src, 'телефонные_книги')
        for item in found:
            if isinstance(item, str):
                books.append(item)
            elif isinstance(item, list):
                books.extend([str(b) for b in item if b])
            elif isinstance(item, dict):
                for k, v in item.items():
                    if v and isinstance(v, str):
                        books.append(v)
    return normalize_list(books)

def find_best_birthdate(birthdates):
    """Выбирает самую полную дату рождения из списка"""
    if not birthdates:
        return None
    best = None
    best_score = 0
    for bd in birthdates:
        if not bd:
            continue
        # Считаем, сколько частей есть в дате
        parts = re.split(r'[./-]', str(bd))
        score = len(parts)
        # Приоритет у формата с годом
        if re.search(r'\d{4}', str(bd)):
            score += 10
        # Приоритет у формата дд.мм.гггг
        if re.match(r'\d{1,2}[./-]\d{1,2}[./-]\d{4}', str(bd)):
            score += 20
        if score > best_score:
            best_score = score
            best = bd
    return best

# ===== СБОР ДАННЫХ =====
async def collect_phone_data(query: str):
    bigbase, nightsearch, seon, snusbase, jitler = await asyncio.gather(
        bigbase_search(query),
        nightsearch_search(query),
        seon_search(query),
        snusbase_search(query),
        jitler_search_with_balancer(query, "number")
    )
    
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
        'banks': [],
        'phone_books': []
    }
    
    # Собираем все возможные даты рождения
    all_birthdates = []
    
    # ---- 1. ПАРСИНГ BIGBASE ----
    if bigbase:
        dossier = bigbase.get('dossier', {})
        head = dossier.get('head', {})
        result['operator'] = head.get('phone_operator')
        result['region'] = head.get('phone_region')
        result['country'] = head.get('phone_country_info')
        
        connections = bigbase.get('connections', {})
        persons = connections.get('person', [])
        for person in persons:
            fio_list = person.get('fio', [])
            for fio_item in fio_list:
                if fio_item.get('value'):
                    result['fio'] = fio_item['value']
                    break
            if result['fio']:
                break
            birthdate_list = person.get('birthdate', [])
            for bd_item in birthdate_list:
                if bd_item.get('value'):
                    all_birthdates.append(bd_item['value'])
                    break
            if all_birthdates:
                break
    
    # ---- 2. ПАРСИНГ ДРУГИХ API ----
    for src in [nightsearch, seon, snusbase, jitler]:
        if not src:
            continue
        
        if not result['fio']:
            fio_from_src = deep_find(src, 'full_name') or deep_find(src, 'fio') or deep_find(src, 'name')
            if fio_from_src:
                result['fio'] = fio_from_src
        
        # Собираем даты рождения из API
        birthdate = deep_find(src, 'birthdate') or deep_find(src, 'birth_date') or deep_find(src, 'date_of_birth')
        if birthdate:
            all_birthdates.append(birthdate)
        
        if not result['operator']:
            result['operator'] = deep_find(src, 'operator') or deep_find(src, 'oper')
        if not result['region']:
            result['region'] = deep_find(src, 'region') or deep_find(src, 'reg')
        if not result['country']:
            result['country'] = deep_find(src, 'country')
        
        result['emails'].extend(deep_find_all(src, 'email') + deep_find_all(src, 'mail') + deep_find_all(src, 'e-mail'))
        result['telegrams'].extend(deep_find_all(src, 'telegram') + deep_find_all(src, 'tg'))
        
        vk = deep_find(src, 'vk') or deep_find(src, 'vkontakte')
        if vk:
            if isinstance(vk, dict) and vk.get('name'):
                birth_from_vk = extract_birthdate_from_text(vk.get('name', ''))
                if birth_from_vk:
                    all_birthdates.append(birth_from_vk)
            result['vk'] = vk
        
        inst = deep_find(src, 'instagram')
        if inst and clean_value(inst):
            result['instagram'] = inst
        
        tt = deep_find(src, 'tiktok')
        if tt and clean_value(tt):
            result['tiktok'] = tt
        
        ok = deep_find(src, 'ok') or deep_find(src, 'odnoklassniki')
        if ok:
            if isinstance(ok, dict) and ok.get('name'):
                birth_from_ok = extract_birthdate_from_text(ok.get('name', ''))
                if birth_from_ok:
                    all_birthdates.append(birth_from_ok)
            result['ok'] = ok
        
        result['banks'].extend(deep_find_all(src, 'bank') + deep_find_all(src, 'banks') + deep_find_all(src, 'account'))
    
    # Выбираем лучшую дату рождения
    if all_birthdates:
        result['birthdate'] = find_best_birthdate(all_birthdates)
    
    result['phone_books'] = extract_phone_books({
        'bigbase': bigbase,
        'nightsearch': nightsearch,
        'seon': seon,
        'jitler': jitler
    })
    
    result['emails'] = normalize_list(result['emails'])
    result['telegrams'] = normalize_list(result['telegrams'])
    result['banks'] = normalize_list(result['banks'])
    
    return result

async def collect_id_data(query: str, search_type: str):
    bigbase, jitler = await asyncio.gather(
        bigbase_search(query),
        jitler_search_with_balancer(query, search_type)
    )
    
    result = {
        'query': query,
        'telegrams': [],
        'groups': [],
        'interests': [],
        'username_history': [],
        'gift_ids': [],
        'emails': [],
        'vk': None,
        'instagram': None,
        'tiktok': None,
        'ok': None,
        'banks': []
    }
    
    for src in [bigbase, jitler]:
        if not src:
            continue
        result['telegrams'].extend(deep_find_all(src, 'telegram') + deep_find_all(src, 'tg'))
        result['groups'].extend(deep_find_all(src, 'groups') + deep_find_all(src, 'chats'))
        result['interests'].extend(deep_find_all(src, 'interests'))
        result['username_history'].extend(deep_find_all(src, 'username_history') + deep_find_all(src, 'history') + deep_find_all(src, 'name_history'))
        result['gift_ids'].extend(deep_find_all(src, 'gifts') + deep_find_all(src, 'presents'))
        result['emails'].extend(deep_find_all(src, 'email') + deep_find_all(src, 'mail') + deep_find_all(src, 'e-mail'))
        
        vk = deep_find(src, 'vk') or deep_find(src, 'vkontakte')
        if vk and clean_value(vk):
            result['vk'] = vk
        inst = deep_find(src, 'instagram')
        if inst and clean_value(inst):
            result['instagram'] = inst
        tt = deep_find(src, 'tiktok')
        if tt and clean_value(tt):
            result['tiktok'] = tt
        ok = deep_find(src, 'ok') or deep_find(src, 'odnoklassniki')
        if ok and clean_value(ok):
            result['ok'] = ok
        result['banks'].extend(deep_find_all(src, 'bank') + deep_find_all(src, 'banks') + deep_find_all(src, 'account'))
    
    result['telegrams'] = normalize_list(result['telegrams'])
    result['groups'] = normalize_list(result['groups'])
    result['interests'] = normalize_list(result['interests'])
    result['gift_ids'] = normalize_list(result['gift_ids'])
    result['emails'] = normalize_list(result['emails'])
    result['banks'] = normalize_list(result['banks'])
    
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
    
    # Основные данные: ФИО, дата рождения, возраст
    has_personal = data.get('fio') or data.get('birthdate') or data.get('age')
    if has_personal:
        lines.append("\n👤 Основные данные")
        if data.get('fio'):
            lines.append(f"├ ФИО: {data['fio']}")
        if data.get('birthdate'):
            lines.append(f"├ Дата рождения: {data['birthdate']}")
        if data.get('age'):
            lines.append(f"└ Возраст: {data['age']}")
    
    if data.get('phone_books'):
        lines.append(f"\n🔎 Телефонные книги: {', '.join(data['phone_books'][:10])}")
    
    if data.get('vk'):
        vk_clean = clean_value(data['vk'])
        if vk_clean:
            lines.append(f"\n🧑‍💻 Вконтакте: {vk_clean}")
    
    if data.get('ok'):
        ok_clean = clean_value(data['ok'])
        if ok_clean:
            lines.append(f"\n👨‍🦳 Одноклассники: {ok_clean}")
    
    if data.get('tiktok') and clean_value(data['tiktok']):
        lines.append(f"\n👩‍🦲 TikTok: {clean_value(data['tiktok'])}")
    
    if data.get('banks'):
        lines.append(f"\n🏦 Мобильный банк: {', '.join(data['banks'])}")
    
    if data.get('telegrams'):
        clean_tgs = []
        for tg in data['telegrams']:
            cleaned = extract_telegram(tg)
            if cleaned:
                clean_tgs.append(cleaned)
        if clean_tgs:
            lines.append(f"\n💬 Telegram: {', '.join(clean_tgs)}")
    
    if data.get('emails'):
        lines.append(f"\n📧 E-mail: {', '.join(data['emails'])}")
    
    lines.append(f"\n👁 Интересовались этим: {views}")
    return "\n".join(lines)

def format_id_report(data: dict, views: int) -> str:
    lines = ["```"]
    if data.get('query'):
        lines.append(f"# ID: {data['query']}")
    if data.get('telegrams'):
        clean_tgs = []
        for tg in data['telegrams']:
            cleaned = extract_telegram(tg)
            if cleaned:
                clean_tgs.append(cleaned)
        if clean_tgs:
            lines.append(f"# Найденные аккаунты: {', '.join(clean_tgs)}")
    if data.get('groups'):
        lines.append(f"# Группы [{len(data['groups'])}]:")
        for group in data['groups']:
            lines.append(str(group))
    if data.get('interests'):
        lines.append(f"# Интересы [{len(data['interests'])}]:")
        for interest in data['interests']:
            lines.append(str(interest))
    if data.get('gift_ids'):
        lines.append("# Подарочные связи:")
        lines.append(", ".join(data['gift_ids']))
    if data.get('username_history'):
        lines.append("# История изменения имени:")
        for entry in data['username_history']:
            if isinstance(entry, dict):
                date = entry.get('date', '')
                nick = entry.get('nick', '')
                name = entry.get('name', '')
                uid = entry.get('id', '')
                parts = []
                if date:
                    parts.append(date)
                parts.append("→")
                if nick:
                    parts.append(nick)
                if name:
                    parts.append(name)
                if uid:
                    parts.append(uid)
                lines.append(" ".join(parts))
            else:
                lines.append(str(entry))
    if data.get('emails'):
        lines.append(f"# Email: {', '.join(data['emails'])}")
    if data.get('vk') and clean_value(data['vk']):
        lines.append(f"# ВКонтакте: {clean_value(data['vk'])}")
    if data.get('instagram') and clean_value(data['instagram']):
        lines.append(f"# Instagram: {clean_value(data['instagram'])}")
    if data.get('tiktok') and clean_value(data['tiktok']):
        lines.append(f"# TikTok: {clean_value(data['tiktok'])}")
    if data.get('ok') and clean_value(data['ok']):
        lines.append(f"# Одноклассники: {clean_value(data['ok'])}")
    if data.get('banks'):
        lines.append(f"# Банки: {', '.join(data['banks'])}")
    lines.append(f"👁 Интересовались этим: {views}")
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
    parts = callback.data.split("_", 2)
    if len(parts) < 3:
        await callback.answer("Ошибка формата")
        return
    direction = parts[1]
    id_str = parts[2]

    cache_key = f"{id_str}:{direction}"
    cached = await get_id_report(cache_key)
    if cached:
        data = cached.get('data', {})
        views = await get_unique_views_id(id_str, callback.from_user.id)
        detailed = format_id_report(data, views)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📲 Открыть в Telegram", url=f"tg://user?id={id_str}")]
        ])
        await callback.message.edit_text(detailed, parse_mode="Markdown", reply_markup=keyboard)
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
    data = await collect_id_data(id_str, search_type)
    views = await get_unique_views_id(id_str, callback.from_user.id)
    detailed = format_id_report(data, views)
    
    if any([data.get('telegrams'), data.get('groups'), data.get('interests'), data.get('gift_ids')]):
        await save_id_report(cache_key, {'data': data})
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Открыть в Telegram", url=f"tg://user?id={id_str}")]
    ])
    await callback.message.edit_text(detailed, parse_mode="Markdown", reply_markup=keyboard)
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
    print("🚀 Бот запущен (дата рождения из всех API, только одна)")

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
