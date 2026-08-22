import os
import asyncio
import re
import json
import asyncpg
import aiohttp
from aiohttp import web
from datetime import datetime, date
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
JITLER_TOKENS_STR = os.getenv("JITLER_TOKENS", "")
JITLER_TOKENS = [t.strip() for t in JITLER_TOKENS_STR.split(",") if t.strip()]

db_pool = None

async def get_pool():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
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

# ===== НОРМАЛИЗАЦИЯ ЗНАЧЕНИЙ =====
def normalize_values(values):
    """Преобразует любые значения в строки, удаляет None/пустые строки и дубли."""
    result = []
    for value in values:
        if isinstance(value, dict):
            result.append(json.dumps(value, ensure_ascii=False))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    result.append(json.dumps(item, ensure_ascii=False))
                elif item not in (None, ""):
                    result.append(str(item))
        elif value not in (None, ""):
            result.append(str(value))
    return list(dict.fromkeys(result))

# ===== ИНИЦИАЛИЗАЦИЯ БАЗЫ =====
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Создаём таблицы
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
        
        # id_reports – проверяем и создаём колонку key если её нет
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS id_reports (
                key TEXT PRIMARY KEY,
                data JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Проверяем, есть ли колонка key
        col_check = await conn.fetchrow('''
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'id_reports' AND column_name = 'key'
        ''')
        if not col_check:
            # Если колонки нет, добавляем её (но PRIMARY KEY уже не сможем добавить через ALTER)
            # Проще пересоздать таблицу
            await conn.execute('DROP TABLE IF EXISTS id_reports')
            await conn.execute('''
                CREATE TABLE id_reports (
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

# ===== ФУНКЦИИ РАБОТЫ С БАЗОЙ =====
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

# ===== РАСЧЁТ ВОЗРАСТА =====
def calculate_age(birthdate_str: str) -> int:
    if not birthdate_str:
        return None
    for fmt in ('%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y'):
        try:
            bdate = datetime.strptime(birthdate_str.strip(), fmt).date()
            today = date.today()
            age = today.year - bdate.year - ((today.month, today.day) < (bdate.month, bdate.day))
            return age
        except ValueError:
            continue
    match = re.search(r'\b(\d{4})\b', birthdate_str)
    if match:
        year = int(match.group(1))
        if 1900 < year < date.today().year:
            return date.today().year - year
    return None

# ===== ГЛУБОКИЙ ПОИСК =====
def deep_search(obj, key, default=None):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = deep_search(v, key, default)
            if result != default:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = deep_search(item, key, default)
            if result != default:
                return result
    return default

# ===== ФУНКЦИИ API =====
async def bigbase_search(query: str):
    url = "https://bigbase.top/api/search"
    headers = {"Authorization": BIGBASE_TOKEN, "Content-Type": "application/json"}
    payload = {"search": query, "page": 0}
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                error_text = await resp.text()
                print(f"BigBase HTTP {resp.status}: {error_text}")
                return {}
        except Exception as e:
            print(f"BigBase exception: {e}")
            return {}

async def nightsearch_search(query: str):
    if not NIGHTSEARCH_API_KEY:
        return {}
    url = "https://nightsearch.life/api/search"
    headers = {"X-API-Key": NIGHTSEARCH_API_KEY, "Content-Type": "application/json"}
    payload = {"query": query, "search_type": "phone"}
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                error_text = await resp.text()
                print(f"Nightsearch HTTP {resp.status}: {error_text}")
                return {}
        except Exception as e:
            print(f"Nightsearch exception: {e}")
            return {}

async def seon_search(query: str):
    if not SEON_API_KEY:
        return {}
    url = "https://api.seon.io/SeonRestService/phone-api/v2"
    headers = {"X-API-KEY": SEON_API_KEY, "Content-Type": "application/json"}
    payload = {"phone": query}
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                error_text = await resp.text()
                print(f"SEON HTTP {resp.status}: {error_text}")
                return {}
        except Exception as e:
            print(f"SEON exception: {e}")
            return {}

async def jitler_search_with_balancer(query: str, search_type: str = "number"):
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(len(JITLER_TOKENS) * 2):
            token = await balancer.get_token()
            if not token:
                print("JITLER: нет доступных токенов")
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
                        print(f"JITLER токен {token} превысил лимит")
                        continue
                    else:
                        error_text = await resp.text()
                        print(f"JITLER HTTP {resp.status}: {error_text}")
                        return {}
            except Exception as e:
                print(f"JITLER exception: {e}")
                continue
    return {}

def is_nonempty_dict(d):
    return isinstance(d, dict) and any(v for v in d.values() if v not in (None, "", [], {}))

# ===== ПАРСИНГ TELEGRAM-АККАУНТОВ =====
def parse_telegrams(raw_data):
    telegrams = []
    if not raw_data:
        return telegrams
    if isinstance(raw_data, list):
        for item in raw_data:
            if isinstance(item, dict):
                uname = item.get('username') or item.get('user') or item.get('nick')
                uid = item.get('id') or item.get('tg_id') or item.get('user_id')
                if uname and uid:
                    telegrams.append((uname, uid))
                elif isinstance(item, str):
                    parts = item.strip().split()
                    if len(parts) >= 2:
                        uname = parts[0]
                        uid = parts[1]
                        if uname.startswith('@'):
                            telegrams.append((uname, uid))
            elif isinstance(item, str):
                parts = item.strip().split()
                if len(parts) >= 2:
                    uname = parts[0]
                    uid = parts[1]
                    if uname.startswith('@'):
                        telegrams.append((uname, uid))
    elif isinstance(raw_data, dict):
        uname = raw_data.get('username') or raw_data.get('user') or raw_data.get('nick')
        uid = raw_data.get('id') or raw_data.get('tg_id') or raw_data.get('user_id')
        if uname and uid:
            telegrams.append((uname, uid))
    elif isinstance(raw_data, str):
        parts = re.split(r'[,;]\s*', raw_data)
        for part in parts:
            part = part.strip()
            if part:
                tokens = part.split()
                if len(tokens) >= 2:
                    uname = tokens[0]
                    uid = tokens[1]
                    if uname.startswith('@'):
                        telegrams.append((uname, uid))
    seen = set()
    unique = []
    for uname, uid in telegrams:
        key = (uname, uid)
        if key not in seen:
            seen.add(key)
            unique.append(key)
    return unique

# ===== СБОР ДАННЫХ (исправленный) =====
async def collect_data(query: str, search_type: str, is_phone: bool = False):
    tasks = [
        bigbase_search(query),
        nightsearch_search(query),
        seon_search(query),
        jitler_search_with_balancer(query, search_type),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    bigbase = results[0] if isinstance(results[0], dict) else {}
    nightsearch = results[1] if isinstance(results[1], dict) else {}
    seon = results[2] if isinstance(results[2], dict) else {}
    jitler = results[3] if isinstance(results[3], dict) else {}

    # Логирование (для отладки)
    print("\n=== BIGBASE ===")
    print(json.dumps(bigbase, ensure_ascii=False, indent=2)[:1000])
    print("\n=== NIGHTSEARCH ===")
    print(json.dumps(nightsearch, ensure_ascii=False, indent=2)[:1000])
    print("\n=== SEON ===")
    print(json.dumps(seon, ensure_ascii=False, indent=2)[:1000])
    print("\n=== JITLER ===")
    print(json.dumps(jitler, ensure_ascii=False, indent=2)[:1000])

    sources = [bigbase, nightsearch, seon, jitler]

    # ---- Оператор, регион, страна ----
    if is_phone:
        op = None
        reg = None
        country = None
        for src in sources:
            if not src:
                continue
            op = deep_search(src, 'operator') or deep_search(src, 'оператор') or deep_search(src, 'oper') or op
            reg = deep_search(src, 'region') or deep_search(src, 'регион') or deep_search(src, 'reg') or reg
            country = deep_search(src, 'country') or deep_search(src, 'страна') or country
            if op and reg and country:
                break
    else:
        op = reg = country = None

    # ---- Контакты (телефонные книги) ----
    contacts = []
    telegram_raw = None
    for src in sources:
        if not src:
            continue
        books = deep_search(src, 'phone_books') or deep_search(src, 'contacts') or deep_search(src, 'телефонные_книги')
        if books:
            if isinstance(books, list):
                contacts.extend(books)
            else:
                contacts.append(books)
        tg = deep_search(src, 'telegram') or deep_search(src, 'tg')
        if tg and not telegram_raw:
            telegram_raw = tg

    # Нормализуем contacts
    contacts = normalize_values(contacts)

    # ---- ФИО, дата рождения, возраст, соцсети, банки, email ----
    fio = None
    birthdate = None
    age = None
    socials = {}
    banks = []
    emails = []

    for src in sources:
        if not src:
            continue
        fio = fio or (
            deep_search(src, 'full_name') or
            deep_search(src, 'fio') or
            deep_search(src, 'name') or
            deep_search(src, 'ФИО')
        )
        birthdate = birthdate or (
            deep_search(src, 'birthdate') or
            deep_search(src, 'birth_date') or
            deep_search(src, 'date_of_birth') or
            deep_search(src, 'дата рождения')
        )
        age = age or deep_search(src, 'age')

        for key in ['vk', 'vkontakte', 'instagram', 'tiktok', 'ok', 'odnoklassniki', 'twitter', 'facebook']:
            value = deep_search(src, key)
            if value and key not in socials:
                socials[key] = value

        bank = deep_search(src, 'bank') or deep_search(src, 'banks') or deep_search(src, 'account')
        if bank:
            if isinstance(bank, list):
                banks.extend(bank)
            else:
                banks.append(bank)

        email = deep_search(src, 'email') or deep_search(src, 'mail') or deep_search(src, 'e-mail')
        if email:
            if isinstance(email, list):
                emails.extend(email)
            else:
                emails.append(email)

    # Нормализуем banks и emails
    banks = normalize_values(banks)
    emails = normalize_values(emails)

    # ---- Для ID собираем дополнительные данные ----
    telegram_id = None
    reg_date = None
    username = None
    groups = []
    interests = []
    history = []
    gift_ids = []

    if not is_phone:
        for src in [bigbase, nightsearch, seon]:
            if not src:
                continue
            if not telegram_id:
                telegram_id = (
                    deep_search(src, 'telegram_id') or
                    deep_search(src, 'tg_id') or
                    deep_search(src, 'user_id')
                )
            if not username:
                username = (
                    deep_search(src, 'username') or
                    deep_search(src, 'user_name') or
                    deep_search(src, 'telegram_username') or
                    deep_search(src, 'nick')
                )
            if not reg_date:
                reg_date = deep_search(src, 'registration_date') or deep_search(src, 'reg_date') or deep_search(src, 'date')
            if not groups:
                grp = deep_search(src, 'groups') or deep_search(src, 'chats')
                if grp:
                    if isinstance(grp, list):
                        groups.extend(grp)
                    else:
                        groups.append(grp)
            if not interests:
                intr = deep_search(src, 'interests')
                if intr:
                    if isinstance(intr, list):
                        interests.extend(intr)
                    else:
                        interests.append(intr)
            hist = deep_search(src, 'username_history') or deep_search(src, 'history') or deep_search(src, 'name_history')
            if hist:
                if isinstance(hist, list):
                    for item in hist:
                        if isinstance(item, dict):
                            date = item.get('date') or item.get('time')
                            nick = item.get('username') or item.get('name')
                            name = item.get('full_name') or item.get('first_name')
                            uid = item.get('id') or item.get('tg_id')
                            if nick:
                                history.append({'date': date, 'nick': nick, 'name': name, 'id': uid})
                        elif isinstance(item, str):
                            parts = re.split(r'→|,', item)
                            if len(parts) >= 2:
                                date_part = parts[0].strip()
                                rest = parts[1].strip()
                                nick_match = re.search(r'(@\w+)', rest)
                                nick = nick_match.group(1) if nick_match else None
                                id_match = re.search(r'\b(\d{10,12})\b', rest)
                                uid = id_match.group(1) if id_match else None
                                name = rest
                                if nick:
                                    name = name.replace(nick, '').strip()
                                if uid:
                                    name = name.replace(uid, '').strip()
                                if name and name.endswith(','):
                                    name = name[:-1].strip()
                                history.append({'date': date_part, 'nick': nick, 'name': name, 'id': uid})
                elif isinstance(hist, str):
                    for line in hist.split('\n'):
                        if line.strip():
                            parts = re.split(r'→|,', line)
                            if len(parts) >= 2:
                                date_part = parts[0].strip()
                                rest = parts[1].strip()
                                nick_match = re.search(r'(@\w+)', rest)
                                nick = nick_match.group(1) if nick_match else None
                                id_match = re.search(r'\b(\d{10,12})\b', rest)
                                uid = id_match.group(1) if id_match else None
                                name = rest
                                if nick:
                                    name = name.replace(nick, '').strip()
                                if uid:
                                    name = name.replace(uid, '').strip()
                                if name and name.endswith(','):
                                    name = name[:-1].strip()
                                history.append({'date': date_part, 'nick': nick, 'name': name, 'id': uid})
            gft = deep_search(src, 'gifts') or deep_search(src, 'presents')
            if gft:
                if isinstance(gft, list):
                    for item in gft:
                        if isinstance(item, dict):
                            for val in item.values():
                                if isinstance(val, str):
                                    nums = re.findall(r'\b\d{10,12}\b', val)
                                    gift_ids.extend(nums)
                                elif isinstance(val, int):
                                    gift_ids.append(str(val))
                        elif isinstance(item, str):
                            nums = re.findall(r'\b\d{10,12}\b', item)
                            gift_ids.extend(nums)
                elif isinstance(gft, str):
                    nums = re.findall(r'\b\d{10,12}\b', gft)
                    gift_ids.extend(nums)

        # Нормализуем списки
        groups = normalize_values(groups)
        interests = normalize_values(interests)
        gift_ids = normalize_values(gift_ids)
        
        # Уникализация истории (оставляем как есть, она из словарей)
        seen_hist = set()
        unique_history = []
        for h in history:
            key = (h.get('date'), h.get('nick'), h.get('id'))
            if key not in seen_hist:
                seen_hist.add(key)
                unique_history.append(h)
        history = unique_history

    # Парсим Telegram-аккаунты
    telegrams = parse_telegrams(telegram_raw)
    if telegram_id and username:
        exists = any(uid == telegram_id for _, uid in telegrams)
        if not exists:
            telegrams.append((username, telegram_id))

    merged = {
        'query': query,
        'operator': op,
        'region': reg,
        'country': country,
        'contacts': contacts,
        'telegrams': telegrams,
        'telegram_id': telegram_id,
        'reg_date': reg_date,
        'username': username,
        'groups': groups,
        'interests': interests,
        'username_history': history,
        'gift_ids': gift_ids,
        'fio': fio,
        'birthdate': birthdate,
        'age': age,
        'socials': socials,
        'banks': banks,
        'emails': emails
    }

    full_data = {
        'bigbase': bigbase,
        'nightsearch': nightsearch,
        'seon': seon,
        'jitler': jitler,
        'merged': merged
    }
    return full_data

# ===== ФОРМАТИРОВАНИЕ ДЛЯ ТЕЛЕФОНА =====
def format_phone_report(merged: dict, full_data: dict, phone: str, views: int) -> str:
    lines = []
    lines.append("📱")
    lines.append(f"├ Телефон: {phone}")
    if merged.get('operator'):
        lines.append(f"├ Оператор: {merged['operator']}")
    if merged.get('region'):
        lines.append(f"├ Регион: {merged['region']}")
    if merged.get('country'):
        lines.append(f"└ Страна: {merged['country']}")

    fio = merged.get('fio')
    birthdate = merged.get('birthdate')
    age = merged.get('age')
    if birthdate:
        calculated_age = calculate_age(birthdate)
        if calculated_age is not None:
            age = calculated_age

    if fio or birthdate or age is not None:
        lines.append("\n👤 Основные данные")
        if fio:
            lines.append(f"├ ФИО: {fio}")
        if birthdate:
            lines.append(f"├ Дата рождения: {birthdate}")
        if age is not None:
            lines.append(f"└ Возраст: {age}")

    if merged.get('contacts'):
        contacts_str = ", ".join(merged['contacts'])
        lines.append(f"\n🔎 Телефонные книги: {contacts_str}")

    socials = merged.get('socials', {})
    banks = merged.get('banks', [])
    emails = merged.get('emails', [])

    if socials:
        lines.append("\n🧑‍💻 Социальные сети:")
        for platform, val in socials.items():
            if platform in ('vk', 'vkontakte'):
                lines.append(f"├ Вконтакте: {val}")
            elif platform == 'instagram':
                lines.append(f"├ Instagram: {val}")
            elif platform == 'tiktok':
                lines.append(f"├ TikTok: {val}")
            elif platform in ('ok', 'odnoklassniki'):
                lines.append(f"├ Одноклассники: {val}")
            else:
                lines.append(f"├ {platform.capitalize()}: {val}")

    if banks:
        unique_banks = list(dict.fromkeys(banks))
        lines.append(f"\n🏦 Мобильный банк: {', '.join(unique_banks)}")

    if merged.get('telegrams'):
        tg_list = merged['telegrams']
        if len(tg_list) == 1:
            uname, uid = tg_list[0]
            lines.append(f"\n💬 Telegram: {uname} {uid}")
        else:
            lines.append("\n💬 Telegram:")
            for uname, uid in tg_list:
                lines.append(f"├ {uname} {uid}")

    if emails:
        unique_emails = list(dict.fromkeys(emails))
        lines.append(f"\n📧 E-mail: {', '.join(unique_emails)}")

    lines.append(f"\n👁 Интересовались этим: {views}")
    return "\n".join(lines)

# ===== ФОРМАТИРОВАНИЕ ДЛЯ ID =====
def format_id_report(merged: dict, full_data: dict, query: str, views: int) -> str:
    lines = []
    lines.append("```")
    if merged.get('telegram_id'):
        lines.append(f"# Telegram ID: {merged['telegram_id']}")
    if merged.get('username'):
        lines.append(f"# Username: {merged['username']}")
    if merged.get('reg_date'):
        lines.append(f"# Дата регистрации: {merged['reg_date']}")
    if merged.get('fio'):
        lines.append(f"# ФИО: {merged['fio']}")
    if merged.get('birthdate'):
        lines.append(f"# Дата рождения: {merged['birthdate']}")
    if merged.get('age'):
        lines.append(f"# Возраст: {merged['age']}")
    if merged.get('emails'):
        lines.append(f"# Email: {', '.join(merged['emails'])}")
    if merged.get('socials'):
        soc_str = ", ".join([f"{k}: {v}" for k, v in merged['socials'].items()])
        lines.append(f"# Соцсети: {soc_str}")
    if merged.get('banks'):
        lines.append(f"# Банки: {', '.join(merged['banks'])}")
    if merged.get('contacts'):
        lines.append(f"# Телефонные книги: {', '.join(merged['contacts'])}")
    lines.append("")

    telegrams = merged.get('telegrams', [])
    if telegrams:
        lines.append("# Найденные аккаунты:")
        for uname, uid in telegrams:
            lines.append(f"{uname} {uid}")
        lines.append("")

    groups = merged.get('groups', [])
    if groups:
        lines.append(f"# Группы [{len(groups)}]:")
        for group in groups:
            lines.append(group)
        lines.append("")

    interests = merged.get('interests', [])
    if interests:
        lines.append(f"# Интересы [{len(interests)}]:")
        for interest in interests:
            lines.append(interest)
        lines.append("")

    gift_ids = merged.get('gift_ids', [])
    if gift_ids:
        lines.append("# Подарочные связи:")
        lines.append(", ".join(gift_ids))
        lines.append("")

    history = merged.get('username_history', [])
    if history:
        lines.append("# История изменения имени:")
        for entry in history:
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
            line = " ".join(parts)
            lines.append(line)
        lines.append("")

    lines.append(f"👁 Интересовались этим: {views}")
    lines.append("```")
    return "\n".join(lines)

# ===== БОТ =====
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ----- ПРИВЕТСТВИЕ И МЕНЮ -----
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
        await message.reply("✅ Вы зарегистрированы! Доступ к поиску безлимитный.")
    else:
        await message.reply("👋 Добро пожаловать обратно!")

    help_text = (
        "**Добро пожаловать, агент!**\n\n"
        "ℹ️ Примеры для ввода команд:\n\n"
        "🕵️ **Личность:**\n"
        "Навальный Алексей Анатольевич 04.06.1976 \n"
        "(Можно искать и по неполным данным: ФИО, возрасту или части даты рождения.)\n\n"
        "📲 **Контакты:**\n"
        "79999688666 – номер телефона\n"
        "79999688666@mail.ru – email\n\n"
        "🚘 **Транспорт:**\n"
        "В395ОК199 – номер автомобиля\n"
        "XTA211440C5106924 – VIN автомобиля\n\n"
        "💬 **Социальные сети:**\n"
        "vk.com/sherlock – Вконтакте\n"
        "tiktok.com/@sherlock – Tiktok\n"
        "instagram.com/sherlock – Instagram\n"
        "ok.ru/profile/58460 – Одноклассники\n\n"
        "📟 **Telegram:**\n"
        "@sherlock, tg123456 – логин или ID\n\n"
        "📄 **Документы:**\n"
        "/vu 1234567890 – водительские права\n"
        "/passport 1234567890 – паспорт\n"
        "/snils 12345678901 – СНИЛС\n"
        "/inn 123456789012 – ИНН\n\n"
        "🌐 **Онлайн-следы:**\n"
        "/tag хирург москва – поиск по телефонным книгам\n"
        "sherlock.com или 1.1.1.1 – домен или IP\n\n"
        "🏚 **Недвижимость:**\n"
        "/adr Город, Улица, 1\n"
        "77:01:0004042:6987 - кадастровый номер\n\n"
        "🏢 **Юридическое лицо:**\n"
        "/inn 2540214547 – ИНН\n"
        "1107449004464 – ОГРН или ОГРНИП"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🤖 Мои боты", callback_data="my_bots")],
        [InlineKeyboardButton(text="🤝 Партнёрская программа", callback_data="referral")]
    ])

    await message.reply(help_text, parse_mode="Markdown", reply_markup=keyboard)

# ----- ОБРАБОТЧИКИ КНОПОК МЕНЮ -----
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
        "Приглашайте друзей и получайте бонусы!\n"
        "За каждого приглашённого, который зарегистрируется, вы получите +2 запроса (но у вас и так бесконечно).\n\n"
        f"Ваша реферальная ссылка:\n{link}"
    )
    await callback.message.edit_text(text)  # без Markdown
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
        await message.reply(f"✅ Бот @{username} добавлен как зеркало! Запросы бесконечные.")
    except Exception as e:
        await message.reply(f"❌ Ошибка проверки токена: {e}")

# ----- КОМАНДА /raw ДЛЯ ОТЛАДКИ -----
@dp.message(Command("raw"))
async def raw_cmd(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите номер: `/raw 79220517377`")
        return
    query = args[1].strip()
    digits = re.sub(r'\D', '', query)
    if not digits or len(digits) < 10:
        await message.reply("❌ Введите корректный номер (10-12 цифр).")
        return
    data = await collect_data(digits, "number", is_phone=True)
    text_json = json.dumps(data, ensure_ascii=False, indent=2)
    if len(text_json) > 4000:
        text_json = text_json[:4000] + "\n... (обрезано)"
    await message.reply(f"```\n{text_json}\n```", parse_mode="Markdown")

# ----- ОБРАБОТЧИКИ ДЛЯ КОМАНД ДОКУМЕНТОВ -----
@dp.message(Command("vu"))
@dp.message(Command("passport"))
@dp.message(Command("snils"))
@dp.message(Command("inn"))
@dp.message(Command("adr"))
@dp.message(Command("tag"))
async def document_command(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Укажите идентификатор после команды, например: `/vu 1234567890`")
        return
    id_str = args[1].strip()
    if not id_str.isdigit():
        await message.reply("❌ Идентификатор должен быть числом.")
        return
    await show_direction_choice(message, id_str)

# ----- ПОКАЗ КЛАВИАТУРЫ ВЫБОРА НАПРАВЛЕНИЯ -----
async def show_direction_choice(message: types.Message, id_str: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="ИНН (юр. лицо)", callback_data=f"dir_inn_{id_str}")],
        [InlineKeyboardButton(text="Паспорт", callback_data=f"dir_passport_{id_str}")],
        [InlineKeyboardButton(text="Водительское удостоверение", callback_data=f"dir_driver_{id_str}")],
        [InlineKeyboardButton(text="ВКонтакте", callback_data=f"dir_vk_{id_str}")],
        [InlineKeyboardButton(text="Telegram", callback_data=f"dir_tg_{id_str}")]
    ])
    await message.reply(f"Обнаружен идентификатор: {id_str}\n\nВыберите направление для поиска:", reply_markup=keyboard)

# ----- ОБРАБОТЧИК ВЫБОРА НАПРАВЛЕНИЯ -----
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
        merged = cached.get('merged', {})
        views = await get_unique_views_id(id_str, callback.from_user.id)
        detailed = format_id_report(merged, cached, id_str, views)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📲 Открыть в Telegram", url=f"tg://user?id={id_str}")]
        ])
        await callback.message.edit_text(detailed, parse_mode="Markdown", reply_markup=keyboard)
        await callback.answer()
        return

    if direction == "inn":
        search_type = "sherlock"
    elif direction == "passport":
        search_type = "sherlock"
    elif direction == "driver":
        search_type = "sherlock"
    elif direction == "vk":
        search_type = "vks"
    elif direction == "tg":
        search_type = "sherlock"
    else:
        await callback.answer("Неизвестное направление")
        return

    status_msg = await callback.message.edit_text(f"🔍 Поиск по {direction}...")
    data = await collect_data(id_str, search_type, is_phone=False)
    merged = data.get('merged', {})
    has_data = any([
        merged.get('telegrams'),
        merged.get('groups'),
        merged.get('interests'),
        merged.get('username_history'),
        merged.get('gift_ids'),
        merged.get('telegram_id'),
        merged.get('username'),
        merged.get('fio'),
        merged.get('emails'),
        merged.get('socials')
    ])
    if has_data:
        await save_id_report(cache_key, data)

    views = await get_unique_views_id(id_str, callback.from_user.id)
    detailed = format_id_report(merged, data, id_str, views)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Открыть в Telegram", url=f"tg://user?id={id_str}")]
    ])
    await status_msg.edit_text(detailed, parse_mode="Markdown", reply_markup=keyboard)
    await callback.answer()

# ===== ОБРАБОТЧИК ДЛЯ ЧИСЕЛ (телефон или ID) =====
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
        cached = await get_report(digits)
        if cached:
            merged = cached.get('merged', {})
            views = await get_unique_views_phone(digits, message.from_user.id)
            detailed = format_phone_report(merged, cached, digits, views)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📲 Telegram", url=f"tg://resolve?phone={digits}"),
                 InlineKeyboardButton(text="💬 WhatsApp", url=f"https://wa.me/{digits}")]
            ])
            await message.reply(detailed, parse_mode="Markdown", reply_markup=keyboard)
            return

        status = await message.reply("🔍 Сбор данных...")
        data = await collect_data(digits, "number", is_phone=True)
        merged = data.get('merged', {})
        has_data = any([
            merged.get('operator'),
            merged.get('region'),
            merged.get('country'),
            merged.get('contacts'),
            merged.get('telegrams'),
            merged.get('fio'),
            merged.get('birthdate'),
            merged.get('emails'),
            merged.get('socials'),
            merged.get('banks')
        ])
        if has_data:
            await save_report(digits, data)
        views = await get_unique_views_phone(digits, message.from_user.id)
        detailed = format_phone_report(merged, data, digits, views)
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📲 Telegram", url=f"tg://resolve?phone={digits}"),
             InlineKeyboardButton(text="💬 WhatsApp", url=f"https://wa.me/{digits}")]
        ])
        await status.edit_text(detailed, parse_mode="Markdown", reply_markup=keyboard)
    else:
        await show_direction_choice(message, digits)

# ===== ВЕБ-СЕРВЕР ДЛЯ HEALTH CHECK =====
async def health_check(request):
    return web.Response(text="OK", status=200)

# ===== ОСНОВНАЯ ФУНКЦИЯ =====
async def main():
    await init_db()
    print("🚀 Бот запущен (исправленная версия с normalize_values)")

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
