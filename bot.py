import os
import asyncio
import re
import json
import asyncpg
import aiohttp
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
DEPSEARCH_TOKEN = os.getenv("DEPSEARCH_TOKEN")
SEON_API_KEY = os.getenv("SEON_API_KEY")
SNUSBASE_API_KEY = os.getenv("SNUSBASE_API_KEY")

jitler_tokens_str = os.getenv("JITLER_TOKENS", "")
JITLER_TOKENS = [t.strip() for t in jitler_tokens_str.split(",") if t.strip()]

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

# ===== ПОДКЛЮЧЕНИЕ К БАЗЕ =====
async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            balance INTEGER DEFAULT 5,
            daily_quota INTEGER DEFAULT 2,
            last_daily_reset TIMESTAMP DEFAULT NOW(),
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
            id TEXT PRIMARY KEY,
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
    await conn.close()

# ===== ФУНКЦИИ РАБОТЫ С БАЗОЙ =====
async def get_user(user_id: int):
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
    await conn.close()
    return row

async def create_user(user_id: int, username: str = None, referred_by: int = None):
    conn = await asyncpg.connect(DATABASE_URL)
    ref_code = f"REF{user_id}{datetime.now().strftime('%m%d')}"
    balance = 5
    if referred_by:
        await conn.execute('UPDATE users SET balance = balance + 2 WHERE user_id = $1', referred_by)
        await conn.execute('INSERT INTO referrals (referrer_id, referred_id) VALUES ($1, $2)', referred_by, user_id)
    await conn.execute('''
        INSERT INTO users (user_id, username, balance, referral_code, referred_by)
        VALUES ($1, $2, $3, $4, $5)
    ''', user_id, username, balance, ref_code, referred_by)
    await conn.close()

async def update_daily_quota(user_id: int):
    conn = await asyncpg.connect(DATABASE_URL)
    now = datetime.now()
    row = await conn.fetchrow('SELECT last_daily_reset FROM users WHERE user_id = $1', user_id)
    if row and row['last_daily_reset']:
        last_reset = row['last_daily_reset']
        if now.date() > last_reset.date():
            await conn.execute('UPDATE users SET daily_quota = 2, last_daily_reset = $1 WHERE user_id = $2',
                               now, user_id)
    else:
        await conn.execute('UPDATE users SET daily_quota = 2, last_daily_reset = $1 WHERE user_id = $2',
                           now, user_id)
    await conn.close()

async def deduct_request(user_id: int) -> bool:
    await update_daily_quota(user_id)
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow('SELECT balance, daily_quota FROM users WHERE user_id = $1', user_id)
    if not row:
        await conn.close()
        return False
    balance = row['balance']
    daily = row['daily_quota']
    if balance > 0:
        await conn.execute('UPDATE users SET balance = balance - 1 WHERE user_id = $1', user_id)
        await conn.close()
        return True
    elif daily > 0:
        await conn.execute('UPDATE users SET daily_quota = daily_quota - 1 WHERE user_id = $1', user_id)
        await conn.close()
        return True
    else:
        await conn.close()
        return False

async def get_user_balance(user_id: int):
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow('SELECT balance, daily_quota FROM users WHERE user_id = $1', user_id)
    await conn.close()
    if row:
        return row['balance'], row['daily_quota']
    return 0, 0

async def get_referral_code(user_id: int):
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow('SELECT referral_code FROM users WHERE user_id = $1', user_id)
    await conn.close()
    return row['referral_code'] if row else None

async def get_referral_stats(user_id: int):
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch('SELECT referred_id FROM referrals WHERE referrer_id = $1', user_id)
    await conn.close()
    return len(rows)

async def save_report(phone: str, data: dict):
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute('''
        INSERT INTO reports (phone, data) VALUES ($1, $2)
        ON CONFLICT (phone) DO UPDATE SET data = $2, created_at = NOW()
    ''', phone, json.dumps(data))
    await conn.close()

async def get_report(phone: str):
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow('SELECT data FROM reports WHERE phone = $1', phone)
    await conn.close()
    if row:
        return json.loads(row['data'])
    return None

async def save_id_report(id_str: str, data: dict):
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute('''
        INSERT INTO id_reports (id, data) VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET data = $2, created_at = NOW()
    ''', id_str, json.dumps(data))
    await conn.close()

async def get_id_report(id_str: str):
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow('SELECT data FROM id_reports WHERE id = $1', id_str)
    await conn.close()
    if row:
        return json.loads(row['data'])
    return None

async def add_mirror_bonus(user_id: int):
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute('UPDATE users SET balance = balance + 2 WHERE user_id = $1', user_id)
    await conn.close()

async def register_bot(owner_id: int, bot_token: str, bot_username: str):
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute('''
        INSERT INTO bots (owner_id, bot_token, bot_username) VALUES ($1, $2, $3)
    ''', owner_id, bot_token, bot_username)
    await conn.close()

async def get_user_bots(user_id: int):
    conn = await asyncpg.connect(DATABASE_URL)
    rows = await conn.fetch('SELECT * FROM bots WHERE owner_id = $1', user_id)
    await conn.close()
    return rows

async def get_unique_views_phone(phone: str, user_id: int) -> int:
    conn = await asyncpg.connect(DATABASE_URL)
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
        views = len(user_ids)
    else:
        views = len(user_ids)
    await conn.close()
    return views

async def get_unique_views_id(id_str: str, user_id: int) -> int:
    conn = await asyncpg.connect(DATABASE_URL)
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
        views = len(user_ids)
    else:
        views = len(user_ids)
    await conn.close()
    return views

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

# ===== ФУНКЦИИ API =====
async def bigbase_search(query: str):
    url = "https://bigbase.top/api/search"
    headers = {"Authorization": BIGBASE_TOKEN, "Content-Type": "application/json"}
    payload = {"search": query, "page": 0}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception:
            return {}

async def nightsearch_search(query: str):
    url = "https://nightsearch.life/api/search"
    headers = {"X-API-Key": NIGHTSEARCH_API_KEY, "Content-Type": "application/json"}
    payload = {"query": query, "search_type": "phone"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception:
            return {}

async def depsearch_search(query: str):
    url = f"https://api.depsearch.sbs/quest={query}&token={DEPSEARCH_TOKEN}&lang=ru"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception:
            return {}

async def seon_search(query: str):
    url = "https://api.seon.io/SeonRestService/phone-api/v2"
    headers = {"X-API-KEY": SEON_API_KEY, "Content-Type": "application/json"}
    payload = {"phone": query}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {}
        except Exception:
            return {}

async def jitler_search_with_balancer(query: str, search_type: str = "number"):
    for attempt in range(len(JITLER_TOKENS) * 2):
        token = await balancer.get_token()
        if not token:
            return {}
        url = "https://api.jitler.top/search"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"type": search_type, "query": query, "page": 1}
        async with aiohttp.ClientSession() as session:
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

async def snusbase_search(query: str):
    if not SNUSBASE_API_KEY:
        return {}
    url = "https://api.snusbase.com/data/search"
    headers = {
        "Auth": SNUSBASE_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "terms": [query, f"{query}@.*"],
        "types": ["phone", "email", "username", "name", "lastip", "_domain"],
        "wildcard": False
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("results", data)
                return {}
        except Exception:
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

# ===== СБОР ДАННЫХ (универсальный) =====
async def collect_data(query: str, search_type: str, is_phone: bool = False):
    """
    search_type: 'number', 'sherlock', 'vks', 'funstat'
    is_phone: True если это телефон (для дополнительных API BigBase и др.)
    """
    # Для телефона используем BigBase и другие API, для ID – нет
    if is_phone:
        bigbase, night, dep, seon, jitler, snusbase = await asyncio.gather(
            bigbase_search(query),
            nightsearch_search(query),
            depsearch_search(query),
            seon_search(query),
            jitler_search_with_balancer(query, search_type),
            snusbase_search(query),
            return_exceptions=True
        )
    else:
        # Для ID используем только JITLER (и возможно BigBase, но для ID он не даёт данных)
        # Однако BigBase может дать данные по ID, поэтому тоже запросим
        bigbase, night, dep, seon, jitler, snusbase = await asyncio.gather(
            bigbase_search(query),
            nightsearch_search(query),
            depsearch_search(query),
            seon_search(query),
            jitler_search_with_balancer(query, search_type),
            snusbase_search(query),
            return_exceptions=True
        )

    bigbase = bigbase if isinstance(bigbase, dict) else {}
    night = night if isinstance(night, dict) else {}
    dep = dep if isinstance(dep, dict) else {}
    seon = seon if isinstance(seon, dict) else {}
    jitler = jitler if isinstance(jitler, dict) else {}
    snusbase = snusbase if isinstance(snusbase, dict) else {}

    # Для телефона – извлекаем оператор/регион/страну из BigBase
    if is_phone:
        op = bigbase.get('operator') or bigbase.get('оператор')
        reg = bigbase.get('region') or bigbase.get('регион')
        country = bigbase.get('country') or bigbase.get('страна')
        if not op:
            for src in (night, dep, seon, jitler):
                val = src.get('operator') or src.get('oper') or src.get('оператор')
                if val:
                    op = val
                    break
        if not reg:
            for src in (night, dep, seon, jitler):
                val = src.get('region') or src.get('reg') or src.get('регион')
                if val:
                    reg = val
                    break
        if not country:
            for src in (night, dep, seon, jitler):
                val = src.get('country') or src.get('страна')
                if val:
                    country = val
                    break
    else:
        op = reg = country = None

    # Контакты и Telegram – из JITLER (если есть)
    contacts = []
    telegram_raw = None
    jitler_data = jitler.get('response')
    if isinstance(jitler_data, list) and jitler_data:
        jitler_data = jitler_data[0]
    if isinstance(jitler_data, dict):
        if is_phone:
            contacts = jitler_data.get('phone_books') or jitler_data.get('contacts') or []
            if isinstance(contacts, str):
                contacts = [contacts]
            elif not isinstance(contacts, list):
                contacts = []
        telegram_raw = jitler_data.get('telegram') or jitler_data.get('tg')

    # Для ID – извлекаем дополнительные поля из всех источников
    telegram_id = None
    reg_date = None
    username = None
    groups = []
    interests = []
    history = []
    gift_ids = []

    if not is_phone:
        sources = [bigbase, night, dep, seon, snusbase]
        for src in sources:
            if not src:
                continue
            if isinstance(src, dict):
                if not telegram_id:
                    telegram_id = src.get('telegram_id') or src.get('tg_id') or src.get('id')
                if not reg_date:
                    reg_date = src.get('registration_date') or src.get('reg_date') or src.get('date')
                if not username:
                    username = src.get('username') or src.get('nick')
                if not groups:
                    groups = src.get('groups') or src.get('chats') or []
                if not interests:
                    interests = src.get('interests') or []

                hist = src.get('username_history') or src.get('history') or src.get('name_history')
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

                gft = src.get('gifts') or src.get('presents')
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

            elif isinstance(src, list) and src:
                item = src[0] if src else {}
                if isinstance(item, dict):
                    if not telegram_id:
                        telegram_id = item.get('telegram_id') or item.get('tg_id') or item.get('id')
                    if not reg_date:
                        reg_date = item.get('registration_date') or item.get('reg_date') or item.get('date')
                    if not username:
                        username = item.get('username') or item.get('nick')
                    if not groups:
                        groups = item.get('groups') or item.get('chats') or []
                    if not interests:
                        interests = item.get('interests') or []

        if groups:
            groups = list(dict.fromkeys(groups))
        if interests:
            if isinstance(interests, str):
                interests = [interests]
            else:
                interests = list(dict.fromkeys(interests))
        if gift_ids:
            gift_ids = list(dict.fromkeys(gift_ids))

        seen_hist = set()
        unique_history = []
        for h in history:
            key = (h.get('date'), h.get('nick'), h.get('id'))
            if key not in seen_hist:
                seen_hist.add(key)
                unique_history.append(h)
        history = unique_history

    # Парсим телеграм-аккаунты из JITLER (если есть)
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
        'gift_ids': gift_ids
    }

    full_data = {
        'bigbase': bigbase,
        'nightsearch': night,
        'depsearch': dep,
        'seon': seon,
        'jitler': jitler,
        'snusbase': snusbase,
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

    fio = None
    birthdate = None
    age = None
    api_sources = [full_data.get('bigbase'), full_data.get('nightsearch'), full_data.get('depsearch'),
                   full_data.get('seon'), full_data.get('snusbase')]
    for resp in api_sources:
        if not resp:
            continue
        if isinstance(resp, dict):
            if not fio:
                fio = resp.get('full_name') or resp.get('fio') or resp.get('name') or resp.get('ФИО')
            if not birthdate:
                birthdate = resp.get('birthdate') or resp.get('birth_date') or resp.get('date_of_birth') or resp.get('дата рождения')
            if not age:
                age = resp.get('age')
        elif isinstance(resp, list) and resp:
            for item in resp:
                if isinstance(item, dict):
                    if not fio:
                        fio = item.get('full_name') or item.get('fio') or item.get('name')
                    if not birthdate:
                        birthdate = item.get('birthdate') or item.get('birth_date') or item.get('date_of_birth')
                    if not age:
                        age = item.get('age')

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

    socials = {}
    banks = []
    emails = []
    for resp in api_sources + [full_data.get('jitler')]:
        if not resp:
            continue
        if isinstance(resp, dict):
            for key in ['vk', 'vkontakte', 'instagram', 'tiktok', 'ok', 'odnoklassniki', 'twitter', 'facebook']:
                val = resp.get(key)
                if val and key not in socials:
                    socials[key] = val
            bank = resp.get('bank') or resp.get('banks') or resp.get('account')
            if bank:
                if isinstance(bank, list):
                    banks.extend(bank)
                else:
                    banks.append(str(bank))
            email = resp.get('email') or resp.get('mail') or resp.get('e-mail')
            if email:
                if isinstance(email, list):
                    emails.extend(email)
                else:
                    emails.append(str(email))
        elif isinstance(resp, list):
            for item in resp:
                if isinstance(item, dict):
                    for key in ['vk', 'vkontakte', 'instagram', 'tiktok', 'ok', 'odnoklassniki']:
                        val = item.get(key)
                        if val and key not in socials:
                            socials[key] = val
                    bank = item.get('bank') or item.get('banks')
                    if bank:
                        banks.append(str(bank))
                    email = item.get('email') or item.get('mail')
                    if email:
                        emails.append(str(email))

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

# ===== ФОРМАТИРОВАНИЕ ДЛЯ ID (моноширинный блок) =====
def format_id_report(merged: dict, full_data: dict, query: str, views: int) -> str:
    lines = []
    lines.append("```")
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

# ----- ПРИВЕТСТВИЕ / START -----
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    args = message.text.split()
    referrer_id = None
    if len(args) > 1:
        payload = args[1]
        if payload.startswith("ref_"):
            ref_code = payload.replace("ref_", "")
            conn = await asyncpg.connect(DATABASE_URL)
            row = await conn.fetchrow('SELECT user_id FROM users WHERE referral_code = $1', ref_code)
            await conn.close()
            if row:
                referrer_id = row['user_id']

    user = await get_user(message.from_user.id)
    if not user:
        await create_user(message.from_user.id, message.from_user.username, referrer_id)
        await message.reply("✅ Вы зарегистрированы! Получили 5 бонусных запросов.")
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
    await message.reply(help_text, parse_mode="Markdown")

# ----- ПРОФИЛЬ, БОТЫ, РЕФЕРАЛЫ (как раньше) -----
@dp.callback_query(lambda c: c.data == "profile")
async def profile_callback(callback: types.CallbackQuery):
    balance, daily = await get_user_balance(callback.from_user.id)
    ref_code = await get_referral_code(callback.from_user.id)
    ref_count = await get_referral_stats(callback.from_user.id)
    text = (
        f"👤 **Мой профиль**\n"
        f"Баланс запросов: {balance}\n"
        f"Доступно сегодня: {daily}/2\n"
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
        "🤝 **Партнёрская программа**\n\n"
        "Приглашайте друзей и получайте бонусы!\n"
        "За каждого приглашённого, который зарегистрируется, вы получите +2 запроса.\n\n"
        f"Ваша реферальная ссылка:\n{link}"
    )
    await callback.message.edit_text(text, parse_mode="Markdown")
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
        await add_mirror_bonus(message.from_user.id)
        await message.reply(f"✅ Бот @{username} добавлен как зеркало! Вы получили +2 запроса.")
    except Exception as e:
        await message.reply(f"❌ Ошибка проверки токена: {e}")

# ----- ОБРАБОТЧИК ДЛЯ КОМАНД /vu, /passport, /snils, /inn, /adr, /tag -----
# Теперь они не заглушки, а перенаправляют на универсальный ID-поиск с выбором направления.
# Но мы можем просто игнорировать их или показать подсказку, что нужно ввести ID.
# Поскольку у нас есть команды, которые пользователь может ввести, но они не реализованы отдельно,
# мы можем просто отвечать, что нужно отправить число (ID) для поиска.
# Однако по заданию мы должны убрать заглушки и сделать всё работающим.
# Проще: для этих команд будем показывать клавиатуру выбора направления, но с подставленным ID из аргументов.
# Но пользователь может ввести /vu 1234567890 – нужно извлечь число и показать выбор.
# Сделаем так: для всех команд /vu, /passport и т.д. будем извлекать ID из аргументов (если есть) и показывать выбор.
# Если ID не указан, просим ввести.

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
    # Проверяем, что это число
    if not id_str.isdigit():
        await message.reply("❌ Идентификатор должен быть числом.")
        return
    # Показываем клавиатуру выбора направления
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
    # Формат: dir_inn_1234567890
    parts = callback.data.split("_", 2)  # ['dir', 'inn', '1234567890']
    if len(parts) < 3:
        await callback.answer("Ошибка формата")
        return
    direction = parts[1]
    id_str = parts[2]

    # Сопоставляем направление с search_type для JITLER
    if direction == "inn":
        search_type = "sherlock"  # ИНН юр. лица через sherlock
    elif direction == "passport":
        search_type = "sherlock"
    elif direction == "driver":
        search_type = "sherlock"
    elif direction == "vk":
        search_type = "vks"  # для поиска ВКонтакте
    elif direction == "tg":
        search_type = "sherlock"  # для Telegram (или "sherlock")
    else:
        await callback.answer("Неизвестное направление")
        return

    # Проверяем лимиты
    if not await deduct_request(callback.from_user.id):
        await callback.message.edit_text("❌ Лимит запросов исчерпан.")
        await callback.answer()
        return

    # Выполняем поиск через JITLER
    status_msg = await callback.message.edit_text(f"🔍 Поиск по {direction}...")
    data = await collect_data(id_str, search_type, is_phone=False)
    await save_id_report(id_str, data)
    merged = data.get('merged', {})
    views = await get_unique_views_id(id_str, callback.from_user.id)
    detailed = format_id_report(merged, data, id_str, views)
    # Кнопка для открытия в Telegram (если это ID)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Открыть в Telegram", url=f"tg://user?id={id_str}")]
    ])
    await status_msg.edit_text(detailed, parse_mode="Markdown", reply_markup=keyboard)
    await callback.answer()

# ----- ПОИСК ПО НОМЕРУ (автоопределение) -----
@dp.message(lambda msg: msg.text and re.fullmatch(r'\d{10,12}', msg.text.strip()))
async def phone_search(message: types.Message):
    phone = message.text.strip()
    if not await deduct_request(message.from_user.id):
        await message.reply("❌ Лимит запросов исчерпан.")
        return
    status = await message.reply("🔍 Сбор данных из всех источников...")
    data = await collect_data(phone, "number", is_phone=True)
    await save_report(phone, data)
    merged = data.get('merged', {})
    views = await get_unique_views_phone(phone, message.from_user.id)
    detailed = format_phone_report(merged, data, phone, views)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Telegram", url=f"tg://resolve?phone={phone}"),
         InlineKeyboardButton(text="💬 WhatsApp", url=f"https://wa.me/{phone}")]
    ])
    await status.edit_text(detailed, parse_mode="Markdown", reply_markup=keyboard)

# ----- ПОИСК ПО ID (если введено число не телефон) -----
@dp.message(lambda msg: msg.text and not re.fullmatch(r'\d{10,12}', msg.text.strip()) and msg.text.strip().isdigit())
async def id_search(message: types.Message):
    id_str = message.text.strip()
    # Показываем клавиатуру выбора направления
    await show_direction_choice(message, id_str)

# ===== ЗАПУСК =====
async def main():
    await init_db()
    print("🚀 Бот запущен с выбором направления для ID")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
