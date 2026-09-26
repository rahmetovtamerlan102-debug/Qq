import os
import re
import json
import random
import string
import time
import asyncio
import aiosqlite
import aiohttp
from aiohttp import web
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, StateFilter
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile, PreCheckoutQuery, LabeledPrice
)
from bs4 import BeautifulSoup
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# === ENV ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан")

DB_PATH = os.getenv("DB_PATH", "dataseeker.db")

BIGBASE_TOKEN = os.getenv("BIGBASE_TOKEN", "")
BIGBASE_BASE = os.getenv("BIGBASE_BASE", "https://bigbase.top")

SEON_API_KEY = os.getenv("SEON_API_KEY")
SEON_BASE = os.getenv("SEON_BASE", "https://api.seon.io")

SNUSBASE_API_KEY = os.getenv("SNUSBASE_API_KEY")
SNUSBASE_BASE = os.getenv("SNUSBASE_BASE", "https://api.snusbase.com")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

db_conn = None
db_lock = asyncio.Lock()
http_session = None

cache = {}
CACHE_TTL = timedelta(hours=1)

def get_cache_key(func_name: str, query: str) -> str:
    return f"{func_name}:{hashlib.md5(query.encode()).hexdigest()}"

API_TIMEOUTS = {
    "bigbase": 15.0,
    "seon": 6.0,
    "snusbase": 6.0,
    "ipapi": 3.0
}

# === FSM ===
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
    return "дней"

# === БАЗА ===
async def init_db():
    global db_conn
    db_conn = await aiosqlite.connect(DB_PATH)
    db_conn.row_factory = aiosqlite.Row
    await db_conn.execute("PRAGMA journal_mode=WAL")
    await db_conn.execute("PRAGMA synchronous=NORMAL")
    async with db_lock:
        await db_conn.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                daily_requests INTEGER DEFAULT 0,
                last_request_date TEXT DEFAULT (date('now')),
                bonus_requests INTEGER DEFAULT 0,
                referral_code TEXT UNIQUE,
                referred_by INTEGER
            );
            CREATE TABLE IF NOT EXISTS reports (
                phone TEXT PRIMARY KEY,
                data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS phone_views (
                phone TEXT PRIMARY KEY,
                user_ids TEXT DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS promo_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE,
                max_uses INTEGER NOT NULL,
                used_count INTEGER DEFAULT 0,
                requests_granted INTEGER NOT NULL,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                bonus_given INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(referrer_id, referred_id)
            );
            CREATE TABLE IF NOT EXISTS purchases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                invoice_id TEXT UNIQUE,
                amount REAL,
                currency TEXT,
                requests INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                confirmed_at TIMESTAMP
            );
        ''')
        await db_conn.commit()

async def get_http_session():
    global http_session
    if http_session is None:
        http_session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
    return http_session

# === ВСПОМОГАТЕЛЬНЫЕ ===
def calculate_age_from_birthdate(birthdate_str):
    if not birthdate_str:
        return None
    try:
        for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
            try:
                bd = datetime.strptime(birthdate_str, fmt)
                today = datetime.now()
                return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
            except ValueError:
                continue
        cleaned = birthdate_str.replace('/', '.').replace('-', '.')
        bd = datetime.strptime(cleaned, '%d.%m.%Y')
        today = datetime.now()
        return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
    except Exception:
        return None

# === HTML ПАРСЕР ===
async def parse_html_page(url: str, query: str = None):
    session = await get_http_session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
            html = await response.text()
            if response.status != 200:
                return {'error': f'HTTP {response.status}'}
            if 'Just a moment' in html or 'cf-browser-verification' in html:
                return {'error': 'Cloudflare protection detected'}
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()
            result = {
                'url': url,
                'title': soup.title.string.strip() if soup.title else None,
                'emails': [], 'phones': [], 'addresses': [],
                'snils': [], 'inn': [], 'passport': [],
                'birthdates': [], 'socials': [], 'telegrams': [],
                'full_text': text[:5000]
            }
            result['emails'] = list(set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)))
            for pattern in [r'\+7\s?\(?\d{3}\)?\s?\d{3}[-\s]?\d{2}[-\s]?\d{2}',
                            r'8\s?\(?\d{3}\)?\s?\d{3}[-\s]?\d{2}[-\s]?\d{2}']:
                result['phones'].extend(re.findall(pattern, text))
            result['phones'] = list(set(result['phones']))[:10]
            for pattern in [r'(?:г\.?|город)\s*[А-Яа-я\-]+\s*(?:ул\.?|улица)\s*[А-Яа-я\-]+\s*(?:д\.?|дом)\s*\d+[А-Яа-я]?',
                            r'(?:ул\.?|улица)\s*[А-Яа-я\-]+\s*(?:д\.?|дом)\s*\d+[А-Яа-я]?']:
                result['addresses'].extend(re.findall(pattern, text, re.IGNORECASE))
            result['addresses'] = list(set(result['addresses']))[:10]
            result['snils'] = list(set(re.findall(r'\d{3}[-\s]?\d{3}[-\s]?\d{3}[-\s]?\d{2}', text)))[:5]
            for pattern in [r'\b\d{10}\b', r'\b\d{12}\b']:
                result['inn'].extend(re.findall(pattern, text))
            result['inn'] = [i for i in set(result['inn']) if 10 <= len(i) <= 12][:5]
            for pattern in [r'\d{4}\s?\d{6}', r'серия\s*\d{4}\s*номер\s*\d{6}']:
                result['passport'].extend(re.findall(pattern, text, re.IGNORECASE))
            result['passport'] = list(set(result['passport']))[:5]
            for pattern in [r'\d{2}[./-]\d{2}[./-]\d{4}', r'\d{4}[./-]\d{2}[./-]\d{2}']:
                result['birthdates'].extend(re.findall(pattern, text))
            result['birthdates'] = list(set(result['birthdates']))[:5]
            socials_map = {
                'vk': r'(?:https?://)?(?:www\.)?vk\.com/[^\s"\']+',
                'ok': r'(?:https?://)?(?:www\.)?ok\.ru/[^\s"\']+',
                'instagram': r'(?:https?://)?(?:www\.)?instagram\.com/[^\s"\']+',
                'tiktok': r'(?:https?://)?(?:www\.)?tiktok\.com/@[^\s"\']+',
            }
            for _, pattern in socials_map.items():
                result['socials'].extend(re.findall(pattern, text, re.IGNORECASE)[:3])
            for pattern in [r'@[a-zA-Z0-9_]{5,32}', r't\.me/[a-zA-Z0-9_]+']:
                result['telegrams'].extend(re.findall(pattern, text))
            result['telegrams'] = list(set(result['telegrams']))[:10]
            if query:
                found = []
                ql = query.lower()
                for p in soup.find_all(['p', 'div', 'span', 'li']):
                    t = p.get_text(strip=True)
                    if ql in t.lower():
                        found.append({'type': 'text', 'content': t[:200] + ('...' if len(t) > 200 else '')})
                        if len(found) >= 10:
                            break
                result['found_data'] = found
            return result
    except asyncio.TimeoutError:
        return {'error': 'Timeout'}
    except Exception as e:
        return {'error': str(e)}

# === BIGBASE ===
async def bigbase_search(query: str):
    if not BIGBASE_TOKEN:
        logger.debug("[bigbase] skipped: no token")
        return {}
    session = await get_http_session()
    url = f"{BIGBASE_BASE}/api/search"
    headers = {
        "Authorization": BIGBASE_TOKEN,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    clean = re.sub(r'\D', '', query)
    t0 = time.monotonic()
    try:
        async with session.post(
            url,
            json={"search": clean, "page": 0},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["bigbase"]),
        ) as resp:
            body = await resp.text()
            elapsed = time.monotonic() - t0
            logger.info(f"BigBase status={resp.status} t={elapsed:.2f}s len={len(body)}")
            if resp.status != 200:
                return {}
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return {}
    except asyncio.TimeoutError:
        logger.warning("[bigbase] TIMEOUT")
        return {}
    except Exception as e:
        logger.error(f"BigBase exception: {e}")
        return {}

# === ПАРСЕР BIGBASE ===
BIGBASE_KEY_MAP = {
    'фио': 'ФИО', 'имя': 'Имя', 'фамилия': 'Фамилия', 'отчество': 'Отчество',
    'рабочее фио': 'Рабочее ФИО',
    'телефон': 'Телефон', 'телефоны': 'Телефон', 'номер телефона': 'Телефон',
    'email': 'Email', 'почта': 'Email', 'e-mail': 'Email',
    'дата рождения': 'Дата рождения', 'др': 'Дата рождения',
    'возраст': 'Возраст', 'пол': 'Пол',
    'адрес': 'Адрес', 'город': 'Город', 'регион': 'Регион', 'страна': 'Страна',
    'индекс': 'Индекс',
    'паспорт': 'Паспорт', 'серия паспорта': 'Серия паспорта',
    'номер паспорта': 'Номер паспорта', 'тип паспорта': 'Тип паспорта',
    'кем выдан': 'Кем выдан', 'дата выдачи': 'Дата выдачи',
    'инн': 'ИНН', 'снилс': 'СНИЛС', 'огрн': 'ОГРН', 'огрнип': 'ОГРНИП',
    'автомобиль': 'Автомобиль', 'авто': 'Автомобиль', 'транспорт': 'Транспорт',
    'госномер': 'Госномер', 'модель': 'Модель', 'vin': 'VIN',
    'класс автомобиля': 'Класс автомобиля', 'цвет': 'Цвет',
    'текущая база': 'Текущая база', 'первая база': 'Первая база',
    'база': 'База', 'id базы': 'ID базы',
    'статус': 'Статус', 'статус (текст)': 'Статус',
    'рейтинг': 'Рейтинг', 'сегмент': 'Сегмент', 'тип': 'Тип',
    'источник': 'Источник', 'база данных': 'База данных',
    'дата первой активности': 'Дата первой активности',
    'дата начала': 'Дата начала', 'дата обновления': 'Дата обновления',
    'дата регистрации': 'Дата регистрации',
    'id звонка': 'ID звонка', 'id организации': 'ID организации',
    'информация диспетчера': 'Информация диспетчера',
    'нет активности': 'Нет активности',
    'статус email (текст)': 'Статус email',
    'изображения': 'Изображения', 'изображения авто': 'Изображения авто',
    'компания': 'Компания', 'организация': 'Организация', 'должность': 'Должность',
    'login': 'Логин', 'логин': 'Логин', 'nickname': 'Никнейм',
    'telegram': 'Telegram', 'vk': 'ВКонтакте', 'ok': 'Одноклассники',
    'instagram': 'Instagram', 'tiktok': 'TikTok', 'whatsapp': 'WhatsApp',
    'актуальность': 'Актуальность',
}

JUNK_KEYS = {'id', 'record_id', 'rec_id', 'base_id', 'internal_id',
             'id автомобиля', 'id класса автомобиля', 'id первой базы'}


def _clean_pair(key, value):
    if key is None:
        return None, None
    k_str = str(key).strip()
    k_low = k_str.lower()
    if k_low in JUNK_KEYS:
        return None, None
    if value in (None, "", [], {}):
        return None, None
    if value == "0" and k_low not in ('нет активности', 'изображения', 'изображения авто', 'рейтинг'):
        return None, None
    if isinstance(value, str):
        if 'T00:00:00' in value:
            value = value.split('T')[0]
        elif re.match(r'^\d{4}-\d{2}-\d{2}T', value):
            value = value.split('T')[0]
    ru = BIGBASE_KEY_MAP.get(k_low, k_str)
    return ru, value


def _parse_base_record(record):
    out = {}
    if not isinstance(record, list) or not record:
        return out
    if all(isinstance(x, list) and len(x) == 2 for x in record):
        for k, v in record:
            ru, val = _clean_pair(k, v)
            if ru and ru not in out:
                out[ru] = val
        return out
    if all(not isinstance(x, (list, dict)) for x in record) and len(record) % 2 == 0:
        for i in range(0, len(record), 2):
            ru, val = _clean_pair(record[i], record[i + 1])
            if ru and ru not in out:
                out[ru] = val
        return out
    return out


def _collect_connections(connections):
    result = {'ФИО': [], 'Телефон': [], 'Транспорт': []}
    if not isinstance(connections, list):
        return result
    for conn in connections:
        if not isinstance(conn, dict):
            continue
        ctype = conn.get('type', '')
        for fio in conn.get('fio', []) or []:
            if isinstance(fio, dict) and fio.get('value') and fio['value'] not in result['ФИО']:
                result['ФИО'].append(fio['value'])
        for ph in conn.get('phone', []) or []:
            if isinstance(ph, dict) and ph.get('value') and ph['value'] not in result['Телефон']:
                result['Телефон'].append(ph['value'])
        for tr in conn.get('transport', []) or []:
            if isinstance(tr, dict) and tr.get('model') and tr['model'] not in result['Транспорт']:
                result['Транспорт'].append(tr['model'])
        title = conn.get('title')
        if title and ctype == 'person' and title not in result['ФИО']:
            result['ФИО'].append(title)
    return result


def parse_bigbase(data):
    if not isinstance(data, dict):
        return []
    parsed = []

    dossier = data.get('dossier') if isinstance(data.get('dossier'), dict) else {}
    head = dossier.get('head') if isinstance(dossier.get('head'), dict) else {}
    if head:
        hf = {}
        if head.get('phone_country_info'):
            hf['Страна'] = head['phone_country_info']
        if head.get('phone_operator'):
            hf['Оператор'] = head['phone_operator']
        if head.get('phone_region'):
            hf['Регион'] = head['phone_region']
        if head.get('phone_operator_inn'):
            hf['ИНН оператора'] = head['phone_operator_inn']
        if head.get('phone_code_country'):
            hf['Код страны'] = head['phone_code_country']
        if hf:
            parsed.append({"type": "result", "data": hf, "source": "Инфо о номере"})

    records = data.get('records')
    if isinstance(records, list):
        for rec in records:
            if not isinstance(rec, dict):
                continue
            fields = {}
            base_record = rec.get('base_record')
            if isinstance(base_record, list):
                fields.update(_parse_base_record(base_record))

            conn = _collect_connections(rec.get('connections'))
            if conn['ФИО'] and 'ФИО' not in fields:
                fields['ФИО'] = ', '.join(conn['ФИО'])
            if conn['Телефон'] and 'Телефон' not in fields:
                fields['Телефон'] = ', '.join(conn['Телефон'])
            if conn['Транспорт'] and 'Транспорт' not in fields and 'Автомобиль' not in fields:
                fields['Транспорт'] = ', '.join(conn['Транспорт'])

            base_info = rec.get('base_info')
            if isinstance(base_info, dict):
                if base_info.get('name'):
                    fields['Источник'] = base_info['name']
                if base_info.get('description'):
                    fields['Описание базы'] = base_info['description']
                if base_info.get('date_relevance'):
                    fields['Актуальность базы'] = base_info['date_relevance']

            if fields:
                parsed.append({
                    "type": "result",
                    "data": fields,
                    "source": fields.get('Источник', 'BigBase')
                })

    if not parsed and isinstance(data.get('connections'), dict):
        top_conn = _collect_connections(data['connections'].get('person'))
        if top_conn['ФИО'] or top_conn['Телефон'] or top_conn['Транспорт']:
            fields = {}
            if top_conn['ФИО']:
                fields['ФИО'] = ', '.join(top_conn['ФИО'])
            if top_conn['Телефон']:
                fields['Телефон'] = ', '.join(top_conn['Телефон'])
            if top_conn['Транспорт']:
                fields['Транспорт'] = ', '.join(top_conn['Транспорт'])
            parsed.append({"type": "result", "data": fields, "source": "BigBase"})

    return parsed

# === SEON ===
async def seon_search(query: str):
    if not SEON_API_KEY:
        return {}
    session = await get_http_session()
    url = f"{SEON_BASE}/SeonRestService/phone-api/v2"
    headers = {"X-API-KEY": SEON_API_KEY, "Content-Type": "application/json"}
    try:
        async with session.post(url, json={"phone": query}, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["seon"])) as resp:
            text = await resp.text()
            logger.info(f"SEON status={resp.status} len={len(text)}")
            return json.loads(text) if resp.status == 200 else {}
    except Exception as e:
        logger.error(f"SEON error: {e}")
        return {}

def parse_seon(data):
    if not data or not isinstance(data, dict):
        return []
    parsed = []
    payload = data.get('data') if isinstance(data.get('data'), dict) else data
    phone_info = {}
    if isinstance(payload, dict):
        if payload.get('phone'):
            phone_info['Телефон'] = payload['phone']
        risk = payload.get('risk_scores')
        if isinstance(risk, dict):
            for k, v in risk.items():
                phone_info[k.replace('_', ' ').title()] = v
        agg = payload.get('account_aggregates')
        if isinstance(agg, dict):
            if agg.get('total_registration') is not None:
                phone_info['Всего регистраций'] = agg['total_registration']
            if isinstance(agg.get('business'), dict):
                biz = agg['business']
                if biz.get('total_registration') is not None:
                    phone_info['Бизнес-регистраций'] = biz['total_registration']
    if phone_info:
        parsed.append({"type": "phone_info", "data": phone_info})
    if data.get('email'):
        parsed.append({"type": "email", "data": {"Email": data['email']}})
    return parsed

# === SNUSBASE ===
async def snusbase_search(query: str):
    if not SNUSBASE_API_KEY:
        return {}
    session = await get_http_session()
    url = f"{SNUSBASE_BASE}/data/search"
    headers = {"Auth": SNUSBASE_API_KEY, "Content-Type": "application/json"}
    payload = {"terms": [query], "types": ["email", "username", "name", "lastip"], "wildcard": False}
    try:
        async with session.post(url, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["snusbase"])) as resp:
            text = await resp.text()
            logger.info(f"Snusbase status={resp.status} len={len(text)}")
            return json.loads(text) if resp.status == 200 else {}
    except Exception as e:
        logger.error(f"Snusbase error: {e}")
        return {}

def parse_snusbase(data):
    if not data or not isinstance(data, dict):
        return []
    parsed = []
    results = data.get('data') or data.get('results') or []
    if isinstance(results, dict):
        results = [results]
    key_mapping = {
        'email': 'Email', 'username': 'Имя пользователя', 'password': 'Пароль',
        'hash': 'Хеш', 'phone': 'Телефон', 'address': 'Адрес', 'name': 'Имя',
        'full_name': 'ФИО', 'city': 'Город', 'country': 'Страна',
        'ip': 'IP адрес', 'source': 'Источник', 'lastip': 'Последний IP',
        'login': 'Логин',
    }
    for item in results:
        if isinstance(item, dict):
            fields = {key_mapping.get(k, k): v for k, v in item.items() if v not in (None, "", [], {})}
            if fields:
                parsed.append({"type": "result", "data": fields})
    return parsed

# === IP-API ===
async def ip_info_search(query: str):
    session = await get_http_session()
    url = f"http://ip-api.com/json/{query}?fields=status,message,country,regionName,city,zip,lat,lon,timezone,isp,org,as,query"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=API_TIMEOUTS["ipapi"])) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data if data.get('status') == 'success' else {}
    except Exception:
        pass
    return {}

# === СБОР ДАННЫХ ===
async def collect_general_data(query: str, search_type: str = "phone"):
    cache_key = get_cache_key(search_type, query)
    if cache_key in cache:
        cached_time, data = cache[cache_key]
        if datetime.now() - cached_time < CACHE_TTL:
            return data

    original_query = query
    if search_type == "phone":
        query = re.sub(r'[^0-9]', '', query)

    tasks = {
        'bigbase': asyncio.create_task(bigbase_search(query)),
        'seon': asyncio.create_task(seon_search(query)),
        'snusbase': asyncio.create_task(snusbase_search(query)),
    }
    if search_type == "ip":
        tasks['ipapi'] = asyncio.create_task(ip_info_search(query))

    results = {}
    for name, task in tasks.items():
        try:
            timeout = 20.0 if name == "bigbase" else 12.0
            results[name] = await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"{name} TIMEOUT")
            results[name] = {}
            task.cancel()
        except Exception as e:
            logger.error(f"{name} exception: {e}")
            results[name] = {}

    bigbase = results.get('bigbase', {}) or {}
    seon = results.get('seon', {}) or {}
    snusbase = results.get('snusbase', {}) or {}
    ipdata = results.get('ipapi', {}) if search_type == "ip" else {}

    bigbase_parsed = parse_bigbase(bigbase)
    seon_parsed = parse_seon(seon)
    snusbase_parsed = parse_snusbase(snusbase)

    logger.info(f"Parsed: big={len(bigbase_parsed)}, seon={len(seon_parsed)}, snus={len(snusbase_parsed)}")

    result = {
        'query': original_query, 'type': search_type,
        'operator': None, 'region': None, 'country': None, 'city': None,
        'fio': None, 'birthdate': None, 'age': None, 'address': None,
        'emails': [], 'telegrams': [],
        'vk': None, 'instagram': None, 'tiktok': None, 'ok': None,
        'phone_books': [], 'extra': {}, 'sources': [], 'records_count': 0,
        'grouped': {},
    }

    sources_set = set()
    records_count = 0
    seen_records = set()
    grouped = {}

    if ipdata:
        result['country'] = ipdata.get('country') or result['country']
        result['region'] = ipdata.get('regionName') or result['region']
        result['city'] = ipdata.get('city') or result['city']
        if ipdata.get('isp'):
            result['operator'] = ipdata['isp']
        extra_ip = {k: ipdata[k] for k in ['country', 'regionName', 'city', 'zip', 'lat', 'lon', 'timezone', 'isp', 'org', 'as'] if ipdata.get(k)}
        if extra_ip:
            records_count += 1
            grouped.setdefault("IP информация", []).append(extra_ip)
            sources_set.add("ip-api.com")

    for block in bigbase_parsed:
        if block['type'] != 'result':
            continue
        fields = dict(block['data'])
        if not fields:
            continue
        record_key = "|".join(f"{k}={v}" for k, v in sorted(fields.items()))
        if record_key in seen_records:
            continue
        seen_records.add(record_key)
        source_name = block.get('source') or fields.get('Источник') or 'BigBase'
        sources_set.add(source_name)
        records_count += 1
        grouped.setdefault(source_name, []).append(fields)

        if fields.get('Телефон'):
            for ph in str(fields['Телефон']).split(','):
                ph = ph.strip()
                if ph and ph not in result['phone_books']:
                    result['phone_books'].append(ph)
        if fields.get('ФИО') and not result['fio']:
            result['fio'] = str(fields['ФИО'])
        if fields.get('Дата рождения') and not result['birthdate']:
            bd = str(fields['Дата рождения'])
            result['birthdate'] = bd
            age = calculate_age_from_birthdate(bd)
            if age is not None:
                result['age'] = age
        if fields.get('Адрес') and not result['address']:
            result['address'] = str(fields['Адрес'])
        if fields.get('Email'):
            for em in str(fields['Email']).split(','):
                em = em.strip()
                if em and em not in result['emails']:
                    result['emails'].append(em)
        if fields.get('Оператор') and not result['operator']:
            result['operator'] = str(fields['Оператор'])
        if fields.get('Город') and not result['city']:
            result['city'] = str(fields['Город'])
        if fields.get('Регион') and not result['region']:
            result['region'] = str(fields['Регион'])
        if fields.get('Страна') and not result['country']:
            result['country'] = str(fields['Страна'])

    for block in seon_parsed:
        if block['type'] == 'phone_info':
            info = block['data']
            if info.get('Оператор') and not result['operator']:
                result['operator'] = str(info['Оператор'])
            if info.get('Страна') and not result['country']:
                result['country'] = str(info['Страна'])
            records_count += 1
            grouped.setdefault("SEON", []).append(info)
            sources_set.add("SEON")
        elif block['type'] == 'email':
            em = block['data'].get('Email')
            if em and str(em) not in result['emails']:
                result['emails'].append(str(em))
                sources_set.add("SEON")

    for block in snusbase_parsed:
        if block['type'] == 'result':
            fields = block['data']
            record_key = "|".join(f"{k}={v}" for k, v in sorted(fields.items()))
            if record_key in seen_records:
                continue
            seen_records.add(record_key)
            sources_set.add("Snusbase")
            records_count += 1
            grouped.setdefault("Snusbase", []).append(fields)
            em = fields.get('Email')
            if em and str(em) not in result['emails']:
                result['emails'].append(str(em))

    result['sources'] = list(sources_set)
    result['records_count'] = records_count
    result['emails'] = list(dict.fromkeys([e for e in result['emails'] if e and '@' in str(e)]))
    result['grouped'] = grouped

    cache[cache_key] = (datetime.now(), result)
    return result

# === HTML ОТЧЁТ ===
def _esc(v):
    return str(v).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def generate_html_report(data: dict, views: int = 0) -> str:
    query = data.get('query', '')
    grouped = data.get('grouped', {})
    total_records = data.get('records_count', 0)

    main_info = []
    if data.get('fio'): main_info.append(("ФИО", data['fio']))
    if data.get('birthdate'): main_info.append(("Дата рождения", data['birthdate']))
    if data.get('age') is not None: main_info.append(("Возраст", f"{data['age']} лет"))
    if data.get('address'): main_info.append(("Адрес", data['address']))
    if data.get('phone_books'): main_info.append(("Телефон", ', '.join(data['phone_books'][:10])))
    if data.get('emails'): main_info.append(("Email", ', '.join(data['emails'][:5])))
    if data.get('operator'): main_info.append(("Оператор", data['operator']))
    if data.get('region'): main_info.append(("Регион", data['region']))
    if data.get('country'): main_info.append(("Страна", data['country']))
    if data.get('city'): main_info.append(("Город", data['city']))

    sidebar_items = ""
    for idx, (source, rows) in enumerate(grouped.items(), start=1):
        sidebar_items += (
            f'<a href="#base{idx}" class="nav-item">'
            f'<span class="nav-flag">🇷🇺</span>'
            f'<span class="nav-text">{_esc(source[:40])}</span>'
            f'<span class="nav-count">{len(rows)}</span>'
            f'</a>'
        )

    bases_html = ""
    for idx, (source, rows) in enumerate(grouped.items(), start=1):
        rows_html = ""
        for r_idx, fields in enumerate(rows, 1):
            visible = [(k, v) for k, v in fields.items()
                       if k not in ('Источник', 'Описание базы', 'Актуальность базы', 'Актуальность')
                       and v is not None and str(v).strip()]
            sub = f'<div class="sub-divider">Запись {r_idx} из {len(rows)}</div>' if len(rows) > 1 else ''
            rows_html += sub + "".join(
                f'<div class="field"><div class="field-label">{_esc(k)}</div>'
                f'<div class="field-value">{_esc(v)}</div></div>'
                for k, v in visible
            )

        meta_pairs = []
        for mk in ('Актуальность', 'Актуальность базы', 'Описание базы'):
            for f in rows:
                if f.get(mk):
                    meta_pairs.append((mk, f[mk]))
                    break
        meta_html = ""
        if meta_pairs:
            meta_html = '<div class="base-meta-row">' + "".join(
                f'<span><b>{_esc(mk)}:</b> {_esc(mv)}</span>' for mk, mv in meta_pairs
            ) + '</div>'

        bases_html += f'''
        <div class="base-card open" id="base{idx}">
            <div class="base-header" onclick="toggleCard(this)">
                <div class="base-header-left">
                    <span class="flag">🇷🇺</span>
                    <span class="base-name">{_esc(source)}</span>
                </div>
                <div class="base-header-right">
                    <span class="rec-badge">{len(rows)}</span>
                    <svg class="arrow" width="14" height="9" viewBox="0 0 21 12"><path d="M1 1L10.5 10L20 1" stroke="#A5AAB4" stroke-width="3" fill="none" stroke-linecap="round"/></svg>
                </div>
            </div>
            <div class="base-body"><div class="base-content">
                {rows_html}
                {meta_html}
            </div></div>
        </div>
        '''

    if not bases_html:
        bases_html = '<div class="base-card open"><div class="base-header"><div class="base-header-left"><span class="base-name">❌ Данные не найдены</span></div></div></div>'

    return f'''<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Отчёт: {_esc(query)}</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
       background: #0b0d10; color: #e8eaee; font-size: 14px; line-height: 1.5;
       -webkit-font-smoothing: antialiased; padding-bottom: 40px; }}
.container {{ max-width: 1200px; margin: 0 auto; padding: 0 16px; }}

.header {{ padding: 24px 0 18px; }}
.header-inner {{ background: #13161b; border-radius: 16px; padding: 20px 24px; }}
.row-top {{ display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; margin-bottom: 16px; }}
.query-block {{ display: flex; align-items: center; gap: 10px; min-width: 0; }}
.query-label {{ font-size: 15px; color: #8b919b; font-weight: 500; }}
.query-value {{ background: #0b0d10; padding: 8px 16px; border-radius: 10px;
               font-size: 16px; font-weight: 600; color: #fff; letter-spacing: .3px; }}
.stats {{ display: flex; gap: 8px; align-items: center; }}
.stat-chip {{ background: #0b0d10; border-radius: 10px; padding: 8px 14px;
             display: flex; align-items: center; gap: 8px; }}
.stat-label {{ font-size: 12px; color: #8b919b; }}
.stat-num {{ background: #ff851f; color: #fff; font-size: 13px; font-weight: 700;
            border-radius: 6px; padding: 2px 10px; }}
.actions {{ display: flex; gap: 10px; }}
.btn {{ border: none; cursor: pointer; border-radius: 10px; padding: 10px 22px;
       font-size: 13px; font-weight: 600; color: #fff; transition: opacity .15s; }}
.btn:hover {{ opacity: .85; }}
.btn-primary {{ background: #ff8119; }}
.btn-secondary {{ background: #1c2129; }}

.layout {{ display: flex; gap: 16px; align-items: flex-start; }}
.sidebar {{ width: 240px; flex-shrink: 0; position: sticky; top: 16px; }}
.sidebar-title {{ font-size: 11px; color: #6b7280; text-transform: uppercase;
                 letter-spacing: .8px; font-weight: 600; margin-bottom: 10px; padding-left: 4px; }}
.sidebar-list {{ background: #13161b; border-radius: 14px; padding: 10px; max-height: calc(100vh - 60px); overflow-y: auto; }}
.sidebar-list::-webkit-scrollbar {{ width: 5px; }}
.sidebar-list::-webkit-scrollbar-thumb {{ background: #2a3039; border-radius: 3px; }}
.nav-item {{ display: flex; align-items: center; gap: 8px; padding: 8px 10px;
            border-radius: 9px; text-decoration: none; color: #cfd3da;
            font-size: 13px; transition: background .15s; }}
.nav-item:hover {{ background: #1c2129; color: #ff851f; }}
.nav-flag {{ font-size: 14px; flex-shrink: 0; }}
.nav-text {{ flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.nav-count {{ background: #1c2129; color: #8b919b; font-size: 11px; font-weight: 600;
             padding: 1px 7px; border-radius: 6px; flex-shrink: 0; }}

.content {{ flex: 1; min-width: 0; }}
.section-title {{ font-size: 11px; color: #6b7280; text-transform: uppercase;
                 letter-spacing: .8px; font-weight: 600; margin: 0 0 10px 4px; }}

.main-info {{ background: #13161b; border-radius: 14px; padding: 18px 22px; margin-bottom: 18px; }}
.main-info-grid {{ display: grid; grid-template-columns: 1fr; gap: 2px; }}
.info-item {{ display: flex; justify-content: space-between; gap: 12px;
             padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,.04); }}
.info-item:last-child {{ border-bottom: none; }}
.info-key {{ color: #8b919b; font-size: 13px; font-weight: 500; }}
.info-val {{ color: #ff9c3f; font-weight: 600; font-size: 13px;
            text-align: right; max-width: 60%; word-break: break-word; }}

.base-card {{ background: #13161b; border-radius: 14px; margin-bottom: 12px; overflow: hidden; }}
.base-header {{ display: flex; align-items: center; justify-content: space-between;
               padding: 14px 20px; cursor: pointer; gap: 12px;
               background: #0f1216; user-select: none; }}
.base-header-left {{ display: flex; align-items: center; gap: 10px; min-width: 0; flex: 1; }}
.base-header-right {{ display: flex; align-items: center; gap: 10px; flex-shrink: 0; }}
.flag {{ font-size: 16px; flex-shrink: 0; }}
.base-name {{ font-size: 14px; font-weight: 600; color: #fff;
             overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.rec-badge {{ background: #1c2129; color: #9aa0aa; font-size: 11px; font-weight: 700;
             padding: 3px 10px; border-radius: 8px; }}
.arrow {{ transition: transform .25s; }}
.base-card.open .arrow {{ transform: rotate(180deg); }}
.base-body {{ max-height: 0; overflow: hidden; transition: max-height .35s ease; }}
.base-card.open .base-body {{ max-height: 20000px; }}
.base-content {{ padding: 6px 20px 16px; }}

.field {{ display: grid; grid-template-columns: 1fr 1.5fr; gap: 12px;
         padding: 9px 0; border-bottom: 1px solid rgba(255,255,255,.03); align-items: center; }}
.field:last-of-type {{ border-bottom: none; }}
.field-label {{ color: #8b919b; font-size: 13px; font-weight: 500; }}
.field-value {{ color: #ff9c3f; font-size: 13px; font-weight: 600;
               text-align: right; word-break: break-word; }}
.sub-divider {{ font-size: 11px; color: #6b7280; text-transform: uppercase;
               letter-spacing: .8px; font-weight: 600;
               padding: 12px 0 6px; margin-top: 6px;
               border-top: 1px solid rgba(255,255,255,.05); }}
.sub-divider:first-child {{ border-top: none; margin-top: 0; }}
.base-meta-row {{ display: flex; flex-wrap: wrap; gap: 16px; padding-top: 10px;
                 margin-top: 8px; border-top: 1px solid rgba(255,255,255,.05);
                 font-size: 11.5px; color: #6b7280; }}
.base-meta-row b {{ color: #8b919b; font-weight: 600; }}

@media (max-width: 780px) {{
    .layout {{ flex-direction: column; }}
    .sidebar {{ display: none; }}
    .query-value {{ font-size: 14px; padding: 6px 12px; }}
    .query-label {{ font-size: 13px; }}
    .header-inner {{ padding: 16px; }}
    .base-header {{ padding: 12px 14px; }}
    .base-content {{ padding: 4px 14px 12px; }}
    .field {{ grid-template-columns: 1fr 1.2fr; gap: 8px; }}
    .field-label, .field-value, .info-key, .info-val {{ font-size: 12px; }}
    .main-info {{ padding: 14px 16px; }}
    .btn {{ padding: 9px 18px; font-size: 12px; }}
}}
</style></head><body>

<header class="header"><div class="container"><div class="header-inner">
  <div class="row-top">
    <div class="query-block">
      <span class="query-label">Запрос:</span>
      <span class="query-value">{_esc(query)}</span>
    </div>
    <div class="stats">
      <div class="stat-chip"><span class="stat-label">Результатов:</span><span class="stat-num">{total_records}</span></div>
      <div class="stat-chip"><span class="stat-label">Просмотров:</span><span class="stat-num">{views}</span></div>
    </div>
  </div>
  <div class="actions">
    <button onclick="downloadPDF()" class="btn btn-primary">Сохранить в PDF</button>
    <button onclick="window.print()" class="btn btn-secondary">Печатать</button>
  </div>
</div></div></header>

<div class="container"><div class="layout">

  <aside class="sidebar">
    <div class="sidebar-title">Найдено в базах</div>
    <div class="sidebar-list">
      <a href="#main" class="nav-item">
        <span class="nav-flag">📋</span>
        <span class="nav-text">Основная информация</span>
      </a>
      {sidebar_items}
    </div>
  </aside>

  <main class="content" id="printArea">

    <div class="section-title">Основная информация</div>
    <div id="main" class="main-info">
      <div class="main-info-grid">
        {''.join(f'<div class="info-item"><div class="info-key">{_esc(k)}</div><div class="info-val">{_esc(v)}</div></div>' for k, v in main_info) if main_info else '<div class="info-item"><div class="info-key" style="width:100%;text-align:center">Нет данных</div></div>'}
      </div>
    </div>

    <div class="section-title" style="margin-top:22px">Записи из баз</div>
    {bases_html}

  </main>

</div></div>

<script>
function toggleCard(el) {{
  el.parentElement.classList.toggle('open');
}}
function downloadPDF() {{
  const el = document.getElementById('printArea');
  html2pdf().set({{
    margin: [8, 8, 8, 8],
    filename: 'report_{_esc(query)}.pdf',
    image: {{ type: 'jpeg', quality: 0.95 }},
    html2canvas: {{ scale: 2, backgroundColor: '#0b0d10', useCORS: true }},
    jsPDF: {{ unit: 'mm', format: 'a4', orientation: 'portrait' }}
  }}).from(el).save();
}}
</script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js"></script>
</body></html>'''

# === USERS ===
async def generate_referral_code(user_id: int) -> str:
    async with db_lock:
        while True:
            code = f"REF{user_id}{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"
            async with db_conn.execute('SELECT user_id FROM users WHERE referral_code = ?', (code,)) as cur:
                if not await cur.fetchone():
                    break
        await db_conn.execute('UPDATE users SET referral_code = ? WHERE user_id = ?', (code, user_id))
        await db_conn.commit()
    return code

async def get_referral_stats(user_id: int):
    async with db_lock:
        async with db_conn.execute('SELECT COUNT(*) FROM referrals WHERE referrer_id = ?', (user_id,)) as cur:
            row = await cur.fetchone()
    invited = row[0] if row else 0
    return invited, invited

async def get_user_available_requests(user_id: int) -> int:
    async with db_lock:
        async with db_conn.execute(
            'SELECT daily_requests, bonus_requests, last_request_date FROM users WHERE user_id = ?', (user_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return 0
    today = datetime.now().strftime('%Y-%m-%d')
    daily = row['daily_requests'] if row['last_request_date'] == today else 0
    limit = 5 + (row['bonus_requests'] or 0)
    available = limit - daily
    return available if available > 0 else 0

async def use_request(user_id: int) -> bool:
    if await get_user_available_requests(user_id) <= 0:
        return False
    today = datetime.now().strftime('%Y-%m-%d')
    async with db_lock:
        await db_conn.execute(
            'UPDATE users SET daily_requests = daily_requests + 1, last_request_date = ? WHERE user_id = ?',
            (today, user_id)
        )
        await db_conn.commit()
    return True

async def create_user(user_id: int, username: str = None, referred_by: int = None):
    async with db_lock:
        await db_conn.execute(
            'INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)', (user_id, username)
        )
        await db_conn.commit()
    code = await generate_referral_code(user_id)
    if referred_by and referred_by != user_id:
        async with db_lock:
            async with db_conn.execute('SELECT user_id FROM users WHERE user_id = ?', (referred_by,)) as cur:
                ref = await cur.fetchone()
            if ref:
                try:
                    await db_conn.execute(
                        'INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)',
                        (referred_by, user_id)
                    )
                    await db_conn.execute(
                        'UPDATE users SET bonus_requests = bonus_requests + 1 WHERE user_id = ?',
                        (referred_by,)
                    )
                    await db_conn.commit()
                except Exception:
                    pass
    return code

async def get_user(user_id: int):
    async with db_lock:
        async with db_conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cur:
            return await cur.fetchone()

async def get_referral_code(user_id: int):
    async with db_lock:
        async with db_conn.execute('SELECT referral_code FROM users WHERE user_id = ?', (user_id,)) as cur:
            row = await cur.fetchone()
    return row['referral_code'] if row else None

async def save_report(phone: str, data: dict):
    async with db_lock:
        await db_conn.execute(
            'INSERT INTO reports (phone, data) VALUES (?, ?) ON CONFLICT(phone) DO UPDATE SET data = excluded.data, created_at = CURRENT_TIMESTAMP',
            (phone, json.dumps(data, ensure_ascii=False))
        )
        await db_conn.commit()

async def get_unique_views_phone(phone: str, user_id: int) -> int:
    async with db_lock:
        async with db_conn.execute('SELECT user_ids FROM phone_views WHERE phone = ?', (phone,)) as cur:
            row = await cur.fetchone()
        user_ids = json.loads(row['user_ids']) if row and row['user_ids'] else []
        if user_id not in user_ids:
            user_ids.append(user_id)
        await db_conn.execute(
            'INSERT INTO phone_views (phone, user_ids) VALUES (?, ?) ON CONFLICT(phone) DO UPDATE SET user_ids = excluded.user_ids',
            (phone, json.dumps(user_ids))
        )
        await db_conn.commit()
    return len(user_ids)

# === ПРОМОКОДЫ ===
async def create_promo_code(code: str, max_uses: int, requests_granted: int, created_by: int) -> bool:
    async with db_lock:
        try:
            await db_conn.execute(
                'INSERT INTO promo_codes (code, max_uses, requests_granted, created_by) VALUES (?, ?, ?, ?)',
                (code, max_uses, requests_granted, created_by)
            )
            await db_conn.commit()
            return True
        except Exception:
            return False

async def get_promo_code(code: str):
    async with db_lock:
        async with db_conn.execute('SELECT * FROM promo_codes WHERE code = ?', (code,)) as cur:
            return await cur.fetchone()

async def activate_promo_code(user_id: int, code: str):
    async with db_lock:
        async with db_conn.execute('SELECT * FROM promo_codes WHERE code = ?', (code,)) as cur:
            row = await cur.fetchone()
        if not row:
            return False, "Промокод не найден."
        if row['used_count'] >= row['max_uses']:
            return False, "Промокод уже использован максимальное количество раз."
        await db_conn.execute('UPDATE promo_codes SET used_count = used_count + 1 WHERE code = ?', (code,))
        await db_conn.execute(
            'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
            (row['requests_granted'], user_id)
        )
        await db_conn.commit()
    return True, f"Промокод активирован! Вы получили {row['requests_granted']} дополнительных запросов."

async def get_all_promo_codes():
    async with db_lock:
        async with db_conn.execute('SELECT * FROM promo_codes ORDER BY created_at DESC') as cur:
            return await cur.fetchall()

async def delete_promo_code(code: str):
    async with db_lock:
        await db_conn.execute('DELETE FROM promo_codes WHERE code = ?', (code,))
        await db_conn.commit()

# === STARS ===
async def create_stars_invoice(user_id: int, stars_price: int, requests_count: int):
    temp_invoice_id = f"stars_{user_id}_{int(datetime.now().timestamp())}"
    async with db_lock:
        await db_conn.execute(
            'INSERT INTO purchases (user_id, invoice_id, amount, currency, requests, status) VALUES (?, ?, ?, ?, ?, ?)',
            (user_id, temp_invoice_id, stars_price, 'XTR', requests_count, 'pending')
        )
        await db_conn.commit()
    prices = [LabeledPrice(label=f"{requests_count} запросов", amount=stars_price)]
    try:
        await bot.send_invoice(
            chat_id=user_id,
            title=f"Пополнение: {requests_count} запросов",
            description=f"Вы получаете {requests_count} дополнительных запросов.",
            provider_token="",
            currency="XTR",
            prices=prices,
            start_parameter=f"stars_{user_id}_{int(datetime.now().timestamp())}",
            payload=json.dumps({"user_id": user_id, "requests": requests_count, "temp_invoice_id": temp_invoice_id}),
        )
        return True, temp_invoice_id
    except Exception as e:
        logger.error(f"Stars invoice error: {e}")
        async with db_lock:
            await db_conn.execute('DELETE FROM purchases WHERE invoice_id = ?', (temp_invoice_id,))
            await db_conn.commit()
        return False, None

async def process_stars_payment(charge_id: str, user_id: int, payload: str):
    data = json.loads(payload)
    temp_invoice_id = data.get("temp_invoice_id")
    requests = data.get("requests", 0)
    async with db_lock:
        async with db_conn.execute(
            'SELECT * FROM purchases WHERE invoice_id = ? AND user_id = ?', (temp_invoice_id, user_id)
        ) as cur:
            purchase = await cur.fetchone()
        if not purchase or purchase['status'] != 'pending':
            return
        await db_conn.execute(
            'UPDATE purchases SET invoice_id = ?, status = ?, confirmed_at = CURRENT_TIMESTAMP WHERE invoice_id = ? AND user_id = ?',
            (charge_id, 'confirmed', temp_invoice_id, user_id)
        )
        await db_conn.execute(
            'UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?',
            (requests, user_id)
        )
        await db_conn.commit()
    try:
        await bot.send_message(user_id, f"Оплата Stars подтверждена! Начислено {requests} запросов.")
    except Exception:
        pass

# === BOT ===
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("parse"))
async def parse_html_command(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Доступ запрещён.")
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Укажите URL: `/parse https://example.com [запрос]`", parse_mode="Markdown")
        return
    url = args[1]
    query = ' '.join(args[2:]) if len(args) > 2 else None
    status = await message.reply("🔍 Парсим страницу...")
    try:
        result = await parse_html_page(url, query)
        if result.get('error'):
            await status.edit_text(f"❌ Ошибка: {result['error']}")
            return
        text = f"📄 **{result.get('title', 'Без заголовка')}**\n🔗 {url}\n\n"
        if result.get('emails'): text += f"📧 Email: {', '.join(result['emails'][:5])}\n"
        if result.get('phones'): text += f"📞 Телефоны: {', '.join(result['phones'][:5])}\n"
        if result.get('addresses'): text += f"📍 Адреса: {', '.join(result['addresses'][:3])}\n"
        if result.get('inn'): text += f"📄 ИНН: {', '.join(result['inn'][:3])}\n"
        if result.get('telegrams'): text += f"📱 Telegram: {', '.join(result['telegrams'][:3])}\n"
        await status.edit_text(text[:4000])
        file = BufferedInputFile(result.get('full_text', '').encode('utf-8'),
                                 filename=f"parse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        await message.reply_document(file, caption="📄 Полный текст")
    except Exception as e:
        await status.edit_text(f"❌ Ошибка: {e}")

@dp.message(Command("start"))
async def start_cmd(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    user = await get_user(user_id)
    referrer_id = None
    args = message.text.split()
    if len(args) > 1:
        payload = args[1]
        ref_code = payload[4:] if payload.startswith("ref_") else (payload[3:] if payload.startswith("ref") else None)
        if ref_code:
            async with db_lock:
                async with db_conn.execute('SELECT user_id FROM users WHERE referral_code = ?', (ref_code,)) as cur:
                    row = await cur.fetchone()
            if row and row['user_id'] != user_id:
                referrer_id = row['user_id']
    if not user:
        await create_user(user_id, username, referrer_id)
        if referrer_id:
            try:
                await bot.send_message(referrer_id, f"По вашей ссылке пришёл @{username or 'пользователь'}, +1 запрос.")
            except Exception:
                pass

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")],
    ])
    text = (
        "🕵️ dataseeker — твой бесплатный цифровой детектив.\n\n"
        "Типы поиска:\n\n"
        "┌ Контакты:\n├ Телефон → +79999999999\n└ Email → ivanov@gmail.com\n\n"
        "┌ Соцсети:\n└ VK → vk.com/id1234567\n\n"
        "┌ Онлайн-следы:\n└ IP → 185.85.219.243\n\n"
        "┌ Физ. лица:\n├ ИНН → /inn 123456789012\n└ ФИО → Иванов Иван Иванович\n\n"
        "Каждые 24 часа выдаётся по 5 бесплатных запросов.\n\n"
        "📎 /parse URL [запрос] — парсинг страницы"
    )
    await message.reply(text, reply_markup=keyboard)

@dp.message(Command("inn"))
async def inn_cmd(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Укажите ИНН: `/inn 123456789012`", parse_mode="Markdown")
        return
    inn = args[1].strip()
    if not re.match(r'^\d{10,12}$', inn):
        await message.reply("Неверный формат ИНН.")
        return
    await process_general_query(message, inn, "inn")

def detect_type(text: str) -> str:
    cleaned = re.sub(r'\s+', '', text)
    if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', text):
        return "email"
    if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', cleaned):
        return "ip"
    if re.search(r'vk\.com/', text, re.IGNORECASE):
        return "vk"
    if re.match(r'^\+?\d{10,15}$', cleaned):
        return "phone"
    words = text.split()
    if len(words) >= 3 and all(re.match(r'^[А-Яа-я\-]+$', w) for w in words[:3]):
        return "fio"
    return "phone"

async def process_general_query(message: Message, query: str, search_type: str):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await create_user(user_id, message.from_user.username)
    if await get_user_available_requests(user_id) <= 0:
        await message.reply("Лимит запросов исчерпан.")
        return
    status = await message.reply(f"🔍 Поиск по {search_type}...")
    try:
        data = await collect_general_data(query, search_type)
        if search_type == "phone":
            views = await get_unique_views_phone(query, user_id)
            await save_report(query, data)
        else:
            views = 0
        html = generate_html_report(data, views)
        file = BufferedInputFile(html.encode('utf-8'), filename=f"report_{search_type}_{query}.html")
        await status.delete()
        await message.reply_document(file, caption=f"📋 Отчёт по {search_type}: {query}")
        await use_request(user_id)
    except Exception as e:
        logger.exception("process_general_query error")
        await status.edit_text(f"❌ Ошибка: {e}")

@dp.message(lambda msg: msg.text and not msg.text.startswith('/'), StateFilter(None))
async def universal_handler(message: Message):
    text = message.text.strip()
    await process_general_query(message, text, detect_type(text))

# === КОЛБЭКИ ===
@dp.callback_query(lambda c: c.data == "my_profile")
async def my_profile_cb(cb: CallbackQuery):
    user_id = cb.from_user.id
    user = await get_user(user_id)
    if not user:
        await cb.answer("Не зарегистрированы.")
        return
    available = await get_user_available_requests(user_id)
    text = f"Ваш профиль\nID: {user_id}\nДоступно запросов: {available}\nРегистрация: {user['created_at']}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ввести промокод", callback_data="enter_promo")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")],
    ])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

@dp.callback_query(lambda c: c.data == "referral_system")
async def referral_system_cb(cb: CallbackQuery):
    user_id = cb.from_user.id
    invited, bonuses = await get_referral_stats(user_id)
    code = await get_referral_code(user_id)
    bot_username = (await bot.get_me()).username
    text = (f"Реферальная система\n\n+1 запрос за каждого друга.\n"
            f"Ссылка:\nhttps://t.me/{bot_username}?start=ref{code}\n\n"
            f"Приглашено: {invited}\nБонусов: {bonuses}")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]])
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

@dp.callback_query(lambda c: c.data == "buy_requests")
async def buy_requests_cb(cb: CallbackQuery):
    buttons = []
    for pkg in PACKAGES:
        if pkg["requests"] == 1000:
            buttons.append([InlineKeyboardButton(
                text=f"Выгодный · {pkg['requests']} запр. · {pkg['stars']}⭐",
                callback_data=f"pkg_{pkg['requests']}_{pkg['usd']}_{pkg['stars']}"
            )])
        else:
            buttons.append(InlineKeyboardButton(
                text=f"{pkg['requests']} запр. · {pkg['stars']}⭐",
                callback_data=f"pkg_{pkg['requests']}_{pkg['usd']}_{pkg['stars']}"
            ))
    rows, row = [], []
    for btn in buttons:
        if isinstance(btn, list):
            if row: rows.append(row); row = []
            rows.append(btn)
        else:
            row.append(btn)
            if len(row) == 2: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton(text="Назад", callback_data="back_to_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await cb.message.edit_text("Выбери тариф:\n\nЧем больше пакет — тем дешевле запрос.", reply_markup=kb)
    await cb.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("pkg_"))
async def pkg_selected_cb(cb: CallbackQuery):
    parts = cb.data.split("_")
    rq, usd, stars = int(parts[1]), float(parts[2]), int(parts[3])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Оплатить {stars}⭐", callback_data=f"pay_stars_{rq}_{stars}")],
        [InlineKeyboardButton(text="Назад", callback_data="buy_requests")],
    ])
    await cb.message.edit_text(
        f"Пакет: {rq} запросов\nЦена: {stars}⭐\n\nОплата через Telegram Stars.",
        reply_markup=kb
    )
    await cb.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("pay_stars_"))
async def pay_stars_cb(cb: CallbackQuery):
    parts = cb.data.split("_")
    rq, stars = int(parts[2]), int(parts[3])
    ok, _ = await create_stars_invoice(cb.from_user.id, stars, rq)
    if not ok:
        await cb.message.edit_text(
            "Ошибка создания счёта.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="buy_requests")]])
        )
    else:
        await cb.message.edit_text(
            "Счёт создан. Оплатите в Telegram.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="buy_requests")]])
        )
    await cb.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(q: PreCheckoutQuery):
    await q.answer(ok=True)

@dp.message(lambda m: m.successful_payment is not None)
async def success_payment_handler(message: Message):
    sp = message.successful_payment
    await process_stars_payment(sp.telegram_payment_charge_id, message.from_user.id, sp.invoice_payload)

@dp.callback_query(lambda c: c.data == "enter_promo")
async def enter_promo_cb(cb: CallbackQuery, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="promo_back")]])
    await cb.message.edit_text("Введите промокод:", reply_markup=kb)
    await state.set_state(EnterPromo.waiting_for_code)
    await cb.answer()

@dp.callback_query(lambda c: c.data == "promo_back")
async def promo_back_cb(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await my_profile_cb(cb)

@dp.message(EnterPromo.waiting_for_code)
async def enter_promo_process(message: Message, state: FSMContext):
    code = message.text.strip()
    ok, msg = await activate_promo_code(message.from_user.id, code)
    await message.reply(msg)
    await state.clear()

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu_cb(cb: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой профиль", callback_data="my_profile")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton(text="Пополнить запросы", callback_data="buy_requests")],
        [InlineKeyboardButton(text="Поддержка", url="tg://resolve?domain=crytcore")],
    ])
    await cb.message.edit_text("Выберите действие:", reply_markup=kb)
    await cb.answer()

# === АДМИН ===
def get_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="Создать промокод", callback_data="admin_create_promo")],
        [InlineKeyboardButton(text="Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Выдать запросы", callback_data="admin_give")],
        [InlineKeyboardButton(text="Список промокодов", callback_data="admin_list_promo")],
        [InlineKeyboardButton(text="Удалить промокод", callback_data="admin_delete_promo")],
        [InlineKeyboardButton(text="Платежи", callback_data="admin_payments")],
        [InlineKeyboardButton(text="Закрыть", callback_data="admin_close")],
    ])

@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Доступ запрещён.")
        return
    await message.reply("Админ-панель", reply_markup=get_admin_kb())

@dp.callback_query(lambda c: c.data and c.data.startswith("admin_"))
async def admin_cb(cb: CallbackQuery, state: FSMContext = None):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("Доступ запрещён.")
        return
    action = cb.data.replace("admin_", "")
    back_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_back")]])
    if action == "stats":
        async with db_lock:
            async with db_conn.execute('SELECT COUNT(*) FROM users') as cur: users = (await cur.fetchone())[0]
            async with db_conn.execute('SELECT COUNT(*) FROM reports') as cur: reports = (await cur.fetchone())[0]
            async with db_conn.execute('SELECT COUNT(*) FROM promo_codes') as cur: promos = (await cur.fetchone())[0]
            async with db_conn.execute("SELECT COUNT(*) FROM purchases WHERE status='confirmed'") as cur: payments = (await cur.fetchone())[0]
        await cb.message.edit_text(
            f"Статистика\nПользователей: {users}\nОтчётов: {reports}\nПромокодов: {promos}\nПлатежей: {payments}",
            reply_markup=back_kb
        )
    elif action == "back":
        await cb.message.edit_text("Админ-панель", reply_markup=get_admin_kb())
    elif action == "close":
        await cb.message.delete()
    elif action == "create_promo":
        await cb.message.edit_text("Введите промокод:", reply_markup=back_kb)
        if state: await state.set_state(PromoCreation.waiting_for_code)
    elif action == "broadcast":
        await cb.message.edit_text("Введите текст рассылки:", reply_markup=back_kb)
        if state: await state.set_state(Broadcast.waiting_for_text)
    elif action == "give":
        await cb.message.edit_text("Введите ID пользователя:", reply_markup=back_kb)
        if state: await state.set_state(GiveRequests.waiting_for_user_id)
    elif action == "list_promo":
        promos = await get_all_promo_codes()
        text = "Нет промокодов." if not promos else "Промокоды:\n" + "\n".join(
            [f"{p['code']} — {p['used_count']}/{p['max_uses']}" for p in promos]
        )
        await cb.message.edit_text(text, reply_markup=back_kb)
    elif action == "delete_promo":
        promos = await get_all_promo_codes()
        if not promos:
            await cb.message.edit_text("Нет промокодов.", reply_markup=back_kb)
        else:
            rows = [[InlineKeyboardButton(text=f"❌ {p['code']}", callback_data=f"delpromo_{p['code']}")] for p in promos]
            rows.append([InlineKeyboardButton(text="Назад", callback_data="admin_back")])
            await cb.message.edit_text("Выберите промокод для удаления:",
                                       reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    elif action == "payments":
        async with db_lock:
            async with db_conn.execute('SELECT * FROM purchases ORDER BY created_at DESC LIMIT 10') as cur:
                rows_db = await cur.fetchall()
        text = "Нет платежей." if not rows_db else "Последние платежи:\n" + "\n".join(
            [f"{p['user_id']} — {p['requests']} запр., {p['amount']} {p['currency']}, {p['status']}" for p in rows_db]
        )
        await cb.message.edit_text(text, reply_markup=back_kb)
    await cb.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("delpromo_"))
async def delpromo_cb(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("Доступ запрещён.")
        return
    code = cb.data.replace("delpromo_", "")
    await delete_promo_code(code)
    await cb.answer(f"Удалён: {code}")
    await cb.message.edit_text("Админ-панель", reply_markup=get_admin_kb())

@dp.message(PromoCreation.waiting_for_code)
async def promo_code_input(message: Message, state: FSMContext):
    code = message.text.strip()
    if not re.match(r'^[A-Za-z0-9_\-]+$', code):
        await message.reply("Некорректный промокод.")
        return
    if await get_promo_code(code):
        await message.reply("Такой промокод уже есть.")
        return
    await state.update_data(code=code)
    await state.set_state(PromoCreation.waiting_for_max_uses)
    await message.reply("Максимальное количество активаций:")

@dp.message(PromoCreation.waiting_for_max_uses)
async def promo_max_uses(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n <= 0:
            await message.reply("Число должно быть > 0.")
            return
        await state.update_data(max_uses=n)
        await state.set_state(PromoCreation.waiting_for_requests)
        await message.reply("Сколько запросов даёт промокод:")
    except ValueError:
        await message.reply("Введите целое число.")

@dp.message(PromoCreation.waiting_for_requests)
async def promo_requests(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n <= 0:
            await message.reply("Число должно быть > 0.")
            return
        data = await state.get_data()
        ok = await create_promo_code(data['code'], data['max_uses'], n, message.from_user.id)
        await message.reply(f"Промокод создан: {data['code']}" if ok else "Ошибка создания.")
        await state.clear()
    except ValueError:
        await message.reply("Введите целое число.")

@dp.message(GiveRequests.waiting_for_user_id)
async def give_uid(message: Message, state: FSMContext):
    try:
        uid = int(message.text.strip())
        await state.update_data(target_user_id=uid)
        await state.set_state(GiveRequests.waiting_for_amount)
        await message.reply("Количество запросов:")
    except ValueError:
        await message.reply("Введите ID.")

@dp.message(GiveRequests.waiting_for_amount)
async def give_amount(message: Message, state: FSMContext):
    try:
        n = int(message.text.strip())
        if n <= 0:
            await message.reply("Количество > 0.")
            return
        data = await state.get_data()
        uid = data['target_user_id']
        if not await get_user(uid):
            await message.reply(f"Пользователь {uid} не найден.")
            await state.clear()
            return
        async with db_lock:
            await db_conn.execute('UPDATE users SET bonus_requests = bonus_requests + ? WHERE user_id = ?', (n, uid))
            await db_conn.commit()
        await message.reply(f"Выдано {n} запросов пользователю {uid}.")
        await state.clear()
    except ValueError:
        await message.reply("Введите число.")

@dp.message(Broadcast.waiting_for_text)
async def broadcast_input(message: Message, state: FSMContext):
    text = message.text
    async with db_lock:
        async with db_conn.execute('SELECT user_id FROM users') as cur:
            users = await cur.fetchall()
    count = 0
    for u in users:
        try:
            await bot.send_message(u['user_id'], text)
            count += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await message.reply(f"Рассылка завершена. Отправлено {count}.")
    await state.clear()

# === ЗАПУСК ===
async def main():
    await asyncio.sleep(1)
    await init_db()
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="Bot is running"))
    app.router.add_get("/health", lambda r: web.Response(text="OK"))
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, skip_updates=True, allowed_updates=["message", "callback_query"])
    finally:
        await runner.cleanup()
        await bot.session.close()
        if db_conn:
            await db_conn.close()
        if http_session:
            await http_session.close()

if __name__ == "__main__":
    asyncio.run(main())
