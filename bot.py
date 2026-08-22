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
JITLER_TOKENS_STR = os.getenv("JITLER_TOKENS", "")
JITLER_TOKENS = [t.strip() for t in JITLER_TOKENS_STR.split(",") if t.strip()]

db_pool = None

async def get_pool():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
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
    """Рекурсивно ищет ключ в любой структуре данных."""
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
    """Возвращает список всех значений ключа."""
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

async def collect_data(query: str):
    bigbase = await bigbase_search(query)
    nightsearch = await nightsearch_search(query)
    seon = await seon_search(query)
    jitler = await jitler_search_with_balancer(query, "number")
    
    # Логирование полных ответов
    print("\n=== BIGBASE ===")
    print(json.dumps(bigbase, ensure_ascii=False, indent=2)[:10000])
    print("\n=== NIGHTSEARCH ===")
    print(json.dumps(nightsearch, ensure_ascii=False, indent=2)[:10000])
    print("\n=== SEON ===")
    print(json.dumps(seon, ensure_ascii=False, indent=2)[:10000])
    print("\n=== JITLER ===")
    print(json.dumps(jitler, ensure_ascii=False, indent=2)[:10000])
    
    # Ищем все возможные поля
    result = {
        'phone': query,
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
        'all_data': []
    }
    
    # Парсим BigBase
    if bigbase and isinstance(bigbase, dict):
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
            
            # Ищем дату рождения
            birthdate_list = person.get('birthdate', [])
            for bd_item in birthdate_list:
                if bd_item.get('value'):
                    result['birthdate'] = bd_item['value']
                    break
            if result['birthdate']:
                break
        
        # Собираем все данные из BigBase для вывода
        for key in ['email', 'mail', 'e-mail', 'telegram', 'tg', 'vk', 'vkontakte', 'instagram', 'tiktok', 'ok', 'odnoklassniki', 'bank', 'banks', 'account']:
            values = deep_find_all(bigbase, key)
            if values:
                if key in ['email', 'mail', 'e-mail']:
                    result['emails'].extend(values)
                elif key in ['telegram', 'tg']:
                    result['telegrams'].extend(values)
                elif key == 'vk' or key == 'vkontakte':
                    result['vk'] = values[0]
                elif key == 'instagram':
                    result['instagram'] = values[0]
                elif key == 'tiktok':
                    result['tiktok'] = values[0]
                elif key == 'ok' or key == 'odnoklassniki':
                    result['ok'] = values[0]
                elif key in ['bank', 'banks', 'account']:
                    result['banks'].extend(values)
    
    # Парсим другие API
    for src in [nightsearch, seon, jitler]:
        if not src:
            continue
        if not result['fio']:
            result['fio'] = deep_find(src, 'full_name') or deep_find(src, 'fio') or deep_find(src, 'name')
        if not result['birthdate']:
            result['birthdate'] = deep_find(src, 'birthdate') or deep_find(src, 'birth_date') or deep_find(src, 'date_of_birth')
        if not result['age']:
            age_val = deep_find(src, 'age')
            if age_val:
                result['age'] = age_val
        if not result['operator']:
            result['operator'] = deep_find(src, 'operator') or deep_find(src, 'oper')
        if not result['region']:
            result['region'] = deep_find(src, 'region') or deep_find(src, 'reg')
        if not result['country']:
            result['country'] = deep_find(src, 'country')
        if not result['vk']:
            result['vk'] = deep_find(src, 'vk') or deep_find(src, 'vkontakte')
        if not result['instagram']:
            result['instagram'] = deep_find(src, 'instagram')
        if not result['tiktok']:
            result['tiktok'] = deep_find(src, 'tiktok')
        if not result['ok']:
            result['ok'] = deep_find(src, 'ok') or deep_find(src, 'odnoklassniki')
        
        emails = deep_find_all(src, 'email') + deep_find_all(src, 'mail') + deep_find_all(src, 'e-mail')
        result['emails'].extend(emails)
        
        tgs = deep_find_all(src, 'telegram') + deep_find_all(src, 'tg')
        result['telegrams'].extend(tgs)
        
        banks = deep_find_all(src, 'bank') + deep_find_all(src, 'banks') + deep_find_all(src, 'account')
        result['banks'].extend(banks)
    
    # Удаляем дубли
    if result['emails']:
        result['emails'] = list(dict.fromkeys([str(e) for e in result['emails'] if e]))
    if result['telegrams']:
        result['telegrams'] = list(dict.fromkeys([str(t) for t in result['telegrams'] if t]))
    if result['banks']:
        result['banks'] = list(dict.fromkeys([str(b) for b in result['banks'] if b]))
    
    return result

def format_report(data: dict) -> str:
    lines = []
    lines.append("📱")
    lines.append(f"├ Телефон: {data['phone']}")
    
    if data.get('operator'):
        lines.append(f"├ Оператор: {data['operator']}")
    if data.get('region'):
        lines.append(f"├ Регион: {data['region']}")
    if data.get('country'):
        lines.append(f"└ Страна: {data['country']}")
    
    has_personal = data.get('fio') or data.get('birthdate') or data.get('age')
    if has_personal:
        lines.append("\n👤 Основные данные")
        if data.get('fio'):
            lines.append(f"├ ФИО: {data['fio']}")
        if data.get('birthdate'):
            lines.append(f"├ Дата рождения: {data['birthdate']}")
        if data.get('age'):
            lines.append(f"└ Возраст: {data['age']}")
    
    if data.get('emails'):
        lines.append(f"\n📧 E-mail: {', '.join(data['emails'])}")
    
    if data.get('telegrams'):
        lines.append(f"\n💬 Telegram: {', '.join(data['telegrams'])}")
    
    if data.get('vk'):
        lines.append(f"\n📘 ВКонтакте: {data['vk']}")
    if data.get('instagram'):
        lines.append(f"\n📷 Instagram: {data['instagram']}")
    if data.get('tiktok'):
        lines.append(f"\n🎵 TikTok: {data['tiktok']}")
    if data.get('ok'):
        lines.append(f"\n👥 Одноклассники: {data['ok']}")
    
    if data.get('banks'):
        lines.append(f"\n🏦 Банки: {', '.join(data['banks'])}")
    
    return "\n".join(lines)

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
    
    if not is_phone:
        await message.reply("❌ Введите корректный номер телефона.")
        return
    
    status = await message.reply("🔍 Поиск...")
    data = await collect_data(digits)
    views = await get_unique_views_phone(digits, message.from_user.id)
    
    report = format_report(data)
    report += f"\n\n👁 Интересовались этим: {views}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Telegram", url=f"tg://resolve?phone={digits}"),
         InlineKeyboardButton(text="💬 WhatsApp", url=f"https://wa.me/{digits}")]
    ])
    
    await status.edit_text(report, parse_mode="Markdown", reply_markup=keyboard)

async def health_check(request):
    return web.Response(text="OK", status=200)

async def main():
    await init_db()
    print("🚀 Бот запущен (с полным выводом данных)")

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
