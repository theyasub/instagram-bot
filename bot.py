import os
import csv
import asyncio
import psycopg2
import time
import random
import pickle
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.types import InputFile
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, RateLimitError
from dotenv import load_dotenv

# ---------------- CONFIG ----------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
IG_USERNAME = os.getenv("IG_USERNAME")
IG_PASSWORD = os.getenv("IG_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "instagram_bot")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PORT = os.getenv("DB_PORT", "5432")

MAX_REQUESTS_PER_HOUR = int(os.getenv("MAX_REQUESTS_PER_HOUR", "40"))
MAX_REQUESTS_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "3"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
cl = Client()
cl.delay_range = [0, 0]  # delay yo‘q

LOG_FILE = "logs.txt"

# ---------------- Logger ----------------
def log(text: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{now}] {text}\n")
    except Exception:
        print(f"[{now}] (log error) {text}")

# ---------------- Rate limiter ----------------
class RateLimiter:
    def __init__(self):
        self.requests = []
        self.hourly_limit = MAX_REQUESTS_PER_HOUR
        self.minute_limit = MAX_REQUESTS_PER_MINUTE

    def can_make_request(self):
        now = time.time()
        self.requests = [r for r in self.requests if now - r < 3600]
        if len(self.requests) >= self.hourly_limit:
            return False, "hourly"
        recent = [r for r in self.requests if now - r < 60]
        if len(recent) >= self.minute_limit:
            return False, "minute"
        return True, None

    def add_request(self):
        self.requests.append(time.time())

    def wait_time(self):
        now = time.time()
        recent = [r for r in self.requests if now - r < 60]
        if len(recent) >= self.minute_limit:
            oldest = min(recent)
            return int(60 - (now - oldest)) + 1
        if len(self.requests) >= self.hourly_limit:
            oldest = min(self.requests)
            return int(3600 - (now - oldest)) + 1
        return 0

rate_limiter = RateLimiter()

# ---------------- Database ----------------
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, port=DB_PORT
    )

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        telegram_username TEXT,
        phone_number TEXT,
        requests_used INTEGER DEFAULT 0,
        requests_limit INTEGER DEFAULT 5,
        reset_date TIMESTAMP DEFAULT NOW(),
        created_at TIMESTAMP DEFAULT NOW()
    )''')
    conn.commit()
    conn.close()

def create_user(user_id, telegram_username, phone_number=None):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        INSERT INTO users (user_id, telegram_username, phone_number, requests_used, requests_limit, reset_date)
        VALUES (%s, %s, %s, 0, 5, NOW())
        ON CONFLICT (user_id) DO UPDATE
        SET telegram_username = EXCLUDED.telegram_username,
            phone_number = COALESCE(EXCLUDED.phone_number, users.phone_number)
    ''', (user_id, telegram_username, phone_number))
    conn.commit()
    conn.close()

def get_user(user_id):
    conn = get_db_connection()
    c = conn.cursor(cursor_factory=RealDictCursor)
    c.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def check_reset_needed(user_id):
    conn = get_db_connection()
    c = conn.cursor(cursor_factory=RealDictCursor)
    c.execute("SELECT reset_date FROM users WHERE user_id=%s", (user_id,))
    row = c.fetchone()
    if row:
        reset_date = row['reset_date']
        if not reset_date or datetime.now() > (reset_date + timedelta(days=1)):
            c.execute('''UPDATE users SET requests_used = 0, reset_date = NOW() WHERE user_id=%s''', (user_id,))
            conn.commit()
    conn.close()

def increment_request(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE users SET requests_used = requests_used + 1 WHERE user_id=%s", (user_id,))
    conn.commit()
    conn.close()

# ---------------- Instagram login ----------------
def insta_login():
    session_file = "session.json"
    try:
        if os.path.exists(session_file):
            log("Session fayl topildi, yuklanmoqda...")
            cl.load_settings(session_file)
            try:
                cl.login(IG_USERNAME, IG_PASSWORD)
                cl.get_timeline_feed()
                log("Instagram sessiya orqali login muvaffaqiyatli!")
                return True
            except LoginRequired:
                log("Session eskirgan. O‘chirilyapti...")
                os.remove(session_file)

        log("Yangi login amalga oshirilmoqda...")
        cl.login(IG_USERNAME, IG_PASSWORD)
        cl.dump_settings(session_file)
        log("Yangi sessiya saqlandi!")
        return True
    except Exception as e:
        log(f"insta_login error: {e}")
        return False

# ---------------- Helper funksiyalar ----------------
def calculate_virality(likes, comments, views):
    if views == 0:
        return 0
    engagement = likes + comments
    return round((engagement / views) * 100, 2)

def create_progress_bar(percent: int) -> str:
    percent = max(0, min(100, int(percent)))
    filled = int(percent / 10)
    bar = "█" * filled + "░" * (10 - filled)
    return f"[{bar}] {percent}%"

async def safe_edit(msg_obj, text):
    try:
        await msg_obj.edit_text(text)
    except Exception:
        try:
            await asyncio.sleep(0.5)
            await msg_obj.edit_text(text)
        except Exception:
            pass

# ---------------- Tez media yuklash + kesh ----------------
async def fetch_medias_fast(username_input, user_id_ig, total):
    os.makedirs("cache", exist_ok=True)
    cache_file = os.path.join("cache", f"{username_input}.pkl")

    # Keshdan o‘qish
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "rb") as f:
                cached = pickle.load(f)
                if cached and len(cached) >= total:
                    log(f"Keshdan o‘qildi: {username_input} ({len(cached)} ta post)")
                    return cached[:total]
        except Exception as e:
            log(f"Kesh o‘qishda xato: {e}")

    # Parallel yuklash
    step = 30
    tasks = [asyncio.to_thread(cl.user_medias_v1, user_id_ig, i) for i in range(0, total, step)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    medias = []
    for r in results:
        if isinstance(r, list):
            medias.extend(r)
    medias = medias[:total]

    # Keshga yozish
    try:
        with open(cache_file, "wb") as f:
            pickle.dump(medias, f)
        log(f"Kesh saqlandi: {username_input} ({len(medias)} ta post)")
    except Exception as e:
        log(f"Kesh yozishda xato: {e}")

    return medias

# ---------------- Bot handlerlari ----------------
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Foydalanuvchi"
    user = get_user(user_id)
    if not user or not user.get("phone_number"):
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        contact_btn = types.KeyboardButton("📞 Telefon raqamni yuborish", request_contact=True)
        keyboard.add(contact_btn)
        await message.reply(
            "🚀 Instagram Tahlil Botiga xush kelibsiz!\n\nTelefon raqamingizni yuboring 👇",
            reply_markup=keyboard
        )
        return
    await message.reply("✅ Siz ro‘yxatdan o‘tgan ekansiz. Endi username yuboring (masalan: ummmusfira).")

@dp.message_handler(content_types=types.ContentType.CONTACT)
async def contact_handler(message: types.Message):
    user_id = message.from_user.id
    phone_number = message.contact.phone_number
    username = message.from_user.username or "Foydalanuvchi"
    create_user(user_id, username, phone_number)
    await message.reply("🎉 Ro‘yxatdan o‘tdingiz! Endi username yuboring.", reply_markup=types.ReplyKeyboardRemove())

@dp.message_handler()
async def analyze_user(message: types.Message):
    user_id = message.from_user.id
    username_input = message.text.strip().lstrip('@')
    if not username_input or ' ' in username_input:
        await message.reply("❗ To‘g‘ri username kiriting, masalan: ummmusfira")
        return

    # foydalanuvchini tekshirish
    user = get_user(user_id)
    if not user or not user.get("phone_number"):
        await message.reply("📞 Avval /start ni bosib ro‘yxatdan o‘ting.")
        return

    check_reset_needed(user_id)
    remaining = user['requests_limit'] - user['requests_used']
    if remaining <= 0:
        await message.reply("❌ Kunlik limit tugagan. Ertaga urinib ko‘ring.")
        return

    can_request, _ = rate_limiter.can_make_request()
    if not can_request:
        wt = rate_limiter.wait_time()
        await message.reply(f"⏳ {wt} soniya kuting, iltimos.")
        return

    status_msg = await message.reply(f"🔍 @{username_input} tahlil qilinmoqda...")

    try:
        rate_limiter.add_request()
        await asyncio.sleep(random.uniform(0.8, 1.5))

        if not cl.user_id:
            insta_login()

        user_id_ig = await asyncio.to_thread(cl.user_id_from_username, username_input)
        user_info = await asyncio.to_thread(cl.user_info, user_id_ig)
        total_posts = getattr(user_info, 'media_count', 0) or 0
        amount = min(total_posts, 200)

        if total_posts == 0:
            await safe_edit(status_msg, f"ℹ️ @{username_input} da post topilmadi.")
            return

        base_msg = f"✅ @{username_input} topildi — {total_posts} post.\n\n📥 Yuklanmoqda...\n\n"
        progress_state = {"percent": 0, "running": True}

        async def progress_anim():
            while progress_state["running"]:
                progress_state["percent"] = min(95, progress_state["percent"] + random.choice([1, 2]))
                await safe_edit(status_msg, base_msg + create_progress_bar(progress_state["percent"]))
                await asyncio.sleep(random.uniform(0.5, 1.0))

        animator = asyncio.create_task(progress_anim())
        medias = await fetch_medias_fast(username_input, user_id_ig, amount)
        progress_state["running"] = False
        animator.cancel()
        await safe_edit(status_msg, base_msg + create_progress_bar(100))
        await asyncio.sleep(0.4)

        if not medias:
            await safe_edit(status_msg, "❌ Postlar yuklanmadi.")
            return

        media_list = []
        for m in medias:
            likes = getattr(m, 'like_count', 0)
            comments = getattr(m, 'comment_count', 0)
            views = getattr(m, 'video_view_count', 0) or getattr(m, 'view_count', 0) or likes
            score = (likes * 0.5) + (comments * 1.5) + (views * 0.001)
            media_list.append({'media': m, 'likes': likes, 'comments': comments, 'views': views, 'score': score})

        media_list.sort(key=lambda x: x['score'], reverse=True)
        top_50 = media_list[:50]

        filename = f"{username_input}_top50.csv"
        with open(filename, "w", newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(["Rank", "Link", "Ko‘rishlar", "Layklar", "Kommentlar", "Viral (%)", "Score", "Turi", "Sana", "Tavsif"])
            for i, item in enumerate(top_50, 1):
                m = item['media']
                virality = calculate_virality(item['likes'], item['comments'], item['views'])
                media_type = "Reels" if getattr(m, 'media_type', 1) == 2 else "Post"
                caption = (getattr(m, 'caption_text', '') or '')[:150]
                date_str = getattr(m, 'taken_at', None)
                date_str = date_str.strftime("%Y-%m-%d %H:%M") if date_str else ''
                writer.writerow([i, f"https://www.instagram.com/p/{getattr(m, 'code', '')}/",
                                 item['views'], item['likes'], item['comments'],
                                 virality, round(item['score'], 2), media_type, date_str, caption])

        increment_request(user_id)
        new_user = get_user(user_id)
        remain = new_user['requests_limit'] - new_user['requests_used']

        await message.reply_document(
            InputFile(filename),
            caption=f"✅ TOP {len(top_50)} @{username_input}\n📊 {len(medias)} postdan tanlandi\nQolgan so‘rovlar: {remain}/{new_user['requests_limit']}"
        )
        os.remove(filename)

    except Exception as e:
        log(f"analyze_user error ({username_input}): {e}")
        await message.reply("⚙️ Bot vaqtincha nosoz. Keyinroq urinib ko‘ring.")

# ---------------- Run ----------------
if __name__ == '__main__':
    init_db()
    if not insta_login():
        log("⚠️ Instagram login muvaffaqiyatsiz.")
    log("Bot ishga tushdi.")
    print("🤖 Bot ishga tushdi...")
    asyncio.run(dp.start_polling(bot))

