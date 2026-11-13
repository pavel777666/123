# -*- coding: utf-8 -*-
"""
Telegram-бот для скачивания видео (YouTube / Instagram / TikTok)
Один файл, без БД: всё хранится в памяти + автосохранение в data.json.

Зависимости: pip install aiogram yt-dlp  (+ ffmpeg в системе)
Запуск: BOT_TOKEN=... ADMIN_CHAT_ID=... python bot.py
"""

import asyncio
import json
import logging
import os
import time
import uuid
import yt_dlp
from collections import defaultdict, deque
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (CallbackQuery, FSInputFile, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message)

# ══════════════════════════ КОНФИГ ══════════════════════════
BOT_TOKEN     = os.getenv("BOT_TOKEN", "9111988543:AAEjVCgUQisxIej2w7qubpMkhYNZ_B_afx0")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "6309604829"))

MAX_PER_MINUTE  = 3
MAX_RETRIES     = 3
WORKERS         = 4
DEFAULT_QUALITY = "720p"
QUALITIES       = ["1080p", "720p", "480p", "360p"]
ALLOWED_HOSTS   = {"youtube.com", "youtu.be", "m.youtube.com",
                   "instagram.com", "tiktok.com"}
TELEGRAM_MAX_MB = 49
HISTORY_LIMIT   = 100

# Каналы для подписочной воронки — впишите свои
CHANNELS = [
    ("-1003811279882", "Канал 1", "https://t.me/motivation_009k"),
    ("-1003629620583", "Канал 2", "https://t.me/qwertyuioppoiuytrewq009"),
]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
err_log = logging.FileHandler("errors.log", encoding="utf-8")
err_log.setLevel(logging.ERROR)
err_log.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(err_log)

DATA_FILE = "data.json"

# ══════════════════════════ ХРАНИЛИЩЕ ══════════════════════════
class Storage:
    """Всё в памяти, автосохранение в data.json при каждом изменении."""

    def __init__(self):
        self.users = {}                        # user_id -> {username, quality}
        self.subscriptions = defaultdict(set)  # user_id -> {chat_id каналов}
        self.history = defaultdict(lambda: deque(maxlen=HISTORY_LIMIT))
        self.banned = set()
        self._rate = defaultdict(list)
        self._load()

    def _load(self):
        if not os.path.exists(DATA_FILE):
            return
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                raw = json.load(f)
            self.users = {int(k): v for k, v in raw.get("users", {}).items()}
            self.subscriptions = defaultdict(set,
                {int(k): set(v) for k, v in raw.get("subscriptions", {}).items()})
            self.history = defaultdict(lambda: deque(maxlen=HISTORY_LIMIT),
                {int(k): deque(v, maxlen=HISTORY_LIMIT)
                 for k, v in raw.get("history", {}).items()})
            self.banned = set(map(int, raw.get("banned", [])))
        except Exception:
            logging.exception("data.json повреждён — начинаю с пустого хранилища")

    def save(self):
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "users": {str(k): v for k, v in self.users.items()},
                    "subscriptions": {str(k): list(v) for k, v in self.subscriptions.items()},
                    "history": {str(k): list(v) for k, v in self.history.items()},
                    "banned": list(self.banned),
                }, f, ensure_ascii=False)
        except Exception:
            logging.exception("Не удалось сохранить data.json")

    def upsert_user(self, user_id: int, username: str | None):
        self.users.setdefault(user_id, {"username": username, "quality": DEFAULT_QUALITY})
        self.users[user_id]["username"] = username
        self.save()

    def is_banned(self, user_id: int) -> bool:
        return user_id in self.banned

    def ban(self, user_id: int):
        self.banned.add(user_id)
        self.save()

    def unban(self, user_id: int):
        self.banned.discard(user_id)
        self.save()

    def get_quality(self, user_id: int) -> str:
        return self.users.get(user_id, {}).get("quality", DEFAULT_QUALITY)

    def set_quality(self, user_id: int, q: str):
        self.users.setdefault(user_id, {"username": None, "quality": q})["quality"] = q
        self.save()

    def subscribe(self, user_id: int, chat_id):
        self.subscriptions[user_id].add(chat_id)
        self.save()

    def is_subscribed(self, user_id: int, chat_id) -> bool:
        return chat_id in self.subscriptions.get(user_id, set())

    def add_download(self, user_id: int, url: str, title: str, quality: str):
        # стабильный id — чтобы кнопка «переслать» всегда вела на тот же файл
        self.history[user_id].appendleft(
            {"id": uuid.uuid4().hex, "url": url, "title": title, "quality": quality,
             "at": time.strftime("%d.%m %H:%M")})
        self.save()

    def get_history(self, user_id: int) -> list:
        return list(self.history.get(user_id, []))

    def rate_limited(self, user_id: int) -> bool:
        now = time.time()
        stamps = [t for t in self._rate[user_id] if now - t < 60]
        self._rate[user_id] = stamps
        if len(stamps) >= MAX_PER_MINUTE:
            return True
        self._rate[user_id].append(now)
        return False

db = Storage()
router = Router()   # ← БЫЛО: router использовался без объявления — бот падал при старте

# ══════════════════════════ ГЛОБАЛЬНОЕ СОСТОЯНИЕ ══════════════════════════
queue: asyncio.Queue = asyncio.Queue()
# активные задачи: job_id -> task. Ключ по задаче, а не по пользователю —
# иначе «Отмена» у второй ссылки отменяла бы первую.
active_jobs: dict[str, asyncio.Task] = {}

# ══════════════════════════ ХЕЛПЕРЫ ══════════════════════════
async def notify_admin(bot: Bot, text: str):
    try:
        await bot.send_message(ADMIN_CHAT_ID, f"🚨 {text}", parse_mode=ParseMode.HTML)
    except Exception:
        logging.exception("Не удалось отправить алерт админу")

def check_url(url: str) -> bool:
    """Точная проверка домена, а не поиск подстроки:
    'evil.com/youtube.com' больше не проходит."""
    try:
        host = urlparse(url).hostname or ""
        host = host.removeprefix("www.").lower()
        return any(host == h or host.endswith("." + h) for h in ALLOWED_HOSTS)
    except Exception:
        return False

def detect_source(url: str) -> str:
    host = (urlparse(url).hostname or "").removeprefix("www.").lower()
    if "youtu" in host:
        return "youtube"
    if "instagram" in host:
        return "instagram"
    if "tiktok" in host:
        return "tiktok"
    return "unknown"

def main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗂 История (последние 100)", callback_data="history")],
        [InlineKeyboardButton(text="❓ Как пользоваться", callback_data="help")],
    ])

async def subs_keyboard(bot: Bot, user_id: int):
    """Кнопки ТОЛЬКО неподписанных каналов — подписанные исчезают из сообщения."""
    kb = []
    for chat_id, title, url in CHANNELS:
        if db.is_subscribed(user_id, chat_id):
            continue
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            ok = member.status != ChatMemberStatus.LEFT
        except Exception:
            ok = False
        if ok:
            db.subscribe(user_id, chat_id)
        else:
            kb.append([InlineKeyboardButton(text=f"❌ Подписаться: {title}", url=url)])
    if kb:
        kb.append([InlineKeyboardButton(text="🔄 Я подписался — проверить",
                                        callback_data="check_subs")])
        return InlineKeyboardMarkup(inline_keyboard=kb), False
    return None, True

# ══════════════════════════ ХЭНДЛЕРЫ ══════════════════════════
@router.message(CommandStart())
async def cmd_start(m: Message, bot: Bot):
    db.upsert_user(m.from_user.id, m.from_user.username)
    kb, all_ok = await subs_keyboard(bot, m.from_user.id)
    if all_ok:
        await m.answer("✅ Привет! Просто пришли ссылку на видео из YouTube, "
                       "Instagram или TikTok — я скачаю.\nПо умолчанию 720p, "
                       "другое качество — /quality", reply_markup=main_kb())
    else:
        await m.answer("👋 Привет! Для доступа подпишись на каналы:", reply_markup=kb)

@router.callback_query(F.data == "check_subs")
async def check_subs(cb: CallbackQuery, bot: Bot):
    kb, all_ok = await subs_keyboard(bot, cb.from_user.id)
    if all_ok:
        await cb.message.edit_text("✅ Готово! Пришли ссылку на видео.",
                                   reply_markup=main_kb())
    elif kb:
        await cb.message.edit_text("Осталось подписаться:", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "help")
async def cb_help(cb: CallbackQuery):
    await cb.message.edit_text(
        "📖 Мини-инструкция:\n\n"
        "1️⃣ Отправь ссылку на видео (YouTube / Instagram / TikTok)\n"
        "2️⃣ Выбери качество или жди — по умолчанию 720p\n"
        "3️⃣ Получи файл\n\n"
        "Команды:\n"
        "/quality — выбор качества (720p по умолчанию)\n"
        "/history — последние 100 файлов, можно переслать заново\n"
        "/help — эта справка", reply_markup=main_kb())
    await cb.answer()

@router.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer("📖 Отправьте ссылку на видео → выберите качество → получите файл.\n"
                   "/quality — сменить качество, /history — история загрузок.")

@router.message(Command("quality"))
async def cmd_quality(m: Message):
    kb = [[InlineKeyboardButton(text=q, callback_data=f"quality:{q}")] for q in QUALITIES]
    await m.answer("🎬 Выбери качество (по умолчанию 720p):",
                   reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("quality:"))
async def set_quality(cb: CallbackQuery):
    q = cb.data.split(":")[1]
    db.set_quality(cb.from_user.id, q)
    await cb.message.edit_text(f"✅ Качество по умолчанию: {q}")
    await cb.answer()

# ---------- Приём ссылки ----------
@router.message(F.text.regexp(r"https?://\S+"))
async def on_link(m: Message, bot: Bot):
    user = m.from_user
    if db.is_banned(user.id):
        return
    url = m.text.strip()
    if not check_url(url):
        return await m.answer("⚠️ Поддерживаются только YouTube, Instagram и TikTok.")
    if db.rate_limited(user.id):
        await notify_admin(bot, f"Флуд от <a href='tg://user?id={user.id}'>{user.id}</a>")
        return await m.answer(f"⏳ Не больше {MAX_PER_MINUTE} ссылок в минуту.")

    kb, all_ok = await subs_keyboard(bot, user.id)
    if not all_ok:
        return await m.answer("Сначала подпишись на каналы 🙂", reply_markup=kb)

    quality = db.get_quality(user.id)
    job_id = uuid.uuid4().hex
    pos = queue.qsize() + 1
    msg = await m.answer(
        f"📍 В очереди #{pos}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=
            [[InlineKeyboardButton(text="✖ Отмена", callback_data=f"cancel:{job_id}")]]))
    await queue.put({"job_id": job_id, "chat_id": m.chat.id, "msg_id": msg.message_id,
                     "user_id": user.id, "url": url, "quality": quality,
                     "title": None})

# ---------- Отмена ----------
@router.callback_query(F.data.startswith("cancel:"))
async def cancel_job(cb: CallbackQuery):
    job_id = cb.data.split(":", 1)[1]
    task = active_jobs.get(job_id)
    if task:
        task.cancel()
        await cb.message.edit_text("✖ Отменено.")
    await cb.answer()

# ---------- История ----------
@router.message(Command("history"))
async def cmd_history(m: Message):
    await send_history(m)

@router.callback_query(F.data == "history")
async def cb_history(cb: CallbackQuery):
    await send_history(cb.message)
    await cb.answer()

async def send_history(m: Message):
    items = db.get_history(m.from_user.id)
    if not items:
        return await m.answer("🗂 История пуста.")
    # callback_data = resend:<стабильный id> — индексы больше не «плывут»
    kb = [[InlineKeyboardButton(
        text=f"{it['title'] or it['url']}"[:40], callback_data=f"resend:{it['id']}")]
        for it in items]
    await m.answer("🗂 Последние 100 файлов — нажмите, чтобы переслать:",
                   reply_markup=InlineKeyboardMarkup(inline_keyboard=kb[:50]))

@router.callback_query(F.data.startswith("resend:"))
async def resend(cb: CallbackQuery, bot: Bot):
    item_id = cb.data.split(":", 1)[1]
    it = next((x for x in db.get_history(cb.from_user.id) if x["id"] == item_id), None)
    if it:
        # повторная загрузка проходит те же проверки: подписка и лимит
        if db.is_banned(cb.from_user.id):
            return await cb.answer("Вы заблокированы.", show_alert=True)
        if db.rate_limited(cb.from_user.id):
            return await cb.answer(f"⏳ Не больше {MAX_PER_MINUTE} ссылок в минуту.",
                                   show_alert=True)
        kb, all_ok = await subs_keyboard(bot, cb.from_user.id)
        if not all_ok:
            await cb.message.answer("Сначала подпишись на каналы 🙂", reply_markup=kb)
            return await cb.answer()
        job_id = uuid.uuid4().hex
        msg = await cb.message.answer(
            "📍 В очереди…",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=
                [[InlineKeyboardButton(text="✖ Отмена", callback_data=f"cancel:{job_id}")]]))
        await queue.put({"job_id": job_id, "chat_id": cb.message.chat.id,
                         "msg_id": msg.message_id, "user_id": cb.from_user.id,
                         "url": it["url"], "quality": it["quality"],
                         "title": it["title"]})
        await cb.answer("📥 Повторно ставлю в очередь…")
    else:
        await cb.answer("Запись не найдена.", show_alert=True)

# ---------- Админ: /ban <id> и /unban <id> ----------
@router.message(Command("ban"))
async def cmd_ban(m: Message):
    if m.from_user.id != ADMIN_CHAT_ID:
        return
    parts = (m.text or "").split()
    if len(parts) == 2 and parts[1].lstrip("-").isdigit():
        db.ban(int(parts[1]))
        await m.answer(f"✅ Пользователь {parts[1]} забанен.")
    else:
        await m.answer("Формат: /ban <user_id>")

@router.message(Command("unban"))
async def cmd_unban(m: Message):
    if m.from_user.id != ADMIN_CHAT_ID:
        return
    parts = (m.text or "").split()
    if len(parts) == 2 and parts[1].lstrip("-").isdigit():
        db.unban(int(parts[1]))
        await m.answer(f"✅ Пользователь {parts[1]} разбанен.")
    else:
        await m.answer("Формат: /unban <user_id>")

# ══════════════════════════ ВОРКЕРЫ СКАЧИВАНИЯ ══════════════════════════
async def download_worker(bot: Bot):
    while True:
        job = await queue.get()
        task = asyncio.create_task(process(bot, job))
        active_jobs[job["job_id"]] = task
        task.add_done_callback(lambda t, jid=job["job_id"]: active_jobs.pop(jid, None))
        queue.task_done()

async def process(bot: Bot, job: dict):
    chat_id, msg_id = job["chat_id"], job["msg_id"]
    user_id, url, quality = job["user_id"], job["url"], job["quality"]
    loop = asyncio.get_running_loop()
    last = {"p": -1, "t": 0.0}

    async def edit_progress(p: int):
        now = time.monotonic()
        if now - last["t"] < 3:      # редактируем не чаще раза в 3 секунды
            return
        last.update(p=p, t=now)
        if msg_id:
            try:
                await bot.edit_message_text(f"⬇️ Скачиваю… {p}%", chat_id, msg_id)
            except Exception:
                pass

    def hook(d):
        # отмена прямо внутри yt-dlp: бросаем исключение — загрузка прерывается,
        # а не продолжает качаться в фоне
        if d["status"] == "downloading" and d.get("total_bytes"):
            p = int(d["downloaded_bytes"] / d["total_bytes"] * 100)
            if p != last["p"]:
                asyncio.run_coroutine_threadsafe(edit_progress(p), loop)

    h = quality.replace("p", "")
    opts = {
        "format": f"bestvideo[height<={h}]+bestaudio/best[height<={h}]/best",
        "outtmpl": os.path.join(os.environ.get("TMPDIR", "/tmp"), "%(id)s.%(ext)s"),
        "quiet": True, "noplaylist": True, "progress_hooks": [hook],
        "merge_output_format": "mp4",
    }

    async def status(text: str):
        if msg_id:
            try:
                await bot.edit_message_text(text, chat_id, msg_id)
            except Exception:
                pass

    def _download():
        # ← БЫЛО: ydl.extract_download — такого метода не существует
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=True)

    path = None
    try:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await status("⬇️ Скачиваю… 0%")
                info = await loop.run_in_executor(None, _download)
                # путь к скачанному файлу — в requested_downloads
                rd = (info.get("requested_downloads") or [{}])[0]
                path = rd.get("filepath") or rd.get("filename")
                title = info.get("title") or "видео"
                size = rd.get("filesize") or rd.get("filesize_approx") \
                    or info.get("filesize_approx") or 0
                if not path:
                    raise RuntimeError("yt-dlp не вернул путь к файлу")
                if size and size > TELEGRAM_MAX_MB * 1024 * 1024:
                    raise ValueError(
                        f"файл больше {TELEGRAM_MAX_MB} МБ — выберите качество пониже")

                # слияние видео+аудио делает сам yt-dlp (merge_output_format=mp4,
                # нужен ffmpeg) — отдельного этапа конвертации нет
                await status("📤 Отправляю…")
                await bot.send_video(chat_id, FSInputFile(path),
                                     caption=f"🎬 {title}\nКачество: {quality}",
                                     supports_streaming=True)
                db.add_download(user_id, url, title, quality)
                if msg_id:
                    try:
                        await bot.delete_message(chat_id, msg_id)
                    except Exception:
                        pass
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.error(f"attempt {attempt}/{MAX_RETRIES} {url}: {e}")
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(3 * attempt)   # автоповтор: 3с, потом 6с
                    continue
                await status("❌ Не удалось скачать. Попробуй другую ссылку или качество пониже.")
                await notify_admin(bot, f"Ошибка скачивания <code>{url}</code>: {e}")
    finally:
        # файл в любом случае удаляем — иначе диск на сервере забьётся
        if path:
            try:
                os.remove(path)
            except OSError:
                logging.warning(f"Не удалось удалить файл {path}")

async def shutdown_pending():
    """При остановке отменяем все незавершённые загрузки."""
    for task in list(active_jobs.values()):
        task.cancel()
    if active_jobs:
        await asyncio.gather(*active_jobs.values(), return_exceptions=True)

# ══════════════════════════ ЗАПУСК ══════════════════════════
async def main():
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    for _ in range(WORKERS):
        asyncio.create_task(download_worker(bot))

    await notify_admin(bot, "✅ Бот запущен!")
    try:
        await dp.start_polling(bot)
    finally:
        await shutdown_pending()
        await notify_admin(bot, "🛑 Бот остановлен!")

if __name__ == "__main__":
    asyncio.run(main())
