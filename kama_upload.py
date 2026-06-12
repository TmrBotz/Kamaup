# ======================================================
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# ======================================================

"""
📦 kama_upload.py — Download → Upload → Post → Deep Link

Flow:
  MP4 URL
    → asyncio Queue (non-blocking)
      → Pyrogram download (streaming)
        → Upload Channel upload
          → MongoDB save (message_id)
            → Deep link bana (+ optional shorten)
              → Posting Channel pe photo + inline button
"""

import os
import re
import asyncio
import logging
import tempfile
import time
import html

import aiohttp

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.error import TelegramError

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# ⚙️ CONFIG
# ══════════════════════════════════════════════

SOURCE_NAME = "Kamababax"

# MTProto credentials — my.telegram.org se lena
API_ID   = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

# Channels
UPLOAD_CHANNEL_ID  = os.environ["KAMA_UPLOAD_CHANNEL_ID"]   # private upload channel
POSTING_CHANNEL_ID = os.environ["KAMA_CHANNEL_ID"]           # posting channel (same as kama.py)

# Bot username — deep link ke liye
BOT_USERNAME = os.environ["BOT_USERNAME"]   # without @, e.g. "MyBot"

# Queue limits
QUEUE_MAX_SIZE     = int(os.environ.get("KAMA_QUEUE_MAX", "50"))
DOWNLOAD_WORKERS   = int(os.environ.get("KAMA_DOWNLOAD_WORKERS", "2"))

# Retry config
DOWNLOAD_RETRIES   = 3
UPLOAD_RETRIES     = 2

# Shortener (reuse from kama.py)
GPLINKS_ENABLED    = (
    os.environ.get("GPLINKS_ENABLED", "False").lower() == "true"
)

# Footer for forwarded videos
VIDEO_FOOTER = os.environ.get(
    "KAMA_VIDEO_FOOTER",
    "\n\n📢 <b>@{bot_username}</b>"
)

# ══════════════════════════════════════════════
# 🔌 GLOBALS
# ══════════════════════════════════════════════

_col          = None          # MongoDB collection: kama_videos
_pyro_client  = None          # Pyrogram client (singleton)
_job_queue    = None          # asyncio.Queue
_active_jobs  = set()         # post_id set — duplicate guard
_workers_started = False

# ══════════════════════════════════════════════
# 🔌 INIT
# ══════════════════════════════════════════════

def init(db):
    """
    kama.py ki tarah call karo main.py se:
        import kama_upload
        kama_upload.init(db)
    """
    global _col
    _col = db["kama_videos"]
    log.info(f"[{SOURCE_NAME}|Upload] MongoDB collection ready: kama_videos")


async def start_workers(ptb_bot: Bot):
    """
    Background worker tasks start karo.
    Application startup pe ek baar call karo.
    """
    global _pyro_client, _job_queue, _workers_started

    if _workers_started:
        return

    # ── Pyrogram client init ──────────────────
    _pyro_client = Client(
        name="kama_uploader",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,          # session file nahi banegi
    )
    await _pyro_client.start()
    log.info(f"[{SOURCE_NAME}|Upload] ✅ Pyrogram client started")

    # ── Channel resolve karo — PEER_ID_INVALID fix ──
    try:
        chat = await _pyro_client.get_chat(int(UPLOAD_CHANNEL_ID))
        log.info(
            f"[{SOURCE_NAME}|Upload] "
            f"✅ Upload channel resolved: {chat.title}"
        )
    except Exception as e:
        log.error(
            f"[{SOURCE_NAME}|Upload] "
            f"❌ Upload channel resolve failed: {e}"
        )
        raise

    # ── Queue init ────────────────────────────
    _job_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

    # ── Workers launch ────────────────────────
    for i in range(DOWNLOAD_WORKERS):
        asyncio.create_task(
            _worker(i + 1, ptb_bot),
            name=f"kama_upload_worker_{i+1}"
        )

    _workers_started = True
    log.info(
        f"[{SOURCE_NAME}|Upload] "
        f"🚀 {DOWNLOAD_WORKERS} workers started"
    )


async def stop_workers():
    """Graceful shutdown — app stop hone pe call karo."""
    global _pyro_client

    if _pyro_client and _pyro_client.is_connected:
        await _pyro_client.stop()
        log.info(f"[{SOURCE_NAME}|Upload] Pyrogram client stopped")

# ══════════════════════════════════════════════
# 📥 ENQUEUE JOB
# ══════════════════════════════════════════════

async def enqueue(
    post_id: str,
    mp4_url: str,
    title: str,
    thumbnail: str,
    ptb_bot: Bot,
):
    """
    kama.py se call karo jab naya post mile.
    Non-blocking — seedha return kar deta hai.
    """

    # ── Duplicate guard ───────────────────────
    if post_id in _active_jobs:
        log.info(f"[{SOURCE_NAME}|Upload] Already queued: {post_id}")
        return

    # ── DB mein check ─────────────────────────
    if await _video_exists(post_id):
        log.info(f"[{SOURCE_NAME}|Upload] Already uploaded: {post_id}")
        return

    job = {
        "post_id":   post_id,
        "mp4_url":   mp4_url,
        "title":     title,
        "thumbnail": thumbnail,
    }

    try:
        _job_queue.put_nowait(job)
        _active_jobs.add(post_id)
        log.info(
            f"[{SOURCE_NAME}|Upload] "
            f"📥 Queued ({_job_queue.qsize()}/{QUEUE_MAX_SIZE}): "
            f"{title[:40]}"
        )
    except asyncio.QueueFull:
        log.warning(
            f"[{SOURCE_NAME}|Upload] "
            f"⚠️ Queue full! Skipping: {title[:40]}"
        )

# ══════════════════════════════════════════════
# ⚙️ WORKER
# ══════════════════════════════════════════════

async def _worker(worker_id: int, ptb_bot: Bot):
    """
    Background worker — queue se job uthata hai,
    download → upload → post karta hai.
    """
    log.info(f"[{SOURCE_NAME}|Worker-{worker_id}] Ready")

    while True:
        job = await _job_queue.get()
        post_id = job["post_id"]

        try:
            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"▶ Processing: {job['title'][:40]}"
            )

            await _process_job(job, ptb_bot, worker_id)

        except Exception as e:
            log.error(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"❌ Job failed: {e}",
                exc_info=True,
            )

        finally:
            _active_jobs.discard(post_id)
            _job_queue.task_done()

# ══════════════════════════════════════════════
# 🔄 PROCESS JOB
# ══════════════════════════════════════════════

async def _process_job(job: dict, ptb_bot: Bot, worker_id: int):

    post_id   = job["post_id"]
    mp4_url   = job["mp4_url"]
    title     = job["title"]
    thumbnail = job["thumbnail"]

    # ── Step 1: Download ──────────────────────
    tmp_path = await _download_video(mp4_url, post_id, worker_id)

    if not tmp_path:
        log.error(
            f"[{SOURCE_NAME}|Worker-{worker_id}] "
            f"Download failed, skipping: {title[:40]}"
        )
        return

    # ── Step 2: Upload ────────────────────────
    message_id = await _upload_video(
        tmp_path, title, thumbnail, worker_id
    )

    # Temp file delete karo
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    if not message_id:
        log.error(
            f"[{SOURCE_NAME}|Worker-{worker_id}] "
            f"Upload failed, skipping: {title[:40]}"
        )
        return

    # ── Step 3: DB save ───────────────────────
    await _save_video(post_id, message_id, title, thumbnail)

    # ── Step 4: Deep link ─────────────────────
    deep_link = _make_deep_link(post_id)

    # ── Step 5: Shorten (optional) ────────────
    if GPLINKS_ENABLED:
        from kama import shorten_url
        deep_link = await asyncio.get_event_loop().run_in_executor(
            None, shorten_url, deep_link
        )

    # ── Step 6: Post to posting channel ───────
    await _post_to_channel(
        ptb_bot, title, thumbnail, deep_link
    )

    log.info(
        f"[{SOURCE_NAME}|Worker-{worker_id}] "
        f"✅ Complete: {title[:40]}"
    )

# ══════════════════════════════════════════════
# ⬇️ DOWNLOAD
# ══════════════════════════════════════════════

async def _download_video(
    url: str,
    post_id: str,
    worker_id: int,
) -> str | None:
    """
    Streaming download — memory efficient.
    Returns temp file path ya None on failure.
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        tmp_path = None
        try:
            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"⬇️ Download attempt {attempt}: {url[:60]}"
            )

            safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", post_id)[:40]
            tmp = tempfile.NamedTemporaryFile(
                suffix=".mp4",
                delete=False,
                prefix=f"kama_{safe_id}_",
            )
            tmp_path = tmp.name
            tmp.close()

            timeout = aiohttp.ClientTimeout(
                total=600,        # 10 min max
                connect=30,
                sock_read=60,
            )

            async with aiohttp.ClientSession(
                headers=HEADERS,
                timeout=timeout,
            ) as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()

                    total = int(
                        resp.headers.get("Content-Length", 0)
                    )

                    downloaded = 0
                    last_log   = 0

                    with open(tmp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(
                            512 * 1024   # 512KB chunks
                        ):
                            f.write(chunk)
                            downloaded += len(chunk)

                            # Har 10MB pe log karo
                            if downloaded - last_log >= 10 * 1024 * 1024:
                                pct = (
                                    f"{downloaded/total*100:.0f}%"
                                    if total
                                    else f"{downloaded//1024//1024}MB"
                                )
                                log.info(
                                    f"[{SOURCE_NAME}|Worker-{worker_id}] "
                                    f"⬇️ {pct} downloaded"
                                )
                                last_log = downloaded

            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"✅ Download complete: "
                f"{downloaded//1024//1024}MB"
            )
            return tmp_path

        except Exception as e:
            log.warning(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"Download attempt {attempt} failed: {e}"
            )

            # Temp file cleanup
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

            if attempt < DOWNLOAD_RETRIES:
                await asyncio.sleep(5 * attempt)   # backoff

    return None

# ══════════════════════════════════════════════
# ⬆️ UPLOAD
# ══════════════════════════════════════════════

async def _upload_video(
    file_path: str,
    title: str,
    thumbnail: str,
    worker_id: int,
) -> int | None:
    """
    Pyrogram se upload channel pe video upload karo.
    Returns message_id ya None on failure.
    """

    # Thumbnail download karo (temp file)
    thumb_path = await _download_thumbnail(thumbnail, worker_id)

    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"⬆️ Upload attempt {attempt}: {title[:40]}"
            )

            msg = await _pyro_client.send_video(
                chat_id=UPLOAD_CHANNEL_ID,
                video=file_path,
                caption=f"🎬 {title}",
                thumb=thumb_path if thumb_path else None,
                supports_streaming=True,
            )

            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"✅ Uploaded! message_id: {msg.id}"
            )

            # Thumb cleanup
            if thumb_path:
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass

            return msg.id

        except FloodWait as e:
            log.warning(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"FloodWait: {e.value}s — waiting..."
            )
            await asyncio.sleep(e.value + 2)
            # FloodWait ke baad retry count consume mat karo
            continue

        except RPCError as e:
            log.warning(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"Upload attempt {attempt} RPC error: {e}"
            )

        except Exception as e:
            log.warning(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"Upload attempt {attempt} failed: {e}"
            )

        if attempt < UPLOAD_RETRIES:
            await asyncio.sleep(10 * attempt)

    # Thumb cleanup on final failure
    if thumb_path:
        try:
            os.remove(thumb_path)
        except Exception:
            pass

    return None


async def _download_thumbnail(
    url: str,
    worker_id: int,
) -> str | None:
    """Thumbnail download karo temp file mein."""

    if not url or not url.startswith("http"):
        return None

    try:
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()

                tmp = tempfile.NamedTemporaryFile(
                    suffix=".jpg",
                    delete=False,
                    prefix="kama_thumb_",
                )
                tmp_path = tmp.name
                tmp.close()

                with open(tmp_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(
                        64 * 1024
                    ):
                        f.write(chunk)

        return tmp_path

    except Exception as e:
        log.warning(
            f"[{SOURCE_NAME}|Worker-{worker_id}] "
            f"Thumb download failed: {e}"
        )
        return None

# ══════════════════════════════════════════════
# 🗃️ MONGODB
# ══════════════════════════════════════════════

async def _video_exists(post_id: str) -> bool:
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: _col.find_one({"post_id": post_id}),
        )
        return result is not None
    except Exception as e:
        log.error(f"[{SOURCE_NAME}|Upload] _video_exists error: {e}")
        return False


async def _save_video(
    post_id: str,
    message_id: int,
    title: str,
    thumbnail: str,
):
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: _col.update_one(
                {"post_id": post_id},
                {
                    "$set": {
                        "post_id":    post_id,
                        "message_id": message_id,
                        "title":      title,
                        "thumbnail":  thumbnail,
                        "created_at": int(time.time()),
                    }
                },
                upsert=True,
            ),
        )
        log.info(
            f"[{SOURCE_NAME}|Upload] "
            f"💾 Saved: {post_id} → msg {message_id}"
        )
    except Exception as e:
        log.error(f"[{SOURCE_NAME}|Upload] _save_video error: {e}")


async def get_video_message_id(post_id: str) -> int | None:
    """
    /start handler ke liye — post_id se message_id nikalo.
    """
    try:
        loop = asyncio.get_event_loop()
        doc = await loop.run_in_executor(
            None,
            lambda: _col.find_one({"post_id": post_id}),
        )
        if doc:
            return doc.get("message_id")
    except Exception as e:
        log.error(f"[{SOURCE_NAME}|Upload] get_video_message_id error: {e}")
    return None

# ══════════════════════════════════════════════
# 🔗 DEEP LINK
# ══════════════════════════════════════════════

def _make_deep_link(post_id: str) -> str:
    """
    t.me/BotUsername?start=kama_POST_ID
    """
    return f"https://t.me/{BOT_USERNAME}?start=kama_{post_id}"

# ══════════════════════════════════════════════
# 📨 POST TO CHANNEL
# ══════════════════════════════════════════════

async def _post_to_channel(
    bot: Bot,
    title: str,
    thumbnail: str,
    deep_link: str,
):
    """
    Posting channel pe photo + caption + inline button bhejo.
    """

    caption = (
        f"🎬 <b>{html.escape(title)}</b>\n\n"
        f"📥 Video directly apne inbox mein paane ke liye\n"
        f"neeche button dabao 👇"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                text="▶️ Watch / Download",
                url=deep_link,
            )
        ]
    ])

    try:
        if thumbnail and thumbnail.startswith("http"):
            await bot.send_photo(
                chat_id=POSTING_CHANNEL_ID,
                photo=thumbnail,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        else:
            await bot.send_message(
                chat_id=POSTING_CHANNEL_ID,
                text=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

        log.info(
            f"[{SOURCE_NAME}|Upload] "
            f"📢 Posted to channel: {title[:40]}"
        )

    except TelegramError as e:
        log.error(
            f"[{SOURCE_NAME}|Upload] "
            f"Post to channel failed: {e}"
        )

# ══════════════════════════════════════════════
# 👤 /start DEEP LINK HANDLER
# ══════════════════════════════════════════════

async def handle_start(update, context):
    """
    PTB handler — /start kama_POST_ID
    
    main.py mein register karo:
        app.add_handler(CommandHandler("start", kama_upload.handle_start))
    """

    args = context.args

    # ── Normal /start (no deep link) ─────────
    if not args or not args[0].startswith("kama_"):
        await update.message.reply_text(
            "👋 <b>Welcome!</b>\n\n"
            "Channel pe jaake videos access karo.",
            parse_mode=ParseMode.HTML,
        )
        return

    post_id = args[0][len("kama_"):]   # "kama_" prefix hata do
    user_id = update.effective_user.id

    # ── DB se message_id nikalo ───────────────
    message_id = await get_video_message_id(post_id)

    if not message_id:
        await update.message.reply_text(
            "⚠️ Video abhi upload ho rahi hai, "
            "thoda wait karo aur dobara try karo! ⏳",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Processing message ────────────────────
    wait_msg = await update.message.reply_text(
        "⏳ Video bhej raha hoon...",
        parse_mode=ParseMode.HTML,
    )

    # ── copy_message — no forward tag ─────────
    try:
        footer = VIDEO_FOOTER.format(bot_username=BOT_USERNAME)

        await _pyro_client.copy_message(
            chat_id=user_id,
            from_chat_id=UPLOAD_CHANNEL_ID,
            message_id=message_id,
            caption=(
                f"🎬 Video\n{footer}"
            ),
        )

        await wait_msg.delete()

        log.info(
            f"[{SOURCE_NAME}|Upload] "
            f"✅ Sent to user {user_id}: msg {message_id}"
        )

    except TelegramError as e:

        # User ne bot block kiya ho / DM band ho
        err_str = str(e).lower()

        if "blocked" in err_str or "chat not found" in err_str:
            await wait_msg.edit_text(
                "❌ Bot ko pehle start karo!\n\n"
                f"👉 @{BOT_USERNAME} pe jaao aur /start karo.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await wait_msg.edit_text(
                f"❌ Error: <code>{html.escape(str(e))}</code>",
                parse_mode=ParseMode.HTML,
            )

        log.error(
            f"[{SOURCE_NAME}|Upload] "
            f"copy_message failed for user {user_id}: {e}"
        )

    except RPCError as e:
        await wait_msg.edit_text(
            f"❌ Pyrogram error: <code>{html.escape(str(e))}</code>",
            parse_mode=ParseMode.HTML,
        )
        log.error(
            f"[{SOURCE_NAME}|Upload] "
            f"Pyrogram copy error: {e}"
        )

# ======================================================
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# ======================================================
