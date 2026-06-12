# ======================================================
# 🔥 Fixed & Rewritten by Professional Developer
# Original: @TMR_Supportt_bot | Tmr_Developer
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

🔧 FIXES:
  - PEER_ID_INVALID: proper peer resolution
  - asyncio.get_event_loop() deprecated → asyncio.get_running_loop()
  - MongoDB motor (async) driver use karo
  - Session path persistent (crash pe nahi delete hoti)
  - copy_message proper error handling
  - FloodWait retry logic fix
  - Worker shutdown graceful
"""

import os
import re
import asyncio
import logging
import tempfile
import time
import html
from typing import Optional

import aiohttp
from motor.motor_asyncio import AsyncIOMotorCollection  # pip install motor

from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    RPCError,
    PeerIdInvalid,
    ChatAdminRequired,
    UserNotParticipant,
)

from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.error import TelegramError

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# ⚙️ CONFIG — env se lo
# ══════════════════════════════════════════════

SOURCE_NAME = "Kamababax"

API_ID    = int(os.environ["TELEGRAM_API_ID"])
API_HASH  = os.environ["TELEGRAM_API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

# Private upload channel — numeric ID chahiye: -100xxxxxxxxxx
UPLOAD_CHANNEL_ID  = os.environ["KAMA_UPLOAD_CHANNEL_ID"]
POSTING_CHANNEL_ID = os.environ["KAMA_CHANNEL_ID"]
BOT_USERNAME       = os.environ["BOT_USERNAME"]   # without @

QUEUE_MAX_SIZE   = int(os.environ.get("KAMA_QUEUE_MAX", "50"))
DOWNLOAD_WORKERS = int(os.environ.get("KAMA_DOWNLOAD_WORKERS", "2"))

DOWNLOAD_RETRIES = 3
UPLOAD_RETRIES   = 3

GPLINKS_ENABLED = (
    os.environ.get("GPLINKS_ENABLED", "False").lower() == "true"
)

VIDEO_FOOTER = os.environ.get(
    "KAMA_VIDEO_FOOTER",
    "\n\n📢 <b>@{bot_username}</b>",
)

# Session file path — /tmp use mat karo (restart pe delete hoti hai)
# Persistent path use karo
SESSION_PATH = os.environ.get("KAMA_SESSION_PATH", "./kama_uploader")

# ══════════════════════════════════════════════
# 🔌 GLOBALS
# ══════════════════════════════════════════════

_col: Optional[AsyncIOMotorCollection] = None
_pyro_client: Optional[Client]         = None
_job_queue: Optional[asyncio.Queue]    = None
_active_jobs: set                      = set()
_workers_started: bool                 = False
_UPLOAD_PEER: Optional[int]            = None   # resolved integer chat_id


# ══════════════════════════════════════════════
# 🔌 INIT
# ══════════════════════════════════════════════

def init(db):
    """
    main.py se call karo:
        import kama_upload
        kama_upload.init(db)

    db = motor AsyncIOMotorDatabase instance hona chahiye
    """
    global _col
    _col = db["kama_videos"]
    log.info(f"[{SOURCE_NAME}|Upload] MongoDB collection ready: kama_videos")


# ══════════════════════════════════════════════
# 🚀 START WORKERS
# ══════════════════════════════════════════════

async def start_workers(ptb_bot: Bot):
    """
    Application startup pe ek baar call karo.
    Pyrogram client start karta hai aur background workers launch karta hai.
    """
    global _pyro_client, _job_queue, _workers_started, _UPLOAD_PEER

    if _workers_started:
        log.warning(f"[{SOURCE_NAME}|Upload] Workers already started, skipping.")
        return

    # ── Pyrogram Client Init ──────────────────
    # FIX: session path persistent hona chahiye
    # Bot mode mein: api_id + api_hash + bot_token
    _pyro_client = Client(
        name=SESSION_PATH,
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
    )

    await _pyro_client.start()
    log.info(f"[{SOURCE_NAME}|Upload] ✅ Pyrogram client started")

    # ── Peer Resolution — PEER_ID_INVALID Fix ──
    # Problem: Bot fresh start pe private channel ka peer cache empty hota hai
    # Pyrogram integer chat_id se directly kaam nahi karta jab tak
    # peer ek baar seen na ho.
    #
    # CORRECT FIX: join_chat ya get_chat se peer resolve karo.
    # Bot pehle se channel mein admin hona chahiye.
    #
    # Method: send_message → delete (warmup) approach RISKY hai agar
    # channel mein message permission nahi. Isliye get_chat() hi use karo
    # aur agar PeerIdInvalid aaye to channel ID check karo.

    raw_id = _parse_channel_id(UPLOAD_CHANNEL_ID)
    log.info(f"[{SOURCE_NAME}|Upload] Resolving upload channel: {raw_id}")

    for attempt in range(1, 4):
        try:
            # get_chat peer ko resolve karta hai aur session mein cache karta hai
            chat = await _pyro_client.get_chat(raw_id)
            _UPLOAD_PEER = chat.id

            log.info(
                f"[{SOURCE_NAME}|Upload] ✅ Upload channel resolved: "
                f"'{chat.title}' | peer_id={_UPLOAD_PEER} | type={chat.type}"
            )
            break

        except PeerIdInvalid:
            # Bot ne channel join nahi kiya / channel ID galat hai
            # Fix: Bot ko channel mein add karo manually
            log.error(
                f"[{SOURCE_NAME}|Upload] ❌ PeerIdInvalid (attempt {attempt}): "
                f"Channel ID={raw_id}\n"
                f"SOLUTION: Bot ko channel ka admin banao phir restart karo.\n"
                f"Channel ID format: -100xxxxxxxxxx (negative number)"
            )
            if attempt == 3:
                raise RuntimeError(
                    f"PEER_ID_INVALID: Bot channel access nahi kar sakta.\n"
                    f"Channel ID: {raw_id}\n"
                    f"Fix: Bot ko private channel mein admin banao."
                )
            await asyncio.sleep(3)

        except ChatAdminRequired:
            log.error(
                f"[{SOURCE_NAME}|Upload] ❌ Bot channel admin nahi hai!\n"
                f"Fix: Bot ko KAMA_UPLOAD_CHANNEL_ID={raw_id} mein admin banao."
            )
            raise

        except Exception as e:
            log.error(
                f"[{SOURCE_NAME}|Upload] ❌ Channel resolve error (attempt {attempt}): {e}"
            )
            if attempt == 3:
                raise
            await asyncio.sleep(5 * attempt)

    # ── Queue Init ────────────────────────────
    _job_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

    # ── Workers Launch ────────────────────────
    for i in range(DOWNLOAD_WORKERS):
        asyncio.create_task(
            _worker(i + 1, ptb_bot),
            name=f"kama_upload_worker_{i + 1}",
        )

    _workers_started = True
    log.info(
        f"[{SOURCE_NAME}|Upload] 🚀 {DOWNLOAD_WORKERS} workers started"
    )


async def stop_workers():
    """Graceful shutdown."""
    global _pyro_client, _workers_started

    # Queue drain karo
    if _job_queue:
        await _job_queue.join()

    if _pyro_client and _pyro_client.is_connected:
        await _pyro_client.stop()
        log.info(f"[{SOURCE_NAME}|Upload] Pyrogram client stopped")

    _workers_started = False


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
    kama.py se call karo jab naya post mile. Non-blocking.
    """
    if not _workers_started:
        log.error(f"[{SOURCE_NAME}|Upload] Workers not started! Call start_workers() first.")
        return

    # Duplicate guard
    if post_id in _active_jobs:
        log.info(f"[{SOURCE_NAME}|Upload] Already queued: {post_id}")
        return

    # DB mein check
    if await _video_exists(post_id):
        log.info(f"[{SOURCE_NAME}|Upload] Already in DB: {post_id}")
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
            f"[{SOURCE_NAME}|Upload] 📥 Queued "
            f"({_job_queue.qsize()}/{QUEUE_MAX_SIZE}): {title[:50]}"
        )
    except asyncio.QueueFull:
        log.warning(f"[{SOURCE_NAME}|Upload] ⚠️ Queue full! Skipping: {title[:50]}")


# ══════════════════════════════════════════════
# ⚙️ WORKER
# ══════════════════════════════════════════════

async def _worker(worker_id: int, ptb_bot: Bot):
    log.info(f"[{SOURCE_NAME}|Worker-{worker_id}] ✅ Ready")

    while True:
        job = await _job_queue.get()
        post_id = job["post_id"]

        try:
            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"▶ Processing: {job['title'][:50]}"
            )
            await _process_job(job, ptb_bot, worker_id)

        except Exception as e:
            log.error(
                f"[{SOURCE_NAME}|Worker-{worker_id}] ❌ Job failed: {e}",
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

    # Step 1: Download
    tmp_path = await _download_video(mp4_url, post_id, worker_id)
    if not tmp_path:
        log.error(f"[{SOURCE_NAME}|Worker-{worker_id}] ❌ Download failed: {title[:50]}")
        return

    # Step 2: Upload
    message_id = await _upload_video(tmp_path, title, thumbnail, worker_id)

    # Temp file cleanup
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    if not message_id:
        log.error(f"[{SOURCE_NAME}|Worker-{worker_id}] ❌ Upload failed: {title[:50]}")
        return

    # Step 3: DB save
    await _save_video(post_id, message_id, title, thumbnail)

    # Step 4: Deep link
    deep_link = _make_deep_link(post_id)

    # Step 5: Shorten (optional)
    if GPLINKS_ENABLED:
        try:
            from kama import shorten_url
            loop = asyncio.get_running_loop()
            deep_link = await loop.run_in_executor(None, shorten_url, deep_link)
        except Exception as e:
            log.warning(f"[{SOURCE_NAME}|Worker-{worker_id}] Shorten failed: {e}")

    # Step 6: Post to channel
    await _post_to_channel(ptb_bot, title, thumbnail, deep_link)

    log.info(f"[{SOURCE_NAME}|Worker-{worker_id}] ✅ Complete: {title[:50]}")


# ══════════════════════════════════════════════
# ⬇️ DOWNLOAD VIDEO
# ══════════════════════════════════════════════

DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


async def _download_video(
    url: str,
    post_id: str,
    worker_id: int,
) -> Optional[str]:
    """
    Streaming download. Returns temp file path or None.
    """
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        tmp_path = None
        try:
            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"⬇️ Download attempt {attempt}/{DOWNLOAD_RETRIES}: {url[:70]}"
            )

            safe_id  = re.sub(r"[^a-zA-Z0-9_-]", "_", post_id)[:40]
            tmp      = tempfile.NamedTemporaryFile(
                suffix=".mp4",
                delete=False,
                prefix=f"kama_{safe_id}_",
            )
            tmp_path = tmp.name
            tmp.close()

            timeout = aiohttp.ClientTimeout(
                total=900,     # 15 min max (large files ke liye)
                connect=30,
                sock_read=120,
            )

            async with aiohttp.ClientSession(
                headers=DOWNLOAD_HEADERS,
                timeout=timeout,
            ) as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()

                    total      = int(resp.headers.get("Content-Length", 0))
                    downloaded = 0
                    last_log   = 0

                    with open(tmp_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(512 * 1024):
                            f.write(chunk)
                            downloaded += len(chunk)

                            if downloaded - last_log >= 10 * 1024 * 1024:
                                pct = (
                                    f"{downloaded / total * 100:.0f}%"
                                    if total
                                    else f"{downloaded // 1024 // 1024}MB"
                                )
                                log.info(
                                    f"[{SOURCE_NAME}|Worker-{worker_id}] "
                                    f"⬇️ Progress: {pct}"
                                )
                                last_log = downloaded

            file_size = os.path.getsize(tmp_path)
            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"✅ Download complete: {file_size // 1024 // 1024}MB"
            )
            return tmp_path

        except Exception as e:
            log.warning(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"Download attempt {attempt} failed: {type(e).__name__}: {e}"
            )
            if tmp_path:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            if attempt < DOWNLOAD_RETRIES:
                await asyncio.sleep(5 * attempt)

    return None


# ══════════════════════════════════════════════
# ⬆️ UPLOAD VIDEO
# ══════════════════════════════════════════════

async def _upload_video(
    file_path: str,
    title: str,
    thumbnail: str,
    worker_id: int,
) -> Optional[int]:
    """
    Pyrogram se private channel pe video upload karo.
    Returns message_id or None.
    """
    global _UPLOAD_PEER

    # _UPLOAD_PEER verify karo
    if _UPLOAD_PEER is None:
        log.warning(f"[{SOURCE_NAME}|Worker-{worker_id}] _UPLOAD_PEER None, re-resolving...")
        raw_id = _parse_channel_id(UPLOAD_CHANNEL_ID)
        try:
            chat = await _pyro_client.get_chat(raw_id)
            _UPLOAD_PEER = chat.id
            log.info(f"[{SOURCE_NAME}|Worker-{worker_id}] Re-resolved: {_UPLOAD_PEER}")
        except Exception as e:
            log.error(f"[{SOURCE_NAME}|Worker-{worker_id}] Re-resolve failed: {e}")
            return None

    # Thumbnail download
    thumb_path = await _download_thumbnail(thumbnail, worker_id)

    attempt = 0
    while attempt < UPLOAD_RETRIES:
        attempt += 1
        try:
            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"⬆️ Upload attempt {attempt}/{UPLOAD_RETRIES}: {title[:50]}"
            )

            msg = await _pyro_client.send_video(
                chat_id=_UPLOAD_PEER,
                video=file_path,
                caption=f"🎬 {title}",
                thumb=thumb_path if thumb_path else None,
                supports_streaming=True,
                # Progress callback (optional — large files ke liye helpful)
                progress=_upload_progress,
                progress_args=(worker_id, title),
            )

            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"✅ Uploaded! message_id={msg.id}"
            )

            if thumb_path:
                _cleanup_file(thumb_path)

            return msg.id

        except FloodWait as e:
            # FIX: FloodWait pe attempt count mat badhao
            wait_time = e.value + 5
            log.warning(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"⏳ FloodWait: {e.value}s — waiting {wait_time}s..."
            )
            await asyncio.sleep(wait_time)
            attempt -= 1   # retry free mein milti hai FloodWait ke baad

        except PeerIdInvalid:
            log.error(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"❌ PeerIdInvalid: channel_id={_UPLOAD_PEER}\n"
                f"Fix: Bot ko channel admin banao."
            )
            if thumb_path:
                _cleanup_file(thumb_path)
            return None

        except ChatAdminRequired:
            log.error(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"❌ Bot channel admin nahi hai! channel_id={_UPLOAD_PEER}"
            )
            if thumb_path:
                _cleanup_file(thumb_path)
            return None

        except RPCError as e:
            log.warning(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"Upload attempt {attempt} RPC error: {type(e).__name__}: {e}"
            )
            if attempt < UPLOAD_RETRIES:
                await asyncio.sleep(10 * attempt)

        except Exception as e:
            log.warning(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"Upload attempt {attempt} failed: {type(e).__name__}: {e}"
            )
            if attempt < UPLOAD_RETRIES:
                await asyncio.sleep(10 * attempt)

    if thumb_path:
        _cleanup_file(thumb_path)

    return None


async def _upload_progress(current: int, total: int, worker_id: int, title: str):
    """Upload progress callback."""
    if total:
        pct = current / total * 100
        if pct % 20 < 1:   # Har 20% pe log karo
            log.info(
                f"[{SOURCE_NAME}|Worker-{worker_id}] "
                f"⬆️ Upload: {pct:.0f}% — {title[:30]}"
            )


async def _download_thumbnail(url: str, worker_id: int) -> Optional[str]:
    """Thumbnail download karo temp file mein."""
    if not url or not url.startswith("http"):
        return None

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()

                tmp      = tempfile.NamedTemporaryFile(
                    suffix=".jpg",
                    delete=False,
                    prefix="kama_thumb_",
                )
                tmp_path = tmp.name
                tmp.close()

                with open(tmp_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        f.write(chunk)

        return tmp_path

    except Exception as e:
        log.warning(f"[{SOURCE_NAME}|Worker-{worker_id}] Thumb download failed: {e}")
        return None


# ══════════════════════════════════════════════
# 🗃️ MONGODB — ASYNC (motor)
# ══════════════════════════════════════════════

async def _video_exists(post_id: str) -> bool:
    """FIX: motor async driver use karo — blocking nahi karta event loop ko."""
    try:
        result = await _col.find_one({"post_id": post_id})
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
    """FIX: motor async driver."""
    try:
        await _col.update_one(
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
        )
        log.info(f"[{SOURCE_NAME}|Upload] 💾 Saved: {post_id} → msg {message_id}")
    except Exception as e:
        log.error(f"[{SOURCE_NAME}|Upload] _save_video error: {e}")


async def get_video_message_id(post_id: str) -> Optional[int]:
    """/start handler ke liye — post_id se message_id nikalo."""
    try:
        doc = await _col.find_one({"post_id": post_id})
        if doc:
            return doc.get("message_id")
    except Exception as e:
        log.error(f"[{SOURCE_NAME}|Upload] get_video_message_id error: {e}")
    return None


# ══════════════════════════════════════════════
# 🔗 DEEP LINK
# ══════════════════════════════════════════════

def _make_deep_link(post_id: str) -> str:
    """t.me/BotUsername?start=kama_POST_ID"""
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
    """Posting channel pe photo + caption + inline button."""
    caption = (
        f"🎬 <b>{html.escape(title)}</b>\n\n"
        f"📥 Video directly apne inbox mein paane ke liye\n"
        f"neeche button dabao 👇"
    )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            text="▶️ Watch / Download",
            url=deep_link,
        )
    ]])

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

        log.info(f"[{SOURCE_NAME}|Upload] 📢 Posted: {title[:50]}")

    except TelegramError as e:
        log.error(f"[{SOURCE_NAME}|Upload] Post to channel failed: {e}")


# ══════════════════════════════════════════════
# 👤 /start DEEP LINK HANDLER
# ══════════════════════════════════════════════

async def handle_start(update, context):
    """
    PTB handler — /start kama_POST_ID

    main.py mein register karo:
        from telegram.ext import CommandHandler
        app.add_handler(CommandHandler("start", kama_upload.handle_start))
    """
    args    = context.args
    user_id = update.effective_user.id

    # Normal /start
    if not args or not args[0].startswith("kama_"):
        await update.message.reply_text(
            "👋 <b>Welcome!</b>\n\nChannel pe jaake videos access karo.",
            parse_mode=ParseMode.HTML,
        )
        return

    post_id = args[0][len("kama_"):]

    # DB se message_id lo
    message_id = await get_video_message_id(post_id)

    if not message_id:
        await update.message.reply_text(
            "⚠️ Video abhi upload ho rahi hai, "
            "thoda wait karo aur dobara try karo! ⏳",
            parse_mode=ParseMode.HTML,
        )
        return

    wait_msg = await update.message.reply_text(
        "⏳ Video bhej raha hoon...",
        parse_mode=ParseMode.HTML,
    )

    # copy_message — forward tag nahi aata, original quality maintain hoti hai
    try:
        from_chat = _UPLOAD_PEER if _UPLOAD_PEER else _parse_channel_id(UPLOAD_CHANNEL_ID)
        footer    = VIDEO_FOOTER.format(bot_username=BOT_USERNAME)

        await _pyro_client.copy_message(
            chat_id=user_id,
            from_chat_id=from_chat,
            message_id=message_id,
            caption=f"🎬 Video{footer}",
            parse_mode="html",
        )

        await wait_msg.delete()

        log.info(
            f"[{SOURCE_NAME}|Upload] ✅ Sent to user {user_id}: msg {message_id}"
        )

    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        # Retry once
        try:
            await _pyro_client.copy_message(
                chat_id=user_id,
                from_chat_id=from_chat,
                message_id=message_id,
            )
            await wait_msg.delete()
        except Exception as retry_err:
            await wait_msg.edit_text(
                f"❌ Retry failed: <code>{html.escape(str(retry_err))}</code>",
                parse_mode=ParseMode.HTML,
            )

    except UserNotParticipant:
        await wait_msg.edit_text(
            "❌ Pehle bot start karo!\n\n"
            f"👉 @{BOT_USERNAME} pe jaao aur /start karo.",
            parse_mode=ParseMode.HTML,
        )

    except RPCError as e:
        err_msg = str(e)
        log.error(f"[{SOURCE_NAME}|Upload] copy_message Pyrogram error: {e}")

        if "PEER_ID_INVALID" in err_msg:
            await wait_msg.edit_text(
                "❌ Bot se pehle ek baar baat karo — DM mein /start bhejo.",
                parse_mode=ParseMode.HTML,
            )
        elif "USER_IS_BLOCKED" in err_msg:
            await wait_msg.edit_text(
                "❌ Tumne bot block kiya hua hai. Unblock karo phir try karo.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await wait_msg.edit_text(
                f"❌ Error: <code>{html.escape(err_msg)}</code>",
                parse_mode=ParseMode.HTML,
            )

    except TelegramError as e:
        err_str = str(e).lower()
        log.error(f"[{SOURCE_NAME}|Upload] copy_message PTB error: {e}")

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

    except Exception as e:
        log.error(f"[{SOURCE_NAME}|Upload] Unexpected error: {e}", exc_info=True)
        await wait_msg.edit_text(
            f"❌ Unexpected error: <code>{html.escape(str(e))}</code>",
            parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════
# 🛠️ HELPERS
# ══════════════════════════════════════════════

def _parse_channel_id(raw: str) -> int:
    """
    Channel ID ko safely integer mein convert karo.
    Supports: '-100xxxxxxxxxx', '100xxxxxxxxxx', '@channel'

    Private channel ke liye numeric ID chahiye:
    → Telegram pe jaao → Channel Info → Copy ID
    → Ya @userinfobot se lo
    """
    raw = raw.strip()

    if raw.startswith("@"):
        # Username — Pyrogram resolve kar dega
        # Lekin private channel ke liye numeric ID better hai
        return raw  # type: ignore

    try:
        val = int(raw)
        # Agar positive ho aur 100 se start kare — make it -100xxxx
        if val > 0:
            return int(f"-100{val}")
        return val
    except ValueError:
        raise ValueError(
            f"Invalid UPLOAD_CHANNEL_ID: '{raw}'\n"
            f"Format: -100xxxxxxxxxx (negative integer)"
        )


def _cleanup_file(path: str):
    """Safe file delete."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# ══════════════════════════════════════════════
# 📋 REQUIREMENTS
# ══════════════════════════════════════════════
#
# pip install pyrogram tgcrypto python-telegram-bot aiohttp motor pymongo
#
# Environment Variables:
#   TELEGRAM_API_ID          → my.telegram.org se
#   TELEGRAM_API_HASH        → my.telegram.org se
#   BOT_TOKEN                → @BotFather se
#   KAMA_UPLOAD_CHANNEL_ID   → -100xxxxxxxxxx (private channel, bot must be admin)
#   KAMA_CHANNEL_ID          → posting channel ID or @username
#   BOT_USERNAME             → bot ka username (without @)
#   KAMA_SESSION_PATH        → (optional) session file path, default: ./kama_uploader
#   KAMA_QUEUE_MAX           → (optional) default: 50
#   KAMA_DOWNLOAD_WORKERS    → (optional) default: 2
#   GPLINKS_ENABLED          → (optional) true/false
#   KAMA_VIDEO_FOOTER        → (optional) video caption footer
#
# ══════════════════════════════════════════════
# 🔧 COMMON ERRORS & FIXES
# ══════════════════════════════════════════════
#
# PEER_ID_INVALID:
#   → Bot ko private channel mein ADMIN banao
#   → KAMA_UPLOAD_CHANNEL_ID format check karo: -100xxxxxxxxxx
#   → Session file delete karo aur restart karo
#
# ChatAdminRequired:
#   → Bot ke paas "Post Messages" permission nahi
#   → Channel Settings → Administrators → Bot → Enable permissions
#
# USER_IS_BLOCKED:
#   → User ne bot block kiya hai — unblock karne ko bolo
#
# FloodWait:
#   → Bahut zyada requests — DOWNLOAD_WORKERS kam karo
#   → Bot account pe flood limit hit ho rahi hai
#
# Motor not found:
#   → pip install motor
#   → main.py mein: from motor.motor_asyncio import AsyncIOMotorClient
#   →   client = AsyncIOMotorClient(MONGO_URI)
#   →   db = client["your_db"]
#   →   kama_upload.init(db)
#
# ======================================================
# Fixed by Professional Developer
# Original: @TMR_Supportt_bot | Tmr_Developer
# ======================================================
