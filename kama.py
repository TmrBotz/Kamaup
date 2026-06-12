# ======================================================
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# ======================================================

"""
🎬 kama.py — Kamababax Scraper
"""

import os
import re
import html
import time
import asyncio
import logging
import requests

from bs4 import BeautifulSoup
from urllib.parse import quote

from telegram import Bot, Update
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import ContextTypes

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# ⚙️ CONFIG
# ══════════════════════════════════════════════

SOURCE_NAME    = "Kamababax"
CHANNEL_ID     = os.environ["KAMA_CHANNEL_ID"]

BASE_URL       = os.environ.get(
    "KAMA_URL",
    "https://www.kamababax.com/"
)

CHECK_INTERVAL = int(
    os.environ.get("KAMA_INTERVAL", "300")
)

DB_COLLECTION  = "kama_seen"

# ── GPlinks ─────────────────────────────────

GPLINKS_API_KEY = os.environ.get(
    "GPLINKS_API_KEY",
    "348b12e457524c0c12090532e8581c045e2902e5"
)

GPLINKS_ENABLED = (
    os.environ.get("GPLINKS_ENABLED", "False").lower()
    == "true"
)

# ══════════════════════════════════════════════
# 🔌 INIT
# ══════════════════════════════════════════════

_col = None


def init(db):
    global _col

    _col = db[DB_COLLECTION]

    log.info(
        f"[{SOURCE_NAME}] MongoDB collection ready: "
        f"{DB_COLLECTION}"
    )

# ══════════════════════════════════════════════
# 🌐 HTTP SESSION
# ══════════════════════════════════════════════

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,*/*;q=0.8"
    ),
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ══════════════════════════════════════════════
# 🔗 GPLINKS SHORTENER
# — Sirf deep link pe lagega (mp4 url pe nahi)
# ══════════════════════════════════════════════

def shorten_url(long_url: str) -> str:

    if not GPLINKS_ENABLED:
        return long_url

    if not GPLINKS_API_KEY:
        return long_url

    try:

        encoded_url = quote(long_url, safe="")

        api_url = (
            f"https://linkshortify.com/api"
            f"?api={GPLINKS_API_KEY}"
            f"&url={encoded_url}"
        )

        resp = SESSION.get(api_url, timeout=10)

        resp.raise_for_status()

        data = resp.json()

        if data.get("status") == "success":

            short = data.get("shortenedUrl", "").strip()

            if short:

                log.info(
                    f"[{SOURCE_NAME}] "
                    f"GPlinks ✓ {short}"
                )

                return short

    except Exception as e:

        log.warning(
            f"[{SOURCE_NAME}] "
            f"GPlinks error: {e}"
        )

    return long_url

# ══════════════════════════════════════════════
# 🍃 MONGODB
# ══════════════════════════════════════════════

def is_seen(post_id: str) -> bool:

    try:

        return (
            _col.find_one({"post_id": post_id})
            is not None
        )

    except Exception as e:

        log.error(
            f"[{SOURCE_NAME}] is_seen error: {e}"
        )

        return False


def mark_seen(post_id: str, url: str):

    try:

        _col.update_one(
            {"post_id": post_id},
            {
                "$set": {
                    "post_id": post_id,
                    "url": url,
                }
            },
            upsert=True
        )

    except Exception as e:

        log.error(
            f"[{SOURCE_NAME}] mark_seen error: {e}"
        )

# ══════════════════════════════════════════════
# 📡 HOMEPAGE SCRAPE
# ══════════════════════════════════════════════

def fetch_latest_posts() -> list:

    log.info(
        f"[{SOURCE_NAME}] Homepage scrape: "
        f"{BASE_URL}"
    )

    posts = []

    try:

        resp = SESSION.get(BASE_URL, timeout=15)

        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        seen_ids = set()

        for article in soup.find_all(
            "article",
            class_="thumb-block"
        ):

            post_id = article.get("data-post-id")

            if not post_id:
                continue

            if post_id in seen_ids:
                continue

            seen_ids.add(post_id)

            a = article.find("a", href=True)

            if not a:
                continue

            url = a["href"].strip()

            title_tag = article.find(
                "span",
                class_="title"
            )

            title = (
                title_tag.get_text(strip=True)
                if title_tag
                else url
            )

            posts.append({
                "post_id": post_id,
                "title": title,
                "url": url,
            })

    except Exception as e:

        log.error(
            f"[{SOURCE_NAME}] "
            f"Homepage fetch error: {e}"
        )

    log.info(
        f"[{SOURCE_NAME}] "
        f"{len(posts)} posts mile"
    )

    return posts

# ══════════════════════════════════════════════
# 🕷️ SCRAPE VIDEO PAGE
# ══════════════════════════════════════════════

def scrape_download_links(video_url: str) -> dict:

    log.info(
        f"[{SOURCE_NAME}] Scraping: {video_url}"
    )

    try:

        resp = SESSION.get(video_url, timeout=15)

        resp.raise_for_status()

    except requests.RequestException as e:

        log.error(
            f"[{SOURCE_NAME}] "
            f"Page fetch error: {e}"
        )

        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── DIRECT MP4 ──────────────────────

    video_meta = soup.find(
        "meta",
        itemprop="contentURL"
    )

    if not video_meta:

        log.warning(
            f"[{SOURCE_NAME}] "
            f"contentURL meta nahi mila"
        )

        return {}

    mp4_url = (
        video_meta.get("content", "")
        .strip()
    )

    if not mp4_url.startswith("http"):

        log.warning(
            f"[{SOURCE_NAME}] Invalid MP4 URL"
        )

        return {}

    # ── THUMBNAIL ───────────────────────

    thumb_meta = soup.find(
        "meta",
        itemprop="thumbnailUrl"
    )

    thumbnail = ""

    if thumb_meta:

        thumbnail = (
            thumb_meta.get("content", "")
            .strip()
        )

    # ── TITLE ───────────────────────────

    title_meta = soup.find(
        "meta",
        itemprop="name"
    )

    title = ""

    if title_meta:

        title = (
            title_meta.get("content", "")
            .strip()
        )

    # ── MP4 url pe shortener nahi lagega ─

    log.info(
        f"[{SOURCE_NAME}] ✓ MP4 extracted"
    )

    return {
        "title": title,
        "thumbnail": thumbnail,
        "mp4_url": mp4_url,       # raw url — shortener nahi
        "quality_data": [
            {
                "quality": "HD Video",
                "links": [
                    ("Direct MP4", mp4_url)
                ]
            }
        ]
    }

# ══════════════════════════════════════════════
# 🎯 PROCESS URL
# — Sirf enqueue karta hai, _send nahi karta
# ══════════════════════════════════════════════

async def process_url(
    bot: Bot,
    video_url: str,
    post: dict = None
) -> bool:

    import kama_upload

    data = scrape_download_links(video_url)

    if not data:
        return False

    if post is None:
        post = {
            "post_id": video_url,
            "title": data.get("title") or video_url,
        }

    mp4_url   = data.get("mp4_url", "")
    title     = data.get("title") or post.get("title") or "Unknown"
    thumbnail = data.get("thumbnail", "")
    post_id   = post.get("post_id", video_url)

    if not mp4_url:
        return False

    # ── Queue mein daalo — non-blocking ──
    await kama_upload.enqueue(
        post_id=post_id,
        mp4_url=mp4_url,
        title=title,
        thumbnail=thumbnail,
        ptb_bot=bot,
    )

    return True

# ══════════════════════════════════════════════
# 💬 COMMAND: /kama <url>
# ══════════════════════════════════════════════

async def cmd_kama(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not context.args:

        await update.message.reply_text(
            f"❌ URL do!\n\n"
            f"<code>/kama "
            f"https://www.kamababax.com/xxx/</code>\n\n"
            f"📢 Channel: <b>{SOURCE_NAME}</b>",
            parse_mode=ParseMode.HTML,
        )

        return

    video_url = context.args[0].strip()

    if not video_url.startswith("http"):

        await update.message.reply_text(
            "❌ Valid URL do (http/https)",
            parse_mode=ParseMode.HTML,
        )

        return

    msg = await update.message.reply_text(
        f"⏳ [{SOURCE_NAME}] Scraping...\n"
        f"🔗 <code>{video_url}</code>",
        parse_mode=ParseMode.HTML,
    )

    try:

        ok = await process_url(
            context.bot,
            video_url
        )

        text = (

            f"✅ Queue mein add ho gayi!\n"
            f"Download + upload background mein hoga.\n"
            f"🔗 <code>{video_url}</code>"

            if ok else

            f"⚠️ Links nahi mile.\n"
            f"🔗 <code>{video_url}</code>"
        )

        await msg.edit_text(
            text,
            parse_mode=ParseMode.HTML
        )

    except Exception as e:

        log.error(
            f"[{SOURCE_NAME}] cmd error: {e}",
            exc_info=True
        )

        await msg.edit_text(
            f"❌ Error:\n<code>{e}</code>",
            parse_mode=ParseMode.HTML,
        )

# ══════════════════════════════════════════════
# 🔄 AUTO LOOP
# ══════════════════════════════════════════════

async def rss_loop(bot: Bot):

    log.info(
        f"[{SOURCE_NAME}] 🔁 Scrape loop "
        f"— interval: {CHECK_INTERVAL}s "
        f"→ channel: {CHANNEL_ID}"
    )

    while True:

        try:

            posts = fetch_latest_posts()

            new_posts = [
                p
                for p in posts
                if not is_seen(p["post_id"])
            ]

            if not new_posts:

                log.info(
                    f"[{SOURCE_NAME}] "
                    f"Koi naya post nahi."
                )

            else:

                log.info(
                    f"[{SOURCE_NAME}] "
                    f"🆕 {len(new_posts)} naye posts!"
                )

                for post in new_posts:

                    log.info(
                        f"▶ {post['title']}"
                    )

                    ok = await process_url(
                        bot,
                        post["url"],
                        post
                    )

                    if ok:
                        mark_seen(
                            post["post_id"],
                            post["url"]
                        )

                    await asyncio.sleep(3)

        except Exception as e:

            log.error(
                f"[{SOURCE_NAME}] loop error: {e}",
                exc_info=True
            )

        log.info(
            f"[{SOURCE_NAME}] "
            f"⏳ {CHECK_INTERVAL}s "
            f"baad phir check...\n"
        )

        await asyncio.sleep(CHECK_INTERVAL)

# ======================================================
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# ======================================================
