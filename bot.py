# ======================================================
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# ======================================================

"""
🎬  bot.py — Main Entry Point
"""

import os
import logging
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from pymongo import MongoClient
from telegram import Bot, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

import kama
import kama_upload

# ══════════════════════════════════════════════
# ⚙️  CONFIG
# ══════════════════════════════════════════════

BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
PORT      = int(os.environ.get("PORT", "8000"))

# ══════════════════════════════════════════════
# 📝  LOGGING
# ══════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# 🏥  HEALTH CHECK SERVER
# ══════════════════════════════════════════════

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    log.info(f"Health server port {PORT} pe shuru hua")
    server.serve_forever()

# ══════════════════════════════════════════════
# 💬  COMMANDS
# ══════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args and context.args[0].startswith("kama_"):
        await kama_upload.handle_start(update, context)
        return

    await update.message.reply_text(
        "👋 <b>Kamababax Bot</b>\n\n"
        "Yeh bot Kamababax site monitor karta hai aur\n"
        "naye videos automatically channel pe post karta hai.\n\n"
        "📋 <b>Commands:</b>\n"
        "• /start — Yeh message\n"
        "• /help — Detailed help\n"
        "• /status — Bot aur MongoDB ki status\n\n"
        "🕷️ <b>Manual Scrape:</b>\n"
        "• /kama &lt;url&gt; — Kama channel pe post karo",
        parse_mode=ParseMode.HTML,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):

# ======================================================
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# ======================================================

    await update.message.reply_text(
        "📖 <b>Help</b>\n\n"
        "🔸 /kama &lt;url&gt;\n"
        "   <code>/kama https://www.kamababax.com/xxx/</code>\n\n"
        "🔸 /status — Bot aur MongoDB ki status",
        parse_mode=ParseMode.HTML,
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = context.bot_data["db"]
    try:
        db.client.admin.command("ping")
        db_status  = "✅ Connected"
        kama_count = db["kama_seen"].count_documents({})
        vid_count  = db["kama_videos"].count_documents({})
    except Exception as e:
        db_status  = f"❌ {e}"
        kama_count = vid_count = "N/A"

    await update.message.reply_text(
        "📊 <b>Bot Status</b>\n\n"
        f"🤖 Bot       : ✅ Running\n"
        f"🍃 MongoDB   : {db_status}\n\n"
        f"🕷️ <b>Scraper:</b>\n"
        f"• Kama seen  : {kama_count}\n"
        f"• Videos DB  : {vid_count}\n"
        f"• Channel    : <code>{kama.CHANNEL_ID}</code>",
        parse_mode=ParseMode.HTML,
    )

# ══════════════════════════════════════════════
# 🚀  MAIN
# ══════════════════════════════════════════════

async def main():
    # 1. Health server
    threading.Thread(target=start_health_server, daemon=True).start()

    # 2. MongoDB
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command("ping")
        db = mongo_client["movie_rss_bot"]
        log.info("✅ MongoDB connected!")
    except Exception as e:
        log.error(f"❌ MongoDB failed: {e}")
        raise

    # 3. Init
    kama.init(db)
    kama_upload.init(db)

    # 4. Telegram app
    app = Application.builder().token(BOT_TOKEN).build()
    app.bot_data["db"] = db

    # 5. Commands
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("kama",   kama.cmd_kama))

    log.info("🤖 Bot shuru! RSS loop chal raha hai.")

    bot = Bot(token=BOT_TOKEN)

    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)

        await kama_upload.start_workers(bot)

        try:
            await asyncio.gather(
                kama.rss_loop(bot),
            )
        finally:
            await kama_upload.stop_workers()

        await app.updater.stop()
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())

# ======================================================
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# 🔥 Code Created by @TMR_Supportt_bot | Tmr_Developer
# ======================================================

