import asyncio
import math
import os
import logging
import shutil
import base64
import urllib.request 
from bson.objectid import ObjectId 
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from quart import Quart, request, render_template, redirect, url_for, jsonify, make_response, Response
from hypercorn.config import Config
from hypercorn.asyncio import serve
from itsdangerous import URLSafeTimedSerializer
import xml.etree.ElementTree as ET
import zipfile
import re
import html

# --- CONFIGURATION ---
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    CHANNEL_ID = int(os.environ.get("CHANNEL_ID")) 
    ADMIN_ID = int(os.environ.get("ADMIN_ID"))
    
    AZURE_URL = os.environ.get("AZURE_URL")
    if not AZURE_URL: raise ValueError("Missing AZURE_URL")
    
    PORT = int(os.environ.get("PORT", 8080))
    PUBLIC_URL = (os.environ.get("PUBLIC_URL") or f"http://0.0.0.0:{PORT}").rstrip('/')
    SECRET_KEY = os.environ.get("SECRET_KEY", "CHANGE_THIS_TO_RANDOM_STRING")

    DB_NAME = os.environ.get("DB_NAME", "novel_library")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")

except Exception as e:
    print(f"❌ CONFIG ERROR: {e}")
    exit(1)

# --- DEBUG LOGGING (CRITICAL) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Enable Pyrogram Logs to see if Telegram is sending updates
logging.getLogger("pyrogram").setLevel(logging.INFO)
logging.getLogger("hypercorn").setLevel(logging.INFO)

# --- DATABASE ---
try:
    azure_client = AsyncIOMotorClient(AZURE_URL)
    db = azure_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info("✅ Database Connected")
except Exception as e:
    logger.error(f"❌ DB Connection Error: {e}")
    exit(1)

# --- BACKGROUND OPTIMIZER ---
async def ensure_indexes():
    """Fixes database silently in the background."""
    await asyncio.sleep(5) 
    try:
        idxs = await collection.index_information()
        # Kill the old blocking index
        if "$**_text" in idxs: 
            logger.info("🗑️ Deleting old wildcard index...")
            await collection.drop_index("$**_text")
        
        # Create the new fast index
        if "TextIndex" not in idxs:
            logger.info("🛠 Creating Text Index...")
            await collection.create_index(
                [("title", "text"), ("synopsis", "text")], 
                name="TextIndex", weights={"title": 10, "synopsis": 1}, 
                background=True
            )
        
        await collection.create_index("file_unique_id", unique=True, background=True)
        logger.info("✅ Database Indexes Verified.")
    except Exception as e:
        logger.error(f"⚠️ Index Check Failed: {e}")

# --- WEB APP ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)

# --- BOT CLEAN STARTUP ---
# Nuke session folder to prevent stale connection bugs
if os.path.exists("sessions"):
    shutil.rmtree("sessions")
os.makedirs("sessions")

app = Client("sessions/novel_bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- RESCUE ROUTE ---
@web_app.route('/fix_bot')
async def fix_bot():
    """Force-clears webhook via browser if bot is stuck"""
    try:
        await app.delete_webhook()
        return "<h1>✅ Webhook Nuke Sent. Restarting polling...</h1>"
    except Exception as e:
        return f"<h1>❌ Error: {e}</h1>"

@web_app.route('/health')
async def health(): return "OK", 200

@web_app.route('/')
async def index():
    user_id = get_user_from_cookie()
    return await render_template('index.html', query="", results=[], count=0, pagination_list=[], user_id=user_id, bot_username=(await app.get_me()).username)

# --- COMMANDS ---
@app.on_message(filters.command("ping"))
async def ping_cmd(client, message):
    logger.info(f"📨 PING received from {message.from_user.id}")
    await message.reply("🏓 **Pong!** I am online.")

@app.on_message(filters.command("fix_search") & filters.user(ADMIN_ID))
async def fix_search_cmd(client, message):
    s = await message.reply("🛠 **Running Maintenance...**")
    asyncio.create_task(ensure_indexes())
    await s.edit("✅ Optimization started in background.")

# --- SEARCH LOGIC ---
@app.on_message(filters.text & filters.incoming & ~filters.command(["start", "ping", "stats", "index", "stop_index", "url", "export", "import", "fix_search"]))
async def bot_search(client, message):
    q = message.text.strip()
    if not q: return
    
    words = q.split()
    search_terms = " ".join([f'"{w}"' for w in words])
    mongo_query = {"$text": {"$search": search_terms}}
    
    try:
        # 5s Timeout prevents bot freeze
        cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)
        
        if cnt == 0:
            # Fallback Regex
            and_cond = [{"$or": [{"title": re.compile(re.escape(w), re.I)}, {"synopsis": re.compile(re.escape(w), re.I)}]} for w in words]
            mongo_query = {"$and": and_cond}
            cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)

        if cnt == 0: return await message.reply("❌ No matches.")
        
        cursor = collection.find(mongo_query, {"title": 1, "author": 1})
        res = await cursor.limit(8).to_list(length=8)
        
        btns = []
        for b in res:
            title = b.get('title') or "Unknown"
            title = re.sub(r'\s+(c|ch|chap|vol|v)\.?\s*\d+(?:[-–]\d+)?.*$', '', title, flags=re.IGNORECASE).strip()
            btns.append([InlineKeyboardButton(title[:40], callback_data=f"v:{str(b['_id'])}")])
        
        await message.reply(f"🔎 Results: <b>{html.escape(q)}</b> ({cnt})", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Search Error: {e}")
        await message.reply("⚠️ Search error (Database Busy).")

# --- CALLBACKS ---
@app.on_callback_query()
async def cb_handler(client, cb):
    d = cb.data
    if d.startswith("v:"):
        bid = d.split(':')[1]
        b = await collection.find_one({"_id": ObjectId(bid)})
        if not b: return await cb.answer("Not found", show_alert=True)
        
        title = html.escape(b.get('title','?'))
        author = html.escape(b.get('author','?'))
        syn = html.escape(b.get('synopsis','No Text').strip())
        
        # RESTORED UI
        caption = f"<blockquote><b>{title}</b>\n👤 {author}</blockquote>\n\n<blockquote expandable>{syn}</blockquote>"
        kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
        
        await cb.message.delete()
        if b.get('cover_image'):
            try:
                f = io.BytesIO(b['cover_image']); f.name="c.jpg"
                await client.send_photo(cb.message.chat.id, f, caption=caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
            except: await client.send_message(cb.message.chat.id, caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        else:
            await client.send_message(cb.message.chat.id, caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
    
    elif d.startswith("d:"):
        bid = d.split(':')[1]
        b = await collection.find_one({"_id": ObjectId(bid)})
        await client.send_document(cb.message.chat.id, b['file_id'], caption=f"📖 {b.get('title')}")

# --- HELPERS & ROUTES ---
def get_user_from_cookie():
    try: return serializer.loads(request.cookies.get('auth_token'), max_age=86400*30)
    except: return None

@web_app.route('/api/download/<book_id>')
async def api_download(book_id):
    user_id = get_user_from_cookie()
    if not user_id: return jsonify({"status": "error"}), 401
    b = await collection.find_one({"_id": ObjectId(book_id)})
    await app.send_document(int(user_id), b['file_id'], caption=f"📖 {b.get('title')}")
    return jsonify({"status": "ok"})

@web_app.route('/api/details/<book_id>')
async def api_details(book_id):
    b = await collection.find_one({"_id": ObjectId(book_id)}, {"synopsis": 1})
    return jsonify({"status": "ok", "synopsis": b.get('synopsis', 'No synopsis.')})

@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if b and b.get('cover_image'): return Response(b['cover_image'], mimetype='image/jpeg')
    except: pass
    return Response(base64.b64decode('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7'), mimetype='image/gif')

# --- STARTUP SEQUENCE ---
async def main():
    logger.info("🤖 Starting...")
    await app.start()
    
    # 1. FORCE CLEAR WEBHOOK
    try: await app.delete_webhook()
    except: pass
    
    me = await app.get_me()
    logger.info(f"✅ Bot Started: @{me.username}")
    
    # 2. START INDEXER IN BACKGROUND (Don't block!)
    asyncio.create_task(ensure_indexes())
    
    # 3. START WEB SERVER
    config = Config(); config.bind = [f"0.0.0.0:{PORT}"]
    asyncio.create_task(serve(web_app, config))
    
    # 4. START POLLING
    await idle()
    await app.stop()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
