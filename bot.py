import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import re
import sqlite3
import base64
import urllib.request 
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from bson.objectid import ObjectId 
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

# --- PYROBLACK IMPORTS ---
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

# --- WEB SERVER IMPORTS ---
from quart import Quart, request, render_template, redirect, url_for, jsonify, make_response
from hypercorn.config import Config
from hypercorn.asyncio import serve
from itsdangerous import URLSafeTimedSerializer

# --- CONFIGURATION ---
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    CHANNEL_ID = int(os.environ.get("CHANNEL_ID")) 
    ADMIN_ID = int(os.environ.get("ADMIN_ID"))
    
    AZURE_URL = os.environ.get("AZURE_URL")
    if not AZURE_URL: raise ValueError("Missing AZURE_URL")
    
    # Optional: URL to download cache.db from on cold start
    CACHE_DUMP_URL = os.environ.get("CACHE_DUMP_URL")
    
    PORT = int(os.environ.get("PORT", 8080))
    PUBLIC_URL = os.environ.get("PUBLIC_URL") or f"http://0.0.0.0:{PORT}"
    SECRET_KEY = os.environ.get("SECRET_KEY", "CHANGE_THIS_TO_RANDOM_STRING")

    DB_NAME = os.environ.get("DB_NAME", "novel_library")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")

except Exception as e:
    print(f"❌ CONFIG ERROR: {e}")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("hypercorn").setLevel(logging.INFO)
warnings.filterwarnings("ignore")

# --- MONGODB CONNECTION ---
try:
    azure_client = AsyncIOMotorClient(AZURE_URL)
    db = azure_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info("✅ Connected to MongoDB.")
except Exception as e:
    logger.error(f"❌ DB Connection Error: {e}")
    exit(1)

# --- SQLITE LOCAL CACHE ---
class LocalSearchDB:
    def __init__(self, db_path="cache.db"):
        self.db_path = db_path
        self.conn = None
        self.ready = False

    def init_db(self):
        """Initialize SQLite with FTS5"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS books_fts USING fts5(
                mongo_id UNINDEXED, 
                title, 
                author, 
                synopsis, 
                file_id UNINDEXED, 
                cover_exists UNINDEXED,
                tokenize='porter ascii'
            )
        """)
        self.conn.commit()

    def get_last_id(self):
        """Get the latest MongoDB Object ID stored in cache"""
        try:
            cursor = self.conn.cursor()
            # FTS5 tables don't support simple MAX() on unindexed columns fast, 
            # but usually, we insert in order.
            # However, for robustness, we just scan for the 'latest' inserted if IDs are monotonic.
            # MongoDB ObjectIDs ARE strictly increasing over time.
            
            # Since mongo_id is UNINDEXED in FTS5, this might be slow on 100k rows.
            # Optimization: Create a standard side-table for tracking sync state.
            cursor.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
            self.conn.commit()
            
            cursor.execute("SELECT value FROM meta WHERE key='last_sync_id'")
            row = cursor.fetchone()
            return row[0] if row else None
        except: return None

    def update_last_id(self, last_id):
        cursor = self.conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('last_sync_id', ?)", (str(last_id),))
        self.conn.commit()

    def add_batch(self, books_list):
        cursor = self.conn.cursor()
        data = []
        last_id = None
        
        for b in books_list:
            t = b.get('title', '') or ''
            a = b.get('author', '') or ''
            s = b.get('synopsis', '') or ''
            fid = b.get('file_id', '')
            has_cov = 1 if b.get('cover_image') else 0
            
            data.append((str(b['_id']), t, a, s, fid, has_cov))
            last_id = b['_id']
            
        cursor.executemany("INSERT INTO books_fts(mongo_id, title, author, synopsis, file_id, cover_exists) VALUES (?, ?, ?, ?, ?, ?)", data)
        self.conn.commit()
        
        if last_id:
            self.update_last_id(last_id)

    def search(self, query, page=1, limit=24):
        cursor = self.conn.cursor()
        offset = (page - 1) * limit
        clean_q = re.sub(r'[^\w\s]', '', query).strip()
        if not clean_q: return [], 0

        words = clean_q.split()
        try:
            if len(words) == 1:
                sql = """
                    SELECT *, rowid FROM books_fts 
                    WHERE title LIKE ? OR author LIKE ? OR synopsis LIKE ?
                    ORDER BY rank LIMIT ? OFFSET ?
                """
                wild = f"%{clean_q}%"
                cursor.execute(sql, (wild, wild, wild, limit, offset))
                rows = cursor.fetchall()
                
                c_sql = "SELECT count(*) FROM books_fts WHERE title LIKE ? OR author LIKE ? OR synopsis LIKE ?"
                cursor.execute(c_sql, (wild, wild, wild))
                total = cursor.fetchone()[0]
                
            else:
                fts_query = " AND ".join([f'"{w}"' for w in words])
                sql = "SELECT *, rowid FROM books_fts WHERE books_fts MATCH ? ORDER BY rank LIMIT ? OFFSET ?"
                cursor.execute(sql, (fts_query, limit, offset))
                rows = cursor.fetchall()
                cursor.execute("SELECT count(*) FROM books_fts WHERE books_fts MATCH ?", (fts_query,))
                total = cursor.fetchone()[0]

            results = []
            for r in rows:
                results.append({
                    "_id": r['mongo_id'],
                    "title": r['title'],
                    "author": r['author'],
                    "synopsis": r['synopsis'],
                    "file_id": r['file_id'],
                    "has_cover": bool(r['cover_exists'])
                })
            return results, total

        except Exception as e:
            logger.error(f"SQL Search Error: {e}")
            return [], 0

    def get_by_id(self, mongo_id):
        cursor = self.conn.cursor()
        # FTS queries on non-indexed columns are slow, but getting by ID is rare (only on download)
        # For better performance, query MongoDB for single item details
        return None 

local_db = LocalSearchDB()

# --- WEB APP INIT ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)

# --- GLOBAL STATE ---
indexing_active = False
BOT_USERNAME = None

# --- CACHE MANAGEMENT ---
def check_and_download_cache():
    """Checks if cache.db exists. If not, tries to download it."""
    if os.path.exists("cache.db"):
        logger.info("📂 Found local cache.db")
        return

    if CACHE_DUMP_URL:
        logger.info(f"⬇️ Downloading cache from {CACHE_DUMP_URL}...")
        try:
            urllib.request.urlretrieve(CACHE_DUMP_URL, "cache.db")
            logger.info("✅ Download complete.")
        except Exception as e:
            logger.error(f"❌ Failed to download cache: {e}")
    else:
        logger.info("⚠️ No CACHE_DUMP_URL provided. Starting with empty cache.")

async def sync_mongo_to_sqlite():
    """Smart Sync: Only fetches what is missing"""
    logger.info("🔄 Initializing Cache...")
    local_db.init_db()
    
    last_id_str = local_db.get_last_id()
    query = {}
    
    if last_id_str:
        try:
            query = {"_id": {"$gt": ObjectId(last_id_str)}}
            logger.info(f"🔄 Resuming sync from ID: {last_id_str}")
        except:
            logger.warning("⚠️ Invalid last ID, resyncing all.")
    
    count = 0
    batch = []
    
    # We only fetch text fields + cover boolean. NO binary data.
    projection = {"title": 1, "author": 1, "synopsis": 1, "file_id": 1, "cover_image": {"$slice": 1}}
    
    cursor = collection.find(query, projection).sort("_id", 1) # Sort by ID ascending is crucial
    
    async for doc in cursor:
        batch.append(doc)
        if len(batch) >= 1000:
            local_db.add_batch(batch)
            count += len(batch)
            batch = []
            if count % 5000 == 0: logger.info(f"📥 Synced +{count} books...")
    
    if batch:
        local_db.add_batch(batch)
        count += len(batch)
        
    local_db.ready = True
    logger.info(f"✅ Sync Complete. Added {count} new books.")

# --- HELPERS ---
def get_user_from_cookie():
    token = request.cookies.get('auth_token')
    if not token: return None
    try:
        return serializer.loads(token, max_age=86400*30)
    except:
        return None

def get_display_title(book_doc):
    db_title = book_doc.get('title')
    if db_title and db_title.strip() and db_title != "Unknown Title":
        return db_title.strip()
    fname = book_doc.get('file_name')
    if fname:
        return fname.replace('.epub', '').replace('_', ' ').replace('-', ' ').strip()
    return "Unknown Book"

# --- WEB ROUTES ---
@web_app.route('/health')
async def health():
    return "OK", 200

@web_app.route('/login')
async def login():
    token = request.args.get('token')
    if not token: return "❌ No token.", 400
    try:
        user_id = serializer.loads(token, max_age=3600)
        resp = await make_response(redirect(url_for('index')))
        resp.set_cookie('auth_token', serializer.dumps(user_id), max_age=86400*30)
        return resp
    except:
        return "❌ Invalid link.", 400

@web_app.route('/')
async def index():
    user_id = get_user_from_cookie()
    return await render_template('index.html', query="", results=[], count=0, page=1, total_pages=0, user_id=user_id, bot_username=BOT_USERNAME)

@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    try:
        if not ObjectId.is_valid(book_id): return "", 404
        book = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if book and book.get('cover_image'):
            return await make_response(book['cover_image'], 200, {'Content-Type': 'image/jpeg'})
    except: pass
    return "", 404

@web_app.route('/search')
async def search():
    user_id = get_user_from_cookie()
    raw_query = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    
    if not raw_query:
        return await render_template('index.html', query="", results=[], count=0, page=1, total_pages=0, user_id=user_id, bot_username=BOT_USERNAME)
    
    try:
        results, total_count = local_db.search(raw_query, page=page, limit=24)
        total_pages = math.ceil(total_count / 24)
        
        return await render_template(
            'index.html', 
            query=raw_query, 
            results=results, 
            count=total_count, 
            page=page, 
            total_pages=total_pages, 
            user_id=user_id, 
            bot_username=BOT_USERNAME
        )
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return await render_template('index.html', query=raw_query, results=[], count=0, page=1, total_pages=0, error="Search failed.", user_id=user_id)

@web_app.route('/api/download/<book_id>')
async def api_download(book_id):
    user_id = get_user_from_cookie()
    if not user_id: return jsonify({"status": "error", "message": "Not logged in"}), 401
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)})
        if not b: return jsonify({"status": "error", "message": "Book not found"}), 404
        
        await app.send_document(
            chat_id=int(user_id),
            document=b['file_id'],
            caption=f"📖 {get_display_title(b)}\n\n<i>Sent via Web Interface</i>",
            parse_mode=ParseMode.HTML
        )
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- BOT INIT ---
if not os.path.exists("sessions"): os.makedirs("sessions")
app = Client("sessions/novel_bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, sleep_threshold=60)

# --- BOT SEARCH ---
@app.on_message(filters.text & filters.private & ~filters.command(["start", "url", "index", "stats", "export_cache", "import"]))
async def bot_search_handler(client, message):
    q = message.text.strip()
    if len(q) < 2: return
    
    results, count = local_db.search(q, page=1, limit=8)
    
    if not results: return await message.reply("❌ No matches found.")
    
    txt = f"🔎 **Results for:** `{html.escape(q)}`\nFound: {count}\n\n"
    btns = []
    
    for b in results:
        title = b['title'][:50] if b['title'] else "Unknown"
        btns.append([InlineKeyboardButton(title, callback_data=f"v:{b['_id']}")])
        
    await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)

@app.on_callback_query()
async def callback_handler(client, cb):
    d = cb.data
    if d.startswith("v:"):
        bid = d.split(":")[1]
        b_mongo = await collection.find_one({"_id": ObjectId(bid)})
        if not b_mongo: return await cb.answer("Not found.", show_alert=True)
        
        t = (f"<blockquote><b>{html.escape(get_display_title(b_mongo))}</b>\n"
             f"<i>{html.escape(b_mongo.get('author','Unknown'))}</i></blockquote>\n\n"
             f"<blockquote expandable>{html.escape(b_mongo.get('synopsis','No synopsis.')[:1000])}</blockquote>")
        
        kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
        
        await cb.message.delete()
        if b_mongo.get('cover_image'):
            try:
                f = io.BytesIO(b_mongo['cover_image']); f.name="c.jpg"
                await client.send_photo(cb.message.chat.id, f, caption=t, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
            except:
                await client.send_message(cb.message.chat.id, t, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        else:
            await client.send_message(cb.message.chat.id, t, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif d.startswith("d:"):
        bid = d.split(":")[1]
        b = await collection.find_one({"_id": ObjectId(bid)})
        if b:
            await cb.answer("🚀 Sending...")
            await client.send_document(cb.message.chat.id, b['file_id'], caption=f"📖 {get_display_title(b)}")
        else:
            await cb.answer("Error.", show_alert=True)

# --- COMMANDS ---
@app.on_message(filters.command("start"))
async def start_handler(client, message):
    await message.reply("👋 **Library Bot**\nCached Engine (Persistent) ⚡\nSend text to search.")

@app.on_message(filters.command("url"))
async def url_cmd(client, message):
    token = serializer.dumps(message.from_user.id)
    await message.reply(f"🔗 [Login to Web Interface]({PUBLIC_URL}/login?token={token})", disable_web_page_preview=True)

@app.on_message(filters.command("export_cache") & filters.user(ADMIN_ID))
async def export_cache_cmd(client, message):
    """Sends the cache.db file to admin so it can be saved/hosted."""
    if os.path.exists("cache.db"):
        await message.reply_document("cache.db", caption="💾 **Current Search Cache**\nUpload this to a URL and set `CACHE_DUMP_URL` to skip full syncs.")
    else:
        await message.reply("❌ No cache file found.")

# ... [Indexing code omitted for brevity, same as before] ...
# Just ensure when indexing new files, you call local_db.add_batch([new_book]) to keep sync.

async def main():
    # 1. Download if missing
    await asyncio.to_thread(check_and_download_cache)
    
    # 2. Sync changes
    asyncio.create_task(sync_mongo_to_sqlite())
    
    await app.start()
    try: urllib.request.urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=True")
    except: pass
    
    global BOT_USERNAME
    me = await app.get_me()
    BOT_USERNAME = me.username
    logger.info(f"✅ Bot: @{BOT_USERNAME}")
    
    config = Config()
    config.bind = [f"0.0.0.0:{PORT}"]
    asyncio.create_task(serve(web_app, config))
    await idle()
    await app.stop()

if __name__ == '__main__':
    app.run(main())
