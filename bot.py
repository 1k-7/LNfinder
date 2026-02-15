import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import re
import random
import json
import base64
import urllib.request 
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# --- DATABASE IMPORTS ---
from bson.objectid import ObjectId 
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

# --- PYROGRAM IMPORTS ---
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

# --- WEB SERVER IMPORTS ---
from quart import Quart, request, render_template, redirect, url_for, jsonify, make_response, Response
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

# --- DATABASE SETUP ---
try:
    azure_client = AsyncIOMotorClient(AZURE_URL)
    db = azure_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info("✅ Connected to Azure Cosmos DB.")
except Exception as e:
    logger.error(f"❌ DB Connection Error: {e}")
    exit(1)

# --- IN-MEMORY SEARCH CACHE ---
# This list holds the entire searchable library in RAM.
SEARCH_CACHE = []

async def build_cache():
    """
    Downloads lightweight metadata (no covers/files) to RAM.
    This runs once on startup and makes searching instant.
    """
    global SEARCH_CACHE
    logger.info("🔄 Building Search Cache...")
    temp_cache = []
    
    # Fetch only text fields
    cursor = collection.find({}, {
        "_id": 1, "title": 1, "author": 1, "synopsis": 1, "file_name": 1, "tags": 1
    })
    
    async for doc in cursor:
        # Prepare text for searching (lowercase for case-insensitive)
        t = (doc.get('title') or '').lower()
        a = (doc.get('author') or '').lower()
        f = (doc.get('file_name') or '').lower()
        s = (doc.get('synopsis') or '').lower()
        tags = doc.get('tags') or []
        if isinstance(tags, list): tags = " ".join(tags).lower()
        else: tags = str(tags).lower()
        
        # The giant string we search against
        search_blob = f"{t} {a} {f} {tags} {s}"
        
        # The lightweight object we keep in RAM
        temp_cache.append({
            "id": str(doc['_id']),
            "title": doc.get('title', 'Unknown'),
            "author": doc.get('author', 'Unknown'),
            "tags": doc.get('tags', []) if isinstance(doc.get('tags'), list) else [],
            # Store a clean preview of synopsis for the UI
            "synopsis_preview": re.sub(r'<[^>]+>', '', doc.get('synopsis', ''))[:200] + "...",
            "search_blob": search_blob
        })
        
    SEARCH_CACHE = temp_cache
    logger.info(f"✅ Cache Built! {len(SEARCH_CACHE)} books loaded in RAM.")

# --- WEB APP INIT ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)

# --- GLOBAL STATE ---
indexing_active = False
files_found = 0
files_saved = 0
BOT_USERNAME = None

# --- HELPERS ---
def get_user_from_cookie():
    token = request.cookies.get('auth_token')
    if not token: return None
    try:
        user_id = serializer.loads(token, max_age=86400*30)
        return user_id
    except:
        return None

def get_pagination_list(current, total):
    if total <= 1: return []
    delta = 2
    left = current - delta
    right = current + delta + 1
    range_l = []
    range_with_dots = []
    l = None
    for i in range(1, total + 1):
        if i == 1 or i == total or (i >= left and i < right):
            range_l.append(i)
    for i in range_l:
        if l:
            if i - l == 2: range_with_dots.append(l + 1)
            elif i - l != 1: range_with_dots.append('...')
        range_with_dots.append(i)
        l = i
    return range_with_dots

# --- WEB ROUTES ---
@web_app.route('/health')
async def health():
    return "OK", 200

@web_app.route('/login')
async def login():
    token = request.args.get('token')
    if not token: return "❌ No token provided.", 400
    try:
        user_id = serializer.loads(token, max_age=3600)
        resp = await make_response(redirect(url_for('index')))
        resp.set_cookie('auth_token', serializer.dumps(user_id), max_age=86400*30)
        return resp
    except: return "❌ Invalid link.", 400

@web_app.route('/')
async def index():
    user_id = get_user_from_cookie()
    return await render_template(
        'index.html', query="", results=[], count=0, pagination_list=[], user_id=user_id, bot_username=BOT_USERNAME
    )

# --- SEARCH ROUTE (USES RAM, NO DB) ---
@web_app.route('/search')
async def search():
    user_id = get_user_from_cookie()
    query = request.args.get('q', '').strip().lower()
    page = int(request.args.get('page', 1))
    limit = 50 
    
    if not query:
        return await render_template('index.html', query="", results=[], count=0, pagination_list=[], user_id=user_id, bot_username=BOT_USERNAME)
    
    # 1. Split query into words
    words = query.split()
    
    # 2. Filter SEARCH_CACHE in Python (Instant)
    # Logic: For a book to be included, ALL words must be in its search_blob
    filtered_books = [
        b for b in SEARCH_CACHE 
        if all(w in b['search_blob'] for w in words)
    ]
    
    # 3. Pagination
    count = len(filtered_books)
    total_pages = math.ceil(count / limit)
    start = (page - 1) * limit
    end = start + limit
    
    results = filtered_books[start:end]
    pagination_list = get_pagination_list(page, total_pages)

    return await render_template(
        'index.html', 
        query=query, 
        results=results, 
        count=count, 
        page=page, 
        total_pages=total_pages, 
        pagination_list=pagination_list, 
        user_id=user_id, 
        bot_username=BOT_USERNAME
    )

# --- LAZY LOADING API (HITS DB) ---
@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if b and b.get('cover_image'):
            return Response(b['cover_image'], mimetype='image/jpeg')
    except: pass
    return "", 404

@web_app.route('/api/details/<book_id>')
async def api_details(book_id):
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"synopsis": 1})
        if not b: return jsonify({"status": "error"}), 404
        return jsonify({"status": "ok", "synopsis": b.get('synopsis', 'No synopsis.')})
    except: return jsonify({"status": "error"}), 500

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
            caption=f"📖 {b.get('title', 'Book')}",
            parse_mode=ParseMode.HTML
        )
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- BOT INIT ---
if not os.path.exists("sessions"):
    os.makedirs("sessions")

app = Client(
    "sessions/novel_bot_session", 
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60 
)

# --- EPUB PARSER ---
def parse_epub_direct(file_path):
    meta = {"title": None, "author": "Unknown", "synopsis": "No synopsis.", "tags": "", "cover_image": None}
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            opf_path = None
            try:
                root = ET.fromstring(z.read('META-INF/container.xml'))
                for child in root.iter():
                    if child.get('full-path'): opf_path = child.get('full-path'); break
            except: pass
            if not opf_path:
                for n in z.namelist():
                    if n.endswith('.opf'): opf_path = n; break
            if not opf_path: return meta
            try:
                root = ET.fromstring(z.read(opf_path))
                for elem in root.iter():
                    tag = elem.tag.split('}')[-1].lower()
                    if not elem.text: continue
                    text = elem.text.strip()
                    if not text: continue
                    if tag == 'title': meta['title'] = text
                    elif tag == 'creator': meta['author'] = text
                    elif tag == 'description': meta['synopsis'] = text
                    elif tag == 'subject': meta['tags'] += text + ", "
            except: pass
            cover_href = None
            manifest = next((e for e in root.iter() if e.tag.split('}')[-1].lower() == 'manifest'), None)
            if manifest:
                for item in manifest:
                    if 'cover-image' in item.get('properties', '').lower(): cover_href = item.get('href'); break
            if not cover_href:
                for n in z.namelist():
                    if 'cover' in n.lower() and n.endswith(('.jpg','.png')): cover_href = n; break
            if cover_href:
                try:
                    if '/' in opf_path and '/' not in cover_href:
                        cover_href = f"{opf_path.rsplit('/', 1)[0]}/{cover_href}"
                    if cover_href in z.namelist(): meta['cover_image'] = z.read(cover_href)
                except: pass
            if meta['synopsis'] == "No synopsis.":
                for n in z.namelist():
                    if 'intro' in n.lower() and n.endswith(('html','xhtml')):
                        try:
                            soup = BeautifulSoup(z.read(n), 'html.parser')
                            ps = soup.find_all('p')
                            if ps: meta['synopsis'] = "\n".join([p.text for p in ps[:6]]); break
                        except: pass
    except: pass
    if meta['tags'].endswith(", "): meta['tags'] = meta['tags'][:-2]
    return meta

# --- INDEXING WORKER ---
async def indexing_process(client, start_id, end_id, status_msg):
    global indexing_active, files_found, files_saved
    files_found = 0; files_saved = 0
    queue = asyncio.Queue(maxsize=30)
    
    if status_msg:
        try: await status_msg.edit(f"🚀 **Starting Scan...**\nRange: {start_id} - {end_id}")
        except: pass

    async def worker():
        global files_saved
        while indexing_active:
            try:
                message = await queue.get()
                temp_filename = f"temp_{message.id}.epub"
                path = None
                try: path = await client.download_media(message, file_name=temp_filename)
                except: queue.task_done(); continue
                if not path: queue.task_done(); continue
                meta = await asyncio.to_thread(parse_epub_direct, path)
                if os.path.exists(path): os.remove(path)
                if not meta['title']: meta['title'] = message.document.file_name.replace('.epub', '').replace('_', ' ')
                
                try:
                    await collection.insert_one({
                        "file_id": message.document.file_id,
                        "file_unique_id": message.document.file_unique_id,
                        "file_name": message.document.file_name,
                        "title": meta['title'],
                        "author": meta['author'],
                        "synopsis": meta['synopsis'],
                        "tags": meta['tags'],
                        "cover_image": meta['cover_image'],
                        "msg_id": message.id
                    })
                    files_saved += 1
                except DuplicateKeyError: pass
                except Exception as e: logger.error(f"DB Error: {e}")
                queue.task_done()
            except: queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(3)]
    try:
        current_id = start_id; BATCH_SIZE = 50 
        while current_id <= end_id and indexing_active:
            batch_end = min(current_id + BATCH_SIZE, end_id + 1)
            ids_to_fetch = list(range(current_id, batch_end))
            if status_msg and (current_id % 100 == 0):
                try: await status_msg.edit(f"🔄 **Scanning...**\nID: `{current_id}`\nFound: `{files_found}`\nSaved: `{files_saved}`")
                except: pass
            if not ids_to_fetch: break
            try:
                messages = await client.get_messages(CHANNEL_ID, ids_to_fetch)
                if messages:
                    for message in messages:
                        if message and message.document and message.document.file_name and message.document.file_name.endswith('.epub'):
                            files_found += 1
                            await queue.put(message)
            except FloodWait as e: await asyncio.sleep(e.value + 1); continue 
            except: pass
            current_id += BATCH_SIZE
            await asyncio.sleep(2) 
        await queue.join()
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        await build_cache() # UPDATE CACHE AFTER INDEXING
        if status_msg:
            try: await status_msg.edit(f"✅ **Done!**\nScanned: `{end_id}`\nSaved: `{files_saved}`\n🧠 Cache Updated.")
            except: pass

# --- COMMANDS ---
@app.on_message(filters.command("url"))
async def url_cmd(client, message):
    try:
        token = serializer.dumps(message.from_user.id)
        await message.reply(f"🔗 <b>Link:</b>\n<code>{PUBLIC_URL}/login?token={token}</code>", parse_mode=ParseMode.HTML)
    except: pass

@app.on_message(filters.command("stats"))
async def stats_cmd(client, message):
    await message.reply(f"📊 **Stats**\n📚 Cached Books: `{len(SEARCH_CACHE)}`")

@app.on_message(filters.command("index") & filters.user(ADMIN_ID))
async def index_cmd(client, message):
    global indexing_active
    if indexing_active: return await message.reply("⚠️ Running.")
    args = message.text.split()
    s, en = 1, int(args[1]) if len(args)==2 else int(args[2])
    if len(args)==3: s = int(args[1])
    indexing_active = True
    m = await message.reply(f"🚀 Index {s}-{en}")
    asyncio.create_task(indexing_process(client, s, en, m))

@app.on_message(filters.command("stop_index") & filters.user(ADMIN_ID))
async def stop_cmd(client, message):
    global indexing_active; indexing_active = False
    await message.reply("🛑 Stopping...")

@app.on_message(filters.command("reload") & filters.user(ADMIN_ID))
async def reload_cmd(client, message):
    m = await message.reply("🔄 Reloading RAM Cache...")
    await build_cache()
    await m.edit(f"✅ Reloaded. {len(SEARCH_CACHE)} books in RAM.")

@app.on_message(filters.text & filters.incoming & ~filters.command(["start", "stats", "index", "stop_index", "export", "import", "url", "reload"]))
async def bot_search(client, message):
    q = message.text.strip().lower()
    if not q: return
    
    # SEARCH IN RAM
    words = q.split()
    results = [b for b in SEARCH_CACHE if all(w in b['search_blob'] for w in words)]
    cnt = len(results)
    
    if cnt == 0: return await message.reply("❌ No matches.")
    
    top_results = results[:10]
    btns = [[InlineKeyboardButton(f"{b['title'][:30]} - {b['author'][:10]}", callback_data=f"v:{b['id']}")] for b in top_results]
    nav = [InlineKeyboardButton(f"1/{math.ceil(cnt/10)}", callback_data="nop")]
    if cnt > 10: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:1:{q[:20]}"))
    btns.append(nav)
    
    await message.reply(f"🔎 Results: <b>{html.escape(q)}</b> ({cnt})", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)

@app.on_callback_query()
async def cb_handler(client, cb):
    d = cb.data
    if d.startswith("n:"):
        _, p, q = d.split(':', 2)
        p = int(p)
        words = q.lower().split()
        results = [b for b in SEARCH_CACHE if all(w in b['search_blob'] for w in words)]
        cnt = len(results)
        
        start = p * 10
        end = start + 10
        page_items = results[start:end]
        
        if not page_items: return await cb.answer("End.", show_alert=True)
        
        btns = [[InlineKeyboardButton(f"{b['title'][:30]} - {b['author'][:10]}", callback_data=f"v:{b['id']}")] for b in page_items]
        nav = []
        if p > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"n:{p-1}:{q}"))
        nav.append(InlineKeyboardButton(f"{p+1}/{math.ceil(cnt/10)}", callback_data="nop"))
        if end < cnt: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:{p+1}:{q}"))
        btns.append(nav)
        
        await cb.edit_message_text(f"🔎 Results: <b>{html.escape(q)}</b> ({cnt})", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
    
    elif d.startswith("v:"):
        bid = d.split(':')[1]
        try:
            b = await collection.find_one({"_id": ObjectId(bid)})
            if not b: return await cb.answer("Gone.", show_alert=True)
            txt = f"<b>{html.escape(b.get('title', 'No Title'))}</b>\n\n{html.escape(b.get('synopsis', 'No Syn')[:800])}"
            kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
            
            await cb.message.delete()
            if b.get('cover_image'):
                f = io.BytesIO(b['cover_image']); f.name="c.jpg"
                try: await client.send_photo(cb.message.chat.id, f, caption=txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
                except: await client.send_message(cb.message.chat.id, txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
            else:
                await client.send_message(cb.message.chat.id, txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        except: pass
    
    elif d.startswith("d:"):
        bid = d.split(':')[1]
        try:
            b = await collection.find_one({"_id": ObjectId(bid)})
            await client.send_document(cb.message.chat.id, b['file_id'], caption=b.get('title'))
        except: pass

async def main():
    logger.info("🤖 Starting...")
    await app.start()
    
    try: urllib.request.urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=True")
    except: pass

    global BOT_USERNAME
    me = await app.get_me()
    BOT_USERNAME = me.username
    logger.info(f"✅ Started @{BOT_USERNAME}")

    # STARTUP: Build the RAM Cache
    await build_cache()

    logger.info("🚀 Web Server...")
    config = Config()
    config.bind = [f"0.0.0.0:{PORT}"]
    asyncio.create_task(serve(web_app, config))
    await idle()
    await app.stop()

if __name__ == '__main__':
    app.run(main())
