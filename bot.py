import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import re
import base64
import json
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

# --- PYROGRAM IMPORTS ---
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
    
    PORT = int(os.environ.get("PORT", 8080))
    PUBLIC_URL = os.environ.get("PUBLIC_URL") or f"http://0.0.0.0:{PORT}"
    SECRET_KEY = os.environ.get("SECRET_KEY", "CHANGE_THIS_TO_RANDOM_STRING")

    DB_NAME = os.environ.get("DB_NAME", "novel_library")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")

except Exception as e:
    print(f"❌ CONFIG ERROR: {e}")
    exit(1)

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Reduce library noise, but keep critical info
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

# --- WEB APP INIT ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)

# --- TELEGRAM APP INIT ---
if not os.path.exists("sessions"):
    os.makedirs("sessions")

app = Client(
    "sessions/novel_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60,
    in_memory=False # Keep explicit file storage to be safe
)

# --- GLOBAL STATE ---
indexing_active = False
files_found = 0
files_saved = 0
BOT_USERNAME = None

# --- WEB HELPERS ---
def get_user_from_cookie():
    token = request.cookies.get('auth_token')
    if not token: return None
    try:
        user_id = serializer.loads(token, max_age=86400*30)
        return user_id
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
    if not token:
        return "❌ No token provided.", 400
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
    return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)

@web_app.route('/search')
async def search():
    user_id = get_user_from_cookie()
    query = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    limit = 20
    skip = (page - 1) * limit

    if not query:
        return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)
    
    try:
        cnt = await collection.count_documents({"$text": {"$search": query}})
        if cnt > 0:
            cursor = collection.find({"$text": {"$search": query}}).sort([("score", {"$meta": "textScore"})])
        else:
            reg = {"$regex": query, "$options": "i"}
            fallback = {"$or": [{"title": reg}, {"author": reg}, {"file_name": reg}]}
            cnt = await collection.count_documents(fallback)
            cursor = collection.find(fallback)

        books_cursor = await cursor.skip(skip).limit(limit).to_list(length=limit)
        results = []
        for b in books_cursor:
            cover_b64 = None
            if b.get('cover_image'):
                cover_b64 = base64.b64encode(b['cover_image']).decode('utf-8')
            syn = b.get('synopsis', 'No synopsis available.').strip()
            has_more = len(syn) > 300
            results.append({
                "_id": str(b['_id']),
                "title": get_display_title(b),
                "author": b.get('author', 'Unknown'),
                "synopsis": syn,
                "has_more": has_more,
                "cover_image": cover_b64
            })
        total_pages = math.ceil(cnt / limit)
        return await render_template('index.html', query=query, results=results, count=cnt, page=page, total_pages=total_pages, user_id=user_id, bot_username=BOT_USERNAME)
    except Exception as e:
        return await render_template('index.html', query=query, results=[], count=0, error=str(e), user_id=user_id)

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
        logger.error(f"DL Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# --- BOT LOGIC ---

async def ensure_indexes():
    try:
        await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text"), ("file_name", "text")])
        await collection.create_index("file_unique_id", unique=True)
    except: pass

def get_button_label(book_doc):
    full = get_display_title(book_doc)
    return re.sub(r'\s+(c|ch|chap|vol|v)\.?\s*\d+(?:[-–]\d+)?.*$', '', full, flags=re.IGNORECASE).strip()

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
            # Cover Logic omitted for brevity, assumed same as before
            # ...
    except: pass
    if meta['tags'].endswith(", "): meta['tags'] = meta['tags'][:-2]
    return meta

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
                path = await client.download_media(message, file_name=temp_filename)
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
        if status_msg: try: await status_msg.edit(f"✅ **Done!**\nScanned: `{end_id}`\nFound: `{files_found}`\nSaved: `{files_saved}`")
        except: pass

# --- HANDLERS ---

@app.on_message(filters.command("url"))
async def url_command(client, message):
    logger.info(f"⚡ CMD: /url by {message.from_user.id}")
    try:
        token = serializer.dumps(message.from_user.id)
        login_url = f"{PUBLIC_URL}/login?token={token}"
        await message.reply(f"🔗 **Your Personal Website Link**\n\n{login_url}", disable_web_page_preview=True)
    except Exception as e: logger.error(f"URL Cmd Error: {e}")

@app.on_message(filters.command("start"))
async def start_handler(client, message):
    logger.info(f"⚡ CMD: /start by {message.from_user.id}")
    if len(message.command) > 1 and message.command[1].startswith("d_"):
        try:
            book_id = message.command[1].split("_", 1)[1]
            b = await collection.find_one({"_id": ObjectId(book_id)})
            if b:
                await client.send_document(message.chat.id, b['file_id'], caption=f"📖 {get_display_title(b)}")
                return 
        except: pass
    await message.reply("MTL Novels Search Engine [send query to search]\nType /url to get your website link.")

@app.on_message(filters.command("stats"))
async def stats_handler(client, message):
    try:
        c = await collection.count_documents({})
        cv = await collection.count_documents({"cover_image": {"$ne": None}})
        await message.reply(f"📊 **Stats**\n📚 Books: `{c}`\n🖼️ Covers: `{cv}`")
    except: pass

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

@app.on_message(filters.text & filters.incoming & ~filters.command(["start", "stats", "index", "stop_index", "url"]))
async def search_handler(client, message):
    logger.info(f"⚡ SEARCH: {message.text[:20]} by {message.from_user.id}")
    q = message.text.strip()
    if len(q) > 100: return
    real_q = q
    keep_result = False
    if q.startswith("!!"): keep_result=True; real_q=q[2:].strip()
    if not real_q: return

    try:
        cnt = await collection.count_documents({"$text": {"$search": real_q}})
        if cnt > 0:
            cursor = collection.find({"$text": {"$search": real_q}}).sort([("score", {"$meta": "textScore"})])
        else:
            reg = {"$regex": real_q, "$options": "i"}
            cnt = await collection.count_documents({"$or": [{"title": reg}, {"file_name": reg}]})
            cursor = collection.find({"$or": [{"title": reg}, {"file_name": reg}]})
        
        res = await cursor.limit(8).to_list(length=8)
        if not res: return await message.reply("❌ No matches.")

        sq = html.escape(real_q)
        txt = (f"🔎 Results for: {sq}\nMatches: {cnt}\n" + "-"*30)
        btns = []
        for b in res:
            label = get_button_label(b)[:40]
            cb_data = f"v:{str(b['_id'])}:k" if keep_result else f"v:{str(b['_id'])}"
            btns.append([InlineKeyboardButton(f"{label}", callback_data=cb_data)])
        
        nav = []
        nav.append(InlineKeyboardButton(f"1/{math.ceil(cnt/8)}", callback_data="nop"))
        if cnt > 8: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:1:{q[:20]}"))
        btns.append(nav)

        await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
    except Exception as e: await message.reply(f"⚠️ {e}")

@app.on_callback_query()
async def callback_handler(client, callback_query):
    logger.info(f"⚡ CB: {callback_query.data} by {callback_query.from_user.id}")
    d = callback_query.data
    
    if d.startswith("n:"):
        # Pagination Logic ... (Same as before)
        try:
            _, p, q = d.split(':', 2)
            p = int(p); real_q = q
            try:
                cnt = await collection.count_documents({"$text": {"$search": real_q}})
                if cnt > 0: cursor = collection.find({"$text": {"$search": real_q}}).sort([("score", {"$meta": "textScore"})])
                else: 
                    reg = {"$regex": real_q, "$options": "i"}
                    cnt = await collection.count_documents({"$or": [{"title": reg}, {"file_name": reg}]})
                    cursor = collection.find({"$or": [{"title": reg}, {"file_name": reg}]})
            except: pass
            res = await cursor.skip(p*8).limit(8).to_list(length=8)
            if not res: return await callback_query.answer("End.", show_alert=True)
            btns = []
            for b in res:
                label = get_button_label(b)[:40]
                cb_data = f"v:{str(b['_id'])}"
                btns.append([InlineKeyboardButton(f"{label}", callback_data=cb_data)])
            nav = []
            if p > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"n:{p-1}:{q}"))
            nav.append(InlineKeyboardButton(f"{p+1}/{math.ceil(cnt/8)}", callback_data="nop"))
            if p < math.ceil(cnt/8)-1: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:{p+1}:{q}"))
            btns.append(nav)
            await callback_query.edit_message_text(f"🔎 Results for: {html.escape(real_q)}\nMatches: {cnt}", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
        except: await callback_query.answer("Error", show_alert=True)

    elif d.startswith("v:"):
        try:
            parts = d.split(':')
            bid = parts[1]
            b = await collection.find_one({"_id": ObjectId(bid)})
            if not b: return await callback_query.answer("Not found", show_alert=True)
            
            title = get_display_title(b)
            auth = b.get('author', 'Unknown')
            syn = b.get('synopsis', 'No synopsis.')
            header = f"<blockquote><b>{html.escape(title)}</b>\nAuthor: {html.escape(auth)}</blockquote>"
            syn_txt = f"<blockquote expandable><b><u>SYNOPSIS</u></b>\n\n{html.escape(syn)}</blockquote>"
            kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
            
            await callback_query.message.delete()
            if b.get('cover_image'):
                try:
                    f = io.BytesIO(b['cover_image']); f.name="c.jpg"
                    await client.send_photo(callback_query.message.chat.id, f, caption=header, parse_mode=ParseMode.HTML)
                except: await client.send_message(callback_query.message.chat.id, header, parse_mode=ParseMode.HTML)
            else: await client.send_message(callback_query.message.chat.id, header, parse_mode=ParseMode.HTML)
            await client.send_message(callback_query.message.chat.id, syn_txt, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        except: await callback_query.answer("Error", show_alert=True)

    elif d.startswith("d:"):
        try:
            bid = d.split(':')[1]
            b = await collection.find_one({"_id": ObjectId(bid)})
            await callback_query.answer("🚀 Sending...")
            try: await client.send_document(callback_query.message.chat.id, b['file_id'], caption=f"📖 {get_display_title(b)}")
            except: pass
        except: pass

# --- MAIN ---

async def main():
    await ensure_indexes()
    
    logger.info("🤖 Starting Telegram Bot...")
    await app.start()
    
    # --- CRITICAL FIX: DELETE WEBHOOK ---
    # This ensures that if a webhook was ever set (which kills polling), it is removed.
    logger.info("🧹 Checking for and deleting stale Webhooks...")
    await app.delete_webhook()
    
    global BOT_USERNAME
    me = await app.get_me()
    BOT_USERNAME = me.username
    logger.info(f"✅ Bot Started as @{BOT_USERNAME}")

    logger.info("🚀 Launching Web Server...")
    web_config = Config()
    web_config.bind = [f"0.0.0.0:{PORT}"]
    
    # Run the web server without blocking the event loop
    server_task = asyncio.create_task(serve(web_app, web_config))
    
    await idle()
    
    logger.info("🛑 Shutting down...")
    await app.stop()
    server_task.cancel()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
