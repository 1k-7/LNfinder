import asyncio
import math
import os
import logging
import warnings
import io
import re
import html
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

# --- DATABASE ---
try:
    azure_client = AsyncIOMotorClient(AZURE_URL)
    db = azure_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info("✅ Connected to Azure Cosmos DB.")
except Exception as e:
    logger.error(f"❌ DB Connection Error: {e}")
    exit(1)

# --- IN-MEMORY CACHE ---
SEARCH_CACHE = []
CACHE_LOADING = False

async def build_cache():
    global SEARCH_CACHE, CACHE_LOADING
    if CACHE_LOADING: return
    CACHE_LOADING = True
    logger.info("🔄 Building Search Cache...")
    temp_cache = []
    try:
        # Fetch minimal fields for RAM
        cursor = collection.find({}, {
            "_id": 1, "title": 1, "author": 1, "synopsis": 1, "file_name": 1, "tags": 1
        })
        async for doc in cursor:
            t = (doc.get('title') or '').lower()
            a = (doc.get('author') or '').lower()
            f = (doc.get('file_name') or '').lower()
            tags = doc.get('tags') or []
            if isinstance(tags, list): tags = " ".join(tags).lower()
            else: tags = str(tags).lower()
            s = (doc.get('synopsis') or '').lower()
            
            temp_cache.append({
                "id": str(doc['_id']),
                "title": doc.get('title', 'Unknown'),
                "author": doc.get('author', 'Unknown'),
                "tags": doc.get('tags', []) if isinstance(doc.get('tags'), list) else [],
                # Clean synopsis for UI preview
                "synopsis_preview": re.sub(r'<[^>]+>', '', doc.get('synopsis', ''))[:300] + "...",
                "search_blob": f"{t} {a} {f} {tags} {s}"
            })
        SEARCH_CACHE = temp_cache
        logger.info(f"✅ Cache Built: {len(SEARCH_CACHE)} books.")
    except Exception as e:
        logger.error(f"❌ Cache Build Failed: {e}")
    finally:
        CACHE_LOADING = False

# --- WEB APP ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)
global BOT_USERNAME
BOT_USERNAME = None

def get_button_label(title):
    return re.sub(r'\s+(c|ch|chap|vol|v)\.?\s*\d+(?:[-–]\d+)?.*$', '', title, flags=re.IGNORECASE).strip()

@web_app.route('/health')
async def health(): return "OK", 200

@web_app.route('/login')
async def login():
    token = request.args.get('token')
    if not token: return "❌ No token.", 400
    try:
        user_id = serializer.loads(token, max_age=3600)
        resp = await make_response(redirect(url_for('index')))
        resp.set_cookie('auth_token', serializer.dumps(user_id), max_age=86400*30)
        return resp
    except: return "❌ Invalid link.", 400

@web_app.route('/')
async def index():
    try:
        token = request.cookies.get('auth_token')
        user_id = serializer.loads(token, max_age=86400*30) if token else None
    except: user_id = None
    return await render_template('index.html', query="", results=[], count=0, pagination_list=[], user_id=user_id, bot_username=BOT_USERNAME)

@web_app.route('/search')
async def search():
    try:
        token = request.cookies.get('auth_token')
        user_id = serializer.loads(token, max_age=86400*30) if token else None
    except: user_id = None

    query = request.args.get('q', '').strip().lower()
    page = int(request.args.get('page', 1))
    limit = 50 
    
    if not query:
        return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)
    
    if not SEARCH_CACHE and CACHE_LOADING:
        return await render_template('index.html', query=query, error="⚠️ System Initializing... Refresh in 10s.", results=[], count=0, user_id=user_id)
    
    words = query.split()
    results = [b for b in SEARCH_CACHE if all(w in b['search_blob'] for w in words)]
    count = len(results)
    total_pages = math.ceil(count / limit)
    start = (page - 1) * limit
    results_slice = results[start:start+limit]
    
    # Pagination
    delta = 2; left = page - delta; right = page + delta + 1
    pagination_list = []
    l = None
    for i in range(1, total_pages + 1):
        if i == 1 or i == total_pages or (i >= left and i < right):
            if l:
                if i - l == 2: pagination_list.append(l + 1)
                elif i - l != 1: pagination_list.append('...')
            pagination_list.append(i)
            l = i

    return await render_template(
        'index.html', query=query, results=results_slice, count=count, 
        page=page, total_pages=total_pages, pagination_list=pagination_list, 
        user_id=user_id, bot_username=BOT_USERNAME
    )

@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if b and b.get('cover_image'): return Response(b['cover_image'], mimetype='image/jpeg')
    except: pass
    return "", 404

@web_app.route('/api/details/<book_id>')
async def api_details(book_id):
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"synopsis": 1})
        if not b: return jsonify({"status": "error"}), 404
        # Return full synopsis for the "Show More" button
        return jsonify({"status": "ok", "synopsis": b.get('synopsis', 'No synopsis.')})
    except: return jsonify({"status": "error"}), 500

@web_app.route('/api/download/<book_id>')
async def api_download(book_id):
    try:
        token = request.cookies.get('auth_token')
        user_id = serializer.loads(token, max_age=86400*30) if token else None
        if not user_id: return jsonify({"status": "error", "message": "Not logged in"}), 401
        
        b = await collection.find_one({"_id": ObjectId(book_id)})
        if not b: return jsonify({"status": "error", "message": "Book not found"}), 404
        
        await app.send_document(
            chat_id=int(user_id),
            document=b['file_id'],
            caption=f"📖 {b.get('title', 'Book')}\n\n<i>Sent via Web Interface</i>",
            parse_mode=ParseMode.HTML
        )
        return jsonify({"status": "ok"})
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

# --- BOT ---
if not os.path.exists("sessions"): os.makedirs("sessions")
app = Client("sessions/novel_bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- EPUB PARSER (For Admin Uploads) ---
def parse_epub_direct(file_path):
    meta = {"title": None, "author": "Unknown", "synopsis": "No synopsis.", "tags": "", "cover_image": None}
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            opf_path = next((n for n in z.namelist() if n.endswith('.opf')), None)
            if not opf_path: return meta
            root = ET.fromstring(z.read(opf_path))
            ns = {'dc': 'http://purl.org/dc/elements/1.1/', 'opf': 'http://www.idpf.org/2007/opf'}
            
            # Simple metadata extraction
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}title'): meta['title'] = elem.text
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}creator'): meta['author'] = elem.text
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}description'): meta['synopsis'] = elem.text
            
            # Cover extraction logic (simplified for stability)
            for item in root.findall('.//{http://www.idpf.org/2007/opf}item'):
                if 'cover' in item.get('id', '').lower() or 'cover' in item.get('properties', '').lower():
                    href = item.get('href')
                    if '/' in opf_path: href = os.path.join(os.path.dirname(opf_path), href)
                    if href in z.namelist(): meta['cover_image'] = z.read(href); break
    except: pass
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
                
                title = meta['title'] or message.document.file_name.replace('.epub', '').replace('_', ' ')
                
                try:
                    await collection.insert_one({
                        "file_id": message.document.file_id,
                        "file_unique_id": message.document.file_unique_id,
                        "file_name": message.document.file_name,
                        "title": title,
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
            ids = list(range(current_id, min(current_id + BATCH_SIZE, end_id + 1)))
            if not ids: break
            
            if status_msg and (current_id % 100 == 0):
                try: await status_msg.edit(f"🔄 **Scanning...**\nID: `{current_id}`\nFound: `{files_found}`\nSaved: `{files_saved}`")
                except: pass
                
            try:
                messages = await client.get_messages(CHANNEL_ID, ids)
                for m in messages:
                    if m and m.document and m.document.file_name and m.document.file_name.endswith('.epub'):
                        files_found += 1
                        await queue.put(m)
            except FloodWait as e: await asyncio.sleep(e.value + 1); continue
            except: pass
            
            current_id += BATCH_SIZE
            await asyncio.sleep(2) 
        await queue.join()
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        await build_cache()
        if status_msg:
            try: await status_msg.edit(f"✅ **Done!**\nScanned: `{end_id}`\nFound: `{files_found}`\nSaved: `{files_saved}`\n🧠 Cache Updated.")
            except: pass

@app.on_message(filters.command("url"))
async def url_cmd(client, message):
    try:
        token = serializer.dumps(message.from_user.id)
        await message.reply(f"🔗 <b>Link:</b>\n<code>{PUBLIC_URL}/login?token={token}</code>", parse_mode=ParseMode.HTML)
    except: pass

@app.on_message(filters.command("index") & filters.user(ADMIN_ID))
async def index_cmd(client, message):
    global indexing_active
    if indexing_active: return await message.reply("⚠️ Running.")
    args = message.text.split()
    s = int(args[1]) if len(args) > 1 else 1
    en = int(args[2]) if len(args) > 2 else s + 100
    indexing_active = True
    m = await message.reply(f"🚀 Index {s}-{en}")
    asyncio.create_task(indexing_process(client, s, en, m))

@app.on_message(filters.command("stop_index") & filters.user(ADMIN_ID))
async def stop_cmd(client, message):
    global indexing_active; indexing_active = False
    await message.reply("🛑 Stopping...")

@app.on_message(filters.text & filters.incoming & ~filters.command(["start", "index", "stop_index", "url"]))
async def bot_search(client, message):
    q = message.text.strip().lower()
    if not q: return
    
    # RAM SEARCH
    words = q.split()
    results = [b for b in SEARCH_CACHE if all(w in b['search_blob'] for w in words)]
    cnt = len(results)
    
    if cnt == 0: return await message.reply("❌ No matches.")
    
    # UI: Clean Button List
    top_results = results[:10]
    btns = []
    for b in top_results:
        label = get_button_label(b['title'])[:40]
        btns.append([InlineKeyboardButton(label, callback_data=f"v:{b['id']}")])
    
    nav = [InlineKeyboardButton(f"1/{math.ceil(cnt/10)}", callback_data="nop")]
    if cnt > 10: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:1:{q[:20]}"))
    btns.append(nav)
    
    await message.reply(f"🔎 Results: <b>{html.escape(q)}</b> ({cnt})", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)

@app.on_callback_query()
async def cb_handler(client, cb):
    d = cb.data
    
    # --- PAGINATION ---
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
        
        btns = []
        for b in page_items:
            label = get_button_label(b['title'])[:40]
            btns.append([InlineKeyboardButton(label, callback_data=f"v:{b['id']}")])
        
        nav = []
        if p > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"n:{p-1}:{q}"))
        nav.append(InlineKeyboardButton(f"{p+1}/{math.ceil(cnt/10)}", callback_data="nop"))
        if end < cnt: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:{p+1}:{q}"))
        btns.append(nav)
        
        await cb.edit_message_text(f"🔎 Results: <b>{html.escape(q)}</b> ({cnt})", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
    
    # --- VIEW DETAILS (Polished UI) ---
    elif d.startswith("v:"):
        bid = d.split(':')[1]
        try:
            b = await collection.find_one({"_id": ObjectId(bid)})
            if not b: return await cb.answer("Book not found in DB.", show_alert=True)
            
            # Formatted Output
            title = html.escape(b.get('title', 'Unknown Title'))
            author = html.escape(b.get('author', 'Unknown'))
            syn = html.escape(b.get('synopsis', 'No synopsis available.').strip())
            
            # The "Polished" UX
            caption = (
                f"<blockquote><b>{title}</b>\n"
                f"👤 {author}</blockquote>\n\n"
                f"<blockquote expandable>{syn}</blockquote>"
            )
            
            kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
            
            # Delete old menu to keep chat clean
            await cb.message.delete()
            
            if b.get('cover_image'):
                f = io.BytesIO(b['cover_image']); f.name="cover.jpg"
                try: await client.send_photo(cb.message.chat.id, f, caption=caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
                except: await client.send_message(cb.message.chat.id, caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
            else:
                await client.send_message(cb.message.chat.id, caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        except Exception as e:
            await cb.answer("Error opening book.", show_alert=True)
    
    # --- DOWNLOAD ---
    elif d.startswith("d:"):
        bid = d.split(':')[1]
        try:
            b = await collection.find_one({"_id": ObjectId(bid)})
            await cb.answer("🚀 Sending file...")
            await client.send_document(cb.message.chat.id, b['file_id'], caption=f"📖 {b.get('title')}")
        except: await cb.answer("File not found.", show_alert=True)

async def main():
    logger.info("🤖 Starting...")
    await app.start()
    try: urllib.request.urlopen(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=True")
    except: pass
    global BOT_USERNAME; BOT_USERNAME = (await app.get_me()).username
    logger.info(f"✅ Started @{BOT_USERNAME}")
    asyncio.create_task(build_cache()) # Background loading
    config = Config(); config.bind = [f"0.0.0.0:{PORT}"]
    logger.info(f"🚀 Web Server on {PORT}")
    await serve(web_app, config)
    await idle(); await app.stop()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
