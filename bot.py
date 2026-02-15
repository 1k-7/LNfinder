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

# --- HELPERS ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)
global BOT_USERNAME
BOT_USERNAME = None

def get_button_label(title):
    # Clean title for buttons (remove chapter numbers etc)
    return re.sub(r'\s+(c|ch|chap|vol|v)\.?\s*\d+(?:[-–]\d+)?.*$', '', title, flags=re.IGNORECASE).strip()

def get_user_from_cookie():
    token = request.cookies.get('auth_token')
    if not token: return None
    try: return serializer.loads(token, max_age=86400*30)
    except: return None

# --- WEB ROUTES ---
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
    user_id = get_user_from_cookie()
    return await render_template('index.html', query="", results=[], count=0, pagination_list=[], user_id=user_id, bot_username=BOT_USERNAME)

@web_app.route('/search')
async def search():
    user_id = get_user_from_cookie()
    query = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    limit = 50 
    skip = (page - 1) * limit
    
    if not query:
        return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)
    
    try:
        # --- FIXED SEARCH LOGIC (STRICT AND) ---
        # 1. Split query into words
        words = query.split()
        
        # 2. Wrap each word in quotes to enforce "Phrase Match"
        # In MongoDB Text Search, "word" means strict match. 
        # "Hogwarts" "System" -> Document MUST contain BOTH terms.
        search_terms = " ".join([f'"{w}"' for w in words])
        
        mongo_query = {"$text": {"$search": search_terms}}
        
        # 3. Projection: Only fetch what we need for the LIST. 
        # NO SYNOPSIS. NO IMAGES. This makes it light.
        projection = {"title": 1, "author": 1, "tags": 1, "file_name": 1, "_id": 1}
        
        # 4. Execute (No Sort - Rely on Text Score implicitly)
        cnt = await collection.count_documents(mongo_query)
        cursor = collection.find(mongo_query, projection)
        books_cursor = await cursor.skip(skip).limit(limit).to_list(length=limit)
        
        results = []
        for b in books_cursor:
            title = b.get('title')
            # Fallback title if missing
            if not title: title = b.get('file_name', 'Unknown').replace('.epub','').replace('_',' ')
            
            results.append({
                "_id": str(b['_id']),
                "title": title,
                "author": b.get('author', 'Unknown'),
                "tags": b.get('tags', []) if isinstance(b.get('tags'), list) else []
            })

        # Pagination Logic
        total_pages = math.ceil(cnt / limit)
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
            'index.html', query=query, results=results, count=cnt, 
            page=page, total_pages=total_pages, pagination_list=pagination_list, 
            user_id=user_id, bot_username=BOT_USERNAME
        )
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return await render_template('index.html', query=query, results=[], count=0, error=str(e), user_id=user_id)

# --- LAZY ASSETS ROUTES ---
@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    try:
        # DB Hit: Only for cover image
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if b and b.get('cover_image'): return Response(b['cover_image'], mimetype='image/jpeg')
    except: pass
    return "", 404

@web_app.route('/api/details/<book_id>')
async def api_details(book_id):
    try:
        # DB Hit: Only for synopsis
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"synopsis": 1})
        if not b: return jsonify({"status": "error"}), 404
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

# --- ADMIN TOOLS ---
def parse_epub_direct(file_path):
    # Lightweight parser to extract metadata for indexing
    meta = {"title": None, "author": "Unknown", "synopsis": "No synopsis.", "tags": "", "cover_image": None}
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            opf_path = next((n for n in z.namelist() if n.endswith('.opf')), None)
            if not opf_path: return meta
            root = ET.fromstring(z.read(opf_path))
            # Extract basic tags
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}title'): meta['title'] = elem.text
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}creator'): meta['author'] = elem.text
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}description'): meta['synopsis'] = elem.text
            
            # Extract cover (simple heuristic)
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
        if status_msg:
            try: await status_msg.edit(f"✅ **Done!**\nScanned: `{end_id}`\nFound: `{files_found}`\nSaved: `{files_saved}`")
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

@app.on_message(filters.command("fix_search") & filters.user(ADMIN_ID))
async def fix_search_cmd(client, message):
    # THIS IS THE MOST IMPORTANT COMMAND
    s = await message.reply("🛠 **Rebuilding Index...**")
    try:
        await collection.drop_indexes()
        # Create a TEXT index on Title and Synopsis.
        # This enables the "Strict AND" query mode.
        await collection.create_index(
            [("title", "text"), ("synopsis", "text")],
            name="TextIndex"
        )
        await collection.create_index("file_unique_id", unique=True)
        await collection.create_index("msg_id")
        await s.edit("✅ **Fixed!**\nDatabase optimized for Strict AND Search.")
    except Exception as e:
        await s.edit(f"❌ Error: {e}")

@app.on_message(filters.text & filters.incoming & ~filters.command(["start", "index", "stop_index", "url", "fix_search"]))
async def bot_search(client, message):
    q = message.text.strip()
    if not q: return
    
    # 1. Search DB with Strict Text Search
    words = q.split()
    search_terms = " ".join([f'"{w}"' for w in words])
    mongo_query = {"$text": {"$search": search_terms}}
    
    cnt = await collection.count_documents(mongo_query)
    
    if cnt == 0: return await message.reply("❌ No matches.")
    
    # Fetch top 10 for Bot UI
    cursor = collection.find(mongo_query, {"title": 1, "author": 1})
    res = await cursor.limit(10).to_list(length=10)
    
    btns = []
    for b in res:
        title = b.get('title') or "Unknown"
        btns.append([InlineKeyboardButton(get_button_label(title)[:40], callback_data=f"v:{str(b['_id'])}")])
    
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
        
        words = q.split()
        search_terms = " ".join([f'"{w}"' for w in words])
        mongo_query = {"$text": {"$search": search_terms}}
        
        cnt = await collection.count_documents(mongo_query)
        cursor = collection.find(mongo_query, {"title": 1, "author": 1})
        res = await cursor.skip(p*10).limit(10).to_list(length=10)
        
        if not res: return await cb.answer("End.", show_alert=True)
        
        btns = []
        for b in res:
            title = b.get('title') or "Unknown"
            btns.append([InlineKeyboardButton(get_button_label(title)[:40], callback_data=f"v:{str(b['_id'])}")])
        
        nav = []
        if p > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"n:{p-1}:{q}"))
        nav.append(InlineKeyboardButton(f"{p+1}/{math.ceil(cnt/10)}", callback_data="nop"))
        if (p+1)*10 < cnt: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:{p+1}:{q}"))
        btns.append(nav)
        
        await cb.edit_message_text(f"🔎 Results: <b>{html.escape(q)}</b> ({cnt})", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
    
    # --- VIEW DETAILS (Restored Nice UI) ---
    elif d.startswith("v:"):
        bid = d.split(':')[1]
        try:
            b = await collection.find_one({"_id": ObjectId(bid)})
            if not b: return await cb.answer("Gone.", show_alert=True)
            
            title = html.escape(b.get('title', 'Unknown'))
            author = html.escape(b.get('author', 'Unknown'))
            syn = html.escape(b.get('synopsis', 'No synopsis available.').strip())
            
            # YOUR REQUESTED FORMATTING
            caption = (
                f"<blockquote><b>{title}</b>\n"
                f"👤 {author}</blockquote>\n\n"
                f"<blockquote expandable>{syn}</blockquote>"
            )
            
            kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
            
            await cb.message.delete()
            if b.get('cover_image'):
                f = io.BytesIO(b['cover_image']); f.name="cover.jpg"
                try: await client.send_photo(cb.message.chat.id, f, caption=caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
                except: await client.send_message(cb.message.chat.id, caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
            else:
                await client.send_message(cb.message.chat.id, caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        except: await cb.answer("Error.", show_alert=True)
    
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
    config = Config(); config.bind = [f"0.0.0.0:{PORT}"]
    logger.info(f"🚀 Web Server on {PORT}")
    await serve(web_app, config)
    await idle(); await app.stop()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
