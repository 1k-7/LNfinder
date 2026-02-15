import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import re
import shutil
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
    PUBLIC_URL = (os.environ.get("PUBLIC_URL") or f"http://0.0.0.0:{PORT}").rstrip('/')
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

# --- BACKGROUND TASKS ---
async def ensure_indexes():
    """Runs in background to fix DB without freezing the bot."""
    await asyncio.sleep(5) 
    try:
        indexes = await collection.index_information()
        # Drop conflicting wildcard index if it exists
        if "$**_text" in indexes:
            logger.info("🗑️ Removing old wildcard index...")
            await collection.drop_index("$**_text")

        if "TextIndex" not in indexes:
            logger.info("🛠 Creating Text Index (Background)...")
            # background=True ensures the DB doesn't lock up during this
            await collection.create_index(
                [("title", "text"), ("synopsis", "text")], 
                name="TextIndex", 
                weights={"title": 10, "synopsis": 1},
                background=True 
            )
        
        await collection.create_index("file_unique_id", unique=True, background=True)
        await collection.create_index("msg_id", background=True)
        logger.info("✅ Database Indexes Verified.")
    except Exception as e:
        logger.error(f"❌ Index Error (Non-fatal): {e}")

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
    try: return serializer.loads(token, max_age=86400*30)
    except: return None

def get_display_title(book_doc):
    db_title = book_doc.get('title')
    if db_title and db_title.strip() and db_title != "Unknown Title":
        return db_title.strip()
    fname = book_doc.get('file_name')
    if fname:
        return fname.replace('.epub', '').replace('_', ' ').replace('-', ' ').strip()
    return "Unknown Book"

def get_button_label(book_doc):
    full = get_display_title(book_doc)
    return re.sub(r'\s+(c|ch|chap|vol|v)\.?\s*\d+(?:[-–]\d+)?.*$', '', full, flags=re.IGNORECASE).strip()

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

@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    try:
        # 2s Timeout to prevent hanging
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if b and b.get('cover_image'): return Response(b['cover_image'], mimetype='image/jpeg')
    except: pass
    return Response(base64.b64decode('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7'), mimetype='image/gif')

@web_app.route('/search')
async def search():
    user_id = get_user_from_cookie()
    query = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    limit = 30
    skip = (page - 1) * limit

    if not query:
        return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)
    
    try:
        words = query.split()
        search_terms = " ".join([f'"{w}"' for w in words])
        mongo_query = {"$text": {"$search": search_terms}}
        
        # Max Time 5s to prevent timeouts
        cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)
        if cnt == 0:
             and_conditions = []
             for word in words:
                 reg = re.compile(re.escape(word), re.IGNORECASE)
                 and_conditions.append({"$or": [{"title": reg}, {"synopsis": reg}, {"file_name": reg}]})
             mongo_query = { "$and": and_conditions }
             cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)

        projection = {"title": 1, "author": 1, "synopsis": 1, "tags": 1, "file_name": 1, "_id": 1}
        
        cursor = collection.find(mongo_query, projection)
        books_cursor = await cursor.skip(skip).limit(limit).to_list(length=limit)
        
        results = []
        for b in books_cursor:
            syn = b.get('synopsis', 'No synopsis available.').strip()
            syn = re.sub(r'<[^>]+>', '', syn)
            
            results.append({
                "_id": str(b['_id']),
                "title": get_display_title(b),
                "author": b.get('author', 'Unknown'),
                "synopsis": syn,
                "tags": b.get('tags', '').split(',') if b.get('tags') else []
            })

        total_pages = math.ceil(cnt / limit)
        pagination_list = get_pagination_list(page, total_pages)

        return await render_template(
            'index.html', query=query, results=results, count=cnt, 
            page=page, total_pages=total_pages, pagination_list=pagination_list, 
            user_id=user_id, bot_username=BOT_USERNAME
        )
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return await render_template('index.html', query=query, results=[], error="Database Timeout or Error.", user_id=user_id)

@web_app.route('/api/download/<book_id>')
async def api_download(book_id):
    user_id = get_user_from_cookie()
    if not user_id: return jsonify({"status": "error", "message": "Not logged in"}), 401
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)})
        await app.send_document(
            chat_id=int(user_id),
            document=b['file_id'],
            caption=f"📖 {get_display_title(b)}\n\n<i>Sent via Web Interface</i>",
            parse_mode=ParseMode.HTML
        )
        return jsonify({"status": "ok"})
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500

# --- BOT INIT ---
if os.path.exists("sessions"):
    try: shutil.rmtree("sessions")
    except: pass
os.makedirs("sessions")

app = Client("sessions/novel_bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- EPUB PARSER ---
def parse_epub_direct(file_path):
    meta = {"title": None, "author": "Unknown", "synopsis": "No synopsis.", "tags": "", "cover_image": None}
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            opf_path = next((n for n in z.namelist() if n.endswith('.opf')), None)
            if not opf_path: return meta
            root = ET.fromstring(z.read(opf_path))
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}title'): meta['title'] = elem.text
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}creator'): meta['author'] = elem.text
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}description'): meta['synopsis'] = elem.text
            for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}subject'): 
                if elem.text: meta['tags'] += elem.text + ", "
            
            cover_id = None
            for meta_tag in root.findall('.//{http://www.idpf.org/2007/opf}meta'):
                if meta_tag.get('name') == 'cover': cover_id = meta_tag.get('content')
            
            for item in root.findall('.//{http://www.idpf.org/2007/opf}item'):
                if item.get('id') == cover_id or 'cover-image' in item.get('properties', '').lower():
                    href = item.get('href')
                    if '/' in opf_path: href = os.path.join(os.path.dirname(opf_path), href)
                    if href in z.namelist(): meta['cover_image'] = z.read(href); break
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

@app.on_message(filters.command("ping"))
async def ping_cmd(client, message):
    # This command touches NO database. Use it to check if bot is alive.
    await message.reply("🏓 Pong! Bot is alive.")

@app.on_message(filters.command("url"))
async def url_cmd(client, message):
    try:
        token = serializer.dumps(message.from_user.id)
        await message.reply(f"🔗 <b>Link:</b>\n<code>{PUBLIC_URL}/login?token={token}</code>", parse_mode=ParseMode.HTML)
    except: pass

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
    s = int(args[1]) if len(args) > 1 else 1
    en = int(args[2]) if len(args) > 2 else s + 100
    indexing_active = True
    m = await message.reply(f"🚀 Index {s}-{en}")
    asyncio.create_task(indexing_process(client, s, en, m))

@app.on_message(filters.command("stop_index") & filters.user(ADMIN_ID))
async def stop_cmd(client, message):
    global indexing_active; indexing_active = False
    await message.reply("🛑 Stopping...")

@app.on_message(filters.command("export") & filters.user(ADMIN_ID))
async def export_cmd(client, message):
    s = await message.reply("📦 Exporting...")
    try:
        with open("lib.json", 'w') as f:
            f.write('[')
            first = True
            async for d in collection.find({}):
                if not first: f.write(',')
                first = False
                if d.get('cover_image'): d['cover_image'] = base64.b64encode(d['cover_image']).decode()
                d['_id'] = str(d['_id'])
                json.dump(d, f)
            f.write(']')
        with zipfile.ZipFile("lib.zip", 'w', zipfile.ZIP_DEFLATED) as z: z.write("lib.json")
        await client.send_document(message.chat.id, "lib.zip", caption="✅ Backup")
    except Exception as e: await s.edit(f"❌ {e}")
    finally:
        if os.path.exists("lib.json"): os.remove("lib.json")
        if os.path.exists("lib.zip"): os.remove("lib.zip")

@app.on_message(filters.command("import") & filters.user(ADMIN_ID))
async def import_cmd(client, message):
    if not message.reply_to_message or not message.reply_to_message.document: return await message.reply("Reply file.")
    s = await message.reply("📥 Importing...")
    path = await message.reply_to_message.download()
    try:
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path, 'r') as z: z.extractall(); path = z.namelist()[0]
        with open(path, 'r') as f: data = json.load(f)
        for c in [data[i:i+50] for i in range(0,len(data),50)]:
            for x in c:
                if x.get('cover_image'): 
                    try: x['cover_image'] = base64.b64decode(x['cover_image'])
                    except: x['cover_image'] = None
                if '_id' in x: del x['_id']
                try: await collection.replace_one({"file_unique_id":x['file_unique_id']},x,upsert=True)
                except: pass
        await s.edit("✅ Done")
    except Exception as e: await s.edit(f"❌ {e}")
    finally:
        if os.path.exists(path): os.remove(path)

@app.on_message(filters.command("fix_search") & filters.user(ADMIN_ID))
async def fix_search_cmd(client, message):
    s = await message.reply("🛠 **Rebuilding Index...**")
    await ensure_indexes()
    await s.edit("✅ **Fixed!**")

# --- BOT SEARCH ---
@app.on_message(filters.text & filters.incoming & ~filters.command(["start", "index", "stop_index", "url", "export", "import", "fix_search", "stats", "ping"]))
async def bot_search(client, message):
    q = message.text.strip()
    if not q: return
    
    words = q.split()
    search_terms = " ".join([f'"{w}"' for w in words])
    mongo_query = {"$text": {"$search": search_terms}}
    
    # 5s Timeout on DB calls to stop bot freezing
    try:
        cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)
        if cnt == 0:
            and_conditions = []
            for word in words:
                reg = re.compile(re.escape(word), re.IGNORECASE)
                and_conditions.append({"$or": [{"title": reg}, {"synopsis": reg}]})
            mongo_query = { "$and": and_conditions }
            cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)

        if cnt == 0: return await message.reply("❌ No matches found.")
        
        cursor = collection.find(mongo_query, {"title": 1, "author": 1})
        res = await cursor.limit(8).to_list(length=8)
        
        btns = []
        for b in res:
            btns.append([InlineKeyboardButton(get_button_label(b.get('title', 'Unknown'))[:40], callback_data=f"v:{str(b['_id'])}")])
        
        nav = [InlineKeyboardButton(f"1/{math.ceil(cnt/8)}", callback_data="nop")]
        if cnt > 8: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:1:{q[:20]}"))
        btns.append(nav)
        
        await message.reply(f"🔎 Results: <b>{html.escape(q)}</b> ({cnt})", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply(f"⚠️ Search Timeout. Try fewer words.")

@app.on_callback_query()
async def cb_handler(client, cb):
    d = cb.data
    if d.startswith("n:"):
        _, p, q = d.split(':', 2)
        p = int(p)
        words = q.split()
        search_terms = " ".join([f'"{w}"' for w in words])
        mongo_query = {"$text": {"$search": search_terms}}
        
        try:
            cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)
            if cnt == 0:
                 and_conditions = []
                 for word in words:
                     reg = re.compile(re.escape(word), re.IGNORECASE)
                     and_conditions.append({"$or": [{"title": reg}, {"synopsis": reg}]})
                 mongo_query = { "$and": and_conditions }
            
            cursor = collection.find(mongo_query, {"title": 1, "author": 1})
            res = await cursor.skip(p*8).limit(8).to_list(length=8)
            
            btns = []
            for b in res:
                btns.append([InlineKeyboardButton(get_button_label(b.get('title', 'Unknown'))[:40], callback_data=f"v:{str(b['_id'])}")])
            
            nav = []
            if p > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"n:{p-1}:{q}"))
            nav.append(InlineKeyboardButton(f"{p+1}/{math.ceil(cnt/8)}", callback_data="nop"))
            if (p+1)*8 < cnt: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:{p+1}:{q}"))
            btns.append(nav)
            
            await cb.edit_message_text(f"🔎 Results: <b>{html.escape(q)}</b> ({cnt})", reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
        except: await cb.answer("Timeout.", show_alert=True)
    
    elif d.startswith("v:"):
        bid = d.split(':')[1]
        b = await collection.find_one({"_id": ObjectId(bid)})
        if not b: return await cb.answer("Gone.", show_alert=True)
        
        title = html.escape(b.get('title', 'Unknown'))
        author = html.escape(b.get('author', 'Unknown'))
        syn = html.escape(b.get('synopsis', 'No synopsis.').strip())
        
        caption = (f"<blockquote><b>{title}</b>\nAuthor: {author}</blockquote>\n\n"
                   f"<blockquote expandable><b>SYNOPSIS</b>\n\n{syn}</blockquote>")
        
        kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
        
        await cb.message.delete()
        if b.get('cover_image'):
            f = io.BytesIO(b['cover_image']); f.name="c.jpg"
            try: await client.send_photo(cb.message.chat.id, f, caption=caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
            except: await client.send_message(cb.message.chat.id, caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        else:
            await client.send_message(cb.message.chat.id, caption, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
    
    elif d.startswith("d:"):
        bid = d.split(':')[1]
        b = await collection.find_one({"_id": ObjectId(bid)})
        await cb.answer("🚀 Sending...")
        await client.send_document(cb.message.chat.id, b['file_id'], caption=f"📖 {get_display_title(b)}")

async def main():
    logger.info("🤖 Starting...")
    await app.start()
    
    try: await app.delete_webhook()
    except: pass
    
    global BOT_USERNAME; BOT_USERNAME = (await app.get_me()).username
    logger.info(f"✅ Started @{BOT_USERNAME}")
    
    # Non-blocking Index Fix
    asyncio.create_task(ensure_indexes())
    
    config = Config(); config.bind = [f"0.0.0.0:{PORT}"]
    logger.info(f"🚀 Web Server on {PORT}")
    
    asyncio.create_task(serve(web_app, config))
    await idle()
    await app.stop()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
