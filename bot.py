import asyncio
import math
import os
import logging
import shutil
import base64
import re
import html
import zipfile
import xml.etree.ElementTree as ET
import urllib.request

# --- IMPORTS ---
from bson.objectid import ObjectId 
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from quart import Quart, request, render_template, redirect, url_for, jsonify, make_response, Response
from hypercorn.config import Config
from hypercorn.asyncio import serve
from itsdangerous import URLSafeTimedSerializer

# --- CONFIGURATION ---
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN").strip()
    CHANNEL_ID = int(os.environ.get("CHANNEL_ID")) 
    ADMIN_ID = int(os.environ.get("ADMIN_ID"))
    AZURE_URL = os.environ.get("AZURE_URL")
    PORT = int(os.environ.get("PORT", 8080))
    PUBLIC_URL = (os.environ.get("PUBLIC_URL") or f"http://0.0.0.0:{PORT}").rstrip('/')
    SECRET_KEY = os.environ.get("SECRET_KEY", "CHANGE_THIS_TO_RANDOM_STRING")
    DB_NAME = os.environ.get("DB_NAME", "novel_library")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")
except Exception as e:
    print(f"❌ CONFIG ERROR: {e}")
    exit(1)

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.INFO)
logging.getLogger("hypercorn").setLevel(logging.INFO)

# --- DATABASE ---
try:
    azure_client = AsyncIOMotorClient(AZURE_URL)
    db = azure_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info("✅ Database Client Created")
except Exception as e:
    logger.error(f"❌ Database Init Failed: {e}")
    exit(1)

# --- WEB APP ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)

# --- BOT CLIENT ---
if os.path.exists("sessions"):
    try: shutil.rmtree("sessions")
    except: pass
os.makedirs("sessions")

# IPv6 False is critical for cloud hosting stability
app = Client("sessions/novel_bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, ipv6=False)

# --- GLOBAL VARS ---
indexing_active = False
files_found = 0
files_saved = 0
BOT_USERNAME = None

# --- HELPERS ---
def get_button_label(title):
    return re.sub(r'\s+(c|ch|chap|vol|v)\.?\s*\d+(?:[-–]\d+)?.*$', '', title, flags=re.IGNORECASE).strip()

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
    try: user_id = serializer.loads(request.cookies.get('auth_token'), max_age=86400*30)
    except: user_id = None
    return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)

@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if b and b.get('cover_image'): return Response(b['cover_image'], mimetype='image/jpeg')
    except: pass
    return Response(base64.b64decode('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7'), mimetype='image/gif')

@web_app.route('/search')
async def web_search():
    try: user_id = serializer.loads(request.cookies.get('auth_token'), max_age=86400*30)
    except: user_id = None
    
    q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    limit = 30; skip = (page-1)*limit
    
    if not q: return await render_template('index.html', query="", results=[], user_id=user_id, bot_username=BOT_USERNAME)
    
    try:
        words = q.split()
        search_terms = " ".join([f'"{w}"' for w in words])
        mongo_query = {"$text": {"$search": search_terms}}
        
        cnt = await collection.count_documents(mongo_query)
        if cnt == 0:
            and_cond = [{"$or": [{"title": re.compile(re.escape(w), re.I)}, {"synopsis": re.compile(re.escape(w), re.I)}]} for w in words]
            mongo_query = {"$and": and_cond}
            cnt = await collection.count_documents(mongo_query)
            
        cursor = collection.find(mongo_query, {"title": 1, "author": 1, "synopsis": 1, "tags": 1, "_id": 1})
        books = await cursor.skip(skip).limit(limit).to_list(length=limit)
        
        results = []
        for b in books:
            syn = re.sub(r'<[^>]+>', '', b.get('synopsis', ''))
            results.append({
                "_id": str(b['_id']),
                "title": b.get('title'),
                "author": b.get('author'),
                "synopsis": syn,
                "tags": b.get('tags', [])
            })
            
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
                
        return await render_template('index.html', query=q, results=results, count=cnt, page=page, total_pages=total_pages, pagination_list=pagination_list, user_id=user_id, bot_username=BOT_USERNAME)
    except Exception as e:
        return await render_template('index.html', query=q, results=[], error=str(e), user_id=user_id)

@web_app.route('/api/download/<book_id>')
async def api_dl(book_id):
    try: user_id = serializer.loads(request.cookies.get('auth_token'), max_age=86400*30)
    except: return jsonify({"status": "error"}), 401
    b = await collection.find_one({"_id": ObjectId(book_id)})
    await app.send_document(user_id, b['file_id'], caption=f"📖 {b.get('title')}")
    return jsonify({"status": "ok"})

# --- BOT HANDLERS ---
@app.on_message(filters.command("start"))
async def start_handler(client, message):
    logger.info(f"CMD /start from {message.from_user.id}")
    if len(message.command) > 1 and message.command[1].startswith("d_"):
        try:
            bid = message.command[1].split("_", 1)[1]
            b = await collection.find_one({"_id": ObjectId(bid)})
            if b: await client.send_document(message.chat.id, b['file_id'], caption=f"📖 {b.get('title')}")
        except: pass
        return
    await message.reply("👋 **Library Bot**\n\nSearch: Just type text\nLink: /url\nStats: /stats\nPing: /ping")

@app.on_message(filters.command("ping"))
async def ping(c, m): 
    logger.info(f"CMD /ping from {m.from_user.id}")
    await m.reply("🏓 Pong!")

@app.on_message(filters.command("url"))
async def url_cmd(c, m):
    try:
        t = serializer.dumps(m.from_user.id)
        await m.reply(f"🔗 **Link:**\n<code>{PUBLIC_URL}/login?token={t}</code>", parse_mode=ParseMode.HTML)
    except: pass

@app.on_message(filters.command("stats"))
async def stats(c, m):
    cnt = await collection.count_documents({})
    await m.reply(f"📚 Books: {cnt}")

@app.on_message(filters.command("fix_search") & filters.user(ADMIN_ID))
async def fix_search(c, m):
    s = await m.reply("🛠 Indexing...")
    asyncio.create_task(ensure_indexes())
    await s.edit("✅ Background task started.")

@app.on_message(filters.text & filters.incoming & ~filters.command(["start", "ping", "url", "stats", "index", "export", "import", "fix_search", "stop_index"]))
async def bot_search(c, m):
    logger.info(f"SEARCH from {m.from_user.id}: {m.text}")
    q = m.text.strip()
    if not q: return
    
    words = q.split()
    search_terms = " ".join([f'"{w}"' for w in words])
    mongo_query = {"$text": {"$search": search_terms}}
    
    try:
        cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)
        if cnt == 0:
            and_cond = [{"$or": [{"title": re.compile(re.escape(w), re.I)}, {"synopsis": re.compile(re.escape(w), re.I)}]} for w in words]
            mongo_query = {"$and": and_cond}
            cnt = await collection.count_documents(mongo_query, maxTimeMS=5000)
        
        if cnt == 0: return await m.reply("❌ No matches.")
        
        cursor = collection.find(mongo_query, {"title": 1, "author": 1})
        res = await cursor.limit(8).to_list(length=8)
        
        btns = [[InlineKeyboardButton(get_button_label(b.get('title','?'))[:40], callback_data=f"v:{str(b['_id'])}")] for b in res]
        nav = [InlineKeyboardButton(f"1/{math.ceil(cnt/8)}", callback_data="nop")]
        if cnt > 8: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:1:{q[:20]}"))
        btns.append(nav)
        
        await m.reply(f"🔎 Found {cnt} books:", reply_markup=InlineKeyboardMarkup(btns))
    except Exception as e:
        logger.error(f"Search Error: {e}")
        await m.reply("⚠️ Error.")

@app.on_callback_query()
async def cb_handler(c, cb):
    d = cb.data
    if d.startswith("n:"):
        _, p, q = d.split(':', 2)
        p = int(p)
        words = q.split()
        search_terms = " ".join([f'"{w}"' for w in words])
        mongo_query = {"$text": {"$search": search_terms}}
        
        try:
            cnt = await collection.count_documents(mongo_query)
            if cnt == 0:
                and_cond = [{"$or": [{"title": re.compile(re.escape(w), re.I)}, {"synopsis": re.compile(re.escape(w), re.I)}]} for w in words]
                mongo_query = {"$and": and_cond}
                cnt = await collection.count_documents(mongo_query)
                
            cursor = collection.find(mongo_query, {"title": 1, "author": 1})
            res = await cursor.skip(p*8).limit(8).to_list(length=8)
            
            btns = [[InlineKeyboardButton(get_button_label(b.get('title','?'))[:40], callback_data=f"v:{str(b['_id'])}")] for b in res]
            nav = []
            if p > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"n:{p-1}:{q}"))
            nav.append(InlineKeyboardButton(f"{p+1}/{math.ceil(cnt/8)}", callback_data="nop"))
            if (p+1)*8 < cnt: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:{p+1}:{q}"))
            btns.append(nav)
            
            await cb.edit_message_text(f"🔎 Found {cnt} books:", reply_markup=InlineKeyboardMarkup(btns))
        except: await cb.answer("Error", show_alert=True)

    elif d.startswith("v:"):
        bid = d.split(':')[1]
        b = await collection.find_one({"_id": ObjectId(bid)})
        if not b: return await cb.answer("Not found", show_alert=True)
        
        cap = f"<blockquote><b>{html.escape(b.get('title','?'))}</b>\n👤 {html.escape(b.get('author','?'))}</blockquote>\n\n<blockquote expandable>{html.escape(b.get('synopsis','').strip())}</blockquote>"
        kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
        
        await cb.message.delete()
        if b.get('cover_image'):
            f = io.BytesIO(b['cover_image']); f.name="c.jpg"
            try: await c.send_photo(cb.message.chat.id, f, caption=cap, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
            except: await c.send_message(cb.message.chat.id, cap, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        else:
            await c.send_message(cb.message.chat.id, cap, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif d.startswith("d:"):
        bid = d.split(':')[1]
        b = await collection.find_one({"_id": ObjectId(bid)})
        await cb.answer("🚀 Sending...")
        await c.send_document(cb.message.chat.id, b['file_id'], caption=f"📖 {b.get('title')}")

# --- ADMIN FEATURES ---
def parse_epub_direct(file_path):
    meta = {"title": None, "author": "Unknown", "synopsis": "No synopsis.", "tags": "", "cover_image": None}
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            opf_path = next((n for n in z.namelist() if n.endswith('.opf')), None)
            if opf_path:
                root = ET.fromstring(z.read(opf_path))
                for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}title'): meta['title'] = elem.text
                for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}creator'): meta['author'] = elem.text
                for elem in root.findall('.//{http://purl.org/dc/elements/1.1/}description'): meta['synopsis'] = elem.text
                
                cover_id = None
                for m in root.findall('.//{http://www.idpf.org/2007/opf}meta'):
                    if m.get('name') == 'cover': cover_id = m.get('content')
                for i in root.findall('.//{http://www.idpf.org/2007/opf}item'):
                    if i.get('id') == cover_id or 'cover-image' in i.get('properties', ''):
                        href = i.get('href')
                        if '/' in opf_path: href = os.path.join(os.path.dirname(opf_path), href)
                        if href in z.namelist(): meta['cover_image'] = z.read(href); break
    except: pass
    return meta

async def indexing_process(client, start_id, end_id, status_msg):
    global indexing_active, files_found, files_saved
    files_found = 0; files_saved = 0
    queue = asyncio.Queue(maxsize=20)
    
    async def worker():
        global files_saved
        while indexing_active:
            try:
                msg = await queue.get()
                path = await client.download_media(msg, file_name=f"temp_{msg.id}.epub")
                if path:
                    meta = await asyncio.to_thread(parse_epub_direct, path)
                    os.remove(path)
                    title = meta['title'] or msg.document.file_name
                    try:
                        await collection.insert_one({
                            "file_id": msg.document.file_id,
                            "file_unique_id": msg.document.file_unique_id,
                            "file_name": msg.document.file_name,
                            "title": title, "author": meta['author'], "synopsis": meta['synopsis'],
                            "tags": meta['tags'], "cover_image": meta['cover_image'], "msg_id": msg.id
                        })
                        files_saved += 1
                    except DuplicateKeyError: pass
                queue.task_done()
            except: queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(3)]
    try:
        current = start_id
        while current <= end_id and indexing_active:
            batch = list(range(current, min(current + 50, end_id + 1)))
            if status_msg and current % 100 == 0:
                try: 
                    await status_msg.edit(f"🔄 Scan: {current}\nFound: {files_found}\nSaved: {files_saved}")
                except: pass
            
            try:
                msgs = await client.get_messages(CHANNEL_ID, batch)
                for m in msgs:
                    if m and m.document and m.document.file_name and m.document.file_name.endswith('.epub'):
                        files_found += 1
                        await queue.put(m)
            except: pass
            current += 50
            await asyncio.sleep(2)
        await queue.join()
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        if status_msg: try: await status_msg.edit(f"✅ Done!\nSaved: {files_saved}")
        except: pass

@app.on_message(filters.command("index") & filters.user(ADMIN_ID))
async def index_cmd(c, m):
    global indexing_active
    if indexing_active: return await m.reply("Busy.")
    args = m.text.split()
    start = int(args[1]) if len(args) > 1 else 1
    end = int(args[2]) if len(args) > 2 else start + 100
    indexing_active = True
    msg = await m.reply(f"🚀 Indexing {start}-{end}...")
    asyncio.create_task(indexing_process(c, start, end, msg))

@app.on_message(filters.command("stop_index") & filters.user(ADMIN_ID))
async def stop_index(c, m):
    global indexing_active; indexing_active = False
    await m.reply("🛑 Stopping...")

@app.on_message(filters.command("export") & filters.user(ADMIN_ID))
async def export_cmd(c, m):
    s = await m.reply("📦 Exporting...")
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
    await c.send_document(m.chat.id, "lib.json", caption="Backup")
    os.remove("lib.json")

@app.on_message(filters.command("import") & filters.user(ADMIN_ID))
async def import_cmd(c, m):
    if not m.reply_to_message: return await m.reply("Reply to file.")
    s = await m.reply("📥 Importing...")
    path = await m.reply_to_message.download()
    with open(path, 'r') as f: data = json.load(f)
    for i in range(0, len(data), 50):
        batch = data[i:i+50]
        for d in batch:
            if d.get('cover_image'): d['cover_image'] = base64.b64decode(d['cover_image'])
            del d['_id']
            try: await collection.replace_one({"file_unique_id":d['file_unique_id']}, d, upsert=True)
            except: pass
    os.remove(path)
    await s.edit("✅ Done.")

# --- BACKGROUND TASKS ---
async def ensure_indexes():
    """Runs safely in background."""
    await asyncio.sleep(5)
    try:
        idxs = await collection.index_information()
        if "$**_text" in idxs: await collection.drop_index("$**_text")
        if "TextIndex" not in idxs:
            await collection.create_index([("title", "text"), ("synopsis", "text")], name="TextIndex", weights={"title": 10, "synopsis": 1}, background=True)
        await collection.create_index("file_unique_id", unique=True, background=True)
    except: pass

async def main():
    logger.info("🤖 Starting...")
    await app.start()
    
    # ⚠️ CRITICAL: Check webhook status
    try:
        logger.info("💥 Clearing any stuck Webhooks...")
        await app.delete_webhook()
        logger.info("✅ Webhook Cleared! Bot is entering Polling Mode.")
    except Exception as e:
        logger.error(f"⚠️ Webhook Clear Failed: {e}")
    
    global BOT_USERNAME; BOT_USERNAME = (await app.get_me()).username
    logger.info(f"✅ Bot Started: @{BOT_USERNAME}")
    
    asyncio.create_task(ensure_indexes())
    
    config = Config(); config.bind = [f"0.0.0.0:{PORT}"]
    logger.info(f"🚀 Web Server starting on port {PORT}")
    asyncio.create_task(serve(web_app, config))
    
    await idle()
    await app.stop()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
