import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import re
import json
import base64
import urllib.request
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

# --- PYROGRAM IMPORTS ---
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

# --- WEB SERVER IMPORTS ---
from quart import Quart, request, render_template, redirect, url_for, jsonify, make_response, send_file
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
    
    # Support both standard Mongo URI and Azure
    MONGO_URL = os.environ.get("MONGO_URI") or os.environ.get("MONGO_URL") or os.environ.get("AZURE_URL")
    if not MONGO_URL: raise ValueError("Missing MONGO_URI")
    
    PORT = int(os.environ.get("PORT", 8080))
    PUBLIC_URL = os.environ.get("PUBLIC_URL") or f"http://0.0.0.0:{PORT}"
    SECRET_KEY = os.environ.get("SECRET_KEY", "CHANGE_THIS_TO_RANDOM_STRING_IN_PROD")

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
    # Adding serverSelectionTimeoutMS helps fail fast on DNS errors
    mongo_client = AsyncIOMotorClient(MONGO_URL, serverSelectionTimeoutMS=5000)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info("✅ Connected to MongoDB.")
except Exception as e:
    logger.error(f"❌ DB Connection Error: {e}")
    exit(1)

# --- WEB APP INIT ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)

# --- GLOBAL STATE ---
indexing_active = False
BOT_USERNAME = None

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
    return book_doc.get('file_name', 'Unknown Book').replace('.epub', '').replace('_', ' ').strip()

def format_strict_query(query):
    """
    Transforms 'Harry Potter' into '"Harry" "Potter"'
    to enforce strict AND logic in MongoDB Text Search.
    """
    cleaned = re.sub(r'[^\w\s]', '', query).strip()
    if not cleaned: return ""
    terms = cleaned.split()
    # Wrapping terms in quotes mandates their presence
    return " ".join([f'"{term}"' for term in terms])

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
    except:
        return "❌ Invalid or expired link.", 400

@web_app.route('/')
async def index():
    user_id = get_user_from_cookie()
    return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)

@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    """Lazy load route for images"""
    try:
        if not ObjectId.is_valid(book_id): return "", 404
        book = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if book and book.get('cover_image'):
            return await make_response(book['cover_image'], 200, {'Content-Type': 'image/jpeg'})
    except Exception:
        pass
    # Return a 1x1 transparent pixel or empty response on failure
    return "", 404

@web_app.route('/search')
async def search():
    user_id = get_user_from_cookie()
    raw_query = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    limit = 30 # User Requirement: 30 items per page
    skip = (page - 1) * limit

    if not raw_query:
        return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)
    
    try:
        formatted_query = format_strict_query(raw_query)
        
        # 1. Try Strict Text Search
        count = await collection.count_documents({"$text": {"$search": formatted_query}})
        cursor = None

        if count > 0:
            cursor = collection.find(
                {"$text": {"$search": formatted_query}},
                {"score": {"$meta": "textScore"}} 
            ).sort([("score", {"$meta": "textScore"})])
        else:
            # 2. Fallback: Title Regex ONLY (Fast)
            reg = {"$regex": re.escape(raw_query), "$options": "i"}
            query_filter = {"title": reg}
            count = await collection.count_documents(query_filter)
            cursor = collection.find(query_filter)

        # Optimize: Don't fetch full cover image binary, just check existence
        books_cursor = cursor.project({
            "title": 1, "author": 1, "synopsis": 1, "file_name": 1, 
            "cover_image": {"$slice": 1} 
        }).skip(skip).limit(limit)

        results = []
        async for b in books_cursor:
            syn = b.get('synopsis', 'No synopsis available.').strip()
            
            has_cover = False
            if b.get('cover_image') and len(b['cover_image']) > 0:
                has_cover = True

            results.append({
                "_id": str(b['_id']),
                "title": get_display_title(b),
                "author": b.get('author', 'Unknown'),
                "synopsis": syn,
                "has_cover": has_cover
            })

        total_pages = math.ceil(count / limit)
        return await render_template(
            'index.html', 
            query=raw_query, 
            results=results, 
            count=count, 
            page=page, 
            total_pages=total_pages, 
            user_id=user_id, 
            bot_username=BOT_USERNAME
        )
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return await render_template('index.html', query=raw_query, results=[], count=0, error="An error occurred during search.", user_id=user_id)

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

# --- BOT SETUP ---
if not os.path.exists("sessions"): os.makedirs("sessions")

app = Client(
    "sessions/novel_bot_session", 
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60 
)

# --- MAINTENANCE & INDEXING ---
async def ensure_indexes():
    """Run strict index creation on startup"""
    try:
        logger.info("⚙️ Verifying Database Indexes...")
        await collection.create_index(
            [("title", "text"), ("synopsis", "text"), ("author", "text")],
            weights={"title": 10, "synopsis": 5, "author": 1},
            name="TextSearchIndex"
        )
        await collection.create_index("file_unique_id", unique=True)
        logger.info("✅ Indexes Verified.")
    except Exception as e:
        logger.warning(f"⚠️ Index check warning: {e}")

@app.on_message(filters.command("fix_search") & filters.user(ADMIN_ID))
async def fix_search_cmd(client, message):
    """Admin command to rebuild indexes"""
    m = await message.reply("⚙️ Rebuilding indexes... This may take a moment.")
    try:
        await collection.drop_indexes()
        await ensure_indexes()
        await m.edit("✅ Search indexes completely rebuilt.")
    except Exception as e:
        await m.edit(f"❌ Error: {e}")

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
                    props = item.get('properties', '').lower()
                    if 'cover-image' in props: cover_href = item.get('href'); break
            if not cover_href:
                for elem in root.iter():
                    if elem.tag.split('}')[-1].lower() == 'meta' and elem.get('name') == 'cover':
                        cid = elem.get('content')
                        if manifest:
                            for item in manifest:
                                if item.get('id') == cid: cover_href = item.get('href'); break
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
    if not meta['title']: 
        meta['title'] = os.path.basename(file_path).replace('.epub','').replace('_',' ')
    return meta

async def indexing_process(client, start_id, end_id, status_msg):
    global indexing_active
    files_saved = 0
    files_found = 0
    queue = asyncio.Queue(maxsize=30)
    
    if status_msg: await status_msg.edit(f"🚀 **Starting Scan...**\nRange: {start_id} - {end_id}")

    async def worker():
        nonlocal files_saved
        while indexing_active:
            try:
                message = await queue.get()
                temp_filename = f"temp_{message.id}.epub"
                path = await client.download_media(message, file_name=temp_filename)
                if not path: queue.task_done(); continue
                
                meta = await asyncio.to_thread(parse_epub_direct, path)
                if os.path.exists(path): os.remove(path)
                
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
    
    current_id = start_id; BATCH_SIZE = 50 
    while current_id <= end_id and indexing_active:
        batch_end = min(current_id + BATCH_SIZE, end_id + 1)
        ids = list(range(current_id, batch_end))
        
        if status_msg and (current_id % 100 == 0):
             await status_msg.edit(f"🔄 **Scanning...**\nID: `{current_id}`\nSaved: `{files_saved}`")
             
        try:
            messages = await client.get_messages(CHANNEL_ID, ids)
            for m in messages:
                if m.document and m.document.file_name and m.document.file_name.lower().endswith('.epub'):
                    files_found += 1
                    await queue.put(m)
        except FloodWait as e: await asyncio.sleep(e.value + 1)
        except: pass
        current_id += BATCH_SIZE
        await asyncio.sleep(1)

    await queue.join()
    for w in workers: w.cancel()
    indexing_active = False
    if status_msg: await status_msg.edit(f"✅ **Done!**\nScanned: `{end_id}`\nSaved: `{files_saved}`")

# --- BOT COMMANDS ---
@app.on_message(filters.command("start"))
async def start_handler(client, message):
    if len(message.command) > 1 and message.command[1].startswith("d_"):
        try:
            book_id = message.command[1].split("_", 1)[1]
            b = await collection.find_one({"_id": ObjectId(book_id)})
            if b:
                await message.reply_document(b['file_id'], caption=f"📖 {get_display_title(b)}")
                return 
        except: pass
        
    await message.reply(
        "👋 **Welcome to the Library!**\n\n"
        "🔎 Send me any text to search the database.\n"
        "🌐 Type /url to get your web login link.\n"
        "📊 Type /stats for database info."
    )

@app.on_message(filters.command("ping"))
async def ping_handler(client, message):
    await message.reply("🏓 Pong! System is online.")

@app.on_message(filters.command("stats"))
async def stats_handler(client, message):
    c = await collection.count_documents({})
    await message.reply(f"📊 **Library Stats**\n\n📚 Books Indexed: `{c}`")

@app.on_message(filters.command("url"))
async def url_command(client, message):
    token = serializer.dumps(message.from_user.id)
    login_url = f"{PUBLIC_URL}/login?token={token}"
    await message.reply(f"🔗 **Web Access Link**\n\n[Click Here to Login]({login_url})\n\n<i>Valid for 1 hour.</i>", disable_web_page_preview=True)

@app.on_message(filters.command("index") & filters.user(ADMIN_ID))
async def index_cmd(client, message):
    global indexing_active
    if indexing_active: return await message.reply("⚠️ Running.")
    args = message.text.split()
    try:
        s, en = 1, int(args[1]) if len(args)==2 else int(args[2])
        if len(args)==3: s = int(args[1])
        indexing_active = True
        m = await message.reply(f"🚀 Index {s}-{en}")
        asyncio.create_task(indexing_process(client, s, en, m))
    except: await message.reply("Usage: /index <start> <end>")

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

# --- BOT SEARCH HANDLER ---
@app.on_message(filters.text & filters.private & ~filters.command(["start", "ping", "stats", "url", "index", "stop_index", "fix_search", "export", "import"]))
async def bot_search_handler(client, message):
    q = message.text.strip()
    if len(q) < 2: return
    
    formatted_query = format_strict_query(q)
    
    # Strict Search
    cursor = collection.find(
        {"$text": {"$search": formatted_query}},
        {"score": {"$meta": "textScore"}}
    ).sort([("score", {"$meta": "textScore"})]).limit(8)
    
    results = await cursor.to_list(length=8)
    
    if not results:
        # Fallback to Title Regex
        reg = {"$regex": re.escape(q), "$options": "i"}
        results = await collection.find({"title": reg}).limit(8).to_list(length=8)
        
    if not results:
        return await message.reply("❌ No matches found.")
        
    txt = f"🔎 **Search Results for:** `{html.escape(q)}`\n\nSelect a book below:"
    
    btns = []
    for b in results:
        title = get_display_title(b)
        # Callback data: v:ID
        btns.append([InlineKeyboardButton(title[:50], callback_data=f"v:{str(b['_id'])}")])
        
    await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query()
async def callback_handler(client, cb):
    data = cb.data
    if data.startswith("v:"):
        try:
            bid = data.split(":")[1]
            b = await collection.find_one({"_id": ObjectId(bid)})
            if not b: return await cb.answer("Book removed.", show_alert=True)
            
            title = get_display_title(b)
            author = b.get('author', 'Unknown')
            synopsis = b.get('synopsis', 'No synopsis available.')
            
            # Using Telegram Blockquotes & Expandable Blockquotes
            text = (
                f"<blockquote><b>{html.escape(title)}</b>\n"
                f"<i>{html.escape(author)}</i></blockquote>\n\n"
                f"<blockquote expandable>{html.escape(synopsis)}</blockquote>"
            )
            
            kb = [[InlineKeyboardButton("📥 Download EPUB", callback_data=f"d:{bid}")]]
            
            await cb.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        except Exception as e:
            await cb.answer("Error loading details.", show_alert=True)
            
    elif data.startswith("d:"):
        bid = data.split(":")[1]
        b = await collection.find_one({"_id": ObjectId(bid)})
        if b:
            await cb.answer("🚀 Sending file...")
            await client.send_document(cb.message.chat.id, b['file_id'], caption=f"📖 {get_display_title(b)}")
        else:
            await cb.answer("File not found.", show_alert=True)

# --- MAIN ENTRY POINT ---
async def main():
    await ensure_indexes()
    logger.info("🤖 Starting Telegram Bot...")
    await app.start()
    
    global BOT_USERNAME
    me = await app.get_me()
    BOT_USERNAME = me.username
    logger.info(f"✅ Bot Started: @{BOT_USERNAME}")

    # Start Web Server
    web_config = Config()
    web_config.bind = [f"0.0.0.0:{PORT}"]
    asyncio.create_task(serve(web_app, web_config))
    
    await idle()
    await app.stop()

if __name__ == '__main__':
    app.run(main())
