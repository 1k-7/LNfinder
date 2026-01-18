import asyncio
import os
import logging
import warnings
import io
import zipfile
import html
import re
import base64
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

# --- PYROGRAM IMPORTS ---
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
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
# in_memory=True is CRITICAL for containers to prevent "New Auth Key" loops
app = Client(
    "novel_bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,  
    sleep_threshold=60
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

# --- LIFECYCLE HOOKS (THE FIX) ---
@web_app.before_serving
async def start_bot():
    """Starts the bot when the web server starts."""
    logger.info("🤖 Starting Telegram Bot...")
    try:
        await app.start()
        global BOT_USERNAME
        me = await app.get_me()
        BOT_USERNAME = me.username
        logger.info(f"✅ Bot Started as @{BOT_USERNAME}")
    except Exception as e:
        logger.error(f"❌ Bot Start Error: {e}")

@web_app.after_serving
async def stop_bot():
    """Stops the bot when the web server stops."""
    logger.info("🛑 Stopping Bot...")
    if app.is_connected:
        await app.stop()

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
    if not query:
        return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)
    
    try:
        # 1. Full Text Search
        cnt = await collection.count_documents({"$text": {"$search": query}})
        if cnt > 0:
            cursor = collection.find({"$text": {"$search": query}}).sort([("score", {"$meta": "textScore"})])
        else:
            # 2. Regex Fallback (Fuzzy Search)
            reg = {"$regex": query, "$options": "i"}
            fallback = {"$or": [{"title": reg}, {"author": reg}, {"file_name": reg}]}
            cnt = await collection.count_documents(fallback)
            cursor = collection.find(fallback)

        books_cursor = await cursor.limit(50).to_list(length=50)
        
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
            
        return await render_template('index.html', query=query, results=results, count=cnt, user_id=user_id, bot_username=BOT_USERNAME)
    except Exception as e:
        return await render_template('index.html', query=query, results=[], count=0, error=str(e), user_id=user_id)

@web_app.route('/api/download/<book_id>')
async def api_download(book_id):
    user_id = get_user_from_cookie()
    if not user_id:
        return jsonify({"status": "error", "message": "Not logged in"}), 401
    
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)})
        if not b:
            return jsonify({"status": "error", "message": "Book not found"}), 404
        
        # Send file directly via Telegram
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

# --- BOT HELPERS & PARSER ---

async def ensure_indexes():
    try:
        await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text"), ("file_name", "text")])
        await collection.create_index("file_unique_id", unique=True)
        await collection.create_index("msg_id")
    except: pass

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
    return meta

# --- INDEXING PROCESS ---
async def indexing_process(client, start_id, end_id, status_msg):
    global indexing_active, files_found, files_saved
    files_found = 0
    files_saved = 0
    queue = asyncio.Queue(maxsize=30)
    
    if status_msg: 
        try: await status_msg.edit(f"🚀 **Starting Scan...**\nRange: {start_id} - {end_id}"); except: pass

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
                    print(f"✅ Saved: {meta['title']}")
                except DuplicateKeyError: pass
                except Exception as e: logger.error(f"DB Error: {e}")
                queue.task_done()
            except: queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(3)]
    
    try:
        current_id = start_id
        BATCH_SIZE = 50 
        while current_id <= end_id and indexing_active:
            batch_end = min(current_id + BATCH_SIZE, end_id + 1)
            ids_to_fetch = list(range(current_id, batch_end))
            
            if status_msg and (current_id % 100 == 0):
                try: await status_msg.edit(f"🔄 **Scanning...**\nID: `{current_id}`\nFound: `{files_found}`\nSaved: `{files_saved}`"); except: pass

            if not ids_to_fetch: break
            try:
                messages = await client.get_messages(CHANNEL_ID, ids_to_fetch)
                if messages:
                    for message in messages:
                        if message and message.document and message.document.file_name and message.document.file_name.endswith('.epub'):
                            # REMOVED DUPLICATE GLOBAL HERE - Corrected
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
        if status_msg: try: await status_msg.edit(f"✅ **Done!**\nScanned: `{end_id}`\nFound: `{files_found}`\nSaved: `{files_saved}`"); except: pass

# --- TELEGRAM HANDLERS ---

@app.on_message(filters.command("url"))
async def url_command(client, message):
    logger.info(f"CMD /url from {message.from_user.id}")
    try:
        token = serializer.dumps(message.from_user.id)
        login_url = f"{PUBLIC_URL}/login?token={token}"
        await message.reply(f"🔗 **Your Personal Website Link**\n\n{login_url}", disable_web_page_preview=True)
    except Exception as e: logger.error(f"URL Cmd Error: {e}")

@app.on_message(filters.command("start"))
async def start_handler(client, message):
    logger.info(f"CMD /start from {message.from_user.id}")
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
    logger.info("CMD /stats")
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

@app.on_message(filters.command("migrate") & filters.user(ADMIN_ID))
async def migrate_cmd(client, message):
    if not legacy_collections: return await message.reply("❌ No Legacy")
    s = await message.reply("🚀 Migrating...")
    t=0
    for c in legacy_collections:
        async for d in c.find({}):
            if '_id' in d: del d['_id']
            try: await collection.insert_one(d); t+=1
            except: pass
            if t%100==0:
                try: await s.edit(f"📥 {t}")
                except: pass
    await s.edit(f"✅ {t} Done")

@app.on_message(filters.text & filters.incoming & ~filters.command(["start", "stats", "index", "stop_index", "export", "import", "migrate", "url"]))
async def search_handler(client, message):
    q = message.text.strip()
    if len(q) > 100: return
    
    keep_result = False
    real_q = q
    if q.startswith("!!"): keep_result=True; real_q=q[2:].strip()
    if not real_q: return
    
    try:
        cnt = await collection.count_documents({"$text": {"$search": real_q}})
        if cnt > 0:
            cursor = collection.find({"$text": {"$search": real_q}}).sort([("score", {"$meta": "textScore"})])
        else:
            reg = {"$regex": real_q, "$options": "i"}
            reg_filter = {"$or": [{"title": reg}, {"file_name": reg}]}
            cnt = await collection.count_documents(reg_filter)
            cursor = collection.find(reg_filter)
        
        res = await cursor.limit(8).to_list(length=8)
        if not res: return await message.reply("❌ No matches.")

        sq = html.escape(real_q)
        line_sep = "-" * 101
        txt = (f"🔎 Results fetched for your search : {sq}\nTotal Matches: {cnt}\n{line_sep}")
        
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
    d = callback_query.data
    
    if d.startswith("n:"):
        try:
            _, p, q = d.split(':', 2)
            p = int(p)
            real_q = q
            keep_result = False
            if q.startswith("!!"): keep_result=True; real_q=q[2:].strip()

            try:
                cnt = await collection.count_documents({"$text": {"$search": real_q}})
                if cnt > 0:
                    cursor = collection.find({"$text": {"$search": real_q}}).sort([("score", {"$meta": "textScore"})])
                else:
                    reg = {"$regex": real_q, "$options": "i"}
                    reg_filter = {"$or": [{"title": reg}, {"file_name": reg}]}
                    cnt = await collection.count_documents(reg_filter)
                    cursor = collection.find(reg_filter)
            except: pass
            
            res = await cursor.skip(p*8).limit(8).to_list(length=8)
            if not res: return await callback_query.answer("End.", show_alert=True)
            
            btns = []
            for b in res:
                label = get_button_label(b)[:40]
                cb_data = f"v:{str(b['_id'])}:k" if keep_result else f"v:{str(b['_id'])}"
                btns.append([InlineKeyboardButton(f"{label}", callback_data=cb_data)])
            
            nav = []
            if p > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"n:{p-1}:{q}"))
            nav.append(InlineKeyboardButton(f"{p+1}/{math.ceil(cnt/8)}", callback_data="nop"))
            if p < math.ceil(cnt/8)-1: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:{p+1}:{q}"))
            btns.append(nav)
            
            sq = html.escape(real_q)
            line_sep = "-" * 101
            txt = (f"🔎 Results fetched for your search : {sq}\nTotal Matches: {cnt}\n{line_sep}")
            
            await callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
        except: await callback_query.answer("Error", show_alert=True)

    elif d.startswith("v:"):
        try:
            parts = d.split(':')
            bid = parts[1]
            keep_msg = len(parts) > 2 and parts[2] == 'k'

            b = await collection.find_one({"_id": ObjectId(bid)})
            if not b: return await callback_query.answer("Not found", show_alert=True)
            
            title = get_display_title(b)
            auth = b.get('author', 'Unknown')
            syn = b.get('synopsis', 'No synopsis.')
            
            header_html = (f"<blockquote><b>{html.escape(title)}</b>\nAuthor: {html.escape(auth)}</blockquote>")
            syn_html = (f"<blockquote expandable><b><u>SYNOPSIS</u></b>\n\n{html.escape(syn)}</blockquote>")
            kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
            
            if not keep_msg: await callback_query.message.delete()
            else: await callback_query.answer("Opening...")

            if b.get('cover_image'):
                try:
                    f = io.BytesIO(b['cover_image']); f.name="c.jpg"
                    await client.send_photo(callback_query.message.chat.id, f, caption=header_html, parse_mode=ParseMode.HTML)
                except: await client.send_message(callback_query.message.chat.id, header_html, parse_mode=ParseMode.HTML)
            else:
                await client.send_message(callback_query.message.chat.id, header_html, parse_mode=ParseMode.HTML)
            
            await client.send_message(callback_query.message.chat.id, syn_html, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
                
        except Exception as e: 
            logger.error(f"View Error: {e}")
            await callback_query.answer("Error displaying.", show_alert=True)

    elif d.startswith("d:"):
        try:
            bid = d.split(':')[1]
            b = await collection.find_one({"_id": ObjectId(bid)})
            await callback_query.answer("🚀 Sending...")
            try: await client.send_document(callback_query.message.chat.id, b['file_id'], caption=f"📖 {get_display_title(b)}")
            except: 
                try: await client.copy_message(callback_query.message.chat.id, CHANNEL_ID, b['msg_id'])
                except: await callback_query.answer("File lost.", show_alert=True)
        except: pass

async def main():
    await ensure_indexes()
    
    # Configuration
    hypercorn_config = Config()
    hypercorn_config.bind = [f"0.0.0.0:{PORT}"]
    
    # Run server (Bot starts via @before_serving hook)
    await serve(web_app, hypercorn_config)

if __name__ == '__main__':
    asyncio.run(main())
