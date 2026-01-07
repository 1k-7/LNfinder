import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import random
import json
import base64
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from telethon import TelegramClient, events, Button
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError, BulkWriteError, OperationFailure

# --- CONFIGURATION ---
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    CHANNEL_ID = int(os.environ.get("CHANNEL_ID")) 
    ADMIN_ID = int(os.environ.get("ADMIN_ID"))
    
    AZURE_URL = os.environ.get("AZURE_URL")
    if not AZURE_URL: raise ValueError("Missing AZURE_URL")
        
    LEGACY_STR = os.environ.get("MONGO_URI") or os.environ.get("MONGO_URL") or ""
    LEGACY_URIS = LEGACY_STR.split() if LEGACY_STR else []
    
    DB_NAME = os.environ.get("DB_NAME", "novel_library")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")

except Exception as e:
    print(f"❌ CONFIG ERROR: {e}")
    raise e

# Suppress warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- DATABASE SETUP ---
try:
    azure_client = AsyncIOMotorClient(AZURE_URL)
    db = azure_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info("✅ Connected to Azure Cosmos DB.")
except Exception as e:
    logger.error(f"❌ Connection Error: {e}")
    exit(1)

# Legacy DBs
legacy_collections = []
for uri in LEGACY_URIS:
    try:
        cli = AsyncIOMotorClient(uri)
        legacy_collections.append(cli[DB_NAME][COLLECTION_NAME])
    except: pass

# --- GLOBAL STATE ---
indexing_active = False
files_processed = 0

# --- HELPER: Ensure Indexes ---
async def ensure_indexes():
    logger.info("⚙️ Verifying Indexes...")
    try:
        await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text")])
        await collection.create_index("file_unique_id", unique=True)
        await collection.create_index("msg_id")
        logger.info("✅ Indexes Ready.")
    except Exception as e:
        logger.warning(f"⚠️ Index Warning: {e}")

# --- METADATA EXTRACTION ---
def parse_epub_direct(file_path):
    meta = {
        "title": None,
        "author": "Unknown Author",
        "synopsis": "No synopsis available.",
        "tags": "",
        "cover_image": None
    }
    
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            opf_path = None
            try:
                container = z.read('META-INF/container.xml')
                root = ET.fromstring(container)
                for child in root.iter():
                    if child.get('full-path'):
                        opf_path = child.get('full-path')
                        break
            except: pass
            
            if not opf_path:
                for name in z.namelist():
                    if name.endswith('.opf'):
                        opf_path = name
                        break
            
            if not opf_path: return meta

            opf_data = z.read(opf_path)
            try:
                root = ET.fromstring(opf_data)
                for elem in root.iter():
                    tag = elem.tag.split('}')[-1].lower() if '}' in elem.tag else elem.tag.lower()
                    if not elem.text: continue
                    text = elem.text.strip()
                    if not text: continue

                    if tag == 'title': meta['title'] = text
                    elif tag == 'creator': meta['author'] = text
                    elif tag == 'description': meta['synopsis'] = text
                    elif tag == 'subject': meta['tags'] += text + ", "
            except: pass

            cover_href = None
            manifest = None
            for elem in root.iter():
                tag = elem.tag.split('}')[-1].lower()
                if tag == 'manifest':
                    manifest = elem
                    break
            
            if manifest:
                for item in manifest:
                    item_id = item.get('id', '').lower()
                    props = item.get('properties', '').lower()
                    if 'cover' in item_id or 'cover-image' in props:
                        cover_href = item.get('href')
                        break
            
            if not cover_href:
                for elem in root.iter():
                    tag = elem.tag.split('}')[-1].lower()
                    if tag == 'meta' and elem.get('name') == 'cover':
                        cover_id = elem.get('content')
                        if manifest:
                            for item in manifest:
                                if item.get('id') == cover_id:
                                    cover_href = item.get('href')
                                    break
            
            if not cover_href:
                for name in z.namelist():
                    lower_name = name.lower()
                    if 'cover.jpg' in lower_name or 'cover.png' in lower_name:
                        cover_href = name
                        break

            if cover_href:
                try:
                    if '/' in opf_path and '/' not in cover_href and cover_href not in z.namelist():
                        folder = opf_path.rsplit('/', 1)[0]
                        full_path = f"{folder}/{cover_href}"
                    else:
                        full_path = cover_href
                    
                    if full_path in z.namelist():
                        meta['cover_image'] = z.read(full_path)
                except: pass

            if meta['synopsis'] == "No synopsis available.":
                for name in z.namelist():
                    if 'intro' in name.lower() or 'desc' in name.lower():
                        if name.endswith(('html', 'xhtml')):
                            try:
                                html_content = z.read(name)
                                soup = BeautifulSoup(html_content, 'html.parser')
                                ps = soup.find_all('p')
                                if ps:
                                    meta['synopsis'] = "\n".join([p.get_text() for p in ps[:6]])
                                    break
                            except: pass
    except Exception: pass
    
    if meta['tags'].endswith(", "): meta['tags'] = meta['tags'][:-2]
    return meta

# --- INDEXING PROCESS ---
async def indexing_process(client, start_id, end_id, status_msg=None):
    global indexing_active, files_processed
    queue = asyncio.Queue(maxsize=30)
    
    if status_msg: 
        try: await status_msg.edit("🚀 **Starting Scan...**")
        except: pass

    async def worker(worker_id):
        while indexing_active:
            try:
                message = await queue.get()
                temp_filename = f"temp_{worker_id}_{message.id}.epub"
                try: path = await message.download_media(file=temp_filename)
                except: queue.task_done(); continue
                
                if not path: queue.task_done(); continue

                meta = await asyncio.to_thread(parse_epub_direct, path)
                if os.path.exists(path): os.remove(path)

                if not meta['title']:
                    meta['title'] = message.file.name.replace('.epub', '').replace('_', ' ')

                try:
                    await collection.insert_one({
                        "file_id": message.file.id,
                        "file_unique_id": str(message.file.id),
                        "file_name": message.file.name,
                        "title": meta['title'],
                        "author": meta['author'],
                        "synopsis": meta['synopsis'],
                        "tags": meta['tags'],
                        "cover_image": meta['cover_image'],
                        "msg_id": message.id
                    })
                    global files_processed
                    files_processed += 1
                    print(f"✅ Saved: {meta['title']}")
                except DuplicateKeyError: pass
                except Exception as e: logger.error(f"Write Error: {e}")
                
                queue.task_done()
            except Exception as e: logger.error(f"Worker Error: {e}"); queue.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(5)]
    
    try:
        current_id = start_id
        BATCH_SIZE = 50 
        while current_id <= end_id and indexing_active:
            batch_end = min(current_id + BATCH_SIZE, end_id + 1)
            ids_to_fetch = list(range(current_id, batch_end))
            if not ids_to_fetch: break

            try:
                messages = await client.get_messages(CHANNEL_ID, ids=ids_to_fetch)
                for message in messages:
                    if message and message.file and message.file.name and message.file.name.endswith('.epub'):
                        await queue.put(message)
                
                if status_msg and (files_processed % 20 == 0):
                    try: await status_msg.edit(f"🔄 **Syncing...**\nScanning: `{current_id}`\nSaved: `{files_processed}`")
                    except: pass
            except Exception as e: logger.error(f"Batch Error: {e}")
            current_id += BATCH_SIZE
            await asyncio.sleep(0.5) 
        await queue.join()
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        if status_msg: 
            try: await status_msg.edit(f"✅ **Done!**\nAdded: `{files_processed}`")
            except: pass

# --- STARTUP ---
async def startup_check():
    logger.info("⚙️ Bot Starting...")
    asyncio.create_task(ensure_indexes())
    
    max_id = 0
    try:
        last_book = await collection.find_one(sort=[("msg_id", -1)])
        if last_book: max_id = last_book.get('msg_id', 0)
    except: pass
    logger.info(f"📍 Ready. Resume ID: {max_id + 1}")

# --- BOT SETUP ---
bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
PAGE_SIZE = 8

# --- MIGRATION ---
@bot.on(events.NewMessage(pattern='/migrate', from_users=[ADMIN_ID]))
async def migrate_handler(event):
    if not legacy_collections: return await event.respond("❌ No legacy DBs.")
    status = await event.respond("🚀 **Migrating...**")
    total = 0
    for col in legacy_collections:
        cursor = col.find({})
        async for doc in cursor:
            if '_id' in doc: del doc['_id']
            try:
                await collection.insert_one(doc)
                total += 1
                if total % 100 == 0: await status.edit(f"📥 Migrating... {total}")
            except DuplicateKeyError: pass
            except: pass
    await status.edit(f"✅ Migrated {total} books.")

# --- EXPORT / IMPORT ---
@bot.on(events.NewMessage(pattern='/export', from_users=[ADMIN_ID]))
async def export_handler(event):
    status = await event.respond("📦 **Exporting from Azure...**")
    file_path = "library_backup.json"
    zip_path = "library_backup.zip"
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('[')
            first = True
            count = 0
            async for doc in collection.find({}):
                if not first: f.write(',')
                first = False
                if doc.get('cover_image'):
                    doc['cover_image'] = base64.b64encode(doc['cover_image']).decode('utf-8')
                doc['_id'] = str(doc['_id'])
                json.dump(doc, f)
                count += 1
                if count % 1000 == 0:
                    try: await status.edit(f"📦 Exporting... ({count})")
                    except: pass
            f.write(']')
            
        await status.edit(f"📦 Compressing...")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(file_path)
            
        await status.edit("🚀 Uploading...")
        await bot.send_file(event.chat_id, zip_path, caption=f"✅ **Azure Backup**\nBooks: {count}")
    except Exception as e:
        await event.respond(f"❌ Error: {e}")
    finally:
        if os.path.exists(file_path): os.remove(file_path)
        if os.path.exists(zip_path): os.remove(zip_path)

@bot.on(events.NewMessage(pattern='/import', from_users=[ADMIN_ID]))
async def import_handler(event):
    args = event.text.split()
    new_bot = 'nb' in args
    reply = await event.get_reply_message()
    if not reply or not reply.file: return await event.respond("Reply to file.")
    
    status = await event.respond("📥 **Importing to Azure...**")
    path = await reply.download_media()
    
    try:
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path, 'r') as z:
                z.extractall()
                path = z.namelist()[0]
        
        with open(path, 'r') as f: data = json.load(f)
        
        total = len(data)
        await status.edit(f"📥 Importing {total} books...")
        
        imported = 0
        batch_size = 50
        
        for i in range(0, total, batch_size):
            chunk = data[i:i+batch_size]
            for item in chunk:
                if item.get('cover_image'):
                    try: item['cover_image'] = base64.b64decode(item['cover_image'])
                    except: item['cover_image'] = None
                if '_id' in item: del item['_id']
                try:
                    await collection.replace_one({"file_unique_id": item['file_unique_id']}, item, upsert=True)
                except: pass
            
            imported += len(chunk)
            if i % 500 == 0:
                try: await status.edit(f"📥 Importing... {imported}/{total}")
                except: pass
                
        await status.edit(f"✅ **Done!**")
    except Exception as e:
        await event.respond(f"❌ Error: {e}")
    finally:
        if os.path.exists(path): os.remove(path)

# --- COMMANDS ---
@bot.on(events.NewMessage(pattern='/stats'))
async def stats_handler(event):
    try:
        docs = await collection.count_documents({})
        covers = await collection.count_documents({"cover_image": {"$ne": None}})
        await event.respond(f"📊 **Azure Stats**\n📚 Books: `{docs}`\n🖼️ Covers: `{covers}`\n🔄 Running: `{indexing_active}`")
    except Exception as e: await event.respond(f"⚠️ Error: {e}")

# --- SEARCH ---
@bot.on(events.NewMessage)
async def search_handler(event):
    if event.text.startswith('/'): return
    await perform_search(event, event.text.strip(), 0)

async def perform_search(event, query, page):
    skip = page * PAGE_SIZE
    
    try:
        # Failsafe Search: Text -> Regex
        try:
            cnt = await collection.count_documents({"$text": {"$search": query}})
            if cnt > 0:
                cursor = collection.find({"$text": {"$search": query}}, {"score": {"$meta": "textScore"}}).sort([("score", {"$meta": "textScore"})])
            else:
                raise OperationFailure("Fallback")
        except (OperationFailure, Exception):
            regex_query = {
                "$or": [
                    {"title": {"$regex": query, "$options": "i"}}, 
                    {"file_name": {"$regex": query, "$options": "i"}},
                    {"author": {"$regex": query, "$options": "i"}},
                    {"tags": {"$regex": query, "$options": "i"}}
                ]
            }
            cnt = await collection.count_documents(regex_query)
            cursor = collection.find(regex_query)
            
        res = await cursor.skip(skip).limit(PAGE_SIZE).to_list(length=PAGE_SIZE)
        
        if not res:
            if isinstance(event, events.CallbackQuery.Event): await event.answer("End.", alert=True)
            else: await event.respond("❌ No matches.")
            return

        safe_q = html.escape(query)
        txt = f"<blockquote>🔎 Search results for : <b>{safe_q}</b>\nMatches <b>{cnt}</b></blockquote>"
        btns = []
        
        for b in res:
            # === TITLE FIX ===
            # Priority: DB Title -> Filename -> "Unknown"
            # Ensure we don't display "Unknown Title" if it's stored in DB that way
            title = b.get('title')
            if not title or title == "Unknown Title":
                title = b.get('file_name', 'Unknown Book')
            
            lbl = f"📖 {title[:30]}"
            btns.append([Button.inline(lbl, data=f"view:{str(b['_id'])}")])
            
        total_p = math.ceil(cnt / PAGE_SIZE)
        nav = []
        cb_q = query[:30]
        if page > 0: nav.append(Button.inline("⬅️ Prev", data=f"nav:{page-1}:{cb_q}"))
        nav.append(Button.inline(f"{page+1}/{total_p}", data="noop"))
        if page < total_p - 1: nav.append(Button.inline("Next ➡️", data=f"nav:{page+1}:{cb_q}"))
        if nav: btns.append(nav)
        
        if isinstance(event, events.CallbackQuery.Event): await event.edit(txt, buttons=btns, parse_mode='html')
        else: await event.respond(txt, buttons=btns, parse_mode='html')
        
    except Exception as e:
        await event.respond(f"⚠️ Error: `{e}`")

@bot.on(events.CallbackQuery)
async def callback(event):
    data = event.data.decode()
    if data == "noop": return await event.answer("Page Info")
    
    if data.startswith("nav:"):
        try:
            _, p, q = data.split(':', 2)
            await perform_search(event, q, int(p))
        except: await event.answer("Error", alert=True)
        
    elif data.startswith("view:"):
        try:
            _, oid = data.split(':')
            from bson.objectid import ObjectId
            b = await collection.find_one({"_id": ObjectId(oid)})
            if not b: return await event.answer("Not found", alert=True)
            
            # === SMART TITLE DISPLAY ===
            title = b.get('title')
            if not title or title == "Unknown Title":
                title = b.get('file_name', 'Unknown Book')
            title = html.escape(title)
            
            author = html.escape(b.get('author', 'Unknown'))
            syn = html.escape(b.get('synopsis', 'No synopsis.'))
            
            h_html = f"<blockquote><b>{title}</b>\nAuthor: {author}</blockquote>"
            # === COLLAPSIBLE BLOCKQUOTE ===
            b_html = f"<blockquote expandable><b><u>SYNOPSIS</u></b>\n{syn}</blockquote>"
            btns = [[Button.inline("📥 Download EPUB", data=f"dl:{oid}")]]
            
            await event.delete()
            if b.get('cover_image'):
                f = io.BytesIO(b['cover_image'])
                f.name = "cover.jpg"
                if len(h_html + b_html) <= 1024:
                    await bot.send_file(event.chat_id, f, caption=h_html+"\n"+b_html, buttons=btns, parse_mode='html')
                else:
                    await bot.send_file(event.chat_id, f, caption=h_html, parse_mode='html')
                    if len(b_html) > 4096:
                        chunks = [b_html[i:i+4096] for i in range(0, len(b_html), 4096)]
                        for i, c in enumerate(chunks):
                            if i == len(chunks)-1: await bot.send_message(event.chat_id, c, buttons=btns, parse_mode='html')
                            else: await bot.send_message(event.chat_id, c, parse_mode='html')
                    else:
                        await bot.send_message(event.chat_id, b_html, buttons=btns, parse_mode='html')
            else:
                if len(h_html + b_html) > 4096:
                    await bot.send_message(event.chat_id, h_html, parse_mode='html')
                    await bot.send_message(event.chat_id, b_html, buttons=btns, parse_mode='html')
                else:
                    await bot.send_message(event.chat_id, h_html+"\n"+b_html, buttons=btns, parse_mode='html')
        except: await event.answer("Error displaying.", alert=True)

    elif data.startswith("dl:"):
        try:
            _, oid = data.split(':')
            from bson.objectid import ObjectId
            b = await collection.find_one({"_id": ObjectId(oid)})
            await event.answer("🚀 Sending...")
            
            # === BUTTON CAPTION FIX ===
            # Explicitly fallback to filename if title is missing
            t = b.get('title')
            if not t or t == "Unknown Title": t = b.get('file_name', 'Book')
            
            try: await bot.send_file(event.chat_id, b['file_id'], caption=f"📖 {t}")
            except: 
                try: await bot.forward_messages(event.chat_id, b['msg_id'], CHANNEL_ID)
                except: await event.answer("❌ File lost.", alert=True)
        except: pass

@bot.on(events.NewMessage(pattern='/index', from_users=[ADMIN_ID]))
async def start_index_handler(event):
    global indexing_active
    if indexing_active: return await event.respond("⚠️ Running.")
    args = event.text.split()
    start, end = 1, 0
    if len(args) == 2: end = int(args[1])
    elif len(args) == 3: start, end = int(args[1]), int(args[2])
    else: return await event.respond("Usage: `/index <end>`")
    indexing_active = True
    msg = await event.respond(f"🚀 **Indexing** {start}-{end}")
    asyncio.create_task(indexing_process(bot, start, end, msg))

@bot.on(events.NewMessage(pattern='/stop_index', from_users=[ADMIN_ID]))
async def stop_index_handler(event):
    global indexing_active
    indexing_active = False
    await event.respond("🛑 Stopping...")

print("Bot Running...")
bot.loop.run_until_complete(startup_check())
bot.run_until_disconnected()
