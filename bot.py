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
from telethon.errors import MessageNotModifiedError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

# --- CONFIGURATION ---
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    CHANNEL_ID = int(os.environ.get("CHANNEL_ID")) 
    ADMIN_ID = int(os.environ.get("ADMIN_ID"))
    
    MONGO_STR = os.environ.get("MONGO_URI") or os.environ.get("MONGO_URL")
    if not MONGO_STR:
        raise ValueError("MONGO_URI or MONGO_URL is missing")
    
    MONGO_URIS = MONGO_STR.split()
    DB_NAME = os.environ.get("DB_NAME", "novel_library")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")
except Exception as e:
    print(f"❌ CONFIG ERROR: {e}")
    raise e

# Suppress warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- DATABASE SETUP (Multi-DB) ---
mongo_clients = []
collections = []

for uri in MONGO_URIS:
    try:
        client = AsyncIOMotorClient(uri)
        col = client[DB_NAME][COLLECTION_NAME]
        mongo_clients.append(client)
        collections.append(col)
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB instance: {e}")

if not collections:
    raise RuntimeError("No working MongoDB connections available!")

logger.info(f"✅ Connected to {len(collections)} database(s).")

# --- GLOBAL STATE ---
indexing_active = False
files_processed = 0
total_files_found = 0

# --- HELPER: Ensure Indexes ---
async def ensure_global_indexes():
    tasks = []
    for col in collections:
        t1 = col.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text")])
        t2 = col.create_index("file_unique_id", unique=True)
        t3 = col.create_index("msg_id")
        tasks.extend([t1, t2, t3])
    await asyncio.gather(*tasks)

# --- METADATA EXTRACTION ---
def parse_epub_direct(file_path):
    meta = {
        "title": "Unknown Title",
        "author": "Unknown Author",
        "synopsis": "No synopsis available.",
        "tags": "",
        "cover_image": None
    }
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            try:
                container = z.read('META-INF/container.xml')
                root = ET.fromstring(container)
                ns = {'n': 'urn:oasis:names:tc:opendocument:xmlns:container'}
                opf_path = root.find('.//n:rootfile', ns).get('full-path')
            except:
                opf_path = next((f for f in z.namelist() if f.endswith('.opf')), None)
            
            if not opf_path: return meta

            opf_data = z.read(opf_path)
            it = ET.iterparse(io.BytesIO(opf_data))
            for _, el in it:
                if '}' in el.tag: el.tag = el.tag.split('}', 1)[1]
            root = it.root
            
            metadata = root.find('metadata')
            manifest = root.find('manifest')
            
            if metadata is not None:
                t = metadata.find('title')
                if t is not None and t.text: meta['title'] = t.text
                c = metadata.find('creator')
                if c is not None and c.text: meta['author'] = c.text
                d = metadata.find('description')
                if d is not None and d.text: meta['synopsis'] = d.text
                subjects = [s.text for s in metadata.findall('subject') if s.text]
                meta['tags'] = ", ".join(subjects)

            cover_href = None
            if metadata is not None:
                for m in metadata.findall('meta'):
                    if m.get('name') == 'cover':
                        cover_id = m.get('content')
                        for item in manifest.findall('item'):
                            if item.get('id') == cover_id:
                                cover_href = item.get('href')
                                break
            if not cover_href and manifest is not None:
                for item in manifest.findall('item'):
                    if item.get('properties') == 'cover-image':
                        cover_href = item.get('href')
                        break
            if not cover_href:
                for name in z.namelist():
                    if 'cover.jpg' in name.lower():
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
                    if 'intro' in name.lower() and name.endswith(('.html', '.xhtml')):
                        try:
                            html = z.read(name)
                            soup = BeautifulSoup(html, 'html.parser')
                            div = soup.find('div', class_='synopsis')
                            if div:
                                meta['synopsis'] = div.get_text(strip=True)
                                break
                        except: pass
    except Exception: pass
    return meta

# --- INDEXING PROCESS ---
async def indexing_process(client, start_id, end_id, status_msg=None):
    global indexing_active, files_processed, total_files_found
    queue = asyncio.Queue(maxsize=30)
    
    if status_msg: 
        try: await status_msg.edit("📚 **Syncing Database State...**")
        except: pass

    existing_ids = set()
    for col in collections:
        async for doc in col.find({}, {"file_unique_id": 1}):
            existing_ids.add(doc.get('file_unique_id'))
            
    if status_msg: 
        try: await status_msg.edit(f"✅ State Loaded ({len(existing_ids)} books).\n🚀 **Starting Scan...**")
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

                if meta['title'] == "Unknown Title": meta['title'] = message.file.name

                target_col = random.choice(collections)
                try:
                    await target_col.insert_one({
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
                except Exception as e: logger.error(f"DB Error: {e}")
                queue.task_done()
            except Exception as e: logger.error(f"Worker Error: {e}"); queue.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(5)]
    
    try:
        current_id = start_id
        last_update_count = 0
        BATCH_SIZE = 50 
        while current_id <= end_id and indexing_active:
            batch_end = min(current_id + BATCH_SIZE, end_id + 1)
            ids_to_fetch = list(range(current_id, batch_end))
            if not ids_to_fetch: break

            try:
                messages = await client.get_messages(CHANNEL_ID, ids=ids_to_fetch)
                if messages:
                    for message in messages:
                        if not message: continue
                        if message.file and message.file.name and message.file.name.endswith('.epub'):
                            total_files_found += 1
                            if str(message.file.id) in existing_ids: continue
                            await queue.put(message)
                
                if status_msg and (total_files_found - last_update_count >= 20):
                    try:
                        await status_msg.edit(f"🔄 **Syncing Library...**\nScanning ID: `{current_id}` / `{end_id}`\nFound Files: `{total_files_found}`\nSaved New: `{files_processed}`")
                        last_update_count = total_files_found
                    except: pass
            except Exception as e: logger.error(f"Batch Error {current_id}: {e}")
            current_id += BATCH_SIZE
            await asyncio.sleep(0.5) 
        await queue.join()
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        if status_msg: 
            try: 
                await status_msg.edit(f"✅ **Sync Complete!**\nScanned up to: `{end_id}`\nTotal Added: `{files_processed}`")
            except: pass

# --- STARTUP ---
async def startup_sync():
    global indexing_active
    logger.info("⚙️ Bot Starting...")
    asyncio.create_task(ensure_global_indexes())
    
    max_id = 0
    for col in collections:
        last_book = await col.find_one(sort=[("msg_id", -1)])
        if last_book and last_book.get('msg_id', 0) > max_id: max_id = last_book['msg_id']
    start_id = max_id + 1

    try:
        latest = await bot.get_messages(CHANNEL_ID, limit=1)
        if latest:
            end_id = latest[0].id
            logger.info(f"📍 Resume ID: {start_id} | Channel End: {end_id}")
            if start_id < end_id:
                logger.info("🚀 Auto-Resume Started.")
                indexing_active = True
                asyncio.create_task(indexing_process(bot, start_id, end_id, None))
    except Exception as e: logger.error(f"Startup Error: {e}")

# --- BOT SETUP ---
bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
PAGE_SIZE = 8

# --- EXPORT / IMPORT LOGIC ---

@bot.on(events.NewMessage(pattern='/export', from_users=[ADMIN_ID]))
async def export_handler(event):
    status = await event.respond("📦 **Starting Export...**\nGathering data from databases...")
    file_path = "library_backup.json"
    zip_path = "library_backup.zip"
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('[')
            first = True
            count = 0
            
            for col in collections:
                async for doc in col.find({}):
                    if not first: f.write(',')
                    first = False
                    
                    if doc.get('cover_image'):
                        doc['cover_image'] = base64.b64encode(doc['cover_image']).decode('utf-8')
                    
                    doc['_id'] = str(doc['_id'])
                    
                    json.dump(doc, f)
                    count += 1
                    if count % 1000 == 0:
                        try: await status.edit(f"📦 Exporting... ({count} books)")
                        except: pass
            
            f.write(']')
            
        await status.edit(f"📦 Compressing {count} books...")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(file_path)
            
        await status.edit("🚀 Uploading Backup...")
        await bot.send_file(event.chat_id, zip_path, caption=f"✅ **Library Backup**\nBooks: {count}")
        
    except Exception as e:
        await event.respond(f"❌ Export Failed: {e}")
        logger.error(f"Export Error: {e}")
    finally:
        if os.path.exists(file_path): os.remove(file_path)
        if os.path.exists(zip_path): os.remove(zip_path)

@bot.on(events.NewMessage(pattern='/import', from_users=[ADMIN_ID]))
async def import_handler(event):
    args = event.text.split()
    new_bot_mode = 'nb' in args
    
    reply = await event.get_reply_message()
    if not reply or not reply.file:
        return await event.respond("❌ Please reply to a JSON or ZIP file.")
    
    status = await event.respond("📥 **Downloading Backup...**")
    path = await reply.download_media()
    
    json_path = path
    temp_files = [path]
    
    try:
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path, 'r') as zip_ref:
                json_filename = zip_ref.namelist()[0]
                zip_ref.extractall()
                json_path = json_filename
                temp_files.append(json_filename)
        
        await status.edit("📥 **Parsing JSON...**")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        total = len(data)
        await status.edit(f"📥 **Importing {total} books...**\nMode: {'New Bot (Refetch IDs)' if new_bot_mode else 'Standard Restore'}")
        
        imported = 0
        batch_size = 50
        
        async def process_batch(batch):
            if new_bot_mode:
                msg_ids = [item.get('msg_id') for item in batch if item.get('msg_id')]
                if msg_ids:
                    try:
                        msgs = await bot.get_messages(CHANNEL_ID, ids=msg_ids)
                        msg_map = {m.id: m for m in msgs if m}
                        
                        for item in batch:
                            m_id = item.get('msg_id')
                            if m_id and m_id in msg_map:
                                msg = msg_map[m_id]
                                if msg.file:
                                    item['file_id'] = msg.file.id
                                    item['file_unique_id'] = str(msg.file.id)
                    except Exception as e:
                        logger.error(f"Batch fetch error: {e}")

            for item in batch:
                if item.get('cover_image'):
                    try: item['cover_image'] = base64.b64decode(item['cover_image'])
                    except: item['cover_image'] = None
                
                if '_id' in item: del item['_id']
                
                target = random.choice(collections)
                try:
                    await target.replace_one(
                        {"file_unique_id": item['file_unique_id']},
                        item,
                        upsert=True
                    )
                except: pass

        for i in range(0, total, batch_size):
            chunk = data[i:i+batch_size]
            await process_batch(chunk)
            imported += len(chunk)
            if i % 500 == 0:
                try: await status.edit(f"📥 Importing... {imported}/{total}")
                except: pass
                
        await status.edit(f"✅ **Import Complete!**\nProcessed: {imported}")
        
    except Exception as e:
        await event.respond(f"❌ Import Failed: {e}")
        logger.error(f"Import Error: {e}")
    finally:
        for t in temp_files:
            if os.path.exists(t): os.remove(t)

# --- STANDARD COMMANDS ---

@bot.on(events.NewMessage(pattern='/stats'))
async def stats_handler(event):
    total_docs = 0
    total_covers = 0
    for col in collections:
        total_docs += await col.count_documents({})
        total_covers += await col.count_documents({"cover_image": {"$ne": None}})
    await event.respond(f"📊 **Infinite Library Stats**\n\n🗄️ Databases: `{len(collections)}`\n📚 Total Books: `{total_docs}`\n🖼️ With Covers: `{total_covers}`\n🔄 Indexer Status: `{indexing_active}`")

@bot.on(events.NewMessage(pattern='/index', from_users=[ADMIN_ID]))
async def start_index_handler(event):
    global indexing_active
    if indexing_active: return await event.respond("⚠️ Indexer running.")
    args = event.text.split()
    start, end = 1, 0
    if len(args) == 2: end = int(args[1])
    elif len(args) == 3: start, end = int(args[1]), int(args[2])
    else: return await event.respond("Usage: `/index <end_id>`")
    indexing_active = True
    msg = await event.respond(f"🚀 **Starting Manual Index**\nRange: {start} - {end}")
    asyncio.create_task(indexing_process(bot, start, end, msg))

@bot.on(events.NewMessage(pattern='/stop_index', from_users=[ADMIN_ID]))
async def stop_index_handler(event):
    global indexing_active
    indexing_active = False
    await event.respond("🛑 **Stopping Indexer...**")

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.respond("📚 **Welcome to the Novel Library Bot!**\nSend a keyword to search.")

# --- SEARCH & VIEW ---

@bot.on(events.NewMessage)
async def search_handler(event):
    if event.text.startswith('/'): return
    await perform_search(event, event.text.strip(), 0)

async def perform_search(event, query, page):
    skip_count = page * PAGE_SIZE
    fetch_limit = (page + 1) * PAGE_SIZE
    
    async def fetch(col, idx):
        cnt = await col.count_documents({"$text": {"$search": query}})
        if cnt > 0:
            cur = col.find({"$text": {"$search": query}}, {"score": {"$meta": "textScore"}}).sort([("score", {"$meta": "textScore"})])
        else:
            reg = {"$or": [{"title": {"$regex": query, "$options": "i"}}, {"author": {"$regex": query, "$options": "i"}}, {"tags": {"$regex": query, "$options": "i"}}]}
            cnt = await col.count_documents(reg)
            cur = col.find(reg)
            
        docs = await cur.limit(fetch_limit).to_list(length=fetch_limit)
        for d in docs: 
            d['__db'] = idx
            if 'score' not in d: d['score'] = 0
        return cnt, docs

    tasks = [fetch(col, i) for i, col in enumerate(collections)]
    res = await asyncio.gather(*tasks)
    
    total = sum(r[0] for r in res)
    all_docs = []
    for r in res: all_docs.extend(r[1])
    all_docs.sort(key=lambda x: (x.get('score', 0), x.get('title', '')), reverse=True)
    
    final_page = all_docs[skip_count : skip_count + PAGE_SIZE]
    
    if not final_page:
        if isinstance(event, events.CallbackQuery.Event): await event.answer("No more results.", alert=True)
        else: await event.respond("❌ No matches.")
        return

    safe_q = html.escape(query)
    txt = f"<blockquote>🔎 Search results for : <b>{safe_q}</b>\nMatches <b>{total}</b></blockquote>"
    btns = []
    for b in final_page:
        lbl = f"📖 {b['title'][:35]}"
        if b.get('author') and b['author'] != "Unknown Author": lbl += f" — {b['author'][:15]}"
        btns.append([Button.inline(lbl, data=f"view:{b['__db']}:{str(b['_id'])}")])
        
    total_pages = math.ceil(total / PAGE_SIZE)
    nav = []
    cb_q = query[:30]
    if page > 0: nav.append(Button.inline("⬅️ Prev", data=f"nav:{page-1}:{cb_q}"))
    nav.append(Button.inline(f"{page+1}/{total_pages}", data="noop"))
    if page < total_pages - 1: nav.append(Button.inline("Next ➡️", data=f"nav:{page+1}:{cb_q}"))
    if nav: btns.append(nav)
    
    if isinstance(event, events.CallbackQuery.Event): await event.edit(txt, buttons=btns, parse_mode='html')
    else: await event.respond(txt, buttons=btns, parse_mode='html')

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
            _, db_idx, oid = data.split(':')
            b = await collections[int(db_idx)].find_one({"_id": ObjectId(oid)})
            if not b: return await event.answer("Not found", alert=True)
            
            from bson.objectid import ObjectId
            
            title = html.escape(b['title'])
            author = html.escape(b.get('author', 'Unknown'))
            syn = html.escape(b.get('synopsis') or "No synopsis.")
            
            h_html = f"<blockquote><b>{title}</b>\nAuthor: {author}</blockquote>"
            # COLLAPSIBLE BLOCKQUOTE FOR SYNOPSIS
            b_html = f"<blockquote expandable><b><u>SYNOPSIS</u></b>\n{syn}</blockquote>"
            
            btns = [[Button.inline("📥 Download EPUB", data=f"dl:{db_idx}:{oid}")]]
            
            await event.delete()
            if b.get('cover_image'):
                f = io.BytesIO(b['cover_image'])
                f.name = "cover.jpg"
                if len(h_html + b_html) <= 1024:
                    await bot.send_file(event.chat_id, f, caption=h_html+"\n"+b_html, buttons=btns, parse_mode='html')
                else:
                    await bot.send_file(event.chat_id, f, caption=h_html, parse_mode='html')
                    # Send synopsis separately
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
        except Exception as e: logger.error(f"View Error: {e}")

    elif data.startswith("dl:"):
        try:
            _, db_idx, oid = data.split(':')
            from bson.objectid import ObjectId
            b = await collections[int(db_idx)].find_one({"_id": ObjectId(oid)})
            await event.answer("🚀 Sending...")
            try: await bot.send_file(event.chat_id, b['file_id'], caption=f"📖 {b['title']}")
            except: 
                try: await bot.forward_messages(event.chat_id, b['msg_id'], CHANNEL_ID)
                except: await event.answer("❌ File lost.", alert=True)
        except: pass

print("Bot Running...")
bot.loop.run_until_complete(startup_sync())
bot.run_until_disconnected()
