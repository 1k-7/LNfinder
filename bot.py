import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from telethon import TelegramClient, events, Button
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

# --- CONFIGURATION ---
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    CHANNEL_ID = int(os.environ.get("CHANNEL_ID")) 
    ADMIN_ID = int(os.environ.get("ADMIN_ID"))
    
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
    DB_NAME = os.environ.get("DB_NAME", "novel_library")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")
except Exception as e:
    print("❌ ERROR: Missing Env Vars!")
    raise e

# Suppress warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- DATABASE SETUP ---
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# --- GLOBAL STATE ---
indexing_active = False
files_processed = 0
total_files_found = 0

# --- BACKGROUND INDEX INIT ---
async def ensure_indexes():
    """Run DB index creation in background to avoid blocking startup."""
    try:
        logger.info("⚙️ Optimizing Database Indexes...")
        await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text")])
        await collection.create_index("file_unique_id", unique=True)
        await collection.create_index("msg_id")
        logger.info("✅ Database Indexes Ready.")
    except Exception as e:
        logger.error(f"Index Creation Failed: {e}")

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
            # 1. Find OPF
            try:
                container = z.read('META-INF/container.xml')
                root = ET.fromstring(container)
                ns = {'n': 'urn:oasis:names:tc:opendocument:xmlns:container'}
                opf_path = root.find('.//n:rootfile', ns).get('full-path')
            except:
                opf_path = next((f for f in z.namelist() if f.endswith('.opf')), None)
            
            if not opf_path: return meta

            # 2. Parse OPF
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

            # 3. Extract Cover
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

            # 4. Fallback Synopsis
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

    except Exception:
        pass
    
    return meta

# --- INDEXING PROCESS ---
async def indexing_process(client, start_id, end_id, status_msg=None):
    global indexing_active, files_processed, total_files_found
    
    queue = asyncio.Queue(maxsize=20)
    
    async def worker(worker_id):
        while indexing_active:
            try:
                message = await queue.get()
                temp_filename = f"temp_{worker_id}_{message.id}.epub"
                
                try:
                    path = await message.download_media(file=temp_filename)
                except:
                    queue.task_done()
                    continue
                
                if not path:
                    queue.task_done()
                    continue

                meta = await asyncio.to_thread(parse_epub_direct, path)
                
                if os.path.exists(path):
                    os.remove(path)

                if meta['title'] == "Unknown Title":
                    meta['title'] = message.file.name

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
                    
                except DuplicateKeyError:
                    pass
                except Exception as e:
                    logger.error(f"DB Error: {e}")

                queue.task_done()
                
            except Exception as e:
                logger.error(f"⚠️ Worker Error: {e}")
                queue.task_done()

    # Start 5 concurrent workers
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
                            await queue.put(message)
                
                if status_msg and (total_files_found - last_update_count >= 20):
                    try:
                        await status_msg.edit(
                            f"🔄 **Syncing Library...**\n"
                            f"Scanning ID: `{current_id}` / `{end_id}`\n"
                            f"Found Files: `{total_files_found}`\n"
                            f"Saved New: `{files_processed}`"
                        )
                        last_update_count = total_files_found
                    except: pass
            
            except Exception as e:
                logger.error(f"Batch Error {current_id}: {e}")
            
            current_id += BATCH_SIZE
            await asyncio.sleep(0.5) 

        await queue.join()
        
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        if status_msg:
            try: await status_msg.edit(f"✅ **Sync Complete!**\nScanned up to: `{end_id}`\nTotal Added: `{files_processed}`")
            except: pass

# --- STARTUP ---
async def startup_sync():
    global indexing_active
    logger.info("⚙️ Bot Starting...")
    
    # FIX: Wrap index creation in a task properly
    asyncio.create_task(ensure_indexes())

    try:
        last_book = await collection.find_one(sort=[("msg_id", -1)])
        start_id = (last_book['msg_id'] + 1) if (last_book and 'msg_id' in last_book) else 1
        
        latest = await bot.get_messages(CHANNEL_ID, limit=1)
        if latest:
            end_id = latest[0].id
            logger.info(f"📍 Resume ID: {start_id} | Channel End: {end_id}")
            if start_id < end_id:
                logger.info("🚀 Auto-Resume Started.")
                indexing_active = True
                asyncio.create_task(indexing_process(bot, start_id, end_id, None))
    except Exception as e:
        logger.error(f"Startup Error: {e}")

# --- BOT HANDLERS ---
bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
PAGE_SIZE = 8

@bot.on(events.NewMessage(pattern='/stats'))
async def stats_handler(event):
    count = await collection.count_documents({})
    covers = await collection.count_documents({"cover_image": {"$ne": None}})
    await event.respond(f"📊 **Library Stats**\n\n📚 Books Indexed: `{count}`\n🖼️ With Covers: `{covers}`\n🔄 Indexer Status: `{indexing_active}`")

@bot.on(events.NewMessage(pattern='/index', from_users=[ADMIN_ID]))
async def start_index_handler(event):
    global indexing_active
    if indexing_active: return await event.respond("⚠️ Indexer is already running.")
    
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

# --- SEARCH LOGIC ---
@bot.on(events.NewMessage)
async def search_handler(event):
    if event.text.startswith('/'): return
    query = event.text.strip()
    await perform_search(event, query, 0)

async def perform_search(event, query, page):
    skip_count = page * PAGE_SIZE
    
    ft_count = await collection.count_documents({"$text": {"$search": query}})
    
    if ft_count > 0:
        cursor = collection.find(
            {"$text": {"$search": query}}, 
            {"score": {"$meta": "textScore"}}
        ).sort([("score", {"$meta": "textScore"})])
        total_count = ft_count
    else:
        regex_query = {
            "$or": [
                {"title": {"$regex": query, "$options": "i"}}, 
                {"author": {"$regex": query, "$options": "i"}}, 
                {"tags": {"$regex": query, "$options": "i"}}
            ]
        }
        cursor = collection.find(regex_query)
        total_count = await collection.count_documents(regex_query)
    
    results = await cursor.skip(skip_count).limit(PAGE_SIZE).to_list(length=PAGE_SIZE)
    
    if not results:
        if isinstance(event, events.CallbackQuery.Event):
            await event.answer("No more results.", alert=True)
        else:
            await event.respond("❌ No matching novels found.")
        return

    safe_query = html.escape(query)
    text = (
        f"<blockquote>🔎 Search results for : <b>{safe_query}</b>\n"
        f"Matches <b>{total_count}</b></blockquote>"
    )
    
    buttons = []
    for b in results:
        label = f"📖 {b['title'][:35]}" 
        if b.get('author') and b['author'] != "Unknown Author":
            label += f" — {b['author'][:15]}"
        buttons.append([Button.inline(label, data=f"view:{str(b['_id'])}")])
    
    total_pages = math.ceil(total_count / PAGE_SIZE)
    nav = []
    cb_query = query[:30] 
    
    if page > 0:
        nav.append(Button.inline("⬅️ Prev", data=f"nav:{page-1}:{cb_query}"))
    nav.append(Button.inline(f"{page+1}/{total_pages}", data="noop"))
    if page < total_pages - 1:
        nav.append(Button.inline("Next ➡️", data=f"nav:{page+1}:{cb_query}"))
    
    if nav: buttons.append(nav)
    
    if isinstance(event, events.CallbackQuery.Event):
        await event.edit(text, buttons=buttons, parse_mode='html')
    else:
        await event.respond(text, buttons=buttons, parse_mode='html')

@bot.on(events.CallbackQuery)
async def callback(event):
    data = event.data.decode()
    
    if data == "noop":
        await event.answer("Page Info")
        return

    if data.startswith("nav:"):
        try:
            parts = data.split(':', 2)
            page = int(parts[1])
            query = parts[2]
            await perform_search(event, query, page)
        except:
            await event.answer("Navigation error.", alert=True)
            
    elif data.startswith("view:"):
        from bson.objectid import ObjectId
        b = await collection.find_one({"_id": ObjectId(data.split(':')[1])})
        if not b: return await event.answer("Not found", alert=True)
        
        title = html.escape(b['title'])
        author = html.escape(b.get('author', 'Unknown'))
        raw_synopsis = b.get('synopsis')
        if not raw_synopsis or raw_synopsis.strip() == "": raw_synopsis = "No synopsis available."
        synopsis = html.escape(raw_synopsis)
        
        header_html = f"<blockquote><b>{title}</b>\nAuthor: {author}</blockquote>"
        # Synopsis Header
        body_html = f"<blockquote><b><u>SYNOPSIS</u></b>\n{synopsis}</blockquote>"
        
        btns = [[Button.inline("📥 Download EPUB", data=f"dl:{str(b['_id'])}")]]
        
        await event.delete()
        
        if b.get('cover_image'):
            f = io.BytesIO(b['cover_image'])
            f.name = "cover.jpg"
            full_html = f"{header_html}\n{body_html}"
            
            if len(full_html) <= 1024:
                # Fits in one message
                await bot.send_file(event.chat_id, f, caption=full_html, buttons=btns, parse_mode='html')
            else:
                # Split
                await bot.send_file(event.chat_id, f, caption=header_html, parse_mode='html')
                
                # Check Synopsis Length
                if len(body_html) > 4096:
                    chunks = [body_html[i:i+4096] for i in range(0, len(body_html), 4096)]
                    for i, chunk in enumerate(chunks):
                        if i == len(chunks) - 1:
                            await bot.send_message(event.chat_id, chunk, buttons=btns, parse_mode='html')
                        else:
                            await bot.send_message(event.chat_id, chunk, parse_mode='html')
                else:
                    await bot.send_message(event.chat_id, body_html, buttons=btns, parse_mode='html')
        else:
            # Text Only
            full_html = f"{header_html}\n{body_html}"
            if len(full_html) > 4096:
                await bot.send_message(event.chat_id, header_html, parse_mode='html')
                if len(body_html) > 4096:
                    chunks = [body_html[i:i+4096] for i in range(0, len(body_html), 4096)]
                    for i, chunk in enumerate(chunks):
                        if i == len(chunks) - 1:
                            await bot.send_message(event.chat_id, chunk, buttons=btns, parse_mode='html')
                        else:
                            await bot.send_message(event.chat_id, chunk, parse_mode='html')
                else:
                    await bot.send_message(event.chat_id, body_html, buttons=btns, parse_mode='html')
            else:
                await bot.send_message(event.chat_id, full_html, buttons=btns, parse_mode='html')

    elif data.startswith("dl:"):
        from bson.objectid import ObjectId
        b = await collection.find_one({"_id": ObjectId(data.split(':')[1])})
        await event.answer("🚀 Sending file...")
        try:
            await bot.send_file(event.chat_id, b['file_id'], caption=f"📖 {b['title']}")
        except:
            try: await bot.forward_messages(event.chat_id, b['msg_id'], CHANNEL_ID)
            except: await event.answer("❌ File not found.", alert=True)

print("Bot Running...")
bot.loop.run_until_complete(startup_sync())
bot.run_until_disconnected()
