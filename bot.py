import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from telethon import TelegramClient, events, Button
from motor.motor_asyncio import AsyncIOMotorClient

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
    print("❌ ERROR: Missing Env Vars! Ensure API_ID, CHANNEL_ID, ADMIN_ID are set.")
    raise e

# Suppress warnings
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- DATABASE SETUP ---
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]

# --- GLOBAL STATE ---
indexing_active = False
indexing_task = None
files_processed = 0
total_files_found = 0

# --- DIRECT ZIP PARSING (The "Logic" Fix) ---
def parse_epub_direct(file_path):
    """
    Directly reads the OPF file from the EPUB (ZIP) without parsing chapters.
    This bypasses ebooklib errors on "corrupt" content.
    """
    meta = {
        "title": "Unknown Title",
        "author": "Unknown Author",
        "synopsis": "No synopsis available.",
        "tags": "",
        "cover_image": None
    }
    
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            # 1. Find the OPF file (contains metadata)
            try:
                container = z.read('META-INF/container.xml')
                root = ET.fromstring(container)
                # Namespace handling is annoying in XML, so we strip or wildcard
                ns = {'n': 'urn:oasis:names:tc:opendocument:xmlns:container'}
                opf_path = root.find('.//n:rootfile', ns).get('full-path')
            except:
                # Fallback: Search for any .opf file
                opf_path = next((f for f in z.namelist() if f.endswith('.opf')), None)
            
            if not opf_path: return meta # Truly invalid EPUB

            # 2. Parse OPF
            opf_data = z.read(opf_path)
            # Remove namespaces to make finding tags easy
            it = ET.iterparse(io.BytesIO(opf_data))
            for _, el in it:
                if '}' in el.tag: el.tag = el.tag.split('}', 1)[1]
            root = it.root
            
            metadata = root.find('metadata')
            manifest = root.find('manifest')
            
            if metadata is not None:
                # Extract Text Info
                t = metadata.find('title')
                if t is not None and t.text: meta['title'] = t.text
                
                c = metadata.find('creator')
                if c is not None and c.text: meta['author'] = c.text
                
                d = metadata.find('description')
                if d is not None and d.text: meta['synopsis'] = d.text

                # Extract Tags (Subject)
                subjects = [s.text for s in metadata.findall('subject') if s.text]
                meta['tags'] = ", ".join(subjects)

            # 3. Extract Cover
            # Try to find the cover item in manifest
            cover_href = None
            
            # Method A: Look for meta name="cover"
            if metadata is not None:
                for m in metadata.findall('meta'):
                    if m.get('name') == 'cover':
                        cover_id = m.get('content')
                        # Find href for this id
                        for item in manifest.findall('item'):
                            if item.get('id') == cover_id:
                                cover_href = item.get('href')
                                break
            
            # Method B: Look for item properties="cover-image"
            if not cover_href and manifest is not None:
                for item in manifest.findall('item'):
                    if item.get('properties') == 'cover-image':
                        cover_href = item.get('href')
                        break
            
            # Method C: Filename 'cover.jpg' (lncrawl standard)
            if not cover_href:
                # Check if cover.jpg exists in zip directly
                # We need to resolve path relative to OPF folder if needed
                # But usually lncrawl puts it in OEBPS/images or similar
                for name in z.namelist():
                    if 'cover.jpg' in name.lower():
                        cover_href = name # This is full path in zip
                        break

            # Read Cover Bytes
            if cover_href:
                try:
                    # If href is relative to OPF, resolve it
                    if '/' in opf_path and '/' not in cover_href and cover_href not in z.namelist():
                        folder = opf_path.rsplit('/', 1)[0]
                        full_path = f"{folder}/{cover_href}"
                    else:
                        full_path = cover_href
                    
                    if full_path in z.namelist():
                        meta['cover_image'] = z.read(full_path)
                except: pass

            # 4. Fallback Synopsis (Intro HTML)
            if meta['synopsis'] == "No synopsis available.":
                # Scan for intro file
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

    except Exception as e:
        logger.error(f"Error parsing ZIP: {e}")
        # Even if ZIP parse fails, we return what we found so far (e.g. filename as title)
    
    return meta

# --- INDEXING PROCESS ---
async def indexing_process(client, start_id, end_id, status_msg=None):
    global indexing_active, files_processed, total_files_found
    
    # Pre-load IDs
    existing_ids = set()
    async for doc in collection.find({}, {"file_unique_id": 1}):
        existing_ids.add(doc.get('file_unique_id'))
    
    logger.info(f"📚 Loaded {len(existing_ids)} existing books.")
    
    queue = asyncio.Queue(maxsize=10)
    
    async def worker(worker_id):
        while indexing_active:
            try:
                message = await queue.get()
                
                temp_filename = f"temp_{worker_id}_{message.id}.epub"
                path = await message.download_media(file=temp_filename)
                
                if not path:
                    queue.task_done()
                    continue

                # USE NEW DIRECT PARSER
                meta = await asyncio.to_thread(parse_epub_direct, path)
                
                # Cleanup
                if os.path.exists(path):
                    os.remove(path)

                # Ensure we have at least a title
                if meta['title'] == "Unknown Title":
                    meta['title'] = message.file.name # Last resort

                # Insert
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
                    
                    has_cover = "🖼️" if meta['cover_image'] else "❌"
                    print(f"✅ Saved: {meta['title']} [{has_cover}]")
                    
                except Exception as e:
                    if "E11000" not in str(e): # Ignore duplicate key errors
                        logger.error(f"DB Error: {e}")

                queue.task_done()
                
            except Exception as e:
                logger.error(f"⚠️ Worker Error: {e}")
                queue.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(3)]
    
    try:
        current_id = start_id
        last_update_count = 0
        BATCH_SIZE = 50 

        logger.info(f"🚀 Scanning {start_id} -> {end_id}")

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
                            
                            # SKIP CHECK
                            if str(message.file.id) in existing_ids:
                                continue
                            
                            await queue.put(message)
                
                if status_msg and (total_files_found - last_update_count >= 20):
                    try:
                        await status_msg.edit(
                            f"🔄 **Syncing...**\n"
                            f"ID: {current_id} / {end_id}\n"
                            f"Found: {total_files_found}\n"
                            f"Saved: {files_processed}"
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
            try: await status_msg.edit(f"✅ **Sync Complete!**\nScanned: {end_id}\nTotal Added: {files_processed}")
            except: pass

# --- STARTUP ---
async def startup_sync():
    global indexing_active, indexing_task
    logger.info("⚙️ Startup Check...")
    
    last_book = await collection.find_one(sort=[("msg_id", -1)])
    start_id = (last_book['msg_id'] + 1) if (last_book and 'msg_id' in last_book) else 1

    try:
        latest = await bot.get_messages(CHANNEL_ID, limit=1)
        if latest:
            end_id = latest[0].id
            logger.info(f"📍 Resume ID: {start_id} | Channel End: {end_id}")
            
            if start_id < end_id:
                logger.info("🚀 Auto-Resume Started.")
                indexing_active = True
                indexing_task = asyncio.create_task(indexing_process(bot, start_id, end_id, None))
            else:
                logger.info("✅ Synced.")
    except Exception as e:
        logger.error(f"Startup Error: {e}")

# --- BOT ---
bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
PAGE_SIZE = 5

@bot.on(events.NewMessage(pattern='/stats'))
async def stats_handler(event):
    count = await collection.count_documents({})
    covers = await collection.count_documents({"cover_image": {"$ne": None}})
    await event.respond(f"📊 **Stats**\nBooks: `{count}`\nCovers: `{covers}`\nRunning: `{indexing_active}`")

@bot.on(events.NewMessage(pattern='/index', from_users=[ADMIN_ID]))
async def start_index_handler(event):
    global indexing_active, indexing_task
    if indexing_active: return await event.respond("⚠️ Running.")
    
    args = event.text.split()
    start, end = 1, 0
    if len(args) == 2: end = int(args[1])
    elif len(args) == 3: start, end = int(args[1]), int(args[2])
    else: return await event.respond("Usage: `/index <end>`")
    
    indexing_active = True
    msg = await event.respond(f"🚀 Indexing {start}-{end}")
    
    await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text")])
    await collection.create_index("file_unique_id", unique=True)
    await collection.create_index("msg_id")
    
    indexing_task = asyncio.create_task(indexing_process(bot, start, end, msg))

@bot.on(events.NewMessage(pattern='/stop_index', from_users=[ADMIN_ID]))
async def stop_index_handler(event):
    global indexing_active
    indexing_active = False
    await event.respond("🛑 Stopping...")

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.respond("📚 **Novel Bot**")

# --- SEARCH ---
@bot.on(events.NewMessage)
async def search_handler(event):
    if event.text.startswith('/'): return
    query = event.text.strip()
    
    cursor = collection.find({"$text": {"$search": query}}, {"score": {"$meta": "textScore"}}).sort([("score", {"$meta": "textScore"})])
    results = await cursor.to_list(length=50)
    
    if not results:
        cursor = collection.find({
            "$or": [{"title": {"$regex": query, "$options": "i"}}, {"author": {"$regex": query, "$options": "i"}}, {"tags": {"$regex": query, "$options": "i"}}]
        })
        results = await cursor.to_list(length=20)
    
    if not results: return await event.respond("❌ No results found.")
    await send_page(event.chat_id, query, 0, results)

async def send_page(chat_id, query, page, results):
    total_pages = math.ceil(len(results) / PAGE_SIZE)
    start, end = page * PAGE_SIZE, (page + 1) * PAGE_SIZE
    chunk = results[start:end]
    
    text = f"🔎 **Results:** `{query}`\nPage {page+1}/{total_pages}"
    buttons = [[Button.inline(f"📖 {b['title'][:40]}", data=f"view:{str(b['_id'])}")] for b in chunk]
    
    nav = []
    if page > 0: nav.append(Button.inline("⬅️", data=f"nav:{page-1}"))
    nav.append(Button.inline(f"{page+1}/{total_pages}", data="noop"))
    if page < total_pages - 1: nav.append(Button.inline("➡️", data=f"nav:{page+1}"))
    if nav: buttons.append(nav)
    
    await bot.send_message(chat_id, text, buttons=buttons)

@bot.on(events.CallbackQuery)
async def callback(event):
    data = event.data.decode()
    if data.startswith("nav:"): await event.answer("Search again.", alert=True)
    elif data.startswith("view:"):
        from bson.objectid import ObjectId
        b = await collection.find_one({"_id": ObjectId(data.split(':')[1])})
        if not b: return await event.answer("Not found")
        
        caption = f"**{b['title']}**\nAuthor: {b['author']}\n\n{(b.get('synopsis') or '')[:800]}..."
        btns = [[Button.inline("📥 Download", data=f"dl:{str(b['_id'])}")]]
        
        await event.delete()
        if b.get('cover_image'):
            f = io.BytesIO(b['cover_image'])
            f.name = "cover.jpg"
            await bot.send_file(event.chat_id, f, caption=caption, buttons=btns)
        else:
            await bot.send_message(event.chat_id, caption, buttons=btns)
            
    elif data.startswith("dl:"):
        from bson.objectid import ObjectId
        b = await collection.find_one({"_id": ObjectId(data.split(':')[1])})
        await event.answer("Sending...")
        try: await bot.send_file(event.chat_id, b['file_id'], caption=b['title'])
        except: await bot.forward_messages(event.chat_id, b['msg_id'], CHANNEL_ID)

print("Bot Running...")
bot.loop.run_until_complete(startup_sync())
bot.run_until_disconnected()
