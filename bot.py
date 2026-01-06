import asyncio
import math
import os
import logging
import warnings
import io
from bs4 import BeautifulSoup
from telethon import TelegramClient, events, Button
from motor.motor_asyncio import AsyncIOMotorClient
import ebooklib
from ebooklib import epub

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

# --- METADATA EXTRACTION ---
def extract_metadata(file_path):
    """
    Extracts metadata safely. Returns None only if file is completely unreadable.
    """
    try:
        book = epub.read_epub(file_path)
        
        # 1. Title (Safe Extraction)
        try:
            title = book.get_metadata('DC', 'title')[0][0]
        except (IndexError, TypeError):
            title = "Unknown Title"
        
        # 2. Author (Safe Extraction)
        try:
            author = book.get_metadata('DC', 'creator')[0][0]
        except (IndexError, TypeError):
            author = "Unknown Author"
            
        # 3. Tags / Subjects (For better search, e.g. "Fantasy")
        tags = []
        try:
            # get_metadata returns list of tuples: [('Fantasy', {}), ('Action', {})]
            subjects = book.get_metadata('DC', 'subject')
            for s in subjects:
                if isinstance(s, tuple) and s[0]:
                    tags.append(str(s[0]))
        except Exception:
            pass
        
        tags_str = ", ".join(tags) if tags else ""

        # 4. Synopsis
        synopsis = None
        try:
            desc = book.get_metadata('DC', 'description')
            if desc:
                synopsis = desc[0][0]
        except: pass
        
        if not synopsis:
            # Fallback to intro.xhtml
            for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
                if 'intro' in item.get_name().lower():
                    try:
                        soup = BeautifulSoup(item.get_content(), 'html.parser')
                        div = soup.find('div', class_='synopsis')
                        if div:
                            synopsis = div.get_text(strip=True)
                            break
                    except: pass
        
        # 5. Cover (Strict & Safe)
        cover_image = None
        try:
            for item in book.get_items_of_type(ebooklib.ITEM_IMAGE):
                name = item.get_name().lower()
                # Priority 1: Exact match for lncrawl
                if 'cover.jpg' in name:
                    cover_image = item.get_content()
                    break
                # Priority 2: Standard check
                if hasattr(item, 'is_cover') and item.is_cover():
                    cover_image = item.get_content()
                    break
                # Priority 3: Loose name match
                if 'cover' in name:
                    cover_image = item.get_content()
                    # Keep looking for better match, but store this
        except Exception:
            pass # Keep None if failed

        return {
            "title": title,
            "author": author,
            "synopsis": synopsis or "No synopsis available.",
            "tags": tags_str,
            "cover_image": cover_image
        }
    except Exception as e:
        logger.error(f"❌ Failed to read EPUB structure {file_path}: {e}")
        return None

# --- INDEXING WORKER ---
async def indexing_process(client, status_msg, start_id, end_id):
    global indexing_active, files_processed, total_files_found
    
    existing_ids = set()
    async for doc in collection.find({}, {"file_unique_id": 1}):
        existing_ids.add(doc.get('file_unique_id'))
    
    logger.info(f"📚 Database loaded: {len(existing_ids)} existing books.")
    
    queue = asyncio.Queue()
    
    async def worker(worker_id):
        while indexing_active:
            try:
                message = await queue.get()
                
                temp_filename = f"temp_{worker_id}_{message.id}.epub"
                path = await message.download_media(file=temp_filename)
                
                if not path:
                    queue.task_done()
                    continue

                meta = await asyncio.to_thread(extract_metadata, path)
                
                if os.path.exists(path):
                    os.remove(path)

                if meta:
                    await collection.insert_one({
                        "file_id": message.file.id,
                        "file_unique_id": str(message.file.id),
                        "file_name": message.file.name,
                        "title": meta['title'],
                        "author": meta['author'],
                        "synopsis": meta['synopsis'],
                        "tags": meta['tags'], # Added tags
                        "cover_image": meta['cover_image'],
                        "msg_id": message.id
                    })
                    global files_processed
                    files_processed += 1
                    
                    has_cover = "🖼️" if meta['cover_image'] else "❌"
                    print(f"✅ Saved: {meta['title']} [{has_cover}]")
                
                queue.task_done()
                
            except Exception as e:
                logger.error(f"⚠️ Worker Error: {e}")
                queue.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(3)]
    
    try:
        current_id = start_id
        last_update_count = 0
        BATCH_SIZE = 20

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
                            if str(message.file.id) in existing_ids:
                                continue
                            await queue.put(message)
                
                if total_files_found - last_update_count >= 10:
                    try:
                        await status_msg.edit(
                            f"🔄 **Indexing...**\n"
                            f"ID: {current_id}\n"
                            f"Found: {total_files_found}\n"
                            f"Saved: {files_processed}"
                        )
                        last_update_count = total_files_found
                    except: pass
            
            except Exception as e:
                logger.error(f"Batch Error {current_id}: {e}")
            
            current_id += BATCH_SIZE
            await asyncio.sleep(2) 

        await queue.join()
        
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        try:
            await status_msg.edit(f"✅ **Done!**\nScanned: {end_id}\nSaved: {files_processed}")
        except: pass

# --- BOT LOGIC ---
bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
PAGE_SIZE = 5

@bot.on(events.NewMessage(pattern='/stats'))
async def stats_handler(event):
    count = await collection.count_documents({})
    covers = await collection.count_documents({"cover_image": {"$ne": None}})
    await event.respond(f"📊 **Stats**\nBooks: `{count}`\nCovers: `{covers}`")

@bot.on(events.NewMessage(pattern='/index', from_users=[ADMIN_ID]))
async def start_index_handler(event):
    global indexing_active, indexing_task
    
    if indexing_active:
        return await event.respond("⚠️ Indexing is already running.")

    args = event.text.split()
    if len(args) < 2:
        return await event.respond("⚠️ **Usage:**\n`/index <last_msg_id>`")
    
    try:
        if len(args) == 3:
            start_id, end_id = int(args[1]), int(args[2])
        else:
            start_id, end_id = 1, int(args[1])
    except ValueError:
        return await event.respond("❌ Invalid ID.")

    indexing_active = True
    status_msg = await event.respond(f"🚀 **Starting**\nRange: {start_id}-{end_id}...")
    
    # Updated Index to include tags
    await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text")])
    await collection.create_index("file_unique_id", unique=True)
    
    indexing_task = asyncio.create_task(indexing_process(bot, status_msg, start_id, end_id))

@bot.on(events.NewMessage(pattern='/stop_index', from_users=[ADMIN_ID]))
async def stop_index_handler(event):
    global indexing_active
    indexing_active = False
    await event.respond("🛑 Stopping...")

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.respond("📚 **Novel Bot**\nSend a keyword to search.\nAdmin: `/index <id>`")

# --- SEARCH LOGIC ---
@bot.on(events.NewMessage)
async def search_handler(event):
    if event.text.startswith('/'): return
    query = event.text.strip()
    
    # 1. Text Search (Matches Title, Author, Synopsis, TAGS)
    cursor = collection.find(
        {"$text": {"$search": query}}, 
        {"score": {"$meta": "textScore"}}
    ).sort([("score", {"$meta": "textScore"})])
    results = await cursor.to_list(length=50)
    
    # 2. Regex Fallback
    if not results:
        cursor = collection.find({
            "$or": [
                {"title": {"$regex": query, "$options": "i"}},
                {"author": {"$regex": query, "$options": "i"}},
                {"tags": {"$regex": query, "$options": "i"}}
            ]
        })
        results = await cursor.to_list(length=20)
    
    if not results:
        return await event.respond("❌ No results found.")
    
    await send_page(event.chat_id, query, 0, results)

async def send_page(chat_id, query, page, results):
    total_pages = math.ceil(len(results) / PAGE_SIZE)
    start, end = page * PAGE_SIZE, (page + 1) * PAGE_SIZE
    chunk = results[start:end]
    
    text = f"🔎 **Results for:** `{query}`\nPage {page+1}/{total_pages}"
    
    buttons = []
    for b in chunk:
        buttons.append([Button.inline(f"📖 {b['title'][:40]}", data=f"view:{str(b['_id'])}")])
    
    nav = []
    if page > 0: nav.append(Button.inline("⬅️", data=f"nav:{page-1}"))
    nav.append(Button.inline(f"{page+1}/{total_pages}", data="noop"))
    if page < total_pages - 1: nav.append(Button.inline("➡️", data=f"nav:{page+1}"))
    if nav: buttons.append(nav)
    
    await bot.send_message(chat_id, text, buttons=buttons)

@bot.on(events.CallbackQuery)
async def callback(event):
    data = event.data.decode()
    
    if data.startswith("nav:"):
        await event.answer("Please search again to navigate.", alert=True)
    
    elif data.startswith("view:"):
        from bson.objectid import ObjectId
        b = await collection.find_one({"_id": ObjectId(data.split(':')[1])})
        if not b: return await event.answer("Not found")
        
        syn = (b.get('synopsis') or "No synopsis")[:800]
        tags = b.get('tags', '')
        tag_line = f"\n🏷️ **Tags:** {tags}\n" if tags else ""
        
        caption = f"**{b['title']}**\nAuthor: {b['author']}{tag_line}\n\n{syn}..."
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
        try:
            await bot.send_file(event.chat_id, b['file_id'], caption=b['title'])
        except:
            await bot.forward_messages(event.chat_id, b['msg_id'], CHANNEL_ID)

print("Bot Running...")
bot.run_until_disconnected()
