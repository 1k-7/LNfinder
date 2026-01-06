import asyncio
import math
import io
import os
import logging
import warnings
from bs4 import BeautifulSoup
from telethon import TelegramClient, events, Button
from motor.motor_asyncio import AsyncIOMotorClient
from ebooklib import epub

# --- CONFIGURATION (Loaded from Environment Variables) ---
# We use os.environ.get() to read the variables set in Docker/Cloud
try:
    API_ID = int(os.environ.get("API_ID"))
    API_HASH = os.environ.get("API_HASH")
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    # Channel ID and Admin ID must be integers
    CHANNEL_ID = int(os.environ.get("CHANNEL_ID")) 
    ADMIN_ID = int(os.environ.get("ADMIN_ID"))
    
    # DB Defaults
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
    DB_NAME = os.environ.get("DB_NAME", "novel_library")
    COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")
except TypeError as e:
    print("❌ ERROR: Missing Environment Variables! Please ensure API_ID, CHANNEL_ID, and ADMIN_ID are set and are Integers.")
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
def extract_metadata(file_bytes):
    try:
        book = epub.read_epub(io.BytesIO(file_bytes))
        
        # Title & Author
        title_meta = book.get_metadata('DC', 'title')
        title = title_meta[0][0] if title_meta else "Unknown"
        
        author_meta = book.get_metadata('DC', 'creator')
        author = author_meta[0][0] if author_meta else "Unknown"
        
        # Synopsis
        synopsis = None
        desc_meta = book.get_metadata('DC', 'description')
        if desc_meta:
            synopsis = desc_meta[0][0]
        
        if not synopsis:
            # Fallback to intro.xhtml for lncrawl generated epubs
            for item in book.get_items_of_type(epub.IN_EpubHtml):
                if 'intro' in item.get_name().lower():
                    try:
                        soup = BeautifulSoup(item.get_content(), 'html.parser')
                        div = soup.find('div', class_='synopsis')
                        if div:
                            synopsis = div.get_text(strip=True)
                            break
                    except: pass
        
        # Cover
        cover_image = None
        for item in book.get_items_of_type(epub.IN_EpubImage):
            if item.is_cover() or 'cover' in item.get_name().lower():
                cover_image = item.get_content()
                break
                
        return {
            "title": title,
            "author": author,
            "synopsis": synopsis or "No synopsis available.",
            "cover_image": cover_image
        }
    except Exception as e:
        # logger.error(f"Metadata error: {e}")
        return None

# --- INDEXING WORKER ---
async def indexing_process(client, status_msg):
    global indexing_active, files_processed, total_files_found
    
    # 1. Load existing IDs to skip
    existing_ids = set()
    async for doc in collection.find({}, {"file_unique_id": 1}):
        existing_ids.add(doc['file_unique_id'])
    
    logger.info(f"Loaded {len(existing_ids)} existing books.")
    
    queue = asyncio.Queue()
    
    # Worker function
    async def worker():
        while indexing_active:
            try:
                message = await queue.get()
                file_bytes = await message.download_media(file=bytes)
                meta = await asyncio.to_thread(extract_metadata, file_bytes)
                
                if meta:
                    await collection.insert_one({
                        "file_id": message.file.id,
                        "file_unique_id": message.file.id,
                        "file_name": message.file.name,
                        "title": meta['title'],
                        "author": meta['author'],
                        "synopsis": meta['synopsis'],
                        "cover_image": meta['cover_image'],
                        "msg_id": message.id
                    })
                queue.task_done()
                
                global files_processed
                files_processed += 1
            except Exception as e:
                logger.error(f"Error in worker: {e}")
                queue.task_done()

    # Start 5 concurrent workers
    workers = [asyncio.create_task(worker()) for _ in range(5)]
    
    try:
        count = 0
        last_update_count = 0
        
        # Iterate channel history
        async for message in client.iter_messages(CHANNEL_ID, reverse=True):
            if not indexing_active: break
            
            if message.file and message.file.name and message.file.name.endswith('.epub'):
                total_files_found += 1
                if message.file.id in existing_ids:
                    continue
                
                await queue.put(message)
                count += 1
                
                # Update status message every 50 files
                if count - last_update_count >= 50:
                    try:
                        await status_msg.edit(f"🔄 **Indexing in Progress**\nFound: {total_files_found}\nQueued: {count}\nProcessed: {files_processed}")
                        last_update_count = count
                    except: pass

        await queue.join()
        
    finally:
        # Cleanup
        for w in workers: w.cancel()
        indexing_active = False
        try:
            await status_msg.edit(f"✅ **Indexing Complete!**\nTotal Scanned: {total_files_found}\nNew Added: {files_processed}")
        except: pass

# --- BOT LOGIC ---
bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
PAGE_SIZE = 5

@bot.on(events.NewMessage(pattern='/index', from_users=[ADMIN_ID]))
async def start_index_handler(event):
    global indexing_active, indexing_task
    
    if indexing_active:
        await event.respond("⚠️ Indexing is already running.")
        return

    indexing_active = True
    status_msg = await event.respond("🚀 **Starting Indexing...**\nReading database state...")
    
    # Ensure indexes exist
    await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text")])
    await collection.create_index("file_unique_id", unique=True)
    
    # Start background task
    indexing_task = asyncio.create_task(indexing_process(bot, status_msg))

@bot.on(events.NewMessage(pattern='/stop_index', from_users=[ADMIN_ID]))
async def stop_index_handler(event):
    global indexing_active
    if not indexing_active:
        await event.respond("⚠️ No indexing process running.")
        return
        
    indexing_active = False
    await event.respond("🛑 **Stopping...** (Workers will finish current tasks)")

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.respond("📚 **Novel Bot**\n\nSend a keyword to search.\nAdmin commands: `/index`, `/stop_index`")

# --- SEARCH & DOWNLOAD LOGIC ---
@bot.on(events.NewMessage)
async def search_handler(event):
    if event.text.startswith('/'): return
    query = event.text.strip()
    
    cursor = collection.find({"$text": {"$search": query}}, {"score": {"$meta": "textScore"}}).sort([("score", {"$meta": "textScore"})])
    results = await cursor.to_list(length=50)
    
    if not results:
        await event.respond("❌ No results found.")
        return
    
    await send_page(event.chat_id, query, 0, results)

async def send_page(chat_id, query, page, results):
    total_pages = math.ceil(len(results) / PAGE_SIZE)
    start, end = page * PAGE_SIZE, (page + 1) * PAGE_SIZE
    chunk = results[start:end]
    
    text = f"🔎 **Results for:** `{query}`\nPage {page+1}/{total_pages}"
    buttons = [[Button.inline(f"📖 {b['title']}", data=f"view:{str(b['_id'])}")] for b in chunk]
    
    nav = []
    if page > 0: nav.append(Button.inline("⬅️", data=f"nav:{page-1}:{query[:15]}"))
    nav.append(Button.inline(f"{page+1}/{total_pages}", data="noop"))
    if page < total_pages - 1: nav.append(Button.inline("➡️", data=f"nav:{page+1}:{query[:15]}"))
    if nav: buttons.append(nav)
    
    await bot.send_message(chat_id, text, buttons=buttons)

@bot.on(events.CallbackQuery)
async def callback(event):
    data = event.data.decode()
    if data.startswith("nav:"):
        _, page, query = data.split(':', 2)
        cursor = collection.find({"$text": {"$search": query}}, {"score": {"$meta": "textScore"}}).sort([("score", {"$meta": "textScore"})])
        results = await cursor.to_list(length=50)
        await event.delete()
        await send_page(event.chat_id, query, int(page), results)
    
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
        try:
            await bot.send_file(event.chat_id, b['file_id'], caption=b['title'])
        except:
            # Fallback if file_id expired
            await bot.forward_messages(event.chat_id, b['msg_id'], CHANNEL_ID)

print("Bot Running...")
bot.run_until_disconnected()
