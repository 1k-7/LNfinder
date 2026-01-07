import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import re
import random
import json
import base64
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    Message,
    MessageEntity
)
from pyrogram.enums import ParseMode, MessageEntityType
from pyrogram.errors import FloodWait

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

legacy_collections = []
for uri in LEGACY_URIS:
    try:
        cli = AsyncIOMotorClient(uri)
        legacy_collections.append(cli[DB_NAME][COLLECTION_NAME])
    except: pass

# --- GLOBAL STATE ---
indexing_active = False
files_found = 0
files_saved = 0

# --- HELPERS ---
async def ensure_indexes():
    try:
        await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text"), ("file_name", "text")])
        await collection.create_index("file_unique_id", unique=True)
        await collection.create_index("msg_id")
    except: pass

def get_display_title(book_doc):
    """Priority: DB Title -> Filename"""
    db_title = book_doc.get('title')
    if db_title and db_title.strip() and db_title != "Unknown Title":
        return db_title.strip()
    
    fname = book_doc.get('file_name')
    if fname:
        return fname.replace('.epub', '').replace('_', ' ').replace('-', ' ').strip()
    return "Unknown Book"

def get_button_label(book_doc):
    """Strips chapter info for buttons."""
    full = get_display_title(book_doc)
    return re.sub(r'\s+(c|ch|chap|vol|v)\.?\s*\d+(?:[-–]\d+)?.*$', '', full, flags=re.IGNORECASE).strip()

def len_utf16(text):
    """Telegram requires UTF-16 code unit offsets."""
    return len(text.encode('utf-16-le')) // 2

# --- METADATA PARSER ---
def parse_epub_direct(file_path):
    meta = {"title": None, "author": "Unknown", "synopsis": "No synopsis.", "tags": "", "cover_image": None}
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            opf_path = None
            try:
                root = ET.fromstring(z.read('META-INF/container.xml'))
                for child in root.iter():
                    if child.get('full-path'): 
                        opf_path = child.get('full-path')
                        break
            except: pass
            if not opf_path:
                for n in z.namelist():
                    if n.endswith('.opf'): 
                        opf_path = n
                        break
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
                    if 'cover-image' in props: 
                        cover_href = item.get('href')
                        break
            if not cover_href:
                for elem in root.iter():
                    if elem.tag.split('}')[-1].lower() == 'meta' and elem.get('name') == 'cover':
                        cid = elem.get('content')
                        if manifest:
                            for item in manifest:
                                if item.get('id') == cid: 
                                    cover_href = item.get('href')
                                    break
            if not cover_href:
                for n in z.namelist():
                    if 'cover' in n.lower() and n.endswith(('.jpg','.png')): 
                        cover_href = n
                        break
            
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
                            if ps: 
                                meta['synopsis'] = "\n".join([p.text for p in ps[:6]])
                                break
                        except: pass
    except: pass
    if meta['tags'].endswith(", "): meta['tags'] = meta['tags'][:-2]
    return meta

# --- PYROGRAM CLIENT ---
# sleep_threshold=60 prevents the bot from freezing for too long on FloodWait
app = Client(
    "novel_bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60 
)

# --- INDEXING PROCESS ---
async def indexing_process(client, start_id, end_id, status_msg):
    global indexing_active, files_found, files_saved
    
    # Reset counters on new run
    files_found = 0
    files_saved = 0
    
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
                
                path = None
                try:
                    path = await client.download_media(message, file_name=temp_filename)
                except:
                    queue.task_done()
                    continue
                
                if not path:
                    queue.task_done()
                    continue

                meta = await asyncio.to_thread(parse_epub_direct, path)
                if os.path.exists(path): os.remove(path)

                if not meta['title']: 
                    meta['title'] = message.document.file_name.replace('.epub', '').replace('_', ' ')

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
                except DuplicateKeyError:
                    pass
                except Exception as e:
                    logger.error(f"DB Error: {e}")
                
                queue.task_done()
            except Exception as e:
                logger.error(f"Worker Error: {e}")
                queue.task_done()

    # Start 3 workers
    workers = [asyncio.create_task(worker()) for _ in range(3)]
    
    try:
        current_id = start_id
        BATCH_SIZE = 50 
        
        while current_id <= end_id and indexing_active:
            batch_end = min(current_id + BATCH_SIZE, end_id + 1)
            ids_to_fetch = list(range(current_id, batch_end))
            
            # Update status
            if status_msg and (current_id % 100 == 0):
                try:
                    await status_msg.edit(
                        f"🔄 **Scanning...**\n"
                        f"ID: `{current_id}`\n"
                        f"Found: `{files_found}`\n"
                        f"Saved: `{files_saved}`"
                    )
                except FloodWait as fw:
                    await asyncio.sleep(fw.value)
                except: pass

            if not ids_to_fetch: break

            try:
                messages = await client.get_messages(CHANNEL_ID, ids_to_fetch)
                
                if messages:
                    for message in messages:
                        # Filter EPUBs
                        if message and message.document and message.document.file_name and message.document.file_name.endswith('.epub'):
                            global files_found
                            files_found += 1
                            await queue.put(message)
                
            except FloodWait as e:
                logger.warning(f"FloodWait: Sleeping {e.value}s")
                await asyncio.sleep(e.value + 1)
                continue 
            except Exception as e:
                logger.error(f"Batch Error: {e}")
            
            current_id += BATCH_SIZE
            await asyncio.sleep(2) 
            
        await queue.join()
        
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        if status_msg:
            try:
                await status_msg.edit(
                    f"✅ **Done!**\n"
                    f"Scanned: `{end_id}`\n"
                    f"Found: `{files_found}`\n"
                    f"Saved: `{files_saved}`"
                )
            except: pass

# --- COMMANDS ---

@app.on_message(filters.command("start"))
async def start_handler(client, message):
    await message.reply("📚 **Novel Bot (Pyrogram)**")

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
    except Exception as e:
        await s.edit(f"❌ {e}")
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
    except Exception as e:
        await s.edit(f"❌ {e}")
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

# --- SEARCH & VIEW ---

@app.on_message(filters.text & ~filters.command(["start", "stats", "index", "stop_index", "export", "import", "migrate"]))
async def search_handler(client, message):
    q = message.text.strip()
    try:
        try:
            cnt = await collection.count_documents({"$text": {"$search": q}})
            if cnt>0: cur = collection.find({"$text": {"$search": q}}).sort([("score", {"$meta": "textScore"})])
            else: raise Exception
        except:
            reg = {"$or": [
                {"title": {"$regex": q, "$options": "i"}},
                {"author": {"$regex": q, "$options": "i"}},
                {"file_name": {"$regex": q, "$options": "i"}}
            ]}
            cnt = await collection.count_documents(reg)
            cur = collection.find(reg)
        
        res = await cur.limit(8).to_list(length=8)
        if not res: return await message.reply("❌ No matches.")

        sq = html.escape(q)
        txt = f"<blockquote>🔎 Search: <b>{sq}</b>\nMatches: <b>{cnt}</b></blockquote>"
        btns = []
        for b in res:
            label = get_button_label(b)[:40] 
            btns.append([InlineKeyboardButton(f"📖 {label}", callback_data=f"v:{str(b['_id'])}")])
        
        btns.append([InlineKeyboardButton("➡️ Next", callback_data=f"n:1:{q[:20]}")])
        await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)
    except Exception as e: await message.reply(f"⚠️ {e}")

@app.on_callback_query()
async def callback_handler(client, callback_query):
    d = callback_query.data
    
    if d.startswith("n:"):
        try:
            _, p, q = d.split(':', 2)
            p = int(p)
            try:
                cnt = await collection.count_documents({"$text": {"$search": q}})
                if cnt>0: cur = collection.find({"$text": {"$search": q}}).sort([("score", {"$meta": "textScore"})])
                else: raise Exception
            except:
                reg = {"$or": [{"title": {"$regex": q, "$options": "i"}}, {"file_name": {"$regex": q, "$options": "i"}}]}
                cnt = await collection.count_documents(reg)
                cur = collection.find(reg)
            
            res = await cur.skip(p*8).limit(8).to_list(length=8)
            if not res: return await callback_query.answer("End.", show_alert=True)
            
            btns = []
            for b in res:
                label = get_button_label(b)[:40]
                btns.append([InlineKeyboardButton(f"📖 {label}", callback_data=f"v:{str(b['_id'])}")])
            
            nav = []
            if p>0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"n:{p-1}:{q}"))
            nav.append(InlineKeyboardButton(f"{p+1}/{math.ceil(cnt/8)}", callback_data="nop"))
            if p < math.ceil(cnt/8)-1: nav.append(InlineKeyboardButton("➡️", callback_data=f"n:{p+1}:{q}"))
            btns.append(nav)
            
            await callback_query.edit_message_text(
                f"<blockquote>🔎 Search: <b>{html.escape(q)}</b>\nMatches: <b>{cnt}</b></blockquote>", 
                reply_markup=InlineKeyboardMarkup(btns), 
                parse_mode=ParseMode.HTML
            )
        except: await callback_query.answer("Error", show_alert=True)

    elif d.startswith("v:"):
        try:
            bid = d.split(':')[1]
            from bson.objectid import ObjectId
            b = await collection.find_one({"_id": ObjectId(bid)})
            if not b: return await callback_query.answer("Not found", show_alert=True)
            
            title = get_display_title(b)
            auth = b.get('author', 'Unknown')
            syn = b.get('synopsis', 'No synopsis.')
            
            # --- HEADER ENTITIES (Offset 0 relative to Message 1) ---
            header_text = f"{title}\nAuthor: {auth}"
            header_len = len_utf16(header_text)
            title_len = len_utf16(title)
            
            header_entities = [
                MessageEntity(type=MessageEntityType.BLOCKQUOTE, offset=0, length=header_len),
                MessageEntity(type=MessageEntityType.BOLD, offset=0, length=title_len)
            ]

            # --- SYNOPSIS ENTITIES (Offset 0 relative to Message 2) ---
            syn_label = "SYNOPSIS"
            syn_full_text = f"{syn_label}\n{syn}"
            syn_total_len = len_utf16(syn_full_text)
            label_len = len_utf16(syn_label)
            
            try:
                qt_type = MessageEntityType.EXPANDABLE_BLOCKQUOTE
            except AttributeError:
                qt_type = MessageEntityType.BLOCKQUOTE
            
            syn_entities = [
                # 1. Expandable Quote around EVERYTHING
                MessageEntity(type=qt_type, offset=0, length=syn_total_len),
                # 2. Bold + Underline for the label
                MessageEntity(type=MessageEntityType.BOLD, offset=0, length=label_len),
                MessageEntity(type=MessageEntityType.UNDERLINE, offset=0, length=label_len)
            ]
            
            kb = [[InlineKeyboardButton("📥 Download", callback_data=f"d:{bid}")]]
            
            await callback_query.message.delete()
            
            # 1. Header Message
            if b.get('cover_image'):
                try:
                    f = io.BytesIO(b['cover_image']); f.name="c.jpg"
                    await client.send_photo(
                        callback_query.message.chat.id, 
                        f, 
                        caption=header_text, 
                        caption_entities=header_entities, 
                        parse_mode=None
                    )
                except:
                    await client.send_message(
                        callback_query.message.chat.id, 
                        header_text, 
                        entities=header_entities, 
                        parse_mode=None
                    )
            else:
                await client.send_message(
                    callback_query.message.chat.id, 
                    header_text, 
                    entities=header_entities, 
                    parse_mode=None
                )
            
            # 2. Synopsis Message (With Expandable Blockquote)
            await client.send_message(
                callback_query.message.chat.id, 
                syn_full_text, 
                entities=syn_entities, 
                reply_markup=InlineKeyboardMarkup(kb),
                parse_mode=None
            )
        except Exception as e: 
            logger.error(f"View Error: {e}")
            await callback_query.answer("Error displaying.", show_alert=True)

    elif d.startswith("d:"):
        try:
            bid = d.split(':')[1]
            from bson.objectid import ObjectId
            b = await collection.find_one({"_id": ObjectId(bid)})
            await callback_query.answer("🚀 Sending...")
            try: await client.send_document(
                callback_query.message.chat.id, 
                b['file_id'], 
                caption=f"📖 {get_display_title(b)}"
            )
            except: 
                try: await client.copy_message(callback_query.message.chat.id, CHANNEL_ID, b['msg_id'])
                except: await callback_query.answer("File lost.", show_alert=True)
        except: pass

async def main():
    await ensure_indexes()
    logger.info("Bot Started")
    await app.start()
    
    global indexing_active
    try:
        last = await collection.find_one(sort=[("msg_id", -1)])
        if last: logger.info(f"Resume ID: {last.get('msg_id', 0) + 1}")
    except: pass
    
    await idle()
    await app.stop()

if __name__ == '__main__':
    app.run(main())
