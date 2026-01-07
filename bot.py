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
from telethon import TelegramClient, events, Button
from telethon.tl.types import (
    MessageEntityBold, 
    MessageEntityUnderline, 
    MessageEntityBlockquoteExpandable
)
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError

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

legacy_collections = []
for uri in LEGACY_URIS:
    try:
        cli = AsyncIOMotorClient(uri)
        legacy_collections.append(cli[DB_NAME][COLLECTION_NAME])
    except: pass

# --- GLOBAL STATE ---
indexing_active = False
files_processed = 0

# --- HELPERS ---
def len_utf16(text):
    """Telegram requires UTF-16 offsets for entities."""
    return len(text.encode('utf-16-le')) // 2

async def ensure_indexes():
    try:
        await collection.create_index([("title", "text"), ("author", "text"), ("synopsis", "text"), ("tags", "text"), ("file_name", "text")])
        await collection.create_index("file_unique_id", unique=True)
        await collection.create_index("msg_id")
    except: pass

def get_display_title(book_doc):
    """
    Returns the Title from DB. 
    Only falls back to File Name if Title is explicitly None/Empty.
    """
    # 1. Trust DB Title
    if book_doc.get('title'):
        return book_doc['title'].strip()
    
    # 2. Fallback to Filename (Cleaned)
    fname = book_doc.get('file_name')
    if fname:
        return fname.replace('.epub', '').replace('_', ' ').strip()
        
    return "Unknown Title"

def get_button_label(book_doc):
    """
    Takes the Display Title and strips 'c1-23', 'ch 5', etc for the button label.
    """
    full_title = get_display_title(book_doc)
    # Regex to remove chapter info at the end (e.g., "Title c1-20" -> "Title")
    clean_label = re.sub(r'\s+(c|ch|chap|vol|v)\.?\s*\d+(?:[-–]\d+)?.*$', '', full_title, flags=re.IGNORECASE)
    return clean_label.strip()

# --- METADATA PARSER ---
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
                    tag = elem.tag.split('}')[-1].lower()
                    if tag == 'meta' and elem.get('name') == 'cover':
                        cid = elem.get('content')
                        if manifest:
                            for item in manifest:
                                if item.get('id') == cid: cover_href = item.get('href'); break
                        break
            
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

# --- INDEXER ---
async def indexing_process(client, start_id, end_id, status_msg=None):
    global indexing_active, files_processed
    queue = asyncio.Queue(maxsize=30)
    
    if status_msg:
        try:
            await status_msg.edit("🚀 **Scanning...**")
        except:
            pass

    async def worker():
        while indexing_active:
            try:
                msg = await queue.get()
                fname = f"temp_{msg.id}.epub"
                
                path = None
                try: 
                    path = await msg.download_media(file=fname)
                except: 
                    queue.task_done()
                    continue
                
                if not path: 
                    queue.task_done()
                    continue
                    
                meta = await asyncio.to_thread(parse_epub_direct, path)
                if os.path.exists(path): os.remove(path)

                if not meta['title']: 
                    # Clean filename as title if metadata is missing
                    meta['title'] = msg.file.name.replace('.epub', '').replace('_', ' ')

                try:
                    await collection.insert_one({
                        "file_id": msg.file.id,
                        "file_unique_id": str(msg.file.id),
                        "file_name": msg.file.name,
                        "title": meta['title'],
                        "author": meta['author'],
                        "synopsis": meta['synopsis'],
                        "tags": meta['tags'],
                        "cover_image": meta['cover_image'],
                        "msg_id": msg.id
                    })
                    global files_processed
                    files_processed += 1
                    print(f"✅ {meta['title']}")
                except DuplicateKeyError:
                    pass
                
                queue.task_done()
            except: 
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(5)]
    
    try:
        curr = start_id
        while curr <= end_id and indexing_active:
            batch = min(curr + 50, end_id + 1)
            ids = list(range(curr, batch))
            if not ids: break
            try:
                msgs = await client.get_messages(CHANNEL_ID, ids=ids)
                for m in msgs:
                    if m and m.file and m.file.name and m.file.name.endswith('.epub'):
                        await queue.put(m)
                        
                if status_msg and files_processed % 20 == 0:
                    try: 
                        await status_msg.edit(f"🔄 Scan: `{curr}`\nSaved: `{files_processed}`")
                    except: 
                        pass
            except: 
                pass
            
            curr += 50
            await asyncio.sleep(0.5)
        await queue.join()
    finally:
        for w in workers: w.cancel()
        indexing_active = False
        if status_msg: 
            try: 
                await status_msg.edit(f"✅ Done! Added: `{files_processed}`")
            except: 
                pass

# --- STARTUP ---
async def startup():
    logger.info("⚙️ Starting...")
    asyncio.create_task(ensure_indexes())
    mid = 0
    try:
        l = await collection.find_one(sort=[("msg_id", -1)])
        if l: mid = l.get('msg_id', 0)
    except: pass
    logger.info(f"📍 Resume ID: {mid+1}")

bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
PAGE_SIZE = 8

# --- HANDLERS ---
@bot.on(events.NewMessage(pattern='/start'))
async def start(e): await e.respond("📚 **Novel Bot**")

@bot.on(events.NewMessage(pattern='/stats'))
async def stats(e):
    try:
        c = await collection.count_documents({})
        cv = await collection.count_documents({"cover_image": {"$ne": None}})
        await e.respond(f"📊 **Stats**\n📚 Books: `{c}`\n🖼️ Covers: `{cv}`")
    except: pass

@bot.on(events.NewMessage(pattern='/index', from_users=[ADMIN_ID]))
async def idx(e):
    global indexing_active
    if indexing_active: return await e.respond("⚠️ Running.")
    args = e.text.split()
    s, en = 1, int(args[1]) if len(args)==2 else int(args[2])
    if len(args)==3: s = int(args[1])
    indexing_active = True
    m = await e.respond(f"🚀 Index {s}-{en}")
    asyncio.create_task(indexing_process(bot, s, en, m))

@bot.on(events.NewMessage(pattern='/stop_index', from_users=[ADMIN_ID]))
async def stop(e):
    global indexing_active; indexing_active = False
    await e.respond("🛑 Stopping...")

@bot.on(events.NewMessage(pattern='/export', from_users=[ADMIN_ID]))
async def exp(e):
    s = await e.respond("📦 Exporting...")
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
        await bot.send_file(e.chat_id, "lib.zip", caption="✅ Backup")
    except Exception as x: await e.respond(f"❌ {x}")
    finally:
        if os.path.exists("lib.json"): os.remove("lib.json")
        if os.path.exists("lib.zip"): os.remove("lib.zip")

@bot.on(events.NewMessage(pattern='/import', from_users=[ADMIN_ID]))
async def imp(e):
    r = await e.get_reply_message()
    if not r or not r.file: return
    s = await e.respond("📥 Importing...")
    p = await r.download_media()
    try:
        if zipfile.is_zipfile(p):
            with zipfile.ZipFile(p,'r') as z: z.extractall(); p=z.namelist()[0]
        with open(p,'r') as f: d=json.load(f)
        for c in [d[i:i+50] for i in range(0,len(d),50)]:
            for x in c:
                if x.get('cover_image'): 
                    try: x['cover_image'] = base64.b64decode(x['cover_image'])
                    except: x['cover_image'] = None
                if '_id' in x: del x['_id']
                try: await collection.replace_one({"file_unique_id":x['file_unique_id']},x,upsert=True)
                except: pass
        await s.edit("✅ Done")
    except Exception as x: await e.respond(f"❌ {x}")
    finally:
        if os.path.exists(p): os.remove(p)

@bot.on(events.NewMessage(pattern='/migrate', from_users=[ADMIN_ID]))
async def mig(e):
    if not legacy_collections: return await e.respond("❌ No Legacy DBs")
    s = await e.respond("🚀 Migrating...")
    t=0
    for c in legacy_collections:
        async for d in c.find({}):
            if '_id' in d: del d['_id']
            try: await collection.insert_one(d); t+=1
            except: pass
            if t%100==0: await s.edit(f"📥 {t}")
    await s.edit(f"✅ {t} Done")

# --- SEARCH & VIEW ---
@bot.on(events.NewMessage)
async def search(e):
    if e.text.startswith('/'): return
    q = e.text.strip()
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
        if not res: return await e.respond("❌ No matches.")

        sq = html.escape(q)
        txt = f"<blockquote>🔎 Search: <b>{sq}</b>\nMatches: <b>{cnt}</b></blockquote>"
        btns = []
        for b in res:
            # CLEAN LABEL FOR BUTTON (removes c1-23)
            label = get_button_label(b)[:40] 
            btns.append([Button.inline(f"📖 {label}", data=f"v:{str(b['_id'])}")])
        
        btns.append([Button.inline("➡️ Next", data=f"n:1:{q[:20]}")])
        await e.respond(txt, buttons=btns, parse_mode='html')
    except Exception as x: await e.respond(f"⚠️ {x}")

@bot.on(events.CallbackQuery)
async def cb(e):
    d = e.data.decode()
    
    if d.startswith("n:"): # Nav
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
            if not res: return await e.answer("End.", alert=True)
            
            btns = []
            for b in res:
                label = get_button_label(b)[:40]
                btns.append([Button.inline(f"📖 {label}", data=f"v:{str(b['_id'])}")])
            
            nav = []
            if p>0: nav.append(Button.inline("⬅️", data=f"n:{p-1}:{q}"))
            nav.append(Button.inline(f"{p+1}/{math.ceil(cnt/8)}", data="nop"))
            if p < math.ceil(cnt/8)-1: nav.append(Button.inline("➡️", data=f"n:{p+1}:{q}"))
            btns.append(nav)
            
            await e.edit(f"<blockquote>🔎 Search: <b>{html.escape(q)}</b>\nMatches: <b>{cnt}</b></blockquote>", buttons=btns, parse_mode='html')
        except: await e.answer("Error", alert=True)

    elif d.startswith("v:"): # View
        try:
            bid = d.split(':')[1]
            from bson.objectid import ObjectId
            b = await collection.find_one({"_id": ObjectId(bid)})
            if not b: return await e.answer("Not found", alert=True)
            
            # --- FULL TITLE (From DB) ---
            title = get_display_title(b)
            auth = b.get('author', 'Unknown')
            syn = b.get('synopsis', 'No synopsis.')
            
            # --- MANUALLY CONSTRUCT ENTITIES ---
            header_text = f"{title}\nAuthor: {auth}\n\n"
            syn_label = "SYNOPSIS\n"
            full_text = header_text + syn_label + syn
            
            # Offsets
            off_title = 0
            len_title = len_utf16(title)
            
            off_syn_block = len_utf16(header_text)
            len_syn_block = len_utf16(syn_label + syn)
            
            off_syn_label = off_syn_block
            len_syn_label = len_utf16("SYNOPSIS")
            
            entities = [
                MessageEntityBold(offset=off_title, length=len_title),
                MessageEntityBlockquoteExpandable(offset=off_syn_block, length=len_syn_block),
                MessageEntityBold(offset=off_syn_label, length=len_syn_label),
                MessageEntityUnderline(offset=off_syn_label, length=len_syn_label)
            ]
            
            btn = [[Button.inline("📥 Download", data=f"d:{bid}")]]
            
            await e.delete()
            if b.get('cover_image'):
                f = io.BytesIO(b['cover_image']); f.name="c.jpg"
                await bot.send_file(e.chat_id, f, caption=full_text, formatting_entities=entities, buttons=btn)
            else:
                await bot.send_message(e.chat_id, full_text, formatting_entities=entities, buttons=btn)
        except Exception as x: 
            logger.error(f"View Error: {x}")
            await e.answer("Error displaying.", alert=True)

    elif d.startswith("d:"): # Download
        try:
            bid = d.split(':')[1]
            from bson.objectid import ObjectId
            b = await collection.find_one({"_id": ObjectId(bid)})
            await e.answer("🚀 Sending...")
            try: await bot.send_file(e.chat_id, b['file_id'], caption=f"📖 {get_display_title(b)}")
            except: 
                try: await bot.forward_messages(e.chat_id, b['msg_id'], CHANNEL_ID)
                except: await e.answer("File lost.", alert=True)
        except: pass

print("Bot Running...")
bot.loop.run_until_complete(startup())
bot.run_until_disconnected()
