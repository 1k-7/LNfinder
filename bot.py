import asyncio
import math
import os
import logging
import warnings
import io
import zipfile
import html
import re
import base64
import urllib.request
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# --- DATABASE IMPORTS ---
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import TextIndexVersion
from pymongo.errors import DuplicateKeyError

# --- TELEGRAM IMPORTS ---
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

# --- WEB SERVER IMPORTS ---
from quart import Quart, request, render_template, redirect, url_for, jsonify, make_response
from hypercorn.config import Config
from hypercorn.asyncio import serve
from itsdangerous import URLSafeTimedSerializer

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", 0))
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))

# Database Config
MONGO_URL = os.environ.get("MONGO_URI") or os.environ.get("AZURE_URL")
DB_NAME = os.environ.get("DB_NAME", "novel_library")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "books")

# Web Config
PORT = int(os.environ.get("PORT", 8080))
PUBLIC_URL = os.environ.get("PUBLIC_URL", f"http://0.0.0.0:{PORT}")
SECRET_KEY = os.environ.get("SECRET_KEY", "CHANGE_THIS_TO_SECURE_RANDOM")

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BookBot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# --- DATABASE CONNECTION ---
try:
    if not MONGO_URL:
        raise ValueError("MONGO_URI is missing.")
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info("✅ Connected to MongoDB.")
except Exception as e:
    logger.critical(f"❌ DB Connection Failed: {e}")
    exit(1)

# --- WEB APP INITIALIZATION ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)

# --- HELPER FUNCTIONS ---
def get_user_from_cookie():
    token = request.cookies.get('auth_token')
    if not token: return None
    try:
        return serializer.loads(token, max_age=86400*30)
    except:
        return None

def get_display_title(book_doc):
    """Returns a clean title from the document."""
    if book_doc.get('title') and book_doc['title'] != "Unknown Title":
        return book_doc['title'].strip()
    return book_doc.get('file_name', 'Unknown Book').replace('.epub', '').replace('_', ' ').strip()

def format_strict_search(query):
    """Wraps terms in quotes to enforce MongoDB 'AND' logic."""
    terms = query.split()
    # MongoDB text search treats "term1 term2" as OR by default. 
    # "\"term1\" \"term2\"" forces AND.
    return " ".join([f'"{t}"' for t in terms])

# --- INDEXING & ADMIN ---
async def ensure_indexes(rebuild=False):
    """Creates or Rebuilds indexes."""
    try:
        if rebuild:
            await collection.drop_indexes()
            logger.info("♻️ Indexes dropped for rebuild.")
        
        # Text Index for Search
        await collection.create_index(
            [("title", "text"), ("synopsis", "text"), ("tags", "text")],
            weights={"title": 10, "tags": 5, "synopsis": 1},
            name="BookTextIndex"
        )
        # Unique Index for File IDs
        await collection.create_index("file_unique_id", unique=True)
        logger.info("✅ Indexes verified.")
    except Exception as e:
        logger.error(f"❌ Index Error: {e}")

# --- WEB ROUTES ---
@web_app.route('/cover/<book_id>')
async def serve_cover(book_id):
    """Lazy loads cover image from DB. Returns placeholder if missing."""
    try:
        book = await collection.find_one({"_id": ObjectId(book_id)}, {"cover_image": 1})
        if book and book.get('cover_image'):
            response = await make_response(book['cover_image'])
            response.headers['Content-Type'] = 'image/jpeg'
            response.headers['Cache-Control'] = 'public, max-age=86400'
            return response
    except:
        pass
    return "No Cover", 404

@web_app.route('/')
async def index():
    user_id = get_user_from_cookie()
    return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)

@web_app.route('/search')
async def search():
    user_id = get_user_from_cookie()
    query = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    limit = 30
    skip = (page - 1) * limit

    if not query:
        return await render_template('index.html', query="", results=[], count=0, user_id=user_id, bot_username=BOT_USERNAME)

    try:
        # STRICT AND SEARCH
        search_query = format_strict_search(query)
        
        # Count Matches
        count = await collection.count_documents({"$text": {"$search": search_query}})
        
        # Fetch Results
        cursor = collection.find(
            {"$text": {"$search": search_query}},
            {"score": {"$meta": "textScore"}, "cover_image": 0} # Exclude cover image for speed
        ).sort([("score", {"$meta": "textScore"})]).skip(skip).limit(limit)
        
        books = await cursor.to_list(length=limit)
        
        results = []
        for b in books:
            syn = b.get('synopsis', 'No synopsis available.').strip()
            results.append({
                "_id": str(b['_id']),
                "title": get_display_title(b),
                "author": b.get('author', 'Unknown'),
                "synopsis": syn,
                "has_more": len(syn) > 200
            })

        total_pages = math.ceil(count / limit)
        return await render_template('index.html', query=query, results=results, count=count, page=page, total_pages=total_pages, user_id=user_id, bot_username=BOT_USERNAME)

    except Exception as e:
        logger.error(f"Search Error: {e}")
        return await render_template('index.html', query=query, results=[], count=0, error="Search failed. Try fewer keywords.", user_id=user_id)

@web_app.route('/login')
async def login():
    token = request.args.get('token')
    if not token: return "Invalid Token", 400
    try:
        user_id = serializer.loads(token, max_age=3600)
        resp = await make_response(redirect(url_for('index')))
        resp.set_cookie('auth_token', serializer.dumps(user_id), max_age=86400*30)
        return resp
    except:
        return "Expired or Invalid Link", 400

@web_app.route('/api/download/<book_id>')
async def api_download(book_id):
    user_id = get_user_from_cookie()
    if not user_id: return jsonify({"status": "error", "message": "Unauthorized"}), 401
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)})
        if not b: return jsonify({"status": "error", "message": "Book not found"}), 404
        
        await bot.send_document(
            chat_id=int(user_id),
            document=b['file_id'],
            caption=f"📖 <b>{get_display_title(b)}</b>\n\n<i>Sent via Web Library</i>",
            parse_mode=ParseMode.HTML
        )
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- TELEGRAM BOT ---
if not os.path.exists("sessions"): os.makedirs("sessions")
bot = Client("sessions/bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
BOT_USERNAME = ""

@bot.on_message(filters.command("start"))
async def start_cmd(client, message):
    # Check for Deep Linking (Download via Web)
    if len(message.command) > 1 and message.command[1].startswith("d_"):
        try:
            book_id = message.command[1].split("_", 1)[1]
            b = await collection.find_one({"_id": ObjectId(book_id)})
            if b:
                await message.reply_document(b['file_id'], caption=f"📖 {get_display_title(b)}")
                return
        except: pass
    
    await message.reply(
        "👋 <b>Welcome to the Library!</b>\n\n"
        "Send me the name of a novel to search.\n"
        "Type /url to access the Web Dashboard.",
        parse_mode=ParseMode.HTML
    )

@bot.on_message(filters.command("url"))
async def url_cmd(client, message):
    token = serializer.dumps(message.from_user.id)
    link = f"{PUBLIC_URL}/login?token={token}"
    await message.reply(
        f"🔗 <b>Web Dashboard Login</b>\n\n<a href='{link}'>Click here to access the library</a>\n\n<i>Valid for 1 hour.</i>",
        disable_web_page_preview=True
    )

@bot.on_message(filters.command("fix_search") & filters.user(ADMIN_ID))
async def fix_search_cmd(client, message):
    msg = await message.reply("🛠 <b>Rebuilding Indexes...</b>")
    await ensure_indexes(rebuild=True)
    await msg.edit("✅ <b>Search Indexes Fixed!</b>")

@bot.on_message(filters.command("stats"))
async def stats_cmd(client, message):
    books = await collection.count_documents({})
    covers = await collection.count_documents({"cover_image": {"$ne": None}})
    await message.reply(f"📊 <b>Library Stats</b>\n\n📚 Books: <code>{books}</code>\n🖼 Covers: <code>{covers}</code>")

@bot.on_message(filters.text & ~filters.command(["start", "url", "stats", "fix_search"]))
async def telegram_search(client, message):
    query = message.text.strip()
    if len(query) < 2: return
    
    search_query = format_strict_search(query)
    count = await collection.count_documents({"$text": {"$search": search_query}})
    
    if count == 0:
        return await message.reply("❌ <b>No matches found.</b>\nTry specific keywords.")
    
    cursor = collection.find(
        {"$text": {"$search": search_query}},
        {"score": {"$meta": "textScore"}}
    ).sort([("score", {"$meta": "textScore"})]).limit(8)
    
    results = await cursor.to_list(length=8)
    
    buttons = []
    for b in results:
        btn_text = f"{get_display_title(b)[:30]}..."
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"view:{str(b['_id'])}")])
    
    await message.reply(
        f"🔎 <b>Search Results:</b> <code>{html.escape(query)}</code>\nFound {count} books.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@bot.on_callback_query(filters.regex(r"^view:"))
async def view_book(client, callback):
    try:
        book_id = callback.data.split(":")[1]
        b = await collection.find_one({"_id": ObjectId(book_id)})
        
        if not b:
            return await callback.answer("Book not found.", show_alert=True)
        
        title = get_display_title(b)
        author = b.get('author', 'Unknown')
        synopsis = b.get('synopsis', 'No synopsis available.').strip()
        
        # Blockquote Formatting
        text = (
            f"<blockquote><b>{html.escape(title)}</b>\n"
            f"👤 {html.escape(author)}</blockquote>\n\n"
            f"<blockquote expandable>{html.escape(synopsis)}</blockquote>"
        )
        
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Download EPUB", callback_data=f"dl:{book_id}")]
        ])
        
        # Delete old message to clean up chat, or edit if preferred
        await callback.message.delete() 
        
        if b.get('cover_image'):
            f = io.BytesIO(b['cover_image'])
            f.name = "cover.jpg"
            await client.send_photo(callback.message.chat.id, f, caption=text, reply_markup=markup)
        else:
            await client.send_message(callback.message.chat.id, text, reply_markup=markup)
            
    except Exception as e:
        logger.error(f"View Error: {e}")
        await callback.answer("Error viewing book.")

@bot.on_callback_query(filters.regex(r"^dl:"))
async def download_book(client, callback):
    book_id = callback.data.split(":")[1]
    b = await collection.find_one({"_id": ObjectId(book_id)})
    if b:
        await callback.answer("🚀 Sending file...")
        await client.send_document(callback.message.chat.id, b['file_id'], caption=f"📖 {get_display_title(b)}")
    else:
        await callback.answer("File not found.", show_alert=True)

# --- MAIN EXECUTION ---
async def main():
    await ensure_indexes()
    
    # Start Bot
    await bot.start()
    global BOT_USERNAME
    me = await bot.get_me()
    BOT_USERNAME = me.username
    logger.info(f"🤖 Bot active: @{BOT_USERNAME}")
    
    # Start Web Server
    config = Config()
    config.bind = [f"0.0.0.0:{PORT}"]
    await serve(web_app, config)
    
    await idle()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())