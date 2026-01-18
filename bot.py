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

# --- WEB APP ---
web_app = Quart(__name__, template_folder='template')
serializer = URLSafeTimedSerializer(SECRET_KEY)

# --- TELEGRAM APP ---
# CRITICAL FIX: in_memory=True prevents session file corruption loops on containers
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

# --- WEB ROUTES ---

@web_app.route('/health')
async def health():
    return "OK", 200

@web_app.route('/login')
async def login():
    token = request.args.get('token')
    if not token:
        return "❌ No token provided. Use /url in the bot.", 400
    try:
        user_id = serializer.loads(token, max_age=3600)
        resp = await make_response(redirect(url_for('index')))
        resp.set_cookie('auth_token', serializer.dumps(user_id), max_age=86400*30)
        return resp
    except Exception:
        return "❌ Invalid or expired link. Generate a new one with /url.", 400

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
        cnt = await collection.count_documents({"$text": {"$search": query}})
        if cnt > 0:
            cursor = collection.find({"$text": {"$search": query}}).sort([("score", {"$meta": "textScore"})])
        else:
            reg_query = {"$regex": query, "$options": "i"}
            fallback_filter = {
                "$or": [
                    {"title": reg_query},
                    {"author": reg_query},
                    {"file_name": reg_query}
                ]
            }
            cnt = await collection.count_documents(fallback_filter)
            cursor = collection.find(fallback_filter)

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
            
        return await render_template(
            'index.html', 
            query=query, 
            results=results, 
            count=cnt,
            user_id=user_id,
            bot_username=BOT_USERNAME
        )
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return await render_template('index.html', query=query, results=[], count=0, error="An error occurred during search.", user_id=user_id)

@web_app.route('/api/download/<book_id>')
async def api_download(book_id):
    user_id = get_user_from_cookie()
    if not user_id:
        return jsonify({"status": "error", "message": "Not logged in. Use /url in bot."}), 401
    
    try:
        b = await collection.find_one({"_id": ObjectId(book_id)})
        if not b:
            return jsonify({"status": "error", "message": "Book not found"}), 404
        
        try:
            await app.send_document(
                chat_id=int(user_id),
                document=b['file_id'],
                caption=f"📖 {get_display_title(b)}\n\n<i>Sent via Web Interface</i>",
                parse_mode=ParseMode.HTML
            )
            return jsonify({"status": "ok"})
        except Exception as telegram_err:
            logger.error(f"Telegram Send Error: {telegram_err}")
            try:
                await app.copy_message(
