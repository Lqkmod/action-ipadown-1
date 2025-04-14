# --- START OF FILE main.py ---

import asyncio
import time
import telebot
import qrcode
import io
import requests
import tempfile
import subprocess
import sys
import random
import json
import os
import sqlite3
import hashlib # Keep for now, might be used elsewhere indirectly or planned
import zipfile # Keep for now
from datetime import datetime, timedelta, date
from threading import Lock
from bs4 import BeautifulSoup
from PIL import Image, ImageOps, ImageDraw, ImageFont # Keep for QR code and potentially future image features
from io import BytesIO
from urllib.parse import urljoin, urlparse, urldefrag # Keep for now
from telebot import TeleBot, types
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import google.generativeai as genai # For /gg
from gtts import gTTS # For /voice
import psutil # For /status
import platform # For /status
import aiohttp # Needed if /share helper functions were active

# --- Bot Configuration ---
TOKEN = '6939568666:AAEJNPIsn3qxN4OFv_0wqJgLV0XVFFq8L4I'  # <--- REPLACE WITH YOUR BOT TOKEN
ADMIN_ID = 7193749511   # <--- REPLACE WITH YOUR ADMIN USER ID
ALLOWED_GROUP_ID = -1002191171631 # <--- REPLACE WITH YOUR TARGET GROUP ID
DB_FILE = 'user_data.db'
VIP_PRICE = "60K" # Displayed price
VIP_DURATION_DAYS = 30 # Default VIP duration in days
ADMIN_USERNAME = "dichcutelegram" # <--- REPLACE WITH YOUR TELEGRAM USERNAME (no @)
BOT_USERNAME = "spampython_bot"  # <--- REPLACE WITH YOUR BOT'S USERNAME (no @)
CHAT_GROUP_LINK = "https://t.me/dinotoolk" # <--- REPLACE WITH YOUR CHAT GROUP LINK
GEMINI_API_KEY = 'YOUR_GEMINI_API_KEY' # <--- REPLACE WITH YOUR GEMINI API KEY
PAYMENT_IMAGE_URL = 'https://i.imgur.com/FEC2Gbf.jpeg' # <--- REPLACE WITH YOUR QR CODE IMAGE URL for /mua
API_KEY_THACHMORA = "autp-250" # API Key for spamvip (Consider hiding this better)

# --- Feature Settings ---
FREE_SPAM_COOLDOWN = 100 # Seconds
VIP_SPAM_COOLDOWN = 50 # Seconds
FREE_SPAM_LIMIT = 2 # Max loops for /spam
VIP_SMS_LIMIT = 50 # Max loops for /smsvip
VIP_CALL_LIMIT = 30 # Max loops for /spamvip (Call)
FREE_SHARE_LIMIT = 400
VIP_SHARE_LIMIT = 1000
FREE_SHARE_COOLDOWN = timedelta(seconds=300)
VIP_SHARE_COOLDOWN = timedelta(seconds=100)

# --- Global Variables ---
bot = TeleBot(TOKEN, parse_mode='Markdown') # Default parse mode
admins = {ADMIN_ID}
allowed_users = [] # Populated from DB on start
bot_active = True
admin_mode = False
free_spam_enabled = True # Controls /spam command availability
private_chat_enabled = False # Controls if bot works outside group
start_time = time.time()
last_command_time = {} # User cooldowns for specific commands
last_usage = {} # General cooldown for spam commands
user_cooldowns = {} # Cooldown for /share (if active)
share_count = {}
share_log = []
# Tracks running spam processes {sdt: [process1, process2, ...]} - MODIFIED TO LIST
running_spams = {}
blacklist = {"112", "113", "114", "115"} # Add emergency numbers
global_lock = Lock() # For potential thread safety needs
users_requested_payment = {} # Tracks users who used /mua

# --- Database Setup ---
def init_db():
    """Initializes the SQLite database and table if they don't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            expiration_time TEXT, -- Store as ISO format string (YYYY-MM-DD HH:MM:SS) or NULL for permanent
            username TEXT -- Store username for easier identification
        )
    ''')
    conn.commit()
    conn.close()
    print("Database initialized.")

def load_users_from_database():
    """Loads active VIP user IDs from the database into the allowed_users list."""
    global allowed_users
    allowed_users.clear()
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Select users whose expiration time is in the future OR is NULL (permanent)
        cursor.execute('SELECT user_id FROM users WHERE expiration_time IS NULL OR expiration_time >= ?', (now_str,))
        rows = cursor.fetchall()
        allowed_users = [row[0] for row in rows]
        print(f"Loaded {len(allowed_users)} active VIP users from database.")
    except sqlite3.Error as e:
        print(f"Error loading users from database: {e}")
    finally:
        if conn:
            conn.close()

def save_user_to_database(user_id, expiration_time, username=None):
    """Saves or updates a user's VIP status in the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # Format expiration_time to string or None
        expiration_str = expiration_time.strftime('%Y-%m-%d %H:%M:%S') if expiration_time else None
        cursor.execute(
            '''
            INSERT OR REPLACE INTO users (user_id, expiration_time, username)
            VALUES (?, ?, ?)
            ''',
            (user_id, expiration_str, username)
        )
        conn.commit()
        print(f"Saved user {user_id} (Username: {username}, Expiry: {expiration_str if expiration_str else 'Permanent'}) to database.")
    except sqlite3.Error as e:
        print(f"Error saving user {user_id} to database: {e}")
    finally:
        if conn:
            conn.close()
    load_users_from_database() # Reload the list after changes

def remove_user_from_database(user_id):
    """Removes a user from the database."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        conn.commit()
        print(f"Removed user {user_id} from database.")
    except sqlite3.Error as e:
        print(f"Error removing user {user_id} from database: {e}")
    finally:
        if conn:
            conn.close()
    load_users_from_database() # Reload the list

def delete_expired_users_from_db():
    """Deletes users whose VIP subscription has expired."""
    deleted_count = 0
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Delete users whose expiration time is not NULL and is in the past
        deleted_count = cursor.execute('DELETE FROM users WHERE expiration_time IS NOT NULL AND expiration_time < ?', (now_str,)).rowcount
        conn.commit()
        if deleted_count > 0:
            print(f"Deleted {deleted_count} expired users from database.")
    except sqlite3.Error as e:
        print(f"Error deleting expired users from database: {e}")
    finally:
        if conn:
            conn.close()
    if deleted_count > 0:
        load_users_from_database() # Reload the list if changes were made
    return deleted_count

# --- Gemini AI Setup ---
try:
    if GEMINI_API_KEY and GEMINI_API_KEY != 'YOUR_GEMINI_API_KEY':
        genai.configure(api_key=GEMINI_API_KEY)
        generation_config = {
            "temperature": 1, "top_p": 0.95, "top_k": 64,
            "max_output_tokens": 8192, "response_mime_type": "text/plain",
        }
        gemini_model = genai.GenerativeModel(model_name="gemini-pro", generation_config=generation_config)
        gemini_chat_session = gemini_model.start_chat(history=[])
        print("Gemini AI Model initialized successfully.")
    else:
        gemini_model = None
        print("Gemini AI key not configured. /gg command disabled.")
except Exception as e:
    print(f"Error initializing Gemini AI: {e}")
    gemini_model = None # Disable Gemini features if init fails

# --- Helper Functions ---
def get_time_vietnam():
    # Consider using pytz for accurate timezone handling if server isn't in Vietnam time
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def is_admin(user_id):
    """Checks if a user ID belongs to an admin."""
    return user_id in admins

def delete_user_message(message):
    """Safely attempts to delete the user's command message."""
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except telebot.apihelper.ApiTelegramException as e:
        # Log errors except for the common "message to delete not found"
        if 'message to delete not found' not in str(e) and 'message can\'t be deleted' not in str(e):
             print(f"Could not delete message {message.message_id} in chat {message.chat.id}: {e}")
    except Exception as e:
        print(f"Unexpected error deleting message {message.message_id}: {e}")


def check_bot_status(message):
    """Checks if the bot is active and respects admin mode. Returns True if okay to proceed."""
    if not bot_active:
        bot.reply_to(message, '🤖 Bot hiện đang tạm dừng hoạt động.')
        delete_user_message(message) # Clean up command
        return False
    if admin_mode and not is_admin(message.from_user.id):
        bot.reply_to(message, '⚙️ Chế độ bảo trì đang bật, chỉ Admin có thể dùng lệnh.')
        delete_user_message(message) # Clean up command
        return False
    return True

def check_group_or_chat_mode(message):
    """Checks if the command is allowed in the current chat context. Returns True if okay."""
    if not private_chat_enabled and message.chat.id != ALLOWED_GROUP_ID:
        try:
            bot.reply_to(message, f'🔔 Vui lòng sử dụng bot trong nhóm chat chỉ định: {CHAT_GROUP_LINK}')
            delete_user_message(message) # Clean up command
        except Exception as e:
             print(f"Error replying or deleting in wrong chat {message.chat.id}: {e}")
        return False
    return True

def get_user_mention(user):
    """Gets a formatted mention for a user, preferring username."""
    if not user: # Add a check in case user object is None
        return "Người dùng không xác định"
    if user.username:
        return f"@{user.username}"
    else:
        # Use Markdown link for better clickability
        return f"[{user.first_name or 'User'}](tg://user?id={user.id})"

def mask_phone(phone_number):
    """Masks a phone number, showing first 3 and last 3 digits."""
    if isinstance(phone_number, str) and len(phone_number) > 6:
        return f"{phone_number[:3]}***{phone_number[-3:]}"
    elif isinstance(phone_number, str) and len(phone_number) > 0 :
         return f"{phone_number[0]}***{phone_number[-1]}" # Mask shorter numbers differently
    return "***" # Mask completely if invalid or very short


# --- Bot Command Handlers ---

@bot.message_handler(commands=['start'])
def send_welcome(message):
    user = message.from_user
    # Use the get_user_mention function defined above
    welcome_text = f"👋 Xin chào {get_user_mention(user)}!\n\nTôi là *DINO BOT* 🦖, trợ lý của bạn.\nChọn một tùy chọn bên dưới:"

    markup = InlineKeyboardMarkup(row_width=2)
    btn_buy = InlineKeyboardButton("💰 Mua VIP", callback_data='buy_vip')
    btn_cmd = InlineKeyboardButton("📜 Danh Sách Lệnh", callback_data='show_commands')
    btn_grp = InlineKeyboardButton("👥 Nhóm Chat", url=CHAT_GROUP_LINK)
    btn_adm = InlineKeyboardButton("🧑‍💻 Liên Hệ Admin", url=f"https://t.me/{ADMIN_USERNAME}")
    markup.add(btn_buy, btn_cmd) # First row
    markup.add(btn_grp, btn_adm) # Second row

    try:
        bot.reply_to(message, welcome_text, reply_markup=markup, parse_mode='Markdown') # Ensure parse_mode
    except Exception as e:
        print(f"Error sending welcome message: {e}")
    delete_user_message(message) # Delete the /start command


# --- Callback Handler for /start menu ---
@bot.callback_query_handler(func=lambda call: call.data in ['buy_vip', 'show_commands'])
def handle_menu_callbacks(call):
    chat_id = call.message.chat.id
    user = call.from_user # <<< Use call.from_user to identify who clicked
    user_id = user.id
    message_id = call.message.message_id # Message to potentially edit

    bot.answer_callback_query(call.id) # Acknowledge the callback immediately

    if call.data == 'buy_vip':
        # Pass the 'call' object to mua_command to access call.from_user
        mua_command(call.message, from_callback=True, callback_user=user) # Pass user object
    elif call.data == 'show_commands':
        help_text = get_help_text(user_id)
        try:
            # Edit the welcome message to show commands instead of sending a new one
            bot.edit_message_text(help_text, chat_id, message_id, reply_markup=call.message.reply_markup, parse_mode='Markdown')
        except telebot.apihelper.ApiTelegramException as e:
            if "message is not modified" in str(e):
                pass # Ignore if the text is already the help text
            else:
                print(f"Error editing message for commands: {e}")
                # Fallback: send as a new message if editing fails badly
                bot.send_message(chat_id, help_text, parse_mode='Markdown')


# --- Help Text Function ---
def get_help_text(user_id):
    """Generates the help text, showing admin commands if the user is an admin."""
    is_user_admin = is_admin(user_id)
    name_bot = "DINO TOOL"
    response_message = (
        f"🦖 *{name_bot} - Danh Sách Lệnh*\n\n"
        f"*Lệnh Chung:*\n"
        f"`/start` - Menu chính & giới thiệu\n"
        f"`/mua` - 💰 Mua VIP ({VIP_PRICE}/{VIP_DURATION_DAYS} ngày)\n"
        f"`/plan` - 📅 Kiểm tra hạn VIP của bạn\n" # <<< ADDED /plan
        f"`/id` - 🆔 Lấy User ID Telegram\n"
        f"`/qr` <nội dung> - 🖼️ Tạo mã QR\n"
        f"`/voice` <văn bản> - 🗣️ Text-to-Speech (Tiếng Việt)\n"
        f"`/gg` <câu hỏi> - 🧠 Chat với Google Gemini AI\n"
        #f"`/generate_image` <mô tả> - 🎨 Tạo ảnh (Chưa có)\n"
        f"`/face` - 🧑 Ảnh mặt người ngẫu nhiên\n"
        f"`/tiktok` <link> - 🎵 Tải video/nhạc TikTok\n"
        f"`/tool` - 🛠️ Link tải công cụ khác\n"
        f"`/time` - ⏱️ Thời gian hoạt động của Bot\n"
        f"`/ad` - 🧑‍💻 Thông tin Admin\n"
        f"`/tv` - 🇻🇳 Đổi ngôn ngữ TG sang Tiếng Việt\n\n"

        f"*Lệnh SPAM:*\n"
        f"`/spam` <SĐT> <lần> - 💣 Spam SMS (Free, max {FREE_SPAM_LIMIT}, CD {FREE_SPAM_COOLDOWN}s)\n"
        f"`/smsvip` <SĐT> <lần> - 🌟 Spam SMS (VIP, max {VIP_SMS_LIMIT}, CD {VIP_SPAM_COOLDOWN}s)\n"
        f"`/spamvip` <SĐT> <lần> - 📞 Spam Call (VIP, max {VIP_CALL_LIMIT}, CD {VIP_SPAM_COOLDOWN}s)\n"
        f"`/dungspam` <SĐT> - 🛑 Dừng spam cho SĐT (VIP)\n\n"

        #f"*Lệnh Share Facebook (Beta):*\n"
        #f"`/share` <link> <số lần> - 👍 Share FB (Free: {FREE_SHARE_LIMIT}, VIP: {VIP_SHARE_LIMIT})\n\n"
    )

    if is_user_admin:
        response_message += (
            f"*Lệnh Admin:*\n"
            f"`/add` <ID> [ngày] - ✅ Thêm VIP (0=vĩnh viễn)\n"
            f"`/remove` <ID> - ❌ Xóa VIP\n"
            f"`/cleanup` - 🧹 Xóa VIP hết hạn khỏi DB\n"
            f"`/listvip` - 👥 Xem danh sách VIP\n" # <<< ADDED /listvip
            f"`/rs` - 🔄 Khởi động lại Bot\n"
            f"`/status` - 📊 Trạng thái hệ thống & Bot\n"
            f"`/on` / `/off` - 🟢/🔴 Bật/Tắt Bot\n"
            f"`/admod` / `/unadmod` - 🔒/🔓 Bật/Tắt chế độ Admin\n"
            f"`/freeon` / `/freeoff` - ✅/❌ Bật/Tắt lệnh `/spam`\n"
            f"`/chaton` / `/chatoff` - 💬 Bật/Tắt chế độ chat riêng\n"
            f"`/abl` <SĐT> - 🚫 Thêm SĐT vào blacklist spam\n"
        )
    response_message += f"\nTham gia nhóm chat: {CHAT_GROUP_LINK}"
    return response_message

@bot.message_handler(commands=['help'])
def send_help(message):
    """Handles the /help command."""
    help_text = get_help_text(message.from_user.id)
    try:
        # Use send_message instead of reply_to for help to avoid nested replies
        bot.send_message(message.chat.id, help_text, parse_mode='Markdown')
    except Exception as e:
         print(f"Error sending help message: {e}")
    delete_user_message(message)


# --- Spam Commands ---

@bot.message_handler(commands=['spam'])
def spam_free(message):
    if not check_bot_status(message): return
    if not check_group_or_chat_mode(message): return # Message deleted inside check

    if not free_spam_enabled:
        bot.reply_to(message, "❌ Lệnh `/spam` (Free) hiện đang tạm tắt. Vui lòng thử lại sau.")
        delete_user_message(message)
        return

    user_id = message.from_user.id
    current_time = time.time()

    # Cooldown Check
    if user_id in last_usage and current_time - last_usage[user_id] < FREE_SPAM_COOLDOWN:
        wait_time = FREE_SPAM_COOLDOWN - (current_time - last_usage[user_id])
        bot.reply_to(message, f"⏳ Vui lòng đợi `{wait_time:.1f}` giây trước khi dùng `/spam`.")
        delete_user_message(message)
        return

    # Argument Parsing & Validation
    params = message.text.split()[1:]
    if len(params) != 2:
        bot.reply_to(message, f"⚠️ Sai cú pháp!\nVí dụ: `/spam 09xxxxxxxx {FREE_SPAM_LIMIT}`")
        delete_user_message(message)
        return

    sdt, count_str = params
    original_sdt = sdt # Keep original for script

    if not sdt.isdigit() or not (9 <= len(sdt) <= 11):
         bot.reply_to(message, "📞 Số điện thoại không hợp lệ.")
         delete_user_message(message)
         return
    if not count_str.isdigit():
        bot.reply_to(message, f"🔢 Số lần spam phải là số.")
        delete_user_message(message)
        return
    count = int(count_str)
    if not (1 <= count <= FREE_SPAM_LIMIT):
        bot.reply_to(message, f"🔢 Số lần spam Free tối đa là `{FREE_SPAM_LIMIT}`.")
        delete_user_message(message)
        return
    if sdt in blacklist:
        bot.reply_to(message, f"🚫 Số điện thoại `{sdt}` trong blacklist.")
        delete_user_message(message)
        return

    # Update Cooldown & Format Message
    last_usage[user_id] = current_time
    user_info = get_user_mention(message.from_user)
    masked_sdt = mask_phone(original_sdt)
    estimated_duration_minutes = 2 # Fixed estimate for free
    end_time = datetime.now() + timedelta(minutes=estimated_duration_minutes)
    formatted_end_time = end_time.strftime("%H:%M:%S %d/%m/%Y")

    confirmation_msg = f'''🚀 *Bắt Đầu SPAM (`/spam`)* 🚀
-----------------------------------
👤 Người gửi: {user_info} (`{user_id}`)
📞 Mục tiêu: `{masked_sdt}`
🔁 Số lần: `{count}`
⏳ Thời gian ước tính: `~{estimated_duration_minutes} phút`
🕒 Kết thúc dự kiến: `{formatted_end_time}`
-----------------------------------
🛠️ _Bot đang thực hiện yêu cầu..._'''
    msg = None
    try:
        msg = bot.send_message(message.chat.id, confirmation_msg, parse_mode="Markdown") # Send instead of reply
    except Exception as e:
        print(f"Error sending initial spam confirmation: {e}")
        delete_user_message(message)
        return # Stop if cannot send confirmation
    delete_user_message(message) # Delete user command now

    # Run Script
    script_filename = "dec.py"
    process = None
    try:
        if not os.path.isfile(script_filename):
             raise FileNotFoundError(f"Script file '{script_filename}' not found.")

        # No need for temp file if script exists locally
        process = subprocess.Popen([sys.executable, script_filename, original_sdt, str(count)]) # Use sys.executable
        # Store as a list even for single script commands for consistency with /dungspam
        running_spams[original_sdt] = [process] # Store as list
        print(f"[SPAM FREE] Started script for {original_sdt} (User: {user_id}) PID: {process.pid}")

        time.sleep(2) # Give script a moment to start or fail
        if process.poll() is not None: # Check if process terminated quickly
             bot.edit_message_text(confirmation_msg.replace("🛠️ _Bot đang thực hiện yêu cầu..._", "❌ _Lỗi: Script không thể chạy._"), msg.chat.id, msg.message_id, parse_mode="Markdown")
             raise Exception(f"Script execution failed early for {original_sdt}")
        else:
             bot.edit_message_text(confirmation_msg.replace("🛠️ _Bot đang thực hiện yêu cầu..._", "✅ _Đang chạy..._"), msg.chat.id, msg.message_id, parse_mode="Markdown")

    except Exception as e:
        error_text = confirmation_msg.split('-----------------------------------')[0] + f'-----------------------------------\n❌ Lỗi khi bắt đầu spam:\n`{e}`'
        if msg: # Edit message if it was sent successfully
            try:
                bot.edit_message_text(error_text, msg.chat.id, msg.message_id, parse_mode="Markdown")
            except Exception as edit_e:
                print(f"Failed to edit message on spam error: {edit_e}")
        print(f"[ERROR] Failed to start spam script for {original_sdt}: {e}")
        if original_sdt in running_spams: del running_spams[original_sdt] # Untrack if failed

				
@bot.message_handler(commands=['smsvip'])
def vipsms(message):
    if not check_bot_status(message): return
    if not check_group_or_chat_mode(message): return

    user_id = message.from_user.id
    if user_id not in allowed_users:
        bot.reply_to(message, "❌ Bạn không phải VIP hoặc VIP đã hết hạn. /mua để nâng cấp.")
        delete_user_message(message)
        return

    current_time = time.time()

    # Cooldown Check
    if user_id in last_usage and current_time - last_usage[user_id] < VIP_SPAM_COOLDOWN:
        wait_time = VIP_SPAM_COOLDOWN - (current_time - last_usage[user_id])
        bot.reply_to(message, f"⏳ VIP đợi `{wait_time:.1f}` giây trước khi spam SMS tiếp.")
        delete_user_message(message)
        return

    # Argument Parsing & Validation
    params = message.text.split()[1:]
    if len(params) != 2:
        bot.reply_to(message, f"⚠️ Sai cú pháp!\nVí dụ: `/smsvip 09xxxxxxxx {VIP_SMS_LIMIT}`")
        delete_user_message(message)
        return

    sdt, count_str = params
    original_sdt = sdt

    if not sdt.isdigit() or not (9 <= len(sdt) <= 11):
         bot.reply_to(message, "📞 Số điện thoại không hợp lệ.")
         delete_user_message(message)
         return
    if not count_str.isdigit():
        bot.reply_to(message, f"🔢 Số lần spam phải là số.")
        delete_user_message(message)
        return
    count = int(count_str)
    if not (1 <= count <= VIP_SMS_LIMIT):
        bot.reply_to(message, f"🔢 Số lần VIP SMS tối đa là `{VIP_SMS_LIMIT}`.")
        delete_user_message(message)
        return
    if sdt in blacklist:
        bot.reply_to(message, f"🚫 Số điện thoại `{sdt}` trong blacklist.")
        delete_user_message(message)
        return

    # Update Cooldown & Format Message
    last_usage[user_id] = current_time
    user_info = get_user_mention(message.from_user)
    masked_sdt = mask_phone(original_sdt)
    estimated_duration_minutes = 5 + (count // 10) # Rough guess
    end_time = datetime.now() + timedelta(minutes=estimated_duration_minutes)
    formatted_end_time = end_time.strftime("%H:%M:%S %d/%m/%Y")

    confirmation_msg = f'''🚀 *Bắt Đầu SPAM (`/smsvip`)* 🚀
-----------------------------------
🌟 Người gửi: {user_info} (`{user_id}`)
📞 Mục tiêu: `{masked_sdt}`
🔁 Số lần: `{count}`
⏳ Thời gian ước tính: `~{estimated_duration_minutes} phút`
🕒 Kết thúc dự kiến: `{formatted_end_time}`
-----------------------------------
🛠️ _Bot đang thực hiện yêu cầu..._'''
    msg = None
    try:
        msg = bot.send_message(message.chat.id, confirmation_msg, parse_mode="Markdown") # Send instead of reply
    except Exception as e:
        print(f"Error sending initial smsvip confirmation: {e}")
        delete_user_message(message)
        return
    delete_user_message(message)

    # Run Script (Identical logic to free spam for dec.py)
    script_filename = "dec.py"
    process = None
    try:
        if not os.path.isfile(script_filename):
             raise FileNotFoundError(f"Script file '{script_filename}' not found.")

        process = subprocess.Popen([sys.executable, script_filename, original_sdt, str(count)])
        running_spams[original_sdt] = [process] # Store as list
        print(f"[SPAM SMS VIP] Started script for {original_sdt} (User: {user_id}) PID: {process.pid}")

        time.sleep(2)
        if process.poll() is not None:
             bot.edit_message_text(confirmation_msg.replace("🛠️ _Bot đang thực hiện yêu cầu..._", "❌ _Lỗi: Script không thể chạy._"), msg.chat.id, msg.message_id, parse_mode="Markdown")
             raise Exception(f"Script execution failed early for {original_sdt}")
        else:
              bot.edit_message_text(confirmation_msg.replace("🛠️ _Bot đang thực hiện yêu cầu..._", "✅ _Đang chạy..._"), msg.chat.id, msg.message_id, parse_mode="Markdown")

    except Exception as e:
        error_text = confirmation_msg.split('-----------------------------------')[0] + f'-----------------------------------\n❌ Lỗi khi bắt đầu spam:\n`{e}`'
        if msg:
            try:
                bot.edit_message_text(error_text, msg.chat.id, msg.message_id, parse_mode="Markdown")
            except Exception as edit_e:
                print(f"Failed to edit message on smsvip error: {edit_e}")
        print(f"[ERROR] Failed to start spam SMS VIP script for {original_sdt}: {e}")
        if original_sdt in running_spams: del running_spams[original_sdt]
import telebot
import time
import requests # <--- Thêm thư viện requests
import os
import sys
from datetime import datetime, timedelta

# --- Giả định các biến và hàm này đã được định nghĩa ---
# from your_bot_file import bot, allowed_users, last_usage, blacklist
# from your_utils import (
#     check_bot_status, check_group_or_chat_mode, delete_user_message,
#     get_user_mention, mask_phone
# )
# VIP_SPAM_COOLDOWN = 60 # Ví dụ: 60 giây cooldown
# --- Kết thúc phần giả định ---

CALL_LIMIT = 20 # Giới hạn tối đa 20 cuộc gọi
API_KEY = "autp-250" # API key của bạn
API_URL_TEMPLATE = "https://thachmora.site/otp/?key={key}&sdt={sdt}&so={count}"

# Biến last_usage phải là dictionary được định nghĩa ở phạm vi toàn cục
# Ví dụ: last_usage = {}
# Biến allowed_users phải là list hoặc set được định nghĩa ở phạm vi toàn cục
# Ví dụ: allowed_users = {123456789, 987654321} # ID của người dùng VIP
# Biến blacklist phải là list hoặc set được định nghĩa ở phạm vi toàn cục
# Ví dụ: blacklist = {"0123456789"}

@bot.message_handler(commands=['call'])
def call_spam(message):
    # --- Các kiểm tra ban đầu (giữ nguyên từ /smsvip) ---
    if not check_bot_status(message): return
    if not check_group_or_chat_mode(message): return

    user_id = message.from_user.id
    if user_id not in allowed_users:
        bot.reply_to(message, "❌ Bạn không phải VIP hoặc VIP đã hết hạn. /mua để nâng cấp.")
        delete_user_message(message)
        return

    current_time = time.time()

    # Cooldown Check (giữ nguyên từ /smsvip)
    if user_id in last_usage and current_time - last_usage[user_id] < VIP_SPAM_COOLDOWN:
        wait_time = VIP_SPAM_COOLDOWN - (current_time - last_usage[user_id])
        bot.reply_to(message, f"⏳ VIP đợi `{wait_time:.1f}` giây trước khi spam CALL tiếp.")
        delete_user_message(message)
        return

    # --- Phân tích đối số và xác thực ---
    params = message.text.split()[1:]
    if len(params) != 2:
        # Cập nhật thông báo ví dụ
        bot.reply_to(message, f"⚠️ Sai cú pháp!\nVí dụ: `/call 09xxxxxxxx {CALL_LIMIT}`")
        delete_user_message(message)
        return

    sdt, count_str = params
    original_sdt = sdt

    # Xác thực SĐT (giữ nguyên)
    if not sdt.isdigit() or not (9 <= len(sdt) <= 11):
         bot.reply_to(message, "📞 Số điện thoại không hợp lệ.")
         delete_user_message(message)
         return

    # Xác thực số lần (cập nhật giới hạn)
    if not count_str.isdigit():
        bot.reply_to(message, f"🔢 Số lần gọi phải là số.")
        delete_user_message(message)
        return
    count = int(count_str)
    if not (1 <= count <= CALL_LIMIT): # Sử dụng CALL_LIMIT
        bot.reply_to(message, f"🔢 Số lần gọi tối đa là `{CALL_LIMIT}`.")
        delete_user_message(message)
        return

    # Kiểm tra blacklist (giữ nguyên)
    if sdt in blacklist:
        bot.reply_to(message, f"🚫 Số điện thoại `{sdt}` trong blacklist.")
        delete_user_message(message)
        return

    # --- Cập nhật cooldown và gửi tin nhắn xác nhận ---
    last_usage[user_id] = current_time
    user_info = get_user_mention(message.from_user)
    masked_sdt = mask_phone(original_sdt)

    # Tin nhắn xác nhận (cập nhật văn bản cho phù hợp với "call")
    confirmation_msg_template = f'''🚀 *Bắt Đầu SPAM (`/call`)* 🚀
-----------------------------------
🌟 Người gửi: {user_info} (`{user_id}`)
📞 Mục tiêu: `{masked_sdt}`
🔁 Số lần: `{count}`
-----------------------------------
{{status_placeholder}}''' # Sử dụng placeholder cho trạng thái

    msg = None
    try:
        # Gửi tin nhắn ban đầu với trạng thái "đang xử lý"
        status_text = "🛠️ _Đang gửi yêu cầu đến API..._"
        msg = bot.send_message(message.chat.id, confirmation_msg_template.format(status_placeholder=status_text), parse_mode="Markdown")
    except Exception as e:
        print(f"Error sending initial call confirmation: {e}")
        delete_user_message(message) # Xóa lệnh gốc nếu gửi lỗi
        return

    delete_user_message(message) # Xóa lệnh gốc của người dùng

    # --- Thực hiện gọi API ---
    try:
        # Tạo URL API
        api_url = API_URL_TEMPLATE.format(key=API_KEY, sdt=original_sdt, count=count)

        # Gửi yêu cầu GET đến API với timeout
        response = requests.get(api_url, timeout=20) # Timeout 20 giây

        # Kiểm tra mã trạng thái HTTP, nếu là lỗi (4xx, 5xx) thì báo lỗi
        response.raise_for_status()

        # Giả sử thành công nếu không có lỗi ở trên
        print(f"[CALL SPAM] API call successful for {original_sdt} (User: {user_id}). Status: {response.status_code}")
        # Bạn có thể kiểm tra thêm nội dung response.text nếu API trả về thông tin cụ thể
        # Ví dụ: if "thanh cong" in response.text.lower(): ...

        # Cập nhật tin nhắn thành công
        final_status = "✅ _Yêu cầu spam call đã được gửi thành công qua API!_"
        bot.edit_message_text(confirmation_msg_template.format(status_placeholder=final_status), msg.chat.id, msg.message_id, parse_mode="Markdown")

    except requests.exceptions.Timeout:
        error_text = "❌ _Lỗi: API không phản hồi (timeout)._"
        print(f"[ERROR] API call timed out for {original_sdt} (User: {user_id})")
        try:
            bot.edit_message_text(confirmation_msg_template.format(status_placeholder=error_text), msg.chat.id, msg.message_id, parse_mode="Markdown")
        except Exception as edit_e:
            print(f"Failed to edit message on call API timeout error: {edit_e}")

    except requests.exceptions.RequestException as e:
        # Bắt các lỗi khác từ thư viện requests (mạng, HTTP status code lỗi, v.v.)
        error_text = f"❌ _Lỗi khi gọi API spam call: Mã lỗi {response.status_code if 'response' in locals() else 'N/A'}._"
        print(f"[ERROR] API call failed for {original_sdt} (User: {user_id}): {e}")
        try:
            # Hiển thị lỗi cụ thể hơn nếu có thể
            detailed_error = str(e)
            if response and response.text:
                 detailed_error += f"\nAPI Response: {response.text[:200]}" # Show first 200 chars of response

            error_text = f"❌ _Lỗi khi gọi API spam call._\n`{detailed_error}`"
            bot.edit_message_text(confirmation_msg_template.format(status_placeholder=error_text[:4000]), msg.chat.id, msg.message_id, parse_mode="Markdown") # Giới hạn độ dài lỗi
        except Exception as edit_e:
            print(f"Failed to edit message on call API request error: {edit_e}")

    except Exception as e:
        # Bắt các lỗi không mong muốn khác
        error_text = f"❌ _Lỗi không xác định trong quá trình xử lý: {e}_"
        print(f"[ERROR] Unknown error during call spam processing for {original_sdt} (User: {user_id}): {e}")
        try:
            bot.edit_message_text(confirmation_msg_template.format(status_placeholder=error_text), msg.chat.id, msg.message_id, parse_mode="Markdown")
        except Exception as edit_e:
            print(f"Failed to edit message on unknown call error: {edit_e}")

# --- Đảm bảo bạn đã đăng ký handler với bot ---
# Ví dụ:
# if __name__ == '__main__':
#    print("Bot is running...")
#    bot.infinity_polling()
@bot.message_handler(commands=['spamvip'])
def supersms_call(message): # Combined API + 2 Scripts call/SMS spam
    """Handles the VIP Call/SMS spam command using API and two scripts."""
    if not check_bot_status(message): return
    if not check_group_or_chat_mode(message): return

    user_id = message.from_user.id
    if user_id not in allowed_users:
        bot.reply_to(message, "❌ Bạn không phải VIP hoặc VIP đã hết hạn. /mua để nâng cấp.")
        delete_user_message(message)
        return

    current_time = time.time()

    # Cooldown Check
    if user_id in last_usage and current_time - last_usage[user_id] < VIP_SPAM_COOLDOWN:
        wait_time = VIP_SPAM_COOLDOWN - (current_time - last_usage[user_id])
        bot.reply_to(message, f"⏳ VIP đợi `{wait_time:.1f}` giây trước khi spam Call/SMS tiếp.")
        delete_user_message(message)
        return

    # Argument Parsing & Validation
    params = message.text.split()[1:]
    if len(params) != 2:
        bot.reply_to(message, f"⚠️ Sai cú pháp!\nVí dụ: `/spamvip 09xxxxxxxx {VIP_CALL_LIMIT}`")
        delete_user_message(message)
        return

    sdt, count_str = params
    original_sdt = sdt

    if not sdt.isdigit() or not (9 <= len(sdt) <= 11):
         bot.reply_to(message, "📞 Số điện thoại không hợp lệ.")
         delete_user_message(message)
         return
    if not count_str.isdigit():
        bot.reply_to(message, f"🔢 Số lần spam phải là số.")
        delete_user_message(message)
        return
    count = int(count_str)
    if not (1 <= count <= VIP_CALL_LIMIT):
        bot.reply_to(message, f"🔢 Số lần VIP Call/SMS tối đa là `{VIP_CALL_LIMIT}`.")
        delete_user_message(message)
        return
    if original_sdt in blacklist:
        bot.reply_to(message, f"🚫 Số điện thoại `{original_sdt}` trong blacklist.")
        delete_user_message(message)
        return

    # Update Cooldown & Format Initial Message
    last_usage[user_id] = current_time
    user_info = get_user_mention(message.from_user)
    masked_sdt = mask_phone(original_sdt)
    # Thời gian ước tính có thể cần điều chỉnh nếu 2 script chạy song song lâu hơn
    estimated_duration_minutes = 12 + (count // 4) # Slightly adjusted guess
    end_time = datetime.now() + timedelta(minutes=estimated_duration_minutes)
    formatted_end_time = end_time.strftime("%H:%M:%S %d/%m/%Y")

    confirmation_msg_base = f'''🚀 *Bắt Đầu SPAM (`/spamvip`)* 🚀
-----------------------------------
📞 Người gửi: {user_info} (`{user_id}`)
📞 Mục tiêu: `{masked_sdt}`
🔁 Số lần: `{count}`
⏳ Thời gian ước tính: `~{estimated_duration_minutes} phút`
🕒 Kết thúc dự kiến: `{formatted_end_time}`
-----------------------------------'''
    confirmation_msg_progress = confirmation_msg_base + "\n🛠️ _Bot đang thực hiện yêu cầu (API & Scripts)..._"
    msg = None
    try:
        # Send a new message instead of replying to avoid potential issues
        msg = bot.send_message(message.chat.id, confirmation_msg_progress, parse_mode="Markdown")
    except Exception as e:
        print(f"Error sending initial spamvip confirmation: {e}")
        # No message to delete if sending failed, just return
        return
    finally:
        # Delete the user's original command message *after* attempting to send confirmation
        delete_user_message(message)

    # Execute API Call and Scripts
    api_call_successful = False
    script_dec_started = False
    script_ii_started = False
    api_error_msg = ""
    script_error_msg_dec = ""
    script_error_msg_ii = ""
    processes_started = [] # Keep track of successfully started processes

    # 1. Call ThachMora API
    api_key = API_KEY_THACHMORA
    api_url = f"https://thachmora.site/otp/?key={api_key}&sdt={original_sdt}&so={count}"
    try:
        print(f"[SPAMVIP API] Calling: {api_url}")
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        print(f"[SPAMVIP API] OK for {original_sdt}. Status: {response.status_code}.")
        api_call_successful = True
    except requests.exceptions.Timeout:
        api_error_msg = "API timeout"
        print(f"[SPAMVIP API] Timeout for {original_sdt}")
    except requests.exceptions.RequestException as e:
        api_error_msg = f"API error: {type(e).__name__}"
        print(f"[SPAMVIP API] Fail for {original_sdt}: {e}")

    # --- Chạy Script 1: dec.py ---
    script_filename_dec = "dec.py"
    process_dec = None
    try:
        if not os.path.isfile(script_filename_dec):
            raise FileNotFoundError(f"Script file '{script_filename_dec}' not found.")

        process_dec = subprocess.Popen([sys.executable, script_filename_dec, original_sdt, str(count)])
        print(f"[SPAMVIP Script DEC] Started {script_filename_dec} for {original_sdt} (User: {user_id}) PID: {process_dec.pid}")

        time.sleep(2) # Cho script thời gian khởi động/lỗi
        if process_dec.poll() is not None:
             return_code = process_dec.returncode
             raise Exception(f"Script {script_filename_dec} exited early code {return_code}")

        script_dec_started = True
        processes_started.append(process_dec) # Add to list if started

    except FileNotFoundError as fnf_e:
        script_error_msg_dec = "File not found"
        print(f"[SPAMVIP Script DEC] Error: {fnf_e}")
    except Exception as script_e:
         script_error_msg_dec = f"{type(script_e).__name__}"
         print(f"[SPAMVIP Script DEC] Error running {script_filename_dec} for {original_sdt}: {script_e}")

    # --- Chạy Script 2: ii.py ---
    script_filename_ii = "ii.py" # *** ĐẢM BẢO FILE NÀY TỒN TẠI ***
    process_ii = None
    try:
        if not os.path.isfile(script_filename_ii):
            raise FileNotFoundError(f"Script file '{script_filename_ii}' not found.")

        process_ii = subprocess.Popen([sys.executable, script_filename_ii, original_sdt, str(count)])
        print(f"[SPAMVIP Script II] Started {script_filename_ii} for {original_sdt} (User: {user_id}) PID: {process_ii.pid}")

        time.sleep(2) # Cho script thời gian khởi động/lỗi
        if process_ii.poll() is not None:
             return_code = process_ii.returncode
             raise Exception(f"Script {script_filename_ii} exited early code {return_code}")

        script_ii_started = True
        processes_started.append(process_ii) # Add to list if started

    except FileNotFoundError as fnf_e:
        script_error_msg_ii = "File not found"
        print(f"[SPAMVIP Script II] Error: {fnf_e}")
    except Exception as script_e:
         script_error_msg_ii = f"{type(script_e).__name__}"
         print(f"[SPAMVIP Script II] Error running {script_filename_ii} for {original_sdt}: {script_e}")

    # --- Add successfully started processes to tracking ---
    if processes_started:
        # If there are already processes running for this number, add to the list
        if original_sdt in running_spams:
             running_spams[original_sdt].extend(processes_started)
        else: # Otherwise, create a new entry
             running_spams[original_sdt] = processes_started
    elif original_sdt in running_spams and not running_spams[original_sdt]: # Clean up if entry exists but no process started now
        # This condition might be redundant if the above logic works correctly
        del running_spams[original_sdt]

    # --- Update Status Message Based on Results ---
    started_items = []
    failed_items = []

    # API Status
    if api_call_successful: started_items.append("API")
    elif api_error_msg: failed_items.append(f"API ({api_error_msg})")

    # Script dec.py Status
    if script_dec_started: started_items.append(f"Script {script_filename_dec}")
    elif script_error_msg_dec: failed_items.append(f"Script {script_filename_dec} ({script_error_msg_dec})")

    # Script ii.py Status
    if script_ii_started: started_items.append(f"Script {script_filename_ii}")
    elif script_error_msg_ii: failed_items.append(f"Script {script_filename_ii} ({script_error_msg_ii})")

    # Construct the final status line
    status_line = ""
    if started_items:
        status_line = f"✅ _{' & '.join(started_items)} đang chạy..._"
        if failed_items:
            status_line += f"\n⚠️ _Lỗi: {'; '.join(failed_items)}_"
    elif failed_items:
        status_line = f"❌ *Thất bại!* Không thể bắt đầu: {'; '.join(failed_items)}"
    else:
        status_line = "❓ _Trạng thái không xác định. Không có API hay Script nào được bắt đầu hoặc báo lỗi._"

    final_status_msg = confirmation_msg_base + "\n" + status_line
    if msg: # Edit the status message sent earlier
        try:
            bot.edit_message_text(final_status_msg, msg.chat.id, msg.message_id, parse_mode="Markdown")
        except Exception as edit_e:
            print(f"Error editing final spamvip status message: {edit_e}")
            # Attempt to send as new if edit fails
            try:
                bot.send_message(message.chat.id, final_status_msg, parse_mode="Markdown")
            except Exception as final_send_e:
                 print(f"Error sending final status for spamvip after edit failure: {final_send_e}")
    else: # This case should not happen if initial send was successful, but as fallback:
        try:
            bot.send_message(message.chat.id, final_status_msg, parse_mode="Markdown")
        except Exception as final_send_e:
            print(f"Error sending final failure status for spamvip (no initial msg): {final_send_e}")


# --- DUNG SPAM (FIXED) ---
@bot.message_handler(commands=['dungspam'])
def stop_spam(message):
    """Stops running spam processes for a given phone number (VIP only)."""
    user_id = message.from_user.id
    if user_id not in allowed_users:
        bot.reply_to(message, "❌ Chỉ VIP mới có thể dừng spam.")
        delete_user_message(message)
        return

    args = message.text.split()
    if len(args) != 2:
        bot.reply_to(message, "⚠️ Sai cú pháp!\nVí dụ: `/dungspam 09xxxxxxxx`")
        delete_user_message(message)
        return

    sdt_to_stop = args[1]
    if not sdt_to_stop.isdigit():
        bot.reply_to(message,"⚠️ Số điện thoại không hợp lệ.")
        delete_user_message(message)
        return

    # Retrieve the LIST of processes for the phone number
    processes_to_stop = running_spams.get(sdt_to_stop)
    stopped_count = 0
    failed_to_stop = False
    already_finished_count = 0

    if processes_to_stop: # Check if the key exists and has a list
        print(f"[STOP SPAM] Request for {sdt_to_stop} by User {user_id}. Found {len(processes_to_stop)} process entries.")
        # Use a copy to iterate safely if modifying the list during iteration (though we remove the key later)
        process_list_copy = list(processes_to_stop)
        active_processes_found = False

        for i, process in enumerate(process_list_copy):
            if process and hasattr(process, 'poll') and callable(process.poll): # Check if it's a valid process object
                if process.poll() is None: # Check if process is currently running
                    active_processes_found = True
                    try:
                        pid_to_stop = process.pid # Get PID before potential termination
                        print(f"[STOP SPAM] Attempting to stop process {i+1}/{len(process_list_copy)} (PID: {pid_to_stop}) for {sdt_to_stop}...")
                        process.terminate() # Send SIGTERM first
                        time.sleep(0.5) # Give it a moment
                        if process.poll() is None: # If still running, force kill
                            print(f"[STOP SPAM] Process {pid_to_stop} did not terminate, sending SIGKILL...")
                            process.kill()
                            time.sleep(0.1) # Short delay after kill
                        # Verify it stopped
                        if process.poll() is not None:
                            print(f"[STOP SPAM] Process {pid_to_stop} stopped successfully.")
                            stopped_count += 1
                        else:
                             print(f"[STOP SPAM] FAILED to stop process {pid_to_stop} even after kill.")
                             failed_to_stop = True

                    except Exception as e:
                        print(f"[ERROR] Error stopping process PID {process.pid} for {sdt_to_stop}: {e}")
                        failed_to_stop = True
                else:
                    # Process was found but already finished
                    print(f"[STOP SPAM] Process {i+1}/{len(process_list_copy)} (PID: {process.pid}) for {sdt_to_stop} was already finished.")
                    already_finished_count += 1
            else:
                 print(f"[STOP SPAM] Found invalid or non-process entry at index {i} for {sdt_to_stop}.")


        # Remove the entry from tracking AFTER attempting to stop all
        # Check if key still exists before deleting (might be removed by another thread?)
        if sdt_to_stop in running_spams:
            del running_spams[sdt_to_stop]
            print(f"[STOP SPAM] Removed tracking entry for {sdt_to_stop}.")

        # Send confirmation based on results
        if stopped_count > 0 and not failed_to_stop:
             bot.reply_to(message, f"✅ Đã dừng thành công {stopped_count} tiến trình spam đang chạy cho số `{sdt_to_stop}`.")
        elif stopped_count > 0 and failed_to_stop:
             bot.reply_to(message, f"⚠️ Đã dừng {stopped_count} tiến trình spam cho số `{sdt_to_stop}`, nhưng có lỗi xảy ra khi dừng một số tiến trình khác.")
        elif failed_to_stop: # stopped_count is 0 but failed_to_stop is True
             bot.reply_to(message, f"❌ Lỗi: Không thể dừng các tiến trình spam đang chạy cho `{sdt_to_stop}`.")
        elif not active_processes_found and already_finished_count > 0 : # No active process found, but tracked ones were finished
            bot.reply_to(message, f"ℹ️ Không tìm thấy tiến trình spam *đang chạy* cho số `{sdt_to_stop}` (các tiến trình được ghi nhận đã kết thúc).")
        elif not active_processes_found and already_finished_count == 0: # No active and none finished (e.g. invalid entry)
             bot.reply_to(message, f"ℹ️ Không tìm thấy tiến trình spam nào đang chạy hoặc đã kết thúc được ghi nhận cho `{sdt_to_stop}`.")

    else: # Key not found in running_spams
        bot.reply_to(message, f"ℹ️ Không tìm thấy tiến trình spam nào được ghi nhận cho số `{sdt_to_stop}`.")

    delete_user_message(message)


# --- Other Utility Commands ---

@bot.message_handler(commands=['id'])
def get_user_id(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    response = f"👤 ID của bạn: `{user_id}`\n"
    if message.chat.type != "private":
        response += f"🏢 ID Nhóm này: `{chat_id}`"
    try:
        bot.reply_to(message, response)
    except Exception as e:
        print(f"Error replying to /id: {e}")
    delete_user_message(message)

@bot.message_handler(commands=['qr'])
def generate_qr(message):
    text = message.text[len('/qr '):].strip()
    if not text:
        bot.reply_to(message, '⚠️ Cần nội dung để tạo QR.\n`/qr text here`')
        delete_user_message(message)
        return

    try:
        qr = qrcode.make(text)
        bio = io.BytesIO()
        bio.name = 'qrcode.png'
        qr.save(bio, 'PNG')
        bio.seek(0)
        bot.send_photo(message.chat.id, bio, caption=f"🖼️ QR cho:\n`{text}`")
    except Exception as e:
        bot.reply_to(message, f"❌ Lỗi tạo QR: {e}")
        print(f"Error generating QR code: {e}")
    delete_user_message(message)

@bot.message_handler(commands=['voice'])
def handle_voice(message):
    text = message.text[len('/voice '):].strip()
    if not text:
        bot.reply_to(message, '⚠️ Cần văn bản.\n`/voice hello world`')
        delete_user_message(message)
        return

    tmp_file = None # Define outside try block
    try:
        tts = gTTS(text=text, lang='vi')
        # Create a temporary file correctly
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp_file_obj:
            tmp_file = tmp_file_obj.name # Get the name
            tts.save(tmp_file) # Save to the temporary file path

        # Send the voice message
        with open(tmp_file, 'rb') as voice_file:
            bot.send_voice(chat_id=message.chat.id, voice=voice_file, caption=f"🗣️ Giọng nói cho:\n_{text}_", parse_mode="Markdown") # Added parse_mode

    except Exception as e:
        bot.reply_to(message, f"❌ Lỗi tạo giọng nói: {e}")
        print(f"Error in gTTS or sending voice: {e}")
    finally:
        # Clean up the temporary file
        if tmp_file and os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except Exception as e:
                print(f"Error removing temporary voice file {tmp_file}: {e}")
        delete_user_message(message)


@bot.message_handler(commands=['gg'])
def handle_gemini(message):
    if not gemini_model:
        bot.reply_to(message, "❌ Tính năng Gemini AI chưa được cấu hình.")
        delete_user_message(message)
        return

    user_input = message.text[len('/gg '):].strip()
    if not user_input:
        bot.reply_to(message, "⚠️ Cần câu hỏi cho Gemini.\n`/gg Capital of Vietnam?`")
        delete_user_message(message)
        return

    thinking_msg = None # Define outside try
    try:
        # Optional: Add a "Thinking..." message
        thinking_msg = bot.reply_to(message, "🧠 _Gemini đang suy nghĩ..._")

        # Send message to Gemini
        response = gemini_chat_session.send_message(user_input)

        # Edit the thinking message with the response
        bot.edit_message_text(f"🤖 *Gemini AI:* \n\n{response.text}",
                              thinking_msg.chat.id, thinking_msg.message_id, parse_mode="Markdown") # Added parse_mode

    except Exception as e:
        error_message = f"❌ Lỗi khi giao tiếp với Gemini AI:\n`{e}`"
        # Try to edit the thinking message, or send a new one if editing fails
        try:
             if thinking_msg:
                  bot.edit_message_text(error_message, thinking_msg.chat.id, thinking_msg.message_id, parse_mode="Markdown") # Added parse_mode
             else: # If sending 'thinking' message failed
                  bot.reply_to(message, error_message, parse_mode="Markdown") # Added parse_mode
        except: # Fallback if editing error message fails
              bot.reply_to(message, error_message, parse_mode="Markdown") # Added parse_mode
        print(f"Error interacting with Gemini AI: {e}")
        # Consider resetting chat session on certain errors
        # global gemini_chat_session
        # gemini_chat_session = gemini_model.start_chat(history=[])
    finally:
          delete_user_message(message)

@bot.message_handler(commands=['face'])
def send_random_face(message):
    url = "https://thispersondoesnotexist.com/"
    msg = None
    try:
        msg = bot.reply_to(message, "🧑 _Đang tải ảnh mặt người..._")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        if response.content:
            photo = BytesIO(response.content)
            bot.delete_message(msg.chat.id, msg.message_id) # Delete thinking message
            bot.send_photo(message.chat.id, photo, caption="✨ Gương mặt này không tồn tại!")
        else:
             if msg: bot.edit_message_text("❌ Không nhận được dữ liệu ảnh.", msg.chat.id, msg.message_id)
             else: bot.reply_to(message, "❌ Không nhận được dữ liệu ảnh.")
    except requests.exceptions.RequestException as e:
         error_txt = f"❌ Lỗi khi lấy ảnh: `{e}`"
         if msg: bot.edit_message_text(error_txt, msg.chat.id, msg.message_id, parse_mode="Markdown") # Added parse_mode
         else: bot.reply_to(message, error_txt, parse_mode="Markdown") # Added parse_mode
         print(f"Error fetching random face: {e}")
    except Exception as e:
        error_txt = f"❌ Lỗi không xác định: `{e}`"
        if msg: bot.edit_message_text(error_txt, msg.chat.id, msg.message_id, parse_mode="Markdown") # Added parse_mode
        else: bot.reply_to(message, error_txt, parse_mode="Markdown") # Added parse_mode
        print(f"Unexpected error in /face: {e}")
    finally:
        delete_user_message(message)


@bot.message_handler(commands=['tiktok'])
def tiktok_command(message):
    command_parts = message.text.split(maxsplit=1)
    wait_msg = None
    try:
        if len(command_parts) != 2:
             bot.reply_to(message, "⚠️ Cần link TikTok.\n`/tiktok <link>`")
             delete_user_message(message)
             return

        url = command_parts[1].strip()
        api_url = f'https://tikwm.com/api/?url={url}'
        wait_msg = bot.reply_to(message, "⏳ _Đang lấy thông tin TikTok..._")

        response = requests.get(api_url, timeout=20)
        response.raise_for_status()
        data = response.json()

        if data and data.get('code') == 0 and 'data' in data:
            video_title = data['data'].get('title', '').strip()
            # Prefer no-watermark video if available
            video_url_nowm = data['data'].get('play')
            video_url_wm = data['data'].get('wmplay')
            video_url = video_url_nowm or video_url_wm # Choose one that exists

            music_title = data['data'].get('music_info', {}).get('title', 'N/A').strip()
            music_url = data['data'].get('music_info', {}).get('play', 'N/A')

            reply_message = f"🎵 *Thông tin TikTok*\n\n"
            if video_title: reply_message += f"🎬 *Tiêu đề:* {video_title}\n"
            # Add download links using inline keyboard buttons
            markup = InlineKeyboardMarkup()
            buttons = []
            if video_url_nowm: buttons.append(InlineKeyboardButton("🎬 Video (Ko WM)", url=video_url_nowm))
            if video_url_wm and not video_url_nowm : buttons.append(InlineKeyboardButton("🎬 Video (Có WM)", url=video_url_wm)) # Offer WM only if no WM link
            if music_url != 'N/A': buttons.append(InlineKeyboardButton("🎶 Audio", url=music_url))

            if buttons:
                # Arrange buttons neatly (max 2 per row)
                if len(buttons) == 3:
                     markup.row(buttons[0], buttons[1])
                     markup.row(buttons[2])
                elif len(buttons) == 2:
                     markup.row(buttons[0], buttons[1])
                else: # len == 1
                     markup.add(buttons[0])

                bot.edit_message_text(reply_message, wait_msg.chat.id, wait_msg.message_id, reply_markup=markup, disable_web_page_preview=True, parse_mode="Markdown") # Added parse_mode
            else:
                 reply_message += "_Không tìm thấy link tải video/audio._"
                 bot.edit_message_text(reply_message, wait_msg.chat.id, wait_msg.message_id, parse_mode="Markdown") # Added parse_mode

        else:
            error_detail = data.get('msg', 'Lỗi không xác định từ API')
            bot.edit_message_text(f"❌ Lỗi lấy dữ liệu TikTok:\n`{error_detail}`", wait_msg.chat.id, wait_msg.message_id, parse_mode="Markdown") # Added parse_mode

    except requests.exceptions.RequestException as e:
         if wait_msg: bot.edit_message_text(f"❌ Lỗi kết nối API TikTok: `{e}`", wait_msg.chat.id, wait_msg.message_id, parse_mode="Markdown") # Added parse_mode
         else: bot.reply_to(message, f"❌ Lỗi kết nối API TikTok: `{e}`", parse_mode="Markdown") # Added parse_mode
         print(f"Error fetching TikTok data: {e}")
    except json.JSONDecodeError:
         if wait_msg: bot.edit_message_text("❌ Lỗi đọc dữ liệu JSON từ API TikTok.", wait_msg.chat.id, wait_msg.message_id)
         else: bot.reply_to(message, "❌ Lỗi đọc dữ liệu JSON từ API TikTok.")
    except Exception as e:
         error_txt = f"❌ Lỗi không xác định trong /tiktok: `{e}`"
         if wait_msg: bot.edit_message_text(error_txt, wait_msg.chat.id, wait_msg.message_id, parse_mode="Markdown") # Added parse_mode
         else: bot.reply_to(message, error_txt, parse_mode="Markdown") # Added parse_mode
         print(f"Unexpected error in /tiktok: {e}")
    finally:
         delete_user_message(message)


@bot.message_handler(commands=['tool'])
def send_tool_links(message):
    markup = types.InlineKeyboardMarkup()
    # Add your actual tool links here
    markup.add(types.InlineKeyboardButton(text="🛠️ Tool LeQuocKhang", url="https://lequockhang.site/tool.html"))
    # markup.add(types.InlineKeyboardButton(text="🚀 Tool VIP", url="https://example.com/vip_tool"))
    try:
        bot.reply_to(message, "🔗 Các liên kết công cụ:", reply_markup=markup)
    except Exception as e:
        print(f"Error sending tool links: {e}")
    delete_user_message(message)

@bot.message_handler(commands=['tv'])
def tieng_viet(message):
    keyboard = types.InlineKeyboardMarkup()
    # Found a likely working link, replace abcxyz if needed
    url_button = types.InlineKeyboardButton("🇻🇳 Đặt Tiếng Việt 🇻🇳", url='https://t.me/setlanguage/vi-beta')
    keyboard.add(url_button)
    try:
        bot.send_message(message.chat.id, 'Nhấn nút bên dưới để đổi ngôn ngữ Telegram sang Tiếng Việt:', reply_markup=keyboard)
    except Exception as e:
        print(f"Error sending language link: {e}")
    delete_user_message(message)

@bot.message_handler(commands=['ad'])
def send_admin_info(message):
    try:
        bot.reply_to(message, f"👑 *Admin:* @{ADMIN_USERNAME}\n🆔 *ID:* `{ADMIN_ID}`") # Already uses Markdown
    except Exception as e:
        print(f"Error replying to /ad: {e}")
    delete_user_message(message)

# --- VIP Purchase Command (FIXED - SyntaxError corrected) ---

@bot.message_handler(commands=['mua'])
def mua_command(message, from_callback=False, callback_user=None): # Added callback_user
    """Handles the /mua command to show payment info."""
    if from_callback and callback_user:
        user = callback_user # User who clicked the button
    else:
        user = message.from_user # User who sent the command

    user_id = user.id
    user_info = get_user_mention(user) # Get mention for the correct user

    response_message = (
        f"💰 *Yêu Cầu Mua VIP*\n\n"
        f"👤 Người dùng: {user_info} (`{user_id}`)\n"
        f"✨ Gói VIP: *{VIP_PRICE} / {VIP_DURATION_DAYS} ngày*\n\n"
        f"*Thông Tin Thanh Toán:*\n"
        f"🏦 Ngân hàng: *VIETCOMBANK*\n"
        f"💳 STK: `1049565152`\n"
        f"👤 Tên TK: LE QUOC KHANG\n"
        f"📝 Nội dung CK: `{user_id}` (*BẮT BUỘC*)\n\n"
        f"👉 Sau khi CK, nhấn *'Gửi Ảnh CK'* và gửi ảnh chụp màn hình giao dịch thành công.\n"
        f"👉 Hoặc liên hệ Admin nếu cần hỗ trợ."
    )

    users_requested_payment[user_id] = True # Mark user

    markup = InlineKeyboardMarkup(row_width=1)
    # Direct link to the bot itself for sending the photo
    btn_send_photo = InlineKeyboardButton("📸 Gửi Ảnh CK", url=f"https://t.me/{BOT_USERNAME}?start=send_payment")
    btn_admin = InlineKeyboardButton("🧑‍💻 Liên Hệ Admin", url=f"https://t.me/{ADMIN_USERNAME}")
    markup.add(btn_send_photo, btn_admin)

    # Determine chat ID based on context
    chat_id_to_send = message.chat.id

    try:
        # Send the QR code image URL along with the text description
        if PAYMENT_IMAGE_URL and 'http' in PAYMENT_IMAGE_URL:
            bot.send_photo(
                chat_id=chat_id_to_send,
                photo=PAYMENT_IMAGE_URL,
                caption=response_message,
                reply_markup=markup,
                parse_mode='Markdown' # Ensure parse mode for caption
            )
        else:
             # Fallback if no valid image URL is configured
             bot.send_message(
                  chat_id=chat_id_to_send,
                  text=response_message, # <<< SYNTAX ERROR FIXED HERE (added text=)
                  reply_markup=markup,
                  parse_mode='Markdown', # Ensure parse mode
                  disable_web_page_preview=True
              )
    except Exception as e:
        # Send text message as fallback if photo send fails
        error_text = f"❌ Lỗi gửi ảnh QR. Thông tin thanh toán:\n\n{response_message}" # Store text in var
        bot.send_message(
            chat_id=chat_id_to_send,
            text=error_text, # <<< SYNTAX ERROR FIXED HERE (added text=)
            reply_markup=markup,
            parse_mode='Markdown', # Ensure parse mode
            disable_web_page_preview=True
        )
        print(f"Error sending payment info photo/message: {e}")

    # Only delete the original command message if it wasn't triggered by a callback button
    if not from_callback:
        delete_user_message(message)
    # If triggered by callback, we might want to delete the original /start message or edit it
    # For now, let's not delete the message the button was attached to


# --- Handle Payment Photo ---
@bot.message_handler(content_types=['photo'])
def handle_payment_photo(message):
    user_id = message.from_user.id

    # Check if this user initiated /mua recently
    if users_requested_payment.get(user_id):
        # Get user mention using the function
        user_info = get_user_mention(message.from_user)
        file_id = message.photo[-1].file_id # Best quality photo

        admin_caption = f"📸 Ảnh thanh toán VIP từ:\n👤 {user_info}\n🆔 `{user_id}`\n\n👉 Kiểm tra và dùng lệnh:\n`/add {user_id}`"
        try:
            # Forward the photo WITH CAPTION to the Admin
            bot.send_photo(chat_id=ADMIN_ID, photo=file_id, caption=admin_caption, parse_mode='Markdown') # Specify parse mode
            bot.reply_to(message, "✅ Đã nhận được ảnh thanh toán của bạn. Admin sẽ kiểm tra và kích hoạt VIP sớm!")
            print(f"[PAYMENT] Received payment photo from {user_id}. Forwarded to Admin.")
            # Remove user from tracking dict after successful forward
            del users_requested_payment[user_id]
        except Exception as e:
            bot.reply_to(message, f"❌ Lỗi gửi ảnh đến Admin. Vui lòng liên hệ Admin trực tiếp qua @{ADMIN_USERNAME} và gửi ảnh này nhé.")
            print(f"Error forwarding payment photo from {user_id} to Admin {ADMIN_ID}: {e}")


# --- Admin Commands ---

@bot.message_handler(commands=['add', 'adduser'])
def add_user(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, '❌ Bạn không có quyền.')
        delete_user_message(message)
        return

    args = message.text.split()
    # Usage: /add <user_id> [days] (0 or no days = permanent)
    if not (2 <= len(args) <= 3):
        bot.reply_to(message, f'⚠️ Sai cú pháp!\n`/add <ID> [số ngày]`\n(Mặc định {VIP_DURATION_DAYS} ngày, `0` hoặc để trống số ngày = Vĩnh viễn)')
        delete_user_message(message)
        return

    try:
        user_id_to_add = int(args[1])
    except ValueError:
        bot.reply_to(message, '❌ ID người dùng không hợp lệ.')
        delete_user_message(message)
        return

    expiration_time = None # Default to None (Permanent if no days specified)
    expiration_text = "Vĩnh Viễn" # Default text
    days_specified = False

    if len(args) == 3: # Days argument provided
        try:
            days = int(args[2])
            if days > 0:
                 expiration_time = datetime.now() + timedelta(days=days)
                 expiration_text = f"{days} ngày"
                 days_specified = True
            elif days == 0: # Explicitly 0 means permanent
                 expiration_time = None
                 expiration_text = "Vĩnh Viễn"
                 days_specified = True
            else: # days < 0
                 bot.reply_to(message, '❌ Số ngày phải >= 0.')
                 delete_user_message(message)
                 return
        except ValueError:
             bot.reply_to(message, '❌ Số ngày không hợp lệ.')
             delete_user_message(message)
             return
    elif len(args) == 2: # No days argument, use default duration *unless* config duration is 0/None
        if VIP_DURATION_DAYS and VIP_DURATION_DAYS > 0 :
             days = VIP_DURATION_DAYS
             expiration_time = datetime.now() + timedelta(days=days)
             expiration_text = f"{days} ngày (mặc định)"
        else: # If default is 0/None, then no argument means permanent
             expiration_time = None
             expiration_text = "Vĩnh Viễn"

    # Try to fetch user info for better messaging
    added_user_info_obj = None
    username_to_add = None
    try:
        added_user_info_obj = bot.get_chat(user_id_to_add)
        username_to_add = added_user_info_obj.username
    except Exception as e:
        print(f"Info: Could not fetch user info for {user_id_to_add}: {e}")

    # Save to database
    save_user_to_database(user_id_to_add, expiration_time, username_to_add)

    # Confirmation messages
    added_user_mention = get_user_mention(added_user_info_obj) # Use the helper function
    expiry_info = expiration_time.strftime('%d/%m/%Y %H:%M:%S') if expiration_time else "Vĩnh viễn"

    confirmation_group = (f"✅ Đã thêm VIP thành công!\n"
                          f"👤 Người dùng: {added_user_mention}\n"
                          f"⏳ Thời hạn: *{expiration_text}*\n"
                          f"📅 Hết hạn: `{expiry_info}`")
    try:
        bot.reply_to(message, confirmation_group, parse_mode='Markdown') # Add parse mode
    except Exception as e:
        print(f"Error sending group confirmation for /add: {e}")

    # Notify the added user
    try:
        confirmation_user = (f"🎉 Chúc mừng! Bạn đã được cấp quyền *VIP*.\n"
                             f"⏳ Thời hạn: *{expiration_text}*\n"
                             f"📅 Hết hạn: `{expiry_info}`\n\n"
                             f"Tham gia nhóm chat {CHAT_GROUP_LINK} nhé!")
        bot.send_message(user_id_to_add, confirmation_user, parse_mode='Markdown') # Add parse mode
    except Exception as e:
        # Try sending to group again if private message fails
        try:
            bot.reply_to(message, f"⚠️ Không thể gửi thông báo riêng cho {added_user_mention}. Vui lòng thông báo cho họ.", parse_mode='Markdown')
        except Exception as group_fallback_e:
             print(f"Error sending group fallback notification for /add to {user_id_to_add}: {group_fallback_e}")
        print(f"Error sending private notification for /add to {user_id_to_add}: {e}")

    delete_user_message(message)


@bot.message_handler(commands=['remove', 'removeuser'])
def remove_user_cmd(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, '❌ Bạn không có quyền.')
        delete_user_message(message)
        return

    args = message.text.split()
    if len(args) != 2:
        bot.reply_to(message, '⚠️ Sai cú pháp!\n`/remove <ID người dùng>`')
        delete_user_message(message)
        return

    try:
        user_id_to_remove = int(args[1])
    except ValueError:
        bot.reply_to(message, '❌ ID người dùng không hợp lệ.')
        delete_user_message(message)
        return

    # Check if user actually exists in DB before attempting removal
    conn_check = None
    user_existed = False
    username_removed = None # Store username if found
    try:
        conn_check = sqlite3.connect(DB_FILE)
        cursor_check = conn_check.cursor()
        # Fetch username along with existence check
        cursor_check.execute("SELECT username FROM users WHERE user_id = ?", (user_id_to_remove,))
        result = cursor_check.fetchone()
        if result:
             user_existed = True
             username_removed = result[0]
    except sqlite3.Error as e:
        bot.reply_to(message, f"❌ Lỗi kiểm tra database: {e}")
        print(f"SQLite error checking user existence in /remove: {e}")
        if conn_check: conn_check.close()
        delete_user_message(message)
        return
    finally:
         if conn_check: conn_check.close()


    if user_existed:
        remove_user_from_database(user_id_to_remove) # This handles DB and reloads list
        removed_user_display = f"@{username_removed}" if username_removed else f"ID `{user_id_to_remove}`"
        bot.reply_to(message, f'✅ Đã xóa người dùng VIP {removed_user_display}.')
        print(f"[ADMIN] Removed VIP User {removed_user_display} by Admin {message.from_user.id}")
        # Optionally notify the removed user
        try:
            bot.send_message(user_id_to_remove, "ℹ️ Quyền VIP của bạn đã bị Admin thu hồi.")
        except Exception as e:
            # Don't worry too much if notification fails
            print(f"Info: Could not notify user {user_id_to_remove} about VIP removal: {e}")
    else:
        bot.reply_to(message, f'ℹ️ User ID `{user_id_to_remove}` không có trong danh sách VIP.')

    delete_user_message(message)



@bot.message_handler(commands=['cleanup'])
def cleanup_expired_users_cmd(message): # Renamed function slightly
    if not is_admin(message.from_user.id):
        bot.reply_to(message, '❌ Bạn không có quyền.')
        delete_user_message(message)
        return

    deleted_count = delete_expired_users_from_db() # This function does the work

    if deleted_count > 0:
        bot.reply_to(message, f'🧹 Đã xóa `{deleted_count}` VIP hết hạn khỏi database.')
        print(f"[ADMIN] Cleaned up {deleted_count} expired users by Admin {message.from_user.id}")
    else:
        bot.reply_to(message, '🧹 Không có VIP nào hết hạn để xóa.')

    delete_user_message(message)

# --- START OF ADDITIONS/MODIFICATIONS ---
import logging
import math # Needed for format_timedelta

# Configure logging (optional, but good practice if using logging calls)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Define the missing helper function to get VIP users ---
def get_all_vip_users():
    """Fetches all active (non-expired or permanent) VIP users from the database."""
    conn = None
    users = []
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Select users whose expiration time is NULL (permanent) or in the future
        cursor.execute(
            'SELECT user_id, username, expiration_time FROM users WHERE expiration_time IS NULL OR expiration_time >= ? ORDER BY user_id',
            (now_str,)
        )
        users = cursor.fetchall() # fetchall returns a list of tuples
        logging.info(f"Fetched {len(users)} active VIP users from DB.")
    except sqlite3.Error as e:
        logging.error(f"Error fetching VIP users from database: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return users

# --- Define the missing helper function to format timedelta ---
def format_timedelta(delta):
    """Formats a timedelta object into a human-readable string (days, hours, minutes)."""
    if delta is None:
        return "N/A"

    total_seconds = int(delta.total_seconds())

    if total_seconds < 0:
        return "đã qua" # Or handle negative delta as needed

    days, remainder_seconds = divmod(total_seconds, 86400) # 86400 seconds in a day
    hours, remainder_seconds = divmod(remainder_seconds, 3600) # 3600 seconds in an hour
    minutes, seconds = divmod(remainder_seconds, 60)

    parts = []
    if days > 0:
        parts.append(f"{days} ngày")
    if hours > 0:
        parts.append(f"{hours} giờ")
    if minutes > 0 and days == 0: # Only show minutes if less than a day
         parts.append(f"{minutes} phút")

    if not parts:
        return "dưới 1 phút" # Or handle very short durations

    return ", ".join(parts)

# --- END OF ADDITIONS/MODIFICATIONS ---


## --- Sửa lại hàm list_vip_users_command ---
@bot.message_handler(commands=['listvip'])
def list_vip_users_command(message):
    """Handles the /listvip command to show VIP users with HTML formatting."""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "🚫 Bạn không có quyền sử dụng lệnh này.")
        return
    try:
        logging.info(f"Admin {message.from_user.id} requested VIP list.")
        active_vips = get_all_vip_users() # Assumes this function is defined and works

        if not active_vips:
            bot.reply_to(message, "ℹ️ Hiện không có thành viên VIP nào đang hoạt động.")
            return

        response_parts = [f"<b>📜 Danh Sách VIP Đang Hoạt Động ({len(active_vips)}) 📜</b>\n--------------------\n"]
        now = datetime.now()
        max_msg_len = 4000

        for user_id, username, expiration_time_str in active_vips:
            user_display_html = ""
            safe_username = username.replace('<', '<').replace('>', '>').replace('&', '&') if username else None
            if safe_username:
                 user_display_html = f"@{safe_username}"
            else:
                 user_display_html = f"User_{user_id}"
            user_info_str = f"👤 <code>{user_id}</code> ({user_display_html})"

            expiry_info_str = ""
            if expiration_time_str is None:
                expiry_info_str = "✨ <b>Vĩnh viễn</b>"
            else:
                expiry_dt = None # Initialize expiry_dt
                # --- START CHANGE: Try multiple formats ---
                possible_formats = [
                    '%Y-%m-%d %H:%M:%S.%f',  # Format with microseconds
                    '%Y-%m-%dT%H:%M:%S.%f', # Format with 'T' separator and microseconds
                    '%Y-%m-%d %H:%M:%S'   # Original format without microseconds
                ]
                for fmt in possible_formats:
                    try:
                        expiry_dt = datetime.strptime(expiration_time_str, fmt)
                        # If parsing succeeds, break the loop
                        break
                    except ValueError:
                        # If parsing fails, try the next format
                        continue
                # --- END CHANGE ---

                if expiry_dt: # Check if parsing succeeded with any format
                    if expiry_dt > now:
                        remaining = expiry_dt - now
                        expiry_info_str = f"⏳ Hết hạn: {expiry_dt.strftime('%d/%m/%Y %H:%M')} (~{format_timedelta(remaining)} còn lại)" # Assumes format_timedelta exists
                    else:
                        expiry_info_str = f"❌ Đã hết hạn: {expiry_dt.strftime('%d/%m/%Y %H:%M')}"
                        logging.warning(f"Expired user {user_id} appeared in /listvip. Expiry: {expiration_time_str}")
                else: # If all formats failed
                    expiry_info_str = f"⚠️ Lỗi định dạng ngày DB: '<code>{expiration_time_str}</code>'"
                    logging.error(f"Could not parse date format for user {user_id} with any known format: {expiration_time_str}")


            line = f"{user_info_str}\n   {expiry_info_str}\n--------------------\n"

            if len(response_parts[-1].encode('utf-8')) + len(line.encode('utf-8')) > max_msg_len:
                response_parts.append(line)
            else:
                response_parts[-1] += line

        first_message = None
        for i, part in enumerate(response_parts):
             msg_content = part
             try:
                 if i == 0:
                     first_message = bot.reply_to(message, msg_content, parse_mode='HTML')
                 else:
                     reply_to_msg_id = first_message.message_id if first_message else None
                     bot.send_message(message.chat.id, msg_content, parse_mode='HTML', reply_to_message_id=reply_to_msg_id)
                 time.sleep(0.3)
             except telebot.apihelper.ApiTelegramException as send_err:
                 logging.error(f"Error sending VIP list part {i+1}/{len(response_parts)}: {send_err}", exc_info=True)
                 try: # Fallback to plain text
                     plain_text_part = msg_content.replace('<b>','').replace('</b>','').replace('<code>','').replace('</code>','')
                     if i == 0: bot.reply_to(message, f"Lỗi gửi phần {i+1} (thử dạng text):\n{plain_text_part[:3500]}")
                     else: bot.send_message(message.chat.id, f"Lỗi gửi phần {i+1} (thử dạng text):\n{plain_text_part[:3500]}", reply_to_message_id=reply_to_msg_id)
                 except Exception as fallback_err: logging.error(f"Failed plain text fallback: {fallback_err}")
                 break # Stop if sending fails

    except Exception as handler_err:
        logging.error(f"Error in /listvip handler: {handler_err}", exc_info=True)
        bot.reply_to(message, f"❌ Lỗi khi hiển thị danh sách VIP: {handler_err}")
    finally:
        # Decide whether to delete the command message
        delete_user_message(message)


# --- Sửa lại hàm check_vip_plan ---
@bot.message_handler(commands=['plan'])
def check_vip_plan(message):
    """Checks the VIP status and expiration date for the user."""
    user = message.from_user
    user_id = user.id
    user_info = get_user_mention(user) # Assumes this function uses Markdown
    conn = None
    response_message = ""

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT expiration_time, username FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()

        if result:
            expiration_time_str, db_username = result

            # Optional username update logic here... (keep as before if needed)
            current_username = user.username
            if db_username != current_username:
                 try:
                      logging.info(f"[PLAN] Updating username for {user_id} from '{db_username}' to '{current_username}'")
                      cursor.execute("UPDATE users SET username = ? WHERE user_id = ?", (current_username, user_id))
                      conn.commit()
                 except sqlite3.Error as update_e:
                      logging.error(f"[ERROR] Failed to update username for {user_id} during /plan check: {update_e}")

            if expiration_time_str is None:
                response_message = f"✨ {user_info}, bạn là *VIP Vĩnh Viễn*!"
            else:
                expiry_dt = None # Initialize expiry_dt
                # --- START CHANGE: Try multiple formats ---
                possible_formats = [
                    '%Y-%m-%d %H:%M:%S.%f',  # Format with microseconds
                    '%Y-%m-%dT%H:%M:%S.%f', # Format with 'T' separator and microseconds
                    '%Y-%m-%d %H:%M:%S'   # Original format without microseconds
                ]
                for fmt in possible_formats:
                    try:
                        expiry_dt = datetime.strptime(expiration_time_str, fmt)
                        # If parsing succeeds, break the loop
                        break
                    except ValueError:
                        # If parsing fails, try the next format
                        continue
                # --- END CHANGE ---

                if expiry_dt: # Check if parsing succeeded
                    now = datetime.now()
                    if expiry_dt >= now:
                        time_left = expiry_dt - now
                        # Keep Markdown for /plan as intended
                        remaining_str_md = f"*{format_timedelta(time_left)}*" # Assumes format_timedelta exists
                        expiry_display_md = expiry_dt.strftime('%d/%m/%Y %H:%M:%S')
                        response_message = (f"✅ {user_info}, bạn là *VIP*.\n"
                                           f"📅 Hết hạn vào: `{expiry_display_md}`\n"
                                           f"⏳ Còn lại: {remaining_str_md}")
                    else:
                        expiry_display_md = expiry_dt.strftime('%d/%m/%Y %H:%M:%S')
                        response_message = f"⚠️ {user_info}, VIP của bạn đã *hết hạn* vào `{expiry_display_md}`.\n👉 /mua để gia hạn."
                else: # If all formats failed
                     response_message = f"❓ {user_info}, có lỗi xảy ra khi kiểm tra hạn VIP của bạn (dữ liệu ngày không hợp lệ: `{expiration_time_str}`)."
                     logging.error(f"Could not parse date format in /plan for user {user_id} with any known format: {expiration_time_str}")

        else:
            response_message = f"❌ {user_info}, bạn *chưa phải* là VIP.\n👉 /mua để nâng cấp."

        bot.reply_to(message, response_message, parse_mode='Markdown') # Keep Markdown for /plan

    except sqlite3.Error as e:
        bot.reply_to(message, f"❌ Lỗi database khi kiểm tra VIP.")
        logging.error(f"Database error in /plan for user {user_id}: {e}", exc_info=True)
    except telebot.apihelper.ApiTelegramException as tg_plan_e:
         bot.reply_to(message, f"❌ Lỗi Telegram khi kiểm tra VIP: {tg_plan_e}")
         logging.error(f"Telegram API error in /plan for user {user_id}: {tg_plan_e}", exc_info=True)
         logging.error(f"Failed /plan message text: {response_message}")
    except Exception as e:
        bot.reply_to(message, f"❌ Lỗi không xác định khi kiểm tra VIP.")
        logging.error(f"Unexpected error in /plan for user {user_id}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
        delete_user_message(message)
# --- Other Admin Commands ---
@bot.message_handler(commands=['rs'])
def handle_reset(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Bạn không có quyền!")
        delete_user_message(message)
        return

    try:
        bot.reply_to(message, "🔄 *Đang khởi động lại Bot...*")
        print(f"[ADMIN] Restart command issued by Admin {message.from_user.id}")
        # Stop polling gracefully before restarting
        # Kill running spam processes before restarting
        print("[RESTART] Stopping all running spam processes...")
        active_pids = []
        # Use list() to create a copy of keys for safe iteration while modifying the dict
        for sdt in list(running_spams.keys()):
            process_list = running_spams.get(sdt) # Get the list
            if process_list: # Check if list exists
                # Use list() again for safe iteration over the inner list
                for process in list(process_list):
                    if process and hasattr(process, 'poll') and callable(process.poll) and process.poll() is None:
                        try:
                            pid = process.pid
                            active_pids.append(pid)
                            print(f"[RESTART] Terminating PID {pid} for {sdt}...")
                            process.terminate()
                        except Exception as kill_e:
                            print(f"[RESTART] Error terminating PID {getattr(process, 'pid', 'N/A')}: {kill_e}")
            # Clean up the entry for this phone number regardless of success/failure stopping
            if sdt in running_spams:
                 del running_spams[sdt]
        time.sleep(1) # Allow termination signals to be processed

        # Check again and kill if needed using psutil
        if active_pids:
            print(f"[RESTART] Verifying termination of {len(active_pids)} processes...")
            for pid in active_pids:
                try:
                    p = psutil.Process(pid)
                    if p.is_running():
                        print(f"[RESTART] Force killing PID {pid}...")
                        p.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                     pass # Already stopped or cannot access
                except Exception as force_kill_e:
                     print(f"[RESTART] Error force killing PID {pid}: {force_kill_e}")

        print("[RESTART] Attempting to stop polling...")
        bot.stop_polling()
        time.sleep(2) # Allow potential threads to finish
        print("[RESTART] Executing restart...")
        # Execute the script again, replacing the current process
        os.execl(sys.executable, sys.executable, *sys.argv)
    except Exception as e:
        bot.reply_to(message, f"❌ Lỗi khởi động lại: {e}")
        print(f"Error during restart: {e}")
        # Attempt to restart polling if exec fails
        print("Restart failed, attempting to resume polling...")
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=60)
        except Exception as poll_e:
             print(f"FATAL: Could not resume polling after restart failure: {poll_e}")


@bot.message_handler(commands=['status'])
def status(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "❌ Bạn không có quyền!")
        delete_user_message(message)
        return
    try:
        # System Info
        uname = platform.uname()
        cpu_usage = psutil.cpu_percent(interval=0.5) # Shorter interval for quicker response
        memory_info = psutil.virtual_memory()
        disk_info = psutil.disk_usage('/')

        # Bot Info
        active_vips = len(allowed_users) # Use the loaded list
        uptime_seconds = int(time.time() - start_time)
        uptime_minutes, up_secs = divmod(uptime_seconds, 60)
        uptime_hours, up_mins = divmod(uptime_minutes, 60)
        uptime_days, up_hrs = divmod(uptime_hours, 24)
        uptime_str = f"{uptime_days}d {up_hrs}h {up_mins}m {up_secs}s"

        # Count active processes more accurately
        running_process_count = 0
        active_sdt_list = []
        sdt_processed_for_counting = set() # Avoid double counting if multiple entries exist transiently
        for sdt, process_list in running_spams.items():
            if process_list: # Check if list is not empty
                sdt_has_active_process = False
                for process in process_list:
                    if process and hasattr(process, 'poll') and callable(process.poll) and process.poll() is None:
                        running_process_count += 1
                        sdt_has_active_process = True
                if sdt_has_active_process and sdt not in sdt_processed_for_counting:
                    active_sdt_list.append(mask_phone(sdt)) # Add masked phone if active
                    sdt_processed_for_counting.add(sdt)

        status_message = (
            f"📊 *Trạng Thái Hệ Thống & Bot*\n\n"
            f"*Bot:*\n"
            f"- Status: {'🟢 Hoạt động' if bot_active else '🔴 Tạm dừng'}\n"
            f"- Admin Mode: {'🔒 Bật' if admin_mode else '🔓 Tắt'}\n"
            f"- Spam Free: {'✅ Bật' if free_spam_enabled else '❌ Tắt'}\n"
            f"- Chat Riêng: {'💬 Bật' if private_chat_enabled else '🏢 Chỉ Nhóm'}\n"
            f"- VIP hoạt động: `{active_vips}`\n"
            f"- Tiến trình Spam: `{running_process_count}`" # Changed variable name
        )
        if active_sdt_list:
             status_message += f" (cho SĐT: {', '.join(active_sdt_list)})"
        status_message += (
            f"\n- Uptime: `{uptime_str}`\n\n"
            f"*Server:*\n"
            f"- OS: `{uname.system} {uname.release}`\n"
            f"- CPU: `{cpu_usage:.1f}%`\n"
            f"- RAM: `{memory_info.percent:.1f}%` (`{memory_info.available / (1024 ** 3):.2f}` GB free)\n"
            f"- Disk: `{disk_info.percent:.1f}%` (`{disk_info.free / (1024 ** 3):.2f}` GB free)"
        )
        bot.reply_to(message, status_message, parse_mode='Markdown') # Ensure parse mode
    except Exception as e:
        bot.reply_to(message, f"❌ Lỗi lấy trạng thái: {e}")
        print(f"Error getting status: {e}")
    delete_user_message(message)

# --- Toggle Commands (Admin only) ---
# No changes needed for these toggle commands

@bot.message_handler(commands=['on', 'off'])
def toggle_bot_active(message):
    if not is_admin(message.from_user.id): return delete_user_message(message)
    global bot_active
    action = message.text.split()[0].lower()
    if action == '/on':
        bot_active = True; bot.reply_to(message, '🟢 Bot đã *bật*.'); print(f"[ADMIN] Bot enabled by {message.from_user.id}")
    elif action == '/off':
        bot_active = False; bot.reply_to(message, '🔴 Bot đã *tắt*.'); print(f"[ADMIN] Bot disabled by {message.from_user.id}")
    delete_user_message(message)

@bot.message_handler(commands=['admod', 'unadmod'])
def toggle_admin_mode(message):
    if not is_admin(message.from_user.id): return delete_user_message(message)
    global admin_mode
    action = message.text.split()[0].lower()
    if action == '/admod':
        admin_mode = True; bot.reply_to(message, '🔒 Chế độ Admin (bảo trì) *bật*.'); print(f"[ADMIN] Admin mode enabled by {message.from_user.id}")
    elif action == '/unadmod':
        admin_mode = False; bot.reply_to(message, '🔓 Chế độ Admin (bảo trì) *tắt*.'); print(f"[ADMIN] Admin mode disabled by {message.from_user.id}")
    delete_user_message(message)

@bot.message_handler(commands=['freeon', 'freeoff'])
def toggle_free_spam(message):
    if not is_admin(message.from_user.id): return delete_user_message(message)
    global free_spam_enabled
    action = message.text.split()[0].lower()
    if action == '/freeon':
        free_spam_enabled = True; bot.reply_to(message, '✅ Lệnh `/spam` (Free) đã *bật*.'); print(f"[ADMIN] Free spam enabled by {message.from_user.id}")
    elif action == '/freeoff':
        free_spam_enabled = False; bot.reply_to(message, '❌ Lệnh `/spam` (Free) đã *tắt*.'); print(f"[ADMIN] Free spam disabled by {message.from_user.id}")
    delete_user_message(message)

@bot.message_handler(commands=['chaton', 'chatoff'])
def toggle_private_chat(message):
    if not is_admin(message.from_user.id): return delete_user_message(message)
    global private_chat_enabled
    action = message.text.split()[0].lower()
    if action == '/chaton':
        private_chat_enabled = True; bot.reply_to(message, '💬 Chế độ chat riêng *bật* (Hoạt động mọi nơi).'); print(f"[ADMIN] Private chat enabled by {message.from_user.id}")
    elif action == '/chatoff':
        private_chat_enabled = False; bot.reply_to(message, f'🏢 Chế độ chat riêng *tắt* (Chỉ hoạt động trong nhóm `{ALLOWED_GROUP_ID}`).'); print(f"[ADMIN] Private chat disabled by {message.from_user.id}")
    delete_user_message(message)

@bot.message_handler(commands=['abl'])
def add_to_blacklist(message):
    if not is_admin(message.from_user.id): return delete_user_message(message)
    params = message.text.split()[1:]
    if len(params) != 1: bot.reply_to(message, "⚠️ Sai cú pháp!\nVí dụ: `/abl 09xxxxxxxx`"); delete_user_message(message); return
    phone_number = params[0]
    if not phone_number.isdigit(): bot.reply_to(message, "📞 Số điện thoại không hợp lệ."); delete_user_message(message); return
    if phone_number in blacklist: bot.reply_to(message, f"ℹ️ Số `{phone_number}` đã có trong blacklist.")
    else: blacklist.add(phone_number); bot.reply_to(message, f"🚫 Đã thêm `{phone_number}` vào blacklist spam."); print(f"[ADMIN] Added {phone_number} to blacklist by {message.from_user.id}")
    delete_user_message(message)


# --- Facebook Share (Commented Out) ---
# ...

# --- Catch-all ---
# ...

# --- Main Execution ---
if __name__ == "__main__":
    print("-" * 30)
    print("Initializing Database...")
    init_db()
    print("Loading VIP Users...")
    load_users_from_database()
    print("Cleaning up expired VIP users...")
    delete_expired_users_from_db() # Clean up on start

    # Optional: Load blacklist from file here
    # try:
    #     with open("blacklist.txt", "r") as f:
    #         current_blacklist = {line.strip() for line in f if line.strip()}
    #     blacklist.update(current_blacklist) # Add loaded numbers to default ones
    #     print(f"Loaded {len(current_blacklist)} numbers from blacklist.txt. Total blacklist size: {len(blacklist)}")
    # except FileNotFoundError:
    #     print("blacklist.txt not found, starting with default blacklist.")
    # except Exception as e:
    #     print(f"Error loading blacklist.txt: {e}")


    print("-" * 30)
    print(f"Bot Admin ID: {ADMIN_ID}")
    print(f"Allowed Group ID: {ALLOWED_GROUP_ID}")
    print(f"Bot Username: @{BOT_USERNAME}")
    print(f"Admin Username: @{ADMIN_USERNAME}")
    print(f"Bot starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 30)
    print("Starting polling loop...")

    # Use infinity_polling for robustness
    while True:
        try:
             print("Bot connected and polling...")
             # Consider shorter timeout if bot becomes unresponsive often
             bot.infinity_polling(timeout=60, long_polling_timeout=60)
        except requests.exceptions.ReadTimeout as e:
             print(f"ReadTimeout: {e}. Reconnecting after 5s...")
             time.sleep(5)
        except requests.exceptions.ConnectionError as e:
             print(f"ConnectionError: {e}. Retrying in 15 seconds...")
             time.sleep(15)
        except telebot.apihelper.ApiTelegramException as e:
            print(f"API Exception: {e}. Continuing polling...")
            if 'Unauthorized' in str(e):
                 print("FATAL: Bot token might be invalid. Stopping.")
                 br
            elif 'Conflict: terminated by other getUpdates request' in str(e):
                 print("Polling conflict detected. Waiting 30s before retrying...")
                 time.sleep(30)
            else:
                 # Reduce sleep time for non-critical API errors?
                 time.sleep(10)
        except Exception as e:
             print(f"An unexpected error occurred: {e}")
             # Log the full traceback for unexpected errors
             import traceback
             traceback.print_exc()
             print("Restarting polling in 30 seconds...")
             time.sleep(30)

    print("Bot stopped.")
# --- END OF FILE main.py ---
