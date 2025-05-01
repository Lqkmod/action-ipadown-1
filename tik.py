# -*- coding: utf-8 -*-
import logging
import httpx
import json
import html
import os
import time
import random
import string
import re
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict

# Thêm import cho Inline Keyboard
from telegram import Update, Message, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
    CallbackQueryHandler,
    ApplicationHandlerStop,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError

# --- Cấu hình ---
BOT_TOKEN = "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" # <--- THAY TOKEN BOT CỦA BẠN
API_KEY = "khangdino99" # <--- API KEY TIM (NẾU CẦN CHO /tim, có thể bỏ trống)
ADMIN_USER_ID = 7193749511 # <<< --- ID TELEGRAM SỐ CỦA ADMIN

# ID của bot/user nhận bill - **QUAN TRỌNG: PHẢI LÀ ID SỐ**
BILL_FORWARD_TARGET_ID = 7193749511 # <<< --- THAY BẰNG ID SỐ CỦA @khangtaixiu_bot HOẶC ADMIN

# ID Nhóm chính để nhận thống kê (tùy chọn). Nếu không có nhóm, đặt thành None.
ALLOWED_GROUP_ID = -1002191171631 # <--- ID NHÓM CHÍNH HOẶC None
# Link mời nhóm (hiển thị trong menu /start, chỉ hoạt động nếu ALLOWED_GROUP_ID được đặt)
GROUP_LINK = "https://t.me/dinotool" # <<<--- THAY BẰNG LINK NHÓM CỦA BẠN (nếu có)

# --- API & Keys (Thay thế bằng API của bạn nếu có) ---
LINK_SHORTENER_API_KEY = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" # API Key Link Shortener (ví dụ: Yeumoney)
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # URL chứa key (thay key bằng {key})
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # URL API rút gọn link
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # URL API tăng tim (nếu có)
FOLLOW_API_URL_BASE = "https://api.thanhtien.site/lynk/dino/telefl.php" # URL API tăng follow

# --- Thời gian (giây) ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60   # Cooldown /tim, /fl (15 phút)
GETKEY_COOLDOWN_SECONDS = 2 * 60    # Cooldown /getkey (2 phút)
KEY_EXPIRY_SECONDS = 6 * 3600     # Key chưa nhập hết hạn sau 6 giờ
ACTIVATION_DURATION_SECONDS = 6 * 3600 # Key dùng được trong 6 giờ sau khi nhập
CLEANUP_INTERVAL_SECONDS = 3600   # Job dọn dẹp chạy mỗi giờ
TREO_INTERVAL_SECONDS = 15 * 60   # Khoảng cách giữa các lần treo (15 phút)
TREO_FAILURE_MSG_DELETE_DELAY = 15 # Xóa tin báo lỗi treo sau 15 giây
TREO_STATS_INTERVAL_SECONDS = 24 * 3600 # Job thống kê chạy mỗi 24 giờ
USER_GAIN_HISTORY_SECONDS = 24 * 3600 # Lưu lịch sử gain 24 giờ cho /xemfl24h

# --- Thông tin VIP & Thanh toán (Thay thế bằng thông tin của bạn) ---
VIP_PRICES = {
    15: {"price": "15.000 VND", "limit": 2, "duration_days": 15},
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
}
QR_CODE_URL = "https://i.imgur.com/49iY7Ft.jpeg" # Link ảnh QR Code (phải là link trực tiếp đến ảnh)
BANK_ACCOUNT = "KHANGDINO"                 # Số tài khoản
BANK_NAME = "VCB BANK"                    # Tên ngân hàng
ACCOUNT_NAME = "LE QUOC KHANG"              # Tên chủ tài khoản
PAYMENT_NOTE_PREFIX = "VIP DinoTool ID" # Nội dung CK: "VIP DinoTool ID <user_id>"

# --- Lưu trữ & Biến Global ---
DATA_FILE = "bot_persistent_data.json"
user_tim_cooldown = {}          # {user_id_str: timestamp}
user_fl_cooldown = defaultdict(dict) # {user_id_str: {target_lowercase: timestamp}}
user_getkey_cooldown = {}       # {user_id_str: timestamp}
valid_keys = {}                 # {key: {data}}
activated_users = {}            # {user_id_str: expiry_timestamp}
vip_users = {}                  # {user_id_str: {data}}
active_treo_tasks = defaultdict(dict) # {user_id_str: {target_lowercase: Task}} (RUNTIME)
persistent_treo_configs = {}    # {user_id_str: {target_lowercase: chat_id}} (PERSISTENT)
treo_stats = defaultdict(lambda: defaultdict(int)) # {uid_str: {target_lowercase: gain}} (For Stats Job)
last_stats_report_time = 0
user_daily_gains = defaultdict(lambda: defaultdict(list)) # {uid_str: {target_lowercase: [(ts, gain)]}} (/xemfl24h)
pending_bill_user_ids = set()   # User IDs waiting to send bill photo

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO,
    handlers=[logging.FileHandler("bot.log", encoding='utf-8'), logging.StreamHandler()]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.JobQueue").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.Application").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# --- Kiểm tra cấu hình thiết yếu ---
if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN": logger.critical("!!! BOT_TOKEN is missing !!!"); exit(1)
if not ADMIN_USER_ID: logger.critical("!!! ADMIN_USER_ID is missing !!!"); exit(1)
if not BILL_FORWARD_TARGET_ID or not isinstance(BILL_FORWARD_TARGET_ID, int):
    logger.critical("!!! BILL_FORWARD_TARGET_ID is missing/invalid (Must be numeric ID) !!!"); exit(1)
else: logger.info(f"Bill target: {BILL_FORWARD_TARGET_ID}")
if ALLOWED_GROUP_ID:
     logger.info(f"Stats reporting enabled for Group ID: {ALLOWED_GROUP_ID}")
     if not GROUP_LINK or GROUP_LINK == "YOUR_GROUP_INVITE_LINK": logger.warning("Group link missing/placeholder.")
     else: logger.info(f"Group Link: {GROUP_LINK}")
else: logger.warning("ALLOWED_GROUP_ID not set. Stats reporting disabled.")
# Optional checks (can comment out if not needed)
# if not LINK_SHORTENER_API_KEY: logger.warning("Link shortener API key missing. /getkey might fail.")
# if not API_KEY: logger.warning("API_KEY (for /tim) missing.")

# --- Hàm Lưu/Tải Dữ Liệu (Đảm bảo key lowercase) ---
def save_data():
    # Explicitly use globals that are modified
    global valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown, treo_stats, last_stats_report_time, persistent_treo_configs, user_daily_gains
    data_to_save = {}
    try:
        # Prepare data with string/lowercase keys where appropriate
        s_activated = {str(k): v for k, v in activated_users.items()}
        s_tim_cd = {str(k): v for k, v in user_tim_cooldown.items()}
        plain_fl_cd = {str(uid): {str(t).lower(): ts for t, ts in targets.items()} for uid, targets in user_fl_cooldown.items() if targets}
        s_getkey_cd = {str(k): v for k, v in user_getkey_cooldown.items()}
        s_vip = {str(k): v for k, v in vip_users.items()}
        plain_stats = {str(uid): {str(t).lower(): g for t, g in targets.items()} for uid, targets in treo_stats.items() if targets}
        s_persist_treo = {str(uid): {str(t).lower(): int(cid) for t, cid in configs.items()} for uid, configs in persistent_treo_configs.items() if configs}
        s_daily_gains = {
            str(uid): { str(t).lower(): [(float(ts), int(g)) for ts, g in gl if isinstance(ts, (int, float)) and isinstance(g, int)]
                        for t, gl in tdata.items() if gl }
            for uid, tdata in user_daily_gains.items() if tdata
        }

        data_to_save = {
            "valid_keys": valid_keys, "activated_users": s_activated, "vip_users": s_vip,
            "user_cooldowns": {"tim": s_tim_cd, "fl": plain_fl_cd, "getkey": s_getkey_cd},
            "treo_stats": plain_stats, "last_stats_report_time": last_stats_report_time,
            "persistent_treo_configs": s_persist_treo, "user_daily_gains": s_daily_gains
        }
        # Atomic write using temporary file
        temp_file = DATA_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, DATA_FILE)
        logger.debug("Data saved.")

    except Exception as e:
        logger.error(f"SAVE DATA FAILED: {e}", exc_info=True)
        # Cleanup temp file if error occurs during write/replace
        if 'temp_file' in locals() and os.path.exists(temp_file):
            try: os.remove(temp_file)
            except Exception as e_rem: logger.error(f"Failed remove temp save file: {e_rem}")

def load_data():
    # Explicitly use globals to modify them
    global valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown, treo_stats, last_stats_report_time, persistent_treo_configs, user_daily_gains
    # Reset global variables to ensure clean state before loading
    valid_keys, activated_users, vip_users = {}, {}, {}
    user_tim_cooldown, user_getkey_cooldown = {}, {}
    user_fl_cooldown = defaultdict(dict)
    treo_stats = defaultdict(lambda: defaultdict(int))
    persistent_treo_configs = {}
    user_daily_gains = defaultdict(lambda: defaultdict(list))
    last_stats_report_time = 0

    if not os.path.exists(DATA_FILE):
        logger.info(f"{DATA_FILE} not found. Starting fresh.")
        return # Nothing to load

    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        valid_keys = data.get("valid_keys", {})
        activated_users = data.get("activated_users", {})
        vip_users = data.get("vip_users", {})

        # Cooldowns
        cooldowns = data.get("user_cooldowns", {})
        user_tim_cooldown = cooldowns.get("tim", {})
        loaded_fl = cooldowns.get("fl", {})
        if isinstance(loaded_fl, dict):
            user_fl_cooldown = defaultdict(dict, {str(uid): {str(t).lower(): ts for t, ts in targets.items()}
                                                  for uid, targets in loaded_fl.items() if isinstance(targets, dict)})
        user_getkey_cooldown = cooldowns.get("getkey", {})

        # Treo Stats
        loaded_stats = data.get("treo_stats", {})
        if isinstance(loaded_stats, dict):
            treo_stats = defaultdict(lambda: defaultdict(int))
            for uid, targets in loaded_stats.items():
                if isinstance(targets, dict):
                    treo_stats[str(uid)] = defaultdict(int, {str(t).lower(): int(g) for t, g in targets.items()})
        last_stats_report_time = data.get("last_stats_report_time", 0)

        # Persistent Treo Configs
        loaded_persist = data.get("persistent_treo_configs", {})
        if isinstance(loaded_persist, dict):
            persistent_treo_configs = {str(uid): {str(t).lower(): int(cid) for t, cid in configs.items() if isinstance(t, str) and isinstance(cid, int)}
                                        for uid, configs in loaded_persist.items() if isinstance(configs, dict)}

        # Daily Gains
        loaded_gains = data.get("user_daily_gains", {})
        if isinstance(loaded_gains, dict):
            user_daily_gains = defaultdict(lambda: defaultdict(list))
            for uid, tdata in loaded_gains.items():
                 user_id_key = str(uid)
                 if isinstance(tdata, dict):
                     for target, gain_list in tdata.items():
                         target_key = str(target).lower()
                         if isinstance(gain_list, list):
                              valid_gain_entries = []
                              for item in gain_list:
                                  try: # Validate each entry structure and type
                                       if isinstance(item, (list, tuple)) and len(item) == 2: valid_gain_entries.append((float(item[0]), int(item[1])))
                                  except (ValueError, TypeError, IndexError): pass # Skip invalid entries silently during load
                              if valid_gain_entries: user_daily_gains[user_id_key][target_key].extend(valid_gain_entries)

        logger.info(f"Data loaded successfully from {DATA_FILE}")

    except (json.JSONDecodeError, TypeError, Exception) as e:
        logger.error(f"LOAD DATA FAILED: {e}. Using default empty data.", exc_info=True)
        # Reset globals again on load failure
        valid_keys, activated_users, vip_users = {}, {}, {}
        user_tim_cooldown, user_getkey_cooldown = {}, {}
        user_fl_cooldown = defaultdict(dict)
        treo_stats = defaultdict(lambda: defaultdict(int))
        persistent_treo_configs = {}
        user_daily_gains = defaultdict(lambda: defaultdict(list))
        last_stats_report_time = 0


# --- Helper Functions ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Safely attempts to delete the user's message."""
    msg_id = message_id or (update.message.message_id if update and update.message else None)
    chat_id = update.effective_chat.id if update and update.effective_chat else None
    if msg_id and chat_id:
        try: await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except (Forbidden, BadRequest): pass # Ignore common deletion errors
        except Exception as e: logger.error(f"Delete Msg Err: {e}", exc_info=True)

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Job to delete a message by ID."""
    data = context.job.data
    if isinstance(data, dict): # Basic check for job data format
        chat_id, message_id = data.get('chat_id'), data.get('message_id')
        if chat_id and message_id:
            try: await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except (Forbidden, BadRequest): pass
            except Exception as e: logger.error(f"Delete Job Err: {e}", exc_info=True)

async def send_temporary_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, duration: int = 15, parse_mode: str = ParseMode.HTML, reply: bool = True):
    """Sends a message and schedules its deletion."""
    if not update or not update.effective_chat: return
    chat_id = update.effective_chat.id
    sent_msg = None
    try:
        reply_to = update.message.message_id if reply and update.message else None
        try:
            sent_msg = await context.bot.send_message(chat_id, text, parse_mode=parse_mode, reply_to_message_id=reply_to, disable_web_page_preview=True)
        except BadRequest as e: # Handle reply error by sending without reply
            if reply_to and "reply message not found" in str(e).lower():
                sent_msg = await context.bot.send_message(chat_id, text, parse_mode=parse_mode, disable_web_page_preview=True)
            else: raise e # Re-raise other errors
        if sent_msg and context.job_queue:
            job_name = f"del_{chat_id}_{sent_msg.message_id}"
            context.job_queue.run_once(delete_message_job, duration, data={'chat_id': chat_id, 'message_id': sent_msg.message_id}, name=job_name)
    except Exception as e: logger.error(f"Send Temp Msg Err: {e}", exc_info=True)

def generate_random_key(length=8):
    """Generates a random key string."""
    return f"Dinotool-{''.join(random.choices(string.ascii_uppercase + string.digits, k=length))}"

# --- VIP/Key Status Checkers ---
def is_user_vip(user_id: int) -> bool:
    vip_data = vip_users.get(str(user_id))
    try: return bool(vip_data and time.time() < float(vip_data.get("expiry", 0)))
    except (ValueError, TypeError): return False

def get_vip_limit(user_id: int) -> int:
    if is_user_vip(user_id):
        try: return int(vip_users.get(str(user_id), {}).get("limit", 0))
        except (ValueError, TypeError): pass
    return 0

def is_user_activated_by_key(user_id: int) -> bool:
    expiry = activated_users.get(str(user_id))
    try: return bool(expiry and time.time() < float(expiry))
    except (ValueError, TypeError): return False

def can_use_feature(user_id: int) -> bool:
    """Check if user can use standard features like /tim, /fl."""
    return is_user_vip(user_id) or is_user_activated_by_key(user_id)

# --- Treo Task Management (Uses lowercase keys) ---
async def stop_treo_task(user_id_str: str, target_key: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Command") -> bool:
    """Stops a specific treo task/config using its lowercase key. Returns True if modified."""
    global persistent_treo_configs, active_treo_tasks
    user_id_str, target_key = str(user_id_str), str(target_key).lower()
    modified, needs_save = False, False

    # Stop Runtime Task if exists and running
    user_tasks = active_treo_tasks.get(user_id_str)
    if user_tasks and target_key in user_tasks:
        task = user_tasks.pop(target_key, None) # Atomically get and remove
        if not user_tasks: active_treo_tasks.pop(user_id_str, None) # Clean up user dict if empty
        if task and isinstance(task, asyncio.Task) and not task.done():
            name = getattr(task, 'get_name', lambda: f"task_{user_id_str}_{target_key}")()
            logger.info(f"[Stop Task] Cancelling RT '{name}' Reason: {reason}")
            task.cancel()
            try: await asyncio.wait_for(task, 0.5) # Brief wait for cancellation
            except (asyncio.CancelledError, asyncio.TimeoutError): pass
            except Exception as e: logger.error(f"[Stop Task] Await Cancel Err '{name}': {e}")
        modified = True
        logger.info(f"[Stop Task] RT entry processed for {user_id_str}->{target_key}.")

    # Remove Persistent Config if exists
    user_configs = persistent_treo_configs.get(user_id_str)
    if user_configs and target_key in user_configs:
        user_configs.pop(target_key, None)
        if not user_configs: persistent_treo_configs.pop(user_id_str, None)
        logger.info(f"[Stop Task] PS config removed for {user_id_str}->{target_key}.")
        needs_save = True
        modified = True

    if needs_save: save_data()
    return modified

async def stop_all_treo_tasks_for_user(user_id_str: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Command") -> int:
    """Stops all treo tasks/configs for a user. Returns count stopped/removed."""
    user_id_str = str(user_id_str)
    keys_p = set(persistent_treo_configs.get(user_id_str, {}).keys())
    keys_r = set(active_treo_tasks.get(user_id_str, {}).keys())
    keys_to_stop = keys_p | keys_r # Combine all known keys (lowercase)

    if not keys_to_stop:
        logger.info(f"[Stop All] No items found for user {user_id_str}.")
        return 0

    logger.info(f"[Stop All] User {user_id_str}: Stopping {len(keys_to_stop)} items. Reason: {reason}")
    count = 0
    # Use gather for slight potential concurrency, mostly for clean code
    results = await asyncio.gather(*[stop_treo_task(user_id_str, k, context, reason) for k in keys_to_stop], return_exceptions=True)
    for res in results:
        if isinstance(res, bool) and res: count += 1 # Count successful stops/removals
        elif isinstance(res, Exception): logger.error(f"[Stop All] Error during single stop: {res}")
    logger.info(f"[Stop All] User {user_id_str}: Finished. Count={count}/{len(keys_to_stop)}.")
    # Save happens within stop_treo_task if needed
    return count


# --- API Call Logic ---
async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """Calls the Follow API. Assumes target_username has original casing needed by API."""
    api_params = {"user": target_username, "userid": user_id_str, "tokenbot": bot_token}
    log_target = html.escape(target_username) # Log with original case
    logger.info(f"[API Call /fl] User {user_id_str} -> @{log_target}")
    result = {"success": False, "message": "Lỗi không xác định.", "data": None}

    try:
        async with httpx.AsyncClient(verify=False, timeout=120.0) as client:
            resp = await client.get(FOLLOW_API_URL_BASE, params=api_params, headers={'User-Agent': 'TG Bot FL Caller v3.2'})
            content_type = resp.headers.get("content-type", "").lower()
            response_text = await resp.atext(encoding='utf-8', errors='replace')
            response_preview = response_text[:500].replace('\n', ' ')
            logger.debug(f"[API Resp /fl @{log_target}] Status={resp.status_code} Type='{content_type}' Preview='{response_preview}...'")

            if resp.status_code == 200:
                data, is_json = None, "application/json" in content_type
                try:
                    if is_json or (response_text.strip().startswith("{") and response_text.strip().endswith("}")):
                        data = json.loads(response_text)
                        result["data"] = data
                        status = data.get("status"); message = data.get("message")
                        is_success = status is True or str(status).lower() in ['true', 'success', 'ok', '200']
                        result["success"] = is_success
                        result["message"] = str(message or ("Thành công." if is_success else "Thất bại."))
                    else: # Plain text 200 OK
                        is_err = any(w in response_text.lower() for w in ['lỗi','error','fail'])
                        result["success"] = len(response_text) < 150 and not is_err
                        result["message"] = "Thành công (non-JSON)." if result["success"] else f"Lỗi API (text: {response_preview}...)"
                except json.JSONDecodeError:
                    result = {"success": False, "message": "Lỗi: API trả về JSON không hợp lệ.", "data": None}
                except Exception as e_proc:
                    result = {"success": False, "message": f"Lỗi xử lý data API: {type(e_proc).__name__}", "data": None}
                    logger.error(f"[API Proc Err /fl @{log_target}]: {e_proc}", exc_info=True)
            else: # HTTP error
                result = {"success": False, "message": f"Lỗi API ({resp.status_code}).", "data": None}

    except httpx.TimeoutException: result = {"success": False, "message": "Lỗi: API Timeout.", "data": None}
    except httpx.NetworkError as e: result = {"success": False, "message": "Lỗi: Network Error.", "data": None}; logger.error(f"[API Net Err /fl @{log_target}]: {e}")
    except Exception as e: result = {"success": False, "message": "Lỗi hệ thống Bot.", "data": None}; logger.error(f"[API Call Err /fl @{log_target}]: {e}", exc_info=True)

    result["message"] = html.escape(str(result.get("message", "Lỗi không rõ.")))
    return result

async def call_tim_api(video_url: str, api_key: str) -> dict:
    """Calls the Tim API."""
    # This function needs implementation based on the Tim API spec
    # Placeholder implementation:
    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=api_key or "")
    logger.info(f"[API Call /tim] URL: {api_url.replace(api_key,'***') if api_key else api_url}")
    try:
        async with httpx.AsyncClient(verify=False, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'TG Tim Bot v3.2'})
            if resp.status_code == 200 and "application/json" in resp.headers.get("content-type","").lower():
                try:
                     data = resp.json()
                     status = data.get("status") or data.get("success")
                     success = status is True or str(status).lower() in ["success","true","ok","200"]
                     return {"success": success, "message": data.get("message", "API Response"), "data": data.get("data")}
                except json.JSONDecodeError: return {"success": False, "message": "API response sai format JSON.", "data": None}
            else: return {"success": False, "message": f"Lỗi API tăng tim (HTTP {resp.status_code}).", "data": None}
    except httpx.TimeoutException: return {"success": False, "message": "Lỗi: API tăng tim Timeout.", "data": None}
    except httpx.NetworkError: return {"success": False, "message": "Lỗi: Mạng khi gọi API tim.", "data": None}
    except Exception as e: logger.error(f"Tim API Error: {e}", exc_info=True); return {"success": False, "message": "Lỗi hệ thống Bot khi gọi API tim.", "data": None}


# --- Command Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    logger.info(f"User {user.id} /start or /menu in {chat_id}")
    act_h = ACTIVATION_DURATION_SECONDS // 3600; treo_m = TREO_INTERVAL_SECONDS // 60
    welcome = (f"👋 <b>Chào {user.mention_html()}!</b>\n"
               f"🤖 DinoTool - Bot hỗ trợ TikTok.\n"
               f"✨ Free: <code>/getkey</code> » <code>/nhapkey <key></code> ({act_h}h <code>/tim</code>, <code>/fl</code>).\n"
               f"👑 VIP: <code>/treo</code> (~{treo_m}p/lần), <code>/xemfl24h</code>.\n"
               f"👇 Chọn tùy chọn:")
    kb = [[InlineKeyboardButton("👑 Mua VIP", callback_data="show_muatt")],
          [InlineKeyboardButton("📜 Lệnh", callback_data="show_lenh")]]
    if GROUP_LINK and GROUP_LINK != "YOUR_GROUP_INVITE_LINK":
        kb.append([InlineKeyboardButton("💬 Nhóm", url=GROUP_LINK)])
    kb.append([InlineKeyboardButton("👨‍💻 Admin", url=f"tg://user?id={ADMIN_USER_ID}")])
    try:
        await delete_user_message(update, context)
        await context.bot.send_message(chat_id, welcome, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e: logger.warning(f"/start Err {user.id}: {e}")

async def menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query; await query.answer()
    user = query.from_user; chat = query.message.chat
    if not user or not chat or not query.data or not query.message: return
    cmd_name = query.data.split('_')[1]
    logger.info(f"Callback '{query.data}' by {user.id}")
    # Simulate command call
    fake_msg = Message(message_id=query.message.message_id + 1001, date=datetime.now(), chat=chat, from_user=user, text=f"/{cmd_name}")
    fake_update = Update(update_id=update.update_id + 1001, message=fake_msg)
    try: await query.delete_message()
    except Exception: pass # Ignore if message was already deleted
    if cmd_name == "muatt": await muatt_command(fake_update, context)
    elif cmd_name == "lenh": await lenh_command(fake_update, context)

async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id = user.id; uid_str = str(user_id)
    is_vip = is_user_vip(user_id); is_key = is_user_activated_by_key(user_id)
    can_std = is_vip or is_key
    status_ln = [f"👤 <b>{user.mention_html()}</b> (<code>{user_id}</code>)"]
    status = ""
    if is_vip:
        d=vip_users.get(uid_str,{}); exp=d.get("expiry"); lim=d.get("limit","?"); exp_s=f"{datetime.fromtimestamp(float(exp)):%d/%m %H:%M}" if exp else "N/A"; status=f"👑 VIP (Hạn: {exp_s}, Limit: {lim})"
    elif is_key: exp=activated_users.get(uid_str); exp_s=f"{datetime.fromtimestamp(float(exp)):%d/%m %H:%M}" if exp else "N/A"; status=f"🔑 Key Active (Hạn: {exp_s})"
    else: status="▫️ Thường"
    status_ln.append(f"<b>Trạng thái:</b> {status}")
    status_ln.append(f"⚡️ <b>/tim, /fl:</b> {'✅' if can_std else '❌ (Cần VIP/Key)'}")
    t_count=len(persistent_treo_configs.get(uid_str,{})); t_lim=get_vip_limit(user_id) if is_vip else 0
    status_ln.append(f"⚙️ <b>/treo:</b> {'✅' if is_vip else '❌ VIP'} (Treo: {t_count}/{t_lim})")

    tf_m=TIM_FL_COOLDOWN_SECONDS//60; gk_m=GETKEY_COOLDOWN_SECONDS//60; act_h=ACTIVATION_DURATION_SECONDS//3600; key_h=KEY_EXPIRY_SECONDS//3600; treo_m=TREO_INTERVAL_SECONDS//60
    cmds = ["📜 <b>LỆNH</b>",
            "<u>Free</u>:", f"/getkey (⏳{gk_m}p)", f"/nhapkey <key> (Dùng {act_h}h)",
            "<u>Tác vụ</u>:", f"/tim <link> (⏳{tf_m}p)", f"/fl <user> (⏳{tf_m}p)",
            "<u>VIP</u>:", "/muatt", f"/treo <user> (~{treo_m}p)", "/dungtreo <user|all>", "/listtreo", "/xemfl24h",
            "<u>Khác</u>:", "/menu", "/lenh"]
    if user_id == ADMIN_USER_ID:
        pkgs = ', '.join(map(str, VIP_PRICES))
        cmds.extend(["\n<u>Admin</u>:", f"/addtt <id> <gói:{pkgs}>", "/mess <text>"])

    help_text = "\n".join(status_ln) + "\n" + "\n".join([f"  <code>{l}</code>" if l.startswith("/") else f"<b>{l}</b>" if l.endswith(":") else l for l in cmds])
    try:
        await delete_user_message(update, context)
        await context.bot.send_message(chat_id, help_text, ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e: logger.warning(f"/lenh Err {user.id}: {e}")


async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id=user.id; uid_str=str(user_id); now=time.time(); msg_id=update.message.message_id

    if not can_use_feature(user_id):
        await send_temporary_message(update, context, f"⚠️ {user.mention_html()} cần VIP/Key cho /tim."); await delete_user_message(update, context); return

    last_use = user_tim_cooldown.get(uid_str)
    if last_use and now < last_use + TIM_FL_COOLDOWN_SECONDS:
        rem = (last_use + TIM_FL_COOLDOWN_SECONDS) - now
        await send_temporary_message(update, context, f"⏳ /tim: Chờ {rem:.0f} giây."); await delete_user_message(update, context); return

    args = context.args; url_raw = args[0] if args else None; url = None
    if url_raw: url = re.search(r"(https?://(?:www\.|m\.|vm\.|vt\.)?tiktok\.com/\S+)", url_raw)
    if not url:
        await send_temporary_message(update, context, "⚠️ Cú pháp: /tim <link_video>"); await delete_user_message(update, context); return
    video_url = url.group(1)

    await delete_user_message(update, context, msg_id) # Delete command first
    processing_msg = await context.bot.send_message(chat_id, "⏳ Đang xử lý /tim...") # Send status

    api_key = API_KEY or ""
    api_result = await call_tim_api(video_url, api_key) # Use the helper function

    final_text = ""
    if api_result["success"]:
        user_tim_cooldown[uid_str] = time.time(); save_data()
        d = api_result.get("data",{}); a=d.get("author","?"); v=html.escape(d.get("video_url", video_url)); db=d.get('digg_before','?'); di=d.get('digg_increased','?'); da=d.get('digg_after','?')
        final_text = f"❤️ <b>Tim OK!</b> {user.mention_html()}\n🎬 <a href='{v}'>{a or 'Video'}</a>\n📊 {db} +{di} » {da}"
    else: final_text = f"💔 Lỗi /tim: {api_result.get('message', 'API Error')}"

    try: await context.bot.edit_message_text(final_text, chat_id, processing_msg.message_id, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception: # If edit fails (e.g., message deleted), try sending new
        try: await context.bot.send_message(chat_id, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e_send: logger.error(f"/tim final send err: {e_send}")

async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id=user.id; uid_str=str(user_id); mention=user.mention_html(); now=time.time(); msg_id=update.message.message_id

    if not can_use_feature(user_id):
        await send_temporary_message(update, context, f"⚠️ {mention} cần VIP/Key cho /fl."); await delete_user_message(update, context); return

    args = context.args; target_user = None; err = None
    if not args: err = "⚠️ Cú pháp: /fl <username>"
    else:
        target_raw = args[0].strip().lstrip("@")
        if not target_raw: err = "⚠️ Username không được trống."
        else: target_user = target_raw # Keep original case for API call? Or use lowercase? Assume lowercase ok for API.
              target_key = target_user.lower() # ALWAYS use lowercase for internal dict key

    if err: await send_temporary_message(update, context, err); await delete_user_message(update, context); return

    # Check cooldown using lowercase key
    last_use = user_fl_cooldown.get(uid_str, {}).get(target_key)
    cooldown = TIM_FL_COOLDOWN_SECONDS
    if last_use and now < last_use + cooldown:
        rem = (last_use + cooldown) - now
        await send_temporary_message(update, context, f"⏳ /fl @{html.escape(target_user)}: Chờ {rem:.0f}s."); await delete_user_message(update, context); return

    await delete_user_message(update, context, msg_id) # Delete command
    proc_msg = await context.bot.send_message(chat_id, f"⏳ Đang xử lý /fl cho <code>@{html.escape(target_user)}</code>...", parse_mode=ParseMode.HTML)

    # Schedule background task
    context.application.create_task(
        process_fl_request_background(context, chat_id, uid_str, target_user, target_key, proc_msg.message_id, mention),
        name=f"fl_bg_{uid_str}_{target_key}")

async def process_fl_request_background(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id_str: str, target_username: str, target_key: str, processing_msg_id: int, invoking_user_mention: str):
    """Background task for /fl. Uses target_key (lowercase) for cooldown."""
    log_target = html.escape(target_username) # Use original case for logging if desired
    logger.info(f"[BG /fl] User {user_id_str} -> @{log_target}")
    # Call API with original case (target_username) assuming API might need it.
    # If API is case-insensitive, using target_key here is also fine.
    api_result = await call_follow_api(user_id_str, target_username, context.bot.token)
    success = api_result["success"]; api_message = api_result["message"]; api_data = api_result["data"]
    final_text = ""; info = ""; follow = ""

    if api_data and isinstance(api_data, dict): # Parse optional detailed info
        try:
            n=html.escape(str(api_data.get("name","?"))); u=html.escape(str(api_data.get("username",target_key))); a=api_data.get("avatar")
            info = f"👤 <a href='https://tiktok.com/@{u}'>{n}</a>" + (f" <a href='{html.escape(a)}'>🖼️</a>" if a else "")
            b=api_data.get("followers_before"); d=api_data.get("followers_add"); f=api_data.get("followers_after")
            if any(x is not None for x in [b,d,f]): # Only show if data exists
                bs = f"{int(b):,}" if isinstance(b,(int,float)) else str(b or '?')
                ds = "?"; # Handle follower delta carefully
                if isinstance(d,(int,float)): ds = f"+{int(d):,}" if d > 0 else f"{int(d):,}"
                elif isinstance(d,str): try: di=int(re.sub(r'[^\d-]','',d)); ds=f"+{di:,}" if di > 0 else f"{di:,}" except: ds=html.escape(d[:10])+"?"
                fs = f"{int(f):,}" if isinstance(f,(int,float)) else str(f or '?')
                follow = f"📈 <code>{html.escape(bs)} → {ds} → {html.escape(fs)}</code>"
        except Exception as e: logger.warning(f"[BG /fl Parse Err @{log_target}]: {e}")

    if success:
        # Use target_key (lowercase) for cooldown dict
        user_fl_cooldown[user_id_str][target_key] = time.time(); save_data()
        final_text = f"✅ <b>Follow OK!</b> {invoking_user_mention}\n{info}\n{follow}".strip()
    else: final_text = f"❌ <b>Lỗi /fl!</b> {invoking_user_mention}\n@{html.escape(target_username)}\n💬 <i>{api_message}</i>".strip()

    try: await context.bot.edit_message_text(final_text, chat_id, processing_msg_id, ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e: logger.warning(f"[BG /fl Edit Err]: {e}")

async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id=user.id; uid_str=str(user_id); now=time.time(); msg_id=update.message.message_id

    last_use = user_getkey_cooldown.get(uid_str)
    cooldown = GETKEY_COOLDOWN_SECONDS
    if last_use and now < last_use + cooldown:
        rem = (last_use + cooldown) - now
        await send_temporary_message(update, context, f"⏳ /getkey: Chờ {rem:.0f} giây."); await delete_user_message(update, context); return

    if not LINK_SHORTENER_API_KEY or not BLOGSPOT_URL_TEMPLATE or not LINK_SHORTENER_API_BASE_URL:
        await send_temporary_message(update, context, "❌ Lỗi cấu hình /getkey. Báo Admin."); await delete_user_message(update, context); return

    await delete_user_message(update, context, msg_id)
    proc_msg = await context.bot.send_message(chat_id, "⏳ Đang tạo link key...")

    gen_key = generate_random_key()
    while gen_key in valid_keys: gen_key = generate_random_key()
    key_stored = False; final_text = ""

    try:
        exp_ts = now + KEY_EXPIRY_SECONDS
        valid_keys[gen_key] = {"user_id_generator":user_id, "generation_time":now, "expiry_time":exp_ts, "used_by":None, "activation_time":None}
        save_data(); key_stored = True; # Save key first

        target_url = BLOGSPOT_URL_TEMPLATE.format(key=gen_key) + f"&_t={int(now%1000)}" # Add simple cache buster
        shorten_params = {"token": LINK_SHORTENER_API_KEY, "format": "json", "url": target_url}

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(LINK_SHORTENER_API_BASE_URL, params=shorten_params, headers={'User-Agent':'TG KeyGen Bot v3.2'})
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if data.get("status") == "success" and data.get("shortenedUrl"):
                        user_getkey_cooldown[uid_str] = time.time(); save_data() # Update cooldown on success
                        short_url = html.escape(data["shortenedUrl"])
                        key_h = KEY_EXPIRY_SECONDS // 3600
                        final_text = (f"🚀 <b>Link Key:</b> {user.mention_html()}\n🔗 <a href='{short_url}'>{short_url}</a>\n"
                                      f"📝 Click link -> Lấy key (VD: <code>Dinotool-XXX</code>) -> Gửi <code>/nhapkey <key></code>\n"
                                      f"⏳ Key hết hạn nhập sau {key_h} giờ.")
                    else: final_text = f"❌ Lỗi tạo link: {html.escape(data.get('message','API error'))}"
                except json.JSONDecodeError: final_text = "❌ Lỗi tạo link: API response sai."
            else: final_text = f"❌ Lỗi tạo link: API connect failed ({resp.status_code})."

    except Exception as e:
        logger.error(f"/getkey Err: {e}", exc_info=True); final_text = "❌ Lỗi hệ thống khi tạo key."
        if key_stored and valid_keys.get(gen_key,{}).get("used_by") is None: # Cleanup unused key if error occurred after saving
             valid_keys.pop(gen_key, None); save_data(); logger.info(f"Removed unused key {gen_key} due to error.")

    finally: # Edit processing message
        if proc_msg:
            try: await context.bot.edit_message_text(final_text, chat_id, proc_msg.message_id, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception: # Fallback send new if edit fails
                 try: await context.bot.send_message(chat_id, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                 except Exception as e_send: logger.error(f"/getkey final send err: {e_send}")


async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id=user.id; uid_str=str(user_id); now=time.time(); msg_id=update.message.message_id

    args = context.args; key_in = args[0].strip() if args else None; err = None
    prefix = "Dinotool-"
    if not key_in: err = "⚠️ Chưa nhập key. Cú pháp: /nhapkey Dinotool-KEY"
    elif not key_in.startswith(prefix) or len(key_in) <= len(prefix): err = "⚠️ Key sai định dạng."

    if err: await send_temporary_message(update, context, err); await delete_user_message(update, context); return

    await delete_user_message(update, context, msg_id)
    logger.info(f"User {user_id} /nhapkey: '{key_in}'")
    final_text = ""
    key_data = valid_keys.get(key_in)

    if not key_data: final_text = f"❌ Key <code>{html.escape(key_in)}</code> không tồn tại."
    elif key_data.get("used_by") is not None:
        used_uid = key_data["used_by"]
        time_s = f" lúc {datetime.fromtimestamp(float(key_data['activation_time'])):%H:%M %d/%m}" if key_data.get('activation_time') else ""
        final_text = f"⚠️ Bạn đã dùng key này rồi{time_s}." if str(used_uid) == uid_str else f"❌ Key đã bị user khác (...{str(used_uid)[-4:]}) dùng{time_s}."
    elif now > float(key_data.get("expiry_time", 0)): final_text = f"❌ Key <code>{html.escape(key_in)}</code> đã hết hạn nhập."
    else: # Activate!
        try:
            exp_ts = now + ACTIVATION_DURATION_SECONDS
            activated_users[uid_str] = exp_ts
            valid_keys[key_in]["used_by"] = user_id
            valid_keys[key_in]["activation_time"] = now
            save_data()
            act_h = ACTIVATION_DURATION_SECONDS//3600; exp_s = f"{datetime.fromtimestamp(exp_ts):%H:%M %d/%m/%Y}"
            logger.info(f"Key '{key_in}' activated by {user_id}. Expires: {exp_s}")
            final_text = f"✅ <b>Key OK!</b> {user.mention_html()}\n✨ Đã kích hoạt {act_h} giờ sử dụng /tim, /fl.\n⏳ Hết hạn: <b>{exp_s}</b>."
        except Exception as e:
             logger.error(f"Key activation Err {user_id} {key_in}: {e}", exc_info=True); final_text="❌ Lỗi hệ thống khi kích hoạt key."
             # Attempt rollback on error
             if valid_keys.get(key_in, {}).get("used_by") == user_id:
                 valid_keys[key_in]["used_by"] = None; valid_keys[key_in]["activation_time"] = None
             if uid_str in activated_users: activated_users.pop(uid_str, None)
             try: save_data()
             except Exception: logger.error("Failed save rollback state")

    try: await context.bot.send_message(chat_id, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e: logger.error(f"/nhapkey reply err {user.id}: {e}")

async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays VIP purchase info, QR, and send bill button."""
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return # Needs user for payment note
    original_message_id = update.message.message_id if update.message else None
    user_id = user.id
    pay_note = f"{PAYMENT_NOTE_PREFIX} {user_id}"

    text_ln = ["👑 <b>Thông Tin Mua VIP</b> 👑", "Trở thành VIP để mở khóa <code>/treo</code>, <code>/xemfl24h</code>."]
    text_ln.append("💎 <b>Các Gói:</b>")
    for days, info in VIP_PRICES.items():
        text_ln.append(f"\n⭐️ <b>Gói {info['duration_days']} Ngày</b>: {info['price']} (Limit {info['limit']})")
    text_ln.extend(["\n🏦 <b>Thanh toán:</b>", f"   NH: <b>{BANK_NAME}</b>",
                    f"   STK: <a href='https://t.me/share/url?url={html.escape(BANK_ACCOUNT)}'><code>{html.escape(BANK_ACCOUNT)}</code></a> (copy)",
                    f"   Tên TK: <b>{ACCOUNT_NAME}</b>",
                    "\n📝 <b>Nội dung CK (Quan trọng!):</b>",
                    f"   » <code>{html.escape(pay_note)}</code> <a href='https://t.me/share/url?url={html.escape(pay_note)}'>(copy)</a>",
                    "\n📸 <b>Sau khi CK thành công:</b>",
                    "   1. Chụp ảnh bill.", "   2. Nhấn nút 'Gửi Bill' bên dưới.", "   3. Gửi ảnh bill vào chat này."])
    text = "\n".join(text_ln)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📸 Gửi Bill Thanh Toán", callback_data=f"prompt_send_bill_{user_id}")]])

    # Delete original /muatt command if possible
    if original_message_id: await delete_user_message(update, context, original_message_id)

    # Try sending Photo first, fallback to text
    try:
        if QR_CODE_URL and QR_CODE_URL.startswith("http"):
            await context.bot.send_photo(chat_id, photo=QR_CODE_URL, caption=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        else: raise ValueError("No valid QR URL") # Trigger fallback
    except Exception as e:
        logger.warning(f"Muatt: QR send failed ({e}), falling back to text.")
        try: await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True)
        except Exception as e_txt: logger.error(f"Muatt fallback text err: {e_txt}")

async def prompt_send_bill_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query; await query.answer()
    user = query.from_user; msg = query.message
    if not user or not msg or not query.data: return
    try: expected_uid = int(query.data.split("_")[-1])
    except (ValueError, IndexError): return logger.warning(f"Invalid bill prompt cb: {query.data}")
    if user.id != expected_uid: return await query.answer("Đây không phải yêu cầu của bạn.", show_alert=True)

    pending_bill_user_ids.add(user.id)
    if context.job_queue: # Schedule removal from pending list after timeout
        job_name = f"rm_pend_bill_{user.id}"; jobs = context.job_queue.get_jobs_by_name(job_name)
        for j in jobs: j.schedule_removal() # Remove old job if exists
        context.job_queue.run_once(remove_pending_bill_user_job, 15*60, data={'user_id':user.id}, name=job_name)
    logger.info(f"User {user.id} pending bill in chat {msg.chat_id}")
    prompt = f"✅ {user.mention_html()}, vui lòng gửi ảnh bill vào <b>chat này</b>."
    try: await context.bot.send_message(msg.chat_id, prompt, ParseMode.HTML) # Don't delete msg with button
    except Exception as e: logger.error(f"Bill prompt send err {user.id}: {e}")

async def remove_pending_bill_user_job(context: ContextTypes.DEFAULT_TYPE):
    uid = context.job.data.get('user_id')
    if uid in pending_bill_user_ids:
        pending_bill_user_ids.discard(uid)
        logger.info(f"Timeout remove user {uid} from pending bill list.")

async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles incoming photos/image documents IF the user is in pending_bill_user_ids."""
    if not update or not update.message or not update.effective_user or not update.effective_chat: return
    user = update.effective_user; chat = update.effective_chat; msg = update.message

    # --- CRITICAL CHECK: Only process if user is pending ---
    if user.id not in pending_bill_user_ids: return

    is_photo = bool(msg.photo)
    is_img_doc = bool(msg.document and msg.document.mime_type and msg.document.mime_type.startswith('image/'))

    if not is_photo and not is_img_doc: return # Ignore other messages from pending user

    logger.info(f"BILL received from PENDING {user.id} in chat {chat.id}. FWD to {BILL_FORWARD_TARGET_ID}")

    # Remove user from pending list & cancel timeout job
    pending_bill_user_ids.discard(user.id)
    if context.job_queue:
        for j in context.job_queue.get_jobs_by_name(f"rm_pend_bill_{user.id}"): j.schedule_removal()

    # --- Forward bill with info ---
    info_lines = [f"📄 <b>BILL Thanh Toán</b>", f"👤 <b>Từ:</b> {user.mention_html()} (<code>{user.id}</code>)",
                  f"💬 <b>Trong:</b> {html.escape(chat.title or chat.type)} (<code>{chat.id}</code>)"]
    if msg.caption: info_lines.append(f"📝 <b>Note:</b> {html.escape(msg.caption[:200])}")
    info_text = "\n".join(info_lines)

    try:
        await context.bot.forward_message(BILL_FORWARD_TARGET_ID, chat.id, msg.message_id)
        await context.bot.send_message(BILL_FORWARD_TARGET_ID, info_text, ParseMode.HTML, disable_web_page_preview=True)
        await msg.reply_html("✅ Đã nhận và chuyển bill đến Admin!")
        logger.info(f"Bill forward success user {user.id}")
    except Exception as e:
        logger.error(f"Bill Forward/Info Err to {BILL_FORWARD_TARGET_ID}: {e}", exc_info=True)
        await msg.reply_html(f"❌ Lỗi khi gửi bill đến Admin. Liên hệ <a href='tg://user?id={ADMIN_USER_ID}'>Admin</a> trực tiếp.")
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID: # Notify admin if target is different
             try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi fwd bill từ {user.id} đến {BILL_FORWARD_TARGET_ID}: {e}")
             except Exception: pass

    raise ApplicationHandlerStop # Stop processing this message further

async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin adds/renews VIP."""
    if not update or not update.message or not update.effective_user or update.effective_user.id != ADMIN_USER_ID: return

    args = context.args; err=None; target_uid=None; days_key=None; limit=0; dur_days=0
    valid_keys_str = ', '.join(map(str, VIP_PRICES.keys()))

    if len(args) != 2: err = f"⚠️ Cú pháp: /addtt <user_id> <gói:{valid_keys_str}>"
    else:
        try: target_uid = int(args[0])
        except ValueError: err = f"⚠️ User ID '{html.escape(args[0])}' không hợp lệ."
        if not err:
            try:
                days_key = int(args[1])
                if days_key not in VIP_PRICES: err = f"⚠️ Gói {days_key} sai. Chọn: {valid_keys_str}."
                else: limit = VIP_PRICES[days_key]["limit"]; dur_days = VIP_PRICES[days_key]["duration_days"]
            except ValueError: err = f"⚠️ Gói '{html.escape(args[1])}' không phải số."

    if err: return await update.message.reply_html(err)

    uid_str = str(target_uid); now = time.time(); current_vip = vip_users.get(uid_str)
    start_time = now; op_type = "Nâng cấp"
    if current_vip: # Check if renewing
        try:
             if float(current_vip.get("expiry", 0)) > now: start_time = float(current_vip["expiry"]); op_type = "Gia hạn"
        except (ValueError, TypeError): logger.warning(f"Invalid existing expiry for {uid_str}")

    new_exp = start_time + dur_days * 86400; new_exp_s = f"{datetime.fromtimestamp(new_exp):%H:%M %d/%m/%Y}"
    vip_users[uid_str] = {"expiry": new_exp, "limit": limit}; save_data()
    logger.info(f"ADMIN: {op_type} VIP {dur_days}d for {uid_str}. Expiry={new_exp_s}, Limit={limit}")

    # Notify Admin
    admin_msg = f"✅ Đã <b>{op_type} {dur_days} ngày VIP</b>!\nUser: <code>{target_uid}</code>\nHạn: <b>{new_exp_s}</b>\nLimit: <b>{limit}</b>"
    await update.message.reply_html(admin_msg)

    # Notify User (try PM, fallback group)
    user_mention = f"User <code>{target_uid}</code>"
    try: info = await context.bot.get_chat(target_uid); user_mention = info.mention_html() or f"<a href='tg://user?id={target_uid}'>User</a>"
    except Exception: pass
    user_msg = (f"🎉 Chúc mừng {user_mention}!\nBạn đã được <b>{op_type} {dur_days} ngày VIP</b>.\n"
                f"⏳ Hạn đến: <b>{new_exp_s}</b>\n🚀 Limit treo: <b>{limit}</b>\n"
                f"Dùng <code>/lenh</code> để xem lệnh.")
    try: await context.bot.send_message(target_uid, user_msg, ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e_pm:
        logger.warning(f"Failed PM notify VIP {target_uid}: {e_pm}")
        if ALLOWED_GROUP_ID:
            try: await context.bot.send_message(ALLOWED_GROUP_ID, user_msg, ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e_grp: logger.error(f"Failed group notify VIP {target_uid}: {e_grp}")


async def mess_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin broadcasts message to active users (VIP/Key)."""
    if not update or not update.message or not update.effective_user or update.effective_user.id != ADMIN_USER_ID: return

    msg_parts = update.message.text.split(' ', 1)
    if len(msg_parts) < 2 or not msg_parts[1].strip(): return await update.message.reply_text("⚠️ Cú pháp: /mess <nội dung>")
    msg_to_send = msg_parts[1].strip()

    # Get recipients (active VIP or Key users)
    recipients = set()
    now = time.time()
    recipients.update(int(uid) for uid, d in vip_users.items() if now < float(d.get("expiry", 0)))
    recipients.update(int(uid) for uid, exp in activated_users.items() if now < float(exp))

    if not recipients: return await update.message.reply_text("ℹ️ Không có user active nào để gửi.")

    logger.info(f"ADMIN: Broadcast starting to {len(recipients)} users.")
    await update.message.reply_html(f"⏳ Bắt đầu gửi đến <b>{len(recipients)}</b> users...")

    s_count, f_count, b_count = 0, 0, 0; failed_ids = []
    for uid in recipients:
        try:
            await context.bot.send_message(uid, msg_to_send, ParseMode.HTML, disable_web_page_preview=True)
            s_count += 1
        except Forbidden: b_count += 1; f_count += 1; failed_ids.append(str(uid))
        except TelegramError as e: f_count += 1; failed_ids.append(str(uid)); logger.warning(f"Broadcast err {uid}: {e}")
        except Exception as e: f_count += 1; failed_ids.append(str(uid)); logger.error(f"Broadcast unexpected err {uid}: {e}", exc_info=True)
        await asyncio.sleep(0.05) # Throttle sends

    res_msg = (f"✅ <b>Broadcast Hoàn Tất!</b>\n"
               f"Thành công: <b>{s_count}</b> | Thất bại: <b>{f_count}</b> (Blocked: {b_count})\n")
    # if failed_ids: res_msg += f"IDs lỗi (VD): <code>{', '.join(failed_ids[:10])}{'...' if len(failed_ids)>10 else ''}</code>"
    try: await context.bot.send_message(ADMIN_USER_ID, res_msg, ParseMode.HTML)
    except Exception as e: logger.error(f"Failed send broadcast result to admin: {e}")

async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Background loop for /treo."""
    global user_daily_gains, treo_stats
    uid_int = int(user_id_str); target_key = target_username.lower() # Use lowercase key internally
    log_target = html.escape(target_username) # Original case for display/API
    task_name = f"treo_{user_id_str}_{log_target}"; logger.info(f"[Treo Task Start] '{task_name}' in {chat_id}")
    mention = f"User<code>{user_id_str[-4:]}</code>"; try: u=await context.bot.get_chat(uid_int); mention=u.mention_html() or mention except: pass

    last_call, consecutive_fails = 0, 0; MAX_FAILS = 5

    try:
        while True:
            now = time.time(); app = context.application
            current_task = asyncio.current_task()

            # 1. Pre-sleep Checks: Config, Runtime Task Match, VIP Status
            if not (user_id_str in persistent_treo_configs and target_key in persistent_treo_configs[user_id_str]):
                logger.warning(f"[Treo Stop] '{task_name}' - Persistent config missing."); break
            if not (active_treo_tasks.get(user_id_str,{}).get(target_key) is current_task):
                 logger.warning(f"[Treo Stop] '{task_name}' - Runtime task mismatch."); break
            if not is_user_vip(uid_int):
                logger.warning(f"[Treo Stop] '{task_name}' - User not VIP."); await stop_treo_task(user_id_str, target_key, context, "VIP Expired"); break

            # 2. Sleep until next interval
            if last_call > 0:
                wait = TREO_INTERVAL_SECONDS - (now - last_call)
                if wait > 0:
                    logger.debug(f"[Treo Wait] '{task_name}' waiting {wait:.1f}s.")
                    await asyncio.sleep(wait) # Can raise CancelledError here

            call_time = time.time(); last_call = call_time # Update time BEFORE call

            # 3. Pre-API Checks (double check after sleep)
            if not persistent_treo_configs.get(user_id_str,{}).get(target_key): logger.warning(f"[Treo Stop] '{task_name}' - Config removed during wait."); break
            if not is_user_vip(uid_int): logger.warning(f"[Treo Stop] '{task_name}' - VIP expired during wait."); await stop_treo_task(user_id_str, target_key, context, "VIP Expired Wait"); break

            # 4. Call API (use original target_username casing for API)
            logger.info(f"[Treo Run] '{task_name}' calling API for @{log_target}")
            result = await call_follow_api(user_id_str, target_username, app.bot.token)
            success=result["success"]; msg=result["message"]; data=result["data"]; gain=0

            # 5. Process Result & Format Status
            status_title, info, follow = "", "", ""
            if success:
                consecutive_fails = 0; logger.info(f"[Treo Success] '{task_name}'")
                status_title = f"✅ <code>@{log_target}</code> {mention}: OK!"
                if data: # Try parse details
                     try:
                         n=html.escape(str(data.get("name","?"))); u=html.escape(str(data.get("username",target_key))); av=data.get("avatar")
                         info = f"👤 <a href='https://tiktok.com/@{u}'>{n}</a>"+ (f" <a href='{html.escape(a)}'>🖼️</a>" if a else "")
                         b=data.get("followers_before"); d=data.get("followers_add"); f=data.get("followers_after");
                         # Calculate gain
                         if isinstance(d,(int,float)): gain = int(d)
                         elif isinstance(d,str): try: gain=int(re.sub(r'[^\d-]','',d)) except: pass
                         # Format follow string
                         if any(x is not None for x in [b,d,f]):
                              bs = f"{int(b):,}" if isinstance(b,(int,float)) else str(b or '?')
                              ds = f"+{gain:,}" if gain>0 else f"{gain:,}" if gain is not None else "?"
                              fs = f"{int(f):,}" if isinstance(f,(int,float)) else str(f or '?')
                              follow = f"📈 <code>{html.escape(bs)} → {ds} → {html.escape(fs)}</code>"
                         elif gain > 0: follow = f"📈 Tăng: <b>+{gain:,}</b>" # Fallback if only gain provided

                     except Exception as e: logger.warning(f"[Treo Parse Err @{log_target}] {e}")

                # *** Record Positive Gain ***
                if gain > 0:
                     treo_stats[user_id_str][target_key] += gain # For job stats
                     user_daily_gains[user_id_str][target_key].append((call_time, gain)) # For /xemfl24h
                     logger.info(f"[Treo Gain] '{task_name}' +{gain}")
                     # No immediate save needed here, handled by save_data calls elsewhere or shutdown

            else: # Failure
                consecutive_fails += 1; logger.warning(f"[Treo Fail] '{task_name}' ({consecutive_fails}/{MAX_FAILS}) Msg: {msg}")
                status_title = f"❌ <code>@{log_target}</code> {mention}: Lỗi!"
                info = f"💬 <i>{msg} ({consecutive_fails}/{MAX_FAILS})</i>"
                if consecutive_fails >= MAX_FAILS:
                    logger.error(f"[Treo Stop] '{task_name}' Max failures reached."); await stop_treo_task(user_id_str, target_key, context, "Max API Fails"); break

            # 6. Send Status Message
            status_lines = [status_title, info, follow]
            status_full = "\n".join(filter(None, status_lines)) # Filter empty strings
            sent_status = None
            try:
                 sent_status = await app.bot.send_message(chat_id, status_full, ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)
                 if not success and sent_status and app.job_queue: # Delete failure messages after delay
                     job_name_del = f"del_treo_fail_{sent_status.message_id}"
                     app.job_queue.run_once(delete_message_job, TREO_FAILURE_MSG_DELETE_DELAY, data={'chat_id':chat_id,'message_id':sent_status.message_id}, name=job_name_del)
            except Forbidden: logger.error(f"[Treo Stop] Bot forbidden in {chat_id} for '{task_name}'"); await stop_treo_task(user_id_str, target_key, context, "Bot Forbidden"); break
            except TelegramError as e: logger.error(f"[Treo Send Err] Chat {chat_id} for '{task_name}': {e}")
            except Exception as e: logger.error(f"[Treo Send Err Unexp] '{task_name}': {e}", exc_info=True)

    except asyncio.CancelledError: logger.info(f"[Treo Task Cancelled] '{task_name}'")
    except Exception as e: logger.error(f"[Treo Task FATAL] '{task_name}': {e}", exc_info=True); await stop_treo_task(user_id_str, target_key, context, f"FATAL Error: {e}") # Attempt cleanup
    finally:
         logger.info(f"[Treo Task End] '{task_name}'")
         # Optional: Ensure runtime task dict is cleaned up if task exited without stop_treo_task
         try: # Use try-except as current_task() can fail if loop closed
             if active_treo_tasks.get(user_id_str,{}).get(target_key) is asyncio.current_task() and asyncio.current_task().done():
                active_treo_tasks.get(user_id_str,{}).pop(target_key, None)
                if not active_treo_tasks.get(user_id_str): active_treo_tasks.pop(user_id_str, None)
                logger.info(f"[Treo Task Final Cleanup] Removed self-terminated task '{task_name}'")
         except Exception: pass


async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Starts auto-following (VIP only). Uses lowercase key."""
    if not update or not update.message or not update.effective_user: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id=user.id; uid_str=str(user_id); mention=user.mention_html(); msg_id=update.message.message_id

    if not is_user_vip(user_id):
        await send_temporary_message(update, context, "⚠️ Lệnh /treo chỉ dành cho VIP."); await delete_user_message(update, context); return

    args = context.args; target_user = None; err = None
    if not args: err = "⚠️ Cú pháp: /treo <username>"
    else: target_raw = args[0].strip().lstrip("@"); err = "⚠️ Username trống." if not target_raw else None; target_user=target_raw # Keep original case for API

    if err: await send_temporary_message(update, context, err); await delete_user_message(update, context); return

    target_key = target_user.lower() # Always use lowercase for internal key
    log_target = html.escape(target_user) # Original case for logging/display
    vip_limit = get_vip_limit(user_id)
    p_configs = persistent_treo_configs.get(uid_str, {})
    current_count = len(p_configs)

    if target_key in p_configs: err = f"⚠️ Đã treo <code>@{log_target}</code> rồi.";
    elif current_count >= vip_limit: err = f"⚠️ Đã đạt limit ({current_count}/{vip_limit}). Dùng /dungtreo để xóa."

    if err: await send_temporary_message(update, context, err); await delete_user_message(update, context); return

    await delete_user_message(update, context, msg_id) # Delete command first
    proc_msg = await context.bot.send_message(chat_id, f"⏳ Bắt đầu treo cho <code>@{log_target}</code>...", parse_mode=ParseMode.HTML)
    task = None

    try:
        app = context.application
        task = app.create_task(
            run_treo_loop(uid_str, target_user, context, chat_id), # Pass original username for API if needed
            name=f"treo_{uid_str}_{log_target}_{chat_id}"
        )
        # Add to structures using lowercase key
        active_treo_tasks[uid_str][target_key] = task
        persistent_treo_configs.setdefault(uid_str, {})[target_key] = chat_id
        save_data()
        new_count = len(persistent_treo_configs[uid_str])
        logger.info(f"TREO START: {uid_str} -> @{log_target} (Key:{target_key}). Count={new_count}/{vip_limit}")
        treo_m = TREO_INTERVAL_SECONDS//60
        success_txt = f"✅ <b>Bắt Đầu Treo OK!</b>\n@{log_target}\nSlot: {new_count}/{vip_limit} (~{treo_m}p/lần)"
        await context.bot.edit_message_text(success_txt, chat_id, proc_msg.message_id, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Treo Start Err {uid_str}->@{log_target}: {e}", exc_info=True)
        await context.bot.edit_message_text(f"❌ Lỗi khi bắt đầu treo <code>@{log_target}</code>.", chat_id, proc_msg.message_id, parse_mode=ParseMode.HTML)
        # Attempt rollback
        if task and not task.done(): task.cancel()
        active_treo_tasks.get(uid_str, {}).pop(target_key, None)
        if not active_treo_tasks.get(uid_str): active_treo_tasks.pop(uid_str, None)
        if persistent_treo_configs.get(uid_str, {}).pop(target_key, None):
            if not persistent_treo_configs.get(uid_str): persistent_treo_configs.pop(uid_str, None)
            save_data()


async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stops treo for one or all targets. Uses lowercase key internally."""
    if not update or not update.message or not update.effective_user: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id=user.id; uid_str=str(user_id); msg_id=update.message.message_id

    args = context.args; target_arg = args[0].strip() if args else None; err = None
    current_configs = persistent_treo_configs.get(uid_str, {})
    current_keys = list(current_configs.keys()) # These are already lowercase

    if not target_arg: err = "⚠️ Cú pháp: /dungtreo <username> | all" + (" (Bạn chưa treo user nào)" if not current_keys else "")
    elif target_arg.lower() == "all":
        await delete_user_message(update, context, msg_id)
        if not current_keys: return await context.bot.send_message(chat_id, "ℹ️ Bạn không có user nào đang treo.")
        stopped_count = await stop_all_treo_tasks_for_user(uid_str, context, f"User Cmd /dungtreo all")
        await context.bot.send_message(chat_id, f"✅ Đã dừng treo cho <b>{stopped_count}</b> tài khoản.")
        return # Finished 'all' case
    else:
        target_key = target_arg.lstrip("@").lower() # Key to stop (lowercase)
        if not target_key: err = "⚠️ Username không được trống."
        elif target_key not in current_keys: err = f"⚠️ Không tìm thấy <code>@{html.escape(target_arg)}</code> trong danh sách treo."

    if err: await send_temporary_message(update, context, err); await delete_user_message(update, context, msg_id); return

    # Stop single target using lowercase key
    log_display = html.escape(target_arg.lstrip('@')) # Original case for message
    logger.info(f"User {user_id} /dungtreo @{log_display} (Key:{target_key})")
    await delete_user_message(update, context, msg_id)
    stopped = await stop_treo_task(uid_str, target_key, context, f"User Cmd /dungtreo")

    if stopped:
        new_count = len(persistent_treo_configs.get(uid_str, {})); limit = get_vip_limit(user_id)
        await context.bot.send_message(chat_id, f"✅ Đã dừng treo cho <code>@{log_display}</code>.\n(Slot: {new_count}/{limit if is_user_vip(user_id) else 'N/A'})", parse_mode=ParseMode.HTML)
    else: # Should ideally not happen if target_key was in current_keys check
        await send_temporary_message(update, context, f"⚠️ Không dừng được <code>@{log_display}</code> (đã dừng?).", duration=20)


async def listtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lists currently configured treo targets (using lowercase keys)."""
    if not update or not update.message or not update.effective_user: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id=user.id; uid_str=str(user_id); msg_id=update.message.message_id
    await delete_user_message(update, context, msg_id) # Delete command first

    user_configs = persistent_treo_configs.get(uid_str, {})
    sorted_keys = sorted(list(user_configs.keys())) # Keys are lowercase
    is_vip = is_user_vip(user_id); limit = get_vip_limit(user_id) if is_vip else 0
    lines = [f"📊 <b>Danh Sách Đang Treo</b> {user.mention_html()}"]

    if not sorted_keys:
         lines.append("\n<i>Bạn chưa treo tài khoản nào.</i>" + ("\nDùng <code>/treo <user></code>" if is_vip else ""))
    else:
        lines.append(f"\n🔍 Số lượng: <b>{len(sorted_keys)} / {limit if is_vip else 'N/A'}</b>")
        for key in sorted_keys:
            # Check runtime status (estimation)
            task = active_treo_tasks.get(uid_str, {}).get(key)
            is_running = bool(task and not task.done())
            icon = "▶️" if is_running else "⏸️" # Use icons
            lines.append(f"  {icon} <code>@{html.escape(key)}</code>") # Display lowercase key
        lines.append("\n\nℹ️ Dùng: <code>/dungtreo <user|all></code> để dừng.")

    try: await context.bot.send_message(chat_id, "\n".join(lines), ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e: logger.error(f"/listtreo send err {user.id}: {e}")

async def xemfl24h_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shows follower gain in the last 24h."""
    if not update or not update.message or not update.effective_user: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id=user.id; uid_str=str(user_id); msg_id=update.message.message_id
    await delete_user_message(update, context, msg_id)

    # Permission check (Optional: Uncomment if only VIPs can use)
    # if not is_user_vip(user_id): return await send_temporary_message(update, context, "⚠️ Lệnh /xemfl24h chỉ dành cho VIP.")

    now = time.time(); threshold = now - USER_GAIN_HISTORY_SECONDS
    user_gains = user_daily_gains.get(uid_str, {})
    gains_24h = defaultdict(int); total_gain = 0

    for target_key, gain_list in user_gains.items(): # Keys are lowercase
        target_total = sum(g for ts, g in gain_list if ts >= threshold)
        if target_total > 0:
            gains_24h[target_key] += target_total; total_gain += target_total

    lines = [f"📈 <b>Follow Tăng (24 Giờ Qua)</b> {user.mention_html()}"]
    if not gains_24h: lines.append("\n<i>Không có dữ liệu tăng follow.</i>")
    else:
        lines.append(f"\n✨ Tổng cộng: <b style='color:blue;'>+{total_gain:,}</b> follow ✨")
        for key, gain in sorted(gains_24h.items(), key=lambda item: item[1], reverse=True):
             lines.append(f"  • <code>@{html.escape(key)}</code>: <b style='color:green;'>+{gain:,}</b>") # Display lowercase key

    try: await context.bot.send_message(chat_id, "\n".join(lines), ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e: logger.error(f"/xemfl24h send err {user.id}: {e}")


# --- Periodic Jobs ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    """Periodic job to report overall follower gains."""
    global last_stats_report_time, treo_stats
    now = time.time(); target_chat = ALLOWED_GROUP_ID
    # Skip if no target group or too soon
    if not target_chat:
        if treo_stats: treo_stats.clear(); logger.info("[Stats Job] Cleared stats (no target chat)."); save_data()
        last_stats_report_time = now # Still update time to prevent frequent checks
        return
    if last_stats_report_time and now < last_stats_report_time + TREO_STATS_INTERVAL_SECONDS * 0.95: return

    logger.info("[Stats Job] Starting...")
    # Snapshot and reset current stats safely
    snapshot = {}; stats_cleared = False
    try:
        snapshot = {uid: dict(targets) for uid, targets in treo_stats.items()} # Simple snapshot
        treo_stats.clear(); last_stats_report_time = now; save_data()
        stats_cleared = True
        logger.info(f"[Stats Job] Process snapshot ({len(snapshot)} users), stats cleared.")
    except Exception as e: logger.error(f"[Stats Job] Snapshot/Clear Err: {e}", exc_info=True); return # Abort if snapshot/clear fails

    if not snapshot: logger.info("[Stats Job] No stats data to report."); return

    # Process snapshot
    top_gainers = [] # (gain, uid_str, target_key)
    total = 0
    for uid, targets in snapshot.items():
        for target_key, gain in targets.items(): # Keys should be lowercase from save/load
             try: g = int(gain); total += g; top_gainers.append((g, str(uid), str(target_key))) if g > 0 else None
             except: pass # Ignore invalid gain

    if not top_gainers: logger.info("[Stats Job] No positive gains found."); return

    top_gainers.sort(key=lambda x: x[0], reverse=True)
    stats_h = TREO_STATS_INTERVAL_SECONDS // 3600
    report = [f"📊 <b>Thống Kê Follow ({stats_h}h Qua)</b>", f"Tổng: <b style='color:blue;'>+{total:,}</b>", "\n🏆 <b>Top Treo:</b>"]
    mentions = {}
    app = context.application
    for i, (gain, uid, key) in enumerate(top_gainers[:10]):
         mention = mentions.get(uid)
         if not mention:
              try: u = await app.bot.get_chat(int(uid)); mention=u.mention_html() or f"<code>...{uid[-4:]}</code>"
              except: mention = f"<code>...{uid[-4:]}</code>"
              mentions[uid] = mention
         medal = "🥇🥈🥉"[i] if i < 3 else "🏅"
         report.append(f"  {medal} +{gain:,} <code>@{html.escape(key)}</code> ({mention})")

    try: await app.bot.send_message(target_chat, "\n".join(report), ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)
    except Exception as e: logger.error(f"[Stats Job] Send Err to {target_chat}: {e}", exc_info=True)
    logger.info("[Stats Job] Finished.")


async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Cleanup expired keys, activations, VIPs, old gain data, and stop tasks for expired VIPs."""
    # (Implementation seems fine, already reviewed above)
    # Re-use the existing logic for brevity in this block
    global valid_keys, activated_users, vip_users, user_daily_gains
    now = time.time(); changed, gains_clean = False, False; ks, u_key, u_vip = [],[],[]; vips_stop=set()
    logger.info("[Cleanup] Starting...")
    # Find expired items
    for k, d in list(valid_keys.items()):
        if d.get("used_by") is None and now > float(d.get("expiry_time", 0)): ks.append(k)
    for u, e in list(activated_users.items()):
        if now > float(e): u_key.append(u)
    for u, d in list(vip_users.items()):
        if now > float(d.get("expiry", 0)): u_vip.append(u); vips_stop.add(u)
    # Clean gains
    exp_ts = now-USER_GAIN_HISTORY_SECONDS; users_empty=set()
    for u,tgts in user_daily_gains.items():
        tgts_empty=set()
        for tk,gl in tgts.items():
            orig_len=len(gl); new_gl=[(ts,g) for ts,g in gl if ts>=exp_ts]
            if len(new_gl)<orig_len: gains_clean=True; user_daily_gains[u][tk]=new_gl; (tgts_empty.add(tk) if not new_gl else None)
            elif not gl: tgts_empty.add(tk)
        if tgts_empty: gains_clean=True; [tgts.pop(t,None) for t in tgts_empty]; (users_empty.add(u) if not tgts else None)
    if users_empty: gains_clean=True; [user_daily_gains.pop(u,None) for u in users_empty]
    # Delete items
    if any(lst for lst in [ks,u_key,u_vip]): changed=True; logger.info(f"Cleanup: Keys={len(ks)}, KeyAct={len(u_key)}, VIP={len(u_vip)}")
    [valid_keys.pop(k,None) for k in ks]; [activated_users.pop(u,None) for u in u_key]; [vip_users.pop(u,None) for u in u_vip]
    # Stop tasks
    if vips_stop:
        logger.info(f"Cleanup: Stopping tasks for {len(vips_stop)} expired VIPs.")
        await asyncio.gather(*[stop_all_treo_tasks_for_user(u_s,context,"VIP Expired (Cleanup)") for u_s in vips_stop], return_exceptions=True)
    # Save if changed
    if changed or gains_clean: logger.info("Cleanup: Saving data changes."); save_data()
    logger.info("[Cleanup] Finished.")


# --- Restore & Shutdown ---
async def restore_treo_tasks(context: ContextTypes.DEFAULT_TYPE):
    """Restores persistent treo tasks on startup."""
    global active_treo_tasks # Modifies this global
    logger.info("[Restore] Starting treo task restoration...")
    restored_count = 0; needs_save = False
    configs_to_check = dict(persistent_treo_configs) # Iterate over a copy
    tasks_to_start = [] # (uid_str, target_key, target_display, chat_id) - Collect valid tasks first
    users_to_prune = defaultdict(list) # {uid: [key_to_remove]}
    current_counts = defaultdict(int)

    # 1. Validate configs and identify tasks to start/prune
    for uid, user_configs in configs_to_check.items():
        try:
            uid_int = int(uid)
            if not is_user_vip(uid_int):
                 logger.warning(f"[Restore] User {uid} not VIP, pruning all {len(user_configs)} configs.")
                 users_to_prune[uid].extend(user_configs.keys())
                 continue # Skip this user's tasks
            limit = get_vip_limit(uid_int)
            for target_key, chat_id in user_configs.items(): # Key is lowercase
                 target_display = target_key # Simple display
                 if current_counts[uid] >= limit:
                     logger.warning(f"[Restore] User {uid} limit ({limit}) reached, pruning @{target_display}.")
                     users_to_prune[uid].append(target_key)
                     continue
                 # Check if already running (very quick restart)
                 if uid in active_treo_tasks and target_key in active_treo_tasks[uid] and not active_treo_tasks[uid][target_key].done():
                      logger.info(f"[Restore] Task {uid}->@{target_display} already active.")
                      current_counts[uid] += 1
                      continue
                 # Mark for start
                 tasks_to_start.append((uid, target_key, target_display, chat_id))
                 current_counts[uid] += 1
        except ValueError: logger.error(f"[Restore] Invalid user ID '{uid}', pruning."); users_to_prune[uid].extend(user_configs.keys())
        except Exception as e: logger.error(f"[Restore] Error checking user {uid}: {e}",exc_info=True); users_to_prune[uid].extend(user_configs.keys())

    # 2. Prune invalid/overlimit configs from persistent_treo_configs
    if users_to_prune:
        needs_save = True
        logger.info(f"[Restore] Pruning {sum(len(v) for v in users_to_prune.values())} configs for {len(users_to_prune)} users.")
        for uid, keys_to_del in users_to_prune.items():
            if uid in persistent_treo_configs:
                for key in keys_to_del: persistent_treo_configs[uid].pop(key, None)
                if not persistent_treo_configs[uid]: persistent_treo_configs.pop(uid, None) # Remove empty user entry

    # 3. Start valid tasks
    app = context.application
    if tasks_to_start:
        logger.info(f"[Restore] Creating {len(tasks_to_start)} tasks...")
        for uid, target_k, target_d, cid in tasks_to_start:
            try:
                task = app.create_task(
                    run_treo_loop(uid, target_d, context, cid), # Pass original case target name to loop if API needs it, otherwise target_k
                    name=f"treo_{uid}_{target_k}_{cid}_rst"
                )
                active_treo_tasks[uid][target_k] = task # Use lowercase key
                restored_count += 1
                await asyncio.sleep(0.05) # Stagger startup
            except Exception as e:
                 logger.error(f"[Restore] Failed create task {uid}->@{target_k}: {e}")
                 # Remove potentially broken persistent config entry if creation fails
                 if persistent_treo_configs.get(uid,{}).pop(target_k,None):
                     if not persistent_treo_configs.get(uid): persistent_treo_configs.pop(uid,None)
                     needs_save = True
        logger.info(f"[Restore] Started {restored_count}/{len(tasks_to_start)} planned.")

    # 4. Final save if pruning or creation errors occurred
    if needs_save: logger.info("[Restore] Saving pruned config data."); save_data()
    logger.info("[Restore] Finished.")

async def shutdown_tasks(context: ContextTypes.DEFAULT_TYPE):
    """Gracefully cancels running treo tasks on shutdown."""
    logger.info("[Shutdown] Cancelling running treo tasks...")
    tasks = []
    for user_tasks in active_treo_tasks.values():
        for task in user_tasks.values():
            if isinstance(task, asyncio.Task) and not task.done():
                tasks.append(task)
    if not tasks: return logger.info("[Shutdown] No active tasks to cancel.")
    logger.info(f"[Shutdown] Cancelling {len(tasks)} tasks...")
    [t.cancel() for t in tasks]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
    errors = sum(1 for r in results if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError))
    logger.info(f"[Shutdown] Tasks cancelled: {cancelled}, Errors: {errors}")


# --- Main Execution ---
async def main_async() -> None:
    """Initializes and runs the bot application."""
    start_time = time.time()
    print(f"--- Bot DinoTool Starting ({datetime.now():%Y-%m-%d %H:%M:%S}) ---")
    load_data()
    print(f"Load complete. Keys={len(valid_keys)}, Act={len(activated_users)}, VIP={len(vip_users)}, TreoCfgs={sum(len(v) for v in persistent_treo_configs.values())}")

    # --- Application Setup ---
    app = (Application.builder().token(BOT_TOKEN)
           .job_queue(JobQueue()).connect_timeout(60).read_timeout(90).write_timeout(90)
           .pool_timeout(120).http_version("1.1").build())

    # --- Register Handlers ---
    handlers = [
        CommandHandler(("start", "menu"), start_command), CommandHandler("lenh", lenh_command),
        CommandHandler("getkey", getkey_command), CommandHandler("nhapkey", nhapkey_command),
        CommandHandler("tim", tim_command), CommandHandler("fl", fl_command),
        CommandHandler("muatt", muatt_command), CommandHandler("treo", treo_command),
        CommandHandler("dungtreo", dungtreo_command), CommandHandler("listtreo", listtreo_command),
        CommandHandler("xemfl24h", xemfl24h_command), CommandHandler("addtt", addtt_command),
        CommandHandler("mess", mess_command),
        CallbackQueryHandler(menu_callback_handler, pattern="^show_(muatt|lenh)$"),
        CallbackQueryHandler(prompt_send_bill_callback, pattern="^prompt_send_bill_\d+$"),
        MessageHandler((filters.PHOTO | filters.Document.IMAGE) & (~filters.COMMAND) & filters.UpdateType.MESSAGE, handle_photo_bill, block=True), # Block=True if high priority needed
    ]
    app.add_handlers(handlers, group=0) # Add command handlers in group 0
    # Note: Bill handler was previously group -1, adjusted to block=True. Verify desired behavior.
    logger.info(f"Registered {len(handlers)} handlers.")
    # --- End Handlers ---

    await app.initialize()
    logger.info("Application initialized.")

    # --- Schedule Jobs ---
    jq = app.job_queue
    if jq:
        jq.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_job")
        logger.info(f"Job 'cleanup' scheduled: Interval={CLEANUP_INTERVAL_SECONDS}s, First=60s")
        if ALLOWED_GROUP_ID:
            jq.run_repeating(report_treo_stats, interval=TREO_STATS_INTERVAL_SECONDS, first=120, name="stats_report_job") # Delay stats slightly more
            logger.info(f"Job 'stats_report' scheduled: Interval={TREO_STATS_INTERVAL_SECONDS}s, First=120s")
        else: logger.info("Stats report job disabled.")
    else: logger.error("JobQueue not available!")

    # --- Restore Tasks ---
    # Pass application context needed by restore logic (to create tasks)
    await restore_treo_tasks(ContextTypes.DEFAULT_TYPE(application=app))

    init_duration = time.time() - start_time
    logger.info(f"Bot ready! (Init: {init_duration:.2f}s). Starting polling...")
    print(f"--- Bot Ready (Init: {init_duration:.2f}s) ---")

    # --- Run Bot ---
    await app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

    # --- Shutdown (Reached when run_polling stops) ---
    logger.info("Polling stopped. Shutting down...")
    await shutdown_tasks(ContextTypes.DEFAULT_TYPE(application=app))
    logger.info("Final data save...")
    save_data()
    await app.shutdown()
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    try: asyncio.run(main_async())
    except KeyboardInterrupt: logger.info("KeyboardInterrupt received. Exiting.")
    except Exception as e: logger.critical(f"FATAL ERROR in main: {e}", exc_info=True)
    finally: print("--- Bot Stopped ---"); logger.info("Bot stopped.")
