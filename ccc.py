
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
from telegram import Update, Message, InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
    CallbackQueryHandler,
    ApplicationHandlerStop
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError

# --- Cấu hình ---
BOT_TOKEN = "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" # <--- TOKEN CỦA BẠN
API_KEY = "khangdino99" # <--- API KEY TIM (VẪN CẦN CHO LỆNH /tim)
ADMIN_USER_ID = 7193749511 # <<< --- ID TELEGRAM CỦA ADMIN (Người quản lý bot)

# --- YÊU CẦU 2: ID của bot @khangtaixiu_bot để nhận bill ---
# !!! QUAN TRỌNG: Bạn cần tìm ID SỐ của bot @khangtaixiu_bot và thay thế giá trị dưới đây !!!
# Cách tìm: Chat với @userinfobot, gửi username @khangtaixiu_bot vào đó.
BILL_FORWARD_TARGET_ID = 7193749511 # <<< --- THAY THẾ BẰNG ID SỐ CỦA @khangtaixiu_bot
# ----------------------------------------------------------------

# ID Nhóm chính để nhận bill và thống kê. Các lệnh khác hoạt động mọi nơi.
# Nếu không muốn giới hạn, đặt thành None, nhưng bill và thống kê sẽ không hoạt động hoặc cần sửa logic.
ALLOWED_GROUP_ID = -1002191171631 # <--- ID NHÓM CHÍNH CỦA BẠN HOẶC None

LINK_SHORTENER_API_KEY = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" # Token Yeumoney
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # Link đích chứa key
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # API Yeumoney

# --- Thời gian ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 phút
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 phút
KEY_EXPIRY_SECONDS = 6 * 3600   # 6 giờ (Key chưa nhập)
ACTIVATION_DURATION_SECONDS = 6 * 3600 # 6 giờ (Sau khi nhập key)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 giờ
TREO_INTERVAL_SECONDS = 15 * 60 # 15 phút (Khoảng cách giữa các lần gọi API /treo)
TREO_FAILURE_MSG_DELETE_DELAY = 5 # 5 giây (Thời gian xoá tin nhắn treo thất bại)
TREO_STATS_INTERVAL_SECONDS = 24 * 3600 # 24 giờ (Khoảng cách thống kê follow tăng)

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM
FOLLOW_API_URL_BASE = "https://api.thanhtien.site/lynk/dino/telefl.php" # API FOLLOW MỚI

# --- Thông tin VIP ---
VIP_PRICES = {
    15: {"price": "15.000 VND", "limit": 2, "duration_days": 15},
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
}
QR_CODE_URL = "https://i.imgur.com/49iY7Ft.jpeg" # Link ảnh QR Code
BANK_ACCOUNT = "KHANGDINO" # <--- THAY STK CỦA BẠN
BANK_NAME = "VCB BANK" # <--- THAY TÊN NGÂN HÀNG
ACCOUNT_NAME = "LE QUOC KHANG" # <--- THAY TÊN CHỦ TK
PAYMENT_NOTE_PREFIX = "VIP DinoTool ID" # Nội dung chuyển khoản sẽ là: "VIP DinoTool ID <user_id>"

# --- Lưu trữ ---
DATA_FILE = "bot_persistent_data.json"

# --- Biến toàn cục ---
user_tim_cooldown = {}
user_fl_cooldown = {} # {user_id_str: {target_username: timestamp}}
user_getkey_cooldown = {}
valid_keys = {} # {key: {"user_id_generator": ..., "expiry_time": ..., "used_by": ..., "activation_time": ...}}
activated_users = {} # {user_id_str: expiry_timestamp} - Người dùng kích hoạt bằng key
vip_users = {} # {user_id_str: {"expiry": expiry_timestamp, "limit": user_limit}} - Người dùng VIP
active_treo_tasks = {} # {user_id_str: {target_username: asyncio.Task}} - Lưu các task /treo đang chạy (runtime)
persistent_treo_configs = {} # {user_id_str: {target_username: chat_id}} - Lưu để khôi phục sau restart

treo_stats = defaultdict(lambda: defaultdict(int)) # {user_id_str: {target_username: gain_since_last_report}}
last_stats_report_time = 0 # Thời điểm báo cáo thống kê gần nhất

# Lưu trữ tạm thời ID người dùng đã nhấn nút gửi bill để check ảnh tiếp theo
pending_bill_user_ids = set() # Set of user_ids (int)

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO,
    handlers=[logging.FileHandler("bot.log", encoding='utf-8'), logging.StreamHandler()] # Log ra file và console
)
# Giảm log nhiễu từ thư viện http và telegram.ext scheduling
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.JobQueue").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.Application").setLevel(logging.INFO) # Giữ INFO cho Application để xem khởi động
logger = logging.getLogger(__name__)

# --- Kiểm tra cấu hình ---
if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN": logger.critical("!!! BOT_TOKEN is missing !!!"); exit(1)
if not BILL_FORWARD_TARGET_ID or not isinstance(BILL_FORWARD_TARGET_ID, int) or BILL_FORWARD_TARGET_ID == 123456789: # Thêm kiểm tra placeholder
    logger.critical("!!! BILL_FORWARD_TARGET_ID is missing, invalid, or still the placeholder! Find the NUMERIC ID of @khangtaixiu_bot using @userinfobot !!!"); exit(1)
else: logger.info(f"Bill forwarding target set to: {BILL_FORWARD_TARGET_ID}")

if ALLOWED_GROUP_ID:
     logger.info(f"Bill forwarding source and Stats reporting restricted to Group ID: {ALLOWED_GROUP_ID}")
else:
     logger.warning("!!! ALLOWED_GROUP_ID is not set. Bill forwarding and Stats reporting will be disabled. !!!")

if not LINK_SHORTENER_API_KEY: logger.critical("!!! LINK_SHORTENER_API_KEY is missing !!!"); exit(1)
if not API_KEY: logger.warning("!!! API_KEY (for /tim) is missing. /tim command might fail. !!!")
if not ADMIN_USER_ID: logger.critical("!!! ADMIN_USER_ID is missing !!!"); exit(1)

# --- Hàm lưu/tải dữ liệu ---
def save_data():
    global persistent_treo_configs # Đảm bảo truy cập biến global
    # Chuyển key là số thành string để đảm bảo tương thích JSON
    string_key_activated_users = {str(k): v for k, v in activated_users.items()}
    string_key_tim_cooldown = {str(k): v for k, v in user_tim_cooldown.items()}
    string_key_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in user_fl_cooldown.items()}
    string_key_getkey_cooldown = {str(k): v for k, v in user_getkey_cooldown.items()}
    string_key_vip_users = {str(k): v for k, v in vip_users.items()}
    string_key_treo_stats = {str(uid): dict(targets) for uid, targets in treo_stats.items()}

    # Lưu persistent_treo_configs - Chuyển cả key và value sang kiểu phù hợp
    string_key_persistent_treo = {
        str(uid): {str(target): int(chatid) for target, chatid in configs.items()}
        for uid, configs in persistent_treo_configs.items() if configs # Chỉ lưu user có config
    }

    data_to_save = {
        "valid_keys": valid_keys,
        "activated_users": string_key_activated_users,
        "vip_users": string_key_vip_users,
        "user_cooldowns": {
            "tim": string_key_tim_cooldown,
            "fl": string_key_fl_cooldown,
            "getkey": string_key_getkey_cooldown
        },
        "treo_stats": string_key_treo_stats,
        "last_stats_report_time": last_stats_report_time,
        "persistent_treo_configs": string_key_persistent_treo # <-- Đã thêm key mới
    }
    try:
        temp_file = DATA_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        os.replace(temp_file, DATA_FILE) # Atomic replace
        logger.debug(f"Data saved successfully to {DATA_FILE}")
    except Exception as e:
        logger.error(f"Failed to save data to {DATA_FILE}: {e}", exc_info=True)
        # Cố gắng dọn dẹp file tạm nếu có lỗi
        if os.path.exists(temp_file):
            try: os.remove(temp_file)
            except Exception as e_rem: logger.error(f"Failed to remove temporary save file {temp_file}: {e_rem}")

def load_data():
    global valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown, treo_stats, last_stats_report_time, persistent_treo_configs # <-- Thêm persistent_treo_configs
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                valid_keys = data.get("valid_keys", {})
                # Load user data with string keys directly
                activated_users = data.get("activated_users", {})
                vip_users = data.get("vip_users", {})

                all_cooldowns = data.get("user_cooldowns", {})
                user_tim_cooldown = all_cooldowns.get("tim", {})
                user_fl_cooldown = all_cooldowns.get("fl", defaultdict(dict)) # Ensure inner dict exists
                user_getkey_cooldown = all_cooldowns.get("getkey", {})

                loaded_stats = data.get("treo_stats", {})
                treo_stats = defaultdict(lambda: defaultdict(int))
                for uid_str, targets in loaded_stats.items():
                    if isinstance(targets, dict): # Check type
                        for target, gain in targets.items():
                             try:
                                 treo_stats[str(uid_str)][str(target)] = int(gain) # Convert keys and value
                             except (ValueError, TypeError):
                                 logger.warning(f"Skipping invalid treo stat entry: user {uid_str}, target {target}, gain {gain}")

                last_stats_report_time = data.get("last_stats_report_time", 0)

                # Load persistent_treo_configs <-- Đoạn mới
                loaded_persistent_treo = data.get("persistent_treo_configs", {})
                persistent_treo_configs = {}
                for uid_str, configs in loaded_persistent_treo.items():
                    user_id_key = str(uid_str) # Ensure outer key is string
                    persistent_treo_configs[user_id_key] = {}
                    if isinstance(configs, dict): # Check inner type
                        for target, chatid in configs.items():
                             try:
                                 persistent_treo_configs[user_id_key][str(target)] = int(chatid) # Convert inner key and value
                             except (ValueError, TypeError):
                                 logger.warning(f"Skipping invalid persistent treo config entry: user {user_id_key}, target {target}, chatid {chatid}")

                logger.info(f"Data loaded successfully from {DATA_FILE}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            valid_keys, activated_users, vip_users = {}, {}, {}
            user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}
            treo_stats = defaultdict(lambda: defaultdict(int))
            last_stats_report_time = 0
            persistent_treo_configs = {} # <-- Khởi tạo rỗng
    except (json.JSONDecodeError, TypeError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Using empty data structures.", exc_info=True)
        # Reset all global data structures on error
        valid_keys, activated_users, vip_users = {}, {}, {}
        user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}
        treo_stats = defaultdict(lambda: defaultdict(int))
        last_stats_report_time = 0
        persistent_treo_configs = {} # <-- Reset

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn người dùng một cách an toàn."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return

    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except Forbidden:
         logger.debug(f"Cannot delete message {msg_id_to_delete} in chat {original_chat_id}. Bot might not be admin or message too old.")
    except BadRequest as e:
        # Các lỗi thông thường khi tin nhắn không tồn tại hoặc không thể xóa
        if "Message to delete not found" in str(e).lower() or \
           "message can't be deleted" in str(e).lower() or \
           "MESSAGE_ID_INVALID" in str(e).upper() or \
           "message identifier is not specified" in str(e).lower():
            logger.debug(f"Could not delete message {msg_id_to_delete} (already deleted?): {e}")
        else:
            # Log các lỗi BadRequest khác
            logger.warning(f"BadRequest error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}", exc_info=True)

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Job được lên lịch để xóa tin nhắn."""
    job_data = context.job.data
    chat_id = job_data.get('chat_id')
    message_id = job_data.get('message_id')
    job_name = context.job.name
    if chat_id and message_id:
        logger.debug(f"Job '{job_name}' running to delete message {message_id} in chat {chat_id}")
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(f"Job '{job_name}' successfully deleted message {message_id}")
        except Forbidden:
             logger.info(f"Job '{job_name}' cannot delete message {message_id}. Bot might not be admin or message too old.")
        except BadRequest as e:
            if "Message to delete not found" in str(e).lower() or "message can't be deleted" in str(e).lower():
                logger.info(f"Job '{job_name}' could not delete message {message_id} (already deleted?): {e}")
            else:
                 logger.warning(f"Job '{job_name}' BadRequest deleting message {message_id}: {e}")
        except TelegramError as e:
             logger.warning(f"Job '{job_name}' Telegram error deleting message {message_id}: {e}")
        except Exception as e:
            logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else:
        logger.warning(f"Job '{job_name}' called missing chat_id or message_id.")

async def send_temporary_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, duration: int = 15, parse_mode: str = ParseMode.HTML, reply: bool = True):
    """Gửi tin nhắn và tự động xóa sau một khoảng thời gian."""
    if not update or not update.effective_chat: return

    chat_id = update.effective_chat.id
    sent_message = None
    try:
        # Chỉ reply nếu update.message tồn tại và reply=True
        reply_to_msg_id = update.message.message_id if reply and update.message else None

        if reply_to_msg_id:
            try:
                sent_message = await context.bot.send_message(
                    chat_id=chat_id, text=text, parse_mode=parse_mode,
                    disable_web_page_preview=True, reply_to_message_id=reply_to_msg_id
                )
            except BadRequest as e:
                if "reply message not found" in str(e).lower():
                     logger.debug(f"Reply message {reply_to_msg_id} not found for temporary message. Sending without reply.")
                     sent_message = await context.bot.send_message(
                         chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True
                     )
                else: raise # Ném lại lỗi BadRequest khác
        else:
            sent_message = await context.bot.send_message(
                chat_id=chat_id, text=text, parse_mode=parse_mode, disable_web_page_preview=True
            )

        if sent_message and context.job_queue:
            job_name = f"del_temp_{chat_id}_{sent_message.message_id}"
            context.job_queue.run_once(
                delete_message_job,
                duration,
                data={'chat_id': chat_id, 'message_id': sent_message.message_id},
                name=job_name
            )
            logger.debug(f"Scheduled job '{job_name}' to delete message {sent_message.message_id} in {duration}s")
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Error sending temporary message to {chat_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in send_temporary_message to {chat_id}: {e}", exc_info=True)

def generate_random_key(length=8):
    """Tạo key ngẫu nhiên dạng Dinotool-xxxx."""
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"Dinotool-{random_part}"

# --- Cập nhật hàm stop_treo_task và thêm stop_all_treo_tasks_for_user ---
async def stop_treo_task(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown") -> bool:
    """Dừng một task treo cụ thể VÀ xóa khỏi persistent config. Trả về True nếu dừng/xóa thành công, False nếu không tìm thấy."""
    global persistent_treo_configs, active_treo_tasks # Cần truy cập để sửa đổi
    task = None
    was_active_runtime = False
    removed_persistent = False
    data_saved = False

    # 1. Dừng task đang chạy (nếu có)
    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
        task = active_treo_tasks[user_id_str][target_username]
        if task and not task.done():
            was_active_runtime = True
            task.cancel()
            logger.info(f"[Treo Task Stop] Attempting to cancel RUNTIME task for user {user_id_str} -> @{target_username}. Reason: {reason}")
            try:
                # Chờ task bị hủy trong thời gian ngắn
                await asyncio.wait_for(task, timeout=1.0)
            except asyncio.CancelledError:
                logger.info(f"[Treo Task Stop] Runtime Task {user_id_str} -> @{target_username} confirmed cancelled.")
            except asyncio.TimeoutError:
                 logger.warning(f"[Treo Task Stop] Timeout waiting for cancelled runtime task {user_id_str}->{target_username}.")
            except Exception as e:
                 # Log lỗi nhưng vẫn tiếp tục quá trình xóa khỏi dict
                 logger.error(f"[Treo Task Stop] Error awaiting cancelled runtime task for {user_id_str}->{target_username}: {e}")
        # Luôn xóa khỏi runtime dict nếu key tồn tại
        del active_treo_tasks[user_id_str][target_username]
        if not active_treo_tasks[user_id_str]: # Nếu user không còn task nào thì xóa user khỏi dict
            del active_treo_tasks[user_id_str]
        logger.info(f"[Treo Task Stop] Removed task entry for {user_id_str} -> @{target_username} from active (runtime) tasks.")
    else:
        logger.debug(f"[Treo Task Stop] No active runtime task found for {user_id_str} -> @{target_username}. Checking persistent config.")

    # 2. Xóa khỏi persistent config (nếu có)
    if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
        del persistent_treo_configs[user_id_str][target_username]
        if not persistent_treo_configs[user_id_str]: # Nếu user không còn config nào thì xóa user khỏi dict
            del persistent_treo_configs[user_id_str]
        logger.info(f"[Treo Task Stop] Removed entry for {user_id_str} -> @{target_username} from persistent_treo_configs.")
        save_data() # Lưu ngay sau khi thay đổi cấu hình persistent
        data_saved = True
        removed_persistent = True
    else:
         logger.debug(f"[Treo Task Stop] Entry for {user_id_str} -> @{target_username} not found in persistent_treo_configs.")

    # Trả về True nếu task runtime bị hủy HOẶC config persistent bị xóa
    return was_active_runtime or removed_persistent

async def stop_all_treo_tasks_for_user(user_id_str: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown"):
    """Dừng tất cả các task treo của một user và xóa khỏi persistent config."""
    stopped_count = 0
    # Lấy danh sách target từ cả runtime và persistent để đảm bảo không bỏ sót
    targets_in_persistent = list(persistent_treo_configs.get(user_id_str, {}).keys())
    targets_in_runtime = list(active_treo_tasks.get(user_id_str, {}).keys())
    all_targets_to_check = set(targets_in_persistent + targets_in_runtime)

    if not all_targets_to_check:
        logger.info(f"No active or persistent treo tasks found for user {user_id_str} to stop.")
        return

    logger.info(f"Stopping all {len(all_targets_to_check)} potential treo tasks for user {user_id_str}. Reason: {reason}")
    # Lặp qua bản sao của set để tránh lỗi thay đổi kích thước khi lặp
    for target_username in list(all_targets_to_check):
        if await stop_treo_task(user_id_str, target_username, context, reason):
            stopped_count += 1

    logger.info(f"Finished stopping tasks for user {user_id_str}. Stopped/Removed: {stopped_count}/{len(all_targets_to_check)} target(s).")

async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn (keys, activations, VIPs)."""
    global valid_keys, activated_users, vip_users, persistent_treo_configs # <-- persistent_treo_configs cần được check
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate_key = []
    users_to_deactivate_vip = []
    vip_users_to_stop_tasks = [] # User ID (string) của VIP hết hạn cần dừng task
    basic_data_changed = False # Flag để biết có cần save_data() không

    logger.info("[Cleanup] Starting cleanup job...")

    # Check expired keys (chưa sử dụng)
    for key, data in list(valid_keys.items()):
        try:
            # Chỉ xóa key chưa dùng và đã hết hạn
            if data.get("used_by") is None and current_time > float(data.get("expiry_time", 0)):
                keys_to_remove.append(key)
        except (ValueError, TypeError):
             keys_to_remove.append(key) # Xóa key có dữ liệu không hợp lệ

    # Check expired key activations
    for user_id_str, expiry_timestamp in list(activated_users.items()):
        try:
            if current_time > float(expiry_timestamp):
                users_to_deactivate_key.append(user_id_str)
        except (ValueError, TypeError):
             users_to_deactivate_key.append(user_id_str) # Xóa user có dữ liệu không hợp lệ

    # Check expired VIP activations
    for user_id_str, vip_data in list(vip_users.items()):
        try:
            if current_time > float(vip_data.get("expiry", 0)):
                users_to_deactivate_vip.append(user_id_str)
                vip_users_to_stop_tasks.append(user_id_str) # <-- Thêm vào danh sách cần dừng task
        except (ValueError, TypeError):
            users_to_deactivate_vip.append(user_id_str)
            vip_users_to_stop_tasks.append(user_id_str) # <-- Dừng task nếu dữ liệu VIP không hợp lệ

    # Perform deletions from basic data structures
    if keys_to_remove:
        logger.info(f"[Cleanup] Removing {len(keys_to_remove)} expired unused keys.")
        for key in keys_to_remove:
            if key in valid_keys: del valid_keys[key]; basic_data_changed = True
    if users_to_deactivate_key:
         logger.info(f"[Cleanup] Deactivating {len(users_to_deactivate_key)} users (key system).")
         for user_id_str in users_to_deactivate_key:
             if user_id_str in activated_users: del activated_users[user_id_str]; basic_data_changed = True
    if users_to_deactivate_vip:
         logger.info(f"[Cleanup] Deactivating {len(users_to_deactivate_vip)} VIP users from list.")
         for user_id_str in users_to_deactivate_vip:
             if user_id_str in vip_users: del vip_users[user_id_str]; basic_data_changed = True

    # Stop tasks for expired/invalid VIPs <-- Logic mới
    if vip_users_to_stop_tasks:
         logger.info(f"[Cleanup] Scheduling stop for tasks of {len(vip_users_to_stop_tasks)} expired/invalid VIP users.")
         app = context.application
         for user_id_str in vip_users_to_stop_tasks:
             # Chạy bất đồng bộ để không chặn job cleanup chính
             app.create_task(
                 stop_all_treo_tasks_for_user(user_id_str, context, reason="VIP Expired/Removed during Cleanup"),
                 name=f"cleanup_stop_tasks_{user_id_str}"
             )
             # Lưu ý: stop_all_treo_tasks_for_user sẽ tự gọi save_data() khi xóa persistent config

    # Chỉ lưu nếu dữ liệu cơ bản thay đổi. Việc dừng task đã tự lưu.
    if basic_data_changed:
        logger.info("[Cleanup] Basic data (keys/activation/vip list) changed, saving...")
        save_data()
    else:
        logger.info("[Cleanup] No basic data changes found. Treo task stopping handles its own saving.")

    logger.info("[Cleanup] Cleanup job finished.")

# --- Kiểm tra VIP/Key (Giữ nguyên) ---
def is_user_vip(user_id: int) -> bool:
    """Kiểm tra trạng thái VIP."""
    user_id_str = str(user_id)
    vip_data = vip_users.get(user_id_str)
    if vip_data:
        try: return time.time() < float(vip_data.get("expiry", 0))
        except (ValueError, TypeError): return False
    return False

def get_vip_limit(user_id: int) -> int:
    """Lấy giới hạn treo user của VIP."""
    user_id_str = str(user_id)
    if is_user_vip(user_id):
        # Trả về limit đã lưu hoặc 0 nếu không có
        return vip_users.get(user_id_str, {}).get("limit", 0)
    return 0 # Không phải VIP thì không có limit

def is_user_activated_by_key(user_id: int) -> bool:
    """Kiểm tra trạng thái kích hoạt bằng key."""
    user_id_str = str(user_id)
    expiry_time_str = activated_users.get(user_id_str)
    if expiry_time_str:
        try: return time.time() < float(expiry_time_str)
        except (ValueError, TypeError): return False
    return False

def can_use_feature(user_id: int) -> bool:
    """Kiểm tra xem user có thể dùng tính năng (/tim, /fl) không."""
    return is_user_vip(user_id) or is_user_activated_by_key(user_id)

# --- Logic API Follow (Giữ nguyên) ---
async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """Gọi API follow và trả về kết quả."""
    api_params = {"user": target_username, "userid": user_id_str, "tokenbot": bot_token}
    log_api_params = api_params.copy()
    log_api_params["tokenbot"] = f"...{bot_token[-6:]}" if len(bot_token) > 6 else "***"
    logger.info(f"[API Call] User {user_id_str} calling Follow API for @{target_username} with params: {log_api_params}")
    result = {"success": False, "message": "Lỗi không xác định khi gọi API.", "data": None}
    try:
        # Tăng timeout một chút nếu API chậm
        async with httpx.AsyncClient(verify=False, timeout=90.0) as client:
            resp = await client.get(FOLLOW_API_URL_BASE, params=api_params, headers={'User-Agent': 'TG Bot FL Caller'})
            content_type = resp.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try:
                # Thử các encoding phổ biến
                encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1']
                decoded = False
                resp_bytes = await resp.aread()
                for enc in encodings_to_try:
                    try:
                        response_text_for_debug = resp_bytes.decode(enc, errors='strict')[:1000] # Giới hạn độ dài log
                        logger.debug(f"[API Call @{target_username}] Decoded response with {enc}")
                        decoded = True
                        break
                    except UnicodeDecodeError:
                        logger.debug(f"[API Call @{target_username}] Failed to decode with {enc}")
                        continue
                if not decoded:
                    response_text_for_debug = resp_bytes.decode('utf-8', errors='replace')[:1000] # Fallback
                    logger.warning(f"[API Call @{target_username}] Could not decode response with common encodings, using replace.")
            except Exception as e_read_outer:
                 logger.error(f"[API Call @{target_username}] Error reading/decoding response body: {e_read_outer}")

            logger.debug(f"[API Call @{target_username}] Status: {resp.status_code}, Content-Type: {content_type}")
            logger.debug(f"[API Call @{target_username}] Response text snippet: {response_text_for_debug}...") # Log snippet để debug

            if resp.status_code == 200:
                if "application/json" in content_type:
                    try:
                        # Thử giải mã phần còn lại nếu cần
                        full_response_text = response_text_for_debug
                        if len(resp_bytes) > 1000:
                            try:
                                full_response_text += resp_bytes[1000:].decode('utf-8', errors='ignore')
                            except Exception as e_decode_rest:
                                logger.warning(f"[API Call @{target_username}] Error decoding rest of response: {e_decode_rest}")
                        data = json.loads(full_response_text)

                        logger.debug(f"[API Call @{target_username}] JSON Data: {data}")
                        result["data"] = data
                        api_status = data.get("status")
                        api_message = data.get("message", None) # Giữ None nếu không có

                        # Linh hoạt hơn khi check status
                        if isinstance(api_status, bool): result["success"] = api_status
                        elif isinstance(api_status, str): result["success"] = api_status.lower() in ['true', 'success', 'ok']
                        else: result["success"] = False # Mặc định là False nếu không nhận dạng được

                        # Xử lý message
                        if result["success"] and api_message is None: api_message = "Follow thành công."
                        elif not result["success"] and api_message is None: api_message = f"Follow thất bại (API status={api_status})."
                        elif api_message is None: api_message = "Không có thông báo từ API."
                        result["message"] = str(api_message) # Đảm bảo message là string

                    except json.JSONDecodeError:
                        logger.error(f"[API Call @{target_username}] Response 200 OK (JSON type) but not valid JSON. Text: {response_text_for_debug}...")
                        # Cố gắng trích lỗi từ HTML nếu có
                        error_match = re.search(r'<pre>(.*?)</pre>', response_text_for_debug, re.DOTALL | re.IGNORECASE)
                        result["message"] = f"Lỗi API (HTML): {html.escape(error_match.group(1).strip())}" if error_match else "Lỗi: API trả về dữ liệu JSON không hợp lệ."
                        result["success"] = False
                    except Exception as e_proc:
                        logger.error(f"[API Call @{target_username}] Error processing API JSON data: {e_proc}", exc_info=True)
                        result["message"] = "Lỗi xử lý dữ liệu JSON từ API."
                        result["success"] = False
                else:
                     # Xử lý trường hợp không phải JSON nhưng có thể thành công
                     logger.warning(f"[API Call @{target_username}] Response 200 OK but wrong Content-Type: {content_type}. Text: {response_text_for_debug}...")
                     # Heuristic: Nếu text ngắn và không chứa chữ "lỗi" / "error", coi như thành công
                     if len(response_text_for_debug) < 200 and "lỗi" not in response_text_for_debug.lower() and "error" not in response_text_for_debug.lower():
                         result["success"] = True
                         result["message"] = "Follow thành công (phản hồi không chuẩn JSON)."
                     else:
                         result["success"] = False
                         result["message"] = f"Lỗi định dạng phản hồi API (Type: {content_type})."
            else:
                 logger.error(f"[API Call @{target_username}] HTTP Error Status: {resp.status_code}. Text: {response_text_for_debug}...")
                 result["message"] = f"Lỗi từ API follow (Code: {resp.status_code})."
                 result["success"] = False
    except httpx.TimeoutException:
        logger.warning(f"[API Call @{target_username}] API timeout.")
        result["message"] = f"Lỗi: API timeout khi follow @{html.escape(target_username)}."
        result["success"] = False
    except httpx.ConnectError as e_connect:
        logger.error(f"[API Call @{target_username}] Connection error: {e_connect}", exc_info=False) # Không cần stacktrace đầy đủ cho lỗi kết nối
        result["message"] = f"Lỗi kết nối đến API follow @{html.escape(target_username)}."
        result["success"] = False
    except httpx.RequestError as e_req: # Bao gồm các lỗi mạng khác
        logger.error(f"[API Call @{target_username}] Network error: {e_req}", exc_info=False)
        result["message"] = f"Lỗi mạng khi kết nối API follow @{html.escape(target_username)}."
        result["success"] = False
    except Exception as e_unexp:
        logger.error(f"[API Call @{target_username}] Unexpected error during API call: {e_unexp}", exc_info=True)
        result["message"] = f"Lỗi hệ thống Bot khi xử lý follow @{html.escape(target_username)}."
        result["success"] = False

    # Đảm bảo message luôn là string
    if not isinstance(result["message"], str):
        result["message"] = str(result["message"]) if result["message"] is not None else "Lỗi không xác định."
    logger.info(f"[API Call @{target_username}] Final result: Success={result['success']}, Message='{result['message'][:200]}...'") # Log kết quả cuối cùng
    return result

# --- Handlers ---

# <<<***>>> THAY ĐỔI LỆNH /start ĐỂ HIỂN THỊ MENU <<<***>>>
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start hiển thị menu chính."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user: return

    is_vip = is_user_vip(user.id)
    is_active = is_user_activated_by_key(user.id)
    can_use = is_vip or is_active

    user_status = ""
    if is_vip:
        vip_data = vip_users.get(str(user.id), {})
        expiry_ts = vip_data.get("expiry")
        limit = vip_data.get("limit", "?")
        expiry_str = "Không rõ"
        if expiry_ts:
            try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%y %H:%M')
            except (ValueError, TypeError, OSError): pass
        user_status = f"👑 <b>VIP</b> (Hết hạn: {expiry_str} | Treo: {limit})"
    elif is_active:
        expiry_ts = activated_users.get(str(user.id))
        expiry_str = "Không rõ"
        if expiry_ts:
            try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%y %H:%M')
            except (ValueError, TypeError, OSError): pass
        user_status = f"🔑 <b>Đã kích hoạt</b> (Hết hạn: {expiry_str})"
    else:
        user_status = "▫️ <b>Thành viên thường</b>"

    # Xây dựng menu động
    keyboard_buttons = []

    # Hàng 1: Lấy key / Nhập key
    row1 = [InlineKeyboardButton("🔑 Lấy Key Miễn Phí", callback_data="menu_getkey")]
    if not can_use: # Chỉ hiện Nhập Key nếu chưa kích hoạt/VIP
        row1.append(InlineKeyboardButton("🔓 Nhập Key", callback_data="menu_nhapkey"))
    keyboard_buttons.append(row1)

    # Hàng 2: Tăng Tim / Tăng Follow (chỉ dẫn)
    keyboard_buttons.append([
        InlineKeyboardButton("❤️ Tăng Tim Video", callback_data="menu_explain_tim"),
        InlineKeyboardButton("👤 Tăng Follow", callback_data="menu_explain_fl")
    ])

    # Hàng 3: Nâng cấp VIP / Thông tin VIP
    keyboard_buttons.append([InlineKeyboardButton("👑 Nâng Cấp/Mua VIP", callback_data="menu_muatt")])

    # Hàng 4: Chức năng VIP (nếu là VIP)
    if is_vip:
        keyboard_buttons.append([
             InlineKeyboardButton("⚙️ Bắt Đầu Treo", callback_data="menu_explain_treo"),
             InlineKeyboardButton("🛑 Dừng Treo", callback_data="menu_explain_dungtreo"),
        ])
        keyboard_buttons.append([InlineKeyboardButton("📊 Danh Sách Treo", callback_data="menu_listtreo")])

    # Hàng 5: Trợ giúp / Liên hệ
    keyboard_buttons.append([
        InlineKeyboardButton("ℹ️ Trợ Giúp Lệnh", callback_data="menu_help"),
        InlineKeyboardButton(f"💬 Liên Hệ Admin", url=f"tg://user?id={ADMIN_USER_ID}")
    ])

    keyboard = InlineKeyboardMarkup(keyboard_buttons)

    msg = (f"👋 <b>Xin chào {user.mention_html()}!</b>\n"
           f"🤖 Chào mừng bạn đến với <b>DinoTool Bot</b>.\n\n"
           f"💡 <b>Trạng thái hiện tại:</b> {user_status}\n\n"
           f"👇 Vui lòng chọn chức năng bên dưới:")

    try:
        # Xóa lệnh /start cũ nếu có thể
        await delete_user_message(update, context)
        # Gửi menu mới
        await context.bot.send_message(chat_id=chat_id, text=msg, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /start menu to {user.id} in chat {chat_id}: {e}")

# <<<***>>> THÊM CALLBACK QUERY HANDLER CHO MENU <<<***>>>
async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý các nút bấm từ menu inline."""
    query = update.callback_query
    user = query.from_user
    if not query or not user: return

    await query.answer() # Luôn trả lời callback để tắt loading
    data = query.data
    chat_id = query.message.chat_id
    message_id = query.message.message_id

    logger.info(f"Button '{data}' pressed by user {user.id} in chat {chat_id}")

    # --- Logic xử lý các nút ---
    if data == "menu_getkey":
        # Gửi hướng dẫn hoặc trực tiếp gọi logic (đơn giản là gửi hướng dẫn)
        text = "🔑 Để lấy key miễn phí, vui lòng gõ lệnh:\n<code>/getkey</code>"
        try:
            # Chỉnh sửa tin nhắn menu gốc
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=query.message.reply_markup) # Giữ lại menu
        except BadRequest as e:
             if "Message is not modified" not in str(e): logger.warning(f"Error editing message for menu_getkey: {e}")
        except Exception as e: logger.error(f"Error handling menu_getkey: {e}", exc_info=True)

    elif data == "menu_nhapkey":
        text = "🔓 Để nhập key bạn đã lấy, vui lòng gõ lệnh:\n<code>/nhapkey Dinotool-KEYCUABAN</code>\n(Thay <code>Dinotool-KEYCUABAN</code> bằng key của bạn)"
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=query.message.reply_markup)
        except BadRequest as e:
             if "Message is not modified" not in str(e): logger.warning(f"Error editing message for menu_nhapkey: {e}")
        except Exception as e: logger.error(f"Error handling menu_nhapkey: {e}", exc_info=True)

    elif data == "menu_explain_tim":
        text = ("❤️ Để tăng tim cho video TikTok, vui lòng gõ lệnh:\n"
                "<code>/tim &lt;link_video_tiktok&gt;</code>\n\n"
                "<i>(Bạn cần là VIP hoặc đã kích hoạt Key)</i>")
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=query.message.reply_markup)
        except BadRequest as e:
             if "Message is not modified" not in str(e): logger.warning(f"Error editing message for menu_explain_tim: {e}")
        except Exception as e: logger.error(f"Error handling menu_explain_tim: {e}", exc_info=True)

    elif data == "menu_explain_fl":
        text = ("👤 Để tăng follow cho tài khoản TikTok, vui lòng gõ lệnh:\n"
                "<code>/fl &lt;username_tiktok&gt;</code>\n\n"
                "<i>(Bạn cần là VIP hoặc đã kích hoạt Key)</i>")
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=query.message.reply_markup)
        except BadRequest as e:
             if "Message is not modified" not in str(e): logger.warning(f"Error editing message for menu_explain_fl: {e}")
        except Exception as e: logger.error(f"Error handling menu_explain_fl: {e}", exc_info=True)

    elif data == "menu_muatt":
        # Gọi trực tiếp hàm xử lý /muatt, nhưng cần tạo một update giả lập đủ dùng
        # Hoặc đơn giản là gửi lại thông tin mua TT
        # -> Gọi lại hàm `muatt_command` để hiển thị đầy đủ thông tin và QR
        # Cần đảm bảo muatt_command có thể xử lý update từ callback (hoặc tạo update giả)
        # Đơn giản nhất: Gửi tin nhắn mới
        await muatt_command(update, context) # muatt_command sẽ tự xóa lệnh cũ nếu có (nhưng ở đây ko có)
                                            # và gửi thông tin mới. Nó cũng xử lý update từ query.

    elif data == "menu_explain_treo":
        text = ("⚙️ Để tự động tăng follow (chỉ VIP), vui lòng gõ lệnh:\n"
                "<code>/treo &lt;username_tiktok&gt;</code>")
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=query.message.reply_markup)
        except BadRequest as e:
             if "Message is not modified" not in str(e): logger.warning(f"Error editing message for menu_explain_treo: {e}")
        except Exception as e: logger.error(f"Error handling menu_explain_treo: {e}", exc_info=True)

    elif data == "menu_explain_dungtreo":
        text = ("🛑 Để dừng tự động tăng follow (chỉ VIP), vui lòng gõ lệnh:\n"
                "<code>/dungtreo &lt;username_tiktok&gt;</code>")
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=query.message.reply_markup)
        except BadRequest as e:
             if "Message is not modified" not in str(e): logger.warning(f"Error editing message for menu_explain_dungtreo: {e}")
        except Exception as e: logger.error(f"Error handling menu_explain_dungtreo: {e}", exc_info=True)

    elif data == "menu_listtreo":
        # Gọi logic của /listtreo trực tiếp (vì không cần input)
        await listtreo_command(update, context) # Lệnh này sẽ gửi tin nhắn mới chứa danh sách

    elif data == "menu_help":
        # Gọi logic của /lenh trực tiếp
        await lenh_command(update, context) # Lệnh này sẽ gửi tin nhắn mới chứa trợ giúp

    # --- Nút Prompt Gửi Bill (Từ /muatt) ---
    elif data.startswith("prompt_send_bill_"):
         await prompt_send_bill_callback(update, context) # Gọi hàm xử lý riêng cho nút này

    else:
        logger.warning(f"Unhandled button callback data: {data}")
        # Có thể gửi thông báo lỗi nếu cần
        # await query.edit_message_text("Lỗi: Nút này chưa được xử lý.")


async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /lenh - Hiển thị danh sách lệnh và trạng thái user."""
    if not update or not update.effective_user: return # Sửa check update.message thành update.effective_user
    user = update.effective_user
    # Xử lý cả khi lệnh được gọi từ nút bấm (update.message sẽ None)
    chat_id = update.effective_chat.id
    if not user or not chat_id: return

    user_id = user.id
    user_id_str = str(user_id)
    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60
    act_h = ACTIVATION_DURATION_SECONDS // 3600
    key_exp_h = KEY_EXPIRY_SECONDS // 3600
    treo_interval_m = TREO_INTERVAL_SECONDS // 60

    is_vip = is_user_vip(user_id)
    is_key_active = is_user_activated_by_key(user_id)
    can_use_std_features = is_vip or is_key_active

    status_lines = []
    status_lines.append(f"👤 <b>Người dùng:</b> {user.mention_html()} (<code>{user_id}</code>)")

    if is_vip:
        vip_data = vip_users.get(user_id_str, {})
        expiry_ts = vip_data.get("expiry")
        limit = vip_data.get("limit", "?")
        expiry_str = "Không rõ"
        if expiry_ts:
            try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%Y %H:%M')
            except (ValueError, TypeError, OSError): pass # Bỏ qua lỗi nếu timestamp không hợp lệ
        status_lines.append(f"👑 <b>Trạng thái:</b> VIP ✨ (Hết hạn: {expiry_str}, Giới hạn treo: {limit} users)")
    elif is_key_active:
        expiry_ts = activated_users.get(user_id_str)
        expiry_str = "Không rõ"
        if expiry_ts:
            try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%Y %H:%M')
            except (ValueError, TypeError, OSError): pass
        status_lines.append(f"🔑 <b>Trạng thái:</b> Đã kích hoạt (Key) (Hết hạn: {expiry_str})")
    else:
        status_lines.append("▫️ <b>Trạng thái:</b> Thành viên thường")

    status_lines.append(f"⚡️ <b>Quyền dùng /tim, /fl:</b> {'✅ Có thể' if can_use_std_features else '❌ Chưa thể (Cần VIP/Key)'}")

    # Hiển thị trạng thái treo chính xác hơn dựa trên persistent_treo_configs
    if is_vip:
        current_treo_count = len(persistent_treo_configs.get(user_id_str, {})) # Đếm từ config đã lưu
        vip_limit = get_vip_limit(user_id)
        status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ✅ Có thể (Đang treo: {current_treo_count}/{vip_limit} users)")
    else:
         status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ❌ Chỉ dành cho VIP")

    cmd_lines = ["\n\n📜=== <b>DANH SÁCH LỆNH</b> ===📜"]
    cmd_lines.append("\n<b><u>🔑 Lệnh Miễn Phí (Kích hoạt Key):</u></b>")
    cmd_lines.append(f"  <code>/getkey</code> - Lấy link nhận key (⏳ {gk_cd_m}p/lần, Key hiệu lực {key_exp_h}h)")
    cmd_lines.append(f"  <code>/nhapkey &lt;key&gt;</code> - Kích hoạt tài khoản (Sử dụng {act_h}h)")
    cmd_lines.append("\n<b><u>❤️ Lệnh Tăng Tương Tác (Cần VIP/Key):</u></b>")
    cmd_lines.append(f"  <code>/tim &lt;link_video&gt;</code> - Tăng tim cho video TikTok (⏳ {tf_cd_m}p/lần)")
    cmd_lines.append(f"  <code>/fl &lt;username&gt;</code> - Tăng follow cho tài khoản TikTok (⏳ {tf_cd_m}p/user)")
    cmd_lines.append("\n<b><u>👑 Lệnh VIP:</u></b>")
    cmd_lines.append(f"  <code>/muatt</code> - Thông tin và hướng dẫn mua VIP")
    cmd_lines.append(f"  <code>/treo &lt;username&gt;</code> - Tự động chạy <code>/fl</code> mỗi {treo_interval_m} phút (Dùng slot)")
    cmd_lines.append(f"  <code>/dungtreo &lt;username&gt;</code> - Dừng treo cho một tài khoản")
    cmd_lines.append(f"  <code>/listtreo</code> - Xem danh sách tài khoản đang treo") # <-- Đã thêm
    if user_id == ADMIN_USER_ID:
        cmd_lines.append("\n<b><u>🛠️ Lệnh Admin:</u></b>")
        valid_vip_packages = ', '.join(map(str, VIP_PRICES.keys()))
        cmd_lines.append(f"  <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code> - Thêm/gia hạn VIP (Gói: {valid_vip_packages})")
        # Thêm lệnh xem list treo của user khác (tùy chọn)
        # cmd_lines.append(f"  <code>/adminlisttreo &lt;user_id&gt;</code> - Xem list treo của user khác")
    cmd_lines.append("\n<b><u>ℹ️ Lệnh Chung:</u></b>")
    cmd_lines.append(f"  <code>/start</code> - Hiển thị Menu chính") # Cập nhật mô tả /start
    cmd_lines.append(f"  <code>/lenh</code> - Xem lại bảng lệnh và trạng thái này")
    cmd_lines.append("\n<i>Lưu ý: Các lệnh yêu cầu VIP/Key chỉ hoạt động khi bạn có trạng thái tương ứng.</i>")

    help_text = "\n".join(status_lines + cmd_lines)
    try:
        # Nếu lệnh được gọi bằng cách gõ /lenh (có message), xóa lệnh gốc
        if update.message:
            await delete_user_message(update, context)
        # Gửi tin nhắn trợ giúp mới
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /lenh message to {user.id} in chat {chat_id}: {e}")


async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tim."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {user.mention_html()}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để dùng lệnh này!\n\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code>\n"
                   f"👑 Hoặc: <code>/muatt</code> để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    # Check Cooldown
    last_usage = user_tim_cooldown.get(user_id_str)
    if last_usage:
        try:
            elapsed = current_time - float(last_usage)
            if elapsed < TIM_FL_COOLDOWN_SECONDS:
                rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {user.mention_html()}, đợi <b>{rem_time:.0f} giây</b> nữa để dùng <code>/tim</code>."
                await send_temporary_message(update, context, cd_msg, duration=15)
                await delete_user_message(update, context, original_message_id)
                return
        except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp for /tim user {user_id_str}. Resetting.")
             if user_id_str in user_tim_cooldown: del user_tim_cooldown[user_id_str]; save_data()

    # Parse Arguments
    args = context.args
    video_url = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Chưa nhập link video.\n<b>Cú pháp:</b> <code>/tim https://tiktok.com/...</code>")
    elif "tiktok.com/" not in args[0] or not args[0].startswith(("http://", "https://")):
        err_txt = f"⚠️ Link <code>{html.escape(args[0])}</code> không hợp lệ. Phải là link video TikTok."
    else:
        # Cố gắng trích xuất link chuẩn hơn
        match = re.search(r"(https?://(?:www\.|vm\.|vt\.)?tiktok\.com/(?:@[a-zA-Z0-9_.]+/video/|v/|t/)?\d[\d._]*)", args[0])
        video_url = match.group(1) if match else args[0] # Fallback nếu regex không khớp

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return
    if not video_url: # Double check
        await send_temporary_message(update, context, "⚠️ Không thể xử lý link video.", duration=20)
        await delete_user_message(update, context, original_message_id)
        return
    if not API_KEY:
        logger.error(f"Missing API_KEY for /tim command triggered by user {user_id}")
        await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "❌ Lỗi cấu hình: Bot thiếu API Key. Báo Admin.", duration=20)
        return

    # Call API
    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    log_api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key="***")
    logger.info(f"User {user_id} calling /tim API: {log_api_url}")

    processing_msg = None
    final_response_text = ""
    try:
        # Gửi tin nhắn chờ và xóa lệnh gốc
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý yêu cầu tăng tim...</i></b> ❤️")
        await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc NGAY SAU KHI gửi tin chờ

        async with httpx.AsyncClient(verify=False, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot Tim Caller'})
            content_type = resp.headers.get("content-type","").lower()
            response_text_for_debug = ""
            try: response_text_for_debug = (await resp.aread()).decode('utf-8', errors='replace')[:500]
            except Exception: pass

            logger.debug(f"/tim API response status: {resp.status_code}, content-type: {content_type}")

            if resp.status_code == 200 and "application/json" in content_type:
                try:
                    data = resp.json()
                    logger.debug(f"/tim API response data: {data}")
                    if data.get("success"): # API nên trả về boolean 'success'
                        user_tim_cooldown[user_id_str] = time.time() # Đặt cooldown
                        save_data() # Lưu cooldown
                        d = data.get("data", {}) # Lấy phần data nếu có
                        # Escape HTML để tránh lỗi hiển thị
                        a = html.escape(str(d.get("author", "?")))
                        v = html.escape(str(d.get("video_url", video_url))) # Fallback về link gốc nếu API không trả về
                        db = html.escape(str(d.get('digg_before', '?')))
                        di = html.escape(str(d.get('digg_increased', '?')))
                        da = html.escape(str(d.get('digg_after', '?')))
                        final_response_text = (
                            f"🎉 <b>Tăng Tim Thành Công!</b> ❤️\n"
                            f"👤 Cho: {user.mention_html()}\n\n"
                            f"📊 <b>Thông tin Video:</b>\n"
                            f"🎬 <a href='{v}'>Link Video</a>\n"
                            f"✍️ Tác giả: <code>{a}</code>\n"
                            f"👍 Trước: <code>{db}</code> ➜ 💖 Tăng: <code>+{di}</code> ➜ ✅ Sau: <code>{da}</code>"
                        )
                    else:
                        # Lấy message lỗi từ API
                        api_msg = data.get('message', 'Không rõ lý do từ API')
                        logger.warning(f"/tim API call failed for user {user_id}. API message: {api_msg}")
                        final_response_text = f"💔 <b>Tăng Tim Thất Bại!</b>\n👤 Cho: {user.mention_html()}\nℹ️ Lý do: <code>{html.escape(api_msg)}</code>"
                except json.JSONDecodeError as e_json:
                    logger.error(f"/tim API response 200 OK but not valid JSON. Error: {e_json}. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API</b>\n👤 Cho: {user.mention_html()}\nℹ️ API không trả về JSON hợp lệ."
            else:
                logger.error(f"/tim API call HTTP error or wrong content type. Status: {resp.status_code}, Type: {content_type}. Text: {response_text_for_debug}...")
                final_response_text = f"❌ <b>Lỗi Kết Nối API Tăng Tim</b>\n👤 Cho: {user.mention_html()}\nℹ️ Mã lỗi: {resp.status_code}. Vui lòng thử lại sau."
    except httpx.TimeoutException:
        logger.warning(f"/tim API call timeout for user {user_id}")
        final_response_text = f"❌ <b>Lỗi Timeout</b>\n👤 Cho: {user.mention_html()}\nℹ️ API tăng tim không phản hồi kịp thời."
    except httpx.RequestError as e_req: # Bắt lỗi mạng chung
        logger.error(f"/tim API call network error for user {user_id}: {e_req}", exc_info=False)
        final_response_text = f"❌ <b>Lỗi Mạng</b>\n👤 Cho: {user.mention_html()}\nℹ️ Không thể kết nối đến API tăng tim."
    except Exception as e_unexp:
        logger.error(f"Unexpected error during /tim command for user {user_id}: {e_unexp}", exc_info=True)
        final_response_text = f"❌ <b>Lỗi Hệ Thống Bot</b>\n👤 Cho: {user.mention_html()}\nℹ️ Đã xảy ra lỗi. Báo Admin."
    finally:
        # Luôn cố gắng cập nhật tin nhắn chờ bằng kết quả cuối cùng
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
            except BadRequest as e_edit:
                if "Message is not modified" in str(e_edit):
                     logger.debug(f"Message {processing_msg.message_id} was not modified for /tim result.")
                elif "message to edit not found" in str(e_edit).lower():
                     logger.warning(f"Processing message {processing_msg.message_id} for /tim not found for editing.")
                else: logger.warning(f"Failed to edit /tim msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit_unexp:
                 logger.warning(f"Unexpected error editing /tim msg {processing_msg.message_id}: {e_edit_unexp}")
        else:
            # Nếu không có tin nhắn chờ (ví dụ lỗi xảy ra trước khi gửi), gửi tin nhắn mới
            logger.warning(f"Processing message for /tim user {user_id} was None. Sending new message.")
            try:
                await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e_send: logger.error(f"Failed to send final /tim message for user {user_id}: {e_send}")

# --- /fl Command ---
async def process_fl_request_background(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_id_str: str,
    target_username: str,
    processing_msg_id: int,
    invoking_user_mention: str
):
    """Hàm chạy nền xử lý API follow và cập nhật kết quả."""
    logger.info(f"[BG Task /fl] Starting for user {user_id_str} -> @{target_username}")
    api_result = await call_follow_api(user_id_str, target_username, context.bot.token)
    success = api_result["success"]
    api_message = api_result["message"]
    api_data = api_result["data"]
    final_response_text = ""
    user_info_block = ""
    follower_info_block = ""

    # Xử lý thông tin chi tiết từ API (nếu có)
    if api_data and isinstance(api_data, dict):
        try:
            name = html.escape(str(api_data.get("name", "?")))
            # API có thể trả về username chuẩn, dùng nó nếu có
            tt_username_from_api = api_data.get("username")
            tt_username = html.escape(str(tt_username_from_api if tt_username_from_api else target_username))
            tt_user_id = html.escape(str(api_data.get("user_id", "?")))
            khu_vuc = html.escape(str(api_data.get("khu_vuc", "Không rõ")))
            avatar = api_data.get("avatar", "")
            create_time = html.escape(str(api_data.get("create_time", "?")))

            # Xây dựng khối thông tin user
            user_info_lines = [f"👤 <b>Tài khoản:</b> <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)"]
            if tt_user_id != "?": user_info_lines.append(f"🆔 <b>ID TikTok:</b> <code>{tt_user_id}</code>")
            if khu_vuc != "Không rõ": user_info_lines.append(f"🌍 <b>Khu vực:</b> {khu_vuc}")
            if create_time != "?": user_info_lines.append(f"📅 <b>Ngày tạo TK:</b> {create_time}")
            # Chỉ thêm link avatar nếu nó là URL hợp lệ
            if avatar and isinstance(avatar, str) and avatar.startswith("http"):
                user_info_lines.append(f"🖼️ <a href='{html.escape(avatar)}'>Xem Avatar</a>")
            user_info_block = "\n".join(user_info_lines) + "\n" # Thêm dòng trống

            # Xây dựng khối thông tin follower
            f_before = html.escape(str(api_data.get("followers_before", "?")))
            f_add = html.escape(str(api_data.get("followers_add", "?")))
            f_after = html.escape(str(api_data.get("followers_after", "?")))
            # Chỉ hiển thị nếu có ít nhất một thông tin follower
            if any(x != "?" for x in [f_before, f_add, f_after]):
                follower_lines = ["📈 <b>Số lượng Follower:</b>"]
                if f_before != "?": follower_lines.append(f"   Trước: <code>{f_before}</code>")
                if f_add != "?" and f_add != "0": # Hiển thị tăng khác 0
                     follower_lines.append(f"   Tăng:   <b><code>+{f_add}</code></b> ✨")
                elif f_add == "0": # Hiển thị tăng 0 nếu API trả về
                    follower_lines.append(f"   Tăng:   <code>+{f_add}</code>")
                if f_after != "?": follower_lines.append(f"   Sau:    <code>{f_after}</code>")
                if len(follower_lines) > 1: # Chỉ thêm nếu có dòng nào ngoài tiêu đề
                     follower_info_block = "\n".join(follower_lines)
        except Exception as e_parse:
            logger.error(f"[BG Task /fl] Error parsing API data for @{target_username}: {e_parse}. Data: {api_data}")
            # Fallback nếu lỗi parse data
            user_info_block = f"👤 <b>Tài khoản:</b> <code>@{html.escape(target_username)}</code>\n(Lỗi xử lý thông tin chi tiết từ API)"
            follower_info_block = ""

    # Xây dựng tin nhắn kết quả cuối cùng
    if success:
        current_time_ts = time.time()
        # Cập nhật cooldown cho user và target cụ thể
        user_fl_cooldown.setdefault(str(user_id_str), {})[target_username] = current_time_ts
        save_data() # Lưu cooldown mới
        logger.info(f"[BG Task /fl] Success for user {user_id_str} -> @{target_username}. Cooldown updated.")
        final_response_text = (
            f"✅ <b>Tăng Follow Thành Công!</b>\n"
            f"✨ Cho: {invoking_user_mention}\n\n"
            # Thêm thông tin user và follower nếu có
            f"{user_info_block if user_info_block else f'👤 <b>Tài khoản:</b> <code>@{html.escape(target_username)}</code>\n'}" # Đảm bảo có ít nhất tên user
            f"{follower_info_block if follower_info_block else ''}"
        )
    else:
        logger.warning(f"[BG Task /fl] Failed for user {user_id_str} -> @{target_username}. API Message: {api_message}")
        final_response_text = (
            f"❌ <b>Tăng Follow Thất Bại!</b>\n"
            f"👤 Cho: {invoking_user_mention}\n"
            f"🎯 Target: <code>@{html.escape(target_username)}</code>\n\n"
            f"💬 Lý do API: <i>{html.escape(api_message or 'Không rõ')}</i>\n\n" # Hiển thị lý do lỗi
            f"{user_info_block if user_info_block else ''}" # Vẫn hiển thị thông tin user nếu có
        )
        # Gợi ý nếu lỗi là do thời gian chờ
        if isinstance(api_message, str) and "đợi" in api_message.lower() and ("phút" in api_message.lower() or "giây" in api_message.lower()):
            final_response_text += f"\n\n<i>ℹ️ API yêu cầu chờ đợi. Vui lòng thử lại sau.</i>"

    # Cập nhật tin nhắn chờ
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=processing_msg_id, text=final_response_text,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
        logger.info(f"[BG Task /fl] Edited message {processing_msg_id} for user {user_id_str} -> @{target_username}")
    except BadRequest as e:
         # Bỏ qua lỗi "Message is not modified"
         if "Message is not modified" in str(e): logger.debug(f"[BG Task /fl] Message {processing_msg_id} was not modified.")
         elif "message to edit not found" in str(e).lower(): logger.warning(f"[BG Task /fl] Message {processing_msg_id} not found for editing.")
         else: logger.error(f"[BG Task /fl] BadRequest editing msg {processing_msg_id}: {e}")
    except Exception as e:
        logger.error(f"[BG Task /fl] Failed to edit msg {processing_msg_id}: {e}", exc_info=True)

# <<<***>>> XÓA BỎ REGEX TRONG LỆNH /fl <<<***>>>
async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /fl - Check quyền, cooldown, gửi tin chờ và chạy task nền."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    invoking_user_mention = user.mention_html()
    current_time = time.time()
    original_message_id = update.message.message_id

    # 1. Check quyền
    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để dùng lệnh này!\n\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code>\n"
                   f"👑 Hoặc: <code>/muatt</code> để nâng cấp VIP.")
        await send_temporary_message(update, context, err_msg, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Parse Arguments
    args = context.args
    target_username = None
    err_txt = None
    # --- DÒNG REGEX ĐÃ BỊ XÓA BỎ ---

    if not args:
        err_txt = ("⚠️ Chưa nhập username TikTok.\n<b>Cú pháp:</b> <code>/fl username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@") # Xóa @ nếu có
        if not uname: err_txt = "⚠️ Username không được trống."
        # --- DÒNG KIỂM TRA BẰNG REGEX ĐÃ BỊ XÓA BỎ ---
        # elif not re.match(username_regex, uname): # <- Dòng này bị xóa
        #     err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
        #                f"(Chỉ chứa chữ, số, '.', '_', dài 2-24 ký tự)") # <- Dòng này bị xóa

        # Giữ lại các kiểm tra khác
        elif uname.startswith('.') or uname.endswith('.') or uname.startswith('_') or uname.endswith('_'):
             err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ (không được bắt đầu/kết thúc bằng '.' hoặc '_')."
        # Kiểm tra độ dài cơ bản (có thể giữ lại hoặc bỏ nếu API tự xử lý)
        elif not (2 <= len(uname) <= 34): # Giới hạn username TikTok thường dài hơn 24
            err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> có độ dài không hợp lệ (thường từ 2-34 ký tự)."
        else: target_username = uname # Username hợp lệ (theo các kiểm tra còn lại)

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # 3. Check Cooldown (chỉ check nếu username hợp lệ)
    if target_username:
        user_cds = user_fl_cooldown.get(user_id_str, {}) # Lấy dict cooldown của user
        last_usage = user_cds.get(target_username) # Lấy timestamp cho target cụ thể
        if last_usage:
            try:
                elapsed = current_time - float(last_usage)
                if elapsed < TIM_FL_COOLDOWN_SECONDS:
                     rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                     cd_msg = f"⏳ {invoking_user_mention}, đợi <b>{rem_time:.0f} giây</b> nữa để dùng <code>/fl</code> cho <code>@{html.escape(target_username)}</code>."
                     await send_temporary_message(update, context, cd_msg, duration=15)
                     await delete_user_message(update, context, original_message_id)
                     return # Dừng xử lý nếu đang cooldown
            except (ValueError, TypeError):
                 # Xóa cooldown hỏng nếu có
                 logger.warning(f"Invalid cooldown timestamp for /fl user {user_id_str} target {target_username}. Resetting.")
                 if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                     del user_fl_cooldown[user_id_str][target_username]; save_data()

    # 4. Gửi tin nhắn chờ và chạy nền
    processing_msg = None
    try:
        # Đảm bảo target_username vẫn tồn tại trước khi chạy task
        if not target_username: raise ValueError("Target username became None unexpectedly before processing")

        processing_msg = await update.message.reply_html(
            f"⏳ {invoking_user_mention}, đã nhận yêu cầu tăng follow cho <code>@{html.escape(target_username)}</code>. Đang xử lý..."
        )
        await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc

        logger.info(f"Scheduling background task for /fl user {user_id} target @{target_username}")
        # Chạy hàm xử lý API trong nền
        context.application.create_task(
            process_fl_request_background(
                context=context, chat_id=chat_id, user_id_str=user_id_str,
                target_username=target_username, processing_msg_id=processing_msg.message_id,
                invoking_user_mention=invoking_user_mention # Truyền mention để dùng trong task nền
            ),
            name=f"fl_bg_{user_id_str}_{target_username}" # Đặt tên cho task để dễ debug
        )
    except (BadRequest, Forbidden, TelegramError, ValueError) as e:
        # Lỗi khi gửi tin nhắn chờ hoặc lên lịch task
        logger.error(f"Failed to send processing message or schedule task for /fl @{target_username or '???'}: {e}")
        await delete_user_message(update, context, original_message_id) # Cố gắng xóa lệnh gốc nếu chưa xóa
        # Cố gắng cập nhật tin nhắn chờ (nếu đã gửi) để báo lỗi
        if processing_msg:
            try: await context.bot.edit_message_text(chat_id, processing_msg.message_id, f"❌ Lỗi khi bắt đầu xử lý yêu cầu /fl cho @{html.escape(target_username or '???')}. Vui lòng thử lại.")
            except Exception: pass # Bỏ qua nếu không sửa được
    except Exception as e:
         # Lỗi không mong muốn khác
         logger.error(f"Unexpected error in fl_command for user {user_id} target @{target_username or '???'}: {e}", exc_info=True)
         await delete_user_message(update, context, original_message_id)

# --- Lệnh /getkey (Giữ nguyên logic, cải thiện logging/error handling) ---
async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    # Check Cooldown
    last_usage = user_getkey_cooldown.get(user_id_str)
    if last_usage:
        try:
            elapsed = current_time - float(last_usage)
            if elapsed < GETKEY_COOLDOWN_SECONDS:
                remaining = GETKEY_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {user.mention_html()}, đợi <b>{remaining:.0f} giây</b> nữa để dùng <code>/getkey</code>."
                await send_temporary_message(update, context, cd_msg, duration=15)
                await delete_user_message(update, context, original_message_id)
                return
        except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp for /getkey user {user_id_str}. Resetting.")
             if user_id_str in user_getkey_cooldown: del user_getkey_cooldown[user_id_str]; save_data()

    # Tạo Key và Link
    generated_key = generate_random_key()
    while generated_key in valid_keys: # Đảm bảo key là duy nhất
        logger.warning(f"Key collision detected for {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    # Thêm cache buster đơn giản để tránh cache phía trình duyệt/CDN
    cache_buster = f"&ts={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster
    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    # Log params nhưng ẩn token
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}" if len(LINK_SHORTENER_API_KEY) > 6 else "***", "format": "json", "url": final_target_url }
    logger.info(f"User {user_id} requesting key. Generated: {generated_key}. Target URL for shortener: {final_target_url}")

    processing_msg = None
    final_response_text = ""
    key_stored_successfully = False # Flag để biết key đã được lưu chưa

    try:
        # Gửi tin nhắn chờ và xóa lệnh gốc
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key, vui lòng chờ...</i></b> 🔑")
        await delete_user_message(update, context, original_message_id)

        # Lưu Key tạm thời TRƯỚC khi gọi API rút gọn
        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = {
            "user_id_generator": user_id, "generation_time": generation_time,
            "expiry_time": expiry_time, "used_by": None, "activation_time": None
        }
        save_data() # Lưu ngay khi key được tạo
        key_stored_successfully = True
        logger.info(f"Key {generated_key} stored for user {user_id}. Expires at {datetime.fromtimestamp(expiry_time).isoformat()}.")

        # Gọi API Rút Gọn Link
        logger.debug(f"Calling shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_shortener_params}")
        async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
            headers = {'User-Agent': 'Telegram Bot Key Generator'} # User-Agent tùy chỉnh
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers=headers)
            response_content_type = response.headers.get("content-type", "").lower()
            response_text_for_debug = ""
            try: response_text_for_debug = (await response.aread()).decode('utf-8', errors='replace')[:500]
            except Exception: pass
            logger.debug(f"Shortener API response status: {response.status_code}, content-type: {response_content_type}")
            logger.debug(f"Shortener API response text snippet: {response_text_for_debug}...")

            if response.status_code == 200:
                try:
                    response_data = response.json()
                    logger.debug(f"Parsed shortener API response: {response_data}")
                    status = response_data.get("status")
                    generated_short_url = response_data.get("shortenedUrl") # Tên key có thể khác nhau tùy API

                    if status == "success" and generated_short_url:
                        # Thành công -> đặt cooldown
                        user_getkey_cooldown[user_id_str] = time.time()
                        save_data() # Lưu cooldown
                        logger.info(f"Successfully generated short link for user {user_id}: {generated_short_url}. Key {generated_key} confirmed.")
                        final_response_text = (
                            f"🚀 <b>Link Lấy Key Của Bạn ({user.mention_html()}):</b>\n\n"
                            # Escape URL để tránh lỗi HTML nếu URL chứa ký tự đặc biệt
                            f"🔗 <a href='{html.escape(generated_short_url)}'>{html.escape(generated_short_url)}</a>\n\n"
                            f"📝 <b>Hướng dẫn:</b>\n"
                            f"   1️⃣ Click vào link trên.\n"
                            f"   2️⃣ Làm theo các bước trên trang web để nhận Key (VD: <code>Dinotool-ABC123XYZ</code>).\n"
                            f"   3️⃣ Copy Key đó và quay lại đây.\n"
                            f"   4️⃣ Gửi lệnh: <code>/nhapkey &lt;key_ban_vua_copy&gt;</code>\n\n"
                            f"⏳ <i>Key chỉ có hiệu lực để nhập trong <b>{KEY_EXPIRY_SECONDS // 3600} giờ</b>. Hãy nhập sớm!</i>"
                        )
                    else:
                        # Lỗi từ API rút gọn link
                        api_message = response_data.get("message", "Lỗi không xác định từ API rút gọn link.")
                        logger.error(f"Shortener API returned error for user {user_id}. Status: {status}, Message: {api_message}. Data: {response_data}")
                        final_response_text = f"❌ <b>Lỗi Khi Tạo Link:</b>\n<code>{html.escape(str(api_message))}</code>\nVui lòng thử lại sau hoặc báo Admin."
                        # Không cần xóa key đã lưu ở đây, để user có thể thử lại /getkey sau
                except json.JSONDecodeError:
                    logger.error(f"Shortener API Status 200 but JSON decode failed. Type: '{response_content_type}'. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API:</b> Máy chủ rút gọn link trả về dữ liệu không hợp lệ. Vui lòng thử lại sau."
            else:
                 # Lỗi HTTP từ API rút gọn link
                 logger.error(f"Shortener API HTTP error. Status: {response.status_code}. Type: '{response_content_type}'. Text: {response_text_for_debug}...")
                 final_response_text = f"❌ <b>Lỗi Kết Nối API Tạo Link</b> (Mã: {response.status_code}). Vui lòng thử lại sau hoặc báo Admin."
    except httpx.TimeoutException:
        logger.warning(f"Shortener API timeout during /getkey for user {user_id}")
        final_response_text = "❌ <b>Lỗi Timeout:</b> Máy chủ tạo link không phản hồi kịp thời. Vui lòng thử lại sau."
    except httpx.ConnectError as e_connect: # Lỗi kết nối cụ thể
        logger.error(f"Shortener API connection error during /getkey for user {user_id}: {e_connect}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Kết Nối:</b> Không thể kết nối đến máy chủ tạo link. Vui lòng kiểm tra mạng hoặc thử lại sau."
    except httpx.RequestError as e_req: # Lỗi mạng chung khác
        logger.error(f"Shortener API network error during /getkey for user {user_id}: {e_req}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Mạng</b> khi gọi API tạo link. Vui lòng thử lại sau."
    except Exception as e_unexp:
        logger.error(f"Unexpected error during /getkey command for user {user_id}: {e_unexp}", exc_info=True)
        final_response_text = "❌ <b>Lỗi Hệ Thống Bot</b> khi tạo key. Vui lòng báo Admin."
        # Nếu lỗi xảy ra sau khi đã lưu key, và key chưa được dùng, nên xóa key đó đi để tránh key "mồ côi"
        if key_stored_successfully and generated_key in valid_keys and valid_keys[generated_key].get("used_by") is None:
            try:
                del valid_keys[generated_key]
                save_data()
                logger.info(f"Removed unused key {generated_key} due to unexpected error in /getkey.")
            except Exception as e_rem: logger.error(f"Failed to remove unused key {generated_key} after error: {e_rem}")

    finally:
        # Cập nhật tin nhắn chờ bằng kết quả cuối cùng
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
            except BadRequest as e_edit:
                 if "Message is not modified" in str(e_edit): logger.debug(f"/getkey msg {processing_msg.message_id} not modified.")
                 elif "message to edit not found" in str(e_edit).lower(): logger.warning(f"Processing message {processing_msg.message_id} for /getkey not found.")
                 else: logger.warning(f"Failed to edit /getkey msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit_unexp:
                 logger.warning(f"Unexpected error editing /getkey msg {processing_msg.message_id}: {e_edit_unexp}")
        else:
             logger.warning(f"Processing message for /getkey user {user_id} was None. Sending new message.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
             except Exception as e_send: logger.error(f"Failed to send final /getkey message for user {user_id}: {e_send}")

# --- Lệnh /nhapkey (Giữ nguyên logic, cải thiện logging/error handling) ---
async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)

    # Parse Input
    args = context.args
    submitted_key = None
    err_txt = ""
    key_prefix = "Dinotool-"
    # Regex chính xác hơn: Prefix + chữ IN HOA hoặc số, ít nhất 1 ký tự
    key_format_regex = re.compile(r"^" + re.escape(key_prefix) + r"[A-Z0-9]+$")

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập key.\n"
                   "<b>Cú pháp đúng:</b> <code>/nhapkey Dinotool-KEYCỦABẠN</code>")
    elif len(args) > 1:
        err_txt = f"⚠️ Bạn đã nhập quá nhiều từ. Chỉ nhập key thôi.\nVí dụ: <code>/nhapkey {generate_random_key()}</code>"
    else:
        key_input = args[0].strip()
        if not key_format_regex.match(key_input):
             err_txt = (f"⚠️ Key <code>{html.escape(key_input)}</code> sai định dạng.\n"
                        f"Phải bắt đầu bằng <code>{key_prefix}</code> và theo sau là chữ IN HOA/số.")
        else:
            submitted_key = key_input # Key hợp lệ về mặt định dạng

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # Validate Key Logic
    logger.info(f"User {user_id} attempting key activation with: '{submitted_key}'")
    key_data = valid_keys.get(submitted_key)
    final_response_text = ""

    if not key_data:
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' not found.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> không hợp lệ hoặc không tồn tại. Dùng <code>/getkey</code> để lấy key mới."
    elif key_data.get("used_by") is not None:
        # Key đã được sử dụng
        used_by_id = key_data["used_by"]
        activation_time_ts = key_data.get("activation_time")
        used_time_str = ""
        if activation_time_ts:
            try: used_time_str = f" lúc {datetime.fromtimestamp(float(activation_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
            except (ValueError, TypeError, OSError): pass # Bỏ qua lỗi format time

        if str(used_by_id) == user_id_str:
             logger.info(f"Key validation: User {user_id} already used key '{submitted_key}'{used_time_str}.")
             final_response_text = f"⚠️ Bạn đã kích hoạt key <code>{html.escape(submitted_key)}</code> này rồi{used_time_str}."
        else:
             logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by user {used_by_id}{used_time_str}.")
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được người khác sử dụng{used_time_str}."
    elif current_time > float(key_data.get("expiry_time", 0)):
        # Key đã hết hạn (chưa được sử dụng)
        expiry_time_ts = key_data.get("expiry_time")
        expiry_time_str = ""
        if expiry_time_ts:
            try: expiry_time_str = f" vào lúc {datetime.fromtimestamp(float(expiry_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
            except (ValueError, TypeError, OSError): pass

        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' expired{expiry_time_str}.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn sử dụng{expiry_time_str}. Dùng <code>/getkey</code> để lấy key mới."
        # Xóa key hết hạn khỏi danh sách khi có người cố gắng nhập
        if submitted_key in valid_keys:
             del valid_keys[submitted_key]; save_data(); logger.info(f"Removed expired key {submitted_key} upon activation attempt.")
    else:
        # Key hợp lệ, chưa sử dụng, chưa hết hạn -> Kích hoạt
        try:
            # Cập nhật thông tin key
            key_data["used_by"] = user_id
            key_data["activation_time"] = current_time

            # Thêm user vào danh sách kích hoạt
            activation_expiry_ts = current_time + ACTIVATION_DURATION_SECONDS
            activated_users[user_id_str] = activation_expiry_ts
            save_data() # Lưu cả hai thay đổi

            expiry_dt = datetime.fromtimestamp(activation_expiry_ts)
            expiry_str = expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')
            act_hours = ACTIVATION_DURATION_SECONDS // 3600
            logger.info(f"Key '{submitted_key}' successfully activated by user {user_id}. Activation expires at {expiry_str}.")
            final_response_text = (f"✅ <b>Kích Hoạt Key Thành Công!</b>\n\n"
                                   f"👤 Người dùng: {user.mention_html()}\n"
                                   f"🔑 Key: <code>{html.escape(submitted_key)}</code>\n\n"
                                   f"✨ Bạn có thể sử dụng <code>/tim</code> và <code>/fl</code>.\n"
                                   f"⏳ Hết hạn vào: <b>{expiry_str}</b> (sau {act_hours} giờ)."
                                 )
        except Exception as e_activate:
             logger.error(f"Unexpected error during key activation process for user {user_id} key {submitted_key}: {e_activate}", exc_info=True)
             final_response_text = f"❌ Lỗi hệ thống khi kích hoạt key <code>{html.escape(submitted_key)}</code>. Báo Admin."
             # Rollback nếu lỗi xảy ra giữa chừng
             if submitted_key in valid_keys and valid_keys[submitted_key].get("used_by") == user_id:
                 valid_keys[submitted_key]["used_by"] = None
                 valid_keys[submitted_key]["activation_time"] = None
             if user_id_str in activated_users: del activated_users[user_id_str]
             save_data() # Lưu lại trạng thái rollback

    # Gửi phản hồi và xóa lệnh gốc
    await delete_user_message(update, context, original_message_id)
    try:
        # Gửi kết quả cuối cùng
        await update.message.reply_html(final_response_text, disable_web_page_preview=True)
    except Exception as e:
         logger.error(f"Failed to send /nhapkey final response to user {user_id}: {e}")

# --- Lệnh /muatt (SỬA THEO YÊU CẦU 2) ---
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị thông tin mua VIP và nút yêu cầu gửi bill."""
    # Xác định xem update đến từ message hay callback query
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user or not chat_id: return

    original_message_id = None
    if update.message: # Lệnh được gõ
        original_message_id = update.message.message_id
    # Nếu từ callback query, không có message_id gốc để xóa

    user_id = user.id
    # Tạo nội dung chuyển khoản dựa trên ID người dùng
    payment_note = f"{PAYMENT_NOTE_PREFIX} {user_id}"

    # Xây dựng nội dung tin nhắn
    text_lines = ["👑 <b>Thông Tin Nâng Cấp VIP - DinoTool</b> 👑",
                  "\nTrở thành VIP để mở khóa <code>/treo</code>, không cần lấy key và nhiều ưu đãi!",
                  "\n💎 <b>Các Gói VIP Hiện Có:</b>"]
    for days_key, info in VIP_PRICES.items():
        text_lines.extend([f"\n⭐️ <b>Gói {info['duration_days']} Ngày:</b>",
                           f"   - 💰 Giá: <b>{info['price']}</b>",
                           f"   - ⏳ Thời hạn: {info['duration_days']} ngày",
                           f"   - 🚀 Treo tối đa: <b>{info['limit']} tài khoản</b> TikTok"])
    text_lines.extend(["\n🏦 <b>Thông tin thanh toán:</b>",
                       f"   - Ngân hàng: <b>{BANK_NAME}</b>",
                       # Cho phép copy STK và Nội dung CK
                       f"   - STK: <a href=\"https://t.me/share/url?url={BANK_ACCOUNT}\"><code>{BANK_ACCOUNT}</code></a> (👈 Click để copy)",
                       f"   - Tên chủ TK: <b>{ACCOUNT_NAME}</b>",
                       "\n📝 <b>Nội dung chuyển khoản (Quan trọng!):</b>",
                       f"   » Chuyển khoản với nội dung <b>CHÍNH XÁC</b> là:",
                       f"   » <a href=\"https://t.me/share/url?url={payment_note}\"><code>{payment_note}</code></a> (👈 Click để copy)",
                       f"   <i>(Sai nội dung có thể khiến giao dịch xử lý chậm)</i>",
                       "\n📸 <b>Sau Khi Chuyển Khoản Thành Công:</b>",
                       f"   1️⃣ Chụp ảnh màn hình biên lai (bill) giao dịch.",
                       # Hướng dẫn nhấn nút và gửi ảnh VÀO CHAT HIỆN TẠI
                       f"   2️⃣ Nhấn nút 'Gửi Bill Thanh Toán' bên dưới.",
                       f"   3️⃣ Bot sẽ yêu cầu bạn gửi ảnh bill <b><u>VÀO CUỘC TRÒ CHUYỆN NÀY</u></b>.", # Nhấn mạnh gửi vào đây
                       f"   4️⃣ Gửi ảnh bill của bạn vào đây.",
                       f"   5️⃣ Bot sẽ tự động chuyển tiếp ảnh đến Admin để xác nhận.",
                       # Không cần nói gửi vào group nào nữa
                       f"   6️⃣ Admin sẽ kiểm tra và kích hoạt VIP sớm nhất.",
                       "\n<i>Cảm ơn bạn đã quan tâm và ủng hộ DinoTool!</i> ❤️"])
    text = "\n".join(text_lines)

    # Tạo nút bấm Inline
    keyboard = InlineKeyboardMarkup([
        # Thêm user_id vào callback_data để biết ai đã nhấn nút
        [InlineKeyboardButton("📸 Gửi Bill Thanh Toán", callback_data=f"prompt_send_bill_{user_id}")]
    ])

    # Nếu lệnh được gõ, xóa lệnh /muatt gốc
    if original_message_id:
        await delete_user_message(update, context, original_message_id)

    # Gửi tin nhắn có ảnh QR và caption kèm nút bấm
    try:
        # Nếu gọi từ nút bấm, gửi tin nhắn mới
        # Nếu gọi từ lệnh /muatt, cũng gửi tin nhắn mới
        await context.bot.send_photo(chat_id=chat_id, photo=QR_CODE_URL, caption=text,
                                   parse_mode=ParseMode.HTML, reply_markup=keyboard)
        logger.info(f"Sent /muatt info with prompt button to user {user_id} in chat {chat_id}")
    except (BadRequest, Forbidden, TelegramError) as e:
        # Nếu gửi ảnh lỗi (ví dụ link QR hỏng), gửi dạng text
        logger.error(f"Error sending /muatt photo+caption to chat {chat_id}: {e}. Falling back to text.")
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML,
                                           disable_web_page_preview=True, reply_markup=keyboard)
            logger.info(f"Sent /muatt fallback text info with prompt button to user {user_id} in chat {chat_id}")
        except Exception as e_text:
             logger.error(f"Error sending fallback text for /muatt to chat {chat_id}: {e_text}")
    except Exception as e_unexp:
        logger.error(f"Unexpected error sending /muatt command to chat {chat_id}: {e_unexp}", exc_info=True)

# --- Callback Handler cho nút "Gửi Bill Thanh Toán" (SỬA THEO YÊU CẦU 2) ---
async def prompt_send_bill_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý khi người dùng nhấn nút Gửi Bill."""
    query = update.callback_query
    user = query.from_user
    chat_id = query.message.chat_id
    if not query or not user: return

    # Lấy user_id từ callback_data
    callback_data = query.data
    expected_user_id = None
    try:
        if callback_data.startswith("prompt_send_bill_"):
            expected_user_id = int(callback_data.split("_")[-1])
    except (ValueError, IndexError):
        logger.warning(f"Invalid callback_data format: {callback_data}")
        await query.answer("Lỗi: Dữ liệu nút không hợp lệ.", show_alert=True)
        return

    # Chỉ người dùng ban đầu nhấn /muatt mới được tương tác với nút này
    if user.id != expected_user_id:
        await query.answer("Bạn không phải người yêu cầu thanh toán.", show_alert=True)
        logger.info(f"User {user.id} tried to click bill prompt button for user {expected_user_id} in chat {chat_id}")
        return

    # Thêm user ID vào danh sách chờ nhận bill
    pending_bill_user_ids.add(user.id)
    # Lên lịch xóa user khỏi danh sách chờ sau một thời gian (vd: 15 phút) nếu họ không gửi ảnh
    if context.job_queue:
        # Xóa job cũ nếu có (phòng trường hợp nhấn nút nhiều lần)
        existing_jobs = context.job_queue.get_jobs_by_name(f"remove_pending_bill_{user.id}")
        for job in existing_jobs:
            job.schedule_removal()
            logger.debug(f"Removed existing pending bill timeout job for user {user.id}")
        # Tạo job mới
        context.job_queue.run_once(
            remove_pending_bill_user_job,
            15 * 60, # 15 phút
            data={'user_id': user.id},
            name=f"remove_pending_bill_{user.id}"
        )

    await query.answer() # Xác nhận đã nhận callback
    logger.info(f"User {user.id} clicked 'prompt_send_bill' button in chat {chat_id}. Added to pending list.")

    # Gửi tin nhắn yêu cầu gửi ảnh VÀO CHAT NÀY
    prompt_text = f"📸 {user.mention_html()}, vui lòng gửi ảnh chụp màn hình biên lai thanh toán của bạn <b><u>vào cuộc trò chuyện này</u></b>."
    try:
        # Gửi tin nhắn yêu cầu mới
        await context.bot.send_message(chat_id=chat_id, text=prompt_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error sending bill prompt message to {user.id} in chat {chat_id}: {e}", exc_info=True)

async def remove_pending_bill_user_job(context: ContextTypes.DEFAULT_TYPE):
    """Job để xóa user khỏi danh sách chờ nhận bill."""
    job_data = context.job.data
    user_id = job_data.get('user_id')
    if user_id in pending_bill_user_ids:
        pending_bill_user_ids.remove(user_id)
        logger.info(f"Removed user {user_id} from pending bill list due to timeout.")

# --- Xử lý nhận ảnh bill (SỬA THEO YÊU CẦU 2) ---
# Handler này sẽ chạy cho TẤT CẢ các ảnh gửi cho bot (trong PM hoặc group bot có mặt)
async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý ảnh/document ảnh gửi đến bot VÀ chỉ chuyển tiếp nếu user nằm trong danh sách chờ."""
    if not update or not update.message: return
    # Bỏ qua nếu là command
    if update.message.text and update.message.text.startswith('/'): return

    user = update.effective_user
    chat = update.effective_chat
    message = update.message
    if not user or not chat or not message: return

    # Kiểm tra xem người gửi có trong danh sách chờ nhận bill không
    if user.id not in pending_bill_user_ids:
        # logger.debug(f"Ignoring photo from user {user.id} in chat {chat.id} - not in pending bill list.")
        return # Không làm gì nếu user không trong danh sách chờ

    # Kiểm tra xem tin nhắn có phải là ảnh hoặc document ảnh không
    is_photo = bool(message.photo)
    is_image_document = bool(message.document and message.document.mime_type and message.document.mime_type.startswith('image/'))
    if not is_photo and not is_image_document:
        # logger.debug(f"Ignoring non-image message from pending user {user.id} in chat {chat.id}")
        return # Chỉ xử lý ảnh

    # Nếu user trong danh sách chờ và gửi ảnh -> Xử lý bill
    logger.info(f"Bill photo/document received from PENDING user {user.id} in chat {chat.id} (Type: {chat.type}). Forwarding to {BILL_FORWARD_TARGET_ID}.")

    # Xóa user khỏi danh sách chờ sau khi nhận được ảnh
    pending_bill_user_ids.discard(user.id)
    # Hủy job timeout nếu có
    if context.job_queue:
         jobs = context.job_queue.get_jobs_by_name(f"remove_pending_bill_{user.id}")
         for job in jobs: job.schedule_removal(); logger.debug(f"Removed pending bill timeout job for user {user.id}")

    # Tạo caption cho tin nhắn chuyển tiếp
    forward_caption_lines = [f"📄 <b>Bill Nhận Được Từ User</b>",
                             f"👤 <b>User:</b> {user.mention_html()} (<code>{user.id}</code>)"]
    # Thêm thông tin chat gốc (quan trọng để biết user gửi từ đâu)
    if chat.type == 'private':
        forward_caption_lines.append(f"💬 <b>Chat gốc:</b> PM với Bot")
    elif chat.title:
         forward_caption_lines.append(f"👥 <b>Chat gốc:</b> {html.escape(chat.title)} (<code>{chat.id}</code>)")
    else:
         forward_caption_lines.append(f"❓ <b>Chat gốc:</b> ID <code>{chat.id}</code>")
    # Lấy link tin nhắn gốc (nếu có thể)
    try:
        # message.link có thể không hoạt động trong mọi trường hợp, dùng get_message_link nếu cần
        message_link = message.link
        if message_link: forward_caption_lines.append(f"🔗 <a href='{message_link}'>Link Tin Nhắn Gốc</a>")
    except AttributeError: logger.debug(f"Could not get message link for message {message.message_id} in chat {chat.id}")

    # Thêm caption gốc của ảnh (nếu có)
    original_caption = message.caption
    if original_caption: forward_caption_lines.append(f"\n📝 <b>Caption gốc:</b>\n{html.escape(original_caption[:500])}{'...' if len(original_caption) > 500 else ''}")

    forward_caption_text = "\n".join(forward_caption_lines)

    # Chuyển tiếp tin nhắn gốc (ảnh) và gửi kèm thông tin
    try:
        # 1. Chuyển tiếp tin nhắn chứa ảnh
        await context.bot.forward_message(chat_id=BILL_FORWARD_TARGET_ID, from_chat_id=chat.id, message_id=message.message_id)
        # 2. Gửi tin nhắn thông tin bổ sung
        await context.bot.send_message(chat_id=BILL_FORWARD_TARGET_ID, text=forward_caption_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

        logger.info(f"Successfully forwarded bill message {message.message_id} from user {user.id} (chat {chat.id}) and sent info to {BILL_FORWARD_TARGET_ID}.")

        # Gửi xác nhận cho người dùng đã gửi bill thành công
        try:
            await message.reply_html("✅ Đã nhận và chuyển tiếp bill của bạn đến Admin để xử lý. Vui lòng chờ nhé!")
        except Exception as e_reply:
            logger.warning(f"Failed to send confirmation reply to user {user.id} in chat {chat.id}: {e_reply}")

    except Forbidden as e:
        logger.error(f"Bot cannot forward/send message to BILL_FORWARD_TARGET_ID ({BILL_FORWARD_TARGET_ID}). Check permissions/block status. Error: {e}")
        # Thông báo lỗi cho Admin nếu target không phải là Admin
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
            try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Lý do: Bot bị chặn hoặc thiếu quyền.")
            except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
        # Thông báo lỗi cho người dùng
        try: await message.reply_html(f"❌ Đã xảy ra lỗi khi gửi bill của bạn đến Admin. Vui lòng liên hệ Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a> để được hỗ trợ.")
        except Exception: pass
    except TelegramError as e_fwd:
         logger.error(f"Telegram error forwarding/sending bill message {message.message_id} to {BILL_FORWARD_TARGET_ID}: {e_fwd}")
         if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
              try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi Telegram khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Lỗi: {e_fwd}")
              except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
         try: await message.reply_html(f"❌ Đã xảy ra lỗi khi gửi bill của bạn đến Admin. Vui lòng liên hệ Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a> để được hỗ trợ.")
         except Exception: pass
    except Exception as e:
        logger.error(f"Unexpected error forwarding/sending bill to {BILL_FORWARD_TARGET_ID}: {e}", exc_info=True)
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
             try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi không xác định khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Chi tiết log.")
             except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
        try: await message.reply_html(f"❌ Đã xảy ra lỗi khi gửi bill của bạn đến Admin. Vui lòng liên hệ Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a> để được hỗ trợ.")
        except Exception: pass

    # Dừng xử lý update này để các handler khác không nhận nữa
    raise ApplicationHandlerStop

# --- Lệnh /addtt (Admin - Giữ nguyên logic, cải thiện logging) ---
async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cấp VIP cho người dùng (chỉ Admin)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    chat = update.effective_chat
    # Kiểm tra Admin ID
    if not admin_user or not chat or admin_user.id != ADMIN_USER_ID:
        # logger.debug(f"Ignoring /addtt command from non-admin user {admin_user.id if admin_user else 'Unknown'}")
        return # Âm thầm bỏ qua nếu không phải admin

    # Parse Arguments
    args = context.args
    err_txt = None
    target_user_id = None
    days_key_input = None
    limit = None
    duration_days = None
    valid_day_keys = list(VIP_PRICES.keys())
    valid_days_str = ', '.join(map(str, valid_day_keys))

    if len(args) != 2:
        err_txt = (f"⚠️ Sai cú pháp.\n<b>Dùng:</b> <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code>\n"
                   f"<b>Các gói hợp lệ:</b> {valid_days_str}\n"
                   f"<b>Ví dụ:</b> <code>/addtt 123456789 {valid_day_keys[0] if valid_day_keys else '15'}</code>")
    else:
        # Validate User ID
        try: target_user_id = int(args[0])
        except ValueError: err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ. Phải là một số."

        # Validate VIP package key
        if not err_txt:
            try:
                days_key_input = int(args[1])
                if days_key_input not in VIP_PRICES:
                    err_txt = f"⚠️ Gói ngày <code>{days_key_input}</code> không hợp lệ. Chỉ chấp nhận: <b>{valid_days_str}</b>."
                else:
                    # Lấy thông tin gói VIP hợp lệ
                    vip_info = VIP_PRICES[days_key_input]
                    limit = vip_info["limit"]
                    duration_days = vip_info["duration_days"]
            except ValueError: err_txt = f"⚠️ Gói ngày '<code>{html.escape(args[1])}</code>' không phải là số hợp lệ."

    if err_txt:
        try: await update.message.reply_html(err_txt)
        except Exception as e_reply: logger.error(f"Failed to send error reply to admin {admin_user.id}: {e_reply}")
        return

    # Cập nhật dữ liệu VIP
    target_user_id_str = str(target_user_id)
    current_time = time.time()
    current_vip_data = vip_users.get(target_user_id_str)
    start_time = current_time # Mặc định bắt đầu từ bây giờ
    operation_type = "Nâng cấp lên" # Mặc định là nâng cấp mới

    # Kiểm tra nếu user đã là VIP và còn hạn -> Gia hạn
    if current_vip_data:
         try:
             current_expiry = float(current_vip_data.get("expiry", 0))
             if current_expiry > current_time:
                 start_time = current_expiry # Bắt đầu từ ngày hết hạn cũ
                 operation_type = "Gia hạn thêm"
                 logger.info(f"Admin {admin_user.id}: User {target_user_id_str} already VIP. Extending from {datetime.fromtimestamp(start_time).isoformat()}.")
             else:
                 logger.info(f"Admin {admin_user.id}: User {target_user_id_str} was VIP but expired. Treating as new activation.")
         except (ValueError, TypeError):
             logger.warning(f"Admin {admin_user.id}: Invalid expiry data for user {target_user_id_str}. Treating as new activation.")

    # Tính toán thời gian hết hạn mới và lưu dữ liệu
    new_expiry_ts = start_time + duration_days * 86400 # 86400 giây/ngày
    new_expiry_dt = datetime.fromtimestamp(new_expiry_ts)
    new_expiry_str = new_expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')
    vip_users[target_user_id_str] = {"expiry": new_expiry_ts, "limit": limit}
    save_data()
    logger.info(f"Admin {admin_user.id} processed VIP for {target_user_id_str}: {operation_type} {duration_days} days. New expiry: {new_expiry_str}, Limit: {limit}")

    # Thông báo cho Admin
    admin_msg = (f"✅ Đã <b>{operation_type} {duration_days} ngày VIP</b> thành công!\n\n"
                 f"👤 User ID: <code>{target_user_id}</code>\n✨ Gói: {duration_days} ngày\n"
                 f"⏳ Hạn sử dụng mới: <b>{new_expiry_str}</b>\n🚀 Giới hạn treo: <b>{limit} users</b>")
    try: await update.message.reply_html(admin_msg)
    except Exception as e: logger.error(f"Failed to send confirmation message to admin {admin_user.id} in chat {chat.id}: {e}")

    # Thông báo cho người dùng (trong group chính nếu có, nếu không thì báo admin)
    user_mention = f"User ID <code>{target_user_id}</code>" # Mặc định
    try:
        # Cố gắng lấy mention hoặc link của user
        target_user_info = await context.bot.get_chat(target_user_id)
        if target_user_info:
             # Ưu tiên mention_html, sau đó đến link, cuối cùng là ID
             user_mention = target_user_info.mention_html() or \
                            (f"<a href='tg://user?id={target_user_id}'>User {target_user_id}</a>") or \
                            user_mention
    except Exception as e_get_chat:
        logger.warning(f"Could not get chat info for target user {target_user_id}: {e_get_chat}. Using ID instead.")

    # Tin nhắn thông báo cho user
    user_notify_msg = (f"🎉 Chúc mừng {user_mention}! 🎉\n\n"
                       f"Bạn đã được Admin <b>{operation_type} {duration_days} ngày VIP</b> thành công!\n\n"
                       f"✨ Gói VIP: <b>{duration_days} ngày</b>\n⏳ Hạn sử dụng đến: <b>{new_expiry_str}</b>\n"
                       f"🚀 Giới hạn treo: <b>{limit} tài khoản</b>\n\n"
                       f"Cảm ơn bạn đã ủng hộ DinoTool! ❤️\n(Dùng <code>/start</code> để xem Menu hoặc <code>/lenh</code> để xem lại trạng thái)")

    # Gửi thông báo vào group chính hoặc cho admin nếu group không set
    target_chat_id_for_notification = ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else ADMIN_USER_ID
    log_target = f"group {ALLOWED_GROUP_ID}" if ALLOWED_GROUP_ID else f"admin {ADMIN_USER_ID}"
    logger.info(f"Sending VIP notification for {target_user_id} to {log_target}")
    try:
        await context.bot.send_message(chat_id=target_chat_id_for_notification, text=user_notify_msg, parse_mode=ParseMode.HTML)
    except Exception as e_send_notify:
        logger.error(f"Failed to send VIP notification for user {target_user_id} to chat {target_chat_id_for_notification}: {e_send_notify}")
        # Báo lỗi cho admin nếu gửi thông báo thất bại và target không phải là admin
        if admin_user.id != target_chat_id_for_notification:
             try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} vào chat {target_chat_id_for_notification}. Lỗi: {e_send_notify}")
             except Exception: pass

# --- Logic Treo (Cập nhật để kiểm tra VIP và xử lý lỗi tốt hơn) ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Vòng lặp chạy nền cho lệnh /treo, gửi thông báo trạng thái và tự dừng khi cần."""
    user_id_int = int(user_id_str) # Chuyển sang int để dùng is_user_vip
    task_name = f"treo_{user_id_str}_{target_username}_in_{chat_id}"
    logger.info(f"[Treo Task Start] Task '{task_name}' started.")

    # Lấy mention người dùng (nếu có thể) để hiển thị đẹp hơn
    invoking_user_mention = f"User ID <code>{user_id_str}</code>"
    try:
        user_info = await context.bot.get_chat(user_id_int)
        if user_info and user_info.mention_html():
             invoking_user_mention = user_info.mention_html()
        elif user_info: # Fallback to link if mention_html fails
            invoking_user_mention = f"<a href='tg://user?id={user_id_int}'>User {user_id_str}</a>"
    except Exception as e_get_mention:
        logger.debug(f"Could not get mention for user {user_id_str} in task {task_name}: {e_get_mention}")

    last_api_call_time = 0 # Thời điểm gọi API lần cuối
    consecutive_failures = 0 # Đếm số lần lỗi liên tiếp
    MAX_CONSECUTIVE_FAILURES = 5 # Ngưỡng dừng task nếu lỗi liên tục

    try:
        while True:
            current_time = time.time()

            # 1. Kiểm tra xem task có còn trong active_treo_tasks không
            #    (để xử lý trường hợp task bị thay thế hoặc xóa thủ công)
            current_task_in_dict = active_treo_tasks.get(user_id_str, {}).get(target_username)
            current_asyncio_task = asyncio.current_task()
            if current_task_in_dict is not current_asyncio_task:
                 logger.warning(f"[Treo Task Stop] Task '{task_name}' seems replaced or removed from active_treo_tasks dict. Stopping.")
                 # Không cần gọi stop_treo_task vì nó đã bị quản lý bởi task khác hoặc đã được dừng
                 break # Thoát vòng lặp

            # 2. Kiểm tra trạng thái VIP
            if not is_user_vip(user_id_int):
                logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping task '{task_name}'.")
                # Dừng task và xóa config persistent
                await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired in loop")
                try:
                    # Gửi thông báo dừng cho người dùng vào chat gốc
                    await context.bot.send_message(
                        chat_id,
                        f"ℹ️ {invoking_user_mention}, việc treo cho <code>@{html.escape(target_username)}</code> đã dừng do VIP hết hạn.",
                        parse_mode=ParseMode.HTML, disable_notification=True
                    )
                except Exception as e_send_stop:
                     logger.warning(f"Failed to send VIP expiry stop message for task {task_name}: {e_send_stop}")
                break # Thoát vòng lặp

            # 3. Tính toán thời gian chờ trước khi gọi API
            if last_api_call_time > 0: # Chỉ chờ nếu không phải lần chạy đầu tiên
                elapsed_since_last_call = current_time - last_api_call_time
                wait_needed = TREO_INTERVAL_SECONDS - elapsed_since_last_call
                if wait_needed > 0:
                    logger.debug(f"[Treo Task Wait] Task '{task_name}' waiting for {wait_needed:.1f}s before next API call.")
                    await asyncio.sleep(wait_needed)

            # Cập nhật thời gian trước khi gọi API
            last_api_call_time = time.time()

            # 4. Gọi API Follow
            logger.info(f"[Treo Task Run] Task '{task_name}' executing follow for @{target_username}")
            api_result = await call_follow_api(user_id_str, target_username, context.bot.token)
            success = api_result["success"]
            api_message = api_result["message"] or "Không có thông báo từ API."
            gain = 0

            if success:
                consecutive_failures = 0 # Reset bộ đếm lỗi
                if api_result.get("data") and isinstance(api_result["data"], dict):
                    try:
                        gain_str = str(api_result["data"].get("followers_add", "0"))
                        # Xử lý trường hợp gain_str có thể là số thập phân hoặc có ký tự lạ
                        gain_match = re.search(r'\d+', gain_str)
                        gain = int(gain_match.group(0)) if gain_match else 0
                        if gain > 0:
                            treo_stats[user_id_str][target_username] += gain
                            logger.info(f"[Treo Task Stats] Task '{task_name}' added {gain} followers. Cycle gain for user: {treo_stats[user_id_str][target_username]}")
                        # Không log warning nếu gain = 0 vì đó là trường hợp bình thường
                    except (ValueError, TypeError, KeyError, AttributeError) as e_gain:
                         logger.warning(f"[Treo Task Stats] Task '{task_name}' error parsing gain: {e_gain}. Data: {api_result.get('data')}")
                         gain = 0 # Mặc định là 0 nếu lỗi parse
                else:
                    logger.info(f"[Treo Task Success] Task '{task_name}' successful but no data/gain info. API Msg: {api_message[:100]}...") # Log một phần message
            else: # API call thất bại
                consecutive_failures += 1
                logger.warning(f"[Treo Task Fail] Task '{task_name}' failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}). API Msg: {api_message[:100]}...")
                gain = 0
                # Kiểm tra nếu lỗi liên tục quá nhiều lần -> dừng task
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(f"[Treo Task Stop] Task '{task_name}' stopping due to {consecutive_failures} consecutive failures.")
                    await stop_treo_task(user_id_str, target_username, context, reason=f"{consecutive_failures} consecutive API failures")
                    try:
                        await context.bot.send_message(
                            chat_id,
                            f"⚠️ {invoking_user_mention}: Treo cho <code>@{html.escape(target_username)}</code> đã tạm dừng do lỗi API liên tục. Vui lòng kiểm tra và thử <code>/treo</code> lại sau.",
                            parse_mode=ParseMode.HTML, disable_notification=True
                        )
                    except Exception as e_send_fail_stop:
                        logger.warning(f"Failed to send consecutive failure stop message for task {task_name}: {e_send_fail_stop}")
                    break # Thoát vòng lặp

            # 5. Gửi thông báo trạng thái (thành công hoặc thất bại không quá ngưỡng)
            status_lines = []
            sent_status_message = None
            try:
                user_display_name = invoking_user_mention # Dùng mention đã lấy ở trên
                if success:
                    status_lines.append(f"✅ {user_display_name}: Treo <code>@{html.escape(target_username)}</code> thành công!") # Thêm tên user
                    status_lines.append(f"➕ Thêm: <b>{gain}</b>")
                    # Chỉ hiển thị message API nếu nó khác các thông báo thành công mặc định
                    default_success_msgs = ["Follow thành công.", "Success", "success"]
                    if api_message and api_message not in default_success_msgs:
                         status_lines.append(f"💬 <i>{html.escape(api_message[:150])}{'...' if len(api_message)>150 else ''}</i>") # Giới hạn độ dài
                    # else: status_lines.append(f"💬 Không có thông báo từ API.") # Có thể bỏ dòng này cho gọn
                else: # Thất bại (chưa đến ngưỡng dừng)
                    status_lines.append(f"❌ {user_display_name}: Treo <code>@{html.escape(target_username)}</code> thất bại!")
                    status_lines.append(f"➕ Thêm: 0")
                    status_lines.append(f"💬 Lý do: <i>{html.escape(api_message[:150])}{'...' if len(api_message)>150 else ''}</i>")

                status_msg = "\n".join(status_lines)
                sent_status_message = await context.bot.send_message(chat_id=chat_id, text=status_msg, parse_mode=ParseMode.HTML, disable_notification=True)

                # Lên lịch xóa tin nhắn thất bại sau một khoảng thời gian ngắn
                if not success and sent_status_message and context.job_queue:
                    job_name_del = f"del_treo_fail_{chat_id}_{sent_status_message.message_id}"
                    context.job_queue.run_once(
                        delete_message_job,
                        TREO_FAILURE_MSG_DELETE_DELAY,
                        data={'chat_id': chat_id, 'message_id': sent_status_message.message_id},
                        name=job_name_del
                    )
                    logger.debug(f"Scheduled job '{job_name_del}' to delete failure message {sent_status_message.message_id} in {TREO_FAILURE_MSG_DELETE_DELAY}s.")
            except Forbidden:
                logger.warning(f"Could not send treo status for '{task_name}' to chat {chat_id}. Bot might be kicked/blocked. Stopping task.")
                await stop_treo_task(user_id_str, target_username, context, reason=f"Bot Forbidden in chat {chat_id}")
                break # Thoát vòng lặp nếu không gửi được tin nhắn
            except TelegramError as e_send:
                 logger.error(f"Error sending treo status for '{task_name}' to chat {chat_id}: {e_send}")
                 # Có thể tiếp tục chạy nếu lỗi gửi tin nhắn không nghiêm trọng? Hoặc dừng? -> Hiện tại vẫn chạy tiếp
            except Exception as e_unexp_send:
                 logger.error(f"Unexpected error sending treo status for '{task_name}' to chat {chat_id}: {e_unexp_send}", exc_info=True)

            # 6. Chờ đợi cho chu kỳ tiếp theo (sleep đã được chuyển lên đầu vòng lặp sau)
            # logger.debug(f"[Treo Task Sleep] Task '{task_name}' completed cycle. Will wait before next.")
            # Không cần sleep ở đây nữa, sleep sẽ được tính ở đầu vòng lặp tiếp theo dựa trên last_api_call_time

    except asyncio.CancelledError:
        # Task bị hủy từ bên ngoài (vd: /dungtreo, shutdown, cleanup)
        logger.info(f"[Treo Task Cancelled] Task '{task_name}' was cancelled externally.")
        # Không cần gọi stop_treo_task vì nơi hủy task phải chịu trách nhiệm đó
    except Exception as e:
        # Lỗi không mong muốn trong vòng lặp
        logger.error(f"[Treo Task Error] Unexpected error in task '{task_name}': {e}", exc_info=True)
        try:
            # Thông báo lỗi nghiêm trọng cho user
            await context.bot.send_message(
                chat_id,
                f"💥 {invoking_user_mention}: Lỗi nghiêm trọng khi treo <code>@{html.escape(target_username)}</code>. Tác vụ đã dừng. Lỗi: {html.escape(str(e))}",
                parse_mode=ParseMode.HTML, disable_notification=True
            )
        except Exception as e_send_fatal:
             logger.error(f"Failed to send fatal error message for task {task_name}: {e_send_fatal}")
        # Dừng task và xóa config khi có lỗi nghiêm trọng
        await stop_treo_task(user_id_str, target_username, context, reason=f"Unexpected Error: {e}")
    finally:
        logger.info(f"[Treo Task End] Task '{task_name}' finished.")
        # Dọn dẹp task khỏi active_treo_tasks nếu nó kết thúc tự nhiên (ít khả năng xảy ra với while True)
        # hoặc nếu nó bị lỗi mà chưa được xóa ở trên.
        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
             task_in_dict = active_treo_tasks[user_id_str].get(target_username)
             current_task = None
             try: current_task = asyncio.current_task()
             except RuntimeError: pass # Có thể lỗi nếu task đã kết thúc hoàn toàn
             # Chỉ xóa nếu task trong dict chính là task hiện tại và nó đã kết thúc
             if task_in_dict is current_task and task_in_dict and task_in_dict.done():
                del active_treo_tasks[user_id_str][target_username]
                if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                logger.info(f"[Treo Task Cleanup] Removed finished/failed task '{task_name}' from active tasks dict in finally block.")

# --- Lệnh /treo (VIP - Cập nhật để lưu persistent config) ---
async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bắt đầu treo tự động follow cho một user (chỉ VIP). Lưu config."""
    global persistent_treo_configs, active_treo_tasks # Khai báo để sửa đổi
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()

    # 1. Check VIP
    if not is_user_vip(user_id):
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/treo</code> chỉ dành cho <b>VIP</b>.\nDùng <code>/muatt</code> để nâng cấp."
        await send_temporary_message(update, context, err_msg, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Parse Arguments
    args = context.args
    target_username = None
    err_txt = None
    # --- DÒNG REGEX ĐÃ BỊ XÓA BỎ ---
    # username_regex = r"^[a-zA-Z0-9_.]{2,24}$" # Regex cũ

    if not args: err_txt = ("⚠️ Chưa nhập username TikTok cần treo.\n<b>Cú pháp:</b> <code>/treo username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname: err_txt = "⚠️ Username không được trống."
        # --- DÒNG KIỂM TRA BẰNG REGEX ĐÃ BỊ XÓA BỎ ---
        # elif not re.match(username_regex, uname): err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n(Chữ, số, '.', '_', dài 2-24)")
        elif uname.startswith('.') or uname.endswith('.') or uname.startswith('_') or uname.endswith('_'): err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ (không bắt đầu/kết thúc bằng '.' hoặc '_')."
        elif not (2 <= len(uname) <= 34): # Giữ lại kiểm tra độ dài
            err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> có độ dài không hợp lệ (thường từ 2-34 ký tự)."
        else: target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # 3. Check Giới Hạn và Trạng Thái Treo Hiện Tại
    if target_username:
        vip_limit = get_vip_limit(user_id)
        # Lấy danh sách target từ persistent config là đủ để kiểm tra limit và trùng lặp
        persistent_user_configs = persistent_treo_configs.get(user_id_str, {})
        current_treo_count = len(persistent_user_configs)

        # Kiểm tra xem đã treo target này chưa (dựa trên persistent config)
        if target_username in persistent_user_configs:
            logger.info(f"User {user_id} tried to /treo target @{target_username} which is already in persistent config.")
            msg = f"⚠️ Bạn đã đang treo cho <code>@{html.escape(target_username)}</code> rồi. Dùng <code>/dungtreo {target_username}</code> để dừng."
            await send_temporary_message(update, context, msg, duration=20)
            await delete_user_message(update, context, original_message_id)
            return

        # Kiểm tra giới hạn VIP
        if current_treo_count >= vip_limit:
             logger.warning(f"User {user_id} tried to /treo target @{target_username} but reached limit ({current_treo_count}/{vip_limit}).")
             limit_msg = (f"⚠️ Đã đạt giới hạn treo tối đa! ({current_treo_count}/{vip_limit} tài khoản).\n"
                          f"Dùng <code>/dungtreo &lt;username&gt;</code> để giải phóng slot hoặc nâng cấp gói VIP.")
             await send_temporary_message(update, context, limit_msg, duration=30)
             await delete_user_message(update, context, original_message_id)
             return

        # 4. Bắt đầu Task Treo Mới và Lưu Config
        try:
            app = context.application
            # Tạo task chạy nền
            task = app.create_task(
                run_treo_loop(user_id_str, target_username, context, chat_id),
                name=f"treo_{user_id_str}_{target_username}_in_{chat_id}" # Đặt tên cho task
            )
            # Thêm task vào dict runtime
            active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
            # Thêm vào dict persistent config
            persistent_treo_configs.setdefault(user_id_str, {})[target_username] = chat_id
            # Lưu dữ liệu ngay lập tức
            save_data()
            logger.info(f"Successfully created task '{task.get_name()}' and saved persistent config for user {user_id} -> @{target_username} in chat {chat_id}")

            # Thông báo thành công
            new_treo_count = len(persistent_treo_configs.get(user_id_str, {})) # Lấy số lượng mới nhất
            success_msg = (f"✅ <b>Bắt Đầu Treo Thành Công!</b>\n\n"
                           f"👤 Cho: {invoking_user_mention}\n🎯 Target: <code>@{html.escape(target_username)}</code>\n"
                           f"⏳ Tần suất: Mỗi {TREO_INTERVAL_SECONDS // 60} phút\n📊 Slot đã dùng: {new_treo_count}/{vip_limit}")
            await update.message.reply_html(success_msg)
            await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc sau khi báo thành công

        except Exception as e_start_task:
             logger.error(f"Failed to start treo task or save config for user {user_id} target @{target_username}: {e_start_task}", exc_info=True)
             await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi bắt đầu treo cho <code>@{html.escape(target_username)}</code>. Báo Admin.", duration=20)
             await delete_user_message(update, context, original_message_id)
             # Rollback nếu có lỗi xảy ra
             if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
                  del persistent_treo_configs[user_id_str][target_username]
                  if not persistent_treo_configs[user_id_str]: del persistent_treo_configs[user_id_str]
                  save_data() # Lưu lại trạng thái rollback
                  logger.info(f"Rolled back persistent config for {user_id_str} -> @{target_username} due to start error.")
             if 'task' in locals() and task and not task.done(): task.cancel() # Hủy task nếu đã tạo
             if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                 del active_treo_tasks[user_id_str][target_username]
                 if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                 logger.info(f"Rolled back active task entry for {user_id_str} -> @{target_username} due to start error.")
    else:
        # Trường hợp target_username không được gán (lỗi logic?)
        logger.error(f"/treo command for user {user_id}: target_username became None unexpectedly.")
        await send_temporary_message(update, context, "❌ Lỗi không xác định khi xử lý username.", duration=15)
        await delete_user_message(update, context, original_message_id)

# --- Lệnh /dungtreo (VIP - Cập nhật để dùng hàm stop_treo_task) ---
async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dừng việc treo tự động follow cho một user."""
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()

    # Parse Arguments
    args = context.args
    target_username_clean = None
    err_txt = None
    # Lấy danh sách target từ persistent config để hiển thị nếu không nhập arg
    persistent_user_configs = persistent_treo_configs.get(user_id_str, {})
    current_targets = list(persistent_user_configs.keys())

    if not args:
        if not current_targets:
            err_txt = ("⚠️ Chưa nhập username cần dừng treo.\n<b>Cú pháp:</b> <code>/dungtreo username</code>\n<i>(Hiện bạn không có tài khoản nào được cấu hình treo.)</i>")
        else:
            targets_str = ', '.join([f'<code>@{html.escape(t)}</code>' for t in current_targets])
            err_txt = (f"⚠️ Cần chỉ định username muốn dừng treo.\n<b>Cú pháp:</b> <code>/dungtreo username</code>\n"
                       f"<b>Đang treo:</b> {targets_str}")
    else:
        target_username_clean = args[0].strip().lstrip("@")
        if not target_username_clean: err_txt = "⚠️ Username không được để trống."
        # Không cần kiểm tra định dạng username ở đây, chỉ cần xem nó có trong danh sách không

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    # Dừng Task và Xóa Config bằng hàm helper
    if target_username_clean:
        logger.info(f"User {user_id} requesting to stop treo for @{target_username_clean}")
        # Gọi hàm stop_treo_task, nó sẽ lo cả runtime và persistent
        stopped = await stop_treo_task(user_id_str, target_username_clean, context, reason=f"User command /dungtreo by {user_id}")

        # Xóa lệnh /dungtreo gốc
        await delete_user_message(update, context, original_message_id)

        if stopped:
            # Thông báo thành công và cập nhật số slot
            new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
            vip_limit = get_vip_limit(user_id) # Lấy limit hiện tại
            is_still_vip = is_user_vip(user_id) # Kiểm tra lại trạng thái VIP
            limit_display = f"{vip_limit}" if is_still_vip else "N/A (VIP hết hạn)"
            await update.message.reply_html(f"✅ Đã dừng treo và xóa cấu hình cho <code>@{html.escape(target_username_clean)}</code>.\n(Slot đã dùng: {new_treo_count}/{limit_display})")
        else:
            # Thông báo nếu không tìm thấy target để dừng
            await send_temporary_message(update, context, f"⚠️ Không tìm thấy cấu hình treo nào cho <code>@{html.escape(target_username_clean)}</code> để dừng.", duration=20)

# --- Lệnh /listtreo (MỚI - THEO YÊU CẦU 3) ---
async def listtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị danh sách các tài khoản TikTok đang được treo bởi người dùng."""
    # Xử lý cả khi gọi từ lệnh hoặc nút bấm
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user or not chat_id: return

    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = None
    if update.message: # Lệnh được gõ
        original_message_id = update.message.message_id

    logger.info(f"User {user_id} requested /listtreo in chat {chat_id}")

    # Lấy danh sách target từ persistent_treo_configs của user này
    user_treo_configs = persistent_treo_configs.get(user_id_str, {})
    treo_targets = list(user_treo_configs.keys())

    # Xây dựng tin nhắn phản hồi
    reply_lines = [f"📊 <b>Danh Sách Tài Khoản Đang Treo</b>",
                   f"👤 Cho: {user.mention_html()}"]

    if not treo_targets:
        reply_lines.append("\nBạn hiện không treo tài khoản TikTok nào.")
    else:
        vip_limit = get_vip_limit(user_id)
        is_currently_vip = is_user_vip(user_id)
        limit_display = f"{vip_limit}" if is_currently_vip else "N/A (VIP hết hạn)"
        reply_lines.append(f"\n🔍 Số lượng: <b>{len(treo_targets)} / {limit_display}</b> tài khoản")
        # Sắp xếp danh sách theo alphabet cho dễ nhìn
        for target in sorted(treo_targets):
            reply_lines.append(f"  - <code>@{html.escape(target)}</code>")
        reply_lines.append("\nℹ️ Dùng <code>/dungtreo &lt;username&gt;</code> để dừng treo.")

    reply_text = "\n".join(reply_lines)

    try:
        # Nếu lệnh được gõ, xóa lệnh /listtreo gốc
        if original_message_id:
            await delete_user_message(update, context, original_message_id)
        # Gửi danh sách
        await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed to send /listtreo response to user {user_id} in chat {chat_id}: {e}")
        try:
            # Vẫn cố gắng xóa lệnh gốc ngay cả khi gửi lỗi
            if original_message_id:
                await delete_user_message(update, context, original_message_id)
            # Gửi thông báo lỗi tạm thời
            await send_temporary_message(update, context, "❌ Đã có lỗi xảy ra khi lấy danh sách treo.", duration=15, reply=False) # Không reply nếu lỗi
        except: pass # Bỏ qua nếu xóa/gửi lỗi tiếp

# --- Job Thống Kê Follow Tăng (Giữ nguyên logic, cải thiện logging) ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    """Job chạy định kỳ để thống kê và báo cáo user treo tăng follow."""
    global last_stats_report_time, treo_stats
    current_time = time.time()
    # Chỉ chạy nếu đã đến lúc hoặc lần đầu tiên
    if current_time < last_stats_report_time + TREO_STATS_INTERVAL_SECONDS * 0.95 and last_stats_report_time != 0:
        logger.debug(f"[Stats Job] Skipping report, not time yet. Next approx: {datetime.fromtimestamp(last_stats_report_time + TREO_STATS_INTERVAL_SECONDS)}")
        return

    logger.info(f"[Stats Job] Starting statistics report job. Last report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")
    target_chat_id_for_stats = ALLOWED_GROUP_ID

    # Kiểm tra xem có group ID để gửi không
    if not target_chat_id_for_stats:
        logger.info("[Stats Job] ALLOWED_GROUP_ID is not set. Stats report skipped.")
        # Reset stats để tránh tích lũy vô hạn nếu group ID bị unset sau này
        if treo_stats:
             logger.warning("[Stats Job] Clearing treo_stats because ALLOWED_GROUP_ID is not set.")
             treo_stats.clear()
             save_data() # Lưu trạng thái đã clear
        return

    stats_snapshot = {}
    if treo_stats:
        try:
            # Tạo deep copy của stats để xử lý, tránh race condition nếu task treo cập nhật giữa chừng
            stats_snapshot = json.loads(json.dumps(treo_stats))
        except Exception as e_copy:
             logger.error(f"[Stats Job] Error creating stats snapshot: {e_copy}. Aborting stats run."); return

    # Xóa stats hiện tại và cập nhật thời gian báo cáo NGAY LẬP TỨC
    treo_stats.clear()
    last_stats_report_time = current_time
    save_data() # Lưu trạng thái mới (stats rỗng, time cập nhật)
    logger.info(f"[Stats Job] Cleared current stats and updated last report time to {datetime.fromtimestamp(last_stats_report_time).isoformat()}. Processing snapshot with {len(stats_snapshot)} users.")

    if not stats_snapshot:
        logger.info("[Stats Job] No stats data found in snapshot. Skipping report content generation.")
        # Có thể gửi tin nhắn "Không có dữ liệu" nếu muốn
        # try: await context.bot.send_message(chat_id=target_chat_id_for_stats, text="📊 Không có dữ liệu tăng follow nào trong 24 giờ qua.", disable_notification=True)
        # except: pass
        return

    # Xử lý snapshot để lấy top gainers
    top_gainers = [] # List of tuples: (gain, user_id_str, target_username)
    total_gain_all = 0
    for user_id_str, targets in stats_snapshot.items():
        if isinstance(targets, dict):
            for target_username, gain in targets.items():
                # Chỉ xử lý gain là số nguyên dương
                if isinstance(gain, int) and gain > 0:
                    top_gainers.append((gain, str(user_id_str), str(target_username)))
                    total_gain_all += gain
                elif gain > 0: # Log nếu gain dương nhưng không phải int
                     logger.warning(f"[Stats Job] Invalid gain type ({type(gain)}) for {user_id_str}->{target_username}. Skipping.")
        else: logger.warning(f"[Stats Job] Invalid target structure for user {user_id_str} in snapshot. Skipping.")

    if not top_gainers:
        logger.info("[Stats Job] No positive gains found after processing snapshot. Skipping report generation.")
        # Có thể gửi tin nhắn "Không có dữ liệu" nếu muốn
        return

    # Sắp xếp theo gain giảm dần
    top_gainers.sort(key=lambda x: x[0], reverse=True)

    # Tạo nội dung báo cáo
    report_lines = [f"📊 <b>Thống Kê Tăng Follow (24 Giờ Qua)</b> 📊",
                    f"<i>(Tổng cộng: <b>{total_gain_all:,}</b> follow được tăng bởi các tài khoản đang treo)</i>", # Format số với dấu phẩy
                    "\n🏆 <b>Top Tài Khoản Treo Hiệu Quả Nhất:</b>"]

    num_top_to_show = 10 # Số lượng hiển thị trong top
    displayed_count = 0
    user_mentions_cache = {} # Cache mention để tránh gọi get_chat nhiều lần

    for gain, user_id_str, target_username in top_gainers[:num_top_to_show]:
        user_mention = user_mentions_cache.get(user_id_str)
        if not user_mention:
            try:
                # Cố gắng lấy mention của người treo
                user_info = await context.bot.get_chat(int(user_id_str))
                m = user_info.mention_html()
                # Fallback nếu mention_html None
                user_mention = m if m else f"<a href='tg://user?id={user_id_str}'>User {user_id_str}</a>"
            except Exception as e_get_chat:
                logger.warning(f"[Stats Job] Failed to get mention for user {user_id_str}: {e_get_chat}")
                user_mention = f"User <code>{user_id_str}</code>" # Fallback về ID
            user_mentions_cache[user_id_str] = user_mention # Lưu vào cache

        # Format dòng top
        report_lines.append(f"  🏅 <b>+{gain:,} follow</b> cho <code>@{html.escape(target_username)}</code> (Treo bởi: {user_mention})")
        displayed_count += 1

    if not displayed_count:
        report_lines.append("  <i>Không có dữ liệu tăng follow đáng kể trong kỳ này.</i>")

    report_lines.append(f"\n🕒 <i>Cập nhật tự động sau mỗi 24 giờ.</i>")

    report_text = "\n".join(report_lines)

    # Gửi báo cáo vào group
    try:
        await context.bot.send_message(chat_id=target_chat_id_for_stats, text=report_text,
                                       parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True) # Gửi yên lặng
        logger.info(f"[Stats Job] Successfully sent statistics report to group {target_chat_id_for_stats}.")
    except Exception as e:
        logger.error(f"[Stats Job] Failed to send statistics report to group {target_chat_id_for_stats}: {e}", exc_info=True)

    logger.info("[Stats Job] Statistics report job finished.")


# --- Hàm helper bất đồng bộ để dừng task khi tắt bot ---
async def shutdown_async_tasks(tasks_to_cancel: list[asyncio.Task]):
    """Helper async function to cancel and wait for tasks during shutdown."""
    if not tasks_to_cancel:
        logger.info("No active treo tasks found to cancel during shutdown.")
        return

    logger.info(f"Attempting to gracefully cancel {len(tasks_to_cancel)} active treo tasks...")
    # Hủy tất cả các task
    for task in tasks_to_cancel:
        if task and not task.done():
            task.cancel()

    # Chờ các task hoàn thành (hoặc bị hủy) với timeout
    results = await asyncio.gather(*[asyncio.wait_for(task, timeout=2.0) for task in tasks_to_cancel], return_exceptions=True)
    logger.info("Finished waiting for treo task cancellations during shutdown.")

    cancelled_count, errors_count, finished_count = 0, 0, 0
    for i, result in enumerate(results):
        task = tasks_to_cancel[i]
        task_name = f"Task_{i}" # Tên mặc định
        try:
             if task: task_name = task.get_name() or task_name # Lấy tên task nếu có
        except Exception: pass # Bỏ qua nếu không lấy được tên

        if isinstance(result, asyncio.CancelledError):
            cancelled_count += 1
            logger.info(f"Task '{task_name}' confirmed cancelled during shutdown.")
        elif isinstance(result, asyncio.TimeoutError):
            errors_count += 1
            logger.warning(f"Task '{task_name}' timed out during shutdown cancellation.")
        elif isinstance(result, Exception):
            errors_count += 1
            logger.error(f"Error occurred in task '{task_name}' during shutdown processing: {result}", exc_info=False) # Log lỗi, không cần trace đầy đủ
        else:
            finished_count += 1
            logger.debug(f"Task '{task_name}' finished normally during shutdown.") # Ít khi xảy ra với loop vô hạn

    logger.info(f"Shutdown task summary: {cancelled_count} cancelled, {errors_count} errors/timeouts, {finished_count} finished normally.")

# --- Main Function (Cập nhật để khôi phục task treo) ---
def main() -> None:
    """Khởi động và chạy bot."""
    start_time = time.time()
    print("--- Bot DinoTool Starting ---"); print(f"Timestamp: {datetime.now().isoformat()}")
    print("\n--- Configuration Summary ---")
    print(f"Bot Token: {'Loaded' if BOT_TOKEN else 'Missing!'}"); print(f"Primary Group ID (Bills/Stats): {ALLOWED_GROUP_ID}" if ALLOWED_GROUP_ID else "ALLOWED_GROUP_ID: Not Set (Bills/Stats Disabled)")
    print(f"Bill Forward Target ID: {BILL_FORWARD_TARGET_ID}"); print(f"Admin User ID: {ADMIN_USER_ID}")
    print(f"Link Shortener Key: {'Loaded' if LINK_SHORTENER_API_KEY else 'Missing!'}"); print(f"Tim API Key: {'Loaded' if API_KEY else 'Missing!'}")
    print(f"Follow API URL: {FOLLOW_API_URL_BASE}"); print(f"Data File: {DATA_FILE}")
    print(f"Key Expiry: {KEY_EXPIRY_SECONDS / 3600:.1f}h | Activation: {ACTIVATION_DURATION_SECONDS / 3600:.1f}h")
    print(f"Cooldowns: Tim/Fl={TIM_FL_COOLDOWN_SECONDS / 60:.1f}m | GetKey={GETKEY_COOLDOWN_SECONDS / 60:.1f}m")
    print(f"Treo: Interval={TREO_INTERVAL_SECONDS / 60:.1f}m | Fail Delete Delay={TREO_FAILURE_MSG_DELETE_DELAY}s | Stats Interval={TREO_STATS_INTERVAL_SECONDS / 3600:.1f}h")
    print(f"VIP Prices: {VIP_PRICES}"); print(f"Payment: {BANK_NAME} - {BANK_ACCOUNT} - {ACCOUNT_NAME}"); print("-" * 30)

    print("Loading persistent data...")
    load_data()
    print(f"Load complete. Keys: {len(valid_keys)}, Activated: {len(activated_users)}, VIPs: {len(vip_users)}")
    print(f"Cooldowns: Tim={len(user_tim_cooldown)}, Fl={sum(len(v) for v in user_fl_cooldown.values())} targets, GetKey={len(user_getkey_cooldown)}")
    # Đếm số lượng target treo đã lưu
    persistent_treo_count = sum(len(targets) for targets in persistent_treo_configs.values())
    print(f"Persistent Treo Configs Loaded: {persistent_treo_count} targets for {len(persistent_treo_configs)} users")
    print(f"Initial Treo Stats Users: {len(treo_stats)}, Last Stats Report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")

    # Cấu hình Application
    application = (Application.builder().token(BOT_TOKEN).job_queue(JobQueue())
                   .pool_timeout(120).connect_timeout(60).read_timeout(90).write_timeout(90)
                   .get_updates_pool_timeout(120).http_version("1.1").build())

    # Lên lịch các job định kỳ
    jq = application.job_queue
    if jq:
        jq.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_expired_data_job")
        logger.info(f"Scheduled cleanup job every {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes.")
        if ALLOWED_GROUP_ID:
            # Chạy job thống kê lần đầu sau 5 phút, sau đó mỗi 24h
            jq.run_repeating(report_treo_stats, interval=TREO_STATS_INTERVAL_SECONDS, first=300, name="report_treo_stats_job")
            logger.info(f"Scheduled statistics report job every {TREO_STATS_INTERVAL_SECONDS / 3600:.1f} hours (first run in 5 min).")
        else:
             logger.info("Statistics report job skipped (ALLOWED_GROUP_ID not set).")
    else: logger.error("JobQueue is not available. Scheduled jobs will not run.")

    # Register Handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("lenh", lenh_command))
    application.add_handler(CommandHandler("getkey", getkey_command))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command))
    application.add_handler(CommandHandler("tim", tim_command))
    application.add_handler(CommandHandler("fl", fl_command))
    application.add_handler(CommandHandler("muatt", muatt_command))
    application.add_handler(CommandHandler("treo", treo_command))
    application.add_handler(CommandHandler("dungtreo", dungtreo_command))
    application.add_handler(CommandHandler("listtreo", listtreo_command)) # <-- Đã thêm
    application.add_handler(CommandHandler("addtt", addtt_command))

    # <<<***>>> THÊM CALLBACK QUERY HANDLER CHO MENU <<<***>>>
    # Pattern="^menu_" để bắt tất cả các nút menu và các nút khác như prompt_send_bill
    application.add_handler(CallbackQueryHandler(button_callback_handler, pattern="^(menu_|prompt_send_bill_)"))
    logger.info("Registered callback query handler for inline menus and bill prompt.")

    # Message handler cho ảnh bill (Ưu tiên cao hơn các handler khác để bắt bill trước)
    # Chạy cho cả private và group, lọc user trong hàm xử lý
    photo_bill_filter = (filters.PHOTO | filters.Document.IMAGE) & (~filters.COMMAND) & filters.UpdateType.MESSAGE
    # Đặt group=-1 để chạy trước các handler command/message mặc định (group=0) và CallbackQueryHandler (mặc định group=0)
    application.add_handler(MessageHandler(photo_bill_filter, handle_photo_bill), group=-1) # <-- group=-1
    logger.info("Registered photo/bill handler (priority -1) for pending users.")

    # Khởi động lại các task treo đã lưu <-- LOGIC MỚI QUAN TRỌNG
    print("\nRestarting persistent treo tasks...")
    restored_count = 0
    users_to_cleanup = [] # Danh sách user không còn VIP để xóa config
    tasks_to_create = [] # List of tuples: (user_id_str, target_username, chat_id_int)

    if persistent_treo_configs:
        # Lặp qua bản sao của keys để tránh lỗi thay đổi dict khi lặp
        for user_id_str in list(persistent_treo_configs.keys()):
            try:
                user_id_int = int(user_id_str)
                # Kiểm tra VIP trước khi khôi phục
                if not is_user_vip(user_id_int):
                    logger.warning(f"User {user_id_str} from persistent config is no longer VIP. Scheduling config cleanup.")
                    users_to_cleanup.append(user_id_str)
                    continue # Bỏ qua user này

                # Kiểm tra giới hạn VIP
                vip_limit = get_vip_limit(user_id_int)
                targets_for_user = persistent_treo_configs.get(user_id_str, {})
                current_user_restored_count = 0

                # Lặp qua bản sao của target keys
                for target_username in list(targets_for_user.keys()):
                    if current_user_restored_count >= vip_limit:
                         logger.warning(f"User {user_id_str} reached VIP limit ({vip_limit}) during restore. Skipping persistent target @{target_username} and potentially others.")
                         # Xóa config dư thừa khỏi persistent data
                         if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
                              del persistent_treo_configs[user_id_str][target_username]
                              # Không cần save_data() ở đây, sẽ save sau khi dọn dẹp xong users_to_cleanup
                         continue # Bỏ qua các target còn lại của user này nếu đã đủ limit

                    # Kiểm tra xem task đã chạy chưa (trường hợp restart cực nhanh)
                    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                        logger.info(f"Task for {user_id_str} -> @{target_username} seems already active (runtime). Skipping restore.")
                        current_user_restored_count += 1 # Vẫn tính vào limit
                        continue

                    chat_id_int = targets_for_user[target_username] # Lấy chat_id đã lưu
                    logger.info(f"Scheduling restore for treo task: user {user_id_str} -> @{target_username} in chat {chat_id_int}")
                    # Thêm vào danh sách để tạo task sau khi application đã sẵn sàng
                    tasks_to_create.append((user_id_str, target_username, chat_id_int))
                    current_user_restored_count += 1

            except ValueError:
                logger.error(f"Invalid user_id '{user_id_str}' found in persistent_treo_configs. Scheduling cleanup.")
                users_to_cleanup.append(user_id_str)
            except Exception as e_outer_restore:
                logger.error(f"Unexpected error processing persistent treo config for user {user_id_str}: {e_outer_restore}", exc_info=True)

    # Dọn dẹp config của user không còn VIP hoặc ID lỗi
    if users_to_cleanup:
        logger.info(f"Cleaning up persistent treo configs for {len(users_to_cleanup)} non-VIP or invalid users...")
        cleaned_count = 0
        for user_id_str_clean in users_to_cleanup:
            if user_id_str_clean in persistent_treo_configs:
                del persistent_treo_configs[user_id_str_clean]
                cleaned_count += 1
        if cleaned_count > 0:
            save_data() # Lưu lại sau khi đã dọn dẹp
            logger.info(f"Removed persistent configs for {cleaned_count} users.")

    # Tạo các task treo đã lên lịch
    if tasks_to_create:
        logger.info(f"Creating {len(tasks_to_create)} restored treo tasks...")
        for user_id_str, target_username, chat_id_int in tasks_to_create:
            try:
                # Tạo context giả lập đủ để chạy task (chỉ cần application)
                # Context thực sự sẽ được tạo trong run_treo_loop khi cần gửi tin nhắn
                default_context = ContextTypes.DEFAULT_TYPE(application=application, chat_id=None, user_id=None) # chat_id và user_id sẽ được truyền vào loop
                task = application.create_task(
                    run_treo_loop(user_id_str, target_username, default_context, chat_id_int), # Truyền chat_id vào loop
                    name=f"treo_{user_id_str}_{target_username}_in_{chat_id_int}_restored"
                )
                active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
                restored_count += 1
            except Exception as e_create:
                logger.error(f"Failed to create restored task for {user_id_str} -> @{target_username}: {e_create}", exc_info=True)
                # Cố gắng xóa config persistent nếu không tạo được task
                if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
                    del persistent_treo_configs[user_id_str][target_username]
                    if not persistent_treo_configs[user_id_str]: del persistent_treo_configs[user_id_str]
                    save_data()
                    logger.warning(f"Removed persistent config for {user_id_str} -> @{target_username} due to task creation failure.")

    print(f"Successfully restored and started {restored_count} treo tasks."); print("-" * 30)

    print("\nBot initialization complete. Starting polling...")
    logger.info("Bot initialization complete. Starting polling...")
    run_duration = time.time() - start_time; print(f"(Initialization took {run_duration:.2f} seconds)")

    # Chạy bot
    try:
        # drop_pending_updates=True để bỏ qua các update xảy ra khi bot offline
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Stopping bot gracefully..."); logger.info("KeyboardInterrupt detected. Stopping bot...")
    except Exception as e:
        print(f"\nCRITICAL ERROR: Bot stopped due to an unhandled exception: {e}"); logger.critical(f"CRITICAL ERROR: Bot stopped due to unhandled exception: {e}", exc_info=True)
    finally:
        print("\nInitiating shutdown sequence..."); logger.info("Initiating shutdown sequence...")
        # Thu thập các task đang chạy từ active_treo_tasks
        tasks_to_stop_on_shutdown = []
        if active_treo_tasks:
            logger.info("Collecting active runtime treo tasks for shutdown...")
            # Lặp qua bản sao để tránh lỗi thay đổi dict khi lặp
            for targets in list(active_treo_tasks.values()):
                for task in list(targets.values()):
                    # Chỉ thêm task đang chạy và chưa hoàn thành
                    if task and not task.done():
                        tasks_to_stop_on_shutdown.append(task)

        # Hủy các task đang chạy
        if tasks_to_stop_on_shutdown:
            print(f"Found {len(tasks_to_stop_on_shutdown)} active runtime treo tasks. Attempting cancellation...")
            try:
                 # Chạy hàm helper để hủy và chờ
                 # Sử dụng run_until_complete nếu loop còn chạy, nếu không thì chỉ cancel
                 loop = asyncio.get_event_loop()
                 if loop.is_running():
                      loop.run_until_complete(shutdown_async_tasks(tasks_to_stop_on_shutdown))
                 else:
                      logger.warning("Event loop not running during shutdown. Attempting direct cancellation.")
                      for task in tasks_to_stop_on_shutdown: task.cancel()
            except RuntimeError as e_runtime:
                 logger.error(f"RuntimeError during async task shutdown: {e_runtime}. Attempting direct cancellation.")
                 for task in tasks_to_stop_on_shutdown: task.cancel()
            except Exception as e_shutdown:
                 logger.error(f"Error during async task shutdown: {e_shutdown}", exc_info=True)
                 # Vẫn cố gắng hủy trực tiếp nếu gather lỗi
                 for task in tasks_to_stop_on_shutdown: task.cancel()
        else:
            print("No active runtime treo tasks found at shutdown.")

        # Lưu dữ liệu lần cuối
        print("Attempting final data save..."); logger.info("Attempting final data save...")
        save_data()
        print("Final data save attempt complete.")
        print("Bot has stopped."); logger.info("Bot has stopped."); print(f"Shutdown timestamp: {datetime.now().isoformat()}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Ghi lỗi nghiêm trọng nếu hàm main không thể chạy
        print(f"\nFATAL ERROR: Could not execute main function: {e}")
        logger.critical(f"FATAL ERROR preventing main execution: {e}", exc_info=True)
        # Cố gắng ghi lỗi vào file riêng
        try:
            with open("fatal_error.log", "a", encoding='utf-8') as f:
                import traceback
                f.write(f"\n--- {datetime.now().isoformat()} ---\n")
                f.write(f"FATAL ERROR: {e}\n")
                traceback.print_exc(file=f)
                f.write("-" * 30 + "\n")
        except Exception as e_log: print(f"Additionally, failed to write fatal error to log file: {e_log}")

