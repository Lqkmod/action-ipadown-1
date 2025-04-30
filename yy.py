
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
    ApplicationHandlerStop,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError

# --- Cấu hình ---
BOT_TOKEN = "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" # <--- TOKEN CỦA BẠN
API_KEY = "khangdino99" # <--- API KEY TIM (VẪN CẦN CHO LỆNH /tim)
ADMIN_USER_ID = 7193749511 # <<< --- ID TELEGRAM CỦA ADMIN (Người quản lý bot)

# ID của bot @khangtaixiu_bot để nhận bill
BILL_FORWARD_TARGET_ID = 7193749511 # <<< --- THAY THẾ BẰNG ID SỐ CỦA @khangtaixiu_bot

# ID Nhóm chính để nhận bill và thống kê. Nếu không muốn giới hạn, đặt thành None.
ALLOWED_GROUP_ID = -1002191171631 # <--- ID NHÓM CHÍNH CỦA BẠN HOẶC None
# !!! QUAN TRỌNG: Thêm link mời nhóm của bạn vào đây để nút menu hoạt động !!!
GROUP_LINK = "YOUR_GROUP_INVITE_LINK" # <<<--- THAY THẾ BẰNG LINK NHÓM CỦA BẠN

LINK_SHORTENER_API_KEY = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" # Token Yeumoney
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # Link đích chứa key
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # API Yeumoney

# --- Thời gian ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 phút
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 phút
KEY_EXPIRY_SECONDS = 6 * 3600   # 6 giờ (Key chưa nhập)
ACTIVATION_DURATION_SECONDS = 6 * 3600 # 6 giờ (Sau khi nhập key)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 giờ
TREO_INTERVAL_SECONDS = 600 # <<< --- THAY ĐỔI: 10 phút (Khoảng cách giữa các lần gọi API /treo)
TREO_FAILURE_MSG_DELETE_DELAY = 10 # 10 giây (Thời gian xoá tin nhắn treo thất bại)
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
active_treo_tasks = {} # {user_id_str: {target_username: asyncio.Task}} - Lưu các task /treo đang chạy (RUNTIME)
persistent_treo_configs = {} # {user_id_str: {target_username: chat_id}} - Lưu để khôi phục sau restart (PERSISTENT)

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
if not BILL_FORWARD_TARGET_ID or not isinstance(BILL_FORWARD_TARGET_ID, int) or BILL_FORWARD_TARGET_ID == 123456789:
    logger.critical("!!! BILL_FORWARD_TARGET_ID is missing, invalid, or still the placeholder! Find the NUMERIC ID of @khangtaixiu_bot using @userinfobot !!!"); exit(1)
else: logger.info(f"Bill forwarding target set to: {BILL_FORWARD_TARGET_ID}")

if ALLOWED_GROUP_ID:
     logger.info(f"Bill forwarding source and Stats reporting restricted to Group ID: {ALLOWED_GROUP_ID}")
     if not GROUP_LINK or GROUP_LINK == "YOUR_GROUP_INVITE_LINK":
         logger.warning("!!! GROUP_LINK is not set or is placeholder. 'Nhóm Chính' button in menu might not work.")
     else:
         logger.info(f"Group Link for menu set to: {GROUP_LINK}")
else:
     logger.warning("!!! ALLOWED_GROUP_ID is not set. Bill forwarding and Stats reporting will be disabled. 'Nhóm Chính' button in menu will be hidden.")

if not LINK_SHORTENER_API_KEY: logger.critical("!!! LINK_SHORTENER_API_KEY is missing !!!"); exit(1)
if not API_KEY: logger.warning("!!! API_KEY (for /tim) is missing. /tim command might fail. !!!")
if not ADMIN_USER_ID: logger.critical("!!! ADMIN_USER_ID is missing !!!"); exit(1)

# --- Hàm lưu/tải dữ liệu (Cập nhật để xử lý persistent_treo_configs) ---
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
                # Sử dụng defaultdict(dict) để đảm bảo user_fl_cooldown[uid] luôn là dict
                user_fl_cooldown = defaultdict(dict)
                loaded_fl_cooldown = all_cooldowns.get("fl", {})
                if isinstance(loaded_fl_cooldown, dict):
                    user_fl_cooldown.update(loaded_fl_cooldown)

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
                if isinstance(loaded_persistent_treo, dict): # Ensure loaded data is a dict
                    for uid_str, configs in loaded_persistent_treo.items():
                        user_id_key = str(uid_str) # Ensure outer key is string
                        persistent_treo_configs[user_id_key] = {}
                        if isinstance(configs, dict): # Check inner type
                            for target, chatid in configs.items():
                                try:
                                    # Ensure target is string, chatid is int
                                    persistent_treo_configs[user_id_key][str(target)] = int(chatid)
                                except (ValueError, TypeError):
                                    logger.warning(f"Skipping invalid persistent treo config entry: user {user_id_key}, target {target}, chatid {chatid}")
                        else:
                             logger.warning(f"Invalid config type for user {user_id_key} in persistent_treo_configs: {type(configs)}. Skipping.")
                else:
                    logger.warning(f"persistent_treo_configs in data file is not a dict: {type(loaded_persistent_treo)}. Initializing empty.")


                logger.info(f"Data loaded successfully from {DATA_FILE}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            # Đặt giá trị mặc định là dict rỗng hoặc defaultdict
            valid_keys, activated_users, vip_users = {}, {}, {}
            user_tim_cooldown, user_getkey_cooldown = {}, {}
            user_fl_cooldown = defaultdict(dict) # Ensure it's a defaultdict
            treo_stats = defaultdict(lambda: defaultdict(int))
            last_stats_report_time = 0
            persistent_treo_configs = {} # <-- Khởi tạo rỗng
    except (json.JSONDecodeError, TypeError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Using empty data structures.", exc_info=True)
        # Reset all global data structures on error
        valid_keys, activated_users, vip_users = {}, {}, {}
        user_tim_cooldown, user_getkey_cooldown = {}, {}
        user_fl_cooldown = defaultdict(dict) # Ensure it's a defaultdict
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

        send_params = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': parse_mode,
            'disable_web_page_preview': True
        }
        if reply_to_msg_id:
            send_params['reply_to_message_id'] = reply_to_msg_id

        try:
            sent_message = await context.bot.send_message(**send_params)
        except BadRequest as e:
            if "reply message not found" in str(e).lower() and reply_to_msg_id:
                 logger.debug(f"Reply message {reply_to_msg_id} not found for temporary message. Sending without reply.")
                 del send_params['reply_to_message_id'] # Xóa key reply
                 sent_message = await context.bot.send_message(**send_params)
            else:
                 raise # Ném lại lỗi BadRequest khác

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

# --- Cập nhật hàm stop_treo_task và thêm stop_all_treo_tasks_for_user (QUAN TRỌNG cho persistent) ---
async def stop_treo_task(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown") -> bool:
    """Dừng một task treo cụ thể (runtime VÀ persistent). Trả về True nếu dừng/xóa thành công, False nếu không tìm thấy."""
    global persistent_treo_configs, active_treo_tasks # Cần truy cập để sửa đổi
    task = None
    was_active_runtime = False
    removed_persistent = False
    data_saved = False

    user_id_str = str(user_id_str) # Đảm bảo là string
    target_username = str(target_username) # Đảm bảo là string

    # 1. Dừng task đang chạy (runtime)
    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
        task = active_treo_tasks[user_id_str].get(target_username)
        if task and isinstance(task, asyncio.Task) and not task.done():
            was_active_runtime = True
            task_name = task.get_name() if hasattr(task, 'get_name') else f"task_{user_id_str}_{target_username}"
            logger.info(f"[Treo Task Stop] Attempting to cancel RUNTIME task '{task_name}'. Reason: {reason}")
            task.cancel()
            try:
                # Chờ task bị hủy trong thời gian ngắn để cleanup
                await asyncio.wait_for(task, timeout=1.0)
            except asyncio.CancelledError:
                logger.info(f"[Treo Task Stop] Runtime Task '{task_name}' confirmed cancelled.")
            except asyncio.TimeoutError:
                 logger.warning(f"[Treo Task Stop] Timeout waiting for cancelled runtime task '{task_name}'.")
            except Exception as e:
                 logger.error(f"[Treo Task Stop] Error awaiting cancelled runtime task '{task_name}': {e}")
        # Luôn xóa khỏi runtime dict nếu key tồn tại
        if target_username in active_treo_tasks[user_id_str]:
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
    """Dừng tất cả các task treo của một user (runtime và persistent)."""
    stopped_count = 0
    user_id_str = str(user_id_str) # Đảm bảo là string

    # Lấy danh sách target từ persistent config để đảm bảo xóa hết config
    # Không cần lấy từ runtime vì stop_treo_task sẽ kiểm tra cả hai
    targets_in_persistent = list(persistent_treo_configs.get(user_id_str, {}).keys())

    if not targets_in_persistent:
        logger.info(f"No persistent treo configs found for user {user_id_str} to stop.")
        # Vẫn kiểm tra runtimeเผื่อ trường hợp task chạy mà config chưa kịp lưu (hiếm)
        if user_id_str in active_treo_tasks:
            targets_in_runtime = list(active_treo_tasks.get(user_id_str, {}).keys())
            if targets_in_runtime:
                logger.warning(f"Found runtime tasks for user {user_id_str} without persistent config during stop_all. Targets: {targets_in_runtime}. Attempting stop.")
                targets_in_persistent = targets_in_runtime # Dùng list runtime để stop
            else:
                return # Không có gì để dừng

    logger.info(f"Stopping all {len(targets_in_persistent)} potential treo tasks/configs for user {user_id_str}. Reason: {reason}")
    # Lặp qua bản sao của list target
    for target_username in list(targets_in_persistent):
        if await stop_treo_task(user_id_str, target_username, context, reason):
            stopped_count += 1
        else:
             logger.warning(f"stop_treo_task reported failure for {user_id_str} -> @{target_username} during stop_all, but it should have existed in persistent list.")

    logger.info(f"Finished stopping tasks/configs for user {user_id_str}. Stopped/Removed: {stopped_count}/{len(targets_in_persistent)} target(s).")

# --- Job Cleanup (Cập nhật để dừng task VIP hết hạn) ---
async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn (keys, activations, VIPs) VÀ dừng task treo của VIP hết hạn."""
    global valid_keys, activated_users, vip_users, persistent_treo_configs # persistent_treo_configs không cần check ở đây, sẽ xử lý qua stop_all_treo_tasks_for_user
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate_key = []
    users_to_deactivate_vip = []
    vip_users_to_stop_tasks = [] # User ID (string) của VIP hết hạn cần dừng task
    basic_data_changed = False # Flag để biết có cần save_data() không (không tính save từ stop_treo)

    logger.info("[Cleanup] Starting cleanup job...")

    # Check expired keys (chưa sử dụng)
    for key, data in list(valid_keys.items()):
        try:
            if data.get("used_by") is None and current_time > float(data.get("expiry_time", 0)):
                keys_to_remove.append(key)
        except (ValueError, TypeError): keys_to_remove.append(key)

    # Check expired key activations
    for user_id_str, expiry_timestamp in list(activated_users.items()):
        try:
            if current_time > float(expiry_timestamp): users_to_deactivate_key.append(user_id_str)
        except (ValueError, TypeError): users_to_deactivate_key.append(user_id_str)

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
         unique_users_to_stop = set(vip_users_to_stop_tasks) # Đảm bảo mỗi user chỉ dừng 1 lần
         logger.info(f"[Cleanup] Scheduling stop for tasks of {len(unique_users_to_stop)} expired/invalid VIP users.")
         app = context.application
         for user_id_str_stop in unique_users_to_stop:
             # Chạy bất đồng bộ để không chặn job cleanup chính
             # stop_all_treo_tasks_for_user sẽ lo cả runtime và persistent removal + save_data
             app.create_task(
                 stop_all_treo_tasks_for_user(user_id_str_stop, context, reason="VIP Expired/Removed during Cleanup"),
                 name=f"cleanup_stop_tasks_{user_id_str_stop}"
             )
             # Lưu ý: stop_all_treo_tasks_for_user tự gọi save_data() khi xóa persistent config

    # Chỉ lưu nếu dữ liệu cơ bản (keys/activation/vip list) thay đổi.
    # Việc dừng task đã tự lưu trong stop_all_treo_tasks_for_user -> stop_treo_task.
    if basic_data_changed:
        logger.info("[Cleanup] Basic data (keys/activation/vip list) changed, saving...")
        save_data()
    else:
        logger.info("[Cleanup] No basic data changes found. Treo task stopping handles its own saving if necessary.")

    logger.info("[Cleanup] Cleanup job finished.")

# --- Kiểm tra VIP/Key ---
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
        try:
            # Mặc định là 0 nếu limit không có hoặc không phải số
            limit = int(vip_users.get(user_id_str, {}).get("limit", 0))
            return limit
        except (ValueError, TypeError):
            return 0
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

# --- Logic API Follow ---
async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """Gọi API follow và trả về kết quả."""
    api_params = {"user": target_username, "userid": user_id_str, "tokenbot": bot_token}
    log_api_params = api_params.copy()
    log_api_params["tokenbot"] = f"...{bot_token[-6:]}" if len(bot_token) > 6 else "***"
    logger.info(f"[API Call] User {user_id_str} calling Follow API for @{target_username} with params: {log_api_params}")
    result = {"success": False, "message": "Lỗi không xác định khi gọi API.", "data": None}
    try:
        async with httpx.AsyncClient(verify=False, timeout=90.0) as client: # Tăng timeout lên 90s
            resp = await client.get(FOLLOW_API_URL_BASE, params=api_params, headers={'User-Agent': 'TG Bot FL Caller'})
            content_type = resp.headers.get("content-type", "").lower()
            response_text_full = ""
            try:
                # Thử các encoding phổ biến
                encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1']
                decoded = False
                resp_bytes = await resp.aread()
                for enc in encodings_to_try:
                    try:
                        response_text_full = resp_bytes.decode(enc, errors='strict')
                        logger.debug(f"[API Call @{target_username}] Decoded response with {enc}. Length: {len(response_text_full)}")
                        decoded = True
                        break
                    except UnicodeDecodeError:
                        logger.debug(f"[API Call @{target_username}] Failed to decode with {enc}")
                        continue
                if not decoded:
                    response_text_full = resp_bytes.decode('utf-8', errors='replace') # Fallback
                    logger.warning(f"[API Call @{target_username}] Could not decode response with common encodings, using replace. Length: {len(response_text_full)}")
            except Exception as e_read_outer:
                 logger.error(f"[API Call @{target_username}] Error reading/decoding response body: {e_read_outer}")
                 response_text_full = "[Error reading response body]"

            response_text_for_debug = response_text_full[:1000] # Giới hạn độ dài log
            logger.debug(f"[API Call @{target_username}] Status: {resp.status_code}, Content-Type: {content_type}")
            if len(response_text_full) > 1000: logger.debug(f"[API Call @{target_username}] Response snippet: {response_text_for_debug}...")
            else: logger.debug(f"[API Call @{target_username}] Response text: {response_text_full}")

            if resp.status_code == 200:
                if "application/json" in content_type:
                    try:
                        data = json.loads(response_text_full)
                        logger.debug(f"[API Call @{target_username}] JSON Data: {data}")
                        result["data"] = data
                        api_status = data.get("status")
                        api_message = data.get("message", None) # Giữ None nếu không có

                        # Linh hoạt hơn khi check status
                        if isinstance(api_status, bool): result["success"] = api_status
                        elif isinstance(api_status, str): result["success"] = api_status.lower() in ['true', 'success', 'ok', '200'] # Thêm '200'
                        elif isinstance(api_status, int): result["success"] = api_status == 200 # Thêm check số
                        else: result["success"] = False

                        # Xử lý message
                        if result["success"] and api_message is None: api_message = "Follow thành công."
                        elif not result["success"] and api_message is None: api_message = f"Follow thất bại (API status={api_status})."
                        elif api_message is None: api_message = "Không có thông báo từ API."
                        result["message"] = str(api_message)

                    except json.JSONDecodeError:
                        logger.error(f"[API Call @{target_username}] Response 200 OK (JSON type) but not valid JSON. Text: {response_text_for_debug}...")
                        error_match = re.search(r'<pre>(.*?)</pre>', response_text_full, re.DOTALL | re.IGNORECASE)
                        result["message"] = f"Lỗi API (HTML?): {html.escape(error_match.group(1).strip())}" if error_match else "Lỗi: API trả về dữ liệu JSON không hợp lệ."
                        result["success"] = False
                    except Exception as e_proc:
                        logger.error(f"[API Call @{target_username}] Error processing API JSON data: {e_proc}", exc_info=True)
                        result["message"] = "Lỗi xử lý dữ liệu JSON từ API."
                        result["success"] = False
                else:
                     logger.warning(f"[API Call @{target_username}] Response 200 OK but wrong Content-Type: {content_type}. Text: {response_text_for_debug}...")
                     # Heuristic: Nếu text ngắn và không chứa chữ "lỗi" / "error", coi như thành công
                     if len(response_text_full) < 200 and "lỗi" not in response_text_full.lower() and "error" not in response_text_full.lower() and "fail" not in response_text_full.lower():
                         result["success"] = True
                         result["message"] = "Follow thành công (phản hồi không chuẩn JSON)."
                     else:
                         result["success"] = False
                         # Cố gắng trích lỗi từ HTML nếu có
                         error_match = re.search(r'<pre>(.*?)</pre>', response_text_full, re.DOTALL | re.IGNORECASE)
                         html_error = f": {html.escape(error_match.group(1).strip())}" if error_match else "."
                         result["message"] = f"Lỗi định dạng phản hồi API (Type: {content_type}){html_error}"

            else:
                 logger.error(f"[API Call @{target_username}] HTTP Error Status: {resp.status_code}. Text: {response_text_for_debug}...")
                 result["message"] = f"Lỗi từ API follow (Code: {resp.status_code})."
                 result["success"] = False

    except httpx.TimeoutException:
        logger.warning(f"[API Call @{target_username}] API timeout.")
        result["message"] = f"Lỗi: API timeout khi follow @{html.escape(target_username)}."
        result["success"] = False
    except httpx.ConnectError as e_connect:
        logger.error(f"[API Call @{target_username}] Connection error: {e_connect}", exc_info=False)
        result["message"] = f"Lỗi kết nối đến API follow @{html.escape(target_username)}."
        result["success"] = False
    except httpx.RequestError as e_req:
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
    logger.info(f"[API Call @{target_username}] Final result: Success={result['success']}, Message='{result['message'][:200]}...'")
    return result


# --- Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start hoặc /menu - Hiển thị menu chính."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user: return

    logger.info(f"User {user.id} used /start or /menu in chat {chat_id}")

    # Tạo nội dung chào mừng
    act_h = ACTIVATION_DURATION_SECONDS // 3600
    treo_interval_m = TREO_INTERVAL_SECONDS // 60
    welcome_text = (
        f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
        f"🤖 Chào mừng bạn đến với <b>DinoTool</b> - Bot hỗ trợ TikTok.\n\n"
        f"✨ <b>Cách sử dụng cơ bản (Miễn phí):</b>\n"
        f"   » Dùng <code>/getkey</code> và <code>/nhapkey &lt;key&gt;</code> để kích hoạt {act_h} giờ sử dụng <code>/tim</code>, <code>/fl</code>.\n\n"
        f"👑 <b>Nâng cấp VIP:</b>\n"
        f"   » Mở khóa <code>/treo</code> (tự động chạy /fl mỗi {treo_interval_m} phút), không cần key, giới hạn cao hơn.\n\n"
        f"👇 <b>Chọn một tùy chọn bên dưới:</b>"
    )

    # Tạo các nút cho menu
    keyboard_buttons = []
    # Nút Mua VIP (callback để gọi /muatt)
    keyboard_buttons.append([InlineKeyboardButton("👑 Mua VIP", callback_data="show_muatt")])
    # Nút Lệnh (callback để gọi /lenh)
    keyboard_buttons.append([InlineKeyboardButton("📜 Lệnh Bot", callback_data="show_lenh")])
    # Nút Nhóm (chỉ hiện nếu GROUP_LINK được set)
    if GROUP_LINK and GROUP_LINK != "YOUR_GROUP_INVITE_LINK":
         keyboard_buttons.append([InlineKeyboardButton("💬 Nhóm Chính", url=GROUP_LINK)])
    # Nút Admin
    keyboard_buttons.append([InlineKeyboardButton("👨‍💻 Liên hệ Admin", url=f"tg://user?id={ADMIN_USER_ID}")])

    reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    try:
        # Xóa lệnh gốc (/start hoặc /menu)
        await delete_user_message(update, context)
        # Gửi tin nhắn chào mừng kèm menu
        await context.bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /start or /menu message to {user.id} in chat {chat_id}: {e}")

# Callback handler cho các nút menu
async def menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer() # Luôn trả lời callback để nút không bị treo loading

    callback_data = query.data
    user = query.from_user
    chat_id = query.message.chat_id
    if not user: return

    logger.info(f"Menu callback '{callback_data}' triggered by user {user.id} in chat {chat_id}")

    if callback_data == "show_muatt":
        # Xóa tin nhắn menu cũ
        try: await query.delete_message()
        except Exception as e: logger.debug(f"Could not delete old menu message: {e}")
        # Gọi hàm xử lý của /muatt
        # Cần tạo một "Update" giả lập hoặc gọi trực tiếp hàm logic
        # Cách đơn giản là gửi tin nhắn hướng dẫn user gõ lệnh
        # await context.bot.send_message(chat_id, "Vui lòng gõ lệnh <code>/muatt</code> để xem thông tin mua VIP.", parse_mode=ParseMode.HTML)
        # Cách tốt hơn: Gọi trực tiếp hàm xử lý logic của muatt_command
        # Tạo một đối tượng Update và Message giả lập đủ để muatt_command chạy
        fake_message = Message(message_id=query.message.message_id + 1, # ID giả
                               date=datetime.now(), chat=query.message.chat, from_user=user, text="/muatt")
        fake_update = Update(update_id=update.update_id + 1, message=fake_message) # ID giả
        # Chạy hàm muatt_command với dữ liệu giả lập
        await muatt_command(fake_update, context)

    elif callback_data == "show_lenh":
        # Xóa tin nhắn menu cũ
        try: await query.delete_message()
        except Exception as e: logger.debug(f"Could not delete old menu message: {e}")
        # Gọi hàm xử lý của /lenh
        fake_message = Message(message_id=query.message.message_id + 1, date=datetime.now(), chat=query.message.chat, from_user=user, text="/lenh")
        fake_update = Update(update_id=update.update_id + 1, message=fake_message)
        await lenh_command(fake_update, context)

    # Có thể thêm các callback khác ở đây nếu menu có nhiều tầng

async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /lenh - Hiển thị danh sách lệnh và trạng thái user."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user: return

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
            except (ValueError, TypeError, OSError): pass
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

    # Hiển thị trạng thái treo dựa trên persistent_treo_configs
    current_treo_count = len(persistent_treo_configs.get(user_id_str, {})) # Đếm từ config đã lưu
    if is_vip:
        vip_limit = get_vip_limit(user_id)
        status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ✅ Có thể (Đang treo: {current_treo_count}/{vip_limit} users)")
    else:
         status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ❌ Chỉ dành cho VIP (Đang treo: {current_treo_count}/0 users)") # Vẫn hiển thị số đang treo nếu có config cũ

    cmd_lines = ["\n\n📜=== <b>DANH SÁCH LỆNH</b> ===📜"]
    cmd_lines.append("\n<b><u>🧭 Điều Hướng:</u></b>")
    cmd_lines.append(f"  <code>/menu</code> - Mở menu chính")
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
    cmd_lines.append(f"  <code>/listtreo</code> - Xem danh sách tài khoản đang treo")
    if user_id == ADMIN_USER_ID:
        cmd_lines.append("\n<b><u>🛠️ Lệnh Admin:</u></b>")
        valid_vip_packages = ', '.join(map(str, VIP_PRICES.keys()))
        cmd_lines.append(f"  <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code> - Thêm/gia hạn VIP (Gói: {valid_vip_packages})")
        # cmd_lines.append(f"  <code>/adminlisttreo &lt;user_id&gt;</code> - Xem list treo của user khác (tùy chọn)")
    cmd_lines.append("\n<b><u>ℹ️ Lệnh Chung:</u></b>")
    cmd_lines.append(f"  <code>/start</code> - Hiển thị menu chào mừng")
    cmd_lines.append(f"  <code>/lenh</code> - Xem lại bảng lệnh và trạng thái này")
    cmd_lines.append("\n<i>Lưu ý: Các lệnh yêu cầu VIP/Key chỉ hoạt động khi bạn có trạng thái tương ứng.</i>")

    help_text = "\n".join(status_lines + cmd_lines)
    try:
        # Xóa lệnh /lenh gốc để tránh spam chat
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

    # Parse Arguments (URL validation)
    args = context.args
    video_url = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Chưa nhập link video.\n<b>Cú pháp:</b> <code>/tim https://tiktok.com/...</code>")
    elif "tiktok.com/" not in args[0] or not args[0].startswith(("http://", "https://")):
        # Kiểm tra link rút gọn vm.tiktok.com hoặc vt.tiktok.com
        if not re.match(r"https?://(vm|vt)\.tiktok\.com/", args[0]):
             err_txt = f"⚠️ Link <code>{html.escape(args[0])}</code> không hợp lệ. Phải là link video TikTok (tiktok.com, vm.tiktok.com, vt.tiktok.com)."
    else:
        # Cố gắng trích xuất link chuẩn hơn (bao gồm cả link rút gọn)
        match = re.search(r"(https?://(?:www\.|vm\.|vt\.)?tiktok\.com/(?:@[a-zA-Z0-9_.]+/video/|v/|t/)?[\w.-]+)", args[0])
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
        await send_temporary_message(update, context, "❌ Lỗi cấu hình: Bot thiếu API Key cho chức năng này. Báo Admin.", duration=20)
        return

    # Call API
    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    log_api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key="***")
    logger.info(f"User {user_id} calling /tim API: {log_api_url}")

    processing_msg = None
    final_response_text = ""
    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý yêu cầu tăng tim...</i></b> ❤️")
        await delete_user_message(update, context, original_message_id)

        async with httpx.AsyncClient(verify=False, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot Tim Caller'})
            content_type = resp.headers.get("content-type","").lower()
            response_text_full = ""
            try:
                 resp_bytes = await resp.aread()
                 response_text_full = resp_bytes.decode('utf-8', errors='replace')
            except Exception as e_read: logger.error(f"/tim API read error: {e_read}")

            response_text_for_debug = response_text_full[:500]
            logger.debug(f"/tim API response status: {resp.status_code}, content-type: {content_type}")
            logger.debug(f"/tim API response snippet: {response_text_for_debug}...")

            if resp.status_code == 200 and "application/json" in content_type:
                try:
                    data = json.loads(response_text_full)
                    logger.debug(f"/tim API response data: {data}")
                    # Check success based on API response structure
                    if data.get("status") == "success" or data.get("success") == True: # Check cả hai kiểu
                        user_tim_cooldown[user_id_str] = time.time()
                        save_data()
                        d = data.get("data", {})
                        a = html.escape(str(d.get("author", "?")))
                        v = html.escape(str(d.get("video_url", video_url)))
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
                        api_msg = data.get('message', 'Không rõ lý do từ API')
                        logger.warning(f"/tim API call failed for user {user_id}. API message: {api_msg}")
                        final_response_text = f"💔 <b>Tăng Tim Thất Bại!</b>\n👤 Cho: {user.mention_html()}\nℹ️ Lý do: <code>{html.escape(api_msg)}</code>"
                except json.JSONDecodeError as e_json:
                    logger.error(f"/tim API response 200 OK but not valid JSON. Error: {e_json}. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API Tăng Tim</b>\n👤 Cho: {user.mention_html()}\nℹ️ API không trả về JSON hợp lệ."
            else:
                logger.error(f"/tim API call HTTP error or wrong content type. Status: {resp.status_code}, Type: {content_type}. Text: {response_text_for_debug}...")
                final_response_text = f"❌ <b>Lỗi Kết Nối API Tăng Tim</b>\n👤 Cho: {user.mention_html()}\nℹ️ Mã lỗi: {resp.status_code}. Vui lòng thử lại sau."
    except httpx.TimeoutException:
        logger.warning(f"/tim API call timeout for user {user_id}")
        final_response_text = f"❌ <b>Lỗi Timeout</b>\n👤 Cho: {user.mention_html()}\nℹ️ API tăng tim không phản hồi kịp thời."
    except httpx.RequestError as e_req:
        logger.error(f"/tim API call network error for user {user_id}: {e_req}", exc_info=False)
        final_response_text = f"❌ <b>Lỗi Mạng</b>\n👤 Cho: {user.mention_html()}\nℹ️ Không thể kết nối đến API tăng tim."
    except Exception as e_unexp:
        logger.error(f"Unexpected error during /tim command for user {user_id}: {e_unexp}", exc_info=True)
        final_response_text = f"❌ <b>Lỗi Hệ Thống Bot</b>\n👤 Cho: {user.mention_html()}\nℹ️ Đã xảy ra lỗi. Báo Admin."
    finally:
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
            except BadRequest as e_edit:
                if "Message is not modified" not in str(e_edit): logger.warning(f"Failed to edit /tim msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit_unexp: logger.warning(f"Unexpected error editing /tim msg {processing_msg.message_id}: {e_edit_unexp}")
        else:
            logger.warning(f"Processing message for /tim user {user_id} was None. Sending new message.")
            try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
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

    if api_data and isinstance(api_data, dict):
        try:
            name = html.escape(str(api_data.get("name", "?")))
            tt_username_from_api = api_data.get("username")
            tt_username = html.escape(str(tt_username_from_api if tt_username_from_api else target_username))
            tt_user_id = html.escape(str(api_data.get("user_id", "?")))
            khu_vuc = html.escape(str(api_data.get("khu_vuc", "Không rõ")))
            avatar = api_data.get("avatar", "")
            create_time = html.escape(str(api_data.get("create_time", "?")))

            user_info_lines = [f"👤 <b>Tài khoản:</b> <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)"]
            if tt_user_id != "?": user_info_lines.append(f"🆔 <b>ID TikTok:</b> <code>{tt_user_id}</code>")
            if khu_vuc != "Không rõ": user_info_lines.append(f"🌍 <b>Khu vực:</b> {khu_vuc}")
            if create_time != "?": user_info_lines.append(f"📅 <b>Ngày tạo TK:</b> {create_time}")
            if avatar and isinstance(avatar, str) and avatar.startswith("http"): user_info_lines.append(f"🖼️ <a href='{html.escape(avatar)}'>Xem Avatar</a>")
            user_info_block = "\n".join(user_info_lines) + "\n"

            f_before = html.escape(str(api_data.get("followers_before", "?")))
            f_add_raw = api_data.get("followers_add", "?") # Giữ nguyên kiểu dữ liệu
            f_after = html.escape(str(api_data.get("followers_after", "?")))

            # Xử lý f_add linh hoạt hơn
            f_add_display = "?"
            f_add_int = 0
            if f_add_raw != "?":
                 try:
                     # Cố gắng chuyển đổi sang số nguyên, bỏ qua dấu '+' hoặc các ký tự không phải số
                     f_add_str_cleaned = re.sub(r'[^\d-]', '', str(f_add_raw)) # Giữ lại dấu trừ nếu có
                     if f_add_str_cleaned: f_add_int = int(f_add_str_cleaned)
                     f_add_display = f"+{f_add_int}" if f_add_int >= 0 else str(f_add_int) # Thêm dấu + cho số dương
                 except ValueError: f_add_display = html.escape(str(f_add_raw)) # Hiển thị nguyên bản nếu không phải số

            if any(x != "?" for x in [f_before, f_add_raw, f_after]):
                follower_lines = ["📈 <b>Số lượng Follower:</b>"]
                if f_before != "?": follower_lines.append(f"   Trước: <code>{f_before}</code>")
                if f_add_display != "?" and f_add_int > 0:
                    follower_lines.append(f"   Tăng:   <b><code>{f_add_display}</code></b> ✨")
                elif f_add_display != "?": # Hiển thị cả tăng 0 hoặc âm
                    follower_lines.append(f"   Tăng:   <code>{f_add_display}</code>")
                if f_after != "?": follower_lines.append(f"   Sau:    <code>{f_after}</code>")
                if len(follower_lines) > 1: follower_info_block = "\n".join(follower_lines)
        except Exception as e_parse:
            logger.error(f"[BG Task /fl] Error parsing API data for @{target_username}: {e_parse}. Data: {api_data}")
            user_info_block = f"👤 <b>Tài khoản:</b> <code>@{html.escape(target_username)}</code>\n(Lỗi xử lý thông tin chi tiết từ API)"
            follower_info_block = ""

    if success:
        current_time_ts = time.time()
        # Cập nhật cooldown trong cấu trúc defaultdict
        user_fl_cooldown[str(user_id_str)][target_username] = current_time_ts
        save_data()
        logger.info(f"[BG Task /fl] Success for user {user_id_str} -> @{target_username}. Cooldown updated.")
        final_response_text = (
            f"✅ <b>Tăng Follow Thành Công!</b>\n"
            f"✨ Cho: {invoking_user_mention}\n\n"
            f"{user_info_block if user_info_block else f'👤 <b>Tài khoản:</b> <code>@{html.escape(target_username)}</code>\n'}"
            f"{follower_info_block if follower_info_block else ''}"
        )
    else:
        logger.warning(f"[BG Task /fl] Failed for user {user_id_str} -> @{target_username}. API Message: {api_message}")
        final_response_text = (
            f"❌ <b>Tăng Follow Thất Bại!</b>\n"
            f"👤 Cho: {invoking_user_mention}\n"
            f"🎯 Target: <code>@{html.escape(target_username)}</code>\n\n"
            f"💬 Lý do API: <i>{html.escape(api_message or 'Không rõ')}</i>\n\n"
            f"{user_info_block if user_info_block else ''}"
        )
        if isinstance(api_message, str) and ("đợi" in api_message.lower() or "wait" in api_message.lower()) and ("phút" in api_message.lower() or "giây" in api_message.lower() or "minute" in api_message.lower() or "second" in api_message.lower()):
            final_response_text += f"\n\n<i>ℹ️ API yêu cầu chờ đợi. Vui lòng thử lại sau hoặc sử dụng <code>/treo {target_username}</code> nếu bạn là VIP.</i>"

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=processing_msg_id, text=final_response_text,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
        logger.info(f"[BG Task /fl] Edited message {processing_msg_id} for user {user_id_str} -> @{target_username}")
    except BadRequest as e:
         if "Message is not modified" not in str(e): logger.error(f"[BG Task /fl] BadRequest editing msg {processing_msg_id}: {e}")
    except Forbidden:
        logger.error(f"[BG Task /fl] Forbidden: Cannot edit message {processing_msg_id} in chat {chat_id}. Bot might be blocked or not in chat.")
    except Exception as e:
        logger.error(f"[BG Task /fl] Failed to edit msg {processing_msg_id}: {e}", exc_info=True)


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
    # Regex kiểm tra username TikTok hợp lệ (tương đối) - GIỮ LẠI VALIDATION NÀY
    username_regex = r"^[a-zA-Z0-9._]{2,24}$" # Cho phép gạch dưới và dấu chấm

    if not args:
        err_txt = ("⚠️ Chưa nhập username TikTok.\n<b>Cú pháp:</b> <code>/fl username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@") # Xóa @ nếu có
        if not uname:
            err_txt = "⚠️ Username không được trống."
        # Bỏ qua kiểm tra regex nếu muốn nới lỏng, nhưng giữ lại kiểm tra cơ bản
        elif not re.match(username_regex, uname):
             err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
                        f"(Phải từ 2-24 ký tự, chỉ chứa chữ cái, số, dấu chấm '.', dấu gạch dưới '_')")
        elif uname.startswith('.') or uname.endswith('.') or uname.startswith('_') or uname.endswith('_'):
             err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ (không được bắt đầu/kết thúc bằng '.' hoặc '_')."
        # Kiểm tra xem username có chứa '..' liên tiếp không
        elif '..' in uname:
             err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ (không được chứa '..' liên tiếp)."
        else:
            target_username = uname # Username hợp lệ

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # 3. Check Cooldown (chỉ check nếu username hợp lệ)
    if target_username:
        # Sử dụng cấu trúc defaultdict đã load
        user_cds = user_fl_cooldown.get(user_id_str, {}) # Lấy dict cooldown của user, trả về dict rỗng nếu user chưa có
        last_usage = user_cds.get(target_username) # Lấy timestamp cho target cụ thể

        if last_usage:
            try:
                elapsed = current_time - float(last_usage)
                if elapsed < TIM_FL_COOLDOWN_SECONDS:
                     rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                     cd_msg = f"⏳ {invoking_user_mention}, đợi <b>{rem_time:.0f} giây</b> nữa để dùng <code>/fl</code> cho <code>@{html.escape(target_username)}</code>."
                     await send_temporary_message(update, context, cd_msg, duration=15)
                     await delete_user_message(update, context, original_message_id)
                     return
            except (ValueError, TypeError):
                 logger.warning(f"Invalid cooldown timestamp for /fl user {user_id_str} target {target_username}. Resetting.")
                 if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                     del user_fl_cooldown[user_id_str][target_username]; save_data()

    # 4. Gửi tin nhắn chờ và chạy nền
    processing_msg = None
    try:
        if not target_username: raise ValueError("Target username became None unexpectedly before processing")

        processing_msg = await update.message.reply_html(
            f"⏳ {invoking_user_mention}, đã nhận yêu cầu tăng follow cho <code>@{html.escape(target_username)}</code>. Đang xử lý..."
        )
        await delete_user_message(update, context, original_message_id)

        logger.info(f"Scheduling background task for /fl user {user_id} target @{target_username}")
        context.application.create_task(
            process_fl_request_background(
                context=context, chat_id=chat_id, user_id_str=user_id_str,
                target_username=target_username, processing_msg_id=processing_msg.message_id,
                invoking_user_mention=invoking_user_mention
            ),
            name=f"fl_bg_{user_id_str}_{target_username}"
        )
    except (BadRequest, Forbidden, TelegramError, ValueError) as e:
        logger.error(f"Failed to send processing message or schedule task for /fl @{html.escape(target_username or '???')}: {e}")
        await delete_user_message(update, context, original_message_id)
        if processing_msg:
            try: await context.bot.edit_message_text(chat_id, processing_msg.message_id, f"❌ Lỗi khi bắt đầu xử lý yêu cầu /fl cho @{html.escape(target_username or '???')}. Vui lòng thử lại.")
            except Exception: pass
    except Exception as e:
         logger.error(f"Unexpected error in fl_command for user {user_id} target @{html.escape(target_username or '???')}: {e}", exc_info=True)
         await delete_user_message(update, context, original_message_id)
         await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi chạy /fl cho @{html.escape(target_username or '???')}. Báo Admin.", duration=20)


# --- Lệnh /getkey ---
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
    while generated_key in valid_keys:
        logger.warning(f"Key collision detected for {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    cache_buster = f"&ts={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster
    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}" if len(LINK_SHORTENER_API_KEY) > 6 else "***", "format": "json", "url": final_target_url }
    logger.info(f"User {user_id} requesting key. Generated: {generated_key}. Target URL for shortener: {final_target_url}")

    processing_msg = None
    final_response_text = ""
    key_stored_successfully = False

    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key, vui lòng chờ...</i></b> 🔑")
        await delete_user_message(update, context, original_message_id)

        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = {
            "user_id_generator": user_id, "generation_time": generation_time,
            "expiry_time": expiry_time, "used_by": None, "activation_time": None
        }
        save_data()
        key_stored_successfully = True
        logger.info(f"Key {generated_key} stored for user {user_id}. Expires at {datetime.fromtimestamp(expiry_time).isoformat()}.")

        logger.debug(f"Calling shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_shortener_params}")
        async with httpx.AsyncClient(timeout=30.0, verify=True) as client:
            headers = {'User-Agent': 'Telegram Bot Key Generator'}
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers=headers)
            response_content_type = response.headers.get("content-type", "").lower()
            response_text_full = ""
            try:
                resp_bytes = await response.aread()
                response_text_full = resp_bytes.decode('utf-8', errors='replace')
            except Exception as e_read: logger.error(f"/getkey shortener read error: {e_read}")

            response_text_for_debug = response_text_full[:500]
            logger.debug(f"Shortener API response status: {response.status_code}, content-type: {response_content_type}")
            logger.debug(f"Shortener API response snippet: {response_text_for_debug}...")

            if response.status_code == 200:
                try:
                    response_data = response.json()
                    logger.debug(f"Parsed shortener API response: {response_data}")
                    status = response_data.get("status")
                    generated_short_url = response_data.get("shortenedUrl")

                    if status == "success" and generated_short_url:
                        user_getkey_cooldown[user_id_str] = time.time()
                        save_data()
                        logger.info(f"Successfully generated short link for user {user_id}: {generated_short_url}. Key {generated_key} confirmed.")
                        final_response_text = (
                            f"🚀 <b>Link Lấy Key Của Bạn ({user.mention_html()}):</b>\n\n"
                            f"🔗 <a href='{html.escape(generated_short_url)}'>{html.escape(generated_short_url)}</a>\n\n"
                            f"📝 <b>Hướng dẫn:</b>\n"
                            f"   1️⃣ Click vào link trên.\n"
                            f"   2️⃣ Làm theo các bước trên trang web để nhận Key (VD: <code>Dinotool-ABC123XYZ</code>).\n"
                            f"   3️⃣ Copy Key đó và quay lại đây.\n"
                            f"   4️⃣ Gửi lệnh: <code>/nhapkey &lt;key_ban_vua_copy&gt;</code>\n\n"
                            f"⏳ <i>Key chỉ có hiệu lực để nhập trong <b>{KEY_EXPIRY_SECONDS // 3600} giờ</b>. Hãy nhập sớm!</i>"
                        )
                    else:
                        api_message = response_data.get("message", "Lỗi không xác định từ API rút gọn link.")
                        logger.error(f"Shortener API returned error for user {user_id}. Status: {status}, Message: {api_message}. Data: {response_data}")
                        final_response_text = f"❌ <b>Lỗi Khi Tạo Link:</b>\n<code>{html.escape(str(api_message))}</code>\nVui lòng thử lại sau hoặc báo Admin."
                except json.JSONDecodeError:
                    logger.error(f"Shortener API Status 200 but JSON decode failed. Type: '{response_content_type}'. Text: {response_text_for_debug}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API Rút Gọn Link:</b> Máy chủ trả về dữ liệu không hợp lệ. Vui lòng thử lại sau."
            else:
                 logger.error(f"Shortener API HTTP error. Status: {response.status_code}. Type: '{response_content_type}'. Text: {response_text_for_debug}...")
                 final_response_text = f"❌ <b>Lỗi Kết Nối API Tạo Link</b> (Mã: {response.status_code}). Vui lòng thử lại sau hoặc báo Admin."
    except httpx.TimeoutException:
        logger.warning(f"Shortener API timeout during /getkey for user {user_id}")
        final_response_text = "❌ <b>Lỗi Timeout:</b> Máy chủ tạo link không phản hồi kịp thời. Vui lòng thử lại sau."
    except httpx.ConnectError as e_connect:
        logger.error(f"Shortener API connection error during /getkey for user {user_id}: {e_connect}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Kết Nối:</b> Không thể kết nối đến máy chủ tạo link. Vui lòng kiểm tra mạng hoặc thử lại sau."
    except httpx.RequestError as e_req:
        logger.error(f"Shortener API network error during /getkey for user {user_id}: {e_req}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Mạng</b> khi gọi API tạo link. Vui lòng thử lại sau."
    except Exception as e_unexp:
        logger.error(f"Unexpected error during /getkey command for user {user_id}: {e_unexp}", exc_info=True)
        final_response_text = "❌ <b>Lỗi Hệ Thống Bot</b> khi tạo key. Vui lòng báo Admin."
        if key_stored_successfully and generated_key in valid_keys and valid_keys[generated_key].get("used_by") is None:
            try:
                del valid_keys[generated_key]
                save_data()
                logger.info(f"Removed unused key {generated_key} due to unexpected error in /getkey.")
            except Exception as e_rem: logger.error(f"Failed to remove unused key {generated_key} after error: {e_rem}")

    finally:
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
            except BadRequest as e_edit:
                 if "Message is not modified" not in str(e_edit): logger.warning(f"Failed to edit /getkey msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit_unexp: logger.warning(f"Unexpected error editing /getkey msg {processing_msg.message_id}: {e_edit_unexp}")
        else:
             logger.warning(f"Processing message for /getkey user {user_id} was None. Sending new message.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
             except Exception as e_send: logger.error(f"Failed to send final /getkey message for user {user_id}: {e_send}")

# --- Lệnh /nhapkey ---
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
    key_format_regex = re.compile(r"^" + re.escape(key_prefix) + r"[A-Z0-9]+$")

    if not args: err_txt = ("⚠️ Bạn chưa nhập key.\n<b>Cú pháp đúng:</b> <code>/nhapkey Dinotool-KEYCỦABẠN</code>")
    elif len(args) > 1: err_txt = f"⚠️ Bạn đã nhập quá nhiều từ. Chỉ nhập key thôi.\nVí dụ: <code>/nhapkey {generate_random_key()}</code>"
    else:
        key_input = args[0].strip()
        if not key_format_regex.match(key_input): err_txt = (f"⚠️ Key <code>{html.escape(key_input)}</code> sai định dạng.\nPhải bắt đầu bằng <code>{key_prefix}</code> và theo sau là chữ IN HOA/số.")
        else: submitted_key = key_input

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
        used_by_id = key_data["used_by"]
        activation_time_ts = key_data.get("activation_time")
        used_time_str = ""
        if activation_time_ts:
            try: used_time_str = f" lúc {datetime.fromtimestamp(float(activation_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
            except (ValueError, TypeError, OSError): pass

        if str(used_by_id) == user_id_str:
             logger.info(f"Key validation: User {user_id} already used key '{submitted_key}'{used_time_str}.")
             final_response_text = f"⚠️ Bạn đã kích hoạt key <code>{html.escape(submitted_key)}</code> này rồi{used_time_str}."
        else:
             logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by user {used_by_id}{used_time_str}.")
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được người khác sử dụng{used_time_str}."
    elif current_time > float(key_data.get("expiry_time", 0)):
        expiry_time_ts = key_data.get("expiry_time")
        expiry_time_str = ""
        if expiry_time_ts:
            try: expiry_time_str = f" vào lúc {datetime.fromtimestamp(float(expiry_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
            except (ValueError, TypeError, OSError): pass

        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' expired{expiry_time_str}.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn sử dụng{expiry_time_str}. Dùng <code>/getkey</code> để lấy key mới."
        if submitted_key in valid_keys:
             del valid_keys[submitted_key]; save_data(); logger.info(f"Removed expired key {submitted_key} upon activation attempt.")
    else:
        try:
            key_data["used_by"] = user_id
            key_data["activation_time"] = current_time
            activation_expiry_ts = current_time + ACTIVATION_DURATION_SECONDS
            activated_users[user_id_str] = activation_expiry_ts
            save_data()

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
             # Rollback
             if submitted_key in valid_keys and valid_keys[submitted_key].get("used_by") == user_id:
                 valid_keys[submitted_key]["used_by"] = None
                 valid_keys[submitted_key]["activation_time"] = None
             if user_id_str in activated_users: del activated_users[user_id_str]
             save_data()

    # Gửi phản hồi và xóa lệnh gốc
    await delete_user_message(update, context, original_message_id)
    try:
        await update.message.reply_html(final_response_text, disable_web_page_preview=True)
    except Exception as e:
         logger.error(f"Failed to send /nhapkey final response to user {user_id}: {e}")


# --- Lệnh /muatt ---
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị thông tin mua VIP và nút yêu cầu gửi bill."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user: return
    original_message_id = update.message.message_id
    user_id = user.id
    payment_note = f"{PAYMENT_NOTE_PREFIX} {user_id}"

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
                       f"   - STK: <a href=\"https://t.me/share/url?url={BANK_ACCOUNT}\"><code>{BANK_ACCOUNT}</code></a> (👈 Click để copy)",
                       f"   - Tên chủ TK: <b>{ACCOUNT_NAME}</b>",
                       "\n📝 <b>Nội dung chuyển khoản (Quan trọng!):</b>",
                       f"   » Chuyển khoản với nội dung <b>CHÍNH XÁC</b> là:",
                       f"   » <a href=\"https://t.me/share/url?url={payment_note}\"><code>{payment_note}</code></a> (👈 Click để copy)",
                       f"   <i>(Sai nội dung có thể khiến giao dịch xử lý chậm)</i>",
                       "\n📸 <b>Sau Khi Chuyển Khoản Thành Công:</b>",
                       f"   1️⃣ Chụp ảnh màn hình biên lai (bill) giao dịch.",
                       f"   2️⃣ Nhấn nút 'Gửi Bill Thanh Toán' bên dưới.",
                       f"   3️⃣ Bot sẽ yêu cầu bạn gửi ảnh bill <b><u>VÀO CUỘC TRÒ CHUYỆN NÀY</u></b>.",
                       f"   4️⃣ Gửi ảnh bill của bạn vào đây.",
                       f"   5️⃣ Bot sẽ tự động chuyển tiếp ảnh đến Admin để xác nhận.",
                       f"   6️⃣ Admin sẽ kiểm tra và kích hoạt VIP sớm nhất.",
                       "\n<i>Cảm ơn bạn đã quan tâm và ủng hộ DinoTool!</i> ❤️"])
    text = "\n".join(text_lines)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📸 Gửi Bill Thanh Toán", callback_data=f"prompt_send_bill_{user_id}")]
    ])

    # Xóa lệnh /muatt gốc (chỉ xóa nếu nó đến từ message, không xóa nếu đến từ callback)
    if original_message_id and update.message and original_message_id == update.message.message_id:
         await delete_user_message(update, context, original_message_id)

    try:
        await context.bot.send_photo(chat_id=chat_id, photo=QR_CODE_URL, caption=text,
                                   parse_mode=ParseMode.HTML, reply_markup=keyboard)
        logger.info(f"Sent /muatt info with prompt button to user {user_id} in chat {chat_id}")
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.error(f"Error sending /muatt photo+caption to chat {chat_id}: {e}. Falling back to text.")
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML,
                                           disable_web_page_preview=True, reply_markup=keyboard)
            logger.info(f"Sent /muatt fallback text info with prompt button to user {user_id} in chat {chat_id}")
        except Exception as e_text:
             logger.error(f"Error sending fallback text for /muatt to chat {chat_id}: {e_text}")
    except Exception as e_unexp:
        logger.error(f"Unexpected error sending /muatt command to chat {chat_id}: {e_unexp}", exc_info=True)

# --- Callback Handler cho nút "Gửi Bill Thanh Toán" ---
async def prompt_send_bill_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = query.from_user
    chat_id = query.message.chat_id
    if not query or not user: return

    callback_data = query.data
    expected_user_id = None
    try:
        if callback_data.startswith("prompt_send_bill_"): expected_user_id = int(callback_data.split("_")[-1])
    except (ValueError, IndexError):
        logger.warning(f"Invalid callback_data format: {callback_data}")
        await query.answer("Lỗi: Dữ liệu nút không hợp lệ.", show_alert=True); return

    if user.id != expected_user_id:
        await query.answer("Bạn không phải người yêu cầu thanh toán.", show_alert=True)
        logger.info(f"User {user.id} tried to click bill prompt button for user {expected_user_id} in chat {chat_id}")
        return

    pending_bill_user_ids.add(user.id)
    if context.job_queue:
        context.job_queue.run_once(
            remove_pending_bill_user_job, 15 * 60, data={'user_id': user.id}, name=f"remove_pending_bill_{user.id}"
        )

    await query.answer()
    logger.info(f"User {user.id} clicked 'prompt_send_bill' button in chat {chat_id}. Added to pending list.")

    prompt_text = f"📸 {user.mention_html()}, vui lòng gửi ảnh chụp màn hình biên lai thanh toán của bạn <b><u>vào cuộc trò chuyện này</u></b>."
    try:
        # Gửi tin nhắn yêu cầu trong chat hiện tại
        await context.bot.send_message(chat_id=chat_id, text=prompt_text, parse_mode=ParseMode.HTML)
        # Không xóa tin nhắn cũ có nút bấm, để user biết họ đã bấm
    except Exception as e:
        logger.error(f"Error sending bill prompt message to {user.id} in chat {chat_id}: {e}", exc_info=True)
        # Có thể gửi lại vào PM nếu gửi vào group lỗi? (phức tạp hơn)

async def remove_pending_bill_user_job(context: ContextTypes.DEFAULT_TYPE):
    """Job để xóa user khỏi danh sách chờ nhận bill."""
    job_data = context.job.data
    user_id = job_data.get('user_id')
    if user_id in pending_bill_user_ids:
        pending_bill_user_ids.remove(user_id)
        logger.info(f"Removed user {user_id} from pending bill list due to timeout.")

# --- Xử lý nhận ảnh bill ---
async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý ảnh/document ảnh gửi đến bot VÀ chỉ chuyển tiếp nếu user nằm trong danh sách chờ."""
    if not update or not update.message: return
    if update.message.text and update.message.text.startswith('/'): return

    user = update.effective_user
    chat = update.effective_chat
    message = update.message
    if not user or not chat or not message: return

    # Kiểm tra xem người gửi có trong danh sách chờ nhận bill không
    if user.id not in pending_bill_user_ids: return

    is_photo = bool(message.photo)
    is_image_document = bool(message.document and message.document.mime_type and message.document.mime_type.startswith('image/'))
    if not is_photo and not is_image_document: return

    logger.info(f"Bill photo/document received from PENDING user {user.id} in chat {chat.id} (Type: {chat.type}). Forwarding to {BILL_FORWARD_TARGET_ID}.")

    pending_bill_user_ids.discard(user.id)
    if context.job_queue:
         jobs = context.job_queue.get_jobs_by_name(f"remove_pending_bill_{user.id}")
         for job in jobs: job.schedule_removal(); logger.debug(f"Removed pending bill timeout job for user {user.id}")

    forward_caption_lines = [f"📄 <b>Bill Nhận Được Từ User</b>",
                             f"👤 <b>User:</b> {user.mention_html()} (<code>{user.id}</code>)"]
    if chat.type == 'private': forward_caption_lines.append(f"💬 <b>Chat gốc:</b> PM với Bot")
    elif chat.title: forward_caption_lines.append(f"👥 <b>Chat gốc:</b> {html.escape(chat.title)} (<code>{chat.id}</code>)")
    else: forward_caption_lines.append(f"❓ <b>Chat gốc:</b> ID <code>{chat.id}</code>")

    try:
        message_link = message.link
        if message_link: forward_caption_lines.append(f"🔗 <a href='{message_link}'>Link Tin Nhắn Gốc</a>")
    except AttributeError: logger.debug(f"Could not get message link for message {message.message_id} in chat {chat.id}")

    original_caption = message.caption
    if original_caption: forward_caption_lines.append(f"\n📝 <b>Caption gốc:</b>\n{html.escape(original_caption[:500])}{'...' if len(original_caption) > 500 else ''}")

    forward_caption_text = "\n".join(forward_caption_lines)

    try:
        # Chuyển tiếp tin nhắn gốc
        await context.bot.forward_message(chat_id=BILL_FORWARD_TARGET_ID, from_chat_id=chat.id, message_id=message.message_id)
        # Gửi tin nhắn thông tin bổ sung
        await context.bot.send_message(chat_id=BILL_FORWARD_TARGET_ID, text=forward_caption_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

        logger.info(f"Successfully forwarded bill message {message.message_id} from user {user.id} (chat {chat.id}) and sent info to {BILL_FORWARD_TARGET_ID}.")
        try: await message.reply_html("✅ Đã nhận và chuyển tiếp bill của bạn đến Admin để xử lý. Vui lòng chờ nhé!")
        except Exception as e_reply: logger.warning(f"Failed to send confirmation reply to user {user.id} in chat {chat.id}: {e_reply}")

    except Forbidden as e:
        logger.error(f"Bot cannot forward/send message to BILL_FORWARD_TARGET_ID ({BILL_FORWARD_TARGET_ID}). Check permissions/block status. Error: {e}")
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
            try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Lý do: Bot bị chặn hoặc thiếu quyền.")
            except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
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

    raise ApplicationHandlerStop # Dừng xử lý để các handler khác không nhận ảnh này nữa

# --- Lệnh /addtt (Admin) ---
async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cấp VIP cho người dùng (chỉ Admin)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    chat = update.effective_chat
    if not admin_user or not chat or admin_user.id != ADMIN_USER_ID: return

    args = context.args
    err_txt = None
    target_user_id = None
    days_key_input = None
    limit = None
    duration_days = None
    valid_day_keys = list(VIP_PRICES.keys())
    valid_days_str = ', '.join(map(str, valid_day_keys))

    if len(args) != 2: err_txt = (f"⚠️ Sai cú pháp.\n<b>Dùng:</b> <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code>\n<b>Gói:</b> {valid_days_str}\n<b>VD:</b> <code>/addtt 123456789 {valid_day_keys[0] if valid_day_keys else '15'}</code>")
    else:
        try: target_user_id = int(args[0])
        except ValueError: err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ."

        if not err_txt:
            try:
                days_key_input = int(args[1])
                if days_key_input not in VIP_PRICES: err_txt = f"⚠️ Gói ngày <code>{days_key_input}</code> không hợp lệ. Chỉ chấp nhận: <b>{valid_days_str}</b>."
                else:
                    vip_info = VIP_PRICES[days_key_input]
                    limit = vip_info["limit"]
                    duration_days = vip_info["duration_days"]
            except ValueError: err_txt = f"⚠️ Gói ngày '<code>{html.escape(args[1])}</code>' không phải số."

    if err_txt:
        try: await update.message.reply_html(err_txt)
        except Exception as e_reply: logger.error(f"Failed to send error reply to admin {admin_user.id}: {e_reply}")
        return

    target_user_id_str = str(target_user_id)
    current_time = time.time()
    current_vip_data = vip_users.get(target_user_id_str)
    start_time = current_time
    operation_type = "Nâng cấp lên"

    if current_vip_data:
         try:
             current_expiry = float(current_vip_data.get("expiry", 0))
             if current_expiry > current_time:
                 start_time = current_expiry
                 operation_type = "Gia hạn thêm"
                 logger.info(f"Admin {admin_user.id}: Extending VIP for {target_user_id_str} from {datetime.fromtimestamp(start_time).isoformat()}.")
             else: logger.info(f"Admin {admin_user.id}: User {target_user_id_str} was VIP but expired. Activating new.")
         except (ValueError, TypeError): logger.warning(f"Admin {admin_user.id}: Invalid expiry for user {target_user_id_str}. Activating new.")

    new_expiry_ts = start_time + duration_days * 86400
    new_expiry_dt = datetime.fromtimestamp(new_expiry_ts)
    new_expiry_str = new_expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')
    vip_users[target_user_id_str] = {"expiry": new_expiry_ts, "limit": limit}
    save_data()
    logger.info(f"Admin {admin_user.id} processed VIP for {target_user_id_str}: {operation_type} {duration_days} days. New expiry: {new_expiry_str}, Limit: {limit}")

    admin_msg = (f"✅ Đã <b>{operation_type} {duration_days} ngày VIP</b> thành công!\n\n"
                 f"👤 User ID: <code>{target_user_id}</code>\n✨ Gói: {duration_days} ngày\n"
                 f"⏳ Hạn mới: <b>{new_expiry_str}</b>\n🚀 Limit: <b>{limit} users</b>")
    try: await update.message.reply_html(admin_msg)
    except Exception as e: logger.error(f"Failed to send confirmation to admin {admin_user.id} in chat {chat.id}: {e}")

    user_mention = f"User ID <code>{target_user_id}</code>"
    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        if target_user_info:
            user_mention = target_user_info.mention_html() or \
                           (f"<a href='tg://user?id={target_user_id}'>User {target_user_id}</a>") # Link fallback
    except Exception as e_get_chat: logger.warning(f"Could not get chat info for {target_user_id}: {e_get_chat}.")

    user_notify_msg = (f"🎉 Chúc mừng {user_mention}! 🎉\n\n"
                       f"Bạn đã được Admin <b>{operation_type} {duration_days} ngày VIP</b>!\n\n"
                       f"✨ Gói VIP: <b>{duration_days} ngày</b>\n⏳ Hạn đến: <b>{new_expiry_str}</b>\n"
                       f"🚀 Limit treo: <b>{limit} tài khoản</b>\n\n"
                       f"Cảm ơn bạn đã ủng hộ DinoTool! ❤️\n(Dùng <code>/menu</code> hoặc <code>/lenh</code> để xem lại)")

    target_chat_id_for_notification = ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else ADMIN_USER_ID
    log_target = f"group {ALLOWED_GROUP_ID}" if ALLOWED_GROUP_ID else f"admin {ADMIN_USER_ID}"
    logger.info(f"Sending VIP notification for {target_user_id} to {log_target}")
    try:
        await context.bot.send_message(chat_id=target_chat_id_for_notification, text=user_notify_msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e_send_notify:
        logger.error(f"Failed to send VIP notification for user {target_user_id} to chat {target_chat_id_for_notification}: {e_send_notify}")
        if admin_user.id != target_chat_id_for_notification:
             try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} vào chat {target_chat_id_for_notification}. Lỗi: {e_send_notify}")
             except Exception: pass

# --- Logic Treo ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Vòng lặp chạy nền cho lệnh /treo, sử dụng persistent config."""
    user_id_int = int(user_id_str)
    task_name = f"treo_{user_id_str}_{target_username}_in_{chat_id}"
    logger.info(f"[Treo Task Start] Task '{task_name}' started/resumed.")

    invoking_user_mention = f"User ID <code>{user_id_str}</code>"
    try:
        # Chỉ lấy mention một lần khi bắt đầu task để giảm API call
        user_info = await context.application.bot.get_chat(user_id_int) # Dùng application.bot
        if user_info and user_info.mention_html(): invoking_user_mention = user_info.mention_html()
    except Exception as e_get_mention: logger.debug(f"Could not get mention for user {user_id_str} in task {task_name}: {e_get_mention}")

    last_api_call_time = 0
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 5 # Ngưỡng dừng task nếu lỗi liên tục

    try:
        while True:
            current_time = time.time()
            app = context.application # Lấy application từ context

            # 1. Kiểm tra xem config persistent còn tồn tại không (quan trọng khi restart hoặc bị xóa bởi lệnh khác)
            # và task hiện tại có phải là task đang được quản lý không
            current_persistent_config_exists = persistent_treo_configs.get(user_id_str, {}).get(target_username) == chat_id
            current_runtime_task = active_treo_tasks.get(user_id_str, {}).get(target_username)
            current_asyncio_task = asyncio.current_task()

            if not current_persistent_config_exists:
                 logger.warning(f"[Treo Task Stop] Persistent config for task '{task_name}' missing. Stopping.")
                 # Không cần gọi stop_treo_task vì config đã bị xóa rồi, chỉ cần dọn runtime
                 if current_runtime_task is current_asyncio_task:
                      if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                          del active_treo_tasks[user_id_str][target_username]
                          if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                          logger.info(f"[Treo Task Stop] Removed runtime task '{task_name}' due to missing persistent config.")
                 break # Thoát vòng lặp

            if current_runtime_task is not current_asyncio_task:
                 logger.warning(f"[Treo Task Stop] Task '{task_name}' seems replaced in runtime dict (found {type(current_runtime_task)}). Stopping this instance.")
                 break # Thoát vòng lặp để task mới (nếu có) chạy

            # 2. Kiểm tra trạng thái VIP (trong mỗi vòng lặp)
            if not is_user_vip(user_id_int):
                logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping task '{task_name}'.")
                await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired in loop") # Sẽ xóa cả persistent
                try:
                    await app.bot.send_message(
                        chat_id, f"ℹ️ {invoking_user_mention}, việc treo cho <code>@{html.escape(target_username)}</code> đã dừng do VIP hết hạn.",
                        parse_mode=ParseMode.HTML, disable_notification=True )
                except Exception as e_send_stop: logger.warning(f"Failed to send VIP expiry stop message for task {task_name}: {e_send_stop}")
                break

            # 3. Tính toán thời gian chờ
            if last_api_call_time > 0:
                elapsed_since_last_call = current_time - last_api_call_time
                wait_needed = TREO_INTERVAL_SECONDS - elapsed_since_last_call
                if wait_needed > 0:
                    logger.debug(f"[Treo Task Wait] Task '{task_name}' waiting for {wait_needed:.1f}s.")
                    await asyncio.sleep(wait_needed)
                else:
                     logger.debug(f"[Treo Task Wait] Task '{task_name}' - No wait needed (elapsed {elapsed_since_last_call:.1f}s >= interval {TREO_INTERVAL_SECONDS}s).")


            last_api_call_time = time.time() # Cập nhật thời gian NGAY TRƯỚC KHI gọi API

            # --- Kiểm tra lại config và VIP trước khi gọi API (double check) ---
            if not persistent_treo_configs.get(user_id_str, {}).get(target_username) == chat_id:
                 logger.warning(f"[Treo Task Stop] Persistent config for '{task_name}' disappeared before API call. Stopping.")
                 break
            if not is_user_vip(user_id_int):
                 logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP before API call. Stopping task '{task_name}'.")
                 await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired before API call")
                 break
            # --- Kết thúc double check ---

            # 4. Gọi API Follow
            logger.info(f"[Treo Task Run] Task '{task_name}' executing follow for @{target_username}")
            api_result = await call_follow_api(user_id_str, target_username, app.bot.token)
            success = api_result["success"]
            api_message = api_result["message"] or "Không có thông báo từ API."
            gain = 0

            if success:
                consecutive_failures = 0
                if api_result.get("data") and isinstance(api_result["data"], dict):
                    try:
                        gain_str = str(api_result["data"].get("followers_add", "0"))
                        gain_match = re.search(r'[\+\-]?\d+', gain_str) # Tìm số nguyên có dấu hoặc không
                        gain = int(gain_match.group(0)) if gain_match else 0
                        if gain > 0:
                            treo_stats[user_id_str][target_username] += gain
                            logger.info(f"[Treo Task Stats] Task '{task_name}' added {gain} followers. Cycle gain for user: {treo_stats[user_id_str][target_username]}")
                    except (ValueError, TypeError, KeyError, AttributeError) as e_gain:
                         logger.warning(f"[Treo Task Stats] Task '{task_name}' error parsing gain: {e_gain}. Data: {api_result.get('data')}")
                         gain = 0
                else: logger.info(f"[Treo Task Success] Task '{task_name}' successful. API Msg: {api_message[:100]}...")
            else:
                consecutive_failures += 1
                logger.warning(f"[Treo Task Fail] Task '{task_name}' failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}). API Msg: {api_message[:100]}...")
                gain = 0
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(f"[Treo Task Stop] Task '{task_name}' stopping due to {consecutive_failures} consecutive failures.")
                    await stop_treo_task(user_id_str, target_username, context, reason=f"{consecutive_failures} consecutive API failures")
                    try:
                        await app.bot.send_message(
                            chat_id, f"⚠️ {invoking_user_mention}: Treo cho <code>@{html.escape(target_username)}</code> đã tạm dừng do lỗi API liên tục. Vui lòng kiểm tra và thử <code>/treo</code> lại sau.",
                            parse_mode=ParseMode.HTML, disable_notification=True )
                    except Exception as e_send_fail_stop: logger.warning(f"Failed to send consecutive failure stop message for task {task_name}: {e_send_fail_stop}")
                    break # Thoát vòng lặp

            # 5. Gửi thông báo trạng thái
            status_lines = []
            sent_status_message = None
            try:
                user_display_name = invoking_user_mention
                if success:
                    status_lines.append(f"✅ Treo <code>@{html.escape(target_username)}</code> bởi {user_display_name}: Thành công!")
                    status_lines.append(f"➕ Tăng: <b>{gain}</b>")
                    default_success_msgs = ["Follow thành công.", "Success", "success"]
                    if api_message and api_message not in default_success_msgs and gain == 0: # Chỉ hiện message nếu gain = 0 hoặc message lạ
                         status_lines.append(f"💬 <i>{html.escape(api_message[:150])}{'...' if len(api_message)>150 else ''}</i>")
                else:
                    status_lines.append(f"❌ Treo <code>@{html.escape(target_username)}</code> bởi {user_display_name}: Thất bại ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")
                    status_lines.append(f"💬 Lý do: <i>{html.escape(api_message[:150])}{'...' if len(api_message)>150 else ''}</i>")

                status_msg = "\n".join(status_lines)
                sent_status_message = await app.bot.send_message(chat_id=chat_id, text=status_msg, parse_mode=ParseMode.HTML, disable_notification=True)

                if not success and sent_status_message and app.job_queue:
                    job_name_del = f"del_treo_fail_{chat_id}_{sent_status_message.message_id}"
                    app.job_queue.run_once(
                        delete_message_job, TREO_FAILURE_MSG_DELETE_DELAY,
                        data={'chat_id': chat_id, 'message_id': sent_status_message.message_id}, name=job_name_del )
                    logger.debug(f"Scheduled job '{job_name_del}' to delete failure msg {sent_status_message.message_id} in {TREO_FAILURE_MSG_DELETE_DELAY}s.")
            except Forbidden:
                logger.error(f"[Treo Task Stop] Bot Forbidden in chat {chat_id}. Cannot send status for '{task_name}'. Stopping task.")
                await stop_treo_task(user_id_str, target_username, context, reason=f"Bot Forbidden in chat {chat_id}")
                break
            except TelegramError as e_send: logger.error(f"Error sending treo status for '{task_name}' to chat {chat_id}: {e_send}")
            except Exception as e_unexp_send: logger.error(f"Unexpected error sending treo status for '{task_name}' to chat {chat_id}: {e_unexp_send}", exc_info=True)

            # 6. Chờ cho chu kỳ tiếp theo (đã chuyển lên đầu vòng lặp)

    except asyncio.CancelledError:
        logger.info(f"[Treo Task Cancelled] Task '{task_name}' was cancelled externally.")
        # Không cần làm gì thêm ở đây, nơi cancel (stop_treo_task, shutdown) sẽ lo việc cleanup
    except Exception as e:
        logger.error(f"[Treo Task Error] Unexpected error in task '{task_name}': {e}", exc_info=True)
        try:
            await context.application.bot.send_message( # Dùng context.application.bot
                chat_id, f"💥 {invoking_user_mention}: Lỗi nghiêm trọng khi treo <code>@{html.escape(target_username)}</code>. Tác vụ đã dừng. Lỗi: {html.escape(str(e))}",
                parse_mode=ParseMode.HTML, disable_notification=True )
        except Exception as e_send_fatal: logger.error(f"Failed to send fatal error message for task {task_name}: {e_send_fatal}")
        # Cố gắng dừng và xóa config khi có lỗi nghiêm trọng
        await stop_treo_task(user_id_str, target_username, context, reason=f"Unexpected Error: {e}")
    finally:
        logger.info(f"[Treo Task End] Task '{task_name}' finished.")
        # Dọn dẹp task khỏi active_treo_tasks nếu nó vẫn còn đó và đã kết thúc
        # Điều này quan trọng nếu task kết thúc do lỗi mà không qua stop_treo_task
        final_runtime_task = active_treo_tasks.get(user_id_str, {}).get(target_username)
        current_task_obj = None
        try: current_task_obj = asyncio.current_task()
        except RuntimeError: pass # Nếu task đã kết thúc hoàn toàn

        if final_runtime_task is current_task_obj and final_runtime_task and final_runtime_task.done():
             if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                del active_treo_tasks[user_id_str][target_username]
                if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                logger.info(f"[Treo Task Cleanup] Removed finished/failed task '{task_name}' from active tasks dict in finally block.")


# --- Lệnh /treo (VIP - Cập nhật để lưu persistent config) ---
async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bắt đầu treo tự động follow cho một user (chỉ VIP). Lưu config."""
    global persistent_treo_configs, active_treo_tasks
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
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/treo</code> chỉ dành cho <b>VIP</b>.\nDùng <code>/muatt</code> để nâng cấp hoặc <code>/menu</code>."
        await send_temporary_message(update, context, err_msg, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Parse Arguments (Giữ validation username)
    args = context.args
    target_username = None
    err_txt = None
    username_regex = r"^[a-zA-Z0-9._]{2,24}$"

    if not args: err_txt = ("⚠️ Chưa nhập username TikTok cần treo.\n<b>Cú pháp:</b> <code>/treo username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname: err_txt = "⚠️ Username không được trống."
        elif not re.match(username_regex, uname): err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n(2-24 ký tự, chữ, số, '.', '_')")
        elif uname.startswith('.') or uname.endswith('.') or uname.startswith('_') or uname.endswith('_'): err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ (không bắt đầu/kết thúc bằng '.' hoặc '_')."
        elif '..' in uname: err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ (không được chứa '..' liên tiếp)."
        else: target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # 3. Check Giới Hạn và Trạng Thái Treo Hiện Tại
    if target_username:
        vip_limit = get_vip_limit(user_id)
        # Kiểm tra dựa trên persistent config
        persistent_user_configs = persistent_treo_configs.get(user_id_str, {})
        current_treo_count = len(persistent_user_configs)

        if target_username in persistent_user_configs:
            logger.info(f"User {user_id} tried to /treo target @{target_username} which is already in persistent config.")
            msg = f"⚠️ Bạn đã đang treo cho <code>@{html.escape(target_username)}</code> rồi. Dùng <code>/dungtreo {target_username}</code> để dừng."
            await send_temporary_message(update, context, msg, duration=20)
            await delete_user_message(update, context, original_message_id)
            return

        if current_treo_count >= vip_limit:
             logger.warning(f"User {user_id} tried to /treo target @{target_username} but reached limit ({current_treo_count}/{vip_limit}).")
             limit_msg = (f"⚠️ Đã đạt giới hạn treo tối đa! ({current_treo_count}/{vip_limit} tài khoản).\n"
                          f"Dùng <code>/dungtreo &lt;username&gt;</code> để giải phóng slot hoặc nâng cấp gói VIP.")
             await send_temporary_message(update, context, limit_msg, duration=30)
             await delete_user_message(update, context, original_message_id)
             return

        # 4. Bắt đầu Task Treo Mới và Lưu Config
        task = None # Khởi tạo task là None
        try:
            app = context.application
            # Tạo task chạy nền
            # Truyền context vào đây để run_treo_loop có thể truy cập application/bot/job_queue
            task = app.create_task(
                run_treo_loop(user_id_str, target_username, context, chat_id),
                name=f"treo_{user_id_str}_{target_username}_in_{chat_id}"
            )
            # Thêm task vào dict runtime
            active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
            # Thêm vào dict persistent config
            persistent_treo_configs.setdefault(user_id_str, {})[target_username] = chat_id
            # Lưu dữ liệu ngay lập tức
            save_data()
            logger.info(f"Successfully created task '{task.get_name()}' and saved persistent config for user {user_id} -> @{target_username} in chat {chat_id}")

            # Thông báo thành công
            new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
            treo_interval_m = TREO_INTERVAL_SECONDS // 60
            success_msg = (f"✅ <b>Bắt Đầu Treo Thành Công!</b>\n\n"
                           f"👤 Cho: {invoking_user_mention}\n🎯 Target: <code>@{html.escape(target_username)}</code>\n"
                           f"⏳ Tần suất: Mỗi {treo_interval_m} phút\n📊 Slot đã dùng: {new_treo_count}/{vip_limit}")
            await update.message.reply_html(success_msg)
            await delete_user_message(update, context, original_message_id)

        except Exception as e_start_task:
             logger.error(f"Failed to start treo task or save config for user {user_id} target @{target_username}: {e_start_task}", exc_info=True)
             await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi bắt đầu treo cho <code>@{html.escape(target_username)}</code>. Báo Admin.", duration=20)
             await delete_user_message(update, context, original_message_id)
             # --- Rollback ---
             # Hủy task nếu đã tạo và chưa xong
             if task and isinstance(task, asyncio.Task) and not task.done():
                 task.cancel()
                 logger.info(f"Rolled back: Cancelled runtime task for {user_id_str} -> @{target_username} due to start error.")
             # Xóa khỏi runtime dict nếu tồn tại
             if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                 del active_treo_tasks[user_id_str][target_username]
                 if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                 logger.info(f"Rolled back: Removed active task entry for {user_id_str} -> @{target_username}.")
            # Xóa khỏi persistent config nếu đã thêm vào
             if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
                  del persistent_treo_configs[user_id_str][target_username]
                  if not persistent_treo_configs[user_id_str]: del persistent_treo_configs[user_id_str]
                  save_data() # Lưu lại trạng thái persistent đã rollback
                  logger.info(f"Rolled back: Removed persistent config for {user_id_str} -> @{target_username}.")
             # --- End Rollback ---
    else:
        logger.error(f"/treo command for user {user_id}: target_username became None unexpectedly.")
        await send_temporary_message(update, context, "❌ Lỗi không xác định khi xử lý username.", duration=15)
        await delete_user_message(update, context, original_message_id)

# --- Lệnh /dungtreo (VIP - Dùng hàm stop_treo_task) ---
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
    persistent_user_configs = persistent_treo_configs.get(user_id_str, {})
    current_targets = list(persistent_user_configs.keys())

    if not args:
        if not current_targets: err_txt = ("⚠️ Chưa nhập username cần dừng treo.\n<b>Cú pháp:</b> <code>/dungtreo username</code>\n<i>(Bạn không có tài khoản nào đang treo.)</i>")
        else:
            targets_str = ', '.join([f'<code>@{html.escape(t)}</code>' for t in current_targets])
            err_txt = (f"⚠️ Cần chỉ định username muốn dừng treo.\n<b>Cú pháp:</b> <code>/dungtreo username</code>\n"
                       f"<b>Đang treo:</b> {targets_str}")
    else:
        target_username_clean = args[0].strip().lstrip("@")
        if not target_username_clean: err_txt = "⚠️ Username không được để trống."

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    # Dừng Task và Xóa Config
    if target_username_clean:
        logger.info(f"User {user_id} requesting to stop treo for @{target_username_clean}")
        stopped = await stop_treo_task(user_id_str, target_username_clean, context, reason=f"User command /dungtreo by {user_id}")

        await delete_user_message(update, context, original_message_id)

        if stopped:
            new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
            vip_limit = get_vip_limit(user_id)
            is_still_vip = is_user_vip(user_id)
            limit_display = f"{vip_limit}" if is_still_vip else "N/A" # Hiển thị N/A nếu hết VIP
            await update.message.reply_html(f"✅ Đã dừng treo và xóa cấu hình cho <code>@{html.escape(target_username_clean)}</code>.\n(Slot đã dùng: {new_treo_count}/{limit_display})")
        else:
            await send_temporary_message(update, context, f"⚠️ Không tìm thấy cấu hình treo nào cho <code>@{html.escape(target_username_clean)}</code> để dừng.", duration=20)

# --- Lệnh /listtreo ---
async def listtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị danh sách các tài khoản TikTok đang được treo bởi người dùng."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id

    logger.info(f"User {user_id} requested /listtreo in chat {chat_id}")

    # Lấy danh sách từ persistent_treo_configs
    user_treo_configs = persistent_treo_configs.get(user_id_str, {})
    treo_targets = list(user_treo_configs.keys())

    reply_lines = [f"📊 <b>Danh Sách Tài Khoản Đang Treo</b>",
                   f"👤 Cho: {user.mention_html()}"]

    if not treo_targets:
        reply_lines.append("\nBạn hiện không treo tài khoản TikTok nào.")
        if is_user_vip(user_id):
             reply_lines.append("Dùng <code>/treo &lt;username&gt;</code> để bắt đầu.")
        else:
            reply_lines.append("Nâng cấp VIP để sử dụng tính năng này (<code>/muatt</code>).")
    else:
        vip_limit = get_vip_limit(user_id)
        is_currently_vip = is_user_vip(user_id)
        limit_display = f"{vip_limit}" if is_currently_vip else "N/A (VIP hết hạn?)"
        reply_lines.append(f"\n🔍 Số lượng: <b>{len(treo_targets)} / {limit_display}</b> tài khoản")
        for target in sorted(treo_targets):
             # Kiểm tra xem task runtime có đang chạy không (chỉ là thông tin tham khảo)
             is_running = user_id_str in active_treo_tasks and target in active_treo_tasks[user_id_str] and not active_treo_tasks[user_id_str][target].done()
             status_icon = "▶️" if is_running else "⏸️" # Icon trạng thái (ước lượng)
             reply_lines.append(f"  {status_icon} <code>@{html.escape(target)}</code>")
        reply_lines.append("\nℹ️ Dùng <code>/dungtreo &lt;username&gt;</code> để dừng.")
        reply_lines.append("<i>(Trạng thái ▶️/⏸️ chỉ là ước lượng tại thời điểm xem)</i>")

    reply_text = "\n".join(reply_lines)

    try:
        await delete_user_message(update, context, original_message_id)
        await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed to send /listtreo response to user {user_id} in chat {chat_id}: {e}")
        try:
            await delete_user_message(update, context, original_message_id)
            await send_temporary_message(update, context, "❌ Đã có lỗi xảy ra khi lấy danh sách treo.", duration=15)
        except: pass

# --- Job Thống Kê Follow Tăng ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    """Job chạy định kỳ để thống kê và báo cáo user treo tăng follow."""
    global last_stats_report_time, treo_stats
    current_time = time.time()
    if current_time < last_stats_report_time + TREO_STATS_INTERVAL_SECONDS * 0.95 and last_stats_report_time != 0:
        logger.debug(f"[Stats Job] Skipping report, not time yet. Next approx: {datetime.fromtimestamp(last_stats_report_time + TREO_STATS_INTERVAL_SECONDS)}")
        return

    logger.info(f"[Stats Job] Starting statistics report job. Last report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")
    target_chat_id_for_stats = ALLOWED_GROUP_ID

    if not target_chat_id_for_stats:
        logger.info("[Stats Job] ALLOWED_GROUP_ID is not set. Stats report skipped.")
        if treo_stats:
             logger.warning("[Stats Job] Clearing treo_stats because ALLOWED_GROUP_ID is not set.")
             treo_stats.clear()
             save_data()
        return

    stats_snapshot = {}
    if treo_stats:
        try: stats_snapshot = json.loads(json.dumps(treo_stats)) # Deep copy
        except Exception as e_copy: logger.error(f"[Stats Job] Error creating stats snapshot: {e_copy}. Aborting."); return

    treo_stats.clear()
    last_stats_report_time = current_time
    save_data() # Lưu trạng thái mới
    logger.info(f"[Stats Job] Cleared current stats and updated last report time to {datetime.fromtimestamp(last_stats_report_time).isoformat()}. Processing snapshot with {len(stats_snapshot)} users.")

    if not stats_snapshot:
        logger.info("[Stats Job] No stats data found in snapshot. Skipping report content generation.")
        return

    top_gainers = []
    total_gain_all = 0
    for user_id_str, targets in stats_snapshot.items():
        if isinstance(targets, dict):
            for target_username, gain in targets.items():
                if isinstance(gain, int) and gain > 0:
                    top_gainers.append((gain, str(user_id_str), str(target_username)))
                    total_gain_all += gain
                elif gain > 0: logger.warning(f"[Stats Job] Invalid gain type ({type(gain)}) for {user_id_str}->{target_username}. Skipping.")
        else: logger.warning(f"[Stats Job] Invalid target structure for user {user_id_str} in snapshot. Skipping.")

    if not top_gainers:
        logger.info("[Stats Job] No positive gains found after processing snapshot. Skipping report generation.")
        return

    top_gainers.sort(key=lambda x: x[0], reverse=True)

    report_lines = [f"📊 <b>Thống Kê Tăng Follow (24 Giờ Qua)</b> 📊",
                    f"<i>(Tổng cộng: <b>{total_gain_all:,}</b> follow được tăng bởi các tài khoản đang treo)</i>",
                    "\n🏆 <b>Top Tài Khoản Treo Hiệu Quả Nhất:</b>"]

    num_top_to_show = 10
    displayed_count = 0
    user_mentions_cache = {}

    app = context.application # Lấy application để gọi bot.get_chat
    for gain, user_id_str_gain, target_username_gain in top_gainers[:num_top_to_show]:
        user_mention = user_mentions_cache.get(user_id_str_gain)
        if not user_mention:
            try:
                user_info = await app.bot.get_chat(int(user_id_str_gain))
                m = user_info.mention_html()
                user_mention = m if m else f"User <code>{user_id_str_gain}</code>"
            except Exception as e_get_chat:
                logger.warning(f"[Stats Job] Failed to get mention for user {user_id_str_gain}: {e_get_chat}")
                user_mention = f"User <code>{user_id_str_gain}</code>"
            user_mentions_cache[user_id_str_gain] = user_mention

        report_lines.append(f"  🏅 <b>+{gain:,} follow</b> cho <code>@{html.escape(target_username_gain)}</code> (Treo bởi: {user_mention})")
        displayed_count += 1

    if not displayed_count: report_lines.append("  <i>Không có dữ liệu tăng follow đáng kể.</i>")

    treo_interval_m = TREO_INTERVAL_SECONDS // 60
    report_lines.append(f"\n🕒 <i>Cập nhật tự động sau mỗi 24 giờ. Treo chạy mỗi {treo_interval_m} phút.</i>")

    report_text = "\n".join(report_lines)

    try:
        await app.bot.send_message(chat_id=target_chat_id_for_stats, text=report_text,
                                   parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)
        logger.info(f"[Stats Job] Successfully sent statistics report to group {target_chat_id_for_stats}.")
    except Exception as e: logger.error(f"[Stats Job] Failed to send statistics report to group {target_chat_id_for_stats}: {e}", exc_info=True)

    logger.info("[Stats Job] Statistics report job finished.")


# --- Hàm helper bất đồng bộ để dừng task khi tắt bot ---
async def shutdown_async_tasks(tasks_to_cancel: list[asyncio.Task], timeout: float = 2.0):
    """Helper async function to cancel and wait for tasks during shutdown."""
    if not tasks_to_cancel:
        logger.info("[Shutdown] No active treo tasks found to cancel.")
        return

    logger.info(f"[Shutdown] Attempting to gracefully cancel {len(tasks_to_cancel)} active treo tasks with {timeout}s timeout...")
    # Hủy tất cả các task
    for task in tasks_to_cancel:
        if task and not task.done():
            task.cancel()

    # Chờ các task hoàn thành (hoặc bị hủy) với timeout
    results = await asyncio.gather(*[asyncio.wait_for(task, timeout=timeout) for task in tasks_to_cancel], return_exceptions=True)
    logger.info("[Shutdown] Finished waiting for treo task cancellations.")

    cancelled_count, errors_count, finished_count = 0, 0, 0
    for i, result in enumerate(results):
        task = tasks_to_cancel[i]
        task_name = f"Task_{i}"
        try:
             if task: task_name = task.get_name() or task_name
        except Exception: pass

        if isinstance(result, asyncio.CancelledError):
            cancelled_count += 1
            logger.info(f"[Shutdown] Task '{task_name}' confirmed cancelled.")
        elif isinstance(result, asyncio.TimeoutError):
            errors_count += 1
            logger.warning(f"[Shutdown] Task '{task_name}' timed out during cancellation.")
        elif isinstance(result, Exception):
            errors_count += 1
            logger.error(f"[Shutdown] Error occurred in task '{task_name}' during processing: {result}", exc_info=False)
        else:
            finished_count += 1
            logger.debug(f"[Shutdown] Task '{task_name}' finished normally.") # Should be rare

    logger.info(f"[Shutdown] Task summary: {cancelled_count} cancelled, {errors_count} errors/timeouts, {finished_count} finished normally.")

# --- Main Function (Cập nhật để khôi phục task treo) ---
def main() -> None:
    """Khởi động và chạy bot."""
    start_time = time.time()
    print("--- Bot DinoTool Starting ---"); print(f"Timestamp: {datetime.now().isoformat()}")
    print("\n--- Configuration Summary ---")
    # (Các dòng print cấu hình khác giữ nguyên)
    print(f"Treo: Interval={TREO_INTERVAL_SECONDS / 60:.1f}m | Fail Delete Delay={TREO_FAILURE_MSG_DELETE_DELAY}s | Stats Interval={TREO_STATS_INTERVAL_SECONDS / 3600:.1f}h")
    print(f"Group Link (for menu): {GROUP_LINK if GROUP_LINK != 'YOUR_GROUP_INVITE_LINK' else 'Not Set!'}")
    print("-" * 30)

    print("Loading persistent data...")
    load_data() # Load data trước khi cấu hình application
    print(f"Load complete. Keys: {len(valid_keys)}, Activated: {len(activated_users)}, VIPs: {len(vip_users)}")
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
            jq.run_repeating(report_treo_stats, interval=TREO_STATS_INTERVAL_SECONDS, first=300, name="report_treo_stats_job")
            logger.info(f"Scheduled statistics report job every {TREO_STATS_INTERVAL_SECONDS / 3600:.1f} hours (first run in 5 min).")
        else: logger.info("Statistics report job skipped (ALLOWED_GROUP_ID not set).")
    else: logger.error("JobQueue is not available. Scheduled jobs will not run.")

    # Register Handlers
    application.add_handler(CommandHandler(("start", "menu"), start_command)) # /start và /menu cùng gọi hàm start_command
    application.add_handler(CommandHandler("lenh", lenh_command))
    application.add_handler(CommandHandler("getkey", getkey_command))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command))
    application.add_handler(CommandHandler("tim", tim_command))
    application.add_handler(CommandHandler("fl", fl_command))
    application.add_handler(CommandHandler("muatt", muatt_command))
    application.add_handler(CommandHandler("treo", treo_command))
    application.add_handler(CommandHandler("dungtreo", dungtreo_command))
    application.add_handler(CommandHandler("listtreo", listtreo_command))
    application.add_handler(CommandHandler("addtt", addtt_command))

    # Callback handler cho menu và nút gửi bill
    application.add_handler(CallbackQueryHandler(menu_callback_handler, pattern="^show_(muatt|lenh)$")) # Menu callbacks
    application.add_handler(CallbackQueryHandler(prompt_send_bill_callback, pattern="^prompt_send_bill_\d+$")) # Bill prompt callback

    # Message handler cho ảnh bill (Ưu tiên cao)
    photo_bill_filter = (filters.PHOTO | filters.Document.IMAGE) & (~filters.COMMAND) & filters.UpdateType.MESSAGE
    application.add_handler(MessageHandler(photo_bill_filter, handle_photo_bill), group=-1)
    logger.info("Registered photo/bill handler (priority -1) for pending users.")

    # --- Khởi động lại các task treo đã lưu ---
    print("\nRestarting persistent treo tasks...")
    restored_count = 0
    users_to_cleanup = []
    tasks_to_create_data = [] # List of tuples: (user_id_str, target_username, chat_id_int)

    # Tạo bản sao của persistent_treo_configs để lặp an toàn
    persistent_treo_snapshot = dict(persistent_treo_configs)

    if persistent_treo_snapshot:
        for user_id_str, targets_for_user in persistent_treo_snapshot.items():
            try:
                user_id_int = int(user_id_str)
                if not is_user_vip(user_id_int):
                    logger.warning(f"[Restore] User {user_id_str} from persistent config is no longer VIP. Scheduling config cleanup.")
                    users_to_cleanup.append(user_id_str)
                    continue

                vip_limit = get_vip_limit(user_id_int)
                current_user_restored_count = 0
                # Tạo bản sao targets để lặp an toàn
                targets_snapshot = dict(targets_for_user)

                for target_username, chat_id_int in targets_snapshot.items():
                    # Kiểm tra limit TRƯỚC KHI thêm vào danh sách tạo task
                    if current_user_restored_count >= vip_limit:
                         logger.warning(f"[Restore] User {user_id_str} reached VIP limit ({vip_limit}). Skipping persistent target @{target_username}.")
                         # Xóa config dư thừa khỏi persistent data GỐC
                         if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
                              del persistent_treo_configs[user_id_str][target_username]
                              # Không save_data() ở đây, save sau khi dọn dẹp xong users_to_cleanup
                         continue # Bỏ qua target này

                    # Kiểm tra xem task đã chạy chưa (trường hợp restart cực nhanh)
                    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                        logger.info(f"[Restore] Task for {user_id_str} -> @{target_username} seems already active (runtime). Skipping restore.")
                        current_user_restored_count += 1 # Vẫn tính vào limit
                        continue

                    logger.info(f"[Restore] Scheduling restore for treo task: user {user_id_str} -> @{target_username} in chat {chat_id_int}")
                    tasks_to_create_data.append((user_id_str, target_username, chat_id_int))
                    current_user_restored_count += 1

            except ValueError:
                logger.error(f"[Restore] Invalid user_id '{user_id_str}' found in persistent_treo_configs. Scheduling cleanup.")
                users_to_cleanup.append(user_id_str)
            except Exception as e_outer_restore:
                logger.error(f"[Restore] Unexpected error processing persistent treo config for user {user_id_str}: {e_outer_restore}", exc_info=True)
                users_to_cleanup.append(user_id_str) # Đánh dấu để dọn dẹp nếu có lỗi

    # Dọn dẹp config của user không còn VIP hoặc ID lỗi
    cleaned_persistent_configs = False
    if users_to_cleanup:
        unique_users_to_cleanup = set(users_to_cleanup)
        logger.info(f"[Restore] Cleaning up persistent treo configs for {len(unique_users_to_cleanup)} non-VIP or invalid users...")
        cleaned_count = 0
        for user_id_str_clean in unique_users_to_cleanup:
            if user_id_str_clean in persistent_treo_configs:
                del persistent_treo_configs[user_id_str_clean]
                cleaned_count += 1
                cleaned_persistent_configs = True
        if cleaned_persistent_configs:
            logger.info(f"Removed persistent configs for {cleaned_count} users.")

    # Lưu lại dữ liệu nếu có config bị xóa do hết VIP/lỗi hoặc do vượt limit
    if cleaned_persistent_configs or any(len(persistent_treo_configs.get(uid, {})) < len(persistent_treo_snapshot.get(uid, {})) for uid in persistent_treo_snapshot):
        logger.info("[Restore] Saving data after cleaning up non-VIP/invalid/over-limit persistent configs.")
        save_data()

    # Tạo các task treo đã lên lịch
    if tasks_to_create_data:
        logger.info(f"[Restore] Creating {len(tasks_to_create_data)} restored treo tasks...")
        for user_id_str_create, target_username_create, chat_id_int_create in tasks_to_create_data:
            try:
                # Tạo context giả lập đủ dùng
                default_context = ContextTypes.DEFAULT_TYPE(application=application, chat_id=None, user_id=None)
                task = application.create_task(
                    run_treo_loop(user_id_str_create, target_username_create, default_context, chat_id_int_create),
                    name=f"treo_{user_id_str_create}_{target_username_create}_in_{chat_id_int_create}_restored"
                )
                active_treo_tasks.setdefault(user_id_str_create, {})[target_username_create] = task
                restored_count += 1
            except Exception as e_create:
                logger.error(f"[Restore] Failed to create restored task for {user_id_str_create} -> @{target_username_create}: {e_create}", exc_info=True)
                # Không xóa config ở đây vì có thể chỉ là lỗi tạm thời khi tạo task
                # Nếu task không chạy được, vòng lặp run_treo_loop sẽ tự xử lý hoặc lần restart sau sẽ thử lại

    print(f"Successfully restored and started {restored_count} treo tasks."); print("-" * 30)
    # --- Kết thúc khôi phục task ---

    print("\nBot initialization complete. Starting polling...")
    logger.info("Bot initialization complete. Starting polling...")
    run_duration = time.time() - start_time; print(f"(Initialization took {run_duration:.2f} seconds)")

    # Chạy bot
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt: print("\nCtrl+C detected. Stopping bot gracefully..."); logger.info("KeyboardInterrupt detected. Stopping bot...")
    except Exception as e: print(f"\nCRITICAL ERROR: Bot stopped due to an unhandled exception: {e}"); logger.critical(f"CRITICAL ERROR: Bot stopped due to unhandled exception: {e}", exc_info=True)
    finally:
        print("\nInitiating shutdown sequence..."); logger.info("Initiating shutdown sequence...")
        # Thu thập các task đang chạy từ active_treo_tasks
        tasks_to_stop_on_shutdown = []
        if active_treo_tasks:
            logger.info("[Shutdown] Collecting active runtime treo tasks...")
            for targets in list(active_treo_tasks.values()): # Lặp qua bản sao
                for task in list(targets.values()):
                    if task and isinstance(task, asyncio.Task) and not task.done():
                        tasks_to_stop_on_shutdown.append(task)

        # Hủy các task đang chạy
        if tasks_to_stop_on_shutdown:
            print(f"[Shutdown] Found {len(tasks_to_stop_on_shutdown)} active runtime treo tasks. Attempting cancellation...")
            try:
                 loop = asyncio.get_running_loop() # Lấy loop đang chạy
                 # Chạy hàm helper để hủy và chờ (trong loop đang chạy)
                 loop.create_task(shutdown_async_tasks(tasks_to_stop_on_shutdown, timeout=2.0))
                 # Chờ một chút để task hủy có thời gian chạy
                 # Lưu ý: Đây không phải là cách chờ hoàn hảo, nhưng tốt hơn là không chờ gì cả
                 # loop.run_until_complete(asyncio.sleep(2.5)) # Tránh dùng run_until_complete ở đây nếu loop đang run_polling
                 print("[Shutdown] Cancellation tasks scheduled. Proceeding with final save...")
                 # Không chờ hoàn tất ở đây để tránh block shutdown quá lâu
            except RuntimeError as e_runtime:
                 logger.error(f"[Shutdown] RuntimeError getting/using event loop: {e_runtime}. Attempting direct cancellation.")
                 for task in tasks_to_stop_on_shutdown: task.cancel()
            except Exception as e_shutdown:
                 logger.error(f"[Shutdown] Error during async task cancellation scheduling: {e_shutdown}", exc_info=True)
                 for task in tasks_to_stop_on_shutdown: task.cancel() # Fallback
        else:
            print("[Shutdown] No active runtime treo tasks found.")

        # Lưu dữ liệu lần cuối (quan trọng để lưu trạng thái persistent)
        print("[Shutdown] Attempting final data save..."); logger.info("Attempting final data save...")
        save_data()
        print("[Shutdown] Final data save attempt complete.")
        print("Bot has stopped."); logger.info("Bot has stopped."); print(f"Shutdown timestamp: {datetime.now().isoformat()}")

if __name__ == "__main__":
    try:
        # Load data lần đầu để kiểm tra lỗi file trước khi khởi động hoàn toàn
        load_data()
        # Chạy hàm main chính
        main()
    except Exception as e_fatal:
        print(f"\nFATAL ERROR: Could not execute main function: {e_fatal}")
        logger.critical(f"FATAL ERROR preventing main execution: {e_fatal}", exc_info=True)
        try:
            with open("fatal_error.log", "a", encoding='utf-8') as f:
                import traceback
                f.write(f"\n--- {datetime.now().isoformat()} ---\nFATAL ERROR: {e_fatal}\n")
                traceback.print_exc(file=f)
                f.write("-" * 30 + "\n")
        except Exception as e_log: print(f"Additionally, failed to write fatal error to log file: {e_log}")

