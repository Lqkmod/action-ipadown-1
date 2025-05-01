
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
# !!! THAY THẾ CÁC GIÁ TRỊ PLACEHOLDER BÊN DƯỚI BẰNG GIÁ TRỊ THỰC TẾ CỦA BẠN !!!
BOT_TOKEN = "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" # <--- TOKEN CỦA BOT TELEGRAM CỦA BẠN
API_KEY = "khangdino99" # <--- API KEY TIM (NẾU CÓ, DÙNG CHO LỆNH /tim) - Có thể để trống nếu không dùng /tim
ADMIN_USER_ID = 7193749511 # <<< --- ID TELEGRAM SỐ CỦA ADMIN (Lấy từ @userinfobot)
BILL_FORWARD_TARGET_ID = 7193749511 # <<< --- ID TELEGRAM SỐ CỦA NƠI NHẬN BILL (VD: ID của @khangtaixiu_bot hoặc Admin)
ALLOWED_GROUP_ID = -1002191171631 # <--- ID NHÓM CHÍNH (SỐ ÂM) hoặc None (Nếu None, một số tính năng báo cáo/nhắc nhở nhóm sẽ tắt)
GROUP_LINK = "https://t.me/dinotool" # <<<--- LINK MỜI NHÓM CỦA BẠN (Nếu có ALLOWED_GROUP_ID)
LINK_SHORTENER_API_KEY = "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" # <--- API KEY YEUMONEY CỦA BẠN
QR_CODE_URL = "https://i.imgur.com/49iY7Ft.jpeg" # <--- LINK ẢNH QR CODE THANH TOÁN CỦA BẠN
BANK_ACCOUNT = "KHANGDINO" # <--- SỐ TÀI KHOẢN NGÂN HÀNG
BANK_NAME = "VCB BANK" # <--- TÊN NGÂN HÀNG (VD: VCB, MB, MOMO)
ACCOUNT_NAME = "LE QUOC KHANG" # <--- TÊN CHỦ TÀI KHOẢN
# ----------------------------------------------------------------------------

# --- Các cấu hình khác (Ít thay đổi) ---
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # Link đích chứa key
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # API Yeumoney
PAYMENT_NOTE_PREFIX = "VIP DinoTool ID" # Nội dung chuyển khoản: "VIP DinoTool ID <user_id>"
DATA_FILE = "bot_persistent_data.json" # File lưu dữ liệu
LOG_FILE = "bot.log" # File log

# --- Thời gian (Giây) ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 phút (/tim, /fl)
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 phút (/getkey)
KEY_EXPIRY_SECONDS = 6 * 3600   # 6 giờ (Key chưa nhập)
ACTIVATION_DURATION_SECONDS = 6 * 3600 # 6 giờ (Sau khi nhập key)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 giờ (Job dọn dẹp)
TREO_INTERVAL_SECONDS = 900 # 15 phút (Khoảng cách giữa các lần gọi API /treo)
TREO_FAILURE_MSG_DELETE_DELAY = 15 # 15 giây (Xóa tin nhắn treo thất bại)
TREO_STATS_INTERVAL_SECONDS = 24 * 3600 # 24 giờ (Thống kê follow tăng qua job)
USER_GAIN_HISTORY_SECONDS = 24 * 3600 # Lưu lịch sử gain trong 24 giờ cho /xemfl24h
PENDING_BILL_TIMEOUT_SECONDS = 15 * 60 # 15 phút (Timeout chờ gửi bill sau khi bấm nút)

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM (Cần API_KEY)
FOLLOW_API_URL_BASE = "https://api.thanhtien.site/lynk/dino/telefl.php" # API FOLLOW MỚI

# --- Thông tin VIP ---
VIP_PRICES = {
    # days_key: {"price": "Display Price", "limit": max_treo_users, "duration_days": days}
    15: {"price": "15.000 VND", "limit": 2, "duration_days": 15},
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
}

# --- Biến toàn cục (Sẽ được load/save) ---
user_tim_cooldown = {} # {user_id_str: timestamp}
user_fl_cooldown = defaultdict(dict) # {user_id_str: {target_username: timestamp}}
user_getkey_cooldown = {} # {user_id_str: timestamp}
valid_keys = {} # {key: {"user_id_generator": ..., "expiry_time": ..., "used_by": ..., "activation_time": ...}}
activated_users = {} # {user_id_str: expiry_timestamp} - Người dùng kích hoạt bằng key
vip_users = {} # {user_id_str: {"expiry": expiry_timestamp, "limit": user_limit}} - Người dùng VIP
persistent_treo_configs = {} # {user_id_str: {target_username: chat_id}} - Lưu để khôi phục sau restart
treo_stats = defaultdict(lambda: defaultdict(int)) # {user_id_str: {target_username: gain_since_last_report}} - Dùng cho job thống kê
user_daily_gains = defaultdict(lambda: defaultdict(list)) # {uid_str: {target: [(ts, gain)]}} - Dùng cho /xemfl24h
last_stats_report_time = 0 # Thời điểm báo cáo thống kê gần nhất

# --- Biến Runtime (Không lưu) ---
active_treo_tasks = {} # {user_id_str: {target_username: asyncio.Task}} - Lưu các task /treo đang chạy
pending_bill_user_ids = set() # Set of user_ids (int) - Chờ gửi bill

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler() # Log ra console
    ]
)
# Giảm log nhiễu từ thư viện http và telegram.ext scheduling
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.JobQueue").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.Application").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# --- Kiểm tra cấu hình quan trọng ---
if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN": logger.critical("!!! BOT_TOKEN chưa được cấu hình !!!"); exit(1)
if not isinstance(ADMIN_USER_ID, int) or ADMIN_USER_ID == 123456789: logger.critical("!!! ADMIN_USER_ID chưa được cấu hình hoặc không hợp lệ !!!"); exit(1)
if not isinstance(BILL_FORWARD_TARGET_ID, int) or BILL_FORWARD_TARGET_ID == 123456789: logger.critical("!!! BILL_FORWARD_TARGET_ID chưa được cấu hình hoặc không hợp lệ (Phải là ID số) !!!"); exit(1)
if not LINK_SHORTENER_API_KEY: logger.warning("!!! LINK_SHORTENER_API_KEY chưa được cấu hình. Lệnh /getkey sẽ không hoạt động. !!!")
if not QR_CODE_URL or not QR_CODE_URL.startswith("http"): logger.warning("!!! QR_CODE_URL không hợp lệ. Ảnh QR sẽ không hiển thị trong /muatt. !!!")
if not BANK_ACCOUNT or not BANK_NAME or not ACCOUNT_NAME: logger.warning("!!! Thông tin ngân hàng (BANK_ACCOUNT, BANK_NAME, ACCOUNT_NAME) chưa đầy đủ. /muatt sẽ thiếu thông tin. !!!")
if ALLOWED_GROUP_ID and (not GROUP_LINK or GROUP_LINK == "YOUR_GROUP_INVITE_LINK"): logger.warning("!!! Có ALLOWED_GROUP_ID nhưng GROUP_LINK chưa được cấu hình. Nút 'Nhóm Chính' sẽ không hoạt động. !!!")

logger.info("--- Cấu hình cơ bản đã được kiểm tra ---")
logger.info(f"Admin ID: {ADMIN_USER_ID}")
logger.info(f"Bill Forward Target: {BILL_FORWARD_TARGET_ID}")
logger.info(f"Allowed Group ID: {ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else 'Không giới hạn'}")
logger.info(f"Treo Interval: {TREO_INTERVAL_SECONDS / 60:.1f} phút")
logger.info(f"VIP Packages: {list(VIP_PRICES.keys())} ngày")


# --- Hàm lưu/tải dữ liệu (Đã cập nhật) ---
def save_data():
    global persistent_treo_configs, user_daily_gains
    string_key_activated_users = {str(k): v for k, v in activated_users.items()}
    string_key_tim_cooldown = {str(k): v for k, v in user_tim_cooldown.items()}
    string_key_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in user_fl_cooldown.items()}
    string_key_getkey_cooldown = {str(k): v for k, v in user_getkey_cooldown.items()}
    string_key_vip_users = {str(k): v for k, v in vip_users.items()}
    string_key_treo_stats = {str(uid): dict(targets) for uid, targets in treo_stats.items()}
    string_key_persistent_treo = {
        str(uid): {str(target): int(chatid) for target, chatid in configs.items()}
        for uid, configs in persistent_treo_configs.items() if configs
    }
    string_key_daily_gains = {
        str(uid): {
            str(target): [(float(ts), int(g)) for ts, g in gain_list if isinstance(ts, (int, float)) and isinstance(g, int)]
            for target, gain_list in targets_data.items() if gain_list
        }
        for uid, targets_data in user_daily_gains.items() if targets_data
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
        "persistent_treo_configs": string_key_persistent_treo,
        "user_daily_gains": string_key_daily_gains
    }
    try:
        temp_file = DATA_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        os.replace(temp_file, DATA_FILE)
        logger.debug(f"Data saved successfully to {DATA_FILE}")
    except Exception as e:
        logger.error(f"Failed to save data to {DATA_FILE}: {e}", exc_info=True)
        if os.path.exists(temp_file):
            try: os.remove(temp_file)
            except Exception as e_rem: logger.error(f"Failed to remove temporary save file {temp_file}: {e_rem}")

def load_data():
    global valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown, \
           treo_stats, last_stats_report_time, persistent_treo_configs, user_daily_gains
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                valid_keys = data.get("valid_keys", {})
                activated_users = data.get("activated_users", {})
                vip_users = data.get("vip_users", {})

                all_cooldowns = data.get("user_cooldowns", {})
                user_tim_cooldown = all_cooldowns.get("tim", {})
                user_fl_cooldown = defaultdict(dict)
                loaded_fl_cooldown = all_cooldowns.get("fl", {})
                if isinstance(loaded_fl_cooldown, dict): user_fl_cooldown.update(loaded_fl_cooldown)
                user_getkey_cooldown = all_cooldowns.get("getkey", {})

                loaded_stats = data.get("treo_stats", {})
                treo_stats = defaultdict(lambda: defaultdict(int))
                if isinstance(loaded_stats, dict):
                    for uid_str, targets in loaded_stats.items():
                        if isinstance(targets, dict):
                            for target, gain in targets.items():
                                try: treo_stats[str(uid_str)][str(target)] = int(gain)
                                except (ValueError, TypeError): logger.warning(f"Skipping invalid treo stat entry: user {uid_str}, target {target}, gain {gain}")
                        else: logger.warning(f"Invalid targets type for user {uid_str} in treo_stats: {type(targets)}")

                last_stats_report_time = data.get("last_stats_report_time", 0)

                loaded_persistent_treo = data.get("persistent_treo_configs", {})
                persistent_treo_configs = {}
                if isinstance(loaded_persistent_treo, dict):
                    for uid_str, configs in loaded_persistent_treo.items():
                        user_id_key = str(uid_str)
                        persistent_treo_configs[user_id_key] = {}
                        if isinstance(configs, dict):
                            for target, chatid in configs.items():
                                try: persistent_treo_configs[user_id_key][str(target)] = int(chatid)
                                except (ValueError, TypeError): logger.warning(f"Skipping invalid persistent treo config entry: user {user_id_key}, target {target}, chatid {chatid}")
                        else: logger.warning(f"Invalid config type for user {user_id_key} in persistent_treo_configs: {type(configs)}. Skipping.")
                else: logger.warning(f"persistent_treo_configs in data file is not a dict: {type(loaded_persistent_treo)}. Initializing empty.")

                loaded_daily_gains = data.get("user_daily_gains", {})
                user_daily_gains = defaultdict(lambda: defaultdict(list))
                if isinstance(loaded_daily_gains, dict):
                    for uid_str, targets_data in loaded_daily_gains.items():
                        user_id_key = str(uid_str)
                        if isinstance(targets_data, dict):
                            for target, gain_list in targets_data.items():
                                target_key = str(target)
                                if isinstance(gain_list, list):
                                    valid_gains = []
                                    for item in gain_list:
                                        try:
                                            if isinstance(item, (list, tuple)) and len(item) == 2:
                                                ts = float(item[0])
                                                g = int(item[1])
                                                valid_gains.append((ts, g))
                                            else: logger.warning(f"Skipping invalid gain entry format for user {user_id_key}, target {target_key}: {item}")
                                        except (ValueError, TypeError, IndexError): logger.warning(f"Skipping invalid gain entry value for user {user_id_key}, target {target_key}: {item}")
                                    if valid_gains: user_daily_gains[user_id_key][target_key].extend(valid_gains)
                                else: logger.warning(f"Invalid gain_list type for user {user_id_key}, target {target_key}: {type(gain_list)}. Skipping.")
                        else: logger.warning(f"Invalid targets_data type for user {user_id_key} in user_daily_gains: {type(targets_data)}. Skipping.")
                else: logger.warning(f"user_daily_gains in data file is not a dict: {type(loaded_daily_gains)}. Initializing empty.")

                logger.info(f"Data loaded successfully from {DATA_FILE}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            valid_keys, activated_users, vip_users = {}, {}, {}
            user_tim_cooldown, user_getkey_cooldown = {}, {}
            user_fl_cooldown = defaultdict(dict)
            treo_stats = defaultdict(lambda: defaultdict(int))
            last_stats_report_time = 0
            persistent_treo_configs = {}
            user_daily_gains = defaultdict(lambda: defaultdict(list))
    except (json.JSONDecodeError, TypeError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Using empty data structures.", exc_info=True)
        valid_keys, activated_users, vip_users = {}, {}, {}
        user_tim_cooldown, user_getkey_cooldown = {}, {}
        user_fl_cooldown = defaultdict(dict)
        treo_stats = defaultdict(lambda: defaultdict(int))
        last_stats_report_time = 0
        persistent_treo_configs = {}
        user_daily_gains = defaultdict(lambda: defaultdict(list))

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn người dùng một cách an toàn."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return

    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except Forbidden: logger.debug(f"Cannot delete message {msg_id_to_delete} in chat {original_chat_id}. Bot might not be admin or message too old.")
    except BadRequest as e:
        if "Message to delete not found" in str(e).lower() or \
           "message can't be deleted" in str(e).lower() or \
           "MESSAGE_ID_INVALID" in str(e).upper() or \
           "message identifier is not specified" in str(e).lower(): logger.debug(f"Could not delete message {msg_id_to_delete} (already deleted?): {e}")
        else: logger.warning(f"BadRequest error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}")
    except Exception as e: logger.error(f"Unexpected error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}", exc_info=True)

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
        except Forbidden: logger.info(f"Job '{job_name}' cannot delete message {message_id}. Bot might not be admin or message too old.")
        except BadRequest as e:
            if "Message to delete not found" in str(e).lower() or "message can't be deleted" in str(e).lower(): logger.info(f"Job '{job_name}' could not delete message {message_id} (already deleted?): {e}")
            else: logger.warning(f"Job '{job_name}' BadRequest deleting message {message_id}: {e}")
        except TelegramError as e: logger.warning(f"Job '{job_name}' Telegram error deleting message {message_id}: {e}")
        except Exception as e: logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else: logger.warning(f"Job '{job_name}' called missing chat_id or message_id.")

async def send_temporary_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, duration: int = 15, parse_mode: str = ParseMode.HTML, reply: bool = True):
    """Gửi tin nhắn và tự động xóa sau một khoảng thời gian."""
    if not update or not update.effective_chat: return
    chat_id = update.effective_chat.id
    sent_message = None
    try:
        reply_to_msg_id = update.message.message_id if reply and update.message else None
        send_params = {'chat_id': chat_id, 'text': text, 'parse_mode': parse_mode, 'disable_web_page_preview': True}
        if reply_to_msg_id: send_params['reply_to_message_id'] = reply_to_msg_id

        try: sent_message = await context.bot.send_message(**send_params)
        except BadRequest as e:
            if "reply message not found" in str(e).lower() and reply_to_msg_id:
                 logger.debug(f"Reply message {reply_to_msg_id} not found for temporary message. Sending without reply.")
                 del send_params['reply_to_message_id']
                 sent_message = await context.bot.send_message(**send_params)
            else: raise

        if sent_message and context.job_queue:
            job_name = f"del_temp_{chat_id}_{sent_message.message_id}"
            context.job_queue.run_once( delete_message_job, duration, data={'chat_id': chat_id, 'message_id': sent_message.message_id}, name=job_name)
            logger.debug(f"Scheduled job '{job_name}' to delete message {sent_message.message_id} in {duration}s")
    except (BadRequest, Forbidden, TelegramError) as e: logger.error(f"Error sending temporary message to {chat_id}: {e}")
    except Exception as e: logger.error(f"Unexpected error in send_temporary_message to {chat_id}: {e}", exc_info=True)

def generate_random_key(length=8):
    """Tạo key ngẫu nhiên dạng Dinotool-xxxx."""
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"Dinotool-{random_part}"

# --- Hàm dừng task treo (Đã cập nhật) ---
async def stop_treo_task(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown") -> bool:
    """Dừng một task treo cụ thể (runtime VÀ persistent). Trả về True nếu dừng/xóa thành công, False nếu không tìm thấy."""
    global persistent_treo_configs, active_treo_tasks
    task = None
    was_active_runtime = False
    removed_persistent = False
    data_saved = False
    user_id_str = str(user_id_str)
    target_username = str(target_username)

    # 1. Dừng task đang chạy (runtime)
    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
        task = active_treo_tasks[user_id_str].get(target_username)
        task_name = f"task_{user_id_str}_{target_username}"
        if task and isinstance(task, asyncio.Task) and not task.done():
            was_active_runtime = True
            logger.info(f"[Treo Task Stop] Attempting to cancel RUNTIME task '{task_name}'. Reason: {reason}")
            task.cancel()
            try: await asyncio.wait_for(task, timeout=1.0)
            except asyncio.CancelledError: logger.info(f"[Treo Task Stop] Runtime Task '{task_name}' confirmed cancelled.")
            except asyncio.TimeoutError: logger.warning(f"[Treo Task Stop] Timeout waiting for cancelled runtime task '{task_name}'.")
            except Exception as e: logger.error(f"[Treo Task Stop] Error awaiting cancelled runtime task '{task_name}': {e}")
        # Luôn xóa khỏi runtime dict nếu key tồn tại
        del active_treo_tasks[user_id_str][target_username]
        if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
        logger.info(f"[Treo Task Stop] Removed task entry for {user_id_str} -> @{target_username} from active (runtime) tasks.")
    else:
        logger.debug(f"[Treo Task Stop] No active runtime task found for {user_id_str} -> @{target_username}. Checking persistent config.")

    # 2. Xóa khỏi persistent config (nếu có)
    if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
        del persistent_treo_configs[user_id_str][target_username]
        if not persistent_treo_configs[user_id_str]: del persistent_treo_configs[user_id_str]
        logger.info(f"[Treo Task Stop] Removed entry for {user_id_str} -> @{target_username} from persistent_treo_configs.")
        save_data() # Lưu ngay sau khi thay đổi cấu hình persistent
        data_saved = True
        removed_persistent = True
    else:
         logger.debug(f"[Treo Task Stop] Entry for {user_id_str} -> @{target_username} not found in persistent_treo_configs.")

    # Trả về True nếu task runtime bị hủy HOẶC config persistent bị xóa
    return was_active_runtime or removed_persistent

# --- Hàm dừng TẤT CẢ task treo cho user (Mới) ---
async def stop_all_treo_tasks_for_user(user_id_str: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown") -> int:
    """Dừng tất cả các task treo của một user (runtime và persistent). Trả về số lượng task/config đã dừng/xóa thành công."""
    stopped_count = 0
    user_id_str = str(user_id_str)

    # Lấy danh sách target từ persistent config để đảm bảo xóa hết config
    targets_in_persistent = list(persistent_treo_configs.get(user_id_str, {}).keys())
    targets_in_runtime_only = list(active_treo_tasks.get(user_id_str, {}).keys() - set(targets_in_persistent)) # Những task chạy mà ko có config

    targets_to_process = set(targets_in_persistent) | set(targets_in_runtime_only) # Kết hợp cả hai

    if not targets_to_process:
        logger.info(f"No persistent treo configs or unexpected runtime tasks found for user {user_id_str} to stop.")
        return 0

    logger.info(f"Stopping all {len(targets_to_process)} potential treo tasks/configs for user {user_id_str}. Reason: {reason}")
    if targets_in_runtime_only:
        logger.warning(f"Found {len(targets_in_runtime_only)} runtime tasks without persistent config for user {user_id_str}: {targets_in_runtime_only}. Attempting stop.")

    # Lặp qua set target
    for target_username in targets_to_process:
        if await stop_treo_task(user_id_str, target_username, context, reason):
            stopped_count += 1
        else:
             logger.warning(f"stop_treo_task reported failure for {user_id_str} -> @{target_username} during stop_all, but it should have existed in persistent or runtime list.")

    logger.info(f"Finished stopping tasks/configs for user {user_id_str}. Stopped/Removed: {stopped_count}/{len(targets_to_process)} target(s).")
    return stopped_count


# --- Job Cleanup (Đã cập nhật) ---
async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn VÀ dừng task treo của VIP hết hạn."""
    global valid_keys, activated_users, vip_users, user_daily_gains
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate_key = []
    users_to_deactivate_vip = []
    vip_users_to_stop_tasks = []
    basic_data_changed = False
    gains_cleaned = False

    logger.info("[Cleanup] Starting cleanup job...")

    # Check expired keys (chưa sử dụng)
    for key, data in list(valid_keys.items()):
        try:
            if data.get("used_by") is None and current_time > float(data.get("expiry_time", 0)): keys_to_remove.append(key)
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
                vip_users_to_stop_tasks.append(user_id_str)
        except (ValueError, TypeError):
            users_to_deactivate_vip.append(user_id_str)
            vip_users_to_stop_tasks.append(user_id_str)

    # Cleanup old gains from user_daily_gains
    expiry_threshold = current_time - USER_GAIN_HISTORY_SECONDS
    users_to_remove_from_gains = []
    for user_id_str, targets_data in user_daily_gains.items():
        targets_to_remove_from_user = []
        for target_username, gain_list in targets_data.items():
            valid_gains = [(ts, g) for ts, g in gain_list if ts >= expiry_threshold]
            if len(valid_gains) < len(gain_list):
                gains_cleaned = True
                if valid_gains: user_daily_gains[user_id_str][target_username] = valid_gains
                else: targets_to_remove_from_user.append(target_username)
            elif not valid_gains and not gain_list: targets_to_remove_from_user.append(target_username)

        if targets_to_remove_from_user:
            gains_cleaned = True
            for target in targets_to_remove_from_user:
                if target in user_daily_gains[user_id_str]: del user_daily_gains[user_id_str][target]
            if not user_daily_gains[user_id_str]: users_to_remove_from_gains.append(user_id_str)

    if users_to_remove_from_gains:
        gains_cleaned = True
        for user_id_str_rem in users_to_remove_from_gains:
            if user_id_str_rem in user_daily_gains: del user_daily_gains[user_id_str_rem]
        logger.debug(f"[Cleanup Gains] Removed {len(users_to_remove_from_gains)} users from gain tracking.")

    if gains_cleaned: logger.info("[Cleanup Gains] Finished pruning old gain entries.")

    # Perform deletions
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

    # Stop tasks for expired/invalid VIPs
    if vip_users_to_stop_tasks:
         unique_users_to_stop = set(vip_users_to_stop_tasks)
         logger.info(f"[Cleanup] Scheduling stop for tasks of {len(unique_users_to_stop)} expired/invalid VIP users.")
         app = context.application
         for user_id_str_stop in unique_users_to_stop:
             # Chạy bất đồng bộ
             app.create_task(
                 stop_all_treo_tasks_for_user(user_id_str_stop, context, reason="VIP Expired/Removed during Cleanup"),
                 name=f"cleanup_stop_tasks_{user_id_str_stop}"
             )
             # Lưu ý: stop_all_treo_tasks_for_user tự gọi save_data() nếu xóa persistent config

    # Lưu data nếu có thay đổi cơ bản HOẶC gain data đã được dọn dẹp.
    if basic_data_changed or gains_cleaned:
        if basic_data_changed: logger.info("[Cleanup] Basic data changed, saving...")
        if gains_cleaned: logger.info("[Cleanup] Gain history data cleaned, saving...")
        save_data()
    else:
        logger.info("[Cleanup] No basic data changes or gain cleanup needed. Treo task stopping handles its own saving if necessary.")

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
        try: return int(vip_users.get(user_id_str, {}).get("limit", 0))
        except (ValueError, TypeError): return 0
    return 0

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

# --- Logic API Follow (Đã cập nhật) ---
async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """Gọi API follow và trả về kết quả."""
    api_params = {"user": target_username, "userid": user_id_str, "tokenbot": bot_token}
    log_api_params = api_params.copy()
    log_api_params["tokenbot"] = f"...{bot_token[-6:]}" if len(bot_token) > 6 else "***"
    logger.info(f"[API Call] User {user_id_str} calling Follow API for @{target_username} with params: {log_api_params}")
    result = {"success": False, "message": "Lỗi không xác định khi gọi API.", "data": None}
    try:
        async with httpx.AsyncClient(verify=False, timeout=90.0) as client:
            resp = await client.get(FOLLOW_API_URL_BASE, params=api_params, headers={'User-Agent': 'TG Bot FL Caller'})
            content_type = resp.headers.get("content-type", "").lower()
            response_text_full = ""
            try:
                encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1']
                decoded = False
                resp_bytes = await resp.aread()
                for enc in encodings_to_try:
                    try:
                        response_text_full = resp_bytes.decode(enc, errors='strict')
                        logger.debug(f"[API Call @{target_username}] Decoded response with {enc}.")
                        decoded = True; break
                    except UnicodeDecodeError: logger.debug(f"[API Call @{target_username}] Failed to decode with {enc}")
                if not decoded:
                    response_text_full = resp_bytes.decode('utf-8', errors='replace')
                    logger.warning(f"[API Call @{target_username}] Could not decode response with common encodings, using replace.")
            except Exception as e_read_outer:
                 logger.error(f"[API Call @{target_username}] Error reading/decoding response body: {e_read_outer}")
                 response_text_full = "[Error reading response body]"

            response_text_for_debug = response_text_full[:1000]
            logger.debug(f"[API Call @{target_username}] Status: {resp.status_code}, Content-Type: {content_type}, Snippet: {response_text_for_debug}...")

            if resp.status_code == 200:
                if "application/json" in content_type:
                    try:
                        data = json.loads(response_text_full)
                        logger.debug(f"[API Call @{target_username}] JSON Data: {data}")
                        result["data"] = data
                        api_status = data.get("status")
                        api_message = data.get("message")
                        # Check status linh hoạt
                        if isinstance(api_status, bool): result["success"] = api_status
                        elif isinstance(api_status, str): result["success"] = api_status.lower() in ['true', 'success', 'ok', '200']
                        elif isinstance(api_status, int): result["success"] = api_status == 200
                        else: result["success"] = False
                        # Message mặc định nếu API không trả về
                        if result["success"] and api_message is None: api_message = "Follow thành công."
                        elif not result["success"] and api_message is None: api_message = f"Follow thất bại (API status={api_status})."
                        result["message"] = str(api_message) if api_message is not None else "Không có thông báo từ API."
                    except json.JSONDecodeError:
                        logger.error(f"[API Call @{target_username}] Response 200 OK (JSON type) but not valid JSON.")
                        error_match = re.search(r'<pre>(.*?)</pre>', response_text_full, re.DOTALL | re.IGNORECASE)
                        result["message"] = f"Lỗi API (HTML?): {html.escape(error_match.group(1).strip())}" if error_match else "Lỗi: API trả về dữ liệu JSON không hợp lệ."
                        result["success"] = False
                    except Exception as e_proc:
                        logger.error(f"[API Call @{target_username}] Error processing API JSON data: {e_proc}", exc_info=True)
                        result["message"] = "Lỗi xử lý dữ liệu JSON từ API."
                        result["success"] = False
                else: # 200 OK nhưng không phải JSON
                     logger.warning(f"[API Call @{target_username}] Response 200 OK but wrong Content-Type: {content_type}.")
                     # Heuristic: Text ngắn, không có lỗi -> Thành công
                     if len(response_text_full) < 200 and all(x not in response_text_full.lower() for x in ["lỗi", "error", "fail"]):
                         result["success"] = True
                         result["message"] = "Follow thành công (phản hồi không chuẩn JSON)."
                     else:
                         result["success"] = False
                         error_match = re.search(r'<pre>(.*?)</pre>', response_text_full, re.DOTALL | re.IGNORECASE)
                         html_error = f": {html.escape(error_match.group(1).strip())}" if error_match else "."
                         result["message"] = f"Lỗi định dạng phản hồi API (Type: {content_type}){html_error}"
            else: # Lỗi HTTP
                 logger.error(f"[API Call @{target_username}] HTTP Error Status: {resp.status_code}.")
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

    if not isinstance(result["message"], str): result["message"] = str(result["message"]) if result["message"] is not None else "Lỗi không xác định."
    logger.info(f"[API Call @{target_username}] Final result: Success={result['success']}, Message='{result['message'][:200]}...'")
    return result

# --- Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start hoặc /menu - Hiển thị menu chính."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user: return
    logger.info(f"User {user.id} ({user.username}) used /start or /menu in chat {chat_id}")

    act_h = ACTIVATION_DURATION_SECONDS // 3600
    treo_interval_m = TREO_INTERVAL_SECONDS // 60
    welcome_text = (
        f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
        f"🤖 Chào mừng bạn đến với <b>DinoTool</b> - Bot hỗ trợ TikTok.\n\n"
        f"✨ <b>Cách sử dụng cơ bản (Miễn phí):</b>\n"
        f"   » Dùng <code>/getkey</code> và <code>/nhapkey &lt;key&gt;</code> để kích hoạt {act_h} giờ sử dụng <code>/tim</code>, <code>/fl</code>.\n\n"
        f"👑 <b>Nâng cấp VIP:</b>\n"
        f"   » Mở khóa <code>/treo</code> (tự động chạy /fl mỗi {treo_interval_m} phút), không cần key, giới hạn cao hơn, xem gain 24h (<code>/xemfl24h</code>).\n\n"
        f"👇 <b>Chọn một tùy chọn bên dưới:</b>"
    )

    keyboard_buttons = [
        [InlineKeyboardButton("👑 Mua VIP", callback_data="show_muatt")],
        [InlineKeyboardButton("📜 Lệnh Bot", callback_data="show_lenh")],
    ]
    if ALLOWED_GROUP_ID and GROUP_LINK and GROUP_LINK != "YOUR_GROUP_INVITE_LINK":
         keyboard_buttons.append([InlineKeyboardButton("💬 Nhóm Chính", url=GROUP_LINK)])
    keyboard_buttons.append([InlineKeyboardButton("👨‍💻 Liên hệ Admin", url=f"tg://user?id={ADMIN_USER_ID}")])
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    try:
        await delete_user_message(update, context)
        await context.bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /start or /menu message to {user.id} in chat {chat_id}: {e}")

async def menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    callback_data = query.data
    user = query.from_user
    if not user: return
    logger.info(f"Menu callback '{callback_data}' triggered by user {user.id} in chat {query.message.chat_id}")

    try: await query.delete_message()
    except Exception as e: logger.debug(f"Could not delete old menu message: {e}")

    # Tạo Update giả lập để gọi hàm command tương ứng
    fake_message = Message(message_id=query.message.message_id + 1, date=datetime.now(), chat=query.message.chat, from_user=user, text=f"/{callback_data.split('_')[-1]}")
    fake_update = Update(update_id=update.update_id + 1, message=fake_message)

    if callback_data == "show_muatt": await muatt_command(fake_update, context)
    elif callback_data == "show_lenh": await lenh_command(fake_update, context)

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

    status_lines = [f"👤 <b>Người dùng:</b> {user.mention_html()} (<code>{user_id}</code>)"]
    if is_vip:
        vip_data = vip_users.get(user_id_str, {})
        expiry_ts = vip_data.get("expiry")
        limit = vip_data.get("limit", "?")
        expiry_str = "Không rõ"
        if expiry_ts: try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%Y %H:%M')
                      except (ValueError, TypeError, OSError): pass
        status_lines.append(f"👑 <b>Trạng thái:</b> VIP ✨ (Hết hạn: {expiry_str}, Giới hạn treo: {limit} users)")
    elif is_key_active:
        expiry_ts = activated_users.get(user_id_str)
        expiry_str = "Không rõ"
        if expiry_ts: try: expiry_str = datetime.fromtimestamp(float(expiry_ts)).strftime('%d/%m/%Y %H:%M')
                      except (ValueError, TypeError, OSError): pass
        status_lines.append(f"🔑 <b>Trạng thái:</b> Đã kích hoạt (Key) (Hết hạn: {expiry_str})")
    else:
        status_lines.append("▫️ <b>Trạng thái:</b> Thành viên thường")

    status_lines.append(f"⚡️ <b>Quyền dùng /tim, /fl:</b> {'✅ Có thể' if can_use_std_features else '❌ Chưa thể (Cần VIP/Key)'}")
    current_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
    if is_vip:
        vip_limit = get_vip_limit(user_id)
        status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ✅ Có thể (Đang treo: {current_treo_count}/{vip_limit} users)")
    else:
         status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ❌ Chỉ dành cho VIP (Đang treo: {current_treo_count}/0 users)")

    cmd_lines = ["\n\n📜=== <b>DANH SÁCH LỆNH</b> ===📜"]
    cmd_lines.extend([
        "\n<b><u>🧭 Điều Hướng:</u></b>",
        f"  <code>/menu</code> - Mở menu chính",
        "\n<b><u>🔑 Lệnh Miễn Phí (Kích hoạt Key):</u></b>",
        f"  <code>/getkey</code> - Lấy link nhận key (⏳ {gk_cd_m}p/lần, Key hiệu lực {key_exp_h}h)",
        f"  <code>/nhapkey &lt;key&gt;</code> - Kích hoạt tài khoản (Sử dụng {act_h}h)",
        "\n<b><u>❤️ Lệnh Tăng Tương Tác (Cần VIP/Key):</u></b>",
        f"  <code>/tim &lt;link_video&gt;</code> - Tăng tim cho video TikTok (⏳ {tf_cd_m}p/lần)",
        f"  <code>/fl &lt;username&gt;</code> - Tăng follow cho tài khoản TikTok (⏳ {tf_cd_m}p/user)",
        "\n<b><u>👑 Lệnh VIP:</u></b>",
        f"  <code>/muatt</code> - Thông tin và hướng dẫn mua VIP",
        f"  <code>/treo &lt;username&gt;</code> - Tự động chạy <code>/fl</code> mỗi {treo_interval_m} phút (Dùng slot)",
        f"  <code>/dungtreo &lt;username&gt;</code> - Dừng treo cho một tài khoản",
        f"  <code>/dungtreo</code> - Dừng treo <b>TẤT CẢ</b> tài khoản", # <<< Thêm mô tả dừng tất cả
        f"  <code>/listtreo</code> - Xem danh sách tài khoản đang treo",
        f"  <code>/xemfl24h</code> - Xem số follow đã tăng trong 24 giờ qua (cho các tài khoản đang treo)",
    ])
    if user_id == ADMIN_USER_ID:
        cmd_lines.append("\n<b><u>🛠️ Lệnh Admin:</u></b>")
        valid_vip_packages = ', '.join(map(str, VIP_PRICES.keys()))
        cmd_lines.append(f"  <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code> - Thêm/gia hạn VIP (Gói: {valid_vip_packages})")
        cmd_lines.append(f"  <code>/mess &lt;nội_dung&gt;</code> - Gửi thông báo đến nhóm chính (nếu có)") # <<< Thêm mô tả /mess
        # cmd_lines.append(f"  <code>/adminlisttreo &lt;user_id&gt;</code> - (Chưa impl.) Xem list treo của user khác")
    cmd_lines.extend([
        "\n<b><u>ℹ️ Lệnh Chung:</u></b>",
        f"  <code>/start</code> - Hiển thị menu chào mừng",
        f"  <code>/lenh</code> - Xem lại bảng lệnh và trạng thái này",
        "\n<i>Lưu ý: Các lệnh yêu cầu VIP/Key chỉ hoạt động khi bạn có trạng thái tương ứng.</i>"
    ])

    help_text = "\n".join(status_lines + cmd_lines)
    try:
        # Xóa lệnh /lenh gốc (chỉ xóa nếu nó đến từ message, không xóa nếu đến từ callback)
        if update.message and update.message.message_id:
             await delete_user_message(update, context, update.message.message_id)
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /lenh message to {user.id} in chat {chat_id}: {e}")

async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tim."""
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id
    current_time = time.time()

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {user.mention_html()}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để dùng lệnh này!\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code> | 👑 Hoặc: <code>/muatt</code>")
        await send_temporary_message(update, context, err_msg, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    # Check Cooldown
    last_usage = user_tim_cooldown.get(user_id_str)
    if last_usage and current_time - float(last_usage) < TIM_FL_COOLDOWN_SECONDS:
        rem_time = TIM_FL_COOLDOWN_SECONDS - (current_time - float(last_usage))
        cd_msg = f"⏳ {user.mention_html()}, đợi <b>{rem_time:.0f} giây</b> nữa để dùng <code>/tim</code>."
        await send_temporary_message(update, context, cd_msg, duration=15)
        await delete_user_message(update, context, original_message_id)
        return

    # Parse Arguments & Validate URL
    args = context.args
    video_url = None
    err_txt = None
    if not args: err_txt = ("⚠️ Chưa nhập link video.\n<b>Cú pháp:</b> <code>/tim https://tiktok.com/...</code>")
    else:
        url_input = args[0]
        # Chấp nhận link tiktok.com, vm.tiktok.com, vt.tiktok.com
        if not re.match(r"https?://(?:www\.|vm\.|vt\.)?tiktok\.com/", url_input):
             err_txt = f"⚠️ Link <code>{html.escape(url_input)}</code> không hợp lệ. Phải là link video TikTok."
        else: video_url = url_input # Giữ nguyên link hợp lệ

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return
    if not video_url: # Should not happen if err_txt is None, but double check
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
            try: response_text_full = (await resp.aread()).decode('utf-8', errors='replace')
            except Exception as e_read: logger.error(f"/tim API read error: {e_read}")

            logger.debug(f"/tim API response status: {resp.status_code}, type: {content_type}, snippet: {response_text_full[:500]}...")

            if resp.status_code == 200 and "application/json" in content_type:
                try:
                    data = json.loads(response_text_full)
                    logger.debug(f"/tim API response data: {data}")
                    if data.get("status") == "success" or data.get("success") == True:
                        user_tim_cooldown[user_id_str] = time.time(); save_data()
                        d = data.get("data", {})
                        a = html.escape(str(d.get("author", "?")))
                        v = html.escape(str(d.get("video_url", video_url)))
                        db = html.escape(str(d.get('digg_before', '?')))
                        di = html.escape(str(d.get('digg_increased', '?')))
                        da = html.escape(str(d.get('digg_after', '?')))
                        final_response_text = (
                            f"🎉 <b>Tăng Tim Thành Công!</b> ❤️\n👤 Cho: {user.mention_html()}\n\n"
                            f"📊 <b>Thông tin Video:</b>\n🎬 <a href='{v}'>Link Video</a>\n✍️ Tác giả: <code>{a}</code>\n"
                            f"👍 Trước: <code>{db}</code> ➜ 💖 Tăng: <code>+{di}</code> ➜ ✅ Sau: <code>{da}</code>" )
                    else:
                        api_msg = data.get('message', 'Không rõ lý do từ API')
                        logger.warning(f"/tim API call failed for user {user_id}. API message: {api_msg}")
                        final_response_text = f"💔 <b>Tăng Tim Thất Bại!</b>\n👤 Cho: {user.mention_html()}\nℹ️ Lý do: <code>{html.escape(api_msg)}</code>"
                except json.JSONDecodeError as e_json:
                    logger.error(f"/tim API response 200 OK but not valid JSON: {e_json}. Text: {response_text_full[:500]}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API Tăng Tim</b>\n👤 Cho: {user.mention_html()}\nℹ️ API không trả về JSON hợp lệ."
            else:
                logger.error(f"/tim API call HTTP error {resp.status_code} or wrong type {content_type}. Text: {response_text_full[:500]}...")
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
            try: await context.bot.edit_message_text(chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e_edit: logger.warning(f"Failed to edit /tim msg {processing_msg.message_id}: {e_edit}")
        else: # Should not happen if initial reply succeeded
             logger.warning(f"Processing message for /tim user {user_id} was None. Sending new.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
             except Exception as e_send: logger.error(f"Failed to send final /tim message for user {user_id}: {e_send}")

async def process_fl_request_background(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id_str: str, target_username: str, processing_msg_id: int, invoking_user_mention: str):
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
            f_add_raw = api_data.get("followers_add", "?")
            f_after = html.escape(str(api_data.get("followers_after", "?")))
            f_add_display = "?"
            f_add_int = 0
            if f_add_raw != "?":
                 try:
                     f_add_str_cleaned = re.sub(r'[^\d-]', '', str(f_add_raw))
                     if f_add_str_cleaned: f_add_int = int(f_add_str_cleaned)
                     f_add_display = f"+{f_add_int:,}" if f_add_int >= 0 else f"{f_add_int:,}" # Add comma separator
                 except ValueError: f_add_display = html.escape(str(f_add_raw))

            if any(x != "?" for x in [f_before, f_add_raw, f_after]):
                follower_lines = ["📈 <b>Số lượng Follower:</b>"]
                if f_before != "?": follower_lines.append(f"   Trước: <code>{f_before}</code>")
                if f_add_display != "?" and f_add_int > 0: follower_lines.append(f"   Tăng:   <b><code>{f_add_display}</code></b> ✨")
                elif f_add_display != "?": follower_lines.append(f"   Tăng:   <code>{f_add_display}</code>")
                if f_after != "?": follower_lines.append(f"   Sau:    <code>{f_after}</code>")
                if len(follower_lines) > 1: follower_info_block = "\n".join(follower_lines)
        except Exception as e_parse:
            logger.error(f"[BG Task /fl] Error parsing API data for @{target_username}: {e_parse}. Data: {api_data}")
            user_info_block = f"👤 <b>Tài khoản:</b> <code>@{html.escape(target_username)}</code>\n(Lỗi xử lý thông tin chi tiết từ API)"

    if success:
        user_fl_cooldown[str(user_id_str)][target_username] = time.time(); save_data()
        logger.info(f"[BG Task /fl] Success for user {user_id_str} -> @{target_username}. Cooldown updated.")
        final_response_text = (
            f"✅ <b>Tăng Follow Thành Công!</b>\n✨ Cho: {invoking_user_mention}\n\n"
            f"{user_info_block if user_info_block else f'👤 <b>Tài khoản:</b> <code>@{html.escape(target_username)}</code>\n'}"
            f"{follower_info_block if follower_info_block else ''}" )
    else:
        logger.warning(f"[BG Task /fl] Failed for user {user_id_str} -> @{target_username}. API Message: {api_message}")
        final_response_text = (
            f"❌ <b>Tăng Follow Thất Bại!</b>\n👤 Cho: {invoking_user_mention}\n🎯 Target: <code>@{html.escape(target_username)}</code>\n\n"
            f"💬 Lý do API: <i>{html.escape(api_message or 'Không rõ')}</i>\n\n"
            f"{user_info_block if user_info_block else ''}" )
        if isinstance(api_message, str) and any(x in api_message.lower() for x in ["đợi", "wait", "phút", "giây", "minute", "second"]):
            final_response_text += f"\n\n<i>ℹ️ API yêu cầu chờ đợi. Vui lòng thử lại sau hoặc sử dụng <code>/treo {target_username}</code> nếu bạn là VIP.</i>"

    try:
        await context.bot.edit_message_text( chat_id=chat_id, message_id=processing_msg_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"[BG Task /fl] Edited message {processing_msg_id} for user {user_id_str} -> @{target_username}")
    except Exception as e: logger.error(f"[BG Task /fl] Failed to edit msg {processing_msg_id}: {e}", exc_info=True)

# --- /fl Command (Đã bỏ validation username nghiêm ngặt) ---
async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()
    current_time = time.time()

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để dùng lệnh này!\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code> | 👑 Hoặc: <code>/muatt</code>")
        await send_temporary_message(update, context, err_msg, duration=30)
        await delete_user_message(update, context, original_message_id)
        return

    # Parse Arguments (Chỉ kiểm tra trống)
    args = context.args
    target_username = None
    err_txt = None
    if not args: err_txt = ("⚠️ Chưa nhập username TikTok.\n<b>Cú pháp:</b> <code>/fl username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@") # Xóa @ nếu có
        if not uname: err_txt = "⚠️ Username không được trống."
        # --- VALIDATION ĐÃ BỊ XÓA THEO YÊU CẦU ---
        # username_regex = r"^[a-zA-Z0-9._]{2,24}$"
        # if not re.match(username_regex, uname):
        #      err_txt = (f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ.\n"
        #                 f"(Phải từ 2-24 ký tự, chỉ chứa chữ cái, số, dấu chấm '.', dấu gạch dưới '_')")
        # elif uname.startswith('.') or uname.endswith('.') or uname.startswith('_') or uname.endswith('_'):
        #      err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ (không được bắt đầu/kết thúc bằng '.' hoặc '_')."
        # elif '..' in uname:
        #      err_txt = f"⚠️ Username <code>{html.escape(uname_raw)}</code> không hợp lệ (không được chứa '..' liên tiếp)."
        # --- KẾT THÚC PHẦN BỊ XÓA ---
        else:
            target_username = uname # Lấy username đã được làm sạch (@)

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # Check Cooldown
    if target_username:
        user_cds = user_fl_cooldown.get(user_id_str, {})
        last_usage = user_cds.get(target_username)
        if last_usage and current_time - float(last_usage) < TIM_FL_COOLDOWN_SECONDS:
             rem_time = TIM_FL_COOLDOWN_SECONDS - (current_time - float(last_usage))
             cd_msg = f"⏳ {invoking_user_mention}, đợi <b>{rem_time:.0f} giây</b> nữa để dùng <code>/fl</code> cho <code>@{html.escape(target_username)}</code>."
             await send_temporary_message(update, context, cd_msg, duration=15)
             await delete_user_message(update, context, original_message_id)
             return

    # Gửi tin nhắn chờ và chạy nền
    processing_msg = None
    try:
        if not target_username: raise ValueError("Target username became None unexpectedly before processing")
        processing_msg = await update.message.reply_html(f"⏳ {invoking_user_mention}, đã nhận yêu cầu tăng follow cho <code>@{html.escape(target_username)}</code>. Đang xử lý...")
        await delete_user_message(update, context, original_message_id)

        logger.info(f"Scheduling background task for /fl user {user_id} target @{target_username}")
        context.application.create_task(
            process_fl_request_background( context=context, chat_id=chat_id, user_id_str=user_id_str, target_username=target_username, processing_msg_id=processing_msg.message_id, invoking_user_mention=invoking_user_mention),
            name=f"fl_bg_{user_id_str}_{target_username}" )
    except Exception as e:
         logger.error(f"Failed to send processing message or schedule task for /fl @{html.escape(target_username or '???')}: {e}", exc_info=True)
         await delete_user_message(update, context, original_message_id)
         if processing_msg:
            try: await context.bot.delete_message(chat_id, processing_msg.message_id) # Xóa tin nhắn chờ nếu lỗi
            except Exception: pass
         await send_temporary_message(update, context, f"❌ Lỗi khi bắt đầu xử lý yêu cầu /fl cho @{html.escape(target_username or '???')}. Vui lòng thử lại.", duration=20)


async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id
    current_time = time.time()

    if not LINK_SHORTENER_API_KEY:
        logger.error("LINK_SHORTENER_API_KEY is missing. /getkey cannot function.")
        await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "❌ Lệnh <code>/getkey</code> tạm thời không hoạt động do lỗi cấu hình Bot. Vui lòng báo Admin.", duration=30)
        return

    # Check Cooldown
    last_usage = user_getkey_cooldown.get(user_id_str)
    if last_usage and current_time - float(last_usage) < GETKEY_COOLDOWN_SECONDS:
        remaining = GETKEY_COOLDOWN_SECONDS - (current_time - float(last_usage))
        cd_msg = f"⏳ {user.mention_html()}, đợi <b>{remaining:.0f} giây</b> nữa để dùng <code>/getkey</code>."
        await send_temporary_message(update, context, cd_msg, duration=15)
        await delete_user_message(update, context, original_message_id)
        return

    # Tạo Key và Link
    generated_key = generate_random_key()
    while generated_key in valid_keys: generated_key = generate_random_key()

    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key)
    cache_buster = f"&ts={int(time.time())}{random.randint(100,999)}"
    final_target_url = target_url_with_key + cache_buster
    shortener_params = { "token": LINK_SHORTENER_API_KEY, "format": "json", "url": final_target_url }
    log_shortener_params = { "token": f"...{LINK_SHORTENER_API_KEY[-6:]}", "format": "json", "url": final_target_url }
    logger.info(f"User {user_id} requesting key. Generated: {generated_key}. Target URL: {final_target_url}")

    processing_msg = None
    final_response_text = ""
    key_stored_successfully = False

    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key, vui lòng chờ...</i></b> 🔑")
        await delete_user_message(update, context, original_message_id)

        generation_time = time.time()
        expiry_time = generation_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = { "user_id_generator": user_id, "generation_time": generation_time, "expiry_time": expiry_time, "used_by": None, "activation_time": None }
        save_data(); key_stored_successfully = True
        logger.info(f"Key {generated_key} stored for user {user_id}. Expires at {datetime.fromtimestamp(expiry_time).isoformat()}.")

        logger.debug(f"Calling shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_shortener_params}")
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers={'User-Agent': 'Telegram Bot Key Generator'})
            response_text_full = ""
            try: response_text_full = (await response.aread()).decode('utf-8', errors='replace')
            except Exception as e_read: logger.error(f"/getkey shortener read error: {e_read}")

            logger.debug(f"Shortener API response status: {response.status_code}, type: {response.headers.get('content-type','').lower()}, snippet: {response_text_full[:500]}...")

            if response.status_code == 200:
                try:
                    response_data = response.json()
                    logger.debug(f"Parsed shortener API response: {response_data}")
                    status = response_data.get("status")
                    generated_short_url = response_data.get("shortenedUrl")

                    if status == "success" and generated_short_url:
                        user_getkey_cooldown[user_id_str] = time.time(); save_data()
                        logger.info(f"Successfully generated short link for user {user_id}: {generated_short_url}. Key {generated_key} confirmed.")
                        final_response_text = (
                            f"🚀 <b>Link Lấy Key Của Bạn ({user.mention_html()}):</b>\n\n"
                            f"🔗 <a href='{html.escape(generated_short_url)}'>{html.escape(generated_short_url)}</a>\n\n"
                            f"📝 <b>Hướng dẫn:</b>\n   1️⃣ Click vào link trên.\n   2️⃣ Làm theo các bước để nhận Key (VD: <code>Dinotool-ABC123XYZ</code>).\n"
                            f"   3️⃣ Copy Key đó và quay lại đây.\n   4️⃣ Gửi lệnh: <code>/nhapkey &lt;key_ban_vua_copy&gt;</code>\n\n"
                            f"⏳ <i>Key chỉ có hiệu lực để nhập trong <b>{KEY_EXPIRY_SECONDS // 3600} giờ</b>. Hãy nhập sớm!</i>" )
                    else:
                        api_message = response_data.get("message", "Lỗi không xác định từ API rút gọn link.")
                        logger.error(f"Shortener API returned error for user {user_id}. Status: {status}, Message: {api_message}. Data: {response_data}")
                        final_response_text = f"❌ <b>Lỗi Khi Tạo Link:</b>\n<code>{html.escape(str(api_message))}</code>\nVui lòng thử lại sau hoặc báo Admin."
                except json.JSONDecodeError:
                    logger.error(f"Shortener API Status 200 but JSON decode failed. Text: {response_text_full[:500]}...")
                    final_response_text = f"❌ <b>Lỗi Phản Hồi API Rút Gọn Link:</b> Máy chủ trả về dữ liệu không hợp lệ. Vui lòng thử lại sau."
            else:
                 logger.error(f"Shortener API HTTP error {response.status_code}. Text: {response_text_full[:500]}...")
                 final_response_text = f"❌ <b>Lỗi Kết Nối API Tạo Link</b> (Mã: {response.status_code}). Vui lòng thử lại sau hoặc báo Admin."
    except httpx.TimeoutException:
        logger.warning(f"Shortener API timeout during /getkey for user {user_id}")
        final_response_text = "❌ <b>Lỗi Timeout:</b> Máy chủ tạo link không phản hồi kịp thời. Vui lòng thử lại sau."
    except httpx.RequestError as e_req:
        logger.error(f"Shortener API network error during /getkey for user {user_id}: {e_req}", exc_info=False)
        final_response_text = "❌ <b>Lỗi Mạng</b> khi gọi API tạo link. Vui lòng thử lại sau."
    except Exception as e_unexp:
        logger.error(f"Unexpected error during /getkey command for user {user_id}: {e_unexp}", exc_info=True)
        final_response_text = "❌ <b>Lỗi Hệ Thống Bot</b> khi tạo key. Vui lòng báo Admin."
        if key_stored_successfully and generated_key in valid_keys and valid_keys[generated_key].get("used_by") is None:
            try: del valid_keys[generated_key]; save_data(); logger.info(f"Removed unused key {generated_key} due to unexpected error in /getkey.")
            except Exception as e_rem: logger.error(f"Failed to remove unused key {generated_key} after error: {e_rem}")
    finally:
        if processing_msg:
            try: await context.bot.edit_message_text(chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e_edit: logger.warning(f"Failed to edit /getkey msg {processing_msg.message_id}: {e_edit}")
        else:
             logger.warning(f"Processing message for /getkey user {user_id} was None. Sending new.")
             try: await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
             except Exception as e_send: logger.error(f"Failed to send final /getkey message for user {user_id}: {e_send}")

async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id
    current_time = time.time()

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
        if activation_time_ts: try: used_time_str = f" lúc {datetime.fromtimestamp(float(activation_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
                                except: pass
        if str(used_by_id) == user_id_str:
             logger.info(f"Key validation: User {user_id} already used key '{submitted_key}'{used_time_str}.")
             final_response_text = f"⚠️ Bạn đã kích hoạt key <code>{html.escape(submitted_key)}</code> này rồi{used_time_str}."
        else:
             logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by user {used_by_id}{used_time_str}.")
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được người khác sử dụng{used_time_str}."
    elif current_time > float(key_data.get("expiry_time", 0)):
        expiry_time_ts = key_data.get("expiry_time")
        expiry_time_str = ""
        if expiry_time_ts: try: expiry_time_str = f" vào lúc {datetime.fromtimestamp(float(expiry_time_ts)).strftime('%H:%M:%S %d/%m/%Y')}"
                             except: pass
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' expired{expiry_time_str}.")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn sử dụng{expiry_time_str}. Dùng <code>/getkey</code> để lấy key mới."
    else: # Key hợp lệ
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
            final_response_text = (f"✅ <b>Kích Hoạt Key Thành Công!</b>\n\n👤 Người dùng: {user.mention_html()}\n🔑 Key: <code>{html.escape(submitted_key)}</code>\n\n"
                                   f"✨ Bạn có thể sử dụng <code>/tim</code> và <code>/fl</code>.\n⏳ Hết hạn vào: <b>{expiry_str}</b> (sau {act_hours} giờ).")
        except Exception as e_activate:
             logger.error(f"Unexpected error during key activation process for user {user_id} key {submitted_key}: {e_activate}", exc_info=True)
             final_response_text = f"❌ Lỗi hệ thống khi kích hoạt key <code>{html.escape(submitted_key)}</code>. Báo Admin."
             # Rollback cẩn thận
             if submitted_key in valid_keys and valid_keys[submitted_key].get("used_by") == user_id:
                 valid_keys[submitted_key]["used_by"] = None; valid_keys[submitted_key]["activation_time"] = None
             if user_id_str in activated_users: del activated_users[user_id_str]
             try: save_data()
             except Exception as e_save_rb: logger.error(f"Failed to save data after rollback attempt for key {submitted_key}: {e_save_rb}")

    # Gửi phản hồi và xóa lệnh gốc
    await delete_user_message(update, context, original_message_id)
    try: await update.message.reply_html(final_response_text, disable_web_page_preview=True)
    except Exception as e: logger.error(f"Failed to send /nhapkey final response to user {user_id}: {e}")

# --- Lệnh /muatt (Đã sửa lỗi hiển thị QR và nút) ---
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị thông tin mua VIP, QR code và nút yêu cầu gửi bill."""
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id # Lưu lại để xóa nếu là message
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
                       f"   - STK: <a href=\"https://t.me/share/url?url={html.escape(BANK_ACCOUNT)}\"><code>{html.escape(BANK_ACCOUNT)}</code></a> (👈 Click để copy)",
                       f"   - Tên chủ TK: <b>{ACCOUNT_NAME}</b>",
                       "\n📝 <b>Nội dung chuyển khoản (Quan trọng!):</b>",
                       f"   » Chuyển khoản với nội dung <b>CHÍNH XÁC</b> là:",
                       f"   » <a href=\"https://t.me/share/url?url={html.escape(payment_note)}\"><code>{html.escape(payment_note)}</code></a> (👈 Click để copy)",
                       f"   <i>(Sai nội dung có thể khiến giao dịch xử lý chậm)</i>",
                       "\n📸 <b>Sau Khi Chuyển Khoản Thành Công:</b>",
                       f"   1️⃣ Chụp ảnh màn hình biên lai (bill) giao dịch.",
                       f"   2️⃣ Nhấn nút 'Gửi Bill Thanh Toán' bên dưới.",
                       f"   3️⃣ Bot sẽ yêu cầu bạn gửi ảnh bill <b><u>VÀO CUỘC TRÒ CHUYỆN NÀY</u></b>.",
                       f"   4️⃣ Gửi ảnh bill của bạn vào đây.",
                       f"   5️⃣ Bot sẽ tự động chuyển tiếp ảnh đến Admin/Nơi nhận bill.",
                       f"   6️⃣ Admin sẽ kiểm tra và kích hoạt VIP sớm nhất.",
                       "\n<i>Cảm ơn bạn đã quan tâm và ủng hộ DinoTool!</i> ❤️"])
    caption_text = "\n".join(text_lines)

    keyboard = InlineKeyboardMarkup([
        # Nút này sẽ trigger prompt_send_bill_callback
        [InlineKeyboardButton("📸 Gửi Bill Thanh Toán", callback_data=f"prompt_send_bill_{user_id}")]
    ])

    # Xóa lệnh /muatt gốc (chỉ xóa nếu nó đến từ message)
    if original_message_id and update.message and original_message_id == update.message.message_id:
         try: await delete_user_message(update, context, original_message_id)
         except Exception as e_del: logger.debug(f"Could not delete original /muatt message: {e_del}")

    # Ưu tiên gửi ảnh QR và caption
    photo_sent = False
    if QR_CODE_URL:
        try:
            await context.bot.send_photo(chat_id=chat_id, photo=QR_CODE_URL, caption=caption_text,
                                       parse_mode=ParseMode.HTML, reply_markup=keyboard)
            logger.info(f"Sent /muatt info with QR photo and prompt button to user {user_id} in chat {chat_id}")
            photo_sent = True
        except (BadRequest, Forbidden, TelegramError) as e:
            logger.warning(f"Error sending /muatt photo+caption to chat {chat_id}: {e}. Falling back to text.")
        except Exception as e_unexp_photo:
            logger.error(f"Unexpected error sending /muatt photo+caption to chat {chat_id}: {e_unexp_photo}", exc_info=True)

    # Nếu gửi ảnh lỗi hoặc không có QR_CODE_URL, gửi text
    if not photo_sent:
        try:
            await context.bot.send_message(chat_id=chat_id, text=caption_text, parse_mode=ParseMode.HTML,
                                           disable_web_page_preview=True, reply_markup=keyboard)
            logger.info(f"Sent /muatt fallback text info with prompt button to user {user_id} in chat {chat_id}")
        except Exception as e_text:
             logger.error(f"Error sending fallback text for /muatt to chat {chat_id}: {e_text}")

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

    # Chỉ người bấm nút gốc mới được phản hồi
    if user.id != expected_user_id:
        await query.answer("Bạn không phải người yêu cầu thanh toán.", show_alert=True)
        logger.info(f"User {user.id} tried to click bill prompt button for user {expected_user_id} in chat {chat_id}")
        return

    # Thêm user vào danh sách chờ và đặt timeout
    pending_bill_user_ids.add(user.id)
    if context.job_queue:
        job_name = f"remove_pending_bill_{user.id}"
        # Xóa job cũ nếu có
        jobs = context.job_queue.get_jobs_by_name(job_name)
        for job in jobs: job.schedule_removal(); logger.debug(f"Removed previous pending bill timeout job for user {user.id}")
        # Tạo job mới
        context.job_queue.run_once( remove_pending_bill_user_job, PENDING_BILL_TIMEOUT_SECONDS, data={'user_id': user.id}, name=job_name )
        logger.info(f"User {user.id} clicked 'prompt_send_bill'. Added to pending list. Timeout job '{job_name}' scheduled for {PENDING_BILL_TIMEOUT_SECONDS}s.")

    await query.answer() # Xác nhận đã nhận callback

    prompt_text = f"📸 {user.mention_html()}, vui lòng gửi ảnh chụp màn hình biên lai thanh toán của bạn <b><u>vào cuộc trò chuyện này</u></b> ngay bây giờ."
    try:
        # Gửi tin nhắn yêu cầu bill ngay dưới tin nhắn /muatt
        await query.message.reply_html(text=prompt_text, quote=False) # Không quote lại tin nhắn gốc
        # Không xóa tin nhắn /muatt để user còn thấy thông tin
    except Exception as e:
        logger.error(f"Error sending bill prompt message to {user.id} in chat {chat_id}: {e}", exc_info=True)
        # Nếu gửi reply lỗi, thử gửi tin mới
        try: await context.bot.send_message(chat_id=chat_id, text=prompt_text, parse_mode=ParseMode.HTML)
        except Exception as e2: logger.error(f"Also failed to send bill prompt as new message to {user.id} in chat {chat_id}: {e2}")

async def remove_pending_bill_user_job(context: ContextTypes.DEFAULT_TYPE):
    """Job để xóa user khỏi danh sách chờ nhận bill nếu timeout."""
    job_data = context.job.data
    user_id = job_data.get('user_id')
    job_name = context.job.name
    if user_id in pending_bill_user_ids:
        pending_bill_user_ids.remove(user_id)
        logger.info(f"Job '{job_name}': Removed user {user_id} from pending bill list due to timeout.")
    else:
        logger.debug(f"Job '{job_name}': User {user_id} not found in pending bill list (already sent or removed).")

# --- Xử lý nhận ảnh bill (Đã có sẵn và hoạt động đúng) ---
async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý ảnh/document ảnh VÀ chỉ chuyển tiếp nếu user nằm trong danh sách chờ."""
    if not update or not update.message or (update.message.text and update.message.text.startswith('/')): return
    user = update.effective_user
    chat = update.effective_chat
    message = update.message
    if not user or not chat or not message: return

    # Chỉ xử lý nếu user đang trong danh sách chờ
    if user.id not in pending_bill_user_ids: return

    is_photo = bool(message.photo)
    is_image_document = bool(message.document and message.document.mime_type and message.document.mime_type.startswith('image/'))
    if not is_photo and not is_image_document: return # Bỏ qua nếu không phải ảnh

    logger.info(f"Bill photo/document received from PENDING user {user.id} ({user.username}) in chat {chat.id} (Type: {chat.type}). Forwarding to {BILL_FORWARD_TARGET_ID}.")

    # Xóa user khỏi danh sách chờ và hủy job timeout ngay lập tức
    pending_bill_user_ids.discard(user.id)
    if context.job_queue:
         job_name = f"remove_pending_bill_{user.id}"
         jobs = context.job_queue.get_jobs_by_name(job_name)
         for job in jobs: job.schedule_removal(); logger.debug(f"Removed pending bill timeout job '{job_name}' for user {user.id} after receiving bill.")

    forward_caption_lines = [f"📄 <b>Bill Nhận Được Từ User</b>",
                             f"👤 <b>User:</b> {user.mention_html()} (<code>{user.id}</code>)"]
    if chat.type == 'private': forward_caption_lines.append(f"💬 <b>Chat gốc:</b> PM với Bot")
    elif chat.title: forward_caption_lines.append(f"👥 <b>Chat gốc:</b> {html.escape(chat.title)} (<code>{chat.id}</code>)")
    else: forward_caption_lines.append(f"❓ <b>Chat gốc:</b> ID <code>{chat.id}</code>")
    try:
        message_link = message.link
        if message_link: forward_caption_lines.append(f"🔗 <a href='{message_link}'>Link Tin Nhắn Gốc</a>")
    except AttributeError: logger.debug(f"Could not get message link for message {message.message_id}")

    original_caption = message.caption
    if original_caption: forward_caption_lines.append(f"\n📝 <b>Caption gốc:</b>\n{html.escape(original_caption[:500])}{'...' if len(original_caption) > 500 else ''}")
    forward_caption_text = "\n".join(forward_caption_lines)

    try:
        # Chuyển tiếp tin nhắn chứa ảnh/bill gốc
        await context.bot.forward_message(chat_id=BILL_FORWARD_TARGET_ID, from_chat_id=chat.id, message_id=message.message_id)
        # Gửi tin nhắn thông tin bổ sung (người gửi, chat gốc)
        await context.bot.send_message(chat_id=BILL_FORWARD_TARGET_ID, text=forward_caption_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

        logger.info(f"Successfully forwarded bill message {message.message_id} from user {user.id} and sent info to {BILL_FORWARD_TARGET_ID}.")
        try: await message.reply_html("✅ Đã nhận và chuyển tiếp bill của bạn đến Admin để xử lý. Vui lòng chờ nhé!")
        except Exception as e_reply: logger.warning(f"Failed to send confirmation reply to user {user.id}: {e_reply}")

    except (Forbidden, BadRequest) as e: # Lỗi thường gặp nhất khi bot không có quyền hoặc bị chặn
        logger.error(f"Bot cannot forward/send message to BILL_FORWARD_TARGET_ID ({BILL_FORWARD_TARGET_ID}). Check permissions/block status. Error: {e}")
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID: # Chỉ báo lỗi cho Admin nếu target khác Admin
            try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Lý do: Bot bị chặn hoặc thiếu quyền.\nLỗi: {e}")
            except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
        try: await message.reply_html(f"❌ Đã xảy ra lỗi khi gửi bill của bạn. Vui lòng liên hệ trực tiếp Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a> và gửi bill thủ công.")
        except Exception: pass
    except TelegramError as e_fwd: # Các lỗi Telegram khác
         logger.error(f"Telegram error forwarding/sending bill message {message.message_id} to {BILL_FORWARD_TARGET_ID}: {e_fwd}")
         if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
              try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi Telegram khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Lỗi: {e_fwd}")
              except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
         try: await message.reply_html(f"❌ Đã xảy ra lỗi khi gửi bill của bạn. Vui lòng liên hệ trực tiếp Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a> và gửi bill thủ công.")
         except Exception: pass
    except Exception as e: # Lỗi không xác định
        logger.error(f"Unexpected error forwarding/sending bill to {BILL_FORWARD_TARGET_ID}: {e}", exc_info=True)
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
             try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi không xác định khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Chi tiết log.")
             except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
        try: await message.reply_html(f"❌ Đã xảy ra lỗi khi gửi bill của bạn. Vui lòng liên hệ trực tiếp Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a> và gửi bill thủ công.")
        except Exception: pass

    raise ApplicationHandlerStop # Dừng xử lý, không cho handler khác nhận ảnh này

async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cấp VIP cho người dùng (chỉ Admin)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    chat = update.effective_chat
    if not admin_user or not chat or admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /addtt attempt by {admin_user.id if admin_user else 'Unknown'}")
        return # Không phản hồi gì để tránh lộ lệnh admin

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
                else: vip_info = VIP_PRICES[days_key_input]; limit = vip_info["limit"]; duration_days = vip_info["duration_days"]
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
             if current_expiry > current_time: start_time = current_expiry; operation_type = "Gia hạn thêm"
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
    except Exception as e: logger.error(f"Failed to send confirmation to admin {admin_user.id}: {e}")

    user_mention = f"User ID <code>{target_user_id}</code>"
    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        if target_user_info: user_mention = target_user_info.mention_html() or f"<a href='tg://user?id={target_user_id}'>User {target_user_id}</a>"
    except Exception as e_get_chat: logger.warning(f"Could not get chat info for {target_user_id}: {e_get_chat}.")

    user_notify_msg = (f"🎉 Chúc mừng {user_mention}! 🎉\n\nBạn đã được Admin <b>{operation_type} {duration_days} ngày VIP</b>!\n\n"
                       f"✨ Gói VIP: <b>{duration_days} ngày</b>\n⏳ Hạn đến: <b>{new_expiry_str}</b>\n🚀 Limit treo: <b>{limit} tài khoản</b>\n\n"
                       f"Cảm ơn bạn đã ủng hộ DinoTool! ❤️\n(Dùng <code>/menu</code> hoặc <code>/lenh</code> để xem lại)")
    try:
        await context.bot.send_message(chat_id=target_user_id, text=user_notify_msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"Sent VIP notification for user {target_user_id} to their PM.")
    except (Forbidden, BadRequest) as e_pm:
        logger.warning(f"Failed to send VIP notification to user {target_user_id}'s PM ({e_pm}). Trying group {ALLOWED_GROUP_ID}.")
        if ALLOWED_GROUP_ID:
            try:
                await context.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=user_notify_msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                logger.info(f"Sent VIP notification for user {target_user_id} to group {ALLOWED_GROUP_ID} as fallback.")
            except Exception as e_group:
                logger.error(f"Failed to send VIP notification for user {target_user_id} to group {ALLOWED_GROUP_ID}: {e_group}")
                if admin_user.id != target_user_id:
                     try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} (PM lỗi: {e_pm}, Group lỗi: {e_group})")
                     except Exception: pass
        elif admin_user.id != target_user_id:
             try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} (PM lỗi: {e_pm}, không có group fallback)")
             except Exception: pass
    except Exception as e_send_notify:
        logger.error(f"Unexpected error sending VIP notification for user {target_user_id}: {e_send_notify}", exc_info=True)
        if admin_user.id != target_user_id:
            try: await context.bot.send_message(admin_user.id, f"⚠️ Lỗi không xác định khi gửi thông báo VIP cho user {target_user_id}. Lỗi: {e_send_notify}")
            except Exception: pass

# --- Logic Treo (Đã cập nhật để gửi thông tin ban đầu) ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Vòng lặp chạy nền cho lệnh /treo, gửi thông tin chi tiết lần đầu, ghi gain."""
    global user_daily_gains, treo_stats
    user_id_int = int(user_id_str)
    task_name = f"treo_{user_id_str}_{target_username}_in_{chat_id}"
    logger.info(f"[Treo Task Start/Resume] Task '{task_name}' started.")

    invoking_user_mention = f"User ID <code>{user_id_str}</code>"
    try:
        user_info = await context.application.bot.get_chat(user_id_int)
        if user_info and user_info.mention_html(): invoking_user_mention = user_info.mention_html()
    except Exception as e_get_mention: logger.debug(f"Could not get mention for user {user_id_str} in task {task_name}: {e_get_mention}")

    last_api_call_time = 0
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 5
    initial_info_sent = False # <<< Flag mới

    try:
        while True:
            current_time = time.time()
            app = context.application

            # 1. Kiểm tra config, task runtime, và VIP status (Quan trọng!)
            if persistent_treo_configs.get(user_id_str, {}).get(target_username) != chat_id:
                 logger.warning(f"[Treo Task Stop] Persistent config mismatch/missing for task '{task_name}'. Stopping.")
                 if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                      # Cố gắng dọn dẹp task runtime nếu nó vẫn trỏ đến task này
                      if active_treo_tasks[user_id_str][target_username] is asyncio.current_task():
                          del active_treo_tasks[user_id_str][target_username]
                          if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                          logger.info(f"[Treo Task Stop] Removed runtime task '{task_name}' due to missing/mismatched persistent config.")
                 break # Thoát loop

            if not is_user_vip(user_id_int):
                logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping task '{task_name}'.")
                await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired in loop") # Sẽ xóa cả persistent
                try: await app.bot.send_message( chat_id, f"ℹ️ {invoking_user_mention}, việc treo cho <code>@{html.escape(target_username)}</code> đã dừng do VIP hết hạn.", parse_mode=ParseMode.HTML, disable_notification=True )
                except Exception as e_send_stop: logger.warning(f"Failed to send VIP expiry stop message for task {task_name}: {e_send_stop}")
                break # Thoát loop

            # 2. Tính toán thời gian chờ
            wait_needed = TREO_INTERVAL_SECONDS - (current_time - last_api_call_time)
            if wait_needed > 0:
                logger.debug(f"[Treo Task Wait] Task '{task_name}' waiting for {wait_needed:.1f}s.")
                await asyncio.sleep(wait_needed)
            current_call_time = time.time()
            last_api_call_time = current_call_time

            # 3. Gọi API Follow
            logger.info(f"[Treo Task Run] Task '{task_name}' executing follow for @{target_username}")
            api_result = await call_follow_api(user_id_str, target_username, app.bot.token)
            success = api_result["success"]
            api_message = api_result["message"] or "Không có thông báo từ API."
            api_data = api_result.get("data", {})
            gain = 0

            if success:
                consecutive_failures = 0
                try: # Parse gain
                    gain_str = str(api_data.get("followers_add", "0"))
                    gain_match = re.search(r'[\+\-]?\d+', gain_str)
                    gain = int(gain_match.group(0)) if gain_match else 0
                except (ValueError, TypeError, KeyError, AttributeError) as e_gain:
                     logger.warning(f"[Treo Task Stats] Task '{task_name}' error parsing gain: {e_gain}. Data: {api_data}")
                if gain > 0:
                    treo_stats[user_id_str][target_username] += gain
                    user_daily_gains[user_id_str][target_username].append((current_call_time, gain))
                    logger.info(f"[Treo Task Stats] Task '{task_name}' added {gain} followers. Recorded for job & user stats.")
                else: logger.info(f"[Treo Task Success] Task '{task_name}' successful, gain reported as {gain}. API Msg: {api_message[:100]}...")

                # --- Gửi thông tin chi tiết lần đầu ---
                if not initial_info_sent:
                    try:
                        f_before = html.escape(str(api_data.get("followers_before", "?")))
                        f_after = html.escape(str(api_data.get("followers_after", "?")))
                        avatar = api_data.get("avatar", "")
                        tt_username = html.escape(api_data.get("username", target_username))
                        name = html.escape(str(api_data.get("name", "?")))

                        initial_lines = [f"🚀 Treo cho <a href='https://tiktok.com/@{tt_username}'>@{tt_username}</a> ({name}) đã bắt đầu thành công!"]
                        if avatar and avatar.startswith("http"): initial_lines.append(f"🖼️ <a href='{html.escape(avatar)}'>Ảnh đại diện</a>")
                        if f_before != "?" : initial_lines.append(f"📊 Follow ban đầu: <code>{f_before}</code>")
                        if f_after != "?" : initial_lines.append(f"📈 Follow hiện tại: <code>{f_after}</code>")
                        if gain > 0: initial_lines.append(f"✨ Lần tăng đầu tiên: <b>+{gain:,}</b>")

                        await app.bot.send_message(chat_id=chat_id, text="\n".join(initial_lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)
                        initial_info_sent = True
                        logger.info(f"[Treo Task Initial Info] Sent initial success details for task '{task_name}'.")
                    except Exception as e_send_initial: logger.error(f"Error sending initial treo info for '{task_name}': {e_send_initial}", exc_info=True)

            else: # Thất bại
                consecutive_failures += 1
                logger.warning(f"[Treo Task Fail] Task '{task_name}' failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}). API Msg: {api_message[:100]}...")
                gain = 0
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(f"[Treo Task Stop] Task '{task_name}' stopping due to {consecutive_failures} consecutive failures.")
                    await stop_treo_task(user_id_str, target_username, context, reason=f"{consecutive_failures} consecutive API failures")
                    try: await app.bot.send_message( chat_id, f"⚠️ {invoking_user_mention}: Treo cho <code>@{html.escape(target_username)}</code> đã tạm dừng do lỗi API liên tục. Vui lòng kiểm tra và thử <code>/treo</code> lại sau.", parse_mode=ParseMode.HTML, disable_notification=True )
                    except Exception as e_send_fail_stop: logger.warning(f"Failed to send consecutive failure stop message for task {task_name}: {e_send_fail_stop}")
                    break # Thoát vòng lặp

            # 4. Gửi thông báo trạng thái (Cho các lần sau hoặc lần đầu nếu lỗi gửi chi tiết)
            if initial_info_sent or not success: # Chỉ gửi nếu đã gửi info đầu hoặc bị lỗi
                 status_lines = []
                 sent_status_message = None
                 try:
                     if success:
                         # Chỉ gửi tin nhắn thành công nếu có gain > 0 để giảm spam
                         if gain > 0:
                              status_lines.append(f"✅ Treo <code>@{html.escape(target_username)}</code>: <b>+{gain:,}</b> follow ✨")
                         # Log nhưng không gửi tin nếu gain = 0
                         elif gain == 0: logger.debug(f"[Treo Task Status] Task '{task_name}' success with 0 gain. Skipping message.")
                     else: # Lỗi
                         status_lines.append(f"❌ Treo <code>@{html.escape(target_username)}</code>: Thất bại ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})")
                         status_lines.append(f"💬 <i>{html.escape(api_message[:150])}{'...' if len(api_message)>150 else ''}</i>")

                     if status_lines: # Chỉ gửi nếu có nội dung
                         status_msg = "\n".join(status_lines)
                         sent_status_message = await app.bot.send_message(chat_id=chat_id, text=status_msg, parse_mode=ParseMode.HTML, disable_notification=True)
                         # Lên lịch xóa tin nhắn thất bại
                         if not success and sent_status_message and app.job_queue:
                             job_name_del = f"del_treo_fail_{chat_id}_{sent_status_message.message_id}"
                             app.job_queue.run_once( delete_message_job, TREO_FAILURE_MSG_DELETE_DELAY, data={'chat_id': chat_id, 'message_id': sent_status_message.message_id}, name=job_name_del )
                             logger.debug(f"Scheduled job '{job_name_del}' to delete failure msg {sent_status_message.message_id} in {TREO_FAILURE_MSG_DELETE_DELAY}s.")

                 except Forbidden:
                     logger.error(f"[Treo Task Stop] Bot Forbidden in chat {chat_id}. Cannot send status for '{task_name}'. Stopping task.")
                     await stop_treo_task(user_id_str, target_username, context, reason=f"Bot Forbidden in chat {chat_id}")
                     break # Thoát loop
                 except TelegramError as e_send: logger.error(f"Error sending treo status for '{task_name}' to chat {chat_id}: {e_send}")
                 except Exception as e_unexp_send: logger.error(f"Unexpected error sending treo status for '{task_name}' to chat {chat_id}: {e_unexp_send}", exc_info=True)

            # Chờ cho chu kỳ tiếp theo (đã chuyển lên đầu vòng lặp)

    except asyncio.CancelledError:
        logger.info(f"[Treo Task Cancelled] Task '{task_name}' was cancelled externally.")
    except Exception as e:
        logger.error(f"[Treo Task Error] Unexpected error in task '{task_name}': {e}", exc_info=True)
        try: await context.application.bot.send_message(chat_id, f"💥 {invoking_user_mention}: Lỗi nghiêm trọng khi treo <code>@{html.escape(target_username)}</code>. Tác vụ đã dừng. Lỗi: {html.escape(str(e))}", parse_mode=ParseMode.HTML, disable_notification=True )
        except Exception as e_send_fatal: logger.error(f"Failed to send fatal error message for task {task_name}: {e_send_fatal}")
        await stop_treo_task(user_id_str, target_username, context, reason=f"Unexpected Error: {e}") # Dừng và xóa config
    finally:
        logger.info(f"[Treo Task End] Task '{task_name}' finished.")
        # Dọn dẹp task runtime nếu nó kết thúc mà không qua stop_treo_task
        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
            # Chỉ xóa nếu task trong dict là task hiện tại và đã xong
            current_task_obj = None
            try: current_task_obj = asyncio.current_task()
            except RuntimeError: pass # Task đã kết thúc
            if active_treo_tasks[user_id_str][target_username] is current_task_obj and current_task_obj and current_task_obj.done():
                del active_treo_tasks[user_id_str][target_username]
                if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                logger.info(f"[Treo Task Cleanup] Removed finished/failed task '{task_name}' from active tasks dict in finally block.")


# --- Lệnh /treo (VIP - Đã bỏ validation username nghiêm ngặt) ---
async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bắt đầu treo tự động follow cho một user (chỉ VIP). Lưu config."""
    global persistent_treo_configs, active_treo_tasks
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()

    if not is_user_vip(user_id):
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/treo</code> chỉ dành cho <b>VIP</b>.\nDùng <code>/muatt</code> để nâng cấp hoặc <code>/menu</code>."
        await send_temporary_message(update, context, err_msg, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # Parse Arguments (Chỉ kiểm tra trống)
    args = context.args
    target_username = None
    err_txt = None
    if not args: err_txt = ("⚠️ Chưa nhập username TikTok cần treo.\n<b>Cú pháp:</b> <code>/treo username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname: err_txt = "⚠️ Username không được trống."
        # --- VALIDATION ĐÃ BỊ XÓA THEO YÊU CẦU ---
        else: target_username = uname

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20)
        await delete_user_message(update, context, original_message_id)
        return

    # Check Giới Hạn và Trạng Thái Treo Hiện Tại
    if target_username:
        vip_limit = get_vip_limit(user_id)
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

        # Bắt đầu Task Treo Mới và Lưu Config
        task = None
        try:
            app = context.application
            # Tạo task chạy nền
            task = app.create_task( run_treo_loop(user_id_str, target_username, context, chat_id), name=f"treo_{user_id_str}_{target_username}_in_{chat_id}" )
            # Thêm task vào dict runtime và persistent config
            active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
            persistent_treo_configs.setdefault(user_id_str, {})[target_username] = chat_id
            save_data() # Lưu ngay lập tức
            logger.info(f"Successfully created task '{task.get_name()}' and saved persistent config for user {user_id} -> @{target_username} in chat {chat_id}")

            # Thông báo thành công
            new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
            treo_interval_m = TREO_INTERVAL_SECONDS // 60
            success_msg = (f"✅ <b>Bắt Đầu Treo Thành Công!</b>\n\n👤 Cho: {invoking_user_mention}\n🎯 Target: <code>@{html.escape(target_username)}</code>\n"
                           f"⏳ Tần suất: Mỗi {treo_interval_m} phút\n📊 Slot đã dùng: {new_treo_count}/{vip_limit}\n\n"
                           f"<i>(Thông tin chi tiết về follow sẽ hiện sau lần chạy thành công đầu tiên)</i>")
            await update.message.reply_html(success_msg)
            await delete_user_message(update, context, original_message_id)

        except Exception as e_start_task:
             logger.error(f"Failed to start treo task or save config for user {user_id} target @{target_username}: {e_start_task}", exc_info=True)
             await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi bắt đầu treo cho <code>@{html.escape(target_username)}</code>. Báo Admin.", duration=20)
             await delete_user_message(update, context, original_message_id)
             # Rollback cẩn thận
             if task and isinstance(task, asyncio.Task) and not task.done(): task.cancel()
             if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                 del active_treo_tasks[user_id_str][target_username]
                 if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
             if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
                  del persistent_treo_configs[user_id_str][target_username]
                  if not persistent_treo_configs[user_id_str]: del persistent_treo_configs[user_id_str]
                  save_data() # Lưu lại sau khi rollback config
    else: # target_username is None
        logger.error(f"/treo command for user {user_id}: target_username became None unexpectedly.")
        await send_temporary_message(update, context, "❌ Lỗi không xác định khi xử lý username.", duration=15)
        await delete_user_message(update, context, original_message_id)

# --- Lệnh /dungtreo (Đã sửa lỗi và thêm dừng tất cả) ---
async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dừng việc treo tự động follow cho một hoặc tất cả user."""
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html()
    args = context.args

    await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc trước

    if not args: # Dừng tất cả
        logger.info(f"User {user_id} requesting to stop ALL treo tasks.")
        stopped_count = await stop_all_treo_tasks_for_user(user_id_str, context, reason=f"User command /dungtreo all by {user_id}")
        if stopped_count > 0:
             await update.message.reply_html(f"✅ Đã dừng thành công <b>{stopped_count}</b> tài khoản đang treo.")
        else:
             await send_temporary_message(update, context, "ℹ️ Bạn hiện không có tài khoản nào đang treo để dừng.", duration=20)
    else: # Dừng một target cụ thể
        target_username_clean = args[0].strip().lstrip("@")
        if not target_username_clean:
            await send_temporary_message(update, context, "⚠️ Username không được để trống.", duration=15)
            return

        logger.info(f"User {user_id} requesting to stop treo for @{target_username_clean}")
        stopped = await stop_treo_task(user_id_str, target_username_clean, context, reason=f"User command /dungtreo by {user_id}")

        if stopped:
            new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
            vip_limit = get_vip_limit(user_id)
            limit_display = f"{vip_limit}" if is_user_vip(user_id) else "N/A"
            # <<< Sửa lỗi: Hiển thị thông báo dừng thành công >>>
            await update.message.reply_html(f"✅ Đã dừng treo và xóa cấu hình thành công cho <code>@{html.escape(target_username_clean)}</code>.\n(Slot đã dùng: {new_treo_count}/{limit_display})")
        else:
            # <<< Sửa lỗi: Hiển thị thông báo nếu không tìm thấy >>>
            await send_temporary_message(update, context, f"⚠️ Không tìm thấy cấu hình treo nào đang hoạt động hoặc đã lưu cho <code>@{html.escape(target_username_clean)}</code> để dừng.", duration=20)

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

    # Lấy danh sách từ persistent_treo_configs (đây là nguồn chính xác)
    user_treo_configs = persistent_treo_configs.get(user_id_str, {})
    treo_targets = list(user_treo_configs.keys())

    reply_lines = [f"📊 <b>Danh Sách Tài Khoản Đang Treo</b>", f"👤 Cho: {user.mention_html()}"]

    if not treo_targets:
        reply_lines.append("\nBạn hiện không treo tài khoản TikTok nào.")
        if is_user_vip(user_id): reply_lines.append("Dùng <code>/treo &lt;username&gt;</code> để bắt đầu.")
        else: reply_lines.append("Nâng cấp VIP để sử dụng tính năng này (<code>/muatt</code>).")
    else:
        vip_limit = get_vip_limit(user_id)
        is_currently_vip = is_user_vip(user_id)
        limit_display = f"{vip_limit}" if is_currently_vip else "N/A (VIP hết hạn?)"
        reply_lines.append(f"\n🔍 Số lượng: <b>{len(treo_targets)} / {limit_display}</b> tài khoản")
        for target in sorted(treo_targets):
             # Kiểm tra trạng thái ước lượng từ active_treo_tasks
             is_running = False
             if user_id_str in active_treo_tasks and target in active_treo_tasks[user_id_str]:
                  task = active_treo_tasks[user_id_str][target]
                  if task and isinstance(task, asyncio.Task) and not task.done(): is_running = True
             status_icon = "▶️" if is_running else "⏸️"
             reply_lines.append(f"  {status_icon} <code>@{html.escape(target)}</code>")
        reply_lines.append("\nℹ️ Dùng <code>/dungtreo &lt;username&gt;</code> để dừng hoặc <code>/dungtreo</code> để dừng tất cả.")
        reply_lines.append("<i>(Trạng thái ▶️/⏸️ chỉ là ước lượng tại thời điểm xem)</i>")

    reply_text = "\n".join(reply_lines)
    try:
        await delete_user_message(update, context, original_message_id)
        await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed to send /listtreo response to user {user_id} in chat {chat_id}: {e}")
        try: await delete_user_message(update, context, original_message_id) # Thử xóa lại nếu gửi lỗi
        except: pass
        await send_temporary_message(update, context, "❌ Đã có lỗi xảy ra khi lấy danh sách treo.", duration=15)

# --- Lệnh /xemfl24h (VIP) ---
async def xemfl24h_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị số follow tăng trong 24 giờ qua cho user (từ user_daily_gains)."""
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user: return
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id

    logger.info(f"User {user_id} requested /xemfl24h in chat {chat_id}")
    await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc

    # Yêu cầu VIP để xem thống kê này
    if not is_user_vip(user_id):
        err_msg = f"⚠️ {user.mention_html()}, lệnh <code>/xemfl24h</code> chỉ dành cho <b>VIP</b>."
        await send_temporary_message(update, context, err_msg, duration=20, reply=False) # Gửi không reply vì lệnh gốc đã xóa
        return

    user_gains_all_targets = user_daily_gains.get(user_id_str, {})
    gains_last_24h = defaultdict(int)
    total_gain_user = 0
    current_time = time.time()
    time_threshold = current_time - USER_GAIN_HISTORY_SECONDS # 24 giờ trước

    if not user_gains_all_targets:
        reply_text = f"📊 {user.mention_html()}, không tìm thấy dữ liệu tăng follow nào cho bạn trong 24 giờ qua."
    else:
        for target_username, gain_list in user_gains_all_targets.items():
            gain_for_target = sum(gain for ts, gain in gain_list if ts >= time_threshold)
            if gain_for_target > 0:
                gains_last_24h[target_username] += gain_for_target
                total_gain_user += gain_for_target

        reply_lines = [f"📈 <b>Follow Đã Tăng Trong 24 Giờ Qua</b>", f"👤 Cho: {user.mention_html()}"]
        if not gains_last_24h: reply_lines.append("\n<i>Không có tài khoản nào tăng follow trong 24 giờ qua.</i>")
        else:
            reply_lines.append(f"\n✨ Tổng cộng: <b>+{total_gain_user:,} follow</b>")
            sorted_targets = sorted(gains_last_24h.items(), key=lambda item: item[1], reverse=True)
            for target, gain_value in sorted_targets:
                reply_lines.append(f"  - <code>@{html.escape(target)}</code>: <b>+{gain_value:,}</b>")
        reply_lines.append(f"\n🕒 <i>Dữ liệu được tổng hợp từ các lần treo thành công gần nhất.</i>")
        reply_text = "\n".join(reply_lines)

    try:
        await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed to send /xemfl24h response to user {user_id} in chat {chat_id}: {e}")
        await send_temporary_message(update, context, "❌ Đã có lỗi xảy ra khi xem thống kê follow.", duration=15, reply=False)

# --- Lệnh /mess (Admin - Mới) ---
async def mess_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Gửi thông báo từ Admin đến nhóm chính (ALLOWED_GROUP_ID)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    if not admin_user or admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /mess attempt by {admin_user.id if admin_user else 'Unknown'}")
        return # Không phản hồi gì

    args = context.args
    original_message_id = update.message.message_id
    await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc

    if not args:
        await send_temporary_message(update, context, "⚠️ Thiếu nội dung thông báo.\n<b>Cú pháp:</b> <code>/mess Nội dung cần gửi</code>", duration=20, reply=False)
        return

    if not ALLOWED_GROUP_ID:
        await send_temporary_message(update, context, "⚠️ Không thể gửi thông báo vì <code>ALLOWED_GROUP_ID</code> chưa được cấu hình trong bot.", duration=30, reply=False)
        logger.warning(f"Admin {admin_user.id} tried /mess but ALLOWED_GROUP_ID is not set.")
        return

    message_text = update.message.text.split(' ', 1)[1] # Lấy toàn bộ text sau /mess
    message_to_send = f"📢 <b>Thông báo từ Admin ({admin_user.mention_html()}):</b>\n\n{html.escape(message_text)}"

    try:
        await context.bot.send_message(ALLOWED_GROUP_ID, message_to_send, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await send_temporary_message(update, context, "✅ Đã gửi thông báo thành công đến nhóm chính.", duration=15, reply=False)
        logger.info(f"Admin {admin_user.id} sent message to group {ALLOWED_GROUP_ID}")
    except Forbidden:
        await send_temporary_message(update, context, f"❌ Lỗi: Bot không có quyền gửi tin nhắn vào nhóm <code>{ALLOWED_GROUP_ID}</code>. Kiểm tra xem bot có trong nhóm và có quyền gửi tin không.", duration=30, reply=False)
        logger.error(f"Failed to send /mess to group {ALLOWED_GROUP_ID}: Bot Forbidden.")
    except BadRequest as e:
        await send_temporary_message(update, context, f"❌ Lỗi gửi thông báo đến nhóm <code>{ALLOWED_GROUP_ID}</code>: {html.escape(str(e))}", duration=30, reply=False)
        logger.error(f"Failed to send /mess to group {ALLOWED_GROUP_ID}: BadRequest - {e}")
    except Exception as e:
        await send_temporary_message(update, context, f"❌ Lỗi không xác định khi gửi thông báo: {html.escape(str(e))}", duration=30, reply=False)
        logger.error(f"Unexpected error sending /mess to group {ALLOWED_GROUP_ID}: {e}", exc_info=True)

# --- Job Thống Kê Follow Tăng (Dùng treo_stats) ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    """Job chạy định kỳ để thống kê và báo cáo user treo tăng follow (dùng treo_stats)."""
    global last_stats_report_time, treo_stats
    current_time = time.time()
    if last_stats_report_time != 0 and current_time < last_stats_report_time + TREO_STATS_INTERVAL_SECONDS * 0.95:
        logger.debug(f"[Stats Job] Skipping report, not time yet.")
        return

    logger.info(f"[Stats Job] Starting statistics report job.")
    target_chat_id_for_stats = ALLOWED_GROUP_ID

    if not target_chat_id_for_stats:
        logger.info("[Stats Job] ALLOWED_GROUP_ID is not set. Stats report skipped.")
        if treo_stats: treo_stats.clear(); save_data() # Xóa stats cũ nếu không báo cáo được
        last_stats_report_time = current_time # Cập nhật thời gian để không check lại ngay
        return

    stats_snapshot = {}
    if treo_stats:
        try: stats_snapshot = json.loads(json.dumps(treo_stats)) # Deep copy
        except Exception as e_copy: logger.error(f"[Stats Job] Error creating stats snapshot: {e_copy}. Aborting."); return

    # Xóa stats hiện tại và cập nhật thời gian báo cáo NGAY LẬP TỨC
    treo_stats.clear()
    last_stats_report_time = current_time
    save_data()
    logger.info(f"[Stats Job] Cleared current job stats and updated last report time. Processing snapshot with {len(stats_snapshot)} users.")

    if not stats_snapshot:
        logger.info("[Stats Job] No stats data found in snapshot. Skipping report content.")
        return

    top_gainers = []
    total_gain_all = 0
    for user_id_str, targets in stats_snapshot.items():
        if isinstance(targets, dict):
            for target_username, gain in targets.items():
                try:
                    gain_int = int(gain)
                    if gain_int > 0:
                        top_gainers.append((gain_int, str(user_id_str), str(target_username)))
                        total_gain_all += gain_int
                    elif gain_int < 0: logger.warning(f"[Stats Job] Negative gain ({gain_int}) found for {user_id_str}->{target_username}.")
                except (ValueError, TypeError): logger.warning(f"[Stats Job] Invalid gain type ({type(gain)}) for {user_id_str}->{target_username}.")
        else: logger.warning(f"[Stats Job] Invalid target structure for user {user_id_str} in snapshot.")

    if not top_gainers:
        logger.info("[Stats Job] No positive gains found after processing snapshot. Skipping report generation.")
        return

    top_gainers.sort(key=lambda x: x[0], reverse=True)
    report_lines = [f"📊 <b>Thống Kê Tăng Follow (Chu Kỳ Vừa Qua)</b> 📊",
                    f"<i>(Tổng cộng: <b>{total_gain_all:,}</b> follow được tăng bởi các tài khoản đang treo)</i>",
                    "\n🏆 <b>Top Tài Khoản Treo Hiệu Quả Nhất:</b>"]
    num_top_to_show = 10
    user_mentions_cache = {}
    app = context.application

    for i, (gain, user_id_str_gain, target_username_gain) in enumerate(top_gainers[:num_top_to_show]):
        user_mention = user_mentions_cache.get(user_id_str_gain)
        if not user_mention:
            try:
                user_info = await app.bot.get_chat(int(user_id_str_gain))
                m = user_info.mention_html() or f"<a href='tg://user?id={user_id_str_gain}'>User {user_id_str_gain}</a>"
                user_mention = m if m else f"User <code>{user_id_str_gain}</code>"
            except Exception as e_get_chat:
                logger.warning(f"[Stats Job] Failed to get mention for user {user_id_str_gain}: {e_get_chat}")
                user_mention = f"User <code>{user_id_str_gain}</code>"
            user_mentions_cache[user_id_str_gain] = user_mention
        rank_icon = ["🥇", "🥈", "🥉"][i] if i < 3 else "🏅"
        report_lines.append(f"  {rank_icon} <b>+{gain:,} follow</b> cho <code>@{html.escape(target_username_gain)}</code> (Treo bởi: {user_mention})")

    if not user_mentions_cache: report_lines.append("  <i>Không có dữ liệu tăng follow đáng kể trong chu kỳ này.</i>")
    treo_interval_m = TREO_INTERVAL_SECONDS // 60
    stats_interval_h = TREO_STATS_INTERVAL_SECONDS // 3600
    report_lines.append(f"\n🕒 <i>Cập nhật tự động sau mỗi {stats_interval_h} giờ. Treo chạy mỗi {treo_interval_m} phút.</i>")
    report_text = "\n".join(report_lines)

    try:
        await app.bot.send_message(chat_id=target_chat_id_for_stats, text=report_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)
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
    for task in tasks_to_cancel:
        if task and not task.done(): task.cancel()
    results = await asyncio.gather(*[asyncio.wait_for(task, timeout=timeout) for task in tasks_to_cancel], return_exceptions=True)
    logger.info("[Shutdown] Finished waiting for treo task cancellations.")
    cancelled_count, errors_count, finished_count = 0, 0, 0
    for i, result in enumerate(results):
        task_name = f"Task_{i}"
        try: task_name = tasks_to_cancel[i].get_name() or task_name
        except: pass
        if isinstance(result, asyncio.CancelledError): cancelled_count += 1; logger.info(f"[Shutdown] Task '{task_name}' confirmed cancelled.")
        elif isinstance(result, asyncio.TimeoutError): errors_count += 1; logger.warning(f"[Shutdown] Task '{task_name}' timed out during cancellation.")
        elif isinstance(result, Exception): errors_count += 1; logger.error(f"[Shutdown] Error occurred in task '{task_name}' during cancellation: {result}", exc_info=False)
        else: finished_count += 1
    logger.info(f"[Shutdown] Task summary: {cancelled_count} cancelled, {errors_count} errors/timeouts, {finished_count} finished normally.")

# --- Main Function (Đã cập nhật để khôi phục task treo) ---
def main() -> None:
    start_time = time.time()
    print("--- Bot DinoTool Starting ---"); print(f"Timestamp: {datetime.now().isoformat()}")
    print("\n--- Configuration Summary ---")
    print(f"BOT_TOKEN: ...{BOT_TOKEN[-6:]}")
    print(f"ADMIN_USER_ID: {ADMIN_USER_ID}")
    print(f"BILL_FORWARD_TARGET_ID: {BILL_FORWARD_TARGET_ID}")
    print(f"ALLOWED_GROUP_ID: {ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else 'None (Stats/Mess Disabled)'}")
    print(f"API_KEY (Tim): {'Set' if API_KEY else 'Not Set'}")
    print(f"LINK_SHORTENER_API_KEY: {'Set' if LINK_SHORTENER_API_KEY else '!!! Missing !!!'}")
    print(f"Cooldowns: Tim/Fl={TIM_FL_COOLDOWN_SECONDS/60:.0f}m | GetKey={GETKEY_COOLDOWN_SECONDS/60:.0f}m")
    print(f"Durations: KeyExpiry={KEY_EXPIRY_SECONDS/3600:.1f}h | Activation={ACTIVATION_DURATION_SECONDS/3600:.1f}h | GainHistory={USER_GAIN_HISTORY_SECONDS/3600:.0f}h")
    print(f"Treo: Interval={TREO_INTERVAL_SECONDS / 60:.1f}m | Fail Delete Delay={TREO_FAILURE_MSG_DELETE_DELAY}s | Stats Interval={TREO_STATS_INTERVAL_SECONDS / 3600:.1f}h")
    print(f"Group Link (for menu): {GROUP_LINK if GROUP_LINK != 'YOUR_GROUP_INVITE_LINK' else 'Not Set!'}")
    print("-" * 30)

    print("Loading persistent data...")
    load_data()
    persistent_treo_count = sum(len(targets) for targets in persistent_treo_configs.values())
    gain_user_count = len(user_daily_gains)
    gain_entry_count = sum(len(gl) for targets in user_daily_gains.values() for gl in targets.values())
    print(f"Load complete. Keys: {len(valid_keys)}, Activated: {len(activated_users)}, VIPs: {len(vip_users)}")
    print(f"Persistent Treo: {persistent_treo_count} targets for {len(persistent_treo_configs)} users")
    print(f"User Daily Gains: {gain_entry_count} entries for {gain_user_count} users")
    print(f"Initial Job Stats Users: {len(treo_stats)}, Last Report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")

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
            logger.info(f"Scheduled statistics report job every {TREO_STATS_INTERVAL_SECONDS / 3600:.1f} hours (to group {ALLOWED_GROUP_ID}).")
        else: logger.info("Statistics report job skipped (ALLOWED_GROUP_ID not set).")
    else: logger.error("JobQueue is not available. Scheduled jobs will not run.")

    # --- Register Handlers ---
    # Commands
    application.add_handler(CommandHandler(("start", "menu"), start_command))
    application.add_handler(CommandHandler("lenh", lenh_command))
    application.add_handler(CommandHandler("getkey", getkey_command))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command))
    application.add_handler(CommandHandler("tim", tim_command))
    application.add_handler(CommandHandler("fl", fl_command))
    application.add_handler(CommandHandler("muatt", muatt_command))
    application.add_handler(CommandHandler("treo", treo_command))
    application.add_handler(CommandHandler("dungtreo", dungtreo_command))
    application.add_handler(CommandHandler("listtreo", listtreo_command))
    application.add_handler(CommandHandler("xemfl24h", xemfl24h_command)) # Lệnh xem gain 24h
    # Admin Commands
    application.add_handler(CommandHandler("addtt", addtt_command))
    application.add_handler(CommandHandler("mess", mess_command)) # <<< Lệnh mới /mess

    # Callback Handlers
    application.add_handler(CallbackQueryHandler(menu_callback_handler, pattern="^show_(muatt|lenh)$"))
    application.add_handler(CallbackQueryHandler(prompt_send_bill_callback, pattern="^prompt_send_bill_\d+$"))

    # Message handler cho ảnh bill (Ưu tiên cao để bắt trước khi bot khác xử lý)
    photo_bill_filter = (filters.PHOTO | filters.Document.IMAGE) & (~filters.COMMAND) & filters.UpdateType.MESSAGE
    application.add_handler(MessageHandler(photo_bill_filter, handle_photo_bill), group=-1)
    logger.info("Registered photo/bill handler (priority -1) for pending users.")
    # --- End Handler Registration ---

    # --- Khởi động lại các task treo đã lưu ---
    print("\nRestarting persistent treo tasks...")
    restored_count = 0
    users_to_cleanup_restore = []
    tasks_to_create_data = [] # (user_id_str, target_username, chat_id_int)
    persistent_treo_snapshot = dict(persistent_treo_configs) # Lấy bản sao

    if persistent_treo_snapshot:
        for user_id_str, targets_for_user in persistent_treo_snapshot.items():
            try:
                user_id_int = int(user_id_str)
                if not is_user_vip(user_id_int):
                    logger.warning(f"[Restore] User {user_id_str} from persistent config is no longer VIP. Scheduling config cleanup.")
                    users_to_cleanup_restore.append(user_id_str)
                    continue # Bỏ qua user này

                vip_limit = get_vip_limit(user_id_int)
                current_user_restored_count = 0
                targets_snapshot = dict(targets_for_user) # Lấy bản sao target của user

                for target_username, chat_id_int in targets_snapshot.items():
                    if current_user_restored_count >= vip_limit:
                         logger.warning(f"[Restore] User {user_id_str} reached VIP limit ({vip_limit}). Skipping persistent target @{target_username}.")
                         if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
                              del persistent_treo_configs[user_id_str][target_username] # Xóa config dư thừa
                         continue

                    # Kiểm tra task runtime (hiếm khi cần thiết nhưng để chắc chắn)
                    runtime_task = active_treo_tasks.get(user_id_str, {}).get(target_username)
                    if runtime_task and isinstance(runtime_task, asyncio.Task) and not runtime_task.done():
                         logger.info(f"[Restore] Task for {user_id_str} -> @{target_username} seems already active. Skipping restore.")
                         current_user_restored_count += 1; continue

                    logger.info(f"[Restore] Scheduling restore for treo task: user {user_id_str} -> @{target_username} in chat {chat_id_int}")
                    tasks_to_create_data.append((user_id_str, target_username, chat_id_int))
                    current_user_restored_count += 1

            except ValueError:
                logger.error(f"[Restore] Invalid user_id '{user_id_str}' in persistent_treo_configs. Scheduling cleanup.")
                users_to_cleanup_restore.append(user_id_str)
            except Exception as e_outer_restore:
                logger.error(f"[Restore] Unexpected error processing persistent treo config for user {user_id_str}: {e_outer_restore}", exc_info=True)
                users_to_cleanup_restore.append(user_id_str)

    # Dọn dẹp config persistent của user không hợp lệ/hết VIP/vượt limit
    cleaned_persistent_configs_on_restore = False
    if users_to_cleanup_restore:
        unique_users_to_cleanup = set(users_to_cleanup_restore)
        logger.info(f"[Restore] Cleaning up persistent treo configs for {len(unique_users_to_cleanup)} non-VIP or invalid users...")
        for user_id_str_clean in unique_users_to_cleanup:
            if user_id_str_clean in persistent_treo_configs:
                del persistent_treo_configs[user_id_str_clean]; cleaned_persistent_configs_on_restore = True
    # Check lại xem có config nào bị xóa do vượt limit không
    for uid, targets_orig in persistent_treo_snapshot.items():
         if uid in persistent_treo_configs and len(persistent_treo_configs.get(uid, {})) < len(targets_orig):
             cleaned_persistent_configs_on_restore = True; break
    # Lưu lại nếu có thay đổi config persistent
    if cleaned_persistent_configs_on_restore:
        logger.info("[Restore] Saving data after cleaning up non-VIP/invalid/over-limit persistent configs during restore.")
        save_data()

    # Tạo các task treo đã lên lịch
    if tasks_to_create_data:
        logger.info(f"[Restore] Creating {len(tasks_to_create_data)} restored treo tasks...")
        default_context = ContextTypes.DEFAULT_TYPE(application=application, chat_id=None, user_id=None)
        for user_id_str_create, target_username_create, chat_id_int_create in tasks_to_create_data:
            try:
                task = application.create_task(
                    run_treo_loop(user_id_str_create, target_username_create, default_context, chat_id_int_create),
                    name=f"treo_{user_id_str_create}_{target_username_create}_in_{chat_id_int_create}_restored" )
                active_treo_tasks.setdefault(user_id_str_create, {})[target_username_create] = task
                restored_count += 1
            except Exception as e_create:
                logger.error(f"[Restore] Failed to create restored task for {user_id_str_create} -> @{target_username_create}: {e_create}", exc_info=True)

    print(f"Successfully restored and started {restored_count} treo tasks."); print("-" * 30)
    # --- Kết thúc khôi phục task ---

    print("\nBot initialization complete. Starting polling...")
    logger.info("Bot initialization complete. Starting polling...")
    run_duration = time.time() - start_time; print(f"(Initialization took {run_duration:.2f} seconds)")

    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt: print("\nCtrl+C detected. Stopping bot gracefully..."); logger.info("KeyboardInterrupt detected. Stopping bot...")
    except Exception as e: print(f"\nCRITICAL ERROR: Bot stopped due to: {e}"); logger.critical(f"CRITICAL ERROR: Bot stopped: {e}", exc_info=True)
    finally:
        print("\nInitiating shutdown sequence..."); logger.info("Initiating shutdown sequence...")
        # Thu thập task đang chạy
        tasks_to_stop_on_shutdown = []
        if active_treo_tasks:
            logger.info("[Shutdown] Collecting active runtime treo tasks...")
            for targets in list(active_treo_tasks.values()):
                for task in list(targets.values()):
                    if task and isinstance(task, asyncio.Task) and not task.done(): tasks_to_stop_on_shutdown.append(task)
        # Hủy task
        if tasks_to_stop_on_shutdown:
            print(f"[Shutdown] Found {len(tasks_to_stop_on_shutdown)} active runtime treo tasks. Attempting cancellation...")
            try:
                 loop = asyncio.get_event_loop_policy().get_event_loop()
                 loop.create_task(shutdown_async_tasks(tasks_to_stop_on_shutdown, timeout=2.0)) # Chạy và quên
                 print("[Shutdown] Cancellation tasks scheduled. Proceeding...")
            except Exception as e_shutdown: logger.error(f"[Shutdown] Error scheduling async task cancellation: {e_shutdown}", exc_info=True)
        else: print("[Shutdown] No active runtime treo tasks found.")

        # Lưu dữ liệu lần cuối
        print("[Shutdown] Attempting final data save..."); logger.info("Attempting final data save...")
        save_data()
        print("[Shutdown] Final data save attempt complete.")
        print("Bot has stopped."); logger.info("Bot has stopped."); print(f"Shutdown timestamp: {datetime.now().isoformat()}")

if __name__ == "__main__":
    try: main()
    except Exception as e_fatal:
        print(f"\nFATAL ERROR: Could not execute main function: {e_fatal}")
        logging.critical(f"FATAL ERROR preventing main execution: {e_fatal}", exc_info=True)
        # Ghi lỗi nghiêm trọng ra file riêng nếu cần
        try:
            with open("fatal_error.log", "a", encoding='utf-8') as f:
                import traceback
                f.write(f"\n--- {datetime.now().isoformat()} ---\nFATAL ERROR: {e_fatal}\n"); traceback.print_exc(file=f); f.write("-" * 30 + "\n")
        except Exception as e_log: print(f"Additionally, failed to write fatal error to log file: {e_log}")
