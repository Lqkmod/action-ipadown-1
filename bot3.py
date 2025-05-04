
import logging
import httpx
import json
import html
import os
import time
import random
import string
import re # Đảm bảo đã import
import asyncio
import traceback # Import traceback ở đầu file cho rõ ràng
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import quote # Dùng để mã hóa link cho API

# Thêm import cho Inline Keyboard và các thành phần khác
from telegram import Update, Message, InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
    CallbackQueryHandler,
    ApplicationHandlerStop,
    TypeHandler # Để xử lý shutdown
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError

# --- Cấu hình ---
# !!! THAY THẾ CÁC GIÁ TRỊ PLACEHOLDER BÊN DƯỚI BẰNG GIÁ TRỊ THỰC TẾ CỦA BẠN !!!
BOT_TOKEN = "7416039734:AAE8-vdkSpCzJRvml2nDJip6O1wbkRes2oY" # <--- TOKEN CỦA BOT TELEGRAM CỦA BẠN
API_KEY_TIM = "khangdino99" # <--- API KEY TIM (NẾU CÓ, DÙNG CHO LỆNH /tim) - Có thể để trống nếu không dùng /tim
ADMIN_USER_ID = 7193749511 # <<< --- ID TELEGRAM SỐ CỦA ADMIN (Lấy từ @userinfobot)
BILL_FORWARD_TARGET_ID = 7193749511 # <<< --- ID TELEGRAM SỐ CỦA NƠI NHẬN BILL (VD: ID của @khangtaixiu_bot hoặc Admin)
ALLOWED_GROUP_ID = -1002191171631 # <--- ID NHÓM CHÍNH (SỐ ÂM) hoặc None (Nếu None, một số tính năng báo cáo/nhắc nhở nhóm/ /mess sẽ tắt)
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
TREO_STATS_INTERVAL_SECONDS = 12 * 3600 # 12 giờ (Thống kê follow tăng qua job)
USER_GAIN_HISTORY_SECONDS = 24 * 3600 # Lưu lịch sử gain trong 24 giờ cho /xemfl24h
PENDING_BILL_TIMEOUT_SECONDS = 15 * 60 # 15 phút (Timeout chờ gửi bill sau khi bấm nút)
API_TIMEOUT_SECONDS = 90.0 # Timeout mặc định cho các cuộc gọi API (tăng lên)

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM (Cần API_KEY_TIM)
FOLLOW_API_URL_BASE = "https://api.thanhtien.site/lynk/dino/telefl.php" # API FOLLOW MỚI
CHECK_TIKTOK_API_URL = "https://khangdino.x10.mx/fltik.php" # <<< API /check mới
CHECK_TIKTOK_API_KEY = "khang" # <<< Key cố định cho API /check
SOUNDCLOUD_API_URL = "https://kudodz.x10.mx/api/soundcloud.php" # <<< API /sound mới

# --- Thông tin VIP ---
VIP_PRICES = {
    # days_key: {"price": "Display Price", "limit": max_treo_users, "duration_days": days}
    15: {"price": "15.000 VND", "limit": 2, "duration_days": 15},
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
}

# --- Biến toàn cục (Sẽ được load/save) ---
# Sử dụng string keys cho user ID để đảm bảo tương thích JSON
user_tim_cooldown = {} # {user_id_str: timestamp}
user_fl_cooldown = defaultdict(lambda: defaultdict(float)) # {user_id_str: {target_username: timestamp}}
user_getkey_cooldown = {} # {user_id_str: timestamp}
valid_keys = {} # {key: {"user_id_generator": int, "generation_time": float, "expiry_time": float, "used_by": int | None, "activation_time": float | None}}
activated_users = {} # {user_id_str: expiry_timestamp} - Người dùng kích hoạt bằng key
vip_users = {} # {user_id_str: {"expiry": float, "limit": int}} - Người dùng VIP
persistent_treo_configs = {} # {user_id_str: {target_username_str: chat_id_int}} - Lưu để khôi phục sau restart
treo_stats = defaultdict(lambda: defaultdict(int)) # {user_id_str: {target_username: gain_since_last_report}} - Dùng cho job thống kê
user_daily_gains = defaultdict(lambda: defaultdict(list)) # {uid_str: {target_str: [(ts_float, gain_int)]}} - Dùng cho /xemfl24h
last_stats_report_time = 0.0 # Thời điểm báo cáo thống kê gần nhất

# --- Biến Runtime (Không lưu) ---
active_treo_tasks = {} # {user_id_str: {target_username_str: asyncio.Task}} - Lưu các task /treo đang chạy
pending_bill_user_ids = set() # Set of user_ids (int) - Chờ gửi bill
# --- HTTP Client dùng chung (Cải thiện hiệu năng) ---
# Tạo session dùng chung, sẽ được khởi tạo trong main() và đóng khi tắt
http_client: httpx.AsyncClient | None = None


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
if not BOT_TOKEN or BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN": logger.critical("BOT_TOKEN is not set!")
if not ADMIN_USER_ID or not isinstance(ADMIN_USER_ID, int): logger.critical("ADMIN_USER_ID is not set or invalid!")
if not BILL_FORWARD_TARGET_ID or not isinstance(BILL_FORWARD_TARGET_ID, int): logger.warning("BILL_FORWARD_TARGET_ID is not set, bill forwarding will fail!")
if ALLOWED_GROUP_ID and not isinstance(ALLOWED_GROUP_ID, int): logger.warning("ALLOWED_GROUP_ID is set but invalid format (should be negative integer).")
if ALLOWED_GROUP_ID and not GROUP_LINK: logger.warning("ALLOWED_GROUP_ID is set, but GROUP_LINK is missing.")
if not LINK_SHORTENER_API_KEY: logger.warning("LINK_SHORTENER_API_KEY is not set, /getkey will fail.")
if not QR_CODE_URL: logger.warning("QR_CODE_URL is not set for /muatt.")
if not BANK_ACCOUNT or not BANK_NAME or not ACCOUNT_NAME: logger.warning("Bank payment details are incomplete for /muatt.")
if not API_KEY_TIM and VIDEO_API_URL_TEMPLATE: logger.warning("API_KEY_TIM is not set, /tim might not work.")
if not CHECK_TIKTOK_API_KEY: logger.warning("CHECK_TIKTOK_API_KEY is not set, /check might not work as intended.")
# Không cần kiểm tra SOUNDCLOUD_API_URL vì không có key

logger.info("--- Cấu hình cơ bản đã được kiểm tra ---")
logger.info(f"Admin ID: {ADMIN_USER_ID}")
logger.info(f"Bill Forward Target: {BILL_FORWARD_TARGET_ID}")
logger.info(f"Allowed Group ID: {ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else 'Không giới hạn (/mess, /stats disabled)'}")
logger.info(f"Treo Interval: {TREO_INTERVAL_SECONDS / 60:.1f} phút")
logger.info(f"VIP Packages: {list(VIP_PRICES.keys())} ngày")


# --- Hàm lưu/tải dữ liệu ---
def save_data():
    global persistent_treo_configs, user_daily_gains, last_stats_report_time
    string_key_activated_users = {str(k): v for k, v in activated_users.items()}
    string_key_tim_cooldown = {str(k): v for k, v in user_tim_cooldown.items()}
    string_key_fl_cooldown = {str(uid): {str(uname): float(ts) for uname, ts in udict.items()} for uid, udict in user_fl_cooldown.items()}
    string_key_getkey_cooldown = {str(k): v for k, v in user_getkey_cooldown.items()}
    string_key_vip_users = {str(k): v for k, v in vip_users.items()}
    string_key_treo_stats = {str(uid): {str(target): int(gain) for target, gain in targets.items()} for uid, targets in treo_stats.items()}
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
        "version": 2,
        "valid_keys": valid_keys,
        "activated_users": string_key_activated_users,
        "vip_users": string_key_vip_users,
        "user_cooldowns": {
            "tim": string_key_tim_cooldown,
            "fl": string_key_fl_cooldown,
            "getkey": string_key_getkey_cooldown
        },
        "treo_stats": string_key_treo_stats,
        "last_stats_report_time": float(last_stats_report_time),
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
                data_version = data.get("version", 1)

                valid_keys = data.get("valid_keys", {})
                activated_users = {str(k): float(v) for k, v in data.get("activated_users", {}).items()}
                vip_users = {str(k): {"expiry": float(v.get("expiry", 0)), "limit": int(v.get("limit", 0))} for k, v in data.get("vip_users", {}).items() if isinstance(v, dict)}

                all_cooldowns = data.get("user_cooldowns", {})
                user_tim_cooldown = {str(k): float(v) for k, v in all_cooldowns.get("tim", {}).items()}

                user_fl_cooldown = defaultdict(lambda: defaultdict(float))
                loaded_fl_cooldown = all_cooldowns.get("fl", {})
                if isinstance(loaded_fl_cooldown, dict):
                    for uid_str, targets_dict in loaded_fl_cooldown.items():
                        if isinstance(targets_dict, dict):
                            for target_str, ts_float in targets_dict.items():
                                try: user_fl_cooldown[str(uid_str)][str(target_str)] = float(ts_float)
                                except (ValueError, TypeError): logger.warning(f"Skipping invalid FL cooldown timestamp for {uid_str} -> {target_str}")
                        else: logger.warning(f"Invalid targets_dict type for FL cooldown user {uid_str}")

                user_getkey_cooldown = {str(k): float(v) for k, v in all_cooldowns.get("getkey", {}).items()}

                loaded_stats = data.get("treo_stats", {})
                treo_stats = defaultdict(lambda: defaultdict(int))
                if isinstance(loaded_stats, dict):
                    for uid_str, targets in loaded_stats.items():
                        if isinstance(targets, dict):
                            for target_str, gain in targets.items():
                                try: treo_stats[str(uid_str)][str(target_str)] = int(gain)
                                except (ValueError, TypeError): logger.warning(f"Skipping invalid treo stat entry: user {uid_str}, target {target_str}, gain {gain}")
                        else: logger.warning(f"Invalid targets type for user {uid_str} in treo_stats: {type(targets)}")

                try: last_stats_report_time = float(data.get("last_stats_report_time", 0.0))
                except (ValueError, TypeError): last_stats_report_time = 0.0

                loaded_persistent_treo = data.get("persistent_treo_configs", {})
                persistent_treo_configs = {}
                if isinstance(loaded_persistent_treo, dict):
                    for uid_str, configs in loaded_persistent_treo.items():
                        user_id_key = str(uid_str)
                        persistent_treo_configs[user_id_key] = {}
                        if isinstance(configs, dict):
                            for target_str, chatid in configs.items():
                                try: persistent_treo_configs[user_id_key][str(target_str)] = int(chatid)
                                except (ValueError, TypeError): logger.warning(f"Skipping invalid persistent treo config entry: user {user_id_key}, target {target_str}, chatid {chatid}")
                        else: logger.warning(f"Invalid config type for user {user_id_key} in persistent_treo_configs: {type(configs)}. Skipping.")
                else: logger.warning(f"persistent_treo_configs in data file is not a dict: {type(loaded_persistent_treo)}. Initializing empty.")

                loaded_daily_gains = data.get("user_daily_gains", {})
                user_daily_gains = defaultdict(lambda: defaultdict(list))
                if isinstance(loaded_daily_gains, dict):
                    for uid_str, targets_data in loaded_daily_gains.items():
                        user_id_key = str(uid_str)
                        if isinstance(targets_data, dict):
                            for target_str, gain_list in targets_data.items():
                                target_key = str(target_str)
                                if isinstance(gain_list, list):
                                    valid_gains = []
                                    for item in gain_list:
                                        try:
                                            if isinstance(item, (list, tuple)) and len(item) == 2:
                                                ts = float(item[0])
                                                g = int(item[1])
                                                if isinstance(ts, float) and isinstance(g, int): valid_gains.append((ts, g))
                                                else: logger.warning(f"Skipping invalid gain entry types for user {user_id_key}, target {target_key}: ts={type(ts)}, g={type(g)}")
                                            else: logger.warning(f"Skipping invalid gain entry format for user {user_id_key}, target {target_key}: {item}")
                                        except (ValueError, TypeError, IndexError): logger.warning(f"Skipping invalid gain entry value parsing for user {user_id_key}, target {target_key}: {item}")
                                    if valid_gains:
                                        valid_gains.sort(key=lambda x: x[0])
                                        user_daily_gains[user_id_key][target_key].extend(valid_gains)
                                else: logger.warning(f"Invalid gain_list type for user {user_id_key}, target {target_key}: {type(gain_list)}. Skipping.")
                        else: logger.warning(f"Invalid targets_data type for user {user_id_key} in user_daily_gains: {type(targets_data)}. Skipping.")
                else: logger.warning(f"user_daily_gains in data file is not a dict: {type(loaded_daily_gains)}. Initializing empty.")

                logger.info(f"Data loaded successfully from {DATA_FILE} (Version: {data_version})")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            valid_keys, activated_users, vip_users = {}, {}, {}
            user_tim_cooldown, user_getkey_cooldown = {}, {}
            user_fl_cooldown = defaultdict(lambda: defaultdict(float))
            treo_stats = defaultdict(lambda: defaultdict(int))
            last_stats_report_time = 0.0
            persistent_treo_configs = {}
            user_daily_gains = defaultdict(lambda: defaultdict(list))
    except (json.JSONDecodeError, TypeError, ValueError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Backing up corrupted file and using empty data.", exc_info=True)
        if os.path.exists(DATA_FILE):
            try:
                backup_file = f"{DATA_FILE}.corrupted_{int(time.time())}.json"
                os.rename(DATA_FILE, backup_file)
                logger.info(f"Backed up corrupted data file to {backup_file}")
            except Exception as e_backup:
                logger.error(f"Could not backup corrupted data file {DATA_FILE}: {e_backup}")
        valid_keys, activated_users, vip_users = {}, {}, {}
        user_tim_cooldown, user_getkey_cooldown = {}, {}
        user_fl_cooldown = defaultdict(lambda: defaultdict(float))
        treo_stats = defaultdict(lambda: defaultdict(int))
        last_stats_report_time = 0.0
        persistent_treo_configs = {}
        user_daily_gains = defaultdict(lambda: defaultdict(list))

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return
    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except Forbidden: logger.debug(f"Cannot delete message {msg_id_to_delete} in chat {original_chat_id}.")
    except BadRequest as e:
        common_delete_errors = ["message to delete not found", "message can't be deleted", "message_id_invalid", "message identifier is not specified"]
        if any(err in str(e).lower() for err in common_delete_errors): logger.debug(f"Could not delete message {msg_id_to_delete}: {e}")
        else: logger.warning(f"BadRequest error deleting message {msg_id_to_delete}: {e}")
    except Exception as e: logger.error(f"Unexpected error deleting message {msg_id_to_delete}: {e}", exc_info=True)

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    chat_id = job_data.get('chat_id')
    message_id = job_data.get('message_id')
    job_name = context.job.name or "delete_message_job"
    if chat_id and message_id:
        logger.debug(f"Job '{job_name}' running to delete message {message_id} in chat {chat_id}")
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(f"Job '{job_name}' successfully deleted message {message_id}")
        except Forbidden: logger.info(f"Job '{job_name}' cannot delete message {message_id}.")
        except BadRequest as e:
            common_delete_errors = ["message to delete not found", "message can't be deleted"]
            if any(err in str(e).lower() for err in common_delete_errors): logger.info(f"Job '{job_name}' could not delete message {message_id}: {e}")
            else: logger.warning(f"Job '{job_name}' BadRequest deleting message {message_id}: {e}")
        except TelegramError as e: logger.warning(f"Job '{job_name}' Telegram error deleting message {message_id}: {e}")
        except Exception as e: logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else: logger.warning(f"Job '{job_name}' called missing chat_id or message_id.")

async def send_temporary_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, duration: int = 15, parse_mode: str = ParseMode.HTML, reply: bool = True):
    if not update or not update.effective_chat: return
    chat_id = update.effective_chat.id
    sent_message = None
    try:
        reply_to_msg_id = update.message.message_id if reply and update.message else None
        send_params = {'chat_id': chat_id, 'text': text, 'parse_mode': parse_mode, 'disable_web_page_preview': True}
        if reply_to_msg_id: send_params['reply_to_message_id'] = reply_to_msg_id
        try: sent_message = await context.bot.send_message(**send_params)
        except BadRequest as e:
            if reply_to_msg_id and "reply message not found" in str(e).lower():
                 logger.debug(f"Reply message {reply_to_msg_id} not found for temporary message. Sending without reply.")
                 del send_params['reply_to_message_id']
                 sent_message = await context.bot.send_message(**send_params)
            else: raise
        except (Forbidden, TelegramError) as e_send: logger.error(f"Error sending temporary message to {chat_id}: {e_send}"); return
        if sent_message and context.job_queue:
            job_name = f"del_temp_{chat_id}_{sent_message.message_id}"
            context.job_queue.run_once( delete_message_job, duration, data={'chat_id': chat_id, 'message_id': sent_message.message_id}, name=job_name)
            logger.debug(f"Scheduled job '{job_name}' to delete message {sent_message.message_id} in {duration}s")
    except Exception as e: logger.error(f"Unexpected error in send_temporary_message to {chat_id}: {e}", exc_info=True)

def generate_random_key(length=8):
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"Dinotool-{random_part}"

# --- Hàm API Call chung ---
async def make_api_request(url: str, params: dict | None = None, method: str = "GET", timeout: float = API_TIMEOUT_SECONDS) -> dict:
    """Hàm chung để thực hiện các cuộc gọi API HTTP và xử lý lỗi cơ bản."""
    global http_client
    result = {"success": False, "status_code": None, "data": None, "error": "Unknown error"}
    request_func = None

    # Tạo http_client nếu chưa có (trường hợp gọi trước khi main() khởi tạo)
    client = http_client
    local_client = False # Flag để biết client này có phải tạo cục bộ không
    if client is None:
        logger.warning("http_client is None, creating a temporary client for this request.")
        client = httpx.AsyncClient(verify=False, timeout=timeout, headers={'User-Agent': 'TG Bot API Caller/1.1'})
        local_client = True

    try:
        logger.debug(f"Making API request: Method={method}, URL={url}, Params={params}")
        if method.upper() == "GET":
            response = await client.get(url, params=params, timeout=timeout)
        elif method.upper() == "POST":
            response = await client.post(url, data=params, timeout=timeout) # POST thường dùng data
        elif method.upper() == "HEAD":
            response = await client.head(url, params=params, timeout=timeout)
        else:
            result["error"] = f"Unsupported HTTP method: {method}"
            return result # Trả về lỗi nếu method không hỗ trợ

        result["status_code"] = response.status_code
        content_type = response.headers.get("content-type", "").lower()

        # Xử lý HEAD request riêng vì thường không có body
        if method.upper() == "HEAD":
            if 200 <= response.status_code < 300:
                result["success"] = True
                result["error"] = None
                result["data"] = {"headers": dict(response.headers)} # Trả về headers
                logger.debug(f"API HEAD Request OK: URL={response.url}, Headers={result['data']['headers']}")
            else:
                result["success"] = False
                result["error"] = f"HTTP Error {response.status_code}"
                logger.warning(f"API HEAD Request Failed: URL={url}, Status={response.status_code}")
            return result # HEAD request kết thúc ở đây

        # Xử lý GET/POST (có body)
        response_bytes = await response.aread() # Đọc bytes
        logger.debug(f"API Response Status: {response.status_code}, Content-Type: {content_type}, URL: {response.url}")

        if 200 <= response.status_code < 300:
            result["success"] = True
            # Thử parse JSON nếu content type là JSON
            if "application/json" in content_type:
                try:
                    result["data"] = json.loads(response_bytes.decode('utf-8', errors='replace'))
                    result["error"] = None # Không có lỗi nếu thành công
                    logger.debug(f"API Response JSON Data: {str(result['data'])[:500]}...") # Log data JSON (rút gọn)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error for URL {url}: {e}. Response text: {response_bytes.decode('utf-8', errors='replace')[:500]}...")
                    result["success"] = False
                    result["data"] = response_bytes # Trả về raw bytes nếu không parse được JSON
                    result["error"] = "API response is not valid JSON despite Content-Type."
            else:
                # Nếu không phải JSON, trả về raw bytes
                result["data"] = response_bytes
                result["error"] = None # Không coi là lỗi nếu status 2xx
                logger.debug(f"API Response is not JSON (Content-Type: {content_type}). Returning raw bytes.")
        else: # Lỗi HTTP (4xx, 5xx)
            result["success"] = False
            error_text = response_bytes.decode('utf-8', errors='replace')[:1000] # Lấy text lỗi (rút gọn)
            result["error"] = f"HTTP Error {response.status_code}: {error_text}"
            logger.warning(f"API Request Failed: URL={url}, Status={response.status_code}, Error Text: {error_text}")

    except httpx.TimeoutException as e:
        logger.warning(f"API Request Timeout: URL={url}, Timeout={timeout}s, Error: {e}")
        result["error"] = f"Request timed out after {timeout} seconds."
        result["success"] = False
    except httpx.RequestError as e: # Lỗi mạng hoặc kết nối
        logger.error(f"API Request Network Error: URL={url}, Error: {e}", exc_info=False)
        result["error"] = f"Network error: {e}"
        result["success"] = False
    except Exception as e: # Lỗi không mong muốn khác
        logger.error(f"Unexpected error during API request: URL={url}, Error: {e}", exc_info=True)
        result["error"] = f"Unexpected error: {e}"
        result["success"] = False
    finally:
        # Đóng client cục bộ nếu đã tạo
        if local_client and client:
            await client.aclose()
            logger.debug("Closed temporary http client.")

    return result


# --- Hàm dừng task treo ---
async def stop_treo_task(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown") -> bool:
    global persistent_treo_configs, active_treo_tasks
    was_active_runtime = False
    removed_persistent = False
    user_id_str = str(user_id_str); target_username = str(target_username)
    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
        task = active_treo_tasks[user_id_str].get(target_username)
        task_name = task.get_name() if task else f"task_{user_id_str}_{target_username}"
        if task and isinstance(task, asyncio.Task) and not task.done():
            was_active_runtime = True
            logger.info(f"[Treo Task Stop] Cancelling RUNTIME task '{task_name}'. Reason: {reason}")
            task.cancel()
            try: await asyncio.wait_for(task, timeout=1.0)
            except asyncio.CancelledError: logger.info(f"[Treo Task Stop] Runtime Task '{task_name}' cancelled.")
            except asyncio.TimeoutError: logger.warning(f"[Treo Task Stop] Timeout waiting for cancelled runtime task '{task_name}'.")
            except Exception as e: logger.error(f"[Treo Task Stop] Error awaiting cancelled runtime task '{task_name}': {e}")
        del active_treo_tasks[user_id_str][target_username]
        if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
        logger.info(f"[Treo Task Stop] Removed runtime task entry for {user_id_str} -> @{target_username}.")
    else: logger.debug(f"[Treo Task Stop] No active runtime task found for {user_id_str} -> @{target_username}.")
    if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
        del persistent_treo_configs[user_id_str][target_username]
        if not persistent_treo_configs[user_id_str]: del persistent_treo_configs[user_id_str]
        logger.info(f"[Treo Task Stop] Removed persistent entry for {user_id_str} -> @{target_username}.")
        save_data()
        removed_persistent = True
    else: logger.debug(f"[Treo Task Stop] No persistent entry found for {user_id_str} -> @{target_username}.")
    return was_active_runtime or removed_persistent

# --- Hàm dừng TẤT CẢ task treo cho user ---
async def stop_all_treo_tasks_for_user(user_id_str: str, context: ContextTypes.DEFAULT_TYPE, reason: str = "Unknown") -> int:
    stopped_count = 0
    user_id_str = str(user_id_str)
    targets_in_persistent = list(persistent_treo_configs.get(user_id_str, {}).keys())
    targets_in_runtime = list(active_treo_tasks.get(user_id_str, {}).keys())
    targets_to_process = set(targets_in_persistent) | set(targets_in_runtime)
    if not targets_to_process: logger.info(f"No persistent/runtime tasks found for user {user_id_str} to stop."); return 0
    logger.info(f"Stopping all {len(targets_to_process)} potential treo tasks/configs for user {user_id_str}. Reason: {reason}")
    targets_in_runtime_only = set(targets_in_runtime) - set(targets_in_persistent)
    if targets_in_runtime_only: logger.warning(f"Found {len(targets_in_runtime_only)} runtime tasks without persistent config for user {user_id_str}: {targets_in_runtime_only}.")
    for target_username in list(targets_to_process):
        if await stop_treo_task(user_id_str, target_username, context, reason): stopped_count += 1
        else: logger.warning(f"stop_treo_task reported failure for {user_id_str} -> @{target_username} during stop_all.")
    logger.info(f"Finished stopping tasks/configs for user {user_id_str}. Stopped/Removed: {stopped_count}/{len(targets_to_process)} target(s).")
    return stopped_count


# --- Job Cleanup ---
async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    global valid_keys, activated_users, vip_users, user_daily_gains, persistent_treo_configs
    current_time = time.time()
    keys_to_remove = []; users_to_deactivate_key = []; users_to_deactivate_vip = []
    vip_users_to_stop_tasks = []
    basic_data_changed = False; gains_cleaned = False
    logger.info("[Cleanup] Starting cleanup job...")
    for key, data in list(valid_keys.items()):
        try:
            if data.get("used_by") is None and current_time > float(data.get("expiry_time", 0)): keys_to_remove.append(key)
        except (ValueError, TypeError): keys_to_remove.append(key)
    for user_id_str, expiry_timestamp in list(activated_users.items()):
        try:
            if current_time > float(expiry_timestamp): users_to_deactivate_key.append(user_id_str)
        except (ValueError, TypeError): users_to_deactivate_key.append(user_id_str)
    for user_id_str, vip_data in list(vip_users.items()):
        try:
            if current_time > float(vip_data.get("expiry", 0)):
                users_to_deactivate_vip.append(user_id_str)
                vip_users_to_stop_tasks.append(user_id_str)
        except (ValueError, TypeError):
            users_to_deactivate_vip.append(user_id_str)
            vip_users_to_stop_tasks.append(user_id_str)
    expiry_threshold = current_time - USER_GAIN_HISTORY_SECONDS
    users_to_remove_from_gains = []
    for user_id_str, targets_data in user_daily_gains.items():
        targets_to_remove_from_user = []
        for target_username, gain_list in targets_data.items():
            valid_gains = [(ts, g) for ts, g in gain_list if isinstance(ts, float) and ts >= expiry_threshold]
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
    if vip_users_to_stop_tasks:
         unique_users_to_stop = set(vip_users_to_stop_tasks)
         logger.info(f"[Cleanup] Scheduling stop for tasks of {len(unique_users_to_stop)} expired/invalid VIP users.")
         app = context.application
         for user_id_str_stop in unique_users_to_stop:
             app.create_task(stop_all_treo_tasks_for_user(user_id_str_stop, context, reason="VIP Expired/Removed during Cleanup"), name=f"cleanup_stop_tasks_{user_id_str_stop}")
    if basic_data_changed or gains_cleaned:
        logger.info(f"[Cleanup] Saving data due to changes (Basic Data Changed: {basic_data_changed}, Gains Cleaned: {gains_cleaned}).")
        save_data()
    else: logger.info("[Cleanup] No basic data changes or gain cleanup needed this cycle.")
    logger.info("[Cleanup] Cleanup job finished.")


# --- Kiểm tra VIP/Key ---
def is_user_vip(user_id: int) -> bool:
    user_id_str = str(user_id); vip_data = vip_users.get(user_id_str)
    if vip_data and isinstance(vip_data, dict):
        try: return time.time() < float(vip_data.get("expiry", 0))
        except (ValueError, TypeError): return False
    return False
def get_vip_limit(user_id: int) -> int:
    user_id_str = str(user_id)
    if is_user_vip(user_id):
        vip_data = vip_users.get(user_id_str, {}); limit = vip_data.get("limit", 0)
        try: return int(limit)
        except (ValueError, TypeError): return 0
    return 0
def is_user_activated_by_key(user_id: int) -> bool:
    user_id_str = str(user_id); expiry_time_ts = activated_users.get(user_id_str)
    if expiry_time_ts:
        try: return time.time() < float(expiry_time_ts)
        except (ValueError, TypeError): return False
    return False
def can_use_feature(user_id: int) -> bool: return is_user_vip(user_id) or is_user_activated_by_key(user_id)

# --- Logic API Follow (Dùng make_api_request) ---
async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """Gọi API follow sử dụng hàm make_api_request và trả về kết quả tương thích."""
    api_params = {"user": target_username, "userid": user_id_str, "tokenbot": bot_token}
    log_api_params = api_params.copy()
    log_api_params["tokenbot"] = f"...{bot_token[-6:]}" if len(bot_token) > 6 else "***"
    logger.info(f"[API Call /fl] User {user_id_str} -> @{target_username} with params: {log_api_params}")

    api_result = await make_api_request(FOLLOW_API_URL_BASE, params=api_params, method="GET")

    # Chuẩn hóa output về dạng {success: bool, message: str, data: dict | None}
    result = {"success": False, "message": "Lỗi không xác định.", "data": None}

    if api_result["success"]:
        # Kiểm tra xem data có phải dict không
        if isinstance(api_result["data"], dict):
            result["data"] = api_result["data"]
            api_status = result["data"].get("status")
            api_success_flag = result["data"].get("success")
            api_message = result["data"].get("message")

            # Kiểm tra success/status từ JSON data
            is_json_success = False
            if isinstance(api_success_flag, bool): is_json_success = api_success_flag
            elif isinstance(api_status, bool): is_json_success = api_status
            elif isinstance(api_status, str): is_json_success = api_status.lower() in ['true', 'success', 'ok', '200']
            elif isinstance(api_status, int): is_json_success = api_status == 200

            if is_json_success:
                result["success"] = True
                result["message"] = str(api_message) if api_message is not None else "Follow thành công (không có thông báo)."
            else:
                # Thành công HTTP nhưng JSON báo lỗi
                result["success"] = False
                result["message"] = str(api_message) if api_message is not None else f"Follow thất bại (JSON status={api_status}, success={api_success_flag})."
                logger.warning(f"[API Call /fl @{target_username}] Request OK but JSON indicates failure. Msg: {result['message']}")
        else:
            # HTTP thành công nhưng data không phải dict (có thể là text hoặc bytes khác)
            logger.warning(f"[API Call /fl @{target_username}] Request OK but response data is not a dictionary. Assuming success based on HTTP status.")
            result["success"] = True
            result["message"] = "Follow thành công (phản hồi API không chuẩn JSON)."
            # Không có data để trả về trong trường hợp này
            result["data"] = None
    else:
        # Lỗi HTTP hoặc lỗi request khác
        result["success"] = False
        result["message"] = api_result["error"] # Lấy lỗi từ make_api_request
        result["data"] = api_result["data"] # Có thể chứa raw data lỗi nếu có

    log_message = result["message"][:200] + ('...' if len(result["message"]) > 200 else '')
    logger.info(f"[API Call /fl @{target_username}] Final Result: Success={result['success']}, Message='{log_message}'")
    return result

# --- Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    if not user: return
    logger.info(f"User {user.id} ({user.username or 'NoUsername'}) used /start or /menu in chat {chat_id}")
    act_h = ACTIVATION_DURATION_SECONDS // 3600
    treo_interval_m = TREO_INTERVAL_SECONDS // 60
    welcome_text = (
        f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
        f"🤖 Chào mừng bạn đến với <b>DinoTool</b> - Bot hỗ trợ TikTok đa năng.\n\n"
        f"✨ <b>Cách sử dụng cơ bản (Miễn phí):</b>\n"
        f"   » Dùng <code>/getkey</code> và <code>/nhapkey &lt;key&gt;</code> để kích hoạt <b>{act_h} giờ</b> sử dụng <code>/tim</code>, <code>/fl</code>.\n\n"
        f"👑 <b>Nâng cấp VIP:</b>\n"
        f"   » Mở khóa <code>/treo</code> (tự động chạy <code>/fl</code> mỗi {treo_interval_m} phút), không cần key.\n"
        f"   » Giới hạn treo nhiều tài khoản hơn.\n"
        f"   » Xem thống kê follow tăng 24h (<code>/xemfl24h</code>).\n\n"
        f"👇 <b>Chọn một tùy chọn bên dưới:</b>"
    )
    keyboard_buttons = [
        [InlineKeyboardButton("👑 Mua VIP / Thông tin TT", callback_data="show_muatt")],
        [InlineKeyboardButton("📜 Lệnh Bot / Trạng thái", callback_data="show_lenh")],
        [InlineKeyboardButton("📊 Check Info TikTok", callback_data="show_check")], # Thêm /check
        [InlineKeyboardButton("🎵 Tải Soundcloud", callback_data="show_sound")], # Thêm /sound
    ]
    if ALLOWED_GROUP_ID and GROUP_LINK and GROUP_LINK != "YOUR_GROUP_INVITE_LINK":
         keyboard_buttons.append([InlineKeyboardButton("💬 Nhóm Chính", url=GROUP_LINK)])
    keyboard_buttons.append([InlineKeyboardButton("👨‍💻 Liên hệ Admin", url=f"tg://user?id={ADMIN_USER_ID}")])
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)
    try:
        # Xóa tin nhắn cũ nếu đây là lệnh /start hoặc /menu (có message_id)
        if update.message and update.message.message_id:
            await delete_user_message(update, context)
        # Gửi tin nhắn menu mới
        await context.bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e: logger.warning(f"Failed to send /start msg to {user.id}: {e}")

async def menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query; await query.answer()
    callback_data = query.data; user = query.from_user
    if not user: return
    logger.info(f"Menu callback '{callback_data}' by user {user.id} in chat {query.message.chat_id}")
    try:
        # Cố gắng xóa tin nhắn menu cũ chứa nút bấm
        await query.delete_message(); logger.debug(f"Deleted old menu message {query.message.message_id}")
    except Exception as e: logger.debug(f"Could not delete old menu message {query.message.message_id}: {e}")

    # Tạo một đối tượng Update và Message giả để gọi handler tương ứng
    # Điều này giúp tái sử dụng code của các command handler
    command_name = callback_data.split('_')[-1]
    fake_message = Message(message_id=query.message.message_id + random.randint(1, 1000), # Tạo ID giả
                           date=datetime.now(query.message.date.tzinfo), # Lấy múi giờ từ tin nhắn gốc
                           chat=query.message.chat, # Giữ nguyên chat
                           from_user=user, # Đặt người dùng là người bấm nút
                           text=f"/{command_name}" # Text giả là lệnh tương ứng
                          )
    fake_update = Update(update_id=update.update_id + random.randint(1, 1000), # Tạo ID giả
                         message=fake_message) # Gắn message giả vào update giả

    try:
        # Gọi hàm xử lý lệnh tương ứng với update giả
        if command_name == "muatt": await muatt_command(fake_update, context)
        elif command_name == "lenh": await lenh_command(fake_update, context)
        elif command_name == "check": await check_command(fake_update, context)
        elif command_name == "sound": await sound_command(fake_update, context)
        else: logger.warning(f"Unhandled menu callback command: {command_name}")
    except Exception as e:
        logger.error(f"Error calling handler for callback '{callback_data}': {e}", exc_info=True)
        # Thông báo lỗi cho người dùng trong trường hợp không xử lý được callback
        try: await context.bot.send_message(user.id, f"⚠️ Lỗi khi xử lý yêu cầu '{command_name}'. Vui lòng thử lại hoặc liên hệ Admin.", parse_mode=ParseMode.HTML)
        except Exception: pass # Bỏ qua nếu gửi thông báo lỗi cũng thất bại


# --- Lệnh /lenh ---
async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /lenh - Hiển thị danh sách lệnh và trạng thái user."""
    if not update or not update.message: return # Cần có message (thật hoặc giả)
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id = user.id; user_id_str = str(user_id)
    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60; gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60
    act_h = ACTIVATION_DURATION_SECONDS // 3600; key_exp_h = KEY_EXPIRY_SECONDS // 3600
    treo_interval_m = TREO_INTERVAL_SECONDS // 60
    is_vip = is_user_vip(user_id); is_key_active = is_user_activated_by_key(user_id)
    can_use_std_features = is_vip or is_key_active

    status_lines = [f"👤 <b>Người dùng:</b> {user.mention_html()} (ID: <code>{user_id}</code>)"]
    expiry_str = "Không rõ" # Default expiry string
    if is_vip:
        vip_data = vip_users.get(user_id_str, {}); expiry_ts = vip_data.get("expiry"); limit = vip_data.get("limit", "?")
        if expiry_ts:
            try: expiry_dt = datetime.fromtimestamp(float(expiry_ts)); expiry_str = expiry_dt.strftime('%d/%m/%Y %H:%M')
            except (ValueError, TypeError, OSError) as e: logger.warning(f"Err fmt VIP expiry {expiry_ts}: {e}"); expiry_str = "Lỗi fmt"
        status_lines.append(f"👑 <b>Trạng thái:</b> VIP ✨ (Hết hạn: {expiry_str}, Treo: {limit} users)")
    elif is_key_active:
        expiry_ts = activated_users.get(user_id_str)
        if expiry_ts:
            try: expiry_dt = datetime.fromtimestamp(float(expiry_ts)); expiry_str = expiry_dt.strftime('%d/%m/%Y %H:%M')
            except (ValueError, TypeError, OSError) as e: logger.warning(f"Err fmt key expiry {expiry_ts}: {e}"); expiry_str = "Lỗi fmt"
        status_lines.append(f"🔑 <b>Trạng thái:</b> Đã kích hoạt (Key) (Hết hạn: {expiry_str})")
    else: status_lines.append("▫️ <b>Trạng thái:</b> Thành viên thường")

    status_lines.append(f"\n⚡️ <b>Quyền dùng /tim, /fl:</b> {'✅ Có' if can_use_std_features else '❌ Không (Cần VIP/Key)'}")
    current_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
    vip_limit = get_vip_limit(user_id)
    if is_vip: status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ✅ Có (Đang treo: {current_treo_count}/{vip_limit} users)")
    else: status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ❌ Không (Chỉ VIP) (Treo: {current_treo_count}/0)")

    cmd_lines = ["\n\n📜=== <b>DANH SÁCH LỆNH</b> ===📜"]
    cmd_lines.extend([
        "\n<b><u>🧭 Điều Hướng & Chung:</u></b>",
        f"  <code>/start</code> | <code>/menu</code> - Mở menu chính",
        f"  <code>/lenh</code> - Xem lại bảng lệnh và trạng thái này",
        f"  <code>/check &lt;username&gt;</code> - Xem thông tin tài khoản TikTok", # <<< Thêm /check
        f"  <code>/sound &lt;link&gt;</code> - Tải nhạc từ link SoundCloud", # <<< Thêm /sound
        "\n<b><u>🔑 Lệnh Miễn Phí (Kích hoạt Key):</u></b>",
        f"  <code>/getkey</code> - Lấy link nhận key (⏳ {gk_cd_m}p/lần, Key hiệu lực {key_exp_h}h)",
        f"  <code>/nhapkey &lt;key&gt;</code> - Kích hoạt tài khoản (Sử dụng trong {act_h}h)",
        "\n<b><u>❤️ Lệnh Tăng Tương Tác (Cần VIP/Key):</u></b>",
        f"  <code>/tim &lt;link_video&gt;</code> - Tăng tim cho video TikTok (⏳ {tf_cd_m}p/lần)",
        f"  <code>/fl &lt;username&gt;</code> - Tăng follow cho tài khoản TikTok (⏳ {tf_cd_m}p/user)",
        "\n<b><u>👑 Lệnh VIP:</u></b>",
        f"  <code>/muatt</code> - Thông tin và hướng dẫn mua VIP",
        f"  <code>/treo &lt;username&gt;</code> - Tự động chạy <code>/fl</code> mỗi {treo_interval_m} phút (Dùng slot)",
        f"  <code>/dungtreo &lt;username&gt;</code> - Dừng treo cho một tài khoản",
        f"  <code>/dungtreo</code> - Dừng treo <b>TẤT CẢ</b> tài khoản của bạn", # <<< Mô tả rõ dừng tất cả
        f"  <code>/listtreo</code> - Xem danh sách tài khoản đang treo",
        f"  <code>/xemfl24h</code> - Xem số follow đã tăng trong 24 giờ qua (cho các tài khoản đang treo)",
    ])
    if user_id == ADMIN_USER_ID:
        cmd_lines.append("\n<b><u>🛠️ Lệnh Admin:</u></b>")
        valid_vip_packages = ', '.join(map(str, VIP_PRICES.keys()))
        cmd_lines.append(f"  <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code> - Thêm/gia hạn VIP (Gói: {valid_vip_packages})")
        group_info = f"ID {ALLOWED_GROUP_ID}" if ALLOWED_GROUP_ID else 'Chưa cấu hình nhóm!'
        cmd_lines.append(f"  <code>/mess &lt;nội_dung&gt;</code> - Gửi thông báo đến nhóm chính ({group_info})")
    cmd_lines.append("\n<i>Lưu ý: Các lệnh yêu cầu VIP/Key chỉ hoạt động khi bạn có trạng thái tương ứng và còn hiệu lực.</i>")

    help_text = "\n".join(status_lines + cmd_lines)
    try:
        # Xóa tin nhắn lệnh gốc nếu nó tồn tại và bắt đầu bằng '/' (chỉ xóa lệnh gõ, không xóa kq callback)
        if update.message.message_id and update.message.text and update.message.text.startswith('/'):
            await delete_user_message(update, context, update.message.message_id)
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e: logger.warning(f"Failed to send /lenh message to {user.id}: {e}")


# --- Lệnh /tim (Đã sửa lỗi cú pháp) ---
async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; user_id = user.id; user_id_str = str(user_id)
    chat_id = update.effective_chat.id; original_message_id = update.message.message_id
    current_time = time.time()

    # --- Kiểm tra quyền và cooldown ---
    if not can_use_feature(user_id):
        err_msg = f"⚠️ {user.mention_html()}, bạn cần <b>VIP</b> hoặc <b>key</b> để dùng lệnh này. (<code>/muatt</code> | <code>/getkey</code>)"
        await send_temporary_message(update, context, err_msg, duration=30); await delete_user_message(update, context, original_message_id); return
    last_usage = user_tim_cooldown.get(user_id_str)
    if last_usage:
        elapsed = current_time - float(last_usage); rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
        if rem_time > 0:
            cd_msg = f"⏳ {user.mention_html()}, đợi <b>{rem_time:.0f}s</b> nữa để dùng <code>/tim</code>."
            await send_temporary_message(update, context, cd_msg, duration=15); await delete_user_message(update, context, original_message_id); return

    # --- Parse Arguments ---
    args = context.args; video_url = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu link video.\n<b>Cú pháp:</b> <code>/tim &lt;link_video&gt;</code>")
    else: url_input = args[0]; video_url = url_input if re.match(r"https?://(?:www\.|vm\.|vt\.|m\.)?tiktok\.com/", url_input) else None
    if not video_url or err_txt:
        final_err = err_txt if err_txt else f"⚠️ Link <code>{html.escape(url_input)}</code> không hợp lệ."
        await send_temporary_message(update, context, final_err, duration=20); await delete_user_message(update, context, original_message_id); return

    # --- Kiểm tra API Key ---
    if not API_KEY_TIM:
        logger.error(f"/tim fail: Missing API_KEY_TIM"); await delete_user_message(update, context, original_message_id)
        await send_temporary_message(update, context, "❌ Lỗi cấu hình Bot (thiếu API key /tim). Báo Admin.", duration=30); return

    # --- Chuẩn bị gọi API ---
    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY_TIM)
    log_api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key="***")
    logger.info(f"User {user_id} calling /tim API: {log_api_url}"); processing_msg = None; final_response_text = ""

    try:
        # --- Gửi tin nhắn chờ ---
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý tăng tim...</i></b> ❤️"); await delete_user_message(update, context, original_message_id)

        # --- Gọi API ---
        api_response = await make_api_request(api_url, method="GET")

        # --- Xử lý kết quả API (PHẦN ĐÃ SỬA LỖI) ---
        if api_response["success"] and isinstance(api_response["data"], dict):
            data = api_response["data"]
            is_api_success = data.get("status") == "success" or data.get("success") is True
            if is_api_success:
                user_tim_cooldown[user_id_str] = time.time(); save_data()
                d = data.get("data", {})
                a = html.escape(str(d.get("author", "?")))
                v = html.escape(str(d.get("video_url", video_url)))
                db = html.escape(str(d.get('digg_before', '?')))
                di_raw = d.get('digg_increased', '?') # Lấy giá trị thô
                da = html.escape(str(d.get('digg_after', '?')))

                # --- Phần sửa lỗi cú pháp và logic định dạng số ---
                di_display = "?" # Giá trị hiển thị mặc định

                if di_raw != "?": # Kiểm tra nếu giá trị không phải mặc định "?"
                    # <<< SỬA LỖI: Thụt lề khối try...except >>>
                    try:
                        # Cố gắng chuyển đổi thành số nguyên và định dạng với dấu phẩy
                        # Làm sạch các ký tự không phải số (trừ dấu trừ nếu có)
                        cleaned_di_raw = re.sub(r'[^\d\-]', '', str(di_raw))
                        di_int = int(cleaned_di_raw) if cleaned_di_raw else 0
                        if di_int >= 0:
                             di_display = f"+{di_int:,}" # Định dạng số dương/0 có dấu '+' và phẩy
                        else:
                             di_display = f"{di_int:,}" # Định dạng số âm có dấu phẩy
                    except (ValueError, TypeError):
                        # Nếu không chuyển đổi được, hiển thị giá trị gốc (đã escape)
                        di_display = html.escape(str(di_raw))
                        logger.warning(f"[/tim] Không thể phân tích digg_increased '{di_raw}' thành số nguyên.")
                # --- Kết thúc phần sửa lỗi ---

                final_response_text = f"🎉 <b>Tăng Tim OK!</b> ❤️\n👤 User: {user.mention_html()}\n\n📊 <b>Video:</b>\n🎬 <a href='{v}'>Link</a>\n✍️ Author: <code>{a}</code>\n👍 Trước: <code>{db}</code> ➜💖 Tăng: <b><code>{di_display}</code></b>➜✅ Sau: <code>{da}</code>"
            else: # API thành công nhưng báo lỗi trong JSON
                api_msg = data.get('message', 'API báo lỗi không rõ')
                final_response_text = f"💔 <b>Tăng Tim Fail!</b>\n👤 {user.mention_html()}\nℹ️ Reason: <code>{html.escape(api_msg)}</code>"
        else: # Lỗi HTTP hoặc không phải JSON
            final_response_text = f"❌ <b>Lỗi API /tim</b>\n👤 {user.mention_html()}\nℹ️ {html.escape(api_response['error'] or 'Lỗi không xác định từ API.')}"

    # --- Xử lý Exception chung ---
    except Exception as e:
        logger.error(f"Unexpected /tim error U:{user_id}: {e}", exc_info=True); final_response_text = f"❌ <b>Lỗi Hệ Thống Bot (/tim)</b>\n👤 {user.mention_html()}\nℹ️ Báo Admin."

    # --- Gửi/Sửa tin nhắn kết quả ---
    finally:
        if processing_msg:
            try: await context.bot.edit_message_text(chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e_edit: logger.warning(f"Failed edit /tim msg {processing_msg.message_id}: {e_edit}"); await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML) # Fallback send new
        else: logger.warning(f"/tim U:{user_id} processing msg None"); await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML) # Fallback send new

# --- Hàm chạy nền /fl ---
async def process_fl_request_background(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id_str: str, target_username: str, processing_msg_id: int, invoking_user_mention: str):
    logger.info(f"[BG Task /fl] Start U:{user_id_str} -> @{target_username}")
    api_result = await call_follow_api(user_id_str, target_username, context.bot.token)
    success = api_result["success"]; api_message = api_result["message"]; api_data = api_result.get("data")
    final_response_text = ""; user_info_block = ""; follower_info_block = ""
    if api_data and isinstance(api_data, dict):
        try:
            name=html.escape(str(api_data.get("name","?"))); tt_username_from_api=api_data.get("username"); tt_username = html.escape(str(tt_username_from_api or target_username))
            tt_user_id = html.escape(str(api_data.get("user_id", "?"))); khu_vuc = html.escape(str(api_data.get("khu_vuc", "?"))); create_time = html.escape(str(api_data.get("create_time", "?")))
            user_info_lines = [f"👤 <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)"]
            if tt_user_id!="?": user_info_lines.append(f"🆔 ID: <code>{tt_user_id}</code>")
            if khu_vuc not in ["?", "Không rõ"]: user_info_lines.append(f"🌍 Khu vực: {khu_vuc}")
            user_info_block = "\n".join(user_info_lines) + "\n"
            f_before = api_data.get("followers_before","?"); f_add_raw=api_data.get("followers_add","?"); f_after=api_data.get("followers_after","?")
            f_add_display="?"; f_add_int=0
            if f_add_raw!="?":
                try: f_add_str_cleaned = re.sub(r'[^\d\-]','',str(f_add_raw)); f_add_int = int(f_add_str_cleaned) if f_add_str_cleaned else 0; f_add_display=f"+{f_add_int:,}" if f_add_int >= 0 else f"{f_add_int:,}"
                except (ValueError, TypeError): f_add_display=html.escape(str(f_add_raw))
            if any(x not in ["?", None] for x in [f_before, f_add_raw, f_after]):
                follower_lines = ["📈 <b>Followers:</b>"]
                if f_before not in ["?",None]: follower_lines.append(f"   Trước: <code>{html.escape(str(f_before))}</code>")
                if f_add_display!="?": style = "<b>" if f_add_int > 0 else ""; style_end = "</b>" if f_add_int > 0 else ""; follower_lines.append(f"   Tăng:   {style}<code>{f_add_display}</code>{style_end} ✨")
                if f_after not in ["?",None]: follower_lines.append(f"   Sau:    <code>{html.escape(str(f_after))}</code>")
                if len(follower_lines) > 1: follower_info_block = "\n".join(follower_lines)
        except Exception as e: logger.error(f"[BG /fl] Err parse API data @{target_username}: {e}. Data:{api_data}", exc_info=True); user_info_block = f"👤 <code>@{html.escape(target_username)}</code>\n(Lỗi parse API data)\n"
    if success:
        user_fl_cooldown[str(user_id_str)][target_username] = time.time(); save_data()
        logger.info(f"[BG /fl] Success U:{user_id_str}->@{target_username}. CD updated.")
        final_response_text = f"✅ <b>Follow OK!</b>\n✨ Cho: {invoking_user_mention}\n\n{user_info_block or f'👤 <code>@{html.escape(target_username)}</code>\n'}{follower_info_block or ''}"
    else:
        logger.warning(f"[BG /fl] Fail U:{user_id_str}->@{target_username}. Msg: {api_message}")
        final_response_text = f"❌ <b>Follow Fail!</b>\n👤 {invoking_user_mention}\n🎯 @<code>{html.escape(target_username)}</code>\n\n💬 Reason: <i>{html.escape(api_message or 'Không rõ')}</i>\n\n{user_info_block or ''}"
        if isinstance(api_message, str) and any(kw in api_message.lower() for kw in ["đợi", "wait", "limit", "phút", "giây"]): final_response_text += f"\n\n<i>ℹ️ Thử lại sau hoặc dùng <code>/treo</code> (VIP).</i>"
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=processing_msg_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"[BG /fl] Edited msg {processing_msg_id} for U:{user_id_str}->@{target_username}")
    except (BadRequest, Forbidden) as e: logger.warning(f"[BG /fl] Fail edit msg {processing_msg_id}: {e}. Sending new."); await context.bot.send_message(chat_id, text=final_response_text, parse_mode=ParseMode.HTML)
    except Exception as e: logger.error(f"[BG /fl] Fail edit msg {processing_msg_id}: {e}", exc_info=True)

# --- /fl Command ---
async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; user_id = user.id; user_id_str = str(user_id)
    chat_id = update.effective_chat.id; original_message_id = update.message.message_id
    invoking_user_mention = user.mention_html(); current_time = time.time()
    if not can_use_feature(user_id):
        err_msg = f"⚠️ {invoking_user_mention}, cần <b>VIP/key</b> để dùng lệnh này. (<code>/muatt</code> | <code>/getkey</code>)"
        await send_temporary_message(update, context, err_msg, duration=30); await delete_user_message(update, context, original_message_id); return
    args = context.args; target_username = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu username.\n<b>Cú pháp:</b> <code>/fl username</code>")
    else: uname_raw = args[0].strip(); uname = uname_raw.lstrip("@")
    if not uname or err_txt: final_err = err_txt if err_txt else "⚠️ Username không được trống."
    else: target_username = uname
    if not target_username: await send_temporary_message(update, context, final_err, duration=20); await delete_user_message(update, context, original_message_id); return
    user_cds = user_fl_cooldown.get(user_id_str, {}); last_usage = user_cds.get(target_username)
    if last_usage:
        elapsed = current_time - float(last_usage); rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
        if rem_time > 0:
            cd_msg = f"⏳ {invoking_user_mention}, đợi <b>{rem_time:.0f}s</b> để <code>/fl @{html.escape(target_username)}</code>."
            await send_temporary_message(update, context, cd_msg, duration=15); await delete_user_message(update, context, original_message_id); return
    processing_msg = None
    try:
        if not target_username: raise ValueError("Target username None before processing")
        processing_msg = await update.message.reply_html(f"⏳ {invoking_user_mention}, nhận yêu cầu <code>/fl @{html.escape(target_username)}</code>. Đang xử lý...")
        await delete_user_message(update, context, original_message_id)
        logger.info(f"Scheduling BG task /fl U:{user_id} -> @{target_username}")
        context.application.create_task(process_fl_request_background(context=context, chat_id=chat_id, user_id_str=user_id_str, target_username=target_username, processing_msg_id=processing_msg.message_id, invoking_user_mention=invoking_user_mention), name=f"fl_bg_{user_id_str}_{target_username}")
    except Exception as e: logger.error(f"Fail start /fl @{html.escape(target_username or '???')}: {e}", exc_info=True); await delete_user_message(update, context, original_message_id); await send_temporary_message(update, context, f"❌ Lỗi bắt đầu /fl @{html.escape(target_username or '???')}.", duration=20)

# --- Lệnh /getkey ---
async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; user_id = user.id; user_id_str = str(user_id)
    chat_id = update.effective_chat.id; original_message_id = update.message.message_id
    current_time = time.time()
    if not LINK_SHORTENER_API_KEY:
        logger.error("/getkey fail: Missing LINK_SHORTENER_API_KEY"); await delete_user_message(update, context, original_message_id); await send_temporary_message(update, context, "❌ Lệnh <code>/getkey</code> lỗi cấu hình. Báo Admin.", duration=30); return
    last_usage = user_getkey_cooldown.get(user_id_str)
    if last_usage:
        elapsed = current_time - float(last_usage); remaining = GETKEY_COOLDOWN_SECONDS - elapsed
        if remaining > 0:
            cd_msg = f"⏳ {user.mention_html()}, đợi <b>{remaining:.0f}s</b> để lấy key mới."
            await send_temporary_message(update, context, cd_msg, duration=15); await delete_user_message(update, context, original_message_id); return
    generated_key = generate_random_key()
    while generated_key in valid_keys: logger.warning(f"Gen key exists: {generated_key}"); generated_key = generate_random_key()
    cache_buster = f"&ts={int(time.time())}{random.randint(100,999)}"
    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key) + cache_buster
    shortener_params = {"token": LINK_SHORTENER_API_KEY, "format": "json", "url": target_url_with_key}
    log_shortener_params = shortener_params.copy(); log_shortener_params["token"] = f"...{LINK_SHORTENER_API_KEY[-6:]}" if len(LINK_SHORTENER_API_KEY)>6 else "***"
    logger.info(f"U:{user_id} req key. Gen:{generated_key}. Target:{target_url_with_key}")
    processing_msg=None; final_response_text=""; key_stored=False
    try:
        processing_msg = await update.message.reply_html("<b><i>⏳ Đang tạo link lấy key...</i></b> 🔑"); await delete_user_message(update, context, original_message_id)
        gen_time = time.time(); expiry_time = gen_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = {"user_id_generator":user_id,"generation_time":gen_time,"expiry_time":expiry_time,"used_by":None,"activation_time":None}
        save_data(); key_stored=True; logger.info(f"Key {generated_key} stored U:{user_id}. Expires: {datetime.fromtimestamp(expiry_time).isoformat()}.")
        logger.debug(f"Call shortener API:{LINK_SHORTENER_API_BASE_URL} Params:{log_shortener_params}")
        api_response = await make_api_request(LINK_SHORTENER_API_BASE_URL, params=shortener_params, method="GET", timeout=30.0)
        if api_response["success"] and isinstance(api_response["data"], dict):
            response_data = api_response["data"]; status = response_data.get("status"); short_url = response_data.get("shortenedUrl")
            if status == "success" and short_url and short_url.startswith("http"):
                user_getkey_cooldown[user_id_str] = time.time(); save_data()
                logger.info(f"Short link OK U:{user_id}: {short_url}. Key:{generated_key}.")
                key_exp_h = KEY_EXPIRY_SECONDS // 3600
                final_response_text = (f"🚀 <b>Link Lấy Key ({user.mention_html()}):</b>\n\n🔗 <a href='{html.escape(short_url)}'>{html.escape(short_url)}</a>\n\n"
                                       f"📝 <b>HD:</b> Click link ➜ Lấy Key ➜ Copy Key ➜ Gửi lệnh:\n<code>/nhapkey &lt;key_vừa_copy&gt;</code>\n\n"
                                       f"⏳ <i>Key có hiệu lực nhập trong <b>{key_exp_h} giờ</b>. Nhập sớm!</i>")
            else: api_msg = response_data.get("message", "Lỗi không rõ từ API rút gọn."); logger.error(f"Shortener API err U:{user_id}. Status:{status}, Msg:{api_msg}"); final_response_text = f"❌ <b>Lỗi tạo link:</b>\n<code>{html.escape(api_msg)}</code>\nThử lại sau hoặc báo Admin."
        else: final_response_text = f"❌ <b>Lỗi API tạo link:</b> {html.escape(api_response['error'] or 'Lỗi không xác định.')}"
    except Exception as e: logger.error(f"Unexpected /getkey U:{user_id}: {e}", exc_info=True); final_response_text = "❌ <b>Lỗi hệ thống Bot (/getkey)</b>. Báo Admin."
    if key_stored and generated_key in valid_keys and valid_keys[generated_key]["used_by"] is None and "Lỗi" in final_response_text: # Rollback if error after storing key
        try: del valid_keys[generated_key]; save_data(); logger.info(f"Removed unused key {generated_key} due to error in /getkey.")
        except Exception as e_rem: logger.error(f"Failed remove unused key {generated_key}: {e_rem}")
    finally:
        if processing_msg:
            try: await context.bot.edit_message_text(chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e_edit: logger.warning(f"Failed edit /getkey msg {processing_msg.message_id}: {e_edit}"); await context.bot.send_message(chat_id, text=final_response_text, parse_mode=ParseMode.HTML)
        else: logger.warning(f"/getkey U:{user_id} processing msg None"); await context.bot.send_message(chat_id, text=final_response_text, parse_mode=ParseMode.HTML)

# --- Lệnh /nhapkey (Đã sửa lỗi cú pháp) ---
async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; user_id = user.id; user_id_str = str(user_id)
    chat_id = update.effective_chat.id; original_message_id = update.message.message_id
    current_time = time.time(); args = context.args
    submitted_key = None; err_txt = ""; key_prefix = "Dinotool-"
    key_format_regex = re.compile(r"^" + re.escape(key_prefix) + r"[A-Z0-9]{8,}$")
    if not args: err_txt = ("⚠️ Thiếu key.\n<b>Cú pháp:</b> <code>/nhapkey Dinotool-KEYCUABAN</code>")
    elif len(args) > 1:
        key_input_raw = args[0].strip()
        key_input_maybe = key_input_raw if key_format_regex.match(key_input_raw) else None
        if key_input_maybe: submitted_key = key_input_maybe; logger.warning(f"U:{user_id} used multiple args for /nhapkey, using '{submitted_key}'"); err_txt = f"⚠️ Đã lấy key <code>{html.escape(submitted_key)}</code>. Lần sau chỉ nhập key."
        else: err_txt = f"⚠️ Nhiều từ & từ đầu tiên sai format.\nVD: <code>/nhapkey {generate_random_key()}</code>"
    else:
        key_input = args[0].strip()
        if not key_format_regex.match(key_input): err_txt = (f"⚠️ Key <code>{html.escape(key_input)}</code> sai định dạng. Phải là <code>Dinotool-</code> + 8+ ký tự HOA/số.")
        else: submitted_key = key_input
    if err_txt and not submitted_key: await send_temporary_message(update, context, err_txt, duration=20); await delete_user_message(update, context, original_message_id); return
    elif err_txt and submitted_key: await send_temporary_message(update, context, err_txt, duration=15, reply=True)
    if not submitted_key: logger.error(f"/nhapkey U:{user_id}: submitted_key None"); await send_temporary_message(update, context, "❌ Lỗi xử lý key.", duration=15); await delete_user_message(update, context, original_message_id); return
    logger.info(f"User {user_id} attempt key activate: '{submitted_key}'")
    key_data = valid_keys.get(submitted_key); final_response_text = ""; should_delete_cmd = True
    if not key_data: final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> không tồn tại.\nKiểm tra lại hoặc dùng <code>/getkey</code>."
    elif key_data.get("used_by") is not None:
        used_by_id = key_data["used_by"]; activation_time_ts = key_data.get("activation_time"); used_time_str = ""
        # <<< SỬA LỖI CÚ PHÁP TẠI ĐÂY >>>
        if activation_time_ts:
            # 'try' phải ở dòng mới và thụt vào
            try:
                used_dt = datetime.fromtimestamp(float(activation_time_ts))
                used_time_str = f" lúc {used_dt.strftime('%H:%M %d/%m/%Y')}"
            except Exception as e:
                logger.warning(f"Err fmt act time {activation_time_ts} key {submitted_key}: {e}")
        if int(used_by_id) == user_id: final_response_text = f"⚠️ Bạn đã kích hoạt key này rồi{used_time_str}."
        else: final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã bị người khác dùng{used_time_str}."
    elif current_time > float(key_data.get("expiry_time", 0)):
        expiry_time_ts = key_data.get("expiry_time"); expiry_time_str = ""
        # <<< SỬA LỖI CÚ PHÁP TẠI ĐÂY >>>
        if expiry_time_ts:
             # 'try' phải ở dòng mới và thụt vào
            try:
                expiry_dt = datetime.fromtimestamp(float(expiry_time_ts))
                expiry_time_str = f" vào lúc {expiry_dt.strftime('%H:%M %d/%m/%Y')}"
            except Exception as e:
                logger.warning(f"Err fmt key expiry {expiry_time_ts} key {submitted_key}: {e}")
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn nhập{expiry_time_str}. Dùng <code>/getkey</code> lấy key mới."
    else:
        try:
            key_data["used_by"] = user_id; key_data["activation_time"] = current_time
            activation_expiry_ts = current_time + ACTIVATION_DURATION_SECONDS
            activated_users[user_id_str] = activation_expiry_ts; save_data()
            expiry_dt = datetime.fromtimestamp(activation_expiry_ts); expiry_str = expiry_dt.strftime('%H:%M %d/%m/%Y')
            act_hours = ACTIVATION_DURATION_SECONDS // 3600
            logger.info(f"Key '{submitted_key}' OK activate U:{user_id}. Expires:{expiry_str}.")
            final_response_text = (f"✅ <b>Kích Hoạt Key OK!</b>\n\n👤 User: {user.mention_html()}\n🔑 Key: <code>{html.escape(submitted_key)}</code>\n\n"
                                   f"✨ Đã có quyền dùng <code>/tim</code>, <code>/fl</code>.\n⏳ Hết hạn: <b>{expiry_str}</b> (sau {act_hours} giờ).\n\n"
                                   f"<i>Chúc bạn vui vẻ!</i>")
            should_delete_cmd = False # Giữ lại lệnh khi thành công
        except Exception as e: logger.error(f"Unexpected key activate U:{user_id} key:{submitted_key}: {e}", exc_info=True); final_response_text = f"❌ Lỗi hệ thống khi kích hoạt key. Báo Admin."; await _rollback_nhapkey(submitted_key, user_id_str, user_id) # Cố gắng rollback
    if should_delete_cmd: await delete_user_message(update, context, original_message_id)
    try: reply_mode = not should_delete_cmd; await update.message.reply_html(final_response_text, disable_web_page_preview=True, quote=reply_mode)
    except Exception as e: logger.error(f"Fail send /nhapkey final U:{user_id}: {e}"); await context.bot.send_message(chat_id, final_response_text, parse_mode=ParseMode.HTML) # Fallback

# --- Hàm helper rollback ---
async def _rollback_nhapkey(key: str, user_id_str: str, user_id_int: int):
    """Hàm helper để rollback thao tác nhập key nếu lỗi."""
    try:
        logger.info(f"Attempting rollback for key '{key}' user {user_id_int}.")
        if key in valid_keys and valid_keys[key].get("used_by") == user_id_int:
            valid_keys[key]["used_by"] = None
            valid_keys[key]["activation_time"] = None
            logger.info(f"Rolled back key data for '{key}'.")
        if user_id_str in activated_users:
            del activated_users[user_id_str]
            logger.info(f"Removed user {user_id_str} from activated list.")
        save_data()
        logger.info(f"Rollback save complete for key '{key}'.")
    except Exception as e_rb: logger.error(f"Error during key activation rollback key:{key} user:{user_id_int}: {e_rb}", exc_info=True)

# --- Lệnh /muatt ---
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    # Nếu là lệnh /muatt gõ tay, lấy ID để xóa, nếu từ callback thì ko có
    original_message_id = update.message.message_id if update.message.text and update.message.text.startswith('/') else None
    user_id = user.id; payment_note = f"{PAYMENT_NOTE_PREFIX} {user_id}"
    text_lines = ["👑 <b>Thông Tin Nâng Cấp VIP - DinoTool</b> 👑", "\nNâng VIP để mở khóa <code>/treo</code>, <code>/xemfl24h</code>, không cần key!", "\n💎 <b>Các Gói VIP:</b>"]
    if VIP_PRICES:
        for days_key, info in VIP_PRICES.items(): days=info.get("duration_days","?"); price=info.get("price","?"); limit=info.get("limit","?"); text_lines.extend([f"\n⭐️ <b>Gói {days} Ngày:</b>", f"   - 💰 Giá: <b>{price}</b>", f"   - ⏳ Hạn: {days} ngày", f"   - 🚀 Treo: <b>{limit} TK</b>"])
    else: text_lines.append("\n<i>Liên hệ Admin để biết chi tiết gói.</i>")
    text_lines.extend(["\n🏦 <b>Thông tin thanh toán:</b>", f"   - NH: <b>{BANK_NAME}</b>", f"   - STK: <a href=\"https://t.me/share/url?url={html.escape(BANK_ACCOUNT)}\"><code>{html.escape(BANK_ACCOUNT)}</code></a>", f"   - Tên: <b>{ACCOUNT_NAME}</b>", "\n📝 <b>Nội dung CK (Quan trọng!):</b>", f"   » <a href=\"https://t.me/share/url?url={html.escape(payment_note)}\"><code>{html.escape(payment_note)}</code></a> (Click copy)", f"   <i>(Sai ND có thể xử lý chậm)</i>", "\n📸 <b>Sau Khi CK Thành Công:</b>", f"   1️⃣ Chụp ảnh màn hình bill.", f"   2️⃣ Nhấn nút '<b>📸 Gửi Bill</b>' bên dưới.", f"   3️⃣ Bot sẽ yêu cầu gửi ảnh <b><u>VÀO CHAT NÀY</u></b>.", f"   4️⃣ Gửi ảnh bill vào đây.", f"   5️⃣ Bot tự chuyển tiếp bill đến Admin ({BILL_FORWARD_TARGET_ID}).", f"   6️⃣ Admin kiểm tra & kích hoạt VIP.", "\n<i>Cảm ơn bạn đã ủng hộ!</i> ❤️"])
    caption_text = "\n".join(text_lines)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📸 Gửi Bill Thanh Toán", callback_data=f"prompt_send_bill_{user_id}")]])
    if original_message_id: try: await delete_user_message(update, context, original_message_id); logger.debug(f"Deleted /muatt cmd {original_message_id}")
    except Exception as e: logger.debug(f"Could not delete /muatt cmd {original_message_id}: {e}")
    photo_sent = False
    if QR_CODE_URL and QR_CODE_URL.startswith("http"):
        try: await context.bot.send_photo(chat_id=chat_id, photo=QR_CODE_URL, caption=caption_text, parse_mode=ParseMode.HTML, reply_markup=keyboard); logger.info(f"Sent /muatt QR U:{user_id} C:{chat_id}"); photo_sent = True
        except (BadRequest, Forbidden, TelegramError) as e: logger.warning(f"Error send /muatt photo C:{chat_id}: {e}. Fallback.")
        except Exception as e: logger.error(f"Unexpected err send /muatt photo C:{chat_id}: {e}", exc_info=True)
    if not photo_sent:
        try: await context.bot.send_message(chat_id=chat_id, text=caption_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=keyboard); logger.info(f"Sent /muatt text fallback U:{user_id} C:{chat_id}")
        except Exception as e: logger.error(f"Error sending /muatt fallback text C:{chat_id}: {e}", exc_info=True)

# --- Callback gửi Bill & Handler nhận Bill ---
async def prompt_send_bill_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query; user = query.from_user; chat_id = query.message.chat_id
    if not query or not user: logger.warning("prompt_send_bill trigger no query/user"); return
    callback_data = query.data; expected_user_id = None
    try:
        if callback_data.startswith("prompt_send_bill_"): expected_user_id = int(callback_data.split("_")[-1])
        else: raise ValueError("Invalid fmt")
    except (ValueError, IndexError): logger.warning(f"Invalid CB data fmt: {callback_data}"); await query.answer("Lỗi nút.", show_alert=True); return
    if user.id != expected_user_id: await query.answer("Bạn không phải người yêu cầu.", show_alert=True); logger.info(f"U:{user.id} clicked bill prompt for U:{expected_user_id} C:{chat_id}"); return
    await query.answer()
    pending_bill_user_ids.add(user.id)
    if context.job_queue:
        job_name = f"remove_pending_bill_{user.id}"
        context.job_queue.run_once(remove_pending_bill_user_job, PENDING_BILL_TIMEOUT_SECONDS, data={'user_id': user.id}, name=job_name, job_kwargs={"replace_existing": True})
        logger.info(f"U:{user.id} added to pending bill list. Timeout job '{job_name}' sched/upd {PENDING_BILL_TIMEOUT_SECONDS}s.")
    else: logger.warning("JobQueue NA, cannot schedule pending bill timeout.")
    prompt_text = f"📸 {user.mention_html()}, đã sẵn sàng.\nGửi ảnh chụp màn hình biên lai <b><u>VÀO ĐÂY</u></b> ngay."
    try: await query.message.reply_html(text=prompt_text, quote=False)
    except Exception as e: logger.error(f"Err send bill prompt reply U:{user.id} C:{chat_id}: {e}", exc_info=True); await context.bot.send_message(chat_id=chat_id, text=prompt_text, parse_mode=ParseMode.HTML) # Fallback send new

async def remove_pending_bill_user_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data; user_id = job_data.get('user_id')
    job_name = context.job.name or f"remove_pending_bill_{user_id}"
    if not user_id: logger.warning(f"Job '{job_name}' no user_id."); return
    if user_id in pending_bill_user_ids:
        pending_bill_user_ids.remove(user_id)
        logger.info(f"Job '{job_name}': Removed U:{user_id} from pending bill list (timeout).")
        try: await context.bot.send_message(user_id, "⚠️ Đã hết thời gian chờ gửi bill. Nếu đã TT, bấm lại nút '📸 Gửi Bill' và gửi lại.", parse_mode=ParseMode.HTML)
        except Exception as e: logger.warning(f"Failed notify U:{user_id} bill timeout: {e}")
    else: logger.debug(f"Job '{job_name}': U:{user_id} not in pending list. No action.")

# Dùng TypeHandler để bắt cả ảnh và document ảnh
async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message or update.message.text: return
    user = update.effective_user; chat = update.effective_chat; message = update.message
    if not user or not chat or not message: return
    if user.id not in pending_bill_user_ids: return # Quan trọng: chỉ xử lý user đang chờ
    is_photo = bool(message.photo); is_image_document = bool(message.document and message.document.mime_type and message.document.mime_type.startswith('image/'))
    if not is_photo and not is_image_document: return # Bỏ qua nếu không phải ảnh
    logger.info(f"Bill photo/doc received PENDING U:{user.id} ({user.username or ''}) C:{chat.id}. Fwd to {BILL_FORWARD_TARGET_ID}.")
    pending_bill_user_ids.discard(user.id) # Xóa khỏi ds chờ
    if context.job_queue: # Hủy job timeout
         job_name = f"remove_pending_bill_{user.id}"; jobs = context.job_queue.get_jobs_by_name(job_name)
         for job in jobs: job.schedule_removal(); logger.debug(f"Removed job '{job_name}' for U:{user.id} after bill received.")
    fwd_caption_lines = [f"📄 <b>Bill Từ User</b>", f"👤 <b>User:</b> {user.mention_html()} (<code>{user.id}</code>)"]
    if chat.type == 'private': fwd_caption_lines.append(f"💬 <b>Chat gốc:</b> PM")
    elif chat.title: fwd_caption_lines.append(f"👥 <b>Chat gốc:</b> {html.escape(chat.title)} (<code>{chat.id}</code>)")
    else: fwd_caption_lines.append(f"❓ <b>Chat gốc:</b> '{chat.type}' (<code>{chat.id}</code>)")
    if chat.id < -1000000000000: msg_link = f"https://t.me/c/{str(chat.id).replace('-100','')}/{message.message_id}"; fwd_caption_lines.append(f"🔗 <a href='{msg_link}'>Link Tin Gốc</a>")
    orig_caption = message.caption;
    if orig_caption: truncated = orig_caption[:500] + ('...' if len(orig_caption)>500 else ''); fwd_caption_lines.append(f"\n📝 <b>Caption gốc:</b>\n{html.escape(truncated)}")
    fwd_caption_text = "\n".join(fwd_caption_lines)
    try:
        fwd_msg = await context.bot.forward_message(chat_id=BILL_FORWARD_TARGET_ID, from_chat_id=chat.id, message_id=message.message_id)
        await context.bot.send_message(chat_id=BILL_FORWARD_TARGET_ID, text=fwd_caption_text, parse_mode=ParseMode.HTML, reply_to_message_id=fwd_msg.message_id, disable_web_page_preview=True)
        logger.info(f"OK forwarded bill msg {message.message_id} U:{user.id} to {BILL_FORWARD_TARGET_ID}.")
        await message.reply_html("✅ Đã nhận và chuyển tiếp bill đến Admin. Xin cảm ơn!")
    except (Forbidden, BadRequest) as e: logger.error(f"FAIL FWD bill U:{user.id} to target {BILL_FORWARD_TARGET_ID}: Bot blocked/no perm? Error: {e}"); await message.reply_html(f"❌ Lỗi gửi bill đến Admin. Liên hệ trực tiếp Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a>."); _notify_admin_fwd_fail(context, user, chat, e)
    except TelegramError as e: logger.error(f"FAIL FWD bill U:{user.id} to target {BILL_FORWARD_TARGET_ID}: TG Error: {e}"); await message.reply_html(f"❌ Lỗi Telegram khi gửi bill. Liên hệ trực tiếp Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a>."); _notify_admin_fwd_fail(context, user, chat, e)
    except Exception as e: logger.error(f"FAIL FWD bill U:{user.id} to target {BILL_FORWARD_TARGET_ID}: Unexpected: {e}", exc_info=True); await message.reply_html(f"❌ Lỗi hệ thống khi gửi bill. Liên hệ trực tiếp Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a>."); _notify_admin_fwd_fail(context, user, chat, e)
    raise ApplicationHandlerStop # Dừng xử lý, không cho handler khác nhận ảnh này

async def _notify_admin_fwd_fail(context, user, chat, error):
     """Helper để thông báo cho Admin khi forward bill lỗi."""
     if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
        try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ LỖI FWD BILL ⚠️\nU:{user.id} ({user.mention_html()}) Chat:{chat.id}\nTarget:<code>{BILL_FORWARD_TARGET_ID}</code>\nError:{html.escape(str(error))}", parse_mode=ParseMode.HTML)
        except Exception as e_admin: logger.error(f"Fail notify ADMIN:{ADMIN_USER_ID} FWD bill error: {e_admin}")

# --- Lệnh /addtt ---
async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    admin_user = update.effective_user; chat = update.effective_chat
    if not admin_user or admin_user.id != ADMIN_USER_ID: logger.warning(f"Unauth /addtt attempt by {admin_user.id if admin_user else '?'}") ; return
    args = context.args; err_txt = None; target_user_id = None; days_key_input = None; limit = None; duration_days = None
    valid_day_keys = list(VIP_PRICES.keys()); valid_days_str = ', '.join(map(str, valid_day_keys)) or "Chưa cấu hình"
    if len(args) != 2: err_txt = (f"⚠️ Sai cú pháp.\n<b>Dùng:</b> <code>/addtt &lt;user_id&gt; &lt;gói_ngày&gt;</code>\n<b>Gói:</b> {valid_days_str}\n<b>VD:</b> <code>/addtt 123456789 {valid_day_keys[0] if valid_day_keys else '15'}</code>")
    else:
        try: target_user_id = int(args[0])
        except ValueError: err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ."
        if not err_txt:
            try: days_key_input = int(args[1])
            except ValueError: err_txt = f"⚠️ Gói ngày '<code>{html.escape(args[1])}</code>' không phải số."
            if not err_txt:
                 if days_key_input not in VIP_PRICES: err_txt = f"⚠️ Gói ngày <code>{days_key_input}</code> không có. Chỉ chấp nhận: <b>{valid_days_str}</b>."
                 else: vip_info = VIP_PRICES[days_key_input]; limit = vip_info["limit"]; duration_days = vip_info["duration_days"]
    if err_txt: await update.message.reply_html(err_txt); return
    target_user_id_str = str(target_user_id); current_time = time.time()
    current_vip_data = vip_users.get(target_user_id_str); start_time = current_time; op_type = "Nâng cấp lên"
    if current_vip_data:
         try: current_expiry = float(current_vip_data.get("expiry", 0))
         except (ValueError, TypeError): current_expiry = 0
         if current_expiry > current_time: start_time = current_expiry; op_type = "Gia hạn thêm"; logger.info(f"Admin:{admin_user.id} Extending VIP {target_user_id_str} from {datetime.fromtimestamp(start_time).isoformat()}.")
         else: logger.info(f"Admin:{admin_user.id} U:{target_user_id_str} VIP expired. Activating new.")
    new_expiry_ts = start_time + duration_days * 86400; new_expiry_dt = datetime.fromtimestamp(new_expiry_ts)
    new_expiry_str = new_expiry_dt.strftime('%H:%M %d/%m/%Y')
    vip_users[target_user_id_str] = {"expiry": new_expiry_ts, "limit": limit}; save_data()
    logger.info(f"Admin:{admin_user.id} OK VIP {target_user_id_str}: {op_type} {duration_days}d. Exp:{new_expiry_str}, Lmt:{limit}")
    admin_msg = f"✅ Đã <b>{op_type} {duration_days} ngày VIP</b>!\n👤 User: <code>{target_user_id}</code>\n✨ Gói: {duration_days} ngày\n⏳ Hạn mới: <b>{new_expiry_str}</b>\n🚀 Limit: <b>{limit} users</b>"
    await update.message.reply_html(admin_msg)
    user_mention = f"User ID <code>{target_user_id}</code>"
    try: target_user_info = await context.bot.get_chat(target_user_id); user_mention = target_user_info.mention_html() or f"<a href='tg://user?id={target_user_id}'>User {target_user_id}</a>"
    except Exception as e: logger.warning(f"Could not get chat info for {target_user_id}: {e}.")
    user_notify = (f"🎉 Chúc mừng {user_mention}! 🎉\nBạn đã được Admin <b>{op_type} {duration_days} ngày VIP</b>!\n\n✨ Gói VIP: <b>{duration_days} ngày</b>\n⏳ Hạn đến: <b>{new_expiry_str}</b>\n🚀 Limit treo: <b>{limit} tài khoản</b>\n\nCảm ơn bạn! ❤️ (<code>/menu</code> | <code>/lenh</code>)")
    try: await context.bot.send_message(chat_id=target_user_id, text=user_notify, parse_mode=ParseMode.HTML, disable_web_page_preview=True); logger.info(f"Sent VIP notify PM to {target_user_id}.")
    except (Forbidden, BadRequest) as e_pm:
        logger.warning(f"Failed send VIP notify PM to {target_user_id} ({e_pm}). Trying group {ALLOWED_GROUP_ID}.")
        if ALLOWED_GROUP_ID:
            try: await context.bot.send_message(ALLOWED_GROUP_ID, user_notify, parse_mode=ParseMode.HTML); logger.info(f"Sent VIP notify U:{target_user_id} to group {ALLOWED_GROUP_ID} fallback.")
            except Exception as e_group: logger.error(f"Fail send VIP notify U:{target_user_id} to group {ALLOWED_GROUP_ID}: {e_group}"); _notify_admin_addtt_fail(context, admin_user, target_user_id, e_pm, e_group)
        else: logger.warning(f"No group fallback for U:{target_user_id}."); _notify_admin_addtt_fail(context, admin_user, target_user_id, e_pm, None)
    except Exception as e: logger.error(f"Unexpected error sending VIP notify U:{target_user_id}: {e}", exc_info=True); _notify_admin_addtt_fail(context, admin_user, target_user_id, e, None)

async def _notify_admin_addtt_fail(context, admin_user, target_user_id, pm_error, group_error):
     """Helper thông báo admin khi không gửi được tin nhắn VIP cho user."""
     if admin_user.id != target_user_id: # Không tự thông báo cho mình
         try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho U:{target_user_id}\nPM err:{html.escape(str(pm_error))}\nGroup err:{html.escape(str(group_error)) if group_error else 'N/A'}", parse_mode=ParseMode.HTML)
         except Exception: pass

# --- Logic Treo ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Vòng lặp chạy nền cho /treo, gửi info ban đầu, ghi gain, chạy liên tục."""
    global user_daily_gains, treo_stats
    user_id_int = int(user_id_str) # Chuyển sang int để check VIP
    task_name = f"treo_{user_id_str}_{target_username}_in_{chat_id}"
    logger.info(f"[Treo Task Start/Resume] Task '{task_name}' started.")
    invoking_user_mention = f"User ID <code>{user_id_str}</code>"
    try:
        user_info = await context.application.bot.get_chat(user_id_int)
        if user_info and user_info.mention_html(): invoking_user_mention = user_info.mention_html()
    except Exception as e: logger.debug(f"Could not get mention U:{user_id_str} task {task_name}: {e}")
    last_api_call_time = 0.0 # Thời điểm gọi API gần nhất
    consecutive_failures = 0 # Số lỗi API liên tiếp
    MAX_CONSECUTIVE_FAILURES = 15 # Dừng sau 15 lần lỗi liên tiếp
    initial_info_sent = False # Đã gửi thông tin lần đầu chưa?

    try:
        while True:
            current_time = time.time()
            app = context.application # Lấy application để dùng bot, job_queue

            # 1. Kiểm tra Điều kiện Dừng (Quan trọng!)
            #   a. Config persistent còn tồn tại không? (Tránh trường hợp user /dungtreo)
            if persistent_treo_configs.get(user_id_str, {}).get(target_username) != chat_id:
                 logger.warning(f"[Treo Task Stop] Persistent config mismatch/missing for task '{task_name}'. Stopping.")
                 # Dọn dẹp task khỏi runtime nếu nó đang tồn tại
                 if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
                     current_task_in_dict = active_treo_tasks[user_id_str].get(target_username)
                     current_asyncio_task = asyncio.current_task() # Lấy task hiện tại
                     if current_task_in_dict is current_asyncio_task: # Chỉ xóa nếu đúng là task này
                          del active_treo_tasks[user_id_str][target_username]
                          if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                          logger.info(f"[Treo Task Stop] Removed runtime task '{task_name}' due to missing/mismatched persistent config.")
                 break # Thoát vòng lặp while

            #   b. User còn VIP không?
            if not is_user_vip(user_id_int):
                logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping task '{task_name}'.")
                # Dừng task và xóa config persistent
                await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired in loop")
                # Thông báo cho user (tùy chọn)
                try: await app.bot.send_message(chat_id, f"ℹ️ {invoking_user_mention}, việc treo cho <code>@{html.escape(target_username)}</code> đã tự động dừng do VIP của bạn đã hết hạn.", parse_mode=ParseMode.HTML, disable_notification=True )
                except Exception as e_send_stop: logger.warning(f"Failed send VIP expiry stop msg task {task_name}: {e_send_stop}")
                break # Thoát vòng lặp while

            # 2. Tính toán thời gian chờ & Thực hiện sleep
            time_since_last_call = current_time - last_api_call_time
            wait_needed = TREO_INTERVAL_SECONDS - time_since_last_call
            if wait_needed > 0:
                logger.debug(f"[Treo Task Wait] Task '{task_name}' waiting for {wait_needed:.1f}s.")
                await asyncio.sleep(wait_needed)

            # Đánh dấu thời điểm bắt đầu chu kỳ mới (sau khi sleep)
            current_cycle_start_time = time.time()
            last_api_call_time = current_cycle_start_time # Cập nhật thời điểm gọi API

            # 3. Gọi API Follow
            logger.info(f"[Treo Task Run] Task '{task_name}' executing follow @{target_username}")
            api_result = await call_follow_api(user_id_str, target_username, app.bot.token)
            success = api_result["success"]
            api_message = api_result["message"] or "Không có thông báo."
            api_data = api_result.get("data") # Data có thể là dict hoặc None
            gain = 0

            if success:
                consecutive_failures = 0 # Reset bộ đếm lỗi
                try: # Cố gắng parse gain từ data
                    if api_data and isinstance(api_data, dict):
                        gain_str = str(api_data.get("followers_add", "0"))
                        gain_match = re.search(r'[\+\-]?\d+', gain_str) # Tìm số có dấu +/-
                        gain = int(gain_match.group(0)) if gain_match else 0
                    else: gain = 0 # Không có data hoặc không phải dict thì gain=0
                except (ValueError, TypeError, KeyError, AttributeError) as e_gain:
                     logger.warning(f"[Treo Task Stats] Task '{task_name}' error parsing gain: {e_gain}. Data: {api_data}")
                # Chỉ ghi nhận và thống kê nếu gain > 0
                if gain > 0:
                    treo_stats[user_id_str][target_username] += gain
                    # Thêm vào lịch sử gain 24h
                    user_daily_gains[user_id_str][target_username].append((current_cycle_start_time, gain))
                    logger.info(f"[Treo Task Stats] Task '{task_name}' added +{gain} followers. Recorded for job & /xemfl24h.")
                    save_data() # Lưu data ngay khi có gain mới
                elif gain == 0: logger.info(f"[Treo Task Success] Task '{task_name}' OK, gain=0. API Msg: {api_message[:100]}...")
                else: logger.warning(f"[Treo Task Success] Task '{task_name}' OK but gain={gain} (<0). API Msg: {api_message[:100]}...")

                # --- Gửi thông tin chi tiết LẦN ĐẦU thành công ---
                if not initial_info_sent:
                    sent_initial_success = False # Flag nhỏ trong lần đầu
                    try:
                        initial_lines = []
                        f_before = f_after = name = tt_username_api = avatar = khu_vuc = tt_uid = None
                        if api_data and isinstance(api_data, dict): # Parse lại data nếu có
                            f_before = api_data.get("followers_before"); f_after = api_data.get("followers_after")
                            avatar = api_data.get("avatar"); name = api_data.get("name")
                            tt_username_api = api_data.get("username"); khu_vuc = api_data.get("khu_vuc"); tt_uid = api_data.get("user_id")

                        # Ưu tiên username từ API
                        tt_username_display = html.escape(tt_username_api or target_username)
                        name_display = html.escape(str(name)) if name else "???"
                        header = f"🟢 <b>Treo Tự Động OK!</b> ({invoking_user_mention})\n\n"
                        target_line = f"🎯 <a href='https://tiktok.com/@{tt_username_display}'>{name_display}</a> (<code>@{tt_username_display}</code>)"
                        # Thêm UID và Khu vực nếu có
                        extra_info = []
                        if tt_uid and tt_uid != "?": extra_info.append(f"🆔 <code>{html.escape(str(tt_uid))}</code>")
                        if khu_vuc and khu_vuc not in ["?", "Không rõ"]: extra_info.append(f"🌍 {html.escape(str(khu_vuc))}")
                        if extra_info: target_line += f"\n   {' | '.join(extra_info)}"
                        initial_lines.append(target_line)

                        # Hiển thị thông tin follow nếu có và hợp lệ
                        follow_lines = []
                        try: # Cố gắng parse số để format
                            f_before_num = int(re.sub(r'[^\d]','', str(f_before))) if f_before else None
                            f_after_num = int(re.sub(r'[^\d]','', str(f_after))) if f_after else None
                            if f_before_num is not None: follow_lines.append(f"📊 Trước: <code>{f_before_num:,}</code>")
                            if gain > 0: follow_lines.append(f"✨ Đã tăng: <b>+{gain:,}</b>")
                            if f_after_num is not None: follow_lines.append(f"📈 Sau: <code>{f_after_num:,}</code>")
                        except (ValueError, TypeError): # Nếu parse lỗi, hiển thị gốc
                            if f_before not in ["?",None]: follow_lines.append(f"📊 Trước: <code>{html.escape(str(f_before))}</code>")
                            if gain > 0: follow_lines.append(f"✨ Đã tăng: <b>+{gain:,}</b>")
                            if f_after not in ["?",None]: follow_lines.append(f"📈 Sau: <code>{html.escape(str(f_after))}</code>")

                        if follow_lines: initial_lines.extend(["\n" + line for line in follow_lines])
                        else: initial_lines.append("\n<i>(Không có dữ liệu follow chi tiết)</i>") # Fallback

                        initial_lines.append(f"\n⏳ <i>Tự động chạy lại sau {TREO_INTERVAL_SECONDS//60} phút...</i>")

                        initial_text_msg = header + "\n".join(initial_lines)

                        # Ưu tiên gửi avatar nếu có link hợp lệ
                        sent_with_photo = False
                        if avatar and isinstance(avatar, str) and avatar.startswith("http"):
                            try:
                                await app.bot.send_photo(chat_id=chat_id, photo=avatar, caption=initial_text_msg, parse_mode=ParseMode.HTML, disable_notification=True)
                                sent_with_photo = True
                                logger.info(f"[Treo Task Initial Info] Sent task '{task_name}' details with photo.")
                            except Exception as e_send_photo:
                                logger.warning(f"Failed send avatar initial treo {task_name}: {e_send_photo}. Sending text only.")
                        # Gửi text nếu không có avatar hoặc gửi avatar lỗi
                        if not sent_with_photo:
                           await app.bot.send_message(chat_id=chat_id, text=initial_text_msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)
                           logger.info(f"[Treo Task Initial Info] Sent task '{task_name}' details as text.")

                        initial_info_sent = True # Đánh dấu đã gửi thành công lần đầu
                        sent_initial_success = True
                    except Exception as e_send_initial:
                        logger.error(f"Error sending initial treo info for '{task_name}': {e_send_initial}", exc_info=True)
                    # Nếu không gửi được thông báo chi tiết lần đầu, thì vẫn chạy tiếp nhưng lần sau sẽ gửi thông báo ngắn gọn hơn
                    if not sent_initial_success:
                         initial_info_sent = True # Vẫn đánh dấu đã "cố gắng" gửi để lần sau không thử gửi chi tiết nữa

            else: # API thất bại
                consecutive_failures += 1
                logger.warning(f"[Treo Task Fail] Task '{task_name}' fail ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}). API Msg: {api_message[:150]}...")
                # Không ghi nhận gain
                # Kiểm tra nếu lỗi liên tục quá giới hạn
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(f"[Treo Task Stop] Task '{task_name}' stopping due to {consecutive_failures} consecutive failures.")
                    # Dừng task và xóa config
                    await stop_treo_task(user_id_str, target_username, context, reason=f"{consecutive_failures} consecutive API failures")
                    try:
                        await app.bot.send_message(chat_id, f"⚠️ {invoking_user_mention}: Treo cho <code>@{html.escape(target_username)}</code> đã <b>tạm dừng</b> do gặp lỗi API {consecutive_failures} lần liên tiếp. Vui lòng kiểm tra và thử <code>/treo</code> lại sau nếu muốn.", parse_mode=ParseMode.HTML, disable_notification=True)
                    except Exception as e_send_fail_stop: logger.warning(f"Failed send consecutive failure stop msg task {task_name}: {e_send_fail_stop}")
                    break # Thoát vòng lặp while

            # 4. Gửi thông báo trạng thái NGẮN GỌN (Cho các lần sau, hoặc lần đầu nếu lỗi, hoặc nếu có gain > 0)
            should_send_status = (initial_info_sent and (not success or gain != 0)) or (not initial_info_sent and not success)

            if should_send_status:
                 status_lines = []
                 sent_status_message = None
                 try:
                     if success and gain > 0: # Thành công và có gain
                          status_lines.append(f"✅ Treo <code>@{html.escape(target_username)}</code>: <b>+{gain:,}</b> follow ✨ ({invoking_user_mention})")
                     elif success and gain < 0: # Thành công nhưng giảm follow? (hiếm)
                          status_lines.append(f"📉 Treo <code>@{html.escape(target_username)}</code>: <b>{gain:,}</b> follow ({invoking_user_mention})")
                     elif not success: # Lỗi API
                         status_lines.append(f"❌ Treo <code>@{html.escape(target_username)}</code> fail ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}) ({invoking_user_mention})")
                         if len(api_message) < 100: # Chỉ thêm lý do nếu không quá dài
                              status_lines.append(f"   💬 <i>{html.escape(api_message)}</i>")
                     # Trường hợp success and gain == 0 -> không gửi gì cả ở đây

                     # Gửi tin nhắn nếu có nội dung
                     if status_lines:
                         status_msg = "\n".join(status_lines)
                         sent_status_message = await app.bot.send_message(chat_id=chat_id, text=status_msg, parse_mode=ParseMode.HTML, disable_notification=True) # Gửi yên lặng
                         if not success and sent_status_message and app.job_queue: # Lên lịch xóa tin nhắn thất bại
                             job_name_del = f"del_treo_fail_{chat_id}_{sent_status_message.message_id}"
                             app.job_queue.run_once( delete_message_job, TREO_FAILURE_MSG_DELETE_DELAY, data={'chat_id': chat_id, 'message_id': sent_status_message.message_id}, name=job_name_del )
                             logger.debug(f"Scheduled job '{job_name_del}' to delete fail msg {sent_status_message.message_id} in {TREO_FAILURE_MSG_DELETE_DELAY}s.")

                 except Forbidden:
                     logger.error(f"[Treo Task Stop] Bot Forbidden in chat {chat_id}. Cannot send status for '{task_name}'. Stopping task.")
                     await stop_treo_task(user_id_str, target_username, context, reason=f"Bot Forbidden in chat {chat_id}")
                     break # Thoát loop
                 except TelegramError as e_send:
                     logger.error(f"Error sending treo status for '{task_name}' to chat {chat_id}: {e_send}")
                 except Exception as e_unexp_send:
                     logger.error(f"Unexpected error sending treo status for '{task_name}' to chat {chat_id}: {e_unexp_send}", exc_info=True)

    except asyncio.CancelledError:
        logger.info(f"[Treo Task Cancelled] Task '{task_name}' was cancelled externally.")
    except Exception as e:
        logger.error(f"[Treo Task Error] Unexpected error in task '{task_name}': {e}", exc_info=True)
        try:
            await context.application.bot.send_message(
                chat_id,
                f"💥 {invoking_user_mention}: Lỗi nghiêm trọng khi treo <code>@{html.escape(target_username)}</code>. Task đã dừng.\n<b>Lỗi:</b> {html.escape(str(e)[:200])}...",
                parse_mode=ParseMode.HTML,
                disable_notification=True
            )
        except Exception as e_send_fatal:
            logger.error(f"Failed send fatal error message task {task_name}: {e_send_fatal}")
        await stop_treo_task(user_id_str, target_username, context, reason=f"Unexpected Error in loop: {e}")
    finally:
        logger.info(f"[Treo Task End] Task '{task_name}' finished or stopped.")
        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
            current_task_in_dict = active_treo_tasks[user_id_str].get(target_username)
            current_asyncio_task = asyncio.current_task()
            if current_task_in_dict is current_asyncio_task and current_asyncio_task.done():
                del active_treo_tasks[user_id_str][target_username]
                if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                logger.info(f"[Treo Task Cleanup] Removed finished/failed task '{task_name}' from active tasks dict in finally block.")


# --- Lệnh /treo (VIP) ---
async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bắt đầu treo tự động follow cho một user (chỉ VIP). Lưu config persistent."""
    global persistent_treo_configs, active_treo_tasks
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id; user_id_str = str(user_id); chat_id = update.effective_chat.id
    original_message_id = update.message.message_id; invoking_user_mention = user.mention_html()
    if not is_user_vip(user_id):
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/treo</code> chỉ dành cho <b>VIP</b>. (<code>/muatt</code> | <code>/menu</code>)"
        await send_temporary_message(update, context, err_msg, duration=20); await delete_user_message(update, context, original_message_id); return
    args = context.args; target_username = None; err_txt = None
    if not args: err_txt = ("⚠️ Thiếu username cần treo.\n<b>Cú pháp:</b> <code>/treo username</code>")
    else: uname_raw = args[0].strip(); uname = uname_raw.lstrip("@")
    if not uname or err_txt: final_err = err_txt if err_txt else "⚠️ Username không được trống."
    else: target_username = uname
    if not target_username: await send_temporary_message(update, context, final_err, duration=20); await delete_user_message(update, context, original_message_id); return
    # Kiểm tra Giới Hạn và Trạng Thái Treo Hiện Tại
    vip_limit = get_vip_limit(user_id)
    persistent_user_configs = persistent_treo_configs.get(user_id_str, {})
    current_treo_count = len(persistent_user_configs)
    if target_username in persistent_user_configs:
        logger.info(f"U:{user_id} tried /treo @{target_username} already in persistent config.")
        msg = f"⚠️ Đã đang treo cho <code>@{html.escape(target_username)}</code>. Dùng <code>/dungtreo {target_username}</code> để dừng."
        await send_temporary_message(update, context, msg, duration=20); await delete_user_message(update, context, original_message_id); return
    if current_treo_count >= vip_limit:
         logger.warning(f"U:{user_id} /treo @{target_username} reached limit ({current_treo_count}/{vip_limit}).")
         limit_msg = (f"⚠️ Đã đạt giới hạn treo! ({current_treo_count}/{vip_limit} TK).\nDùng <code>/dungtreo</code> để giải phóng slot hoặc nâng VIP.")
         await send_temporary_message(update, context, limit_msg, duration=30); await delete_user_message(update, context, original_message_id); return
    # Bắt đầu Task Treo Mới và Lưu Config
    task = None
    try:
        app = context.application
        # Tạo task chạy nền (sẽ tự gửi thông báo chi tiết khi chạy lần đầu)
        task = app.create_task( run_treo_loop(user_id_str, target_username, context, chat_id), name=f"treo_{user_id_str}_{target_username}_in_{chat_id}" )
        # Thêm task vào dict runtime và persistent config
        active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
        persistent_treo_configs.setdefault(user_id_str, {})[target_username] = chat_id
        save_data() # Lưu config persistent ngay lập tức
        logger.info(f"OK created task '{task.get_name()}' & saved persistent config U:{user_id} -> @{target_username} C:{chat_id}")
        # Thông báo thành công (ngắn gọn, vì task sẽ báo chi tiết sau)
        new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
        treo_interval_m = TREO_INTERVAL_SECONDS // 60
        success_msg = (f"✅ <b>Đã Lên Lịch Treo Thành Công!</b>\n\n👤 Cho: {invoking_user_mention}\n🎯 Target: <code>@{html.escape(target_username)}</code>\n"
                       f"⏳ Tần suất: Mỗi {treo_interval_m} phút\n📊 Slot đã dùng: {new_treo_count}/{vip_limit}\n\n"
                       f"<i>Bot sẽ tự động chạy và gửi thông báo kết quả. Dùng <code>/listtreo</code> để xem ds.</i>")
        await update.message.reply_html(success_msg)
        await delete_user_message(update, context, original_message_id)
    except Exception as e_start_task:
         logger.error(f"Fail start treo task/save config U:{user_id} -> @{target_username}: {e_start_task}", exc_info=True)
         await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi bắt đầu treo <code>@{html.escape(target_username)}</code>. Báo Admin.", duration=20)
         await delete_user_message(update, context, original_message_id)
         # Cố gắng rollback nếu tạo task hoặc lưu config lỗi
         if task and isinstance(task, asyncio.Task) and not task.done(): task.cancel()
         if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str][target_username]; logger.info(f"Rolled back runtime task entry for @{target_username}")
         if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]: del persistent_treo_configs[user_id_str][target_username]; save_data(); logger.info(f"Rolled back persistent config entry for @{target_username}")

# --- Lệnh /dungtreo ---
async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dừng việc treo tự động follow cho một hoặc tất cả user + phản hồi."""
    if not update or not update.message: return
    user = update.effective_user
    if not user: return
    user_id = user.id; user_id_str = str(user_id)
    original_message_id = update.message.message_id; invoking_user_mention = user.mention_html()
    args = context.args

    await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc trước

    if not args: # --- Dừng tất cả ---
        logger.info(f"User {user_id} requesting to stop ALL treo tasks.")
        stopped_count = await stop_all_treo_tasks_for_user(user_id_str, context, reason=f"User cmd /dungtreo all by {user_id}")
        if stopped_count > 0:
             await update.message.reply_html(f"✅ {invoking_user_mention}, đã dừng và xóa cấu hình thành công cho <b>{stopped_count}</b> tài khoản đang treo của bạn.")
        else:
             await send_temporary_message(update, context, f"ℹ️ {invoking_user_mention}, bạn hiện không có tài khoản nào đang treo để dừng.", duration=20, reply=False)
    else: # --- Dừng một target ---
        target_username_raw = args[0].strip()
        target_username_clean = target_username_raw.lstrip("@")
        if not target_username_clean:
            await send_temporary_message(update, context, "⚠️ Username không được để trống khi dùng <code>/dungtreo &lt;username&gt;</code>.", duration=15, reply=False)
            return

        logger.info(f"User {user_id} requesting to stop treo for @{target_username_clean}")
        # Hàm stop_treo_task sẽ dừng runtime và xóa persistent
        stopped = await stop_treo_task(user_id_str, target_username_clean, context, reason=f"User cmd /dungtreo by {user_id}")

        if stopped:
            new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
            vip_limit = get_vip_limit(user_id)
            limit_display = f"{vip_limit}" if is_user_vip(user_id) else "N/A"
            await update.message.reply_html(f"✅ {invoking_user_mention}, đã dừng treo và xóa cấu hình thành công cho <code>@{html.escape(target_username_clean)}</code>.\n(Slot còn lại: {vip_limit - new_treo_count}/{limit_display})")
        else:
            await send_temporary_message(update, context, f"⚠️ {invoking_user_mention}, không tìm thấy tài khoản <code>@{html.escape(target_username_clean)}</code> trong danh sách đang treo của bạn.", duration=20, reply=False)

# --- Lệnh /listtreo ---
async def listtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị danh sách các tài khoản TikTok đang được cấu hình treo."""
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id = user.id; user_id_str = str(user_id)
    original_message_id = update.message.message_id

    logger.info(f"User {user_id} requested /listtreo in chat {chat_id}")

    # Lấy danh sách từ persistent_treo_configs là chính xác nhất
    user_treo_configs = persistent_treo_configs.get(user_id_str, {})
    treo_targets = sorted(list(user_treo_configs.keys())) # Sắp xếp theo ABC

    reply_lines = [f"📊 <b>Danh Sách Tài Khoản Đang Treo</b>", f"👤 Cho: {user.mention_html()}"]
    is_currently_vip = is_user_vip(user_id) # Check VIP status hiện tại
    vip_limit = get_vip_limit(user_id)
    limit_display = f"{vip_limit}" if is_currently_vip else "N/A (VIP?)"

    if not treo_targets:
        reply_lines.append("\nBạn hiện không treo tài khoản nào.")
        if is_user_vip(user_id): reply_lines.append("Dùng <code>/treo &lt;username&gt;</code> để bắt đầu.")
        else: reply_lines.append("Nâng cấp VIP để sử dụng tính năng này (<code>/muatt</code>).")
    else:
        reply_lines.append(f"\n🔍 Số lượng: <b>{len(treo_targets)} / {limit_display}</b> tài khoản")
        # Lặp qua danh sách target từ persistent config
        for target in treo_targets:
             # Ước lượng trạng thái chạy từ runtime dict
             is_running = False
             if user_id_str in active_treo_tasks and target in active_treo_tasks[user_id_str]:
                  task = active_treo_tasks[user_id_str][target]
                  # Task tồn tại và chưa xong -> coi là đang chạy
                  if task and isinstance(task, asyncio.Task) and not task.done():
                      is_running = True
             status_icon = "▶️ Đang chạy" if is_running else "⏸️ Đã lưu" # Emoji + Text rõ hơn
             reply_lines.append(f"  - {status_icon}: <code>@{html.escape(target)}</code>")
        reply_lines.append("\nℹ️ Dùng <code>/dungtreo &lt;username&gt;</code> hoặc <code>/dungtreo</code> (dừng tất cả).")
        reply_lines.append("<i>(Trạng thái ▶️/⏸️ là ước lượng tại thời điểm xem)</i>")

    reply_text = "\n".join(reply_lines)
    try:
        await delete_user_message(update, context, original_message_id)
        await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed send /listtreo U:{user_id} C:{chat_id}: {e}")
        try: await delete_user_message(update, context, original_message_id)
        except: pass
        await send_temporary_message(update, context, "❌ Lỗi khi lấy danh sách treo.", duration=15, reply=False)

# --- Lệnh /xemfl24h (VIP) ---
async def xemfl24h_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    user_id = user.id; user_id_str = str(user_id)
    original_message_id = update.message.message_id
    logger.info(f"User {user_id} requested /xemfl24h in chat {chat_id}")
    await delete_user_message(update, context, original_message_id)
    if not is_user_vip(user_id):
        err_msg = f"⚠️ {user.mention_html()}, lệnh <code>/xemfl24h</code> chỉ dành cho <b>VIP</b>."
        await send_temporary_message(update, context, err_msg, duration=20, reply=False); return
    user_gains_all_targets = user_daily_gains.get(user_id_str, {}); gains_last_24h = defaultdict(int)
    total_gain_user = 0; current_time = time.time(); time_threshold = current_time - USER_GAIN_HISTORY_SECONDS
    if not user_gains_all_targets: reply_text = f"📊 {user.mention_html()}, không có dữ liệu follow tăng trong 24h qua."
    else:
        for target_username, gain_list in user_gains_all_targets.items():
            gain_for_target = sum(gain for ts, gain in gain_list if isinstance(ts, float) and ts >= time_threshold)
            if gain_for_target > 0: gains_last_24h[target_username] += gain_for_target; total_gain_user += gain_for_target
        reply_lines = [f"📈 <b>Follow Tăng Trong 24 Giờ Qua</b>", f"👤 Cho: {user.mention_html()}"]
        if not gains_last_24h: reply_lines.append("\n<i>Không có tài khoản nào tăng follow trong 24 giờ qua.</i>")
        else:
            reply_lines.append(f"\n✨ Tổng cộng: <b>+{total_gain_user:,} follow</b>")
            sorted_targets = sorted(gains_last_24h.items(), key=lambda item: item[1], reverse=True)
            for target, gain_value in sorted_targets: reply_lines.append(f"  - <code>@{html.escape(target)}</code>: <b>+{gain_value:,}</b>")
        reply_lines.append(f"\n🕒 <i>Dữ liệu từ các lần treo thành công gần nhất.</i>")
        reply_text = "\n".join(reply_lines)
    try: await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e: logger.error(f"Failed send /xemfl24h U:{user_id} C:{chat_id}: {e}"); await send_temporary_message(update, context, "❌ Lỗi xem thống kê follow.", duration=15, reply=False)

# --- Lệnh /mess (Admin - Đã sửa lỗi) ---
async def mess_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Gửi thông báo từ Admin đến nhóm chính (ALLOWED_GROUP_ID)."""
    if not update or not update.message: return
    admin_user = update.effective_user
    if not admin_user or admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /mess attempt by {admin_user.id if admin_user else 'Unknown'}")
        return # Không phản hồi gì cho người không phải admin

    args = context.args
    original_message_id = update.message.message_id
    await delete_user_message(update, context, original_message_id) # Xóa lệnh gốc

    if not args:
        await send_temporary_message(update, context, "⚠️ Thiếu nội dung thông báo.\n<b>Cú pháp:</b> <code>/mess Nội dung cần gửi</code>", duration=20, reply=False)
        return

    # Kiểm tra xem ALLOWED_GROUP_ID đã được cấu hình chưa
    if not ALLOWED_GROUP_ID or not isinstance(ALLOWED_GROUP_ID, int) or ALLOWED_GROUP_ID >= 0:
        await send_temporary_message(update, context, f"⚠️ Không thể gửi: ID nhóm chính (<code>ALLOWED_GROUP_ID</code>) chưa được cấu hình đúng (phải là số âm).", duration=30, reply=False)
        logger.warning(f"Admin {admin_user.id} tried /mess but ALLOWED_GROUP_ID is not configured properly ({ALLOWED_GROUP_ID}).")
        return

    # Lấy toàn bộ text sau lệnh /mess
    # Đảm bảo loại bỏ đúng phần lệnh, kể cả khi có @botusername
    message_text = update.message.text # Giữ nguyên mention entity
    command_part = update.message.text.split()[0] # Phần /mess hoặc /mess@botname
    message_content_raw = "" # Khởi tạo phòng trường hợp không có nội dung

    if len(update.message.text_html) > len(command_part) + 1:
        # <<< SỬA LỖI: Sử dụng update.message.text_html thay vì message.text_html >>>
        try:
            # Tách nội dung HTML sau phần lệnh
            message_content_raw = update.message.text_html.split(' ', 1)[1]
        except IndexError:
             # Trường hợp hiếm gặp: chỉ có lệnh mà không có dấu cách sau đó
             await send_temporary_message(update, context, "⚠️ Nội dung thông báo không được để trống.", duration=20, reply=False)
             return
    else:
         # Nếu độ dài không đủ, tức là không có nội dung sau lệnh
         await send_temporary_message(update, context, "⚠️ Nội dung thông báo không được để trống.", duration=20, reply=False)
         return

    # Nội dung gửi đi, giữ nguyên HTML từ admin để có thể định dạng
    message_to_send = f"📢 <b>Thông báo từ Admin ({admin_user.mention_html()}):</b>\n\n{message_content_raw}"

    try:
        await context.bot.send_message(ALLOWED_GROUP_ID, message_to_send, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        # Gửi xác nhận cho Admin trong chat riêng của họ
        await send_temporary_message(update, context, "✅ Đã gửi thông báo thành công đến nhóm chính.", duration=15, reply=False)
        logger.info(f"Admin {admin_user.id} sent message via /mess to group {ALLOWED_GROUP_ID}")
    except Forbidden:
        await send_temporary_message(update, context, f"❌ Lỗi: Bot không có quyền gửi tin nhắn vào nhóm <code>{ALLOWED_GROUP_ID}</code>. Kiểm tra xem bot có trong nhóm và có quyền gửi tin không.", duration=30, reply=False)
        logger.error(f"Failed to send /mess to group {ALLOWED_GROUP_ID}: Bot Forbidden.")
    except BadRequest as e:
        await send_temporary_message(update, context, f"❌ Lỗi gửi thông báo đến nhóm <code>{ALLOWED_GROUP_ID}</code>: {html.escape(str(e))}", duration=30, reply=False)
        logger.error(f"Failed to send /mess to group {ALLOWED_GROUP_ID}: BadRequest - {e}")
    except Exception as e:
        await send_temporary_message(update, context, f"❌ Lỗi không xác định khi gửi thông báo: {html.escape(str(e))}", duration=30, reply=False)
        logger.error(f"Unexpected error sending /mess to group {ALLOWED_GROUP_ID}: {e}", exc_info=True)


# --- Job Thống Kê Follow Tăng ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    global last_stats_report_time, treo_stats
    current_time = time.time()
    if last_stats_report_time != 0.0 and current_time < last_stats_report_time + TREO_STATS_INTERVAL_SECONDS * 0.95: logger.debug("[Stats Job] Skipping report, not time yet."); return
    logger.info(f"[Stats Job] Starting statistics report job."); target_chat_id_for_stats = ALLOWED_GROUP_ID
    if not target_chat_id_for_stats:
        logger.info("[Stats Job] ALLOWED_GROUP_ID not set. Stats report skipped & data cleared.")
        if treo_stats: treo_stats.clear(); save_data(); logger.info("[Stats Job] Cleared treo_stats data.")
        last_stats_report_time = current_time; return # Cập nhật time để ko check lại ngay
    stats_snapshot = {};
    if treo_stats: try: stats_snapshot = json.loads(json.dumps(treo_stats))
    except Exception as e: logger.error(f"[Stats Job] Error creating stats snapshot: {e}. Aborting."); return
    treo_stats.clear(); last_stats_report_time = current_time; save_data(); logger.info(f"[Stats Job] Cleared job stats, updated time. Processing snapshot with {len(stats_snapshot)} users.")
    if not stats_snapshot: logger.info("[Stats Job] No stats data found. Skipping."); return
    top_gainers = []; total_gain_all = 0
    for user_id_str, targets in stats_snapshot.items():
        if isinstance(targets, dict):
            for target_username, gain in targets.items():
                try: gain_int = int(gain)
                except (ValueError, TypeError): logger.warning(f"[Stats Job] Invalid gain type U:{user_id_str} T:{target_username} G:{gain}"); continue
                if gain_int > 0: top_gainers.append((gain_int, str(user_id_str), str(target_username))); total_gain_all += gain_int
                elif gain_int < 0: logger.warning(f"[Stats Job] Negative gain ({gain_int}) U:{user_id_str}->T:{target_username}.")
        else: logger.warning(f"[Stats Job] Invalid target structure U:{user_id_str}.")
    if not top_gainers: logger.info("[Stats Job] No positive gains. Skipping report."); return
    top_gainers.sort(key=lambda x: x[0], reverse=True)
    report_lines = [f"📊 <b>Thống Kê Treo Follow (Chu Kỳ Gần Nhất)</b> 📊", f"<i>(Tổng cộng: <b>{total_gain_all:,}</b> follow)</i>", "\n🏆 <b>Top Tài Khoản Treo Hiệu Quả:</b>"]
    num_top_to_show = 10; user_mentions_cache = {}; app = context.application
    shown_users = 0
    for i, (gain, user_id_str_gain, target_username_gain) in enumerate(top_gainers):
        if shown_users >= num_top_to_show: break
        user_mention = user_mentions_cache.get(user_id_str_gain)
        if not user_mention:
            try: user_info = await app.bot.get_chat(int(user_id_str_gain)); m = user_info.mention_html(); user_mention = m or f"<a href='tg://user?id={user_id_str_gain}'>User {user_id_str_gain}</a>"
            except Exception as e: logger.warning(f"[Stats Job] Fail get mention U:{user_id_str_gain}: {e}"); user_mention = f"User <code>{user_id_str_gain}</code>"
            user_mentions_cache[user_id_str_gain] = user_mention
        rank_icon = ["🥇", "🥈", "🥉"][shown_users] if shown_users < 3 else "🏅"
        report_lines.append(f"  {rank_icon} <b>+{gain:,} follow</b> cho <code>@{html.escape(target_username_gain)}</code> (By: {user_mention})")
        shown_users += 1
    if not shown_users: report_lines.append("  <i>Không có dữ liệu tăng đáng kể.</i>")
    treo_interval_m = TREO_INTERVAL_SECONDS // 60; stats_interval_h = TREO_STATS_INTERVAL_SECONDS // 3600
    report_lines.append(f"\n🕒 <i>Cập nhật mỗi {stats_interval_h}h. Treo chạy mỗi {treo_interval_m}p.</i>")
    report_text = "\n".join(report_lines)
    try: await app.bot.send_message(target_chat_id_for_stats, report_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True); logger.info(f"[Stats Job] OK sent report to group {target_chat_id_for_stats}.")
    except Exception as e: logger.error(f"[Stats Job] Fail send report group {target_chat_id_for_stats}: {e}", exc_info=True)
    logger.info("[Stats Job] Finished.")


# --- Lệnh /check (Mới) ---
async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lấy thông tin tài khoản TikTok từ API."""
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    # Chỉ message gốc mới có ID này, dùng để xóa nếu là lệnh gõ
    original_message_id = update.message.message_id if update.message.text and update.message.text.startswith('/') else None

    args = context.args
    if not args:
        await send_temporary_message(update, context, "⚠️ Thiếu username TikTok.\n<b>Cú pháp:</b> <code>/check username</code>", duration=15, reply=False) # Không cần reply
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    target_username_raw = args[0].strip()
    target_username = target_username_raw.lstrip("@")
    if not target_username:
        await send_temporary_message(update, context, "⚠️ Username không được trống.", duration=15, reply=False)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    logger.info(f"User {user.id} requested /check for @{target_username}")

    # Gửi tin nhắn chờ và xóa lệnh gốc nếu có
    processing_msg = None
    if original_message_id: # Nếu lệnh đến từ message, xóa nó và gửi tin chờ
         processing_msg = await update.message.reply_html(f"⏳ Đang kiểm tra thông tin <code>@{html.escape(target_username)}</code>...")
         await delete_user_message(update, context, original_message_id)
    # Nếu đến từ callback thì không xóa, chỉ log và không gửi tin chờ (vì đã xóa msg cũ)
    else: logger.debug(f"Handling /check from callback for @{target_username}")


    # Gọi API Check
    api_params = {"user": target_username, "key": CHECK_TIKTOK_API_KEY}
    api_result = await make_api_request(CHECK_TIKTOK_API_URL, params=api_params, method="GET")

    final_response_text = ""
    photo_url = None

    if api_result["success"] and isinstance(api_result["data"], dict):
        data = api_result["data"]
        logger.debug(f"/check API response data: {data}")
        # Kiểm tra trạng thái trong JSON trả về (API này dùng success: true/false)
        if data.get("success") is True:
            # Parse dữ liệu thành công
            uid = data.get("user_id", "?")
            sec_uid = data.get("sec_uid", "?")
            uname = data.get("username", target_username) # Ưu tiên username từ API
            nickname = data.get("nickname", "?")
            followers_raw = data.get("followers", "?") # Có thể là "3,796" hoặc số
            following = data.get("following", "?") # Thêm following nếu có
            hearts = data.get("hearts", "?") # Thêm hearts nếu có
            bio = data.get("bio", "") # Bio có thể trống
            pic = data.get("profilePic", "")
            is_private = data.get("privateAccount", False)
            api_msg = data.get("message", None) # Một số API thành công vẫn có message

            # Định dạng số followers
            followers_display = followers_raw
            if isinstance(followers_raw, str) and followers_raw != "?":
                 try: followers_num = int(re.sub(r'[^\d]', '', followers_raw)); followers_display = f"{followers_num:,}" # Format dấu phẩy
                 except ValueError: pass # Giữ nguyên nếu không parse được

            info_lines = [f"📊 <b>Thông Tin TikTok: @{html.escape(uname)}</b>"]
            info_lines.append(f"👤 Nickname: <b>{html.escape(nickname)}</b> {'🔒 Private' if is_private else ''}")
            info_lines.append(f"❤️ Followers: <code>{followers_display}</code>")
            # Thêm các thông tin khác nếu có
            if following != "?": info_lines.append(f"🫂 Following: <code>{html.escape(str(following))}</code>")
            if hearts != "?": info_lines.append(f"💖 Tổng tim: <code>{html.escape(str(hearts))}</code>")
            if bio: info_lines.append(f"📝 Bio: {html.escape(bio)}")
            if uid != "?": info_lines.append(f"🆔 User ID: <code>{uid}</code>")
            # if sec_uid != "?": info_lines.append(f"🔒 Sec UID: <code>{sec_uid[:10]}...</code>") # Có thể rút gọn sec_uid
            if pic and pic.startswith("http"): photo_url = pic # Lấy URL ảnh đại diện

            final_response_text = "\n".join(info_lines)
            if api_msg: final_response_text += f"\n\n<i>ℹ️ API Message: {html.escape(api_msg)}</i>" # Thêm message API nếu có

        else: # success == false trong JSON
             api_error_msg = data.get("message", "Không tìm thấy user hoặc API báo lỗi.")
             logger.warning(f"/check API call successful but API returned error for @{target_username}. Msg: {api_error_msg}")
             final_response_text = f"❌ Không thể lấy thông tin cho <code>@{html.escape(target_username)}</code>.\nℹ️ Lý do: {html.escape(api_error_msg)}"
    else: # Lỗi HTTP hoặc không phải JSON
        logger.error(f"/check API request failed for @{target_username}. Error: {api_result['error']}")
        final_response_text = f"❌ Lỗi khi gọi API kiểm tra tài khoản <code>@{html.escape(target_username)}</code>.\nℹ️ {html.escape(api_result['error'] or 'Lỗi không xác định')}"

    # Gửi kết quả (có ảnh hoặc chỉ text)
    try:
        if photo_url:
            # Nếu có tin nhắn chờ (lệnh gõ), xóa nó và gửi ảnh mới
            if processing_msg:
                 try: await context.bot.delete_message(chat_id, processing_msg.message_id)
                 except Exception: pass # Bỏ qua nếu xóa lỗi
            await context.bot.send_photo(chat_id, photo=photo_url, caption=final_response_text, parse_mode=ParseMode.HTML)
            logger.info(f"Sent /check result for @{target_username} with photo.")
        else:
            # Nếu không có ảnh, chỉ gửi/sửa text
            if processing_msg:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            else: # Gửi text mới nếu từ callback
                 await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            logger.info(f"Sent /check result for @{target_username} as text.")
    except (BadRequest, Forbidden, TelegramError) as e:
         logger.error(f"Error sending /check final result for @{target_username} C:{chat_id}: {e}")
         # Fallback gửi text lỗi nếu gửi kết quả thất bại
         fallback_error_text = f"❌ Lỗi khi gửi kết quả /check cho @{target_username}."
         if processing_msg:
             try: await context.bot.edit_message_text(chat_id, processing_msg.message_id, text=fallback_error_text)
             except Exception: pass # Bỏ qua nếu sửa cũng lỗi
         else:
              try: await context.bot.send_message(chat_id, fallback_error_text)
              except Exception: pass

# --- Lệnh /sound (Mới) ---
async def sound_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lấy thông tin và link tải bài hát từ SoundCloud."""
    if not update or not update.message: return
    user = update.effective_user; chat_id = update.effective_chat.id
    if not user: return
    # Chỉ message gốc mới có ID này, dùng để xóa nếu là lệnh gõ
    original_message_id = update.message.message_id if update.message.text and update.message.text.startswith('/') else None

    args = context.args
    if not args:
        await send_temporary_message(update, context, "⚠️ Thiếu link SoundCloud.\n<b>Cú pháp:</b> <code>/sound &lt;link_soundcloud&gt;</code>", duration=15, reply=False)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    # Lấy link từ argument đầu tiên
    soundcloud_link = args[0].strip()

    # Kiểm tra cơ bản xem có phải link soundcloud không
    if not re.match(r"https?://(?:www\.)?soundcloud\.com/", soundcloud_link):
        await send_temporary_message(update, context, f"⚠️ Link <code>{html.escape(soundcloud_link)}</code> không giống link SoundCloud hợp lệ.", duration=20, reply=False)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    logger.info(f"User {user.id} requested /sound for link: {soundcloud_link}")

    # Gửi tin nhắn chờ và xóa lệnh gốc nếu có
    processing_msg = None
    if original_message_id:
         processing_msg = await update.message.reply_html(f"⏳ Đang xử lý link SoundCloud...")
         await delete_user_message(update, context, original_message_id)
    else: logger.debug(f"Handling /sound from callback for {soundcloud_link}")

    # Gọi API SoundCloud (URL Encode link trước khi truyền)
    encoded_link = quote(soundcloud_link, safe='') # Mã hóa URL
    api_url = f"{SOUNDCLOUD_API_URL}?link={encoded_link}"
    logger.debug(f"Calling SoundCloud API: {api_url}")
    api_result = await make_api_request(api_url, method="GET") # Không cần params vì đã có trong URL

    final_response_text = ""
    audio_url = None
    thumbnail_url = None
    keyboard = None # Khởi tạo keyboard là None

    if api_result["success"] and isinstance(api_result["data"], dict):
        data = api_result["data"]
        logger.debug(f"/sound API response data: {data}")
        # API này dùng status: "success"
        if data.get("status") == "success":
            # Parse dữ liệu
            title = data.get("title", "Không có tiêu đề")
            duration = data.get("duration", "?") # Thường là mm:ss
            thumbnail = data.get("thumbnail", "")
            author = data.get("author", {}).get("name", "Không rõ tác giả") if isinstance(data.get("author"), dict) else "Không rõ tác giả"
            download_url = data.get("download", "") # Link tải mp3

            info_lines = [f"🎵 <b>Thông Tin Bài Hát SoundCloud</b> 🎵"]
            info_lines.append(f"🎶 Tiêu đề: <b>{html.escape(title)}</b>")
            info_lines.append(f"👤 Tác giả: {html.escape(author)}")
            if duration != "?": info_lines.append(f"⏱ Thời lượng: {html.escape(duration)}")

            if download_url and download_url.startswith("http"):
                audio_url = download_url
                # Tạo nút bấm để tải trực tiếp
                download_button = InlineKeyboardButton("⏬ Tải về MP3", url=download_url)
                keyboard = InlineKeyboardMarkup([[download_button]]) # Gán keyboard ở đây
            else:
                info_lines.append("\n❌ <i>Không tìm thấy link tải trực tiếp.</i>")
                # keyboard vẫn là None

            final_response_text = "\n".join(info_lines)
            if thumbnail and thumbnail.startswith("http"): thumbnail_url = thumbnail

        else: # status != success
             api_error_msg = data.get("message", "API báo lỗi không rõ.")
             logger.warning(f"/sound API call successful but API returned error for link {soundcloud_link}. Msg: {api_error_msg}")
             final_response_text = f"❌ Không thể xử lý link SoundCloud.\nℹ️ Lý do: {html.escape(api_error_msg)}"
             # keyboard vẫn là None
    else: # Lỗi HTTP hoặc không phải JSON
        logger.error(f"/sound API request failed for link {soundcloud_link}. Error: {api_result['error']}")
        final_response_text = f"❌ Lỗi khi gọi API SoundCloud.\nℹ️ {html.escape(api_result['error'] or 'Lỗi không xác định')}"
        # keyboard vẫn là None

    # Gửi kết quả
    try:
        # Ưu tiên gửi ảnh thumbnail nếu có
        if thumbnail_url:
             # Xóa tin nhắn chờ cũ trước khi gửi
             if processing_msg:
                 try: await context.bot.delete_message(chat_id, processing_msg.message_id)
                 except Exception: pass
             await context.bot.send_photo(chat_id, photo=thumbnail_url, caption=final_response_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
             logger.info(f"Sent /sound result for {soundcloud_link} with photo.")
        # Nếu không có thumbnail, chỉ gửi text
        else:
             if processing_msg:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=processing_msg.message_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=keyboard)
             else: # Gửi mới nếu từ callback
                 await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=keyboard)
             logger.info(f"Sent /sound result for {soundcloud_link} as text.")

    except (BadRequest, Forbidden, TelegramError) as e:
         logger.error(f"Error sending /sound final result C:{chat_id} link:{soundcloud_link}: {e}")
         fallback_error_text = f"❌ Lỗi khi gửi kết quả /sound."
         if processing_msg:
             try: await context.bot.edit_message_text(chat_id, processing_msg.message_id, text=fallback_error_text)
             except Exception: pass
         else:
              try: await context.bot.send_message(chat_id, fallback_error_text)
              except Exception: pass


# --- Hàm helper bất đồng bộ để dừng task khi tắt bot ---
async def shutdown_async_tasks(tasks_to_cancel: list[asyncio.Task], timeout: float = 2.0):
    """Helper async function to cancel and wait for treo tasks during shutdown."""
    if not tasks_to_cancel: logger.info("[Shutdown] No active treo tasks found to cancel."); return
    logger.info(f"[Shutdown] Attempting graceful cancel for {len(tasks_to_cancel)} active treo tasks ({timeout}s timeout)...")
    # Hủy tất cả
    for task in tasks_to_cancel:
        if task and not task.done(): task.cancel()
    # Chờ chúng hoàn thành (bị hủy hoặc lỗi timeout)
    results = await asyncio.gather(*[asyncio.wait_for(task, timeout=timeout) for task in tasks_to_cancel], return_exceptions=True)
    logger.info("[Shutdown] Finished waiting for treo task cancellations.")
    cancelled_count, errors_count, finished_count = 0, 0, 0
    for i, result in enumerate(results):
        task_name = f"Task_{i}"; task = tasks_to_cancel[i]
        try: task_name = task.get_name() or task_name
        except Exception: pass
        if isinstance(result, asyncio.CancelledError): cancelled_count += 1; logger.info(f"[Shutdown] Task '{task_name}' cancelled.")
        elif isinstance(result, asyncio.TimeoutError): errors_count += 1; logger.warning(f"[Shutdown] Task '{task_name}' timed out.")
        elif isinstance(result, Exception): errors_count += 1; logger.error(f"[Shutdown] Task '{task_name}' error: {result}", exc_info=False)
        else: finished_count += 1; logger.debug(f"[Shutdown] Task '{task_name}' finished normally.")
    logger.info(f"[Shutdown] Task Summary: {cancelled_count} cancelled, {errors_count} errors/timeouts, {finished_count} finished normally.")

# --- Hàm xử lý tín hiệu tắt (Mới) ---
async def shutdown_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles application shutdown triggered by updater stop."""
    logger.warning("Received shutdown signal. Initiating final procedures...")
    # Thực hiện các thao tác dọn dẹp cần thiết ở đây
    # Thu thập các task đang chạy
    tasks_to_stop_on_shutdown = []
    if active_treo_tasks:
        logger.info("[Shutdown Handler] Collecting active treo tasks...")
        for targets in list(active_treo_tasks.values()):
            for task in list(targets.values()):
                if task and isinstance(task, asyncio.Task) and not task.done():
                    tasks_to_stop_on_shutdown.append(task)
    # Hủy các task
    if tasks_to_stop_on_shutdown:
        logger.info(f"[Shutdown Handler] Found {len(tasks_to_stop_on_shutdown)} tasks. Scheduling cancellation...")
        # Tạo task mới để chạy hàm hủy, không await trực tiếp để handler này kết thúc nhanh
        asyncio.create_task(shutdown_async_tasks(tasks_to_stop_on_shutdown, timeout=2.0))
    else: logger.info("[Shutdown Handler] No active treo tasks found.")
    # Lưu data lần cuối
    logger.info("[Shutdown Handler] Performing final data save...")
    save_data()
    logger.info("[Shutdown Handler] Final data save attempt complete.")
    # Đóng http client nếu đã mở
    global http_client
    if http_client:
        logger.info("[Shutdown Handler] Closing shared HTTP client...")
        await http_client.aclose()
        http_client = None
        logger.info("[Shutdown Handler] Shared HTTP client closed.")
    logger.warning("Shutdown handler finished.")


# --- Main Function (Khởi động bot, khôi phục task, xử lý tắt) ---
def main() -> None:
    """Khởi động, chạy bot và xử lý shutdown."""
    global http_client # Khai báo để có thể gán giá trị
    start_time = time.time()
    print("--- Bot DinoTool Starting ---"); print(f"Timestamp: {datetime.now().isoformat()}")
    print("\n--- Configuration Summary ---")
    print(f"Bot Token: ...{BOT_TOKEN[-6:] if len(BOT_TOKEN)>6 else '***'}")
    print(f"Admin User ID: {ADMIN_USER_ID}")
    print(f"Bill Forward Target ID: {BILL_FORWARD_TARGET_ID}")
    print(f"Allowed Group ID: {ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else 'None'}")
    print(f"Group Link: {GROUP_LINK if GROUP_LINK else 'None'}")
    print(f"Link Shortener API Key: {'Set' if LINK_SHORTENER_API_KEY else 'Not Set'}")
    print(f"QR Code URL: {'Set' if QR_CODE_URL else 'Not Set'}")
    print(f"Payment Info: {BANK_NAME} / {BANK_ACCOUNT} / {ACCOUNT_NAME}")
    print(f"Tim API Key: {'Set' if API_KEY_TIM else 'Not Set'}")
    print(f"Check TikTok API Key: {'Set' if CHECK_TIKTOK_API_KEY else 'Not Set'}")
    print(f"Data File: {DATA_FILE}")
    print(f"Log File: {LOG_FILE}")
    print("-" * 30)

    print("Loading persistent data...")
    load_data() # Load data trước
    persistent_treo_count = sum(len(targets) for targets in persistent_treo_configs.values())
    gain_user_count = len(user_daily_gains)
    gain_entry_count = sum(len(gl) for targets in user_daily_gains.values() for gl in targets.values())
    print(f"Load OK. Keys:{len(valid_keys)} Act:{len(activated_users)} VIP:{len(vip_users)}")
    print(f"Persistent Treo: {persistent_treo_count} targets / {len(persistent_treo_configs)} users")
    print(f"Daily Gains: {gain_entry_count} entries / {gain_user_count} users")
    print(f"Init Job Stats Users: {len(treo_stats)}, Last Rpt: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")

    # --- Khởi tạo HTTP Client dùng chung ---
    print("Initializing shared HTTP client...")
    http_client = httpx.AsyncClient(
        verify=False, # Bỏ qua kiểm tra SSL nếu cần
        timeout=httpx.Timeout(API_TIMEOUT_SECONDS, connect=15.0), # Timeout tổng và timeout kết nối
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=100), # Tăng giới hạn kết nối
        http2=True, # Ưu tiên HTTP/2
        headers={'User-Agent': 'TG Bot DinoTool/1.2'}
    )
    print("Shared HTTP client initialized.")

    # Cấu hình Application
    application = (Application.builder().token(BOT_TOKEN)
                   # Thêm cấu hình tắt an toàn
                   .shutdown_grace_period(5.0) # Chờ 5s cho các handler hoàn thành trước khi gọi shutdown_handler
                   # .job_queue(JobQueue()) # Job queue được tạo tự động
                   # Tăng pool timeout nếu cần, nhưng API_TIMEOUT đã xử lý timeout request
                   .pool_timeout(120).connect_timeout(30).read_timeout(API_TIMEOUT_SECONDS + 10).write_timeout(120)
                   .http_version("1.1").build()) # Dùng HTTP/1.1 vẫn ổn định

    # Lên lịch các job định kỳ
    jq = application.job_queue
    if jq:
        jq.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_expired_data_job")
        logger.info(f"Scheduled cleanup job every {CLEANUP_INTERVAL_SECONDS / 60:.0f} mins.")
        if ALLOWED_GROUP_ID:
            jq.run_repeating(report_treo_stats, interval=TREO_STATS_INTERVAL_SECONDS, first=180, name="report_treo_stats_job") # Chạy sau 3p
            logger.info(f"Scheduled stats report job every {TREO_STATS_INTERVAL_SECONDS / 3600:.1f}h to group {ALLOWED_GROUP_ID}.")
        else: logger.info("Stats report job skipped (ALLOWED_GROUP_ID not set).")
    else: logger.error("JobQueue is not available. Scheduled jobs will not run.")

    # --- Register Handlers ---
    # Commands
    application.add_handler(CommandHandler(("start", "menu"), start_command))
    application.add_handler(CommandHandler("lenh", lenh_command))
    application.add_handler(CommandHandler("getkey", getkey_command))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command)) # Đã sửa lỗi cú pháp
    application.add_handler(CommandHandler("tim", tim_command))
    application.add_handler(CommandHandler("fl", fl_command))
    application.add_handler(CommandHandler("muatt", muatt_command))
    application.add_handler(CommandHandler("treo", treo_command))
    application.add_handler(CommandHandler("dungtreo", dungtreo_command))
    application.add_handler(CommandHandler("listtreo", listtreo_command))
    application.add_handler(CommandHandler("xemfl24h", xemfl24h_command))
    application.add_handler(CommandHandler("check", check_command)) # Lệnh /check mới
    application.add_handler(CommandHandler("sound", sound_command)) # Lệnh /sound mới
    # Admin Commands
    application.add_handler(CommandHandler("addtt", addtt_command))
    application.add_handler(CommandHandler("mess", mess_command))

    # Callback Handlers
    # Sử dụng regex để linh hoạt hơn, bắt show_abc, show_xyz,...
    application.add_handler(CallbackQueryHandler(menu_callback_handler, pattern="^show_"))
    application.add_handler(CallbackQueryHandler(prompt_send_bill_callback, pattern="^prompt_send_bill_\d+$"))

    # Message handler cho ảnh bill (Ưu tiên cao)
    # Dùng TypeHandler thay cho MessageHandler để chắc chắn bắt được cả ảnh và doc ảnh
    # Chỉ xử lý tin nhắn không phải text và đến từ user đang chờ gửi bill
    application.add_handler(TypeHandler(Update, handle_photo_bill), group=-1) # Priority -1
    logger.info("Registered photo/bill TypeHandler (priority -1) for pending users.")

    # Thêm handler để xử lý tắt bot an toàn (ưu tiên thấp nhất)
    application.add_handler(TypeHandler(Update, shutdown_handler), group=10) # Priority 10
    logger.info("Registered shutdown handler (priority 10).")
    # --- End Handler Registration ---

    # --- Khởi động lại các task treo đã lưu ---
    print("\nRestarting persistent treo tasks...")
    restored_count = 0
    users_to_cleanup_restore = [] # user_id_str
    tasks_to_create_data = [] # List of (user_id_str, target_username_str, chat_id_int)
    # Tạo snapshot để lặp qua an toàn
    persistent_treo_snapshot = dict(persistent_treo_configs)

    if persistent_treo_snapshot:
        logger.info(f"Found {len(persistent_treo_snapshot)} users with persistent treo configs.")
        for user_id_str, targets_for_user in persistent_treo_snapshot.items():
            try:
                user_id_int = int(user_id_str)
                # Kiểm tra user còn VIP và còn hạn không?
                if not is_user_vip(user_id_int):
                    logger.warning(f"[Restore] U:{user_id_str} non-VIP. Scheduling config cleanup.")
                    users_to_cleanup_restore.append(user_id_str)
                    continue # Bỏ qua tất cả target của user này

                vip_limit = get_vip_limit(user_id_int)
                current_user_restored_count = 0 # Đếm số task đã khôi phục cho user này
                # Lặp qua bản sao targets của user
                targets_snapshot = dict(targets_for_user)

                for target_username, chat_id_int in targets_snapshot.items():
                    # Kiểm tra limit TRƯỚC khi thêm vào danh sách tạo task
                    if current_user_restored_count >= vip_limit:
                         logger.warning(f"[Restore] U:{user_id_str} reached limit ({vip_limit}). Skipping @{target_username}.")
                         # Xóa config dư thừa khỏi persistent data GỐC
                         if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
                              del persistent_treo_configs[user_id_str][target_username]
                              # Sẽ save_data() sau khi dọn dẹp xong
                         continue # Bỏ qua target này

                    # Kiểm tra task đã chạy chưa (hiếm khi xảy ra)
                    runtime_task = active_treo_tasks.get(user_id_str, {}).get(target_username)
                    if runtime_task and isinstance(runtime_task, asyncio.Task) and not runtime_task.done():
                         logger.info(f"[Restore] Task U:{user_id_str}->@{target_username} already active. Skipping.")
                         current_user_restored_count += 1 # Vẫn tính vào limit
                         continue
                    else:
                         if runtime_task: logger.warning(f"[Restore] Found finished/invalid task U:{user_id_str}->@{target_username}. Attempting restore.")

                    logger.info(f"[Restore] Scheduling restore: U:{user_id_str} -> @{target_username} C:{chat_id_int}")
                    tasks_to_create_data.append((user_id_str, target_username, chat_id_int))
                    current_user_restored_count += 1

            except ValueError: logger.error(f"[Restore] Invalid user_id '{user_id_str}'. Scheduling cleanup."); users_to_cleanup_restore.append(user_id_str)
            except Exception as e: logger.error(f"[Restore] Error processing U:{user_id_str}: {e}", exc_info=True); users_to_cleanup_restore.append(user_id_str)

    # Dọn dẹp config persistent của user không hợp lệ/hết VIP/vượt limit
    cleaned_persistent_configs_on_restore = False
    if users_to_cleanup_restore:
        unique_users = set(users_to_cleanup_restore)
        logger.info(f"[Restore] Cleaning up persistent treo configs for {len(unique_users)} non-VIP/invalid users...")
        for uid_clean in unique_users:
            if uid_clean in persistent_treo_configs: del persistent_treo_configs[uid_clean]; cleaned_persistent_configs_on_restore = True
        if cleaned_persistent_configs_on_restore: logger.info(f"Removed persistent configs for {len(unique_users)} users.")

    # Check lại xem có config nào bị xóa do vượt limit không
    overlimit_cleaned = False
    for uid_snap, targets_snap in persistent_treo_snapshot.items():
         if uid_snap in persistent_treo_configs: # Chỉ check user còn tồn tại
             if len(persistent_treo_configs.get(uid_snap, {})) < len(targets_snap):
                 overlimit_cleaned = True; logger.info(f"[Restore] Detected over-limit cleanup for U:{uid_snap}.")
                 break
    if overlimit_cleaned: cleaned_persistent_configs_on_restore = True

    # Lưu lại data nếu có config bị xóa
    if cleaned_persistent_configs_on_restore: logger.info("[Restore] Saving data after cleaning persistent configs."); save_data()

    # Tạo các task treo đã lên lịch
    if tasks_to_create_data:
        logger.info(f"[Restore] Creating {len(tasks_to_create_data)} restored treo tasks...")
        # Tạo context mặc định để truyền vào task (chứa application)
        default_context = ContextTypes.DEFAULT_TYPE(application=application, chat_id=None, user_id=None)
        for uid_create, target_create, cid_create in tasks_to_create_data:
            try:
                task = application.create_task(run_treo_loop(uid_create, target_create, default_context, cid_create), name=f"treo_{uid_create}_{target_create}_C{cid_create}_restored")
                active_treo_tasks.setdefault(uid_create, {})[target_create] = task
                restored_count += 1
            except Exception as e_create: logger.error(f"[Restore] Failed create task U:{uid_create}->@{target_create}: {e_create}", exc_info=True)
    print(f"Successfully restored and started {restored_count} treo tasks."); print("-" * 30)
    # --- Kết thúc khôi phục task ---

    run_duration = time.time() - start_time; print(f"(Initialization took {run_duration:.2f} seconds)")
    print("\nBot is now polling for updates...")
    logger.info("Bot initialization complete. Starting polling...")

    # Chạy bot và xử lý tắt an toàn
    # application.run_polling() sẽ chạy cho đến khi nhận tín hiệu dừng (SIGINT, SIGTERM, SIGABRT)
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

    # --- Code sau run_polling() chỉ chạy khi bot dừng ---
    print("\nPolling stopped. Application is shutting down..."); logger.warning("Polling stopped. Application is shutting down...")
    # Không cần gọi shutdown_handler thủ công ở đây vì nó đã được đăng ký với Application và sẽ tự chạy khi stop
    # Chỉ cần chờ application shutdown hoàn toàn
    print("Bot has stopped."); logger.warning("Bot has stopped."); print(f"Shutdown timestamp: {datetime.now().isoformat()}")
    # Đảm bảo client HTTP được đóng nếu chưa kịp đóng trong shutdown_handler
    if http_client:
        logger.warning("HTTP client was still open after shutdown sequence. Closing now.")
        # Cần chạy trong event loop nếu nó còn chạy, nếu không thì không cần async
        try:
            loop = asyncio.get_event_loop_policy().get_event_loop()
            if loop.is_running(): loop.run_until_complete(http_client.aclose())
            else: pass # Loop đã đóng, không làm gì
        except Exception as e_close: logger.error(f"Error closing HTTP client manually at the very end: {e_close}")
        finally: http_client = None


# <<< Đã sửa lỗi thụt lề khối try...except...finally cuối cùng >>>
if __name__ == "__main__":
    # try này không thụt vào
    try:
        main()
    # except này ngang cấp với try
    except Exception as e_fatal:
        print(f"\nFATAL ERROR in main execution: {e_fatal}")
        logging.critical(f"FATAL ERROR in main: {e_fatal}", exc_info=True)
        # try bên trong except này phải thụt vào
        try:
            with open("fatal_error.log", "a", encoding='utf-8') as f:
                # import traceback # Đã import ở đầu file
                f.write(f"\n--- {datetime.now().isoformat()} ---\nFATAL ERROR: {e_fatal}\n")
                # Tách lệnh ra cho rõ ràng hơn
                traceback.print_exc(file=f)
                f.write("-" * 30 + "\n")
        # except này ngang cấp với try bên trong
        except Exception as e_log:
            print(f"Could not write fatal error to log file: {e_log}")
    # finally này ngang cấp với try và except bao ngoài
    finally:
        # Đảm bảo client HTTP được đóng ngay cả khi main bị lỗi nghiêm trọng
        if http_client:
            print("Attempting final HTTP client closure after fatal error...")
            # try bên trong finally này phải thụt vào
            try:
                loop = asyncio.get_event_loop_policy().get_event_loop()
                if loop.is_running(): loop.run_until_complete(http_client.aclose())
            # except này ngang cấp với try bên trong
            except Exception as e_close_fatal:
                print(f"Error in final HTTP client closure: {e_close_fatal}")
        print("Exiting program.")


