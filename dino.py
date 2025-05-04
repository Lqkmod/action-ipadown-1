
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

# Thêm import cho Inline Keyboard và các thành phần khác
from telegram import (
    Update,
    Message,
    InputMediaPhoto,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Chat, # Thêm Chat để type hint
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
    CallbackQueryHandler,
    ApplicationHandlerStop, # Để dừng xử lý handler
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError

# --- Cấu hình ---
# !!! THAY THẾ CÁC GIÁ TRỊ PLACEHOLDER BÊN DƯỚI BẰNG GIÁ TRỊ THỰC TẾ CỦA BẠN !!!
BOT_TOKEN = "7416039734:AAE8-vdkSpCzJRvml2nDJip6O1wbkRes2oY" # <--- TOKEN CỦA BOT TELEGRAM CỦA BẠN
# API_KEY dùng cho /tim (có thể không cần nếu API tim ko yêu cầu)
API_KEY = "khangdino99" # <--- API KEY TIM CỦA BẠN (NẾU CÓ)
ADMIN_USER_ID = 7193749511 # <<< --- ID TELEGRAM SỐ CỦA ADMIN (Lấy từ @userinfobot)
BILL_FORWARD_TARGET_ID = 7193749511 # <<< --- ID TELEGRAM SỐ CỦA NƠI NHẬN BILL (Có thể là Admin hoặc bot khác)
ALLOWED_GROUP_ID = -1002191171631 # <--- ID NHÓM CHÍNH (SỐ ÂM, để nhận stats) hoặc None (Nếu None, stats/mess sẽ không hoạt động)
GROUP_LINK = "https://t.me/dinotool" # <<<--- LINK MỜI NHÓM CỦA BẠN (Hiển thị ở /start)
# API Key Check TikTok Info (/check command)
TIKTOK_CHECK_API_KEY = "khang" # API Key cho API check info
# API Key cho Yeumoney Link Shortener (/getkey command)
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
DATA_FILE = "bot_persistent_data_v2.json" # File lưu dữ liệu (đổi tên để tránh xung đột nếu chạy lại code cũ)
LOG_FILE = "bot_v2.log" # File log

# --- Thời gian (Giây) ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 phút (/tim, /fl)
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 phút (/getkey)
KEY_EXPIRY_SECONDS = 6 * 3600   # 6 giờ (Key chưa nhập)
ACTIVATION_DURATION_SECONDS = 6 * 3600 # 6 giờ (Sau khi nhập key)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 giờ (Job dọn dẹp)
TREO_INTERVAL_SECONDS = 900 # 15 phút (Khoảng cách giữa các lần gọi API /treo)
TREO_FAILURE_MSG_DELETE_DELAY = 20 # 20 giây (Xóa tin nhắn treo thất bại)
TREO_STATS_INTERVAL_SECONDS = 24 * 3600 # 24 giờ (Thống kê follow tăng qua job)
USER_GAIN_HISTORY_SECONDS = 24 * 3600 # Lưu lịch sử gain trong 24 giờ cho /xemfl24h
PENDING_BILL_TIMEOUT_SECONDS = 15 * 60 # 15 phút (Timeout chờ gửi bill sau khi bấm nút)
SHUTDOWN_TASK_CANCEL_TIMEOUT = 3.0 # Giây (Timeout chờ task treo hủy khi tắt bot)

# --- API Endpoints ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}" # API TIM (Cần API_KEY)
FOLLOW_API_URL_BASE = "https://api.thanhtien.site/lynk/dino/telefl.php" # API FOLLOW MỚI
TIKTOK_CHECK_API_URL = "https://khangdino.x10.mx/fltik.php" # API /check
SOUNDCLOUD_API_URL = "https://kudodz.x10.mx/api/soundcloud.php" # API /sound

# --- Thông tin VIP ---
VIP_PRICES = {  # <<< SỬA: Đổi tên thành VIP_PRICES
    # days_key: {"price": "Display Price", "limit": max_treo_users, "duration_days": days}
    30: {"price": "30.000 VND", "limit": 5, "duration_days": 30},
    # Thêm các gói khác nếu muốn
}
# Tìm limit cao nhất để dùng làm mặc định cho /addtt <days>
# Dòng này sẽ hoạt động đúng khi VIP_PRICES được định nghĩa đúng tên
DEFAULT_VIP_LIMIT = max(info["limit"] for info in VIP_PRICES.values()) if VIP_PRICES else 1

# --- Biến toàn cục (Sẽ được load/save) ---
user_tim_cooldown = {} # {user_id_str: timestamp}
user_fl_cooldown = defaultdict(dict) # {user_id_str: {target_username: timestamp}}
user_getkey_cooldown = {} # {user_id_str: timestamp}
valid_keys = {} # {key: {"user_id_generator": ..., "expiry_time": ..., "used_by": ..., "activation_time": ...}}
activated_users = {} # {user_id_str: expiry_timestamp} - Người dùng kích hoạt bằng key
vip_users = {} # {user_id_str: {"expiry": expiry_timestamp, "limit": user_limit}} - Người dùng VIP <--- Sẽ lưu lại
persistent_treo_configs = {} # {user_id_str: {target_username: chat_id}} - Lưu để khôi phục sau restart <--- Sẽ lưu lại
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
logging.getLogger("apscheduler").setLevel(logging.WARNING) # Thư viện job mới
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
if not TIKTOK_CHECK_API_KEY: logger.warning("!!! TIKTOK_CHECK_API_KEY chưa được cấu hình. Lệnh /check có thể không hoạt động. !!!")

logger.info("--- Cấu hình cơ bản đã được kiểm tra ---")
logger.info(f"Admin ID: {ADMIN_USER_ID}")
logger.info(f"Bill Forward Target: {BILL_FORWARD_TARGET_ID}")
logger.info(f"Allowed Group ID: {ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else 'Không giới hạn (Stats/Mess Tắt)'}")
logger.info(f"Treo Interval: {TREO_INTERVAL_SECONDS / 60:.1f} phút")
logger.info(f"VIP Packages: {list(VIP_PRICES.keys())} ngày (Default Limit: {DEFAULT_VIP_LIMIT})")


# --- Hàm lưu/tải dữ liệu (Đã kiểm tra tính đầy đủ) ---
def save_data():
    """Lưu dữ liệu vào file JSON một cách an toàn."""
    global persistent_treo_configs, user_daily_gains, treo_stats, vip_users # <<< Thêm vip_users vào global để chắc chắn
    current_time_save = time.time() # Để debug timestamp
    logger.debug(f"Initiating save_data at {datetime.fromtimestamp(current_time_save).isoformat()}")
    # Chuyển đổi keys sang string nếu cần và đảm bảo kiểu dữ liệu đúng
    try:
        string_key_activated_users = {str(k): float(v) for k, v in activated_users.items()}
        string_key_tim_cooldown = {str(k): float(v) for k, v in user_tim_cooldown.items()}
        string_key_fl_cooldown = {str(uid): {uname: float(ts) for uname, ts in udict.items()} for uid, udict in user_fl_cooldown.items()}
        string_key_getkey_cooldown = {str(k): float(v) for k, v in user_getkey_cooldown.items()}
        # <<< Đảm bảo lưu vip_users đúng định dạng >>>
        string_key_vip_users = {
            str(k): {"expiry": float(v.get("expiry", 0)), "limit": int(v.get("limit", 0))}
            for k, v in vip_users.items() if isinstance(v, dict) # Chỉ lưu nếu value là dict
        }

        # Đảm bảo treo_stats chỉ chứa số nguyên
        cleaned_treo_stats = defaultdict(lambda: defaultdict(int))
        for uid_str, targets in treo_stats.items():
            for target, gain in targets.items():
                try: cleaned_treo_stats[str(uid_str)][str(target)] = int(gain)
                except (ValueError, TypeError): pass # Bỏ qua nếu không phải số

        string_key_treo_stats = dict(cleaned_treo_stats) # Chuyển thành dict thường để JSON hóa

        # <<< Đảm bảo lưu persistent_treo_configs đúng định dạng >>>
        string_key_persistent_treo = {
            str(uid): {str(target): int(chatid) for target, chatid in configs.items() if isinstance(chatid, int)}
            for uid, configs in persistent_treo_configs.items() if configs and isinstance(configs, dict)
        }

        string_key_daily_gains = {
            str(uid): {
                str(target): [(float(ts), int(g)) for ts, g in gain_list if isinstance(ts, (int, float)) and isinstance(g, int)]
                for target, gain_list in targets_data.items() if gain_list
            }
            for uid, targets_data in user_daily_gains.items() if targets_data
        }

        data_to_save = {
            "version": "2.1", # Thêm version để theo dõi cấu trúc file
            "save_timestamp": current_time_save,
            "valid_keys": valid_keys, # Key dữ liệu phức tạp, giữ nguyên
            "activated_users": string_key_activated_users,
            "vip_users": string_key_vip_users, # <<< ĐÃ THÊM >>>
            "user_cooldowns": {
                "tim": string_key_tim_cooldown,
                "fl": string_key_fl_cooldown,
                "getkey": string_key_getkey_cooldown
            },
            "treo_stats": string_key_treo_stats,
            "last_stats_report_time": float(last_stats_report_time),
            "persistent_treo_configs": string_key_persistent_treo, # <<< ĐÃ THÊM >>>
            "user_daily_gains": string_key_daily_gains
        }
    except Exception as e_prepare:
        logger.error(f"Lỗi khi chuẩn bị dữ liệu để lưu: {e_prepare}", exc_info=True)
        return # Không lưu nếu chuẩn bị lỗi

    try:
        temp_file = DATA_FILE + ".tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        # Đổi tên file tạm thành file chính (atomic operation trên nhiều OS)
        os.replace(temp_file, DATA_FILE)
        logger.debug(f"Data saved successfully to {DATA_FILE}")
    except Exception as e:
        logger.error(f"Failed to save data to {DATA_FILE}: {e}", exc_info=True)
        # Cố gắng xóa file tạm nếu còn tồn tại
        if os.path.exists(temp_file):
            try: os.remove(temp_file)
            except Exception as e_rem: logger.error(f"Failed to remove temporary save file {temp_file}: {e_rem}")

# --- HÀM LOAD DATA (Đã sửa lỗi syntax và thêm load vip_users/persistent_treo_configs) ---
def load_data():
    """Tải dữ liệu từ file JSON, xử lý lỗi và kiểu dữ liệu."""
    global valid_keys, activated_users, vip_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown, \
           treo_stats, last_stats_report_time, persistent_treo_configs, user_daily_gains

    # Reset về trạng thái rỗng trước khi load
    valid_keys, activated_users, vip_users = {}, {}, {}
    user_tim_cooldown, user_getkey_cooldown = {}, {}
    user_fl_cooldown = defaultdict(dict)
    treo_stats = defaultdict(lambda: defaultdict(int))
    last_stats_report_time = 0
    persistent_treo_configs = {} # <<< Reset persistent_treo_configs >>>
    user_daily_gains = defaultdict(lambda: defaultdict(list))

    if not os.path.exists(DATA_FILE):
        logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
        return # Không cần làm gì thêm

    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            file_version = data.get("version", "unknown")
            save_ts = data.get("save_timestamp")
            save_time_str = f" at {datetime.fromtimestamp(save_ts).isoformat()}" if save_ts else ""
            logger.info(f"Loading data from {DATA_FILE} (Version: {file_version}{save_time_str})")

            # Load từng phần và kiểm tra kiểu dữ liệu cẩn thận
            valid_keys = data.get("valid_keys", {})
            if not isinstance(valid_keys, dict): valid_keys = {}

            loaded_activated = data.get("activated_users", {})
            if isinstance(loaded_activated, dict):
                activated_users = {str(k): float(v) for k, v in loaded_activated.items() if isinstance(v, (int, float))}

            # <<< Load vip_users >>>
            loaded_vip = data.get("vip_users", {})
            if isinstance(loaded_vip, dict):
                vip_users = {
                    str(k): {"expiry": float(v.get("expiry", 0)), "limit": int(v.get("limit", 0))}
                    for k, v in loaded_vip.items() if isinstance(v, dict) and "expiry" in v and "limit" in v # Check cấu trúc chặt chẽ hơn
                }
            elif loaded_vip: # Nếu tồn tại nhưng không phải dict
                logger.warning(f"vip_users in data file is not a dict: {type(loaded_vip)}. Initializing empty.")

            all_cooldowns = data.get("user_cooldowns", {})
            if isinstance(all_cooldowns, dict):
                loaded_tim_cd = all_cooldowns.get("tim", {})
                if isinstance(loaded_tim_cd, dict):
                    user_tim_cooldown = {str(k): float(v) for k, v in loaded_tim_cd.items() if isinstance(v, (int, float))}

                loaded_fl_cd = all_cooldowns.get("fl", {})
                if isinstance(loaded_fl_cd, dict):
                    for uid_str, targets_cd in loaded_fl_cd.items():
                        if isinstance(targets_cd, dict):
                             user_fl_cooldown[str(uid_str)] = {
                                 uname: float(ts) for uname, ts in targets_cd.items() if isinstance(ts, (int, float))
                             }

                loaded_getkey_cd = all_cooldowns.get("getkey", {})
                if isinstance(loaded_getkey_cd, dict):
                    user_getkey_cooldown = {str(k): float(v) for k, v in loaded_getkey_cd.items() if isinstance(v, (int, float))}

            loaded_stats = data.get("treo_stats", {})
            if isinstance(loaded_stats, dict):
                for uid_str, targets_stat in loaded_stats.items():
                    if isinstance(targets_stat, dict):
                        for target, gain in targets_stat.items():
                            try: treo_stats[str(uid_str)][str(target)] = int(gain)
                            except (ValueError, TypeError): logger.warning(f"Skipping invalid treo stat entry: {uid_str}, {target}, {gain}")

            last_stats_report_time = float(data.get("last_stats_report_time", 0))

            # <<< Load persistent_treo_configs (Sửa lỗi và kiểm tra chặt chẽ) >>>
            loaded_persistent_treo = data.get("persistent_treo_configs", {})
            if isinstance(loaded_persistent_treo, dict):
                for uid_str, configs in loaded_persistent_treo.items():
                    if isinstance(configs, dict):
                        valid_user_configs = {}
                        has_invalid_entry = False
                        for target, chatid_val in configs.items():
                            try:
                                # Đảm bảo chatid là số nguyên
                                chatid_int = int(chatid_val)
                                valid_user_configs[str(target)] = chatid_int
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid chat_id '{chatid_val}' for user {uid_str} -> @{target} in persistent_treo_configs. Skipping this entry.")
                                has_invalid_entry = True

                        # Chỉ thêm user nếu có ít nhất 1 config hợp lệ
                        if valid_user_configs:
                            persistent_treo_configs[str(uid_str)] = valid_user_configs
                        elif configs and not has_invalid_entry: # Log nếu user có config nhưng tất cả đều lỗi type khác
                             logger.warning(f"No valid integer chat_id found for user {uid_str} in configs: {configs}")
                    else:
                        logger.warning(f"Invalid config structure type ({type(configs)}) for user {uid_str} in persistent_treo_configs.")
            elif loaded_persistent_treo: # Nếu tồn tại nhưng không phải dict
                 logger.warning(f"persistent_treo_configs in data file is not a dict: {type(loaded_persistent_treo)}. Initializing empty.")
            # --- KẾT THÚC PHẦN SỬA LỖI LOAD persistent_treo_configs ---

            loaded_daily_gains = data.get("user_daily_gains", {})
            if isinstance(loaded_daily_gains, dict):
                current_time_load = time.time() # Lấy thời gian hiện tại một lần
                for uid_str, targets_data in loaded_daily_gains.items():
                    if isinstance(targets_data, dict):
                        for target, gain_list in targets_data.items():
                            if isinstance(gain_list, list):
                                valid_gains = []
                                for item in gain_list:
                                    try:
                                        if isinstance(item, (list, tuple)) and len(item) == 2:
                                            ts = float(item[0])
                                            g = int(item[1])
                                            # Chỉ thêm nếu timestamp hợp lệ (vd: không quá xa tương lai/quá khứ)
                                            # Giới hạn 30 ngày quá khứ, 1 ngày tương lai (đề phòng lỗi đồng hồ)
                                            time_diff = current_time_load - ts
                                            if -86400 < time_diff < 30 * 86400:
                                                valid_gains.append((ts, g))
                                            else: logger.debug(f"Skipping gain entry with out-of-range timestamp ({time_diff/86400:.1f} days ago): {item}")
                                        else: logger.debug(f"Skipping invalid gain entry format: {item}")
                                    except (ValueError, TypeError, IndexError): logger.debug(f"Skipping invalid gain entry value: {item}")
                                if valid_gains: user_daily_gains[str(uid_str)][str(target)].extend(valid_gains)

            logger.info(f"Data loaded successfully from {DATA_FILE}")

    except json.JSONDecodeError as e_json:
        logger.error(f"Failed to decode JSON from {DATA_FILE}: {e_json}. Using empty data structures.", exc_info=False)
        # Reset lại lần nữa để chắc chắn là rỗng
        valid_keys, activated_users, vip_users = {}, {}, {}; user_tim_cooldown, user_getkey_cooldown = {}, {}; user_fl_cooldown = defaultdict(dict)
        treo_stats = defaultdict(lambda: defaultdict(int)); last_stats_report_time = 0; persistent_treo_configs = {}; user_daily_gains = defaultdict(lambda: defaultdict(list))
    except (TypeError, ValueError, KeyError, Exception) as e:
        logger.error(f"Failed to load or parse data from {DATA_FILE}: {e}. Using empty data structures.", exc_info=True)
        # Reset lại lần nữa
        valid_keys, activated_users, vip_users = {}, {}, {}; user_tim_cooldown, user_getkey_cooldown = {}, {}; user_fl_cooldown = defaultdict(dict)
        treo_stats = defaultdict(lambda: defaultdict(int)); last_stats_report_time = 0; persistent_treo_configs = {}; user_daily_gains = defaultdict(lambda: defaultdict(list))

# --- Hàm trợ giúp ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Xóa tin nhắn người dùng một cách an toàn."""
    if not update or not update.effective_chat: return
    msg_id_to_delete = message_id or (update.message.message_id if update.message else None)
    original_chat_id = update.effective_chat.id
    if not msg_id_to_delete: return

    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except Forbidden: logger.debug(f"Cannot delete message {msg_id_to_delete} in chat {original_chat_id}. Bot might not be admin or message too old.")
    except BadRequest as e:
        # Các lỗi BadRequest thường gặp khi xóa tin nhắn không cần log warning
        common_delete_errors = [ "message to delete not found", "message can't be deleted",
                                 "message_id_invalid", "message identifier is not specified" ]
        if any(err in str(e).lower() for err in common_delete_errors):
            logger.debug(f"Could not delete message {msg_id_to_delete} (already deleted or invalid?): {e}")
        else:
            logger.warning(f"BadRequest error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting message {msg_id_to_delete} in chat {original_chat_id}: {e}", exc_info=True)

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Job được lên lịch để xóa tin nhắn."""
    job_data = context.job.data if context.job else {}
    chat_id = job_data.get('chat_id')
    message_id = job_data.get('message_id')
    job_name = context.job.name if context.job else "unknown_del_job"
    if chat_id and message_id:
        logger.debug(f"Job '{job_name}' running to delete message {message_id} in chat {chat_id}")
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(f"Job '{job_name}' successfully deleted message {message_id}")
        except Forbidden: logger.info(f"Job '{job_name}' cannot delete message {message_id}. Bot might not be admin or message too old.")
        except BadRequest as e:
            common_delete_errors = [ "message to delete not found", "message can't be deleted" ]
            if any(err in str(e).lower() for err in common_delete_errors):
                logger.info(f"Job '{job_name}' could not delete message {message_id} (already deleted?): {e}")
            else:
                logger.warning(f"Job '{job_name}' BadRequest deleting message {message_id}: {e}")
        except TelegramError as e: logger.warning(f"Job '{job_name}' Telegram error deleting message {message_id}: {e}")
        except Exception as e: logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else: logger.warning(f"Job '{job_name}' called missing chat_id or message_id. Data: {job_data}")

async def send_temporary_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, duration: int = 15, parse_mode: str = ParseMode.HTML, reply: bool = True):
    """Gửi tin nhắn và tự động xóa sau một khoảng thời gian."""
    if not update or not update.effective_chat: return
    chat_id = update.effective_chat.id
    sent_message = None
    try:
        reply_to_msg_id = update.message.message_id if reply and update.message else None
        send_params = {'chat_id': chat_id, 'text': text, 'parse_mode': parse_mode, 'disable_web_page_preview': True}
        if reply_to_msg_id: send_params['reply_to_message_id'] = reply_to_msg_id

        try:
            sent_message = await context.bot.send_message(**send_params)
        except BadRequest as e:
            # Nếu lỗi do tin nhắn trả lời không tồn tại, thử gửi mà không trả lời
            if reply_to_msg_id and ("reply message not found" in str(e).lower() or "message to reply not found" in str(e).lower()):
                 logger.debug(f"Reply message {reply_to_msg_id} not found for temporary message. Sending without reply.")
                 del send_params['reply_to_message_id']
                 sent_message = await context.bot.send_message(**send_params)
            else: raise # Ném lại các lỗi BadRequest khác

        if sent_message and context.job_queue:
            # Tạo tên job duy nhất
            job_name = f"del_temp_{chat_id}_{sent_message.message_id}_{int(time.time())}"
            context.job_queue.run_once(
                delete_message_job,
                duration,
                data={'chat_id': chat_id, 'message_id': sent_message.message_id},
                name=job_name
            )
            logger.debug(f"Scheduled job '{job_name}' to delete message {sent_message.message_id} in {duration}s")
    except (Forbidden, TelegramError) as e:
        logger.error(f"Error sending/scheduling temporary message to {chat_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in send_temporary_message to {chat_id}: {e}", exc_info=True)

def generate_random_key(length=8):
    """Tạo key ngẫu nhiên dạng Dinotool-xxxx."""
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"Dinotool-{random_part}"

# --- Hàm dừng task treo (Cập nhật để xử lý persistent) ---
async def stop_treo_task(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE | None, reason: str = "Unknown") -> bool:
    """Dừng một task treo cụ thể (runtime VÀ persistent). Trả về True nếu dừng/xóa thành công."""
    global persistent_treo_configs, active_treo_tasks
    task = None
    was_active_runtime = False
    removed_persistent = False
    user_id_str = str(user_id_str)
    target_username = str(target_username)
    task_name = f"treo_{user_id_str}_{target_username}" # Để log cho nhất quán

    # 1. Dừng task đang chạy (runtime)
    if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
        task = active_treo_tasks[user_id_str].get(target_username)
        if task and isinstance(task, asyncio.Task) and not task.done():
            was_active_runtime = True
            logger.info(f"[Treo Task Stop] Attempting to cancel RUNTIME task '{task_name}'. Reason: {reason}")
            task.cancel()
            # Cho task một chút thời gian để xử lý việc hủy bỏ
            try:
                # Không dùng wait_for ở đây vì có thể gây deadlock nếu task bị kẹt
                # Chỉ cần sleep ngắn để scheduler xử lý cancellation
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                logger.info(f"[Treo Task Stop] Sleep after cancel for '{task_name}' was cancelled.")
            except Exception as e: logger.error(f"[Treo Task Stop] Error during brief sleep after cancelling runtime task '{task_name}': {e}")
            logger.info(f"[Treo Task Stop] Runtime Task '{task_name}' cancellation requested.")
        # Luôn xóa khỏi runtime dict nếu key tồn tại
        del active_treo_tasks[user_id_str][target_username]
        if not active_treo_tasks[user_id_str]: # Xóa user key nếu không còn target nào
            del active_treo_tasks[user_id_str]
        logger.info(f"[Treo Task Stop] Removed task entry for {user_id_str} -> @{target_username} from active (runtime) tasks.")
    else:
        logger.debug(f"[Treo Task Stop] No active runtime task found for {task_name}. Checking persistent config.")

    # 2. Xóa khỏi persistent config (nếu có)
    if user_id_str in persistent_treo_configs and target_username in persistent_treo_configs[user_id_str]:
        del persistent_treo_configs[user_id_str][target_username]
        if not persistent_treo_configs[user_id_str]: # Xóa user key nếu không còn target nào
            del persistent_treo_configs[user_id_str]
        removed_persistent = True
        logger.info(f"[Treo Task Stop] Removed entry for {user_id_str} -> @{target_username} from persistent_treo_configs. Triggering save.")
        save_data() # <<< LƯU NGAY SAU KHI XÓA PERSISTENT >>>
    else:
         logger.debug(f"[Treo Task Stop] Entry for {user_id_str} -> @{target_username} not found in persistent_treo_configs.")

    # Trả về True nếu task runtime bị hủy HOẶC config persistent bị xóa
    return was_active_runtime or removed_persistent

# --- Hàm dừng TẤT CẢ task treo cho user (Đã kiểm tra) ---
async def stop_all_treo_tasks_for_user(user_id_str: str, context: ContextTypes.DEFAULT_TYPE | None, reason: str = "Unknown") -> int:
    """Dừng tất cả các task treo của một user (runtime và persistent). Trả về số lượng task/config đã dừng/xóa thành công."""
    stopped_count = 0
    user_id_str = str(user_id_str)

    # Lấy danh sách target từ persistent config LÀ CHÍNH
    # Dùng list() để tạo bản sao, tránh lỗi thay đổi dict khi đang lặp
    targets_in_persistent = list(persistent_treo_configs.get(user_id_str, {}).keys())

    # Luôn kiểm tra cả runtime tasks phòng trường hợp bất đồng bộ
    runtime_only_targets = list(active_treo_tasks.get(user_id_str, {}).keys())
    all_targets_to_stop = set(targets_in_persistent) | set(runtime_only_targets)

    if not all_targets_to_stop:
        logger.info(f"No persistent or runtime treo tasks found for user {user_id_str} to stop (triggered by '{reason}').")
        return 0

    logger.info(f"Stopping all {len(all_targets_to_stop)} potential treo configs/tasks for user {user_id_str}. Reason: {reason}")

    # Lặp qua danh sách tất cả target có thể
    for target_username in all_targets_to_stop:
        # Hàm stop_treo_task sẽ xử lý cả runtime và persistent removal + save_data
        if await stop_treo_task(user_id_str, target_username, context, reason):
            stopped_count += 1
        # Không cần log warning nếu không dừng được vì có thể target chỉ tồn tại ở runtime và đã dừng

    logger.info(f"Finished stopping tasks/configs for user {user_id_str}. Stopped/Removed: {stopped_count}/{len(all_targets_to_stop)} target(s) processed.")
    # Lưu ý: save_data() đã được gọi trong mỗi lần stop_treo_task xóa persistent config thành công.
    return stopped_count


# --- Job Cleanup (Cập nhật để dừng task VIP hết hạn) ---
async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job dọn dẹp dữ liệu hết hạn VÀ dừng task treo của VIP hết hạn."""
    global valid_keys, activated_users, vip_users, user_daily_gains
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate_key = []
    users_to_deactivate_vip = []
    vip_users_to_stop_tasks = [] # User ID strings
    basic_data_changed = False
    gains_cleaned = False

    logger.info("[Cleanup] Starting cleanup job...")

    # 1. Check expired keys (chưa sử dụng)
    for key, data in list(valid_keys.items()):
        try:
            expiry = data.get("expiry_time")
            used = data.get("used_by")
            if expiry is None: raise ValueError("Missing expiry_time")
            if used is None and current_time > float(expiry):
                keys_to_remove.append(key)
        except (ValueError, TypeError, KeyError):
            logger.warning(f"[Cleanup] Removing potentially invalid key entry: {key} - Data: {data}")
            keys_to_remove.append(key)

    # 2. Check expired key activations
    for user_id_str, expiry_timestamp in list(activated_users.items()):
        try:
            if current_time > float(expiry_timestamp):
                users_to_deactivate_key.append(user_id_str)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Removing invalid activated_users entry for ID {user_id_str}: {expiry_timestamp}")
            users_to_deactivate_key.append(user_id_str)

    # 3. Check expired VIP activations
    for user_id_str, vip_data in list(vip_users.items()):
        try:
            expiry = vip_data.get("expiry")
            limit = vip_data.get("limit") # Kiểm tra cả limit để phát hiện cấu trúc lỗi
            if expiry is None or limit is None: raise ValueError("Missing expiry or limit")
            if current_time > float(expiry):
                users_to_deactivate_vip.append(user_id_str)
                vip_users_to_stop_tasks.append(user_id_str) # <<< Đánh dấu để dừng task >>>
        except (ValueError, TypeError, KeyError):
            logger.warning(f"[Cleanup] Removing invalid vip_users entry for ID {user_id_str}: {vip_data}")
            users_to_deactivate_vip.append(user_id_str)
            vip_users_to_stop_tasks.append(user_id_str) # <<< Cũng dừng task nếu dữ liệu VIP lỗi >>>

    # 4. Cleanup old gains from user_daily_gains
    expiry_threshold = current_time - USER_GAIN_HISTORY_SECONDS
    users_to_remove_from_gains = []
    targets_to_remove_overall = defaultdict(list) # {user_id_str: [target1, target2]}

    for user_id_str, targets_data in user_daily_gains.items():
        for target_username, gain_list in targets_data.items():
            valid_gains = [(ts, g) for ts, g in gain_list if isinstance(ts, (int, float)) and ts >= expiry_threshold]
            if len(valid_gains) < len(gain_list): # Nếu có entry bị xóa
                gains_cleaned = True
                if valid_gains:
                    user_daily_gains[user_id_str][target_username] = valid_gains
                else:
                    # Đánh dấu target này để xóa khỏi user
                    targets_to_remove_overall[user_id_str].append(target_username)
            elif not gain_list: # Nếu list rỗng ngay từ đầu
                targets_to_remove_overall[user_id_str].append(target_username)

    # Thực hiện xóa target và user khỏi daily gains
    if targets_to_remove_overall:
        gains_cleaned = True
        for user_id_str_rem_target, targets_list in targets_to_remove_overall.items():
            if user_id_str_rem_target in user_daily_gains:
                for target in targets_list:
                    if target in user_daily_gains[user_id_str_rem_target]:
                        del user_daily_gains[user_id_str_rem_target][target]
                # Nếu user không còn target nào thì đánh dấu user để xóa
                if not user_daily_gains[user_id_str_rem_target]:
                    users_to_remove_from_gains.append(user_id_str_rem_target)

    if users_to_remove_from_gains:
        gains_cleaned = True
        for user_id_str_rem_user in set(users_to_remove_from_gains): # Dùng set để tránh xóa nhiều lần
            if user_id_str_rem_user in user_daily_gains:
                del user_daily_gains[user_id_str_rem_user]
        logger.debug(f"[Cleanup Gains] Removed {len(set(users_to_remove_from_gains))} users from gain tracking.")

    if gains_cleaned: logger.info("[Cleanup Gains] Finished pruning old gain entries.")

    # 5. Perform deletions from basic data structures
    if keys_to_remove:
        logger.info(f"[Cleanup] Removing {len(keys_to_remove)} expired/invalid unused keys.")
        for key in set(keys_to_remove):
            if key in valid_keys:
                del valid_keys[key]
                basic_data_changed = True
    if users_to_deactivate_key:
         logger.info(f"[Cleanup] Deactivating {len(users_to_deactivate_key)} users (key system).")
         for user_id_str in set(users_to_deactivate_key):
             if user_id_str in activated_users:
                 del activated_users[user_id_str]
                 basic_data_changed = True
    if users_to_deactivate_vip:
         logger.info(f"[Cleanup] Deactivating {len(users_to_deactivate_vip)} VIP users from list.")
         for user_id_str in set(users_to_deactivate_vip):
             if user_id_str in vip_users:
                 del vip_users[user_id_str]
                 basic_data_changed = True # <<< Đánh dấu thay đổi >>>

    # 6. Stop tasks for expired/invalid VIPs
    # <<< PHẢI CHẠY SAU KHI XÓA VIP KHỎI LIST VIP_USERS >>>
    if vip_users_to_stop_tasks:
         unique_users_to_stop = set(vip_users_to_stop_tasks)
         logger.info(f"[Cleanup] Scheduling stop for tasks of {len(unique_users_to_stop)} expired/invalid VIP users.")
         app = context.application
         if app:
             for user_id_str_stop in unique_users_to_stop:
                 # Chạy bất đồng bộ để không chặn job cleanup chính
                 # <<< stop_all_treo_tasks_for_user sẽ lo cả runtime và persistent removal + save_data >>>
                 app.create_task(
                     stop_all_treo_tasks_for_user(user_id_str_stop, context, reason="VIP Expired/Removed during Cleanup"),
                     name=f"cleanup_stop_tasks_{user_id_str_stop}"
                 )
         else:
             logger.error("[Cleanup] Application context not found, cannot schedule async task stopping.")

    # 7. Lưu data nếu có thay đổi cơ bản HOẶC gain data đã được dọn dẹp.
    # Việc dừng task VIP đã tự lưu trong stop_all_treo_tasks_for_user -> stop_treo_task.
    if basic_data_changed or gains_cleaned:
        if basic_data_changed: logger.info("[Cleanup] Basic data changed, saving...")
        if gains_cleaned: logger.info("[Cleanup] Gain history data cleaned, saving...")
        save_data()
    else:
        logger.info("[Cleanup] No basic data changes or gain cleanup needed this cycle.")

    logger.info("[Cleanup] Cleanup job finished.")


# --- Kiểm tra VIP/Key (Giữ nguyên) ---
def is_user_vip(user_id: int) -> bool:
    """Kiểm tra trạng thái VIP còn hạn."""
    user_id_str = str(user_id)
    vip_data = vip_users.get(user_id_str)
    if vip_data and isinstance(vip_data, dict):
        try:
            expiry = float(vip_data.get("expiry", 0))
            return time.time() < expiry
        except (ValueError, TypeError): return False
    return False

def get_vip_limit(user_id: int) -> int:
    """Lấy giới hạn treo user của VIP (chỉ trả về nếu còn hạn)."""
    user_id_str = str(user_id)
    if is_user_vip(user_id): # Chỉ trả về limit nếu VIP còn hạn
        try:
            limit = int(vip_users.get(user_id_str, {}).get("limit", 0))
            return limit
        except (ValueError, TypeError): return 0
    return 0 # Không phải VIP hoặc hết hạn -> limit 0

def is_user_activated_by_key(user_id: int) -> bool:
    """Kiểm tra trạng thái kích hoạt bằng key còn hạn."""
    user_id_str = str(user_id)
    expiry_timestamp = activated_users.get(user_id_str)
    if expiry_timestamp:
        try:
            return time.time() < float(expiry_timestamp)
        except (ValueError, TypeError): return False
    return False

def can_use_feature(user_id: int) -> bool:
    """Kiểm tra xem user có thể dùng tính năng (/tim, /fl, /check, /sound) không (VIP hoặc Key còn hạn)."""
    return is_user_vip(user_id) or is_user_activated_by_key(user_id)

# --- Logic API Calls (Cải thiện xử lý lỗi và content-type) ---
async def call_api(url: str, params: dict | None = None, method: str = "GET", timeout: float = 60.0, api_name: str = "Unknown") -> dict:
    """Hàm gọi API chung, trả về dict {'success': bool, 'message': str, 'data': dict|None, 'status_code': int|None}."""
    log_params = params.copy() if params else {}
    # Che các key nhạy cảm trong log
    for key in ['key', 'token', 'tokenbot', 'api_key']:
        if key in log_params:
            val = log_params[key]
            log_params[key] = f"...{val[-6:]}" if isinstance(val, str) and len(val) > 6 else "***"

    logger.info(f"[{api_name} API Call] Requesting {method} {url} with params: {log_params}")
    result = {"success": False, "message": "Lỗi không xác định khi gọi API.", "data": None, "status_code": None}
    try:
        async with httpx.AsyncClient(verify=False, timeout=timeout) as client: # Tắt verify SSL
            if method.upper() == "GET":
                resp = await client.get(url, params=params, headers={'User-Agent': f'TG Bot {api_name} Caller'})
            elif method.upper() == "POST":
                resp = await client.post(url, data=params, headers={'User-Agent': f'TG Bot {api_name} Caller'})
            else:
                result["message"] = f"Phương thức HTTP không hỗ trợ: {method}"
                return result

            result["status_code"] = resp.status_code
            content_type = resp.headers.get("content-type", "").lower()
            response_text_full = ""
            try:
                # Thử decode với nhiều encoding phổ biến
                encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1']
                decoded = False
                resp_bytes = await resp.aread()
                for enc in encodings_to_try:
                    try:
                        response_text_full = resp_bytes.decode(enc, errors='strict')
                        logger.debug(f"[{api_name} API Call] Decoded response with {enc}.")
                        decoded = True; break
                    except UnicodeDecodeError: logger.debug(f"[{api_name} API Call] Failed to decode with {enc}")
                if not decoded:
                    response_text_full = resp_bytes.decode('utf-8', errors='replace') # Fallback
                    logger.warning(f"[{api_name} API Call] Could not decode response with common encodings, using replace.")
            except Exception as e_read_outer:
                 logger.error(f"[{api_name} API Call] Error reading/decoding response body: {e_read_outer}")
                 response_text_full = "[Error reading response body]"

            response_text_for_debug = response_text_full[:500] + ('...' if len(response_text_full)>500 else '')
            logger.debug(f"[{api_name} API Call] Status: {resp.status_code}, Content-Type: {content_type}, Snippet: {response_text_for_debug}")

            if resp.status_code == 200:
                if "application/json" in content_type or "text/json" in content_type: # Chấp nhận cả text/json
                    try:
                        data = json.loads(response_text_full)
                        result["data"] = data
                        # Kiểm tra các key success phổ biến
                        api_status = data.get("status", data.get("success")) # Ưu tiên 'status'
                        api_message = data.get("message", data.get("msg", data.get("reason"))) # Ưu tiên 'message'

                        if isinstance(api_status, bool): result["success"] = api_status
                        elif isinstance(api_status, str): result["success"] = api_status.lower() in ['true', 'success', 'ok', '200']
                        elif isinstance(api_status, int): result["success"] = api_status in [200, 1, 0] # 0 cũng có thể là success trong vài API
                        else: result["success"] = False # Mặc định là false nếu không rõ

                        result["message"] = str(api_message) if api_message is not None else ("Thành công." if result["success"] else "Thất bại không rõ lý do.")
                    except json.JSONDecodeError:
                        logger.error(f"[{api_name} API Call] Response 200 OK but not valid JSON ({content_type}).")
                        # Cố gắng trích lỗi từ HTML nếu có
                        error_match = re.search(r'<pre>(.*?)</pre>', response_text_full, re.DOTALL | re.IGNORECASE)
                        error_detail = f": {html.escape(error_match.group(1).strip())}" if error_match else "."
                        result["message"] = f"Lỗi API (Không phải JSON){error_detail}"
                        result["success"] = False
                    except Exception as e_proc:
                        logger.error(f"[{api_name} API Call] Error processing API JSON data: {e_proc}", exc_info=True)
                        result["message"] = "Lỗi xử lý dữ liệu JSON từ API."
                        result["success"] = False
                else: # 200 OK nhưng không phải JSON
                     logger.warning(f"[{api_name} API Call] Response 200 OK but wrong Content-Type: {content_type}.")
                     # Heuristic: Phản hồi ngắn, không lỗi -> OK
                     if len(response_text_full) < 100 and all(w not in response_text_full.lower() for w in ['error', 'lỗi', 'fail']):
                         result["success"] = True
                         result["message"] = "Thành công (Phản hồi không chuẩn JSON)."
                         result["data"] = {"raw_response": response_text_full}
                     else:
                         result["success"] = False
                         error_match = re.search(r'<pre>(.*?)</pre>', response_text_full, re.DOTALL | re.IGNORECASE)
                         html_error = f": {html.escape(error_match.group(1).strip())}" if error_match else "."
                         result["message"] = f"Lỗi định dạng phản hồi API (Type: {content_type}){html_error}"
            else: # Lỗi HTTP
                 logger.error(f"[{api_name} API Call] HTTP Error Status: {resp.status_code}.")
                 result["message"] = f"Lỗi từ API (Mã HTTP: {resp.status_code})."
                 result["success"] = False
                 # Cố gắng lấy message lỗi từ JSON nếu có
                 if "application/json" in content_type or "text/json" in content_type:
                     try:
                         error_data = json.loads(response_text_full)
                         error_msg = error_data.get("message", error_data.get("msg"))
                         if error_msg: result["message"] += f" {html.escape(str(error_msg))}"
                     except Exception: pass # Bỏ qua nếu không parse được
                 # Thử lấy lỗi từ HTML
                 else:
                     error_match = re.search(r'<pre>(.*?)</pre>', response_text_full, re.DOTALL | re.IGNORECASE)
                     html_error_detail = f"\nChi tiết: {html.escape(error_match.group(1).strip())}" if error_match else ""
                     result["message"] += html_error_detail


    except httpx.TimeoutException:
        logger.warning(f"[{api_name} API Call] API timeout.")
        result["message"] = "Lỗi: API timeout."
        result["success"] = False
    except httpx.ConnectError as e_connect:
        logger.error(f"[{api_name} API Call] Connection error: {e_connect}", exc_info=False)
        result["message"] = "Lỗi kết nối đến API."
        result["success"] = False
    except httpx.RequestError as e_req:
        logger.error(f"[{api_name} API Call] Network error: {e_req}", exc_info=False)
        result["message"] = "Lỗi mạng khi kết nối API."
        result["success"] = False
    except Exception as e_unexp:
        logger.error(f"[{api_name} API Call] Unexpected error during API call: {e_unexp}", exc_info=True)
        result["message"] = "Lỗi hệ thống Bot khi xử lý API."
        result["success"] = False

    # Đảm bảo message luôn là string
    if not isinstance(result["message"], str): result["message"] = str(result.get("message", "Lỗi không xác định."))
    logger.info(f"[{api_name} API Call] Final result: Success={result['success']}, Code={result['status_code']}, Message='{result['message'][:200]}...'")
    return result

async def call_follow_api(user_id_str: str, target_username: str, bot_token: str) -> dict:
    """Gọi API follow cụ thể."""
    params = {"user": target_username, "userid": user_id_str, "tokenbot": bot_token}
    return await call_api(FOLLOW_API_URL_BASE, params=params, method="GET", timeout=90.0, api_name="Follow")

async def call_tiktok_check_api(username: str) -> dict:
    """Gọi API check info TikTok."""
    params = {"user": username, "key": TIKTOK_CHECK_API_KEY}
    return await call_api(TIKTOK_CHECK_API_URL, params=params, method="GET", timeout=30.0, api_name="TikTok Check")

async def call_soundcloud_api(link: str) -> dict:
    """Gọi API SoundCloud."""
    params = {"link": link}
    return await call_api(SOUNDCLOUD_API_URL, params=params, method="GET", timeout=45.0, api_name="SoundCloud")

# --- Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /start hoặc /menu - Hiển thị menu chính."""
    if not update or not update.effective_user: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    logger.info(f"User {user.id} ({user.username or 'NoUsername'}) used /start or /menu in chat {chat_id} (type: {update.effective_chat.type})")

    act_h = ACTIVATION_DURATION_SECONDS // 3600
    treo_interval_m = TREO_INTERVAL_SECONDS // 60
    welcome_text = (
        f"👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
        f"🤖 Chào mừng bạn đến với <b>DinoTool</b> - Bot hỗ trợ TikTok.\n\n"
        f"✨ <b>Cách sử dụng cơ bản (Miễn phí):</b>\n"
        f"   » Dùng <code>/getkey</code> và <code>/nhapkey &lt;key&gt;</code> để kích hoạt {act_h} giờ sử dụng <code>/tim</code>, <code>/fl</code>.\n\n"
        f"👑 <b>Nâng cấp VIP:</b>\n"
        f"   » Mở khóa <code>/treo</code> (tự động chạy /fl mỗi {treo_interval_m} phút), không cần key, giới hạn cao hơn, xem gain 24h (<code>/xemfl24h</code>), kiểm tra info (<code>/check</code>) và các lệnh khác.\n\n"
        f"👇 <b>Chọn một tùy chọn bên dưới:</b>"
    )

    keyboard_buttons = [
        [InlineKeyboardButton("👑 Mua VIP", callback_data="show_muatt")],
        [InlineKeyboardButton("📜 Lệnh Bot", callback_data="show_lenh")],
    ]
    # Chỉ hiện nút nhóm nếu có link và ID nhóm
    if ALLOWED_GROUP_ID and GROUP_LINK and GROUP_LINK != "YOUR_GROUP_INVITE_LINK":
         keyboard_buttons.append([InlineKeyboardButton("💬 Nhóm Chính", url=GROUP_LINK)])
    keyboard_buttons.append([InlineKeyboardButton("👨‍💻 Liên hệ Admin", url=f"tg://user?id={ADMIN_USER_ID}")])
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    try:
        # Xóa lệnh /start hoặc /menu gốc nếu là tin nhắn
        if update.message:
            await delete_user_message(update, context, update.message.message_id)

        # Gửi tin nhắn chào mừng kèm menu
        await context.bot.send_message(
            chat_id=chat_id,
            text=welcome_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /start or /menu message to {user.id} in chat {chat_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in start_command for user {user.id}: {e}", exc_info=True)

async def menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý callback từ các nút trong menu chính."""
    query = update.callback_query
    if not query or not query.from_user: return
    user = query.from_user
    callback_data = query.data
    logger.info(f"Menu callback '{callback_data}' triggered by user {user.id} ({user.username}) in chat {query.message.chat_id}")

    try:
        await query.answer() # Luôn trả lời callback trước
    except Exception as e_ans:
        logger.warning(f"Failed to answer menu callback '{callback_data}' for user {user.id}: {e_ans}")
        return # Không xử lý tiếp nếu không trả lời được callback

    # Xóa tin nhắn menu cũ đi
    try:
        await query.delete_message()
    except Exception as e:
        logger.debug(f"Could not delete old menu message {query.message.message_id}: {e}")

    # Tạo Update giả lập để gọi hàm command tương ứng
    command_name = callback_data.split('_')[-1] # vd: show_muatt -> muatt
    fake_message_text = f"/{command_name}"

    # Tạo đối tượng Chat và User từ query
    effective_chat = Chat(id=query.message.chat.id, type=query.message.chat.type)
    from_user = user # Đã lấy từ query.from_user

    # Tạo đối tượng Message giả
    fake_message = Message(
        message_id=query.message.message_id + random.randint(1000, 9999), # ID giả ngẫu nhiên
        date=datetime.now(),
        chat=effective_chat,
        from_user=from_user,
        text=fake_message_text
        # Bỏ qua các thuộc tính khác không cần thiết
    )
    # Tạo đối tượng Update giả
    fake_update = Update(
        update_id=update.update_id + random.randint(1000, 9999), # ID giả ngẫu nhiên
        message=fake_message
    )

    # Gọi hàm xử lý lệnh tương ứng
    try:
        if callback_data == "show_muatt":
            await muatt_command(fake_update, context)
        elif callback_data == "show_lenh":
            await lenh_command(fake_update, context)
        # Thêm các callback khác nếu cần
    except Exception as e:
        logger.error(f"Error executing command handler from callback '{callback_data}' for user {user.id}: {e}", exc_info=True)
        try:
            # Gửi thông báo lỗi nếu thực thi handler lỗi
            await context.bot.send_message(user.id, f"⚠️ Đã xảy ra lỗi khi xử lý yêu cầu '{command_name}'. Vui lòng thử lại sau hoặc báo Admin.", parse_mode=ParseMode.HTML)
        except Exception as e_send_err:
             logger.error(f"Failed to send error message to user {user.id} after callback handler error: {e_send_err}")


# --- Lệnh /lenh (Đã sửa lỗi SyntaxError và cập nhật) ---
async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /lenh - Hiển thị danh sách lệnh và trạng thái user."""
    if not update or not update.effective_user: return
    user = update.effective_user
    chat_id = update.effective_chat.id

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
        if expiry_ts:
            try:
                expiry_dt = datetime.fromtimestamp(float(expiry_ts))
                expiry_str = expiry_dt.strftime('%d/%m/%Y %H:%M') # <<< Đã sửa
            except (ValueError, TypeError, OSError) as e:
                logger.warning(f"Error formatting VIP expiry for user {user_id}: {e}. Timestamp: {expiry_ts}")
                expiry_str = "Lỗi định dạng" # Gán giá trị nếu lỗi
        status_lines.append(f"👑 <b>Trạng thái:</b> VIP ✨ (Hết hạn: {expiry_str}, Giới hạn treo: {limit} users)")
    elif is_key_active:
        expiry_ts = activated_users.get(user_id_str)
        expiry_str = "Không rõ"
        if expiry_ts:
            try:
                expiry_dt = datetime.fromtimestamp(float(expiry_ts))
                expiry_str = expiry_dt.strftime('%d/%m/%Y %H:%M') # <<< Đã sửa
            except (ValueError, TypeError, OSError) as e:
                logger.warning(f"Error formatting Key expiry for user {user_id}: {e}. Timestamp: {expiry_ts}")
                expiry_str = "Lỗi định dạng"
        status_lines.append(f"🔑 <b>Trạng thái:</b> Đã kích hoạt (Key) (Hết hạn: {expiry_str})")
    else:
        status_lines.append("▫️ <b>Trạng thái:</b> Thành viên thường")

    status_lines.append(f"⚡️ <b>Quyền dùng /tim, /fl:</b> {'✅ Có thể' if can_use_std_features else '❌ Chưa thể (Cần VIP/Key)'}")
    current_treo_count = len(persistent_treo_configs.get(user_id_str, {})) # <<< Đọc từ persistent
    vip_limit = get_vip_limit(user_id) # Lấy limit chỉ khi còn VIP
    if is_vip:
        status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ✅ Có thể (Đang treo: {current_treo_count}/{vip_limit} users)")
    else:
         # Hiển thị limit mặc định nếu user hết hạn VIP nhưng vẫn còn config treo (bất thường)
         current_limit_display = vip_users.get(user_id_str, {}).get("limit", 0) if user_id_str in vip_users else 0
         status_lines.append(f"⚙️ <b>Quyền dùng /treo:</b> ❌ Chỉ dành cho VIP (Đang treo: {current_treo_count}/{current_limit_display} users)")

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
        f"  <code>/dungtreo</code> - Dừng treo <b>TẤT CẢ</b> tài khoản",
        f"  <code>/listtreo</code> - Xem danh sách tài khoản đang treo",
        f"  <code>/xemfl24h</code> - Xem số follow đã tăng trong 24 giờ qua (cho các tài khoản đang treo)",
        "\n<b><u>📊 Lệnh Tiện Ích (VIP/Key):</u></b>", # Thêm nhóm mới
        f"  <code>/check &lt;username&gt;</code> - Kiểm tra thông tin tài khoản TikTok",
        f"  <code>/sound &lt;link_soundcloud&gt;</code> - Lấy thông tin bài nhạc SoundCloud",
    ])
    if user_id == ADMIN_USER_ID:
        cmd_lines.append("\n<b><u>🛠️ Lệnh Admin:</u></b>")
        cmd_lines.append(f"  <code>/addtt &lt;user_id&gt; &lt;số_ngày&gt;</code> - Thêm/gia hạn VIP (VD: <code>/addtt 123 30</code>)") # Sửa lại mô tả addtt
        cmd_lines.append(f"  <code>/xoavip &lt;user_id&gt;</code> - Xóa VIP của người dùng (Dừng cả treo)") # <<< THÊM LỆNH XOAVIP >>>
        cmd_lines.append(f"  <code>/mess &lt;nội_dung&gt;</code> - Gửi thông báo đến User VIP/Active") # Sửa mô tả /mess
        # cmd_lines.append(f"  <code>/adminlisttreo &lt;user_id&gt;</code> - (Chưa impl.) Xem list treo của user khác")
    cmd_lines.extend([
        "\n<b><u>ℹ️ Lệnh Chung:</u></b>",
        f"  <code>/start</code> - Hiển thị menu chào mừng",
        f"  <code>/lenh</code> - Xem lại bảng lệnh và trạng thái này",
        "\n<i>Lưu ý: Các lệnh yêu cầu VIP/Key chỉ hoạt động khi bạn có trạng thái tương ứng và còn hạn.</i>"
    ])

    help_text = "\n".join(status_lines + cmd_lines)
    try:
        # Xóa lệnh /lenh gốc (chỉ xóa nếu nó đến từ message)
        if update.message and update.message.message_id:
             await delete_user_message(update, context, update.message.message_id)
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except (BadRequest, Forbidden, TelegramError) as e:
        logger.warning(f"Failed to send /lenh message to {user.id} in chat {chat_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in lenh_command for user {user.id}: {e}", exc_info=True)


# --- Lệnh /tim (Giữ nguyên, đã kiểm tra) ---
async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lệnh /tim."""
    if not update or not update.effective_user: return
    user = update.effective_user
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()
    current_time = time.time()

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để dùng lệnh này!\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code> | 👑 Hoặc: <code>/muatt</code>")
        await send_temporary_message(update, context, err_msg, duration=30, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    # Check Cooldown
    last_usage = user_tim_cooldown.get(user_id_str)
    if last_usage:
        try:
            elapsed = current_time - float(last_usage)
            if elapsed < TIM_FL_COOLDOWN_SECONDS:
                rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {invoking_user_mention}, đợi <b>{rem_time:.0f} giây</b> nữa để dùng <code>/tim</code>."
                await send_temporary_message(update, context, cd_msg, duration=15, reply=True)
                if original_message_id: await delete_user_message(update, context, original_message_id)
                return
        except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp for /tim user {user_id_str}. Resetting.")
             if user_id_str in user_tim_cooldown: del user_tim_cooldown[user_id_str]; save_data()

    # Parse Arguments & Validate URL
    args = context.args
    video_url = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Chưa nhập link video.\n<b>Cú pháp:</b> <code>/tim https://tiktok.com/...</code>")
    else:
        url_input = args[0]
        # Chấp nhận link tiktok.com, vm.tiktok.com, vt.tiktok.com bao gồm cả query params
        if not re.match(r"https?://(?:www\.|vm\.|vt\.)?tiktok\.com/.*", url_input):
             err_txt = f"⚠️ Link <code>{html.escape(url_input)}</code> không hợp lệ. Phải là link video TikTok."
        else:
            video_url = url_input # Giữ nguyên link hợp lệ

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return
    if not video_url: # Double check
        await send_temporary_message(update, context, "⚠️ Không thể xử lý link video.", duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return
    if not API_KEY: # Kiểm tra API Key cấu hình
        logger.error(f"Missing API_KEY for /tim command triggered by user {user_id}")
        await send_temporary_message(update, context, "❌ Lỗi cấu hình: Bot thiếu API Key cho chức năng này. Báo Admin.", duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    # Call API
    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    log_api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key="***")
    logger.info(f"User {user_id} calling /tim API: {log_api_url}")
    processing_msg = None
    final_response_text = ""

    try:
        # Gửi tin nhắn chờ và xóa lệnh gốc
        if update.message:
            processing_msg = await update.message.reply_html("<b><i>⏳ Đang xử lý yêu cầu tăng tim...</i></b> ❤️")
            if original_message_id: await delete_user_message(update, context, original_message_id)
        else: # Trường hợp gọi từ callback hoặc nơi khác không có message gốc rõ ràng
            processing_msg = await context.bot.send_message(chat_id, "<b><i>⏳ Đang xử lý yêu cầu tăng tim...</i></b> ❤️", parse_mode=ParseMode.HTML)

        # Gọi API bằng hàm chung
        api_result = await call_api(api_url, method="GET", timeout=60.0, api_name="Tim")

        if api_result["success"]:
            user_tim_cooldown[user_id_str] = time.time(); save_data()
            d = api_result.get("data", {}) or {} # Đảm bảo d là dict
            a = html.escape(str(d.get("author", "?")))
            v = html.escape(str(d.get("video_url", video_url)))
            db = html.escape(str(d.get('digg_before', '?')))
            di = html.escape(str(d.get('digg_increased', '?')))
            da = html.escape(str(d.get('digg_after', '?')))
            final_response_text = (
                f"🎉 <b>Tăng Tim Thành Công!</b> ❤️\n👤 Cho: {invoking_user_mention}\n\n"
                f"📊 <b>Thông tin Video:</b>\n🎬 <a href='{v}'>Link Video</a>\n✍️ Tác giả: <code>{a}</code>\n"
                f"👍 Trước: <code>{db}</code> ➜ 💖 Tăng: <code>+{di}</code> ➜ ✅ Sau: <code>{da}</code>" )
        else:
            api_msg = api_result["message"]
            logger.warning(f"/tim API call failed for user {user_id}. API message: {api_msg}")
            final_response_text = f"💔 <b>Tăng Tim Thất Bại!</b>\n👤 Cho: {invoking_user_mention}\nℹ️ Lý do: <code>{html.escape(api_msg)}</code>"

    except Exception as e_unexp:
        logger.error(f"Unexpected error during /tim command for user {user_id}: {e_unexp}", exc_info=True)
        final_response_text = f"❌ <b>Lỗi Hệ Thống Bot</b>\n👤 Cho: {invoking_user_mention}\nℹ️ Đã xảy ra lỗi. Báo Admin."
    finally:
        if processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=processing_msg.message_id,
                    text=final_response_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )
            except Exception as e_edit:
                logger.warning(f"Failed to edit /tim processing msg {processing_msg.message_id}: {e_edit}")
                # Nếu edit lỗi, thử gửi tin mới
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=final_response_text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                except Exception as e_send_new:
                     logger.error(f"Also failed to send new final /tim message for user {user_id}: {e_send_new}")
        else: # Trường hợp không có tin nhắn chờ
             logger.warning(f"Processing message for /tim user {user_id} was None. Sending new.")
             try:
                 await context.bot.send_message(
                     chat_id=chat_id,
                     text=final_response_text,
                     parse_mode=ParseMode.HTML,
                     disable_web_page_preview=True
                 )
             except Exception as e_send: logger.error(f"Failed to send final /tim message for user {user_id}: {e_send}")


# --- Hàm chạy nền /fl (Giữ nguyên, đã kiểm tra) ---
async def process_fl_request_background(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id_str: str, target_username: str, processing_msg_id: int, invoking_user_mention: str):
    """Hàm chạy nền xử lý API follow và cập nhật kết quả."""
    logger.info(f"[BG Task /fl] Starting for user {user_id_str} -> @{target_username}")
    api_result = await call_follow_api(user_id_str, target_username, context.bot.token)
    success = api_result["success"]
    api_message = api_result["message"]
    api_data = api_result.get("data") # Có thể là None
    final_response_text = ""
    user_info_block = ""
    follower_info_block = ""

    if api_data and isinstance(api_data, dict):
        try:
            # Trích xuất và escape dữ liệu
            name = html.escape(str(api_data.get("name", "?")))
            tt_username_from_api = api_data.get("username")
            tt_username = html.escape(str(tt_username_from_api if tt_username_from_api else target_username))
            tt_user_id = html.escape(str(api_data.get("user_id", "?")))
            khu_vuc = html.escape(str(api_data.get("khu_vuc", "Không rõ")))
            avatar = api_data.get("avatar", "") # Không cần escape link avatar
            create_time = html.escape(str(api_data.get("create_time", "?")))

            # Xây dựng khối thông tin user
            user_info_lines = [f"👤 <b>Tài khoản:</b> <a href='https://tiktok.com/@{tt_username}'>{name}</a> (<code>@{tt_username}</code>)"]
            if tt_user_id != "?": user_info_lines.append(f"🆔 <b>ID TikTok:</b> <code>{tt_user_id}</code>")
            if khu_vuc != "Không rõ": user_info_lines.append(f"🌍 <b>Khu vực:</b> {khu_vuc}")
            if create_time != "?": user_info_lines.append(f"📅 <b>Ngày tạo TK:</b> {create_time}")
            user_info_block = "\n".join(user_info_lines) + "\n"

            # Xử lý thông tin follower
            f_before_raw = api_data.get("followers_before", "?")
            f_add_raw = api_data.get("followers_add", "?")
            f_after_raw = api_data.get("followers_after", "?")

            f_before_display = "?"
            f_add_display = "?"
            f_after_display = "?"
            f_add_int = 0

            # Hàm helper để làm sạch và định dạng số
            def format_follower_count(count_raw):
                if count_raw is None or count_raw == "?": return "?", None
                try:
                    count_str = re.sub(r'[^\d-]', '', str(count_raw))
                    if count_str:
                        count_int = int(count_str)
                        return f"{count_int:,}", count_int # Định dạng với dấu phẩy
                    return "?", None
                except ValueError: return html.escape(str(count_raw)), None

            f_before_display, _ = format_follower_count(f_before_raw)
            f_add_display, f_add_int = format_follower_count(f_add_raw)
            f_after_display, _ = format_follower_count(f_after_raw)

            if f_add_int is None: f_add_int = 0 # Đảm bảo f_add_int là số
            if f_add_display != "?" and f_add_int > 0: f_add_display = f"+{f_add_display}" # Thêm dấu +

            # Xây dựng khối thông tin follower
            if any(x != "?" for x in [f_before_display, f_add_display, f_after_display]):
                follower_lines = ["📈 <b>Số lượng Follower:</b>"]
                if f_before_display != "?": follower_lines.append(f"   Trước: <code>{f_before_display}</code>")
                if f_add_display != "?":
                    style = "<b>" if f_add_int > 0 else ""
                    style_end = "</b> ✨" if f_add_int > 0 else ""
                    follower_lines.append(f"   Tăng:   {style}<code>{f_add_display}</code>{style_end}")
                if f_after_display != "?": follower_lines.append(f"   Sau:    <code>{f_after_display}</code>")
                if len(follower_lines) > 1: follower_info_block = "\n".join(follower_lines)

        except Exception as e_parse:
            logger.error(f"[BG Task /fl] Error parsing API data for @{target_username}: {e_parse}. Data: {api_data}", exc_info=True)
            user_info_block = f"👤 <b>Tài khoản:</b> <code>@{html.escape(target_username)}</code>\n(Lỗi xử lý thông tin chi tiết từ API)"

    # Tạo tin nhắn phản hồi cuối cùng
    if success:
        user_fl_cooldown[str(user_id_str)][target_username] = time.time(); save_data()
        logger.info(f"[BG Task /fl] Success for user {user_id_str} -> @{target_username}. Cooldown updated.")
        final_response_text = (
            f"✅ <b>Tăng Follow Thành Công!</b>\n✨ Cho: {invoking_user_mention}\n\n"
            f"{user_info_block if user_info_block else f'👤 <b>Tài khoản:</b> <code>@{html.escape(target_username)}</code>\n'}"
            f"{follower_info_block if follower_info_block else ''}"
        )
    else:
        logger.warning(f"[BG Task /fl] Failed for user {user_id_str} -> @{target_username}. API Message: {api_message}")
        final_response_text = (
            f"❌ <b>Tăng Follow Thất Bại!</b>\n👤 Cho: {invoking_user_mention}\n🎯 Target: <code>@{html.escape(target_username)}</code>\n\n"
            f"💬 Lý do API: <i>{html.escape(api_message or 'Không rõ')}</i>\n\n"
            f"{user_info_block if user_info_block else ''}"
        )
        # Thêm gợi ý nếu lỗi liên quan đến cooldown API
        if isinstance(api_message, str) and any(w in api_message.lower() for w in ["đợi", "wait", "phút", "giây", "minute", "second", "limit"]):
            final_response_text += f"\n\n<i>ℹ️ API báo lỗi hoặc yêu cầu chờ đợi. Vui lòng thử lại sau hoặc sử dụng <code>/treo {target_username}</code> nếu bạn là VIP.</i>"

    # Cập nhật tin nhắn chờ
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=processing_msg_id,
            text=final_response_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        logger.info(f"[BG Task /fl] Edited message {processing_msg_id} for user {user_id_str} -> @{target_username}")
    except Exception as e:
        logger.error(f"[BG Task /fl] Failed to edit processing msg {processing_msg_id}: {e}", exc_info=True)
        # Thử gửi tin nhắn mới nếu edit lỗi
        try:
            await context.bot.send_message(
                 chat_id=chat_id,
                 text=final_response_text,
                 parse_mode=ParseMode.HTML,
                 disable_web_page_preview=True
            )
        except Exception as e_send_new:
             logger.error(f"[BG Task /fl] Also failed to send new final /fl message for user {user_id_str}: {e_send_new}")


# --- /fl Command (Giữ nguyên, đã kiểm tra) ---
async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.effective_user: return
    user = update.effective_user
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()
    current_time = time.time()

    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để dùng lệnh này!\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code> | 👑 Hoặc: <code>/muatt</code>")
        await send_temporary_message(update, context, err_msg, duration=30, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    # Parse Arguments (Chỉ kiểm tra trống)
    args = context.args
    target_username = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Chưa nhập username TikTok.\n<b>Cú pháp:</b> <code>/fl username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@") # Xóa @ nếu có ở đầu
        if not uname:
            err_txt = "⚠️ Username không được trống."
        else:
            target_username = uname # Lấy username đã được làm sạch (@)

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return
    if not target_username: # Should not happen if err_txt is None
        await send_temporary_message(update, context, "⚠️ Lỗi xử lý username.", duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    # Check Cooldown cho target cụ thể
    user_cds = user_fl_cooldown.get(user_id_str, {})
    last_usage = user_cds.get(target_username)
    if last_usage:
         try:
            elapsed = current_time - float(last_usage)
            if elapsed < TIM_FL_COOLDOWN_SECONDS:
                rem_time = TIM_FL_COOLDOWN_SECONDS - elapsed
                cd_msg = f"⏳ {invoking_user_mention}, đợi <b>{rem_time:.0f} giây</b> nữa để dùng <code>/fl</code> cho <code>@{html.escape(target_username)}</code>."
                await send_temporary_message(update, context, cd_msg, duration=15, reply=True)
                if original_message_id: await delete_user_message(update, context, original_message_id)
                return
         except (ValueError, TypeError):
             logger.warning(f"Invalid cooldown timestamp for /fl user {user_id_str} target {target_username}. Resetting.")
             if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                 del user_fl_cooldown[user_id_str][target_username]; save_data()

    # Gửi tin nhắn chờ và chạy nền
    processing_msg = None
    try:
        if update.message:
            processing_msg = await update.message.reply_html(f"⏳ {invoking_user_mention}, đã nhận yêu cầu tăng follow cho <code>@{html.escape(target_username)}</code>. Đang xử lý...")
            if original_message_id: await delete_user_message(update, context, original_message_id)
        else: # Trường hợp gọi từ callback
             processing_msg = await context.bot.send_message(chat_id, f"⏳ {invoking_user_mention}, đã nhận yêu cầu tăng follow cho <code>@{html.escape(target_username)}</code>. Đang xử lý...", parse_mode=ParseMode.HTML)

        logger.info(f"Scheduling background task for /fl user {user_id} target @{target_username}")
        context.application.create_task(
            process_fl_request_background(
                context=context,
                chat_id=chat_id,
                user_id_str=user_id_str,
                target_username=target_username,
                processing_msg_id=processing_msg.message_id,
                invoking_user_mention=invoking_user_mention
            ),
            name=f"fl_bg_{user_id_str}_{target_username}_{int(time.time())}" # Thêm timestamp để tên task duy nhất hơn
        )
    except Exception as e:
         logger.error(f"Failed to send processing message or schedule task for /fl @{html.escape(target_username)}: {e}", exc_info=True)
         if original_message_id: await delete_user_message(update, context, original_message_id) # Thử xóa lại lệnh gốc
         if processing_msg:
            try: await context.bot.delete_message(chat_id, processing_msg.message_id) # Xóa tin nhắn chờ nếu lỗi
            except Exception: pass
         await send_temporary_message(update, context, f"❌ Lỗi khi bắt đầu xử lý yêu cầu /fl cho @{html.escape(target_username)}. Vui lòng thử lại.", duration=20, reply=False)


# --- Lệnh /getkey (Giữ nguyên, đã kiểm tra) ---
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
        save_data() # <<< LƯU NGAY KHI THÊM KEY >>>
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
                        save_data() # <<< LƯU COOLDOWN >>>
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
                save_data() # <<< LƯU SAU KHI XÓA KEY LỖI >>>
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


# --- Lệnh /nhapkey (Đã sửa lỗi SyntaxError và phản hồi) ---
async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update or not update.effective_user: return
    user = update.effective_user
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()
    current_time = time.time()

    # Parse Input
    args = context.args
    submitted_key = None
    err_txt = ""
    key_prefix = "Dinotool-"
    # Regex kiểm tra định dạng key chặt chẽ hơn
    key_format_regex = re.compile(r"^" + re.escape(key_prefix) + r"[A-Z0-9]{8}$") # Giả sử key luôn có 8 ký tự sau prefix

    if not args:
        err_txt = ("⚠️ Bạn chưa nhập key.\n<b>Cú pháp đúng:</b> <code>/nhapkey Dinotool-KEYCỦABẠN</code>")
    elif len(args) > 1:
        err_txt = f"⚠️ Bạn đã nhập quá nhiều từ. Chỉ nhập key thôi.\nVí dụ: <code>/nhapkey {generate_random_key()}</code>"
    else:
        key_input = args[0].strip()
        # Check prefix trước
        if not key_input.startswith(key_prefix):
             err_txt = (f"⚠️ Key <code>{html.escape(key_input)}</code> phải bắt đầu bằng <code>{key_prefix}</code>.")
        # Check định dạng đầy đủ (prefix + phần còn lại)
        elif not key_format_regex.match(key_input):
             err_txt = (f"⚠️ Key <code>{html.escape(key_input)}</code> sai định dạng.\nPhải là <code>{key_prefix}</code> theo sau bởi chữ IN HOA/số.")
        else:
            submitted_key = key_input

    # Xóa lệnh gốc trước khi xử lý
    if original_message_id: await delete_user_message(update, context, original_message_id)

    if err_txt:
        # Gửi lỗi mà không cần reply vì lệnh gốc đã xóa
        await send_temporary_message(update, context, err_txt, duration=20, reply=False)
        return

    # Validate Key Logic
    logger.info(f"User {user_id} attempting key activation with: '{submitted_key}'")
    key_data = valid_keys.get(submitted_key)
    final_response_text = ""

    if not key_data:
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' not found.")
        # <<< Phản hồi: Key không tồn tại >>>
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> không hợp lệ hoặc không tồn tại.\nDùng <code>/getkey</code> để lấy key mới."
    elif key_data.get("used_by") is not None:
        used_by_id = key_data["used_by"]
        activation_time_ts = key_data.get("activation_time")
        used_time_str = ""
        if activation_time_ts:
            try:
                # <<< Sửa lỗi SyntaxError + Format thời gian >>>
                used_dt = datetime.fromtimestamp(float(activation_time_ts))
                used_time_str = f" lúc {used_dt.strftime('%H:%M:%S %d/%m/%Y')}" # Sửa định dạng
            except (ValueError, TypeError, OSError) as e:
                logger.warning(f"Error formatting activation time for key {submitted_key}: {e}")
                used_time_str = " (lỗi thời gian)"
        # <<< Phản hồi: Key đã sử dụng >>>
        if str(used_by_id) == user_id_str:
             logger.info(f"Key validation: User {user_id} already used key '{submitted_key}'{used_time_str}.")
             final_response_text = f"⚠️ Bạn đã kích hoạt key <code>{html.escape(submitted_key)}</code> này rồi{used_time_str}."
        else:
             logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' already used by user {used_by_id}{used_time_str}.")
             # Lấy mention của người đã dùng nếu có thể
             used_by_mention = f"User ID <code>{used_by_id}</code>"
             try:
                 used_by_info = await context.bot.get_chat(int(used_by_id))
                 if used_by_info and used_by_info.mention_html(): used_by_mention = used_by_info.mention_html()
             except Exception: pass # Bỏ qua nếu không lấy được info
             final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã được {used_by_mention} sử dụng{used_time_str}."
    elif current_time > float(key_data.get("expiry_time", 0)):
        expiry_time_ts = key_data.get("expiry_time")
        expiry_time_str = ""
        if expiry_time_ts:
            try:
                # <<< Sửa lỗi SyntaxError + Format thời gian >>>
                expiry_dt = datetime.fromtimestamp(float(expiry_time_ts))
                expiry_time_str = f" vào lúc {expiry_dt.strftime('%H:%M:%S %d/%m/%Y')}" # Sửa định dạng
            except (ValueError, TypeError, OSError) as e:
                logger.warning(f"Error formatting expiry time for key {submitted_key}: {e}")
                expiry_time_str = " (lỗi thời gian)"
        logger.warning(f"Key validation failed for user {user_id}: Key '{submitted_key}' expired{expiry_time_str}.")
        # <<< Phản hồi: Key hết hạn >>>
        final_response_text = f"❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn nhập{expiry_time_str}.\nDùng <code>/getkey</code> để lấy key mới."
    else: # Key hợp lệ, chưa dùng, chưa hết hạn
        try:
            key_data["used_by"] = user_id
            key_data["activation_time"] = current_time
            activation_expiry_ts = current_time + ACTIVATION_DURATION_SECONDS
            activated_users[user_id_str] = activation_expiry_ts
            save_data() # <<< LƯU NGAY SAU KHI KÍCH HOẠT >>>

            expiry_dt = datetime.fromtimestamp(activation_expiry_ts)
            expiry_str = expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')
            act_hours = ACTIVATION_DURATION_SECONDS // 3600
            logger.info(f"Key '{submitted_key}' successfully activated by user {user_id}. Activation expires at {expiry_str}.")
            # <<< Phản hồi: Kích hoạt thành công >>>
            final_response_text = (f"✅ <b>Kích Hoạt Key Thành Công!</b>\n\n👤 Người dùng: {invoking_user_mention}\n🔑 Key: <code>{html.escape(submitted_key)}</code>\n\n"
                                   f"✨ Bạn có thể sử dụng <code>/tim</code>, <code>/fl</code>, <code>/check</code>, <code>/sound</code>.\n⏳ Hết hạn vào: <b>{expiry_str}</b> (sau {act_hours} giờ).")
        except Exception as e_activate:
             logger.error(f"Unexpected error during key activation process for user {user_id} key {submitted_key}: {e_activate}", exc_info=True)
             final_response_text = f"❌ Lỗi hệ thống khi kích hoạt key <code>{html.escape(submitted_key)}</code>. Báo Admin."
             # Cố gắng rollback trạng thái nếu lỗi
             if submitted_key in valid_keys and valid_keys[submitted_key].get("used_by") == user_id:
                 valid_keys[submitted_key]["used_by"] = None
                 valid_keys[submitted_key]["activation_time"] = None
             if user_id_str in activated_users:
                 del activated_users[user_id_str]
             try: save_data() # Lưu lại trạng thái rollback
             except Exception as e_save_rb: logger.error(f"Failed to save data after rollback attempt for key {submitted_key}: {e_save_rb}")

    # Gửi phản hồi cuối cùng
    try:
        # Gửi không reply vì lệnh gốc đã xóa
        await context.bot.send_message(chat_id=chat_id, text=final_response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed to send final /nhapkey response to user {user_id}: {e}", exc_info=True)


# --- Lệnh /muatt (Giữ nguyên, đã kiểm tra) ---
async def muatt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị thông tin mua VIP, QR code và nút yêu cầu gửi bill."""
    if not update or not update.effective_user: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id if update.message else None # Lưu lại để xóa nếu là message
    user_id = user.id
    invoking_user_mention = user.mention_html()
    payment_note = f"{PAYMENT_NOTE_PREFIX} {user_id}"

    text_lines = ["👑 <b>Thông Tin Nâng Cấp VIP - DinoTool</b> 👑",
                  f"\nChào {invoking_user_mention}, nâng cấp VIP để mở khóa <code>/treo</code>, không cần lấy key và nhiều ưu đãi!",
                  "\n💎 <b>Các Gói VIP Hiện Có:</b>"]
    if VIP_PRICES:
        for days_key, info in VIP_PRICES.items():
            text_lines.extend([f"\n⭐️ <b>Gói {info['duration_days']} Ngày:</b>",
                               f"   - 💰 Giá: <b>{info['price']}</b>",
                               f"   - ⏳ Thời hạn: {info['duration_days']} ngày",
                               f"   - 🚀 Treo tối đa: <b>{info['limit']} tài khoản</b> TikTok"])
    else:
        text_lines.append("\n   <i>(Chưa có gói VIP nào được cấu hình)</i>")

    text_lines.extend(["\n🏦 <b>Thông tin thanh toán:</b>",
                       f"   - Ngân hàng: <b>{html.escape(BANK_NAME)}</b>",
                       # Thêm nút copy cho STK
                       f"   - STK: <a href=\"https://t.me/share/url?url={html.escape(BANK_ACCOUNT)}\" target=\"_blank\"><code>{html.escape(BANK_ACCOUNT)}</code></a> (👈 Click để copy)",
                       f"   - Tên chủ TK: <b>{html.escape(ACCOUNT_NAME)}</b>",
                       "\n📝 <b>Nội dung chuyển khoản (Quan trọng!):</b>",
                       f"   » Chuyển khoản với nội dung <b>CHÍNH XÁC</b> là:",
                       # Thêm nút copy cho nội dung CK
                       f"   » <a href=\"https://t.me/share/url?url={html.escape(payment_note)}\" target=\"_blank\"><code>{html.escape(payment_note)}</code></a> (👈 Click để copy)",
                       f"   <i>(Sai nội dung có thể khiến giao dịch xử lý chậm)</i>",
                       "\n📸 <b>Sau Khi Chuyển Khoản Thành Công:</b>",
                       f"   1️⃣ Chụp ảnh màn hình biên lai (bill) giao dịch.",
                       f"   2️⃣ Nhấn nút '<b>📸 Gửi Bill Thanh Toán</b>' bên dưới.",
                       f"   3️⃣ Bot sẽ yêu cầu bạn gửi ảnh bill <b><u>VÀO CUỘC TRÒ CHUYỆN NÀY</u></b>.",
                       f"   4️⃣ Gửi ảnh bill của bạn vào đây.",
                       # <<< Sửa lỗi mô tả nơi nhận bill >>>
                       f"   5️⃣ Bot sẽ tự động chuyển tiếp ảnh đến Admin (ID: <code>{BILL_FORWARD_TARGET_ID}</code>).",
                       f"   6️⃣ Admin sẽ kiểm tra và kích hoạt VIP sớm nhất.",
                       "\n<i>Cảm ơn bạn đã quan tâm và ủng hộ DinoTool!</i> ❤️"])
    caption_text = "\n".join(text_lines)

    # Tạo nút bấm gọi callback prompt_send_bill
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📸 Gửi Bill Thanh Toán", callback_data=f"prompt_send_bill_{user_id}")]
    ])

    # Xóa lệnh /muatt gốc (chỉ xóa nếu nó đến từ message)
    if original_message_id and update.message and original_message_id == update.message.message_id:
         try: await delete_user_message(update, context, original_message_id)
         except Exception as e_del: logger.debug(f"Could not delete original /muatt message: {e_del}")

    # Ưu tiên gửi ảnh QR và caption
    photo_sent = False
    if QR_CODE_URL and QR_CODE_URL.startswith("http"):
        try:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=QR_CODE_URL,
                caption=caption_text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
            logger.info(f"Sent /muatt info with QR photo and prompt button to user {user_id} in chat {chat_id}")
            photo_sent = True
        except (BadRequest, Forbidden, TelegramError) as e:
            logger.warning(f"Error sending /muatt photo+caption to chat {chat_id}: {e}. Falling back to text.")
            # Log thêm chi tiết lỗi BadRequest
            if isinstance(e, BadRequest): logger.warning(f"BadRequest details: {e.message}")
        except Exception as e_unexp_photo:
            logger.error(f"Unexpected error sending /muatt photo+caption to chat {chat_id}: {e_unexp_photo}", exc_info=True)

    # Nếu gửi ảnh lỗi hoặc không có QR_CODE_URL, gửi text
    if not photo_sent:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=caption_text, # Gửi toàn bộ nội dung dưới dạng text
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=keyboard # Vẫn gửi nút bấm
            )
            logger.info(f"Sent /muatt fallback text info with prompt button to user {user_id} in chat {chat_id}")
        except Exception as e_text:
             logger.error(f"Error sending fallback text for /muatt to chat {chat_id}: {e_text}", exc_info=True)
             # Thông báo lỗi cho người dùng nếu cả 2 cách đều thất bại
             try: await context.bot.send_message(chat_id, "❌ Đã có lỗi khi hiển thị thông tin thanh toán. Vui lòng liên hệ Admin.")
             except Exception: pass

# --- Callback Handler cho nút "Gửi Bill Thanh Toán" (Giữ nguyên) ---
async def prompt_send_bill_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.message: return
    user = query.from_user
    chat_id = query.message.chat_id
    callback_data = query.data
    invoking_user_mention = user.mention_html()
    logger.info(f"Callback 'prompt_send_bill' triggered by user {user.id} in chat {chat_id}")

    expected_user_id = None
    try:
        # Lấy user_id từ callback_data
        if callback_data.startswith("prompt_send_bill_"):
            expected_user_id = int(callback_data.split("_")[-1])
    except (ValueError, IndexError, TypeError):
        logger.warning(f"Invalid callback_data format: {callback_data} from user {user.id}")
        try: await query.answer("Lỗi: Dữ liệu nút không hợp lệ.", show_alert=True)
        except Exception: pass
        return

    # Chỉ người bấm nút gốc mới được phản hồi
    if user.id != expected_user_id:
        try: await query.answer("Bạn không phải người yêu cầu thanh toán.", show_alert=True)
        except Exception: pass
        logger.info(f"User {user.id} tried to click bill prompt button for user {expected_user_id} in chat {chat_id}")
        return

    # Kiểm tra xem user đã trong danh sách chờ chưa (tránh spam)
    if user.id in pending_bill_user_ids:
        try: await query.answer("Bạn đã yêu cầu gửi bill rồi. Vui lòng gửi ảnh vào chat.", show_alert=True)
        except Exception: pass
        logger.info(f"User {user.id} clicked 'prompt_send_bill' again while already pending.")
        return

    # Thêm user vào danh sách chờ và đặt timeout
    pending_bill_user_ids.add(user.id)
    if context.job_queue:
        job_name = f"remove_pending_bill_{user.id}"
        # Xóa job cũ nếu có (phòng trường hợp hy hữu)
        jobs = context.job_queue.get_jobs_by_name(job_name)
        for job in jobs: job.schedule_removal(); logger.debug(f"Removed previous pending bill timeout job for user {user.id}")
        # Tạo job mới
        context.job_queue.run_once(
            remove_pending_bill_user_job,
            PENDING_BILL_TIMEOUT_SECONDS,
            data={'user_id': user.id, 'chat_id': chat_id}, # Truyền cả chat_id nếu muốn gửi thông báo timeout
            name=job_name
        )
        logger.info(f"User {user.id} clicked 'prompt_send_bill'. Added to pending list. Timeout job '{job_name}' scheduled for {PENDING_BILL_TIMEOUT_SECONDS}s.")

    try: await query.answer() # Xác nhận đã nhận callback
    except Exception: pass # Bỏ qua nếu trả lời lỗi

    prompt_text = f"📸 {invoking_user_mention}, vui lòng gửi ảnh chụp màn hình biên lai thanh toán của bạn <b><u>vào cuộc trò chuyện này</u></b> ngay bây giờ.\n\n<i>(Yêu cầu này sẽ hết hạn sau {PENDING_BILL_TIMEOUT_SECONDS // 60} phút nếu bạn không gửi ảnh.)</i>"
    try:
        # Gửi tin nhắn yêu cầu bill ngay dưới tin nhắn /muatt
        # Không quote để tránh làm dài tin nhắn
        await query.message.reply_html(text=prompt_text, quote=False)
        # Không xóa tin nhắn /muatt để user còn thấy thông tin
    except Exception as e:
        logger.error(f"Error sending bill prompt message to {user.id} in chat {chat_id}: {e}", exc_info=True)
        # Nếu gửi reply lỗi, thử gửi tin mới
        try:
            await context.bot.send_message(chat_id=chat_id, text=prompt_text, parse_mode=ParseMode.HTML)
        except Exception as e2:
             logger.error(f"Also failed to send bill prompt as new message to {user.id} in chat {chat_id}: {e2}")

# --- Job xóa user khỏi danh sách chờ bill (Giữ nguyên) ---
async def remove_pending_bill_user_job(context: ContextTypes.DEFAULT_TYPE):
    """Job để xóa user khỏi danh sách chờ nhận bill nếu timeout."""
    job_data = context.job.data if context.job else {}
    user_id = job_data.get('user_id')
    chat_id = job_data.get('chat_id') # Lấy chat_id từ data
    job_name = context.job.name if context.job else "unknown_pending_remove"

    if user_id in pending_bill_user_ids:
        pending_bill_user_ids.remove(user_id)
        logger.info(f"Job '{job_name}': Removed user {user_id} from pending bill list due to timeout.")
        # (Optional) Gửi thông báo timeout cho user
        if chat_id:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"⏳ Yêu cầu gửi bill của bạn đã hết hạn. Nếu bạn đã thanh toán, vui lòng nhấn nút 'Gửi Bill' lại trong <code>/muatt</code> hoặc liên hệ Admin.",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e_send:
                logger.warning(f"Job '{job_name}': Failed to send timeout notification to user {user_id} in chat {chat_id}: {e_send}")
    else:
        logger.debug(f"Job '{job_name}': User {user_id} not found in pending bill list (already sent or removed).")

# --- Xử lý nhận ảnh bill (Giữ nguyên, đã kiểm tra) ---
async def handle_photo_bill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xử lý ảnh/document ảnh VÀ chỉ chuyển tiếp nếu user nằm trong danh sách chờ."""
    if not update or not update.message or (update.message.text and update.message.text.startswith('/')):
        return # Bỏ qua command và text messages
    user = update.effective_user
    chat = update.effective_chat
    message = update.message
    if not user or not chat or not message: return

    # Chỉ xử lý nếu user đang trong danh sách chờ
    if user.id not in pending_bill_user_ids:
        # logger.debug(f"Ignoring photo from user {user.id} - not in pending_bill_user_ids")
        return # Bỏ qua nếu user không trong danh sách chờ

    # Kiểm tra xem có phải là ảnh hoặc document ảnh không
    is_photo = bool(message.photo)
    is_image_document = bool(message.document and message.document.mime_type and message.document.mime_type.startswith('image/'))

    if not is_photo and not is_image_document:
        # logger.debug(f"Ignoring non-image message from pending user {user.id}")
        return # Bỏ qua nếu không phải ảnh

    logger.info(f"Bill photo/document received from PENDING user {user.id} ({user.username}) in chat {chat.id} (Type: {chat.type}). Forwarding to {BILL_FORWARD_TARGET_ID}.")

    # --- Quan trọng: Xử lý ngay lập tức ---
    # 1. Xóa user khỏi danh sách chờ
    pending_bill_user_ids.discard(user.id)
    # 2. Hủy job timeout
    if context.job_queue:
         job_name = f"remove_pending_bill_{user.id}"
         jobs = context.job_queue.get_jobs_by_name(job_name)
         cancelled_jobs = 0
         for job in jobs:
             job.schedule_removal()
             cancelled_jobs += 1
         if cancelled_jobs > 0:
             logger.debug(f"Removed {cancelled_jobs} pending bill timeout job(s) '{job_name}' for user {user.id} after receiving bill.")
         elif not jobs:
             logger.debug(f"No active pending bill timeout job found for user {user.id} to remove.")
    # --- Kết thúc xử lý tức thì ---

    # Chuẩn bị caption cho tin nhắn chuyển tiếp
    forward_caption_lines = [f"📄 <b>Bill Nhận Được Từ User</b>",
                             f"👤 <b>User:</b> {user.mention_html()} (<code>{user.id}</code>)"]
    if chat.type == 'private': forward_caption_lines.append(f"💬 <b>Chat gốc:</b> PM với Bot")
    elif chat.title: forward_caption_lines.append(f"👥 <b>Chat gốc:</b> {html.escape(chat.title)} (<code>{chat.id}</code>)")
    else: forward_caption_lines.append(f"❓ <b>Chat gốc:</b> ID <code>{chat.id}</code>")
    # Lấy link tin nhắn gốc (có thể thất bại nếu bot không có quyền)
    try:
        # message.link chỉ hoạt động ở public group/channel
        # Tạo link thủ công nếu là private/group
        if chat.username: # Public group/channel
             message_link = f"https://t.me/{chat.username}/{message.message_id}"
        elif chat.type != 'private': # Private group
             # Không có cách lấy link trực tiếp đáng tin cậy cho private group
             message_link = None
        else: # Private chat
             message_link = None # Không có link cho PM

        if message_link: forward_caption_lines.append(f"🔗 <a href='{message_link}'>Link Tin Nhắn Gốc</a>")
        else: forward_caption_lines.append(f"🔗 Tin nhắn ID: <code>{message.message_id}</code> (trong chat gốc)")
    except AttributeError:
        logger.debug(f"Could not get message link/id attributes for message from user {user.id}")
        forward_caption_lines.append("🔗 Không thể lấy link/ID tin nhắn gốc.")

    original_caption = message.caption
    if original_caption:
        # Giới hạn độ dài caption gốc để tránh quá dài
        truncated_caption = original_caption[:500] + ('...' if len(original_caption) > 500 else '')
        forward_caption_lines.append(f"\n📝 <b>Caption gốc:</b>\n{html.escape(truncated_caption)}")

    forward_caption_text = "\n".join(forward_caption_lines)

    # Thực hiện chuyển tiếp và gửi thông tin
    forward_success = False
    try:
        # Chuyển tiếp tin nhắn chứa ảnh/bill gốc
        await context.bot.forward_message(chat_id=BILL_FORWARD_TARGET_ID, from_chat_id=chat.id, message_id=message.message_id)
        # Gửi tin nhắn thông tin bổ sung (người gửi, chat gốc) ngay sau đó
        await context.bot.send_message(
            chat_id=BILL_FORWARD_TARGET_ID,
            text=forward_caption_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        forward_success = True
        logger.info(f"Successfully forwarded bill message {message.message_id} from user {user.id} and sent info to {BILL_FORWARD_TARGET_ID}.")

    except Forbidden as e_forbidden:
        logger.error(f"Bot cannot forward/send message to BILL_FORWARD_TARGET_ID ({BILL_FORWARD_TARGET_ID}). Check permissions/block status. Error: {e_forbidden}")
        # Thông báo cho Admin nếu target khác Admin
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
            try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Lý do: Bot bị chặn hoặc thiếu quyền.\nLỗi: {e_forbidden}")
            except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
    except BadRequest as e_bad_req:
        logger.error(f"BadRequest forwarding/sending bill message {message.message_id} to {BILL_FORWARD_TARGET_ID}: {e_bad_req}")
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
             try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi BadRequest khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Lỗi: {e_bad_req}")
             except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
    except TelegramError as e_fwd: # Các lỗi Telegram khác
         logger.error(f"Telegram error forwarding/sending bill message {message.message_id} to {BILL_FORWARD_TARGET_ID}: {e_fwd}")
         if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
              try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi Telegram khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Lỗi: {e_fwd}")
              except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")
    except Exception as e: # Lỗi không xác định
        logger.error(f"Unexpected error forwarding/sending bill to {BILL_FORWARD_TARGET_ID}: {e}", exc_info=True)
        if ADMIN_USER_ID != BILL_FORWARD_TARGET_ID:
             try: await context.bot.send_message(ADMIN_USER_ID, f"⚠️ Lỗi không xác định khi chuyển tiếp bill từ user {user.id} (chat {chat.id}) đến target {BILL_FORWARD_TARGET_ID}. Chi tiết log.")
             except Exception as e_admin: logger.error(f"Failed to send bill forwarding error notification to ADMIN {ADMIN_USER_ID}: {e_admin}")

    # Gửi phản hồi cho người dùng
    try:
        if forward_success:
            await message.reply_html("✅ Đã nhận và chuyển tiếp bill của bạn đến Admin để xử lý. Vui lòng chờ nhé!")
        else:
            # Thông báo lỗi cho người dùng nếu không gửi được cho admin
            await message.reply_html(f"❌ Đã xảy ra lỗi khi gửi bill của bạn. Vui lòng liên hệ trực tiếp Admin <a href='tg://user?id={ADMIN_USER_ID}'>tại đây</a> và gửi bill thủ công.")
    except Exception as e_reply:
        logger.warning(f"Failed to send confirmation/error reply to user {user.id} after handling bill: {e_reply}")

    # Dừng xử lý handler để tránh các handler khác (ví dụ: handler tin nhắn chung) nhận ảnh này
    raise ApplicationHandlerStop


# --- Lệnh /addtt (Đã sửa để chấp nhận số ngày tùy ý và lưu data) ---
async def addtt_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cấp VIP cho người dùng (chỉ Admin). Chấp nhận số ngày tùy ý."""
    if not update or not update.effective_user: return
    admin_user = update.effective_user
    chat = update.effective_chat
    if not admin_user or not chat or admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /addtt attempt by {admin_user.id if admin_user else 'Unknown'}")
        return # Không phản hồi gì để tránh lộ lệnh admin

    args = context.args
    err_txt = None
    target_user_id = None
    duration_days_input = None
    limit = DEFAULT_VIP_LIMIT # Sử dụng limit mặc định (cao nhất từ config)

    # <<< Sửa cú pháp: /addtt <user_id> <số_ngày> >>>
    if len(args) != 2:
        err_txt = (f"⚠️ Sai cú pháp.\n<b>Dùng:</b> <code>/addtt &lt;user_id&gt; &lt;số_ngày&gt;</code>\n"
                   f"<b>Ví dụ:</b> <code>/addtt 123456789 30</code> (Thêm 30 ngày VIP)\n"
                   f"<i>(Giới hạn treo sẽ mặc định là: {limit} users)</i>")
    else:
        try:
            target_user_id = int(args[0])
            if target_user_id <= 0: raise ValueError("User ID must be positive")
        except ValueError:
            err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ (phải là số nguyên dương)."

        if not err_txt:
            try:
                duration_days_input = int(args[1])
                if duration_days_input <= 0: raise ValueError("Days must be positive")
                # Không cần check gói nữa, chấp nhận số ngày bất kỳ
            except ValueError:
                err_txt = f"⚠️ Số ngày '<code>{html.escape(args[1])}</code>' không hợp lệ (phải là số nguyên dương)."

    if err_txt:
        try: await update.message.reply_html(err_txt)
        except Exception as e_reply: logger.error(f"Failed to send error reply to admin {admin_user.id}: {e_reply}")
        return

    target_user_id_str = str(target_user_id)
    current_time = time.time()
    current_vip_data = vip_users.get(target_user_id_str)
    start_time = current_time
    operation_type = "Nâng cấp lên"
    previous_expiry_str = ""

    if current_vip_data and isinstance(current_vip_data, dict):
         try:
             current_expiry = float(current_vip_data.get("expiry", 0))
             if current_expiry > current_time:
                 start_time = current_expiry # Gia hạn từ ngày hết hạn cũ
                 operation_type = "Gia hạn thêm"
                 # Lấy thông tin hạn cũ để hiển thị
                 try: previous_expiry_str = f" (Hạn cũ: {datetime.fromtimestamp(current_expiry).strftime('%d/%m/%Y %H:%M')})"
                 except Exception: pass
                 logger.info(f"Admin {admin_user.id}: Extending VIP for {target_user_id_str} from {datetime.fromtimestamp(start_time).isoformat()}.")
             else:
                 logger.info(f"Admin {admin_user.id}: User {target_user_id_str} was VIP but expired. Activating new.")
         except (ValueError, TypeError):
             logger.warning(f"Admin {admin_user.id}: Invalid expiry data for user {target_user_id_str}. Activating new.")

    # Tính hạn mới
    new_expiry_ts = start_time + duration_days_input * 86400 # 86400 giây = 1 ngày
    new_expiry_dt = datetime.fromtimestamp(new_expiry_ts)
    new_expiry_str = new_expiry_dt.strftime('%H:%M:%S ngày %d/%m/%Y')

    # Cập nhật dữ liệu VIP
    vip_users[target_user_id_str] = {"expiry": new_expiry_ts, "limit": limit}
    save_data() # <<< LƯU NGAY SAU KHI CẬP NHẬT VIP >>>
    logger.info(f"Admin {admin_user.id} processed VIP for {target_user_id_str}: {operation_type} {duration_days_input} days. New expiry: {new_expiry_str}, Limit: {limit}")

    # Thông báo cho Admin
    admin_msg = (f"✅ Đã <b>{operation_type} {duration_days_input} ngày VIP</b> thành công!\n\n"
                 f"👤 User ID: <code>{target_user_id}</code>\n✨ Số ngày: {duration_days_input}\n"
                 f"⏳ Hạn mới: <b>{new_expiry_str}</b>{previous_expiry_str}\n🚀 Limit: <b>{limit} users</b>")
    try: await update.message.reply_html(admin_msg)
    except Exception as e: logger.error(f"Failed to send confirmation to admin {admin_user.id}: {e}")

    # Thông báo cho người dùng
    user_mention = f"User ID <code>{target_user_id}</code>"
    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        if target_user_info and target_user_info.mention_html():
             user_mention = target_user_info.mention_html()
        elif target_user_info and target_user_info.username:
             user_mention = f"@{target_user_info.username}"
        # Nếu không có mention/username, giữ nguyên ID
    except Exception as e_get_chat:
        logger.warning(f"Could not get chat info for {target_user_id}: {e_get_chat}.")

    user_notify_msg = (f"🎉 Chúc mừng {user_mention}! 🎉\n\nBạn đã được Admin <b>{operation_type} {duration_days_input} ngày VIP</b>!\n\n"
                       f"✨ Thời hạn VIP: <b>{duration_days_input} ngày</b>\n⏳ Hạn đến: <b>{new_expiry_str}</b>\n🚀 Limit treo: <b>{limit} tài khoản</b>\n\n"
                       f"Cảm ơn bạn đã ủng hộ DinoTool! ❤️\n(Dùng <code>/menu</code> hoặc <code>/lenh</code> để xem lại)")
    try:
        await context.bot.send_message(chat_id=target_user_id, text=user_notify_msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"Sent VIP notification for user {target_user_id} to their PM.")
    except (Forbidden, BadRequest) as e_pm:
        logger.warning(f"Failed to send VIP notification to user {target_user_id}'s PM ({e_pm}). Trying group {ALLOWED_GROUP_ID}.")
        # Thử gửi vào nhóm chính nếu PM lỗi và nhóm được cấu hình
        if ALLOWED_GROUP_ID:
            group_notify_msg = user_notify_msg + f"\n\n<i>(Gửi vào nhóm do không thể gửi PM cho {user_mention})</i>"
            try:
                await context.bot.send_message(chat_id=ALLOWED_GROUP_ID, text=group_notify_msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                logger.info(f"Sent VIP notification for user {target_user_id} to group {ALLOWED_GROUP_ID} as fallback.")
            except Exception as e_group:
                logger.error(f"Also failed to send VIP notification for user {target_user_id} to group {ALLOWED_GROUP_ID}: {e_group}")
                # Thông báo lỗi cuối cùng cho Admin
                if admin_user.id != target_user_id:
                     try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} (PM lỗi: {e_pm}, Group lỗi: {e_group})")
                     except Exception: pass
        elif admin_user.id != target_user_id:
             # Thông báo lỗi cho Admin nếu không có nhóm fallback
             try: await context.bot.send_message(admin_user.id, f"⚠️ Không thể gửi thông báo VIP cho user {target_user_id} (PM lỗi: {e_pm}, không có group fallback)")
             except Exception: pass
    except Exception as e_send_notify:
        logger.error(f"Unexpected error sending VIP notification for user {target_user_id}: {e_send_notify}", exc_info=True)
        if admin_user.id != target_user_id:
            try: await context.bot.send_message(admin_user.id, f"⚠️ Lỗi không xác định khi gửi thông báo VIP cho user {target_user_id}. Lỗi: {e_send_notify}")
            except Exception: pass

# --- Lệnh /xoavip (Mới) ---
async def xoavip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Xóa VIP của người dùng (chỉ Admin)."""
    if not update or not update.effective_user: return
    admin_user = update.effective_user
    chat = update.effective_chat
    if not admin_user or not chat or admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /xoavip attempt by {admin_user.id if admin_user else 'Unknown'}")
        return

    args = context.args
    err_txt = None
    target_user_id = None

    if len(args) != 1:
        err_txt = ("⚠️ Sai cú pháp.\n<b>Dùng:</b> <code>/xoavip &lt;user_id&gt;</code>\n"
                   "<b>Ví dụ:</b> <code>/xoavip 123456789</code>")
    else:
        try:
            target_user_id = int(args[0])
            if target_user_id <= 0: raise ValueError("User ID must be positive")
        except ValueError:
            err_txt = f"⚠️ User ID '<code>{html.escape(args[0])}</code>' không hợp lệ (phải là số nguyên dương)."

    if err_txt:
        try: await update.message.reply_html(err_txt)
        except Exception as e_reply: logger.error(f"Failed to send /xoavip error reply to admin {admin_user.id}: {e_reply}")
        return

    target_user_id_str = str(target_user_id)
    logger.info(f"Admin {admin_user.id} attempting to remove VIP status from user {target_user_id_str}.")

    # 1. Kiểm tra xem user có đang là VIP không
    if target_user_id_str not in vip_users:
        admin_msg = f"ℹ️ User ID <code>{target_user_id}</code> hiện không có trong danh sách VIP."
        try: await update.message.reply_html(admin_msg)
        except Exception as e: logger.error(f"Failed to send /xoavip not found reply to admin: {e}")
        return

    # 2. Xóa khỏi vip_users
    del vip_users[target_user_id_str]
    logger.info(f"Removed user {target_user_id_str} from vip_users list.")

    # 3. Dừng tất cả task treo của user đó (runtime và persistent)
    logger.info(f"Stopping all treo tasks for user {target_user_id_str} due to /xoavip.")
    stopped_count = await stop_all_treo_tasks_for_user(target_user_id_str, context, reason=f"Admin command /xoavip by {admin_user.id}")
    logger.info(f"Stopped {stopped_count} treo tasks/configs for user {target_user_id_str}.")

    # 4. Lưu dữ liệu
    save_data() # Lưu lại sau khi xóa VIP và dừng treo
    logger.info(f"Saved data after removing VIP and stopping tasks for {target_user_id_str}.")

    # 5. Thông báo cho Admin
    admin_msg = (f"✅ Đã xóa VIP thành công cho User ID <code>{target_user_id}</code>.\n"
                 f"(Đã dừng và xóa {stopped_count} cấu hình treo nếu có)")
    try: await update.message.reply_html(admin_msg)
    except Exception as e: logger.error(f"Failed to send /xoavip confirmation to admin: {e}")

    # 6. Thông báo cho người dùng (nếu có thể)
    user_mention = f"User ID <code>{target_user_id}</code>"
    try:
        target_user_info = await context.bot.get_chat(target_user_id)
        if target_user_info and target_user_info.mention_html(): user_mention = target_user_info.mention_html()
        elif target_user_info and target_user_info.username: user_mention = f"@{target_user_info.username}"
    except Exception: pass

    user_notify_msg = (f"ℹ️ Thông báo: Trạng thái VIP của bạn ({user_mention}) đã được Admin gỡ bỏ.\n"
                       f"Tất cả các tác vụ treo tự động (nếu có) đã được dừng lại.")
    try:
        await context.bot.send_message(chat_id=target_user_id, text=user_notify_msg, parse_mode=ParseMode.HTML)
        logger.info(f"Sent VIP removal notification to user {target_user_id}.")
    except Exception as e_pm:
        logger.warning(f"Failed to send VIP removal notification to user {target_user_id}'s PM: {e_pm}")
        # Không thử gửi vào nhóm vì đây là thông báo gỡ bỏ


# --- Logic Treo (Đã cập nhật để kiểm tra persistent và VIP trong loop) ---
async def run_treo_loop(user_id_str: str, target_username: str, context: ContextTypes.DEFAULT_TYPE, initial_chat_id: int):
    """Vòng lặp chạy nền cho lệnh /treo, kiểm tra config/VIP, gửi thông tin, ghi gain."""
    global user_daily_gains, treo_stats, persistent_treo_configs # Thêm persistent_treo_configs vào global
    user_id_int = int(user_id_str)
    # Tạo tên task duy nhất và dễ nhận biết hơn
    task_name = f"treo_{user_id_str}_{target_username}"
    logger.info(f"[Treo Task Start/Resume] Task '{task_name}' started for chat {initial_chat_id}.")

    # Lấy mention user một lần khi bắt đầu task
    invoking_user_mention = f"User ID <code>{user_id_str}</code>" # Default
    app_for_mention = context.application if context and hasattr(context, 'application') else None
    if app_for_mention:
        try:
            user_info = await app_for_mention.bot.get_chat(user_id_int)
            if user_info:
                mention = user_info.mention_html() or (f"@{user_info.username}" if user_info.username else None)
                if mention: invoking_user_mention = mention
        except Exception as e_get_mention:
            logger.debug(f"Could not get mention for user {user_id_str} in task {task_name}: {e_get_mention}")
    else: logger.warning(f"[Treo Task Start] Cannot get application context for user mention in task '{task_name}'.")


    last_api_call_time = 0
    consecutive_failures = 0
    MAX_CONSECUTIVE_FAILURES = 30 # Giảm số lần thử lại liên tục
    initial_info_sent = False # Flag để chỉ gửi info chi tiết lần đầu
    current_chat_id = initial_chat_id # Chat ID để gửi thông báo, có thể cập nhật nếu config thay đổi

    try:
        while True:
            # --- Kiểm tra điều kiện trước khi chạy ---
            # 0. Lấy Application context mới nhất (quan trọng vì context truyền vào có thể cũ)
            app = context.application if context and hasattr(context, 'application') else None
            if not app or not app.bot:
                 logger.error(f"[Treo Task Stop] Cannot get application/bot context. Stopping task '{task_name}'.")
                 await stop_treo_task(user_id_str, target_username, context, reason="Cannot get app context in loop")
                 break

            # <<< 1. Kiểm tra Config Persistent và Chat ID hiện tại >>>
            persistent_user_config = persistent_treo_configs.get(user_id_str, {})
            saved_chat_id = persistent_user_config.get(target_username)

            if saved_chat_id is None:
                 logger.warning(f"[Treo Task Stop] Persistent config missing for task '{task_name}'. Stopping.")
                 # Không cần gọi stop_treo_task vì config đã bị xóa bởi nơi khác
                 break # Thoát loop
            elif saved_chat_id != current_chat_id:
                 logger.info(f"[Treo Task Update] Chat ID for task '{task_name}' updated from {current_chat_id} to {saved_chat_id}.")
                 current_chat_id = saved_chat_id # Cập nhật chat_id để gửi thông báo đúng nơi

            # <<< 2. Kiểm tra VIP Status >>>
            if not is_user_vip(user_id_int):
                logger.warning(f"[Treo Task Stop] User {user_id_str} no longer VIP. Stopping task '{task_name}'.")
                # <<< Gọi hàm dừng để xóa cả runtime và persistent >>>
                await stop_treo_task(user_id_str, target_username, context, reason="VIP Expired in loop")
                try:
                    await app.bot.send_message(
                        current_chat_id,
                        f"ℹ️ {invoking_user_mention}, việc treo cho <code>@{html.escape(target_username)}</code> đã dừng do VIP hết hạn.",
                        parse_mode=ParseMode.HTML,
                        disable_notification=True
                    )
                except Exception as e_send_stop: logger.warning(f"Failed to send VIP expiry stop message for task {task_name}: {e_send_stop}")
                break # Thoát loop

            # 3. Tính toán thời gian chờ
            current_time = time.time()
            wait_needed = TREO_INTERVAL_SECONDS - (current_time - last_api_call_time)
            if wait_needed > 0:
                logger.debug(f"[Treo Task Wait] Task '{task_name}' waiting for {wait_needed:.1f}s.")
                await asyncio.sleep(wait_needed)

            # --- Thực hiện tác vụ ---
            current_call_time = time.time()
            last_api_call_time = current_call_time # Cập nhật thời gian NGAY TRƯỚC KHI gọi API

            logger.info(f"[Treo Task Run] Task '{task_name}' executing follow for @{target_username}")
            api_result = await call_follow_api(user_id_str, target_username, app.bot.token)
            success = api_result["success"]
            api_message = api_result["message"] or "Không có thông báo từ API."
            api_data = api_result.get("data", {}) if isinstance(api_result.get("data"), dict) else {}
            gain = 0

            # --- Xử lý kết quả API ---
            if success:
                consecutive_failures = 0 # Reset đếm lỗi
                # Parse gain
                try:
                    gain_str = str(api_data.get("followers_add", "0"))
                    gain_match = re.search(r'([\+\-]?\d{1,3}(?:,\d{3})*|\d+)', gain_str)
                    if gain_match:
                         gain_cleaned = gain_match.group(0).replace(',', '')
                         gain = int(gain_cleaned)
                    else: gain = 0
                except (ValueError, TypeError, KeyError, AttributeError) as e_gain:
                     logger.warning(f"[Treo Task Stats] Task '{task_name}' error parsing gain: {e_gain}. Data: {api_data}")
                     gain = 0

                # Ghi nhận gain nếu > 0
                if gain > 0:
                    treo_stats[user_id_str][target_username] += gain
                    user_daily_gains[user_id_str][target_username].append((current_call_time, gain))
                    # Không cần save_data() ở đây liên tục, job cleanup/shutdown sẽ lưu
                    logger.info(f"[Treo Task Stats] Task '{task_name}' added {gain} followers. Recorded.")
                else:
                    logger.info(f"[Treo Task Success] Task '{task_name}' successful, reported gain={gain}. API Msg: {api_message[:100]}...")

                # --- Gửi thông báo thành công ---
                if not initial_info_sent: # Gửi thông tin chi tiết lần đầu
                    try:
                        # ... (logic trích xuất thông tin như cũ) ...
                        f_before_raw = api_data.get("followers_before", "?")
                        f_after_raw = api_data.get("followers_after", "?")
                        tt_username_api = html.escape(api_data.get("username", target_username))
                        name_api = html.escape(str(api_data.get("name", "?")))
                        userid_api = html.escape(str(api_data.get("user_id", "?")))
                        khu_vuc_api = html.escape(str(api_data.get("khu_vuc", "?")))
                        avatar_api = api_data.get("avatar", "")

                        def format_num(raw_val):
                            if raw_val == "?": return "?", None
                            try:
                                clean_str = re.sub(r'[^\d-]', '', str(raw_val))
                                num = int(clean_str)
                                return f"{num:,}", num
                            except: return html.escape(str(raw_val)), None

                        f_before_display, _ = format_num(f_before_raw)
                        f_after_display, _ = format_num(f_after_raw)

                        initial_lines = [f"🟢 <b>Treo cho TikTok <a href='https://tiktok.com/@{tt_username_api}'>@{tt_username_api}</a> thành công!</b> (Lần chạy đầu)",
                                         f"\nNickname: {name_api}"]
                        if userid_api != "?": initial_lines.append(f"User ID: <code>{userid_api}</code>")
                        if f_before_display != "?": initial_lines.append(f"Số follow trước: <code>{f_before_display}</code>")
                        if gain > 0: initial_lines.append(f"Đã tăng: <b>+{gain:,}</b>")
                        elif gain == 0 : initial_lines.append(f"Đã tăng: <code>0</code>")
                        else: initial_lines.append(f"Đã tăng(?): <code>{gain:,}</code>")
                        if f_after_display != "?": initial_lines.append(f"Số follow sau: <code>{f_after_display}</code>")
                        if khu_vuc_api != "?": initial_lines.append(f"Khu vực: {khu_vuc_api} {':flag_vn:' if 'vietnam' in khu_vuc_api.lower() else ''}")

                        caption = "\n".join(initial_lines)
                        photo_to_send = avatar_api if avatar_api and avatar_api.startswith("http") else None

                        # Gửi ảnh kèm caption hoặc chỉ caption
                        if photo_to_send:
                            try:
                                await app.bot.send_photo(chat_id=current_chat_id, photo=photo_to_send, caption=caption, parse_mode=ParseMode.HTML, disable_notification=True)
                            except Exception as e_send_photo:
                                logger.warning(f"Failed to send avatar for initial treo info {task_name}: {e_send_photo}. Sending text only.")
                                await app.bot.send_message(chat_id=current_chat_id, text=caption + f"\n(Không thể tải ảnh đại diện: <a href='{html.escape(avatar_api)}'>link</a>)", parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)
                        else: # Gửi text nếu không có avatar
                             await app.bot.send_message(chat_id=current_chat_id, text=caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True, disable_notification=True)

                        initial_info_sent = True # Đánh dấu đã gửi
                        logger.info(f"[Treo Task Initial Info] Sent initial success details for task '{task_name}'.")
                    except Forbidden:
                        logger.error(f"[Treo Task Stop] Bot Forbidden in chat {current_chat_id}. Cannot send initial info for '{task_name}'. Stopping task.")
                        await stop_treo_task(user_id_str, target_username, context, reason=f"Bot Forbidden in chat {current_chat_id}")
                        break # Thoát loop
                    except Exception as e_send_initial:
                        logger.error(f"Error sending initial treo info for '{task_name}' to chat {current_chat_id}: {e_send_initial}", exc_info=True)
                        initial_info_sent = False

                elif gain > 0: # Gửi thông báo ngắn gọn cho các lần thành công sau (chỉ khi có gain)
                    try:
                         status_msg = f"✅ Treo <code>@{html.escape(target_username)}</code>: <b>+{gain:,}</b> follow ✨"
                         await app.bot.send_message(chat_id=current_chat_id, text=status_msg, parse_mode=ParseMode.HTML, disable_notification=True)
                    except Forbidden:
                         logger.error(f"[Treo Task Stop] Bot Forbidden in chat {current_chat_id}. Cannot send status for '{task_name}'. Stopping task.")
                         await stop_treo_task(user_id_str, target_username, context, reason=f"Bot Forbidden in chat {current_chat_id}")
                         break
                    except Exception as e_send_status:
                         logger.error(f"Error sending subsequent success status for '{task_name}' to chat {current_chat_id}: {e_send_status}")

            else: # API Thất bại
                consecutive_failures += 1
                logger.warning(f"[Treo Task Fail] Task '{task_name}' failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}). API Msg: {api_message[:100]}...")

                # Gửi thông báo lỗi tạm thời
                status_lines = [f"❌ Treo <code>@{html.escape(target_username)}</code>: Thất bại ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES})"]
                status_lines.append(f"💬 <i>{html.escape(api_message[:150])}{'...' if len(api_message)>150 else ''}</i>")
                status_msg = "\n".join(status_lines)
                sent_status_message = None
                try:
                    sent_status_message = await app.bot.send_message(chat_id=current_chat_id, text=status_msg, parse_mode=ParseMode.HTML, disable_notification=True)
                    # Lên lịch xóa tin nhắn thất bại
                    if sent_status_message and app.job_queue:
                        job_name_del = f"del_treo_fail_{current_chat_id}_{sent_status_message.message_id}_{int(time.time())}"
                        app.job_queue.run_once(delete_message_job, TREO_FAILURE_MSG_DELETE_DELAY, data={'chat_id': current_chat_id, 'message_id': sent_status_message.message_id}, name=job_name_del)
                        logger.debug(f"Scheduled job '{job_name_del}' to delete failure msg {sent_status_message.message_id} in {TREO_FAILURE_MSG_DELETE_DELAY}s.")
                except Forbidden:
                    logger.error(f"[Treo Task Stop] Bot Forbidden in chat {current_chat_id}. Cannot send failure status for '{task_name}'. Stopping task.")
                    await stop_treo_task(user_id_str, target_username, context, reason=f"Bot Forbidden in chat {current_chat_id}")
                    break # Thoát loop
                except Exception as e_send_fail:
                    logger.error(f"Error sending failure status for '{task_name}' to chat {current_chat_id}: {e_send_fail}")

                # Kiểm tra nếu lỗi liên tục quá nhiều
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(f"[Treo Task Stop] Task '{task_name}' stopping due to {consecutive_failures} consecutive failures.")
                    # <<< Gọi hàm dừng để xóa config >>>
                    await stop_treo_task(user_id_str, target_username, context, reason=f"{consecutive_failures} consecutive API failures")
                    try:
                        await app.bot.send_message(current_chat_id, f"⚠️ {invoking_user_mention}: Treo cho <code>@{html.escape(target_username)}</code> đã tạm dừng do lỗi API liên tục. Vui lòng kiểm tra và thử <code>/treo</code> lại sau.", parse_mode=ParseMode.HTML, disable_notification=True)
                    except Exception as e_send_fail_stop: logger.warning(f"Failed to send consecutive failure stop message for task {task_name}: {e_send_fail_stop}")
                    break # Thoát vòng lặp

    except asyncio.CancelledError:
        logger.info(f"[Treo Task Cancelled] Task '{task_name}' was cancelled externally (likely by /dungtreo, /xoavip, cleanup or shutdown).")
        # Không cần làm gì thêm, config persistent nên đã được xóa bởi nơi gọi cancel/stop_treo_task
    except Exception as e:
        logger.error(f"[Treo Task Error] Unexpected error in task '{task_name}': {e}", exc_info=True)
        try:
            # Cố gắng lấy app context một lần nữa nếu lỗi xảy ra sớm
            app_on_error = context.application if context and hasattr(context, 'application') else None
            if app_on_error:
                 await app_on_error.bot.send_message(current_chat_id, f"💥 {invoking_user_mention}: Lỗi nghiêm trọng khi treo <code>@{html.escape(target_username)}</code>. Tác vụ đã dừng.\nLỗi: {html.escape(str(e))}", parse_mode=ParseMode.HTML, disable_notification=True)
            else: logger.error(f"[Treo Task Error] Cannot send fatal error message for task {task_name} because app context is unavailable.")
        except Exception as e_send_fatal: logger.error(f"Failed to send fatal error message for task {task_name}: {e_send_fatal}")
        # Dừng và xóa config nếu có lỗi nghiêm trọng
        await stop_treo_task(user_id_str, target_username, context, reason=f"Unexpected Error in loop: {e}")
    finally:
        logger.info(f"[Treo Task End] Task '{task_name}' finished.")
        # Dọn dẹp task khỏi dict runtime nếu nó kết thúc mà không qua stop_treo_task
        if user_id_str in active_treo_tasks and target_username in active_treo_tasks[user_id_str]:
            try:
                current_asyncio_task = asyncio.current_task()
                task_in_dict = active_treo_tasks[user_id_str].get(target_username)
                if task_in_dict is current_asyncio_task and task_in_dict and task_in_dict.done():
                    del active_treo_tasks[user_id_str][target_username]
                    if not active_treo_tasks[user_id_str]: del active_treo_tasks[user_id_str]
                    logger.info(f"[Treo Task Cleanup] Removed finished/failed task '{task_name}' from active tasks dict in finally block.")
            except RuntimeError: logger.debug(f"[Treo Task Cleanup] Could not get current task in finally block for {task_name}.")
            except Exception as e_final_clean: logger.error(f"[Treo Task Cleanup] Error during final runtime cleanup for {task_name}: {e_final_clean}")


# --- Lệnh /treo (VIP - Đã bỏ validation username, lưu chat_id và persistent config) ---
async def treo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bắt đầu treo tự động follow cho một user (chỉ VIP). Lưu config persistent."""
    global persistent_treo_configs, active_treo_tasks
    if not update or not update.effective_user: return
    user = update.effective_user
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id # <<< LƯU CHAT_ID NƠI GỌI LỆNH >>>
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()

    if not is_user_vip(user_id):
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/treo</code> chỉ dành cho <b>VIP</b>.\nDùng <code>/muatt</code> để nâng cấp hoặc <code>/menu</code>."
        await send_temporary_message(update, context, err_msg, duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    # Parse Arguments (Chỉ kiểm tra trống)
    args = context.args
    target_username = None
    err_txt = None
    if not args:
        err_txt = ("⚠️ Chưa nhập username TikTok cần treo.\n<b>Cú pháp:</b> <code>/treo username</code>")
    else:
        uname_raw = args[0].strip()
        uname = uname_raw.lstrip("@")
        if not uname:
            err_txt = "⚠️ Username không được trống."
        else:
            target_username = uname

    # Xóa lệnh gốc trước khi xử lý
    if original_message_id: await delete_user_message(update, context, original_message_id)

    if err_txt:
        await send_temporary_message(update, context, err_txt, duration=20, reply=False)
        return
    if not target_username: # Should not happen
        await send_temporary_message(update, context, "⚠️ Lỗi xử lý username.", duration=20, reply=False)
        return

    # Check Giới Hạn và Trạng Thái Treo Hiện Tại
    vip_limit = get_vip_limit(user_id) # Lấy limit hiện tại (phải còn VIP)
    persistent_user_configs = persistent_treo_configs.get(user_id_str, {})
    current_treo_count = len(persistent_user_configs)

    # Kiểm tra xem đã treo target này chưa (trong persistent)
    if target_username in persistent_user_configs:
        logger.info(f"User {user_id} tried to /treo target @{target_username} which is already in persistent config.")
        msg = f"⚠️ Bạn đã đang treo cho <code>@{html.escape(target_username)}</code> rồi.\nDùng <code>/dungtreo {target_username}</code> để dừng."
        await send_temporary_message(update, context, msg, duration=20, reply=False)
        return

    # Kiểm tra giới hạn VIP
    if current_treo_count >= vip_limit:
         logger.warning(f"User {user_id} tried to /treo target @{target_username} but reached limit ({current_treo_count}/{vip_limit}).")
         limit_msg = (f"⚠️ Đã đạt giới hạn treo tối đa! ({current_treo_count}/{vip_limit} tài khoản).\n"
                      f"Dùng <code>/dungtreo &lt;username&gt;</code> để giải phóng slot hoặc nâng cấp gói VIP.")
         await send_temporary_message(update, context, limit_msg, duration=30, reply=False)
         return

    # --- Bắt đầu Task Treo Mới và Lưu Config ---
    task = None
    try:
        app = context.application
        # Tạo task chạy nền, truyền chat_id vào
        # Tạo context mới để truyền vào task, chứa app context hiện tại
        task_context = ContextTypes.DEFAULT_TYPE(application=app, chat_id=chat_id, user_id=user_id)
        task_name = f"treo_{user_id_str}_{target_username}" # Tên task nhất quán
        task = app.create_task(
            run_treo_loop(user_id_str, target_username, task_context, chat_id), # <<< Truyền chat_id vào loop >>>
            name=task_name
        )

        # <<< Thêm task vào dict runtime VÀ LƯU config persistent >>>
        active_treo_tasks.setdefault(user_id_str, {})[target_username] = task
        persistent_treo_configs.setdefault(user_id_str, {})[target_username] = chat_id # <<< LƯU CHAT ID >>>
        save_data() # <<< LƯU NGAY LẬP TỨC KHI THÊM CONFIG MỚI >>>
        logger.info(f"Successfully created task '{task.get_name()}' and saved persistent config for user {user_id} -> @{target_username} in chat {chat_id}")

        # Thông báo thành công (đơn giản, chi tiết sẽ hiện sau)
        new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
        treo_interval_m = TREO_INTERVAL_SECONDS // 60
        success_msg = (f"✅ <b>Bắt Đầu Treo Thành Công!</b>\n\n👤 Cho: {invoking_user_mention}\n🎯 Target: <code>@{html.escape(target_username)}</code>\n"
                       f"⏳ Tần suất: Mỗi {treo_interval_m} phút\n📊 Slot đã dùng: {new_treo_count}/{vip_limit}\n\n"
                       f"<i>(Kết quả và thông tin chi tiết sẽ hiện tại đây sau lần chạy thành công đầu tiên)</i>")
        # Gửi không reply vì lệnh gốc đã xóa
        await context.bot.send_message(chat_id=chat_id, text=success_msg, parse_mode=ParseMode.HTML)

    except Exception as e_start_task:
         logger.error(f"Failed to start treo task or save config for user {user_id} target @{target_username}: {e_start_task}", exc_info=True)
         await send_temporary_message(update, context, f"❌ Lỗi hệ thống khi bắt đầu treo cho <code>@{html.escape(target_username)}</code>. Báo Admin.", duration=20, reply=False)
         # Cố gắng rollback nếu lỗi
         if task and isinstance(task, asyncio.Task) and not task.done(): task.cancel()
         # Xóa khỏi runtime và persistent nếu đã thêm vào
         rollbacked = await stop_treo_task(user_id_str, target_username, context, "Rollback due to start error")
         if rollbacked: logger.info(f"Rollbacked treo task/config for @{target_username} due to start error.")


# --- Lệnh /dungtreo (Đã sửa lỗi và thêm dừng tất cả, dùng stop_treo_task) ---
async def dungtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Dừng việc treo tự động follow cho một hoặc tất cả user."""
    if not update or not update.effective_user: return
    user = update.effective_user
    user_id = user.id
    user_id_str = str(user_id)
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()
    args = context.args

    # Xóa lệnh gốc trước khi xử lý
    if original_message_id: await delete_user_message(update, context, original_message_id)

    # --- Xử lý /dungtreo không có đối số (Dừng tất cả) ---
    if not args:
        logger.info(f"User {user_id} requesting to stop ALL treo tasks.")
        stopped_count = await stop_all_treo_tasks_for_user(user_id_str, context, reason=f"User command /dungtreo all by {user_id}")
        if stopped_count > 0:
             await context.bot.send_message(chat_id, f"✅ {invoking_user_mention}, đã dừng thành công <b>{stopped_count}</b> tài khoản đang treo.", parse_mode=ParseMode.HTML)
        else:
             await send_temporary_message(update, context, f"ℹ️ {invoking_user_mention}, bạn hiện không có tài khoản nào đang treo để dừng.", duration=20, reply=False)

    # --- Xử lý /dungtreo <username> (Dừng một target) ---
    else:
        target_username_raw = args[0].strip()
        target_username_clean = target_username_raw.lstrip("@")
        if not target_username_clean:
            await send_temporary_message(update, context, "⚠️ Username không được để trống.", duration=15, reply=False)
            return

        logger.info(f"User {user_id} requesting to stop treo for @{target_username_clean}")
        # <<< Gọi hàm dừng task/config (hàm này đã bao gồm xóa persistent và save_data) >>>
        stopped = await stop_treo_task(user_id_str, target_username_clean, context, reason=f"User command /dungtreo by {user_id}")

        if stopped:
            new_treo_count = len(persistent_treo_configs.get(user_id_str, {}))
            vip_limit_display = get_vip_limit(user_id) if is_user_vip(user_id) else "N/A"
            # <<< Phản hồi: Dừng target cụ thể thành công >>>
            await context.bot.send_message(
                chat_id,
                f"✅ {invoking_user_mention}, đã dừng treo và xóa cấu hình thành công cho <code>@{html.escape(target_username_clean)}</code>.\n(Slot còn lại: {vip_limit_display - new_treo_count}/{vip_limit_display})",
                parse_mode=ParseMode.HTML
            )
        else:
            # <<< Phản hồi: Không tìm thấy target cụ thể (trong runtime hoặc persistent) >>>
            await send_temporary_message(
                update, context,
                f"⚠️ {invoking_user_mention}, không tìm thấy cấu hình treo nào đang hoạt động hoặc đã lưu cho <code>@{html.escape(target_username_clean)}</code> để dừng.",
                duration=20, reply=False
            )

# --- Lệnh /listtreo (Lấy từ persistent, hiển thị trạng thái ước lượng) ---
async def listtreo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị danh sách các tài khoản TikTok đang được cấu hình treo bởi người dùng."""
    if not update or not update.effective_user: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()

    logger.info(f"User {user_id} requested /listtreo in chat {chat_id}")

    # Xóa lệnh gốc
    if original_message_id: await delete_user_message(update, context, original_message_id)

    # <<< Lấy danh sách từ persistent_treo_configs là nguồn chính xác nhất >>>
    user_treo_configs = persistent_treo_configs.get(user_id_str, {})
    treo_targets = list(user_treo_configs.keys()) # Lấy danh sách các username đang được cấu hình treo

    reply_lines = [f"📊 <b>Danh Sách Tài Khoản Đang Treo</b>", f"👤 Cho: {invoking_user_mention}"]

    if not treo_targets:
        reply_lines.append("\nBạn hiện không treo tài khoản TikTok nào.")
        if is_user_vip(user_id):
             reply_lines.append("Dùng <code>/treo &lt;username&gt;</code> để bắt đầu.")
        else:
             reply_lines.append("Nâng cấp VIP để sử dụng tính năng này (<code>/muatt</code>).")
    else:
        vip_limit = get_vip_limit(user_id) # Lấy limit nếu còn VIP
        is_currently_vip = is_user_vip(user_id)
        limit_display = f"{vip_limit}" if is_currently_vip else "N/A (VIP hết hạn?)"
        reply_lines.append(f"\n🔍 Số lượng: <b>{len(treo_targets)} / {limit_display}</b> tài khoản")
        # Lặp qua danh sách target đã lưu
        for target in sorted(treo_targets):
             # <<< Kiểm tra trạng thái ước lượng từ active_treo_tasks >>>
             is_running = False
             task_status = "⏸️ (Chưa chạy/Đã dừng)" # Mặc định (hoặc nếu task runtime không tìm thấy)
             if user_id_str in active_treo_tasks and target in active_treo_tasks[user_id_str]:
                  task = active_treo_tasks[user_id_str][target]
                  if task and isinstance(task, asyncio.Task):
                      if not task.done():
                          is_running = True
                          task_status = "▶️ (Đang chạy)"
                      elif task.cancelled():
                          task_status = "⏹️ (Đã hủy)"
                      else: # Task done nhưng không cancel (lỗi?)
                          exc = task.exception()
                          task_status = f"⚠️ (Lỗi: {exc})" if exc else "⏹️ (Đã dừng)"

             reply_lines.append(f"  {task_status} <code>@{html.escape(target)}</code>")
        reply_lines.append("\nℹ️ Dùng <code>/dungtreo &lt;username&gt;</code> hoặc <code>/dungtreo</code> để dừng.")
        reply_lines.append("<i>(Trạng thái ▶️/⏸️/⚠️ chỉ là ước lượng tại thời điểm xem)</i>")

    reply_text = "\n".join(reply_lines)
    try:
        # Gửi không reply vì lệnh gốc đã xóa
        await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed to send /listtreo response to user {user_id} in chat {chat_id}: {e}", exc_info=True)
        await send_temporary_message(update, context, "❌ Đã có lỗi xảy ra khi lấy danh sách treo.", duration=15, reply=False)

# --- Lệnh /xemfl24h (Giữ nguyên, đã kiểm tra) ---
async def xemfl24h_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Hiển thị số follow tăng trong 24 giờ qua cho user (từ user_daily_gains)."""
    if not update or not update.effective_user: return
    user = update.effective_user
    chat_id = update.effective_chat.id
    user_id = user.id
    user_id_str = str(user_id)
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()

    logger.info(f"User {user_id} requested /xemfl24h in chat {chat_id}")
    # Xóa lệnh gốc
    if original_message_id: await delete_user_message(update, context, original_message_id)

    # Yêu cầu VIP để xem thống kê này
    if not is_user_vip(user_id):
        err_msg = f"⚠️ {invoking_user_mention}, lệnh <code>/xemfl24h</code> chỉ dành cho <b>VIP</b>."
        await send_temporary_message(update, context, err_msg, duration=20, reply=False)
        return

    user_gains_all_targets = user_daily_gains.get(user_id_str, {})
    gains_last_24h = defaultdict(int)
    total_gain_user = 0
    current_time = time.time()
    time_threshold = current_time - USER_GAIN_HISTORY_SECONDS # 24 giờ trước

    if not user_gains_all_targets:
        reply_text = f"📊 {invoking_user_mention}, không tìm thấy dữ liệu tăng follow nào cho bạn trong 24 giờ qua."
    else:
        # Lọc và tổng hợp gain trong 24h
        for target_username, gain_list in user_gains_all_targets.items():
            gain_for_target = sum(gain for ts, gain in gain_list if isinstance(ts, (int, float)) and ts >= time_threshold)
            if gain_for_target > 0:
                gains_last_24h[target_username] += gain_for_target
                total_gain_user += gain_for_target

        reply_lines = [f"📈 <b>Follow Đã Tăng Trong 24 Giờ Qua</b>", f"👤 Cho: {invoking_user_mention}"]
        if not gains_last_24h:
            reply_lines.append("\n<i>Không có tài khoản nào tăng follow trong 24 giờ qua.</i>")
        else:
            reply_lines.append(f"\n✨ Tổng cộng: <b>+{total_gain_user:,} follow</b>")
            # Sắp xếp theo gain giảm dần
            sorted_targets = sorted(gains_last_24h.items(), key=lambda item: item[1], reverse=True)
            for target, gain_value in sorted_targets:
                reply_lines.append(f"  - <code>@{html.escape(target)}</code>: <b>+{gain_value:,}</b>")
        reply_lines.append(f"\n🕒 <i>Dữ liệu được tổng hợp từ các lần treo thành công gần nhất.</i>")
        reply_text = "\n".join(reply_lines)

    try:
        # Gửi không reply vì lệnh gốc đã xóa
        await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed to send /xemfl24h response to user {user_id} in chat {chat_id}: {e}", exc_info=True)
        await send_temporary_message(update, context, "❌ Đã có lỗi xảy ra khi xem thống kê follow.", duration=15, reply=False)

# --- Lệnh /mess (Giữ nguyên, đã kiểm tra) ---
async def mess_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Gửi thông báo từ Admin đến tất cả User VIP/Active."""
    if not update or not update.effective_user: return
    admin_user = update.effective_user
    if not admin_user or admin_user.id != ADMIN_USER_ID:
        logger.warning(f"Unauthorized /mess attempt by {admin_user.id if admin_user else 'Unknown'}")
        return # Không phản hồi gì

    original_message_id = update.message.message_id if update.message else None
    args = context.args

    # Xóa lệnh gốc của admin
    if original_message_id: await delete_user_message(update, context, original_message_id)

    if not args:
        await send_temporary_message(update, context, "⚠️ Thiếu nội dung thông báo.\n<b>Cú pháp:</b> <code>/mess Nội dung cần gửi</code>", duration=20, reply=False)
        return

    message_text = update.message.text.split(' ', 1)[1] # Lấy toàn bộ text sau /mess
    message_to_send = f"📢 <b>Thông báo từ Admin:</b>\n\n{html.escape(message_text)}" # Không cần mention admin trong tin nhắn gửi đi

    # Lấy danh sách User ID cần gửi
    target_user_ids = set()
    current_time = time.time()

    # Thêm VIP users còn hạn
    for user_id_str, vip_data in vip_users.items():
        try:
            if float(vip_data.get("expiry", 0)) > current_time:
                 target_user_ids.add(int(user_id_str))
        except (ValueError, TypeError): continue

    # Thêm activated users còn hạn
    for user_id_str, expiry_ts in activated_users.items():
        try:
            if float(expiry_ts) > current_time:
                 target_user_ids.add(int(user_id_str))
        except (ValueError, TypeError): continue

    if not target_user_ids:
         await send_temporary_message(update, context, "ℹ️ Không tìm thấy người dùng VIP hoặc đã kích hoạt key nào để gửi tin nhắn.", duration=20, reply=False)
         logger.info(f"Admin {admin_user.id} tried /mess, but no target users found.")
         return

    logger.info(f"Admin {admin_user.id} initiating /mess broadcast to {len(target_user_ids)} users.")
    await send_temporary_message(update, context, f"⏳ Đang bắt đầu gửi thông báo đến <b>{len(target_user_ids)}</b> người dùng...", duration=10, reply=False)

    success_count = 0
    failure_count = 0
    blocked_count = 0

    # Gửi lần lượt với delay nhỏ để tránh rate limit
    for user_id in target_user_ids:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=message_to_send,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            success_count += 1
            logger.debug(f"/mess: Sent successfully to {user_id}")
        except Forbidden:
            logger.warning(f"/mess: Failed to send to {user_id} - Bot blocked or kicked.")
            failure_count += 1
            blocked_count += 1
        except BadRequest as e:
            logger.warning(f"/mess: Failed to send to {user_id} - BadRequest: {e}")
            failure_count += 1
        except TelegramError as e:
            logger.warning(f"/mess: Failed to send to {user_id} - TelegramError: {e}")
            failure_count += 1
        except Exception as e:
            logger.error(f"/mess: Unexpected error sending to {user_id}: {e}", exc_info=True)
            failure_count += 1

        # Thêm delay nhỏ giữa các lần gửi
        await asyncio.sleep(0.1) # 100ms delay

    # Báo cáo kết quả cho Admin
    result_message = (f"✅ <b>Gửi Thông Báo Hoàn Tất!</b>\n\n"
                      f" Gửi thành công: {success_count}\n"
                      f" Gửi thất bại: {failure_count}")
    if blocked_count > 0: result_message += f" (trong đó {blocked_count} bị chặn/rời)"

    try: await context.bot.send_message(admin_user.id, result_message, parse_mode=ParseMode.HTML)
    except Exception as e_report: logger.error(f"Failed to send /mess report to admin {admin_user.id}: {e_report}")

    logger.info(f"/mess broadcast complete. Success: {success_count}, Failures: {failure_count} (Blocked: {blocked_count})")


# --- Lệnh /check (Giữ nguyên, đã kiểm tra) ---
async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Kiểm tra thông tin tài khoản TikTok."""
    if not update or not update.effective_user: return
    user = update.effective_user
    user_id = user.id
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()

    # Kiểm tra quyền (VIP hoặc Key)
    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để dùng lệnh <code>/check</code>!\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code> | 👑 Hoặc: <code>/muatt</code>")
        await send_temporary_message(update, context, err_msg, duration=30, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    args = context.args
    if not args:
        err_txt = ("⚠️ Chưa nhập username TikTok cần kiểm tra.\n<b>Cú pháp:</b> <code>/check username</code>")
        await send_temporary_message(update, context, err_txt, duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    target_username_raw = args[0].strip()
    target_username = target_username_raw.lstrip("@")
    if not target_username:
        err_txt = "⚠️ Username không được trống."
        await send_temporary_message(update, context, err_txt, duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    processing_msg = None
    final_response_text = ""
    try:
        # Gửi tin nhắn chờ và xóa lệnh gốc
        if update.message:
            processing_msg = await update.message.reply_html(f"⏳ {invoking_user_mention}, đang kiểm tra thông tin tài khoản <code>@{html.escape(target_username)}</code>...")
            if original_message_id: await delete_user_message(update, context, original_message_id)
        else:
             processing_msg = await context.bot.send_message(chat_id, f"⏳ {invoking_user_mention}, đang kiểm tra thông tin tài khoản <code>@{html.escape(target_username)}</code>...", parse_mode=ParseMode.HTML)

        # Gọi API check
        api_result = await call_tiktok_check_api(target_username)

        if api_result["success"]:
            data = api_result.get("data")
            if data and isinstance(data, dict):
                # Trích xuất thông tin
                username = html.escape(data.get("username", target_username))
                nickname = html.escape(data.get("nickname", "?"))
                followers = html.escape(str(data.get("followers", "?"))) # Giữ nguyên string có dấu phẩy nếu API trả về
                user_id_tt = html.escape(str(data.get("user_id", "?")))
                sec_uid = html.escape(str(data.get("sec_uid", "?"))) # Có thể cần hoặc không
                bio = html.escape(data.get("bio", ""))
                profile_pic = data.get("profilePic", "")
                is_private = data.get("privateAccount", False)
                api_success_flag = data.get("success", True) # Check thêm flag success bên trong data

                if not api_success_flag and "message" in data: # API trả về success=false bên trong data
                    error_msg = html.escape(data.get("message", "Lỗi không rõ từ API check."))
                    final_response_text = f"❌ Không thể kiểm tra <code>@{username}</code>.\nLý do API: <i>{error_msg}</i>"
                else:
                    # Định dạng kết quả
                    lines = [f"📊 <b>Thông Tin TikTok: <a href='https://tiktok.com/@{username}'>@{username}</a></b>"]
                    lines.append(f"👤 Nickname: <b>{nickname}</b>")
                    lines.append(f"❤️ Followers: <code>{followers}</code>")
                    if user_id_tt != "?": lines.append(f"🆔 User ID: <code>{user_id_tt}</code>")
                    # lines.append(f"🔒 SecUID: <code>{sec_uid[:10]}...</code>") # Có thể quá dài
                    if bio: lines.append(f"📝 Bio: <i>{bio}</i>")
                    lines.append(f"🔒 Riêng tư: {'✅ Có' if is_private else '❌ Không'}")

                    caption = "\n".join(lines)
                    photo_to_send = profile_pic if profile_pic and profile_pic.startswith("http") else None

                    # Thử gửi ảnh trước
                    photo_sent = False
                    if photo_to_send and processing_msg:
                        try:
                            # Dùng edit message media nếu có ảnh
                            media = InputMediaPhoto(media=photo_to_send, caption=caption, parse_mode=ParseMode.HTML)
                            await context.bot.edit_message_media(
                                chat_id=chat_id,
                                message_id=processing_msg.message_id,
                                media=media
                            )
                            photo_sent = True
                        except BadRequest as e_edit_media:
                            # Lỗi phổ biến: "Message can't be edited" hoặc "There is no media in the message to edit"
                            logger.warning(f"Failed to edit_message_media for /check @{username}: {e_edit_media}. Falling back.")
                        except Exception as e_edit_media_unexp:
                             logger.error(f"Unexpected error editing media for /check @{username}: {e_edit_media_unexp}", exc_info=True)

                    # Nếu không gửi ảnh hoặc edit lỗi, edit text
                    if not photo_sent and processing_msg:
                        final_response_text = caption
                        # Nối link ảnh vào text nếu có
                        if photo_to_send: final_response_text += f"\n🖼️ <a href='{html.escape(photo_to_send)}'>Ảnh đại diện</a>"
                    elif not processing_msg: # Trường hợp không có tin nhắn chờ (hiếm)
                         final_response_text = caption
                         if photo_to_send: final_response_text += f"\n🖼️ <a href='{html.escape(photo_to_send)}'>Ảnh đại diện</a>"

            else: # API success nhưng data rỗng hoặc sai định dạng
                 logger.warning(f"/check @{target_username}: API success but data is missing or invalid. Data: {data}")
                 final_response_text = f"⚠️ Không thể lấy đủ thông tin cho <code>@{html.escape(target_username)}</code>. API trả về dữ liệu không mong đợi."
        else: # API trả về lỗi
            api_msg = api_result["message"]
            logger.warning(f"/check @{target_username} failed. API message: {api_msg}")
            final_response_text = f"❌ Không thể kiểm tra <code>@{html.escape(target_username)}</code>.\nLý do: <i>{html.escape(api_msg)}</i>"

    except Exception as e_unexp:
        logger.error(f"Unexpected error during /check command for @{target_username}: {e_unexp}", exc_info=True)
        final_response_text = f"❌ Lỗi hệ thống Bot khi kiểm tra <code>@{html.escape(target_username)}</code>."

    finally:
        # Chỉ edit text nếu final_response_text có nội dung (trường hợp gửi ảnh thành công thì không cần edit)
        if final_response_text and processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=processing_msg.message_id,
                    text=final_response_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True # Tắt preview cho link ảnh/tiktok
                )
            except BadRequest as e_edit:
                 # Nếu edit lỗi (vd: tin nhắn không đổi), bỏ qua
                 if "message is not modified" not in str(e_edit).lower():
                     logger.warning(f"Failed to edit /check final msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit_final:
                 logger.error(f"Unexpected error editing final /check msg {processing_msg.message_id}: {e_edit_final}")
        elif not processing_msg and final_response_text: # Gửi mới nếu không có tin nhắn chờ
              try:
                  await context.bot.send_message(
                      chat_id=chat_id,
                      text=final_response_text,
                      parse_mode=ParseMode.HTML,
                      disable_web_page_preview=True
                  )
              except Exception as e_send_new:
                   logger.error(f"Failed to send new final /check message for @{target_username}: {e_send_new}")


# --- Lệnh /sound (Giữ nguyên, đã kiểm tra) ---
async def sound_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lấy thông tin bài nhạc SoundCloud."""
    if not update or not update.effective_user: return
    user = update.effective_user
    user_id = user.id
    chat_id = update.effective_chat.id
    original_message_id = update.message.message_id if update.message else None
    invoking_user_mention = user.mention_html()

    # Kiểm tra quyền (VIP hoặc Key) - Bạn có thể bỏ nếu muốn lệnh này miễn phí
    if not can_use_feature(user_id):
        err_msg = (f"⚠️ {invoking_user_mention}, bạn cần là <b>VIP</b> hoặc <b>kích hoạt key</b> để dùng lệnh <code>/sound</code>!\n"
                   f"➡️ Dùng: <code>/getkey</code> » <code>/nhapkey &lt;key&gt;</code> | 👑 Hoặc: <code>/muatt</code>")
        await send_temporary_message(update, context, err_msg, duration=30, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    args = context.args
    if not args:
        err_txt = ("⚠️ Chưa nhập link SoundCloud.\n<b>Cú pháp:</b> <code>/sound https://soundcloud.com/...</code>")
        await send_temporary_message(update, context, err_txt, duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    sound_link = args[0].strip()
    # Kiểm tra sơ bộ link
    if not re.match(r"https?://(?:www\.)?soundcloud\.com/", sound_link):
        err_txt = f"⚠️ Link <code>{html.escape(sound_link)}</code> không giống link SoundCloud hợp lệ."
        await send_temporary_message(update, context, err_txt, duration=20, reply=True)
        if original_message_id: await delete_user_message(update, context, original_message_id)
        return

    processing_msg = None
    final_response_text = ""
    try:
        # Gửi tin nhắn chờ và xóa lệnh gốc
        if update.message:
            processing_msg = await update.message.reply_html(f"⏳ {invoking_user_mention}, đang lấy thông tin từ link SoundCloud...")
            if original_message_id: await delete_user_message(update, context, original_message_id)
        else:
             processing_msg = await context.bot.send_message(chat_id, f"⏳ {invoking_user_mention}, đang lấy thông tin từ link SoundCloud...", parse_mode=ParseMode.HTML)

        # Gọi API SoundCloud
        api_result = await call_soundcloud_api(sound_link)

        if api_result["success"]:
            data = api_result.get("data")
            if data and isinstance(data, dict):
                # --- Xử lý dữ liệu trả về từ API ---
                title = html.escape(data.get("title", "Không rõ tiêu đề"))
                artist = html.escape(data.get("artist", data.get("user", "Không rõ nghệ sĩ"))) # Thử cả 'user'
                thumbnail = data.get("thumbnail", data.get("artwork_url")) # Thử cả hai key phổ biến
                download_url = data.get("download_url", data.get("stream_url")) # Lấy link download hoặc stream
                duration_ms = data.get("duration") # Thường là mili giây

                lines = [f"🎧 <b>Thông Tin SoundCloud</b>"]
                lines.append(f"🎶 Tiêu đề: <b>{title}</b>")
                lines.append(f"👤 Nghệ sĩ: {artist}")

                if duration_ms:
                    try:
                         seconds = int(duration_ms) // 1000
                         minutes = seconds // 60
                         seconds %= 60
                         lines.append(f"⏱️ Thời lượng: {minutes:02d}:{seconds:02d}")
                    except: pass # Bỏ qua nếu duration lỗi

                # Xử lý link tải/nghe
                action_button = None
                if download_url and isinstance(download_url, str) and download_url.startswith("http"):
                    lines.append(f"\n🔗 Link nghe/tải:")
                    # Giới hạn độ dài link hiển thị
                    display_link = download_url[:70] + '...' if len(download_url) > 70 else download_url
                    lines.append(f"   <code>{html.escape(display_link)}</code>")
                    action_button = InlineKeyboardButton("🎵 Nghe/Tải", url=download_url)
                else:
                     lines.append("\n<i>(API không trả về link nghe/tải trực tiếp)</i>")

                caption = "\n".join(lines)
                photo_to_send = thumbnail if thumbnail and isinstance(thumbnail, str) and thumbnail.startswith("http") else None
                reply_markup = InlineKeyboardMarkup([[action_button]]) if action_button else None

                # --- Gửi kết quả ---
                photo_sent = False
                if photo_to_send and processing_msg:
                    try:
                        media = InputMediaPhoto(media=photo_to_send, caption=caption, parse_mode=ParseMode.HTML)
                        await context.bot.edit_message_media(
                            chat_id=chat_id,
                            message_id=processing_msg.message_id,
                            media=media,
                            reply_markup=reply_markup
                        )
                        photo_sent = True
                    except BadRequest as e_edit_media:
                        logger.warning(f"Failed to edit_message_media for /sound: {e_edit_media}. Falling back.")
                    except Exception as e_edit_media_unexp:
                         logger.error(f"Unexpected error editing media for /sound: {e_edit_media_unexp}", exc_info=True)

                if not photo_sent and processing_msg:
                    final_response_text = caption
                    if photo_to_send: final_response_text += f"\n🖼️ <a href='{html.escape(photo_to_send)}'>Ảnh bìa</a>"
                elif not processing_msg: # Không có tin nhắn chờ
                     final_response_text = caption
                     if photo_to_send: final_response_text += f"\n🖼️ <a href='{html.escape(photo_to_send)}'>Ảnh bìa</a>"

                # Lưu reply_markup để dùng khi edit text nếu cần
                context.user_data['sound_reply_markup'] = reply_markup

            else: # API success nhưng data rỗng/lỗi
                logger.warning(f"/sound API success but data invalid. Link: {sound_link}, Data: {data}")
                final_response_text = f"⚠️ Không thể lấy thông tin từ link SoundCloud này. API trả về dữ liệu không mong đợi."
        else: # API trả về lỗi
            api_msg = api_result["message"]
            logger.warning(f"/sound failed for link {sound_link}. API message: {api_msg}")
            final_response_text = f"❌ Không thể lấy thông tin SoundCloud.\nLý do: <i>{html.escape(api_msg)}</i>"

    except Exception as e_unexp:
        logger.error(f"Unexpected error during /sound command for link {sound_link}: {e_unexp}", exc_info=True)
        final_response_text = f"❌ Lỗi hệ thống Bot khi xử lý link SoundCloud."

    finally:
        reply_markup_to_use = context.user_data.pop('sound_reply_markup', None) # Lấy markup đã lưu
        if final_response_text and processing_msg:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=processing_msg.message_id,
                    text=final_response_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=reply_markup_to_use # Gửi markup kèm text nếu edit ảnh lỗi
                )
            except BadRequest as e_edit:
                 if "message is not modified" not in str(e_edit).lower():
                     logger.warning(f"Failed to edit /sound final msg {processing_msg.message_id}: {e_edit}")
            except Exception as e_edit_final:
                 logger.error(f"Unexpected error editing final /sound msg {processing_msg.message_id}: {e_edit_final}")
        elif not processing_msg and final_response_text: # Gửi mới nếu không có tin nhắn chờ
              try:
                  await context.bot.send_message(
                      chat_id=chat_id,
                      text=final_response_text,
                      parse_mode=ParseMode.HTML,
                      disable_web_page_preview=True,
                      reply_markup=reply_markup_to_use
                  )
              except Exception as e_send_new:
                   logger.error(f"Failed to send new final /sound message: {e_send_new}")


# --- Job Thống Kê Follow Tăng (Đã cập nhật để clear stats đúng cách) ---
async def report_treo_stats(context: ContextTypes.DEFAULT_TYPE):
    """Job chạy định kỳ để thống kê và báo cáo user treo tăng follow (dùng treo_stats)."""
    global last_stats_report_time, treo_stats
    current_time = time.time()

    # Kiểm tra nếu ALLOWED_GROUP_ID chưa được set
    if not ALLOWED_GROUP_ID:
        return

    # Kiểm tra thời gian kể từ lần báo cáo trước (thêm 5 phút đệm)
    time_since_last = current_time - last_stats_report_time if last_stats_report_time else float('inf')
    if time_since_last < TREO_STATS_INTERVAL_SECONDS - 300: # Chờ đủ thời gian
        logger.debug(f"[Stats Job] Skipping report, only {time_since_last:.0f}s passed since last report (required ~{TREO_STATS_INTERVAL_SECONDS}s).")
        return

    logger.info(f"[Stats Job] Starting statistics report job. Last report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")

    # <<< Tạo bản sao của stats hiện tại để xử lý, sau đó xóa bản gốc >>>
    stats_snapshot = {}
    if treo_stats:
        try:
            # Sử dụng dict comprehension để copy sâu hơn (cho 2 cấp)
            stats_snapshot = {
                uid: {target: gain for target, gain in targets.items()}
                for uid, targets in treo_stats.items()
            }
            if not stats_snapshot: logger.info("[Stats Job] Snapshot is empty after copy (no stats recorded).")
        except Exception as e_copy:
            logger.error(f"[Stats Job] Error creating stats snapshot: {e_copy}. Aborting report cycle.", exc_info=True)
            last_stats_report_time = current_time # Cập nhật time để tránh lặp lại lỗi ngay
            # Không save_data() vì không có gì thay đổi và muốn giữ lại stats hiện tại
            return

    # --- Critical Section Start ---
    # <<< Xóa stats hiện tại và cập nhật thời gian báo cáo NGAY LẬP TỨC >>>
    treo_stats.clear()
    last_stats_report_time = current_time
    save_data() # <<< LƯU TRẠNG THÁI MỚI (stats rỗng, thời gian cập nhật) >>>
    # --- Critical Section End ---
    logger.info(f"[Stats Job] Cleared current job stats and updated last report time. Processing snapshot with {len(stats_snapshot)} users.")

    # Xử lý snapshot để tạo báo cáo
    top_gainers = [] # List of (gain, user_id_str, target_username)
    total_gain_all = 0
    for user_id_str, targets in stats_snapshot.items():
        if isinstance(targets, dict):
            for target_username, gain in targets.items():
                try:
                    gain_int = int(gain)
                    if gain_int > 0:
                        top_gainers.append((gain_int, str(user_id_str), str(target_username)))
                        total_gain_all += gain_int
                    elif gain_int < 0:
                        logger.warning(f"[Stats Job] Negative gain ({gain_int}) found for {user_id_str}->{target_username} in snapshot.")
                except (ValueError, TypeError):
                    logger.warning(f"[Stats Job] Invalid gain value ({gain}) for {user_id_str}->{target_username}. Skipping.")
        else:
            logger.warning(f"[Stats Job] Invalid target structure type ({type(targets)}) for user {user_id_str} in snapshot.")

    if not top_gainers:
        logger.info("[Stats Job] No positive gains found after processing snapshot. Sending empty report.")
        report_text = f"📊 <b>Thống Kê Tăng Follow (Chu Kỳ Vừa Qua)</b> 📊\n\n<i>Không có dữ liệu tăng follow nào được ghi nhận trong chu kỳ này.</i>"
    else:
        # Sắp xếp theo gain giảm dần
        top_gainers.sort(key=lambda x: x[0], reverse=True)

        report_lines = [f"📊 <b>Thống Kê Tăng Follow (Chu Kỳ Vừa Qua)</b> 📊",
                        f"<i>(Tổng cộng: <b>{total_gain_all:,}</b> follow được tăng bởi các tài khoản đang treo)</i>",
                        "\n🏆 <b>Top Tài Khoản Treo Hiệu Quả Nhất:</b>"]

        num_top_to_show = 10
        user_mentions_cache = {} # Cache để giảm gọi get_chat
        app = context.application # Lấy application để gọi bot.get_chat

        for i, (gain, user_id_str_gain, target_username_gain) in enumerate(top_gainers[:num_top_to_show]):
            user_mention = user_mentions_cache.get(user_id_str_gain)
            if not user_mention:
                try:
                    # Cần dùng int cho get_chat
                    user_info = await app.bot.get_chat(int(user_id_str_gain))
                    m = user_info.mention_html() or (f"@{user_info.username}" if user_info.username else None)
                    user_mention = m if m else f"User <code>{user_id_str_gain}</code>"
                except Exception as e_get_chat:
                    logger.warning(f"[Stats Job] Failed to get mention for user {user_id_str_gain}: {e_get_chat}")
                    user_mention = f"User <code>{user_id_str_gain}</code>" # Fallback ID
                user_mentions_cache[user_id_str_gain] = user_mention

            rank_icon = ["🥇", "🥈", "🥉"][i] if i < 3 else "🏅"
            report_lines.append(f"  {rank_icon} <b>+{gain:,} follow</b> cho <code>@{html.escape(target_username_gain)}</code> (bởi {user_mention})")

        if len(top_gainers) > num_top_to_show:
             report_lines.append(f"  <i>... và {len(top_gainers) - num_top_to_show} tài khoản khác.</i>")

        treo_interval_m = TREO_INTERVAL_SECONDS // 60
        stats_interval_h = TREO_STATS_INTERVAL_SECONDS // 3600
        report_lines.append(f"\n🕒 <i>Cập nhật sau mỗi {stats_interval_h} giờ. Treo chạy mỗi {treo_interval_m} phút.</i>")

        report_text = "\n".join(report_lines)

    # Gửi báo cáo vào nhóm
    try:
        await context.application.bot.send_message(
            chat_id=ALLOWED_GROUP_ID,
            text=report_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            disable_notification=True # Không thông báo ồn ào
        )
        logger.info(f"[Stats Job] Successfully sent statistics report to group {ALLOWED_GROUP_ID}.")
    except Forbidden:
         logger.error(f"[Stats Job] Failed to send statistics report to group {ALLOWED_GROUP_ID}: Bot Forbidden/Kicked.")
         # await context.application.bot.send_message(ADMIN_USER_ID, f"⚠️ Không thể gửi báo cáo thống kê vào nhóm {ALLOWED_GROUP_ID} do bị chặn/kick.")
    except Exception as e:
        logger.error(f"[Stats Job] Failed to send statistics report to group {ALLOWED_GROUP_ID}: {e}", exc_info=True)

    logger.info("[Stats Job] Statistics report job finished.")


# --- Hàm helper bất đồng bộ để dừng task khi tắt bot (Giữ nguyên) ---
async def shutdown_async_tasks(tasks_to_cancel: list[asyncio.Task], timeout: float):
    """Helper async function to cancel and wait for tasks during shutdown."""
    if not tasks_to_cancel:
        logger.info("[Shutdown] No active treo tasks found to cancel.")
        return
    logger.info(f"[Shutdown] Attempting to gracefully cancel {len(tasks_to_cancel)} active treo tasks with {timeout}s timeout...")

    # Hủy tất cả các task
    for task in tasks_to_cancel:
        if task and not task.done():
            task.cancel()
            # Lấy tên task để log
            task_name = "Unknown Task"
            try: task_name = task.get_name()
            except Exception: pass
            logger.debug(f"[Shutdown] Cancellation requested for task '{task_name}'.")

    # Chờ các task hoàn thành (hoặc bị hủy) với timeout tổng
    results = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

    logger.info("[Shutdown] Finished waiting for treo task cancellations.")

    cancelled_count, errors_count, finished_count = 0, 0, 0
    for i, result in enumerate(results):
        task = tasks_to_cancel[i]
        task_name = "Unknown Task"
        try:
             if task: task_name = task.get_name() or f"Task_{i}"
        except Exception: task_name = f"Task_{i}"

        if isinstance(result, asyncio.CancelledError):
            cancelled_count += 1
            logger.info(f"[Shutdown] Task '{task_name}' confirmed cancelled.")
        elif isinstance(result, Exception):
            errors_count += 1
            # Log lỗi nếu task kết thúc với exception thay vì CancelledError
            logger.error(f"[Shutdown] Task '{task_name}' finished with error during cancellation: {result}", exc_info=False)
        else:
            # Task kết thúc bình thường (hiếm khi xảy ra nếu đã cancel) hoặc trả về kết quả
            finished_count += 1
            logger.debug(f"[Shutdown] Task '{task_name}' finished with result: {result}.")

    logger.info(f"[Shutdown] Task cancellation summary: {cancelled_count} cancelled, {errors_count} errors, {finished_count} finished normally/unexpectedly.")


# --- Khởi động lại task treo từ persistent config (Đã cải thiện đáng kể) ---
async def restore_persistent_treo_tasks(application: Application):
    """Khôi phục và khởi động lại các task treo đã lưu, kiểm tra VIP/limit/chat_id."""
    global persistent_treo_configs, active_treo_tasks
    logger.info("--- Restoring Persistent Treo Tasks ---")
    restored_count = 0
    users_to_cleanup = [] # User IDs (str) cần xóa config do hết VIP/lỗi
    configs_to_remove = defaultdict(list) # {user_id_str: [target1, target2]} - Config bị xóa do vượt limit/lỗi chatid
    tasks_to_create_data = [] # List of (user_id_str, target_username, chat_id_int)

    # Tạo bản sao để lặp an toàn
    persistent_treo_snapshot = {
        uid: dict(targets) for uid, targets in persistent_treo_configs.items() if isinstance(targets, dict)
    }

    if not persistent_treo_snapshot:
        logger.info("[Restore] No persistent treo configurations found to restore.")
        return 0 # Trả về số task đã khôi phục

    total_configs_found = sum(len(targets) for targets in persistent_treo_snapshot.values())
    logger.info(f"[Restore] Found {total_configs_found} persistent treo configs for {len(persistent_treo_snapshot)} users. Verifying and restoring...")

    # Tạo context mặc định một lần
    default_context = ContextTypes.DEFAULT_TYPE(application=application, chat_id=None, user_id=None)

    for user_id_str, targets_for_user in persistent_treo_snapshot.items():
        try:
            user_id_int = int(user_id_str)
            # 1. Kiểm tra User còn là VIP không
            if not is_user_vip(user_id_int):
                logger.warning(f"[Restore] User {user_id_str} from persistent config is no longer VIP. Marking for cleanup.")
                users_to_cleanup.append(user_id_str)
                continue # Bỏ qua tất cả target của user này

            # 2. Kiểm tra giới hạn VIP của User
            vip_limit = get_vip_limit(user_id_int) # Limit này đã đảm bảo user còn VIP
            current_user_restore_count = 0 # Đếm số task đã thêm cho user này trong lần restore này

            # Lặp qua các target của user
            for target_username, chat_id_val in targets_for_user.items():
                # 2a. Kiểm tra kiểu dữ liệu chat_id
                try:
                    chat_id_int = int(chat_id_val)
                except (ValueError, TypeError):
                     logger.warning(f"[Restore] Invalid chat_id '{chat_id_val}' for user {user_id_str} -> @{target_username}. Skipping this target and marking for removal.")
                     configs_to_remove[user_id_str].append(target_username)
                     continue

                # 2b. Kiểm tra giới hạn trước khi thêm vào danh sách tạo task
                if current_user_restore_count >= vip_limit:
                     logger.warning(f"[Restore] User {user_id_str} reached VIP limit ({vip_limit}) during restore. Skipping persistent target @{target_username} and marking for removal.")
                     configs_to_remove[user_id_str].append(target_username)
                     continue # Bỏ qua target này

                # 3. Kiểm tra xem task đã chạy chưa (trường hợp restart cực nhanh - hiếm)
                runtime_task = active_treo_tasks.get(user_id_str, {}).get(target_username)
                if runtime_task and isinstance(runtime_task, asyncio.Task) and not runtime_task.done():
                     logger.info(f"[Restore] Task for {user_id_str} -> @{target_username} seems already active (runtime). Skipping restore.")
                     current_user_restore_count += 1 # Vẫn tính vào limit đã dùng
                     continue
                elif runtime_task: # Task cũ đã xong hoặc lỗi
                     logger.info(f"[Restore] Found finished/invalid runtime task for {user_id_str} -> @{target_username}. Will attempt restore.")

                # 4. Thêm vào danh sách cần tạo task
                logger.info(f"[Restore] Scheduling restore for treo task: user {user_id_str} -> @{target_username} in chat {chat_id_int}")
                tasks_to_create_data.append((user_id_str, target_username, chat_id_int))
                current_user_restore_count += 1 # Tăng số task đã lên lịch cho user này

        except ValueError:
            logger.error(f"[Restore] Invalid user_id format '{user_id_str}' found in persistent_treo_configs. Marking for cleanup.")
            users_to_cleanup.append(user_id_str)
        except Exception as e_outer_restore:
            logger.error(f"[Restore] Unexpected error processing persistent treo config for user {user_id_str}: {e_outer_restore}", exc_info=True)
            users_to_cleanup.append(user_id_str) # Đánh dấu để dọn dẹp nếu có lỗi

    # --- Dọn dẹp Config Persistent ---
    config_changed = False
    # Dọn dẹp user không hợp lệ/hết VIP
    if users_to_cleanup:
        unique_users_to_cleanup = set(users_to_cleanup)
        logger.info(f"[Restore Cleanup] Removing persistent configs for {len(unique_users_to_cleanup)} non-VIP or invalid users...")
        for user_id_clean in unique_users_to_cleanup:
            if user_id_clean in persistent_treo_configs:
                del persistent_treo_configs[user_id_clean]
                config_changed = True
        if config_changed: logger.info(f"[Restore Cleanup] Removed configs for {len(unique_users_to_cleanup)} users.")

    # Dọn dẹp target vượt limit / chat_id lỗi
    if configs_to_remove:
        removed_count = 0
        for user_id_rem, targets_list in configs_to_remove.items():
            if user_id_rem in persistent_treo_configs:
                user_dict_changed = False
                for target_rem in targets_list:
                    if target_rem in persistent_treo_configs[user_id_rem]:
                        del persistent_treo_configs[user_id_rem][target_rem]
                        config_changed = True
                        user_dict_changed = True
                        removed_count += 1
                # Xóa luôn user nếu không còn target nào
                if user_dict_changed and not persistent_treo_configs[user_id_rem]:
                    del persistent_treo_configs[user_id_rem]
        if config_changed: logger.info(f"[Restore Cleanup] Removed {removed_count} over-limit/invalid configs.")

    # Lưu lại dữ liệu nếu có thay đổi config
    if config_changed:
        logger.info("[Restore] Saving data after cleaning up persistent configs during restore.")
        save_data()

    # --- Tạo các Task Treo đã lên lịch ---
    if tasks_to_create_data:
        logger.info(f"[Restore] Creating {len(tasks_to_create_data)} restored treo tasks...")
        for user_id_create, target_create, chat_id_create in tasks_to_create_data:
            try:
                # Tạo context mới cho mỗi task để đảm bảo chat_id đúng (mặc dù loop dùng chat_id riêng)
                task_context = ContextTypes.DEFAULT_TYPE(application=application, chat_id=chat_id_create, user_id=int(user_id_create))
                task_name = f"treo_{user_id_create}_{target_create}" # Tên task nhất quán
                task = application.create_task(
                    run_treo_loop(user_id_create, target_create, task_context, chat_id_create),
                    name=task_name + "_restored" # Thêm hậu tố để phân biệt
                )
                # Thêm task mới tạo vào dict runtime
                active_treo_tasks.setdefault(user_id_create, {})[target_create] = task
                restored_count += 1
                # Thêm delay nhỏ giữa các lần tạo task để tránh quá tải CPU/mạng lúc khởi động
                await asyncio.sleep(0.05) # 50ms delay
            except Exception as e_create:
                logger.error(f"[Restore] Failed to create restored task for {user_id_create} -> @{target_create}: {e_create}", exc_info=True)
                # Không xóa config ở đây, lần restart sau sẽ thử lại hoặc cleanup job xử lý
    else:
        logger.info("[Restore] No valid treo tasks to create after verification.")

    logger.info(f"--- Treo Task Restore Complete: {restored_count} tasks started ---")
    return restored_count

# --- Main Function (Đã cập nhật để gọi restore và thêm handler) ---
def main() -> None:
    """Khởi động, khôi phục task và chạy bot."""
    start_time = time.time()
    print(f"--- Bot DinoTool Starting --- | Timestamp: {datetime.now().isoformat()} ---")

    # --- In tóm tắt cấu hình ---
    print("\n--- Configuration Summary ---")
    print(f"BOT_TOKEN: ...{BOT_TOKEN[-6:]}")
    print(f"ADMIN_USER_ID: {ADMIN_USER_ID}")
    print(f"BILL_FORWARD_TARGET_ID: {BILL_FORWARD_TARGET_ID}")
    print(f"ALLOWED_GROUP_ID: {ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else 'None (Stats/Mess Disabled)'}")
    print(f"API_KEY (Tim): {'Set' if API_KEY else 'Not Set'}")
    print(f"LINK_SHORTENER_API_KEY: {'Set' if LINK_SHORTENER_API_KEY else '!!! Missing !!!'}")
    print(f"TIKTOK_CHECK_API_KEY: {'Set' if TIKTOK_CHECK_API_KEY else '!!! Missing !!!'}")
    print(f"QR_CODE_URL: {'Set' if QR_CODE_URL and QR_CODE_URL.startswith('http') else '!!! Invalid or Missing !!!'}")
    print(f"Bank Info: {BANK_NAME} - {BANK_ACCOUNT} - {ACCOUNT_NAME}")
    print(f"Cooldowns (s): Tim/Fl={TIM_FL_COOLDOWN_SECONDS} | GetKey={GETKEY_COOLDOWN_SECONDS}")
    print(f"Durations (s): KeyExpiry={KEY_EXPIRY_SECONDS} | Activation={ACTIVATION_DURATION_SECONDS} | GainHistory={USER_GAIN_HISTORY_SECONDS}")
    print(f"Treo (s): Interval={TREO_INTERVAL_SECONDS} | FailDeleteDelay={TREO_FAILURE_MSG_DELETE_DELAY} | StatsInterval={TREO_STATS_INTERVAL_SECONDS}")
    print(f"VIP Default Limit: {DEFAULT_VIP_LIMIT}")
    print(f"Data File: {DATA_FILE} | Log File: {LOG_FILE}")
    print("-" * 30)

    print("Loading persistent data...")
    load_data() # Load data trước khi cấu hình application
    persistent_treo_count = sum(len(targets) for targets in persistent_treo_configs.values())
    gain_user_count = len(user_daily_gains)
    gain_entry_count = sum(len(gl) for targets in user_daily_gains.values() for gl in targets.values())
    print(f"Load complete. Keys: {len(valid_keys)}, Activated: {len(activated_users)}, VIPs: {len(vip_users)}")
    print(f"Persistent Treo Found: {persistent_treo_count} targets for {len(persistent_treo_configs)} users")
    print(f"User Daily Gains Found: {gain_entry_count} entries for {gain_user_count} users")
    print(f"Initial Job Stats Users: {len(treo_stats)}, Last Report: {datetime.fromtimestamp(last_stats_report_time).isoformat() if last_stats_report_time else 'Never'}")
    print("-" * 30)

    # Cấu hình Application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .job_queue(JobQueue())
        .pool_timeout(120)
        .connect_timeout(60)
        .read_timeout(120)
        .write_timeout(120)
        .get_updates_pool_timeout(120)
        .http_version("1.1") # Sử dụng HTTP/1.1
        # .concurrent_updates(20) # Tăng số lượng update xử lý đồng thời (mặc định 10)
        .build()
    )

    # Lên lịch các job định kỳ
    jq = application.job_queue
    if jq:
        jq.run_repeating(cleanup_expired_data, interval=CLEANUP_INTERVAL_SECONDS, first=60, name="cleanup_expired_data_job")
        logger.info(f"Scheduled cleanup job every {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes.")
        if ALLOWED_GROUP_ID:
            jq.run_repeating(report_treo_stats, interval=TREO_STATS_INTERVAL_SECONDS, first=300, name="report_treo_stats_job")
            logger.info(f"Scheduled statistics report job every {TREO_STATS_INTERVAL_SECONDS / 3600:.1f} hours (to group {ALLOWED_GROUP_ID}).")
        else:
            logger.info("Statistics report job skipped (ALLOWED_GROUP_ID not set).")
    else:
        logger.error("JobQueue is not available. Scheduled jobs will not run.")

    # --- Register Handlers ---
    # Commands User
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
    application.add_handler(CommandHandler("xemfl24h", xemfl24h_command))
    application.add_handler(CommandHandler("check", check_command))
    application.add_handler(CommandHandler("sound", sound_command))

    # Commands Admin
    application.add_handler(CommandHandler("addtt", addtt_command))
    application.add_handler(CommandHandler("xoavip", xoavip_command)) # <<< THÊM HANDLER XOAVIP >>>
    application.add_handler(CommandHandler("mess", mess_command))

    # Callback Handlers
    application.add_handler(CallbackQueryHandler(menu_callback_handler, pattern="^show_(muatt|lenh)$"))
    application.add_handler(CallbackQueryHandler(prompt_send_bill_callback, pattern=r"^prompt_send_bill_\d+$"))

    # Message handler cho ảnh bill (Ưu tiên cao - group -1)
    photo_bill_filter = (filters.PHOTO | filters.Document.IMAGE) & (~filters.COMMAND) & filters.UpdateType.MESSAGE
    application.add_handler(MessageHandler(photo_bill_filter, handle_photo_bill), group=-1)
    logger.info("Registered photo/bill handler (priority -1) for pending users.")
    # --- End Handler Registration ---

    # --- Khởi động lại các task treo đã lưu ---
    async def run_restore_and_start(app: Application):
        # <<< Gọi hàm restore đã cải thiện >>>
        restored_num = await restore_persistent_treo_tasks(app)
        print(f"\n--- Bot initialization complete. Restored {restored_num} treo tasks. Starting polling... ---")
        logger.info(f"Bot initialization complete. Restored {restored_num} treo tasks. Starting polling...")
        init_duration = time.time() - start_time
        print(f"(Initialization + Restore took {init_duration:.2f} seconds)")

    application.post_init = run_restore_and_start

    # Chạy bot và xử lý tắt
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Stopping bot gracefully...")
        logger.info("KeyboardInterrupt detected. Stopping bot...")
    except Exception as e:
        print(f"\nCRITICAL ERROR: Bot stopped unexpectedly due to: {e}")
        logger.critical(f"CRITICAL ERROR: Bot stopped: {e}", exc_info=True)
    finally:
        print("\n--- Initiating Shutdown Sequence ---"); logger.info("Initiating shutdown sequence...")

        # --- Hủy các task treo đang chạy ---
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("[Shutdown] Could not get running event loop. Skipping async task cancellation.")
            loop = None

        if loop and loop.is_running():
            tasks_to_stop_on_shutdown = []
            if active_treo_tasks:
                logger.info("[Shutdown] Collecting active runtime treo tasks...")
                # Lặp qua bản sao để tránh lỗi RuntimeError: dictionary changed size during iteration
                active_tasks_snapshot = {}
                try:
                    active_tasks_snapshot = {
                        uid: dict(targets) for uid, targets in active_treo_tasks.items()
                    }
                except Exception as e_snap:
                    logger.error(f"[Shutdown] Error creating snapshot of active tasks: {e_snap}")

                for targets in active_tasks_snapshot.values():
                    for task in targets.values():
                        if task and isinstance(task, asyncio.Task) and not task.done():
                            tasks_to_stop_on_shutdown.append(task)

            if tasks_to_stop_on_shutdown:
                print(f"[Shutdown] Found {len(tasks_to_stop_on_shutdown)} active tasks. Attempting cancellation (timeout: {SHUTDOWN_TASK_CANCEL_TIMEOUT}s)...")
                shutdown_task = loop.create_task(shutdown_async_tasks(tasks_to_stop_on_shutdown, timeout=SHUTDOWN_TASK_CANCEL_TIMEOUT))
                try:
                    # Chờ task shutdown hoàn thành hoặc timeout
                    # Sử dụng run_until_complete để chờ task bất đồng bộ từ context đồng bộ
                    loop.run_until_complete(asyncio.wait_for(shutdown_task, timeout=SHUTDOWN_TASK_CANCEL_TIMEOUT + 0.5))
                except asyncio.TimeoutError:
                    logger.warning("[Shutdown] Timeout waiting for shutdown_async_tasks to complete.")
                except Exception as e_wait_shutdown:
                     logger.error(f"[Shutdown] Error waiting for shutdown_async_tasks: {e_wait_shutdown}")
                print("[Shutdown] Task cancellation process finished.")
            else:
                print("[Shutdown] No active runtime treo tasks found to cancel.")
        else:
             print("[Shutdown] Event loop not running or unavailable. Cannot cancel async tasks.")

        # Lưu dữ liệu lần cuối (quan trọng!)
        print("[Shutdown] Attempting final data save..."); logger.info("Attempting final data save...")
        save_data()
        print("[Shutdown] Final data save attempt complete.")

        print("--- Bot has stopped. ---"); logger.info("Bot has stopped."); print(f"Shutdown timestamp: {datetime.now().isoformat()}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e_fatal:
        # Ghi lỗi nghiêm trọng ra console và log file
        fatal_timestamp = datetime.now().isoformat()
        error_message = f"\n--- {fatal_timestamp} ---\nFATAL ERROR preventing main execution: {e_fatal}\n"
        print(error_message)
        logging.critical(f"FATAL ERROR preventing main execution: {e_fatal}", exc_info=True)
        # Ghi traceback vào file riêng
        try:
            with open("fatal_error.log", "a", encoding='utf-8') as f:
                import traceback
                f.write(error_message)
                traceback.print_exc(file=f)
                f.write("-" * 50 + "\n")
        except Exception as e_log:
            print(f"Additionally, failed to write fatal error to log file: {e_log}")

