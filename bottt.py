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
import ssl

from telegram import Update, Message, InputMediaVideo # Thêm InputMediaVideo (mặc dù không dùng trực tiếp trong cách tiếp cận này)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError

# --- Configuration ---
# !!! SECURITY: Load secrets from environment variables or a secure config file !!!
# WARNING: Hardcoded fallbacks are insecure! Prefer environment variables.
BOT_TOKEN = os.environ.get("YOUR_BOT_TOKEN", "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8")
API_KEY = os.environ.get("YOUR_TIKTOK_API_KEY", "ngocanvip") # Single API Key for both /tim and /fl
# --- Choose ONE option for Group ID ---
# Option 1: Allow bot ONLY in a specific group
# ALLOWED_GROUP_ID = -1002191171631 # <--- Your Specific Group ID
# Option 2: Allow bot in ANY group (set to None)
ALLOWED_GROUP_ID = None # <--- Set to None to allow in any group

# WARNING: Hardcoded fallbacks are insecure! Prefer environment variables.
LINK_SHORTENER_API_KEY = os.environ.get("YOUR_YEUMONEY_TOKEN", "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7")
BLOGSPOT_URL_TEMPLATE = "https://khangleefuun.blogspot.com/2025/04/key-ngay-body-font-family-arial-sans_11.html?m=1&ma={key}" # Link đích chứa key
LINK_SHORTENER_API_BASE_URL = "https://yeumoney.com/QL_api.php" # API Yeumoney

# --- URL Video để chèn vào phản hồi ---
# Sử dụng link bạn cung cấp, LƯU Ý: link này có thể hết hạn
VIDEO_URL_FOR_REPLIES = "https://v16m-default.tiktokcdn.com/7d38a57e16fdd96d941f859bd7a7ba22/67ffc8be/video/tos/alisg/tos-alisg-pve-0037/o4ALDE6KghqQErHEeEjtCf7RkzV4veIJ6UPycA/?a=0&bti=OUBzOTg7QGo6OjZAL3AjLTAzYCMxNDNg&ch=0&cr=0&dr=0&er=0&lr=all&net=0&cd=0%7C0%7C0%7C0&cv=1&br=4136&bt=2068&cs=0&ds=6&ft=EeF4ntZWD03Q12NvVPLXeIxRSfYFpq_45SY&mime_type=video_mp4&qs=0&rc=ZmZkPDc5ZmVkOjs6ZTw4N0BpM3Q7cTw6Zm94bjMzODgzNEAwLzQvMzZfNjExYTZjXl9hYSMvczNrcjRfcmVgLS1kLy1zcw%3D%3D&vvpl=1&l=20250416171142D38BDDC012C5062710C9&btag=e000b8000"
VIDEO_CAPTION_LIMIT = 1024 # Giới hạn ký tự caption của Telegram

# --- API Endpoints & URLs ---
VIDEO_API_URL_TEMPLATE = "https://nvp310107.x10.mx/tim.php?video_url={video_url}&key={api_key}"
FOLLOW_API_URL_TEMPLATE = "http://www.ngocanfreekey.x10.mx/ftik.php?username=@{username}&key={api_key}"

# --- Time Settings ---
TIM_FL_COOLDOWN_SECONDS = 15 * 60 # 15 minutes
GETKEY_COOLDOWN_SECONDS = 2 * 60  # 2 minutes
KEY_EXPIRY_SECONDS = 12 * 3600   # 12 hours (Unused Key)
ACTIVATION_DURATION_SECONDS = 12 * 3600 # 12 hours (After activation)
CLEANUP_INTERVAL_SECONDS = 3600 # 1 hour

# --- Storage ---
DATA_FILE = "bot_persistent_data.json"

# --- Global Variables ---
user_tim_cooldown = {}
user_fl_cooldown = {}
user_getkey_cooldown = {}
valid_keys = {}
activated_users = {}

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# Reduce log noise from libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# Increase telegram log level slightly for job queue info etc.
logging.getLogger("telegram.ext").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration Checks ---
# (Giữ nguyên phần kiểm tra cấu hình)
if not BOT_TOKEN or BOT_TOKEN == "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8":
    logger.warning("!!! WARNING: Using fallback BOT_TOKEN. Set YOUR_BOT_TOKEN env var for security! !!!")
if not API_KEY or API_KEY == "ngocanvip":
    logger.warning("!!! WARNING: Using fallback API_KEY (nngocanvip). Set YOUR_TIKTOK_API_KEY env var. !!!")
if not LINK_SHORTENER_API_KEY or LINK_SHORTENER_API_KEY == "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7":
    logger.warning("!!! WARNING: Using fallback LINK_SHORTENER_API_KEY. Set YOUR_YEUMONEY_TOKEN env var. !!!")

if "ngocanfreekey.x10.mx" in FOLLOW_API_URL_TEMPLATE and FOLLOW_API_URL_TEMPLATE.startswith("http://"):
    logger.warning("!!! SECURITY WARNING: The /fl API endpoint uses HTTP, not HTTPS. Communication is NOT encrypted. !!!")

if ALLOWED_GROUP_ID: logger.info(f"Bot restricted to Group ID: {ALLOWED_GROUP_ID}")
else: logger.info("Bot configured to run in ANY group.")

# --- Data Persistence Functions ---
# (Giữ nguyên các hàm load/save data)
def save_data():
    string_key_activated_users = {str(k): v for k, v in activated_users.items()}
    string_key_tim_cooldown = {str(k): v for k, v in user_tim_cooldown.items()}
    string_key_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in user_fl_cooldown.items()}
    string_key_getkey_cooldown = {str(k): v for k, v in user_getkey_cooldown.items()}
    data_to_save = {
        "valid_keys": valid_keys,
        "activated_users": string_key_activated_users,
        "user_cooldowns": {
            "tim": string_key_tim_cooldown, "fl": string_key_fl_cooldown, "getkey": string_key_getkey_cooldown
        }
    }
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f: json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        logger.debug(f"Data saved to {DATA_FILE}")
    except Exception as e: logger.error(f"Failed to save data to {DATA_FILE}: {e}", exc_info=True)

def load_data():
    global valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                valid_keys = data.get("valid_keys", {})
                # Ensure keys are strings for activated_users and cooldowns after loading
                raw_activated = data.get("activated_users", {})
                activated_users = {str(k): v for k, v in raw_activated.items()}

                all_cooldowns = data.get("user_cooldowns", {})
                raw_tim_cd = all_cooldowns.get("tim", {})
                user_tim_cooldown = {str(k): v for k, v in raw_tim_cd.items()}

                raw_fl_cd = all_cooldowns.get("fl", {})
                user_fl_cooldown = {str(uid): {uname: ts for uname, ts in udict.items()} for uid, udict in raw_fl_cd.items()}

                raw_getkey_cd = all_cooldowns.get("getkey", {})
                user_getkey_cooldown = {str(k): v for k, v in raw_getkey_cd.items()}

                logger.info(f"Data loaded from {DATA_FILE}")
                logger.info(f"Loaded {len(valid_keys)} pending keys, {len(activated_users)} activated users.")
                logger.debug(f"Loaded cooldowns: /tim={len(user_tim_cooldown)}, /fl={len(user_fl_cooldown)}, /getkey={len(user_getkey_cooldown)}")
        else:
            logger.info(f"{DATA_FILE} not found, initializing empty data structures.")
            valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}
    except (json.JSONDecodeError, Exception) as e:
        logger.error(f"Failed to load or parse {DATA_FILE}: {e}. Initializing empty data.", exc_info=True)
        valid_keys, activated_users, user_tim_cooldown, user_fl_cooldown, user_getkey_cooldown = {}, {}, {}, {}, {}


# --- Helper Functions ---
async def delete_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: int | None = None):
    """Safely attempts to delete a message."""
    msg_id_to_delete = message_id or (update.message.message_id if update and update.message else None)
    original_chat_id = update.effective_chat.id if update and update.effective_chat else None
    if not msg_id_to_delete or not original_chat_id: return
    try:
        await context.bot.delete_message(chat_id=original_chat_id, message_id=msg_id_to_delete)
        logger.debug(f"Deleted message {msg_id_to_delete} in chat {original_chat_id}")
    except (BadRequest, Forbidden) as e:
        # Ignore common non-critical errors
        if "Message to delete not found" in str(e) or \
           "message can't be deleted" in str(e) or \
           "MESSAGE_ID_INVALID" in str(e) or \
           "Not enough rights" in str(e): # Bot might not have delete permissions
            logger.debug(f"Non-critical error deleting message {msg_id_to_delete}: {e}")
        else:
            logger.error(f"BadRequest/Forbidden deleting message {msg_id_to_delete}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error deleting message {msg_id_to_delete}: {e}", exc_info=True)

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Job scheduled to delete a message later."""
    job_data = context.job.data
    chat_id = job_data.get('chat_id')
    message_id = job_data.get('message_id')
    job_name = context.job.name or "UnnamedDeleteJob"
    if chat_id and message_id:
        logger.debug(f"Job '{job_name}' running to delete message {message_id} in chat {chat_id}")
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except (BadRequest, Forbidden) as e:
            # Log common issues as info/debug, others as errors
            if "Message to delete not found" in str(e) or "message can't be deleted" in str(e):
                 logger.info(f"Job '{job_name}' could not delete message {message_id} (likely already gone): {e}")
            elif "Not enough rights" in str(e):
                 logger.info(f"Job '{job_name}' could not delete message {message_id} (no permission): {e}")
            else:
                 logger.error(f"Job '{job_name}' BadRequest/Forbidden deleting message {message_id}: {e}")
        except Exception as e:
            logger.error(f"Job '{job_name}' unexpected error deleting message {message_id}: {e}", exc_info=True)
    else:
        logger.warning(f"Job '{job_name}' called missing chat_id or message_id. Data: {job_data}")

async def send_response(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    video_url: str | None = VIDEO_URL_FOR_REPLIES, # Default to the global video URL
    processing_msg_id: int | None = None,
    original_user_msg_id: int | None = None,
    parse_mode: str = ParseMode.HTML,
    disable_web_page_preview: bool = True,
    reply_to_message: bool = False,
    prefer_video: bool = False # Flag to indicate if this response *should* be a video if possible
) -> Message | None:
    """
    Sends a response, preferring video+caption if specified and possible,
    otherwise sends text. Handles editing/deleting previous messages.
    """
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id if update.effective_user else "N/A"
    sent_message = None
    final_text = text
    can_send_video = prefer_video and video_url and len(final_text) <= VIDEO_CAPTION_LIMIT

    # 1. Delete processing message if it exists and we're sending a new final message (video or text)
    if processing_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=processing_msg_id)
            logger.debug(f"Deleted processing message {processing_msg_id}")
            processing_msg_id = None # Indicate it's gone
        except Exception as e:
            logger.warning(f"Could not delete processing message {processing_msg_id}: {e}")
            # Continue anyway, maybe it was already deleted

    # 2. Try sending Video + Caption if preferred and possible
    if can_send_video:
        try:
            reply_to_msg_id = update.message.message_id if reply_to_message and update and update.message else None
            sent_message = await context.bot.send_video(
                chat_id=chat_id,
                video=video_url,
                caption=final_text,
                parse_mode=parse_mode,
                reply_to_message_id=reply_to_msg_id
            )
            logger.info(f"Sent video response to user {user_id}")
        except (BadRequest, Forbidden, NetworkError) as e:
            logger.warning(f"Failed to send video (Caption len: {len(final_text)}). Error: {e}. Falling back to text.")
            can_send_video = False # Fallback to text
        except Exception as e:
            logger.error(f"Unexpected error sending video to user {user_id}", exc_info=True)
            can_send_video = False # Fallback to text

    # 3. Send Text if video wasn't preferred, failed, or caption was too long
    if not can_send_video:
         # Truncate text if it exceeds Telegram limits
        if len(final_text) > 4096:
            truncated = final_text[:4050].rstrip()
            # Try to close dangling tags if any
            if '<blockquote>' in truncated and '</blockquote>' not in truncated[truncated.rfind('<blockquote>'):]:
                truncated += "</blockquote>"
            final_text = truncated + "...\n<i>(Nội dung bị cắt bớt)</i>"

        reply_to_msg_id = update.message.message_id if reply_to_message and update and update.message else None

        try:
            # Send a new text message (since processing msg was deleted or never existed)
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=final_text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                reply_to_message_id=reply_to_msg_id
            )
            logger.info(f"Sent new text message to user {user_id}")
        except BadRequest as e:
            if "Can't parse entities" in str(e):
                 logger.warning(f"HTML parsing error ('{e}'), sending as plain text.")
                 plain_text = re.sub(r'<[^>]+>', '', text) # Basic strip tags
                 plain_text = f"⚠️ Lỗi hiển thị định dạng.\nNội dung:\n{plain_text}"[:4090]
                 try:
                     sent_message = await context.bot.send_message(
                         chat_id=chat_id,
                         text=plain_text,
                         disable_web_page_preview=True,
                         reply_to_message_id=reply_to_msg_id
                     )
                 except Exception as pt_fallback_e:
                     logger.error(f"Error sending plain text fallback: {pt_fallback_e}", exc_info=True)
            else:
                 logger.error(f"BadRequest sending text (Chat: {chat_id}, User: {user_id}): {e}")
        except Exception as e:
            logger.error(f"Unexpected error sending text (Chat: {chat_id}, User: {user_id}): {e}", exc_info=True)

    # 4. Delete original user message (if requested and response was sent successfully)
    if original_user_msg_id and not reply_to_message and sent_message:
        await delete_user_message(update, context, original_user_msg_id)
    elif original_user_msg_id and not reply_to_message and not sent_message:
         logger.warning(f"Not deleting original message {original_user_msg_id} because sending response failed.")

    return sent_message


def generate_random_key(length=8):
    """Generates a random key string."""
    return f"Dinotool-{''.join(random.choices(string.ascii_letters + string.digits, k=length))}"

async def cleanup_expired_data(context: ContextTypes.DEFAULT_TYPE):
    """Job to clean up expired keys and user activations."""
    global valid_keys, activated_users
    current_time = time.time()
    keys_to_remove = []
    users_to_deactivate = []
    data_changed = False
    logger.debug("[Cleanup] Starting expired data check...")

    # Check expired UNUSED keys
    for key, data in list(valid_keys.items()):
        try:
            # Only remove if NOT used and expired
            if data.get("used_by") is None and current_time > float(data.get("expiry_time", 0)):
                keys_to_remove.append(key)
        except (ValueError, TypeError, AttributeError):
            logger.warning(f"[Cleanup] Found malformed key data for '{key}'. Scheduling for removal.")
            keys_to_remove.append(key) # Remove malformed entries

    # Check expired ACTIVATED users
    for user_id_str, expiry_timestamp_str in list(activated_users.items()):
        try:
            if current_time > float(expiry_timestamp_str):
                users_to_deactivate.append(user_id_str)
        except (ValueError, TypeError):
            logger.warning(f"[Cleanup] Found malformed activation timestamp for user '{user_id_str}'. Scheduling for removal.")
            users_to_deactivate.append(user_id_str) # Remove malformed entries

    if keys_to_remove:
        for key in keys_to_remove:
            if key in valid_keys:
                del valid_keys[key]
                logger.info(f"[Cleanup] Removed expired/invalid unused key: {key}")
                data_changed = True

    if users_to_deactivate:
        for user_id_str in users_to_deactivate:
            if user_id_str in activated_users:
                del activated_users[user_id_str]
                logger.info(f"[Cleanup] Deactivated user due to expired/invalid activation: {user_id_str}")
                data_changed = True

    if data_changed:
        logger.info("[Cleanup] Data changed, saving state.")
        save_data()
    else:
        logger.debug("[Cleanup] No expired data found.")

def is_user_activated(user_id: int) -> bool:
    """Checks if a user is currently activated."""
    user_id_str = str(user_id)
    expiry_time_str = activated_users.get(user_id_str)
    if expiry_time_str:
        try:
            if time.time() < float(expiry_time_str):
                return True # Still active
            else:
                # Expired, remove them now
                logger.info(f"User {user_id_str} activation expired. Removing.")
                del activated_users[user_id_str]
                save_data() # Save change immediately
                return False # Expired
        except (ValueError, TypeError):
             logger.warning(f"Invalid activation timestamp for user {user_id_str}. Removing.")
             del activated_users[user_id_str]
             save_data() # Save change immediately
             return False # Invalid data counts as not activated
    return False # Not in the dictionary


# --- Command Handlers ---
async def lenh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /lenh command (formerly /start)."""
    if not update or not update.message: return
    user = update.effective_user
    chat_type = update.effective_chat.type
    is_allowed_chat = (ALLOWED_GROUP_ID is None) or (chat_type == 'private') or (update.effective_chat.id == ALLOWED_GROUP_ID)

    if not is_allowed_chat:
        logger.info(f"User {user.id} tried /lenh in unauthorized chat {update.effective_chat.id}. Ignoring.")
        # Optionally delete their command if in a group it shouldn't be in
        if chat_type != 'private' and update.effective_chat.id != ALLOWED_GROUP_ID:
             await delete_user_message(update, context)
        return

    # Calculate durations for display
    act_h = ACTIVATION_DURATION_SECONDS // 3600
    key_exp_h = KEY_EXPIRY_SECONDS // 3600
    tf_cd_m = TIM_FL_COOLDOWN_SECONDS // 60
    gk_cd_m = GETKEY_COOLDOWN_SECONDS // 60

    group_note = "\n<i>ℹ️ Bot hoạt động trong mọi nhóm và chat riêng tư.</i>" if ALLOWED_GROUP_ID is None else f"\n<i>ℹ️ Bot chỉ hoạt động trong nhóm được chỉ định và chat riêng tư.</i>"

    msg = (
        f"<blockquote>👋 <b>Xin chào {user.mention_html()}!</b>\n\n"
        f"🤖 Đây là Bot hỗ trợ TikTok của <a href='https://t.me/dinotool'>DinoTool</a>.{group_note}\n\n"
        f"✨ <b>Quy trình sử dụng:</b>\n"
        f"1️⃣ Dùng lệnh <code>/getkey</code> để nhận link lấy key.\n"
        f"2️⃣ Truy cập link, làm theo hướng dẫn để nhận Key.\n"
        f"3️⃣ Kích hoạt bằng lệnh <code>/nhapkey <key_cua_ban></code>.\n"
        f"4️⃣ Sử dụng các lệnh <code>/tim</code>, <code>/fl</code> trong <b>{act_h} giờ</b>.\n\n"
        f"📜 <b>Danh sách lệnh:</b>\n"
        f"🔑 <code>/getkey</code> - Lấy link tạo key (⏳ {gk_cd_m} phút/lần).\n"
        f"⚡️ <code>/nhapkey <key></code> - Kích hoạt bot bằng key (Key chưa dùng hiệu lực {key_exp_h}h).\n"
        f"❤️ <code>/tim <link_video></code> - Tăng tim video (Yêu cầu kích hoạt, ⏳ {tf_cd_m} phút/lần).\n"
        f"👥 <code>/fl <username></code> - Tăng follow tài khoản (Yêu cầu kích hoạt, ⏳ {tf_cd_m} phút/user).\n"
        f"🆘 <code>/lenh</code> - Hiển thị hướng dẫn này."
        f"</blockquote>"
    )

    # Gửi video kèm caption thay vì reply_html
    await send_response(
        update,
        context,
        text=msg,
        video_url=VIDEO_URL_FOR_REPLIES,
        original_user_msg_id=None, # Don't delete user's /lenh command
        reply_to_message=True,     # Reply to the user's command
        prefer_video=True          # Try to send as video
    )

async def tim_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /tim command."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)
    processing_msg: Message | None = None # To store the 'processing' message

    # 1. Check Group (if configured) & Delete if wrong group
    if ALLOWED_GROUP_ID and chat_id != ALLOWED_GROUP_ID:
        logger.warning(f"/tim by {user_id} in wrong group {chat_id}. Deleting.")
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Check Activation Status
    if not is_user_activated(user_id):
        act_msg = (f"<blockquote>⚠️ {user.mention_html()}, bạn cần kích hoạt bot!\n"
                   f"➡️ Dùng: <code>/getkey</code> » Lấy Key » <code>/nhapkey <key></code>.</blockquote>")
        # Send activation required message (text only)
        sent_msg = await send_response(
            update, context, act_msg,
            original_user_msg_id=original_message_id,
            prefer_video=False # No video for errors/warnings
        )
        # Schedule deletion of the warning message
        if sent_msg and context.job_queue:
            context.job_queue.run_once(delete_message_job, 20, data={'chat_id': chat_id, 'message_id': sent_msg.message_id}, name=f"del_act_tim_{sent_msg.message_id}")
        return

    # 3. Check Cooldown
    last_usage_str = user_tim_cooldown.get(user_id_str)
    if last_usage_str:
        try:
            last_usage_time = float(last_usage_str)
            elapsed = current_time - last_usage_time
            if elapsed < TIM_FL_COOLDOWN_SECONDS:
                remaining = TIM_FL_COOLDOWN_SECONDS - elapsed
                cd_msg = f"<blockquote>⏳ {user.mention_html()}, bạn cần chờ thêm <b>{remaining:.0f} giây</b> để dùng lệnh <code>/tim</code>.</blockquote>"
                # Send cooldown message (text only) and delete original
                sent_cd = await send_response(update, context, cd_msg, original_user_msg_id=original_message_id, prefer_video=False)
                # Schedule deletion of the cooldown message
                if sent_cd and context.job_queue:
                    context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd.message_id}, name=f"del_cd_tim_{sent_cd.message_id}")
                return
        except (ValueError, TypeError):
            logger.warning(f"Invalid /tim cooldown timestamp for user {user_id}. Resetting.")
            if user_id_str in user_tim_cooldown: del user_tim_cooldown[user_id_str] # Remove invalid entry
            # Don't save data here, let it save on success or next cleanup

    # 4. Parse Input URL
    args = context.args
    video_url = None
    err_txt = None
    if not args:
        err_txt = ("<blockquote>⚠️ Vui lòng cung cấp link video TikTok.\n"
                   "Ví dụ: <code>/tim https://www.tiktok.com/...</code></blockquote>")
    elif "tiktok.com" not in args[0] or not (args[0].startswith("https://") or args[0].startswith("http://")): # Allow http just in case
        err_txt = f"<blockquote>⚠️ Link không hợp lệ. Phải là link video TikTok.\nCung cấp: <code>{html.escape(args[0][:100])}...</code></blockquote>"
    else:
        video_url = args[0] # Use the first argument as the URL

    if err_txt:
        # Send input error message (text only) and delete original
        sent_err = await send_response(update, context, err_txt, original_user_msg_id=original_message_id, prefer_video=False)
        # Schedule deletion of the error message
        if sent_err and context.job_queue:
            context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err.message_id}, name=f"del_inp_tim_{sent_err.message_id}")
        return

    # 5. Check API Key Configuration
    if not API_KEY:
        logger.error("/tim command failed: API_KEY is not configured.")
        await send_response(update, context, "<blockquote>❌ Lỗi cấu hình Bot: Thiếu API Key cho chức năng Tim. Vui lòng báo Admin.</blockquote>", original_user_msg_id=original_message_id, prefer_video=False)
        return

    # 6. Call API
    api_url = VIDEO_API_URL_TEMPLATE.format(video_url=video_url, api_key=API_KEY)
    logger.info(f"User {user_id} calling /tim API for URL: {video_url[:50]}...")
    final_response_text = ""
    success = False # Flag to track if API call was successful

    try:
        # Send "Processing" message (text only)
        processing_msg = await update.message.reply_html("<blockquote>⏳ <b>Đang xử lý yêu cầu Tim ❤️...</b> Vui lòng đợi!</blockquote>")

        # Make the API call
        async with httpx.AsyncClient(verify=ssl.create_default_context(), timeout=60.0) as client: # Ensure verify=True or provide context
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot Tim/1.3'}) # Version bump
            logger.info(f"/tim API Status: {resp.status_code} for user {user_id}")
            resp.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

            content_type = resp.headers.get("content-type","").lower()
            response_text_preview = resp.text[:500] # For logging/debugging non-JSON responses
            logger.debug(f"/tim Content-Type: {content_type}, Preview: {response_text_preview}")

            if "application/json" in content_type:
                data = None
                try:
                    data = resp.json()
                    logger.debug(f"/tim Parsed JSON for user {user_id}: {data}")

                    if isinstance(data, dict) and data.get("success") is True:
                        # Extract data, assuming structure {success: true, data: {author, create_time, video_url, digg_before, digg_increased, digg_after}}
                        api_data = data.get("data")
                        if not isinstance(api_data, dict):
                             logger.error(f"/tim API Error (User {user_id}): 'data' field is not a dictionary. Value: {api_data}")
                             final_response_text = f"<blockquote>❌ Lỗi API Tim: Cấu trúc dữ liệu trả về không đúng.</blockquote>"
                        else:
                            author = html.escape(str(api_data.get("author", "N/A")))
                            create_time = html.escape(str(api_data.get("create_time", "N/A")))
                            returned_video_url = html.escape(str(api_data.get("video_url", video_url))) # Use original if not returned
                            digg_before = html.escape(str(api_data.get('digg_before', '?')))
                            digg_increased = html.escape(str(api_data.get('digg_increased', '?')))
                            digg_after = html.escape(str(api_data.get('digg_after', '?')))

                            final_response_text = (
                                f"<blockquote>✅ <b>Yêu cầu Tim thành công!</b> ❤️\n\n"
                                f"📊 <b>Chi tiết:</b>\n"
                                f"🎬 <a href='{returned_video_url}'>Link Video</a>\n"
                                f"👤 Tác giả: <code>{author}</code>\n"
                                f"🗓️ Thời gian tạo: <code>{create_time}</code>\n"
                                f"📈 Lượt tim: <code>{digg_before}</code> ➜ <b style='color:green;'>+{digg_increased}</b> ❤️ ➜ <code>{digg_after}</code>"
                                f"</blockquote>"
                            )
                            # Update cooldown only on success
                            user_tim_cooldown[user_id_str] = time.time()
                            save_data()
                            success = True # Mark as successful
                    elif isinstance(data, dict) and data.get("success") is False:
                        api_msg = html.escape(data.get('message', 'API báo lỗi không rõ ràng.'))
                        logger.warning(f"/tim API logical error for user {user_id}. Message: {api_msg}. Full Data: {data}")
                        final_response_text = f"<blockquote>❌ <b>Yêu cầu Tim thất bại!</b>\n<i>Lý do từ API:</i> <code>{api_msg}</code></blockquote>"
                    else:
                        # JSON received, but structure is wrong (e.g., not a dict or 'success' field missing)
                        logger.error(f"/tim API JSON structure invalid for user {user_id}. Data: {data}")
                        final_response_text = f"<blockquote>❌ Lỗi API Tim: Cấu trúc phản hồi JSON không hợp lệ.</blockquote>"

                except json.JSONDecodeError as e:
                    logger.error(f"/tim JSON decode error for user {user_id}. Status: {resp.status_code}. Preview: {response_text_preview}. Error: {e}")
                    final_response_text = "<blockquote>❌ Lỗi đọc phản hồi từ API Tim (Không phải JSON hợp lệ).</blockquote>"
            else:
                # Response was not JSON
                logger.warning(f"/tim API response was not JSON for user {user_id}. Status: {resp.status_code}. Type: {content_type}. Preview: {response_text_preview}")
                final_response_text = f"<blockquote>❌ Lỗi API Tim: Máy chủ trả về định dạng không mong đợi (Không phải JSON).</blockquote>"

    except httpx.TimeoutException:
        logger.warning(f"/tim API call timed out for user {user_id}")
        final_response_text = "<blockquote>❌ Lỗi: Yêu cầu Tim tới API bị timeout. Vui lòng thử lại sau.</blockquote>"
    except httpx.HTTPStatusError as e:
        response_text = e.response.text[:500] if hasattr(e.response, 'text') else 'N/A'
        logger.error(f"/tim HTTP error for user {user_id}. Status: {e.response.status_code}. URL: {e.request.url}. Response: {response_text}")
        final_response_text = f"<blockquote>❌ Lỗi kết nối đến API Tim (Mã lỗi: {e.response.status_code}). Vui lòng thử lại sau hoặc báo Admin.</blockquote>"
    except httpx.RequestError as e: # Catches network errors (DNS, connection refused, etc.)
        logger.error(f"/tim network error for user {user_id}: {e}")
        final_response_text = f"<blockquote>❌ Lỗi mạng khi kết nối đến API Tim.\n<pre>{html.escape(str(e))}</pre></blockquote>"
    except Exception as e:
        logger.error(f"!!! Unexpected error during /tim for user {user_id} !!!", exc_info=True)
        final_response_text = "<blockquote>❌ Lỗi hệ thống không xác định xảy ra trong quá trình xử lý Tim. Vui lòng báo Admin.</blockquote>"

    # 7. Send Final Response (Video if successful, Text otherwise)
    # If successful, prefer_video=True. If error, prefer_video=False.
    await send_response(
        update, context, text=final_response_text,
        processing_msg_id=processing_msg.message_id if processing_msg else None,
        original_user_msg_id=original_message_id,
        prefer_video=success # Send video only on success
    )


# --- CORRECTED /fl COMMAND HANDLER ---
async def fl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /fl command."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)
    processing_msg: Message | None = None
    target_username = None
    success = False # Flag for API success

    # 1. Check Group
    if ALLOWED_GROUP_ID and chat_id != ALLOWED_GROUP_ID:
        logger.warning(f"/fl by {user_id} in wrong group {chat_id}. Deleting.")
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Check Activation
    if not is_user_activated(user_id):
        act_msg = (f"<blockquote>⚠️ {user.mention_html()}, bạn cần kích hoạt bot!\n"
                   f"➡️ Dùng: <code>/getkey</code> » Lấy Key » <code>/nhapkey <key></code>.</blockquote>")
        sent_msg = await send_response(update, context, act_msg, original_user_msg_id=original_message_id, prefer_video=False)
        if sent_msg and context.job_queue:
            context.job_queue.run_once(delete_message_job, 20, data={'chat_id': chat_id, 'message_id': sent_msg.message_id}, name=f"del_act_fl_{sent_msg.message_id}")
        return

    # 3. Parse Input Username
    args = context.args
    err_txt = None
    if not args:
        err_txt = ("<blockquote>⚠️ Vui lòng cung cấp username TikTok.\n"
                   "Ví dụ: <code>/fl tiktokuser</code></blockquote>")
    else:
        uname_raw = args[0].strip()
        # Remove leading '@' if present
        uname = uname_raw.lstrip("@")
        # Validate username format (basic check)
        if not uname:
            err_txt = "<blockquote>⚠️ Username không được để trống.</blockquote>"
        elif not re.match(r"^[a-zA-Z0-9_.]{2,24}$", uname) or uname.endswith('.'):
             # Example Regex: letters, numbers, underscore, period. Min 2, max 24 chars. Cannot end with period.
            err_txt = f"<blockquote>⚠️ Username <code>@{html.escape(uname)}</code> không hợp lệ. Vui lòng kiểm tra lại.</blockquote>"
        else:
            target_username = uname # Assign the cleaned, valid username

    if err_txt:
        sent_err = await send_response(update, context, err_txt, original_user_msg_id=original_message_id, prefer_video=False)
        if sent_err and context.job_queue:
            context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err.message_id}, name=f"del_inp_fl_{sent_err.message_id}")
        return

    # Ensure target_username was set (should be redundant due to above checks, but safe)
    if not target_username:
        logger.error(f"/fl user {user_id}: Logic error, target_username not set after validation.")
        await send_response(update, context, "<blockquote>❌ Lỗi logic nội bộ: Không thể xác định username.</blockquote>", original_user_msg_id=original_message_id, prefer_video=False); return

    # 4. Check Cooldown for this specific username
    user_specific_cooldowns = user_fl_cooldown.get(user_id_str, {})
    last_usage_str = user_specific_cooldowns.get(target_username)
    if last_usage_str:
        try:
            last_usage_time = float(last_usage_str)
            elapsed = current_time - last_usage_time
            if elapsed < TIM_FL_COOLDOWN_SECONDS:
                remaining = TIM_FL_COOLDOWN_SECONDS - elapsed
                cd_msg = f"<blockquote>⏳ {user.mention_html()}, bạn cần chờ thêm <b>{remaining:.0f} giây</b> để dùng lệnh <code>/fl</code> cho <code>@{html.escape(target_username)}</code>.</blockquote>"
                sent_cd = await send_response(update, context, cd_msg, original_user_msg_id=original_message_id, prefer_video=False)
                if sent_cd and context.job_queue:
                    context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd.message_id}, name=f"del_cd_fl_{sent_cd.message_id}")
                return
        except (ValueError, TypeError):
            logger.warning(f"Invalid /fl cooldown timestamp for user {user_id} - target {target_username}. Resetting.")
            if user_id_str in user_fl_cooldown and target_username in user_fl_cooldown[user_id_str]:
                 del user_fl_cooldown[user_id_str][target_username] # Remove invalid entry
                 # Save will happen on success or cleanup

    # 5. Check API Key Configuration
    if not API_KEY:
        logger.error("/fl command failed: API_KEY is not configured.")
        await send_response(update, context, "<blockquote>❌ Lỗi cấu hình Bot: Thiếu API Key cho chức năng Follow. Vui lòng báo Admin.</blockquote>", original_user_msg_id=original_message_id, prefer_video=False)
        return

    # 6. Call API
    # WARNING: Using HTTP endpoint if specified in template
    api_url = FOLLOW_API_URL_TEMPLATE.format(username=target_username, api_key=API_KEY)
    logger.info(f"User {user_id} calling /fl API for @{target_username} (URL: {api_url.split('?')[0]}...)")
    final_response_text = ""

    try:
        # Send "Processing" message
        processing_msg = await update.message.reply_html(f"<blockquote>⏳ <b>Đang xử lý yêu cầu Follow 👥 @{html.escape(target_username)}...</b> Vui lòng đợi!</blockquote>")

        # Make the API call
        # Determine verification based on URL scheme
        verify_ssl = api_url.startswith("https://")
        async with httpx.AsyncClient(verify=verify_ssl, timeout=60.0) as client:
            resp = await client.get(api_url, headers={'User-Agent': 'TG Bot Fl/1.4'}) # Version bump
            logger.info(f"/fl API Status: {resp.status_code} for @{target_username} (User {user_id})")
            resp.raise_for_status()

            content_type = resp.headers.get("content-type","").lower()
            response_text_preview = resp.text[:500]
            logger.debug(f"/fl Content-Type: {content_type}, Preview: {response_text_preview}")

            if "application/json" in content_type:
                data = None
                try:
                    data = resp.json()
                    logger.debug(f"/fl Parsed JSON for @{target_username} (User {user_id}): {data}")

                    # Assume FLAT JSON structure {success: bool, message: str, username: str, nickname: str, user_id: str, before: int, increase: int, after: int}
                    if isinstance(data, dict) and data.get("success") is True:
                        username_resp = html.escape(str(data.get("username", target_username))) # Use input if not returned
                        nickname = html.escape(str(data.get("nickname", "N/A")))
                        user_id_resp = html.escape(str(data.get("user_id", "N/A")))
                        followers_before = html.escape(str(data.get('before', '?')))
                        followers_increase = html.escape(str(data.get('increase', '?')))
                        followers_after = html.escape(str(data.get('after', '?')))

                        final_response_text = (
                            f"<blockquote>✅ <b>Yêu cầu Follow thành công!</b> 👥\n\n"
                            f"📊 <b>Chi tiết:</b>\n"
                            f"👤 Tài khoản: <code>@{username_resp}</code> (ID: <code>{user_id_resp}</code>)\n"
                            f"📛 Tên hiển thị: <b>{nickname}</b>\n"
                            f"📈 Lượt follow: <code>{followers_before}</code> ➜ <b style='color:green;'>+{followers_increase}</b> 👥 ➜ <code>{followers_after}</code>"
                            f"</blockquote>"
                        )
                        # Update cooldown ONLY on full success
                        try:
                            # Ensure the outer dictionary exists for the user
                            if user_id_str not in user_fl_cooldown:
                                user_fl_cooldown[user_id_str] = {}
                            # Set the cooldown for the specific target username
                            user_fl_cooldown[user_id_str][target_username] = time.time()
                            save_data()
                            logger.info(f"Cooldown updated for {user_id_str} - {target_username}")
                            success = True # Mark as successful
                        except Exception as cd_e:
                            logger.error(f"Failed to save /fl cooldown for {user_id}-{target_username}: {cd_e}", exc_info=True)
                            # Continue even if cooldown save fails, main operation succeeded
                            success = True # Still mark as successful for response

                    elif isinstance(data, dict) and data.get("success") is False:
                        api_msg = html.escape(data.get('message', 'API báo lỗi không rõ ràng.'))
                        logger.warning(f"/fl API logical error for @{target_username} (User {user_id}). Message: {api_msg}. Full Data: {data}")
                        final_response_text = f"<blockquote>❌ <b>Yêu cầu Follow thất bại</b> cho @{html.escape(target_username)}!\n<i>Lý do từ API:</i> <code>{api_msg}</code></blockquote>"
                    else:
                        logger.error(f"/fl API JSON structure invalid for @{target_username} (User {user_id}). Data: {data}")
                        final_response_text = f"<blockquote>❌ Lỗi API Follow: Cấu trúc phản hồi JSON không hợp lệ.</blockquote>"

                except json.JSONDecodeError as e:
                    logger.error(f"/fl JSON decode error for @{target_username} (User {user_id}). Status: {resp.status_code}. Preview: {response_text_preview}. Error: {e}")
                    final_response_text = "<blockquote>❌ Lỗi đọc phản hồi từ API Follow (Không phải JSON hợp lệ).</blockquote>"
            else:
                logger.warning(f"/fl API response was not JSON for @{target_username} (User {user_id}). Status: {resp.status_code}. Type: {content_type}. Preview: {response_text_preview}")
                final_response_text = f"<blockquote>❌ Lỗi API Follow: Máy chủ trả về định dạng không mong đợi (Không phải JSON).</blockquote>"

    except httpx.TimeoutException:
        logger.warning(f"/fl API call timed out for @{target_username} (User {user_id})")
        final_response_text = f"<blockquote>❌ Lỗi: Yêu cầu Follow tới API @{html.escape(target_username)} bị timeout. Vui lòng thử lại sau.</blockquote>"
    except httpx.HTTPStatusError as e:
        response_text = e.response.text[:500] if hasattr(e.response, 'text') else 'N/A'
        logger.error(f"/fl HTTP error for @{target_username} (User {user_id}). Status: {e.response.status_code}. URL: {e.request.url}. Response: {response_text}")
        final_response_text = f"<blockquote>❌ Lỗi kết nối đến API Follow (Mã lỗi: {e.response.status_code}). Vui lòng thử lại sau hoặc báo Admin.</blockquote>"
    except httpx.RequestError as e:
        logger.error(f"/fl network error for @{target_username} (User {user_id}): {e}")
        final_response_text = f"<blockquote>❌ Lỗi mạng khi kết nối đến API Follow.\n<pre>{html.escape(str(e))}</pre></blockquote>"
    except Exception as e:
        logger.error(f"!!! Unexpected error during /fl for @{target_username} (User {user_id}) !!!", exc_info=True)
        final_response_text = "<blockquote>❌ Lỗi hệ thống không xác định xảy ra trong quá trình xử lý Follow. Vui lòng báo Admin.</blockquote>"

    # 7. Send Final Response
    await send_response(
        update, context, text=final_response_text,
        processing_msg_id=processing_msg.message_id if processing_msg else None,
        original_user_msg_id=original_message_id,
        prefer_video=success # Send video only on success
    )


async def getkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /getkey command."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)
    processing_msg: Message | None = None
    success = False # Flag for success

    # 1. Check Group
    if ALLOWED_GROUP_ID and chat_id != ALLOWED_GROUP_ID:
        logger.warning(f"/getkey by {user_id} in wrong group {chat_id}. Deleting.")
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Check Cooldown
    last_usage_str = user_getkey_cooldown.get(user_id_str)
    if last_usage_str:
         try:
             last_usage_time = float(last_usage_str)
             elapsed = current_time - last_usage_time
             if elapsed < GETKEY_COOLDOWN_SECONDS:
                remaining = GETKEY_COOLDOWN_SECONDS - elapsed
                cd_msg = f"<blockquote>⏳ {user.mention_html()}, bạn cần chờ thêm <b>{remaining:.0f} giây</b> để dùng lệnh <code>/getkey</code>.</blockquote>"
                sent_cd = await send_response(update, context, cd_msg, original_user_msg_id=original_message_id, prefer_video=False)
                if sent_cd and context.job_queue:
                    context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_cd.message_id}, name=f"del_cd_getkey_{sent_cd.message_id}")
                return
         except (ValueError, TypeError):
             logger.warning(f"Invalid /getkey cooldown timestamp for user {user_id}. Resetting.")
             if user_id_str in user_getkey_cooldown: del user_getkey_cooldown[user_id_str]
             # Save will happen on success or cleanup

    # 3. Generate Key and Target URL
    generated_key = generate_random_key()
    while generated_key in valid_keys:
        logger.warning(f"Key collision detected: {generated_key}. Regenerating.")
        generated_key = generate_random_key()

    # Add timestamp/randomness to avoid caching issues on blogspot/target site if needed
    cache_buster = f"&_t={int(time.time())}{random.randint(100,999)}"
    target_url_with_key = BLOGSPOT_URL_TEMPLATE.format(key=generated_key) + cache_buster

    # 4. Check Link Shortener Configuration
    if not LINK_SHORTENER_API_KEY or not LINK_SHORTENER_API_BASE_URL:
        logger.error("/getkey command failed: Link shortener API key or URL is not configured.")
        await send_response(update, context, "<blockquote>❌ Lỗi cấu hình Bot: Thiếu thông tin API rút gọn link. Vui lòng báo Admin.</blockquote>", original_user_msg_id=original_message_id, prefer_video=False)
        return

    # 5. Call Link Shortener API
    shortener_params = {
        "token": LINK_SHORTENER_API_KEY,
        "format": "json",
        "url": target_url_with_key
    }
    log_params = shortener_params.copy() # For logging without exposing full token/sensitive parts
    log_params["token"] = f"...{LINK_SHORTENER_API_KEY[-4:]}" if LINK_SHORTENER_API_KEY else "N/A"
    log_params["url"] = target_url_with_key[:100] + ('...' if len(target_url_with_key)>100 else '') # Log truncated URL

    logger.info(f"User {user_id} requesting key. Generated: {generated_key}. Target: {target_url_with_key[:60]}...")
    final_response_text = ""
    key_temporarily_saved = False # Flag to track if key needs rollback on error

    try:
        # Send "Processing" message
        processing_msg = await update.message.reply_html("<blockquote>⏳ <b>Đang tạo link lấy key...</b> 🔑 Vui lòng đợi!</blockquote>")

        # Temporarily save the key *before* calling the shortener
        # This prevents a race condition where user might get key before shortener finishes/fails
        gen_time = time.time()
        exp_time = gen_time + KEY_EXPIRY_SECONDS
        valid_keys[generated_key] = {
            "user_id_generator": user_id,
            "generation_time": gen_time,
            "expiry_time": exp_time,
            "used_by": None # Mark as unused initially
        }
        key_temporarily_saved = True
        save_data() # Save immediately to persist the key state
        logger.info(f"Key {generated_key} temporarily saved for user {user_id}.")

        # Call the shortener API
        logger.debug(f"Calling shortener API: {LINK_SHORTENER_API_BASE_URL} with params: {log_params}")
        async with httpx.AsyncClient(timeout=30.0, verify=True, follow_redirects=True) as client: # Ensure HTTPS verification
            headers = {'User-Agent': 'TG Bot KeyGen/1.3'} # Version bump
            response = await client.get(LINK_SHORTENER_API_BASE_URL, params=shortener_params, headers=headers)

            status_code = response.status_code
            content_type = response.headers.get("content-type", "N/A").lower()
            response_text_preview = response.text[:500]
            logger.info(f"Shortener API Status: {status_code}, Type: {content_type} for user {user_id}")
            logger.debug(f"Shortener Response Preview: {response_text_preview}")
            response.raise_for_status() # Check for HTTP errors

            parsed_data = None
            try:
                parsed_data = response.json()
                logger.debug(f"Shortener Parsed JSON: {parsed_data}")
                api_status = parsed_data.get("status")
                short_url = parsed_data.get("shortenedUrl")

                if api_status == "success" and short_url and short_url.startswith(("http://", "https://")):
                    # Success! Update cooldown and prepare message
                    user_getkey_cooldown[user_id_str] = time.time()
                    save_data() # Save cooldown update
                    logger.info(f"Short link generated successfully for user {user_id}: {short_url}")
                    final_response_text = (
                        f"<blockquote>🚀 <b>Link lấy key của bạn đây:</b>\n\n"
                        f"🔗 <a href='{html.escape(short_url)}'>{html.escape(short_url)}</a>\n\n"
                        f"❓ <b>Hướng dẫn:</b>\n"
                        f"1️⃣ Click vào link trên.\n"
                        f"2️⃣ Làm theo các bước trên trang web để nhận Key (Key có dạng: <code>Dinotool-xxxx</code>).\n"
                        f"3️⃣ Quay lại bot và sử dụng lệnh: <code>/nhapkey <key_vua_nhan></code>\n\n"
                        f"⏳ <i>Lưu ý: Key cần được nhập vào bot trong vòng <b>{KEY_EXPIRY_SECONDS // 3600} giờ</b> kể từ lúc bạn nhận được link này.</i>"
                        f"</blockquote>"
                    )
                    key_temporarily_saved = False # Mark as permanent, no rollback needed
                    success = True # Mark as successful

                else:
                    # API reported failure or returned invalid data
                    api_msg = parsed_data.get("message", f"Trạng thái: {api_status}, URL trả về: {short_url}")
                    logger.error(f"Shortener API logical error for user {user_id}. Message: {api_msg}. Full Data: {parsed_data}")
                    final_response_text = f"<blockquote>❌ <b>Lỗi Tạo Link (API):</b>\n<code>{html.escape(str(api_msg))}</code>.\nVui lòng thử lại sau.</blockquote>"

            except json.JSONDecodeError as e:
                logger.error(f"Shortener JSON decode failed. Status: {status_code}, Type: {content_type}, Error: {e}. Preview: {response_text_preview}")
                final_response_text = f"<blockquote>❌ <b>Lỗi API Rút Gọn Link:</b> Phản hồi không đúng định dạng JSON. Vui lòng thử lại sau.</blockquote>"

    except httpx.TimeoutException:
        logger.warning(f"Shortener API call timed out for user {user_id}")
        final_response_text = "<blockquote>❌ <b>Lỗi Timeout:</b> API rút gọn link không phản hồi kịp thời. Vui lòng thử lại sau.</blockquote>"
    except httpx.HTTPStatusError as e:
        response_text = e.response.text[:500] if hasattr(e.response, 'text') else 'N/A'
        logger.error(f"Shortener HTTP error. Status: {e.response.status_code}. URL: {e.request.url}. Response: {response_text}")
        final_response_text = f"<blockquote>❌ <b>Lỗi API Rút Gọn Link</b> (Mã lỗi: {e.response.status_code}). Vui lòng thử lại sau hoặc báo Admin.</blockquote>"
    except httpx.RequestError as e:
        logger.error(f"Shortener network error for user {user_id}: {e}")
        final_response_text = f"<blockquote>❌ <b>Lỗi Mạng</b> khi kết nối đến API rút gọn link.\n<pre>{html.escape(str(e))}</pre></blockquote>"
    except Exception as e:
        logger.error(f"!!! Unexpected error during /getkey for user {user_id} !!!", exc_info=True)
        final_response_text = "<blockquote>❌ <b>Lỗi Hệ Thống Bot</b> xảy ra trong quá trình tạo key. Vui lòng báo Admin.</blockquote>"

    finally:
        # Rollback the key if it was temporarily saved but an error occurred
        if key_temporarily_saved and generated_key in valid_keys:
            logger.warning(f"Rolling back key {generated_key} due to error during shortener call.")
            del valid_keys[generated_key]
            save_data()

        # Send the final response (Video if successful, Text otherwise)
        await send_response(
            update, context, final_response_text,
            processing_msg_id=processing_msg.message_id if processing_msg else None,
            original_user_msg_id=original_message_id,
            disable_web_page_preview=False, # Allow preview for the short link
            prefer_video=success # Send video only on success
        )


async def nhapkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /nhapkey command."""
    if not update or not update.message: return
    chat_id = update.effective_chat.id
    user = update.effective_user
    user_id = user.id
    current_time = time.time()
    original_message_id = update.message.message_id
    user_id_str = str(user_id)
    success = False # Flag for successful activation

    # 1. Check Group
    if ALLOWED_GROUP_ID and chat_id != ALLOWED_GROUP_ID:
        logger.warning(f"/nhapkey by {user_id} in wrong group {chat_id}. Deleting.")
        await delete_user_message(update, context, original_message_id)
        return

    # 2. Parse Input Key
    args = context.args
    submitted_key = None
    err_txt = ""
    if not args:
        err_txt = ("<blockquote>⚠️ Vui lòng nhập Key bạn nhận được.\n"
                   "Ví dụ: <code>/nhapkey Dinotool-AbC123Xy</code></blockquote>")
    elif len(args) > 1:
        err_txt = "<blockquote>⚠️ Lỗi: Chỉ nhập <b>một</b> Key mỗi lần sử dụng lệnh.</blockquote>"
    else:
        key_input = args[0].strip()
        # Basic validation for key format "Prefix-Alphanumeric"
        if not key_input.startswith("Dinotool-") or len(key_input) <= len("Dinotool-"):
            err_txt = f"<blockquote>⚠️ Key <code>{html.escape(key_input)}</code> không đúng định dạng. Key phải bắt đầu bằng <code>Dinotool-</code>.</blockquote>"
        elif not key_input[len("Dinotool-"):].isalnum():
            err_txt = f"<blockquote>⚠️ Key <code>{html.escape(key_input)}</code> không hợp lệ. Phần phía sau <code>Dinotool-</code> chỉ được chứa chữ cái và số.</blockquote>"
        else:
            submitted_key = key_input # Valid format

    if err_txt:
        # Send input error message (text only) and delete original
        sent_err = await send_response(update, context, err_txt, original_user_msg_id=original_message_id, prefer_video=False)
        # Schedule deletion of the error message
        if sent_err and context.job_queue:
            context.job_queue.run_once(delete_message_job, 15, data={'chat_id': chat_id, 'message_id': sent_err.message_id}, name=f"del_err_nhapkey_{sent_err.message_id}")
        return

    # Ensure submitted_key is set (should be redundant)
    if not submitted_key:
         logger.error(f"/nhapkey user {user_id}: Logic error, submitted_key not set after validation.")
         await send_response(update, context, "<blockquote>❌ Lỗi logic nội bộ: Không thể xác định key nhập vào.</blockquote>", original_user_msg_id=original_message_id, prefer_video=False); return


    # 3. Validate Key against stored keys
    logger.info(f"User {user_id} attempting to activate with key: '{submitted_key}'")
    final_response_text = ""
    key_data = valid_keys.get(submitted_key)

    if not key_data:
        # Key does not exist in our records
        final_response_text = f"<blockquote>❌ Key <code>{html.escape(submitted_key)}</code> không hợp lệ hoặc không tồn tại. Vui lòng kiểm tra lại hoặc dùng <code>/getkey</code> để lấy key mới.</blockquote>"
    elif key_data.get("used_by") is not None:
        # Key exists but has already been used
        used_by_user_id = str(key_data["used_by"])
        if used_by_user_id == user_id_str:
            # User is trying to re-use their own key
            if is_user_activated(user_id): # Check if they are *still* active
                try:
                    expiry_timestamp = float(activated_users.get(user_id_str, 0))
                    expiry_dt = time.localtime(expiry_timestamp)
                    # Format time nicely, e.g., 15:30 15/04/2025
                    expiry_str = time.strftime('%H:%M %d/%m/%Y', expiry_dt)
                    final_response_text = f"<blockquote>✅ Bạn <b>đã được kích hoạt</b> và có thể sử dụng các lệnh.\nThời hạn đến: <b>{expiry_str}</b>.\nKhông cần nhập lại key này.</blockquote>"
                    success = True # Count as success because they are active
                except (ValueError, TypeError):
                     # Should not happen if is_user_activated worked, but handle anyway
                     final_response_text = f"<blockquote>✅ Bạn <b>đã được kích hoạt</b>. Không cần nhập lại key này.</blockquote>"
                     success = True
            else:
                # They were activated by this key before, but the activation expired
                final_response_text = f"<blockquote>❌ Key <code>{html.escape(submitted_key)}</code> đã được bạn sử dụng trước đây và thời hạn kích hoạt đã hết. Vui lòng dùng <code>/getkey</code> để lấy key mới.</blockquote>"
        else:
            # Key was used by someone else
            final_response_text = f"<blockquote>❌ Key <code>{html.escape(submitted_key)}</code> đã được sử dụng bởi một người dùng khác. Vui lòng dùng <code>/getkey</code> để lấy key mới.</blockquote>"
    else:
        # Key exists and has not been used yet, check its expiry time
        try:
            key_expiry_time = float(key_data.get("expiry_time", 0))
            if current_time > key_expiry_time:
                # Key has expired before being used
                try:
                    expiry_dt = time.localtime(key_expiry_time)
                    expiry_str = time.strftime('%H:%M %d/%m/%Y', expiry_dt)
                    final_response_text = f"<blockquote>❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn vào lúc {expiry_str} và không thể sử dụng được nữa. Vui lòng dùng <code>/getkey</code> để lấy key mới.</blockquote>"
                except: # Fallback if time formatting fails
                     final_response_text = f"<blockquote>❌ Key <code>{html.escape(submitted_key)}</code> đã hết hạn và không thể sử dụng được nữa. Vui lòng dùng <code>/getkey</code> để lấy key mới.</blockquote>"
                # Remove the expired, unused key from storage
                del valid_keys[submitted_key]
                save_data()
            else:
                # Key is valid, unused, and not expired - Activate the user!
                key_data["used_by"] = user_id # Mark the key as used by this user
                activation_expiry_time = current_time + ACTIVATION_DURATION_SECONDS
                activated_users[user_id_str] = activation_expiry_time # Store activation expiry
                save_data() # Save both key usage and activation status

                try:
                    expiry_dt = time.localtime(activation_expiry_time)
                    expiry_str = time.strftime('%H:%M %d/%m/%Y', expiry_dt)
                    activation_hours = ACTIVATION_DURATION_SECONDS // 3600
                    final_response_text = (
                        f"<blockquote>✅ <b>Kích hoạt thành công!</b>\n\n"
                        f"🔑 Key đã sử dụng: <code>{html.escape(submitted_key)}</code>\n"
                        f"✨ Bạn có thể sử dụng các lệnh <code>/tim</code> và <code>/fl</code>.\n"
                        f"⏳ Thời hạn sử dụng đến: <b>{expiry_str}</b> (Khoảng {activation_hours} giờ)."
                        f"</blockquote>"
                    )
                except: # Fallback if time formatting fails
                     final_response_text = (
                        f"<blockquote>✅ <b>Kích hoạt thành công!</b>\n\n"
                        f"🔑 Key đã sử dụng: <code>{html.escape(submitted_key)}</code>\n"
                        f"✨ Bạn có thể sử dụng các lệnh <code>/tim</code> và <code>/fl</code> trong {ACTIVATION_DURATION_SECONDS // 3600} giờ."
                        f"</blockquote>"
                     )
                success = True # Mark as successful activation
                logger.info(f"User {user_id} successfully activated using key {submitted_key}. Activation expires at {activation_expiry_time}")

        except (ValueError, TypeError):
            logger.error(f"Invalid key data found for key '{submitted_key}'. Removing key.", exc_info=True)
            final_response_text = f"<blockquote>❌ Lỗi dữ liệu với key <code>{html.escape(submitted_key)}</code>. Key này đã bị xóa. Vui lòng dùng <code>/getkey</code> để lấy key mới.</blockquote>"
            # Remove the problematic key entry
            if submitted_key in valid_keys: del valid_keys[submitted_key]
            save_data()

    # 4. Send Final Response
    await send_response(
        update, context, final_response_text,
        original_user_msg_id=original_message_id,
        prefer_video=success # Send video only on successful activation or if already active
    )


# --- Unknown Command Handler ---
async def unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles any command that isn't explicitly defined."""
    if not update or not update.message or not update.message.text or not update.message.text.startswith('/'):
        return # Ignore non-command messages

    command_text = update.message.text.split()[0].split('@')[0] # Extract command e.g., /xyz from /xyz@botname args
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    chat_type = update.effective_chat.type

    # List of known commands
    known_commands = {'/lenh', '/tim', '/fl', '/getkey', '/nhapkey'} # Updated list

    if command_text not in known_commands:
        logger.info(f"Received unknown command '{command_text}' from user {user_id} in chat {chat_id} (type: {chat_type}).")

        # Delete the unknown command message if it's in a group where the bot operates (either specific group or any group if ALLOWED_GROUP_ID is None)
        # Don't delete in private chats.
        should_delete = chat_type in ['group', 'supergroup'] and \
                        (ALLOWED_GROUP_ID is None or chat_id == ALLOWED_GROUP_ID)

        if should_delete:
            logger.debug(f"Deleting unknown command '{command_text}' message {update.message.message_id} in group {chat_id}.")
            await delete_user_message(update, context)
        # Optionally, you could send a message like "Unknown command. Use /lenh for help." but deleting is often cleaner in groups.


# --- Main Function ---
def main() -> None:
    """Starts the bot."""
    print("--- Bot Configuration ---")
    print(f"Bot Token: {'LOADED from Env.' if 'YOUR_BOT_TOKEN' in os.environ else ('!!! USING FALLBACK (Insecure) !!!' if BOT_TOKEN == '7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8' else 'LOADED (Hardcoded/Other)')}")
    print(f"Allowed Group ID: {ALLOWED_GROUP_ID if ALLOWED_GROUP_ID else 'None (Any Group)'}")
    print(f"Yeumoney Token: {'LOADED from Env.' if 'YOUR_YEUMONEY_TOKEN' in os.environ else ('!!! USING FALLBACK (Insecure) !!!' if LINK_SHORTENER_API_KEY == 'cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7' else 'LOADED (Hardcoded/Other)')}")
    print(f"TikTok API Key (Tim/Fl): {'LOADED from Env.' if 'YOUR_TIKTOK_API_KEY' in os.environ else ('!!! USING FALLBACK (Insecure) !!!' if API_KEY == 'ngocanvip' else 'LOADED (Hardcoded/Other)')}")
    print(f"/tim URL: {VIDEO_API_URL_TEMPLATE.split('?')[0]}")
    print(f"/fl URL: {FOLLOW_API_URL_TEMPLATE.split('?')[0]} {'(!!! HTTP WARNING !!!)' if FOLLOW_API_URL_TEMPLATE.startswith('http://') else ''}")
    print(f"Data File: {DATA_FILE}")
    print(f"Video for Replies: {VIDEO_URL_FOR_REPLIES[:70]}...")
    print(f"Key Expiry: {KEY_EXPIRY_SECONDS / 3600:.1f}h | Activation: {ACTIVATION_DURATION_SECONDS / 3600:.1f}h | Cleanup: {CLEANUP_INTERVAL_SECONDS / 60:.0f}m")
    print("-" * 25)
    # Simplified warning check based on fallback values
    using_fallback_token = BOT_TOKEN == "7416039734:AAHi1YS3uxLGg_KAyqddbZL8OxXB1wamga8" and "YOUR_BOT_TOKEN" not in os.environ
    using_fallback_tiktok_key = API_KEY == "ngocanvip" and "YOUR_TIKTOK_API_KEY" not in os.environ
    using_fallback_yeumoney_key = LINK_SHORTENER_API_KEY == "cb879a865cf502e831232d53bdf03813caf549906e1d7556580a79b6d422a9f7" and "YOUR_YEUMONEY_TOKEN" not in os.environ

    if using_fallback_token or using_fallback_tiktok_key or using_fallback_yeumoney_key:
        print("!!! WARNING: One or more fallback secrets are being used because environment variables are not set. This is INSECURE. Please set:")
        if using_fallback_token: print("  - YOUR_BOT_TOKEN")
        if using_fallback_tiktok_key: print("  - YOUR_TIKTOK_API_KEY")
        if using_fallback_yeumoney_key: print("  - YOUR_YEUMONEY_TOKEN")
        print("-" * 25)

    # Exit if critical secrets are *completely* missing (not even fallbacks)
    if not BOT_TOKEN or not API_KEY or not LINK_SHORTENER_API_KEY:
         print("!!! CRITICAL ERROR: One or more required tokens/keys (Bot Token, TikTok API Key, Yeumoney Token) are missing. Check environment variables and script configuration. !!!")
         exit(1)

    print("Loading saved data...")
    load_data()

    # Build Application with adjusted timeouts
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .job_queue(JobQueue())
        .pool_timeout(60)        # Timeout for getting updates
        .connect_timeout(30)     # Timeout for establishing connection
        .read_timeout(40)        # Timeout for reading response
        .write_timeout(40)       # Timeout for sending request
        .build()
    )


    # --- Filters for Command Handlers ---
    # Commands work in private chats OR in the allowed group (if set) OR any group (if not set)
    if ALLOWED_GROUP_ID:
        # Allow in private chats OR the specific group ID
        command_filter = filters.ChatType.PRIVATE | filters.Chat(chat_id=ALLOWED_GROUP_ID)
        # Filter for unknown commands: only apply in the specific group
        group_filter_for_unknown = filters.Chat(chat_id=ALLOWED_GROUP_ID)
    else:
        # Allow in private chats OR any group/supergroup
        command_filter = filters.ChatType.PRIVATE | filters.ChatType.GROUPS
        # Filter for unknown commands: apply in any group/supergroup
        group_filter_for_unknown = filters.ChatType.GROUPS

    # --- Register Handlers ---
    # Add Command Handlers with the calculated filter
    application.add_handler(CommandHandler("lenh", lenh_command, filters=command_filter)) # Changed from start
    application.add_handler(CommandHandler("getkey", getkey_command, filters=command_filter))
    application.add_handler(CommandHandler("nhapkey", nhapkey_command, filters=command_filter))
    application.add_handler(CommandHandler("tim", tim_command, filters=command_filter))
    application.add_handler(CommandHandler("fl", fl_command, filters=command_filter))

    # Add handler for unknown commands, only active in groups based on group_filter_for_unknown
    application.add_handler(MessageHandler(filters.COMMAND & group_filter_for_unknown, unknown_command_handler))

    # --- Schedule Periodic Job ---
    application.job_queue.run_repeating(
        cleanup_expired_data,
        interval=CLEANUP_INTERVAL_SECONDS,
        first=60, # Start after 1 minute
        name="cleanup_expired_data_job"
    )
    print(f"Scheduled data cleanup job running every {CLEANUP_INTERVAL_SECONDS / 60:.0f} minutes.")

    # --- Start Bot ---
    print("Bot is starting polling...")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except Exception as e:
        logger.critical(f"CRITICAL ERROR during polling: {e}", exc_info=True)
        print(f"\n--- CRITICAL POLLING ERROR: {e} ---")
    finally:
        print("\nBot polling stopped.")
        logger.info("Bot polling stopped.")
        print("Attempting final data save before exiting...")
        save_data()
        print("Final data save attempt complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n--- CRITICAL ERROR ON STARTUP OR DURING RUNTIME: {e} ---")
        logger.critical(f"CRITICAL ERROR AT MAIN LEVEL: {e}", exc_info=True)
        print("Attempting emergency data save...")
        try:
            save_data()
            print("Emergency data save attempt complete.")
        except Exception as save_e:
            print(f"Failed emergency data save: {save_e}")
            logger.error(f"Failed emergency data save: {save_e}", exc_info=True)
