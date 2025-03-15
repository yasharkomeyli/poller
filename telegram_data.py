import os
import datetime
import asyncio
import pytz
from telethon import TelegramClient, events
from pymongo import MongoClient
import jdatetime

# گرفتن مسیر یک سطح بالاتر از دایرکتوری فعلی (فرض بر این است که این فایل در telegram-box/poller قرار دارد)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# اتصال به MongoDB


mongo_client = MongoClient("mongodb://admin:Momgodbpass0200Yashar@mongo:27017/telegram_data?authSource=admin")
db = mongo_client["telegram_data"]
messages_collection = db["messages"]
chats_collection = db["chats"]

# ایجاد ایندکس‌ها
messages_collection.create_index([("message_id", 1)], unique=True)
chats_collection.create_index([("chat_id", 1)], unique=True)

# تنظیمات API تلگرام
API_ID = "22435091"
API_HASH = "da17125c6dd25732caad68a778f69568"
PHONE_NUMBER = "+989336531403"

client = TelegramClient('session_name', API_ID, API_HASH)
tehran_tz = pytz.timezone("Asia/Tehran")


def to_shamsi(dt):
    """
    تبدیل یک شی datetime به زمان منطقه تهران (naive) و سپس تبدیل به فرمت شمسی.
    """
    if dt:
        dt_tehran = dt.astimezone(tehran_tz)
        naive_dt = dt_tehran.replace(tzinfo=None)
        return jdatetime.datetime.fromgregorian(datetime=naive_dt).strftime("%Y-%m-%d %H:%M:%S")
    return None


async def update_chat_details(chat):
    """
    دریافت اطلاعات کاربر و دانلود عکس پروفایل در دایرکتوری
    profile_photos (در همان دایرکتوری فایل کد) و ذخیره نام فایل در MongoDB.
    """
    chat_id = chat.id if hasattr(chat, 'id') else None
    if not chat_id:
        return

    chat_username = getattr(chat, 'username', None)
    photos_dir = os.path.join(BASE_DIR, "profile_photos")
    os.makedirs(photos_dir, exist_ok=True)  # ایجاد دایرکتوری در صورت عدم وجود
    file_path = os.path.join(photos_dir, f"{chat_id}.jpg")
    profile_photo_file = None

    if hasattr(chat, 'photo') and chat.photo:
        try:
            profile_photo_file = f"{chat_id}.jpg"
            # اگر فایل قبلاً وجود داشت، حذف شود
            if os.path.exists(file_path):
                os.remove(file_path)
            await client.download_profile_photo(chat, file=file_path)
        except Exception as e:
            print(f"Error downloading profile photo for chat {chat_id}: {e}")
    else:
        if os.path.exists(file_path):
            os.remove(file_path)
        profile_photo_file = None

    chat_update_data = {
        "username": chat_username,
        "profile_photo": profile_photo_file
    }
    chats_collection.update_one({"chat_id": chat_id}, {"$set": chat_update_data}, upsert=True)


async def save_messages(chat_name, chat_id, messages):
    """
    ذخیره تمام پیام‌های دریافت شده (هم incoming و هم outgoing) و آپدیت اطلاعات چت (مانند آخرین پیام و تاریخ آن).
    """
    if not messages:
        return

    last_msg = None
    for msg in messages:
        if msg.date:
            if last_msg is None or msg.date > last_msg.date:
                last_msg = msg

    last_message_date = to_shamsi(last_msg.date) if last_msg and last_msg.date else None
    last_message_text = last_msg.text if last_msg and last_msg.text else ""
    chat_data = {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "last_message_date": last_message_date,
        "last_message_text": last_message_text
    }
    try:
        chats_collection.update_one({"chat_id": chat_id}, {"$set": chat_data}, upsert=True)
        print(f"Updated chat: {chat_name} - Last message at: {last_message_date}")
    except Exception as e:
        print(f"Chat update error: {e}")

    # پردازش همه پیام‌ها بدون توجه به جهت (incoming/outgoing)
    for msg in messages:
        await update_message_data(msg, chat_id, chat_name)


async def update_message_data(msg, chat_id, chat_name):
    """
    بررسی و ذخیره یا به‌روزرسانی پیام در دیتابیس.
    در صورت ویرایش پیام، تنها آپدیت انجام می‌شود.
    """
    existing = messages_collection.find_one({"message_id": msg.id})
    if existing:
        if msg.edit_date:
            handle_edited_message(existing, msg)
        return
    try:
        message_data = await build_message_object(msg, chat_id, chat_name)
        messages_collection.insert_one(message_data)
    except Exception as e:
        print("Error inserting message:", e)


def handle_edited_message(existing, msg):
    """
    در صورت ویرایش پیام، فیلد text آپدیت می‌شود.
    """
    text_list = existing.get("text", [])
    if isinstance(text_list, str):
        text_list = [text_list]
    if msg.text not in text_list:
        text_list.append(msg.text)
        edit_date_shamsi = to_shamsi(msg.edit_date) if msg.edit_date else None
        messages_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": {
                "text": text_list,
                "is_edited": True,
                "edit_date": edit_date_shamsi
            }}
        )


async def build_message_object(msg, chat_id, chat_name):
    """
    ساخت آبجکت پیام برای ذخیره در دیتابیس.
    تاریخ پیام و تاریخ ویرایش به صورت شمسی و زمان تهران ذخیره می‌شوند.
    تغییر جدید:
    - اگر پیام شامل ویس باشد (msg.voice)، داده باینری ویس دانلود شده و در فیلد text ذخیره شده و
      فیلد 'type' برابر "voice" تنظیم می‌شود.
    - اگر پیام شامل عکس باشد (msg.photo)، داده باینری عکس دانلود شده و در فیلد text ذخیره شده و
      فیلد 'type' برابر "image" تنظیم می‌شود.
    - اگر پیام شامل فایل (document) باشد (msg.document)، داده باینری فایل دانلود شده و در فیلد text ذخیره شده و
      فیلد 'type' برابر "file" تنظیم می‌شود.
    - در غیر این صورت، پیام متنی است؛ در این صورت فیلد text شامل متن پیام (در قالب لیست) ذخیره شده و
      فیلد 'type' برابر "text" تنظیم می‌شود.
    """
    if hasattr(msg, 'voice') and msg.voice:
        try:
            # دانلود داده باینری ویس
            voice_data = await client.download_media(msg.voice, file=bytes)
            text_field = voice_data
            message_type = "voice"
        except Exception as e:
            print("Error downloading voice for message", msg.id, ":", e)
            text_field = [msg.text] if msg.text else []
            message_type = "text"
    elif msg.photo:
        try:
            # دانلود داده باینری عکس
            photo_data = await client.download_media(msg.photo, file=bytes)
            text_field = photo_data
            message_type = "image"
        except Exception as e:
            print("Error downloading photo for message", msg.id, ":", e)
            text_field = [msg.text] if msg.text else []
            message_type = "text"
    elif hasattr(msg, 'document') and msg.document:
        try:
            # دانلود داده باینری فایل
            file_data = await client.download_media(msg.document, file=bytes)
            text_field = file_data
            message_type = "file"
        except Exception as e:
            print("Error downloading file for message", msg.id, ":", e)
            text_field = [msg.text] if msg.text else []
            message_type = "text"
    else:
        text_field = [msg.text] if msg.text else []
        message_type = "text"

    return {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "message_id": msg.id,
        "sender_id": msg.sender_id,
        "username": [],  # در صورت نیاز می‌توانید اطلاعات بیشتری اضافه کنید
        "sender_username": getattr(msg.sender, 'username', None),
        "is_outgoing": msg.out,
        "type": message_type,  # تنظیم نوع پیام: "text"، "image"، "voice" یا "file"
        "text": text_field,    # داده متنی یا باینری عکس/ویس/فایل
        "date": to_shamsi(msg.date) if msg.date else None,
        "reply_to_msg_id": msg.reply_to_msg_id,
        "is_edited": bool(msg.edit_date),
        "edit_date": to_shamsi(msg.edit_date) if msg.edit_date else None,
        "redFlag": False,
        "mantegh": [],
    }



async def initial_data_load():
    """
    در اولین اجرا، تمام چت‌ها و پیام‌های موجود (قدیمی) از تلگرام دریافت و ذخیره می‌شوند.
    """
    dialogs = await client.get_dialogs()
    for dialog in dialogs:
        chat = dialog.entity
        chat_id = chat.id
        chat_name = getattr(chat, "title", getattr(chat, "first_name", "Private Chat"))
        # ذخیره اطلاعات پروفایل و سایر جزئیات چت به صورت لینک
        await update_chat_details(chat)
        messages = await client.get_messages(chat_id, limit=100)
        if messages:
            await save_messages(chat_name, chat_id, messages)
    print("Initial data load completed.")


@client.on(events.NewMessage)
async def new_message_handler(event):
    """
    ذخیره پیام جدید و افزایش unread_count (فقط برای پیام‌های دریافتی)
    به همراه آپدیت آخرین پیام.
    اکنون پیام‌های ارسال شده (outgoing) نیز ذخیره می‌شوند.
    """
    msg = event.message
    chat = await event.get_chat()
    chat_id = chat.id
    chat_name = getattr(chat, "title", getattr(chat, "first_name", "Private Chat"))
    message_data = await build_message_object(msg, chat_id, chat_name)
    messages_collection.insert_one(message_data)

    update_fields = {
        "last_message_text": msg.text if msg.text else "",
        "last_message_date": to_shamsi(msg.date.astimezone(tehran_tz)) if msg.date else None
    }
    # افزایش unread_count فقط برای پیام‌های دریافتی (incoming)
    if not msg.out:
        update_data = {"$set": update_fields, "$inc": {"unread_count": 1}}
    else:
        update_data = {"$set": update_fields}
    chats_collection.update_one({"chat_id": chat_id}, update_data, upsert=True)
    print(f"🔵 New message in {chat_name} saved.")


@client.on(events.MessageEdited)
async def message_edited_handler(event):
    msg = event.message
    try:
        chat = await event.get_chat()
        chat_name = getattr(chat, "title", getattr(chat, "first_name", "Private Chat"))
        chat_id = chat.id
    except Exception as e:
        print("Error fetching chat info:", e)
        chat_name = "Unknown Chat"
        chat_id = event.chat_id
        chat = None

    if chat:
        await update_chat_details(chat)
    await update_message_data(msg, chat_id, chat_name)


@client.on(events.MessageDeleted)
async def message_deleted_handler(event):
    for msg_id in event.deleted_ids:
        messages_collection.update_one(
            {"message_id": msg_id},
            {"$set": {"redFlag": True}}
        )
        print(f"Message {msg_id} flagged as deleted.")


@client.on(events.MessageRead)
async def message_read_handler(event):
    """
    زمانی که کاربر پیام‌های یک چت را می‌خواند (مثلاً با باز کردن چت در تلگرام)،
    تعداد unread_count آن چت به 0 تنظیم می‌شود.
    """
    chat_id = getattr(event, 'chat_id', None)
    if chat_id is None:
        chat_id = getattr(event, 'peer_id', None)
    if chat_id is None:
        print("Unable to determine chat_id for MessageRead event")
        return

    chats_collection.update_one(
        {"chat_id": chat_id},
        {"$set": {"unread_count": 0}},
        upsert=True
    )
    print(f"Reset unread_count for chat {chat_id} due to read event.")


async def main():
    await client.start(PHONE_NUMBER)
    print("Connected as user. Starting initial data load...")
    await initial_data_load()
    print("Initial data load completed. Waiting for new messages and read events...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
