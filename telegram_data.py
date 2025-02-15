# scanner.py (Telethon Message Fetcher)
from telethon.sync import TelegramClient
from pymongo import MongoClient
import datetime
import asyncio
from pymongo import InsertOne
from pymongo.errors import DuplicateKeyError, BulkWriteError
import pytz
# MongoDB Configuration

# mongo_client = MongoClient("mongodb://localhost:27017/")

mongo_client = MongoClient("mongodb://mongo:27017/")

db = mongo_client["telegram_data"]
messages_collection = db["messages"]
chats_collection = db["chats"]

# Create Indexes
messages_collection.create_index([("message_id", 1)], unique=True)
chats_collection.create_index([("chat_id", 1)], unique=True)

# Telegram API Configuration
API_ID = "22435091"
API_HASH = "da17125c6dd25732caad68a778f69568"
phone_number = "+989336531403"


client = TelegramClient('session_name', API_ID, API_HASH)
tehran_tz = pytz.timezone("Asia/Tehran")


def save_messages(chat_name, chat_id, messages):
    # پیدا کردن جدیدترین پیام برای تعیین last_message_date
    last_msg = None
    for msg in messages:
        if msg.date:
            if last_msg is None or msg.date > last_msg.date:
                last_msg = msg  # ذخیره جدیدترین پیام چت

    # تبدیل تاریخ به منطقه زمانی تهران
    if last_msg and last_msg.date:
        last_message_date = last_msg.date.astimezone(tehran_tz)
        # استخراج متن پیام، اگر وجود داشته باشد
        last_message_text = last_msg.text if last_msg.text else ""
    else:
        last_message_date = None
        last_message_text = ""

    # به‌روزرسانی اطلاعات چت در دیتابیس
    chat_data = {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "last_message_date": last_message_date.strftime("%Y-%m-%d %H:%M:%S") if last_message_date else None,
        "last_message_text": last_message_text  # فیلد جدید برای ذخیره متن پیام
    }

    try:
        chats_collection.update_one(
            {"chat_id": chat_id},
            {"$set": chat_data},
            upsert=True
        )
        print(f"Updated chat: {chat_name} - Last message at: {last_message_date}")
    except Exception as e:
        print(f"Chat update error: {e}")

    # ذخیره پیام‌ها (همان منطق قبلی برای ذخیره پیام‌های تکی)
    fetched_ids = []
    for msg in messages:
        if msg.text:
            fetched_ids.append(msg.id)
            update_message_data(msg, chat_id, chat_name)

    # علامت‌گذاری پیام‌های حذف‌شده
    messages_collection.update_many(
        {"chat_id": chat_id, "message_id": {"$nin": fetched_ids}},
        {"$set": {"redFlag": True}}
    )

def update_message_data(msg, chat_id, chat_name):
    existing = messages_collection.find_one({"message_id": msg.id})

    if existing:
        if msg.edit_date:
            handle_edited_message(existing, msg)
        return

    try:
        message_data = build_message_object(msg, chat_id, chat_name)
        messages_collection.insert_one(message_data)
    except Exception:
        pass

def handle_edited_message(existing, msg):
    text_list = existing["text"]
    if isinstance(text_list, str):
        text_list = [text_list]

    if msg.text not in text_list:
        text_list.append(msg.text)
        messages_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": {"text": text_list, "is_edited": True, "edit_date": msg.edit_date.strftime("%Y-%m-%d %H:%M:%S")}}
        )

def build_message_object(msg, chat_id, chat_name):
    msg_date = msg.date.astimezone(tehran_tz) if msg.date else None
    edit_date = msg.edit_date.astimezone(tehran_tz) if msg.edit_date else None

    return {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "message_id": msg.id,
        "sender_id": msg.sender_id,
        "username": [],
        "sender_username": getattr(msg.sender, 'username', None),
        "is_outgoing": msg.out,
        "text": [msg.text],
        "date": msg_date.strftime("%Y-%m-%d %H:%M:%S") if msg_date else None,
        "reply_to_msg_id": msg.reply_to_msg_id,
        "is_edited": bool(msg.edit_date),
        "edit_date": edit_date.strftime("%Y-%m-%d %H:%M:%S") if edit_date else None,
        "redFlag": False,
        "mantegh": [],
    }

async def update_unread_counts():
        global client  # استفاده از کلاینت اصلی به جای ایجاد نمونه جدید
        dialogs = await client.get_dialogs()


        for dialog in dialogs:
            chat_id = dialog.id
            unread_count = dialog.unread_count

            # اگر تعداد پیام‌های خوانده‌نشده تغییر کرده، مقدار جدید را ذخیره کن
            chats_collection.update_one(
                {"chat_id": chat_id},
                {"$set": {"unread_count": unread_count}},
                upsert=True
            )

        # حذف unread_count برای چت‌هایی که دیگر پیام خوانده‌نشده ندارند
        chats_collection.update_many(
            {"unread_count": {"$gt": 0}, "chat_id": {"$nin": [d.id for d in dialogs if d.unread_count > 0]}},
            {"$set": {"unread_count": 0}}
        )

async def scanner_loop():
    while True:
        print(f"Scan started: {datetime.datetime.now()}")
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            messages = await client.get_messages(dialog, limit=1000)
            save_messages(dialog.name, dialog.id, messages)

        await update_unread_counts()  # بررسی و آپدیت پیام‌های خوانده‌نشده
        await asyncio.sleep(30)

async def main():
    await client.start(PHONE_NUMBER)
    await scanner_loop()

if __name__ == "__main__":
    client.loop.run_until_complete(main())
