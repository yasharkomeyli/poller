import os
import asyncio
import pytz
from telethon import TelegramClient, events
from pymongo import MongoClient

# ایجاد دایرکتوری ذخیره عکس‌های پروفایل
os.makedirs("profile_photos", exist_ok=True)

# اتصال به MongoDB
# mongo_client = MongoClient("mongodb://localhost:27017/")

mongo_client = MongoClient("mongodb://mongo:27017/")
db = mongo_client["telegram_data"]
messages_collection = db["messages"]
chats_collection = db["chats"]

# تنظیمات API تلگرام
API_ID = "22435091"
API_HASH = "da17125c6dd25732caad68a778f69568"
PHONE_NUMBER = "+989336531403"

client = TelegramClient('session_name', API_ID, API_HASH)
tehran_tz = pytz.timezone("Asia/Tehran")


async def fetch_and_store_initial_data():
    """ دریافت و ذخیره اطلاعات اولیه‌ی چت‌ها و پیام‌ها در دیتابیس (فقط در اجرای اول) """
    dialogs = await client.get_dialogs()

    for dialog in dialogs:
        chat = dialog.entity
        chat_id = chat.id
        chat_name = getattr(chat, "title", getattr(chat, "first_name", "Private Chat"))

        # ذخیره‌ی اطلاعات چت در دیتابیس
        chats_collection.update_one(
            {"chat_id": chat_id},
            {"$set": {
                "chat_name": chat_name,
                "unread_count": dialog.unread_count  # ذخیره تعداد پیام‌های خوانده نشده
            }},
            upsert=True
        )

        # دریافت و ذخیره‌ی آخرین پیام‌های چت
        messages = await client.get_messages(chat_id, limit=50)
        for msg in messages:
            if not messages_collection.find_one({"message_id": msg.id}):
                messages_collection.insert_one(build_message_object(msg, chat_id, chat_name))

    print("✅ اطلاعات اولیه چت‌ها و پیام‌ها ذخیره شد!")


def build_message_object(msg, chat_id, chat_name):
    """ ساخت آبجکت پیام برای ذخیره در دیتابیس """
    msg_date = msg.date.astimezone(tehran_tz) if msg.date else None
    return {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "message_id": msg.id,
        "sender_id": msg.sender_id,
        "text": msg.text,
        "date": msg_date.strftime("%Y-%m-%d %H:%M:%S") if msg_date else None,
        "is_edited": bool(msg.edit_date),
        "redFlag": False
    }


@client.on(events.NewMessage)
async def new_message_handler(event):
    """ ذخیره پیام جدید و افزایش تعداد پیام‌های خوانده نشده """
    msg = event.message
    chat = await event.get_chat()
    chat_id = chat.id
    chat_name = getattr(chat, "title", getattr(chat, "first_name", "Private Chat"))

    messages_collection.insert_one(build_message_object(msg, chat_id, chat_name))

    # افزایش تعداد پیام‌های خوانده نشده فقط برای پیام‌های دریافتی
    if not msg.out:
        chats_collection.update_one(
            {"chat_id": chat_id},
            {"$inc": {"unread_count": 1}},
            upsert=True
        )
        print(f"🔵 پیام جدید در {chat_name} | unread_count افزایش یافت")


@client.on(events.MessageRead)
async def message_read_handler(event):
    """ صفر کردن تعداد پیام‌های خوانده نشده هنگام خواندن چت """
    chat_id = event.chat_id or getattr(event, 'peer_id', None)
    if chat_id:
        chats_collection.update_one(
            {"chat_id": chat_id},
            {"$set": {"unread_count": 0}}
        )
        print(f"✅ پیام‌های چت {chat_id} خوانده شدند (unread_count = 0)")


@client.on(events.MessageDeleted)
async def message_deleted_handler(event):
    """ علامت‌گذاری پیام حذف‌شده با RedFlag """
    for msg_id in event.deleted_ids:
        messages_collection.update_one(
            {"message_id": msg_id},
            {"$set": {"redFlag": True}}
        )
        print(f"🚨 پیام {msg_id} حذف شد و علامت‌گذاری شد")


async def main():
    await client.start(PHONE_NUMBER)
    await fetch_and_store_initial_data()
    print("🚀 برنامه در حال اجراست...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
