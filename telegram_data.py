import os
import datetime
import asyncio
import pytz

from telethon import TelegramClient, events
from pymongo import MongoClient

# ایجاد دایرکتوری برای ذخیره عکس‌های پروفایل (در صورت عدم وجود)
os.makedirs("profile_photos", exist_ok=True)

# اتصال به MongoDB
# mongo_client = MongoClient("mongodb://localhost:27017/")

mongo_client = MongoClient("mongodb://mongo:27017/")
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


async def update_chat_details(chat):
    """
    این تابع اطلاعات کاربر از شی چت (مانند username و عکس پروفایل) را دریافت و در
    کالکشن chats ذخیره می‌کند.
    """
    chat_id = chat.id if hasattr(chat, 'id') else None
    if not chat_id:
        return

    # دریافت نام کاربری (username) در صورتی که وجود داشته باشد
    chat_username = getattr(chat, 'username', None)

    # دانلود عکس پروفایل (در صورت وجود)
    profile_photo_path = None
    if hasattr(chat, 'photo') and chat.photo:
        try:
            # عکس پروفایل در دایرکتوری "profile_photos" با نام {chat_id}.jpg ذخیره می‌شود.
            profile_photo_path = await client.download_profile_photo(chat, file=f"profile_photos/{chat_id}.jpg")
        except Exception as e:
            print(f"Error downloading profile photo for chat {chat_id}: {e}")

    # به‌روزرسانی اطلاعات چت در دیتابیس
    chat_update_data = {
        "username": chat_username,
        "profile_photo": profile_photo_path
    }
    chats_collection.update_one({"chat_id": chat_id}, {"$set": chat_update_data}, upsert=True)


def save_messages(chat_name, chat_id, messages):
    # پیدا کردن جدیدترین پیام برای تعیین last_message_date
    last_msg = None
    for msg in messages:
        if msg.date:
            if last_msg is None or msg.date > last_msg.date:
                last_msg = msg

    # تبدیل تاریخ به منطقه زمانی تهران
    if last_msg and last_msg.date:
        last_message_date = last_msg.date.astimezone(tehran_tz)
        last_message_text = last_msg.text if last_msg.text else ""
    else:
        last_message_date = None
        last_message_text = ""

    # به‌روزرسانی اطلاعات چت در دیتابیس (فیلد unread_count در اینجا تغییر نمی‌کند)
    chat_data = {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "last_message_date": last_message_date.strftime("%Y-%m-%d %H:%M:%S") if last_message_date else None,
        "last_message_text": last_message_text
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

    # ذخیره پیام‌ها (برای هر پیام، اطلاعات ذخیره می‌شود)
    for msg in messages:
        if msg.text:
            update_message_data(msg, chat_id, chat_name)


def update_message_data(msg, chat_id, chat_name):
    existing = messages_collection.find_one({"message_id": msg.id})
    if existing:
        if msg.edit_date:
            handle_edited_message(existing, msg)
        return
    try:
        message_data = build_message_object(msg, chat_id, chat_name)
        messages_collection.insert_one(message_data)
    except Exception as e:
        print("Error inserting message:", e)


def handle_edited_message(existing, msg):
    text_list = existing["text"]
    if isinstance(text_list, str):
        text_list = [text_list]
    if msg.text not in text_list:
        text_list.append(msg.text)
        messages_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": {
                "text": text_list,
                "is_edited": True,
                "edit_date": msg.edit_date.strftime("%Y-%m-%d %H:%M:%S")
            }}
        )


def build_message_object(msg, chat_id, chat_name):
    msg_date = msg.date.astimezone(tehran_tz) if msg.date else None
    edit_date = msg.edit_date.astimezone(tehran_tz) if msg.edit_date else None

    return {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "message_id": msg.id,
        "sender_id": msg.sender_id,
        "username": [],  # در صورت نیاز می‌توانید اطلاعات بیشتر در اینجا اضافه کنید
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


@client.on(events.NewMessage)
async def new_message_handler(event):
    msg = event.message
    try:
        chat = await event.get_chat()
        if hasattr(chat, 'title') and chat.title:
            chat_name = chat.title
        elif hasattr(chat, 'first_name') and chat.first_name:
            chat_name = chat.first_name
        else:
            chat_name = "Private Chat"
        chat_id = chat.id if hasattr(chat, 'id') else event.chat_id
    except Exception as e:
        print("Error fetching chat info:", e)
        chat_name = "Unknown Chat"
        chat_id = event.chat_id
        chat = None

    # به‌روزرسانی اطلاعات کاربر (username و عکس پروفایل) در کالکشن chats
    if chat:
        await update_chat_details(chat)
    else:
        chats_collection.update_one(
            {"chat_id": chat_id},
            {"$set": {"username": None, "profile_photo": None}},
            upsert=True
        )

    # ذخیره پیام جدید
    save_messages(chat_name, chat_id, [msg])

    # اگر پیام دریافتی (incoming) باشد، تعداد unread_count به‌صورت real-time افزایش می‌یابد.
    if not msg.out:
        chats_collection.update_one(
            {"chat_id": chat_id},
            {"$inc": {"unread_count": 1}},
            upsert=True
        )
        print(f"Incremented unread_count for chat {chat_id}")


@client.on(events.MessageEdited)
async def message_edited_handler(event):
    msg = event.message
    try:
        chat = await event.get_chat()
        if hasattr(chat, 'title') and chat.title:
            chat_name = chat.title
        elif hasattr(chat, 'first_name') and chat.first_name:
            chat_name = chat.first_name
        else:
            chat_name = "Private Chat"
        chat_id = chat.id if hasattr(chat, 'id') else event.chat_id
    except Exception as e:
        print("Error fetching chat info:", e)
        chat_name = "Unknown Chat"
        chat_id = event.chat_id
        chat = None

    # به‌روزرسانی اطلاعات کاربر در صورت امکان
    if chat:
        await update_chat_details(chat)

    update_message_data(msg, chat_id, chat_name)


@client.on(events.MessageDeleted)
async def message_deleted_handler(event):
    # هنگامی که پیام حذف می‌شود، آن را با redFlag علامت‌گذاری می‌کنیم.
    for msg_id in event.deleted_ids:
        messages_collection.update_one(
            {"message_id": msg_id},
            {"$set": {"redFlag": True}}
        )
        print(f"Message {msg_id} flagged as deleted.")


@client.on(events.MessageRead)
async def message_read_handler(event):
    """
    وقتی کاربر پیام‌های یک چت را می‌خواند (مثلاً با باز کردن چت در تلگرام)،
    این رویداد trigger شده و تعداد unread_count آن چت به 0 تنظیم می‌شود.
    """
    # تلاش برای به‌دست آوردن chat_id از event
    chat_id = getattr(event, 'chat_id', None)
    if chat_id is None:
        # در صورتی که chat_id مستقیماً موجود نباشد، تلاش می‌کنیم از فیلدهای احتمالی استفاده کنیم
        chat_id = getattr(event, 'peer_id', None)
    if chat_id is None:
        print("Unable to determine chat_id for MessageRead event")
        return

    chats_collection.update_one(
        {"chat_id": chat_id},
        {"$set": {"unread_count": 0}},
        upsert=True
    )
    print(f"Reset unread_count for chat {chat_id} due to read event")


async def main():
    await client.start(PHONE_NUMBER)
    print("Connected as user. Waiting for new messages and read events...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
