from telethon.sync import TelegramClient
from pymongo import MongoClient
import datetime
import asyncio
from pymongo import InsertOne  # این خط را اضافه کنید
from pymongo.errors import DuplicateKeyError, BulkWriteError
# MongoDB connection
# mongo_client = MongoClient("mongodb://localhost:27017/")

mongo_client = MongoClient("mongodb://mongo:27017/")

db = mongo_client["telegram_data"]
messages_collection = db["messages"]
chats_collection = db["chats"]


messages_collection.create_index([("message_id", 1)], unique=True)
chats_collection.create_index([("chat_id", 1)], unique=True)

api_id = "22435091"
api_hash = "da17125c6dd25732caad68a778f69568"
phone_number = "+989370756304"

client = TelegramClient('session_name', api_id, api_hash)

def save_messages(chat_name, chat_id, messages):
    # ذخیره اطلاعات چت
    chat_data = {
        "chat_id": chat_id,
        "chat_name": chat_name,
        'information': [],
        'del-information': []
    }
    try:
        chats_collection.insert_one(chat_data)
        print(f"Inserted chat '{chat_name}' (ID: {chat_id}) into database.")
    except DuplicateKeyError:
        print(f"Chat '{chat_name}' (ID: {chat_id}) already exists. Skipping.")

    # ذخیره پیام‌ها
    fetched_message_ids = []
    for message in messages:
        if message.text:
            fetched_message_ids.append(message.id)

            message_data = {
                "chat_id": chat_id,
                "chat_name": chat_name,
                "message_id": message.id,
                "sender_id": message.sender_id,
                "text": [message.text],
                "date": message.date.strftime("%Y-%m-%d %H:%M:%S") if message.date else None,
                "is_edited": True if message.edit_date else False,
                "edit_date": message.edit_date.strftime("%Y-%m-%d %H:%M:%S") if message.edit_date else None,
            }

            # بررسی وجود پیام در دیتابیس
            try:
                messages_collection.update_one(
                    {"message_id": message.id},  # فیلتر برای پیدا کردن پیام
                    {"$setOnInsert": message_data},  # فقط در صورت نبودن، پیام را وارد کند
                    upsert=True  # ایجاد یا به‌روزرسانی
                )
                print(f"Message {message.id} processed.")
            except Exception as e:
                print(f"Error processing message {message.id}: {e}")

    # پیام‌های حذف‌شده را پرچم‌گذاری کنید
    messages_collection.update_many(
        {"chat_id": chat_id, "message_id": {"$nin": fetched_message_ids}},
        {"$set": {"redFlag": True}}
    )




async def fetch_messages_every_30_seconds():
    while True:
        print(f"Starting update at {datetime.datetime.now()}...")
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            chat_name = dialog.name or "Unknown"
            chat_id = dialog.id
            print(f"Processing chat: {chat_name} (ID: {chat_id})")
            messages = await client.get_messages(dialog, limit=1000)
            save_messages(chat_name, chat_id, messages)
        print("Waiting for 30 seconds before next scan...")
        await asyncio.sleep(30)


async def main():
    await client.start(phone=phone_number)
    print("Logged in successfully!")

    await asyncio.gather(
        fetch_messages_every_30_seconds(),
    )

client.loop.run_until_complete(main())