from telethon.sync import TelegramClient
from pymongo import MongoClient
import datetime
import asyncio
from pymongo import InsertOne
from pymongo.errors import DuplicateKeyError, BulkWriteError
# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")

# mongo_client = MongoClient("mongodb://mongo:27017/")

db = mongo_client["telegram_data"]
messages_collection = db["messages"]
chats_collection = db["chats"]


messages_collection.create_index([("message_id", 1)], unique=True)
chats_collection.create_index([("chat_id", 1)], unique=True)

api_id = "22435091"
api_hash = "da17125c6dd25732caad68a778f69568"
phone_number = "+989336531403"

client = TelegramClient('session_name', api_id, api_hash)

def save_messages(chat_name, chat_id, messages):
    # بررسی و ذخیره اطلاعات چت فقط اگر chat_id جدید باشد
    chat_data = {
        "chat_id": chat_id,
        "chat_name": chat_name,
    }

    try:
        # استفاده از update_one با $setOnInsert برای جلوگیری از تکراری بودن
        result = chats_collection.update_one(
            {"chat_id": chat_id},  # شرط برای پیدا کردن چت
            {"$setOnInsert": chat_data},  # فقط اگر چت موجود نباشد، اضافه می‌شود
            upsert=True  # در صورت عدم وجود، ایجاد می‌کند
        )
        if result.upserted_id:  # اگر داده جدیدی اضافه شد
            print(f"Inserted chat '{chat_name}' (ID: {chat_id}) into database.")
        else:
            print(f"Chat '{chat_name}' (ID: {chat_id}) already exists. Skipping.")
    except Exception as e:
        print(f"Error processing chat '{chat_name}' (ID: {chat_id}): {e}")

    # ادامه برای ذخیره پیام‌ها
    fetched_message_ids = []
    for message in messages:
        if message.text:
            fetched_message_ids.append(message.id)

            # دریافت پیام قبلی از دیتابیس
            existing_message = messages_collection.find_one({"message_id": message.id})

            # اگر پیام در دیتابیس موجود باشد
            if existing_message:
                # اگر پیام ادیت شده باشد
                if message.edit_date:
                    # بررسی و تبدیل فیلد text به لیست در صورت نیاز
                    updated_text_list = existing_message["text"]
                    if isinstance(updated_text_list, str):  # اگر text یک رشته بود
                        updated_text_list = [updated_text_list]  # آن را به لیست تبدیل کن

                    # اضافه کردن متن جدید به لیست تکست
                    if message.text not in updated_text_list:  # بررسی عدم وجود متن جدید در لیست
                        updated_text_list.append(message.text)

                    # به‌روزرسانی پیام در دیتابیس
                    messages_collection.update_one(
                        {"message_id": message.id},
                        {
                            "$set": {
                                "text": updated_text_list,
                                "is_edited": True,
                                "edit_date": message.edit_date.strftime("%Y-%m-%d %H:%M:%S")
                            }
                        }
                    )
                    print(f"Message {message.id} was edited and updated.")
                else:
                    print(f"Message {message.id} exists but was not edited.")
            else:
                # اگر پیام جدید باشد
                message_data = {
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "message_id": message.id,
                    "sender_id": message.sender_id,
                    "sender_username": message.sender.username if message.sender else None,
                    "is_outgoing": message.out,
                    "text": [message.text],  # متن پیام به‌صورت لیست
                    "date": message.date.strftime("%Y-%m-%d %H:%M:%S") if message.date else None,
                    "reply_to_msg_id": message.reply_to_msg_id if hasattr(message, "reply_to_msg_id") else None,
                    "is_edited": bool(message.edit_date),
                    "edit_date": message.edit_date.strftime("%Y-%m-%d %H:%M:%S") if message.edit_date else None,
                    "redFlag": False,
                    'mantegh': []
                }

                try:
                    # فقط پیام‌های جدید وارد دیتابیس شوند
                    messages_collection.insert_one(message_data)
                    print(f"Message {message.id} inserted.")
                except Exception as e:
                    print(f"Error inserting message {message.id}: {e}")

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