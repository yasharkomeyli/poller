from telethon.sync import TelegramClient
from pymongo import MongoClient
import datetime
import asyncio

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
    chat_data = {
        "chat_id": chat_id,
        "chat_name": chat_name,
        'information': [],
        'del-information': []
    }
    try:
        chats_collection.insert_one(chat_data)
        print(f"Inserted chat '{chat_name}' (ID: {chat_id}) into database.")
    except Exception as e:
        if "duplicate key error" in str(e):
            print(f"Chat '{chat_name}' (ID: {chat_id}) already exists. Skipping.")
        else:
            print(f"Error inserting chat: {e}")

    fetched_message_ids = []
    for message in messages:
        if message.text:
            fetched_message_ids.append(message.id)

            # بررسی وجود پیام در دیتابیس
            existing_message = messages_collection.find_one({"message_id": message.id})
            if existing_message:
                # اگر پیام ویرایش شده باشد
                if message.edit_date:
                    # بررسی تفاوت متن جدید با آخرین متن ذخیره‌شده
                    last_text = existing_message["text"][-1]  # آخرین متن موجود در لیست
                    if last_text != message.text:  # فقط اگر متن جدید متفاوت است، اضافه شود
                        updated_text_list = existing_message["text"] + [message.text]
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
                        print(f"Message {message.id} updated with new text.")
                    else:
                        print(f"Message {message.id} edited but text is identical. Skipping update.")
            else:
                # ذخیره پیام جدید با لیست اولیه
                message_data = {
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "message_id": message.id,
                    "sender_id": message.sender_id,
                    "sender_username": message.sender.username if message.sender else None,
                    "is_outgoing": message.out,
                    "text": [message.text],  # ذخیره متن به‌صورت لیست
                    "date": message.date.strftime("%Y-%m-%d %H:%M:%S") if message.date else None,
                    "reply_to_msg_id": message.reply_to_msg_id if hasattr(message, "reply_to_msg_id") else None,
                    "is_edited": True if message.edit_date else False,
                    "edit_date": message.edit_date.strftime("%Y-%m-%d %H:%M:%S") if message.edit_date else None,
                    "redFlag": False,
                    'mantegh': []
                }
                try:
                    messages_collection.insert_one(message_data)
                    print(f"Inserted message {message.id} into database.")
                except Exception as e:
                    print(f"Error inserting message: {e}")

    # چک کردن پیام‌های پاک‌شده
    flag_deleted_messages(chat_id, fetched_message_ids)


def flag_deleted_messages(chat_id, fetched_message_ids):

    # پیام‌های موجود در دیتابیس برای این چت
    db_messages = messages_collection.find({"chat_id": chat_id})

    for message in db_messages:
        if message["message_id"] not in fetched_message_ids:
            # اگر پیام در دیتابیس بود ولی در پیام‌های جدید نیست
            messages_collection.update_one(
                {"_id": message["_id"]},
                {"$set": {"redFlag": True}}
            )
            print(f"Message with ID {message['message_id']} flagged as deleted (redFlag=True).")


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
        await asyncio.sleep(5)

async def main():
    await client.start(phone=phone_number)
    print("Logged in successfully!")

    await asyncio.gather(
        fetch_messages_every_30_seconds(),
    )

# اجرای برنامه
with client:
    client.loop.run_until_complete(main())
