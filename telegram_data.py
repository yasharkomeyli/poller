# scanner.py (Telethon Message Fetcher)
from telethon.sync import TelegramClient
from pymongo import MongoClient
import datetime
import asyncio
from pymongo import InsertOne
from pymongo.errors import DuplicateKeyError, BulkWriteError

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
PHONE_NUMBER = "+989370756304"

client = TelegramClient('session_name', API_ID, API_HASH)


def save_messages(chat_name, chat_id, messages):
    # Update or insert chat info
    chat_data = {"chat_id": chat_id, "chat_name": chat_name}

    try:
        result = chats_collection.update_one(
            {"chat_id": chat_id},
            {"$setOnInsert": chat_data},
            upsert=True
        )
        print(f"Chat {'inserted' if result.upserted_id else 'exists'}: {chat_name}")
    except Exception as e:
        print(f"Chat error: {e}")

    # Process messages
    fetched_ids = []
    for msg in messages:
        if not msg.text:
            continue

        fetched_ids.append(msg.id)
        update_message_data(msg, chat_id, chat_name)

    # Mark deleted messages
    messages_collection.update_many(
        {"chat_id": chat_id, "message_id": {"$nin": fetched_ids}},
        {"$set": {"redFlag": True}}
    )


def update_message_data(msg, chat_id, chat_name):
    # For existing messages
    existing = messages_collection.find_one({"message_id": msg.id})

    if existing:
        if msg.edit_date:
            handle_edited_message(existing, msg)
        return

    # For new messages
    try:
        message_data = build_message_object(msg, chat_id, chat_name)
        messages_collection.insert_one(message_data)
        print(f"Inserted new message {msg.id}")
    except DuplicateKeyError:
        print(f"Duplicate message {msg.id}")


def handle_edited_message(existing, msg):
    text_list = existing["text"]
    if isinstance(text_list, str):
        text_list = [text_list]

    if msg.text not in text_list:
        text_list.append(msg.text)
        update_data = {
            "text": text_list,
            "is_edited": True,
            "edit_date": msg.edit_date.strftime("%Y-%m-%d %H:%M:%S")
        }
        messages_collection.update_one(
            {"_id": existing["_id"]},
            {"$set": update_data}
        )
        print(f"Updated edited message {msg.id}")


def build_message_object(msg, chat_id, chat_name):
    return {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "message_id": msg.id,
        "sender_id": msg.sender_id,
        "sender_username": getattr(msg.sender, 'username', None),
        "is_outgoing": msg.out,
        "text": [msg.text],
        "date": msg.date.strftime("%Y-%m-%d %H:%M:%S") if msg.date else None,
        "reply_to_msg_id": msg.reply_to_msg_id,
        "is_edited": bool(msg.edit_date),
        "edit_date": msg.edit_date.strftime("%Y-%m-%d %H:%M:%S") if msg.edit_date else None,
        "redFlag": False,
        "mantegh": []
    }


async def scanner_loop():
    while True:
        print(f"Scan started: {datetime.datetime.now()}")
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            messages = await client.get_messages(dialog, limit=1000)
            save_messages(dialog.name, dialog.id, messages)
        await asyncio.sleep(30)


async def main():
    await client.start(PHONE_NUMBER)
    await scanner_loop()


if __name__ == "__main__":
    client.loop.run_until_complete(main())