from telethon.sync import TelegramClient


def read_config(file_path):
    config = {}
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for line in lines:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                config[key.strip()] = value.strip().strip('"')
    return config

file_path = 'information.txt'

config_data = read_config(file_path)

api_id = config_data.get('api_id')
api_hash = config_data.get('api_hash')
phone_number = config_data.get('phone_number')

client = TelegramClient('session_name', api_id, api_hash)

def create_session():
    try:
        # شروع ارتباط و ورود
        client.start(phone=phone_number)
        print("Session file created successfully!")
    except Exception as e:
        print(f"Error while creating session: {e}")

# اجرای برنامه
if __name__ == "__main__":
    create_session()
