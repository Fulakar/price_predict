from telethon import TelegramClient
from dotenv import load_dotenv
from pathlib import Path
import os
import sys
sys.path.append(sys.path[0][:-7])
load_dotenv()

API_ID = os.environ.get('API_ID')
API_HASH = os.environ.get('API_HASH')

async def helper_func(client:TelegramClient):
    dialogs = await client.get_dialogs()
    return True

script_dir = Path(__file__).parent
session_path = script_dir / 'MTS_acc.session'

with TelegramClient(str(session_path), API_ID, API_HASH, system_version="4.16.30-vxCUSTOM") as client:
    client.loop.run_until_complete(client.send_message('@Fima_Herosimovich', f'Поиск начат'))
    dataset = client.loop.run_until_complete(helper_func(client))
    client.loop.run_until_complete(client.send_message('@Fima_Herosimovich', f'Поиск закончен'))
