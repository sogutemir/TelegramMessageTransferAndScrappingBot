import configparser
import json
import asyncio
import time
from datetime import date, datetime

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import (GetHistoryRequest)
from telethon.tl.types import (
    PeerChannel
)
import pandas as pd


# some functions to parse json date

last_messages = dict()

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

api_id = #api id
api_hash = '' #api hash

api_hash = str(api_hash)

phone = '905378700077'  # phone number
username = "" #@username

# Create the client and connect
client = TelegramClient(phone, api_id, api_hash)


async def main(phone):
    await client.start()
    print("Client Created")
    # Ensure you're authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    me = await client.get_me()

    user_input_channel = 'Paste your input channel id'

    if user_input_channel.isdigit():
        entity = PeerChannel(int(user_input_channel))
    else:
        entity = user_input_channel

    my_channel = await client.get_entity(entity)

    offset_id = 0
    limit = 100
    all_messages = []
    total_messages = 0
    total_count_limit = 0

    while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            all_messages.append(message.to_dict())
        offset_id = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    last_len = 0
    new_messages = all_messages.copy()

    df = pd.DataFrame(all_messages)
    # delete colums that we dont need
    df.drop(['_', 'id', 'peer_id', 'date', 'out', 'mentioned',
             'media_unread', 'silent', 'post', 'from_scheduled', 'legacy',
             'edit_hide', 'pinned', 'noforwards', 'from_id', 'fwd_from',
             'via_bot_id', 'reply_to', 'media', 'reply_markup', 'entities', 'views',
             'forwards', 'replies', 'edit_date', 'post_author', 'grouped_id',
             'reactions', 'restriction_reason', 'ttl_period'], axis=1, inplace=True)

    all_messages = df
    # print(all_messages)
    # take only 5 all_messages
    all_messages = all_messages.head(5)

    # save to csv

    # all_messages.to_csv('C:/Users/sogut/Desktop/BOT/data.csv', index=False, encoding='utf-8-sig')
    # send message any group
    import requests
    import telepot

    def send_msg(text="", messages: dict = None, limit=1):
        token = "PasteYourToken"
        chat_id = "Paste your send message channel id"
        bot = telepot.Bot(token)
        text = """ %s """ % format(text)
        if messages:
            for i in range(limit):
                text = """ %s """ % format(messages[limit - i - 1]['message'])
                bot.sendMessage(chat_id, text)
        else:
            bot.sendMessage(chat_id, text)

    global last_messages
    if last_messages != new_messages:
        send_msg(messages=new_messages, limit=3)
        last_messages = new_messages.copy()

with client:
    client.loop.run_until_complete(main(phone))
    while True:
            client.loop.run_until_complete(main(phone))
            time.sleep(5)