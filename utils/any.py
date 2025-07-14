import json
import csv
from datetime import datetime

import aiohttp

from aiogram import types
from aiogram.fsm.context import FSMContext

from utils.storage import redis_client
from config import COUNTER_ID, YANDEX_TOKEN


def generate_pretty_amount(price: str | float):
    _sign = "₽"
    price = int(price)

    pretty_price = f"{price:,}".replace(",", " ") + f" {_sign}"

    return pretty_price


def generate_sale_for_price(price: float):
    price = float(price)
    if 0 <= price <= 100:
        _sale = 10
    elif 100 < price <= 500:
        _sale = 50
    elif 500 < price <= 2000:
        _sale = 100
    elif 2000 < price <= 5000:
        _sale = 300
    else:
        _sale = 500

    return _sale


def generate_sale_for_price_popular_product(price: float):
    price = float(price)

    if 1 <= price <= 2000:
        percent = 0.3
    elif 2001 < price <= 35000:
        percent = 0.25
    elif 35001 < price <= 50000:
        percent = 0.2
    elif 50001 < price <= 80000:
        percent = 0.15
    elif 80001 < price <= 110000:
        percent = 0.10
    else:
        percent = 0.07

    _sale = price * percent

    return _sale


async def add_message_to_delete_dict(message: types.Message, state: FSMContext = None):
    chat_id = message.chat.id
    message_date = message.date.timestamp()
    message_id = message.message_id

    # test on myself
    # if chat_id in (int(DEV_ID), 311364517):
    if state is not None:
        data = await state.get_data()

        dict_msg_on_delete: dict = data.get("dict_msg_on_delete")

        if not dict_msg_on_delete:
            dict_msg_on_delete = dict()

        dict_msg_on_delete[message_id] = (chat_id, message_date)

        await state.update_data(dict_msg_on_delete=dict_msg_on_delete)
    else:
        try:
            user_id = message.chat.id
            key = f"fsm:{user_id}:{user_id}:data"

            async with redis_client.pipeline(transaction=True) as pipe:
                user_data: bytes = await pipe.get(key)
                results = await pipe.execute()
                # Извлекаем результат из выполненного pipeline
            # print('RESULTS', results)
            # print('USER DATA (BYTES)', user_data)

            json_user_data: dict = json.loads(results[0])
            # print('USER DATA', json_user_data)

            dict_msg_on_delete: dict = json_user_data.get("dict_msg_on_delete")

            if not dict_msg_on_delete:
                dict_msg_on_delete = dict()

            dict_msg_on_delete[message_id] = (chat_id, message_date)

            json_user_data["dict_msg_on_delete"] = dict_msg_on_delete

            async with redis_client.pipeline(transaction=True) as pipe:
                bytes_data = json.dumps(json_user_data)
                await pipe.set(key, bytes_data)
                results = await pipe.execute()
        except Exception as ex:
            print("ERROR WITH TRY ADD SCHEDULER MESSAGE TO REDIS STORE", ex)


async def send_data_to_yandex_metica(client_id: str, goal_id: str):
    headers = {
        "Authorization": "OAuth {}".format(YANDEX_TOKEN),
    }

    data = [
        ["ClientId", "Target", "DateTime"],
        [client_id, goal_id, datetime.now().timestamp()],
    ]

    with open("test_csv.csv", "w") as _file:
        writer = csv.writer(_file)
        writer.writerows(data)

    file = open("test_csv.csv", "r").read()

    print("CSV FILE", file)

    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession() as session:
        url = f"https://api-metrika.yandex.net/management/v1/counter/{COUNTER_ID}/offline_conversions/upload"
        form_data = aiohttp.FormData()
        form_data.add_field("file", file, filename="test_csv.csv")
        try:
            async with session.post(
                url=url, headers=headers, timeout=timeout, data=form_data
            ) as response:
                resp = await response.json()
                status = response.status
                # h = response.headers['Content-Type']
                print(resp)
                print(status)
        except Exception as ex:
            print("ERROR WITH REQUEST TO YANDEX", ex)

        print(f"YANDEX REQUEST status code {status}")


def generate_percent_to_popular_product(start_price: float, actual_price: float):
    percent = 100 * (1 - (actual_price / start_price))

    return round(percent)
