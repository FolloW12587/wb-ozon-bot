import json
import aiohttp
import csv

from arq import ArqRedis
import pandas as pd

from datetime import datetime, timedelta

from aiogram import types
from aiogram.fsm.context import FSMContext

# from .handlers import check_input_link

# from background.base import _redis_pool

from .storage import redis_client

from config import DEV_ID, COUNTER_ID, YANDEX_TOKEN


def generate_pretty_amount(price: str | float):
    _sign = '₽'
    price = int(price)

    pretty_price = f'{price:,}'.replace(',', ' ') + f' {_sign}'

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
        percent = 0.7

    _sale = price * percent
    
    return _sale



async def add_message_to_delete_dict(message: types.Message,
                                     state: FSMContext = None):
    chat_id = message.chat.id
    message_date = message.date.timestamp()
    message_id = message.message_id

    # test on myself
    # if chat_id in (int(DEV_ID), 311364517):
    if state is not None:
        data = await state.get_data()

        dict_msg_on_delete: dict = data.get('dict_msg_on_delete')

        if not dict_msg_on_delete:
            dict_msg_on_delete = dict()

        dict_msg_on_delete[message_id] = (chat_id, message_date)

        await state.update_data(dict_msg_on_delete=dict_msg_on_delete)
    else:
        try:
            user_id = message.chat.id
            key = f'fsm:{user_id}:{user_id}:data'

            async with redis_client.pipeline(transaction=True) as pipe:
                user_data: bytes = await pipe.get(key)
                results = await pipe.execute()
                #Извлекаем результат из выполненного pipeline
            # print('RESULTS', results)
            # print('USER DATA (BYTES)', user_data)

            json_user_data: dict = json.loads(results[0])
            # print('USER DATA', json_user_data)

            dict_msg_on_delete: dict = json_user_data.get('dict_msg_on_delete')

            if not dict_msg_on_delete:
                dict_msg_on_delete = dict()

            dict_msg_on_delete[message_id] = (chat_id, message_date)

            json_user_data['dict_msg_on_delete'] = dict_msg_on_delete

            async with redis_client.pipeline(transaction=True) as pipe:
                bytes_data = json.dumps(json_user_data)
                await pipe.set(key, bytes_data)
                results = await pipe.execute()
        except Exception as ex:
            print('ERROR WITH TRY ADD SCHEDULER MESSAGE TO REDIS STORE', ex)


async def send_data_to_yandex_metica(client_id: str,
                                     goal_id: str):
    headers ={
        "Authorization": "OAuth {}".format(YANDEX_TOKEN),
        }
    
    data = [
        ['ClientId', 'Target', 'DateTime'],
        [client_id, goal_id, datetime.now().timestamp()],
        ]
    
    with open('test_csv.csv', 'w') as _file:
            writer = csv.writer(_file)
            writer.writerows(data)

    file = open("test_csv.csv", "r").read()

    print('CSV FILE', file)

    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession() as session:
        url = f'https://api-metrika.yandex.net/management/v1/counter/{COUNTER_ID}/offline_conversions/upload'
        form_data = aiohttp.FormData()
        form_data.add_field('file', file, filename='test_csv.csv')
        try:
            async with session.post(url=url,
                                headers=headers,
                                timeout=timeout,
                                data=form_data) as response:
                resp = await response.json()
                status = response.status
                # h = response.headers['Content-Type']
                print(resp)
                print(status)
        except Exception as ex:
            print('ERROR WITH REQUEST TO YANDEX', ex)
        
        print(f'YANDEX REQUEST status code {status}')


def generate_percent_to_popular_product(start_price: float,
                                        actual_price: float):
    percent = 100 * (1 - (actual_price / start_price))

    return round(percent)

# def get_excel_data(path: str):
#     # path = './Электроника.xlsx'


#     df = pd.read_excel(path, header=None)

#     # Преобразуем DataFrame в список списков
#     data_array = df.values.tolist()

#     # Выводим полученный массив
#     # print(data_array)

#     for name, link, _, high_category, low_category, *_ in data_array:
#         print('name', name)
#         print('link', link)
#         print('high_category', high_category)
#         print('low_category', low_category)
    
#     # xls = pd.ExcelFile(path)

#     # sheet_names = xls.sheet_names

#     # # Прочитать все листы в словарь DataFrame
#     # all_sheets = {sheet_name: xls.parse(sheet_name) for sheet_name in sheet_names}

#     # for name, data in all_sheets.items():
#     #     print(f"Лист: {name}")
#     #     print(data)

# async def add_popular_product_to_db(_redis_pool: ArqRedis):
#     data = get_excel_data(path='./Электроника.xlsx')

#     _count = 0

#     for name, link, _, high_category, low_category, *_  in data:
#         if _count > 5:
#             break
#         # add product
#         product_marker = check_input_link(link)

#         product_data = {
#             'name': name,
#             'link': link,
#             'high_category': high_category,
#             'low_category': low_category,
#             'product_marker': product_marker.lower(),
#         }

#         # await _redis_pool.enqueue_job()
#         await _redis_pool.enqueue_job('add_popular_product',
#                                         product_data,
#                                         _queue_name='arq:low')
#         _count += 1


#     pass