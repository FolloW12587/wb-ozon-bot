import asyncio
import json
import re

import aiohttp
import pandas as pd

from datetime import datetime, timedelta
from typing import Any, Literal

from asyncio import sleep

from arq import ArqRedis
import pytz

# import matplotlib.pyplot as plt
import plotly.graph_objects as go

from aiogram import types, Bot
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from sqlalchemy import update, select, and_, or_, insert, exists, Subquery, func
from sqlalchemy.ext.asyncio import AsyncSession

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from bot22 import bot

from db.base import (OzonPunkt, PopularProduct, Punkt,
                     Subscription,
                     User,
                     WbProduct,
                     WbPunkt,
                     OzonProduct,
                     UserJob,
                     UTM,
                     ProductCityGraphic,
                     ProductPrice,
                     Product,
                     UserProduct)

from .exc import NotEnoughGraphicData
from .scheduler import (push_check_ozon_price,
                             push_check_wb_price,
                             add_task_to_delete_old_message_for_users)
from .storage import redis_client
from .any import send_data_to_yandex_metica
from .pics import DEFAULT_PRODUCT_LIST_PHOTO_ID

from keyboards import (add_back_btn,
                       add_pagination_btn, create_back_to_product_btn,
                       create_or_add_exit_btn,
                       create_product_list_for_page_kb, new_add_pagination_btn, new_create_product_list_for_page_kb)

from config import DEV_ID, TEST_PHOTO_LIST


DEFAULT_PAGE_ELEMENT_COUNT = 5


async def state_clear(state: FSMContext):
    data = await state.get_data()

    dict_msg_on_delete: dict = data.get('dict_msg_on_delete')

    await state.clear()

    if dict_msg_on_delete:
        await state.update_data(dict_msg_on_delete=dict_msg_on_delete)


async def add_message_to_delete_dict(message: types.Message,
                                     state: FSMContext):
    chat_id = message.chat.id
    message_date = message.date.timestamp()
    message_id = message.message_id

    # test on myself
    # if chat_id in (int(DEV_ID), 311364517):
    data = await state.get_data()

    dict_msg_on_delete: dict = data.get('dict_msg_on_delete')

    if not dict_msg_on_delete:
        dict_msg_on_delete = dict()

    dict_msg_on_delete[message_id] = (chat_id, message_date)

    await state.update_data(dict_msg_on_delete=dict_msg_on_delete)


def check_input_link(link: str):
    if (link.startswith('https://ozon')) or \
        (link.startswith('https://www.ozon')) or \
        (link.startswith('https://www.wildberries')) or\
        (link.startswith('https://wildberries')):
        
        return 'WB' if link.find('wildberries') > 0 else 'OZON'


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


def generate_pretty_amount(price: str | float):
    _sign = '₽'
    price = int(price)

    pretty_price = f'{price:,}'.replace(',', ' ') + f' {_sign}'

    return pretty_price


async def check_user_last_message_time(user_id: int,
                                       now_time: datetime,
                                       message_text: str,
                                       session: AsyncSession,
                                       state: FSMContext,
                                       scheduler: AsyncIOScheduler):
        lock = asyncio.Lock()

        _message_text = message_text.strip().split()

        _name = link = None

        if len(_message_text) > 1:
            *_name, link = _message_text
            _name = ' '.join(_name)
        else:
            if not message_text.isdigit():
                link = message_text

        # key = f'fsm:{user_id}:{user_id}:data'
        # async with redis_client.pipeline(transaction=True) as pipe:
        async with lock:
                
            state_dict = {}

            # user_data: bytes = await pipe.get(key)
            user_data = await state.get_data()
            # Выполняем все команды в pipeline
            # results = await pipe.execute()
            # Извлекаем результат из выполненного pipeline
            # print(results)
            # print(user_data)

            # user_data: dict = json.loads(results[0])

            if last_action_time := user_data.get('last_action_time'):
                # print(user_data)
                moscow_tz = pytz.timezone('Europe/Moscow')
                _last_action_time = datetime.fromtimestamp(last_action_time).astimezone(moscow_tz)


                # #
                # user_data['percent'] = None
                # #

                time_delta = now_time - timedelta(seconds=10)
                
                moscow_tz = pytz.timezone('Europe/Moscow')
                
                print('ACTUAL TIME', now_time)
                print('LAST TIME FROM REDIS', _last_action_time)
                print('TIMEDELTA', time_delta)

                if time_delta >= datetime.fromtimestamp(last_action_time).astimezone(moscow_tz):
                    # first message
                    #
                    state_dict['percent'] = None
                    state_dict['last_action_time'] = now_time.timestamp()
                    #

                    print(f'first message {message_text}')
                    
                    # write last_action_time to redis
                    # user_data['last_action_time'] = now_time.timestamp()
                    # await state.update_data(state_dict)

                    # sub_user_data = json.dumps(user_data)
                    # await pipe.set(key, sub_user_data)
                    # await pipe.execute()

                    if message_text.isdigit():
                        state_dict['percent'] = message_text
                    else:
                        state_dict['link'] = link
                        state_dict['name'] = _name
                        user_data.update(state_dict)
                        await save_product(user_data,
                                            session,
                                            scheduler)
                        # save product without percent
                else:
                    # second message
                    print(f'second message {message_text}')
                    if message_text.isdigit():
                        print(user_data)
                        _percent = message_text
                        await add_procent_to_product(user_data,
                                                        session,
                                                        _percent)
                        # add percent to product
                    else:
                        print(user_data)
                        state_dict['link'] = link
                        state_dict['name'] = _name

                        percent = user_data.get('percent')
                        user_data.update(state_dict)

                        await save_product(user_data,
                                            session,
                                            scheduler,
                                            percent=percent)
                        # get percent from storage and save product with percent
                        
                    # user_data['last_action_time'] = now_time.timestamp()
                    # await state.update_data(state_dict)
                    pass
            else:
                # first message
                state_dict['percent'] = None

                print(f'first message {message_text}')
                print(user_data)
                
                # write last_action_time to redis
                state_dict['last_action_time'] = now_time.timestamp()
                # await state.update_data(state_dict)
                # sub_user_data = json.dumps(user_data)
                # await pipe.set(key, sub_user_data)
                # await pipe.execute()

                if message_text.isdigit():
                    state_dict['percent'] = message_text
                    pass
                else:
                    state_dict['link'] = link
                    state_dict['name'] = _name
                    user_data.update(state_dict)
                    await save_product(user_data,
                                        session,
                                        scheduler)
                    # save product without percent
                    pass

            await state.update_data(state_dict)
            print('state on end', await state.get_data())

    
async def add_procent_to_product(user_data: dict,
                                 session: AsyncSession,
                                 percent: str):
    msg = user_data.get('msg')
    link: str = user_data.get('link')

    if msg and link:
        if link.find('ozon') > 0:
            query = (
                update(
                    OzonProduct
                )\
                .values(percent=int(percent))
                .where(
                    and_(
                        OzonProduct.user_id == msg[0],
                        OzonProduct.link == link,
                    )
                )
            )

            await session.execute(query)

            try:
                print('percent updated')
                await session.commit()
            except Exception as ex:
                print(ex)
                print('update percent failed')
                await session.rollback()
            #add to ozon
        elif link.find('wildberries') > 0:
            query = (
                update(
                    WbProduct
                )\
                .values(percent=int(percent))
                .where(
                    and_(
                        WbProduct.user_id == msg[0],
                        WbProduct.link == link,
                    )
                )
            )

            await session.execute(query)

            try:
                print('percent updated')
                await session.commit()
            except Exception as ex:
                print(ex)
                print('update percent failed')
                await session.rollback()

            pass
        else:
            pass    


def filter_price(price_data: list):
    current_price = None
    current_idx = None

    new_data = []

    for idx, data in enumerate(price_data):
        _price, _date, _city, main_product_id, name, product_marker = data

        if current_price is None:
            new_data.append(data)
            current_price = _price
            current_idx = idx
        else:
            if _price != current_price:
                
                prev_idx = idx - 1
                if idx > 1 and current_idx != prev_idx and current_price == price_data[prev_idx][0]:
                    new_data.append(price_data[prev_idx])
                
                new_data.append(data)
                current_price = _price
                current_idx = idx

    if new_data[-1][0] == price_data[-1][0]:
        
        if new_data[-1][1] == price_data[-1][1]:
            new_data.pop()
        
        new_data.append(price_data[-1])
    
    return new_data


def generate_date_view_list(date_list: list[datetime]):
    first = date_list[0]
    last = date_list[-1]
    len_date_list = len(date_list)

    if 10 < len_date_list <= 14:
        step = 2
    else:
        step = round(len(date_list) / 10)
        step = 1 if step == 0 else step

    filtered_list = date_list[1:-1][::step]

    new_date_list = [first, ]

    for el in filtered_list:
        if new_date_list[-1].day != el.day:
            new_date_list.append(el)

    if new_date_list[-1].day == last.day:
        new_date_list.pop()

    new_date_list.append(last)

    return new_date_list
    # return date_list[::step]
    # return [first, ] + filtered_list + [last, ]


async def generate_graphic(user_id: int,
                           product_id: int,
                           city_subquery: Subquery,
                           message_id: int,
                           session: AsyncSession,
                           state: FSMContext,
                           is_background: bool = False):
    moscow_tz = pytz.timezone('Europe/Moscow')
    default_value = 'МОСКВА'

    query = (
        select(
            ProductPrice.price,
            ProductPrice.time_price,
            func.coalesce(ProductPrice.city, default_value),
            Product.id,
            Product.name,
            Product.product_marker,
        )\
        .select_from(ProductPrice)\
        .join(Product,
              ProductPrice.product_id == Product.id)\
        .join(UserProduct,
              UserProduct.product_id == Product.id)\
        .outerjoin(Punkt,
                   Punkt.user_id == user_id)\
        .where(
            and_(
                UserProduct.id == product_id,
                UserProduct.user_id == user_id,
                ProductPrice.city == func.coalesce(city_subquery, default_value)
            )
        )
        .order_by(ProductPrice.time_price)
    )

    async with session as _session:
        res = await _session.execute(query)

    res = res.fetchall()

    if not (res and len(res) >= 3):
        raise NotEnoughGraphicData()
    
    price_list = []
    date_list = []

    price_data = filter_price(res)
    
    for el in price_data:
        _price, _date, _city, main_product_id, name, product_marker = el
        # print(_city)
        _date: datetime
        price_list.append(_price)
        # date_list.append(_date.astimezone(tz=moscow_tz).strftime('%d-%m-%y'))
        date_list.append(_date.astimezone(tz=moscow_tz))

    # plt.figure(figsize=(10, 5))
    # plt.plot(date_list, price_list, marker='o', linestyle='-')
    # plt.title(f'Изменение цены для города - {_city}')
    # plt.xlabel('Дата')
    # plt.ylabel('Цена')
    # # plt.xticks(rotation=45)  # Поворачиваем метки по оси X для лучшей читаемости
    # plt.grid()
    # plt.tight_layout()  # Автоматически подгоняет график
    
    # plt.savefig(filename)
    # plt.close()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=date_list, y=price_list, mode='lines+markers'))

    title_name = f'{name}<br>{product_marker.upper()} | {_city}'

    date_view_list = generate_date_view_list(date_list)

    fig.update_layout(title={'text':title_name,
                             'x': 0.5,
                             'xanchor': 'center'},
                      xaxis_title='Дата',
                    #   xaxis_tickformat='%d-%m-%y',
                      yaxis_title='Цена')
    
    fig.update_xaxes(tickvals=date_view_list,
                     tickformat='%d-%m-%y',
                     dtick="D1",
                     tickangle=-45)

    # fig.update_yaxes(tickvals=price_list,
    #                  ticktext=[f'{price:,}'.replace(',', ' ') for price in price_list])

    fig.update_yaxes(ticktext=[f'{price:,}'.replace(',', ' ') for price in price_list])


    # fig.update_layout(
    #     yaxis=dict(
    #         tickvals=y_data,  # Указываем значения для отображения
    #         ticktext=[f"{price:.5f}" for price in y_data]  # Форматируем текст для отображения
    #     )
    # )

    # Сохраняем график как изображение
    filename = "plot.png"
    fig.write_image("plot.png")

    _kb = create_back_to_product_btn(user_id=user_id,
                                     product_id=product_id,
                                     is_background_task=is_background)
    _kb = create_or_add_exit_btn(_kb)

    # photo_msg = await bot.send_photo(chat_id=user_id,
    #                                  photo=types.FSInputFile(path=f'./{filename}'),
    #                                  reply_markup=_kb.as_markup())
    photo_msg = await bot.edit_message_media(chat_id=user_id,
                                             message_id=message_id,
                                             media=types.InputMediaPhoto(media=types.FSInputFile(path=f'./{filename}')),
                                             reply_markup=_kb.as_markup())

    await add_message_to_delete_dict(photo_msg,
                                     state)

    if photo_msg.photo:
        photo_id = photo_msg.photo[0].file_id

        check_graphic_query = (
            select(
                ProductCityGraphic.id
            )\
            .where(
                and_(
                    ProductCityGraphic.city == _city,
                    ProductCityGraphic.product_id == main_product_id,
                )
            )
        )
        async with session as _session:
            check_res = await _session.execute(check_graphic_query)

        graphic_id = check_res.scalar_one_or_none()

        if not graphic_id:

            insert_data = {
                'product_id': main_product_id,
                'city': _city,
                'photo_id': photo_id,
                'time_create': datetime.now(),
            }

            final_query = (
                insert(
                    ProductCityGraphic
                )\
                .values(**insert_data)
            )
        else:
            final_query = (
                update(
                    ProductCityGraphic
                )\
                .values(
                    photo_id=photo_id,
                    time_create=datetime.now(),
                )\
                .where(
                    ProductCityGraphic.id == graphic_id,
                )
            )

        async with session as _session:
            await _session.execute(final_query)
            try:
                await _session.commit()
                print('add success')
                return True
            except Exception as ex:
                await _session.rollback()
                print('add error', ex)

        
async def save_product(user_data: dict,
                       session: AsyncSession,
                       scheduler: AsyncIOScheduler,
                       percent: str = None):
    msg = user_data.get('msg')
    _name = user_data.get('name')
    link: str = user_data.get('link')

    ozon_query = (
        select(OzonProduct.id)\
        .where(OzonProduct.user_id == msg[0])
    )

    wb_query = (
        select(
            WbProduct.id
        )\
        .where(WbProduct.user_id == msg[0])
    )

    async with session as _session:
        res = await _session.execute(ozon_query.union(wb_query))

    products_by_user = res.scalars().all()

    product_count_by_user = len(products_by_user)

    print(f'PRODUCT COUNT BY USER {msg[0]} {product_count_by_user}')

    if product_count_by_user >= 100:
        return True

    if link.find('ozon') > 0:
        # save ozon product
        if link.startswith('https://ozon.ru/t/'):
            _idx = link.find('/t/')
            print(_idx)
            _prefix = '/t/'
            ozon_short_link = 'croppedLink|' + link[_idx+len(_prefix):]
            print(ozon_short_link)
        else:
            _prefix = 'product/'

            _idx = link.rfind('product/')

            ozon_short_link = link[(_idx + len(_prefix)):]

        query = (
            select(
                OzonProduct.id,
            )\
            .where(
                OzonProduct.user_id == msg[0],
                OzonProduct.link == link,
            )
        )
        async with session as _session:
            res = await _session.execute(query)

        res = res.scalar_one_or_none()

        if res:
            return True

        print('do request on OZON API')

        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession() as aiosession:
                _url = f"http://172.18.0.7:8080/product/{ozon_short_link}"
                async with aiosession.get(url=_url,
                            timeout=timeout) as response:

                    print(f'OZON RESPONSE CODE {response.status}')
                    if response.status == 408:
                        print('TIMEOUT')
                        await bot.send_message(chat_id=msg[0],
                                               text='Таймаут API')
                        return True

                    res = await response.text()

            if res == '408 Request Timeout':
                await bot.send_message(chat_id=msg[0],
                                       text=f'status 200, text {res}')
                
                return True
            
            _new_short_link = res.split('|')[0]

            w = re.findall(r'\"cardPrice.*currency?', res)

            _alt = re.findall(r'\"alt.*,?', res)
            _product_name = None
            _product_name_limit = 21
            
            if _alt:
                _product_name = _alt[0].split('//')[0]
                _prefix = f'\"alt\":\"'
                
                _product_name = _product_name[len(_prefix)+2:]
                _product_name = ' '.join(_product_name.split()[:4])

            print(_product_name)

            if w:
                w = w[0].split(',')[:3]

                _d = {
                    'price': None,
                    'originalPrice': None,
                    'cardPrice': None,
                }

                for k in _d:
                    if not all(v for v in _d.values()):
                        for q in w:
                            if q.find(k) != -1:
                                name, price = q.split(':')
                                price = price.replace('\\', '').replace('"', '')
                                price = float(''.join(price.split()[:-1]))
                                print(price)
                                _d[k] = price
                                break
                    else:
                        break

                print(_d)
                start_price = int(_d.get('cardPrice', 0))
                actual_price = int(_d.get('cardPrice', 0))
                basic_price = int(_d.get('price', 0))

                _name = _name if _name else _product_name
            else:
                print('22')
                try:
                    response_data = res.split('|')[-1]

                    json_data: dict = json.loads(response_data)

                    _name = ' '.join(json_data.get('seo').get('title').split()[:4])

                    script_list = json_data.get('seo').get('script')

                    inner_html = script_list[0].get('innerHTML') #.get('offers').get('price')

                    print('innerHTML', inner_html)

                    try:
                        inner_html_json: dict = json.loads(inner_html)
                        offers = inner_html_json.get('offers')

                        _price = offers.get('price')

                        start_price = int(_price)
                        actual_price = int(_price)
                        basic_price = int(_price)

                        print('Price', _price)
                    except Exception as ex:
                        print('problem', ex)
                        return True

                    print('PRICE PARSE ERROR', user_data)
                except Exception as ex:
                    print(ex)
                    return True
#
            _sale = generate_sale_for_price(start_price)

            _data = {
                'link': link,
                'short_link': _new_short_link,
                'actual_price': actual_price,
                'start_price': start_price,
                'basic_price': basic_price,
                'sale': _sale,
                'name': _name,
                'time_create': datetime.now(),
                'user_id': msg[0],
            }

            ozon_product = OzonProduct(**_data)

            session.add(ozon_product)

            await session.flush()

            ozon_product_id = ozon_product.id

            job_id = f'{msg[0]}.ozon.{ozon_product_id}'

            job = scheduler.add_job(push_check_ozon_price,
                            trigger='interval',
                            minutes=15,
                            id=job_id,
                            jobstore='sqlalchemy',
                            coalesce=True,
                            kwargs={'user_id': msg[0],
                                    'product_id': ozon_product_id})
            
            _data = {
                'user_id': msg[0],
                'product_id': ozon_product_id,
                'product_marker': 'ozon_product',
                'job_id': job.id,
            }

            user_job = UserJob(**_data)

            session.add(user_job)

            try:
                await session.commit()
                _text = 'Ozon товар успешно добавлен'
                print(_text)
            except Exception as ex:
                print(ex)
                await session.rollback()
                _text = 'Ozon товар не был добавлен'
                print(_text)
            # else:

        except Exception as ex:
            print(ex)
            return True
        pass

## WB
    elif link.find('wildberries') > 0:
        # save wb product
        _prefix = 'catalog/'

        _idx_prefix = link.find(_prefix)

        short_link = link[_idx_prefix + len(_prefix):].split('/')[0]

        query = (
            select(
                WbProduct.id,
            )\
            .where(
                WbProduct.user_id == msg[0],
                WbProduct.link == link,
            )
        )
        async with session as _session:
            res = await _session.execute(query)

        res = res.scalar_one_or_none()

        if res:
            return True

        query = (
            select(WbPunkt.zone)\
            .join(User,
                WbPunkt.user_id == User.tg_id)\
            .where(User.tg_id == msg[0])
        )
        async with session as session:
            res = await session.execute(query)

            del_zone = res.scalar_one_or_none()

        if not res:
            await bot.send_message(chat_id=msg[0],
                                   text='Не получилось найти пункт выдачи')
            return
        
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession() as aiosession:
                _url = f"http://172.18.0.2:8080/product/{del_zone}/{short_link}"
                async with aiosession.get(url=_url,
                                timeout=timeout) as response:
                    try:
                        res = await response.json()
                        print(res)
                    except Exception as ex:
                        print('API RESPONSE ERROR', ex)
                        return
        except Exception as ex:
            print(ex)
            return True

        d = res.get('data')

        print(d.get('products')[0].get('sizes'))

        sizes = d.get('products')[0].get('sizes')

        _product_name = d.get('products')[0].get('name')

        _basic_price = _product_price = None
        
        for size in sizes:
            _price = size.get('price')
            if _price:
                _basic_price = size.get('price').get('basic')
                _product_price = size.get('price').get('product')

                _basic_price = str(_basic_price)[:-2]
                _product_price = str(_product_price)[:-2]

                print('основная:', _basic_price)
                print('актупльная:', _product_price)

                _product_price = float(_product_price)

        async with session.begin():
            query = (
                select(WbPunkt.id,
                        WbPunkt.zone)\
                .join(User,
                        WbPunkt.user_id == User.tg_id)\
                .where(User.tg_id == msg[0])
            )

            _wb_punkt_id = await session.execute(query)

            _wb_punkt_id = _wb_punkt_id.fetchall()

            _sale = generate_sale_for_price(float(_product_price))

            _data_name = _name if _name else _product_name

            if _wb_punkt_id:
                _wb_punkt_id, zone = _wb_punkt_id[0]
                _data = {
                    'link': link,
                    'short_link': short_link,
                    'start_price': _product_price,
                    'actual_price': _product_price,
                    'sale': _sale,
                    'name': _data_name,
                    'time_create': datetime.now(),
                    'user_id': msg[0],
                    'wb_punkt_id': _wb_punkt_id,
                }

                wb_product = WbProduct(**_data)

                session.add(wb_product)

                await session.flush()

                wb_product_id = wb_product.id

                print('product_id', wb_product_id)
                
                job_id = f'{msg[0]}.wb.{wb_product_id}'
        
                job = scheduler.add_job(push_check_wb_price,
                                trigger='interval',
                                minutes=15,
                                id=job_id,
                                coalesce=True,
                                jobstore='sqlalchemy',
                                kwargs={'user_id': msg[0],
                                        'product_id': wb_product_id})
                
                _data = {
                    'user_id': msg[0],
                    'product_id': wb_product_id,
                    'product_marker': 'wb_product',
                    'job_id': job.id,
                }

                user_job = UserJob(**_data)

                session.add(user_job)

                try:
                    await session.commit()
                except Exception as ex:
                    print(ex)
                    _text = 'Что то пошло не так'
                else:
                    _text = 'Wb товар успешно добавлен'
                    print(_text)
            else:
                _text = 'Что то пошло не так'
                print(_text)
        pass
    else:
        pass


async def clear_state_and_redirect_to_start(message: types.Message | types.CallbackQuery,
                                            state: FSMContext,
                                            bot: Bot):
    await state.clear()

    _kb = add_back_btn(InlineKeyboardBuilder())

    _text = 'Что то пошло не так\nВернитесь в главное меню и попробуйте еще раз'

    await bot.send_message(chat_id=message.from_user.id,
                           text=_text,
                           reply_markup=_kb.as_markup())


async def save_data_to_storage(callback: types.CallbackQuery,
                               state: FSMContext,
                               session: AsyncSession,
                               bot: Bot,
                               scheduler: AsyncIOScheduler,
                               callback_data: str):
    data = await state.get_data()

    async with session as session:
        match callback_data:
            case 'wb_punkt':
                list_punkt: list = data.get('list_punkt', list())

                lat = data.get('lat')
                lon = data.get('lon')
                del_zone = data.get('del_zone')

                _data = {
                    'lat': float(lat),
                    'lon': float(lon),
                    'zone': del_zone,
                    'user_id': callback.from_user.id,
                    'time_create': datetime.now(),
                }

                query = (
                    insert(WbPunkt)\
                    .values(**_data)
                )

                await session.execute(query)

                try:
                    await session.commit()
                    _text = 'Wb пукнт успешно добавлен'
                except Exception:
                    await session.rollback()
                    _text = 'Wb пукнт не удалось добавить'

                if lat and lon:
                    list_punkt.append([lat, lon])
                    await state.update_data(list_punkt=list_punkt)

            case 'ozon_product':
                _data = {
                    'link': data.get('ozon_link'),
                    'short_link': data.get('ozon_short_link'),
                    'actual_price': data.get('ozon_actual_price'),
                    'start_price': data.get('ozon_start_price'),
                    'basic_price': data.get('ozon_basic_price'),
                    'sale': int(data.get('sale')),
                    'name': data.get('ozon_product_name'),
                    'time_create': datetime.now(),
                    'user_id': callback.from_user.id,
                }
                
                ozon_product = OzonProduct(**_data)

                session.add(ozon_product)

                await session.flush()

                ozon_product_id = ozon_product.id

                #          user_id | marker | product_id
                job_id = f'{callback.from_user.id}.ozon.{ozon_product_id}'

                job = scheduler.add_job(push_check_ozon_price,
                                trigger='interval',
                                minutes=15,
                                id=job_id,
                                jobstore='sqlalchemy',
                                coalesce=True,
                                kwargs={'user_id': callback.from_user.id,
                                        'product_id': ozon_product_id})
                
                _data = {
                    'user_id': callback.from_user.id,
                    'product_id': ozon_product_id,
                    'product_marker': 'ozon_product',
                    'job_id': job.id,
                }

                user_job = UserJob(**_data)

                session.add(user_job)

                try:
                    await session.commit()
                    _text = 'Ozon товар успешно добавлен'
                except Exception as ex:
                    print(ex)
                    await session.rollback()
                    _text = 'Ozon товар не был добавлен'
                pass
            case 'wb_product':

                async with session.begin():
                    query = (
                        select(WbPunkt.id,
                               WbPunkt.zone)\
                        .join(User,
                                WbPunkt.user_id == User.tg_id)\
                        .where(User.tg_id == callback.from_user.id)
                    )

                    _wb_punkt_id = await session.execute(query)

                    _wb_punkt_id = _wb_punkt_id.fetchall()

                    print('short_link', data.get('wb_product_id'))

                    if _wb_punkt_id:
                        _wb_punkt_id, zone = _wb_punkt_id[0]
                        _data = {
                            'link': data.get('wb_product_link'),
                            'short_link': data.get('wb_product_id'),
                            'start_price': data.get('wb_start_price'),
                            'actual_price': data.get('wb_product_price'),
                            'sale': float(data.get('sale')),
                            'name': data.get('wb_product_name'),
                            'time_create': datetime.now(),
                            'user_id': callback.from_user.id,
                            'wb_punkt_id': _wb_punkt_id,
                        }

                        wb_product = WbProduct(**_data)

                        session.add(wb_product)

                        await session.flush()

                        wb_product_id = wb_product.id

                        print('product_id', wb_product_id)
                        
                        job_id = f'{callback.from_user.id}.wb.{wb_product_id}'

                        job = scheduler.add_job(push_check_wb_price,
                                        trigger='interval',
                                        minutes=15,
                                        id=job_id,
                                        coalesce=True,
                                        jobstore='sqlalchemy',
                                        kwargs={'user_id': callback.from_user.id,
                                                'product_id': wb_product_id})
                        
                        _data = {
                            'user_id': callback.from_user.id,
                            'product_id': wb_product_id,
                            'product_marker': 'wb_product',
                            'job_id': job.id,
                        }

                        user_job = UserJob(**_data)

                        session.add(user_job)

                        try:
                            await session.commit()
                        except Exception as ex:
                            print(ex)
                            _text = 'Что то пошло не так'
                        else:
                            _text = 'Wb товар успешно добавлен'
                    else:
                        _text = 'Что то пошло не так'

    return _text


async def add_user(message: types.Message,
                   session: AsyncSession,
                   utm_source: str | None):
    free_subscribtion_query = (
        select(
            Subscription.id
        )\
        .where(Subscription.name == 'Free')
    )

    async with session as _session:
        res = await _session.execute(free_subscribtion_query)
    
    free_subscribtion_id = res.scalar_one_or_none()

    if free_subscribtion_id:

        data = {
            'tg_id': message.from_user.id,
            'username': message.from_user.username,
            'first_name': message.from_user.first_name,
            'last_name': message.from_user.last_name,
            'time_create': datetime.now(),
            'subscription_id': free_subscribtion_id,
            'utm_source': utm_source,
        }

        query = (
            insert(
                User
            )\
            .values(**data)
        )
        async with session as _session:
            try:
                await _session.execute(query)
                await _session.commit()
            except Exception as ex:
                print(ex)
                await _session.rollback()
            else:
                await add_task_to_delete_old_message_for_users(user_id=message.from_user.id)
                print('user added')

                if utm_source is not None and not utm_source.startswith('direct'):
                    utm_query = (
                        select(
                            UTM.id,
                            UTM.client_id,
                        )\
                        .where(
                            UTM.keitaro_id == utm_source
                        )
                    )
                    res = await _session.execute(utm_query)
                    utm_from_db = res.fetchall()

                    if utm_from_db:
                        utm_id, client_id = utm_from_db[0]

                        update_utm_query = (
                            update(
                                UTM
                            )\
                            .values(user_id=message.from_user.id)\
                            .where(
                                UTM.id == utm_id
                            )
                        )
                        await _session.execute(update_utm_query)
                        try:
                            await _session.commit()
                        except Exception as ex:
                            print('ERROR WITH UPDATE UTM BY USER', ex)
                        else:
                            # send csv to yandex API
                            await send_data_to_yandex_metica(client_id,
                                                             goal_id='bot_start')
                            # pass
                return True
    else:
        pass


async def check_user(message: types.Message,
                     session: AsyncSession,
                     utm_source: str | None):
    async with session as _session:
        query = (
            select(User)\
            .where(User.tg_id == message.from_user.id)
        )

        res = await _session.execute(query)

        res = res.scalar_one_or_none()

    if res:
        return True
    else:
        return await add_user(message,
                              session,
                              utm_source)
    

async def check_has_punkt(user_id: int,
                          session: AsyncSession):
    wb_punkt_model = WbPunkt
    # ozon_punkt_model = OzonPunkt

    query = (
        # exists()\
        select(
            wb_punkt_model.city,
        )
        .where(
            wb_punkt_model.user_id == user_id
        )
    )

    res = await session.execute(query)

    city_punkt = res.scalar_one_or_none()
    
    # return bool(has_punkt)
    return city_punkt


async def new_check_has_punkt(user_id: int,
                              session: AsyncSession):

    query = (
        select(
            Punkt.city,
        )
        .where(
            Punkt.user_id == user_id
        )
    )

    res = await session.execute(query)

    city_punkt = res.scalar_one_or_none()
    
    return city_punkt


async def show_product_list(product_dict: dict,
                            user_id: int,
                            state: FSMContext):
    data = await state.get_data()

    # print('data' ,data)
    # print('product_dict', product_dict)

    current_page = product_dict.get('current_page')
    product_list = product_dict.get('product_list')
    len_product_list = product_dict.get('len_product_list')
    wb_product_count = product_dict.get('wb_product_count')
    ozon_product_count = product_dict.get('ozon_product_count')

    list_msg: tuple = product_dict.get('list_msg')

    if not product_list:
        await delete_prev_subactive_msg(data)
        sub_active_msg = await bot.send_message(chat_id=user_id,
                                                text='Нет добавленных товаров')
        await add_message_to_delete_dict(sub_active_msg,
                                         state)

        await state.update_data(_add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id))
        return

    start_idx = (current_page - 1) * DEFAULT_PAGE_ELEMENT_COUNT
    end_idx = current_page * DEFAULT_PAGE_ELEMENT_COUNT

    product_list_for_page = product_list[start_idx:end_idx]

    _kb = create_product_list_for_page_kb(product_list_for_page)
    _kb = add_pagination_btn(_kb,
                             product_dict)
    _kb = create_or_add_exit_btn(_kb)

    product_on_current_page_count = len(product_list_for_page)

    _text = f'Ваши товары\n\nВсего товаров: {len_product_list}\nПоказано {product_on_current_page_count} товар(a/ов)'

    _text = f'📝 Список ваших товаров:\n\n🔽 Всего товаров: {len_product_list}\n\n🔵 Товаров с Ozon: {ozon_product_count}\n🟣 Товаров с Wildberries: {wb_product_count}\n\nПоказано {product_on_current_page_count} товаров на странице, нажмите ▶, чтобы листать список'

    if not list_msg:
        list_msg: types.Message = await bot.send_message(chat_id=user_id,
                            text=_text,
                            reply_markup=_kb.as_markup())
        
        await add_message_to_delete_dict(list_msg,
                                         state)
        
        product_dict['list_msg'] = (list_msg.chat.id, list_msg.message_id)

        list_msg_on_delete: list = data.get('list_msg_on_delete')

        if not list_msg_on_delete:
            list_msg_on_delete = list()

        list_msg_on_delete.append(list_msg.message_id)

        await state.update_data(list_msg_on_delete=list_msg_on_delete)
        
    else:
        await bot.edit_message_text(chat_id=user_id,
                                    message_id=list_msg[-1],
                                    text=_text,
                                    reply_markup=_kb.as_markup())
    
    await state.update_data(view_product_dict=product_dict)


# new 
async def new_show_product_list(product_dict: dict,
                            user_id: int,
                            state: FSMContext):
    data = await state.get_data()

    # print('data' ,data)
    # print('product_dict', product_dict)

    current_page = product_dict.get('current_page')
    product_list = product_dict.get('product_list')
    len_product_list = product_dict.get('len_product_list')
    wb_product_count = product_dict.get('wb_product_count')
    ozon_product_count = product_dict.get('ozon_product_count')

    list_msg: tuple = product_dict.get('list_msg')

    if not product_list:
        await delete_prev_subactive_msg(data)
        sub_active_msg = await bot.send_message(chat_id=user_id,
                                                text='Нет добавленных товаров')
        await add_message_to_delete_dict(sub_active_msg,
                                         state)

        await state.update_data(_add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id))
        return

    start_idx = (current_page - 1) * DEFAULT_PAGE_ELEMENT_COUNT
    end_idx = current_page * DEFAULT_PAGE_ELEMENT_COUNT

    product_list_for_page = product_list[start_idx:end_idx]

    _kb = new_create_product_list_for_page_kb(product_list_for_page)
    _kb = new_add_pagination_btn(_kb,
                                 product_dict)
    _kb = create_or_add_exit_btn(_kb)

    product_on_current_page_count = len(product_list_for_page)

    _text = f'Ваши товары\n\nВсего товаров: {len_product_list}\nПоказано {product_on_current_page_count} товар(a/ов)'

    _text = f'📝 Список ваших товаров:\n\n🔽 Всего товаров: {len_product_list}\n\n🔵 Товаров с Ozon: {ozon_product_count}\n🟣 Товаров с Wildberries: {wb_product_count}\n\nПоказано {product_on_current_page_count} товаров на странице, нажмите ▶, чтобы листать список'

    if not list_msg:
        # list_msg: types.Message = await bot.send_message(chat_id=user_id,
        #                                                  text=_text,
        #                                                  reply_markup=_kb.as_markup())

        list_msg: types.Message = await bot.send_photo(chat_id=user_id,
                                                       photo=DEFAULT_PRODUCT_LIST_PHOTO_ID,
                                                       caption=_text,
                                                       reply_markup=_kb.as_markup())

        await add_message_to_delete_dict(list_msg,
                                         state)
        
        product_dict['list_msg'] = (list_msg.chat.id, list_msg.message_id)

        list_msg_on_delete: list = data.get('list_msg_on_delete')

        if not list_msg_on_delete:
            list_msg_on_delete = list()

        list_msg_on_delete.append(list_msg.message_id)

        await state.update_data(list_msg_on_delete=list_msg_on_delete)
        
    else:
        # await bot.edit_message_text(chat_id=user_id,
        #                             message_id=list_msg[-1],
        #                             text=_text,
        #                             reply_markup=_kb.as_markup())
        await bot.edit_message_media(chat_id=user_id,
                                     media=types.InputMediaPhoto(media=DEFAULT_PRODUCT_LIST_PHOTO_ID,
                                                                 caption=_text),
                                     message_id=list_msg[-1],
                                     reply_markup=_kb.as_markup())

    await state.update_data(view_product_dict=product_dict)



async def try_delete_prev_list_msgs(chat_id: int,
                                    state: FSMContext):
    data = await state.get_data()

    list_msg_on_delete: list = data.get('list_msg_on_delete')

    if list_msg_on_delete:
            for msg_id in list_msg_on_delete:
                try:
                    await bot.delete_message(chat_id=chat_id,
                                            message_id=msg_id)
                except Exception as ex:
                    print(ex)
                    continue
    
    await state.update_data(list_msg_on_delete=None)


async def delete_prev_subactive_msg(data: dict):
    subactive_msg: tuple = data.get('_add_msg')
    try:
        await bot.delete_message(chat_id=subactive_msg[0],
                                 message_id=subactive_msg[-1])
    except Exception as ex:
        print(ex)


async def try_delete_faq_messages(data: dict):
    try:
        question_msg_list: list[int] = data.get('question_msg_list', list())
        back_to_faq_msg: tuple = data.get('back_to_faq_msg')
        faq_msg: tuple = data.get('faq_msg')


        _chat_id, _message_id = back_to_faq_msg

        question_msg_list.append(_message_id)

        if faq_msg:
            _, _message_id = faq_msg
            question_msg_list.append(_message_id)

        try:
            await bot.delete_messages(chat_id=_chat_id,
                                    message_ids=question_msg_list)
        except Exception as ex:
            print('ERROR WITH DELETE FAQ MESSAGES')
            pass
    except Exception as ex:
        print('TRY DELETE PREV FAQ MESSAGES', ex)


def get_excel_data(path: str):
    df = pd.read_excel(path, header=None)

    data_array = df.values.tolist()

    return data_array[1:]


async def add_popular_product_to_db(_redis_pool: ArqRedis):
    data = get_excel_data(path='./Все подряд хз поч.xlsx')


    for name, link, _, high_category, low_category, *_  in data:

        product_marker = check_input_link(link)

        product_data = {
            'name': name,
            'link': link,
            'high_category': high_category,
            'low_category': low_category,
            'product_marker': product_marker.lower(),
        }

        await _redis_pool.enqueue_job(f'add_popular_product',
                                        product_data=product_data,
                                        _queue_name='arq:popular')
        
        # чтобы тг не кидал ошибку о спаме в чат
        await sleep(2)

    pass