import json
import re
import pytz
import aiohttp
import asyncio
import aiofiles
import base64

from math import ceil
from datetime import datetime, timedelta
from typing import Literal

from aiogram import types, Bot

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.job import Job

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy import insert, select, and_, update, func, desc
from sqlalchemy.orm import selectinload

from db.base import (Category, ChannelLink, OzonPunkt, PopularProduct, Product, Punkt,
                     Subscription,
                     WbProduct,
                     WbPunkt,
                     User,
                     get_session,
                     UserJob,
                     OzonProduct,
                     UTM,
                     UserProduct,
                     UserProductJob,
                     ProductPrice)

from keyboards import (add_graphic_btn,
                       add_or_create_close_kb,
                       create_remove_and_edit_sale_kb,
                       create_remove_kb,
                       create_remove_popular_kb,
                       new_create_remove_and_edit_sale_kb)

from bot22 import bot

from utils.storage import redis_client
from utils.any import (generate_pretty_amount,
                  generate_sale_for_price,
                  add_message_to_delete_dict,
                  generate_percent_to_popular_product,
                  send_data_to_yandex_metica)
from utils.pics import DEFAULT_PRODUCT_LIST_PHOTO_ID, DEFAULT_PRODUCT_PHOTO_ID
from utils.cities import city_index_dict
from utils.exc import OzonAPICrashError, OzonProductExistsError, WbAPICrashError, WbProductExistsError
from utils.scheduler import new_check_subscription_limit, new_save_product, save_popular_product, scheduler, try_add_product_price_to_db, update_last_send_price_by_user_product

from config import DEV_ID, SUB_DEV_ID, WB_API_URL, OZON_API_URL, JOB_STORE_URL, TEST_PHOTO_ID



async def new_add_product_task(cxt, user_data: dict):
        try:
            scheduler = cxt.get('scheduler')
            product_marker: str = user_data.get('product_marker')
            _add_msg_id: int = user_data.get('_add_msg_id')
            msg: tuple = user_data.get('msg')

            async for session in get_session():
                check_product_limit = await new_check_subscription_limit(user_id=msg[0],
                                                                     marker=product_marker,
                                                                     session=session)
            if check_product_limit:
                _text = f'⛔ Достигнут лимит {product_marker.upper()} товаров по Вашей подписке ⛔\n\nЛимит товаров: {check_product_limit}'
                msg = await bot.edit_message_text(chat_id=msg[0],
                                                  message_id=_add_msg_id,
                                                  text=_text)
                await add_message_to_delete_dict(msg)
                return
            try:
                async for session in get_session():
                    await new_save_product(user_data=user_data,
                                           session=session,
                                           scheduler=scheduler)
            except (OzonProductExistsError, WbProductExistsError) as ex:
                print('PRODUCT EXISTS', ex)
                _text = f'❗️ {product_marker} товар уже есть в Вашем списке'
            except OzonAPICrashError as ex:
                print('OZON API CRASH', ex)
                pass
            except aiohttp.ClientError as ex:
                print('Таймаут по запросу к OZON API', ex)
            except Exception as ex:
                print(ex)
                _text = f'‼️ Возникла ошибка при добавлении {product_marker} товара\n\nПопробуйте повторить позже'
            else:
                _text = f'{product_marker} товар добавлен к отслеживанию✅'

            await bot.edit_message_text(chat_id=msg[0],
                                        message_id=_add_msg_id,
                                        text=_text)
                
        except Exception as ex:
            print('SCHEDULER ADD ERROR', ex)
            await bot.edit_message_text(chat_id=msg[0],
                                        message_id=_add_msg_id,
                                        text=f'{product_marker.upper()} не удалось добавить')



async def new_push_check_ozon_price(cxt,
                                    user_id: str,
                                    product_id: str):
    try:
        print(f'qwe {cxt["job_id"]}')
        print(f'new 222 фоновая задача ozon {user_id}')

        async for session in get_session():
            async with session as _session:
                try:
                    query = (
                        select(
                            Product.id,
                            UserProduct.id,
                            UserProduct.link,
                            Product.short_link,
                            UserProduct.actual_price,
                            UserProduct.start_price,
                            Product.name,
                            UserProduct.sale,
                            Punkt.ozon_zone,
                            Punkt.city,
                            UserProductJob.job_id,
                            Product.photo_id,
                            UserProduct.last_send_price,
                        )\
                        .select_from(UserProduct)\
                        .join(Product,
                            UserProduct.product_id == Product.id)\
                        .outerjoin(Punkt,
                                Punkt.user_id == int(user_id))\
                        .outerjoin(UserProductJob,
                                UserProductJob.user_product_id == UserProduct.id)\
                        .where(
                            and_(
                                UserProduct.id == int(product_id),
                                UserProduct.user_id == int(user_id),
                            )
                        )
                    )

                    res = await _session.execute(query)

                    res = res.fetchall()
                finally:
                    try:
                        await _session.close()
                    except Exception:
                        pass
        if res:
            main_product_id, _id, link, short_link, actual_price, start_price, name, sale, zone, city, job_id, photo_id, last_send_price = res[0]

            name = name if name is not None else 'Отсутствует'
            try:
                timeout = aiohttp.ClientTimeout(total=30)
                async with aiohttp.ClientSession() as aiosession:
                    if zone:
                        _url = f"{OZON_API_URL}/product/{zone}/{short_link}"
                    else:
                        _url = f"{OZON_API_URL}/product/{short_link}"
                    async with aiosession.get(url=_url,
                                timeout=timeout) as response:
                        _status_code = response.status

                        print(_status_code)

                        res = await response.text()
                    
                    if _status_code == 404:
                        raise OzonAPICrashError()

                w = re.findall(r'\"cardPrice.*currency?', res)

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
                                    _name, price = q.split(':')
                                    price = price.replace('\\', '').replace('"', '')
                                    price = float(''.join(price.split()[:-1]))
                                    # print(price)
                                    _d[k] = price
                                    break
                        else:
                            break

                    print(_d)

                    _product_price = _d.get('cardPrice', 0)
                else:
                    try:
                        response_data = res.split('|')[-1]

                        json_data: dict = json.loads(response_data)

                        script_list = json_data.get('seo').get('script')

                        inner_html = script_list[0].get('innerHTML') #.get('offers').get('price')

                        inner_html_json: dict = json.loads(inner_html)
                        offers = inner_html_json.get('offers')

                        _price = offers.get('price')

                        _product_price = _price
                        
                        print('Price', _price)
                    except Exception as ex:
                        print('scheduler parse inner html error', ex)
                        return

                _product_price = float(_product_price)

                await try_add_product_price_to_db(product_id=main_product_id,
                                                city=city,
                                                price=_product_price)

                check_price = _product_price == actual_price

                if check_price:
                    _text = 'цена не изменилась'
                    print(f'{_text} user {user_id} product {name}')
                    return
                else:
                    _waiting_price = start_price - sale

                    update_query = (
                        update(
                            UserProduct
                        )\
                        .values(actual_price=_product_price)\
                        .where(UserProduct.id == product_id)
                    )

                    async for session in get_session():
                        async with session as _session:
                            try:
                                await session.execute(update_query)
                                await session.commit()
                            except Exception as ex:
                                await session.rollback()
                                print(ex)

                    pretty_product_price = generate_pretty_amount(_product_price)
                    pretty_actual_price = generate_pretty_amount(actual_price)
                    pretty_sale = generate_pretty_amount(sale)
                    pretty_start_price = generate_pretty_amount(start_price)

                    if _waiting_price >= _product_price:
                        
                        # проверка, отправлялось ли уведомление с такой ценой в прошлый раз
                        if last_send_price is not None and (last_send_price == _product_price):
                            print(f'LAST SEND PRICE VALIDATION STOP {last_send_price} | {_product_price}')
                            return

                        if actual_price < _product_price:
                            _text = f'🔄 Цена повысилась, но всё ещё входит в выставленный диапазон скидки на товар <a href="{link}">{name}</a>\n\nМаркетплейс: Ozon\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                            _disable_notification = True
                        else:
                            _text = f'🚨 Изменилась цена на <a href="{link}">{name}</a>\n\nМаркетплейс: Ozon\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                            _disable_notification = False

                        _kb = new_create_remove_and_edit_sale_kb(user_id=user_id,
                                                                product_id=product_id,
                                                                marker='ozon',
                                                                job_id=job_id,
                                                                with_redirect=False)
                        
                        _kb = add_or_create_close_kb(_kb)

                        msg = await bot.send_photo(chat_id=user_id,
                                                photo=photo_id,
                                                caption=_text,
                                                disable_notification=_disable_notification,
                                                reply_markup=_kb.as_markup())
                        
                        await update_last_send_price_by_user_product(last_send_price=_product_price,
                                                                    user_product_id=_id)

                        await add_message_to_delete_dict(msg)
                        return

            except OzonAPICrashError as ex:
                print('SCHEDULER OZON API CRUSH', ex)

            except Exception as ex:
                print('OZON SCHEDULER ERROR', ex, ex.args)
    except Exception as ex:
        print('GLOBAL ERROR FROM TASK', ex)



async def new_push_check_wb_price(cxt,
                                  user_id: str,
                                  product_id: str):
    try:
        print(f'qwe {cxt["job_id"]}')
        print(f'new 222 фоновая задача wb {user_id}')

        async for session in get_session():
            async with session as _session:
                try:
                    query = (
                        select(
                            Product.id,
                            UserProduct.id,
                            UserProduct.link,
                            Product.short_link,
                            UserProduct.actual_price,
                            UserProduct.start_price,
                            Product.name,
                            UserProduct.sale,
                            Punkt.wb_zone,
                            Punkt.city,
                            UserProductJob.job_id,
                            Product.photo_id,
                            UserProduct.last_send_price,
                        )\
                        .select_from(UserProduct)\
                        .join(Product,
                            UserProduct.product_id == Product.id)\
                        .outerjoin(Punkt,
                                Punkt.user_id == int(user_id))\
                        .outerjoin(UserProductJob,
                                UserProductJob.user_product_id == UserProduct.id)\
                        .where(
                            and_(
                                UserProduct.id == int(product_id),
                                UserProduct.user_id == int(user_id),
                            )
                        )
                    )

                    res = await _session.execute(query)

                    res = res.fetchall()
                finally:
                    try:
                        await _session.close()
                    except Exception:
                        pass
        if res:
            main_product_id, _id, link, short_link, actual_price, start_price, name, sale, zone, city, job_id, photo_id, last_send_price = res[0]

            name = name if name is not None else 'Отсутствует'

            if not zone:
                zone = -1281648

            try:
                timeout = aiohttp.ClientTimeout(total=15)
                async with aiohttp.ClientSession() as aiosession:
                    _url = f"{WB_API_URL}/product/{zone}/{short_link}"
                    
                    async with aiosession.get(url=_url,
                                            timeout=timeout) as response:
                        _status_code = response.status

                        res = await response.json()

                if _status_code == 404:
                    raise WbAPICrashError()

                d = res.get('data')

                sizes = d.get('products')[0].get('sizes')

                _basic_price = _product_price = None
                
                for size in sizes:
                    _price = size.get('price')
                    
                    if _price:
                        _basic_price = size.get('price').get('basic')
                        _product_price = size.get('price').get('product')

                        _basic_price = str(_basic_price)[:-2]
                        _product_price = str(_product_price)[:-2]

                _product_price = float(_product_price)

                print('Wb price', _product_price)

                await try_add_product_price_to_db(product_id=main_product_id,
                                                city=city,
                                                price=_product_price)
                
                check_price = _product_price == actual_price

                if check_price:
                    _text = 'цена не изменилась'
                    print(f'{_text} user {user_id} product {name}')
                    
                    if int(user_id) == int(DEV_ID):
                        await bot.send_message(chat_id=user_id,
                                            text=f'{_text} user {user_id} product {name}')
                    return
                
                else:
                    update_query = (
                        update(
                            UserProduct
                        )\
                        .values(actual_price=_product_price)\
                        .where(UserProduct.id == product_id)
                    )

                    async for session in get_session():
                        async with session as _session:
                            try:
                                await session.execute(update_query)
                                await session.commit()
                            except Exception as ex:
                                await session.rollback()
                                print(ex)

                    _waiting_price = start_price - sale

                    pretty_product_price = generate_pretty_amount(_product_price)
                    pretty_actual_price = generate_pretty_amount(actual_price)
                    pretty_sale = generate_pretty_amount(sale)
                    pretty_start_price = generate_pretty_amount(start_price)
                    
                    if _waiting_price >= _product_price:

                        # проверка, отправлялось ли уведомление с такой ценой в прошлый раз
                        if last_send_price is not None and (last_send_price == _product_price):
                            print(f'LAST SEND PRICE VALIDATION STOP {last_send_price} | {_product_price}')
                            return

                        if actual_price < _product_price:
                            _text = f'🔄 Цена повысилась, но всё ещё входит в выставленный диапазон скидки на товар <a href="{link}">{name}</a>\n\nМаркетплейс: Wb\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                            _disable_notification = True
                        else:
                            _text = f'🚨 Изменилась цена на <a href="{link}">{name}</a>\n\nМаркетплейс: Wb\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                            _disable_notification = False

                        _kb = new_create_remove_and_edit_sale_kb(user_id=user_id,
                                                                product_id=product_id,
                                                                marker='wb',
                                                                job_id=job_id,
                                                                with_redirect=False)

                        _kb = add_or_create_close_kb(_kb)

                        msg = await bot.send_photo(chat_id=user_id,
                                                photo=photo_id,
                                                caption=_text,
                                                disable_notification=_disable_notification,
                                                reply_markup=_kb.as_markup())
                        
                        await update_last_send_price_by_user_product(last_send_price=_product_price,
                                                                    user_product_id=_id)


                        await add_message_to_delete_dict(msg)
                        return

            except WbAPICrashError as ex:
                print('SCHEDULER WB API CRUSH', ex)

            except Exception as ex:
                print(ex)
                pass
    except Exception as ex:
        print('GLOBAL ERROR FROM WB TASK', ex)


async def add_popular_product(cxt,
                              product_data: dict):   
            scheduler = cxt.get('scheduler')     
            product_marker: str = product_data.get('product_marker')
            print(f'from task {product_data}')

            try:
                async for session in get_session():
                    await save_popular_product(product_data=product_data,
                                               session=session,
                                               scheduler=scheduler)

            except (OzonProductExistsError, WbProductExistsError) as ex:
                print('PRODUCT EXISTS', ex)
                _text = f'❗️ {product_marker} товар уже есть в Вашем списке'
            except OzonAPICrashError as ex:
                print('OZON API CRASH', ex)
                pass
            except aiohttp.ClientError as ex:
                print('Таймаут по запросу к OZON API', ex)
            except Exception as ex:
                print(ex)
                _text = f'‼️ Возникла ошибка при добавлении {product_marker} товара\n\nПопробуйте повторить позже'
            else:
                _text = f'{product_marker} популярный товар добавлен к отслеживанию✅'
                print(_text)


async def push_check_ozon_popular_product(cxt,
                                        product_id: int):
    
    print(f'new фоновая задача ozon (популярный товар)')

    async for session in get_session():
        async with session as _session:
            try:
                query = (
                    select(PopularProduct)
                    .options(
                        selectinload(PopularProduct.product),
                        selectinload(PopularProduct.category)
                            .selectinload(Category.channel_links)
                    )
                    .where(PopularProduct.id == int(product_id))
                )

                res = await _session.execute(query)

                popular_product = res.scalar_one_or_none()
            finally:
                try:
                    await _session.close()
                except Exception:
                    pass
    if not popular_product:
        print('wtf!@!@!@!#!')
    else:
        # print('PRODUCT', popular_product.__dict__)

        # if popular_product.category.channel_links: 
        #     for channel in popular_product.category.channel_links:
        #         print('channel22', channel.channel_id)

        _id = popular_product.id
        link = popular_product.link
        short_link = popular_product.product.short_link
        actual_price = popular_product.actual_price
        start_price = popular_product.start_price
        name = popular_product.product.name
        sale = popular_product.sale
        photo_id = popular_product.product.photo_id

        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession() as aiosession:
                _url = f"{OZON_API_URL}/product/{short_link}"

                async with aiosession.get(url=_url,
                            timeout=timeout) as response:
                    _status_code = response.status

                    print(_status_code)

                    res = await response.text()
                
                if _status_code == 404:
                    raise OzonAPICrashError()

            w = re.findall(r'\"cardPrice.*currency?', res)

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
                                _name, price = q.split(':')
                                price = price.replace('\\', '').replace('"', '')
                                price = float(''.join(price.split()[:-1]))
                                # print(price)
                                _d[k] = price
                                break
                    else:
                        break

                print(_d)

                _product_price = _d.get('cardPrice', 0)
            else:
                try:
                    response_data = res.split('|', maxsplit=1)[-1]

                    json_data: dict = json.loads(response_data)

                    script_list = json_data.get('seo').get('script')

                    inner_html = script_list[0].get('innerHTML') #.get('offers').get('price')

                    inner_html_json: dict = json.loads(inner_html)
                    offers = inner_html_json.get('offers')

                    _price = offers.get('price')

                    _product_price = _price
                    
                    print('Price', _price)
                except Exception as ex:
                    print('scheduler parse inner html error', ex)
                    return

            _product_price = float(_product_price)

            check_price = _product_price == actual_price

            if check_price:
                _text = 'цена не изменилась (популярный товар)'
                print(f'{_text} product {name}')
                return
            else:
                _waiting_price = start_price - sale

                update_query = (
                    update(
                        PopularProduct
                    )\
                    .values(actual_price=_product_price)\
                    .where(PopularProduct.id == product_id)
                )

                async for session in get_session():
                    async with session as _session:
                        try:
                            await _session.execute(update_query)
                            await _session.commit()
                        except Exception as ex:
                            await _session.rollback()
                            print(ex)

                pretty_product_price = generate_pretty_amount(_product_price)
                pretty_actual_price = generate_pretty_amount(actual_price)
                pretty_sale = generate_pretty_amount(sale)
                pretty_start_price = generate_pretty_amount(start_price)

                if _waiting_price >= _product_price:
                    
                    # проверка, отправлялось ли уведомление с такой ценой в прошлый раз
                    # if last_send_price is not None and (last_send_price == _product_price):
                    #     print(f'LAST SEND PRICE VALIDATION STOP {last_send_price} | {_product_price}')
                    #     return

                    # if actual_price < _product_price:
                    #     _text = f'популярный🔄 Цена повысилась, но всё ещё входит в выставленный диапазон скидки на товар <a href="{link}">{name}</a>\n\nМаркетплейс: Ozon\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                    #     _disable_notification = True
                    # else:
                    
                    percent = generate_percent_to_popular_product(start_price,
                                                                  _product_price)
                    _text = f'🔥 {name} <b>-{percent}%</b> 🔥\n\n📉Было {pretty_start_price} -> <b><u>Стало {pretty_product_price}</u></b>\n\n➡️<a href="{link}">Ссылка на товар</a>'

                    if popular_product.category:
                        category_name = popular_product.category.name
                        _text += f'\n\n#{category_name.lower()}'
                    # _text = f'популярный🚨 Изменилась цена на <a href="{link}">{name}</a>\n\nМаркетплейс: Ozon\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                    _disable_notification = False


                    channel_links = [channel.channel_id for channel in popular_product.category.channel_links]

                    print(channel_links)

                    _kb = create_remove_popular_kb(marker=popular_product.product.product_marker,
                                                   popular_product_id=popular_product.id)
                    # _kb = new_create_remove_and_edit_sale_kb(user_id=user_id,
                    #                                          product_id=product_id,
                    #                                          marker='ozon',
                    #                                          job_id=job_id,
                    #                                          with_redirect=False)
                    
                    # _kb = add_graphic_btn(_kb,
                    #                       user_id=user_id,
                    #                       product_id=_id)

                    # _kb = add_or_create_close_kb(_kb)

                    for channel_link in channel_links:
                        msg = await bot.send_photo(chat_id=channel_link,
                                                photo=photo_id,
                                                caption=_text,
                                                disable_notification=_disable_notification,
                                                reply_markup=_kb.as_markup())
                        
                        await asyncio.sleep(0.2)
                        
                    return

        except OzonAPICrashError as ex:
            print('SCHEDULER OZON API CRUSH', ex)

        except Exception as ex:
            print('OZON SCHEDULER ERROR', ex, ex.args)



async def push_check_wb_popular_product(cxt,
                                  product_id: str):
    print(f'new фоновая задача wb (популярные товары)')

    async for session in get_session():
        async with session as _session:
            try:
                query = (
                    select(PopularProduct)
                    .options(
                        selectinload(PopularProduct.product),
                        selectinload(PopularProduct.category)
                            .selectinload(Category.channel_links)
                    )
                    .where(PopularProduct.id == int(product_id))
                )

                res = await _session.execute(query)

                popular_product = res.scalar_one_or_none()
            finally:
                try:
                    await _session.close()
                except Exception:
                    pass
    if not popular_product:
        pass
    else:
        # if popular_product.category.channel_links: 
        #     for channel in popular_product.category.channel_links:
        #         print('channel22', channel.channel_id)

        _id = popular_product.id
        link = popular_product.link
        short_link = popular_product.product.short_link
        actual_price = popular_product.actual_price
        start_price = popular_product.start_price
        name = popular_product.product.name
        sale = popular_product.sale
        photo_id = popular_product.product.photo_id

        zone = -1281648

        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession() as aiosession:
                _url = f"{WB_API_URL}/product/{zone}/{short_link}"
                
                async with aiosession.get(url=_url,
                                          timeout=timeout) as response:
                    _status_code = response.status

                    res = await response.json()

            if _status_code == 404:
                raise WbAPICrashError()

            d = res.get('data')

            sizes = d.get('products')[0].get('sizes')

            _basic_price = _product_price = None
            
            for size in sizes:
                _price = size.get('price')
                
                if _price:
                    _basic_price = size.get('price').get('basic')
                    _product_price = size.get('price').get('product')

                    _basic_price = str(_basic_price)[:-2]
                    _product_price = str(_product_price)[:-2]

            _product_price = float(_product_price)

            print('Wb price', _product_price)
            
            check_price = _product_price == actual_price

            if check_price:
                _text = 'цена не изменилась'
                print(f'{_text} popular product {name}')
                return
            
            else:
                update_query = (
                    update(
                        PopularProduct
                    )\
                    .values(actual_price=_product_price)\
                    .where(PopularProduct.id == product_id)
                )

                async for session in get_session():
                    async with session as _session:
                        try:
                            await _session.execute(update_query)
                            await _session.commit()
                        except Exception as ex:
                            await _session.rollback()
                            print(ex)

                _waiting_price = start_price - sale

                pretty_product_price = generate_pretty_amount(_product_price)
                pretty_actual_price = generate_pretty_amount(actual_price)
                pretty_sale = generate_pretty_amount(sale)
                pretty_start_price = generate_pretty_amount(start_price)
                
                if _waiting_price >= _product_price:

                    percent = generate_percent_to_popular_product(start_price,
                                                                  _product_price)
                    _text = f'🔥 {name} <b>-{percent}%</b> 🔥\n\n📉Было {pretty_start_price} -> <b><u>Стало {pretty_product_price}</u></b>\n\n➡️<a href="{link}">Ссылка на товар</a>'

                    if popular_product.category:
                        category_name = popular_product.category.name
                        _text += f'\n\n#{category_name.lower()}'
                    # _text = f'популярный🚨 Изменилась цена на <a href="{link}">{name}</a>\n\nМаркетплейс: Ozon\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                    _disable_notification = False

                    # проверка, отправлялось ли уведомление с такой ценой в прошлый раз
                    # if last_send_price is not None and (last_send_price == _product_price):
                    #     print(f'LAST SEND PRICE VALIDATION STOP {last_send_price} | {_product_price}')
                    #     return

                    # if actual_price < _product_price:
                    #     _text = f'🔄 Цена повысилась, но всё ещё входит в выставленный диапазон скидки на товар <a href="{link}">{name}</a>\n\nМаркетплейс: Wb\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                    #     _disable_notification = True
                    # else:
                    #     _text = f'🚨 Изменилась цена на <a href="{link}">{name}</a>\n\nМаркетплейс: Wb\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                    #     _disable_notification = False

                    channel_links = [channel.channel_id for channel in popular_product.category.channel_links]

                    _kb = create_remove_popular_kb(marker=popular_product.product.product_marker,
                                                   popular_product_id=popular_product.id)


                    # _kb = new_create_remove_and_edit_sale_kb(user_id=user_id,
                    #                                          product_id=product_id,
                    #                                          marker='wb',
                    #                                          job_id=job_id,
                    #                                          with_redirect=False)
                    # _kb = add_graphic_btn(_kb,
                    #                       user_id=user_id,
                    #                       product_id=_id)

                    # _kb = add_or_create_close_kb(_kb)

                    for channel_link in channel_links:
                        msg = await bot.send_photo(chat_id=channel_link,
                                                photo=photo_id,
                                                caption=_text,
                                                disable_notification=_disable_notification,
                                                reply_markup=_kb.as_markup())
                    
                    return

        except WbAPICrashError as ex:
            print('SCHEDULER WB API CRUSH', ex)

        except Exception as ex:
            print(ex)
            pass


async def periodic_delete_old_message(cxt,
                                      user_id: int):
    print(f'ARQ TASK DELETE OLD MESSAGE USER {user_id}')
    key = f'fsm:{user_id}:{user_id}:data'

    async with redis_client.pipeline(transaction=True) as pipe:
        user_data: bytes = await pipe.get(key)
        results = await pipe.execute()

    if results[0] is not None:
        json_user_data: dict = json.loads(results[0])

        dict_msg_on_delete: dict = json_user_data.get('dict_msg_on_delete')

        message_id_on_delete_list = []

        if dict_msg_on_delete:
            for _key in list(dict_msg_on_delete.keys()):
                chat_id, message_date = dict_msg_on_delete.get(_key)
                date_now = datetime.now()
                # тестовый вариант, удаляем сообщения старше 1 часа
                print((datetime.fromtimestamp(date_now.timestamp()) - datetime.fromtimestamp(message_date)) > timedelta(hours=36))
                if (datetime.fromtimestamp(date_now.timestamp()) - datetime.fromtimestamp(message_date)) > timedelta(hours=36):
                    message_id_on_delete_list.append(_key)
                    del dict_msg_on_delete[_key]

        async with redis_client.pipeline(transaction=True) as pipe:
            bytes_data = json.dumps(json_user_data)
            await pipe.set(key, bytes_data)
            results = await pipe.execute()

        if message_id_on_delete_list:
            iterator_count = ceil(len(message_id_on_delete_list) / 100)

            for i in range(iterator_count):
                idx = i * 100
                _messages_on_delete = message_id_on_delete_list[idx:idx+100]
                
                await bot.delete_messages(chat_id=chat_id,
                                        message_ids=_messages_on_delete)
                await asyncio.sleep(0.2)


async def add_punkt_by_user(cxt,
                            punkt_data: dict):
    punkt_action: str = punkt_data.get('punkt_action')
    city: str = punkt_data.get('city')
    city_index: str = punkt_data.get('index')
    settings_msg: tuple = punkt_data.get('settings_msg')
    user_id: int = punkt_data.get('user_id')

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession() as aiosession:
            wb_url = f"{WB_API_URL}/pickUpPoint/{city_index}"
            ozon_url = f"{OZON_API_URL}/pickUpPoint/{city_index}"

            # Wb
            async with aiosession.get(url=wb_url,
                                      timeout=timeout) as response:
                wb_del_zone = await response.text()

                print('WB DEL ZONE', wb_del_zone)
            # Ozon
            async with aiosession.get(url=ozon_url,
                                      timeout=timeout) as response:
                ozon_del_zone = await response.text()

                print('OZON DEL ZONE', ozon_del_zone)

    except Exception as ex:
        print('DEL ZONE REQUEST ERRROR', ex)
        await bot.edit_message_text(text='Что то пошло не так, просим прощения\n\nПопробуйте повторить позже',
                                    chat_id=settings_msg[0],
                                    message_id=settings_msg[-1])
        return
    
    try:
        wb_del_zone = int(wb_del_zone)
        ozon_del_zone = int(ozon_del_zone)
    except Exception as ex:
        print('RESPONSE ERROR WITH CONVERT DEL ZONE', ex)
        await bot.edit_message_text(text='Что то пошло не так, просим прощения\n\nПопробуйте повторить позже',
                                    chat_id=settings_msg[0],
                                    message_id=settings_msg[-1])
        return
    
    if punkt_action == 'add':
        check_query = (
            select(
                Punkt.id
            )\
            .where(Punkt.user_id == user_id)
        )

        async for session in get_session():
            async with session as _session:
                res = await _session.execute(check_query)
            
        has_punkt = res.scalar_one_or_none()

        if has_punkt:
            print('PUNKT ADD ERROR, PUNKT BY USER EXISTS')
            return

        insert_data = {
            'user_id': user_id,
            'index': int(city_index),
            'city': city,
            'ozon_zone': ozon_del_zone,
            'wb_zone': wb_del_zone,
            'time_create': datetime.now(),
        }

        query = (
            insert(
                Punkt
            )\
            .values(**insert_data)
        )
        success_text = f'✅ Пункт выдачи успешно добавлен (Установленный город - {city}).'
        error_text = f'❌ Не получилось добавить пункт выдачи (Переданный город - {city})'

    elif punkt_action == 'edit':
        update_data = {
            'city': city,
            'index': int(city_index),
            'ozon_zone': ozon_del_zone,
            'wb_zone': wb_del_zone,
            'time_create': datetime.now(),
        }
        query = (
            update(
                Punkt
            )\
            .values(**update_data)\
            .where(Punkt.user_id == user_id)
        )
        
        success_text = f'✅ Пункт выдачи успешно изменён (Новый установленный город - {city}).'
        error_text = f'❌ Не получилось изменить пункт выдачи (Переданный город - {city})'

    else:
        print('!!!!!!!!Такого не должно быть!!!!!!!!')
        return
    
    async for session in get_session():
        try:
            await session.execute(query)
            await session.commit()
        except Exception as ex:
            await session.rollback()
            print('ADD/EDIT PUNKT BY USER ERRROR', ex)
            await bot.edit_message_text(text=error_text,
                                        chat_id=settings_msg[0],
                                        message_id=settings_msg[-1])
        else:
            await bot.edit_message_text(text=success_text,
                                        chat_id=settings_msg[0],
                                        message_id=settings_msg[-1])

