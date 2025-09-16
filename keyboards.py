from typing import Literal

from aiogram import types
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

import config


def create_start_kb():
    _kb = InlineKeyboardBuilder()
    _kb.add(types.InlineKeyboardButton(text="WB бот", callback_data="bot_wb"))
    _kb.row(types.InlineKeyboardButton(text="OZON бот", callback_data="bot_ozon"))
    # _kb.row(types.InlineKeyboardButton(text='Посмотреть цену товара',
    #                                    callback_data='check_price'))

    return _kb


def create_wb_start_kb():
    _kb = InlineKeyboardBuilder()
    _kb.add(
        types.InlineKeyboardButton(
            text="Добавить пункт выдачи", callback_data="add_punkt"
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="Посмотреть свои пункты выдачи", callback_data="list_punkt"
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="Посмотреть цену товара", callback_data="check_price"
        )
    )

    return _kb


def create_bot_start_kb(marker: Literal["wb", "ozon"]):
    _kb = InlineKeyboardBuilder()

    if marker == "wb":
        _kb.add(
            types.InlineKeyboardButton(
                text="Добавить пункт выдачи", callback_data="add_punkt"
            )
        )
        _kb.row(
            types.InlineKeyboardButton(
                text="Посмотреть свои пункты выдачи", callback_data="list_punkt"
            )
        )
        _kb.row(
            types.InlineKeyboardButton(
                text="Добавить товар", callback_data="add_wb_product"
            )
        )
        _kb.row(
            types.InlineKeyboardButton(
                text="Посмотреть товары", callback_data="view_price"
            )
        )

    else:
        _kb.add(
            types.InlineKeyboardButton(
                text="Добавить товар", callback_data="add_product"
            )
        )
        _kb.row(
            types.InlineKeyboardButton(
                text="Посмотреть товар", callback_data="list_product"
            )
        )
        _kb.row(
            types.InlineKeyboardButton(
                text="Удалить все товары", callback_data="remove_all_products"
            )
        )

    return _kb


def create_or_add_cancel_btn(_kb: InlineKeyboardBuilder = None):
    if _kb is None:
        _kb = InlineKeyboardBuilder()

    _kb.row(types.InlineKeyboardButton(text="Отменить", callback_data="cancel"))

    return _kb


def create_or_add_exit_btn(_kb: InlineKeyboardBuilder = None):
    if _kb is None:
        _kb = InlineKeyboardBuilder()

    _kb.row(types.InlineKeyboardButton(text="Закрыть", callback_data="exit"))

    return _kb


def add_back_btn(_kb: InlineKeyboardBuilder):
    _kb.row(types.InlineKeyboardButton(text="На главную", callback_data="to_main"))

    return _kb


def create_done_kb(marker: Literal["wb_punkt", "wb_product", "ozon_product"]):
    _kb = InlineKeyboardBuilder()

    _kb.row(
        types.InlineKeyboardButton(text="Отправить", callback_data=f"done__{marker}")
    )

    return _kb


def create_go_to_subscription_kb():
    _kb = InlineKeyboardBuilder()
    _kb.add(
        types.InlineKeyboardButton(
            text="Перейти к оформлению", callback_data="subscription"
        )
    )
    return _kb


def create_cancel_edit_sale_kb():
    _kb = InlineKeyboardBuilder()
    _kb.add(
        types.InlineKeyboardButton(
            text="Отменить редактирование скидки", callback_data="cancel_edit_sale"
        )
    )
    return _kb


def create_remove_kb(
    user_id: int,
    product_id: str,
    marker: Literal["wb", "ozon"],
    job_id: str,
    _kb: InlineKeyboardBuilder = None,
    with_redirect: bool = True,
):
    if not _kb:
        _kb = InlineKeyboardBuilder()

    _callback_data = f"{marker}_{user_id}_{product_id}_{job_id}"

    if with_redirect:
        _callback_data = f"delete_{_callback_data}"
        # _text = 'Удалить товар'
    else:
        _callback_data = f"delete.no.rd_{_callback_data}"

    _text = "Перестать отслеживать"

    _kb.row(types.InlineKeyboardButton(text=_text, callback_data=_callback_data))

    return _kb


# new
def new_create_remove_and_edit_sale_kb(
    user_id: int,
    product_id: str,
    marker: Literal["wb", "ozon"],
    job_id: str,
    _kb: InlineKeyboardBuilder = None,
    with_redirect: bool = True,
):
    if not _kb:
        _kb = InlineKeyboardBuilder()

    _callback_data = f"new_{marker}_{user_id}_{product_id}"

    if with_redirect:
        delete_callback_data = f"delete.new_{_callback_data}_{job_id}"
        edit_sale_callback_data = f"edit.new.sale_{_callback_data}"
        graphic_callback_data = f"graphic_{user_id}_{product_id}"
    else:
        delete_callback_data = f"delete.new.no.rd_{_callback_data}_{job_id}"
        edit_sale_callback_data = f"edit.new.sale.no.rd_{_callback_data}"
        graphic_callback_data = f"graphic.bg_{user_id}_{product_id}"

    _kb.row(
        types.InlineKeyboardButton(
            text="Изменить сумму скидки", callback_data=edit_sale_callback_data
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="График изменения цен", callback_data=graphic_callback_data
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="Перестать отслеживать", callback_data=delete_callback_data
        )
    )

    return _kb


def create_back_to_product_btn(
    user_id: int, product_id: int, is_background_task: bool = False
):
    _kb = InlineKeyboardBuilder()

    if not is_background_task:
        _kb.row(
            types.InlineKeyboardButton(
                text="Вернуться к товару",
                callback_data=f"back_to_product_{user_id}_{product_id}",
            )
        )
    else:
        _kb.row(
            types.InlineKeyboardButton(
                text="Вернуться к товару",
                callback_data=f"back_to_product.bg_{user_id}_{product_id}",
            )
        )

    return _kb


def create_photo_keyboard(kb_init: str):
    product_kb = InlineKeyboardBuilder()
    match kb_init:
        case "start":
            product_kb.add(
                types.InlineKeyboardButton(
                    text="Следующая", callback_data="product_next"
                )
            )
        case "mid":
            product_kb.add(
                types.InlineKeyboardButton(
                    text="Предыдущая", callback_data="product_prev"
                )
            )
            product_kb.add(
                types.InlineKeyboardButton(
                    text="Следующая", callback_data="product_next"
                )
            )
        case "end":
            product_kb.add(
                types.InlineKeyboardButton(
                    text="Предыдущая", callback_data="product_prev"
                )
            )

    # product_kb.row(types.InlineKeyboardButton(text='Назад',
    #                                         callback_data='cancel'))
    return product_kb


def add_cancel_btn_to_photo_keyboard(_kb: InlineKeyboardBuilder):
    _kb.row(types.InlineKeyboardButton(text="Назад", callback_data="cancel"))
    return _kb


def create_product_list_kb(
    user_id: int, product_list: list, marker: Literal["wb", "ozon"]
):
    _kb = InlineKeyboardBuilder()

    for product in product_list:
        (
            product_id,
            _,  # link,
            _,  # actaul_price,
            _,  # start_price,
            user_id,
            _,  # time_create,
            name,
            _,  # sale,
            _,  # job_id,
        ) = product
        _callback_data = f"view-product_{user_id}_{marker}_{product_id}"

        _kb.row(types.InlineKeyboardButton(text=name, callback_data=_callback_data))

    return _kb


def add_or_create_close_kb(_kb: InlineKeyboardBuilder = None):
    if not _kb:
        _kb = InlineKeyboardBuilder()

    _kb.row(types.InlineKeyboardButton(text="Закрыть", callback_data="close"))

    return _kb


def create_reply_start_kb():
    _kb = ReplyKeyboardBuilder()

    # _kb.add(types.KeyboardButton(text='Добавить товар'))
    _kb.add(types.KeyboardButton(text="Посмотреть товары"))
    # _kb.add(types.KeyboardButton(text="Подписка"))
    _kb.add(types.KeyboardButton(text="Подписка"))
    _kb.row(types.KeyboardButton(text="Настройки"), width=1)

    return _kb


# new
def new_create_product_list_for_page_kb(product_list_for_page: list[tuple]):
    _kb = InlineKeyboardBuilder()

    for product in product_list_for_page:
        (
            product_id,
            _,  # link,
            _,  # actual,
            _,  # start,
            user_id,
            _date,
            marker,
            name,
            _,  # sale,
            _,  # job_id,
        ) = product

        _callback_data = f"view-product_{user_id}_{marker}_{product_id}"

        _kb.row(
            types.InlineKeyboardButton(
                text=f"{marker.upper()} || {name}", callback_data=_callback_data
            )
        )
    return _kb


def new_add_pagination_btn(_kb: InlineKeyboardBuilder, product_dict: dict):
    pages = product_dict.get("pages")
    len_product_list = product_dict.get("len_product_list")
    current_page = product_dict.get("current_page")

    kb_init: str

    if len_product_list <= 5:
        kb_init = "one"
    else:
        if current_page == 1:
            kb_init = "start"
        elif 1 < current_page < pages:
            kb_init = "mid"
        else:
            kb_init = "end"

    match kb_init:
        case "start":
            _kb.row(
                types.InlineKeyboardButton(
                    text=f"{current_page}/{pages}", callback_data="new_pagination_page"
                )
            )
            _kb.add(types.InlineKeyboardButton(text="▶", callback_data="new_page_next"))
        case "mid":
            _kb.row(types.InlineKeyboardButton(text="◀", callback_data="new_page_prev"))
            _kb.add(
                types.InlineKeyboardButton(
                    text=f"{current_page}/{pages}", callback_data="new_pagination_page"
                )
            )
            _kb.add(types.InlineKeyboardButton(text="▶", callback_data="new_page_next"))
        case "end":
            _kb.row(types.InlineKeyboardButton(text="◀", callback_data="new_page_prev"))
            _kb.add(
                types.InlineKeyboardButton(
                    text=f"{current_page}/{pages}", callback_data="new_pagination_page"
                )
            )

    return _kb


def new_create_or_add_return_to_product_list_btn(_kb: InlineKeyboardBuilder = None):
    if not _kb:
        _kb = InlineKeyboardBuilder()

    _kb.row(
        types.InlineKeyboardButton(
            text="Вернуться к списку товаров",
            callback_data="new_return_to_product_list",
        )
    )

    return _kb


def new_create_pagination_page_kb(product_dict: dict):
    _kb = InlineKeyboardBuilder()

    current_page = product_dict.get("current_page")
    pages = product_dict.get("pages")

    for page_num in range(1, pages + 1):
        _text = f"Страница {page_num}"

        if page_num == current_page:
            _text = _text + ("(выбранная)")

        _kb.row(
            types.InlineKeyboardButton(
                text=_text, callback_data=f"new_go_to_page_{page_num}"
            )
        )

    return _kb


def create_settings_kb():
    _kb = InlineKeyboardBuilder()

    _kb.add(
        types.InlineKeyboardButton(text="🏙️Пункт выдачи", callback_data="settings_punkt")
    )
    _kb.row(types.InlineKeyboardButton(text="👫Пригласить", callback_data="settings_invite"))
    _kb.row(types.InlineKeyboardButton(text="❓FAQ", callback_data="settings_faq"))
    _kb.row(
        types.InlineKeyboardButton(text="⚙️Тех. поддержка", url=config.SUPPORT_BOT_URL)
    )

    # _kb.row(types.InlineKeyboardButton(text='Информация о боте',
    #                                    callback_data='settings_company'))

    return _kb


def create_subscription_kb(has_active_subscription: bool, payment_url: str):
    _kb = InlineKeyboardBuilder()

    if has_active_subscription:
        text = "Продлить подписку"
    else:
        text = "Оформить подписку"

    _kb.row(types.InlineKeyboardButton(text=text, url=payment_url))
    return _kb


def create_specific_settings_block_kb(
    marker: Literal["wb", "ozon"], has_punkt: str = None
):
    _kb = InlineKeyboardBuilder()

    if has_punkt:
        _text = f"Изменить {marker.upper()} пункт выдачи"
        _callback_data = f"punkt_edit_{marker}"
    else:
        _text = f"Добавить {marker.upper()} пункт выдачи"
        _callback_data = f"punkt_add_{marker}"

    _kb.row(types.InlineKeyboardButton(text=_text, callback_data=_callback_data))

    if has_punkt:
        _delete_text = f"Удалить {marker.upper()} пункт выдачи"
        _delete_callback_data = f"punkt_delete_{marker}"

        _kb.row(
            types.InlineKeyboardButton(
                text=_delete_text, callback_data=_delete_callback_data
            )
        )

    return _kb


def create_punkt_settings_block_kb(has_punkt: str = None):
    _kb = InlineKeyboardBuilder()

    if has_punkt:
        _text = "Изменить пункт выдачи"
        _callback_data = "punkt_edit"
    else:
        _text = "Добавить пункт выдачи"
        _callback_data = "punkt_add"

    _kb.row(types.InlineKeyboardButton(text=_text, callback_data=_callback_data))

    if has_punkt:
        _delete_text = "Удалить пункт выдачи"
        _delete_callback_data = "punkt_delete"

        _kb.row(
            types.InlineKeyboardButton(
                text=_delete_text, callback_data=_delete_callback_data
            )
        )

    return _kb


def create_faq_kb():
    _kb = InlineKeyboardBuilder()

    _kb.row(types.InlineKeyboardButton(text="FAQ", callback_data="faq"))

    return _kb


def create_back_to_faq_kb():
    _kb = InlineKeyboardBuilder()

    _kb.row(types.InlineKeyboardButton(text="Вернуться", callback_data="back_to_faq"))

    return _kb


def create_or_add_exit_faq_btn(_kb: InlineKeyboardBuilder = None):
    if _kb is None:
        _kb = InlineKeyboardBuilder()

    _kb.row(types.InlineKeyboardButton(text="Закрыть", callback_data="exit_faq"))

    return _kb


def create_question_faq_kb():
    _kb = InlineKeyboardBuilder()

    _kb.row(
        types.InlineKeyboardButton(
            text="Как добавить товар к отслеживанию?",
            callback_data="question_add_product",
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="Как посмотреть добавленные товары?",
            callback_data="question_view_product",
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="Как изменить сумму скидки у товаров?",
            callback_data="question_edit_sale_product",
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="Как перестать отслеживать скидку?",
            callback_data="question_delete_product",
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="Какие уведомления приходят?",
            callback_data="question_send_push_product",
        )
    )
    _kb.row(
        types.InlineKeyboardButton(
            text="Из каких стран можно отслеживать товар?",
            callback_data="question_country_product",
        )
    )

    return _kb


def create_remove_popular_kb(marker: str, popular_product_id: int):
    _kb = InlineKeyboardBuilder()

    _kb.row(
        types.InlineKeyboardButton(
            text="Перестать отслеживать товар",
            callback_data=f"popular_product:{marker}:{popular_product_id}",
        )
    )

    return _kb
