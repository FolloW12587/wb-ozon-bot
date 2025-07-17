from typing import Union

from aiogram import types

Markup = Union[
    types.InlineKeyboardMarkup,
    types.ReplyKeyboardMarkup,
    types.ReplyKeyboardRemove,
    types.ForceReply,
]
