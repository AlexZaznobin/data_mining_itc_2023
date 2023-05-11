import time

TOKEN = "6199462379:AAGZ9Qmzs1uIejPrYnknj0hn3eIL12OVDm0"

import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ParseMode
from aiogram.contrib.middlewares.logging import LoggingMiddleware

from time import sleep

text1="""
Нынче ветрено и волны с перехлестом.
Скоро осень, все изменится в округе.
Смена красок этих трогательней, Постум,
чем наряда перемена у подруги.


Дева тешит до известного предела —
дальше локтя не пойдешь или колена.
Сколь же радостней прекрасное вне тела:
ни объятья невозможны, ни измена! 
"""

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

async def on_start(message: types.Message):
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton("Лучше жить в глухой провинции", callback_data="pressed"))

    await message.reply("Если выпало в империи родиться:", reply_markup=keyboard)

async def on_button_press(call: types.CallbackQuery):
    await call.answer()
    await call.message.edit_text(text1)

dp.register_message_handler(on_start, commands=["start"])
dp.register_callback_query_handler(on_button_press, lambda c: c.data == "pressed")
executor.start_polling(dp, skip_updates=True)