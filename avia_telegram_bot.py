from aviasales_scrpr import load_scraper_config
from aviasales_scrpr import intiniate_result_file
from aviasales_scrpr import get_url_list
from aviasales_scrpr import scrape_per_batch
from interface import get_date_range
from mysql_scraper import save_results_in_database
from proxies import save_file_api_proxy_list
from api_module import choose_closet_airports
from api_module import make_api_price_request
import aiogram.utils.markdown as md
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import asyncio
import datetime
import logging
import pandas as pd
import os

TOKEN = "6199462379:AAGZ9Qmzs1uIejPrYnknj0hn3eIL12OVDm0"

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)

# For example use simple MemoryStorage for Dispatcher.
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


# States
class Form(StatesGroup) :
    start_point = State()
    end_point = State()
    date_range = State()
    request_complete= State()
    tickets_scraped = State()



@dp.message_handler(commands='start')
async def cmd_start (message: types.Message) :
    """
    Conversation's entry point
    """
    keyboard_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    start_button = types.KeyboardButton('/start')
    keyboard_markup.add(start_button)
    await Form.start_point.set()
    await message.reply("Hi there! In what city would we start journey?")


@dp.message_handler(state=Form.start_point)
async def process_start_point (message: types.Message, state: FSMContext) :
    """
    Process start_point
    """
    async with state.proxy() as data :
        data['start_point'] = message.text
        data['user_id'] = message.text
    await state.update_data(user_id=message.from_user.id, chat_id=message.chat.id)
    await Form.next()
    await message.reply("In what city you want to go?")


@dp.message_handler(state=Form.end_point)
async def process_end_point (message: types.Message, state: FSMContext) :
    """
    Process start_point
    """
    async with state.proxy() as data :
        data['end_point'] = message.text

    await Form.next()
    await message.reply("Insert data range in format DDMMDDMM."
                        "(e.g if you want to check tickets in July enter 01073107)")


@dp.message_handler(state=Form.date_range)
async def process_date_range (message: types.Message, state: FSMContext) :
    async with state.proxy() as data :
        data['date_range'] = message.text
        print("`Got request\n: "
              "start_point",
              get_relevant_airport_codes(data['start_point']).values,
              "end_point",
              get_relevant_airport_codes(data['end_point']).values,
              data['date_range'])
        user_id = message.from_user.id
        message_text = f"We are collecting data for you " \
                       f"from {data['start_point']}, " \
                       f"to {data['end_point']}, " \
                       f"in dates {data['date_range']}, " \
                       f"Please wait ..."
        await send_message_to_user(user_id, message_text)
    await Form.request_complete.set()
    await scraping(state)


# @dp.message_handler(state=Form.request_complete)
async def scraping (state: FSMContext):
    async with state.proxy() as data :
        print("Start scraping \n start_point",
              get_relevant_airport_codes(data['start_point']).values,
              "end_point",
              get_relevant_airport_codes(data['end_point']).values,
              data['date_range'])
        user_data = await state.get_data()
        user_id = user_data.get('user_id')
        message_text = f"interesting ... there are " \
                       f"{len(get_relevant_airport_codes(data['start_point']).values)} "\
                       f"airports near {data['start_point']} and "\
                       f"{len(get_relevant_airport_codes(data['end_point']).values)} " \
                       f"airports near {data['end_point']} "
        await send_message_to_user(user_id, message_text)
        config = load_scraper_config()
        os.remove(config['last_request_data'])
        intiniate_result_file(config['result_file'])
        intiniate_result_file(config['last_request_data'])
        start = datetime.datetime.now()
        # if config['use_proxy'] == 1 :
        #     save_file_api_proxy_list(config)

        start_list = list(get_relevant_airport_codes(data['start_point']).values)
        end_list = list(get_relevant_airport_codes(data['end_point']).values)
        url_list = get_url_list(start_list=start_list,
                                start_date=get_date_range(data['date_range'])[0],
                                days_number=get_date_range(data['date_range'])[1],
                                end_list=end_list,
                                config=config)
        if config['search_tolerance_percent'] != -1 :
            tolerance = config['search_tolerance_percent'] / 100 * len(url_list)
        else :
            tolerance = -1
        message_text = f"There are {len(url_list)} combinations. Easy for AI! "
        await send_message_to_user(user_id, message_text)

        scrape_per_batch(url_list, config, logging, tolerance)
        message_text = f"And we got the data. Give me a sec to sort it for you. "
        await send_message_to_user(user_id, message_text)
        if config['use_mysql'] ==1:
            save_results_in_database(config, logging)
        if config['use_mysql'] ==1:
            make_api_price_request(config, logging)
        if os.path.exists(config['last_request_data']) :
            last_request_df = pd.read_csv(config['last_request_data'])
            last_request_df = last_request_df[last_request_df['price'] != 'price']
            last_request_df = last_request_df[last_request_df['price'] != 'None']
            last_request_df = last_request_df[last_request_df['price'] != 'NaN']
            last_request_df['price']=last_request_df['price'].astype(int)
            if last_request_df.shape[0]>0:
                analytics_file = 'avia-analytics.xls'
                save_df_to_excel(last_request_df, analytics_file)
                with open(analytics_file, 'rb') as file :
                    await bot.send_document(data.get('chat_id'), types.InputFile(file), caption=analytics_file)
                # os.remove(analytics_file)
                if last_request_df[last_request_df['price'] == last_request_df['price'].min()].shape[0] > 0 :
                    min_price_url=last_request_df[last_request_df['price'] == last_request_df['price'].min()].apply(url_back, axis=1)
                last_request_df=last_request_df.sort_values(by=['price'])
                last_request_df['url']=last_request_df.apply(url_back, axis=1)
                # print(last_request_df.head())
                await open_custom_keyboard(state , last_request_df.head(10))
            else:
                await bot.send_message(
                    data.get('chat_id'),
                    md.text(
                        "No tickets found. Try another request!",
                        sep='\n',
                    ),
                )
        await state.finish()

@dp.message_handler(state=Form.tickets_scraped)
async def process_option(message: types.Message, state: FSMContext):
    config = load_scraper_config()
    await open_custom_keyboard(message)

async def open_custom_keyboard(state: FSMContext, dataset):
    buttons = []
    keyboard_markup = InlineKeyboardMarkup()
    for i, row in dataset.iterrows():
        button_text = f" {row['flight_date_time'][3:-3]} for {row['price']}$, {row['layovers']} layovers"
        keyboard_markup.add(InlineKeyboardButton(text=button_text, url=row['url']))
    user_data = await state.get_data()
    user_id = user_data.get('user_id')
    await bot.send_message(user_id, "Please select an option:", reply_markup=keyboard_markup)

def get_relevant_airport_codes (city_name) :
    # Normalize the city name by converting it to lowercase and removing leading/trailing spaces
    config = load_scraper_config()
    city_name = city_name.lower().strip()
    airport_codes = choose_closet_airports(config, city_name)
    print(type(airport_codes))
    return airport_codes

def url_back(row):
    datetime_obj = datetime.datetime.strptime(row['flight_date_time'],  '%Y-%m-%d %H:%M:%S')
    data_code =datetime_obj.strftime('%d%m')
    config = load_scraper_config()
    passanger="1"
    end_of_url = "request_source=search_form"
    new_link = config["link_constructor"] +\
               row['start_airport_code'] +\
               data_code + \
               row['end_airport_code'] + \
               passanger+\
               end_of_url
    return new_link

def save_df_to_excel (df, filename) :
    df.to_excel(filename, index=False, engine='openpyxl')
async def send_message_to_user(user_id: int, message: str):
    await bot.send_message(chat_id=user_id, text=message)
if __name__ == '__main__' :
    executor.start_polling(dp, skip_updates=True)
