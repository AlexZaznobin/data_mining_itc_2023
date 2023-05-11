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
class Form(StatesGroup):
    start_point = State()  # Will be represented in storage as 'Form:name'
    end_point = State()  # Will be represented in storage as 'Form:age'
    date_range = State()  # Will be represented in storage as 'Form:gender'


@dp.message_handler(commands='start')
async def cmd_start(message: types.Message):
    """
    Conversation's entry point
    """
    # Set state
    await Form.start_point.set()
    await message.reply("Hi there! In what city would we start journey?")



@dp.message_handler(state=Form.start_point)
async def process_start_point(message: types.Message, state: FSMContext):
    """
    Process start_point
    """
    async with state.proxy() as data:
        data['start_point'] = message.text

    await Form.next()
    await message.reply("Where you want to go?")


@dp.message_handler(state=Form.end_point)
async def process_end_point(message: types.Message, state: FSMContext):
    """
    Process start_point
    """
    async with state.proxy() as data:
        data['end_point'] = message.text

    await Form.next()
    await message.reply("Insert data range in format DDMMDDMM."
                        "(e.g if you want to check tickets in July enter 01073107)")



@dp.message_handler(state=Form.date_range)
async def process_date_range(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        data['date_range'] = message.text
        # markup = types.ReplyKeyboardRemove()
        print("start_point",
              get_relevant_airport_codes(data['start_point']).values,
              "end_point",
              get_relevant_airport_codes(data['end_point']).values,
              data['date_range'])

        pd.set_option('display.max_columns', None)
        config = load_scraper_config()
        need_database=False
        intiniate_result_file(config['result_file'])
        intiniate_result_file(config['last_request_data'])
        start = datetime.datetime.now()
        if config['use_proxy'] == 1 :
            save_file_api_proxy_list(config)

        start_list=list(get_relevant_airport_codes(data['start_point']).values)
        end_list=list(get_relevant_airport_codes(data['end_point']).values)
        url_list= get_url_list(start_list=start_list,
                                start_date=get_date_range(data['date_range'])[0],
                                days_number=get_date_range(data['date_range'])[1],
                                end_list=end_list,
                                config=config)
        if config['search_tolerance_percent'] != -1 :
            tolerance = config['search_tolerance_percent'] / 100 * len(url_list)
        else :
            tolerance = -1

        scrape_per_batch(url_list, config, logging, tolerance)
        if need_database :
            save_results_in_database(config, logging)
        if need_database :
            make_api_price_request(config, logging)
        if os.path.exists(config['last_request_data']) :
            last_request_df=pd.read_csv(config['last_request_data'])
            analytics_file='avia-analytics.xls'
            save_df_to_excel(last_request_df, analytics_file)

            with open(analytics_file, 'rb') as file:
                await bot.send_document(message.chat.id, types.InputFile(file), caption=analytics_file)

            os.remove(analytics_file)

    await bot.send_message(
            message.chat.id,
            md.text(
                "Lets find some tickets, my friend",
                md.text('start_point,', md.code(data['start_point'])),
                md.text('end_point:', md.code(data['end_point'])),
                md.text('data_range:', md.code(data['date_range'])),
                url_list[:10],
                sep='\n',
            ),
            # reply_markup=markup,
            # parse_mode=ParseMode.MARKDOWN,
        )
    #
    # # Finish conversation
    await state.finish()

def get_relevant_airport_codes(city_name):
    # Normalize the city name by converting it to lowercase and removing leading/trailing spaces
    config = load_scraper_config()
    city_name = city_name.lower().strip()
    airport_codes=choose_closet_airports(config, city_name)
    print(type(airport_codes))
    return airport_codes

def save_df_to_excel(df, filename):
    writer = pd.ExcelWriter(filename, engine='openpyxl')
    df.to_excel(writer, index=False)
    writer.save()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)