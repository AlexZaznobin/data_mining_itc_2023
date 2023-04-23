import random
from word2number import w2n
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.webdriver.common.by import By
import requests
import speech_recognition as sr
import warnings
# from pydub import AudioSegment

from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

warnings.filterwarnings("ignore")
CAPCHA_AUDIO_FILE_MP3 = 'capcha_audio.mp3'
CAPCHA_AUDIO_FILE_WAV = 'capcha_audio.wav'


def read_audio_capcha () :
    time.sleep(3)
    r = sr.Recognizer()
    with sr.AudioFile(CAPCHA_AUDIO_FILE_WAV) as source :
        audio_data = r.record(source)
        audio_capcha_text = r.recognize_google(audio_data)
        number_list = audio_capcha_text.split()
        number_list_int = []
        for num_str in number_list :
            try :
                num_int = w2n.word_to_num(num_str)
                number_list_int.append(str(num_int))
            except :
                number_list_int.append(num_str)
        audio_capcha_result = ''.join(number_list_int)
        only_number = []
        for letter in audio_capcha_result :
            if letter.isnumeric() :
                only_number.append(letter)
        audio_capcha_result = ''.join(only_number)
        return audio_capcha_result


def g_capcha_solver (driver, logging) :
    logging.info(f"check if chapcha on place")
    wait = WebDriverWait(driver, 1)
    try :
        re_frame = wait.until(EC.presence_of_all_elements_located((By.XPATH, ".//iframe[@title='reCAPTCHA']")))
        logging.info(f"Capcha found")
        switch_to_audio(driver, logging)
        if request_audio_version(driver, logging) :
            text_of_capcha = read_audio_capcha()
            submit_google_audio_capcha(driver, text_of_capcha)
    except :
        logging.info(f" no capcha frame found")
        return


def set_audio_button_text (driver) :
    current_url = driver.current_url
    if "https://www.aviasales.ru/" in current_url :
        audio_button_text = ".//button[@title='Пройти аудиотест']"
    if "https://www.aviasales.com/" in current_url :
        audio_button_text = ".//button[@title='Get an audio challenge']"
    return audio_button_text


def switch_to_audio (driver, logging) :
    wait = WebDriverWait(driver, 1)
    audio_button_text = set_audio_button_text(driver)
    all_frames = wait.until(EC.presence_of_all_elements_located((By.XPATH, ".//iframe")))
    for index, frame in enumerate(all_frames) :
        driver.switch_to.frame(frame)
        buttons = driver.find_elements(By.XPATH, audio_button_text)
        if buttons != [] :
            time.sleep(random.random() * 3)
            buttons[0].click()
            logging.info(f"Capcha switch to audio version")
            break
        driver.switch_to.default_content()


def request_audio_version (driver, logging) :
    result = ''
    try :
        href_elements = driver.find_elements(By.XPATH, "//a[@href] | //link[@href] | //area[@href]")
        for href_element in href_elements :
            href = href_element.get_attribute("href")
            code=driver.current_url.split('/')[-1]
            if "audio.mp3" in href :
                result="audio.mp3"
                logging.info(f"Capcha switch to audio version was given {result}")
                response = requests.get(href)
                with open(f"{code}_audio.mp3", 'wb') as f :
                    f.write(response.content)
                # sound = AudioSegment.from_mp3(CAPCHA_AUDIO_FILE_MP3)
                # sound.export(CAPCHA_AUDIO_FILE_WAV, format="wav")
            if "our help page" in href.text:
                result="our help page"

                logging.info(f"Capcha switch to audio version was not given {result}")

        return result
    except :
        return False


# files


def submit_google_audio_capcha (driver, audio_capcha_result) :
    try :
        pass
        # href_elements = driver.find_elements(By.XPATH, "//a[@href] | //link[@href] | //area[@href]")
        # for href_element in href_elements:
        #     href = href_element.get_attribute("href")
        #     if "audio.mp3" in href:
        #         response = requests.get(href)
        #         with open(CAPCHA_AUDIO_FILE, 'wb') as f :
        #             f.write(response.content)
        # return True
    except :
        return False
