import os
import schedule
import telebot
import psycopg2
from time import sleep 
from hashlib import md5
from google_drive_downloader import GoogleDriveDownloader as gdd

CHAT_ID  = os.environ['CHAT_ID']
BOT_API_KEY = os.environ['BOT_API_KEY']
DATABASE_URL = os.environ['DATABASE_URL']

bot = telebot.TeleBot(BOT_API_KEY)

def job():
    global CHAT_ID, DATABASE_URL
    db = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = db.cursor()
    gdd.download_file_from_google_drive(file_id = '1M4P09Omqr03qvQdy-jfxQtu5dNio_Sj2', dest_path = './1.pdf')
    gdd.download_file_from_google_drive(file_id = '1HcAvWzYY4RN6EmUa1A7MWrbkd_CmvJoc', dest_path = './2.pdf')
    file_1 = open('1.pdf', 'rb')
    file_2 = open('2.pdf', 'rb')
    file_1_sum  = md5(file_1.read()).hexdigest()
    file_2_sum  = md5(file_2.read()).hexdigest()
    cursor.execute('select rule_1_sum, rule_2_sum from check_sum where id = 1')
    db_date = cursor.fetchone()
    if file_1_sum != db_date[0] or file_2_sum != db_date[1]:
        if file_1_sum != db_date[0] and file_2_sum != db_date[1]:
            message = bot.send_message(CHAT_ID, "Правила Обновлены https://drive.google.com/file/d/1M4P09Omqr03qvQdy-jfxQtu5dNio_Sj2/view?usp=sharing")
            bot.pin_chat_message(CHAT_ID, message.message_id)
            bot.send_message(CHAT_ID, 'https://drive.google.com/file/d/1HcAvWzYY4RN6EmUa1A7MWrbkd_CmvJoc/view?usp=sharing')
            cursor.execute('update check_sum set rule_1_sum = %s, rule_2_sum = %s', (file_1_sum, file_2_sum,))
            db.commit()
            print('Task finish successful')
        elif file_1_sum != db_date[0]:
            message = bot.send_message(CHAT_ID, "Правило обновленно https://drive.google.com/file/d/1M4P09Omqr03qvQdy-jfxQtu5dNio_Sj2/view?usp=sharing")
            bot.pin_chat_message(CHAT_ID, message.message_id)
            cursor.execute('update check_sum set rule_1_sum = ?', file_1_sum)
            db.commit()
            print('Task finish successful')
        elif file_2_sum != db_date[1]:
            message = bot.send_message(CHAT_ID, "Правило обновленно https://drive.google.com/file/d/1HcAvWzYY4RN6EmUa1A7MWrbkd_CmvJoc/view?usp=sharing")
            bot.pin_chat_message(CHAT_ID, message.message_id)
            cursor.execute('update check_sum set rule_2_sum = ?', file_2_sum)
            db.commit()
            print('Task finish successful')
        file_1.close()
        file_2.close()
        try:
            os.remove('1.pdf')
        except FileNotFoundError:
            pass
        try:
            os.remove('2.pdf')
        except FileNotFoundError:
            pass
    else:
        print('Task finish successful rr')

schedule.every(6).hours.do(job)

while True:
    schedule.run_pending()
    sleep(1)
