import time
import datetime          # Библиотека для тайм-менеджмента
import pymysql
import requests
import hashlib      # Библиотека для отправки запросов
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from vk_api import VkApi
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import random
## importing the load_dotenv from the python-dotenv module
from dotenv import load_dotenv

## using existing module to specify location of the .env file
from pathlib import Path
import os

load_dotenv()
env_path = Path('.')/'.env'
load_dotenv(dotenv_path=env_path)

# API-ключ созданный ранее 
vkBotSession = VkApi(token=os.getenv("VK_TOKEN"))
longPoll = VkBotLongPoll(vkBotSession, 203024117)
vk = vkBotSession.get_api()

def send_message(message):
    vk.messages.send(
        peer_id = os.getenv("VK_PEERID"),
        group_id = os.getenv("VK_GROUPID"),
        message = message,
        random_id = random.randint(1, 1000),
        dont_parse_links = 1
    )

# создаём БД
conn = pymysql.connect(host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        db='ads',
        charset='utf8mb4')
cur = conn.cursor()
conn.commit()

def replace_str(string):
    string = string.replace("\xa0", " ")
    string = string.replace("\r\n", " ")
    string = string.replace("\n", " ")
    return string
# Получаем HTML всей страницы
def get_html(url):
    while True:
        try:
            requests.Session()
            response = requests.get(url, headers={'User-Agent': UserAgent().chrome})
            return response.text
        except:
            print("Connection refused by the server..")
            print("Let me sleep for 60 seconds")
            print("ZZzzzz...")
            time.sleep(60)
            print("Was a nice sleep, now let me continue...")


# Получаем ссылки на разделы *квартиры*/*дома*/ и т.д.
def get_section_url(html):
    i = 0
    item = []
    soup = BeautifulSoup(html, 'html.parser')
    objects = soup.find('div', {"class": "scheme"})
    url_html = objects.select('a')
    for url in url_html:
        item.insert(i, 'http://нашгород-35.рф' + url.attrs['href'])
        i += 1
    # удаляем ненужнеые разделы /*главная*/*аренда*/
    item.pop(4)
    return item

def get_url_pages(section_url):
    i = 0
    categories = []
    for section in section_url:
        pages_info = []
        item_html = get_html(section)
        soup = BeautifulSoup(item_html, 'html.parser')
        html_n_pages = soup.find('div', class_="pagination")
        number_none = html_n_pages.select_one("ul")
        if number_none:
            pages_info.insert(0, html_n_pages.select_one("li:nth-last-child(2)").text.strip())
        else:
            pages_info.insert(0, 1)
        pages_info.insert(1, section)
        categories.append(pages_info)
        i += 1
    return categories

# Получаем ссылки на все страницы по категориям и ссылки на все объекты
def get_pages(category_url):
    # j = 0
    categories_pages = []
    for category in category_url:
        object_url = []
        number = int(category[0])
        for i in range(number):
            i += 1
            object_url.insert(0, category[1] + '?&page=' + str(i))
        object_url.reverse()
        categories_pages.append(object_url)
    return categories_pages
    
def get_objects(pages):
    items = []
    # перебор по категориям
    for page in pages:
        item_html = get_html(page[0])
        soup = BeautifulSoup(item_html, 'html.parser')
        html_item = soup.find('div', class_="row products_container")
        items_url = html_item.find_all('div', class_="cart clearfix")
        for item in items_url:
            item_data = []
            item = item.find('a')
            # ссылка на объект
            item_data = []
            item_url = 'http://нашгород-35.рф' + item.attrs['href']
            item_data.insert(0, item_url)
            # mb5 хэш ссылки
            item_data.insert(1, hashlib.md5(item_url.encode('utf-8')).hexdigest())
            # тип объекта
            item_data.insert(2, item.select_one("b:first-child").text.strip())
            # адресс объекта
            item_data.insert(3, replace_str(item.select_one("b:nth-child(2)").text.strip()))
            # информация по объекту
            item_info = item.select_one('span', class_="info")
            item_info = item_info.select_one('p').text.strip()
            item_data.insert(4, replace_str(item_info))
            # цена объекта
            item_data.insert(5, item.find('b', class_="price122").text.strip())
            items.append(item_data)
    return items

# получаем последние 20 статей в базе
def get_last_obj():
    last_obj = []
    cur.execute("""SELECT url_hash FROM object""")
    get_last_obj = cur.fetchall()
    for obj in get_last_obj:
        last_obj.append(obj[0])
    return last_obj

# получаем html статьи и все данные
def add_intodb(items_data, last_obj): 
    for item in items_data:
        item_intodb= []
        if item[1] in last_obj:
            print('--------------------------------------------------')
            print("Объект уже в базе...")
            cur.execute("SELECT price FROM object WHERE url_hash = %s;", (item[1],))
            get_price = cur.fetchone()
            get_price = get_price[0]
            if str(item[5]) == str(get_price):
                print("Цена совпадает...")
            else:
                print('Меняем цену в базе...')
                cur.execute("Update object set price = %s where url_hash = %s;", (item[5], item[1])) #Записываем в БД
                conn.commit()
                message = 'Изменилась цена на объект АН "Наш город" \n Коммерция: ' + item[3] + '\n Новая цена: ' + item[5] + '\n Старая цена: ' + get_price + "\n Ссылка: " + item[0]
                send_message(message)
        else:
            print('--------------------------------------------------')
            print('Записываем объект в базу...')
            #Записываем данные в БД
            item_intodb.insert(0, item[2]) #type_object
            item_intodb.insert(1, item[0]) #url
            item_intodb.insert(2, item[1]) #url_hash
            item_intodb.insert(3, item[3]) #address
            item_intodb.insert(4, item[4]) #info
            item_intodb.insert(5, item[5]) #price
            dt_now = datetime.datetime.now()
            item_intodb.insert(7, dt_now) #записываем дату и время записи в БД
            cur.execute("INSERT INTO object(type_object, url, url_hash, address, info, price, created_at) VALUES(%s, %s, %s, %s, %s, %s, %s);", item_intodb) #Записываем в БД
            conn.commit()
            message = 'АН "Наш город" \n' + item[2] + ": " + item[3] + '\n Цена: ' + item[5] + "\n Ссылка: " + item[0]
            send_message(message)

def main():
    url = 'http://нашгород-35.рф/'
    html = get_html(url)
    section = get_section_url(html)
    categories = get_url_pages(section)
    pages = get_pages(categories)
    items = get_objects(pages)
    last_obj = get_last_obj()
    add_intodb(items, last_obj)
    print("КОНЕЦ ЦИКЛА")
    return {
        'statusCode': 200,
        'body': 'Success parsed',
        'isBase64Encoded': False,
    } 
while True:
    if __name__ == '__main__':
        main()
        time.sleep(3000)