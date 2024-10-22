import requests
import datetime
import time
import logging
import config

import user_agents_selection
from db_operator import stg_data_loader


class Parser_Farma():
    
    def __init__(self, pg_conn_arg:dict, logger: logging, parser, sourse_data:dict) -> None:

        self.pg_conn_arg = pg_conn_arg
        self.logger = logger
        self.max_try_count_for_page = config.max_try_count_for_page
        self.max_try_count_for_url = config.max_try_count_for_url
        self.retry_time = config.retry_time
        self.parser = parser
        self.url = sourse_data ['url']
        self.categorys = sourse_data['categorys']
        self.sourse = sourse_data['sourse']
        self.global_error = False
    
    
    def StartParsing(self):

        #Парсим каждую категорию на сайте в каталоге отдельно
        for category in self.categorys:

            # Если во время парсинга сработал тригет глобальной ошибки прерываем парсинг сайта
            if self.global_error == True:
                break

            # Список данных по каджой категории
            data = []

            # Постраничный обход каждой категории в каталоге
            continueParsing = True
            page = 1
            try_count = 1
            try_count_for_url = 1

            while continueParsing:

                #Выбираем случайный заголовок для User-Agent
                header = user_agents_selection.User_agent_selection()
                headers = {"User-Agent": header}

                #Определяем url в зависимости от каталога и и страницы
                url = self.url.format(category,page)

                # Скачиваем HTML страницы по очереди
                response = requests.get(url, headers=headers)

                # Проверяем валидность полученного ответа
                if response.status_code == 200:

                    # Сырые данные страницы отправляем в Парсер и получаем обработанные данные по каждой странице
                    _data = self.parser(response, category, self.sourse)

                    #Прерыватель поиска
                    # Если первая позиция на новой странице совпадает с первой позицией в этой категории значит пошли по кругу.
                    # Прерываем считывание по этой категории 
                    try:
                        if _data[0] == data[0][0]:
                            continueParsing = False
                            break
                    except:
                        pass

                    #  Добавояем данные станицы в финальный списко данные по категории
                    data.append(_data)

                    #Увеличиваем номер считываемой страницы
                    page += 1

                else:
    
                    # Если от сервера пришел ответ отличный от 200 увеличиваем счетчик попыток и пробуем перезапустить парсинг этой страницы через 10 секунд
                    try_count += 1
                    try_count_for_url = +1
                    time.sleep(self.retry_time)

                    # Если достигнутов максимально количесво попыток подключения к ulr препываем парсинг сайта, значит либо нас забанили, либо изменена структура url.
                    if try_count_for_url == self.max_try_count_for_url:
                        self.logger.info(f"{datetime.datetime.now()}: FAILD parsing {self.sourse}. Upload_day = {datetime.datetime.today()}, GLOBAL ERROR")
                        self.global_error = True
                        break

                    # Если превышено максимальное число попыток прочитать страницу пишем в лог и пробуем следующую страницу
                    if try_count == self.max_try_count_for_page:

                        self.logger.info(f"{datetime.datetime.now()}: FAILD parsing {self.sourse}. Upload_day = {datetime.datetime.today()}, category = {category}, page = {page}.")

                        #Увеличиваем номер считываемой страницы
                        page += 1


            # Загружаем данные в БД по текущей категории 
            stg_data_loader(data, self.pg_conn_arg).LoadData()





