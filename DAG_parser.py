import logging
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator

import config

#импоритруем приложение
from .App_Parsing import Parser_Farma

#импортируем рабоче модули-парсеры
from .workers.parser_farma_planeta_zdorovie import Parser_Farma_Planeta_Zdorovie
from .workers.parser_farma_zdorovie import Parser_Farma_Zdorovie


log = logging.getLogger('Parser_Farma_log')
logging.basicConfig(level=logging.INFO)


@dag( 
    schedule_interval='0 04 * * *',  # Задаем расписание выполнения дага - каждый  день в 4 урта.
    start_date=pendulum.datetime(2024, 10, 10, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - True (нужно).
    tags=['Parser_Farma'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу остановлен.
)
def StartParsing():

    # Получаем аргументы для подключения к ДБ
    pg_conn_arg = config.pg_arg

    #ВОКЕРЫ
    def Zdorovie_Farma_Parsing():
        Parser_Farma(pg_conn_arg, log, Parser_Farma_Zdorovie, config.data_zdorovie).StartParsing()

    def Palneta_Zdorovie_Farma_Parsing():
        Parser_Farma(pg_conn_arg, log, Parser_Farma_Planeta_Zdorovie, config.data_planeta_zdorovie).StartParsing()


    @task_group(group_id='Farma_Parsing')
    def task_group_1():
        task_Zdorovie_Farma_Parsing = PythonOperator(
        task_id='Magnit_Farma_Parsing',
        python_callable = Zdorovie_Farma_Parsing,
        )   

        task_Palneta_Zdorovie_Farma_Parsing = PythonOperator(
        task_id='Magnit_Farma_Parsing',
        python_callable = Palneta_Zdorovie_Farma_Parsing,
        )   
    
    
    

    task_group_1() 

Start_DAG = StartParsing()