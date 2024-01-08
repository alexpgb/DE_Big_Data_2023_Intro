"""
ДЗ для задания 3 по курсу Data Engeneer основы BigData
1. Соберите данные о погоде в разных городах мира за последний месяц. Используйте открытые источники данных, такие как API погодных сервисов или веб-скрейпинг.
2. Выведите график изменения температуры в разных городах, график распределения температуры.
3. Сохранить результаты в HDFS
4. Выгрузить результаты из HDFS на локальный компьютер
"""

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator, ExternalPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)

FILES_NAME_FOR_DOWNLOAD = {}
                        #    {
                        #    '<location_code>':{'file': 'open-meteo-{location_code}_{2023-11-30}_{2023-12-30}.csv',
                        #                       'coordinates':{'lat':<int>, 'long': <int>}},
                        #      ...   
                        # }

KEY_FILE = 'file'
KEY_COORDINATES = 'coordinates'
FILES_PROCESSORS = []
URL_DOWNLOAD_FROM = 'https://archive-api.open-meteo.com/v1/archive'
DIR_DOWNLOAD_TO = '/home/gbss/course_de_bd/hw3/downloads_tmp'
DIR_MERGED_TO = '/home/gbss/course_de_bd/hw3/merged'
FILE_MERGED = 'temp_merged.csv'
PATH_TO_PYTHON_FOR_LOAD_TO_HIVE = '/home/gbss/course_de_bd/venv_hadoop_cluster/bin/python'
PATH_TO_PYTHON_FOR_TRANSFORM = '/home/gbss/course_de_etl/pd/bin/python'
START_DATE = datetime(2023, 11, 30)
END_DATE = datetime(2023, 12, 30)

DOWNLOAD_FILES = True
MERGE_FILES = True
LOAD_DATA = True

def check_downloaded_files(**kwargs):
    dir_download_to = kwargs['dir_download_to']
    files_name_for_download = kwargs['files_name_for_download']
    for file in files_name_for_download:
        full_name = str(Path(Path(dir_download_to) / file))
        is_exists = Path(full_name).exists()
        logging.info(f'{full_name} is exists {is_exists}')
        if not is_exists:
            return False
    return True

def get_locations_codes():
    """
    Функция заглушка
    Возвращает коды локаций
    Предполагается, что модуль будет извлекать коды локаций по которым нужно делать запрос из БД
    """
    return ['chel', 'rio', 'berl']

def get_locations_coordinates(location_codes: list([str]))-> dict:
    """
    Функция заглушка
    Возвращает координаты локаций по кодам
    Предполагается, что модуль будет извлекать координаты локаций по которым нужно делать запрос из БД
    """
    locality = {
        'chel':{'lat':55.149384, 'long': 61.38},
        'berl':{'lat':52.5244, 'long': 13.4105},
        'rio':{'lat':-22.90278, 'long': -43.2075},
        }
    result = {}
    for el in location_codes:
        if el in locality:
            result[el] = locality[el]
    return result


def get_files_name(coordinates:dict, start_date:datetime, end_date:datetime)->dict:
    files_name_for_download = {}
    for loc in coordinates:
        files_name_for_download[loc] = {}
        files_name_for_download[loc][KEY_COORDINATES] = coordinates[loc]
        files_name_for_download[loc][KEY_FILE] =  f'open-meteo-{loc}_{start_date.strftime("%Y-%m-%d")}_{end_date.strftime("%Y-%m-%d")}.csv'
    return files_name_for_download

location_codes = get_locations_codes()

locations_coordinates = get_locations_coordinates(location_codes)

FILES_NAME_FOR_DOWNLOAD = get_files_name(locations_coordinates , START_DATE, END_DATE)

print()

@dag(
    description='Homework in seminar 3 Bigdata course',
    start_date=datetime(2023, 1, 1),
#   https://stackoverflow.com/questions/53191639/airflow-set-dag-to-not-be-automatically-scheduled    
    schedule='@once',
    is_paused_upon_creation=True,
    catchup=False,
    tags=['Homework', 'Seminar 3', 'BD'], 
    )


def bd_l3_s3_hw():

    @task
    def log_result(files_name_for_download):
        logging.info(f'{files_name_for_download}')
        return
    log_result_instance = log_result(FILES_NAME_FOR_DOWNLOAD)

    tasks_downloads = []
    if DOWNLOAD_FILES:
        for key, val in FILES_NAME_FOR_DOWNLOAD.items():
            downloads = BashOperator(
                task_id=f'download_{key}',
                bash_command=f'curl -sSLo {DIR_DOWNLOAD_TO}/{val[KEY_FILE]} '\
                f'"{URL_DOWNLOAD_FROM}?latitude={val[KEY_COORDINATES]["lat"]}&longitude={val[KEY_COORDINATES]["long"]}&start_date={START_DATE.strftime("%Y-%m-%d")}&end_date={END_DATE.strftime("%Y-%m-%d")}&hourly=temperature_2m&timezone=auto&format=csv"',
                )
            tasks_downloads.append(downloads)
    else:
        downloads = EmptyOperator(task_id="skip_download")
        tasks_downloads.append(downloads)

    files_is_downloaded = ShortCircuitOperator(
        task_id="condition_is_True",
        python_callable=check_downloaded_files,
        op_kwargs={'dir_download_to':DIR_DOWNLOAD_TO, \
                'files_name_for_download':[val[KEY_FILE] for val in FILES_NAME_FOR_DOWNLOAD.values()]}, # список наименований файлов
        )   

    # В литературе по Airflow не рекомендуют передвать обрабатываемую информацию между задачами напрямую т.к.:
    # 1. Есть ограничения на объем передаваемой информации.
    # 2. Возможны ошибки связанные с исчерпанием памяти.
    # 3. Это не совпадает с иделологией использования Airflow как оркестратора а не обработчика данных.
    # Поэтому принято решение передавать не данные а ссылки на скачанные файлы.

    # Реализуем запуск обработки в отдельном окружении, чтобы избежать зависимостей Airflow 
    # и зависимостей, необходимых для обработки данных

    if MERGE_FILES:
        @task.external_python(task_id='run_transform', python=PATH_TO_PYTHON_FOR_TRANSFORM)
        def run_transform(path_to_source_files: str, source_files: dict, path_to_target_file: str, target_file: str):
            import sys
            import os
            sys.path.insert(0, '/home/gbss/airflow/dags')
            # import os
            # os.chdir('/home/gbss/airflow/dags')
            from pythonscripts.procdata_de_bd_hw3.de_bd_hw3_procdata import transform
            transform(path_to_source_files, source_files, path_to_target_file, target_file)
            return
            # return os.path.abspath(__file__), os.getcwd(), sys.path
        run_transform_instance = run_transform(DIR_DOWNLOAD_TO, FILES_NAME_FOR_DOWNLOAD, DIR_MERGED_TO, FILE_MERGED)
    else:
        run_transform_instance = EmptyOperator(task_id="skip_transform")
  
    
    if LOAD_DATA:
        @task.external_python(task_id='load_data', python=PATH_TO_PYTHON_FOR_LOAD_TO_HIVE)
        def load_data(path_to_target_file: str, target_file: str)->None:
            import sys
            import os
            sys.path.insert(0, '/home/gbss/airflow/dags')
            from pythonscripts.procdata_de_bd_hw3.de_bd_hw3_request_fom_hive import load_file_from_host_to_container_by_subprocess,\
                                                                                    load_file_from_container_to_hdfs_by_subprocess,\
                                                                                    load_data_to_hive
            load_file_from_host_to_container_by_subprocess('namenode-de', path_to_target_file, target_file,
                                                             '/tmp/', target_file)
            load_file_from_container_to_hdfs_by_subprocess('namenode-de', '/tmp/', target_file,
                                                            '/tmp/', target_file)
            load_data_to_hive('/tmp/' + target_file)
            return
            # except Exception as e:
            #     return 1, f'{e}'        
        load_data_isinstance = load_data(DIR_MERGED_TO, FILE_MERGED)
    else:
        load_data_isinstance = EmptyOperator(task_id="skip_load")

    log_result_instance >> tasks_downloads >> files_is_downloaded >> run_transform_instance >> load_data_isinstance

bd_l3_s3_hw()    
