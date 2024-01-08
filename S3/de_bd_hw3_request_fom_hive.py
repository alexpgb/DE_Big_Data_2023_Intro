"""
Модуль для работы с Hive и hdfs
Выполняется в отдельном окружении python
"""
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TProtocol
from thrift.transport.TTransport import TBufferedTransport
from thrift_sasl import TSaslClientTransport
# from pyhive_service import ThriftHive ## не нашел как установить модуль pyhive_service
from pyhive import hive
# from pyarrow import fs
import subprocess
import logging

logging.basicConfig(level=logging.INFO)


def load_data_to_hive(file_to_load_full_name):
    conn = hive.connect(host='localhost', port=10000, username='admin')
    cursor=conn.cursor()
    cursor.execute("LOAD DATA INPATH %s OVERWRITE INTO TABLE temperature", (file_to_load_full_name,))
    cursor.close()
    conn.close()
    return


def run_cmd(args_list):
        """
        run linux commands
        отсюда:
        https://community.cloudera.com/t5/Community-Articles/Interacting-with-Hadoop-HDFS-using-Python-codes/ta-p/245163
        """
        # import subprocess
        logging.info('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err 


def load_file_from_container_to_hdfs_by_subprocess(container_name, path_in_container, file_name_in_container,
                                                    path_in_hdfs, file_name_in_hdfs):
    print()
    # hdfs dfs -put /tmp/temp_with_tz.csv /tmp/temp_with_tz_.csv
    args = ['docker', 'exec', container_name, 'hdfs', 'dfs', '-put', f'{path_in_container}{file_name_in_container}',
             f'{path_in_hdfs}{file_name_in_hdfs}']
    (ret, out, err) = run_cmd(args)
    logging.info(f'{ret=}, {out=}, {err=}')


def load_file_from_host_to_container_by_subprocess(container_name, path_in_host, file_name_in_host, 
                                                   path_in_container, file_name_in_container):
    # docker cp /home/gbss/course_de_bd/docker/workspace/temp_with_tz.csv namenode-de:/tmp/temp_with_tz_.csv
    print()
    args = ['docker', 'cp', f'{path_in_host}{file_name_in_host}', 
             f'{container_name}:{path_in_container}{file_name_in_container}']
    (ret, out, err) = run_cmd(args)
    logging.info(f'{ret=}, {out=}, {err=}')


def get_file_list_from_hdfs_by_container_and_subprocess(container_name, path_in_hdfs, file_name_mask=''):
    """
    Если файлы не найдены: ret=1 out = b'' len(out) == 0, err Содержит строку "No such file or directory"
    """
    # docker exec namenode-de hdfs dfs -ls /tmp/*
    print()
    args = ['docker', 'exec', container_name, 'hdfs', 'dfs', '-ls', f'{path_in_hdfs}{file_name_mask}']
    (ret, out, err) = run_cmd(args)    
    logging.info(f'{ret=}, {out=}, {err=}')


def main():
    print()
    load_file_from_host_to_container_by_subprocess('namenode-de', '/home/gbss/course_de_bd/hw3/merged/', 'temp_merged.csv',
                                                    '/tmp/', 'temp_merged.csv')

    load_file_from_container_to_hdfs_by_subprocess('namenode-de', '/tmp/', 'temp_merged.csv',
                                                    '/tmp/', 'temp_merged.csv')

    # get_file_list_from_hdfs_by_container_and_subprocess('namenode-de', '/tmp/', 'open-meteo-55.15N61.38E223m_temp.cs')

    # loc = get_locations_codes()
    # logging.info(loc)
    # coordinates = get_locations_coordinates(loc)
    # logging.info(f'{coordinates=}')

    load_data_to_hive('/tmp/'+'temp_merged.csv')

if __name__ == '__main__':
    main()
