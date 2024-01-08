from datetime import datetime,  timezone, timedelta
import csv
import logging

logging.basicConfig(level=logging.INFO)
DATA_HEADER = ['location', 'date_time', 'temperature_celsius']


def transform_file(location:str, source_file:str, target_file:str)->None:
    # logging.info(f'{location=}, {source_file=}, {target_file=}')
    with open(source_file, 'r') as f:
        metadata_header = f.readline()[:-1].split(',')
        print(metadata_header)
        metadata_data = f.readline()[:-1].split(',')
        print(metadata_data)
        metadata = {el:metadata_data[i] for i, el in enumerate(metadata_header)}
        print(metadata['timezone'])
        f.readline() # skip blank line
        f.readline() # skip header line
        # data_header = [] #f.readline()[:-1].split(',')
        # print(data_header)
        with open(target_file, 'w', encoding='utf-8') as f_w_tz:
            csv_writer = csv.DictWriter(f_w_tz, DATA_HEADER)
            csv_writer.writeheader()
            for n, line in enumerate(f):
                row_dict = {}
                row = line[:-1].split(',')
    #             print(row)
    #             timestamp = datetime.strptime(row[0], "%Y-%m-%dT%H:%M").replace(tzinfo=pytz.timezone(metadata['timezone']))
                timestamp = datetime.strptime(row[0], "%Y-%m-%dT%H:%M").replace(tzinfo=timezone(timedelta(days=0, seconds=int(metadata['utc_offset_seconds']))))
                row_dict[DATA_HEADER[0]] = location
                row_dict[DATA_HEADER[1]] = datetime.strftime(timestamp, "%Y-%m-%dT%H:%M%z")
                row_dict[DATA_HEADER[2]] = row[1]
                csv_writer.writerow(row_dict)
    return
    

def merge_files(path_to_source_files, files_to_process, path_to_target_file, target_file):
    logging.info(f'merge_files. {path_to_source_files=}, {files_to_process=}, {path_to_target_file=}, {target_file=} ')
    with open(path_to_target_file + '/' + target_file, 'w', newline='') as tf:
        logging.info(f'merge_files. open{path_to_target_file + "/" + target_file}')
        csv_writer = csv.DictWriter(tf, DATA_HEADER)
        csv_writer.writeheader()
        for file in files_to_process:
            with open(path_to_source_files +'/' + file, 'r', newline='') as sf:
                csv_reader = csv.DictReader(sf)
                for row in csv_reader:
                    csv_writer.writerow(row)
    logging.info(f'{path_to_target_file}/{target_file} done.')
    return

def transform(path_to_source_files, source_files, path_to_target_file, target_file):
    logging.info(f'transform. {path_to_source_files=}, {source_files=}, {path_to_target_file=}, {target_file=} ')
    for loc in source_files:
        transform_file(loc, path_to_source_files+'/'+source_files[loc]['file'],
                        path_to_source_files+'/'+source_files[loc]['file']+'.tmp')
    files_list = [source_files[loc]['file']+'.tmp' for loc in source_files]
    merge_files(path_to_source_files, files_list, path_to_target_file, target_file)
    return

def main():
    print()
    FILES_NAME_FOR_DOWNLOAD = {
                           'chel':{'file': 'open-meteo-chel_2023-11-30_2023-12-30.csv',
                                              'coordinates':{'lat':10, 'long': 15}},
                           'rio':{'file': 'open-meteo-rio_2023-11-30_2023-12-30.csv',
                                              'coordinates':{'lat':10, 'long': 15}},
                           'berl':{'file': 'open-meteo-berl_2023-11-30_2023-12-30.csv',
                                              'coordinates':{'lat':10, 'long': 15}},
                            }

    transform('/home/gbss/course_de_bd/hw3/downloads_tmp',FILES_NAME_FOR_DOWNLOAD,\
               '/home/gbss/course_de_bd/hw3/merged', 'temp_merged.csv')

if __name__ == '__main__':
    main()
    