import re
import os
import io
import sys
import json
import time
import logging
import psycopg2 as pg
from datetime import datetime as dt
import requests
import shutil

class ym:
    
    __slots__ = 'token', 'counter' , 'start_date' , 'end_date' , 'fields' , 'src' , 'request_id', '_info_logger', '_fail_logger', '_con', '_cur', '_path'
        
    def __init__(s, token, counter):
        
        '''
            *******
            ! DOC !
            *******
            TOKEN & COUNTER обязательны.
        '''
        
        s.token = token
        s.counter = counter

        s._info_logger, s._fail_logger = s.get_loggers()

    def get_connection(s):

        try:
            con = pg.connect(host='192.168.166.13', port = 5432, dbname='pole_pg', user = 'master', password='e8k.z85Mn4jGz')
            cur = con.cursor()

            s._info_logger.info('Connection successful OK')

        except Exception:

            s._fail_logger.error(f'Failed to set con {sys.exc_info()} FAIL')

        return con, cur 

    def close_connection(s, con, cur):

        try:
            con.commit()
            cur.close()
            con.close()

            s._info_logger.info('Connection closed OK')

        except Exception:
            s._fail_logger.error(f'Failed to close connection {sys.exc_info()} FAIL')


    def put_table(s, _file = None, _table= None, _truncate= False):

        if _truncate:
            s._dst_cursor.execute(f'truncate table {_table}')
            
        try:

            s._dst_cursor.copy_from(_file, sep = '^', table = str(_table))
            
            s._buffer.seek(0)
            s._buffer.truncate()
            
        except Exception:
            
            s._error_logger.error(f'FAIL to insert. Crash info: {sys.exc_info()}')

    def get_loggers(s):

        _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - func_name:  %(funcName)s - %(message)s')
        _handler = logging.FileHandler('_ym_logger.txt')

        info_logger = logging.getLogger('INFO')
        fail_logger = logging.getLogger('FAIL')

        info_logger.setLevel(logging.INFO)
        fail_logger.setLevel(logging.ERROR)

        _handler.setFormatter(_formatter)

        info_logger.addHandler(_handler)
        fail_logger.addHandler(_handler)

        return info_logger, fail_logger
               
    def get_status(s, request_id = None):

        '''
            *******
            ! DOC !
            *******
            Проверка существующих и обработанных запросов и их статусов
            Для проверки нужно передать номер счетчика (COUNTER) и токен (TOKEN)
            Пустой список - отсутсвуют запросы
        '''
            
        s.request_id = request_id 
            
        _url = f'https://api-metrika.yandex.net/management/v1/counter/{s.counter}/logrequests?'
        _header = {'Authorization': f'OAuth {s.token}'}

        res = requests.get(url = _url, headers = _header)

        if res.status_code != 200:
            s._fail_logger.error(f'Failed to get status BAD REQUEST {res.status_code}')

        else:
            
            if s.request_id is None:
            
                return {x['request_id']:x['status'] for x in json.loads(res.text)['requests']}
            
            else:
                
                return {x['request_id']:x['status'] for x in json.loads(res.text)['requests'] if x['request_id'] == s.request_id}
    
    def check_request(s, start_date = None, end_date = None, 
                      fields = 'all', src = 'v'):

        '''
            *******
            ! DOC !
            *******
            Проверка возможности выполнения запроса:
            - Требуется ввести даты (start_date | end_date) в формате 'YYYY-MM-DD' -> string
            - Также выбрать атрибут fields: либо через запятую перечислить поля либо пусто 
                    (тогда будут переданы все поля соответсвующего типа из файла _HITS_ | _VISITS_).
            При возврате значения True считается что переданный запрос возможен для передачи в API.
        '''
        
        s.start_date = start_date
        s.end_date = end_date
        s.fields = fields
        s.src = src
        
        if s.counter is None or s.token is None:
            s._fail_logger.error(f'Bad token or counter FAIL')

        elif s.start_date is None or s.end_date is None:
            s._fail_logger.error(f'Bad dates FAIL')
            
        else:

            if (s.fields.lower() == 'all' or s.fields is None) and s.src.startswith('v'):
                
                _file = [re.search(r'.*[Vv][Ii][Ss][Ii][Tt][Ss].*', x)[0] for x in os.listdir() if bool(re.search(r'.*[Vv][Ii][Ss][Ii][Tt][Ss].*', x))]
                
                with open(_file[0], 'r') as f:
                    _fields = f.read().replace('\n','').replace('\t','').replace(' ','')
                    
                _src = 'visits'

            elif (s.fields.lower() == 'all' or fields is None) and s.src.startswith('h'):
                
                _file = [re.search(r'.*[Hh][Ii][Tt][Ss].*', x)[0] for x in os.listdir() if bool(re.search(r'.*[Hh][Ii][Tt][Ss].*', x))]
                
                with open(_file[0], 'r') as f:
                    _fields = f.read().replace('\n','').replace('\t','').replace(' ','')
                    
                _src = 'hits'
            
            else:
                
                _file = [re.search(r'.*[Vv][Ii][Ss][Ii][Tt][Ss].*', x)[0] for x in os.listdir() if bool(re.search(r'.*[Vv][Ii][Ss][Ii][Tt][Ss].*', x))]
                
                with open(_file[0], 'r') as f:
                    _fields = f.read().replace('\n','').replace('\t','').replace(' ','')
                
                _src = 'visits'

            _url = f'''https://api-metrika.yandex.net/management/v1/counter/{s.counter}/logrequests/evaluate?date1={s.start_date}&date2={s.end_date}&fields={_fields}&source={_src}'''

            _header = {'Authorization': f'OAuth {s.token}'}

            res = requests.get(url = _url, headers = _header)

            try:
                json.loads(res.text)
                return bool(json.loads(res.text)['log_request_evaluation'])

            except:
                return False

    def make_request(s, start_date = None, end_date = None, 
                      fields = 'all', src = 'v'):
    
        '''
            *******
            ! DOC !
            *******
            Создание запроса.
            Запрос формируется методом POST.
            Перед выполнение запроса производится проверка корректности через ASSERT
        '''        
        
        s.start_date = start_date
        s.end_date = end_date
        s.fields = fields
        s.src = src
        
        if s.counter is None or s.token is None:
            s._fail_logger.error(f'Bad token or counter FAIL')

        elif s.start_date is None or s.end_date is None:
            s._fail_logger.error(f'Bad dates FAIL')
            
        else:

            if (s.fields.lower() == 'all' or s.fields is None) and s.src.startswith('v'):
                
                _file = [re.search(r'.*[Vv][Ii][Ss][Ii][Tt][Ss].*', x)[0] for x in os.listdir() if bool(re.search(r'.*[Vv][Ii][Ss][Ii][Tt][Ss].*', x))]
                
                with open(_file[0], 'r') as f:
                    _fields = f.read().replace('\n','').replace('\t','').replace(' ','')
                    
                _src = 'visits'

            elif (s.fields.lower() == 'all' or fields is None) and s.src.startswith('h'):
                
                _file = [re.search(r'.*[Hh][Ii][Tt][Ss].*', x)[0] for x in os.listdir() if bool(re.search(r'.*[Hh][Ii][Tt][Ss].*', x))]
                
                with open(_file[0], 'r') as f:
                    _fields = f.read().replace('\n','').replace('\t','').replace(' ','')
                    
                _src = 'hits'
            
            else:
                
                _file = [re.search(r'.*[Vv][Ii][Ss][Ii][Tt][Ss].*', x)[0] for x in os.listdir() if bool(re.search(r'.*[Vv][Ii][Ss][Ii][Tt][Ss].*', x))]
             
                with open(_file[0], 'r') as f:
                    _fields = f.read().replace('\n','').replace('\t','').replace(' ','')
                
                _src = 'visits'
                
            #assert s.check_request(start_date=start_date, end_date=end_date, src=src, fields=fields), 'Bad request parameters!!!'

            _url = f'''https://api-metrika.yandex.net/management/v1/counter/{s.counter}/logrequests?date1={start_date}&date2={end_date}&fields={_fields}&source={_src}'''

            _header = {'Authorization': f'OAuth {s.token}'}

            res = requests.post(url = _url, headers = _header)
            
            try:
                return {json.loads(res.text)['log_request']['request_id']:json.loads(res.text)['log_request']['status']}
                
            except Exception:
                return json.loads(res.text)
            
    def get_stats(s, request_id = None):

        '''
            *******
            ! DOC !
            *******
            Проверка атрибутов готового запроса
        '''

        s.request_id = request_id
        
        assert s.request_id != None, 'request_id is required'
        
        _url = f'''https://api-metrika.yandex.net/management/v1/counter/{s.counter}/logrequest/{s.request_id}?'''

        _header = {'Authorization': f'OAuth {s.token}'}

        res = requests.get(url = _url, headers = _header)

        _request_id = json.loads(res.text)['log_request']['request_id']
        _source = json.loads(res.text)['log_request']['source']
        _status = json.loads(res.text)['log_request']['status']
        _size = int(json.loads(res.text)['log_request']['size'])/1024/1024
        _parts = len(json.loads(res.text)['log_request']['parts'])
        _parts_info = {x['part_number']:str(round(x['size']/1024/1024,2)) + ' MB' for x in json.loads(res.text)['log_request']['parts']}

        return {'req_id':_request_id , 'src':_source , 'status':_status , 'size':str(round(_size,2)) + ' MB' , 'parts':_parts , 'size_info':_parts_info}

    def get_logs(s, request_id = None, split = True):
        
        '''
            *******
            ! DOC !
            *******
            Получение логов в файл, запись в таблицу
        '''
    
        s.request_id = request_id
        _header = {'Authorization': f'OAuth {s.token}'}
        
        _now = dt.strftime(dt.now(), '%Y-%m-%d at %H-%M-%S')
        
        _data_loader = io.StringIO()
        
        _file_name = f'logs {s.request_id} created {_now}.txt'
        
        _size = len(s.get_stats(request_id=request_id)['size_info'])

        s._path = os.getcwd() + '/' + _now
    
        if s.request_id is None:
            s._fail_logger.error(f'Bad request id FAIL')

        else:
            
            try:
                os.mkdir(f'{_now}')
                s._info_logger.info(f'Folder {_now} created OK')
            except Exception:
                s._fail_logger.error(f'Failed to create folder {_now}')
            
            os.chdir(os.getcwd() + '/' + _now)
            
            if split:

                s._info_logger.info(f'Accepted multiple file download')
                
                for part in range(_size):

                    try:

                        s._info_logger.info(f'Starting part {part} processing, file {_file_name}')
                    
                        with open(f'Part {part} - ' + _file_name, 'w', encoding='UTF8') as fb:
                    
                            _url = f'https://api-metrika.yandex.net/management/v1/counter/{s.counter}/logrequest/{s.request_id}/part/{part}/download'

                            s._info_logger.info('Making request')

                            _res = requests.get(url = _url, headers = _header)
                            
                            _split_text = _res.text.split('\n')
                            
                            for row in _split_text[1:]:
                                    
                                _data_loader.write(row.replace('\t','^').replace('\n', '\t') + '\n')
                                
                            _data_loader.seek(_data_loader.tell() - 1)
                            _data_loader.truncate()
                                
                            _data_loader.seek(0)
                            fb.write(_data_loader.getvalue())

                    except Exception:
                        s._fail_logger.error(f'Failed to process part {part} with error {sys.exc_info()}')
            
            else:

                s._info_logger.info(f'Accepted single file download')

                try:

                    s._info_logger.info(f'Starting download, opening file {_file_name}')

                    with open(_file_name, 'w', encoding='UTF8') as fb:

                        for i in range(_size):

                            _url = f'https://api-metrika.yandex.net/management/v1/counter/{s.counter}/logrequest/{s.request_id}/part/{i}/download'

                            _res = requests.get(url = _url, headers = _header)

                            _split_text = _res.text.split('\n')

                            for row in _split_text[1:]:
                                _data_loader.write(row.replace('\t','^').replace('\n', '\t') + '\n')

                            _data_loader.seek(_data_loader.tell() - 1)
                            _data_loader.truncate()

                        _data_loader.seek(0)
                        fb.write(_data_loader.getvalue())

                except Exception:
                    s._fail_logger.error(f'Failed to process {sys.exc_info()}')
        return _now

    def update(s, _sdate = None, _edate = None, _fileds = 'all', _src = 'v'):

        _request = s.make_request(start_date = _sdate, end_date = _edate, 
                      fields = _fileds, src = _src)

        _id = list(_request.keys())[0]
        _status = list(_request.values())[0]
        _process = True 
        _counter = 1
        _buffer = io.StringIO()
        _table = '_ym_visits' if _src == 'v' else '_ym_hits'

        s._info_logger.info(f'Starting update, request {_request} table {_table}, dates span {_sdate} to {_edate}')

        while _process:

            if _status == 'processed':

                _process = False

                # GET LOGS block

                s._info_logger.info('Status changed. Request is PROCESSED. Starting LOGS downloads')

                try:

                    s.get_logs(_id)

                except Exception:
                    s._fail_logger.error('Failed to get LOGS - FAIL') 

                os.chdir(s._path)

                # TABLE UPLOAD-REFRESH block

                s._info_logger.info('Starting table update')

                try:

                    s._con, s._cur = s.get_connection()

                    try:

                        for file in os.listdir():

                            s._info_logger.info('Starting batch upload')

                            with open(file, 'r', encoding='utf-8') as f:

                                _buffer.write(f.read())
                                _buffer.seek(0)

                            s._cur.copy_from(_buffer, sep = '^', table = str(_table))

                            s._info_logger.info('Batch upload successful - OK')

                        s.clean(_id)
                        s.clean_path()
                        logging.shutdown()

                    except Exception:
                        s._fail_logger.error(f'Failed to upload {sys.exc_info()}')
                        s.clean(_id)
                        s.clean_path()

                    s.close_connection(s._con, s._cur)

                except Exception:
                    s._fail_logger.error(f'Connection to DB failed - FAIL {sys.exc_info()}')

            else:
                s._info_logger.info(f'Retry {_counter} for {_id}, current status {_status}')

                _counter += 1

                time.sleep(30)

                _status = list(s.get_status(_id).values())[0]
                    
    def clean(s, request):
        
        _clean_url = f'https://api-metrika.yandex.net/management/v1/counter/{s.counter}/logrequest/{request}/clean?'
        
        _header = {'Authorization': f'OAuth {s.token}'}

        try:
        
            requests.post(_clean_url, headers = _header)

            s._info_logger.info(f'Request {request} deleted OK')

        except Exception:

            s._fail_logger.error(f'Failed to delete request {request}')

    def clean_path(s):

        try:

            os.chdir('..')
            shutil.rmtree(s._path)
            s._info_logger.info('Catalog removed - OK')
        except Exception:
            s._fail_logger.error(f'Failed to remove catalog {sys.exc_info()} - FAIL')




                

        





