from flask import Flask, jsonify
from flaskext.mysql import MySQL
import requests
import re
import json
from datetime import datetime, timedelta
import time
import configparser

#собираем данные для работы из script.conf файла
conf = configparser.RawConfigParser()
conf.read('script.conf')
host = conf.get('db_autch', 'host')
user = conf.get('db_autch', 'user')
password = conf.get('db_autch', 'password')
db_name = conf.get('db_autch', 'db_name')

regex2 = r"[()',]"
regex = r"\d{8}-\d{6}"
rg = r"['\[\]]"

app = Flask(__name__)
mysql = MySQL()

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = conf.get('db_autch', 'user')
app.config['MYSQL_DATABASE_PASSWORD'] = conf.get('db_autch', 'password')
app.config['MYSQL_DATABASE_DB'] = conf.get('db_autch', 'db_name')
app.config['MYSQL_DATABASE_HOST'] = conf.get('db_autch', 'host')

mysql.init_app(app)

def split_data(datas):
    stt = datas.split(',')
    rpm = dict(zip([0, 1, 2, 3, 4, 5, 6, 7, 8], stt))
    return rpm


def to_timestamp(date_str):
    time_tuple = time.strptime(date_str, "%Y%m%d-%H%M%S")
    timestamp = time.mktime(time_tuple)
    return timestamp


def get_max_calldate_asterisk():
    cur = mysql.connect().cursor()
    sql = '''
        SELECT DATE_FORMAT(max(calldate),"%Y%m%d-%H%i%s") 
        AS niceDate FROM fn1crm.calls WHERE provider = 'asterisk'
        '''
    cur.execute(sql)
    r = cur.fetchall()
    cur.close()
    result = re.sub(regex2, '', str(r[0]))
    # print(result)
    return (result)

def get_max_calldate_megafon():
    cur = mysql.connect().cursor()
    sql = '''
        SELECT DATE_FORMAT(max(calldate),"%Y%m%d-%H%i%s") 
        AS niceDate FROM fn1crm.calls WHERE provider = 'megafon'
        '''
    cur.execute(sql)
    r = cur.fetchall()
    cur.close()
    result = re.sub(regex2, '', str(r[0]))
    # print(result)
    return (result)



def put_sql(sql_full):
    try:
        conn = mysql.connect()
        cur1 = conn.cursor()
        cur1.execute(sql_full)
        conn.commit()
        cur1.close()
        return ('successfully ;)')
    except Exception as e:
        raise e
        return ('error ;(')


def sql_to_megafon(data):
    sql = []
    for ps in data:
        sqll = 'INSERT INTO fn1crm.calls (provider,calldate,src,dst,context,duration,path,translation) VALUES ("%s", "%s","%s", "%s", "%s", "%s", "%s", "%s");'%('megafon', ps["5"], ps["4"], ps["2"], ps["1"], ps["7"], ps["8"], 'translation etc')
        sql.append(sqll)
    return sql


def sql_to_asterisk(data):
    sql = []
    for ps in data:
        sqll ='INSERT INTO fn1crm.calls (provider,calldate,src,dst,context,duration,path,translation) VALUES ("%s", "%s","%s", "%s", "%s", "%s", "%s", "%s");'%('asterisk', ps["dacalldate"], ps["src"], ps["dst"], ps["dcontext"], ps["duration"],ps["path"], ps["translation"])
        sql.append(sqll)
    return sql


def get_request_parsed(url):
    r = requests.get(url)
    parsed_string = json.loads(r.text)
    pars1 = parsed_string["list"]
    return pars1

def time_ms_to_utc(data):
    # переведем строкое значени стартового времени в тип datatime
    ds_dt = datetime.strptime(data, "%Y%m%d-%H%M%S")
    # отнимаем 3 часа UTC +3 по Москве
    ds_dt_utc = (ds_dt+timedelta(hours=-3, seconds=+1.0))
    m_date = datetime.strftime(ds_dt_utc, "%Y%m%dT%H%M%SZ")
    # print(type(m_date))
    return m_date




@app.route('/api/post')
def mget():
    date_start_a = get_max_calldate_asterisk()
    date_start_m = get_max_calldate_megafon()

    url_m = 'http://192.168.1.12/api/v1/list/megafon/start={}'.format(time_ms_to_utc(date_start_m))
    if not len(get_request_parsed(url_m))==0:
        sql_m = sql_to_megafon(get_request_parsed(url_m))
        # print(sql_m)
        sql_full_m = re.sub(rg, '', str(sql_m)).replace(';,', ';')
        put_sql(sql_full_m)
        ln_m = str(len(get_request_parsed(url_m)))
    else:
        ln_m = '0'
    url_a = 'http://192.168.1.12/api/v1/list/asterisk/start={}'.format(date_start_a)
    if not len(get_request_parsed(url_a)) == 0:
        sql_a = sql_to_asterisk(get_request_parsed(url_a))
        sql_full_a = re.sub(rg, '', str(sql_a)).replace(';,', ';')
        put_sql(sql_full_a)
        ln_a = str(len(get_request_parsed(url_a)))
    else:
        ln_a='0'

    # sql_full = str(sql_m) + str(sql_a)
    # sql_fullj = re.sub(rg, '', sql_full).replace(';,', ';')
    # put_sql(sql_fullj)
    # return str(sql_fullj)
    html_requests = '''
    <h2>Запрос выполнен успешно!</h2>
    <hr />
    <h3>Получено записей по Megafon: {0}</h3>
    <hr />
    <h3>Получено записей по Asterisk: {1}</h3>
    '''.format(ln_m, ln_a)
    return html_requests



@app.route('/')
def iget():
    date_start = get_max_calldate_asterisk()
    t = time_ms_to_utc(get_max_calldate_megafon())
    index = '''

    <div>
    <hr />
     <h2>NOTE:</h2> 
     <p>записать данные с asterisk и megafon в CRM</p>
     <p>/api/v1/post/start=date_start</p>
    <p>формат даты 20180117-000000 год месяц число - часы минуты секунды</p>
    <hr />
    {0}
    <hr />
    {1}
    </div>
    '''.format(date_start, t)

    return index


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)