# -*- coding: utf-8 -*-  
import psycopg2
import os
import argparse
import multiprocessing
from datetime import datetime 

OFFSET_LEFT_TIME =  0
OFFSET_RIGHT_TIME =  300
OFFSET_LEFT_LON =  0
OFFSET_RIGHT_LON =  0.0001
OFFSET_LEFT_LAT =  0
OFFSET_RIGHT_LAT =  0.0001

PARALLEL = 8
CSV_PATH= '/data/csvFile/'

def parseArgs(args):
    parser = argparse.ArgumentParser()
    globalArgs = parser.add_argument_group('Global options')
    globalArgs.add_argument('--prefix', type=str, default='0,1') 
    return parser.parse_args(args)

def matchWifi(inPrefix):

    fileName = CSV_PATH + '/hisID_' + inPrefix +'.txt'

    if not os.path.exists(fileName+'.sort'):
        os.system('sort {} > {}.sort'.format(fileName,fileName))

    idFile = open(fileName+'.sort', 'r')
    idRows = 0
    
    conn = psycopg2.connect(database="wifi", user="gpadmin", password="dev", host="master", port="5432")
    cur = conn.cursor()

    taskStartTime = datetime.now() 
    for line in idFile:
        idRows = idRows + 1
        if idRows % 100 == 0:

            print('{} MACs done. cost:{}s'.format(idRows,(datetime.now() - taskStartTime).seconds))
            break

        mac = line.split(',')[0]
        ct = int(line.split(',')[1])
        lon = float(line.split(',')[2])
        lat = float(line.split(',')[3])

        if lon > 0:
            sqlText =   "select mac from wifi_clean where capture_time >= {} and capture_time <= {} and lon >= \'{}\' "\
                        "and lon <= \'{}\' and lat >= \'{}\' and lat <= \'{}\'".format(
                        ct - OFFSET_LEFT_TIME,
                        ct + OFFSET_RIGHT_TIME, 
                        lon - OFFSET_LEFT_LON ,
                        lon + OFFSET_RIGHT_LON ,
                        lat - OFFSET_LEFT_LAT,
                        lat + OFFSET_RIGHT_LAT)
            cur.execute(sqlText);
            # print(sqlText)
            rows = cur.fetchall()        # all rows in table
            for i in rows:
                print(i)

    # conn.commit()
    # cur.close()
    # conn.close()

if __name__ == "__main__":
    args = None
    args = parseArgs(args)
    
    pool = multiprocessing.Pool(processes = PARALLEL)
    listPrefix = args.prefix.split(',')
    pool.map(matchWifi, listPrefix) 
    pool.close()
    pool.join()