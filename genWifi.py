# -*- coding: utf-8 -*-  
import random
import os
import pandas as pd
import multiprocessing
import argparse

BATCH_SIZE = 3000000            # batch size
PARALLEL = 8                    # 任务并行度

MIN_LON = 106.23                # 最小经度
MIN_LAT = 29.11                 # 最小纬度
MAX_LON = 106.99                # 最大经度
MAX_LAT = 29.99                 # 最大纬度

MAX_WIFI_HOT = 10000            # WIFI探针个数，默认10000
PR_ID_NEAR_WIFI = 0.2           # wifi探针附近ID摄像头的概率，从ID表里去重后得到总探头数
OFFSET_ID_NEAR_WIFI = 0.0001    # wifi探针附近ID摄像头经纬度最大偏移
MAX_ID_HOT_NO_WIFI = 200        # 附近没有wifi探针的ID摄像头的个数，默认200

DUPLI_RATE = 287/160 - 1        # 287亿数据对完全一致的数据去重后剩余160亿
MIN_CAPTURE_TIME = 1535731200   # 2018-09-01 00:00:00
MAX_CAPTURE_TIME = 1546271999   # 2018-12-31 23:59:59
MAC_RECORDS = 'wifiCNT.csv'     # wifiCNT格式为：每mac的记录数，出现的概率
'''
根据实际情况生成,wifiCNT.txt生成语句
select cnt,count(1) cntPR 
  from (select mac,count(1) cnt 
          from wifi 
         group by mac)
 group by cnt    
 order by cnt   
'''
PATH_WIFI_HOT = 'hotWifi.txt'   # wifi热点经纬度维度表
PATH_ID_HOT = 'hotID.txt'       # ID摄像头经纬度维度表

def parseArgs(args):
    parser = argparse.ArgumentParser()
    globalArgs = parser.add_argument_group('Global options')
    globalArgs.add_argument('--prefix', type=str, default='0') 
    return parser.parse_args(args)

def genMac(inPrefix):
    #为避免首字母数据倾斜，首位用随机数，第二段用Prefix来识别
    mac_bin_list = [random.randint(0x10,0xff),inPrefix]
    mac_hex_list = []
    for i in range(1,5):
        #为了避免判断位数小于2位需要左补零，从0X10开始
        i = random.randint(0x10,0xff)
        mac_bin_list.append(i)
    for j in mac_bin_list:
        mac_hex_list.append(hex(j))
    fakeMac = ":".join(mac_hex_list).replace("0x","")
    return fakeMac

def genHotPoint():
    
    fHotWifi = open(PATH_WIFI_HOT, 'w')
    fHotID = open(PATH_ID_HOT, 'w')

    for j in range(1,MAX_WIFI_HOT):
        wifiHotLon = MIN_LON + random.random()*(MAX_LON - MIN_LON)
        wifiHotLat = MIN_LAT + random.random()*(MAX_LAT - MIN_LAT)
        fHotWifi.write("{},{:.5f},{:.5f}".format(j,wifiHotLon,wifiHotLat)+'\n')
        #随机选取20%的wifiHot，在附近生成身份证卡口信息
        if random.random() < PR_ID_NEAR_WIFI:
            idHotLon = wifiHotLon + random.random() * OFFSET_ID_NEAR_WIFI
            idHotLat = wifiHotLat + random.random() * OFFSET_ID_NEAR_WIFI
            fHotID.write("{},{:.5f},{:.5f}".format(j,idHotLon,idHotLat)+'\n')

    #补足剩余的身份证卡口位置，这批地址附近可能没有wifi热点
    for j in range(MAX_WIFI_HOT,MAX_WIFI_HOT + MAX_ID_HOT_NO_WIFI):
        idHotLon = MIN_LON + random.random()*(MAX_LON - MIN_LON)
        idHotLat = MIN_LAT + random.random()*(MAX_LAT - MIN_LAT)
        fHotID.write("{},{:.5f},{:.5f}".format(j,idHotLon,idHotLat)+'\n')

    fHotWifi.close()
    fHotID.close()

def loadWifiCNT():

    totalRecords = 0
    listStart = []
    listEnd = []
    listRecords = []
    listFile = []
    sStart = 0

    fWifiCNT = open(MAC_RECORDS, 'r')
    for line in fWifiCNT:
        cnt = int(line.strip().split(',')[0])
        cntPR = int(line.strip().split(',')[1])
        totalRecords = totalRecords + cntPR
        listFile.append(cntPR)
        listRecords.append(cnt)
    fWifiCNT.close()

    for line in listFile:
        sEnd = sStart + line
        listStart.append(sStart/totalRecords)
        listEnd.append(sEnd/totalRecords)
        
        sStart = sEnd
    fWifiCNT.close()
    
    df = pd.DataFrame({'start': listStart,'end': listEnd,'record': listRecords})
    df.set_index(['start','end'])
    return df

def genWifi(inPrefix,inBatchSize=BATCH_SIZE):
    print("="*20)
    print("preFix:{}\tBatchSize:{}.\nStarting...".format(inPrefix,inBatchSize))
    print("="*20)
    fHisWifi = open('hisWifi_{}.txt'.format(inPrefix), 'w')
    fHisID = open('hisID_{}.txt'.format(inPrefix), 'w')
    
    dicHotWifi = {}
    fHotWifi = open('hotWifi.txt', 'r')
    for line in fHotWifi:
        listWifi = line.strip().split(',')
        dicHotWifi[int(listWifi[0])] = [listWifi[1],listWifi[2]]
    fHotWifi.close()
    
    dicHotID = {}
    fHotID = open('hotID.txt', 'r')
    for line in fHotID:
        listID = line.strip().split(',')
        dicHotID[int(listID[0])] = [listID[1],listID[2]]
    fHotID.close()

    #加载mac可能出现记录数的概率表
    df = loadWifiCNT()
    
    lenHotWifi = len(dicHotWifi)
    macRows = 0
    wifiRows = 0
    idRows = 0
    #start gen wifi 
    for i in range(0,inBatchSize):
        macRows = macRows + 1
        FakeMac = genMac(inPrefix)
        macRecordPR = random.random()
        # macRecordPR可以为0和1，所以要两边都闭合。 
        macRecords = df[(df['start'] <= macRecordPR ) & (df['end'] >= macRecordPR)].iloc[0,2]
        for j in range(macRecords):
            macRowTime = random.randint(MIN_CAPTURE_TIME,MAX_CAPTURE_TIME)
            macRowWifiHot =  random.randint(0,lenHotWifi)

            #判断是否存在该热点，增加容错
            if macRowWifiHot in dicHotWifi:
                macRowLonLat = dicHotWifi[macRowWifiHot]
            else:
                macRowLonLat = [0,0]

            macFileRow = '{},{},{},{},{}'.format(FakeMac,macRowTime,macRowLonLat[0],macRowLonLat[1],macRowWifiHot)+'\n'
            fHisWifi.write(macFileRow)
            wifiRows = wifiRows + 1
            #生成完全相同的重复数据
            if random.random() < DUPLI_RATE:
                fHisWifi.write(macFileRow)
                wifiRows = wifiRows + 1

            #如果该热点附近存在ID探头，10%的概率下同时生成ID数据,时间最大偏移300秒，ID探头与wifi探针经纬度最大偏移0.0001
            if macRowWifiHot in dicHotID and random.random() < 0.1:
                idRowLonLat = dicHotID[macRowWifiHot]
                idTime = random.randint(0,300)
                idFileRow = '{},{},{},{},{}'.format(FakeMac,macRowTime + idTime ,idRowLonLat[0],idRowLonLat[1],macRowWifiHot)+'\n'
                fHisID.write(idFileRow)
                idRows = idRows + 1
                
            # 0.5%概率下有ID信息但没有热点探针的数据
            if random.random() < 0.005:
                idRowHot = random.randint(MAX_WIFI_HOT,MAX_WIFI_HOT + MAX_ID_HOT_NO_WIFI) 
                #判断是否存在该热点，增加容错
                if idRowHot in dicHotWifi:
                    idRowLonLat = dicHotID[idRowHot]
                else:
                    idRowLonLat = [0,0]
                idTime = random.randint(MIN_CAPTURE_TIME,MAX_CAPTURE_TIME)
                idFileRow = '{},{},{},{},{}'.format(FakeMac,idTime,idRowLonLat[0],idRowLonLat[1],idRowHot)+'\n'
                fHisID.write(idFileRow)
                idRows = idRows + 1
            
        if macRows % 10 == 0:
            print('{} MACs done.\twifi:{}\tID:{}\tpreFix:{}.'.format(macRows,wifiRows,idRows,inPrefix))

    print('{} MACs done.\twifi:{}\tID:{}\tpreFix:{}.'.format(macRows,wifiRows,idRows,inPrefix))
    fHisWifi.close()
    fHisID.close()

if __name__ == "__main__":
    args = None
    args = parseArgs(args)
    
    #探针数据如果已经生成，无需重新生成
    if not os.path.exists(PATH_WIFI_HOT):
        genHotPoint()

    pool = multiprocessing.Pool(processes = PARALLEL)
    listPrefix = []
    for i in range(0,PARALLEL):
        listPrefix.append(args.prefix + i)
    pool.map(genWifi, listPrefix) 
    pool.close()
    pool.join()