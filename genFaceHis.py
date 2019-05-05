# -*- coding: utf-8 -*-  
import random
import os
# import pandas as pd
import time
import datetime
import multiprocessing
import argparse


# CREATE TABLE cw_record_camera_capture (
#   CAPTURE_ID                    varchar(64)             NOT NULL                COMMENT '抓拍ID主键',
#   EG_CAPTURE_ID                 varchar(64)             DEFAULT NULL            COMMENT '引擎抓拍ID',
#   CAPTURE_SEQ                   bigint(20)              NOT NULL                COMMENT '全局唯一序号',
#   CAPTURE_TIME                  bigint(20)              NOT NULL                COMMENT '抓拍时间13位时间戳',
#   DEVICE_ID                     varchar(64)             NOT NULL                COMMENT '像机ID设备ID',
#   DEVICE_NAME                   varchar(64)             DEFAULT NULL            COMMENT '设备名称',
#   DEVICE_TYPE                   smallint(6)             DEFAULT NULL            COMMENT '1、像机设备 2、人证设备 3、离线视频 4、终端设备',
#   SUB_TYPE                      smallint(3)             DEFAULT NULL            COMMENT '附属信息类型 1、全景照  2、人证  3、终端识别',
#   DEVICE_ADDRESS                varchar(128)            DEFAULT NULL            COMMENT '设备地址',
#   LNG                           varchar(12)             DEFAULT NULL            COMMENT '经度',
#   LAT                           varchar(12)             DEFAULT NULL            COMMENT '维度',
#   DEV_EXT_FIELD1                varchar(128)            DEFAULT NULL            COMMENT '设备扩展字段1',
#   DEV_EXT_FIELD2                varchar(1024)           DEFAULT NULL            COMMENT '设备扩展字段2',
#   DEV_EXT_FIELD3                text                    DEFAULT NULL            COMMENT '设备扩展字段3',
#   PANORAMA_ID                   varchar(64)             DEFAULT NULL            COMMENT '全景照ID',
#   CAPTURE_GROUP                 varchar(64)             DEFAULT NULL            COMMENT '抓拍照组:预留',
#   CAPTURE_URL                   varchar(128)            DEFAULT NULL            COMMENT '抓拍照URL',
#   QUALITY_SCORE                 decimal(10,4)           DEFAULT NULL            COMMENT '质量分',
#   AGE                           smallint(6)             DEFAULT NULL            COMMENT '年龄',
#   AGE_GROUP                     smallint(6)             DEFAULT NULL            COMMENT '1 小孩     2 中年人   4 老人',
#   GENDER                        smallint(6)             DEFAULT NULL            COMMENT '性别 0::未知、1男性、2：、女性、9:未知说明的性别',
#   RACE                          smallint(6)             DEFAULT NULL            COMMENT '人种：1:黄种人 2：黑种人 4白种人 8新疆人',
#   FACE_TOTAL_SCORE              decimal(10,4)           DEFAULT NULL            COMMENT '人脸质量总分',
#   LIGHT_SCORE                   decimal(10,4)           DEFAULT NULL            COMMENT '光照分',
#   MASK_SCORE                    decimal(10,4)           DEFAULT NULL            COMMENT '口罩得分',
#   MASK_STATE                    smallint(6)             DEFAULT '0'             COMMENT '戴口罩状态，0：否 1：是',
#   CLARITY_SCORE                 decimal(10,4)           DEFAULT NULL            COMMENT '清晰度分',
#   GLASSES_SCORE                 decimal(10,4)           DEFAULT NULL            COMMENT '是否戴眼镜分数',
#   GLASSES_STATE                 smallint(6)             DEFAULT '0'             COMMENT '是否戴眼镜 0：否 1:是',
#   MOUTH_SCORE                   decimal(10,4)           DEFAULT NULL            COMMENT '闭嘴分数',
#   FACE_CLARITY_SCORE            decimal(10,4)           DEFAULT NULL            COMMENT '人脸越清晰分数',
#   SUN_GLASSES_STATE             smallint(6)             DEFAULT '0'             COMMENT '是否戴墨镜 0：否 1：是',
#   SUN_GLASSES_SCORE             decimal(10,4)           DEFAULT NULL            COMMENT '戴墨镜分数',
#   NATION                        smallint(6)             DEFAULT NULL            COMMENT '国籍 1 中国人   2 外国人',
#   PITCH                         decimal(10,4)           DEFAULT NULL            COMMENT '人脸旋转角PITCH',
#   YAW                           decimal(10,4)           DEFAULT NULL            COMMENT '人脸旋转角YAW',
#   ROLL                          decimal(10,4)           DEFAULT NULL            COMMENT '人脸旋转角 ROLL',
#   X                             decimal(10,4)           DEFAULT NULL            COMMENT '人脸框     X',
#   Y                             decimal(10,4)           DEFAULT NULL            COMMENT '人脸框     Y',
#   WIDTH                         decimal(10,4)           DEFAULT NULL            COMMENT '人脸框     WIDTH',
#   HEIGHT                        decimal(10,4)           DEFAULT NULL            COMMENT '人脸框     HEIGHT',
#   PRINT_FACE_SCORE              decimal(10,4)           DEFAULT NULL            COMMENT '是否黑白打印纸张，越大表示越可能是真人',
#   LEFT_BLINK_SCORE              decimal(10, 4)          DEFAULT NULL            COMMENT '左眼睁眼分',
#   RIGHT_BLINK_SCORE             decimal(10, 4)          DEFAULT NULL            COMMENT '右眼睁眼分',
#   LEFT_BLINK_STATE              smallint(6)             DEFAULT '0'             COMMENT '是否闭左眼 0：否 1：是',
#   RIGHT_BLINK_STATE             smallint(6)             DEFAULT '0'             COMMENT '是否闭右眼 0：否 1：是',
#   FROM_SYSTEM                   varchar(64)             DEFAULT NULL            COMMENT '数据来源,系统编号',
#   IS_LINKAGE                    smallint(6)             DEFAULT '0'             COMMENT '是否级联数据,1:是 0:否',
#   FEATURE_STATUS                smallint(6)             DEFAULT '0'             COMMENT '特征提取(0 成功 1 失败)',
#   REMARK                        varchar(128)            DEFAULT NULL            COMMENT '备注',
#   CREATE_TIME                   bigint(20)              NOT NULL                COMMENT '保存时间',
#   CAPTURE_RECEIVE_TIME          bigint(20)              DEFAULT NULL            COMMENT '抓拍接收时间13位时间戳',
#   ENGINE_CALL_TIME              bigint(20)              DEFAULT NULL            COMMENT '引擎调用耗时',
#   YEAR                          smallint(6)             NOT NULL                COMMENT '年YYYY',
#   MONTH                         int(11)                 NOT NULL                COMMENT '月YYYYMM',
#   DAY                           int(11)                 NOT NULL                COMMENT '日YYYYMMDD',
#   MON                           int(11)                 DEFAULT NULL,
#   CAPTURE_EXT_FIELD1            varchar(128)            DEFAULT NULL            COMMENT '扩展字段1',
#   CAPTURE_EXT_FIELD2            varchar(256)            DEFAULT NULL            COMMENT '扩展字段2',
#   CAPTURE_EXT_FIELD3            varchar(255)            DEFAULT NULL            COMMENT '扩展字段3',
#   CAPTURE_EXT_FIELD4            varchar(255)            DEFAULT NULL            COMMENT '扩展字段4',
#   CAPTURE_EXT_FIELD5            varchar(255)            DEFAULT NULL            COMMENT '扩展字段5',
#   PRIMARY KEY (CAPTURE_ID,CAPTURE_TIME,DEVICE_ID,CAPTURE_SEQ),
#   KEY idx_deviceid_capturetime (CAPTURE_TIME,DEVICE_ID) USING BTREE,
#   KEY idx_capturetime_day (CAPTURE_TIME,DAY) USING BTREE
# ) DEFAULT CHARSET=utf8 COMMENT='像机抓拍记录流水表';

BATCH_SIZE = 10000              # batch size
PARALLEL = 2                    # 任务并行度

MIN_LON = 106.23                # 最小经度
MIN_LAT = 29.11                 # 最小纬度
MAX_LON = 131.99                # 最大经度
MAX_LAT = 40.99                 # 最大纬度

MAX_WIFI_HOT = 1000000          # WIFI探针个数，默认10000
MIN_CAPTURE_TIME = 1535731200   # 2018-09-01 00:00:00
MAX_CAPTURE_TIME = 1546271999   # 2018-12-31 23:59:59
DAY_TIME         = 86400

def parseArgs(args):
    parser = argparse.ArgumentParser()
    globalArgs = parser.add_argument_group('Global options')
    globalArgs.add_argument('--prefix', type=int, default=1) 
    globalArgs.add_argument('--parallel', type=int, default=PARALLEL) 
    globalArgs.add_argument('--batch_size', type=int, default=BATCH_SIZE) 
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

def genWifi(taskInputs):
    print("="*20)
    taskID = taskInputs['taskID']
    preFix = taskInputs['preFix']
    # START_CAPTIME_TIME = DAY_TIME * (preFix + taskID -1) + MIN_CAPTURE_TIME
    # END_CAPTURE_TIME =  DAY_TIME * (preFix + taskID) + MIN_CAPTURE_TIME - 1

    START_CAPTIME_TIME = DAY_TIME * (preFix - 1 ) + MIN_CAPTURE_TIME
    END_CAPTURE_TIME =  DAY_TIME * (preFix) + MIN_CAPTURE_TIME - 1

    print("preFix:{}\ttaskID:{}\tstartTime:{}\tendTime:{}\t.\nStarting...".format(preFix,taskID,START_CAPTIME_TIME,END_CAPTURE_TIME))
    print("="*20)
    START_TIME_TEXT = datetime.datetime.fromtimestamp(START_CAPTIME_TIME).strftime("%Y%m%d") 
    fHisWifi = open('/data/flatfile/hisFace_{}_{}.txt'.format(START_TIME_TEXT,taskID), 'w')
    

    LIST_AGE_GROUP = ['1','2','4']     # COMMENT '1 小孩     2 中年人   4 老人',
    LIST_GENDER = ['0','1','2','9']    # '性别 0::未知、1男性、2：、女性、9:未知说明的性别'

    t0 = datetime.datetime.now()

    for i in range(0,BATCH_SIZE):
        idFileRow = []

        CAPTURE_TIME = random.randint(START_CAPTIME_TIME,END_CAPTURE_TIME)
        CAPTURE_TIME_TEXT = datetime.datetime.fromtimestamp(CAPTURE_TIME).strftime("%Y%m%d%H%M%S") 
        CAPTURE_TIME_TEXT_Y = datetime.datetime.fromtimestamp(CAPTURE_TIME).strftime("%Y") 
        CAPTURE_TIME_TEXT_YM = datetime.datetime.fromtimestamp(CAPTURE_TIME).strftime("%Y%m") 
        CAPTURE_TIME_TEXT_YMD = datetime.datetime.fromtimestamp(CAPTURE_TIME).strftime("%Y%m%d") 

        CAM_ID = random.randint(0,MAX_WIFI_HOT-1)
        idFileRow.append(CAM_LIST[CAM_ID]['MAC']+ str(CAPTURE_TIME))                            #   CAPTURE_ID                    varchar(64)             NOT NULL                COMMENT '抓拍ID主键',
        idFileRow.append(CAP_LIST[CAM_ID])                                                      #   EG_CAPTURE_ID                 varchar(64)             DEFAULT NULL            COMMENT '引擎抓拍ID',
        idFileRow.append(str(CAPTURE_TIME) +  str(preFix)  + str(taskID) + str(i))              #   CAPTURE_SEQ                   bigint(20)              NOT NULL                COMMENT '全局唯一序号',
        idFileRow.append(CAPTURE_TIME_TEXT)                                                     #   CAPTURE_TIME                  bigint(20)              NOT NULL                COMMENT '抓拍时间13位时间戳',
        idFileRow.append(CAM_LIST[CAM_ID]['MAC'])                                               #   DEVICE_ID                     varchar(64)             NOT NULL                COMMENT '像机ID设备ID',
        idFileRow.append(CAM_LIST[CAM_ID]['MAC'])                                               #   DEVICE_NAME                   varchar(64)             DEFAULT NULL            COMMENT '设备名称',
        idFileRow.append(CAM_LIST[CAM_ID]['LON'])                                               #   LNG                           varchar(12)             DEFAULT NULL            COMMENT '经度',
        idFileRow.append(CAM_LIST[CAM_ID]['LAT'])                                               #   LAT                           varchar(12)             DEFAULT NULL            COMMENT '维度',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   QUALITY_SCORE                 decimal(10,4)           DEFAULT NULL            COMMENT '质量分',  
        idFileRow.append("{}".format(random.randint(10,80)))                                    #   AGE                           smallint(6)             DEFAULT NULL            COMMENT '年龄',
        idFileRow.append("{}".format(LIST_AGE_GROUP[random.randint(0,2)]))                      #   AGE_GROUP                     smallint(6)             DEFAULT NULL            COMMENT '1 小孩     2 中年人   4 老人',  
        idFileRow.append("{}".format(LIST_GENDER[random.randint(0,3)]))                         #   GENDER                        smallint(6)             DEFAULT NULL            COMMENT '性别 0::未知、1男性、2：、女性、9:未知说明的性别', 
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   FACE_TOTAL_SCORE              decimal(10,4)           DEFAULT NULL            COMMENT '人脸质量总分',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   LIGHT_SCORE                   decimal(10,4)           DEFAULT NULL            COMMENT '光照分',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   MASK_SCORE                    decimal(10,4)           DEFAULT NULL            COMMENT '口罩得分',  
        idFileRow.append("{}".format(random.randint(0,1)))                                      #   MASK_STATE                    smallint(6)             DEFAULT '0'             COMMENT '戴口罩状态，0：否 1：是',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   CLARITY_SCORE                 decimal(10,4)           DEFAULT NULL            COMMENT '清晰度分',  
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   GLASSES_SCORE                 decimal(10,4)           DEFAULT NULL            COMMENT '是否戴眼镜分数',
        idFileRow.append("{}".format(random.randint(0,1)))                                      #   GLASSES_STATE                 smallint(6)             DEFAULT '0'             COMMENT '是否戴眼镜 0：否 1:是',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   MOUTH_SCORE                   decimal(10,4)           DEFAULT NULL            COMMENT '闭嘴分数',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   FACE_CLARITY_SCORE            decimal(10,4)           DEFAULT NULL            COMMENT '人脸越清晰分数',
        idFileRow.append("{}".format(random.randint(0,1)))                                      #   SUN_GLASSES_STATE             smallint(6)             DEFAULT '0'             COMMENT '是否戴墨镜 0：否 1：是',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   SUN_GLASSES_SCORE             decimal(10,4)           DEFAULT NULL            COMMENT '戴墨镜分数',
        idFileRow.append("{}".format(random.randint(1,2)))                                      #   NATION                        smallint(6)             DEFAULT NULL            COMMENT '国籍 1 中国人   2 外国人',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   PITCH                         decimal(10,4)           DEFAULT NULL            COMMENT '人脸旋转角PITCH',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   YAW                           decimal(10,4)           DEFAULT NULL            COMMENT '人脸旋转角YAW',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   ROLL                          decimal(10,4)           DEFAULT NULL            COMMENT '人脸旋转角 ROLL',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   X                             decimal(10,4)           DEFAULT NULL            COMMENT '人脸框     X',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   Y                             decimal(10,4)           DEFAULT NULL            COMMENT '人脸框     Y',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   WIDTH                         decimal(10,4)           DEFAULT NULL            COMMENT '人脸框     WIDTH',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   HEIGHT                        decimal(10,4)           DEFAULT NULL            COMMENT '人脸框     HEIGHT',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   PRINT_FACE_SCORE              decimal(10,4)           DEFAULT NULL            COMMENT '是否黑白打印纸张，越大表示越可能是真人',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   LEFT_BLINK_SCORE              decimal(10, 4)          DEFAULT NULL            COMMENT '左眼睁眼分',
        idFileRow.append("{:.4f}".format(1000*random.random()))                                 #   RIGHT_BLINK_SCORE             decimal(10, 4)          DEFAULT NULL            COMMENT '右眼睁眼分',
        idFileRow.append("{}".format(random.randint(0,1)))                                      #   LEFT_BLINK_STATE              smallint(6)             DEFAULT '0'             COMMENT '是否闭左眼 0：否 1：是',
        idFileRow.append("{}".format(random.randint(0,1)))                                      #   RIGHT_BLINK_STATE             smallint(6)             DEFAULT '0'             COMMENT '是否闭右眼 0：否 1：是',
        idFileRow.append("{}".format(random.randint(0,1)))                                      #   IS_LINKAGE                    smallint(6)             DEFAULT '0'             COMMENT '是否级联数据,1:是 0:否',
        idFileRow.append("{}".format(random.randint(0,1)))                                      #   FEATURE_STATUS                smallint(6)             DEFAULT '0'             COMMENT '特征提取(0 成功 1 失败)',
        idFileRow.append("{}".format(CAPTURE_TIME))                                             #   CREATE_TIME                   bigint(20)              NOT NULL                COMMENT '保存时间',
        idFileRow.append("{}".format(CAPTURE_TIME))                                             #   CAPTURE_RECEIVE_TIME          bigint(20)              DEFAULT NULL            COMMENT '抓拍接收时间13位时间戳',
        idFileRow.append("{}".format(random.randint(0,100)))                                    #   ENGINE_CALL_TIME              bigint(20)              DEFAULT NULL            COMMENT '引擎调用耗时',
        idFileRow.append(CAPTURE_TIME_TEXT_Y)                                                   #   YEAR                          smallint(6)             NOT NULL                COMMENT '年YYYY',
        idFileRow.append(CAPTURE_TIME_TEXT_YM)                                                  #   MONTH                         int(11)                 NOT NULL                COMMENT '月YYYYMM',
        idFileRow.append(CAPTURE_TIME_TEXT_YMD)                                                 #   DAY                           int(11)                 NOT NULL                COMMENT '日YYYYMMDD',
        
        # 写入文件
        fHisWifi.write('|'.join(idFileRow) +'\n')
        if (i +1) % 10000 == 0:
            t1 = datetime.datetime.now()
            dur = (t1 - t0).total_seconds()
            print('{} Records done.\tpreFix:{}\ttaskID:{}\tDur:{:.2f} Sec.'.format(i+1,preFix,taskID,dur)) 

    if (i +1) % 10000 != 0:
        dur = (t1 - t0).total_seconds()
        print('{} Records done.\tpreFix:{}\ttaskID:{}\tDur:{:.2f} Sec.'.format(i+1,preFix,taskID,dur))  

    fHisWifi.close()


if __name__ == "__main__":
    args = None
    args = parseArgs(args)

    PARALLEL = args.parallel
    BATCH_SIZE = args.batch_size
    CAM_LIST = []
    
    CAP_LIST = []
    for i in range(0,MAX_WIFI_HOT):
        CAP_LIST.append(str(random.randint(1,MAX_WIFI_HOT*100)))

    for i in range(0,MAX_WIFI_HOT):
        CAM_INFO = {}
        CAM_INFO['MAC'] = genMac(77)
        CAM_INFO['LON'] = "{:.5f}".format(MIN_LON + (MAX_LON-MIN_LON)* random.random())
        CAM_INFO['LAT'] = "{:.5f}".format(MIN_LAT + (MAX_LAT-MIN_LAT)* random.random())
        CAM_LIST.append(CAM_INFO)

    pool = multiprocessing.Pool(processes = PARALLEL)
    taskInputs = []
    
    for i in range(0,PARALLEL):
        taskInput = {}
        taskInput['taskID'] = i 
        taskInput['preFix'] = args.prefix 
        taskInputs.append(taskInput)

    pool.map(genWifi, taskInputs) 
    pool.close()
    pool.join()