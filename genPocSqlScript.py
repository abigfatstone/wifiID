
# -*- coding: utf-8 -*-  
import random
import os
# import pandas as pd
import time
import datetime
import multiprocessing
import argparse

def parseArgs(args):
	parser = argparse.ArgumentParser()
	globalArgs = parser.add_argument_group('Global options\n')
	globalArgs.add_argument('--start_day', type=int, default=16) 
	globalArgs.add_argument('--stop_day', type=int, default=160) 
	globalArgs.add_argument('--action_type', type=str, default='rebuild_index') 
	return parser.parse_args(args)


def rebuild_index(args):
	fHisWifi = open('rebuild_index.sql', 'w')
	fHisWifi.write('\\timing\n')
	for i in range(args.start_day,args.stop_day):
		fHisWifi.write('drop table af_poc_for_index; \n')
		fHisWifi.write('CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS \n')
		fHisWifi.write('SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; \n')

		fHisWifi.write('alter table af_poc_20b exchange partition pn__{} with table  af_poc_for_index;\n'.format(i))
		fHisWifi.write('select count(1) from af_poc_for_index;\n')
		fHisWifi.write('create index idx_loc_new_1_prt_pn__{}_n on af_poc_for_index(lng,lat);\n'.format(i))
		fHisWifi.write('create index idx_device_id_new_1_prt_pn__{}_n on  af_poc_for_index(DEVICE_ID);\n'.format(i))
		fHisWifi.write('alter table af_poc_20b_NEW exchange partition pn__{} with table af_poc_for_index;\n\n'.format(i))
	fHisWifi.close()

def copy_data(args):

	fHisWifi = open('poc_copy.sql', 'w')
	fHisWifi.write('\\timing\n')
	if args.load_txt == 1:
		fHisWifi.write('drop table af_poc_tmp;             \n')
		fHisWifi.write('create table af_poc_tmp with (appendonly=true,compresslevel=5,orientation=column)\n')
		fHisWifi.write(' as select * from af_poc_ext       \n')
		fHisWifi.write('distributed by (CAPTURE_ID);       \n')

	for i in range(0,args.stop_day):
		interval_str = ' + interval \'{} day\''.format(i)

		fHisWifi.write('drop table af_poc_tmp1;            \n')
		fHisWifi.write('create table af_poc_tmp1 with (appendonly=true,compresslevel=5,orientation=column) as \n')
		# fHisWifi.write('insert into af_poc_20b             \n')
		fHisWifi.write('select                             \n')
		fHisWifi.write('      CAPTURE_ID                   \n')
		fHisWifi.write('     ,EG_CAPTURE_ID                \n')
		fHisWifi.write('     ,CAPTURE_SEQ                  \n')
		fHisWifi.write('     ,CAPTURE_TIME'+interval_str +' as CAPTURE_TIME \n')
		fHisWifi.write('     ,DEVICE_ID                    \n')
		fHisWifi.write('     ,DEVICE_NAME                  \n')
		fHisWifi.write('     ,LNG                          \n')
		fHisWifi.write('     ,LAT                          \n')
		fHisWifi.write('     ,QUALITY_SCORE                \n')
		fHisWifi.write('     ,AGE                          \n')
		fHisWifi.write('     ,AGE_GROUP                    \n')
		fHisWifi.write('     ,GENDER                       \n')
		fHisWifi.write('     ,FACE_TOTAL_SCORE             \n')
		fHisWifi.write('     ,LIGHT_SCORE                  \n')
		fHisWifi.write('     ,MASK_SCORE                   \n')
		fHisWifi.write('     ,MASK_STATE                   \n')
		fHisWifi.write('     ,CLARITY_SCORE                \n')
		fHisWifi.write('     ,GLASSES_SCORE                \n')
		fHisWifi.write('     ,GLASSES_STATE                \n')
		fHisWifi.write('     ,MOUTH_SCORE                  \n')
		fHisWifi.write('     ,FACE_CLARITY_SCORE           \n')
		fHisWifi.write('     ,SUN_GLASSES_STATE            \n')
		fHisWifi.write('     ,SUN_GLASSES_SCORE            \n')
		fHisWifi.write('     ,NATION                       \n')
		fHisWifi.write('     ,PITCH                        \n')
		fHisWifi.write('     ,YAW                          \n')
		fHisWifi.write('     ,ROLL                         \n')
		fHisWifi.write('     ,X                            \n')
		fHisWifi.write('     ,Y                            \n')
		fHisWifi.write('     ,WIDTH                        \n')
		fHisWifi.write('     ,HEIGHT                       \n')
		fHisWifi.write('     ,PRINT_FACE_SCORE             \n')
		fHisWifi.write('     ,LEFT_BLINK_SCORE             \n')
		fHisWifi.write('     ,RIGHT_BLINK_SCORE            \n')
		fHisWifi.write('     ,LEFT_BLINK_STATE             \n')
		fHisWifi.write('     ,RIGHT_BLINK_STATE            \n')
		fHisWifi.write('     ,IS_LINKAGE                   \n')
		fHisWifi.write('     ,FEATURE_STATUS               \n')
		fHisWifi.write('     ,CREATE_TIME                  \n')
		fHisWifi.write('     ,CAPTURE_RECEIVE_TIME         \n')
		fHisWifi.write('     ,ENGINE_CALL_TIME             \n')
		fHisWifi.write('     ,YEAR                         \n')
		fHisWifi.write('     ,MONTH                        \n')
		fHisWifi.write('     ,DAY                          \n')
		fHisWifi.write(' from af_poc_tmp                   \n')
		fHisWifi.write(' distributed by (CAPTURE_ID)       \n')
		fHisWifi.write(';      \n')

		fHisWifi.write('alter table af_poc_20b exchange partition pn__{} with table af_poc_tmp1;\n'.format(i+1))

	fHisWifi.close()


if __name__ == "__main__":
	args = None
	args = parseArgs(args)

	if args.action_type =='copy_day':
		copy_data(args)
	elif args.action_type =='rebuild_index':
		rebuild_index(args)	

