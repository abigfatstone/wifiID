\timing
drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__6 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__6_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__6_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__6 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__7 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__7_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__7_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__7 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__8 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__8_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__8_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__8 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__9 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__9_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__9_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__9 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__10 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__10_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__10_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__10 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__11 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__11_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__11_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__11 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__12 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__12_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__12_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__12 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__13 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__13_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__13_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__13 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__14 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__14_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__14_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__14 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__15 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__15_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__15_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__15 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__16 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__16_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__16_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__16 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__17 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__17_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__17_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__17 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__18 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__18_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__18_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__18 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__19 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__19_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__19_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__19 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__20 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__20_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__20_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__20 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__21 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__21_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__21_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__21 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__22 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__22_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__22_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__22 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__23 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__23_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__23_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__23 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__24 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__24_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__24_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__24 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__25 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__25_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__25_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__25 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__26 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__26_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__26_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__26 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__27 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__27_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__27_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__27 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__28 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__28_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__28_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__28 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__29 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__29_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__29_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__29 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__30 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__30_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__30_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__30 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__31 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__31_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__31_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__31 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__32 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__32_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__32_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__32 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__33 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__33_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__33_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__33 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__34 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__34_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__34_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__34 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__35 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__35_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__35_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__35 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__36 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__36_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__36_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__36 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__37 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__37_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__37_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__37 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__38 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__38_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__38_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__38 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__39 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__39_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__39_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__39 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__40 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__40_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__40_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__40 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__41 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__41_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__41_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__41 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__42 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__42_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__42_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__42 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__43 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__43_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__43_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__43 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__44 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__44_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__44_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__44 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__45 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__45_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__45_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__45 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__46 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__46_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__46_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__46 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__47 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__47_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__47_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__47 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__48 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__48_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__48_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__48 with table af_poc_for_index;

drop table af_poc_for_index; 
CREATE TABLE af_poc_for_index WITH(appendonly=true,compresslevel=5,orientation=column) AS 
SELECT * FROM af_poc_tmp2 LIMIT 0 distributed by (CAPTURE_ID) ; 
alter table af_poc_20b exchange partition pn__49 with table  af_poc_for_index;
select count(1) from af_poc_for_index;
create index idx_loc_new_1_prt_pn__49_n on af_poc_for_index(lng,lat);
create index idx_device_id_new_1_prt_pn__49_n on  af_poc_for_index(DEVICE_ID);
alter table af_poc_20b_NEW exchange partition pn__49 with table af_poc_for_index;

