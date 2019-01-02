drop external table wifi_s_ext;
create external table wifi_s_ext(
mac varchar(128),
capture_time integer,
lon numeric,
lat numeric,
hot_id integer
)
location (
'gpfdist://master:8081/hisWifi_0.txt'
)
FORMAT 'text' (delimiter ',' null '' escape 'OFF');


wifi=# create table wifi_small with(appendonly=true) as select * from wifi_s_ext distributed by (mac);
SELECT 847462964
Time: 255884.313 ms

#把300秒的同一个热点的数据聚合为一条数据
drop table wifi_small_clean;
create table wifi_small_clean with(appendonly=true,compresslevel=5) as 
select ct*300 ct,mac,lat,lon,hot_id from (
select capture_time/300 ct,mac,lat,lon,hot_id from wifi_small group by capture_time/300,mac,lat,lon,hot_id
) as a
distributed by (mac); 
SELECT 472451411
Time: 110768.931 ms

create index idx_wifi_small_ct_geo on wifi_small_clean(ct,lat,lon);
CREATE INDEX
Time: 84677.839 ms

#直接从外部表聚合,可以不聚合
drop table wifi_small_clean;
create table wifi_small_clean with(appendonly=true,compresslevel=5) as 
select ct*300 ct,mac,lat,lon,hot_id from (
select capture_time/300 ct,mac,lat,lon,hot_id from wifi_s_ext group by capture_time/300,mac,lat,lon,hot_id
) as a
distributed by (mac); 
SELECT 472451411
Time: 220908.665 ms

create index idx_wifi_small_ct_geo on wifi_small_clean(ct,lat,lon);
CREATE INDEX
Time: 82497.233 ms

drop external table id_s_ext;
create external table id_s_ext(
id_card varchar(128),
capture_time integer,
lon varchar(128),
lat varchar(128),
hot_id integer
)
location (
'gpfdist://master:8081/hisID_0.txt'
)
FORMAT 'text' (delimiter ',' null '' escape 'OFF');

create table id_small with(appendonly=true,compresslevel=5) as select * from id_s_ext distributed by (id_card);
SELECT 11679144
Time: 2967.681 ms

explain analyze select * from wifi_small_clean 
where ct = (1539498813 / 300)*300 #使用公式情况下不用索引
and lon > 106.81034 - 0.0002 
and lon <= 106.81034 + 0.0002 
and lat >  29.60613 - 0.0002
and lat <= 29.60613 + 0.0002;

                                                       QUERY PLAN                                                        
-------------------------------------------------------------------------------------------------------------------------
 Gather Motion 30:1  (slice1; segments: 30)  (cost=0.00..2496.43 rows=231 width=45)
   Rows out:  0 rows at destination with 6273 ms to end.
   ->  Table Scan on wifi_small_clean  (cost=0.00..2496.40 rows=8 width=45)
         Filter: ct = (1539498813 / 300) AND lon > 106.81014 AND lon <= 106.81054 AND lat > 29.60593 AND lat <= 29.60633
         Rows out:  0 rows (seg0) with 6272 ms to end.
 Slice statistics:
   (slice0)    Executor memory: 386K bytes.
   (slice1)    Executor memory: 325K bytes avg x 30 workers, 325K bytes max (seg0).
 Statement statistics:
   Memory used: 128000K bytes
 Optimizer status: PQO version 3.1.0
 Total runtime: 6274.360 ms
(12 rows)

Time: 6378.532 ms

wifi=# explain analyze select * from wifi_small_clean 
wifi-# where ct = 1539498600
wifi-# and lon > 106.81034 - 0.0002 
wifi-# and lon <= 106.81034 + 0.0002 
wifi-# and lat >  29.60613 - 0.0002
wifi-# and lat <= 29.60613 + 0.0002;
                                                        QUERY PLAN                                                         
---------------------------------------------------------------------------------------------------------------------------
 Gather Motion 30:1  (slice1; segments: 30)  (cost=0.00..431.00 rows=1 width=45)
   Rows out:  2 rows at destination with 1.865 ms to first row, 2.883 ms to end.
   ->  Bitmap Table Scan on wifi_small_clean  (cost=0.00..431.00 rows=1 width=45)
         Recheck Cond: ct = 1539498600 AND lon > 106.81014 AND lon <= 106.81054 AND lat > 29.60593 AND lat <= 29.60633
         Rows out:  Avg 1.0 rows x 2 workers.  Max 1 rows (seg3) with 1.102 ms to first row, 1.130 ms to end.
         ->  Bitmap Index Scan on idx_wifi_small_ct_geo  (cost=0.00..0.00 rows=0 width=0)
               Index Cond: ct = 1539498600 AND lat > 29.60593 AND lat <= 29.60633 AND lon > 106.81014 AND lon <= 106.81054
               Bitmaps out:  Avg 1.0 x 30 workers.  Max 1 (seg0) with 0.034 ms to end, start offset by 50 ms.
               Work_mem used:  9K bytes avg, 9K bytes max (seg3).
 Slice statistics:
   (slice0)    Executor memory: 394K bytes.
   (slice1)    Executor memory: 480K bytes avg x 30 workers, 502K bytes max (seg3).  Work_mem: 9K bytes max.
 Statement statistics:
   Memory used: 128000K bytes
 Optimizer status: PQO version 3.1.0
 Total runtime: 51.341 ms

select * from wifi_small_clean where ct = 1539498600 and lon > 106.81034 - 0.0002 
and lon <= 106.81034 + 0.0002 and lat >  29.60613 - 0.0002
and lat <= 29.60613 + 0.0002;

create index idx_wifi_ct_geo on wifi_clean(capture_time,lat,lon);
