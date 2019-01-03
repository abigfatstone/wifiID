# -*- coding: utf-8 -*-  
import psycopg2
import os


conn = psycopg2.connect(database="wifi", user="gpadmin", password="dev", host="master", port="5432")
 
cur = conn.cursor()

sqlText =   "select mac from wifi_clean where capture_time > {} and capture_time <= {} and lon > \'{}\' "\
            "and lon <= \'{}\' and lat > \'{}\' and lat <= \'{}\'".format(
            1539498813 -150,1539498813 +150, 106.81034 - 0.0002 ,106.81034 + 0.0002 ,29.60613 - 0.0002,29.60613 + 0.0002)
print(sqlText)
cur.execute(sqlText);
rows = cur.fetchall()        # all rows in table
for i in rows:
    print(i)
conn.commit()
cur.close()
conn.close()
