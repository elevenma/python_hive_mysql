#!/bin/bash

echo "select reqtime,unread_status,available_pos,service_name1,feedsnum,tmeta from uve_stats_log where stat_date='$1' and service_name='$2' and uid='$3';" > hive2mysql.sql

hive -f hive2mysql.sql > hy_data.log
