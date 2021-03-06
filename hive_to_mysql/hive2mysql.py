import MySQLdb
import os
import json
import time
def process(str_line):
    value = str_line.split()
    decoded = json.loads(value[5])
 #   print json.dumps(decoded,sort_keys=True,indent=1)
    imp_groups = value[4] + "("
    product = ""
    length = 0
    if(value[5] != "null"):
        length = len(decoded)
        for i in range(0,len(decoded),1):
            length += len(decoded[i]['tmeta_l3'])
    
    mid = ""
    mk = ["0"]*length
    if(decoded):
        ll = 0
        start = 0
        for i in range(0,len(decoded),1):
            if(decoded[i]['product']):
                product = product + decoded[i]['product'] + "/"
            else:
                product = product + 'no_product' + "/"
            if(decoded[i]['position']):
                imp_groups = imp_groups + decoded[i]['position'] + "/"
            else:
                imp_groups = imp_groups + 'no_imp_groups' + "/"
            if(decoded[i]['tmeta_l3']):
                for j in range(0,len(decoded[i]['tmeta_l3'])):
                    if "itemid" in decoded[i]['tmeta_l3'][j]:
                        index = int(decoded[i]['tmeta_l3'][j]['loc'])-1
                        if(decoded[i]['tmeta_l3'][j]['itemid']):
                            mk[index+start] = decoded[i]['tmeta_l3'][j]['itemid']
                        else:
                            mk[index+start] = "no_mid"
                    else:
                        index = int(decoded[i]['tmeta_l3'][j]['loc'])-1
                        mk[index+start] = 'no_mid'
                    ll += 1
                mk[ll] = "|"
                ll += 1
                start = ll
            else:
                mk[ll] = 'no_mid'
                ll += 1
                mk[ll] = "|"
                ll += 1
                start = ll
 
    for mm in mk:
        if(mm != '|'):
            mid +=  mm + ','
        else:
            mid = mid[:-1] + '|'

    if(value[4] == "0"):
        imp_groups = "0"
    else:
        imp_groups = imp_groups[:-1] + ")"
    product = product[:-1]
    mid = mid[:-1]

    time1 = int(value[0])
    time1 = time.localtime(time1)
    timestr = time.strftime('%Y-%m-%d %H:%M:%S',time1)
    
    return (timestr,value[1],value[2],imp_groups,product,mid,value[3])

try:
    conn=MySQLdb.connect(host='10.77.96.56',user='udatastats',passwd='2014udatastats',db='udatastats',port=3306)
    cur = conn.cursor(MySQLdb.cursors.DictCursor)
    cur.execute('select * from hy_hive_mysql where pstat_status=1')
    row=cur.fetchall()
    for data in row:
        cur_str = "sh hive2mysql.sh " + data["stat_date"] + " " + data["service_name"] + " " + data["uid"]
        os.system(cur_str)
        f = open("hy_data.log")
        lines = f.readlines()
        for line in lines:
            if(line):
                (reqtime,unread_status,available_pos,imp_groups,product,mid,service_name) = process(line)
                if(unread_status != "0" or available_pos != "0" or imp_groups != "0" or product !='' or mid != ''):
                    sql_str = 'insert into hy_uid_stats(uid,reqtime,stat_date,unread_status,service_name,available_pos,imp_groups,product,mid) values (\''+ data["uid"] + '\',\''+ reqtime + '\',\'' + data["stat_date"] + '\',\'' + unread_status + '\',\'' + service_name + '\',\'' + available_pos + '\',\'' + imp_groups + '\',\'' + product + '\',\'' + mid + '\')'
                    cur.execute(sql_str)

        hy_str = "update hy_hive_mysql set pstat_status=0,rstat_status=1 where uid='" + data['uid'] + "' and stat_date='" + data['stat_date']+ "' and service_name='" + data['service_name'] + "'"
        cur.execute(hy_str)

    conn.commit()
    cur.close()
    conn.close()
except MySQLdb.Error,e:
    print "Mysql Error %d: %s" % (e.args[0],e.args[1])
