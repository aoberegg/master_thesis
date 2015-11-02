__author__ = 'aoberegg'
from create_database import execute_select
from create_database import execute_insert_statement
import MySQLdb
import json
import time
import requests
from datetime import datetime
venues_us_file_name = "../networks/foursquare_checkins_Dingqi Yang/venues.us.json"

all_token = ["AYR0UY0PHB0BSZDTEEWIHAFTWQONFQLRGMSWCAPODGSJP2CD","UAS2X343RQH3XYSLP4N2XU1TB14UBSFNU51FDJPNIMXCX2CM",
             "ULQ1KA0HGVUKHA3DINWW0LIX4ELA2XWTPDX3YXUBB2VZNCYX","UQAM0GZAN3MK2LHAOPXHN0UNYPWLCMSMDFO3WFOIGWAGHERT",
             "1Q4UF23WXYPF4RU5WPL4WV3IMB5QL2LBBXSPCXDKCJTZUGXW","CQ2SMSOUJJNWDD1PK0P4MLQO0YSRCHESPJWU3CWAJE4HPFSH",
             "GDNJRQAMB5ROR2QTRN0B1R5H5OPZXEKFNRFUPMULHGAQ1K5S","33WM3O2DJITCJYQR2PWR1ZFBSPA42ENTWPZTTQEO1AT2ETTU",
             "J0CXPKPPN0GYR0U3C5IH5QLLFMVYE3DCUVOL5MX2CYKLXL2N","EUM0OOVWQHM1I1L1MW3DLHNYCVHT5RPIHSVPSTE5SFG0KHOD",
             "WF5MLKEIEFGCBQYILHORG042H4JKERBFRF2XN2X1ML5MDEL0","PYSKRFBKY3OISJW0NOEVBDL3IQI5R1EA01QNF2SANMV4HYNB",
             "KSCPWWQ4XVB3JFXZKM20PMNYR1DKM12F2ZUNDR1ALY5ZSPQI","LHXANNUTCMCICMVTJ0I2XZIVDNFGWN42J2B53XAYCAJT5R4V",
             "MNUS0CR4CYNM1EAXZ2YJKMYBYPL4VNO20T5I4DPRXTHFTZAQ","J04V2PG5OIQNQ4JKMC4J01IBFNDECT41Y0EM2HP44S0WECPB",
             "TQC1Q0BVWILKJNZ3BIFV0OZX1ZZX3YNFL03MEUO5HVI3OBMT","KQDUSO03Y34R0JH4T3YGJTFZPSJ5TD1RN41M2M2SIN5AI5D5",
             "X5N4KQCZ4ITSWI40KGHUJTLJ4NKZOJAP2GZ3VZ53D0LGI1WA","CIDMP4JZWZFX3ERLHKMEY0WJWIXAM5NFL4JX0CYVEOMFFZKM",
             "3LMWXRESZCCOR5PYMV1Z550ASR4IGFVZAJGNIGVKLPRCWPOK","3ZW5INZ1Q1TNIDN04UR2QLHG2M2UFAJ1ZIBZ25T0L22W201V",
             "K4YBTB5XE5FZQ2P2OPB5IEPEH2WSAEUE1TWOOVL1YPKVKXOD","SH4UU5UICKT0T3DKBPCW2LYPEHG02RVFZMYNDUJKLR1ZYKOV"
             ]


if __name__ == "__main__":
    db = MySQLdb.connect(host="localhost", # your host, usually localhost
                 user="aoberegger", # your username
                  passwd="foursquare", # your password
                  db="foursquare") # name of the data base
    db.set_character_set('utf8')
    venue_us_file = open(venues_us_file_name, mode = "a")
    sql = "select CATEGORY.name, v.id from CATEGORY inner join VENUE v on(v.category_id = CATEGORY.id) where v.controlled is null order by CATEGORY.id desc"


    curr_token_index = 0
    curr_token = all_token[curr_token_index]
    result = execute_select(sql,db)
    i = 0
    amount_errors = 0
    datetime_begin = datetime.now()
    print(datetime_begin.strftime("%H:%M:%S.%f"))
    second = False
    for row in result:
        try:
            #request_url = "https://api.foursquare.com/v2/venues/"+ str(row[1]) + "&client_id=ARQ3RY0FEUJ3CAHTWANXUX3X5EAFSFYDI5MFSQ2TINYZOWXJ&client_secret=T1HMEYIVX3UOOCOYNOJ0LJMWXKSBI2MS0J3IVVT0GWDNSZIA&v=20151027"
            request_url = "https://api.foursquare.com/v2/venues/"+ str(row[1]) + "?oauth_token="+curr_token+"&v=20151008&locale=en"
            r = requests.get(request_url)
            response_json = r.json()
            if not response_json["response"]["venue"]["categories"]:
                continue
            if response_json["response"]["venue"]["categories"][0]["name"] != row[0]:

                sql_select_category = " select name, id from CATEGORY where name like '%{0}%' ".format(response_json["response"]["venue"]["categories"][0]["name"].replace("'", "\\'").encode('utf8'))
                result = execute_select(sql_select_category, db)
                category_id = 0;
                if len(result) == 0:
                    print("make new")
                    sql_select_category = " select max(id) from CATEGORY "
                    max_id = execute_select(sql_select_category, db)
                    category_id = int(max_id[0][0]) + 1
                    insert_new_category = " INSERT INTO CATEGORY VALUES ( {0} , '{1}' ) ".format(category_id, response_json["response"]["venue"]["categories"][0]["name"].encode('utf8') )
                    execute_insert_statement(insert_new_category, db)
                else:
                    category_id = result[0][1]

                update = " UPDATE VENUE SET category_id = {0} , controlled = 1 where id = '{1}'".format(category_id, str(row[1]))
                execute_insert_statement(update, db)
                amount_errors +=1
            else:
                update = " UPDATE VENUE SET controlled = 1 where id = '{0}'".format(str(row[1]))
                execute_insert_statement(update, db)
            print(i)
            print("found errors: " + str(amount_errors))
            to_write = {}
            entries = {}
            entries["name"] = response_json["response"]["venue"]["name"]
            entries["location"] = response_json["response"]["venue"]["location"]
            entries["categories"] = response_json["response"]["venue"]["categories"]
            to_write[response_json["response"]["venue"]["id"]] = entries
            venue_us_file.write(json.dumps(to_write))
            i+=1
        except Exception as e:
            print ("Fehler:")
            print (str(e))
            print (response_json)

            try:
                if response_json["meta"]["code"] == 403:
                    print(curr_token_index)
                    curr_token_index += 1
                    if curr_token_index < len(all_token):
                        curr_token = all_token[curr_token_index]
                    else:
                        curr_token_index = 0
                        curr_token = all_token[curr_token_index]
                        time.sleep(1)
                else:
                    print("Let's continue")
                    continue
            except Exception as e:
                print(str(e))



    print("durchlaeufe: " + str(i))
    print("amount errors: " + str(amount_errors) )
    duration = datetime.now() - datetime_begin;
    print("Duration: " + str(duration))
    print("Finished at:")
    print(datetime.now().strftime("%H:%M:%S.%f"))
    print("finished")