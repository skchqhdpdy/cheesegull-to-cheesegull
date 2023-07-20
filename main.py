import requests
import pymysql
import config
import sys
from datetime import datetime

from lets_common_log import logUtils as log

conf = config.config("config.ini")

if conf.default:
    log.error("Not found config.ini")
    log.warning("making config.ini ...")
    sys.exit()

if not conf.checkConfig():
    log.error("config ERROR remove config.ini")
    sys.exit()

host = conf.config["db"]["host"]
user = conf.config["db"]["username"]
password = conf.config["db"]["password"]
db = conf.config["db"]["database"]

OSUAPIKEY = conf.config["osu"]["APIKEY"]

db = pymysql.connect(host=host, user=user, password=password, db=db, charset="utf8")
cur = db.cursor()

url = ["https://api.chimu.moe/cheesegull/s/", "https://storage.ripple.moe/api/s/", "https://osu.direct/api/s/", "https://redstar.moe/api/v1/get_beatmaps?s="]

bid = input("bid 입력 : ")

try:
    bsid = requests.get(f"https://redstar.moe/api/get_beatmaps?b={bid}")
    if bsid.status_code == 200:
        bsid = bsid.json()[0]["beatmapset_id"]
except:
    bsid = "0"

def insert_cheesegullDB():
    list = rq_cheesegull()

    try:
        def sql_insert_sets():
            #cheesegull.sets table
            cur.execute("INSERT INTO `gull`.`sets` (`id`, `ranked_status`, `approved_date`, `last_update`, `last_checked`, `artist`, `title`, `creator`, `source`, `tags`, `has_video`, `genre`, `language`, `favourites`, `set_modes`)\
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",\
                        (list['SetId'], list['RankedStatus'], list['ApprovedDate'], list['LastUpdate'], list['LastChecked'], list['Artist'], list['Title'], list['Creator'], list['Source'], list['Tags'], list['HasVideo'], list['Genre'], list['Language'], list['Favourites'], list['set_modes']))
            db.commit()
        sql_insert_sets()
    except:
        log.warning(f"cheesegull.sets delete & insert | bsid = {list['SetId']}")
        cur.execute(f"DELETE FROM gull.sets WHERE id = {list['SetId']}")
        db.commit()
        sql_insert_sets()

    try:
        #cheesegull.beatmaps table
        for i in list['ChildrenBeatmaps']:
            log.debug(i["BeatmapID"])
            log.info(i["DiffName"])
            cur.execute("INSERT INTO `gull`.`beatmaps` (`id`, `parent_set_id`, `diff_name`, `file_md5`, `mode`, `bpm`, `ar`, `od`, `cs`, `hp`, `total_length`, `hit_length`, `playcount`, `passcount`, `max_combo`, `difficulty_rating`)\
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",\
                    (i['BeatmapID'], i['ParentSetID'], i['DiffName'], i["FileMD5"], i['Mode'], i['BPM'], i['AR'], i['OD'], i['CS'], i['HP'], i['TotalLength'], i['HitLength'], i['Playcount'], i['Passcount'], i['MaxCombo'], i['DifficultyRating']))
            db.commit()
    except:
        log.error(f"cheesegull.beatmaps skipped! | bid = {i['BeatmapID']}")
    db.close()


def get_redstarAPI_ChildrenBeatmaps(bsid):
    rqurl = url[3] + str(bsid)
    r = requests.get(rqurl)
    if r.status_code == 200:
        r = r.json()

    Dict = []

    for i in r:
        Dict2 = {}
        Dict2["ParentSetID"] = i["beatmapset_id"]
        Dict2["BeatmapID"] = i["beatmap_id"]
        Dict2["TotalLength"] = i["total_length"]
        Dict2["HitLength"] = i["hit_length"]
        Dict2["DiffName"] = i["version"]
        Dict2["FileMD5"] = i["file_md5"]
        Dict2["CS"] = -1
        Dict2["AR"] = i["diff_approach"]
        Dict2["HP"] = -1
        Dict2["OD"] = i["diff_overall"]
        Dict2["Mode"] = i["mode"]
        Dict2["BPM"] = i["bpm"]
        Dict2["Playcount"] = -1 #i["playcount"]
        Dict2["Passcount"] = -1 #i["passcount"]
        Dict2["MaxCombo"] = i["max_combo"]
        Dict2["DifficultyRating"] = i["difficultyrating"]

        Dict2["RankedStatus"] = i["approved"]
        Dict2["ApprovedDate"] = i["approved_date"]

        Dict.append(Dict2)
    
    return Dict

def rq_cheesegull():
    Dict = {}

    for i in url:
        urlRq = i + str(bsid) if int(bid) > 0 else url[3] + str(bsid)
        log.info(f"Request {urlRq}")
        r = requests.get(urlRq)
        if r.status_code == 200:
            r = r.json()
            break
        else:
            log.error(f"Failed {urlRq}")

    print("")

    try:
        Dict["SetId"] = r["SetId"]
    except:
        try:
            Dict["SetId"] = r["SetID"]
        except:
            try:
                Dict["SetId"] = r[0]["beatmapset_id"]
            except:
                log.error("회생불가")
                sys.exit()

    try:
        Dict["ChildrenBeatmaps"] = r["ChildrenBeatmaps"]
    except:
        Dict["ChildrenBeatmaps"] = get_redstarAPI_ChildrenBeatmaps(bsid)

    try:
        Dict["RankedStatus"] = get_redstarAPI_ChildrenBeatmaps(bsid)[0]["RankedStatus"] #r["RankedStatus"]
    except:
        Dict["RankedStatus"] = r[0]["approved"]

    try:
        Dict["ApprovedDate"] = get_redstarAPI_ChildrenBeatmaps(bsid)[0]["ApprovedDate"] #r["ApprovedDate"]
    except:
        Dict["ApprovedDate"] = r[0]["approved_date"]

    try:
        Dict["LastUpdate"] = r["LastUpdate"]
    except:
        Dict["LastUpdate"] = r[0]["last_update"]

    #Dict["LastChecked"] = r["LastChecked"]
    Dict["LastChecked"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000ZZ')

    try:
        Dict["Artist"] = r["Artist"]
    except:
        Dict["Artist"] = r[0]["artist"]

    #Artist_Unicode = r["Artist_Unicode"]
    
    try:
        Dict["Title"] = r["Title"]
    except:
        Dict["Title"] = r[0]["title"]
    #Title_Unicode = r["Title_Unicode"]
    
    try:
        Dict["Creator"] = r["Creator"]
    except:
        Dict["Creator"] = -1 #r[0]["creator"]

    try:    
        Dict["Source"] = r["Source"]
    except:
        Dict["Source"] = -1 #r[0]["source"]

    try:
        Dict["Tags"] = r["Tags"]
    except:
        Dict["Tags"] = -1 #r[0]["tags"]

    try:
        Dict["HasVideo"] = r["HasVideo"]
    except:
        Dict["HasVideo"] = -1

    try:
        Dict["Genre"] = r["Genre"]
    except:
        Dict["Genre"] = -1 #r[0]["genre_id"]

    try:
        Dict["Language"] = r["Language"]
    except:
        Dict["Language"] = -1 #r[0]["language_id"]

    try:
        Dict["Favourites"] = r["Favourites"]
    except:
        Dict["Favourites"] = -1 #r[0]["favourite_count"]

    try:        
        num = []
        num2 = 0
        for i in Dict["ChildrenBeatmaps"]:
            if i["Mode"] == 0:
                if 1 not in num:
                    num.append(1)
            elif i["Mode"] == 1:
                if 2 not in num:
                    num.append(2)
            elif i["Mode"] == 2:
                if 4 not in num:
                    num.append(4)
            elif i["Mode"] == 3:
                if 8 not in num:
                    num.append(8)
                    
        for i in num:
            num2 += i
        Dict["set_modes"] = num2
    except:
        Dict["set_modes"] = -1
    return Dict

insert_cheesegullDB()