import requests
import pymysql
import config
import sys
from datetime import datetime
import os

from lets_common_log import logUtils as log

conf = config.config("config.ini")

if conf.default:
    log.error("Not found config.ini")
    log.warning("making config.ini ...")
    sys.exit()

if not conf.checkConfig():
    log.error("config ERROR remove config.ini")
    os.remove("config.ini")
    sys.exit()

host = conf.config["db"]["host"]
user = conf.config["db"]["username"]
password = conf.config["db"]["password"]
table = conf.config["db"]["database"]

OSUAPIKEY = conf.config["osu"]["APIKEY"]

db = pymysql.connect(host=host, user=user, password=password, db=table, charset="utf8")
cur = db.cursor()

url = ["https://api.chimu.moe/cheesegull/s/", "https://storage.ripple.moe/api/s/", "https://osu.direct/api/s/", "https://redstar.moe/api/v1/get_beatmaps?s=", f"https://osu.ppy.sh/api/get_beatmaps?k={OSUAPIKEY}&s="]

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
            cur.execute(f"INSERT INTO {table}.`sets` (`id`, `ranked_status`, `approved_date`, `last_update`, `last_checked`, `artist`, `title`, `creator`, `source`, `tags`, `has_video`, `genre`, `language`, `favourites`, `set_modes`)\
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",\
                        (list['SetId'], list['RankedStatus'], list['ApprovedDate'], list['LastUpdate'], list['LastChecked'], list['Artist'], list['Title'], list['Creator'], list['Source'], list['Tags'], list['HasVideo'], list['Genre'], list['Language'], list['Favourites'], list['set_modes']))
            db.commit()
        sql_insert_sets()
    except:
        log.warning(f"{table}.sets delete & insert | bsid = {list['SetId']}")
        cur.execute(f"DELETE FROM {table}.sets WHERE id = {list['SetId']}")
        db.commit()
        sql_insert_sets()

    try:
        #cheesegull.beatmaps table
        for i in list['ChildrenBeatmaps']:
            log.debug(i["BeatmapID"])
            log.info(i["DiffName"])
            cur.execute(f"INSERT INTO {table}.`beatmaps` (`id`, `parent_set_id`, `diff_name`, `file_md5`, `mode`, `bpm`, `ar`, `od`, `cs`, `hp`, `total_length`, `hit_length`, `playcount`, `passcount`, `max_combo`, `difficulty_rating`)\
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",\
                    (i['BeatmapID'], i['ParentSetID'], i['DiffName'], i["FileMD5"], i['Mode'], i['BPM'], i['AR'], i['OD'], i['CS'], i['HP'], i['TotalLength'], i['HitLength'], i['Playcount'], i['Passcount'], i['MaxCombo'], i['DifficultyRating']))
            db.commit()
    except:
        log.error(f"{table}.beatmaps skipped! | bid = {i['BeatmapID']}")
    db.close()


def get_redstarAPI_ChildrenBeatmaps(bsid):
    rqurl = url[3] + str(bsid)
    r = requests.get(rqurl)
    if r.status_code == 200:
        r = r.json()

    r2 = requests.get(f"{url[4]}{bsid}")
    if r2.status_code == 200:
        r2 = r2.json()

    r3 = requests.get(f"{url[0]}{bsid}")
    if r3.status_code == 200:
        r3 = r3.json()["ChildrenBeatmaps"]

    def b(txt, bid):
        if " cs" in txt.lower() and int(bid) < 0:
            bName = txt[:txt.lower().find(' cs')]
            for i in r2:
                if i["version"] == bName:
                    return {"CS": 0, "HP": i["diff_drain"], "playcount": i["playcount"], "passcount": i["passcount"]}

        if " ar" in txt.lower() and int(bid) < 0:
            bName = txt[:txt.lower().find(' ar')]
            for i in r2:
                if i["version"] == bName:
                    return {"CS": i["diff_size"], "HP": i["diff_drain"], "AR": 0, "playcount": i["playcount"], "passcount": i["passcount"]}

        if " hp" in txt.lower() and int(bid) < 0:
            bName = txt[:txt.lower().find(' hp')]
            for i in r2:
                if i["version"] == bName:
                    return {"CS": i["diff_size"], "HP": 0, "playcount": i["playcount"], "Passcount": i["passcount"]}

        if " od" in txt.lower() and int(bid) < 0:
            bName = txt[:txt.lower().find(' od')]
            for i in r2:
                if i["version"] == bName:
                    return {"CS": i["diff_size"], "HP": i["diff_drain"], "OD": 0, "playcount": i["playcount"], "Passcount": i["passcount"]}
                
        for i in r2:
            if i["version"] == txt:
                return {"CS": i["diff_size"], "HP": i["diff_drain"], "playcount": i["playcount"], "passcount": i["passcount"]}

    def c(txt, bid):
        if " cs" in txt.lower() and int(bid) < 0:
            bName = txt[:txt.lower().find(' cs')]
            for i in r3:
                if i["DiffName"] == bName:
                    return {"CS": 0, "HP": i["HP"], "Playcount": i["Playcount"], "Passcount": i["Passcount"]}

        if " ar" in txt.lower() and int(bid) < 0:
            bName = txt[:txt.lower().find(' ar')]
            for i in r3:
                if i["DiffName"] == bName:
                    return {"CS": i["CS"], "HP": i["HP"], "AR": 0, "Playcount": i["Playcount"], "Passcount": i["Passcount"]}

        if " hp" in txt.lower() and int(bid) < 0:
            bName = txt[:txt.lower().find(' hp')]
            for i in r3:
                if i["DiffName"] == bName:
                    return {"CS": i["CS"], "HP": 0, "Playcount": i["Playcount"], "Passcount": i["Passcount"]}

        if " od" in txt.lower() and int(bid) < 0:
            bName = txt[:txt.lower().find(' od')]
            for i in r3:
                if i["DiffName"] == bName:
                    return {"CS": i["CS"], "HP": i["HP"], "OD": 0, "Playcount": i["Playcount"], "Passcount": i["Passcount"]}

        for i in r3:
            #RedstarAPI
            #48498 masterpiece 예외처리 시발련아
            if int(bid) == -14:
                if i["AR"] == 8:
                   return {"CS": i["CS"], "HP": i["HP"], "Playcount": i["Playcount"], "Passcount": i["Passcount"]}
            #RedstarAPI
            #10000000 Black + White (TV Size) 예외처리 시발련아
            elif int(bid) == -19:
                return {"CS": -1, "HP": 7, "Playcount": -1, "Passcount": -1}

            elif i["DiffName"] == txt:
                return {"CS": i["CS"], "HP": i["HP"], "Playcount": i["Playcount"], "Passcount": i["Passcount"]}
            

    Dict = []

    for i in r:
        if (len(r) != len(r2)) or (len(r) != len(r3)):
            bancho = b(i["version"], i["beatmap_id"])
            chimu = c(i["version"], i["beatmap_id"])

        Dict2 = {}
        Dict2["ParentSetID"] = i["beatmapset_id"]
        Dict2["BeatmapID"] = i["beatmap_id"]
        Dict2["TotalLength"] = i["total_length"]
        Dict2["HitLength"] = i["hit_length"]
        Dict2["DiffName"] = i["version"]
        Dict2["FileMD5"] = i["file_md5"]
        try:
            Dict2["CS"] = bancho["diff_size"]
        except:
            log.warning("child | CS | 2차 Bancho, 3차 chimu 까지옴")
            Dict2["CS"] = chimu["CS"]

        Dict2["AR"] = i["diff_approach"]
        try:
            Dict2["HP"] = bancho["diff_drain"]
        except:
            log.warning("child | HP | 2차 Bancho, 3차 chimu 까지옴")
            Dict2["HP"] = chimu["HP"]

        Dict2["OD"] = i["diff_overall"]
        Dict2["Mode"] = i["mode"]
        Dict2["BPM"] = i["bpm"]
        try:
            Dict2["Playcount"] = bancho["playcount"]
        except:
            log.warning("child | Playcount | 2차 Bancho, 3차 chimu 까지옴")
            Dict2["Playcount"] = chimu["Playcount"]

        try:
            Dict2["Passcount"] = bancho["passcount"]
        except:
            log.warning("child | Passcount | 2차 Bancho, 3차 chimu 까지옴")
            Dict2["Passcount"] = chimu["Passcount"]

        Dict2["MaxCombo"] = i["max_combo"]
        Dict2["DifficultyRating"] = i["difficultyrating"]

        Dict2["RankedStatus"] = i["approved"]
        Dict2["ApprovedDate"] = i["approved_date"]
        print("")
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

    r2 = requests.get(f"{url[4]}{bsid}")
    if r2.status_code == 200:
        r2 = r2.json()

    r3 = requests.get(f"{url[0]}{bsid}")
    if r3.status_code == 200:
        r3 = r3.json()

    #RedstarAPI
    #10000000 Black + White (TV Size) 예외처리 시발련아 22
    if int(bid) == -19:
        r3 = {"Creator": "arpia97", "Source": "Mondaiji-tachi ga Isekai kara Kuru Sou Desu yo?", \
              "Tags": "momochi dakedekaane xinely hinsvar kuro usagi bunny girl asuka you izayoi adaption novel opening osutrainer", \
              "HasVideo": 0, "Genre": -1, "Language": -3, "Favourites": -1
              }

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
        try:
            Dict["Creator"] = r2[0]["creator"]
        except:
            log.warning("Creator | 1차 Redstar, 2차 Bancho, 3차 chimu 까지옴")
            Dict["Creator"] = r3["Creator"]

    try:    
        Dict["Source"] = r["Source"]
    except:
        try:
            Dict["Source"] = r2[0]["source"]
        except:
            log.warning("Source | 1차 Redstar, 2차 Bancho, 3차 chimu 까지옴")
            Dict["Source"] = r3["Source"]

    try:
        Dict["Tags"] = r["Tags"]
    except:
        try:
            Dict["Tags"] = r2[0]["tags"] + "osutrainer"
        except:
            log.warning("Tags | 1차 Redstar, 2차 Bancho, 3차 chimu 까지옴")
            Dict["Tags"] = r3["Tags"] + "osutrainer"

    try:
        Dict["HasVideo"] = r["HasVideo"]
    except:
        try:
            Dict["HasVideo"] = r2[0]["video"]
        except:
            log.warning("HasVideo | 1차 Redstar, 2차 Bancho, 3차 chimu 까지옴")
            Dict["HasVideo"] = r3["HasVideo"]

    try:
        Dict["Genre"] = r["Genre"]
    except:
        try:
            Dict["Genre"] = r2[0]["genre_id"]
        except:
            log.warning("Genre | 1차 Redstar, 2차 Bancho, 3차 chimu 까지옴")
            Dict["Genre"] = r3["Genre"]

    try:
        Dict["Language"] = r["Language"]
    except:
        try:
            Dict["Language"] = r2[0]["language_id"]
        except:
            log.warning("Language | 1차 Redstar, 2차 Bancho, 3차 chimu 까지옴")
            Dict["Language"] = r3["Language"]

    try:
        Dict["Favourites"] = r["Favourites"]
    except:
        try:
            Dict["Favourites"] = r2[0]["favourite_count"]
        except:
            log.warning("Language | 1차 Redstar, 2차 Bancho, 3차 chimu 까지옴")
            Dict["Favourites"] = r3["Favourites"]

    print("")

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