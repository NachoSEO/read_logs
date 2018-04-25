import gzip
import re
import pandas as pd

def read_logs(file, type_file):

    ips,date,hour,diff,method,url,status_code,download,referrer,user_agent=[],[],[],[],[],[],[],[],[],[]
    if type_file == "gzip":
        with gzip.open(file, 'rb') as f:
            log_decompressed = f.read()
    elif type_file == "log":
        f = open(file, 'rb')
        log_decompressed = f.read()
    log_decompressed=str(log_decompressed)
    log_decompressed = log_decompressed.split("\\")

    for log in log_decompressed:
        fs = log.find(" ")
        ip = log[:fs].replace("n","").replace("b","").replace("'","")
        ips.append(ip)
        c = log.find("[")
        dp = log.find(":")
        f = log[c:dp].replace("[","")
        date.append(f)
        p = log.find("+")
        h = log[dp+1:p].replace(" ","")
        hour.append(h)
        d = log[p:p+5]
        diff.append(d)
        try:
            me = re.search("GET|POST|HEAD",log).group(0)
        except:
            me = re.search("GET|POST|HEAD",log)
        method.append(me)
        co = log.find(" /")
        ss = log.find(" ",co+1)
        ur = log[co:ss].replace(" ","")
        url.append(ur)
        ss1 = log.find(" ",ss+1)
        ss2 = log.find(" ",ss1+1)
        sta = log[ss1:ss2].replace(" ","")
        status_code.append(sta)
        ss3 = log.find(" ",ss2)
        ss4 = log.find(" ",ss3+1)
        down = log[ss3:ss4].replace(" ","")
        download.append(down)
        start_ref = log.find(" ",ss4)
        end_ref = log.find(" ",start_ref+1)
        ref = log[start_ref:end_ref].replace('"','').replace(" ","")
        referrer.append(ref)
        ua = log[end_ref+1:].replace('"','')
        user_agent.append(ua)

    final_log = pd.DataFrame({
        "IP": ips,
        "date":date,
        "hour":hour,
        "diff":diff,
        "method":method,
        "url":url,
        "status_code":status_code,
        "download":download,
        "referrer":referrer,
        "user_agent":user_agent
    })
    filtered_log = final_log["user_agent"].str.contains("Googlebot", na=False) 
    final_log = final_log[filtered_log]
    try:
        final_log.reset_index(inplace=True)
    except:
        pass
    final_log = final_log[["IP","date","hour","diff","method","url","status_code","download","referrer","user_agent"]]
    final_log
    return final_log

logs = read_logs(r'PATH-TO-FILE', "EXTENSION") # Path: C:\dir\file.extension && Extension: "gzip" | "log"
logs.to_csv("logs.csv", sep="\t", encoding="utf-8")
