from socket import error as SocketError
from urllib.request import urlopen
from urllib.error import HTTPError
from queue import Queue
from bs4 import BeautifulSoup
import os
import csv
import threading


# function to create a working thread
def worker_thread(q, all_crop):
    while True:
        # q.get() returns an id from queue
        url_id = q.get()
        url = "http://ecocrop.fao.org/ecocrop/srv/en/cropView?id=" + url_id
        parse_html(url, all_crop)
        q.task_done()


# function for each worker should do(thread's work)
def parse_html(url, all_crop):
    try:
        html = urlopen(url)
    except HTTPError:
        # this website is dead
        return
    except SocketError:
        # this is for ConnectionResetError: [Errno 104] Connection reset by peer
        return
    bsObj = BeautifulSoup(html, "html.parser")

    col_headers = [th.getText() for th in
                   bsObj.findAll('tr', limit=2)[0].findAll('th')]

    imgCheck = bsObj.find('td')
    if imgCheck.find('img'):
        data_rows = bsObj.findAll('td')[2:]
    else:
        data_rows = bsObj.findAll('td')[1:]

    plant_data = [td.getText() for td in data_rows]

    plant_authority = plant_data[0]
    plant_family = plant_data[1]
    plant_synonym = plant_data[2]
    plant_com_names = plant_data[3]
    plant_editor = plant_data[4]
    plant_code = plant_data[5]
    plant_notes = plant_data[6]
    plant_sources = plant_data[7]

    # print for testing
    print("\n" + url.split("http://ecocrop.fao.org/ecocrop/srv/en/cropView?id=", 1)[1])
    print("Authority : " + plant_authority)
    print("Family : " + plant_family)
    print("Synonym : " + plant_synonym)
    print("Editor : " + plant_editor)

    # tokenizing plant_notes and passing variables into token_dict
    token_dict = unstruct(plant_notes,all_crop,plant_com_names)

    # appending the crop into dictionary
    crops = {}
    for i in range(len(col_headers)):
        if i is not 3:
            crops[col_headers[i]] = plant_data[i]

    # merging two dictionaries
    dcrops = {**crops, **token_dict}
    all_crop.append(dcrops)
    html.close()

    # saving the merged dictionary into CSV file type
    with open(url.split("http://ecocrop.fao.org/ecocrop/srv/en/cropView?id=", 1)[1]+ '.csv', 'w', encoding='utf-8') as f:
        if all_crop:
            w = csv.DictWriter(f, all_crop[0].keys())
            w.writeheader()
            w.writerows(all_crop)
            all_crop.clear()


# function to tokenize the plant_notes
def unstruct(data,all_crop,cName):

    data_string = data

    # initialize
    p_brief = ""
    p_uses = ""
    p_commNames = ""
    p_growP = ""
    p_fur = ""
    p_inspire = ""
    p_sources = ""
    p_killT = ""

    # algorithm to run through every possible keyword
    # pattern 1
    if "BRIEF DESCRIPTION" in data_string:
        pattern1 = data_string.split("BRIEF DESCRIPTION",1)
        if "USES" in pattern1[1] and "GROWING PERIOD" in pattern1[1] :
            pattern1 = pattern1[1].split("USES", 1)
            p_brief = pattern1[0].strip()
            print("Brief Description : " + p_brief)
            pattern1 = pattern1[1].split("GROWING PERIOD",1)
            p_uses = pattern1[0].strip()
            print("Uses : "+p_uses)
        elif "USES" not in pattern1[1] and "GROWING PERIOD" in pattern1[1]:
            pattern1 = pattern1[1].split("GROWING PERIOD", 1)
            p_brief = pattern1[0].strip()
            print("Brief Description : " + p_brief)

        if "COMMON NAMES" in pattern1[1]:
            pattern1 = pattern1[1].split("COMMON NAMES",1)
            p_growP = pattern1[0].strip()
            print("Growing Period : "+p_growP)

        if "FURTHER INF" in pattern1[1]:
            pattern1 = pattern1[1].split("FURTHER INF",1)
            p_commNames = pattern1[0].strip()
            p_fur = pattern1[1].strip()
            print("Common Names : "+p_commNames)
            print("Further Info : "+p_fur)

    # pattern 2
    elif "BRIEF DESCRIPTION" not in data_string and "DESCRIPTION" in data_string:
        pattern2 = data_string.split("DESCRIPTION", 1)

        if "USES" in pattern2[1] and "GROWING PERIOD" in pattern2[1]and "KILLING T" not in pattern2[1]:
            pattern2 = pattern2[1].split("USES",1)
            p_brief = pattern2[0].strip()
            print("Brief Description : "+p_brief)
            pattern2 = pattern2[1].split("GROWING PERIOD",1)
            p_uses = pattern2[0].strip()
            print("Uses : "+p_uses)
        elif "USE:" in pattern2[1] and "GROWING PERIOD" in pattern2[1]and "KILLING T" not in pattern2[1]:
            pattern2 = pattern2[1].split("GROWING PERIOD", 1)
            p_brief = pattern2[0].strip()
            print("Brief Description : "+p_brief)
            pattern2 = pattern2[1].split("USE:", 1)
            p_growP = pattern2[0].strip()
            print("Growing Period : "+p_growP)
        elif "USES" not in pattern2[1] and "GROWING PERIOD" in pattern2[1]:
            pattern2 = pattern2[1].split("GROWING PERIOD", 1)
            p_brief = pattern2[0].strip()
            print("Brief Description : " + p_brief)
        elif "USE:" in pattern2[1] and "GROWING PERIOD" in pattern2[1] and "KILLING T" in pattern2[1]:
            pattern2 = pattern2[1].split("KILLING T", 1)
            p_brief = pattern2[0].strip()
            print("Brief Description : " + p_brief)
            pattern2 = pattern2[1].split("GROWING PERIOD", 1)
            p_killT = pattern2[0].strip()
            print("Killing Temp : " + p_killT)
            pattern2 = pattern2[1].split("USE:", 1)
            p_growP = pattern2[0].strip()
            print("Growing Period : " + p_growP)

        if "COMMON NAMES" in pattern2[0]:
            pattern2 = pattern2[0].split("COMMON NAMES",1)
            p_growP = pattern2[0].strip()
            print("Growing Period : "+p_growP)
        elif "COMMON NAMES" not in pattern2:
            print("Common Names : ")
        elif "COMMON NAMES" in pattern2[1] and "USES" not in pattern2[1] or "USE:" not in pattern2[1]:
            pattern2 = pattern2[1].split("COMMON NAMES", 1)
            p_uses = pattern2[0].strip()
            print("Growing Period : " + p_uses)
        elif "COMMON NAMES" in pattern2[1] and "USES" in pattern2[1] or "USE:" in pattern2[1]:
            pattern2 = pattern2[1].split("COMMON NAMES",1)
            p_uses = pattern2[0].strip()
            print("Uses : "+p_uses)

        if "FURTHER INF" not in data_string:
            print("fu: ")
        elif "FURTHER INF" in pattern2[0]:
            pattern2 = pattern2[0].split("FURTHER INF",1)
            p_commNames = pattern2[0].strip()
            p_fur = pattern2[1].strip()
            print("Common Names : "+p_commNames)
            print("Further Info : "+p_fur)
        elif "FURTHER INF" in pattern2[1]:
            p_commNames = pattern2[1].strip()
            p_fur = pattern2[1].strip()
            print("Common Names : "+p_commNames)
            print("Further Info : "+p_fur)
        elif "FURTHER INF" not in pattern2[0] and "FURTHER INF" not in pattern2[1]:
            print("fu: ") # empty further info

    # pattern 3
    elif "BRIEF DESCRIPTION" not in data_string and "DESCRIPTION" not in data_string and "SOURCES" in data_string:
        pattern3 = data_string.split("SOURCES",1)
        if "INSPIRE" in pattern3[1] and "KILLING T" in pattern3[1] and "GROWING PERIOD" in pattern3[1]:
            pattern3 = pattern3[1].split("INSPIRE")
            p_sources = pattern3[0].strip()
            print("Sources: " + p_sources)
            if "KILLING TEMP" in pattern3[1]:
                pattern3 = pattern3[1].split("KILLING TEMP")
            elif "KILLING TEMP" not in pattern3[1] and "KILLING T" in pattern3[1]:
                pattern3 = pattern3[1].split("KILLING T")
            p_inspire = pattern3[0].strip()
            print("ins: "+p_inspire)
            pattern3 = pattern3[1].split("GROWING PERIOD")
            p_killT = pattern3[0].strip()
            print("Killing Temp : " + p_killT)
        elif "INSPIRE" not in pattern3[1] and "KILLING T" in pattern3[1] and "GROWING PERIOD" in pattern3[1]:
            if "KILLING TEMP" in pattern3[1]:
                pattern3 = pattern3[1].split("KILLING TEMP")
            elif "KILLING TEMP" not in pattern3[1] and "KILLING T" in pattern3[1]:
                pattern3 = pattern3[1].split("KILLING T")
            p_sources = pattern3[0].strip()
            print("Sources : " + p_sources)
            pattern3 = pattern3[1].split("GROWING PERIOD")
            p_killT = pattern3[0].strip()
            print("Killing Temp : "+p_killT)
        elif "INSPIRE" not in pattern3[1] and "KILLING T" not in pattern3[1] and "GROWING PERIOD" in pattern3[1]:
            pattern3 = pattern3[1].split("GROWING PERIOD",1)
            p_sources = pattern3[0].strip()
            print("Sources : "+p_sources)

        if "COMMON NAMES" in pattern3[1]:
            pattern3 = pattern3[1].split("COMMON NAMES",1)
            p_growP = pattern3[0].strip()
            print("Growing Period : "+p_growP)

        if "FURTHER INF" in pattern3[1]:
            pattern3 = pattern3[1].split("FURTHER INF",1)
            p_commNames = pattern3[0].strip()
            p_fur = pattern3[1].strip()
            print("Common Names : "+p_commNames)
            print("Further Info : "+p_fur)

    # pattern 3 Variation
    elif "BRIEF DESCRIPTION" not in data_string and "DESCRIPTION" not in data_string and "SOURCES" not in data_string and "KILLING T" in data_string:
        pattern4 = data_string.split("KILLING T",1)
        if "USES" in pattern4[1] and "GROWING PERIOD" in pattern4[1]:
            pattern4 = pattern4[1].split("USES",1)
            p_brief = pattern4[0].strip()
            print("Killing Temp : "+p_brief)
            pattern4 = pattern4[1].split("GROWING PERIOD",1)
            p_uses = pattern4[0].strip()
            print("Uses : "+p_uses)
        elif "USE:" in pattern4[1] and "GROWING PERIOD" in pattern4[1]:
            pattern4 = pattern4[1].split("USE:",1)
            p_brief = pattern4[0].strip()
            print("Killing Temp : "+p_brief)
            pattern4 = pattern4[1].split("GROWING PERIOD",1)
            p_uses = pattern4[0].strip()
            print("Uses : "+p_uses)
        elif "USES" not in pattern4[1] and "GROWING PERIOD" in pattern4[1]:
            pattern4 = pattern4[1].split("GROWING PERIOD", 1)
            p_brief = pattern4[0].strip()
            print("Killing Temp : " + p_brief)

        if "COMMON NAMES" in pattern4[1]:
            pattern4 = pattern4[1].split("COMMON NAMES",1)
            p_growP = pattern4[0].strip()
            print("Growing Period : "+p_growP)

        if "FURTHER INF" in pattern4[1]:
            pattern4 = pattern4[1].split("FURTHER INF",1)
            p_commNames = pattern4[0].strip()
            p_fur = pattern4[1].strip()
            print("Common Names : "+p_commNames)
            print("Further Info : "+p_fur)

    # pattern 1 Variation (without BRIEF DESCRIPTION and without SOURCES, and without KILLING T)
    elif "BRIEF DESCRIPTION" not in data_string and "DESCRIPTION" not in data_string and "SOURCES" not in data_string and "KILLING T" not in data_string:
        if "GROWING PERIOD" in data_string:
            pattern5 = data_string.split("GROWING PERIOD", 1)
            if "COMMON NAMES" in pattern5[1]:
                pattern5 = pattern5[1].split("COMMON NAMES",1)
                p_growP = pattern5[0].strip()
                print("Growing Period : "+p_growP)
            if "FURTHER INF" in pattern5[1]:
                pattern5 = pattern5[1].split("FURTHER INF",1)
                p_commNames = pattern5[0].strip()
                p_fur = pattern5[1].strip()
                print("Common Names : "+p_commNames)
                print("Further Info : "+p_fur)

    # storing the tokenized variables into dictionary
    c_name = sorted(list(set(cName.split(",")+p_commNames.split(","))))
    p_header = ["Brief Description", "Uses", "Killing Temp", "Growing Period", "Common Names", "Further Info", "Inspire", "Sources"]
    p_token = [p_brief, p_uses, p_killT, p_growP, c_name, p_fur, p_inspire, p_sources]
    
    token_dict = {}
    for i in range(len(p_header)):
        token_dict[p_header[i]] = p_token[i]

    return token_dict


# main function
def main():
    # getting crop_ID from local files
    lst = os.listdir('corp/')
    lst.sort(key=int)
    all_crop = []

    # python's thread safe queue
    # Here, load all the ids into the queue
    q = Queue()
    for ids in lst:
        q.put(ids)

    # 235 threads, with 2350 files, 100 data samples queue on each thread
    for i in range(235):
        t = threading.Thread(target=worker_thread, args=[q, all_crop])
        t.daemon = True
        t.start()

    print("Waiting on threads to finish\n")
    q.join()
    print("All threads finished")

# always good idea to add this guard, i.e. this script only runs if python interpreter is used on this file
if __name__ == '__main__':
    main()
