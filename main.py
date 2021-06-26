import concurrent.futures
import pickle
import random
import time

import requests

start_points = ['The Beatles', 'Led Zeppelin', 'The Rolling Stones', 'Black Sabbath', 'Fluorine', 'Bromine', 'Chlorine',
                'Iodine']
# 'The Beatles', 'Led Zeppelin', 'The Rolling Stones', 'Black Sabbath',
depth = 5
spread = 20
timeout = 15
workers = 1000
max_trials = 10
number_of_extracts_at_a_time = 5
S = requests.Session()

URL = "https://en.wikipedia.org/w/api.php"

globalPARAMS = {
    "action": "query",
    "format": "json",
    "prop": "links",
    "utf8": 1,
    "plnamespace": "0",
    "pllimit": "500",
}
globalPARAMS_extract = {

    "action": "query",
    "format": "json",
    "prop": "extracts",
    "indexpageids": 1,
    "continue": "",
    "utf8": 1,
    "ascii": 1,
    "exlimit": "20",
    "exintro": 1,
    "explaintext": 1,

}


def request(title):
    global URL
    global globalPARAMS
    global skipped_titles
    links = set()
    PARAMS = globalPARAMS.copy()
    PARAMS['titles'] = title
    success = False
    for trial in range(max_trials):
        try:
            R = S.get(url=URL, params=PARAMS, timeout=timeout)
            success = True
            break
        except:
            pass
    if not success:
        skipped_titles += 1
        return links
    DATA = R.json()
    PAGES = DATA["query"]["pages"]
    for k, v in PAGES.items():
        if 'links' in v:
            for l in v["links"]:
                links.add(l['title'])
    return links


def get_random(number_of_elements, links):
    for s in range(number_of_elements):
        try:
            new_links = random.sample(links, number_of_elements - s)
            return new_links
        except:
            pass
    return set()


def do_update(rest, trials, links):
    global title_log
    global all_titles
    global extracts
    if rest == 0:
        return
    links = links.difference(all_titles)

    new_links = get_random(rest, links)
    number_of_sets = len(new_links) // number_of_extracts_at_a_time + 1
    for i in range(number_of_sets):
        request_extracts_iterator(new_links, i)
    all_titles.update(set(new_links))
    for nlink in new_links:
        if nlink in extracts:
            if extracts[nlink] != '':
                rest -= 1
                title_log[layer + 1].add(nlink)
                pass
    do_update(rest, trials - 1, links)
    return


def request_iterator(title):
    global title_log
    links = request(title)
    do_update(rest=spread, trials=max_trials, links=links)
    return


def request_extracts(titles):
    global URL
    global globalPARAMS_extract
    global extracts
    PARAMS = globalPARAMS_extract.copy()
    PARAMS['titles'] = '|'.join(titles)
    success = False
    for trial in range(max_trials):
        try:
            R = S.get(url=URL, params=PARAMS, timeout=timeout)
            success = True
            break
        except:
            pass
    if not success:
        skipped_summaries.update(titles)
        return
    DATA = R.json()
    if 'query' in DATA:
        PAGES = DATA["query"]["pages"]
    else:
        return
    for k, v in PAGES.items():
        if 'extract' in v:
            extracts[v['title']] = v["extract"]
    return


def request_extracts_iterator(new_links, i):
    request_extracts(new_links[number_of_extracts_at_a_time * i:number_of_extracts_at_a_time * (i + 1)])
    return

for start_point in start_points:
    #links = set()
    skipped_titles = 0
    skipped_summaries = set()
    title_log = [{start_point}]
    all_titles = {start_point}
    extracts = {}
    request_extracts([start_point])
    start = time.time()
    for layer in range(depth):
        title_log.append(set())
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(request_iterator, title_log[layer])
            #executor.submit(request_iterator, 'The Beatles')
    end = time.time()

    print('time to get all titles: ', (end - start) / 60)
    start3 = time.time()
    title_log[0].remove(start_point)
    layerorganisation = [{k: extracts[k] for k in title_log[d]} for d in range(depth + 1)]
    for i in range(1, len(layerorganisation)):
        layerorganisation[i].update(layerorganisation[i - 1])
    layerorganisation[0] = {start_point: extracts[start_point]}
    end3 = time.time()
    print('remodeling time: ', (end3 - start3) / 60)
    root = start_point.replace(' ', '_')
    with open(root + '_depth_' + str(depth) + '_spread_' + str(spread), "wb") as f:
        pickle.dump(layerorganisation, f)
    print(start_point, len(layerorganisation[-1]))
