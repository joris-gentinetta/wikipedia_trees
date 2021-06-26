import concurrent.futures
import pickle
import random
import time

import requests

start_points = ['The Beatles', 'Led Zeppelin', 'The Rolling Stones', 'Black Sabbath', 'Fluorine', 'Bromine', 'Chlorine',
                'Iodine'] #the root articles
depth = 5 #maximum depth
spread = 9 #spreading factor
timeout = 15 #time in seconds to wait for session requests
workers = 1000 #maximum number of threads
max_trials = 10 #maximum number of repeated session requests
number_of_extracts_at_a_time = 5 #the number of pages to request in one api call
S = requests.Session() #the session

URL = "https://en.wikipedia.org/w/api.php"

globalPARAMS = { #params for the api call to get the links in the article
    "action": "query",
    "format": "json",
    "prop": "links",
    "utf8": 1,
    "plnamespace": "0",
    "pllimit": "500",
}
globalPARAMS_extract = { #params for the api call to get the summary part of the article

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
# input: title of the article, string
# returns: the links of the article as a set of strings
def request(title):
    global URL
    global globalPARAMS
    global skipped_titles
    links = set()
    PARAMS = globalPARAMS.copy()
    PARAMS['titles'] = title #the title of the article to request the links from
    success = False
    for trial in range(max_trials): #try to get the links max_trial times
        try:
            R = S.get(url=URL, params=PARAMS, timeout=timeout)
            success = True
            break
        except:
            pass
    if not success: #if no success, add the title to the skipped titles and return an empty set
        skipped_titles += 1
        return links
    DATA = R.json()
    PAGES = DATA["query"]["pages"]
    for k, v in PAGES.items(): #add all the links to the 'links' set
        if 'links' in v:
            for l in v["links"]:
                links.add(l['title'])
    return links

#input: the number of elements to return, int and the set of links, string
#output: 'number_of_elements' elements from the links set
# or the links set if there are less links than 'number_of_elements'
def get_random(number_of_elements, links):
    for s in range(number_of_elements):
        try:
            new_links = random.sample(links, number_of_elements - s)
            return new_links
        except:
            pass
    return set()

#recursive function to get valid links
#input: rest: the number of links needed, trials: the maximum number of trials, links: the set of links
#output: none
def do_update(rest, trials, links):
    global title_log
    global all_titles
    global extracts
    if rest == 0: #exit criterium for the recursion
        return
    links = links.difference(all_titles) #the links that have not yet been visited
    new_links = get_random(rest, links) #get 'rest' number of links from 'links'
    number_of_sets = len(new_links) // number_of_extracts_at_a_time + 1 #the number of api calls to make
    for i in range(number_of_sets): #make the api calls
        request_extracts_iterator(new_links, i)
    all_titles.update(set(new_links)) #update the visited links
    for nlink in new_links: #if there is an extract associated with the link, and it is not empty,
                            # the link is added to the title log and the 'rest' nuber of links is decreased by one
        if nlink in extracts:
            if extracts[nlink] != '':
                rest -= 1
                title_log[layer + 1].add(nlink)
                pass
    do_update(rest, trials - 1, links) #if there are still missing links (rest!=0), request more.
    return

#starts the recursion, function to give to the thread workers
#input: title
#output: none
def request_iterator(title):
    global title_log
    links = request(title)
    do_update(rest=spread, trials=max_trials, links=links)
    return

# fills the extracts dictionary
# input: title of the article, string
# returns: none
def request_extracts(titles):
    global URL
    global globalPARAMS_extract
    global extracts
    PARAMS = globalPARAMS_extract.copy()
    PARAMS['titles'] = '|'.join(titles) #params for the api call
    success = False
    for trial in range(max_trials): #try to get the summaries 'max_trials' times
        try:
            R = S.get(url=URL, params=PARAMS, timeout=timeout)
            success = True
            break
        except:
            pass
    if not success: #if note successful, add the titles to the skipped summaries
        skipped_summaries.update(titles)
        return
    DATA = R.json()
    if 'query' in DATA:
        PAGES = DATA["query"]["pages"]
    else:
        return
    for k, v in PAGES.items():
        if 'extract' in v:
            extracts[v['title']] = v["extract"] #add the title: extract key to the extracts
    return

#starts the recursion, function to give to the thread workers
#input: title
#output: none
def request_extracts_iterator(new_links, i):
    request_extracts(new_links[number_of_extracts_at_a_time * i:number_of_extracts_at_a_time * (i + 1)])
    return

for start_point in start_points: #for every root article
    skipped_titles = 0
    skipped_summaries = set()
    title_log = [{start_point}] #add the root article to the 'title_log' and 'all_titles'
    all_titles = {start_point}
    extracts = {}
    request_extracts([start_point]) # get the summary of the root article
    for layer in range(depth):
        title_log.append(set())
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor: #add the titles and summaries of the layer
            executor.map(request_iterator, title_log[layer])
    title_log[0].remove(start_point) #remove the root article
    layerorganisation = [{k: extracts[k] for k in title_log[d]} for d in range(depth + 1)] #list of title: extract dictionaries at every  depth
    for i in range(1, len(layerorganisation)):
        layerorganisation[i].update(layerorganisation[i - 1]) #every layer contains the content of the upper layers
    layerorganisation[0] = {start_point: extracts[start_point]} #layer 0 consists of the root article
    root = start_point.replace(' ', '_')
    with open(root + '_depth_' + str(depth) + '_spread_' + str(spread), "wb") as f:
        pickle.dump(layerorganisation, f)
        #save the datasets (layerorganisation[d] contains all articles up to depth d, without the root article,
        #layerorganisation[0] contains the root article
