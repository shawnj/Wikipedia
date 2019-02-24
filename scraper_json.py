import requests
import urllib3
import sys
import re
import random
import json
from bs4 import BeautifulSoup
from wikipedia import wikipedia
import config
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity

'''
    Need to pip install azure 
    Run python setup.py install

    Get Wikipedia Article Titles and process data calls from Article Titles
'''

DATASET_MARKER=sys.argv[1]
AZURE_TABLE=sys.argv[2]
CAMPAIGN_NAME="GETArticles"

def get_articles():

    with open('articles.json', 'r') as f:
        _articles = json.load(f)

    return _articles

def is_deleted(item):
    try:
        if wikipedia.page(pageid=item):
            return False
    except:
        return True

def get_logdata(item, logtype):
    try:
        _logs = wikipedia.logsearch(item,logtype)
        return list(_logs['query']['logevents'])
    except:
        return list()

def get_templates(item):
    try:
        _templates = wikipedia.templatesearch(item)
        return _templates
    except:
        return list()

def get_revisions(item):
    try:
        _revs = wikipedia.revisionsearch(item, title=True)
        return _revs
    except: 
        return list()

def create_task(dataset_marker,camp_name,created,row_key,part_key,page_id,title,logs,revs,url):
    task = {
        'PartitionKey': part_key, 
        'RowKey': row_key, 
        'PAGEID': page_id, 
        'TOUCHED': created,
        'TITLE': title, 
        'CAMPAIGN': camp_name,
        'URL': url,
        'DATASET': dataset_marker
    }

    _ltmp = []
    for l in logs:
        _ltmp.append(l)

    if _ltmp:
        task.update({"LOGS": str(_ltmp)})
    
    _rtmp = []
    for r in revs:
        _rtmp.append(r)

    if _rtmp:
        task.update({"REVISIONS": str(_rtmp)})

    return task

def create_task_logs(dataset_marker,camp_name,created,row_key,part_key,page_id,title,url,logid,logpage,params,ltype,action,user,timestamp,comment):
    task = {
        'PartitionKey': part_key, 
        'RowKey': row_key, 
        'PAGEID': page_id, 
        'TOUCHED': created,
        'TITLE': title, 
        'CAMPAIGN': camp_name,
        'URL': url,
        'DATASET': dataset_marker,
        'LOGID': logid,
        'LOGPAGE': logpage,
        'PARAMS': params,
        'TYPE': ltype,
        'ACTION': action,
        'USER': user,
        'TIMESTAMP': timestamp,
        'COMMENT': comment
    }

    return task

def create_task_revs(dataset_marker,camp_name,created,row_key,part_key,page_id,title,url,revid,parentid,user,timestamp,comment):
    task = {
        'PartitionKey': part_key, 
        'RowKey': row_key, 
        'PAGEID': page_id, 
        'TOUCHED': created,
        'TITLE': title, 
        'CAMPAIGN': camp_name,
        'URL': url,
        'DATASET': dataset_marker,
        'REVID': revid,
        'PARENTID': parentid,
        'USER': user,
        'TIMESTAMP': timestamp,
        'COMMENT': comment
    }

    return task

def table_service():
    # Get storage connection string from config.py
    account_connection_string = config.STORAGE_CONNECTION_STRING
    # Split into key=value pairs removing empties, then split the pairs into a dict
    config_update = dict(s.split('=', 1) for s in account_connection_string.split(';') if s)
        
    # Authentication
    account_name = config_update.get('AccountName')
    account_key = config_update.get('AccountKey')

    # Instance of TableService
    __table_service__ = TableService(account_name=account_name, account_key=account_key)

    return __table_service__

def main():

    urllib3.disable_warnings()

    collection=get_articles()

    print ("Article Count:" + str(len(collection)))

    table_data = {}

    count = len(collection)

    for r in collection:
        if is_deleted(collection[r]['id']):
            table_data.update({r:{'PAGEID': "Deleted",'TOUCHED': "Deleted", 'URL': "Deleted"}})
        else:
            p = wikipedia.page(pageid=collection[r]['id'])
            table_data.update({r:{'PAGEID': str(p.pageid),'TOUCHED': str(p.touched), 'URL': str(p.url)}})
        count -= 1
        sys.stdout.write("\r%d%%" % count)
        sys.stdout.flush()

    tableservice = table_service()

    keys = [x for x in table_data.keys()]
    values = [x for x in table_data.values()]

    for index, t in enumerate(keys):
        print (t)

        logs = get_logdata(str(t), '')
        #revs = get_revisions(str(t))
        for l in logs:
            print (l)
            if 'commenthidden' not in l:
            #    task = create_task_revs(str(DATASET_MARKER),str(CAMPAIGN_NAME),str(values[index]['TOUCHED']),str(random.randint(100000,99999999)),str(random.randint(100000,99999999)),str(values[index]['PAGEID']),str(t),str(values[index]['URL']),str(l['revid']),str(l['parentid']),str(l['user']),str(l['timestamp']),str(l['comment']))
                task = create_task_logs(str(DATASET_MARKER),str(CAMPAIGN_NAME),str(values[index]['TOUCHED']),str(random.randint(100000,99999999)),str(random.randint(100000,99999999)),str(values[index]['PAGEID']),str(t),str(values[index]['URL']),str(l['logid']),str(l['logpage']),str(l['params']),str(l['type']),str(l['action']),str(l['user']),str(l['timestamp']),str(l['comment']))
                tableservice.insert_entity(AZURE_TABLE, task)

if __name__ == '__main__':
    main()
