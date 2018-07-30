import requests
import sys
import random
from wikipedia import wikipedia
from datetime import datetime
import config
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
from collections import Counter

'''
    Need to pip install azure 
    Run python setup.py install

    Get Wikipedia Article Titles and process data calls from Article Titles
'''

#DATASET_MARKER=sys.argv[1]
AZURE_TABLE=sys.argv[1]
CATEGORY=sys.argv[2]
STARTTIME=sys.argv[3]
ENDTIME=sys.argv[4]
DATASET_MARKER=sys.argv[5]

#FILE_NAME = sys.argv[3]

def get_revisions(item):
    try:
        requests.packages.urllib3.disable_warnings()
        _revs = wikipedia.revisionsearch(item)
        return _revs
    except Exception as e:
        print( "Error: %s" % e )
        return list()

def get_biographies(title,start,end):
    try:
        requests.packages.urllib3.disable_warnings()
        _bios = wikipedia.categorysearchtimestamp(title,start,end)
        return _bios
    except:
        print( "Error getting bios" )
        return list() 

def create_task(dataset_marker,created,row_key,part_key,page_id,title,revs,url):
    task = {
        'PartitionKey': part_key, 
        'RowKey': row_key, 
        'PAGEID': page_id, 
        'CREATED': created,
        'TITLE': title, 
        'URL': url,
        'DATASET': dataset_marker
    }
    
    _rtmp = []
    for r in revs:
        _rtmp.append(r)

    if _rtmp:
        task.update({"REVISIONS": str(_rtmp)})

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
    _tmpb = get_biographies(CATEGORY,STARTTIME,ENDTIME)

    print(str(len(_tmpb)))

    if len(_tmpb) == 0:
        sys.exit()

    table_data = {}

    tableservice = table_service()    
    requests.packages.urllib3.disable_warnings()

    for r in _tmpb:
        try:
            print(r)
            p = wikipedia.page(title=None,pageid=str(r['pageid']))
            _revs = [r for r in get_revisions(str(p.pageid)) if "delet" in str(r)]
            #table_data.update({str(r['pageid']):{'PAGEID': str(p.pageid),'TOUCHED': str(p.touched),'URL': str(p.url),'TITLE': str(p.title)}})
            _task = create_task(str(DATASET_MARKER),str(r['timestamp']),str(p.pageid),str(random.randint(100000,99999999)),str(p.pageid),str(p.title),_revs,str(p.url))
            print (_task)
            tableservice.insert_entity(AZURE_TABLE, _task)
        except Exception as e:
            print("Error: %s" % e)
            continue

    #keys = [x for x in table_data.keys()]
    #values = [x for x in table_data.values()]

    #for index, t in enumerate(keys):
    #    _revs = [r for r in get_revisions(str(t)) if "delet" in str(r)]
    #    _task = create_task(str(DATASET_MARKER),str(values[index]['TOUCHED']),str(values[index]['PAGEID']),str(random.randint(100000,99999999)),str(values[index]['PAGEID']),str(values[index]['TITLE']),_revs,str(values[index]['URL']))
    #    print (_task)
    #    tableservice.insert_entity(AZURE_TABLE, _task)

if __name__ == '__main__':
    main()