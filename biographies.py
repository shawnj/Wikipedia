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
#FILE_NAME = sys.argv[3]

def get_revisions(item, title):
    try:
        requests.packages.urllib3.disable_warnings()
        _revs = wikipedia.revisionsearch(item, title=title)
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
        print( "Error: %s" % e )
        return list() 

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

if __name__ == '__main__':
    main()