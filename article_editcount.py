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

DATASET_MARKER=sys.argv[1]
AZURE_TABLE=sys.argv[2]
FILE_NAME = sys.argv[3]

def get_revisions(item, title):
    try:
        requests.packages.urllib3.disable_warnings()
        _revs = wikipedia.revisionsearch(item, title=title)
        return _revs
    except Exception as e:
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

    if FILE_NAME:
        collection = [x.replace('\n','') for x in open(FILE_NAME).readlines()]
    else:
        sys.exit()

    print ("Article Count:" + str(len(collection)))

    table_data = {}

    count = len(collection)

    for r in collection[901:1100]:
        print(r)
        try:
            requests.packages.urllib3.disable_warnings()
            p = wikipedia.page(r)
            #p = wikipedia.page(r['title'])
            table_data.update({r:{'PAGEID': str(p.pageid),'TITLE': str(p.title),'TOUCHED': str(p.touched), 'URL': str(p.url)}})
        except Exception as e:
            print("Error: %s" % e)
            continue


    tableservice = table_service()

    keys = [x for x in table_data.keys()]
    values = [x for x in table_data.values()]

    for index, t in enumerate(keys):
        print(str(t))
        revs = [r for r in get_revisions(str(t), True) if r.get('comment')]

        print(len(revs))

        users = [x['user'] for x in revs if x.get('user')]

        cu = Counter(users).most_common(25)

        #print(cu)

        for user in cu:
            #for each RevUser we get that data and add it to a separate table in azure
            u = wikipedia.usersearch(user[0])

            _user_info = {}

            if 'invalid' not in u['query']['users'][0] and u['query']['users'][0].get('userid'):
                _user_info.update({
                    'PartitionKey': str(random.randint(100000,99999999)),
                    'RowKey': str(random.randint(100000,99999999)),
                    'USERID': str(u['query']['users'][0]['userid']),
                    'NAME': str(u['query']['users'][0]['name']),
                    'TOTALUSEREDITCOUNT': str(u['query']['users'][0]['editcount']),
                    'REGISTRATION': str(u['query']['users'][0]['registration']),
                    'GROUPS': str(u['query']['users'][0]['groups']),
                    'GENDER': str(u['query']['users'][0]['gender']),
                    'ARTICLETITLE': str(t),
                    'ARTICLEPAGEID': str(values[index]['PAGEID']),
                    'ARTICLEEDITCOUNT': user[1]
                })
            else:
                _user_info.update({
                    'PartitionKey': str(random.randint(100000,99999999)),
                    'RowKey': str(random.randint(100000,99999999)),
                    'USERID': 'invalid',
                    'NAME': str(u['query']['users'][0]['name']),
                    'TOTALUSEREDITCOUNT': 'None',
                    'REGISTRATION': 'None',
                    'GROUPS': 'None',
                    'GENDER': 'None',
                    'ARTICLETITLE': str(t),
                    'ARTICLEPAGEID': str(values[index]['PAGEID']),
                    'ARTICLEEDITCOUNT': user[1]
                })
            print(_user_info)
            tableservice.insert_or_replace_entity('UserEditData', _user_info)

if __name__ == '__main__':
    main()


