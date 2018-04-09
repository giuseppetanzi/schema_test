'''
Created on Apr 6, 2018

@author: gtanzi
'''


import os
import sys
from itertools import combinations

from pymongo import MongoClient


MONGODB_URL = 'mongodb://localhost:27017'

mongo_client = MongoClient(MONGODB_URL)
DB = 'test'
MAIN_COLLECTION = 'mytest'
TARGET_COLLECTION = 'chiavi'



####
# Main start function
####
def main():
    print(' ')

    if len(sys.argv) < 2:
        print('Error: No command argument provided')
        print_usage()
    else:
        command = sys.argv[1].strip().upper()
        COMMANDS.get(command, print_commands_error)(command)


def do_update(*args):
    db = mongo_client.get_database(DB)
    clienti = db.get_collection(MAIN_COLLECTION)
    clienti_search = db.get_collection(TARGET_COLLECTION)
    results = clienti.find()
    index = 0
    clienti_search.drop()
    bulkOp = clienti_search.initialize_unordered_bulk_op()
    for doc in results:
        index += 1
        if (index % 1000) == 0:
            print(index)
            bulkOp.execute()
            bulkOp = clienti_search.initialize_unordered_bulk_op()        
        A_names = doc.get("INTESTAZIONE_A")
        B_names = doc.get("INTESTAZIONE_B")
        id = doc.get("_id")
        combANames = combine(A_names)
        combBNames = combine(B_names)
        mergedList = merge(combANames,combBNames)
        update = {}
        update['searchKey'] = mergedList
        update['parent_id'] = id       
        bulkOp.insert(update)

    bulkOp.execute()   
#        clienti_search.update_one({'parent_id':id}, {'$set':update}, upsert=True)
        
####
# Print out how to use this script
####
def print_usage():
    print('\nUsage:')
    
    
####
# Print script start-up error reason
####
def print_commands_error(command):
    print('Error: Illegal command argument provided: "%s"' % command)
    print_usage()


def merge(a_names,b_names):
    finalList = []
    for a_name in a_names:
        finalList.append("A:" + a_name)
        resultBase = "A:" + a_name + ",B:"
        for b_name in b_names:
            result = resultBase + b_name
            finalList.append(result)
    return finalList

def combine(names):
    lns = names.split()
    lns.sort()
    comb = []
    for i in range(lns.__len__()):
        comb += combinations(lns, i+1)
    comb_list = [ list(t) for t in comb ]
    comb_strings = []
    for elem in comb_list:
        result = ""
        for elem2 in elem:
            result += (elem2 + ",")
         
        result = result[:-1]
        comb_strings.append(result)
    return comb_strings

# Constants




COMMANDS = {
    'UPDATE':    do_update,
}


####
# Main
####
if __name__ == '__main__':
    main()


