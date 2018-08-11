'''
Created on Apr 6, 2018

@author: gtanzi
'''


import os
import sys
import random

from itertools import combinations

from pymongo import MongoClient


MONGODB_URL = 'mongodb://localhost:27017'

mongo_client = MongoClient(MONGODB_URL)
DB = 'test'
MAIN_COLLECTION = 'mynewtest'
TARGET_COLLECTION = 'target4'
logPath="/Users/gtanzi/queries"
maxWords = 4


####
# Main start function
####
def main():
    do_update()


def do_update():
    db = mongo_client.get_database(DB)
    clienti = db.get_collection(MAIN_COLLECTION)
    clienti_target = db.get_collection(TARGET_COLLECTION)
    clienti_target.drop()
    results = clienti.find({'ACCOUNT_TYPE':'F'})
    index = 0
    bulkOp = clienti_target.initialize_unordered_bulk_op()
    for doc in results:
        index += 1
        if (index % 10000) == 0:
            print(index)
            bulkOp.execute()
            bulkOp = clienti_target.initialize_unordered_bulk_op()        
        chiavi = doc.get("SEARCH_HEADER")
        A_names = []
        B_names = []
        for chiave in chiavi:
            if chiave['VALUE'].startswith('A:'):
                A_names.append(chiave['VALUE'][2:])
            elif chiave['VALUE'].startswith('B:'):
                B_names.append(chiave['VALUE'][2:])
                
        combANames = combine(A_names)
        combBNames = combine(B_names)
        mergedList = merge(combANames,combBNames)
        doc['searchKey'] = mergedList
        bulkOp.insert(doc)

    bulkOp.execute()   
#        clienti_target.update_one({'parent_id':id}, {'$set':update}, upsert=True)
        
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
            randomInt = random.randint(1,100000)
            if randomInt == 1:
                printOnFile( result )
    return finalList


def printOnFile(string):
    outFile = open(logPath,"a")
    outFile.write(string+"\n")
    outFile.close()



def combine(names):
    names.sort()
    comb = []
    for i in range(min(len(names),maxWords)):
        comb += combinations(names, i+1)
    comb_list = [ list(t) for t in comb ]
    comb_strings = []
    for elem in comb_list:
        result = ""
        for elem2 in elem:
            result += (elem2 + ",")
         
        result = result[:-1]
        comb_strings.append(result)
    return comb_strings


def old_combine(names):
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
    do_update()


