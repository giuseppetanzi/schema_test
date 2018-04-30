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
MAIN_COLLECTION = 'mytest'
TARGET_COLLECTION = 'chiavi'
filePath="/Users/gtanzi/queries"



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
    clienti_target = db.get_collection(TARGET_COLLECTION)
    results = clienti.find()
    index = 0
    clienti_target.drop()
    bulkOp = clienti_target.initialize_unordered_bulk_op()
    for doc in results:
        index += 1
        if (index % 10000) == 0:
            print(index)
            bulkOp.execute()
            bulkOp = clienti_target.initialize_unordered_bulk_op()        
        A_names = doc.get("INTESTAZIONE_A")
        B_names = doc.get("INTESTAZIONE_B")
        id = doc.get("_id")
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
    outFile = open(filePath,"a")
    outFile.write(string+"\n")
    outFile.close()



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


