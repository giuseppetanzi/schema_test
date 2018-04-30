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
TARGET_COLLECTION = 'target3'
logPath="/Users/gtanzi/data2"


####
# Main start function
####
def main():
    do_update()

def do_update(*args):
    db = mongo_client.get_database(DB)
    clienti = db.get_collection(MAIN_COLLECTION)
    clienti_target = db.get_collection(TARGET_COLLECTION)
    clienti_target.drop()
    results = clienti.find({'CONTROLLO_INT':'F'})
    index = 0
    bulkOp = clienti_target.initialize_unordered_bulk_op()
    doc={}
    while(True):
        try:
            for doc in results:
                index += 1
                if (index % 10000) == 0:
                    print(index)
                    bulkOp.execute()
                    bulkOp = clienti_target.initialize_unordered_bulk_op()        
                chiavi = doc.get("CHIAVE_RICERCA_INTESTAZIONE")
                A_valori = []
                B_valori = []
                randomInt = random.randint(1,100000)
                if randomInt == 1:
                    printOnFile( str(chiavi) )
                for chiave in chiavi:
                    if chiave['VALORE'].startswith('A:'):
                        chiave['VALORE'] = chiave['VALORE'][2:]
                        A_valori.append(chiave)
                    elif chiave['VALORE'].startswith('B:'):
                        chiave['VALORE'] = chiave['VALORE'][2:]
                        B_valori.append(chiave)
                doc['CHIAVE_RICERCA_INTESTAZIONE_A'] = A_valori
                doc['CHIAVE_RICERCA_INTESTAZIONE_B'] = B_valori
                del doc['CHIAVE_RICERCA_INTESTAZIONE']
                bulkOp.insert(doc)
        
            bulkOp.execute()   
        except:
            print(doc)
            bulkOp.execute()
            bulkOp = clienti_target.initialize_unordered_bulk_op()
            continue
#        clienti_target.update_one({'parent_id':id}, {'\set':update}, upsert=True)
        


#        clienti_target.update_one({'parent_id':id}, {'\set':update}, upsert=True)
        
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


def printOnFile(string):
    outFile = open(logPath,"a")
    outFile.write(string+"\n")
    outFile.close()

COMMANDS = {
    'UPDATE':    do_update,
}


####
# Main
####
if __name__ == '__main__':
    main()


