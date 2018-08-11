#! /usr/bin/env python
'''
Created on Apr 6, 2018

@author: gtanzi
'''


import os
import sys
import getopt
import logging

from pymongo import MongoClient, ASCENDING

LOGFORMAT = "%(asctime)s %(levelname)s: %(message)s"
DATEFORMAT = "%Y-%m-%d %H:%M:%S"

MONGODB_URL = 'mongodb://localhost:27017'

DB = 'test'
MAIN_COLLECTION = 'source'
TARGET_COLLECTION = 'target'

####
# Main start function
####

def usage():
    print()

def main():
    global MONGODB_URL, MAIN_COLLECTION, TARGET_COLLECTION, mongo_client

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], "hv", ["help", "verbose", "host=",
                                                            "source=", "target=", "db=", "switch="])
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(1)
    switch = 'B'
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif o in ("--host",):
            MONGODB_URL = a
        elif o in ("--db",):
            DB = a
        elif o in ("--source",):
            MAIN_COLLECTION = a
        elif o in ("--target",):
            TARGET_COLLECTION = a
        elif o in ("--switch",):
            switch = a
        elif o in ("-v", "--verbose"):
            logging.basicConfig(level=logging.DEBUG, format=LOGFORMAT, datefmt=DATEFORMAT)
        else:
            raise Exception("unhandled option %s" % o)

    if not MONGODB_URL.startswith('mongodb://'):
        sys.stderr.write('host must start with "mongodb://"\n')
        sys.exit(1)

    if len(args) > 0:
        sys.stderr.write("too many arguments\n")
        sys.exit(1)

    if not logging.root.handlers[:]:
        logging.basicConfig(level=logging.INFO, format=LOGFORMAT, datefmt=DATEFORMAT)

    if switch not in ["B", "A"]:
        sys.stderr.write("switch must be A, B\n")
        sys.exit(1)

    mongo_client = MongoClient(MONGODB_URL)
    do_update(switch)


def do_update(switch):
    db = mongo_client.get_database(DB)
    clienti = db.get_collection(MAIN_COLLECTION)
    clienti_target = db.get_collection(TARGET_COLLECTION)
    clienti_target.drop()
    results = clienti.find()
    index = 0
    bulk_op = clienti_target.initialize_unordered_bulk_op()
    doc = {}

    if switch == "B":
        clienti_target.create_index([("CHIAVE_B", ASCENDING), ("CHIAVI_A", ASCENDING)])
    else:
        clienti_target.create_index([("CHIAVE_A", ASCENDING), ("CHIAVI_B", ASCENDING)])

    for doc in results:
        try:
            index += 1
            if (index % 10000) == 0:
                print ("RUN " + str(index))
                bulk_op.execute()
                bulk_op = clienti_target.initialize_unordered_bulk_op()
            chiavi = doc.get("CHIAVE_RICERCA_INTESTAZIONE")
            id = doc.get("_id")
            del doc['CHIAVE_RICERCA_INTESTAZIONE']
            del doc['_id']

            fname_set = set()
            lname_set = set()

            for chiave in chiavi:
                if chiave['VALORE'].startswith('A:'):
                    lname_set.add(chiave['VALORE'][2:])
                elif chiave['VALORE'].startswith('B:'):
                    fname_set.add(chiave['VALORE'][2:])

            if switch == "B":
                for name in fname_set:
                    doc["CHIAVE_B"] = name
                    doc["id_padre"] = id
                    tmp_array = fname_set.copy()

                    tmp_array.remove(name)
                    doc["CHIAVI_B"] = list(tmp_array)

                    doc["CHIAVI_A"] = list(lname_set)
                    bulk_op.insert(doc.copy())
            else:
                for name in lname_set:
                    doc["CHIAVE_A"] = name
                    doc["id_padre"] = id
                    tmp_array = lname_set.copy()

                    tmp_array.remove(name)
                    doc["CHIAVI_A"] = list(tmp_array)

                    doc["CHIAVI_B"] = list(fname_set)
                    bulk_op.insert(doc.copy())


        except Exception as e:
            print "ERROR"
            print e
            print(doc)
            bulk_op.execute()
            bulk_op = clienti_target.initialize_unordered_bulk_op()
            continue

    bulk_op.execute()



####
# Main
####
if __name__ == '__main__':
    main()
