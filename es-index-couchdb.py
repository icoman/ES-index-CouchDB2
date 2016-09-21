"""
    Update ES index with values from CouchDB 2.x.x

"""

import ConfigParser

import couchdb
import time, sys, os

from elasticsearch import helpers
from elasticsearch import Elasticsearch




def get_seq_number(seq):
    ret = 0
    try:
        ret = int(seq.split('-')[0])
    except:
        pass
    return ret

def main():
    """
        Main
    """
    #default values
    dbname = 'db1'
    type_name = dbname
    index_name = dbname

    bulk_size = 1000
    CONFIG_FILE = 'default.ini'

    if len(sys.argv) > 1:
        try:
            CONFIG_FILE = sys.argv[1]
        except Exception, ex:
            print ex

    # assume the ini file is in the same directory as the script
    path = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), CONFIG_FILE)
    #parser = ConfigParser.ConfigParser()
    parser = ConfigParser.SafeConfigParser({'dbname':'db1'})
    parser.read(path)
    # the resourceFile is settable via the ini file
    type_name = parser.get('ES', 'type_name')
    index_name = parser.get('ES', 'index_name')
    dbname = parser.get('CouchDB', 'dbname')
    dbindex = parser.get('CouchDB', 'dbindex')
    index_doc_seq = parser.get('CouchDB', 'index_doc_seq')
    bulk_size = int(parser.get('CouchDB','bulk_size'))
    delay = float(parser.get('app','delay'))
    verbose = parser.get('app','verbose')
    


    if verbose == "True":
        print "Indexing couchdb database:",dbname
        print "ES Index:",index_name
        print "Bulk size:",bulk_size

    couch = couchdb.Server() # Assuming localhost:5984
    # If your CouchDB server is running elsewhere, set it up like this:
    #couch = couchdb.Server('http://example.com:5984/')


    # create index database, ignore if already exists
    try: couch.create(dbindex)
    except: pass

    # create couchdb database, ignore if already exists
    try: couch.create(dbname)
    except: pass


    es = Elasticsearch(timeout=60)
    db_couch = couch[dbname]
    dbix = couch['index']   #a couchdb database which keep last sequence processed

    flag_start = True
    while True:
            t1 = time.time()
            try: indexdoc = dbix[index_doc_seq]
            except Exception, ex:
                #print ex
                #create CouchDB document with last seq processed into index database
                nume = "Index: %s, db: %s" % (index_name, dbname)
                print "Create:",nume
                #update couchdb index database to start with seq 0
                indexdoc = {'seq':0, 'nume':nume}
                dbix[index_doc_seq] = indexdoc

            #1 - read last sequence
            last_seq_processed = indexdoc['seq']
            computed_last_seq_processed = last_seq_processed

            #2 - bulk read
            d = db_couch.changes(since=last_seq_processed, limit=bulk_size)
            last_seq_reported, L = d['last_seq'], d['results']
            LL = [] #list with ES documents
            L_id = [] #list with id of documents
            for i in L:
                id = i['id']
                if get_seq_number(computed_last_seq_processed) < get_seq_number(i['seq']):
                    computed_last_seq_processed = i['seq']
                if not id.startswith("_design/"):
                    L_id.append(id)
                else:
                    #skip design documents
                    print "Skip",id
            rows = db_couch.view('_all_docs', keys=L_id, include_docs=True)
            docs = []
            for i in rows:
                if i.doc:
                    docs.append(i.doc)
                else:
                    print "delete",i['id']
                    try: es.delete(index_name , type_name, i['id'])
                    except: pass #ignore error
            for i in docs:
                es_doc = {'_index': index_name, '_type': type_name} # _index and _type
                for x in i.items():
                    es_doc[x[0]] = x[1]
                LL.append(es_doc)

            if not computed_last_seq_processed:
                print "CouchDB empty. Nothing to index."
                time.sleep(delay)
                continue

            #3 - bulk update ES
            helpers.bulk(es, LL)
            t2 = time.time()
            time_delta = t2-t1

            last_seq = db_couch.info()['update_seq']
            total_seq = get_seq_number(last_seq)
            pos_computed_last_seq = get_seq_number(computed_last_seq_processed)

            if last_seq_processed != computed_last_seq_processed:
                #update position
                indexdoc['seq'] = computed_last_seq_processed
                dbix[index_doc_seq] = indexdoc
                print "Index seq %d/%d" % (pos_computed_last_seq, total_seq),
                eps = 1e-6
                if time_delta < eps:
                    time_delta = eps
                rate = bulk_size / time_delta #values per seconds
                left_seconds = float(total_seq - pos_computed_last_seq) / rate
                hours = int(left_seconds / 60.0 / 60.0)
                minutes = int(left_seconds/60.0 - hours*60)
                seconds = int(left_seconds - minutes*60 - hours*60*60)
                if total_seq > 0:
                    print "- %.3f%% (%.2fs)  %.2f val/sec - ETA: %d:%d:%d" % (pos_computed_last_seq*100.0/total_seq, time_delta, rate, hours, minutes, seconds)
            else:
                #waiting for more data to process
                print "Idle. Db=%s, Last seq=%d" % (dbname, total_seq)
                time.sleep(delay)


if __name__ == "__main__":
    main()


