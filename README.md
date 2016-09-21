# Index CouchDB with Elasticsearch

A small script written in [Python](https://www.python.org/) used to index a [CouchDB 2.x](http://couchdb.apache.org/) database with [Elasticsearch](https://www.elastic.co/)  - [blog announcement](http://rainbowheart.ro/498).

The ideea of this script is inspired from [couchdb rivers](https://github.com/elastic/elasticsearch-river-couchdb).

Start Indexing database db1:

![Start Indexing db1](http://rainbowheart.ro/static/uploads/1/2016/9/esindexcouchdb2x1.jpg)

End Indexing database db1:

![End Indexing db1](http://rainbowheart.ro/static/uploads/1/2016/9/esindexcouchdb2x2.jpg)


The python program require an ini file, created like that:

```ini

#
# Config file for indexing CouchDB with Elasticsearch
#


[ES]
index_name = db1
type_name = test1


[CouchDB]
dbname = db1
dbindex = index
index_doc_seq = db1
bulk_size = 1000


[app]
#seconds
delay = 1
verbose = True

```

Section [ES] is for Elasticsearch.

Section [CouchDB]:

dbname = which database is indexed

dbindex = in which database to store last sequence processed

index_doc_seq = name of document which keep last sequence processed

bulk_size = size of batch used to read from CouchDB and to write to ES

Section [app] keep some generic settings, after last sequence is processed, program wait for a while before to check for changes.

Feel free to use this software for both personal and commercial usage.

