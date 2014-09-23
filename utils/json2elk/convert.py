#!/usr/bin/env python
#coding:UTF-8
import urllib2,json
from datetime import datetime
from datetime import date
from elasticsearch import Elasticsearch
#
# start elasticsearch from docker
# sudo docker run -p 8080:8080 -p 9200:9200 -d y12docker/elasticsearch 
#
url = "licenses.json"
eshost = "192.168.2.73"
esindex = 'agr-g0v'
estype = 'license4'

mapping = {
    estype : {
        'properties': {
            u'許可證號': {'type': 'string', 'index': 'not_analyzed'},
            u'中文名稱': {'type': 'string', 'index': 'not_analyzed'},
            u'英文名稱': {'type': 'string', 'index': 'not_analyzed'},
            u'廠商名稱': {'type': 'string', 'index': 'not_analyzed'},
            u'國外原製造廠商': {'type': 'string', 'index': 'not_analyzed'},
            u'有效期限': {'type': 'date'},
            u'廠牌名稱': {'type': 'string', 'index': 'not_analyzed'},
            u'農藥代號': {'type': 'string', 'index': 'not_analyzed'}
        }
    }
}
                 

def convert():
    es = Elasticsearch([{'host': eshost, 'port': 9200}])
    es.indices.create(index=esindex, ignore=400)
    es.indices.put_mapping(index=esindex,doc_type=estype, ignore_conflicts=True, body=mapping)
    jarr = json.load(open(url,'r'),encoding="utf-8")
    # print(json.dumps(jarr[0], indent=4,ensure_ascii=False,encoding='utf8'))
    #print(res)
    for x in jarr:
        try:
            x[u'有效期限'] = yearfix(x[u'有效期限'])
            res = es.index(index=esindex, doc_type=estype, id=x[u'許可證號'],  body=x)
        except ValueError:
            print("Oops! ValueError:")
            print(json.dumps(x, indent=4,ensure_ascii=False,encoding='utf8'))

def yearfix(target):
    list = [int(x) for x in target.split('/')]
    #print(list)
    return date(list[0]+1911,list[1],list[2])
    
def main():
    #print(yearfix('103/12/04'))
    convert()
    
main()

