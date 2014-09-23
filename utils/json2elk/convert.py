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
estype = 'license'

mapping = {
    estype : {
        'properties': {
            'id': {'type': 'string', 'index': 'not_analyzed'},
            'name_tw': {'type': 'string', 'index': 'not_analyzed'},
            'name_en': {'type': 'string', 'index': 'not_analyzed'},
            'vendor': {'type': 'string', 'index': 'not_analyzed'},
            'overseas_factory': {'type': 'string', 'index': 'not_analyzed'},
            'expire': {'type': 'date'},
            'brand': {'type': 'string', 'index': 'not_analyzed'},
            'code': {'type': 'string', 'index': 'not_analyzed'}
        }
    }
}
                 

def convert():
    es = Elasticsearch([{'host': eshost, 'port': 9200}])
    es.indices.create(index=esindex, ignore=400)
    es.indices.delete_mapping(index=esindex,doc_type=estype)
    es.indices.put_mapping(index=esindex,doc_type=estype, body=mapping)
    jarr = json.load(open(url,'r'),encoding="utf-8")
    for x in jarr:
        try:
            y = {}
            y['id'] = x[u'許可證號']
            y['name_tw'] = x[u'中文名稱']
            y['name_en'] = x[u'英文名稱']
            y['vendor'] = x[u'廠商名稱']
            y['overseas_factory'] = x[u'國外原製造廠商']
            y['code'] = x[u'農藥代號']
            y['brand'] = x[u'廠牌名稱']
            y['expire'] = yearfix(x[u'有效期限'])
            y['timestamp'] = datetime.now()
            res = es.index(index=esindex, doc_type=estype, id=y['id'],  body=y)
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

