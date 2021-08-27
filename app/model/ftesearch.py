import json
from elasticsearch import Elasticsearch, helpers
import elasticsearch

class FTESearch:
    def __init__(self):
        self._es = None
        self._index = None

    def _set_index(self, index):
        self._index = index

    def _connect_search(self):
        try:
            self._es = Elasticsearch(cloud_id="i-o-optimized-deployment:dXMtd2VzdDEuZ2NwLmNsb3VkLmVzLmlvJDlhMTVkYzcyNDk5OTQwNWQ5MjkzYTIxZTg3Y2MxZTA1JDljMzljZDE5YjA3ZDQ3MmFhODFjNmNhN2ZhNjVmZDJk",
                       http_auth=("elastic", "mBbLha3eNawp1emPpYuqSf42"))
        except elasticsearch.ElasticsearchException as e:
                print ("The error is", e.info)
                return None

    def full_text_search(self, keyword, field):
        try:
            res = self._es.search(
            index=self._index,body={"query":{"match": { field: keyword}}})
            print ("The results is", res)
            return res
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None

    def multiple_field_query_filter_search(self, sbool, dquery, dtfilter, drfilter):

        #sbool can be must or should

        squery = None
        stfilter = None
        srfilter = None
        nCounter = 0

        # match

        for qval in dquery.items():
            squery = squery + "{match:{" + qval[0] + ":" + qval[1] + "}}"
            squery = squery + "," if not nCounter else squery
        nCounter = 0

        # terms
        for qval in dtfilter.items():
            stfilter = stfilter + "{term:{" + qval[0] + ":" + qval[1] + "}}"
            stfilter = stfilter + "," if not nCounter else stfilter

        # range
        for qval in drfilter.items():
            srfilter = srfilter + "{term:{" + qval[0] + ":" + qval[1] + "}}"
            srfilter = srfilter + "," if not nCounter else srfilter

        filter = stfilter + "," + srfilter if len(drfilter) > 0 else filter = stfilter

        try:
            rs = self._es.search(
                index=self._index, body={"query": {"bool": {"'" + sbool +"'":[squery], "filter":[filter]}}} )
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None

    def regex_search(self, pat, field):
        try:
            res = self._es.search(
                index=self._index,
                body={ "query":{ "regexp": { field: pat}}})
            print ("The regex", res)
            return res
        except elasticsearch.ElasticsearchException as e:
            print (e.info)
            return None
        except:
            print ("Unexplained Error")
            return None

    def bool_search_category(self, categories, price_lo, price_hi, created_dt_from, created_dt_to, updated_dt_from, updated_dt_to, lsort_fields):
        return None
    """
       test = None
       sort_str = None
       for row in lsort_fields.items():
           sort_str = sort_str + "{" + row[0] + ":" + row[1] + "}"
           sort_str = sort_str + "," if len(lsort_fields) > 1 else sort_str
       try:
           res = self._es.search(
               index = self._index,
               body = {
                   "query": {
                       "bool": {
                           "must": {
                               "terms": {
                                   "category": [ categories ]
                               }
                           },
                           "filter": [
                               {"terms": {"category": [categories]}},
                               {"range": {"price": {"gt": price_hi, "lte" : price_lo}}},

                               {"range": {
                                   "created_dt": {
                                       "gte": created_dt_from,
                                       "lte": created_dt_to
                                   }
                               },
                               {"range": {
                                   "updated_dt": {
                                       "gte": updated_dt_from,
                                       "lte": updated_dt_to
                                   }
                               }
                               }}
                           ]
                       }

                   },
                   "sort": [
                       sort_str
                   ]
               }
           )
    """
    def prefix_search(self,prefix, field):
        try:
            res = self._es.search(
                index=self._index,
                body={"query":{"prefix": {field: prefix}}})
            return res
        except elasticsearch.ElasticsearchException as e:
            print (e.info)
            return None
        except:
            return None

    def delete_data_by_bulk(self, _doc_type, _id):
        try:
            action = [
                 {
                    '_op_type': 'delete',
                    '_index': self._index,
                    '_type': _doc_type,
                    '_id': _id,
                }
                ]
            res = helpers.bulk(self._es, action)
            return res
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None


    # update by index
    def update_data_by_index(self,  _doc_type, _id, update_data):
        try:
            res = self._es.update(
                index=self._index,
                doc_type=_doc_type,
                id=_id,
                body=update_data
            )
            return res
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None


    # update by query
    def update_by_query(self, query, field, update_data):
        try:
            _inline = "ctx._source.{field}={update_data}".format(field=field, update_data=update_data)
            _query = {
                "script": {
                   "inline": _inline,
                },
                "query": {
                   "match": query
                }
            }
            res = self._es.update_by_query(body=_query, index=self._index)
            return res
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None



    # update by bulk api
    def update_by_bulk(self, _id, update_data, doc_type):
        try:
            action = [{
                "_id": _id,
                "_type": doc_type,
                "_index": self._index,
                "_source": {"doc": update_data},
                "_op_type": 'update'
            }]
            res = helpers.bulk(self._es, action)
            return res
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None

    # clear all index items
    def delete_index(self):
        try:
            res = self._es.indices.delete(index=self._index, ignore=[400, 404])
            return res
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None

    # All create methods

    def create_index(self):
        try:
            self._es.indices.create(index=self._index, ignore=400)
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None

    def insert_item(self, data):
        try:
        # index and doc_type you can customize by yourself
            res = self._es.index(index=self._index, doc_type='authors', id=5, body=data)
            # index will return insert info: like as created is True or False
            return res
            print(res)
            """
            {'_index': 'test-index', '_type': 'authors', '_id': '5', '_version': 1, 'result': 'created', '_shards': {'total': 2, 's
        uccessful': 1, 'failed': 0}, '_seq_no': 4, '_primary_term': 1}
            """
        except elasticsearch.ElasticsearchException as e:
            print("The error is", e.info)
            return None
        except:
            return None
    def load_jsondata(self, data_dir):
        try:
            with open(data_dir) as f:
                return json.load(f)
        except:
            return None

    def insert_data_by_bulk(self,data):
        try:
            res = helpers.bulk(self._es, data)
            return res
            print(res)
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None

    # delete content but empty item exist
    def delete_by_query(self, query):
        try:
            res = self._es.delete_by_query(
                index=self._index,
                body={"query": {"match": query}},
                _source=True
            )
            return res
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None


    # delet all item
    def delete_one(self, _id):
        try:
            res = self._es.delete(index=self._index, id=_id)
            return res
        except elasticsearch.ElasticsearchException as e:
            return None
        except:
            return None