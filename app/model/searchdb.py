import copy
import bson
import neo4j.exceptions
import pymongo.collection

from .extensions import NeoDB
from flask import current_app, g, jsonify
from pymongo import errors
from datetime import datetime
import dateutil
from bson.codec_options import CodecOptions

class SearchDB:


    def __init__(self):
        self.__dttime = None
        self.__uid = None
        self.__db_handle = None
        self.__product_collection = 'gemift_prod_db'
        self.__search_index = "prod_index"
    def get_db(self):
        return self.__db_handle

    def get_index(self):
        return self.__search_index

    def get_search_collection(self):
        return self.__product_collection

    def get_datetime(self):
        self.__dttime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return self.__dttime

    def get_id(self):
        self.__uid = str(uuid.uuid4())
        return self.__uid


    def search_by_subcategory(self, inputs, output_list):
        # occasion names should be a list
        try:
           # list_subcategory = "'%s'" % "','".join(inputs["subcategory"])

            datefilter = None
            datefilter = '2021-02-01T00:00:00.000Z'
            myDatetime = dateutil.parser.parse(datefilter)
            sort_order = None
            if inputs["sort_order"] == "ASC":
               sort_order = 1
            else:
               sort_order = -1
            """

            search_string = " {$search: {'index': '%s' " \
                            "'compound': { 'must': [ { 'text': { 'query': ['%s'], 'path': 'occasion' } }," \
                            "{ 'compound': { 'filter': [ {  { 'range': { 'gte': %d, 'path': 'age_lo' } } ," \
                            "{'range':{'gte':%d, 'path':'age_hi'}}," \
                            "{'range':{'path':'created_dt', 'gte':ISODate('2021-02-01T00:00:00.000Z')}}] } } ] } } } ," \
                            "  {$sort:{age_lo:-1, created_dt: -1}}])" % (self.get_index(), occasion_names, age_floor, age_ceiling)
            """
            result = None
            test = "prod_index"
            user_collection = pymongo.collection.Collection(g.db, self.get_db())
            result = user_collection.aggregate([{"$search": {"index": self.get_index(),
                                                             "compound": {"must": [{"text": {"query": [inputs["subcategory"]],
                                                                                             "path": "occasion"}},
                                                                                   {"compound": {"filter": [
                                                                                       {"range": {"gte": inputs["age_floor"],
                                                                                                  "path": "age_lo"}},
                                                                                       {"range": {"gte": inputs["age_ceiling"],
                                                                                                  "path": "age_hi"}},
                                                                                       {'range': {'path': 'created_dt',
                                                                                                  'gt': myDatetime}}
                                                                                   ]}}
                                                                                   ]}}},
                                                {"$sort": { "created_dt": -1, "price": sort_order}}])
            if result is not None:
                for doc in result:
                    print("The doc is ", doc)
                    output_list.append(doc)
                print("The result is ", len(output_list), output_list)
                return True
            else:
                print("No output")
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The generic exception is ", e)
            return False

    def search_by_category(self, inputs, output_list):
        # occasion names should be a list
        try:
           # list_subcategory = "'%s'" % "','".join(inputs["subcategory"])

            datefilter = None
            datefilter = '2021-02-01T00:00:00.000Z'
            myDatetime = dateutil.parser.parse(datefilter)
            sort_order = None
            if inputs["sort_order"] == "ASC":
               sort_order = 1
            else:
               sort_order = -1
            """

            search_string = " {$search: {'index': '%s' " \
                            "'compound': { 'must': [ { 'text': { 'query': ['%s'], 'path': 'occasion' } }," \
                            "{ 'compound': { 'filter': [ {  { 'range': { 'gte': %d, 'path': 'age_lo' } } ," \
                            "{'range':{'gte':%d, 'path':'age_hi'}}," \
                            "{'range':{'path':'created_dt', 'gte':ISODate('2021-02-01T00:00:00.000Z')}}] } } ] } } } ," \
                            "  {$sort:{age_lo:-1, created_dt: -1}}])" % (self.get_index(), occasion_names, age_floor, age_ceiling)
            """
            result = None
            test = "prod_index"
            user_collection = pymongo.collection.Collection(g.db, self.get_db())
            result = user_collection.aggregate([{"$search": {"index": self.get_index(),
                                                             "compound": {"must": [{"text": {"query": [inputs["category"]],
                                                                                             "path": "occasion"}},
                                                                                   {"compound": {"filter": [
                                                                                       {"range": {"gte": inputs["age_floor"],
                                                                                                  "path": "age_lo"}},
                                                                                       {"range": {"gte": inputs["age_ceiling"],
                                                                                                  "path": "age_hi"}},
                                                                                       {'range': {'path': 'created_dt',
                                                                                                  'gt': myDatetime}}
                                                                                   ]}}
                                                                                   ]}}},
                                                {"$sort": { "created_dt": -1, "price": sort_order}}])
            if result is not None:
                for doc in result:
                    print("The doc is ", doc)
                    output_list.append(doc)
                print("The result is ", len(output_list), output_list)
                return True
            else:
                print("No output")
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The generic exception is ", e)
            return False

    def search_by_occasion(self, inputs, output_list):
        #occasion names should be a list
        try:
            if inputs["sort_order"] == "ASC":
               sort_order = 1
            else:
               sort_order = -1
            datefilter = None
            datefilter = '2021-02-01T00:00:00.000Z'
            myDatetime = dateutil.parser.parse(datefilter)

            """
            
            search_string = " {$search: {'index': '%s' " \
                            "'compound': { 'must': [ { 'text': { 'query': ['%s'], 'path': 'occasion' } }," \
                            "{ 'compound': { 'filter': [ {  { 'range': { 'gte': %d, 'path': 'age_lo' } } ," \
                            "{'range':{'gte':%d, 'path':'age_hi'}}," \
                            "{'range':{'path':'created_dt', 'gte':ISODate('2021-02-01T00:00:00.000Z')}}] } } ] } } } ," \
                            "  {$sort:{age_lo:-1, created_dt: -1}}])" % (self.get_index(), occasion_names, age_floor, age_ceiling)
            """
            result = None
            test = "prod_index"
            user_collection = pymongo.collection.Collection(g.db, self.get_db())
            result = user_collection.aggregate([ { "$search": { "index": self.get_index(),
                                                    "compound": { "must": [ { "text": { "query": [inputs["occasion_names"]], "path": "occasion" } },
                                                            { "compound": { "filter": [ 
                                                                            { "range": { "gte": inputs["age_floor"], "path": "age_lo" } } ,
                                                                            {"range":{"gte":inputs["age_ceiling"], "path":"age_hi"}},
                                                                            {'range': {'path': 'created_dt','gt': myDatetime}}
                                                                            ] } }
                                                                    ] } } } , {"$sort":{"age_lo":-1, "created_dt": -1, "price": sort_order }}])
            if result is not None:
                for doc in result:
                    print ("The doc is ", doc)
                    output_list.append(doc)
                print ("The result is ", len(output_list), output_list)
                return True
            else:
                print ("No output")
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print ("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print ("The generic exception is ", e)
            return False

    def search_by_occasion_color(self, inputs, output_list):
        # occasion names should be a list
        try:
            if inputs["sort_order"] == "ASC":
                sort_order = 1
            else:
                sort_order = -1
            datefilter = None
            datefilter = '2021-02-01T00:00:00.000Z'
            myDatetime = dateutil.parser.parse(datefilter)

            """

            search_string = " {$search: {'index': '%s' " \
                            "'compound': { 'must': [ { 'text': { 'query': ['%s'], 'path': 'occasion' } }," \
                            "{ 'compound': { 'filter': [ {  { 'range': { 'gte': %d, 'path': 'age_lo' } } ," \
                            "{'range':{'gte':%d, 'path':'age_hi'}}," \
                            "{'range':{'path':'created_dt', 'gte':ISODate('2021-02-01T00:00:00.000Z')}}] } } ] } } } ," \
                            "  {$sort:{age_lo:-1, created_dt: -1}}])" % (self.get_index(), occasion_names, age_floor, age_ceiling)
            """
            result = None
            test = "prod_index"
            user_collection = pymongo.collection.Collection(g.db, self.get_db())
            result = user_collection.aggregate([{"$search": {"index": self.get_index(),
                                                             "compound": {"must": [{"text": {
                                                                 "query": [inputs["occasion_names"]],
                                                                 "path": "occasion"}},
                                                                                   {"compound": {"filter": [
                                                                                       { 'text': { 'query': ['%s'], 'path': 'color' } },
                                                                                       {"range": {
                                                                                           "gte": inputs["age_floor"],
                                                                                           "path": "age_lo"}},
                                                                                       {"range": {
                                                                                           "gte": inputs["age_ceiling"],
                                                                                           "path": "age_hi"}},
                                                                                       {'range': {'path': 'created_dt',
                                                                                                  'gt': myDatetime}}
                                                                                   ]}}
                                                                                   ]}}},
                                                {"$sort": {"age_lo": -1, "created_dt": -1, "price": sort_order}}])
            if result is not None:
                for doc in result:
                    print("The doc is ", doc)
                    output_list.append(doc)
                print("The result is ", len(output_list), output_list)
                return True
            else:
                print("No output")
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The generic exception is ", e)
            return False

    def search_by_occasion_price(self, inputs, output_list):
        # occasion names should be a list
        try:
            if inputs["sort_order"] == "ASC":
                sort_order = 1
            else:
                sort_order = -1
            datefilter = None
            datefilter = '2021-02-01T00:00:00.000Z'
            myDatetime = dateutil.parser.parse(datefilter)

            """

            search_string = " {$search: {'index': '%s' " \
                            "'compound': { 'must': [ { 'text': { 'query': ['%s'], 'path': 'occasion' } }," \
                            "{ 'compound': { 'filter': [ {  { 'range': { 'gte': %d, 'path': 'age_lo' } } ," \
                            "{'range':{'gte':%d, 'path':'age_hi'}}," \
                            "{'range':{'path':'created_dt', 'gte':ISODate('2021-02-01T00:00:00.000Z')}}] } } ] } } } ," \
                            "  {$sort:{age_lo:-1, created_dt: -1}}])" % (self.get_index(), occasion_names, age_floor, age_ceiling)
            """
            result = None
            test = "prod_index"
            user_collection = pymongo.collection.Collection(g.db, self.get_db())
            result = user_collection.aggregate([{"$search": {"index": self.get_index(),
                                                             "compound": {"must": [{"text": {
                                                                 "query": [inputs["occasion_names"]],
                                                                 "path": "occasion"}},
                                                                                   {"compound": {"filter": [
                                                                                       {"range": {
                                                                                           "gte": inputs["age_floor"],
                                                                                           "path": "age_lo"}},
                                                                                       {"range": {
                                                                                           "gte": inputs["price_lo"],
                                                                                           "lte": inputs["price_hi"],
                                                                                           "path": "age_lo"}},
                                                                                       {"range": {
                                                                                           "gte": inputs["age_ceiling"],
                                                                                           "path": "age_hi"}},
                                                                                       {'range': {'path': 'created_dt',
                                                                                                  'gt': myDatetime}}
                                                                                   ]}}
                                                                                   ]}}},
                                                {"$sort": {"age_lo": -1, "created_dt": -1, "price": sort_order}}])
            if result is not None:
                for doc in result:
                    print("The doc is ", doc)
                    output_list.append(doc)
                print("The result is ", len(output_list), output_list)
                return True
            else:
                print("No output")
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The generic exception is ", e)
            return False

    def get_product_detail(self, inputs, output_hash):
        try:
            result = self.__db_handle.self.__product_collection.find({'product_id':inputs["product_id"]})
            output_hash = copy.deepcopy(result)
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print ("The error is ", e)
            return False

    def vote_product(self, inputs):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (b:product{product_id:$product_id_, friend_circle_id:$friend_circle_id) " \
                    " (a.User{user_id:$user_id_) " \
                    " MERGE (a)-[r:VOTE_PRODUCT]->(b) " \
                    " ON MATCH " \
                    " SET r.value = vote_, r.comment = $comment_, r.updated_dt = $updated_dt_ " \
                    " ON CREATE " \
                    " SET r.value = vote_, r.comment = $comment_, r.created_dt = $created_dt_, r.occasion_name=$occasion_name, " \
                    " r.friend_circle_id = $friend_circle_id_, r.occasion_year = $occasion_year_" \
                    " RETURN b.product_id, a.user_id"
            result = driver.run(query, user_id_= inputs["user_id"], product_id_ = inputs["product_id"], vote_= input["vote"],
                                friend_circle_id_ = input["friend_circle_id"], comment_ = inputs["comment"],
                                occasion_name_ = inputs["occasion_name"], occasion_year_ = inputs["occasion_year"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            if result.peek() is None:
                return False
            record = result.single()
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("e.message")
            return False

    def get_product_votes(self, inputs, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:VOTE_PRODUCT]->(f:product) " \
                    " WHERE r.friend_circle_id = $friend_circle_id_" \
                    " AND f.product_id = $product_id_ " \
                    " AND r.occasion_year = $occasion_year, " \
                    " AND r.occasion_name = $occasion_name_" \
                    " RETURN sum(a.user_id) as total_users, r.vote, f.product_id "
            result = driver.run(query, friend_circle_id_=inputs["friend_circle_id"],
                                product_id_ = inputs["product_id"],
                                occasion_name_ = inputs["occasion_name"],
                                occasion_year_ = inputs["occasion_year"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            for record in result:
                loutput.append(
                    {"product_id": record["f.product_id"], "vote": record["r.vote"], "users": record["total_users"]})
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL")
            return False


    def get_product_comments(self, friend_circle_id, product_id, occasion_name, occasion_year, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:VOTE_PRODUCT]->(f:product) " \
                    " WHERE r.friend_circle_id = $friend_circle_id_" \
                    " AND f.product_id = $product_id_" \
                    " AND r.occasion_name = $occasion_name_" \
                    " AND r.occasion_year = $occasion_year_ " \
                    " RETURN a.user_id, r.comment, f.product_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                product_id_ = product_id,
                                occasion_name_ = occasion_name,
                                occasion_year_ = occasion_year)
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            for record in result:
                loutput.append(
                    {"product_id": record["f.product_id"], "comment": record["r.comment"], "user_id": record["a.user_id"]})
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL")
            return False