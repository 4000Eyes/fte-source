import pymongo
import neo4j.exceptions
import pymongo.collection
from .extensions import NeoDB
from flask import current_app, g, jsonify
from pymongo import errors
from datetime import datetime
import dateutil
import uuid
import json



class SearchDB:

    def __init__(self):
        self.__dttime = None
        self.__uid = None
        self.__db_handle = None
        self.__product_collection = None
        self.__search_index = None
        self.__user_index = None

    def get_db(self):
        return self.__db_handle

    def get_index(self):
        self.__search_index = "prod_index"
        return self.__search_index

    def get_user_index(self):
        self.__user_index = "auto_user_gemift"
        return self.__user_index

    def get_category_index(self):
        self.__category_index = "auto_cat_gemift"
        return self.__category_index

    def get_search_collection(self):
        self.__product_collection = "gemift_product_db"
        return str(self.__product_collection)

    def get_user_collection(self):
        self.__user_collection = "user"
        return str(self.__user_collection)

    def get_category_collection(self):
        self.__category_collection = "cat_hierarchy"
        return str(self.__category_collection)

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
            gemift_coll = "gemift_product_db"
            user_collection = pymongo.collection.Collection(g.db, self.get_search_collection())
            result = user_collection.aggregate([{"$search": {"index": self.get_index(),
                                                             "compound": {
                                                                 "must": [{"text": {"query": [inputs["subcategory"]],
                                                                                    "path": "subcategory"}},
                                                                          {"compound": {"filter": [
                                                                              {"range": {"gte": inputs["age_floor"],
                                                                                         "path": "age_lo"}},
                                                                              {"range": {"gte": inputs["age_ceiling"],
                                                                                         "path": "age_hi"}},
                                                                              {'range': {'path': 'created_dt',
                                                                                         'gt': myDatetime}}
                                                                          ]}}
                                                                          ]}}},
                                                {"$sort": {"created_dt": -1, "price": sort_order}}])
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

    def search_gemift_products(self, inputs, output_list):
        # occasion names should be a list
        try:
            # list_subcategory = "'%s'" % "','".join(inputs["subcategory"])
            datefilter = None
            datefilter = '2020-02-01T00:00:00.000Z'
            myDatetime = dateutil.parser.parse(datefilter)
            sort_order = None
            occasion_flag = 0
            category_flag = 0
            must_class_hash = {}
            if inputs["sort_order"] == "ASC":
                sort_order = 1
            else:
                sort_order = -1

            result = None

            if "occasion" in inputs and len(inputs["occasion"]) > 0:
                occasions_hash = {}
                occasions_hash["text"] = {}
                occasions_hash["text"]["query"] = inputs["occasion"]
                occasions_hash["text"]["path"] = "occasion_id"
                occasion_flag = 1
                must_class_hash = occasions_hash

            final_filter_list = []
            if 'gender' in inputs and inputs["gender"] is not None and len(inputs["gender"]) > 0:
                gender_hash = {}
                gender_hash["text"] = {}
                gender_hash["text"]["query"] = inputs["gender"]
                gender_hash["text"]["path"] = "gender"
                final_filter_list.append(gender_hash)

            if "age" in inputs:
                #range_string =  {"range": {"gte": inputs["age_floor"],"path": "age_lo"}}
                age_floor_hash = {}
                age_floor_hash["range"] = {}
                age_floor_hash["range"]["path"] = "age_lo"
                age_floor_hash["range"]["lte"] = int(inputs["age"])


            if "age" in inputs:
                #celing = {"range": {"gte": inputs["age_ceiling"],"path": "age_hi"}}
                age_ceiling_hash = {}
                age_ceiling_hash["range"] = {}
                age_ceiling_hash["range"]["path"] = "age_hi"
                age_ceiling_hash["range"]["gte"] = int(inputs["age"])


            #{'range': {'path': 'created_dt','gt': myDatetime}}

            date_crieteria_hash = {}
            date_crieteria_hash["range"] = {}
            date_crieteria_hash["range"]["gt"] = myDatetime
            date_crieteria_hash["path"] = "created_dt"

            final_filter_list.append(age_floor_hash)
            final_filter_list.append(age_ceiling_hash)

            if "price_from" in inputs and inputs["price_from"] is not None and \
                    "price_to" in inputs and inputs["price_to"] is not None:
                if inputs["price_from"] is None:
                    inputs["price_from"] = 0.01
                if inputs["price_to"] is None:
                    inputs["price_to"] = 999999.00
                price_from_hash = {}
                price_from_hash["range"] = {}
                price_from_hash["range"]["gte"] = inputs["price_from"]
                price_from_hash["range"]["lte"] = inputs["price_to"]
                price_from_hash["range"]["path"] = "price"
                final_filter_list.append(price_from_hash)

                # price_to_hash = {}
                # price_to_hash["range"] = {}
                # price_to_hash["range"]["lte"] = inputs["price_to"]
                # price_to_hash["range"]["path"] = "price"
                # final_filter_list.append(price_to_hash)



            if 'color' in inputs and len(inputs["color"]) > 0:
           # {'text': {'query': ['%s'], 'path': 'color'}},
                color_hash = {}
                color_hash["text"] = {}
                color_hash["text"]["query"] = inputs["color"]
                color_hash["text"]["path"] = "color"
                final_filter_list.append(color_hash)
            if "category" in inputs and len(inputs["category"]) > 0:
                category_hash = {}
                category_hash["text"] = {}
                category_hash["text"]["query"] = inputs["category"]
                category_hash["text"]["path"] = "category"
                if occasion_flag == 1:
                    final_filter_list.append(category_hash)
                else:
                    must_class_hash = category_hash
                    category_flag = 1
            if "subcategory" in inputs and len(inputs["subcategory"]) > 0:
                subcategory_hash = {}
                subcategory_hash["text"] = {}
                subcategory_hash["text"]["query"] = inputs["subcategory"]
                subcategory_hash["text"]["path"] = "interest"
                if occasion_flag == 1 or category_flag == 1:
                    final_filter_list.append(subcategory_hash)
                else:
                    must_class_hash = subcategory_hash
            if len(must_class_hash) <= 0:
                current_app.logger.error("The must class cannot be empty")
                return 2

            search_string = {"$search": {"index": self.get_index(),
                                                             "compound": {"must": [
                                                                 must_class_hash,
                                                                 {"compound": {"filter":  final_filter_list
                                                                 }}
                                                                 ]}}
                             }


            sort_string = {"$sort": { "price": sort_order }}

            skip_size = inputs["page_size"] * (inputs["page_size"] -1 )
            skip_string = {"$skip": skip_size}
            limit_string = {"$limit": inputs["page_number"]}

            pipeline = [search_string, sort_string, skip_string, limit_string]
            print ("The pipeline query is", pipeline)

            user_collection = pymongo.collection.Collection(g.db, self.get_search_collection())
            result = user_collection.aggregate(pipeline)

            """
            result = user_collection.aggregate([{"$search": {"index": self.get_index(),
                                                             "compound": {"must": [
                                                                 {"text": {"query": ["electronics"], "path": "category"}},
                                                                 {"compound": {"filter": [
                                                                     {"range": {"gte": inputs["age_floor"],
                                                                                "path": "age_lo"}},
                                                                     {"range": {"gte": inputs["age_ceiling"],
                                                                                "path": "age_hi"}},
                                                                     {'range': {'path': 'created_dt',
                                                                                'gt': myDatetime}}
                                                                 ]}}
                                                                 ]}}},
                                                {"$sort": xstr}])
            """
            if result is not None:
                for doc in result:
                    print("The doc is ", doc["interest"], doc["age_lo"], doc["age_hi"])
                    output_list.append(doc)
                return 1
            else:
                print("No output")
            return 1

        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return -1

        except Exception as e:
            current_app.logger.error(e)
            print("The generic exception is ", e)
            return -1



    def get_product_detail(self, inputs, output_list):
        try:
            user_collection = pymongo.collection.Collection(g.db, self.get_search_collection())
            result = user_collection.find({'product_id': {"$in": inputs["product_id"]}})
            # DO NOT nullify or None the output list. Some functions send loaded lists for performance
            for doc in result:
                print("The result document is", doc)
                output_list.append(doc)
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def vote_product(self, inputs):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            create_product_query = "MERGE (b:product{product_id:$product_id_}) " \
                                   " ON CREATE set b.created_dt = $created_dt_ , b.product_title = $product_title_," \
                                   " b.price = $price_" \
                                   " RETURN b.product_id"
            presult = txn.run(create_product_query, product_id_ = inputs["product_id"],
                              product_title_ = inputs["product_title"],
                              price_ = inputs["price"],
                              created_dt_ = self.get_datetime())

            b_product_flga = 0
            for row in presult:
                b_product_flag = 1

            if not b_product_flag:
                txn.rollback()
                current_app.logger.error("Unable to insert or update the product")
                return False


            query = "MATCH (b:product{product_id:$product_id_}) ," \
                    " (a:User{user_id:$user_id_}) " \
                    " MERGE (a)-[r:VOTE_PRODUCT]->(b) " \
                    " ON MATCH " \
                    " SET r.value = $vote_, r.comment = $comment_, r.updated_dt = $updated_dt_ " \
                    " ON CREATE " \
                    " SET r.value = $vote_, r.comment = $comment_, r.created_dt = $created_dt_, r.occasion_name=$occasion_name_, " \
                    " r.friend_circle_id = $friend_circle_id_, r.occasion_year = $occasion_year_" \
                    " RETURN b.product_id, a.user_id"

            resultX = txn.run(query, user_id_=inputs["user_id"], product_id_=inputs["product_id"], vote_=inputs["vote"],
                                friend_circle_id_=inputs["friend_circle_id"], comment_=inputs["comment"],
                                occasion_name_=inputs["occasion_name"], occasion_year_=inputs["occasion_year"],
                                created_dt_ = self.get_datetime(), updated_dt_ = self.get_datetime())

            counter = 0
            for record in resultX:
                print("The record is ", record["a.user_id"])
                counter = 1
            print("The  query is ", resultX.consume().query)
            print("The  parameters is ", resultX.consume().parameters)
            if counter ==0:
                current_app.logger.error("Relationship between the user and product is not created " + inputs["user_id"] )
                txn.rollback()
                return False
            txn.commit()

            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            txn.rollback()
            return False

    def get_voted_product_count(self, friend_circle_id, occasion_name, occasion_year, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:VOTE_PRODUCT]->(f:product) " \
                    " WHERE r.friend_circle_id = $friend_circle_id_" \
                    " AND r.occasion_year = $occasion_year_ " \
                    " AND r.occasion_name = $occasion_name_" \
                    " RETURN coalesce(count(f.product_id),0) as total_product"

            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                occasion_name_=occasion_name,
                                occasion_year_=occasion_year)
            #I have to make changes to move away from occasion name to id soon

            for record in result:
                loutput.append(record.data())

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("Error in executing the SQL", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_voted_products(self, inputs, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:VOTE_PRODUCT]->(f:product) " \
                    " WHERE r.friend_circle_id = $friend_circle_id_" \
                    " AND r.occasion_year = $occasion_year_ " \
                    " AND r.occasion_name = $occasion_name_" \
                    " RETURN count(a.user_id) as total_users, r.vote as vote, f.product_id as product_id, " \
                    " f.product_title as product_title, f.price as price"
            result = driver.run(query, friend_circle_id_=inputs["friend_circle_id"],
                                occasion_name_=inputs["occasion_name"],
                                occasion_year_=inputs["occasion_year"])
            #I have to make changes to move away from occasion name to id soon

            for record in result:
                loutput.append(record.data())
            """
            if not self.get_product_comments(inputs, loutput):
                current_app.logger.error(
                    "Error in getting the product details from the product votes function for friend circle id " +
                    inputs["f.product_id"])
                print("Error in getting the product details from the product votes function for friend circle id ", inputs[
                    "f.product_id"])
                return False
            """
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("Error in executing the SQL", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False


    def get_product_comments(self, inputs, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:friend_list)-[r:VOTE_PRODUCT]->(f:product) " \
                    " WHERE r.friend_circle_id = $friend_circle_id_" \
                    " AND f.product_id = $product_id_" \
                    " AND r.occasion_name = $occasion_name_" \
                    " AND r.occasion_year = $occasion_year_ " \
                    " RETURN a.user_id, r.comment, f.product_id"
            result = driver.run(query, friend_circle_id_=inputs["friend_circle_id"],
                                product_id_=inputs["product_id"],
                                occasion_name_=inputs["occasion_name"],
                                occasion_year_=inputs["occasion_year"])
            for record in result:
                loutput.append(record.data())

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL")
            return False


    def get_users(self, username, output_list):
        try:
            project_string = {"$project": { "first_name": 1, "last_name":1, "phone_number":1 , "full_name":1, "user_id":1}}
            search_string = ({"$search": {"index": self.get_user_index(), "autocomplete": { "query": username,"path": "full_name","tokenOrder": "sequential"  }}})
            pipeline = [search_string, project_string]
            user_collection = pymongo.collection.Collection(g.db, self.get_user_collection())

            # "$search": {
            #     "compound": {
            #         "filter": [{
            #             "text": {path: "city", query: "New York"}
            #         }],
            #         "must": [{
            #             "autocomplete": {
            #                 "query": `${request.query.term}
            # `,
            # "path": "name",
            # "fuzzy": {
            #     "maxEdits": 2,
            #     "prefixLength": 3,
            # },
            # }, }
            # }]}

            result = user_collection.aggregate(pipeline)
            if result is not None:
                for doc in result:
                    print("The doc is ", doc)
                    output_list.append(doc)
                print("The result is ", len(output_list), output_list)
                return True
            else:
                print("No output")
                output_list = None
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_cat_hierarchy(self, entity_name, output_list):
        try:
            project_string = {"$project": { "entity_name": 1, "entity_type":1, "entity_id":1}}
            search_string = ({"$search": {"index": self.get_category_index(), "autocomplete": { "query": entity_name,"path": "entity_name","tokenOrder": "sequential"  }}})
            pipeline = [search_string, project_string]
            user_collection = pymongo.collection.Collection(g.db, self.get_category_collection())
            result = user_collection.aggregate(pipeline)
            if result is not None:
                for doc in result:
                    print("The doc is ", doc)
                    output_list.append(doc)
                print("The result is ", len(output_list), output_list)
                return True
            else:
                print("No output")
                output_list = None
            return True
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False