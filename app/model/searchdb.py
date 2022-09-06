import operator

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
                    print ("I am inside subcategory hash")
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

            skip_size = inputs["page_size"] * (inputs["page_number"] -1 )
            skip_string = {"$skip": skip_size}
            limit_string = {"$limit": inputs["page_size"]}

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


    def get_categories(self, inputs, output_list):
        try:
            if "occasion" in inputs and len(inputs["occasion"]) > 0:
                occasions_hash = {}
                occasions_hash["text"] = {}
                occasions_hash["text"]["query"] = inputs["occasion"]
                occasions_hash["text"]["path"] = "occasion_id"
                occasion_flag = 1


            final_filter_list = []
            if 'gender' in inputs and inputs["gender"] is not None and len(inputs["gender"]) > 0:
                gender_hash = {}
                gender_hash["text"] = {}
                gender_hash["text"]["query"] = inputs["gender"]
                gender_hash["text"]["path"] = "gender"
                final_filter_list.append(gender_hash)
            if len(occasions_hash) <= 0:
                current_app.logger.error("The must class cannot be empty")
                return 2

            search_string = {"$search": {"index": self.get_index(),
                                                             "compound": {"must": [
                                                                 occasions_hash,
                                                                 {"compound": {"filter":  final_filter_list
                                                                 }}
                                                                 ]}}
                             }


            group_string = { "$group" : { "_id" : "$category" } }
            pipeline = [search_string,group_string]
            print ("The pipeline query is", pipeline)

            user_collection = pymongo.collection.Collection(g.db, self.get_search_collection())
            result = user_collection.aggregate(pipeline)
            if result is not None:
                for doc in result:
                    print("The doc is ", doc["_id"])
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

    def vote_recommended_product(self, inputs):
        try:
            driver = NeoDB.get_session()
            product_recommendation_vote_query = "MATCH (pc:product_occasion{product_id:$product_id_," \
                                    "occasion_id:$occasion_id_," \
                                    "friend_circle_id:$friend_circle_id_," \
                                    "occasion_year:$occasion_year_}), (u:User{user_id:$user_id_})" \
                                   " MERGE (pc)<-[r:VOTE_RECOMMENDED_PRODUCT]-(u) " \
                                   " ON CREATE set r.vote=$vote_," \
                                    "r.created_dt = $created_dt_," \
                                    "r.comment = $comment_" \
                                    " ON MATCH " \
                                    "set r.vote=$vote_," \
                                    "r.created_dt = $created_dt_," \
                                    "r.comment = $comment_" \
                                    " RETURN pc.product_id as product_id"
            resultX = driver.run(product_recommendation_vote_query, user_id_=inputs["user_id"],
                              product_id_=inputs["product_id"],
                              vote_=inputs["vote"],
                                friend_circle_id_=inputs["friend_circle_id"],
                              occasion_id_ = inputs["occasion_id"],
                              comment_=inputs["comment"],
                                occasion_year_= str(inputs["occasion_year"]),
                                created_dt_ = self.get_datetime(),
                              updated_dt_ = self.get_datetime())

            counter = 0
            for record in resultX:
                print("The record is ", record["product_id"])
                counter = 1
            print("The  query is ", resultX.consume().query)
            print("The  parameters is ", resultX.consume().parameters)
            if counter ==0:
                current_app.logger.error("Relationship between the user and product is not created " + inputs["user_id"] )
                return False

            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
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

            b_product_flag = 0
            for row in presult:
                b_product_flag = 1

            if not b_product_flag:
                txn.rollback()
                current_app.logger.error("Unable to insert or update the product")
                return False

            final_query = "MERGE (pc:product_occasion{product_id:$product_id_," \
                    "occasion_id:$occasion_id_," \
                    "friend_circle_id:$friend_circle_id_," \
                    "occasion_year:$occasion_year_}) " \
                    " ON MATCH SET pc.occasion_name = $occasion_name_ , pc.created_dt = $created_dt_ " \
                    " ON CREATE SET pc.occasion_name = $occasion_name_, pc.updated_dt = $updated_dt_ " \
                    " with pc " \
                    "  MATCH (u:User{user_id:$user_id_}) ," \
                    " (p:product{product_id:$product_id_})" \
                    " MERGE (u)-[r:VOTE_PRODUCT]->(pc)-[:PRODUCT]->(p) " \
                          " ON CREATE set r.vote = $vote_, r.comment = $comment_, r.created_dt = $created_dt_" \
                          " ON MATCH set  r.vote = $vote_, r.comment = $comment_, r.updated_dt = $updated_dt_ " \
                    " return pc.occasion_id, pc.friend_circle_id, u.user_id as user_id"

            resultX = txn.run(final_query, user_id_=inputs["user_id"],
                              product_id_=inputs["product_id"],
                              vote_=inputs["vote"],
                                friend_circle_id_=inputs["friend_circle_id"],
                              occasion_id_ = inputs["occasion_id"],
                              occasion_name_ = inputs["occasion_name"],
                               comment_=inputs["comment"],
                                 occasion_year_= str(inputs["occasion_year"]),
                                created_dt_ = self.get_datetime(),
                              updated_dt_ = self.get_datetime())

            counter = 0
            for record in resultX:
                print("The record is ", record["user_id"])
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
            query = "MATCH (a:User)-[r:VOTE_PRODUCT]->(po:product_occasion)-[:PRODUCT]->(f:product) " \
                    " WHERE po.friend_circle_id = $friend_circle_id_" \
                    " AND po.occasion_year = $occasion_year_ " \
                    " AND po.occasion_name = $occasion_name_" \
                    " RETURN coalesce(count(distinct f.product_id),0) as total_product"

            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                occasion_name_=occasion_name,
                                occasion_year_=str(occasion_year))
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

    def get_recommended_product_vote_count(self, friend_circle_id, occasion_id, occasion_year, loutput, hshoutput = None):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (pc:product_occasion{" \
                                    "occasion_id:$occasion_id_," \
                                    "friend_circle_id:$friend_circle_id_," \
                                    "occasion_year:$occasion_year_})" \
                                    "<-[r:VOTE_RECOMMENDED_PRODUCT]-(u:User)" \
                                    " return " \
                                    " sum(case when r.vote < 0 then 1 end ) as down_vote, " \
                                    " sum(case when  r.vote > 0 then 1  end ) as up_vote, " \
                                    " pc.product_id as product_id," \
                                    " pc.friend_circle_id as friend_circle_id, " \
                                    " pc.occasion_id as occasion_id, " \
                                    " pc.occasion_year as occasion_year"

            result = driver.run(query,
                                friend_circle_id_=friend_circle_id,
                                occasion_id_ =occasion_id,
                                occasion_year_ = str(occasion_year))
            #I have to make changes to move away from occasion name to id soon

            for record in result:
                if hshoutput is not None:
                    hshoutput[record["product_id"]] = {"up_vote": record["up_vote"], "down_vote": record["down_vote"]}
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
            obj_search = SearchDB()
            l_recommend_product_count = []
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:VOTE_PRODUCT]->(f:product_occasion)-[:PRODUCT]->(p:product) " \
                    " WHERE f.friend_circle_id = $friend_circle_id_" \
                    " AND f.occasion_year = $occasion_year_ " \
                    " AND f.occasion_id = $occasion_id_" \
                    " RETURN " \
                    " count(a.user_id) as total_users," \
                    " f.product_id as product_id, " \
                    " p.product_title as product_title, " \
                    "p.price as price"
            result = driver.run(query, friend_circle_id_=inputs["friend_circle_id"],
                                occasion_id_=inputs["occasion_id"],
                                occasion_year_=str(inputs["occasion_year"]))


            hsh_voted_products = {}
            for record in result:
                hsh_voted_products["product_id"] = record["product_id"]
                hsh_voted_products["total_users"] = record["total_users"]
                hsh_voted_products["product_title"] = record["product_title"]
                hsh_voted_products["price"] = record["price"]
                l_recommend_product_count.clear()
                hshOutput = {}
                if not obj_search.get_recommended_product_vote_count(
                        inputs["friend_circle_id"],
                        inputs["occasion_id"],
                        inputs["occasion_year"],
                        l_recommend_product_count,
                        hshOutput):
                    current_app.logger.error("Unable to get vote for recommended products")
                    return False
                hsh_voted_products.update(hshOutput[record["product_id"]] if record["product_id"] in hshOutput else None)
                loutput.append(hsh_voted_products)
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
            query = "MATCH (a:friend_list)-[r:VOTE_PRODUCT]->(po:product_occasion)-[:PRODUCT]->(f:product) " \
                    " WHERE po.friend_circle_id = $friend_circle_id_" \
                    " AND po.product_id = $product_id_" \
                    " AND po.occasion_name = $occasion_name_" \
                    " AND po.occasion_year = $occasion_year_ " \
                    " RETURN a.user_id, r.comment, f.product_id"
            result = driver.run(query, friend_circle_id_=inputs["friend_circle_id"],
                                product_id_=inputs["product_id"],
                                occasion_name_=inputs["occasion_name"],
                                occasion_year_=str(inputs["occasion_year"]))
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