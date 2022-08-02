import json
import neo4j.exceptions
import logging
from flask import current_app
from flask_restful import Resource
from .extensions import NeoDB
from datetime import datetime
import uuid

class CategoryManagementDB(Resource):
    def __init__(self):
        self.__dttime = None
        self.__uid = None

    def get_datetime(self):
        self.__dttime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return self.__dttime
    def get_id(self):
        self.__uid = str(uuid.uuid4())
        return  self.__uid
    def add_merch_category(self, merch_category_name, description, age_lo, age_hi, gender, output):
        try:
            driver = NeoDB.get_session()
            query = "MERGE (a:MerchCat{merch_category_name : $merch_category_name_}) " \
                    " ON MATCH set " \
                    " a.updated_dt = $updated_dt_ " \
                    "ON CREATE set a.merch_category_id = $merch_id_, " \
                    "a.merch_category_name = $merch_category_name_, " \
                    "a.created_dt = $created_dt_, " \
                    "a.descrption = $description_ ," \
                    "a.age_hi = $age_hi_," \
                    "a.age_lo = $age_lo_," \
                    "a.gender = $gender_ " \
                    "RETURN a.merch_category_id, a.created_dt, a.updated_dt"
            result = driver.run(query, merch_id_= self.get_id(),
                                merch_category_name_=merch_category_name,
                                description_ = description,
                                created_dt_ = self.get_datetime(),
                                updated_dt_ = self.get_datetime(),
                                age_hi_ = age_hi,
                                age_lo_ = age_lo,
                                gender_ = gender
                                )

            record = result.single()
            if record is None:
                return False

            output["merch_category_id"] = record["a.merch_category_id"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("The error is", e.message)
            return False

    def link_merch_to_web_nodes(self, merch_category_id, web_category_id):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:MerchCat{merch_category_id:$merch_category_id_})," \
                    "(b:WebCat{web_category_id:$web_category_id_}) " \
                    " MERGE (a)<-[r:PART_OF]-(b)" \
                    " ON CREATE SET r.created_dt  = $created_dt_" \
                    " ON MATCH SET r.updated_dt = $updated_dt_" \
                    " RETURN a.merch_category_id, a.merch_category_name, b.web_category_id, b.web_cat_name"
            result = driver.run(query,
                                merch_category_id_=merch_category_id,
                                web_category_id_ = web_category_id,
                                created_dt_ = self.get_datetime(),
                                updated_dt_ = self.get_datetime())

            record = result.single()

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)

            if record is None:
                return False
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("Error Message ", e.message)
            return False


    def add_web_category(self, web_category_name, description, age_lo, age_hi, gender, output):
        try:
            driver = NeoDB.get_session()
            print ("inside web category function")
            query = "MERGE (a:WebCat{web_category_name:$web_category_name_}) " \
                    " ON MATCH set  " \
                    " a.updated_dt = $updated_dt_ " \
                    "ON CREATE set a.web_category_id = $web_id_, " \
                    "a.web_category_name = $web_category_name_, " \
                    "a.created_dt = $created_dt_ ," \
                    "a.description=$description_ ," \
                    "a.age_hi  = $age_hi_," \
                    "a.age_lo = $age_lo_," \
                    "a.gender = $gender_ " \
                    "RETURN a.web_category_id"
            result = driver.run(query, web_id_= self.get_id(),
                                web_category_name_=web_category_name,
                                description_ = description,
                                updated_dt_ = self.get_datetime(),
                                created_dt_ = self.get_datetime(),
                                age_hi_ = age_hi,
                                age_lo_ = age_lo,
                                gender_ = gender )

            record = result.single()
            if record["a.web_category_id"]:
                output["web_category_id"] = record["a.web_category_id"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("The error is", e.message)
            return False

    def add_web_subcategory(self, web_subcategory_id, web_subcategory_name, description, parent_id, age_lo, age_hi, gender,image_url,output):
        try:
            driver = NeoDB.get_session()
            query = "MERGE (a:WebSubCat{web_subcategory_name:$web_subcategory_name_}) " \
                    " ON MATCH set  " \
                    " a.updated_dt = $updated_dt_ ," \
                    " a.parent_id = $parent_id_, " \
                    "a.description=$description_ ," \
                    "a.age_hi = $age_hi_," \
                    "a.age_lo = $age_lo_," \
                    "a.gender = $gender_, " \
                    "a.image_url = $image_url_ " \
                    "ON CREATE set a.web_subcategory_id = $web_id_, " \
                    "a.web_subcategory_name = $web_subcategory_name_, " \
                    "a.created_dt = $created_dt_ ," \
                    "a.parent_id = $parent_id_," \
                    "a.description=$description_ ," \
                    "a.age_hi = $age_hi_," \
                    "a.age_lo = $age_lo_," \
                    "a.gender = $gender_, " \
                    "a.image_url = $image_url_ " \
                    "RETURN a.web_subcategory_id"
            result = driver.run(query, web_id_= web_subcategory_id,
                                web_subcategory_name_=web_subcategory_name,
                                description_ = description,
                                updated_dt_ = self.get_datetime(),
                                created_dt_ = self.get_datetime(),
                                age_lo_ = age_lo,
                                age_hi_ = age_hi,
                                gender_ = gender,
                                image_url_ = image_url,
                                parent_id_ = parent_id)

            record = result.single()
            if record["a.web_subcategory_id"]:
                output["web_subcategory_id"] = record["a.web_subcategory_id"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("The error is", e.message)
            return False

    def link_subcategory(self, web_category_id, web_subcategory_id, output):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:WebSubCat{web_subcategory_id:$web_subcategory_id_})," \
                    "(b:WebCat{web_category_id:$web_category_id_}) " \
                    " MERGE (a)-[r:SUBCATEGORY_OF]->(b) " \
                    " ON MATCH set r.updated_dt = $updated_dt_" \
                    " ON CREATE set r.created_dt = $created_dt_" \
                    " RETURN a.web_subcategory_id"
            result = driver.run(query, web_category_id_ = web_category_id,
                                web_subcategory_id_=web_subcategory_id,
                                created_dt_ = self.get_datetime(),
                                updated_dt_ = self.get_datetime())

            record = result.single()
            if record is None:
                print ("The category or subcategory does not exist")
                return False
            output["web_subcategory_id"] = record["a.web_subcategory_id"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("The error is", e.message)
            return False


    def add_brand(self, brand_name, description, age_lo, age_hi, gender, output):
        try:
            web_id = uuid.uuid4()
            driver = NeoDB.get_session()
            query = "MERGE (a:brand{brand_name:$brand_name_}) " \
                    " ON MATCH set  a.updated_dt = $updated_dt_ " \
                    "ON CREATE set a.brand_id = $web_id_, " \
                    "a.brand_name = $brand_name_," \
                    " a.created_dt = $created_dt_," \
                    " a.description = $description_ , " \
                    "a.age_hi = $age_hi_," \
                    "a.age_lo = $age_lo_," \
                    "a.gender = $gender_ " \
                    " RETURN a.brand_id"
            result = driver.run(query, web_id_= self.get_id(),
                                brand_name_=brand_name,
                                description_=description,
                                created_dt_ = self.get_datetime(),
                                updated_dt_ = self.get_datetime(),
                                age_lo_= age_lo,
                                age_hi_ = age_hi,
                                gender_ = gender)

            record = result.single()
            if record["a.brand_id"]:
                output["brand_id"] = record["a.brand_id"]
                print ("The brand id is", output["brand_id"])
                return True
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("The error is", e.message)
            return False

    def link_brand_to_subcategory(self, web_subcategory_id, brand_id):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:brand{brand_id:$brand_id_})," \
                    "(b:WebSubCat{web_subcategory_id:$web_subcategory_id_}) " \
                    " MERGE " \
                    " (a)-[r:APPLICABLE_TO]->(b)" \
                    " ON CREATE set r.created_dt = $created_dt_" \
                    " ON MATCH set r.updated_dt = $updated_dt_ " \
                    " RETURN a.web_subcategory_id"
            result = driver.run(query,
                                web_subcategory_id_ = web_subcategory_id,
                                brand_id_ = brand_id,
                                created_dt_ = self.get_datetime() ,
                                updated_dt_ = self.get_datetime() )
            record = result.single()
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            if record is None:
                print ("No mapping")
                return False

            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("Error in executing the SQL" + e.message)
            print ("The error message is", e.message)
            return False
        except Exception as e:
            current_app.logger.error("Error in linking brand to subcategory" + e)
            return False

    # def get_web_category(self, friend_circle_id, output): No need for friend circle id with the new implementation.
    def get_web_category(self, output):
        try:
            driver = NeoDB.get_session()
            query = "match (a:WebCat) " \
                     "return a.web_category_id as web_category_id, a.web_category_name as web_category_name"
            #query = "match (a:WebCat) " \
            #        "where not exists { match (a)<-[r:INTEREST]-(u:User) where r.friend_circle_id = $friend_circle_id_ }  " \
            #         "return a.web_category_id as web_category_id, a.web_category_name as web_category_name"

            #result = driver.run(query, friend_circle_id_ = friend_circle_id)
            result = driver.run(query)
            if result is None:
                return False
            for record in result:
                output.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error message is ", e.message)
            return False
        except Exception as e:
            print ("The error message is ", e)
            return  False

    def get_web_subcategory(self, web_category_id, age_lo, age_hi, gender, output):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:WebSubCat)-[:SUBCATEGORY_OF]->(b:WebCat)" \
                    " WHERE b.web_category_id = $web_category_id_ " \
                    " AND toInteger(a.age_lo) >= $age_lo_ " \
                    " AND toInteger(a.age_hi) <= $age_hi_ " \
                    " AND a.gender = $gender_ " \
                    " RETURN a.web_subcategory_id, a.web_subcategory_name"
            result = driver.run(query, web_category_id_ = web_category_id, age_lo_ = age_lo, age_hi_ = age_hi, gender_ = gender)
            if result in None:
                return False
            for record in result:
                output[record["a.web_subcategory_id"]] = record["a.web_subcategory_name"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("The error message is ", e.message)
            return False

    def get_distinct_category_subcategory(self, output):
        try:
            driver = NeoDB.get_session()
            query = "match (a:WebCat), (b:WebSubCat)" \
                    " where a.web_category_id = b.parent_id " \
                     "return a.web_category_id as category_id, a.web_category_name as category_name," \
                    " b.web_subcategory_id as subcategory_id, b.web_subcategory_name as subcategory_name"
            #query = "match (a:WebCat) " \
            #        "where not exists { match (a)<-[r:INTEREST]-(u:User) where r.friend_circle_id = $friend_circle_id_ }  " \
            #         "return a.web_category_id as web_category_id, a.web_category_name as web_category_name"

            #result = driver.run(query, friend_circle_id_ = friend_circle_id)
            result = driver.run(query)
            if result is None:
                return False
            for record in result:
                output.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error message is ", e.message)
            return False
        except Exception as e:
            print ("The error message is ", e)
            return  False

    def get_brands(self, age_lo, age_hi, gender, output):
        try:
            print ("Into the brand function")
            driver = NeoDB.get_session()
            query = "MATCH (a:brand) " \
                    " WHERE " \
                    " toInteger(a.age_lo) >= $age_lo_ " \
                    " AND toInteger(a.age_hi) <= $age_hi_  " \
                    " AND a.gender = $gender_" \
                    " RETURN a.brand_id, a.brand_name"
            result = driver.run(query, age_hi_ = age_hi, age_lo_ = age_lo, gender_ = gender)
            if result is None:
                return False
            for record in result:
                output[record["a.brand_id"]] = record["a.brand_name"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("The error message is ", e.message)
            return False

    def get_web_subcategory_brands(self, lweb_subcategory_id, age_lo, age_hi, gender, output):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:WebSubCat)-[:SUBCATEGORY_OF]->(b:brand)" \
                    " WHERE a.web_subcategory_id in $lweb_subcategory_id_  " \
                    " AND  toInteger(a.age_lo) >= $age_lo_ " \
                    " AND toInteger(a.age_hi) <= $age_hi_ " \
                    " AND  a.gender = $gender_" \
                    " RETURN distinct b.brand_id, b.brand_name"
            result = driver.run(query, lweb_subcategory_id_ = lweb_subcategory_id, age_lo_= age_lo, age_hi_ = age_hi, gender_ = gender)
            if result in None:
                return False
            for record in result:
                output[record["a.brand_id"]] = record["a.brand_name"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            print ("The error message is ", e.message)
            return False

