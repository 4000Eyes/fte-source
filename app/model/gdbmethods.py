import json
import neo4j.exceptions
import logging
from flask import current_app, g
from flask_restful import Resource
from .extensions import NeoDB
import uuid
from app.model.mongodbfunc import MongoDBFunctions
import pymongo.collection
from datetime import datetime
from pymongo import errors
import collections
import pytz
from datetime import datetime, tzinfo, timedelta
from dateutil.relativedelta import relativedelta


# User object

# user_id
# email_address
# user_name
# location
# gender
# password
# birth date

class GDBUser(Resource):
    FAKE_USER_TYPE = 1
    FAKE_USER_PASSWORD = "TeX54Esa"

    def __init__(self):
        self.__dttime = None
        self.__uid = None

    def get_datetime(self):
        self.__dttime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return self.__dttime

    def get_id(self):
        self.__uid = str(uuid.uuid4())
        return self.__uid

    def get_user(self, email_address, output_hash):
        driver = NeoDB.get_session()
        if driver is None:
            current_app.logger.info("Driver to the database is not initiated")
            print("Driver is not initiated")
            return 0
        print("Inside the get user all function")
        try:

            query = "MATCH (u:User) " \
                    "WHERE u.email_address = $email_address_ " \
                    "RETURN u.user_id, u.user_type"
            results = driver.run(query, email_address_=email_address)
            if results is None:
                print("user does not exist")
                output_data = None
                return True
            for record in results:
                print("The user is", record)
                output_hash['user_id'] = record["u.user_id"]
                output_hash["user_type"] = record["u.user_type"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_hash['user_id'] = None
            output_hash["user_type"] = None
            return False

    def get_user_by_id(self, user_id, output_hash):
        driver = NeoDB.get_session()
        if driver is None:
            current_app.logger.info("Driver to the database is not initiated")
            print("Driver is not initiated")
            return 0
        output_hash["user_id"] = None
        try:
            query = "MATCH (u:User) " \
                    "WHERE u.user_id = $user_id_ " \
                    "RETURN u.email_address, u.user_id, u.user_type, u.first_name, u.last_name, u.phone_number, u.location, u.gender"
            results = driver.run(query, user_id_=user_id)
            if results is None:
                print("user does not exist")
                output_hash = None
                return True
            for record in results:
                print("The user is", record)
                output_hash["email_address"] = record["u.email_address"]
                output_hash["user_id"] = record["u.user_id"]
                output_hash["user_type"] = record["u.user_type"]
                output_hash["first_name"] = record["u.first_name"]
                output_hash["last_name"] = record["u.last_name"]
                output_hash["phone_number"] = record["u.phone_number"]
                output_hash["location"] = record["u.location"]
                output_hash["gender"] = record["u.gender"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_hash["email_address"] = None
            output_hash["user_id"] = None
            output_hash["user_type"] = None
            return False
        except Exception as e:
            print("THere is a syntax error", e)
            output_hash["email_address"] = None
            output_hash["user_id"] = None
            output_hash["user_type"] = None
            return False

    def get_user_by_phone(self, phone_number, output_data):

        try:
            driver = NeoDB.get_session()
            if driver is None:
                current_app.logger.info("Driver to the database is not initiated in get_user_by_phone")
                print("Driver is not initiated get_user_by_phone")
                return 0
            print("Inside the gdb user all function")
            query = "MATCH (u:User) " \
                    "WHERE u.phone_number = $phone_number_ " \
                    "RETURN u.user_id, u.email_address, u.user_type, u.first_name, u.last_name, u.image_url, " \
                    "u.phone_number, u.location, u.gender"

            results = driver.run(query, phone_number_=phone_number)
            if results is None:
                print("user does not exist")
                output_data = None
                return True
            for record in results:
                print("The user is", record)
                output_data["user_id"] = record["u.user_id"]
                output_data["phone_number"] = record["u.phone_number"]
                output_data["user_type"] = record["u.user_type"]
                output_data["first_name"] = record["u.first_name"]
                output_data["last_name"] = record["u.last_name"]
                output_data["email_address"] = record["u.email_address"]
                output_data["location"] = record["u.location"]
                output_data["gender"] = record["u.gender"]
                output_data["image_url"] = record["u.image_url"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_data["user_id"] = None
            output_data["phone_number"] = None
            output_data["user_type"] = None
            output_data["password"] = None
            return False
        except Exception as e:
            print("THere is a syntax error", e)
            output_data["user_id"] = None
            output_data["phone_number"] = None
            output_data["user_type"] = None
            output_data["password"] = None
            return False

    def get_user_by_email(self, email_address, output_data):

        try:
            output_data["user_id"] = None
            output_data["email_address"] = None
            output_data["user_type"] = None
            output_data["password"] = None
            driver = NeoDB.get_session()
            if driver is None:
                current_app.logger.info("Driver to the database is not initiated in get_user_by_phone")
                print("Driver is not initiated get_user_by_phone")
                return 0
            print("Inside the gdb user all function")
            query = "MATCH (u:User) " \
                    "WHERE u.email_address = $email_address_ " \
                    "RETURN u.user_id as user_id, u.email_address as email_address, u.user_type as user_type, " \
                    "u.password as password, u.first_name as first_name, u.last_name as last_name, u.gender as gender," \
                    "u.location as location, u.phone_number as phone_number"

            results = driver.run(query, email_address_=email_address)
            if results is None:
                print("user does not exist")
                return True
            for record in results:
                print("The user is", record)
                output_data["user_id"] = record["user_id"]
                output_data["email_address"] = record["email_address"]
                output_data["user_type"] = record["user_type"]
                output_data["password"] = record["password"]
                output_data["phone_number"] = record["phone_number"]
                output_data["first_name"] = record["first_name"]
                output_data["last_name"] = record["last_name"]
                output_data["location"] = record["location"]
                output_data["gender"] = record["gender"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_data["user_id"] = None
            output_data["email_address"] = None
            output_data["user_type"] = None
            output_data["password"] = None
            return False
        except Exception as e:
            print("THere is a syntax error", e)
            output_data["user_id"] = None
            output_data["email_address"] = None
            output_data["user_type"] = None
            output_data["password"] = None
            return False

    def insert_user(self, user_hash, output_hash):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            # Remember: Need the following in user hash
            # friend_id, external_referrer_id (optional), but the parameter should be there
            user_id = None
            output_hash["email_address"] = None
            output_hash["user_id"] = None
            loutput = []
            if not self.verify_friend_list(user_hash, txn, loutput):
                current_app.logger.error(
                    "Unable to verify the registration request against friend list for email_address" + user_hash.get(
                        "email_address"))
                txn.rollback
                return False

            for record in loutput:
                if "user_id" not in record:
                    break;
                if record["linked_user_id"] is not None:
                    output_hash[
                        "outcome"] = "User already exists for this email and phone combination. Route it to support"
                    output_hash["redirect"] = "Login"
                    current_app.logger.error(
                        "User already exists for this email and phone combination. Route it to support" + user_hash.get(
                            "email_address"))
                    return False

            if user_id is None:
                user_id = self.get_id()

            query = "CREATE (u:User) " \
                    " SET u.email_address = $email_address_, u.password = $password_, u.user_id = $user_id_, u.phone_number = $phone_number_, " \
                    " u.gender = $gender_, u.user_type = $user_type_, u.first_name=$first_name_, u.last_name=$last_name_, u.location = $location_," \
                    " u.mongo_indexed = $mongo_indexed_, u.image_url = $image_url_" \
                    " RETURN u.email_address, u.user_id"

            result = txn.run(query, email_address_=str(user_hash.get('email_address')),
                             password_=str(user_hash.get('password')),
                             user_id_=str(user_id),
                             gender_=user_hash.get("gender"),
                             phone_number_=user_hash.get("phone_number"),
                             user_type_=user_hash.get("user_type"),
                             first_name_=user_hash.get("first_name"),
                             last_name_=user_hash.get("last_name"),
                             location_=user_hash.get("location"),
                             external_referrer_id=user_hash.get("external_referrer_id"),
                             external_referrer_param=user_hash.get("external_referrer_param"),
                             mongo_indexed_=user_hash.get("mongo_indexed"),
                             image_url_ = user_hash.get("image_url"))
            record = result.single()
            info = result.consume().counters.nodes_created
            if info > 0 and record is not None:
                print("The user id is", record["u.user_id"])
                output_hash["email_address"] = record["u.email_address"]
                output_hash["user_id"] = record["u.user_id"]

            if not self.update_friendlist(loutput, user_id, txn):
                txn.rollback()
                current_app.logger.error("We have an issue processing the registration request. Unable to friend list")
                print("We have an issue processing the registration request. Unable to friend list")
                return False
            objMongo = MongoDBFunctions()
            user_hash["referrer_user_id"] = None
            user_hash["user_id"] = output_hash["user_id"]
            if not objMongo.insert_user(user_hash):
                current_app.logger.error("Unable to insert the user into search db" + user_hash.get("email_address"))
                txn.rollback()
                return False

            output_hash["mongo_indexed"] = "Y"

            if not self.update_user(output_hash, txn):
                txn.rollback()
                print("Error in updating the user")
                return {"status": "Failure in updating the inserted record in the same transaction"}, 400
                return False

            hshOutput = {}
            if not self.get_user_summary(user_id, txn, hshOutput):
                txn.rollback()
                current_app.logger.error(
                    "Unable to get the friend circle data for user" + user_hash.get("email_address"))
                return {"status": "Failure"}, 400

            txn.commit()
            return True
        except neo4j.exceptions.Neo4jError as e:
            txn.rollback()
            current_app.logger.error("There is a error " + e.message)
            return False
        except errors.PyMongoError as e:
            txn.rollback
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            txn.rollback
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def insert_user_by_phone(self, user_hash, output_hash):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            # Remember: Need the following in user hash
            # friend_id, external_referrer_id (optional), but the parameter should be there
            user_id = None
            output_hash["phone_number"] = None
            output_hash["user_id"] = None
            loutput = []
            if not self.verify_friend_list_by_phone(user_hash, txn, loutput):
                current_app.logger.error(
                    "Unable to verify the registration request against friend list for phone_number" + user_hash.get(
                        "phone_number"))
                txn.rollback
                return False

            for record in loutput:
                if "user_id" not in record:
                    break;
                if record["linked_user_id"] is not None:
                    output_hash[
                        "outcome"] = "User already exists for this email and phone combination. Route it to support"
                    output_hash["redirect"] = "Login"
                    current_app.logger.error(
                        "User already exists for this email and phone combination. Route it to support" + user_hash.get(
                            "phone_number"))
                    return False

            if user_id is None:
                user_id = self.get_id()

            query = "CREATE (u:User) " \
                    " SET u.email_address = $email_address_, u.password = $password_, u.user_id = $user_id_, u.phone_number = $phone_number_, " \
                    " u.gender = $gender_, u.user_type = $user_type_, u.first_name=$first_name_, u.last_name=$last_name_, u.location = $location_," \
                    " u.mongo_indexed = $mongo_indexed_, u.image_url = $image_url_" \
                    " RETURN u.phone_number, u.user_id"

            result = txn.run(query, email_address_=str(user_hash.get('email_address')),
                             password_=str(user_hash.get('password')),
                             user_id_=str(user_id),
                             gender_=user_hash.get("gender"),
                             phone_number_=user_hash.get("phone_number"),
                             user_type_=user_hash.get("user_type"),
                             first_name_=user_hash.get("first_name"),
                             last_name_=user_hash.get("last_name"),
                             location_=user_hash.get("location"),
                             external_referrer_id=user_hash.get("external_referrer_id"),
                             external_referrer_param=user_hash.get("external_referrer_param"),
                             mongo_indexed_=user_hash.get("mongo_indexed"),
                             image_url_ = user_hash.get("image_url"))
            record = result.single()
            info = result.consume().counters.nodes_created
            if info > 0 and record is not None:
                print("The user id is", record["u.user_id"])
                output_hash["phone_number"] = record["u.phone_number"]
                output_hash["user_id"] = record["u.user_id"]

            if not self.update_friendlist(loutput, user_id, txn):
                txn.rollback()
                current_app.logger.error("We have an issue processing the registration request. Unable to friend list")
                print("We have an issue processing the registration request. Unable to friend list")
                return False
            objMongo = MongoDBFunctions()
            user_hash["referrer_user_id"] = None
            user_hash["user_id"] = output_hash["user_id"]
            if not objMongo.insert_user(user_hash):
                current_app.logger.error("Unable to insert the user into search db" + user_hash.get("email_address"))
                txn.rollback()
                return False

            output_hash["mongo_indexed"] = "Y"

            if not self.update_user(output_hash, txn):
                txn.rollback()
                print("Error in updating the user")
                return {"status": "Failure in updating the inserted record in the same transaction"}, 400
                return False

            hshOutput = {}
            if not self.get_user_summary(user_id, txn, hshOutput):
                txn.rollback()
                current_app.logger.error(
                    "Unable to get the friend circle data for user" + user_hash.get("email_address"))
                return {"status": "Failure"}, 400

            txn.commit()
            return True
        except neo4j.exceptions.Neo4jError as e:
            txn.rollback()
            current_app.logger.error("There is a error " + e.message)
            return False
        except errors.PyMongoError as e:
            txn.rollback
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            txn.rollback
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def delete_user(self, user_id):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (u:User{user_id:$user_id_}) " \
                    " WITH u , count(u) as c" \
                    " DELETE u return c"
            result = driver.run(query, user_id_=user_id)
            if result.peek() is None:
                return False
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False

    def update_user(self, user_hash, txn):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (u:User{user_id:$user_id_}) " \
                    "SET " \
                    " u.mongo_indexed = $mongo_indexed_ " \
                    " return count(u.user_id) as user_count"
            result = txn.run(query, user_id_=user_hash.get("user_id"), mongo_indexed_=user_hash.get("mongo_indexed"))
            record = result.single()
            if record is not None:
                if record["user_count"] > 0:
                    return True
                return False
            else:
                return False
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False

    def update_user_password(self, user_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (u:User{user_id:$user_id_}) " \
                    "SET " \
                    " u.password = $password_ " \
                    " return count(u.user_id) as user_count"
            result = driver.run(query, user_id_=user_hash.get("user_id"), password_=user_hash.get("password"))
            record = result.single()
            if record is not None:
                if record["user_count"] > 0:
                    return True
                return False
            else:
                return False
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False

    def activate_user(self, user_id):
        try:
            driver = NeoDB.get_session()
            query = f"MATCH (u:User{user_id:$user_id}) " \
                    "SET " \
                    " u.user_status = 1 " \
                    " return count(u.user_id) as user_count"
            result = driver.run(query, user_id_=user_id)
            record = result.single()
            if record is not None:
                if record["user_count"] > 0:
                    return True
                return False
            else:
                return False
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False

    def verify_friend_list_by_phone(self, input_hash, txn, loutput):
        try:

            fe_query = "MATCH (a:friend_list) " \
                       " WHERE a.phone_number = $phone_number_  " \
                       " return a.user_id as user_id, a.friend_id as friend_id, a.linked_user_id as linked_user_id, a.linked_status_id as linked_status_id"

            if input_hash["phone_number"] is not None:
                result = txn.run(fe_query, phone_number_=input_hash["phone_number"])
            for record in result:
                r = {}
                r["user_id"] = record["user_id"]
                r["friend_id"] = record["friend_id"]
                r["linked_user_id"] = record["linked_user_id"]
                r["linked_status_id"] = record["linked_status_id"]
                loutput.append(r)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def verify_friend_list(self, input_hash, txn, loutput):
        try:

            fe_query = "MATCH (a:friend_list) " \
                       " WHERE a.email_address = $email_address_  " \
                       " return a.user_id, a.friend_id, a.linked_user_id, a.linked_status_id "

            fep_query = "MATCH (a:friend_list) " \
                        " WHERE a.email_address = $email_address_  " \
                        " return a.user_id, as user_id," \
                        " a.friend_id as friend_id, " \
                        "a.linked_user_id as linked_user_id, " \
                        "a.linked_status_id as linked_status_id"

            fp_query = "MATCH (a:friend_list) " \
                       " WHERE a.phone_number = $phone_number_ " \
                       " return a.user_id, a.friend_id, a.linked_user_id, a.linked_status_id"

            # if input_hash["email_address"] is not None and input_hash["phone_number"] is not None:
            #     result = txn.run(fep_query, email_address_=input_hash["email_address"],
            #                      phone_number_=input_hash["phone_number"])
            # elif input_hash["email_address"] is not None:
            #     result = txn.run(fe_query, email_address_=input_hash["email_address"])
            # elif input_hash["phone_number"] is not None:
            #     result = txn.run(fp_query, phone_number_=input_hash["phone_number"])

            if input_hash["email_address"] is not None:
                result = txn.run(fe_query, email_address_=input_hash["email_address"])
            for record in result:
                r = {}
                r["user_id"] = record["a.user_id"]
                r["friend_id"] = record["a.friend_id"]
                r["linked_user_id"] = record["a.linked_user_id"]
                r["linked_status_id"] = record["a.linked_status_id"]
                loutput.append(r)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def update_friendlist(self, linput, user_id, txn):
        try:

            update_fl_query = "MATCH (fl:friend_list{friend_id:$friend_id_,user_id : $user_id_ }) " \
                              "SET fl.linked_user_id = $linked_user_id_, " \
                              " fl.linked_status_id = 1" \
                              " RETURN fl.user_id"

            for record in linput:
                result = txn.run(update_fl_query, friend_id_=record["friend_id"], user_id_=record["user_id"],
                                 linked_user_id_=user_id)
                if result is None:
                    current_app.logger.error("Unable to update the friend list record with user_id ", record["user_id"])
                    print("Unable to update the friend list record with user_id ", record["user_id"])
                    return False
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False
        except errors.PyMongoError as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    # friend circle object definition

    # friend_circle_id
    # friend_circle_name
    # friend_circle_created_dt
    # friend_circle_updated_dt

    def add_contributor_to_friend_circle__by_email(self, user_id, email_address, friend_circle_id, output):
        try:
            driver = NeoDB.get_session()
            query = "MATCH  (n:friend_list)-[rr]->(x:friend_circle) " \
                    " WHERE n.user_id = $user_id_ " \
                    " AND n.email_address = $email_address_ " \
                    " AND x.friend_circle_id_ = $friend_circle_id " \
                    " AND type(rr) = 'CONTRIBUTOR' " \
                    " CREATE (n)-[:CONTRIBUTOR]->(x:friend_circle) " \
                    " RETURN x.friend_circle_id as friend_circle_id"

            result = driver.run(query, user_id_=user_id, email_address_=email_address,
                                friend_circle_id_=friend_circle_id)
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)

            if result.peek() is None:
                output = []
                return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            friend_circle_id[0] = None
            return False

    def get_user_roles_for_referrer(self, referrer_user_id, friend_circle_id, hshOutput):
        try:
            driver = NeoDB.get_session()
            result = None
            query = " call { MATCH (n:User)-[rr]->(x:friend_circle) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ AND" \
                    " n.user_id = $user_id_ " \
                    " RETURN  n.user_id as user_id, n.first_name as first_name, " \
                    " n.last_name as last_name, n.gender as gender, " \
                    " type(rr) as relationship " \
                    " UNION " \
                    "MATCH (x:friend_circle)-[rr]->(m:friend_list) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ AND " \
                    " m.user_id =  $user_id_  " \
                    " RETURN  m.user_id as user_id, m.first_name as first_name, " \
                    " m.last_name as last_name, m.gender as gender, " \
                    " type(rr) as relationship" \
                    " UNION " \
                    "MATCH (x:friend_circle)<-[rr]-(m:friend_list) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ AND " \
                    " m.user_id = $user_id_ " \
                    " RETURN  m.user_id as user_id, m.first_name as first_name, " \
                    " m.last_name as last_name, m.gender as gender, " \
                    " type(rr) as relationship " \
                    " } " \
                    " return " \
                    " user_id ," \
                    " CASE when relationship = 'CONTRIBUTOR'  then 'Y' else 'N' end as contrib_flag, " \
                    " CASE when relationship = 'SECRET_FRIEND'  then 'Y' else 'N'  end as secret_friend_flag, " \
                    " CASE when relationship = 'CIRCLE_CREATOR'  then 'Y' else 'N' end  as circle_creator_flag "

            result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=referrer_user_id)
            hshOutput["user_id"] = None
            counter = 0
            user_id = None
            for record in result:
                if counter == 0 or record["user_id"] != user_id:
                    user_id = record["user_id"]
                    hshOutput[record["user_id"]] = collections.defaultdict(dict)
                    hshOutput[record["user_id"]]["contrib_flag"] = "N"
                    hshOutput[record["user_id"]]["secret_friend_flag"] = "N"
                    hshOutput[record["user_id"]]["circle_creator_flag"] = "N"
                    counter = 1
                if hshOutput[record["user_id"]]["contrib_flag"] == "N" and record["contrib_flag"] == "Y":
                    hshOutput[record["user_id"]]["contrib_flag"] = record["contrib_flag"]
                else:
                    hshOutput[record["user_id"]]["contrib_flag"] = "N"

                if hshOutput[record["user_id"]]["secret_friend_flag"] == "N" and record["secret_friend_flag"] == "Y":
                    hshOutput[record["user_id"]]["secret_friend_flag"] = record["secret_friend_flag"]
                else:
                    hshOutput[record["user_id"]]["secret_friend_flag"] = "N"

                if hshOutput[record["user_id"]]["circle_creator_flag"] == "N" and record["circle_creator_flag"] == "Y":
                    hshOutput[record["user_id"]]["circle_creator_flag"] = record["circle_creator_flag"]
                else:
                    hshOutput[record["user_id"]]["circle_creator_flag"] = "N"
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            hshOutput["user_id"] = None
            return False
        except Exception as e:
            hshOutput["user_id"] = None
            current_app.logger.error(e)
            return False

    def get_user_roles(self, referred_user_id, referrer_user_id, friend_circle_id, hshOutput):
        try:
            driver = NeoDB.get_session()
            result = None

            query = " call { MATCH (n:User)-[rr]->(x:friend_circle) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ AND" \
                    " n.user_id in [ $user_id_ , $friend_id_ ] " \
                    " RETURN  n.user_id as user_id, n.first_name as first_name, " \
                    " n.last_name as last_name, n.gender as gender, " \
                    " type(rr) as relationship " \
                    " UNION " \
                    "MATCH (x:friend_circle)-[rr]->(m:friend_list) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ AND " \
                    " m.user_id in [ $user_id_ , $friend_id_ ] " \
                    " RETURN  m.user_id as user_id, m.first_name as first_name, " \
                    " m.last_name as last_name, m.gender as gender, " \
                    " type(rr) as relationship" \
                    " UNION " \
                    "MATCH (x:friend_circle)<-[rr]-(m:friend_list) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ AND " \
                    " m.user_id in [ $user_id_ , $friend_id_ ] " \
                    " RETURN  m.user_id as user_id, m.first_name as first_name, " \
                    " m.last_name as last_name, m.gender as gender, " \
                    " type(rr) as relationship " \
                    " } " \
                    " return " \
                    " user_id ," \
                    " CASE when relationship = 'CONTRIBUTOR'  then 'Y' else 'N' end as contrib_flag, " \
                    " CASE when relationship = 'SECRET_FRIEND'  then 'Y' else 'N'  end as secret_friend_flag, " \
                    " CASE when relationship = 'CIRCLE_CREATOR'  then 'Y' else 'N' end  as circle_creator_flag "

            result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=referred_user_id,
                                friend_id_=referrer_user_id)
            counter = 0
            user_id = None
            for record in result:
                if counter == 0 or record["user_id"] != user_id:
                    user_id = record["user_id"]
                    hshOutput[record["user_id"]] = collections.defaultdict(dict)
                    hshOutput[record["user_id"]]["contrib_flag"] = "N"
                    hshOutput[record["user_id"]]["secret_friend_flag"] = "N"
                    hshOutput[record["user_id"]]["circle_creator_flag"] = "N"
                    counter = 1

                if hshOutput[record["user_id"]]["contrib_flag"] == "N" and record["contrib_flag"] == "Y":
                    hshOutput[record["user_id"]]["contrib_flag"] = record["contrib_flag"]
                else:
                    hshOutput[record["user_id"]]["contrib_flag"] = "N"

                if hshOutput[record["user_id"]]["secret_friend_flag"] == "N" and record["secret_friend_flag"] == "Y":
                    hshOutput[record["user_id"]]["secret_friend_flag"] = record["secret_friend_flag"]
                else:
                    hshOutput[record["user_id"]]["secret_friend_flag"] = "N"

                if hshOutput[record["user_id"]]["circle_creator_flag"] == "N" and record["circle_creator_flag"] == "Y":
                    hshOutput[record["user_id"]]["circle_creator_flag"] = record["circle_creator_flag"]
                else:
                    hshOutput[record["user_id"]]["circle_creator_flag"] = "N"
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            hshOutput.clear()
            return False
        except Exception as e:
            current_app.logger.error(e)
            hshOutput.clear()
            return False

    # def get_user_role_as_contrib_secret_friend(self, email_address, referrer_user_id, friend_circle_id, hshOutput): # phone primary key support
    def get_user_role_as_contrib_secret_friend(self, phone_number, referrer_user_id, friend_circle_id, hshOutput):
        try:
            driver = NeoDB.get_session()
            result = None

            # phone primary key support

            # query = " call { " \
            #         "MATCH (x:friend_circle)-[rr]->(m:friend_list) " \
            #         "WHERE x.friend_circle_id = $friend_circle_id_ AND " \
            #         " m.email_address = $email_address_ AND " \
            #         " m.friend_id = $friend_id_ AND " \
            #         " m.source_type = 'DIRECT' " \
            #         " RETURN  m.email_address as email_address, m.user_id as user_id, m.friend_id as friend_id, " \
            #         " type(rr) as relationship" \
            #         " UNION " \
            #         "MATCH (x:friend_circle)<-[rr]-(m:friend_list) " \
            #         "WHERE x.friend_circle_id = $friend_circle_id_ AND " \
            #         " m.email_address = $email_address_ AND" \
            #         " m.friend_id = $friend_id_ AND " \
            #         " m.source_type = 'DIRECT' " \
            #         " RETURN  m.email_address as email_address, m.user_id as user_id, m.friend_id as friend_id, " \
            #         " type(rr) as relationship " \
            #         " } " \
            #         " return " \
            #         " user_id ," \
            #         " CASE when relationship = 'CONTRIBUTOR'  then 'Y' else 'N' end as contrib_flag, " \
            #         " CASE when relationship = 'SECRET_FRIEND'  then 'Y' else 'N'  end as secret_friend_flag, " \
            #         " CASE when relationship = 'CIRCLE_CREATOR'  then 'Y' else 'N' end  as circle_creator_flag "

            query = " call { " \
                    "MATCH (x:friend_circle)-[rr]->(m:friend_list) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ AND " \
                    " m.phone_number = $phone_number_ AND " \
                    " m.friend_id = $friend_id_ AND " \
                    " m.source_type = 'DIRECT' " \
                    " RETURN  m.phone_number as phone_number, m.user_id as user_id, m.friend_id as friend_id, " \
                    " type(rr) as relationship" \
                    " UNION " \
                    "MATCH (x:friend_circle)<-[rr]-(m:friend_list) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ AND " \
                    " m.phone_number = $phone_number_ AND" \
                    " m.friend_id = $friend_id_ AND " \
                    " m.source_type = 'DIRECT' " \
                    " RETURN  m.phone_number as phone_number, m.user_id as user_id, m.friend_id as friend_id, " \
                    " type(rr) as relationship " \
                    " } " \
                    " return " \
                    " user_id ," \
                    " CASE when relationship = 'CONTRIBUTOR'  then 'Y' else 'N' end as contrib_flag, " \
                    " CASE when relationship = 'SECRET_FRIEND'  then 'Y' else 'N'  end as secret_friend_flag, " \
                    " CASE when relationship = 'CIRCLE_CREATOR'  then 'Y' else 'N' end  as circle_creator_flag "

            result = driver.run(query, friend_circle_id_=friend_circle_id, phone_number_=phone_number,
                                friend_id_=referrer_user_id)
            counter = 0
            user_id = None
            for record in result:
                if counter == 0 or record["user_id"] != user_id:
                    user_id = record["email_address"]

                    # phone primary key support

                    # hshOutput[record["email_address"]] = collections.defaultdict(dict)
                    # hshOutput[record["email_address"]]["contrib_flag"] = "N"
                    # hshOutput[record["email_address"]]["secret_friend_flag"] = "N"
                    # hshOutput[record["email_address"]]["circle_creator_flag"] = "N"
                    # hshOutput[record["email_address"]]["user_id"] = None
                    # hshOutput[record["email_address"]]["friend_id"] = None

                    hshOutput[record["phone_number"]] = collections.defaultdict(dict)
                    hshOutput[record["phone_number"]]["contrib_flag"] = "N"
                    hshOutput[record["phone_number"]]["secret_friend_flag"] = "N"
                    hshOutput[record["phone_number"]]["circle_creator_flag"] = "N"
                    hshOutput[record["phone_number"]]["user_id"] = None
                    hshOutput[record["phone_number"]]["friend_id"] = None

                    counter = 1

                if hshOutput[record["user_id"]]["user_id"] is None:
                    hshOutput[record["user_id"]]["user_id"] = record["user_id"]
                    hshOutput[record["user_id"]]["friend_id"] = record["friend_id"]

                if hshOutput[record["user_id"]]["contrib_flag"] == "N" and record["contrib_flag"] == "Y":
                    hshOutput[record["user_id"]]["contrib_flag"] = record["contrib_flag"]
                else:
                    hshOutput[record["user_id"]]["contrib_flag"] = "N"

                if hshOutput[record["user_id"]]["secret_friend_flag"] == "N" and record["secret_friend_flag"] == "Y":
                    hshOutput[record["user_id"]]["secret_friend_flag"] = record["secret_friend_flag"]
                else:
                    hshOutput[record["user_id"]]["secret_friend_flag"] = "N"

                if hshOutput[record["user_id"]]["circle_creator_flag"] == "N" and record["circle_creator_flag"] == "Y":
                    hshOutput[record["user_id"]]["circle_creator_flag"] = record["circle_creator_flag"]
                else:
                    hshOutput[record["user_id"]]["circle_creator_flag"] = "N"
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            hshOutput.clear()
            return False
        except Exception as e:
            current_app.logger.error(e)
            hshOutput.clear()
            return False

    def get_friend_circle(self, friend_circle_id, loutput):
        driver = NeoDB.get_session()
        result = None
        try:
            # made changes on 01/11/2022. Changed the entire query.

            query = " match (ff:friend_list)<-[rr]->(fc:friend_circle)<-[:CIRCLE_CREATOR]-(y:User) " \
                     "  WHERE fc.friend_circle_id = $friend_circle_id_"\
                     " return " \
                     " ff.user_id as user_id," \
                     " ff.first_name as first_name," \
                     " ff.last_name as last_name," \
                     " ff.gender as gender," \
                     " type(rr) as relationship, " \
                     " fc.friend_circle_id as friend_circle_id," \
                     " fc.friend_circle_name as friend_circle_name," \
                     " fc.secret_friend_id as secret_friend_id," \
                     " fc.secret_first_name as secret_first_name," \
                     " fc.secret_last_name as secret_last_name," \
                     " fc.image_url as image_url," \
                     " y.user_id as creator_id," \
                     " y.first_name as creator_first_name," \
                     " y.last_name as creator_last_name"

            # query = "MATCH (n:User)-[rr]->(x:friend_circle) " \
            #         "WHERE x.friend_circle_id = $friend_circle_id_ " \
            #         " RETURN  n.user_id as user_id, n.first_name as first_name, " \
            #         " n.last_name as last_name, n.gender as gender, " \
            #         " type(rr) as relationship, x.image_url as image_url " \
            #         " UNION " \
            #         "MATCH (x:friend_circle)-[rr]->(m:friend_list) " \
            #         "WHERE x.friend_circle_id = $friend_circle_id_ " \
            #         " RETURN  m.user_id as user_id, m.first_name as first_name, " \
            #         " m.last_name as last_name, m.gender as gender, " \
            #         " type(rr) as relationship , x.image_url as image_url" \
            #         " UNION " \
            #         "MATCH (x:friend_circle)<-[rr]-(m:friend_list) " \
            #         "WHERE x.friend_circle_id = $friend_circle_id_" \
            #         " RETURN  m.user_id as user_id, m.first_name as first_name, " \
            #         " m.last_name as last_name, m.gender as gender, " \
            #         " type(rr) as relationship, x.image_url as image_url"

            result = driver.run(query, friend_circle_id_=friend_circle_id)
            for record in result:
                loutput.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            return False

    def get_friend_circles(self, user_id, loutput):
        driver = NeoDB.get_session()
        result = None
        # made changes on 1/11/2022. Changed the query.
        try:
            query =  " match (ff:friend_list)<-[rr]->(fc:friend_circle)"\
                     " call { "\
                     " with fc "\
                     " match (x:User)-[:CIRCLE_CREATOR]->(xf:friend_circle) "\
                     " where x.user_id = $user_id_ "\
                    " and fc.friend_circle_id = xf.friend_circle_id "\
                     " return xf.friend_circle_id " \
                     " union "\
                     " with fc "\
                     " match (pp:friend_list)<-[]->(xf:friend_circle) "\
                     " where ( pp.user_id = $user_id_ or pp.user_id = $user_id_ ) and " \
                     " fc.secret_friend_id <> $user_id_ "\
                     " and pp.application_status in [0,1] "\
                     " and xf.friend_circle_id = fc.friend_circle_id "\
                     " return xf.friend_circle_id "\
                     " } "\
                     " return " \
                     " ff.user_id as user_id," \
                     " ff.first_name as first_name," \
                     " ff.last_name as last_name," \
                     " ff.gender as gender," \
                     " type(rr) as relationship, " \
                     " fc.friend_circle_id as friend_circle_id," \
                     " fc.friend_circle_name as friend_circle_name," \
                     " fc.secret_friend_id as secret_friend_id," \
                     " fc.secret_first_name as secret_first_name," \
                     " fc.secret_last_name as secret_last_name," \
                     " fc.image_url as image_url"

            # query = "MATCH (n:User)-[rr]->(x:friend_circle) " \
            #         "WHERE  n.user_id = $user_id_  and " \
            #         " x.secret_friend_id <> $user_id_ " \
            #         " RETURN  n.user_id as user_id, n.first_name as first_name, " \
            #         " n.last_name as last_name, n.gender as gender, " \
            #         " type(rr) as relationship, x.friend_circle_id as friend_circle_id, x.friend_circle_name as friend_circle_name," \
            #         " x.secret_friend_id as secret_friend_id, x.secret_first_name as secret_first_name, x.secret_last_name as secret_last_name, x.image_url as image_url" \
            #         " UNION " \
            #         "MATCH (x:friend_circle)-[rr]->(m:friend_list) " \
            #         "WHERE ( m.friend_id = $user_id_ or m.linked_user_id = $user_id_ ) AND " \
            #         " x.secret_friend_id <> $user_id_ " \
            #         " RETURN  m.user_id as user_id, m.first_name as first_name, " \
            #         " m.last_name as last_name, m.gender as gender, " \
            #         " type(rr) as relationship, x.friend_circle_id as friend_circle_id, x.friend_circle_name as friend_circle_name," \
            #         " x.secret_friend_id as secret_friend_id, x.secret_first_name as secret_first_name, x.secret_last_name as secret_last_name, x.image_url as image_url" \
            #         " UNION " \
            #         "MATCH (x:friend_circle)<-[rr]-(m:friend_list) " \
            #         "WHERE ( m.friend_id = $user_id_ or m.linked_user_id = $user_id_ )  AND " \
            #         " x.secret_friend_id <> $user_id_ " \
            #         " RETURN  m.user_id as user_id, m.first_name as first_name, " \
            #         " m.last_name as last_name, m.gender as gender, " \
            #         " type(rr) as relationship, x.friend_circle_id as friend_circle_id, x.friend_circle_name as friend_circle_name, " \
            #         " x.secret_friend_id as secret_friend_id, x.secret_first_name as secret_first_name, x.secret_last_name as secret_last_name, x.image_url as image_url" \
            #         " ORDER BY x.friend_circle_id, m.user_id"

            result = driver.run(query, user_id_=user_id)
            counter = 0

            for record in result:
                loutput.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            return False

    def get_friend_circle_attributes(self, friend_circle_id, hsh):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (fc:friend_circle) " \
                    " WHERE fc.friend_circle_id = $friend_circle_id_" \
                    " RETURN fc.friend_circle_id as friend_circle_id, " \
                    "fc.age as age, " \
                    " fc.gender as gender," \
                    " fc.created_dt as created_dt," \
                    " fc.creator_id as creator_id, " \
                    " fc.secret_friend_id as secret_friend_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            for record in result:
                hsh["friend_circle_id"] = record["friend_circle_id"]
                hsh["gender"] = record["gender"]
                hsh["age"] = record["age"]
                hsh["creator_id"] = record["creator_id"]
                hsh["secret_friend_id"] = record["secret_friend_id"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            return False

    def check_user_in_friend_circle(self, referred_user_id, friend_circle_id, hshOutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:friend_list)<-[rr]->(fc:friend_circle)" \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ AND" \
                    " (n.user_id= $referred_user_id_ or n.linked_user_id=$referred_user_id_) " \
                    " RETURN count(n) as user_exists, type(rr) as relation_type " \
                    " UNION " \
                    "MATCH (n:User)-[rr]->(fc:friend_circle)" \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ AND" \
                    " n.user_id= $referred_user_id_  " \
                    " RETURN count(n) as user_exists, type(rr) as relation_type"
            result = driver.run(query, friend_circle_id_=friend_circle_id, referred_user_id_=referred_user_id)
            for record in result:
                print("The user is ", record["user_exists"])
                hshOutput["user_count"] = record["user_exists"]
                hshOutput["relation_type"] = record["relation_type"]
                hshOutput["user_exists"] = record["user_exists"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            hshOutput["user_exists"] = None
            return False
        except Exception as e:
            hshOutput["user_exists"] = None
            current_app.logger.error(e)
            return False

    def check_user_in_friend_circle_by_email(self, email_address, referrer_user_id, friend_circle_id, output_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:friend_list)-[rr]->(fc:friend_circle)" \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ AND" \
                    " n.email_address= $email_address_  AND " \
                    " (type(rr) = 'CIRCLE_CREATOR' OR type(rr) = 'CONTRIBUTOR')  and " \
                    " n.friend_id = $referrer_user_id_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_=email_address,
                                referrer_user_id_=referrer_user_id)
            record = result.single()
            if record is not None:
                print("The user is ", record["user_exists"])
                output_hash["user_exists"] = record["user_exists"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            output_hash["user_exists"] = None
            current_app.logger.error(e.message)
            return False
        except Exception as e:
            output_hash["user_exists"] = None
            current_app.logger.error(e)
            return False

    def check_user_is_secret_friend(self, referred_user_id, referrer_user_id, friend_circle_id, hshOutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:friend_circle)-[:SECRET_FRIEND]->(n:friend_list)" \
                    " WHERE x.friend_circle_id = $friend_circle_id_ AND" \
                    " n.user_id= $referred_user_id_ " \
                    " AND n.friend_id = $referrer_user_id_" \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, referrer_user_id_=referrer_user_id,
                                referred_user_id_=referred_user_id)
            hshOutput["user_exists"] = 0
            for record in result:
                hshOutput["user_exists"] = record["user_exists"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            hshOutput["user_exists"] = None
            current_app.logger.error(e.message)
            return False
        except Exception as e:
            hshOutput["user_exists"] = None
            current_app.logger.error(e)
            return False

    def check_user_is_secret_friend_by_email(self, email_address, referrer_user_id, friend_circle_id, output_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (fc:friend_circle)-[:SECRET_FRIEND]->(n:friend_list)" \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ AND" \
                    " n.email_address= $email_address_ " \
                    " AND n.friend_id = $referrer_user_id_" \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, referrer_user_id_=referrer_user_id,
                                email_address_=email_address)
            record = result.single()
            if record is not None:
                print("The user is ", record["user_exists"])
                output_hash["user_exists"] = record["user_exists"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            output_hash["user_exists"] = None
            print(e.message)
            current_app.logger.error(e.message)
            return False
        except Exception as e:
            output_hash["user_exists"] = None
            current_app.logger.error(e)
            return False

    def check_user_is_admin(self, user_id, friend_circle_id, hshOutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:friend_circle)<-[:CIRCLE_CREATOR]-(n:User)" \
                    " WHERE x.friend_circle_id = $friend_circle_id_ AND" \
                    " n.user_id= $user_id_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=user_id)
            record = result.single()
            if record is not None:
                print("The user is ", record["user_exists"])
                hshOutput["user_exists"] = record["user_exists"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            hshOutput["user_exists"] = None
            return False
        except Exception as e:
            hshOutput["user_exists"] = None
            current_app.logger.error("function name : check user is admin " + e)
            print(e)
            return False

    def check_user_is_admin_by_email(self, email_address, friend_circle_id, output_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (fc:friend_circle)-[:CIRCLE_CREATOR]->(n:User)" \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ AND" \
                    " n.email_address= $email_address_  " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_=email_address)
            for record in result:
                print("The user is ", record["user_exists"])
                output_hash["user_exists"] = record["user_exists"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            output_hash["user_exists"] = None
            print(e.message)
            current_app.logger.error(e.message)
            return False
        except Exception as e:
            output_hash["user_exists"] = None
            current_app.logger.error(e)
            return False

    def check_friend_circle_with_admin_and_secret_friend(self, friend_user_id, secret_user_id, output_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:User)-[:CIRCLE_CREATOR]->(fc:friend_circle)-[SECRET_FRIEND]->(y:friend_list)" \
                    " WHERE n.user_id = $friend_user_id_ AND" \
                    " y.user_id = $friend_user_id_  AND" \
                    " y.friend_id = $secret_friend_id_ " \
                    " RETURN count(fc.friend_circle_id) as user_exists"
            result = driver.run(query, friend_user_id_=friend_user_id, secret_friend_id_=secret_user_id)
            record = result.single()
            if record is not None:
                print("The user is ", record["user_exists"])
                output_hash["user_exists"] = record["user_exists"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)

            return False

    def check_friend_circle_with_admin_and_secret_friend_by_email(self, friend_user_id, secret_email_address,
                                                                  output_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:User)-[:CIRCLE_CREATOR]->(fc:friend_circle)-[SECRET_FRIEND]->(y:friend_circle)" \
                    " WHERE n.user_id = $friend_user_id_ " \
                    " AND y.friend_id = $friend_user_id_ and" \
                    " y.email_address = $email_address_  " \
                    " RETURN count(fc.friend_circle_id) as user_exists"
            result = driver.run(query, friend_user_id_=friend_user_id, email_address_=secret_email_address)
            record = result.single()
            if record is not None:
                print("The user is ", record["user_exists"])
                output_hash["user_exists"] = record["user_exists"]
            else:
                current_app.logger.error(
                    "Checking if the user is secret friend cannot return an empty cursor " + secret_email_address)
                print("Checking if the user is secret friend cannot return an empty cursor " + secret_email_address)
                return False
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            return False

    def check_friend_circle_with_admin_and_secret_friend_by_phone(self, friend_user_id, secret_phone_number,
                                                                  output_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:User)-[:CIRCLE_CREATOR]->(fc:friend_circle)-[SECRET_FRIEND]->(y:friend_list)" \
                    " WHERE n.user_id = $friend_user_id_ " \
                    " AND y.friend_id = $friend_user_id_ and" \
                    " y.phone_number = $phone_number_  " \
                    " RETURN count(fc.friend_circle_id) as user_exists"
            result = driver.run(query, friend_user_id_=friend_user_id, phone_number_=secret_phone_number)
            record = result.single()
            if record is not None:
                print("The user is ", record["user_exists"])
                output_hash["user_exists"] = record["user_exists"]
            else:
                current_app.logger.error(
                    "Checking if the user is secret friend cannot return an empty cursor " + secret_phone_number)
                print("Checking if the user is secret friend cannot return an empty cursor " + secret_phone_number)
                return False
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            return False

        # interest object
        # friend_category_id
        # friend_circle_id
        # friend_category_create_dt
        # friend_category_update_dt

    def link_user_to_web_category(self, user_id, friend_circle_id, lweb_category_id):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            for row in lweb_category_id:
                query = "MATCH (a:User where a.user_id=$user_id_), (" \
                        "b:WebCat{" \
                        "web_category_id:$web_category_id_})" \
                        " MERGE (a)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b) " \
                        " ON CREATE set r.friend_circle_id =$friend_circle_id_ , r.vote = $nvote_," \
                        " r.created_dt = $created_dt_" \
                        " ON MATCH set r.updated_dt = $updated_dt_, r.vote = $nvote_ " \
                        " RETURN r.friend_circle_id"
                result = txn.run(query, user_id_=user_id,
                                 friend_circle_id_=friend_circle_id,
                                 web_category_id_=row["web_category_id"],
                                 created_dt_=self.get_datetime(),
                                 updated_dt_=self.get_datetime(),
                                 nvote_=row["vote"])

                if result is None:
                    print("The combination of user and category does not exist", user_id,
                          lweb_category_id["web_category_id"])
                    txn.rollback()
                    return False
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
            txn.commit()
            return True
        except neo4j.exceptions.Neo4jError as e:
            txn.rollback()
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            txn.rollback()
            current_app.logger.error(e)
            print(e)
            return False

    def link_user_to_web_subcategory(self, user_id, friend_circle_id, lweb_subcategory_id):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            for hsh_web_subcategory_id in lweb_subcategory_id:
                query = "MATCH (a:User where a.user_id=$user_id_),(" \
                        "b:WebSubCat{" \
                        "web_subcategory_id:$web_subcategory_id_}) " \
                        " MERGE (a)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b)" \
                        " ON CREATE set r.friend_circle_id =$friend_circle_id_ , r.vote = $nvote_," \
                        " r.created_dt = $created_dt_" \
                        " ON MATCH set r.updated_dt = $updated_dt_, r.vote = $nvote_ " \
                        " RETURN r.friend_circle_id"
                result = txn.run(query, user_id_=user_id,
                                 friend_circle_id_=friend_circle_id,
                                 web_subcategory_id_=hsh_web_subcategory_id["web_subcategory_id"],
                                 created_dt_=self.get_datetime(),
                                 updated_dt_=self.get_datetime(),
                                 nvote_=hsh_web_subcategory_id["vote"])
                if result is None:
                    print("The combination of user and category does not exist", user_id,
                          hsh_web_subcategory_id["web_subcategory_id"])
                    txn.rollback()
                    return False
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
            txn.commit()
            return True
        except neo4j.exceptions.Neo4jError as e:
            txn.rollback()
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            txn.rollback()
            current_app.logger.error(e)
            print(e)
            return False

    def delete_link_user_to_category(self, friend_circle_id, user_id, web_category_id):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User{user_id:$user_id_})-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebCat{web_category_id:$web_category_id_})" \
                    " DELETE r " \
                    " RETURN r.friend_circle_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                web_category_id_=web_category_id)
            record = result.single()
            if record is None:
                return False
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL", e)
            return False

    def delete_link_user_to_subcategory(self, friend_circle_id, user_id, web_subcategory_id):
        try:
            driver = NeoDB.get_session()
            query = None
            # query = "MATCH (a:User{user_id:$user_id_})-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebSubCat{web_subcategory_id:$web_subcategory_id_}) " \
            #        " DELETE r " \
            #       " return r.friend_circle_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                web_subcategory_id_=web_subcategory_id)
            record = result.single()
            if record is None:
                return False
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL", e)
            return False

    def get_category_interest(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST]->(b:WebCat) " \
                    "  WHERE r.friend_circle_id = $friend_circle_id_ " \
                    "RETURN count(a.user_id) as users, " \
                    "sum(r.vote) as votes, " \
                    "b.web_category_id as category_id," \
                    " b.web_category_name as category_name "
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            for record in result:
                loutput.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False

    def get_category_interest_by_user(self, user_id, hsh_output):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST]->(b:WebCat) " \
                    "  WHERE a.user_id = $user_id_ " \
                    "RETURN r.friend_circle_id as friend_circle_id, " \
                    "count(b.web_category_id) as interests "
            result = driver.run(query, user_id_=user_id)
            for record in result:
                if record["friend_circle_id"] not in hsh_output:
                    hsh_output[record["friend_circle_id"]] = collections.defaultdict(dict)

                hsh_output[record["friend_circle_id"]].update({"interests": record["interests"]})
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False
        except Exception as e:
            print("Error in executing the query", e)
            return False

    def get_subcategory_interest_by_user(self, user_id, hsh_output):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST]->(b:WebSubCat) " \
                    "  WHERE a.user_id = $user_id_ " \
                    "RETURN r.friend_circle_id as friend_circle_id, " \
                    "count(b.web_subcategory_id) as interests "
            result = driver.run(query, user_id_=user_id)
            for record in result:
                if record["friend_circle_id"] not in hsh_output:
                    hsh_output[record["friend_circle_id"]] = collections.defaultdict(dict)

                hsh_output[record["friend_circle_id"]].update(
                    {"interests": record["interests"]})
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False
        except Exception as e:
            print("Error in executing the query", e)
            return False

    def get_subcategory_interest(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebSubCat) " \
                    "RETURN count(a.user_id) as users, " \
                    "sum(r.vote) as votes, " \
                    "b.subcategory_id as subcategory_id," \
                    " b.subcategory_name as subcategory_name "
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            for record in result:
                loutput.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False

    def get_subcategory_smart_recommendation(self, friend_circle_id, age_hi, age_lo, gender, loutput):
        try:
            lsubcat = []
            lcategory_id = []
            list_web_cat = []
            if not self.get_category_interest(friend_circle_id, lcategory_id):
                current_app.logger.error("Error in getting the category data for friend circle id" + friend_circle_id)
                return False
            if lcategory_id is not None:
                for i in range(0, len(lcategory_id)):
                    list_web_cat.append(lcategory_id[i]["category_id"])
            else:
                current_app.logger.info(
                    "Can't do much. Yuo should have some categories chosen prior to this call for this to work")
                return False
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebSubCat) " \
                    "RETURN distinct b.web_subcategory_id as subcategory_id," \
                    " b.web_subcategory_name as subcategory_name, b.parent_id as parent_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            if len(result.data()) <= 0:
                if not self.get_subcategory_initial_recommendation(list_web_cat, age_hi, age_lo, gender, loutput):
                    current_app.logger.error(
                        "Unable to get the initial recommendation for friend circle id" + friend_circle_id)
                    return False
                return True
            else:
                for record in result:
                    lsubcat.append(record["subcategory_id"])
                if not self.get_subcategory_beyond_top_node(lsubcat, age_hi, age_lo, gender, loutput):
                    current_app.logger.error(
                        "Unable to get subcategory recommendation for friend circle id beyond top node " + friend_circle_id)
                    return False
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error message is ", e.message)
            return False
        return True

    def get_subcategory_initial_recommendation(self, lweb_category_id, age_hi, age_lo, gender, loutput):
        try:
            #made changes on 01/13/2022 to remove gender from the query
            #                    " AND a.gender = $gender_ " \
            driver = NeoDB.get_session()
            query = "MATCH (a:WebSubCat)" \
                    " WHERE toInteger(a.age_lo) <= $age_lo_ " \
                    " AND toInteger(a.age_hi) >= $age_hi_ " \
                    " AND a.parent_id in $web_category_id_ " \
                    " RETURN distinct a.web_subcategory_id as web_subcategory_id, a.web_subcategory_name as web_subcategory_name"
            result = driver.run(query, web_category_id_=lweb_category_id, age_lo_=age_lo, age_hi_=age_hi,
                                gender_=gender)
            for record in result:
                loutput.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error message is ", e.message)
            return False

    def get_subcategory_beyond_top_node(self, lweb_subcategory_id, age_hi, age_lo, gender, loutput):
        try:

            driver = NeoDB.get_session()
            query = "MATCH (a:WebSubCat)" \
                    " WHERE  " \
                    "toInteger(a.age_lo) >= $age_lo_ " \
                    " AND toInteger(a.age_hi) <= $age_hi_ " \
                    " AND a.gender = $gender_ " \
                    " AND a.parent_id in $web_subcategory_id " \
                    " RETURN a.web_subcategory_id as web_subcategory_id, a.web_subcategory_name as web_subcategory_name"
            result = driver.run(query, web_category_id_=lweb_subcategory_id, age_lo_=age_lo, age_hi_=age_hi,
                                gender_=gender)
            for record in result:
                loutput.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error message is ", e.message)
            return False

    def get_recently_added_interest(self, friend_circle_id, list_output):
        try:
            driver = NeoDB.get_session()

            query = "MATCH (a:User)-[r:INTEREST]->(w:WebCat) " \
                    " WHERE r.friend_circle_id = $friend_circle_id_ " \
                    " RETURN max(coalesce( r.updated_dt, r.created_dt)) as xdate, u.first_name as first_name," \
                    " u.last_name as last_name, u.image_url" \
                    " UNION " \
                    " MATCH (a:User)-[r:INTEREST]->(w:WebSubCat) " \
                    " WHERE r.friend_circle_id = $friend_circle_id_ " \
                    " RETURN max(coalesce( r.updated_dt, r.created_dt)) as xdate, u.first_name as first_name," \
                    " u.last_name as last_name, u.image_url "

            result = driver.run(query, friend_circle_id_ = friend_circle_id)
            for record in result:
                list_output.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error message is ", e.message)
            return False

    def get_user_summary(self, user_id, txn, hshOutput):
        try:
            # made changes on 01/09/2021 to add to the total count of contributor to count for admin
            # made changes on 01/10/2021 to add approval status to the query
            # made changes on 01/11/2021 to address the underlying issue with the query.
            query =  " match (ff:friend_list)<-[rr]->(fc:friend_circle)"\
                     " call { "\
                     " with fc "\
                     " match (x:User)-[:CIRCLE_CREATOR]->(xf:friend_circle) "\
                     " where x.user_id = $user_id_ "\
                    " and fc.friend_circle_id = xf.friend_circle_id "\
                     " return xf.friend_circle_id " \
                     " union "\
                     " with fc "\
                     " match (pp:friend_list)<-[]->(xf:friend_circle) "\
                     " where ( pp.user_id = $user_id_ or pp.user_id = $user_id_ ) and " \
                     " fc.secret_friend_id <> $user_id_ "\
                     " and pp.application_status in [0,1] "\
                     " and xf.friend_circle_id = fc.friend_circle_id "\
                     " return xf.friend_circle_id "\
                     " } "\
                     " return " \
                     " fc.friend_circle_id as friend_circle_id," \
                     " fc.friend_circle_name as friend_circle_name," \
                     " fc.secret_friend_id as secret_friend_id," \
                     " fc.secret_first_name as secret_first_name," \
                     " fc.secret_last_name as secret_last_name," \
                     " fc.image_url as image_url, " \
                     " min (case ff.user_id when $user_id_ then coalesce(ff.application_status,0) end) as contrib_status," \
                     " count(distinct ff.user_id) + 1 as total_contributors"

            # query = " match (ff:friend_list)<-[rr]->(fc:friend_circle) " \
            #         " call { with ff match (pp:friend_list)<-[]->(fc:friend_circle) " \
            #         " where ( ff.user_id = pp.user_id or ff.user_id = pp.linked_user_id ) and " \
            #         " pp.user_id <> fc.secret_friend_id and " \
            #         " pp.approval_status in (0,1) and " \
            #         " pp.linked_user_id = $user_id_ " \
            #         " return fc.friend_circle_id , pp.approval_status as approval_status " \
            #         "}  " \
            #         " return  fc.friend_circle_id as friend_circle_id," \
            #         " fc.friend_circle_name as friend_circle_name," \
            #         " fc.image_url as image_url, " \
            #         " approval_status, " \
            #         " count(distinct ff.user_id) + 1 as total_contributors"
            if txn is None:
                txn = NeoDB.get_session()

            result = txn.run(query, user_id_=user_id)

            #made changes on 01/12/2022 to replace the key from actual friend circle id to tag "friend_circle_id"

            for record in result:
                #hshOutput[record["friend_circle_id"]] = record.data()
                hshOutput["friend_circle_id"] = record.data()

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)

            # get category interest count
            hsh1 = {}

            if not self.get_category_interest_by_user(user_id, hsh1):
                current_app.logger.error("Error in getting interest count for " + user_id)
                return False

            hsh2 = {}

            if not self.get_subcategory_interest_by_user(user_id, hsh2):
                current_app.logger.error("Error in getting interest count for " + user_id)
                return False

            for key in hsh1:
                if key in hsh2:
                    total_votes = int(hsh1[key]["interests"]) + int(hsh2[key]["interests"])
                else:
                    total_votes = int(hsh1[key]["interests"])
                hshOutput[key].update({"interests": total_votes})

            # for key in hsh1:
            #     hshOutput[key].update(hsh1[key])

            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False
        except Exception as e:
            print("Error in executing the query", e)
            return False

        # friend_occasion

        # friend_occasion_id
        # friend_occasion_date
        # friend_occasion_status
        # friend_occasion_create_dt
        # friend_occasion_update_dt

    def add_occasion(self, user_id, friend_id, friend_circle_id, occasion_id, occasion_date, occasion_timezone, status,
                     output_hash):
        try:
            print("Inside the add occasion function")
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            select_occasion = " MATCH (a:User)-[r:OCCASION]" \
                              "->(f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_})" \
                              "<-[x:IS_MAPPED]-(b:occasion{occasion_id:$occasion_id_}) " \
                              " WHERE (a.user_id = $user_id_ or a.linked_user_id = $user_id_)" \
                              " RETURN count(f.friend_circle_id) as friend_occasion "
            result = txn.run(select_occasion, user_id_=str(user_id), friend_id_=str(friend_id),
                             friend_circle_id_=str(friend_circle_id), occasion_id_=occasion_id)

            if result is not None:
                record = result.single()
                if record["friend_occasion"] <= 0:
                    query = " CREATE (f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_, " \
                            "occasion_date:$occasion_date_, occasion_timezone:$occasion_timezone, created_dt:$created_dt_, friend_occasion_status:$friend_occasion_status_}) " \
                            " WITH f " \
                            " MATCH(a:User WHERE (a.user_id = $user_id_)), " \
                            " (f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_})," \
                            " (b:occasion{occasion_id:$occasion_id_}) " \
                            " MERGE (a)-[:OCCASION]->(f)<-[:IS_MAPPED]-(b) " \
                            " RETURN f.friend_circle_id as friend_circle_id"

                    query_result = txn.run(query, created_dt_=self.get_datetime(),
                                           friend_circle_id_=friend_circle_id,
                                           user_id_=user_id,
                                           occasion_id_=occasion_id,
                                           occasion_date_=occasion_date,
                                           occasion_timezone=occasion_timezone,
                                           friend_occasion_status_=status,
                                           updated_dt_=self.get_datetime()
                                           )

                    if query_result is None:
                        current_app.logger.error("Unable to create occasion for ", user_id, friend_id)
                        print("Unable to create occasion for ", user_id, friend_id)
                        return False
                    for record in query_result:
                        output_hash["friend_circle_id"] = record["friend_circle_id"]

                    if "friend_circle_id" not in output_hash:
                        current_app.logger.error("Friend occasion not created")
                        txn.rollback()
                        return False

                    print("The  query is ", query_result.consume().query)
                    print("The  parameters is ", query_result.consume().parameters)

                txn.commit()
                return True
            else:
                current_app.logger.info("The occasion exists for this user")
                return False
        except neo4j.exceptions.Neo4jError as e:
            print("The error is ", e.message)
            txn.rollback()
            return False

    def create_custom_occasion(self, custom_occasion_name: str, friend_circle_id, frequency, creator_user_id, occasion_date, occasion_timezone, hsh):

        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            status = 1
            occasion_id = None
            check_occasion_query = "match (b:occasion{occasion_name:$occasion_name_, " \
                                   "friend_circle_id:$friend_circle_id_}) return b.occasion_id as occasion_id"

            check_result = txn.run(check_occasion_query,
                                   occasion_name_ = custom_occasion_name.lower(),
                                   friend_circle_id_ = friend_circle_id)
            for occasion_row in check_result:
                occasion_id = occasion_row["occasion_id"]
            if occasion_id is None:
                occasion_id = str(uuid.uuid4())
                query = "MERGE (o:occasion{occasion_id:$occasion_id_,friend_circle_id: $friend_circle_id_}) " \
                        "ON CREATE " \
                        "set " \
                        " o.occasion_id = $occasion_id_," \
                        " o.friend_circle_id = $friend_circle_id_ , "\
                        "o.occasion_name = $occasion_name_, " \
                        "o.occasion_frequency = $occasion_frequency_," \
                        "o.created_dt = $created_dt_," \
                        "o.status_id =$status_ " \
                        "return o.occasion_id as occasion_id"

                result = txn.run(query,
                                occasion_id_ = occasion_id,
                                 friend_circle_id_=friend_circle_id,
                                 status_=status,
                                 occasion_frequency_ = frequency,
                                 occasion_name_ = custom_occasion_name,
                                 created_dt_=self.get_datetime())

                for record in result:
                    hsh["occasion_id"] = record["occasion_id"]
                if hsh["occasion_id"] is None:
                    txn.rollback()
                    current_app.logger.error("Something did not go right. Occasion insertion didnt work")
                    return False

            insert_occasion_query = "MERGE (b:friend_occasion{occasion_id:$occasion_id_, friend_circle_id:$friend_circle_id_})" \
                                    " ON CREATE SET " \
                                    "b.created_dt =$created_dt_," \
                                    "b.occasion_frequency=$occasion_frequency_," \
                                    "b.occasion_date=$occasion_date_," \
                                    "b.occasion_timezone=$occasion_timezone_," \
                                    "b.friend_circle_id=$friend_circle_id_," \
                                    "b.occasion_id = $occasion_id_ " \
                                    " ON MATCH SET " \
                                    "b.occasion_id = $occasion_id_, " \
                                    "b.created_dt =$created_dt_," \
                                    "b.occasion_frequency=$occasion_frequency_," \
                                    "b.occasion_date=$occasion_date_," \
                                    "b.occasion_timezone=$occasion_timezone_," \
                                    "b.friend_circle_id=$friend_circle_id_ " \
                                    " return b.occasion_id as occasion_id"
            insert_result = txn.run(insert_occasion_query, occasion_id_ =occasion_id,
                                    created_dt_ = self.get_datetime(), occasion_frequency_ = frequency ,
                                    occasion_timezone_ = occasion_timezone, occasion_date_ = occasion_date,
                                    friend_circle_id_ = friend_circle_id)
            for record in insert_result:
                hsh["occasion_id"] = record["occasion_id"]
            if hsh["occasion_id"] is None:
                txn.rollback()
                current_app.logger.error("Something did not go right. Occasion insert didnt work")
                return False

            create_mapping_query = "MATCH (u:User{user_id:$user_id_}), " \
                                   "(fo:friend_occasion{friend_circle_id:$friend_circle_id_, " \
                                   "occasion_id:$occasion_id_}), (b:occasion{occasion_id:$occasion_id_}) " \
                                   "MERGE (u)-[r:OCCASION]->(fo)<-[:IS_MAPPED]-(b) " \
                                   "return fo.occasion_id  as occasion_id , u.user_id as user_id"
            map_result = txn.run(create_mapping_query, user_id_ = creator_user_id, occasion_id_ = occasion_id, friend_circle_id_ = friend_circle_id)
            for record in map_result:
                print(record)
                hsh["user_id"] = record["user_id"]

            if hsh["user_id"] is None:
                txn.rollback()
                current_app.logger.error("Something did not go right in creating the relationship")
                return False
            txn.commit()
            return True
        except neo4j.exceptions.Neo4jError as e:
            txn.rollback()
            current_app.logger.error(e.message)
            print(e.message)
            return False
        except Exception as e:
            txn.rollback()
            current_app.logger.error(e)
            print(e)
            return False

    def deactivate_occasion(self, occasion_id, friend_circle_id=None):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (o:occasion{occasion_id:$occasion_id_, friend_circle_id:$friend_circle_id_})" \
                    " SET o.status_id = 0" \
                    " return o.occasion_id"
            result = driver.run(query, occasion_id_ = occasion_id, friend_circle_id_ = friend_circle_id)
            if result is not None:
                for record in result:
                    return True
            else:
                return False
            return False
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print(e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print(e)
            return False

    def approve_occasion(self, user_id, friend_id, friend_circle_id, occasion_id, status, output_hash):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            query = "MATCH " \
                    " (f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_})," \
                    " (b:User{user_id:$friend_id_})-[:CIRCLE_CREATOR]->(fc:friend_circle{friend_circle_id:$friend_circle_id_})" \
                    " set f.friend_occasion_status = $status_ , f.updated_dt = $updated_dt_" \
                    " RETURN f.friend_circle_id as friend_circle_id"

            result = txn.run(query,
                             friend_id_=friend_id,
                             friend_circle_id_=friend_circle_id,
                             status_=status,
                             occasion_id_=occasion_id,
                             updated_dt_=self.get_datetime())
            if result is not None:
                for record in result:
                    output_hash["friend_circle_id"] = record["friend_circle_id"]
            else:
                current_app.logger.error("Unable to approve the vote for ", user_id, friend_id, friend_circle_id)
                print("Unable to insert the vote for ", user_id, friend_id, friend_circle_id)
                return False
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            if len(output_hash) <= 0:
                current_app.logger.error("Unable to approve the vote for ", user_id, friend_id, friend_circle_id)
                print("Unable to insert the vote for ", user_id, friend_id, friend_circle_id)
                txn.rollback()
                return False

            txn.commit()
            return True
        except neo4j.exceptions.Neo4jError as e:
            txn.rollback()
            current_app.logger.error(e.message)
            print(e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print(e)
            return False

    def get_occasion(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    " WHERE " \
                    " f.friend_circle_id = $friend_circle_id_ and " \
                    " b.status_id = 1 " \
                    " RETURN a.user_id as user_id, f.occasion_date as occasion_date , " \
                    "f.occasion_id as occasion_id, b.occasion_name as occasion_name"
            result = driver.run(query,
                                friend_circle_id_=friend_circle_id)
            if result is None:
                loutput = None
            for record in result:
                loutput.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print(e.message)
            return False

    def get_occasion_names(self, list_output, friend_circle_id = None):
        try:
            driver = NeoDB.get_session()
            if friend_circle_id is None:
                query = "MATCH (b:occasion) " \
                        " WHERE " \
                        " b.friend_circle_id is null " \
                        " b.status_id = 1 " \
                        " RETURN  " \
                        "b.occasion_id as occasion_id, b.occasion_name as occasion_name"
                result = driver.run(query)
            else:
                query = "MATCH (b:occasion) " \
                        " WHERE " \
                        " b.friend_circle_id is null and " \
                        " b.status_id = 1 " \
                        " RETURN  " \
                        "b.occasion_id as occasion_id, b.occasion_name as occasion_name, " \
                        "'None' as friend_circle_id, 0 as occasion_frequency" \
                        " UNION " \
                        "MATCH (b:occasion) " \
                        " WHERE " \
                        " b.friend_circle_id = $friend_circle_id_ and " \
                        " b.status_id = 1 " \
                        " RETURN " \
                        "b.occasion_id as occasion_id, b.occasion_name as occasion_name, " \
                        "b.friend_circle_id as friend_circle_id, b.occasion_frequency as occasion_frequency"

                result = driver.run(query,
                                friend_circle_id_=friend_circle_id)
            for record in result:
                list_output.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            if len(list_output) > 0:
                return True
            else:
                return False

            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print(e.message)
            return False

    def get_age_from_occasion(self, friend_circle_id, hsh):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    " WHERE " \
                    " f.friend_circle_id = $friend_circle_id_  and " \
                    " b.occasion_id = 1" \
                    " RETURN a.user_id as user_id, a.friend_id as creator_user_id," \
                    " f.occasion_date as occasion_date , " \
                    "f.occasion_id as occasion_id, b.occasion_name as occasion_name"
            result = driver.run(query,
                                friend_circle_id_=friend_circle_id)
            if result is None:
                hsh = None
                return True
            for record in result:
                if record["occasion_date"] is not None:
                    if not self.convert_occasion_date_to_number(record["occasion_date"], hsh):
                        return False
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print(e.message)
            return False

    def convert_occasion_date_to_number(self, occasion_date, hsh_output):
        try:
            # occasion_date should be in dd-mm-yyyy
            utc_now_dt = datetime.now(tz=pytz.UTC)
            formatted_datetime = utc_now_dt.strftime("%d-%m-%Y %H-%M-%S")
            current_date_time = datetime.strptime(formatted_datetime, "%d-%m-%Y %H-%M-%S")
            first_reminder_date = datetime.strptime(occasion_date, "%d-%m-%Y %H-%M-%S")
            diff = relativedelta(current_date_time.date(), first_reminder_date.date())
            hsh_output["age"] = diff.years
        except Exception as e:
            current_app.logger.error("There is an issue in converting date " + e)
            return None

    def vote_occasion(self, user_id, friend_id, friend_circle_id, occasion_id, flag, value, value_timezone,
                      output_hash):
        try:
            driver = NeoDB.get_session()
            query = " MATCH (a:User{user_id:$user_id_}), " \
                    "(b:friend_occasion{friend_circle_id:$friend_circle_id_,  occasion_id :$occasion_id_})" \
                    " MERGE (a)-[r:VOTE_OCCASION]->(b) " \
                    " ON MATCH set r.status= $status_, r.updated_dt= $updated_dt_, " \
                    " r.value = $value_" \
                    " ON CREATE set r.status = $status_, r.created_dt=$created_dt_, " \
                    " r.friend_circle_id = $friend_circle_id_," \
                    " r.value = $value_," \
                    " r.value_timezone = $value_timezone_" \
                    " RETURN b.friend_circle_id as friend_circle_id"

            result = driver.run(query, user_id_=user_id, friend_id_=friend_id, friend_circle_id_=friend_circle_id,
                                occasion_id_=occasion_id,
                                value_=value, value_timezone_=value_timezone, status_=flag,
                                updated_dt_=self.get_datetime(),
                                created_dt_=self.get_datetime())
            if result is not None:
                for record in result:
                    output_hash["friend_circle_id"] = record["friend_circle_id"]
            else:
                current_app.logger.error("Unable to insert the vote for ", user_id, friend_id, friend_circle_id)
                print("Unable to insert the vote for ", user_id, friend_id, friend_circle_id)
                return False
            if output_hash["friend_circle_id"] is None:
                current_app.logger.error("Unable to insert the vote for ", user_id, friend_id, friend_circle_id)
                print("Unable to insert the vote for ", user_id, friend_id, friend_circle_id)
                return False
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL", e)
            return False

    def get_occasion_votes(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:VOTE_OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    " WHERE f.friend_circle_id = $friend_circle_id_ " \
                    " RETURN a.user_id as user_id,  r.status as status, r.value as value, b.occasion_id as occasion_id "
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            if result is None:
                current_app.logger.error("Houston we have a problem. No votes")
                loutput = None
            for record in result:
                loutput.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL")
            return False

    # structure of tagged products
    #  (n:tagged_product {product_id: XYZ, tagged_category: ["x', "y", "z"], "last_updated_date": "dd-mon-yyyy",
    #  "location":"country", "age_lower" : 45, "age_upper": 43, "price_upper": 3, "price_lower": 54, "gender": "M",
    #  "color": [red,blue, white], "category_relevance" : [3,4,5], uniqueness index: [1..10]
    # )
