import json
import neo4j.exceptions
import logging
from flask import current_app, g
from flask_restful import Resource
from .extensions import NeoDB
import uuid
import pymongo.collection
from datetime import datetime
from pymongo import errors


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
            output_data = None
            return False

    def get_user_by_id(self, user_id, output_hash):
        driver = NeoDB.get_session()
        if driver is None:
            current_app.logger.info("Driver to the database is not initiated")
            print("Driver is not initiated")
            return 0
        print("Inside the gdb user all function")
        try:
            query = "MATCH (u:User) " \
                    "WHERE u.user_id = $user_id_ " \
                    "RETURN u.email_address, u.user_id, u.user_type"
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
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_hash = None
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
                    "RETURN u.user_id, u.email_address, u.user_type"

            results = driver.run(query, phone_number_=phone_number)
            if results is None:
                print("user does not exist")
                output_data = None
                return True
            for record in results:
                print("The user is", record)
                output_data.append(record["u.user_id"])
                output_data.append(record["u.email_address"])
                output_data.append(record["u.user_type"])
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_data = None
            return False

    def get_user_by_email(self, email_address, output_data):

        try:
            driver = NeoDB.get_session()
            if driver is None:
                current_app.logger.info("Driver to the database is not initiated in get_user_by_phone")
                print("Driver is not initiated get_user_by_phone")
                return 0
            print("Inside the gdb user all function")
            query = "MATCH (u:User) " \
                    "WHERE u.email_address = $email_address_ " \
                    "RETURN u.user_id, u.email_address, u.user_type"

            results = driver.run(query, email_address_ =email_address)
            if results is None:
                print("user does not exist")
                output_data = None
                return True
            for record in results:
                print("The user is", record)
                output_data.append(record["u.user_id"])
                output_data.append(record["u.email_address"])
                output_data.append(record["u.user_type"])
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_data = None
            return False

    def insert_user(self, user_hash, output_hash):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            #Remember: Need the following in user hash
            # friend_id, external_referrer_id (optional), but the parameter should be there
            user_id = None
            output_hash["email_address"] = None
            output_hash["user_id"] = None
            loutput = []
            if not self.verify_friend_list(user_hash, txn, loutput ):
                current_app.logger.error("Unable to verify the registration request against friend list for email_address" + user_hash.get("email_address"))
                txn.rollback
                return False
            # I need to decide on how to handle direct registration. That is, if I should insert a record into friend list. If yes, what would the friend id be.

            if len(loutput) == 1 :
                if loutput[0]["linked_user_id"] is not None:
                    output_hash["outcome"] = "User exists"
                    output_hash["redirect"] = "Login"
                    txn.rollback()
                    return True
                elif loutput[0]["linked_user_id"] is None:
                    user_id = loutput[0]["user_id"]
            elif len(loutput) > 1:
                temp_linked_user_id = None
                counter = 0
                for record in loutput:
                    if record["linked_user_id"] is not None and counter == 0:
                        temp_linked_user_id = record["linked_user_id"]
                        user_id = record["user_id"]
                        counter = 1
                    elif record["linked_user_id"] is not None:
                        if temp_linked_user_id != record["linked_user_id"]:
                            output_hash["outcome"] = "Systemic issue. Multiple user id for a given phone number or email address"
                            output_hash["redirect"] = "Site Issue"
                            txn.rollback()
                            return False

            if user_id is None:
                user_id = self.get_id()

            query = "CREATE (u:User) " \
                    " SET u.email_address = $email_address_, u.user_id = $user_id_, u.phone_number = $phone_number_, " \
                    " u.gender = $gender_, u.user_type = $user_type_, u.first_name=$first_name_, u.last_name=$last_name_," \
                    " u.mongo_indexed = $mongo_indexed_" \
                    " RETURN u.email_address, u.user_id"

            result = txn.run(query, email_address_=str(user_hash.get('email_address')),
                                password=str(user_hash.get('password')),
                                user_id_=str(user_id),
                                gender_=user_hash.get("gender"),
                                phone_number_ = user_hash.get("phone_number"),
                                user_type_=user_hash.get("user_type"),
                                first_name_ = user_hash.get("first_name"),
                                last_name_= user_hash.get("last_name"),
                                external_referrer_id = user_hash.get("external_referrer_id"),
                                external_referrer_param = user_hash.get("external_referrer_param"),
                                mongo_indexed_ = user_hash.get("mongo_indexed"))
            record = result.single()
            info = result.consume().counters.nodes_created
            if info > 0 and record is not None:
                print("The user id is", record["u.user_id"])
                output_hash["email_address"] = record["u.email_address"]
                output_hash["user_id"] = record["u.user_id"]

            if not self.update_friendlist(loutput,user_id, txn):
                txn.rollback()
                current_app.logger.error("We have an issue processing the registration request. Unable to friend list")
                print("We have an issue processing the registration request. Unable to friend list")
                return False
            mongo_user_collection = pymongo.collection.Collection(g.db, "user")
            result = mongo_user_collection.find_one({"user_id": output_hash.get("user_id")})
            if result is None:
                mongo_user_collection.insert_one({"user_id": output_hash.get("user_id"),
                                                  "email": user_hash.get("email_address"),
                                                  "password": user_hash.get("password"),
                                                  "phone_number": user_hash.get("phone_number"),
                                                  "gender" : user_hash.get("gender"),
                                                  "first_name" : user_hash.get("first_name"),
                                                  "last_name" : user_hash.get("last_name"),
                                                  "user_type" : user_hash.get("user_type")
                                                  })
                # Check if the user is in the friend list. Go search by email id and phone number if exists.
                # if referrer exists use that to check


                output_hash["mongo_indexed"] = "Y"

                if not self.update_user(output_hash, txn):
                    txn.rollback()
                    print("Error in updating the user")
                    return {"status": "Failure in updating the inserted record in the same transaction"}, 400
                    return False

            friend_circles = []
            for record in loutput:
                if self.get_friend_circles(record["user_id"], friend_circles):
                    current_app.logger.error("Unable to extract friend circle data for " + record["user_id"])
                    txn.rollback()
                    return False
                output_hash.append(loutput)
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
            result = driver.run(query, user_id_ = user_id)
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
            result = txn.run(query, user_id_ = user_hash.get("user_id"), mongo_indexed_ = user_hash.get("mongo_indexed") )
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

    def verify_friend_list(self, input_hash, txn, loutput):
        try:
            loutput = None
            fe_query = "MATCH (a:friend_list) " \
                    " WHERE email_address = $email_address_ " \
                    " return a.user_id, a.friend_id, a.linked_user_id, a.linked_status_id "

            fep_query = "MATCH (a:friend_list) " \
                    " WHERE a.email_address = $email_address_ and a.phone_number = $phone_number_" \
                    " return a.user_id, a.friend_id, a.linked_user_id, a.linked_status_id"

            fp_query =  "MATCH (a:friend_list) " \
                    " WHERE a.phone_number = $phone_number_ " \
                    " return a.user_id, a.friend_id, a.linked_user_id, a.linked_status_id"

            if input_hash["email_address"] is not None  and input_hash["phone_number"] is not None:
                result = txn.run(fep_query,  email_address_ = input_hash["email_address"], phone_number_ = input_hash["phone_number"])
            elif  input_hash["email_address"] is not None:
                result = txn.run(fe_query, email_address_ = input_hash["email_address"])
            elif input_hash["phone_number"] is not None:
                result = txn.run(fp_query, phone_number_ =input_hash["phone_number"])

            for record in result:
                loutput.append(record.data())
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

            update_fl_query = "UPDATE (fl:friend_list) " \
                    "WHERE fl.friend_id=$friend_id_ and " \
                    " fl.user_id = $user_id_ " \
                    "SET fl.linked_user_id = $linked_user_id_, " \
                    " fl.linked_status_id = 1"
            for record in linput:
                result = txn.query(update_fl_query, friend_id_ = record["friend_id"], user_id_ = record["user_id"], linked_user_id_ = user_id)
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

            result = driver.run(query, user_id_=user_id, email_address_ = email_address, friend_circle_id_=friend_circle_id)
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)

            if result.peek() is None:
                output = []
                return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            friend_circle_id[0] = None
            return False


    def get_friend_circle(self, friend_circle_id, loutput):
        driver = NeoDB.get_session()
        result = None
        try:

            query = "MATCH (n:User)-[rr]->(x:friend_circle) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ " \
                    " RETURN  n.user_id as user_id, n.first_name as first_name, " \
                    " n.last_name as last_name, n.gender as gender, " \
                    " type(rr) as relationship " \
                    " UNION " \
                     "MATCH (x:friend_circle)-[rr]->(m:friend_list) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ " \
                    " RETURN  m.user_id as user_id, m.first_name as first_name, " \
                    " m.last_name as last_name, m.gender as gender, " \
                    " type(rr) as relationship" \
                    " UNION " \
                    "MATCH (x:friend_circle)<-[rr]-(m:friend_list) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_" \
                    " RETURN  m.user_id as user_id, m.first_name as first_name, " \
                    " m.last_name as last_name, m.gender as gender, " \
                    " type(rr) as relationship"

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
        try:

            query = "MATCH (n:User)-[rr]->(x:friend_circle) " \
                    "WHERE n.user_id = $user_id_ " \
                    " RETURN  n.user_id as user_id, n.first_name as first_name, " \
                    " n.last_name as last_name, n.gender as gender, " \
                    " type(rr) as relationship, x.friend_circle_id as friend_circle_id, x.friend_circle_name as friend_circle_name" \
                    " UNION " \
                     "MATCH (x:friend_circle)-[rr]->(m:friend_list) " \
                    "WHERE m.friend_id = $user_id_ " \
                    " RETURN  m.user_id as user_id, m.first_name as first_name, " \
                    " m.last_name as last_name, m.gender as gender, " \
                    " type(rr) as relationship, x.friend_circle_id as friend_circle_id, x.friend_circle_name as friend_circle_name" \
                    " UNION " \
                    "MATCH (x:friend_circle)<-[rr]-(m:friend_list) " \
                    "WHERE m.friend_id = $user_id_" \
                    " RETURN  m.user_id as user_id, m.first_name as first_name, " \
                    " m.last_name as last_name, m.gender as gender, " \
                    " type(rr) as relationship, x.friend_circle_id as friend_circle_id, x.friend_circle_name as friend_circle_name"

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

    def check_user_in_friend_circle(self, referred_user_id, referrer_user_id, friend_circle_id, hshOutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:friend_list)<-[rr]->(fc:friend_circle)" \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ AND" \
                    " n.user_id= $referred_user_id_ AND " \
                    " n.friend_id = $referrer_user_id_  " \
                    " RETURN count(n) as user_exists, type(rr) as relation_type " \
                    " UNION " \
                    "MATCH (n:User)-[rr]->(fc:friend_circle)" \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ AND" \
                    " n.user_id= $referred_user_id_  " \
                    " RETURN count(n) as user_exists, type(rr) as relation_type"
            result = driver.run(query, friend_circle_id_=friend_circle_id, referred_user_id_ = referred_user_id, referrer_user_id_ = referrer_user_id)
            for record in result:
                print("The user is ", record["user_exists"])
                hshOutput["user_count"] = record["user_exists"]
                hshOutput["relation_type"] = record["relation_type"]
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
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
            result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_=email_address, referrer_user_id_ = referrer_user_id)
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

    def check_user_is_secret_friend(self, referred_user_id, referrer_user_id, friend_circle_id, hshOutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:friend_circle)-[:SECRET_FRIEND]->(n:friend_list)" \
                    " WHERE x.friend_circle_id = $friend_circle_id_ AND" \
                    " n.user_id= $referred_user_id_ " \
                    " AND n.friend_id = $referrer_user_id_" \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, referrer_user_id_ = referrer_user_id, referred_user_id_=referred_user_id)
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
            return False

    def check_user_is_secret_friend_by_email(self,  email_address, referrer_user_id, friend_circle_id, output_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (fc:friend_circle)-[:SECRET_FRIEND]->(n:friend_list)" \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ AND" \
                    " n.email_address= $email_address_ " \
                    " AND n.friend_id = $referrer_user_id_" \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, referrer_user_id_ = referrer_user_id, email_address_=email_address)
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
            hshOutput = {}
            return False

    def check_user_is_admin_by_email(self,  email_address, friend_circle_id, output_hash):
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
            print(e.message)
            current_app.logger.error(e.message)
            return False

    def check_friend_circle_with_admin_and_secret_friend(self, friend_user_id, secret_user_id, output_hash):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:User)-[:CIRCLE_CREATOR]->(fc:friend_circle)-[SECRET_FRIEND]->(y:friend_circle)" \
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

    def check_friend_circle_with_admin_and_secret_friend_by_email(self, friend_user_id, secret_email_address, output_hash):
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
                output_hash["user_exists"]  = record["user_exists"]
            else:
                current_app.logger.error("Checking if the user is secret friend cannot return an empty cursor " + secret_email_address)
                print("Checking if the user is secret friend cannot return an empty cursor " + secret_email_address)
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

    def link_user_to_web_category(self, user_id, friend_id, friend_circle_id, lweb_category_id):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            for hsh_web_category_id in lweb_category_id:
                query = "MATCH (a:friend_list{user_id:$user_id_, friend_id:$friend_id_}), (" \
                        "b:WebCat{" \
                        "web_category_id:$web_category_id_})" \
                        " MERGE (a)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b) " \
                        " ON CREATE set r.friend_circle_id =$friend_circle_id_ , r.vote = $nvote_," \
                        " r.created_dt = $created_dt_" \
                        " ON MATCH set r.updated_dt = $updated_dt_, r.vote = $nvote_ " \
                        " RETURN r.friend_circle_id"
                result = txn.run(query, user_id_=user_id,
                                    friend_id_ = friend_id,
                                    friend_circle_id_=friend_circle_id,
                                    web_category_id_=hsh_web_category_id["web_category_id"],
                                    created_dt_=self.get_datetime(),
                                    updated_dt_=self.get_datetime(),
                                    nvote_=hsh_web_category_id["vote"])

                if result is None:
                    print("The combination of user and category does not exist", user_id,
                          hsh_web_category_id["web_category_id"])
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
            return  False

    def link_user_to_web_subcategory(self, user_id, friend_id, friend_circle_id, lweb_subcategory_id):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            for hsh_web_subcategory_id in lweb_subcategory_id:
                query = "MATCH (a:friend_list{user_id:$user_id_, friend_id:$friend_id_}),(" \
                        "b:WebSubCat{" \
                        "web_subcategory_id:$web_subcategory_id_}) " \
                        " MERGE (a)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b)" \
                        " ON CREATE set r.friend_circle_id =$friend_circle_id_ , r.vote = $nvote_," \
                        " r.created_dt = $created_dt_" \
                        " ON MATCH set r.updated_dt = $updated_dt_, r.vote = $nvote_ " \
                        " RETURN r.friend_circle_id"
                result = txn.run(query, user_id_=user_id,
                                    friend_id_ = friend_id,
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
            query = "MATCH (a:friend_list)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebCat) " \
                    "RETURN count(a.user_id) as users, " \
                    "sum(r.vote) as votes, " \
                    "b.category_id as category_id," \
                    " b.category_name as category_name "
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            for record in result:
                loutput.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False


    def get_subcategory_interest(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:friend_list)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebSubCat) " \
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


    # friend_occasion

    # friend_occasion_id
    # friend_occasion_date
    # friend_occasion_status
    # friend_occasion_create_dt
    # friend_occasion_update_dt

    def add_occasion(self, user_id, friend_id, friend_circle_id, occasion_id, occasion_date, status, output_hash):
        try:
            print ("Inside the add occasion function")
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            select_occasion = " MATCH (a:friend_list{user_id:$user_id_, friend_id:$friend_id_})-[r:OCCASION]" \
                              "->(f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_})" \
                              "<-[x:IS_MAPPED]-(b:occasion{occasion_id:$occasion_id_})" \
                              " RETURN count(f.friend_circle_id) as friend_occasion "
            result = txn.run(select_occasion, user_id_=str(user_id), friend_id_=str(friend_id),
                             friend_circle_id_ = str(friend_circle_id), occasion_id_=occasion_id)

            if result is not None:
                record = result.single()
                if record["friend_occasion"] <= 0:
                    query = " CREATE (f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_, " \
                            "occasion_date:$occasion_date_, created_dt:$created_dt_, friend_occasion_status:$friend_occasion_status_}) " \
                            " WITH f " \
                            " MATCH(a:friend_list{user_id:$user_id_, friend_id:$friend_id_})," \
                            " (f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_})," \
                            " (b:occasion{occasion_id:$occasion_id_}) " \
                            " MERGE (a)-[:OCCASION]->(f)<-[:IS_MAPPED]-(b) " \
                            " RETURN f.friend_circle_id as friend_circle_id"

                    query_result = txn.run(query, created_dt_=self.get_datetime(),
                                friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                friend_id_ = friend_id,
                                occasion_id_=occasion_id,
                                occasion_date_=occasion_date,
                                friend_occasion_status_ = status,
                                updated_dt_= self.get_datetime()
                                )

                    if query_result is None:
                        current_app.logger.error("Unable to create occasion for ", user_id, friend_id)
                        print("Unable to create occasion for ", user_id, friend_id)
                        return False
                    for record in query_result:
                        output_hash["friend_circle_id"] = record["friend_circle_id"]

                    print("The  query is ", query_result.consume().query)
                    print("The  parameters is ", query_result.consume().parameters)

            txn.commit()
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error is ", e.message)
            txn.rollback()
            return False

    def approve_occasion(self,user_id, friend_id, friend_circle_id, occasion_id, status, output_hash):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            query = "MATCH "\
                    " (f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_})," \
                    " (b:User{user_id:$friend_id_})-[:CIRCLE_CREATOR]->(fc:friend_circle{friend_circle_id:$friend_circle_id_})" \
                    " set f.friend_occasion_status = $status_ , f.updated_dt = $updated_dt_" \
                    " RETURN f.friend_circle_id as friend_circle_id"

            result = txn.run(query,
                                friend_id_ = friend_id,
                                friend_circle_id_=friend_circle_id,
                                status_=status,
                                occasion_id_=occasion_id,
                                updated_dt_ = self.get_datetime())
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
            query = "MATCH (a:friend_list)-[r:OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    " WHERE " \
                    " f.friend_circle_id = $friend_circle_id_ " \
                    " RETURN a.user_id as user_id, a.friend_id as creator_user_id, f.occasion_date as occasion_date , " \
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

    def vote_occasion(self,  user_id, friend_id, friend_circle_id, occasion_id, flag, value, output_hash):
        try:
            driver = NeoDB.get_session()
            query = " MATCH (a:friend_list{user_id:$user_id_, friend_id:$friend_id_}), " \
                    "(b:friend_occasion{friend_circle_id:$friend_circle_id_,  occasion_id :$occasion_id_})" \
                    " MERGE (a)-[r:VOTE_OCCASION]->(b) " \
                    " ON MATCH set r.status= $status_, r.updated_dt= $updated_dt_, " \
                    " r.value = $value_" \
                    " ON CREATE set r.status = $status_, r.created_dt=$created_dt_, " \
                    " r.friend_circle_id = $friend_circle_id_," \
                    " r.value = $value_" \
                    " RETURN b.friend_circle_id as friend_circle_id"

            result = driver.run(query, user_id_=user_id, friend_id_ = friend_id, friend_circle_id_=friend_circle_id, occasion_id_=occasion_id,
                                value_=value, status_ = flag, updated_dt_ = self.get_datetime(), created_dt_=self.get_datetime())
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
            query = "MATCH (a:friend_list)-[r:VOTE_OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    " WHERE f.friend_circle_id = $friend_circle_id_ " \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, r.status as status, r.value as value, b.occasion_id as occasion_id "
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
