import json
import os

import neo4j.exceptions
import logging
from flask import current_app, g
from flask_restful import Resource
from .extensions import NeoDB
from .gdbmethods import GDBUser
from .mongodbfunc import MongoDBFunctions
from datetime import datetime, date
import pymongo.collection, pymongo.errors
import uuid


# friend_list object
# user_id
# friend_id
# email_address
# phone_number
# linked_status
# linked_user_id
# first_name
# last_name
# location
# gender

class FriendListDB:
    def __init__(self):
        self.__dttime = None
        self.__uid = None

    def get_datetime(self):
        self.__dttime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return self.__dttime

    def get_id(self):
        self.__uid = str(uuid.uuid4())
        return self.__uid

    def get_friend_list(self, friend_id, loutput):

        try:
            driver = NeoDB.get_session()

            query = "MATCH (a:friend_list)" \
                    " WHERE a.email_address = $email_address_" \
                    " AND a.friend_id = $friend_id_" \
                    " RETURN a.email_address, a.phone_number, a.name, a.location "
            result = driver.run(query, friend_id_=friend_id)
            for record in result:
                loutput.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_friend(self, email_address, phone_number, user_id, loutput):

        try:
            driver = NeoDB.get_session()
            if email_address is not None:
                query = "MATCH (a:friend_list)" \
                        " WHERE a.email_address = $email_address_" \
                        " AND a.user_id = $user_id_" \
                        " RETURN a.email_address, a.phone_number, a.name, a.location "
                result = driver.run(query, email_address_=email_address, user_id_=user_id)
            elif phone_number is not None:
                query = "MATCH (a:friend_list)" \
                        " WHERE a.phone_number = $phone_number_" \
                        " AND a.user_id = $user_id_" \
                        " RETURN a.user_id, a.email_address, a.phone_number, a.name, a.location "

                result = driver.run(query, phone_number_=phone_number, user_id_=user_id)

            for record in result:
                loutput.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_friend_by_id(self, user_id, referrer_user_id, hshOutput):

        try:
            driver = NeoDB.get_session()
            hshOutput["referred_user_id"] = None
            query = "MATCH (a:friend_list)" \
                    " WHERE " \
                    " a.user_id = $user_id_ AND" \
                    " a.friend_id = $friend_id_ " \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, a.email_address as email_address, " \
                    "a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location as location," \
                    "a.linked_status as linked_status, a.linked_user_id as linked_user_id, a.approval_status as approval_status," \
                    "a.age as age, a.gender as gender"

            result = driver.run(query, user_id_=user_id, friend_id_=referrer_user_id)

            for record in result:
                hshOutput["referred_user_id"] = record["user_id"]
                hshOutput["referrer_user_id"] = record["friend_id"]
                hshOutput["email_address"] = record["email_address"]
                hshOutput["phone_number"] = record["phone_number"]
                hshOutput["location"] = record["location"]
                hshOutput["first_name"] = record["first_name"]
                hshOutput["last_name"] = record["last_name"]
                hshOutput["linked_status"] = record["linked_status"]
                hshOutput["linked_user_id"] = record["linked_user_id"]
                hshOutput["approval_status"] = record["approval_status"]
                hshOutput["age"] = record["age"]
                hshOutput["gender"] = record["gender"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_unique_friend_by_id(self, user_id, hshOutput):

        try:
            driver = NeoDB.get_session()
            hshOutput["referred_user_id"] = None
            query = "MATCH (a:friend_list)" \
                    " WHERE " \
                    " a.user_id = $user_id_ and " \
                    " a.first_name is not null " \
                    " RETURN a.user_id as user_id, a.email_address as email_address, " \
                    "a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location as location," \
                    "a.linked_status as linked_status, a.linked_user_id as linked_user_id, a.approval_status as approval_status," \
                    "a.age as age, a.gender as gender"

            result = driver.run(query, user_id_=user_id)

            for record in result:
                hshOutput["referred_user_id"] = record["user_id"]
                hshOutput["email_address"] = record["email_address"]
                hshOutput["phone_number"] = record["phone_number"]
                hshOutput["location"] = record["location"]
                hshOutput["first_name"] = record["first_name"]
                hshOutput["last_name"] = record["last_name"]
                hshOutput["linked_status"] = record["linked_status"]
                hshOutput["linked_user_id"] = record["linked_user_id"]
                hshOutput["approval_status"] = record["approval_status"]
                hshOutput["age"] = record["age"]
                hshOutput["gender"] = record["gender"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_friend_by_email(self, email_address, referrer_user_id, source_type, hshOutput):

        try:
            driver = NeoDB.get_session()
            hshOutput["referred_user_id"] = None
            query = "MATCH (a:friend_list)" \
                    " WHERE " \
                    " a.email_address = $email_address_ AND" \
                    " a.friend_id = $friend_id_ AND " \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, a.email_address as email_address, " \
                    "a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location as location "

            result = driver.run(query, email_address_=email_address, friend_id_=referrer_user_id,
                                source_type_=source_type)
            for record in result:
                hshOutput["referred_user_id"] = record["user_id"]
                hshOutput["referrer_user_id"] = record["friend_id"]
                hshOutput["email_address"] = record["email_address"]
                hshOutput["phone_number"] = record["phone_number"]
                hshOutput["location"] = record["location"]
                hshOutput["first_name"] = record["first_name"]
                hshOutput["last_name"] = record["last_name"]
                hshOutput["linked_statue"] = record["linked_status"]
                hshOutput["linked_user_id"] = record["linked_user_id"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_friend_by_phone_number(self, phone_number, referrer_user_id, source_type, hshOutput):

        try:
            driver = NeoDB.get_session()
            hshOutput["referred_user_id"] = None
            query = "MATCH (a:friend_list)" \
                    " WHERE " \
                    " a.phone_number = $phone_number_ AND" \
                    " a.friend_id = $friend_id_  " \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, a.email_address as email_address, " \
                    "a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location as location, " \
                    " a.linked_status as linked_status, a.linked_user_id as linked_user_id "

            result = driver.run(query, phone_number_=phone_number, friend_id_=referrer_user_id,
                                source_type_=source_type)
            for record in result:
                hshOutput["referred_user_id"] = record["user_id"]
                hshOutput["referrer_user_id"] = record["friend_id"]
                hshOutput["email_address"] = record["email_address"]
                hshOutput["phone_number"] = record["phone_number"]
                hshOutput["location"] = record["location"]
                hshOutput["first_name"] = record["first_name"]
                hshOutput["last_name"] = record["last_name"]
                hshOutput["linked_status"] = record["linked_status"]
                hshOutput["linked_user_id"] = record["linked_user_id"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False


    def get_friend_su_by_id(self, user_id, list_output):

        try:
            driver = NeoDB.get_session()
            list_output = None
            query = "MATCH (a:friend_list)" \
                    " WHERE " \
                    " a.user_id = $user_id_ " \
                    " RETURN distinct a.user_id as user_id, a.linked_user_id as linked_user_id"

            result = driver.run(query, user_id_=user_id)
            for record in result:
                list_output.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def insert_friend(self, hshuser, txn, loutput):
        try:
            result = None
            print("The email address and the number is ", hshuser["email_address"], hshuser["phone_number"])
            if (hshuser["email_address"] is None and hshuser["phone_number"] is not None) or (
                    hshuser["email_address"] is not None and hshuser["phone_number"] is not None):
                print("Inside teh phone number for loop")
                query = "MERGE (a:friend_list { friend_id:$friend_id_, phone_number:$phone_number_})" \
                        "  ON MATCH set a.email_address=$email_address_, " \
                        "a.linked_status= $linked_status_, " \
                        "a.linked_user_id=$linked_user_id_" \
                        " ON CREATE set a.user_id = $user_id_, a.friend_id=$friend_id_, a.email_address=$email_address_, a.phone_number=$phone_number_, " \
                        " a.first_name=$first_name_, a.last_name=$last_name_, a.linked_status=$linked_status_, a.linked_user_id=$linked_user_id_, a.source_type_ = $source_type_" \
                        " RETURN a.user_id as user_id, " \
                        " a.friend_id as friend_id," \
                        " a.email_address as email_address," \
                        " a.phone_number as phone_number ," \
                        " a.first_name," \
                        " a.last_name ," \
                        " a.location "
                result = txn.run(query,
                                 phone_number_=hshuser["phone_number"],
                                 email_address_=hshuser["email_address"],
                                 user_id_=self.get_id(),
                                 friend_id_=hshuser[
                                     "admin_friend_id"] if "admin_friend_id" in hshuser else self.get_id(),
                                 first_name_=hshuser["first_name"],
                                 last_name_=hshuser["last_name"],
                                 linked_status_=hshuser["linked_status"],
                                 linked_user_id_=hshuser["linked_user_id"],
                                 source_type_=hshuser["source_type"]
                                 )

                print("The result is ", result, result.peek())
                if result is None:
                    loutput = None
                    current_app.logger.error("The user was not inserted with phone " + hshuser["phone_number"] +
                                             " or email " + hshuser["email_address"])
                    return False
                else:

                    for record in result:
                        print("The record is ", record["user_id"], record["phone_number"])
                        # This is get unique key.
                        # We may need to look at this function if we have to get user information by email address.
                        # DO NOT CHANGE and remember the logic. This is key for whatsapp to work,
                        loutput[record["friend_id"] + record["phone_number"]] = record

                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                print("Successfully inserted friend")
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def insert_friend_by_phone(self, hshuser, txn, loutput):
        try:
            result = None
            referrer_user_id = None
            referred_user_id = None

            print("The email address and the number is ", hshuser["email_address"])
            query = "MERGE (a:friend_list {friend_id:$friend_id_, phone_number:$phone_number_}) " \
                    " ON MATCH set a.email_address = $email_address_ , " \
                    "a.linked_status= $linked_status_, " \
                    "a.linked_user_id=$linked_user_id_" \
                    " ON CREATE set a.user_id = $user_id_, " \
                    "a.friend_id=$friend_id_, " \
                    "a.email_address=$email_address_, " \
                    "a.phone_number=$phone_number_, " \
                    "a.linked_status= $linked_status_, " \
                    "a.linked_user_id=$linked_user_id_," \
                    " a.source_type = $source_type_," \
                    " a.location = $location_," \
                    " a.first_name=$first_name_, a.last_name=$last_name_," \
                    " a.approval_status = $approval_status_ ," \
                    " a.gender = $gender_," \
                    " a.age = $age_" \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, a.email_address as email_address, " \
                    "a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location, a.age, a.gender "

            if "referred_user_id" not in hshuser or hshuser["referred_user_id"] is None:
                referred_user_id = self.get_id()
            else:
                referred_user_id = hshuser["referred_user_id"]
            if "referrer_user_id" not in hshuser or hshuser["referrer_user_id"] is None:
                current_app.logger.error("Referrer id to insert a friend cannot be empty")
                return False
            else:
                referrer_user_id = hshuser["referrer_user_id"]
            result = txn.run(query,
                             phone_number_=hshuser["phone_number"],
                             email_address_=hshuser["email_address"],
                             user_id_=referred_user_id,
                             friend_id_=referrer_user_id,
                             first_name_=hshuser["first_name"],
                             last_name_=hshuser["last_name"],
                             linked_status_=hshuser["linked_status"],
                             linked_user_id_=hshuser["linked_user_id"],
                             source_type_=hshuser["source_type"],
                             location_=hshuser["location"],
                             approval_status_=hshuser["approval_status"],
                             age_ = hshuser["age"],
                             gender_ = hshuser["gender"]
                             )
            if result is None:
                loutput = None
                current_app.logger.error("The user was not inserted with phone " + hshuser["phone_number"] +
                                         " or email " + hshuser["email_address"])
                return False
            else:
                for record in result:
                    print("The record is ", record["user_id"])
                    loutput[str(record["friend_id"]) + str(record["phone_number"])] = record
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def insert_friend_by_email(self, hshuser, txn, loutput):
        try:
            result = None
            referrer_user_id = None
            referred_user_id = None

            print("The email address and the number is ", hshuser["email_address"])
            query = "MERGE (a:friend_list {friend_id:$friend_id_, email_address:$email_address_}) " \
                    " ON MATCH set a.phone_number = $phone_number_ , " \
                    "a.linked_status= $linked_status_, " \
                    "a.linked_user_id=$linked_user_id_" \
                    " ON CREATE set a.user_id = $user_id_, " \
                    "a.friend_id=$friend_id_, " \
                    "a.email_address=$email_address_, " \
                    "a.phone_number=$phone_number_, " \
                    "a.linked_status= $linked_status_, " \
                    "a.linked_user_id=$linked_user_id_," \
                    " a.source_type = $source_type_," \
                    " a.first_name=$first_name_, a.last_name=$last_name_" \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, a.email_address as email_address, a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location "

            if "referred_user_id" not in hshuser or hshuser["referred_user_id"] is None:
                referred_user_id = self.get_id()
            else:
                referred_user_id = hshuser["referred_user_id"]
            if "referrer_user_id" not in hshuser or hshuser["referrer_user_id"] is None:
                referrer_user_id = self.get_id()
            else:
                referrer_user_id = hshuser["referrer_user_id"]
            result = txn.run(query,
                             phone_number_=hshuser["phone_number"] if "phone_number" in hshuser else None,
                             email_address_=hshuser["email_address"],
                             user_id_=referred_user_id,
                             friend_id_=referrer_user_id,
                             first_name_=hshuser["first_name"],
                             last_name_=hshuser["last_name"],
                             linked_status_=hshuser["linked_status"],
                             linked_user_id_=hshuser["linked_user_id"],
                             source_type_=hshuser["source_type"]
                             )
            if result is None:
                loutput = None
                current_app.logger.error("The user was not inserted with phone " + hshuser["phone_number"] +
                                         " or email " + hshuser["email_address"])
                return False
            else:
                for record in result:
                    print("The record is ", record["user_id"])
                    loutput[record["friend_id"] + record["email_address"]] = record
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def insert_friend_by_id(self, hshuser, txn, loutput):
        try:
            result = None
            referrer_user_id = None
            referred_user_id = None

            print("The email address and the number is ", hshuser["email_address"])
            query = "MERGE (a:friend_list {friend_id:$friend_id_, user_id:$user_id_}) " \
                    " ON MATCH set a.phone_number = $phone_number_ , " \
                    "a.linked_status= $linked_status_, " \
                    "a.linked_user_id=$linked_user_id_" \
                    " ON CREATE set a.user_id = $user_id_, " \
                    "a.friend_id=$friend_id_, " \
                    "a.email_address=$email_address_, " \
                    "a.phone_number=$phone_number_, " \
                    "a.linked_status= $linked_status_, " \
                    "a.linked_user_id=$linked_user_id_," \
                    " a.source_type = $source_type_," \
                    " a.location = $location_," \
                    " a.first_name=$first_name_, a.last_name=$last_name_," \
                    " a.approval_status = $approval_status_ ," \
                    " a.gender = $gender_ ," \
                    " a.age = $age_ " \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, a.email_address as email_address, " \
                    "a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, " \
                    "a.location as location, a.age as age, a.gender as gender "

            if "referred_user_id" not in hshuser or hshuser["referred_user_id"] is None:
                txn.rollback()
                current_app.logger.error("Referred user id is required to add to friend list and it is missing")
                return False

            referred_user_id = hshuser["referred_user_id"]
            if "referrer_user_id" not in hshuser or hshuser["referrer_user_id"] is None:
                txn.rollback()
                current_app.logger.error("Referrer user id is required to add to friend list and it is missing")
                return False
            referrer_user_id = hshuser["referrer_user_id"]
            result = txn.run(query,
                             phone_number_=hshuser["phone_number"] if "phone_number" in hshuser else None,
                             email_address_=hshuser["email_address"],
                             user_id_=referred_user_id,
                             friend_id_=referrer_user_id,
                             first_name_=hshuser["first_name"],
                             last_name_=hshuser["last_name"],
                             linked_status_=hshuser["linked_status"],
                             linked_user_id_=hshuser["linked_user_id"],
                             source_type_=hshuser["source_type"],
                             location_=hshuser["location"],
                             approval_status_ = hshuser["approval_status"],
                             age_ = hshuser["age"] if "age" in hshuser else None,
                             gender_ = hshuser["gender"] if "gender" in hshuser else None
                             )
            if result is None:
                loutput = None
                current_app.logger.error("The user was not inserted with phone " + hshuser["phone_number"] +
                                         " or email " + hshuser["email_address"])
                return False
            else:
                for record in result:
                    print("The record is ", record["user_id"])
                    loutput["referred_user_id"] = record["user_id"]
                    loutput["referrer_user_id"] = record["friend_id"]
                    loutput["email_address"] = record["email_address"]
                    loutput["first_name"] = record["first_name"]
                    loutput["last_name"] = record["last_name"]
                    loutput["location"] = record["location"]

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def add_friend_to_the_list_and_circle(self, hshuser, admin_flag, loutput):
        try:

            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            result = None
            output_hash = {}
            if hshuser["friend_list_flag"] == "N":
                hshuser["source_type"] = "DIRECT"
                if not self.insert_friend_by_id(hshuser, txn, output_hash):
                    txn.rollback()
                    current_app.logger.error(
                        "Unable to insert the email address into friend circle " + hshuser["email_address"])
                    print("Unable to insert the email address into friend circle " + hshuser["email_address"])
                    return False
                objMongo = MongoDBFunctions()
                hshuser["user_id"] = hshuser["referrer_user_id"]
                if int(os.environ.get("GEMIFT_VERSION")) != 2:
                    if not objMongo.insert_user(hshuser):
                        txn.rollback()
                        current_app.logger.error("Unable to insert the user to the search db" + hshuser["referred_user_id"])
                        return False
            if admin_flag == 1:
                fquery = "MATCH  (n:friend_list), (fc:friend_circle) " \
                         " WHERE n.user_id = $user_id_ " \
                         " and n.friend_id = $friend_id_ " \
                         " AND fc.friend_circle_id = $friend_circle_id_ " \
                         " CREATE (n)-[:CONTRIBUTOR]->(fc) " \
                         " RETURN fc.friend_circle_id as friend_circle_id"

                result = txn.run(fquery, user_id_=hshuser["referred_user_id"], friend_id_=hshuser["referrer_user_id"],
                                 friend_circle_id_=hshuser["friend_circle_id"], created_dt_=self.get_datetime())
                for record in result:
                    loutput["friend_circle_id"] = record["friend_circle_id"]

                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)

                if "friend_circle_id" not in loutput:
                    current_app.logger.error("Unable to add the contributor. Issue with the graph query")
                    return False
                objGDBUser = GDBUser()

                list_output = []
                if not objGDBUser.get_friend_circle(hshuser["friend_circle_id"], list_output):
                    txn.rollback()
                    current_app.logger.error(
                        "Unable to get the friend circle details ffor this contributor" + hshuser["friend_circle_id"])
                    print("Unable to get the friend circle details ffor this contributor" + hshuser["friend_circle_id"])
                    return False
                loutput["data"] = list_output
            else:
                if int(os.environ.get("GEMIFT_VERSION")) != 2:
                    objMongo = MongoDBFunctions()
                    hshuser["user_type"] = "Existing"
                    hshuser["comm_type"] = "Email"
                    if not objMongo.insert_approval_queue(hshuser):
                        txn.rollback()
                        current_app.logger.error(
                            "Unable to insert the record into the approval queue for " + hshuser["email_address"])
                        print("Unable to insert the record into the approval queue for " + hshuser["email_address"])
                        return False
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
            print("The error is ", e)
            return False

    def create_secret_friend(self, hshuser, loutput):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            result = None
            output_hash = {}
            user_output = {}
            referrer_output = {}
            objUser = GDBUser()
            referred_user_id = None
            friend_exists = "N"
            user_exists = 0

            if objUser.get_user_by_id(hshuser["referrer_user_id"], referrer_output):
                if "user_id" not in referrer_output:
                    txn.rollback()
                    current_app.logger.error("The creator is not in the user table" + hshuser["referrer_user_id"])
                    print("The creator is not in the user table ", hshuser["referrer_user_id"])
                    return False
            else:
                current_app.logger.error("Unable to retrieve data from the user table for the referrer")
                txn.rollback()
                return False

            # if objUser.get_user_by_email(hshuser["email_address"], user_output): # phone primary key support
            if objUser.get_user_by_phone(hshuser["phone_number"], user_output):
                if "user_id" in user_output and user_output["user_id"] is not None:
                    if user_output["user_id"] == hshuser["referrer_user_id"] :
                        current_app.logger.error("Creator of the friend circle cannot be the secret friend")
                        return False
                    hshuser["linked_status"] = 1
                    hshuser["linked_user_id"] = user_output["user_id"]
                    user_exists = 1
                else:
                    hshuser["linked_status"] = 0
                    hshuser["linked_user_id"] = None
            else:
                current_app.logger.error("There is an issue getting information by email" + hshuser["phone_number"])
                txn.rollback()
                return False
            # if not self.get_friend_by_email(hshuser["email_address"], "DIRECT", user_output): #phone primary key support
            if not self.get_friend_by_phone_number(hshuser["phone_number"], hshuser["referrer_user_id"], "DIRECT",
                                                   user_output):
                current_app.logger.error("There is an issue getting information by email" + hshuser["phone_number"])
                txn.rollback()
                return False
            else:
                if "referred_user_id" in user_output and user_output["referred_user_id"] is not None:
                    if user_output["referred_user_id"] == hshuser["referrer_user_id"]:
                        current_app.logger.error("Creator cannot be the secret friend")
                        return False
                    friend_exists = "Y"
                    if user_exists == 0:
                        hshuser["linked_status"] = user_output["linked_status"]
                        hshuser["linked_user_id"] = user_output["linked_user_id"]
                else:
                    hshuser["linked_status"] = 0
                    hshuser["linked_user_id"] = None
                    friend_exists = "N"

            hshuser["source_type"] = "DIRECT"
            hshuser["approval_status"] = 0
            referred_user_id = hshuser["referred_user_id"]
            if friend_exists == "N":
                # if not self.insert_friend_by_email(hshuser, txn, output_hash): #phone primary key support
                if not self.insert_friend_by_phone(hshuser, txn, output_hash):
                    txn.rollback()
                    current_app.logger.error(
                        "Unable to insert the email address into friend circle " + hshuser["phone_number"])
                    print("Unable to insert the email address into friend circle " + hshuser["phone_number"])
                    return False
                if hshuser["referred_user_id"] is None:
                    key = str(hshuser["referrer_user_id"]) + str(hshuser["phone_number"])
                    referred_user_id = output_hash[key]["user_id"]
                hshuser["user_id"] = referred_user_id  # Need this for mongo insert

                if int(os.environ.get("GEMIFT_VERSION")) != 2:
                    objMongo = MongoDBFunctions()
                    if not objMongo.insert_user(hshuser):
                        txn.rollback()
                        current_app.logger.error("Unable to insert the user to the search db" + hshuser["referred_user_id"])
                        return False
            if hshuser["group_name"] is None:
                hshuser["group_name"] = "Friend circle name " + hshuser["first_name"] + " " + hshuser["last_name"]

            friend_circle_hash = {}
            if not self.insert_friend_circle(referred_user_id, hshuser["referrer_user_id"], hshuser["group_name"],
                                             loutput, txn, hshuser):
                txn.rollback()
                current_app.logger.error(
                    "Unable to create a friend circle with " + hshuser["referred_user_id"] + " as the secret friend")
                print("Unable to create a friend circle with " + hshuser["referred_user_id"] + " as the secret friend")
                return False
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
            print("The error is ", e)
            return False

    def create_secret_friend_by_id(self, hshuser, loutput):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            result = None
            output_hash = {}
            user_output = {}
            referrer_output = {}
            friend_exists = "N"
            objUser = GDBUser()
            record_exists = 0

            if hshuser["referred_user_id"] == hshuser["referrer_user_id"]:
                current_app.logger.error("Referrer cannot be the secret friend of the group")
                return False

            if objUser.get_user_by_id(hshuser["referrer_user_id"], referrer_output):
                if "user_id" not in referrer_output:
                    txn.rollback()
                    current_app.logger.error("The creator is not in the user table" + hshuser["referrer_user_id"])
                    print("The creator is not in the user table ", hshuser["referrer_user_id"])
                    return False
            else:
                current_app.logger.error("Unable to retrieve data from the user table for the referrer")
                txn.rollback()
                return False

            if objUser.get_user_by_id(hshuser["referred_user_id"], user_output):
                if "user_id" in user_output:
                    if user_output["user_id"] is not None:
                        user_output["linked_status"] = 1
                        user_output["linked_user_id"] = user_output["user_id"]
                        record_exists = 1
                    else:
                        user_output["linked_user_id"] = None
                        user_output["linked_status"] = 0
            else:
                current_app.logger.error("Unable to retrieve data from the user table")
                txn.rollback()
                return False

            if self.get_unique_friend_by_id(hshuser["referred_user_id"], user_output):
                if "referred_user_id" in user_output and user_output["referred_user_id"] is not None:
                    friend_exists = "Y"
            else:
                current_app.logger.error("The user is not in the system" + hshuser["email_address"])
                txn.rollback()
                return False

            if not record_exists and friend_exists == "N":
                 txn.rollback()
                 current_app.logger.error(
                     "Unable to find a record anywhere for this user " + hshuser["referred_user_id"])
                 print("Unable to find a record anywhere for this user " + hshuser["referred_user_id"])
                 return False

            user_output["referrer_user_id"] = hshuser["referrer_user_id"]
            user_output["referred_user_id"] = hshuser["referred_user_id"]
            user_output["approval_status"] = 0
            user_output["source_type"] = "DIRECT"
            if "age" in hshuser :
                if hshuser["age"] is not None:
                    user_output["age"] = hshuser["age"]


            if not self.insert_friend_by_id(user_output, txn, output_hash):
                txn.rollback()
                current_app.logger.error(
                    "Unable to insert the email address into friend circle " + user_output["email_address"])
                print("Unable to insert the email address into friend circle " + user_output["email_address"])
                return False
            objMongo = MongoDBFunctions()

            if int(os.environ.get("GEMIFT_VERSION")) != 2:
                if not objMongo.insert_user(user_output):
                    txn.rollback()
                    current_app.logger.error("Unable to insert the user to the search db" + hshuser["referred_user_id"])
                    return False

            friend_circle_hash = {}
            if not self.insert_friend_circle(hshuser["referred_user_id"], hshuser["referrer_user_id"],
                                             hshuser["group_name"], loutput, txn, user_output):
                txn.rollback()
                current_app.logger.error("Unable to create a friend circle with " + user_output[
                    "referred_user_id"] + " as the secret friend")
                print("Unable to create a friend circle with " + user_output[
                    "referred_user_id"] + " as the secret friend")
                return False
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
            print("The error is ", e)
            return False

    def insert_friend_wrapper(self, hshuser, admin_flag, output_hash):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            if hshuser["friend_list_flag"] == "N":
                hshuser["source_type"] = "DIRECT"
                # if not self.insert_friend_by_email(hshuser, txn, output_hash): #phone primary key support
                if not self.insert_friend_by_phone(hshuser, txn, output_hash):
                    txn.rollback()
                    current_app.logger.error("Unable to insert this user as a friend " + hshuser["email_address"])
                    print("Unable to insert this user as a friend " + hshuser["email_address"])
                    return False
                key = str(hshuser["referrer_user_id"]) + str(hshuser["phone_number"])
                hshuser["referred_user_id"] = output_hash[key]["user_id"]
                hshuser["user_id"] = hshuser["referred_user_id"]
                if int(os.environ.get("GEMIFT_VERSION")) != 2:
                    objMongo = MongoDBFunctions()
                    if not objMongo.insert_user(hshuser):
                        txn.rollback()
                        current_app.logger.error("Unable to insert the user to the search db" + hshuser["referred_user_id"])
                        return False
            else:
                hshuser["referred_user_id"] = hshuser["user_id"]
                hshuser["referrer_user_id"] = hshuser["friend_id"]

            hshuser["user_type"] = "New"
            hshuser["comm_type"] = "Email"

            loutput = {}

            if admin_flag == 1:
                fquery = "MATCH  (n:friend_list), (fc:friend_circle) " \
                         " WHERE n.user_id = $user_id_ " \
                         " and n.friend_id = $friend_id_ " \
                         " AND fc.friend_circle_id = $friend_circle_id_ " \
                         " CREATE (n)-[:CONTRIBUTOR]->(fc) " \
                         " RETURN fc.friend_circle_id as friend_circle_id"

                result = txn.run(fquery, user_id_=hshuser["referred_user_id"], friend_id_=hshuser["referrer_user_id"],
                                 friend_circle_id_=hshuser["friend_circle_id"], created_dt_=self.get_datetime())
                for record in result:
                    loutput["friend_circle_id"] = record["friend_circle_id"]

                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
            else:
                if not objMongo.insert_approval_queue(hshuser):
                    txn.rollback()
                    current_app.logger.error(
                        "Error in inserting the record into approval  queue for " + hshuser["email_address"])
                    print("Error in inserting the record into email user queue for " + hshuser["email_address"])
                    return False
            txn.commit()
            return True
        except neo4j.exceptions.Neo4jError as e:
            txn.rollback()
            current_app.logger.error(e.message)
            print("THere is a syntax error", e.message)
            return False
        except Exception as e:
            txn.rollback()
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def insert_friend_circle(self, user_id, friend_id, friend_circle_name,  output_hash, txn, hshuser=None):

        fid = uuid.uuid4()
        try:
            """
                query = "MATCH (n:User {user_id:$friend_id_}), (m:friend_list{user_id:$user_id_, friend_id:$friend_id_})" \
                        " MERGE (n)-[:CIRCLE_CREATOR]->(x:friend_circle{friend_circle_id:$friend_circle_id_, " \
                        "friend_circle_name:$friend_circle_name_ friend_circle_created_dt:datetime()})-[" \
                        ":SECRET_FRIEND]->(m) " \
                        " RETURN x.friend_circle_id as circle_id"
                """
            select_circle = " MATCH (n:User{user_id:$friend_id_})-[:CIRCLE_CREATOR]->(x:friend_circle)" \
                            "-[:SECRET_FRIEND]->(m:friend_list{user_id:$user_id_, friend_id:$friend_id_}) " \
                            " RETURN count(x.friend_circle_id) as circle_count"

            result = txn.run(select_circle, user_id_=str(user_id), friend_id_=str(friend_id))

            if result is not None:
                record = result.single()
                if record["circle_count"] <= 0:
                    secret_first_name = hshuser["first_name"] if "first_name" in hshuser else None
                    secret_last_name = hshuser["last_name"] if "last_name" in hshuser else None
                    age = hshuser["age"] if "age" in hshuser else None
                    gender = hshuser["gender"] if "gender" in hshuser else None

                    insert_circle = " CREATE (x:friend_circle{friend_circle_id:$friend_circle_id_, " \
                                    "friend_circle_name:$friend_circle_name_, creator_id: $friend_id_, secret_friend_id:$secret_friend_id_, secret_friend_name:$secret_first_name_," \
                                    "secret_last_name:$secret_last_name_, age:$age_, gender:$gender_, created_dt:$created_dt_}) " \
                                    " WITH x " \
                                    " MATCH (n:User{user_id:$friend_id_}), (x:friend_circle{friend_circle_id:$friend_circle_id_})" \
                                    ", (m:friend_list{user_id:$user_id_, friend_id:$friend_id_})" \
                                    " MERGE (n)-[:CIRCLE_CREATOR]->(x)-[:SECRET_FRIEND]->(m)" \
                                    " RETURN x.friend_circle_id as circle_id, x.friend_circle_name as friend_circle_name"
                    result = None
                    output_hash["friend_circle_id"] = None
                    result = txn.run(insert_circle, user_id_=str(user_id), secret_friend_id_=str(user_id),
                                     friend_id_=str(friend_id), friend_circle_id_=str(fid),
                                     friend_circle_name_=friend_circle_name,
                                     secret_first_name_ = secret_first_name,
                                     secret_last_name_ = secret_last_name,
                                     age_ = age,
                                     gender_ = gender,
                                     created_dt_=self.get_datetime())
                    if result is None:
                        current_app.logger.error("The friend circle record was not inserted for the combination ",
                                                 friend_id, user_id, friend_circle_name)
                        return False

                    for record in result:
                        output_hash["friend_circle_id"] = record["circle_id"]
                        output_hash["friend_circle_name"] = record["friend_circle_name"]
                    print("The  query is ", result.consume().query)
                    print("The  parameters is ", result.consume().parameters)
                    if output_hash["friend_circle_id"] is None:
                        current_app.logger.error(
                            "Friend circle id cannot be empty. Something wrong in creating the friend circle")
                        return False
                    print("Successfully inserted friend circle", output_hash["friend_circle_id"])
            else:
                current_app.logger.error("SOmething wrong with the friend circle here")
                return False
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("THere is a syntax error", e.message)
            return False
        except Exception as e:
            print("Error in insert friend circle ", e)
            return False

    def add_contributor_to_friend_circle(self, user_id, friend_id, friend_circle_id, txn):
        try:
            print("Inside teh contrib method")
            query = " MATCH  (n:friend_list{user_id:$user_id_, friend_id:$friend_id_}),(fc:friend_circle{friend_circle_id:$friend_circle_id_}) " \
                    " MERGE (n)-[:CONTRIBUTOR]->(fc) " \
                    " RETURN fc.friend_circle_id as friend_circle_id"

            result = txn.run(query, user_id_=user_id, friend_id_=friend_id, friend_circle_id_=friend_circle_id)

            if result is not None:
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                return True
            return False
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("Error in adding contributors", e)
            return False

    def approve_requests(self, referrer_user_id, referred_user_id, list_friend_circle_id, loutput):
        # How should this work?
        # Check if there is a row in the approval queue table. If exists, update the approval_flag to 1. insert a row in the email queue table.
        # made changes on 01/09/2022 to include approval flag to the query
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            objMongo = MongoDBFunctions()
            approval_queue_collection = pymongo.collection.Collection(g.db, "approval_queue")
            not_approved = 0
            result = approval_queue_collection.find(
                {"referred_user_id": referred_user_id, "referrer_user_id": referrer_user_id,
                 "friend_circle_id": {"$in": list_friend_circle_id}, "approved_flag": not_approved})
            if result is not None:
                for row in result:
                    fquery = "MATCH  (n:friend_list), (fc:friend_circle) " \
                             " WHERE n.user_id = $user_id_ " \
                             " AND n.friend_id = $friend_id_" \
                             " AND fc.friend_circle_id = $friend_circle_id_ " \
                             " MERGE (n)-[:CONTRIBUTOR]->(fc)" \
                             " ON MATCH set n.approval_status = 0 " \
                             " RETURN fc.friend_circle_id as friend_circle_id"
                    result = txn.run(fquery, user_id_=referred_user_id,
                                     friend_id_=referrer_user_id,
                                     friend_circle_id_=row["friend_circle_id"])
                    if result is None:
                        txn.rollback()
                        current_app.logger.error("Unable to add the user to the friend circle")
                        return False
                    for record in result:
                        loutput.append(record.data())
                    print("The  query is ", result.consume().query)
                    print("The  parameters is ", result.consume().parameters)

                    if not objMongo.insert_email_queue(row):
                        txn.rollback()
                        current_app.logger.error(
                            "Unable to insert a row into the email table for " + result["referrer_user_id"])
                        return False
                update_record = approval_queue_collection.update_many(
                    {"referred_user_id": referred_user_id, "referrer_user_id": referrer_user_id,
                     "friend_circle_id": {"$in": list_friend_circle_id}},
                    {"$set": {"approved_flag": 1, "approved_dt": date.today().strftime("%d/%m/%Y")}}
                    )
                if update_record is None:
                    txn.rollback()
                    current_app.logger.error(
                        "Error in updating the records for user " + referrer_user_id + "for circles " + list_friend_circle_id)
                    return False
            txn.commit()
            return True

        except neo4j.exceptions.Neo4jError as e:
            txn.rollback()
            print("THere is a syntax error", e.message)
            return False

        except pymongo.errors as e:
            txn.rollback()
            current_app.logger.error("The error is " + e)
            return False

    def add_secret_friend_age(self, user_id, friend_circle_id, age, gender=None):
        try:
            driver = NeoDB.get_session()
            query = " MATCH (fc:friend_circle { friend_circle_id:$friend_circle_id_} )" \
                    " SET fc.age = $age_," \
                    " fc.gender = $gender_," \
                    " fc.last_set_by = $user_id_, " \
                    " fc.updated_dt = $updated_dt_" \
                    " RETURN fc.friend_circle_id as friend_circle_id"

            result = driver.run(query, user_id_=user_id, age_ = age, gender_ = gender, updated_dt_=self.get_datetime(),
                                friend_circle_id_=friend_circle_id)

            if result is not None:
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                return True
            return False
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("Error in adding contributors", e)
            return False

    def get_secret_friend_age_gender(self, friend_circle_id, list_output):
        try:
            driver = NeoDB.get_session()
            query = " MATCH (fc:friend_circle ) " \
                    " WHERE fc.friend_circle_id = $friend_circle_id_ " \
                    " RETURN fc.friend_circle_id as friend_circle_id, fc.age as age, fc.gender as gender," \
                    " fc.secret_first_name as secret_first_name, fc.secret_last_name as secret_last_name"

            result = driver.run(query,
                                friend_circle_id_=friend_circle_id)

            for record in result:
                list_output.append(record.data())
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("Error in adding contributors", e)
            return False


    def add_relationship(self, user_id, friend_circle_id, relation_type):
        try:
            # Here the assumption is the user cannot set relationship unless registered w
            driver = NeoDB.get_session()
            query = " MATCH  (n:User{user_id:$user_id_}),(fc:friend_circle{friend_circle_id:$friend_circle_id_}) " \
                    " MERGE (n)-[r:RELATIONSHIP]->(fc) " \
                    " ON CREATE " \
                    " SET r.relation_type = $relation_type," \
                    "r.created_dt = $created_dt_ " \
                    " RETURN fc.friend_circle_id as friend_circle_id"

            result = driver.run(query, user_id_=user_id,
                                relation_type_=relation_type,
                                friend_circle_id_=friend_circle_id,
                                created_dt_=self.get_datetime())

            if result is not None:
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                return True
            return False
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("Error in adding contributors", e)
            return False

    def update_gender_age(self, friend_circle_id, gender=None, age=None):
        try:
            driver = NeoDB.get_session()
            if age is not None and gender is not None:
                query = "MATCH (u:friend_circle{friend_circle_id:$friend_circle_id_}) " \
                        "SET " \
                        " u.gender = $gender_, " \
                        " u.age = $age_" \
                        " return count(u.friend_circle_id) as user_count"
                result = driver.run(query, friend_circle_id_= friend_circle_id, gender_=gender, age_ = age)
            elif gender is not None:
                query = "MATCH (u:friend_circle{friend_circle_id:$friend_circle_id_}) " \
                        "SET " \
                        " u.gender = $gender_ " \
                        " return count(u.friend_circle_id) as user_count"
                result = driver.run(query, friend_circle_id_= friend_circle_id, gender_=gender)
            elif age is not None:
                query = "MATCH (u:friend_circle{friend_circle_id:$friend_circle_id_}) " \
                        "SET " \
                        " u.age = $age_ " \
                        " return count(u.friend_circle_id) as user_count"
                result = driver.run(query, friend_circle_id_= friend_circle_id, age_=age)


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


    def upload_image(self, image_type, entity_id, image_url):
        try:
            # Here the assumption is the user cannot set relationship unless registered w
            driver = NeoDB.get_session()
            if image_type == "user":
                query = " MATCH  (n:User{user_id:$user_id_}) " \
                    " SET n.image_url = $image_url_ ," \
                    " n.updated_dt = $updated_dt_"
                result = driver.run(query, user_id_=entity_id,
                                    updated_dt_=self.get_datetime(),
                                    image_url_ = image_url)

            if image_type == "friend_circle":
                query = " MATCH  (n:friend_circle{friend_circle_id:$friend_circle_id_}) " \
                    " SET n.image_url = $image_url_, " \
                    " n.updated_dt_ = $updated_dt_" \
                    " RETURN n.friend_circle_id as friend_circle_id " \

                result = driver.run(query,
                                friend_circle_id_=entity_id,
                                updated_dt_=self.get_datetime(),
                                image_url_ = image_url)

            if result is not None:
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                return True
            return False
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("Error in adding contributors", e)
            return False

    def contributor_approval(self, friend_circle_id, phone_number, approval_status):
        try:
            # Here the assumption is the user cannot set relationship unless registered w
            driver = NeoDB.get_session()

            query = "MATCH (fl:friend_list{phone_number:$phone_number_})-[:CONTRIBUTOR]->(fc:friend_circle{friend_circle_id:$friend_circle_id_}) " \
                    " SET fl.approval_status = $approval_status_, " \
                    " fl.updated_dt = $updated_dt_" \
                    " RETURN fc.friend_circle_id as friend_circle_id, " \
                    " fl.linked_user_id as linked_user_id"

            result = driver.run(query, friend_circle_id_ = friend_circle_id,
                                phone_number_ = phone_number,
                                approval_status_ = approval_status,
                                updated_dt_ = self.get_datetime())

            for record in result:
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                return True
            return False
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("Error in adding contributors", e)
            return False

    def get_open_invites(self,  phone_number,list_output):
            try:
                # Here the assumption is the user cannot set relationship unless registered w
                driver = NeoDB.get_session()
                query = "MATCH (fl:friend_list)-[:CONTRIBUTOR]->(fc:friend_circle) " \
                        " WHERE fl.phone_number = $phone_number_ and " \
                        " fl.approval_status = 0 " \
                        " RETURN fc.friend_circle_id as friend_circle_id, " \
                        " fl.linked_user_id as linked_user_id, " \
                        " fc.friend_circle_name as friend_circle_name, " \
                        " fc.secret_friend_name as secret_first_name ," \
                        " fc.secret_last_name as secret_last_name, " \
                        " fc.secret_friend_id as secret_friend_id"

                result = driver.run(query,
                                    phone_number_=phone_number)

                for record in result:
                    list_output.append(record.data())

                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                return True
            except neo4j.exceptions.Neo4jError as e:
                print("THere is a syntax error", e.message)
                return False
            except Exception as e:
                current_app.logger.error(e)
                print("Error in adding contributors", e)
                return False