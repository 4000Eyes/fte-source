import json
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
#user_id
#friend_id
#email_address
#phone_number
#linked_status
#linked_user_id
#first_name
#last_name
#location
#gender

class FriendListDB:
    def __init__(self):
        self.__dttime = None
        self.__uid = None
    def get_datetime(self):
        self.__dttime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return self.__dttime
    def get_id(self):
        self.__uid = str(uuid.uuid4())
        return  self.__uid

    def get_friend_list(self, friend_id, loutput):

        try:
            driver = NeoDB.get_session()

            query = "MATCH (a:friend_list)" \
                    " WHERE a.email_address = $email_address_" \
                    " AND a.friend_id = $friend_id_" \
                    " RETURN a.email_address, a.phone_number, a.name, a.location "
            result = driver.run (query, friend_id_ = friend_id)
            for record in result:
                loutput.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print ("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print ("The error is ", e)
            return False

    def get_friend(self, email_address, phone_number, user_id, loutput):

        try:
            driver = NeoDB.get_session()
            if email_address is not None:
                query = "MATCH (a:friend_list)" \
                        " WHERE a.email_address = $email_address_" \
                        " AND a.user_id = $user_id_" \
                        " RETURN a.email_address, a.phone_number, a.name, a.location "
                result = driver.run (query, email_address_ = email_address, user_id_ = user_id)
            elif phone_number is not None:
                query = "MATCH (a:friend_list)" \
                        " WHERE a.phone_number = $phone_number_" \
                        " AND a.user_id = $user_id_" \
                        " RETURN a.user_id, a.email_address, a.phone_number, a.name, a.location "

                result = driver.run(query, phone_number_ = phone_number, user_id_=user_id)

            for record in result:
                loutput.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print ("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print ("The error is ", e)
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
                    "a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location as location "

            result = driver.run(query, user_id_ = user_id, friend_id_ = referrer_user_id)

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

            result = driver.run(query, email_address_ = email_address, friend_id_ = referrer_user_id, source_type_ = source_type)
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

    def get_friend_by_phone_number(self, email_address, referrer_user_id, source_type, hshOutput):

        try:
            driver = NeoDB.get_session()
            hshOutput["referred_user_id"] = None
            query = "MATCH (a:friend_list)" \
                    " WHERE " \
                    " a.phone_number = $phone_number AND" \
                    " a.friend_id = $friend_id_ AND " \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, a.email_address as email_address, " \
                    "a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location as location "

            result = driver.run(query, email_address_ = email_address, friend_id_ = referrer_user_id, source_type_ = source_type)
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

    def get_friend_su_by_id(self, user_id, list_output):

        try:
            driver = NeoDB.get_session()
            list_output = None
            query = "MATCH (a:friend_list)" \
                    " WHERE " \
                    " a.user_id = $user_id_ " \
                    " RETURN distinct a.user_id as user_id, a.linked_user_id as linked_user_id"

            result = driver.run(query, user_id_ = user_id)
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
            print ("The email address and the number is ", hshuser["email_address"], hshuser["phone_number"])
            if (hshuser["email_address"] is  None and hshuser["phone_number"] is not None) or (hshuser["email_address"] is not None and hshuser["phone_number"] is not None):
                print ("Inside teh phone number for loop")
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
                                    user_id_= self.get_id(),
                                    friend_id_= hshuser["admin_friend_id"] if "admin_friend_id" in hshuser else self.get_id(),
                                    first_name_=hshuser["first_name"],
                                    last_name_=hshuser["last_name"],
                                    linked_status_=hshuser["linked_status"],
                                    linked_user_id_=hshuser["linked_user_id"],
                                    source_type_ = hshuser["source_type"]
                                    )

                print ("The result is ", result, result.peek())
                if result is None:
                    loutput = None
                    current_app.logger.error("The user was not inserted with phone " +  hshuser["phone_number"] +
                                         " or email " + hshuser["email_address"] )
                    return False
                else:

                    for record in result:
                        print ("The record is ", record["user_id"], record["phone_number"])
                        #This is get unique key.
                        #We may need to look at this function if we have to get user information by email address.
                        # DO NOT CHANGE and remember the logic. This is key for whatsapp to work,
                        loutput[record["friend_id"] + record["phone_number"] ] = record

                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                print ("Successfully inserted friend")
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

            print ("The email address and the number is ", hshuser["email_address"])
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
                                email_address_ = hshuser["email_address"],
                                user_id_ = referred_user_id,
                                friend_id_ = referrer_user_id,
                                first_name_ = hshuser["first_name"],
                                last_name_ = hshuser["last_name"],
                                linked_status_ = hshuser["linked_status"],
                                linked_user_id_ = hshuser["linked_user_id"],
                                source_type_ = hshuser["source_type"]
                                )
            if result is None:
                loutput = None
                current_app.logger.error("The user was not inserted with phone " +  hshuser["phone_number"] +
                                     " or email " + hshuser["email_address"] )
                return False
            else:
                for record in result:
                    print ("The record is ", record["user_id"])
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

            print ("The email address and the number is ", hshuser["email_address"])
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
                    " a.first_name=$first_name_, a.last_name=$last_name_" \
                    " RETURN a.user_id as user_id, a.friend_id as friend_id, a.email_address as email_address, a.phone_number as phone_number, a.first_name as first_name, a.last_name as last_name, a.location as location "

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
                                email_address_ = hshuser["email_address"],
                                user_id_ = referred_user_id,
                                friend_id_ = referrer_user_id,
                                first_name_ = hshuser["first_name"],
                                last_name_ = hshuser["last_name"],
                                linked_status_ = hshuser["linked_status"],
                                linked_user_id_ = hshuser["linked_user_id"],
                                source_type_ = hshuser["source_type"]
                                )
            if result is None:
                loutput = None
                current_app.logger.error("The user was not inserted with phone " +  hshuser["phone_number"] +
                                     " or email " + hshuser["email_address"] )
                return False
            else:
                for record in result:
                    print ("The record is ", record["user_id"])
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
                    current_app.logger.error("Unable to insert the email address into friend circle " + hshuser["email_address"])
                    print("Unable to insert the email address into friend circle " + hshuser["email_address"])
                    return False
                objMongo = MongoDBFunctions()
                if not objMongo.insert_user(hshuser):
                    txn.rollback()
                    current_app.logger.error("Unable to insert the user to the search db" + hshuser["referred_user_id"])
                    return False
            if admin_flag == 1:
                fquery = "MATCH  (n:friend_list), (fc:friend_circle) " \
                        " WHERE n.user_id = $user_id_ " \
                        " AND fc.friend_circle_id = $friend_circle_id_ " \
                        " CREATE (n)-[:CONTRIBUTOR]->(fc) " \
                        " RETURN fc.friend_circle_id as friend_circle_id"

                result = txn.run(fquery, user_id_= hshuser["referred_user_id"], friend_id_=hshuser["referrer_user_id"], friend_circle_id_=hshuser["friend_circle_id"], created_dt_ = self.get_datetime())
                for record in result:
                    loutput["friend_circle_id"] = record["friend_circle_id"]

                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)

                if "friend_circle_id" not in loutput:
                    current_app.logger.error("Unable to add the contributor. Issue with the graph query")
                    return False
                objGDBUser = GDBUser()

                if not objGDBUser.get_friend_circles(hshuser["friend_circle_id"], loutput):
                    txn.rollback()
                    current_app.logger.error("Unable to get the friend circle details ffor this contributor" + hshuser["friend_circle_id"])
                    print ("Unable to get the friend circle details ffor this contributor" + hshuser["friend_circle_id"])
                    return False
            else:
                objMongo = MongoDBFunctions()
                hshuser["user_type"] = "Existing"
                hshuser["comm_type"] = "Email"
                if not objMongo.insert_approval_queue(hshuser):
                    txn.rollback()
                    current_app.logger.error("Unable to insert the record into the approval queue for " + hshuser["email_address"])
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
            objUser = GDBUser()
            referred_user_id = None
            friend_exists = "N"
            if objUser.get_user_by_email(hshuser["email_address"], user_output):
                if "user_id" in user_output and user_output["user_id"] is not None:

                    hshuser["linked_status"] = 1
                    hshuser["linked_user_id"] = user_output["user_id"]
                    user_output["referrer_user_id"] = hshuser["referrer_user_id"]
                    user_output["referred_user_id"] = hshuser["referred_user_id"]
                else:
                    hshuser["linked_status"] = 0
                    hshuser["linked_user_id"] = None
                    if not self.get_friend_by_email(hshuser["email_address"], "DIRECT", user_output):
                        current_app.logger.error("There is an issue getting information by email" + hshuser["email_address"])
                        txn.rollback()
                        return False
                    else:
                        if "referred_user_id" in user_output and user_output["referred_user_id"] is not None:
                            friend_exists = "Y"
                            referred_user_id = user_output["referred_user_id"]
                            hshuser["linked_status"] = user_output["linked_status"]
                            hshuser["linked_user_id"] = user_output["linked_user_id"]
                        else:
                            hshuser["linked_status"] = 0
                            hshuser["linked_user_id"] = None
                            friend_exists = "N"
            else:
                current_app.logger.error("There is an issue getting information by email" +  hshuser["email_address"])
                txn.rollback()
                return False
            hshuser["source_type"] = "DIRECT"

            if friend_exists == "N":
                if not self.insert_friend_by_email(hshuser, txn, output_hash):
                    txn.rollback()
                    current_app.logger.error("Unable to insert the email address into friend circle " + hshuser["email_address"])
                    print("Unable to insert the email address into friend circle " + hshuser["email_address"])
                    return False
                if hshuser["referred_user_id"] is None:
                    key = hshuser["referrer_user_id"] + hshuser["email_address"]
                    referred_user_id = output_hash[key]["user_id"]
                hshuser["user_id"] = referred_user_id # Need this for mongo insert
                objMongo = MongoDBFunctions()
                if not objMongo.insert_user(hshuser):
                    txn.rollback()
                    current_app.logger.error("Unable to insert the user to the search db" + hshuser["referred_user_id"])
                    return False

            if hshuser["group_name"] is None:
                hshuser["group_name"] = "Friend circle name " + hshuser["first_name"] + " " + hshuser["last_name"]

            friend_circle_hash = {}
            if not self.insert_friend_circle(referred_user_id, hshuser["referrer_user_id"], hshuser["group_name"],loutput, txn ):
                txn.rollback()
                current_app.logger.error("Unable to create a friend circle with " + hshuser["referred_user_id"] + " as the secret friend")
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
            friend_exists = "N"
            objUser = GDBUser()
            record_exists = 0
            if objUser.get_user_by_id(hshuser["referred_user_id"], user_output):
                if "user_id" in user_output:
                    if user_output["user_id"] is not None:
                        user_output["linked_status"] = 1
                        user_output["linked_user_id"] = user_output["user_id"]
                        user_output["referrer_user_id"] = hshuser["referrer_user_id"]
                        user_output["referred_user_id"] = hshuser["referred_user_id"]
                        record_exists = 1
                    else:
                        if self.get_friend_by_id(hshuser["referred_user_id"], hshuser["referrer_user_id"], user_output):
                            if "referred_user_id" in user_output and user_output["referred_user_id"] is not None:
                                record_exists = 1
                                friend_exists = "Y"
                            else:
                                user_output["linked_user_id"] = None
                                user_output["linked_status"] = 0

                        else:
                                current_app.logger.error("The user is not in the system" + hshuser["email_address"])
                                txn.rollback()
                                return False
            else:
                current_app.logger.error("Unable to retrieve data from the user table")
                txn.rollback()
                return False

            if not record_exists:
                txn.rollback()
                current_app.logger.error(
                    "Unable to find a record anywhere for this user " + user_output["referred_user_id"])
                print("Unable to find a record anywhere for this user "  + user_output["referred_user_id"])
                return False

            user_output["source_type"] = "DIRECT"
            if friend_exists == "N":
                if not self.insert_friend_by_id(user_output, txn, output_hash):
                    txn.rollback()
                    current_app.logger.error("Unable to insert the email address into friend circle " + user_output["email_address"])
                    print("Unable to insert the email address into friend circle " + user_output["email_address"])
                    return False
                objMongo = MongoDBFunctions()
                if not objMongo.insert_user(hshuser):
                    txn.rollback()
                    current_app.logger.error("Unable to insert the user to the search db" + hshuser["referred_user_id"])
                    return False
            if hshuser["group_name"] is None:
                hshuser["group_name"] = "Friend circle name " + hshuser["first_name"] + " " + hshuser["last_name"]
            friend_circle_hash = {}
            if not self.insert_friend_circle(hshuser["referred_user_id"], hshuser["referrer_user_id"], hshuser["group_name"],loutput, txn ):
                txn.rollback()
                current_app.logger.error("Unable to create a friend circle with " + user_output["referred_user_id"] + " as the secret friend")
                print("Unable to create a friend circle with " + user_output["referred_user_id"] + " as the secret friend")
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

    def insert_friend_wrapper(self, hshuser, output_hash):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            if hshuser["friend_list_flag"] == "N":
                hshuser["source_type"] = "DIRECT"
                if not self.insert_friend_by_email(hshuser, txn, output_hash):
                    txn.rollback()
                    current_app.logger.error("Unable to insert this user as a friend " + hshuser["email_address"])
                    print("Unable to insert this user as a friend " + hshuser["email_address"])
                    return False
                key = hshuser["referrer_user_id"] + hshuser["email_address"]
                hshuser["referred_user_id"] = output_hash[key]["user_id"]
                hshuser["user_id"] = hshuser["referred_user_id"]
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

            if not objMongo.insert_approval_queue(hshuser):
                txn.rollback()
                current_app.logger.error("Error in inserting the record into approval  queue for " + hshuser["email_address"])
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

    def insert_friend_circle(self, user_id, friend_id, friend_circle_name, output_hash, txn):

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
                        insert_circle = " CREATE (x:friend_circle{friend_circle_id:$friend_circle_id_, " \
                                        "friend_circle_name:$friend_circle_name_, creator_id: $friend_id_, secret_friend_id:$secret_friend_id_, created_dt:$created_dt_}) " \
                                        " WITH x " \
                                        " MATCH (n:User{user_id:$friend_id_}), (x:friend_circle{friend_circle_id:$friend_circle_id_})" \
                                        ", (m:friend_list{user_id:$user_id_, friend_id:$friend_id_})" \
                                        " MERGE (n)-[:CIRCLE_CREATOR]->(x)-[:SECRET_FRIEND]->(m)" \
                                        " RETURN x.friend_circle_id as circle_id, x.friend_circle_name as friend_circle_name"
                        result = None
                        output_hash["friend_circle_id"] = None
                        result = txn.run(insert_circle, user_id_=str(user_id), secret_friend_id_ = str(user_id),  friend_id_=str(friend_id), friend_circle_id_=str(fid),
                                        friend_circle_name_=friend_circle_name, created_dt_ = self.get_datetime())
                        if result is None:
                            current_app.logger.error("The friend circle record was not inserted for the combination ", friend_id, user_id, friend_circle_name)
                            return False

                        for record in result:
                            output_hash["friend_circle_id"] = record["circle_id"]
                            output_hash["friend_circle_name"] = record["friend_circle_name"]
                        print("The  query is ",     result.consume().query)
                        print("The  parameters is ", result.consume().parameters)
                        if output_hash["friend_circle_id"] is None:
                            current_app.logger.error("Friend circle id cannot be empty. Something wrong in creating the friend circle")
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
                print ("Error in insert friend circle ", e)
                return False


    def add_contributor_to_friend_circle(self, user_id, friend_id, friend_circle_id,  txn):
        try:
            print("Inside teh contrib method")
            query = " MATCH  (n:friend_list{user_id:$user_id_, friend_id:$friend_id_}),(fc:friend_circle{friend_circle_id:$friend_circle_id_}) " \
                    " MERGE (n)-[:CONTRIBUTOR]->(fc) " \
                    " RETURN fc.friend_circle_id as friend_circle_id"

            result = txn.run(query, user_id_=user_id, friend_id_ = friend_id, friend_circle_id_=friend_circle_id)

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
            print ("Error in adding contributors", e)
            return False


    def approve_requests(self, referrer_user_id, referred_user_id, list_friend_circle_id, loutput):
        # How should this work?
        # Check if there is a row in the approval queue table. If exists, update the approval_flag to 1. insert a row in the email queue table.

        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            objMongo = MongoDBFunctions()
            approval_queue_collection = pymongo.collection.Collection(g.db, "approval_queue")
            not_approved = 0
            result = approval_queue_collection.find({"referred_user_id": referred_user_id, "referrer_user_id":referrer_user_id, "friend_circle_id": {"$in": list_friend_circle_id}, "approved_flag":not_approved})
            if result is not None:
                for row in result:
                    fquery = "MATCH  (n:friend_list), (fc:friend_circle) " \
                             " WHERE n.user_id = $user_id_ " \
                             " AND n.friend_id = $friend_id_" \
                             " AND fc.friend_circle_id = $friend_circle_id_ " \
                             " MERGE (n)-[:CONTRIBUTOR]->(fc) " \
                             " RETURN fc.friend_circle_id as friend_circle_id"
                    result = txn.run(fquery, user_id_= referred_user_id,
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
                        current_app.logger.error("Unable to insert a row into the email table for " + result["referrer_user_id"])
                        return False
                update_record = approval_queue_collection.update_many({"referred_user_id":referred_user_id, "referrer_user_id": referrer_user_id, "friend_circle_id":{"$in": list_friend_circle_id}},
                                                                      {"$set": {"approved_flag" : 1, "approved_dt": date.today().strftime("%d/%m/%Y")}}
                                                                      )
                if update_record is None:
                    txn.rollback()
                    current_app.logger.error("Error in updating the records for user " + referrer_user_id + "for circles " + list_friend_circle_id)
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

    def add_secret_friend_age(self, age, user_id):
        return True
