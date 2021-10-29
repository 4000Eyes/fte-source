import json
import neo4j.exceptions
import logging
from flask import current_app
from flask_restful import Resource
from .extensions import NeoDB
from .gdbmethods import GDBUser
from .mongodbfunc import MongoDBFunctions
from datetime import datetime
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

            if result.peek() is None:
                return False

            for record in result:
                loutput.append(record)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print ("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print ("The error is ", e)
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
                        " a.first_name=$first_name_, a.last_name=$last_name_, a.linked_status=$linked_status_, a.linked_user_id=$linked_user_id_" \
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
                                    linked_user_id_=hshuser["linked_user_id"]
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
                                linked_user_id_ = hshuser["linked_user_id"]
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

    def add_friend_to_the_list_and_circle(self, hshuser, loutput):
        try:

            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            result = None
            output_hash = {}
            if not self.insert_friend_by_email(hshuser, txn, output_hash):
                txn.rollback()
                current_app.logger.error("Unable to insert the email address into friend circle " + hshuser["email_address"])
                print("Unable to insert the email address into friend circle " + hshuser["email_address"])
                return False
            fquery = "MATCH  (n:friend_list), (fc:friend_circle) " \
                    " WHERE n.user_id = $user_id_ " \
                    " AND n.friend_id = $friend_id_" \
                    " AND fc.friend_circle_id = $friend_circle_id_ " \
                    " MERGE (n)-[:CONTRIBUTOR]->(fc) " \
                    " RETURN fc.friend_circle_id as friend_circle_id"
            key = hshuser["referrer_user_id"]+hshuser["email_address"]
            result = txn.run(fquery, user_id_= output_hash[key]["user_id"], friend_id_=hshuser["referrer_user_id"], friend_circle_id_=hshuser["friend_circle_id"])
            if result is None:
                txn.rollback()
                current_app.logger.error("Unable to add the user to the friend circle")
                return False
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            objMongo = MongoDBFunctions()
            hshuser["referred_user_id"] = output_hash[key]["user_id"] # NOTE: Altering the inputs. I know it is a bad idea, but cutting corners for now.
            if not objMongo.insert_approval_queue(hshuser):
                txn.rollback()
                current_app.logger.error("Unable to insert the record into the approval queue for " + hshuser["email_address"])
                print("Unable to insert the record into the approval queue for " + hshuser["email_address"])
                return False
            txn.commit()
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
            if objUser.get_user_by_email(hshuser["email_address"], user_output):
                hshuser["linked_status"] = 1 if "user_id" in user_output else 0
                hshuser["linked_user_id"] = user_output["user_id"] if "user_id" in user_output else None
            if not self.insert_friend_by_email(hshuser, txn, output_hash):
                txn.rollback()
                current_app.logger.error("Unable to insert the email address into friend circle " + hshuser["email_address"])
                print("Unable to insert the email address into friend circle " + hshuser["email_address"])
                return False
            if hshuser["referred_user_id"] is None:
                key = hshuser["referrer_user_id"] + hshuser["email_address"]
            circle_name = "Friend circle name "
            friend_circle_hash = {}
            if not self.insert_friend_circle(output_hash[key]["user_id"], hshuser["referrer_user_id"], circle_name,friend_circle_hash, txn ):
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


    def insert_friend_wrapper(self, hshuser, output_hash):
        try:
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            if not self.insert_friend_by_email(hshuser, txn, output_hash):
                txn.rollback()
                current_app.logger.error("Unable to insert this user as a friend " + hshuser["email_address"])
                print("Unable to insert this user as a friend " + hshuser["email_address"])
                return False
            key = hshuser["referrer_user_id"] + hshuser["email_address"]
            hshuser["referred_user_id"] = output_hash[key]["user_id"]
            objMongo = MongoDBFunctions()
            if not objMongo.insert_email_queue(hshuser):
                txn.rollback()
                current_app.logger.error("Error in inserting the record into email user queue for " + hshuser["email_address"])
                print ("Error in inserting the record into email user queue for " + hshuser["email_address"])
                return False
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

    def insert_friend_circle(self, user_id, friend_id, friend_circle_name, friend_circle_id, txn):

            fid = uuid.uuid4()
            try:
                """
                query = "MATCH (n:User {user_id:$friend_id_}), (m:friend_list{user_id:$user_id_, friend_id:$friend_id_})" \
                        " MERGE (n)-[:CIRCLE_CREATOR]->(x:friend_circle{friend_circle_id:$friend_circle_id_, " \
                        "friend_circle_name:$friend_circle_name_, friend_circle_created_dt:datetime()})-[" \
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
                                        "friend_circle_name:$friend_circle_name_, created_dt:$created_dt_}) " \
                                        " WITH x " \
                                        " MATCH (n:User{user_id:$friend_id_}), (x:friend_circle{friend_circle_id:$friend_circle_id_})" \
                                        ", (m:friend_list{user_id:$user_id_, friend_id:$friend_id_})" \
                                        " MERGE (n)-[:CIRCLE_CREATOR]->(x)-[:SECRET_FRIEND]->(m)" \
                                        " RETURN x.friend_circle_id as circle_id"
                        result = None
                        result = txn.run(insert_circle, user_id_=str(user_id), friend_id_=str(friend_id), friend_circle_id_=str(fid),
                                        friend_circle_name_=friend_circle_name, created_dt_ = self.get_datetime())
                        if result is None:
                            current_app.logger.error("The friend circle record was not inserted for the combination ", friend_id, user_id, friend_circle_name)
                            return False
                        for record in result:
                            friend_circle_id["friend_circle_id"] = record["circle_id"]
                        print("The  query is ",     result.consume().query)
                        print("The  parameters is ", result.consume().parameters)
                        if len(friend_circle_id) <= 0:
                            current_app.logger.error("Friend circle id cannot be empty. Something wrong in creating the friend circle")
                            return False
                        print("Successfully inserted friend circle", friend_circle_id["friend_circle_id"])
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

