import json
import neo4j.exceptions
import logging
from flask import current_app
from flask_restful import Resource
from .extensions import NeoDB
from datetime import datetime
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

    def insert_friend(self, hshuser, loutput):

        try:
            driver = NeoDB.get_session()
            if hshuser["email_address"] is not None and hshuser["phone_number"] is None:

                query = "MERGE (a:friend_list {email_address:$email_address_) " \
                        "ON MATCH set a.phone_number = $phone_number_ , " \
                        "linked_status= $linked_status_, " \
                        "linked_user_id=$linked_user_id_" \
                        "ON CREATE set a.user_id = $user_id_, " \
                        "a.friend_id=$friend_id, " \
                        "a.email_address=$email_address_, " \
                        "a.phone_number=$phone_number, " \
                        " a.first_name=$first_name_, a.last_name=$last_name_" \
                        " RETURN a.user_id, a.friend_id, a.email_address, a.phone_number, a.first_name, a.last_name, a.location "
                result = driver.run(query,
                                    phone_number_=hshuser["phone_number"],
                                    email_address_ = hshuser["email_address"],
                                    user_id_ = self.get_id(),
                                    friend_id = hshuser["user_id"] if "user_id" in hshuser else self.get_id(),
                                    first_name_ = hshuser["first_name"],
                                    last_name_ = hshuser["last_name"],
                                    linked_status_ = hshuser["linked_status"],
                                    linked_user_id = hshuser["linked_user_id"]
                                    )
            if hshuser["email_address"] is  None and hshuser["phone_number"] is not None:
                query = "MERGE (a:friend_list { phone_number:$phone_number_)" \
                        "ON MATCH set a.email_address=$email_address_ " \
                        "linked_status= $linked_status_, " \
                        "linked_user_id=$linked_user_id_" \
                        "ON CREATE set a.user_id = $user_id_, a.friend_id=$friend_id, a.email_address=$email_address_, a.phone_number=$phone_number, " \
                        " a.first_name=$first_name_, a.last_name=$last_name_" \
                        " RETURN a.user_id, a.friend_id, a.email_address, a.phone_number, a.first_name, a.last_name , a.location "
                result = driver.run(query,
                                    phone_number_=hshuser["phone_number"],
                                    email_address_=hshuser["email_address"],
                                    user_id_= self.get_id(),
                                    friend_id = hshuser["user_id"] if "user_id" in hshuser else self.get_id(),
                                    first_name_=hshuser["first_name"],
                                    last_name=hshuser["last_name"],
                                    linked_status_=hshuser["linked_status"],
                                    linked_user_id=hshuser["linked_user_id"]
                                    )
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)

            if result.peek() is None:
                loutput = None
                current_app.logger.error("The user was not inserted with phone " +  hshuser["phone_number"] +
                                         " or email " + hshuser["email_address"] )
                return False
            else:
                for record in result:
                    loutput.append[record]

                return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            print("The error is ", e.message)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False


