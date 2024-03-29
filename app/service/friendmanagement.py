from flask import request, current_app, jsonify
from app.model.gdbmethods import GDBUser
from app.model.friendlistdb import FriendListDB
from app.model.categorydb import CategoryManagementDB
from flask_restful import Resource
from app.model.classhelper import FriendCircleHelper
from flask_jwt_extended import jwt_required
from app.service.general import SiteGeneralFunctions
from app.model.kafka_producer import KafkaMessageProducer
import os
from datetime import datetime, tzinfo, timedelta
from dateutil.relativedelta import relativedelta

import json

#are

class ManageFriendCircle(Resource):
   # @jwt_required()
    def post(self):
        try:
            objGDBUser = GDBUser()
            objFriend = FriendListDB()
            content = request.get_json()

            if content is None:
                current_app.logger.error("No parameters send into the friend circle api (post). Check")
                return {"Error":"failure. No parameters sent "}, 500
            if "request_id" not in content:
                current_app.logger.error("No request id in the request")
                return {"Error": "Request id is missing"}, 400
            request_id = content["request_id"]

            user_list = content["user_list"] if "user_list" in content else None

            if request_id == 5 and content["user_list"] is None:
                current_app.logger.error("User list is a key parameter for request id 5 and it is missine")
                return {"Error": "User list is a key parameter for request id 5 and it is missine"}, 500


            user_info = {}
            user_info["admin_friend_id"] =content["admin_friend_id"] if "admin_friend_id" in content else None
            user_info["referred_user_id"] =content["referred_user_id"] if "referred_user_id" in content else None
            user_info["referrer_user_id"] =content["referrer_user_id"] if "referrer_user_id" in content else None
            user_info["friend_circle_id"] =content["friend_circle_id"] if "friend_circle_id" in content else None
            user_info["country_code"] = content["country_code"] if "country_code" in content else None
            user_info["phone_number"] =content["phone_number"] if "phone_number" in content else None
            user_info["first_name"] = content["first_name"] if "first_name" in content else None
            user_info["last_name"] =content["last_name"] if "last_name" in content else None
            user_info["email_address"]=content["email_address"] if "email_address" in content else None
            user_info["gender"]= content["gender"] if "gender" in content else None
            user_info["location"] = content["location"] if "location" in content else None
            user_info["friend_circle_name"] = content["friend_circle_name"] if "friend_circle_name" in content else None
            user_info["list_friend_circle_id"] = content["list_friend_circle_id"] if "list_friend_circle_id" in content else None
            user_info["group_name"] = content["group_name"] if "group_name" in content else None
            user_info["approval_status"] = content["approval_status"] if "approval_status" in content else None
            user_info["approval_flag"] = content["approval_flag"] if "approval_flag" in content else None
            user_info["image_url"] = content["image_url"] if "image_url" in content else None
            user_info["age"] = content["age"] if "age" in content else None

            # clean up the inputs before calling the functions
            output = []
            # various post methods
            # request_id : 1 --> referring an existing member to an existing friend circle - This will require referrer_user_id, friend_circle_id, friend_user_id
            # request_id : 2 --> referring a non-existing user friend to an existing friend circle - This will require referrer_user_id, friend_circle_id, email_address, name.
            # requests_id : 3 --> creating a friend circle for an existing member as the secret friend - This will require creator_user_id, friend_id, circle name
            # request_id : 4 --> creating a friend circle for a non-existing member as the secret friend - This will require creator_user_id, email_address, name, circle_name
            # request_id : 5 --> a list of friends or contacts from whatsapp to create friend circles.
            # request_id : 6 --> Approve the friend request by the adnin
            # req
            # Note: Admin should be able to add friends without approval.

            output = {}
            if request_id == 1:
                if not objGDBUser.get_user_by_id(user_info["referred_user_id"], output):
                    current_app.logger.error("Error in checking the user table for id", user_info["referred_user_id"])
                    return {"Error": "Failure in accessing the user table for " + user_info["referred_user_id"]}, 400
                if output["user_id"] is not None:
                    user_info["linked_status"] = 1
                    user_info["linked_user_id"] = output.get("user_id")
                    user_info["first_name"] = output["first_name"]
                    user_info["last_name"] = output["last_name"]
                    user_info["email_address"] = output["email_address"]
                    user_info["location"] = output["location"]
                    user_info["country_code"] = output["country_code"]
                    user_info["phone_number"] = output["phone_number"]
                    user_info["gender"] = output["gender"]
                    user_info["friend_list_flag"] = "N"
                    user_info["approval_status"] = 0
                else: # Check if the user is in friend list
                    if not objFriend.get_friend_by_id(user_info["referred_user_id"], user_info["referrer_user_id"],output):
                        current_app.logger.error("Error in checking the user table for id",
                                                 user_info["referred_user_id"])
                        return {"Error": "Failure in accessing the user table for " + user_info[
                            "referred_user_id"]}, 400
                    if output["referred_user_id"] is not None:
                        user_info["first_name"] = output["first_name"]
                        user_info["last_name"] = output["last_name"]
                        user_info["email_address"] = output["email_address"]
                        user_info["location"] = output["location"]
                        user_info["country_code"] = output["country_code"]
                        user_info["phone_number"] = output["phone_number"]
                        user_info["linked_status"] = output["linked_status"]
                        user_info["linked_user_id"] = output["linked_user_id"]
                        user_info["gender"] = output["gender"]
                        user_info["approval_status"] = 0
                        user_info["friend_list_flag"] = "Y"
                    else:
                        output.clear()
                        if not objFriend.get_unique_friend_by_id(user_info["referred_user_id"], output):
                            return {"Error" : "System issue: Unable to find the contributor in the system."}, 400
                        if output["referred_user_id"] is None:
                            return {"Error": "The referred user is not in the system. "}, 400
                        user_info["first_name"] = output["first_name"]
                        user_info["last_name"] = output["last_name"]
                        user_info["email_address"] = output["email_address"]
                        user_info["location"] = output["location"]
                        user_info["country_code"] = output["country_code"]
                        user_info["phone_number"] = output["phone_number"]
                        user_info["linked_status"] = output["linked_status"]
                        user_info["linked_user_id"] = output["linked_user_id"]
                        user_info["gender"] = output["gender"]
                        user_info["approval_status"] = 0
                        user_info["friend_list_flag"] = "N" #Keep the flag as N as we want a record for every user and friend id combination

                hshOutput = {}
                if not objGDBUser.get_user_roles(user_info["referred_user_id"], user_info["referrer_user_id"],user_info["friend_circle_id"], hshOutput):
                    current_app.logger.error("Unablet to get the roles for " + user_info["referred_user_id"])
                    return {"Error" : "Unable to check the roles of the user"}, 400

                if len(hshOutput) <= 0:
                    current_app.logger.error("Unablet to get the roles for " + user_info["referred_user_id"])
                    return {"Error" : "Unable to check the roles of the user"}, 400
                if user_info["referred_user_id"] in hshOutput:
                    if hshOutput[user_info["referred_user_id"]]["contrib_flag"] == "Y":
                        return {"Error": "User is already a contributor to the friend circle "}, 400

                    if hshOutput[user_info["referred_user_id"]]["secret_friend_flag"] == "Y":
                        return {"Error": " recommended user is the secret friend "}, 400

                    if hshOutput[user_info["referred_user_id"]]["circle_creator_flag"] == "Y":
                        return {"Error": " creator cannot be the contributor "}, 400

                is_admin = 0
                if user_info["referrer_user_id"] not in hshOutput:
                    current_app.logger.error("The referrer is not in the friend circle. Something wrong")
                    return {"Error" : "The referrer is not part of the friend circle. Something is wrong"}, 400

                if hshOutput[user_info["referrer_user_id"]]["contrib_flag"] == "N" and hshOutput[user_info["referrer_user_id"]]["circle_creator_flag"] == "N":
                        return {"Error": "The referrer is neither a circle creator nor a contributor" + " user:" + str(user_info["referrer_user_id"])}, 400

                if hshOutput[user_info["referrer_user_id"]]["circle_creator_flag"] == "Y":
                    is_admin = 1

                if not objFriend.add_friend_to_the_list_and_circle(user_info, is_admin, output):
                    current_app.logger.error("Unable to add friend as the contributore" + user_info["referred_user_id"])
                    return {"Error": "Failure. Unable to add friend as contributor"}, 400

                if int(os.environ.get("GEMIFT_VERSION")) == 2:
                    if is_admin == 1:
                        user_info.update({"info_type": "E: User Acceptance Required"})
                    else:
                        user_info.update({"info_type": "E: Approval Required"})
                    """
                    kafka_producer = KafkaMessageProducer(current_app.config["KAFKA_BROKER"],
                                                          current_app.config["KAFKA_FRIEND_EMAIL_TOPIC"])
                    kafka_producer.send_msg(json.dumps(user_info))
                    """
                if is_admin == 1:
                    current_app.logger.error("Reffered friend is successfully added" + user_info["referred_user_id"])
                else:
                    current_app.logger.error("Reffered friend is successfully added to the queue and will require circle creator help" + user_info["referred_user_id"])

                return {"status": json.dumps(output)}, 200

            if request_id == 2:
                # This is an invitation recommended for a non existing friend circle member by an existing member
                # Check if there is a friend circle exists with the recommended email address as a member or a secret
                # friend
                output.clear()

                hshOutput = {}
                friend_record_exists = 0
                referred_user_id = None
                if "country_code" not in user_info or \
                    "phone_number" not in user_info:
                    return {"Error": "Phone number and/or country code is missing"}, 400
                if not objGDBUser.get_user_role_as_contrib_secret_friend(user_info["country_code"], user_info["phone_number"], user_info["referrer_user_id"], user_info["friend_circle_id"], hshOutput):
                    current_app.logger.error("Unable to get the roles for " + user_info["email_address"])
                    return {"Error" : "Unable to check the roles of the user"}, 400
                if user_info["phone_number"] in hshOutput:
                    if hshOutput[str(user_info["country_code"])+str(user_info["phone_number"])]["contrib_flag"] == "Y":
                        return {"status": "User is already a contributor to the friend circle "}, 400
                    if hshOutput[str(user_info["country_code"])+str(user_info["phone_number"])]["secret_friend_flag"] == "Y":
                        return {"status": " recommended user is the secret friend "}, 400
                    if hshOutput[str(user_info["country_code"])+str(user_info["phone_number"])]["circle_creator_flag"] == "Y":
                        return {"status": " creator cannot be the contributor "}, 400
                    if hshOutput[str(user_info["country_code"])+str(user_info["phone_number"])]["user_id"] is not None and hshOutput[user_info["phone_number"]]["friend_id"] is not None:
                        friend_record_exists = 1
                        referred_user_id = hshOutput[user_info["phone_number"]]["user_id"]

                if not objGDBUser.get_user_roles_for_referrer( user_info["referrer_user_id"],user_info["friend_circle_id"], hshOutput):
                    current_app.logger.error("Unablet to get the roles for " + user_info["referrer_user_id"])
                    return {"Error" : "Unable to check the roles of the user"}, 400

                if user_info["referrer_user_id"] not in hshOutput:
                    current_app.logger.error("The referrer is not in the friend circle. Something wrong")
                    return {"Error" : "The referrer is not part of the friend circle. Something is wrong"}, 400

                if len(hshOutput) <= 0:
                    current_app.logger.error("The referrer is not in the system. Bailing out" + user_info["referrer_user_id"])
                    return False

                is_admin = 0

                if hshOutput[user_info["referrer_user_id"]]["contrib_flag"] == "N" and hshOutput[user_info["referrer_user_id"]]["circle_creator_flag"] == "N":
                        return {"Error": "The referrer is neither a circle creator nor a contributore"}, 400

                if hshOutput[user_info["referrer_user_id"]]["circle_creator_flag"] == "Y":
                    is_admin = 1

                if not objGDBUser.get_user_by_phone(user_info["phone_number"], user_info["country_code"], output):
                    current_app.logger.error("Error in checking the user table for id",
                                             user_info["referred_user_id"])
                    return {"Error": "Failure in accessing the user table for " + user_info[
                        "referred_user_id"]}, 400

                user_info["friend_list_flag"] = "N"

                if output.get("user_id") is not None:
                    if (is_admin == 1 or hshOutput[user_info["referrer_user_id"]]["contrib_flag"] == "Y") and output.get("user_id") == user_info["referrer_user_id"]:
                        return {"Error": "creator or contributor cannot be the contributor"}, 400
                    user_info["linked_status"] = 1
                    user_info["linked_user_id"] = output.get("user_id")
                    user_info["referred_user_id"] = output.get("user_id")
                    user_info["approval_status"] = 0
                else:
                    user_info["linked_status"] = 0
                    user_info["linked_user_id"] = None
                    user_info["approval_status"] = 0
                    #if not objFriend.get_friend_by_email(user_info["email_address"], user_info["referrer_user_id"], "DIRECT", output): # phone primary key support

                    if not objFriend.get_friend_by_phone_number(user_info["country_code"], user_info["phone_number"], user_info["referrer_user_id"], "DIRECT", output):
                        current_app.logger.error("Unable to check the presence of record for user " + user_info["email_address"])
                        return {"Error": "Unable to check the presence of user record in the db"}, 400
                    if "referred_user_id" in output and output["referred_user_id"] is not None:
                        user_info["referred_user_id"] = output["referred_user_id"]
                        user_info["linked_status"] = output["linked_status"]
                        user_info["linked_user_id"] = output["linked_user_id"]
                        user_info["friend_list_flag"] = "Y"
                        user_info["approval_status"] = 0
                    else:
                        user_info["linked_status"] = 0
                        user_info["linked_user_id"] = None
                        user_info["approval_status"] = 0
                if not objFriend.insert_friend_wrapper(user_info, is_admin, output):
                    current_app.logger.error("Unable to insert friend into the friend list " + user_info["email_address"])
                    print("Unable to insert friend into the friend list " + user_info["email_address"])
                    return {"Error": "Unable to insert friend into the friend list " + user_info["email_address"]}, 400

                if int(os.environ.get("GEMIFT_VERSION")) == 2:
                    if is_admin == 1:
                        user_info.update({"info_type": "NE: User Acceptance Required"})
                    else:
                        user_info.update({"info_type": "NE: Approval Required"})
                    """
                    kafka_producer = KafkaMessageProducer(current_app.config["KAFKA_BROKER"],
                                                          current_app.config["KAFKA_FRIEND_EMAIL_TOPIC"])
                    kafka_producer.send_msg(json.dumps(user_info))
                    """
                if is_admin:
                    return {"Status": "User added. They need to accept the invite and join"}, 200
                else:
                    return {"status" : "Successfully added. The user has to be approved by the admin"}, 200
            if request_id == 3:
                output = {}

                if objGDBUser.check_friend_circle_with_admin_and_secret_friend(user_info["referrer_user_id"],
                                                                               user_info["referred_user_id"],
                                                                               output) :
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"Error": "Secret Group with this combination exists"}, 400
                if not objFriend.create_secret_friend_by_id(user_info, output):
                    print( "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend")
                    current_app.logger.error( "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend")
                    return {"Error": "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend"}, 400

                user_info.update({"info_type": "SF: Secret Friend For Search"})
                if int(os.environ.get("GEMIFT_VERSION")) == 2:
                    print ("i am going silent for now")
                    """
                    kafka_producer = KafkaMessageProducer(current_app.config["KAFKA_BROKER"],
                                                          current_app.config["KAFKA_SECRET_FRIEND_QUEUE"])
                    kafka_producer.send_msg(json.dumps(user_info))
                    """
                return {"status" : json.loads(json.dumps(output))}, 200
            if request_id == 4:
                #if user_info["email_address"] is None or user_info["referrer_user_id"] is None: #phone primary key support
                if user_info["phone_number"] is None or\
                        user_info["country_code"] is None or \
                        user_info["referrer_user_id"] is None:
                    return {"Error" : "Failure. phone number and/or referrer user id cannot be null"}, 400


                if objGDBUser.check_friend_circle_with_admin_and_secret_friend_by_phone(user_info["referrer_user_id"],user_info["country_code"],user_info["phone_number"], output):
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"Error" : "secret circle for this email exists"}, 400
                output = {}
                user_info.update({"info_type": "SF: Secret Friend For Search"})

                if objFriend.create_secret_friend(user_info, output):
                    return {"status": json.loads(json.dumps(output))}, 200
                else:
                    return {"Error" : "Failure. Unable to create friend circle"}, 401

                if int(os.environ.get("GEMIFT_VERSION")) == 2:
                    """
                    kafka_producer = KafkaMessageProducer(current_app.config["KAFKA_BROKER"],
                                                          current_app.config["KAFKA_SECRET_FRIEND_QUEUE"])
                    kafka_producer.send_msg(json.dumps(user_info))
                    """
            if request_id == 5: # this is for whatsapp integration
                print ("The user list is ", user_list)
                objFriendCircleHelper = FriendCircleHelper()
                if not objFriendCircleHelper.create_circles_from_whatsapp(user_list,user_info["admin_friend_id"]):
                    return {"Error": "Failure in creating circles from the whatsapp contact"}, 400
                return {"status": "success"}, 200

            if request_id == 6:
                loutput = []
                if user_info["referrer_user_id"] is None or user_info["referred_user_id"] is None or user_info["list_friend_circle_id"] is None:
                    current_app.logger.error("One or more equired parameters for request id 6 is missing. The required parameters are referrer_id, referred_id and an array of friend cricle id")
                    return {"Error" : "Unable to continue. Required parameters are missing"}, 400
                if not objFriend.approve_requests(user_info["referrer_user_id"],
                                                  user_info["referred_user_id"],
                                                  user_info["list_friend_circle_id"],
                                                  user_info["approval_flag"],
                                                  loutput):
                    return {"status": "Failure"}, 400

                return {"status": "success"}, 200
            if request_id == 7: # contributor accepting to join the friend circle
                if user_info["friend_circle_id"] is None or \
                        user_info["phone_number"] is None or \
                        user_info["country_code"] is None :
                    current_app.logger.error("The required parameters for this requests are missing")
                    return {"Error" : "Failure: Unable to complete the operation"},400
                if not objFriend.contributor_approval(user_info["friend_circle_id"],
                                                      user_info["referred_user_id"],
                                                      user_info["referrer_user_id"],
                                                      user_info["country_code"],
                                                      user_info["phone_number"],
                                                      user_info["approval_status"]):
                    current_app.logger.error("Unable to process the approval request")
                    return {"Error" : "Failure: Unable to complete the operation"}, 400
                return {"status" : "Success"}, 200

            if request_id == 8: # Delete a member from the friend circle
                if not objGDBUser.delete_member_from_friend_circle(user_info["friend_circle_id"],
                                                               user_info["referrer_user_id"],
                                                               user_info["referred_user_id"],
                                                               list_output):
                    current_app.logger.error("Unable to delete the member from friend circle")
                    return {"Error": "Failure in deleting the member"}, 400
                return {"status":"success"}, 200

            if request_id == 9: # Delete the friend circle
                if not objGDBUser.delete_friend_circle(user_info["referrer_user_id"],
                                                                   user_info["friend_circle_id"],
                                                                    list_output):
                    current_app.logger.error("Unable to delete the  friend circle")
                    return {"Error": "Failure in deleting the friend circle"}, 400
                return {"status": "success"}, 200


            if request_id == 100: # purely for testing purposes
                output.clear()
                print ("Inside request 5")
                if objGDBUser.check_user_in_friend_circle(user_info["referred_user_id"], user_info["friend_circle_id"],
                                                                   output) and output[0] > 0:
                    return {"status": "The user already exists in the friend circle"}, 400
                else:
                    print ("False")
                    return {"status": "Error"}, 400
        except Exception as e:
            print ("The error is ", e)
            return {"Error": "Error in inserting the circle"}, 400

    #@jwt_required()
    def get(self):
        user_id = request.args.get("user_id", type=str)
        friend_circle_id = request.args.get("friend_circle_id", type=str)
        request_id = request.args.get("request_id", type=int)
        phone_number = request.args.get("phone_number", type=str)
        country_code = request.args.get("country_code", type=str)

        sstr = "("
        is_weird = 0
        list_friend_circle_raw = request.args.getlist("list_friend_circle")
        list_friend_circle_ids = None
        for occ in list_friend_circle_raw:
            if str(occ).find(sstr) >= 0:
                occ = occ.replace('(', " ")
                occ = occ.replace(')', " ")
                occ = occ.replace('"', " ")
                occ = occ.replace(" ", "")
                list_friend_circle_ids = list(occ.split(","))
                is_weird = 1
        if not is_weird:
            list_friend_circle_ids = list_friend_circle_raw
        if request_id is None:
            return {"Error": "Failure. No request id present in the request"}, 400
        output = []
        objGDBUser = GDBUser()

        # request == 1 :Get friend circle data by friend circle id
        # request == 2: Get all friend circle data for a given user
        # request == 3 : Get all your friends from the friend list
        # request = 5 : just get the friend circle information

        if request_id == 1:
            # Get specific friend circle data
            print("Enter in the request id 1 loop")
            if objGDBUser.get_friend_circle(friend_circle_id, output):
                data = json.loads(json.dumps(output))
                return {'friend_circle_id': data}, 200
            else:
                return {"Error":"Failure in getting friend circle"}, 400
        if request_id == 2:
            hshOutput = {}
            list_output = []
            if not objGDBUser.get_user_summary(user_id,hshOutput,txn=None,list_output = list_output):
            #if not objGDBUser.get_friend_circles(user_id, output):
                print('There is an issue getting friend_circle_data for ', user_id)
                return {"Error": "unable to get friend circle information. retry"}, 400
            return {'data': json.loads(json.dumps(list_output))}, 200

        if request_id == 3:
            objFriend = FriendListDB()
            if not objFriend.get_friend_list(user_id, output):
                print('There is an issue getting friend_circle_data for ', user_id)
                return {"Error": "unable to get friend circle information. retry"}, 400
            data = json.loads(json.dumps(output))
            return {'friend_list: data'}, 200

        if request_id == 4: # to get all the friend circle invite where the user hasn't approved
            objFriend = FriendListDB()
            list_output = []
            if country_code is None or phone_number is None:
                return {"Error": "Phone number or country code is null"}, 400

            if objFriend.get_open_invites(country_code, phone_number, list_output):
                current_app.logger.error("Unable to get all the open invites")
                return {"Error" : "Failure to get all the open invites"}, 400
            return{"invite" : json.loads(json.dumps(list_output))}, 200

        if request_id == 5: # to get all the friend circle head data
            list_output = []
            if not objGDBUser.get_friend_circle_header(list_friend_circle_ids, list_output):
                current_app.logger.error("Unable to get friend circle header information")
                return {"Error" : "Failure to get friend circle information"}, 400
            return{"data" : json.loads(json.dumps(list_output))}, 200

    def delete(self):
        return {"Item successfully deleted": "thank you"}, 200

class SecretFriendAttributeManagement(Resource):
    def post(self):
        return True
    def get(self):
        return True


class InterestManagement(Resource):

    # get count of users by interest in friend circle
    # interest count by various attributes
    #@jwt_required()
    def post(self):
        try:
            content = request.get_json()
            if content is None:
                current_app.logger.error("No parameters send into the interest api (post). Check")
                return {"Error":"failure. No parameters sent"}, 500

            referred_user_id = content["referred_user_id"] if "referred_user_id" in content else None
            friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None

            if referred_user_id is None  or friend_circle_id is None:
                current_app.logger.error("Required parameters are not sent (referrer, referred, friend_circle_id)")
                print("Required parameters are not sent (user_id, content_user_id, friend_circle_id)")
                return {"Error":"Failure. Referrer id or friend circle id is missing"}, 400
            list_category_id = []
            list_subcategory_id = []
            list_category_id = content["list_category_id"] if "list_category_id" in content else None #this should be list of hashs with each member having category_id and vote
            list_subcategory_id = content["list_subcategory_id"] if "list_subcategory_id" in content else None  # this should be list of hashs with each member having subcategory_id and vote
            request_id = content["request_id"] if "request_id" in content else None

            objGDBUser = GDBUser()
            print ("THe list category id is", list_category_id)
            if request_id == 1: # link use to category and sub category
                print ("The request is", request.path, request.host_url, request.date, request.blueprint, request.endpoint, request.environ)
                hshOutput = {}
                if objGDBUser.check_user_in_friend_circle( referred_user_id, friend_circle_id, hshOutput) :
                    if len(hshOutput) > 0 and  hshOutput["relation_type"] != "SECRET_FRIEND":
                        """
                        if list_category_id is not None:
                            if len(list_category_id) > 0  and not objGDBUser.link_user_to_web_category(referred_user_id, friend_circle_id, list_category_id):
                                print ("Issue inserting the relationship")
                                return {"status":"Failure"}, 400
                        """
                        if list_subcategory_id is not None:
                            if len(list_subcategory_id)  > 0 and not objGDBUser.link_user_to_web_subcategory( referred_user_id,friend_circle_id, list_subcategory_id):
                                print ("Issue inserting the relationship")
                                return {"Error":"Failure in getting subcategory data"}, 400
                else:
                    print ("The user is not part of the circle or a secret friend trying to hack the circle")
                    return {"Error": "Failure"}, 400
            return {"status": "success"}, 200

            if request_id == 2: # Map brands to friend_circle
                list_brand = []
                list_brand = content["list_brand"] if "list_brand" in content else None  # this should be list of hashs with each member having category_id and vote
                if len(list_brand) <= 0:
                    return {"Error": "Brand is required"}, 400
                if objGDBUser.assign_brand_to_friend_circle(user_id, friend_circle_id, l_brand):
                    current_app.logger.error("Unable to map brands to friend circle")
                    return {"status": "Failure"}, 400


        except Exception as e:
            print ("The error is " , e)
            return {"status":"Failure"}, 400


    #jwt_required()
    def get(self):
        user_id = request.args.get("user_id", type=str)
        friend_circle_id = request.args.get("friend_circle_id", type=str)
        age =  request.args.get("age", type=int)
        gender = request.args.get("gender", type=str)
        request_id = request.args.get("request_id", type=int)
        page_size = request.args.get("page_size", type = int)
        page_number = request.args.get("page_number", type=int)
        raw_subcategory_list = request.args.getlist("subcategory_list")

        gender_list = ["M","F","A"]
        sstr = "("
        is_weird = 0
        for occ in raw_subcategory_list:
            if str(occ).find(sstr) >= 0:
                occ = occ.replace('(', " ")
                occ = occ.replace(')', " ")
                occ = occ.replace('"', " ")
                occ = occ.replace(" ", "")
                subcategory_list = list(occ.split(","))
                is_weird = 1
        if not is_weird:
            subcategory_list = raw_subcategory_list

        objGDBUser = GDBUser()
        loutput = []

        objCategory = CategoryManagementDB()

        if request_id == 1:

            if not objCategory.get_web_category(loutput):
                current_app.logger.error("Unable to get the categories")
                return {"status" : "Failure"}, 400
            return {"category": json.dumps(loutput)}, 200

        if request_id == 2:
            hsh = {}
            objFriend = FriendListDB()
            objGDBUser = GDBUser()
            if age is None or gender is None:
                if not objGDBUser.get_friend_circle_attributes(friend_circle_id, hsh):
                    current_app.logger.error("Unable to get friend circle_attributes")
                    return {"Error": "Failure: Unable to get the age and gender from friend circle"}, 400
                age = hsh["age"]
                gender = hsh["gender"]
            else:
                if not objFriend.update_gender_age(friend_circle_id,gender, age):
                    current_app.logger.error("Unable to update the friend circle with age or gender")
                    return {"Error": "Failure: Unable to update the friend circle with the given data"}
            if age is None:
                if not objGDBUser.get_age_from_occasion(friend_circle_id, hsh):
                    current_app.logger.error("Error in getting teh age for the secret friend")
                    return {"Error": "Failure:Error in getting the age"}, 400
                age = hsh["age"]
                if ("lo" not in hsh   or "hi" not in hsh) or (hsh["lo"] is None or hsh["hi"] is None):
                    current_app.logger.error("Age hi or lo is missing or invalid")
                    return {"Error":"Failure: Unable to get age range"}, 400
            # if gender is None:
            #     current_app.logger.error("gender cannot be none and it is")
            #     return {"status" : "Failure: Unable to get the gender"}, 400

            if not SiteGeneralFunctions.get_age_range(int(age), hsh):
                current_app.logger.error("Unable to get age range")
                return {"Error": "Failure: Unable to get age range"}, 400

            if not objGDBUser.get_subcategory_smart_recommendation(friend_circle_id, hsh["hi"], hsh["lo"], gender,
                                                                   loutput):
                current_app.logger.error(
                    "Unable to get smarter recommendation for friend circle id" + friend_circle_id)
                return {"Error": "Failure in getting recommendation"}, 401
            return {"subcategory": json.loads(json.dumps(loutput))}, 200

        if request_id == 3:
            # get all interests by friend circle
            loutput1 = []
            if objGDBUser.get_subcategory_interest(friend_circle_id, loutput1):
                print("successfully retrieved the interest categories for friend circle id:", friend_circle_id)
            else:
                return {"Error": "failure in interest data"}, 400
            return {"subcategories":json.loads(json.dumps(loutput1))}, 200

        if request_id == 4: # get the recently added interest for a given friend_circle
            list_output = []
            if objGDBUser.get_recently_added_interest(friend_circle_id, list_output):
                return {"status" : "Failure in getting the recently added interest"}, 400
            return {"interest": json.dumps(json.loads(list_output))}, 200

        if request_id == 5: # get brands
            loutput = []
            if objGDBUser.get_brands(loutput):
                current_app.logger.error("Unable to get brands")
                return {"status": "Failure: Unable to get brands"}, 400
            return {"brand": json.dumps(json.loads(loutput))}, 200

        if request_id == 6: # get brands by subcategory
            loutput = []
            if objGDBUser.get_brands_by_subcategory(subcategory_list,loutput):
                current_app.logger.error("Unable to get brands")
                return {"status": "Failure: Unable to get brands"}, 400
            return {"brand": json.dumps(json.loads(loutput))}, 200


        if request_id == 7: # get brands by friend_circle
            loutput = []
            if objGDBUser.get_brands_by_friend_circle(friend_circle_id,loutput):
                current_app.logger.error("Unable to get brands")
                return {"status": "Failure: Unable to get brands"}, 400
            return {"brand": json.dumps(json.loads(loutput))}, 200

        if request_id == 8: # get the nodes for a give parent
            hsh = {}
            objFriend = FriendListDB()
            objGDBUser = GDBUser()
            if age is None or gender is None:
                if not objGDBUser.get_friend_circle_attributes(friend_circle_id, hsh):
                    current_app.logger.error("Unable to get friend circle_attributes")
                    return {"status": "Failure: Unable to get the age and gender from friend circle"}, 400
                age = hsh["age"]
                gender = hsh["gender"]
            else:
                if not objFriend.update_gender_age(friend_circle_id,gender, age):
                    current_app.logger.error("Unable to update the friend circle with age or gender")
                    return {"status": "Failure: Unable to update the friend circle with the given data"}
            if age is None:
                if not objGDBUser.get_age_from_occasion(friend_circle_id, hsh):
                    current_app.logger.error("Error in getting teh age for the secret friend")
                    return {"status": "Failure:Error in getting the age"}, 400
                age = hsh["age"]
                if ("lo" not in hsh   or "hi" not in hsh) or (hsh["lo"] is None or hsh["hi"] is None):
                    current_app.logger.error("Age hi or lo is missing or invalid")
                    return {"status":"Failure: Unable to get age range"}, 400
            # if gender is None:
            #     current_app.logger.error("gender cannot be none and it is")
            #     return {"status" : "Failure: Unable to get the gender"}, 400

            if not SiteGeneralFunctions.get_age_range(int(age), hsh):
                current_app.logger.error("Unable to get age range")
                return {"status": "Failure: Unable to get age range"}, 400
            if not objGDBUser.get_subcategory_beyond_top_node(subcategory_list, hsh["hi"], hsh["lo"], gender, loutput):
                current_app.logger.error(
                    "Unable to get smarter recommendation for friend circle id" + friend_circle_id)
                return {"status": "Failure in getting recommendation"}, 401
            return {"subcategory": json.loads(json.dumps(loutput))}, 200

        if request_id == 9: # get category or sub category nodes for a given parent for registered user
            hsh = {}
            if user_id is None or gender is None or age is None:
                return {"status": " One or many of the expected parameters missing ( user_id, gender, age)"}, 400
            if not SiteGeneralFunctions.get_age_range(int(age), hsh):
                current_app.logger.error("Unable to get age range")
                return {"status": "Failure: Unable to get age range"}, 400
            if int(age) == 0:
                hsh["hi"] = 99
                hsh["lo"] = 0
            if not objGDBUser.get_subcategory_beyond_top_node(subcategory_list, hsh["hi"], hsh["lo"], gender, loutput):
                current_app.logger.error(
                    "Unable to get smarter recommendation for friend circle id" + friend_circle_id)
                return {"status": "Failure in getting recommendation"}, 401
            return {"subcategory": json.loads(json.dumps(loutput))}, 200
        if request_id == 10: # v2 interest API
            hsh = {}
            objFriend = FriendListDB()
            objGDBUser = GDBUser()
            if (age is None or gender is None) and (friend_circle_id is not None and len(friend_circle_id.strip()) == 35):
                if not objGDBUser.get_friend_circle_attributes(friend_circle_id, hsh):
                    current_app.logger.error("Unable to get friend circle_attributes")
                    return {"Error": "Failure: Unable to get the age and gender from friend circle"}, 400
                age = hsh["age"]
                gender = hsh["gender"]
            elif gender is not None and age is not None and (friend_circle_id is not None and len(friend_circle_id.strip()) == 35):
                if not objFriend.update_gender_age(friend_circle_id, gender, age):
                    current_app.logger.error("Unable to update the friend circle with age or gender")
                    return {"Error": "Failure: Unable to update the friend circle with the given data"}


            if age is None:
                if (friend_circle_id is not None and len(friend_circle_id.strip()) == 35):
                    if not objGDBUser.get_age_from_occasion(friend_circle_id, hsh):
                        current_app.logger.error("Error in getting teh age for the secret friend")
                        return {"Error": "Failure:Error in getting the age"}, 400
                    if "age" not in hsh:
                        age = 0
                    age = hsh["age"]

            if gender.strip() is not None and len(gender.strip()) > 0 and gender.strip() not in gender_list:
                current_app.logger.error("Unknown gender value passed")
                return {"Error": "Unknown gender value passed"}, 400
            if age is None:
                age = 0
            if not objGDBUser.get_interest_v2( age, gender, page_size, page_number,loutput):
                current_app.logger.error(
                    "Unable to get smarter recommendation for friend circle id" )
                return {"Error": "Failure in getting recommendation V2"}, 401
            return {"subcategory": json.loads(json.dumps(loutput))}, 200

# Here is how the occasion management has been implemented.

# Any user from the friend circle can set the occasion for the secret friend
# Any occasion set by a member of the friend circle will be approved by the admin of the friend circle.
# The admin will receive a notification for all the occasions that are in status of 0, which means the occasion value is waiting for admin
# Any user from the friend circle can vote against the accuracy of the occasion date and provide a new value.
# When this happens the new value will be entered with a status of 0. The admin is expeccted to make the call based on the feedback received

class OccasionManagement(Resource):

    #@jwt_required()
    def post(self):

        content = request.get_json()
        if content is None:
            current_app.logger.error("No parameters send into the occasion api (post). Check")
            return {"status": "failure"}, 500
        creator_user_id = content["creator_user_id"] if "creator_user_id" in content else None
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        occasion_id = content["occasion_id"] if "occasion_id" in content else None
        status = content["status"] if "status" in content else None
        user_id = content["user_id"] if "user_id" in content else None
        occasion_date = content["occasion_date"] if "occasion_date" in content else None
        occasion_timezone = content["occasion_timezone"] if "occasion_timezone" in content else None
        flag = content["flag"] if "flag" in content else None
        value = content["value"] if "value" in content else None
        value_timezone = content["value_timezone"] if "value_timezone" in content else None
        request_id = content["request_id"] if "request_id" in content else None
        occasion_name = content["occasion_name"] if "occasion_name" in content else None
        frequency= content["frequency"] if "frequency" in content else None
        creator_user_id = content["creator_user_id"] if "creator_user_id" in content else None
        objGDBUser = GDBUser()

        # add
        output_hash={}
        status = 0
        if request_id == 1:  # add occasion
            print ("Inside occasion request 1")
            if len(occasion_id) > 14:
                return {"status": "Occasion id is not a standard occasion"}, 400

            if objGDBUser.add_occasion(creator_user_id, friend_circle_id, occasion_id, occasion_date,occasion_timezone, status, output_hash):
                return {"status": "Occasion successfully added"}, 200
            else:
                print ("Failure in adding occasion or occasion already exists")
                return {"Error:": "Failed to add occasion"}, 400

        if request_id == 2:  # vote for occasion
            if objGDBUser.vote_occasion( creator_user_id, friend_circle_id, occasion_id, flag, value, value_timezone, output_hash):
                return {"status" : "Success"}, 200
            else:
                return {"Error:": "Error in voting for the occasion"}, 400

        if request_id == 3:
            status = 1
            if objGDBUser.approve_occasion(creator_user_id, friend_circle_id, occasion_id, status, output_hash):
                return {"status" : "Success"}, 200
            else:
                return {"Error:": "Error in approving the occasion"}, 400

        if request_id == 4: # create custom occasion. This would require friend circle id, occasion_name, occasion_start_date, frequency,
            hsh = {}
            if occasion_name is None or friend_circle_id is None or frequency is None or creator_user_id is None or occasion_date is None or value_timezone is None:
                return {"Error" :"Failure: Insufficient parameters"}
            if not objGDBUser.create_custom_occasion(occasion_name, friend_circle_id, frequency, creator_user_id, occasion_date, occasion_timezone, hsh):
                return {"Error" : "Failure: Unable to create custom occasion"}, 400
            return {"occasion_id": json.loads(json.dumps(hsh)) }, 200

        if request_id == 5: # deactivate custom occasion. This would require custom_occasion_id, admin_user_id
            if occasion_id is None or friend_circle_id is None:
                return {"Error" : "Failure: Missing parameters"}
            if not objGDBUser.deactivate_occasion(occasion_id, friend_circle_id):
                return {"Error" : "Failure: occasion not deactivated"}, 400
            return {"status" : "Successfully deactivated"}, 200

        if request_id == 6:  # reactivate custom occasion. This would require custom_occasion_id, admin_user_id
            if occasion_id is None or friend_circle_id is None:
                return {"Error": "Failure: Missing parameters"}
            if not objGDBUser.reactivate_occasion(occasion_id, friend_circle_id):
                return {"Error": "Failure: occasion not re-activated"}, 400
            return {"status": "Successfully deactivated"}, 200

        if request_id == 7: #Edit occasion
            if user_id is None or \
                    occasion_id is None or \
                    friend_circle_id is None or \
                    occasion_date is None or \
                    occasion_timezone is None:
                return {"Error" : "Required parameters to edit occasion is missing"}, 400
            if not objGDBUser.edit_occasion(user_id,friend_circle_id,occasion_id,occasion_date, occasion_timezone):
                return {"Error": "Edit the occasion is unsuccessful"}, 400
            return {"status": "Successfully updated"}, 200

    @jwt_required()
    def update(self):
        content = request.get_json()
        if content is None:
            current_app.logger.error("No parameters send into the occasion api (put). Check")
            return {"status": "failure"}, 500
        user_id = content["user_id"] if "user_id" in content else None
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        occasion_id = content["occasion_id"] if "occasion_id" in content else None
        status = content["status"] if "status" in content else None
        occasion_date = content["occasion_date"] if "occasion_date" in content else None
        request_id = content["request_id"] if "request_id" in content else None
        objGDBUser = GDBUser()
        if request_id == 1:
            if objGDBUser.update_occasion(user_id, friend_circle_id, occasion_id, occasion_date, status):
                return {"status": "Successfully updated"}, 200
            else:
                return {"Error": "Error in updating the occasion"}, 500

   #@jwt_required()
    def get(self):
        friend_circle_id = request.args.get("friend_circle_id", type=str)
        request_id = request.args.get("request_id", type=int)
        user_id = request.args.get("user_id", type=str)
        objGDBUser = GDBUser()
        loutput1 = []
        loutput2 = []
        data = {}
        if request_id == 1:  # request_id = 1 means get the occasion by friend circle id
            # if not objGDBUser.get_occasion_votes(friend_circle_id,loutput2):
            #     return{"status":"Failure"}, 400
            l_friend_circle = []
            l_friend_circle.append(friend_circle_id)
            if not objGDBUser.get_occasion(l_friend_circle, user_id, loutput1):
                return {"Error": "Failure in getting occasions"}, 400
            #data["occasion_votes"] = loutput2
            return {"occasions": json.loads(json.dumps(loutput1))}, 200

        if request_id == 2: # Get all occasions by user
            loutput = []
            if not objGDBUser.get_occasion_by_user(user_id, loutput):
                print ("Unable to get occasions for user" + str(user_id))
                return {"Error": "Failure. Unable to get occasions by user"}, 400
            return {"occasions": json.loads(json.dumps(loutput))}

        if request_id == 3:
            list_output = []
            if not objGDBUser.get_occasion_names(list_output, friend_circle_id):
                current_app.logger.error("Unable to get occasion names")
                return {"Error" : "Failure is extracting all the occasion names"}, 400
            return {"occasion_name" : json.loads(json.dumps(list_output))}, 200

class FriendAttributes(Resource):
    def post(self):
        content = request.get_json()
        objFriend = FriendListDB()
        request_id = content["request_id"]
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        user_id = content["user_id"] if "user_id" in content else None
        age = content["age"] if "age" in content else None
        gender = content["gender"] if "gender" in content else None
        relation_type = content["relation_type"] if "relation_type" in content else None
        image_type = content["image_type"] if "image_type" in content else None
        image_url = content["image_url"] if "image_url" in content else None
        entity_id = content["entity_id"] if "entity_id" in content else None

        if request is None:
            return {"Error" : " Request id is missing"}, 400
        if request_id == 1: # Adding Age
            if not objFriend.add_secret_friend_age(user_id, friend_circle_id, age, gender):
                return {"Error" : "Failure in adding age"}, 400
        if request_id == 2: # Adding Relationship
            if not objFriend.add_relationship(user_id, friend_circle_id, relation_type):
                return {"Error": "Failure in adding relationship"}, 400
        if request_id == 3:
            if not objFriend.upload_image(image_type, entity_id, image_url):
                return {"Error": "Failure: Unable to load image for " + entity_id}, 400

    def get(self):
        request_id = request.args.get("request_id", type=int)
        user_id = request.args.get("user_id", type=str)
        friend_circle_id = request.args.get("friend_circle_id", type=str)
        objFriend = FriendListDB()
        list_output = []
        if request_id == 1: # get age
            if not objFriend.get_secret_friend_age_gender(friend_circle_id, list_output):
                return {"Error" : " Error in getting age for the friend circle"},400
            return {"status" : json.loads(json.dumps(list_output))}, 200
        return {"status" : "success"}, 200