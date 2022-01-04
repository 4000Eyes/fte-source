from flask import request, current_app, jsonify
from app.model.gdbmethods import GDBUser
from app.model.friendlistdb import FriendListDB
from app.model.categorydb import CategoryManagementDB
from flask_restful import Resource
from app.model.classhelper import FriendCircleHelper
from flask_jwt_extended import jwt_required
from app.service.general import SiteGeneralFunctions
import json

#are

class ManageFriendCircle(Resource):
    FAKE_USER_TYPE = 1
    FAKE_USER_PASSWORD = "TeX54Esa"

   # @jwt_required()
    def post(self):
        try:
            objGDBUser = GDBUser()
            objFriend = FriendListDB()
            content = request.get_json()

            if content is None:
                current_app.logger.error("No parameters send into the friend circle api (post). Check")
                return {"status":"failure"}, 500
            if "request_id" not in content:
                current_app.logger.error("No request id in the request")
                return {"status": "Request id is missing"}, 500
            request_id = content["request_id"]

            user_list = content["user_list"] if "user_list" in content else None

            if request_id == 5 and content["user_list"] is None:
                current_app.logger.error("User list is a key parameter for request id 5 and it is missine")
                return {"status": "User list is a key parameter for request id 5 and it is missine"}, 500


            user_info = {}
            user_info["admin_friend_id"] =  content["admin_friend_id"] if "admin_friend_id" in content else None
            user_info["referred_user_id"] =  content["referred_user_id"] if "referred_user_id" in content else None
            user_info["referrer_user_id"] =    content["referrer_user_id"] if "referrer_user_id" in content else None
            user_info["friend_circle_id"] =  content["friend_circle_id"] if "friend_circle_id" in content else None
            user_info["phone_number"] =  content["phone_number"] if "phone_number" in content else None
            user_info["first_name"] = content["first_name"] if "first_name" in content else None
            user_info["last_name"] =  content["last_name"] if "last_name" in content else None
            user_info["email_address"]  =  content["email_address"] if "email_address" in content else None
            user_info["gender"]= content["gender"] if "gender" in content else None
            user_info["location"] = content["location"] if "location" in content else None
            user_info["friend_circle_name"] = content["friend_circle_name"] if "friend_circle_name" in content else None
            user_info["list_friend_circle_id"] = content["list_friend_circle_id"] if "list_friend_circle_id" in content else None
            user_info["group_name"] = content["group_name"] if "group_name" in content else None

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
                    return {"status": "Failure in accessing the user table for " + user_info["referred_user_id"]}, 400
                if output["user_id"] is not None:
                    user_info["linked_status"] = 1
                    user_info["linked_user_id"] = output.get("user_id")
                    user_info["first_name"] = output["first_name"]
                    user_info["last_name"] = output["last_name"]
                    user_info["email_address"] = output["email_address"]
                    user_info["location"] = output["location"]
                    user_info["phone_number"] = output["phone_number"]
                    user_info["friend_list_flag"] = "N"
                else: # Check if the user is in friend list
                    if not objFriend.get_friend_by_id(user_info["referred_user_id"], user_info["referrer_user_id"],output):
                        current_app.logger.error("Error in checking the user table for id",
                                                 user_info["referred_user_id"])
                        return {"status": "Failure in accessing the user table for " + user_info[
                            "referred_user_id"]}, 400

                    if output["referred_user_id"] is not None:
                        user_info["first_name"] = output["first_name"]
                        user_info["last_name"] = output["last_name"]
                        user_info["email_address"] = output["email_address"]
                        user_info["location"] = output["location"]
                        user_info["phone_number"] = output["phone_number"]
                        user_info["linked_status"] = output["linked_status"]
                        user_info["linked_user_id"] = output["linked_user_id"]
                        user_info["friend_list_flag"] = "Y"
                    else:
                        user_info["linked_status"] = 0
                        user_info["linked_user_id"] = None
                        user_info["friend_list_flag"] = "N"

                hshOutput = {}
                if not objGDBUser.get_user_roles(user_info["referred_user_id"], user_info["referrer_user_id"],user_info["friend_circle_id"], hshOutput):
                    current_app.logger.error("Unablet to get the roles for " + user_info["referred_user_id"])
                    return {"status" : "Unable to check the roles of the user"}, 400

                if len(hshOutput) <= 0:
                    current_app.logger.error("Unablet to get the roles for " + user_info["referred_user_id"])
                    return {"status" : "Unable to check the roles of the user"}, 400
                if user_info["referred_user_id"] in hshOutput:
                    if hshOutput[user_info["referred_user_id"]]["contrib_flag"] == "Y":
                        return {"status", "User is already a contributor to the friend circle "}, 400

                    if hshOutput[user_info["referred_user_id"]]["secret_friend_flag"] == "Y":
                        return {"status", " recommended user is the secret friend "}, 400

                    if hshOutput[user_info["referred_user_id"]]["circle_creator_flag"] == "Y":
                        return {"status", " creator cannot be the contributor "}, 400

                is_admin = 0
                if user_info["referrer_user_id"] not in hshOutput:
                    current_app.logger.error("The referrer is not in the friend circle. Something wrong")
                    return {"status" : "The referrer is not part of the friend circle. Something is wrong"}, 400

                if hshOutput[user_info["referrer_user_id"]]["contrib_flag"] == "N" and hshOutput[user_info["referrer_user_id"]]["circle_creator_flag"] == "N":
                        return {"status": "The referrer is neither a circle creator nor a contributore"}, 400

                if hshOutput[user_info["referrer_user_id"]]["circle_creator_flag"] == "Y":
                    is_admin = 1

                if not objFriend.add_friend_to_the_list_and_circle(user_info, is_admin, output):
                    current_app.logger.error("Unable to add friend as the contributore" + user_info["referred_user_id"])
                    return {"status": "Failure. Unable to add friend as contributor"}, 400

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

                if not objGDBUser.get_user_role_as_contrib_secret_friend(user_info["email_address"], user_info["referrer_user_id"], user_info["friend_circle_id"], hshOutput):
                    current_app.logger.error("Unablet to get the roles for " + user_info["email_address"])
                    return {"status" : "Unable to check the roles of the user"}, 400

                if user_info["email_address"] in hshOutput:

                    if hshOutput[user_info["email_address"]]["contrib_flag"] == "Y":
                        return {"status", "User is already a contributor to the friend circle "}, 400

                    if hshOutput[user_info["email_address"]]["secret_friend_flag"] == "Y":
                        return {"status", " recommended user is the secret friend "}, 400

                    if hshOutput[user_info["email_address"]]["circle_creator_flag"] == "Y":
                        return {"status", " creator cannot be the contributor "}, 400

                    if hshOutput[user_info["email_address"]]["user_id"] is not None and hshOutput[user_info["email_address"]]["friend_id"] is not None:
                        friend_record_exists = 1
                        referred_user_id = hshOutput[user_info["email_address"]]["user_id"]

                if not objGDBUser.get_user_roles_for_referrer( user_info["referrer_user_id"],user_info["friend_circle_id"], hshOutput):
                    current_app.logger.error("Unablet to get the roles for " + user_info["referrer_user_id"])
                    return {"status" : "Unable to check the roles of the user"}, 400

                if len(hshOutput) <= 0:
                    current_app.logger.error("The referrer is not in the system. Bailing out" + user_info["referrer_user_id"])
                    return False

                is_admin = 0

                if user_info["referrer_user_id"] not in hshOutput:
                    current_app.logger.error("The referrer is not in the friend circle. Something wrong")
                    return {"status" : "The referrer is not part of the friend circle. Something is wrong"}, 400

                if hshOutput[user_info["referrer_user_id"]]["contrib_flag"] == "N" and hshOutput[user_info["referrer_user_id"]]["circle_creator_flag"] == "N":
                        return {"status": "The referrer is neither a circle creator nor a contributore"}, 400

                if hshOutput[user_info["referrer_user_id"]]["circle_creator_flag"] == "Y":
                    is_admin = 1

                if not objGDBUser.get_user_by_phone(user_info["phone_number"], output):
                    current_app.logger.error("Error in checking the user table for id",
                                             user_info["referred_user_id"])
                    return {"status": "Failure in accessing the user table for " + user_info[
                        "referred_user_id"]}, 400

                user_info["friend_list_flag"] = "N"

                if output.get("user_id") is not None:
                    user_info["linked_status"] = 1
                    user_info["linked_user_id"] = output.get("user_id")
                else:
                    user_info["linked_status"] = 0
                    user_info["linked_user_id"] = None
                    #if not objFriend.get_friend_by_email(user_info["email_address"], user_info["referrer_user_id"], "DIRECT", output): # phone primary key support
                if not objFriend.get_friend_by_phone_number(user_info["phone_number"], user_info["referrer_user_id"], "DIRECT", output):
                    current_app.logger.error("Unable to check the presence of record for user " + user_info["email_address"])
                    return {"status": "Unable to check the presence of user record in the db"}, 400
                if "referred_user_id" in output and output["referred_user_id"] is not None:
                    user_info["linked_status"] = output["linked_status"]
                    user_info["linked_user_id"] = output["linked_user_id"]
                    user_info["friend_list_flag"] = "Y"
                else:
                    user_info["linked_status"] = 0
                    user_info["linked_user_id"] = None

                if not objFriend.insert_friend_wrapper(user_info, output):
                    current_app.logger.error("Unable to insert friend into the friend list " + user_info["email_address"])
                    print("Unable to insert friend into the friend list " + user_info["email_address"])
                    return {"status": "Unable to insert friend into the friend list " + user_info["email_address"]}, 400
                return {"status" : "Successfully added. The user has to be approved by the admin"}, 200
            if request_id == 3:
                output = {}
                print ("Calling request 3 function")

                if objGDBUser.check_friend_circle_with_admin_and_secret_friend(user_info["referrer_user_id"],
                                                                               user_info["referred_user_id"],
                                                                               output) :
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"status": "Secret Group with this combination exists"}, 400
                if not objFriend.create_secret_friend_by_id(user_info, output):
                    print( "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend")
                    current_app.logger.error( "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend")
                    return {"status": "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend"}, 400
                return {"status" : json.dumps(output)}, 200
            if request_id == 4:
                #if user_info["email_address"] is None or user_info["referrer_user_id"] is None: #phone primary key support
                if user_info["phone_number"] is None or user_info["referrer_user_id"] is None:
                    return {"status" : "Failure. phone number and/or referrer user id cannot be null"}
                # if objGDBUser.check_friend_circle_with_admin_and_secret_friend_by_email(user_info["referrer_user_id"],
                #                                                                         user_info["phone_number"],
                #                                                                         output) :
                #phone primary key support

                if objGDBUser.check_friend_circle_with_admin_and_secret_friend_by_phone(user_info["referrer_user_id"],user_info["phone_number"], output):
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"status" : "secret circle for this email exists"}, 400
                output = {}
                if objFriend.create_secret_friend(user_info, output):
                    return {"status": json.dumps(output)}, 200
                else:
                    return {"status" : "Failure. Unable to create friend circle"}, 401

            if request_id == 5: # this is for whatsapp integration
                print ("The user list is ", user_list)
                objFriendCircleHelper = FriendCircleHelper()
                if not objFriendCircleHelper.create_circles_from_whatsapp(user_list,user_info["admin_friend_id"]):
                    return {"status": "Failure in creating circles from the whatsapp contact"}, 400
                return {"status": "success"}, 200

            if request_id == 6:
                loutput = []
                if user_info["referrer_user_id"] is None or user_info["referred_user_id"] is None or user_info["list_friend_circle_id"] is None:
                    current_app.logger.error("One or more equired parameters for request id 6 is missing. The required parameters are referrer_id, referred_id and an array of friend cricle id")
                    return {"Failure" : "Unable to continue. Required parameters are missing"}, 400
                if not objFriend.approve_requests(user_info["referrer_user_id"], user_info["referred_user_id"], user_info["list_friend_circle_id"], loutput):
                    return {"status": "Failure"}, 400

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

        if request_id is None:
            return {"status": "Failure. No request id present in the request"}, 400
        output = []
        objGDBUser = GDBUser()

        # request == 1 :Get friend circle data by friend circle id
        # request == 2: Get all friend circle data for a given user
        # request == 3 : Get all your friends from the friend list

        if request_id == 1:
            # Get specific friend circle data
            print("Enter in the request id 1 loop")
            if objGDBUser.get_friend_circle(friend_circle_id, output):
                data = json.dumps(output)
                return {'friend_circle_id': data}, 200
            else:
                return {"status":"Failure"}, 400
        if request_id == 2:
            if not objGDBUser.get_friend_circles(user_id, output):
                print('There is an issue getting friend_circle_data for ', user_id)
                return {"Erros": "unable to get friend circle information. retry"}, 500
            data = json.dumps(output)
            return {'friend_circle_id': data}, 200

        if request_id == 3:
            objFriend = FriendListDB()
            if not objFriend.get_friend_list(user_id, output):
                print('There is an issue getting friend_circle_data for ', user_id)
                return {"Erros": "unable to get friend circle information. retry"}, 500
            data = json.dumps(output)
            return {'friend_list: data'}, 200

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
                return {"status":"failure"}, 500

            referred_user_id = content["referred_user_id"] if "referred_user_id" in content else None

            friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None

            if referred_user_id is None  or friend_circle_id is None:
                current_app.logger.error("Required parameters are not sent (referrer, referred, friend_circle_id)")
                print("Required parameters are not sent (user_id, content_user_id, friend_circle_id)")
                return {"status":"Failure"}, 400
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
                        if list_category_id is not None:
                            if len(list_category_id) > 0  and not objGDBUser.link_user_to_web_category(referred_user_id, friend_circle_id, list_category_id):
                                print ("Issue inserting the relationship")
                                return {"status":"Failure"}, 400
                        if list_subcategory_id is not None:
                            if len(list_subcategory_id)  > 0 and not objGDBUser.link_user_to_web_subcategory( referred_user_id,friend_circle_id, list_subcategory_id):
                                print ("Issue inserting the relationship")
                                return {"status":"Failure"}, 400
                else:
                    print ("The user is not part of the circle or a secret friend trying to hack the circle")
                    return {"status": "Failure"}, 400
            return {"status": "success"}, 200
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
        objGDBUser = GDBUser()
        loutput = []

        if request_id == 3:
            # get all interests by friend circle
            if objGDBUser.get_category_interest(friend_circle_id, loutput):
                print("successfully retrieved the interest categories for friend circle id:", friend_circle_id)
                data = json.dumps(loutput)
            else:
                return {"status": "failure"}, 400
            loutput1 = []
            if objGDBUser.get_subcategory_interest(friend_circle_id, loutput1):
                print("successfully retrieved the interest categories for friend circle id:", friend_circle_id)
                loutput.append(loutput1)
                if len(loutput1) > 0:
                    data = json.dumps(loutput)
            else:
                return {"status": "failure"}, 400
            return {'categories': data}, 200

        objCategory = CategoryManagementDB()

        if request_id == 1:

            if not objCategory.get_web_category(loutput):
                current_app.logger.error("Unable to get the categories")
                return {"status" : "Failure"}, 400
            return {"category": json.dumps(loutput)}, 200

        if request_id == 2:
            hsh = {}
            objFriend = FriendListDB()

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
            if gender is None:
                current_app.logger.error("gender cannot be none and it is")
                return {"status" : "Failure: Unable to get the gender"}, 400

            if not SiteGeneralFunctions.get_age_range(age, hsh):
                current_app.logger.error("Unable to get age range")
                return {"status": "Failure: Unable to get age range"}, 400

            if not objGDBUser.get_subcategory_smart_recommendation(friend_circle_id, hsh["hi"], hsh["lo"], gender,
                                                                   loutput):
                current_app.logger.error(
                    "Unable to get smarter recommendation for friend circle id" + friend_circle_id)
                return {"status": "Failure in getting recommendation"}, 401
            return {"subcategory": json.dumps(loutput)}, 200


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
        contributor_user_id = content["contributor_user_id"] if "contributor_user_id" in content else None
        occasion_date = content["occasion_date"] if "occasion_date" in content else None
        occasion_timezone = content["occasion_timezone"] if "occasion_timezone" in content else None
        flag = content["flag"] if "flag" in content else None
        value = content["value"] if "value" in content else None
        value_timezone = content["value_timezone"] if "value_timezone" in content else None
        request_id = content["request_id"] if "request_id" in content else None
        occasion_name = content["occasion_name"] if "occasion_name" in content else None
        frequency= content["frequency"] if "frequency" in content else None
        objGDBUser = GDBUser()

        # add
        output_hash={}
        status = 0
        if request_id == 1:  # add occasion
            print ("Inside occasion request 1")
            if objGDBUser.add_occasion(contributor_user_id, creator_user_id, friend_circle_id, occasion_id, occasion_date,occasion_timezone, status, output_hash):
                return {"status": "Success"}, 200
            else:
                print ("Failure in adding occasion")
                return {"Status:": "Failure"}, 500
        if request_id == 2:  # vote for occasion
            if objGDBUser.vote_occasion(contributor_user_id, creator_user_id, friend_circle_id, occasion_id, flag, value, value_timezone, output_hash):
                return {"status" : "Success"}, 200
            else:
                return {"Failure:": "Error in voting for the occasion"}, 500
        if request_id == 3:
            status = 1
            if objGDBUser.approve_occasion(contributor_user_id, creator_user_id, friend_circle_id, occasion_id, status, output_hash):
                return {"status" : "Success"}, 200
            else:
                return {"Failure:": "Error in voting for the occasion"}, 500

        if request_id == 4: # create custom occasion. This would require friend circle id, occasion_name, occasion_start_date, frequency,
            hsh = {}
            if not objGDBUser.create_custom_occasion(occasion_name, friend_circle_id, frequency, creator_user_id, occasion_date, value_timezone, hsh):
                return {"status" : "Failure: Unable to create custom occasion"}, 400
            return {"occasion_id": json.dumps(hsh) }, 200

        if request_id == 5: # deactivate custom occasion. This would require custom_occasion_id, admin_user_id
            if not objGDBUser.deactivate_occasion(occasion_id, friend_circle_id):
                return {"status" : "Failure: occasion not deactivated"}, 400
            return {"status" : "Successfully deactivated"}, 200

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
                return {}, 200
            else:
                return {"Failure": "Error in updating the occasion"}, 500

   #@jwt_required()
    def get(self):
        friend_circle_id = request.args.get("friend_circle_id", type=str)
        request_id = request.args.get("request_id", type=int)
        objGDBUser = GDBUser()
        loutput1 = []
        loutput2 = []
        if request_id == 1:  # request_id = 1 means get the occasion by friend circle id
            #if objGDBUser.get_occasion(friend_circle_id, loutput1) and objGDBUser.get_occasion_votes(friend_circle_id,loutput):
            if not objGDBUser.get_occasion_votes(friend_circle_id,loutput2):
                return{"status":"Failure"}, 400
            if objGDBUser.get_occasion(friend_circle_id, loutput1):
                loutput1.append(loutput2)
                data = json.dumps(loutput1)
                #final_json = data.replace("\\", "")
                print ("Final json is", data)
                return {"occasions", data}, 200
            else:
                return {"status:": "Error in getting the occasions. try again"}, 500
        data = []
        if request_id == 2: # get age.
            return {"status": json.dumps(data)}, 200

class FriendAttributes(Resource):
    def post(self):
        content = request.get_json()
        objFriend = FriendListDB()
        request_id = content["request_id"]
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        user_id = content["user_id"] if "user_id" in content else None
        age = content["age"] if "age" in content else None
        relation_type = content["relation_type"] if "relation_type" in content else None
        image_type = content["image_type"] if "image_type" in content else None
        image_url = content["image_url"] if "image_url" in content else None
        entity_id = content["entity_id"] if "entity_id" in content else None

        if request is None:
            return {"status" : " Request id is missing"}, 400
        if request_id == 1: # Adding Age
            if not objFriend.add_secret_friend_age(user_id, friend_circle_id, age):
                return {"status" : "Failure in adding age"}, 400
        if request_id == 2: # Adding Relationship
            if not objFriend.add_relationship(user_id, friend_circle_id, relation_type):
                return {"status": "Failure in adding relationship"}, 400
        if request_id == 3:
            if not objFriend.upload_image(image_type, entity_id, image_url):
                return {"status": "Failure: Unable to load image for " + entity_id}

    def get(self):
        request_id = request.args.get("request_id", type=int)
        user_id = request.args.get("user_id", type=str)
        friend_circle_id = request.args.get("friend_circle_id", type=str)
        objFriend = FriendListDB()
        hshOutput = {}
        if request_id == 1: # get age
            if objFriend.get_secret_friend_age(friend_circle_id, hshOutput):
                return {"status" : " Error in getting age for the friend circle"}
            return {"status" : json.dumps(hshOutput)}
        return {"status" : "success"}, 200