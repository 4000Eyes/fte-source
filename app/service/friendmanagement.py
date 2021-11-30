from flask import request, current_app, jsonify
from app.model.gdbmethods import GDBUser
from app.model.friendlistdb import FriendListDB
from app.model.categorydb import CategoryManagementDB
from flask_restful import Resource
from app.model.classhelper import FriendCircleHelper
from flask_jwt_extended import jwt_required
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
            user_info["first_name"] = content["first_name"] if "first_name" in content else None
            user_info["last_name"] =  content["last_name"] if "last_name" in content else None
            user_info["email_address"]  =  content["email_address"] if "email_address" in content else None
            user_info["gender"]= content["gender"] if "gender" in content else None
            user_info["location"] = content["location"] if "location" in content else None
            user_info["gender"] = content["email_address"] if "email_address" in content else None
            user_info["friend_circle_name"] = content["friend_circle_name"] if "friend_circle_name" in content else None
            user_info["list_friend_circle_id"] = content["list_friend_circle_id"] if "list_friend_circle_id" in content else None

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
                if output.get("user_id") is not None:
                    user_info["linked_status"] = 1
                    user_info["linked_user_id"] = output.get("user_id")
                else:
                    user_info["linked_status"] = 0
                    user_info["linked_user_id"] = None
                output = {}
                if objGDBUser.check_user_in_friend_circle( user_info["referred_user_id"], user_info["referrer_user_id"], user_info["friend_circle_id"], output):
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"status": "friend is already part of the friend circle"}, 400
                output = {}
                if objGDBUser.check_user_is_secret_friend(user_info["referrer_user_id"],user_info["referrer_user_id"], user_info["friend_circle_id"], output):
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"status": "secret friend cannot be added as friend"}, 400
                output = {}
                if objGDBUser.check_user_is_admin(user_info["referrer_user_id"], user_info["friend_circle_id"], output) :
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        is_admin = 1
                        if objFriend.add_friend_to_the_list_and_circle(user_info, output) :
                            return {"status": "successfully added friend to the circle"},200
                return {"status": "success"}, 200

            if request_id == 2:
                # This is an invitation recommended for a non existing friend circle member by an existing member
                # Check if there is a friend circle exists with the recommended email address as a member or a secret
                # friend
                output.clear()
                if objGDBUser.check_user_in_friend_circle_by_email( user_info["email_address"],user_info["referrer_user_id"],
                                                                    user_info["friend_circle_id"], output):
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"status": "The user already exists in the friend circle"}, 400
                if objGDBUser.check_user_is_secret_friend_by_email(user_info["email_address"], user_info["referrer_user_id"], user_info["friend_circle_id"],
                                                                   output) :
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"status": "The user exists and the person is also the secret friend of the circle"}, 400
                if objGDBUser.check_user_is_admin( user_info["referrer_user_id"],
                                                           user_info["friend_circle_id"],output):
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        is_admin = 1
                        output = {}
                        if not objGDBUser.get_user(user_info["email_address"], output):
                            current_app.logger.error("Error in checking the user table for id",
                                                     user_info["referred_user_id"])
                            return {"status": "Failure in accessing the user table for " + user_info[
                                "referred_user_id"]}, 400
                        if output.get("user_id") is not None:
                            user_info["linked_status"] = 1
                            user_info["linked_user_id"] = output.get("user_id")
                        else:
                            user_info["linked_status"] = 0
                            user_info["linked_user_id"] = None
                        if not objFriend.insert_friend_wrapper(user_info, output):
                            current_app.logger.error("Unable to insert friend into the friend list " + user_info["email_address"])
                            print("Unable to insert friend into the friend list " + user_info["email_address"])
                            return {"status": "Unable to insert friend into the friend list " + user_info["email_address"]}, 400
                return {"status" : "Success"}, 200
            if request_id == 3:
                output = {}
                print ("Calling request 3 function")
                if objGDBUser.check_friend_circle_with_admin_and_secret_friend(user_info["referrer_user_id"],
                                                                               user_info["referred_user_id"],
                                                                               output) :
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"status": "Secret Group with this combination exists"}, 400
                if not objFriend.create_secret_friend(user_info, output):
                    print( "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend")
                    current_app.logger.error( "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend")
                    return {"status": "Unable to create a friend circle with " + user_info["referred_user_id"] + " as secret friend"}, 400
                return {"status" : "Error in creating the secret group"}, 200
            if request_id == 4:
                if user_info["email_address"] is None or user_info["referrer_user_id"] is None:
                    return {"status" : "Failure. email address and/or referrer user id cannot be null"}
                if objGDBUser.check_friend_circle_with_admin_and_secret_friend_by_email(user_info["referrer_user_id"],
                                                                                        user_info["email_address"],
                                                                                        output) :
                    if output.get("user_exists") is not None and int(output.get("user_exists")) > 0:
                        return {"status" : "secret circle for this email exists"}, 400
                output = {}
                if objFriend.create_secret_friend(user_info, output):
                    return {"status": "successfully added friend to the circle"}, 200

            if request_id == 5: # this is for whatsapp integration
                print ("The user list is ", user_list)
                objFriendCircleHelper = FriendCircleHelper()
                if not objFriendCircleHelper.create_circles_from_whatsapp(user_list,user_info["admin_friend_id"]):
                    return {"status": "Failure in creating circles from the whatsapp contact"}, 400
                return {"status": "success"}, 200

            if request_id == 6:
                if not objFriend.approve_requests(user_info["referrer_user_id"], user_info["referred_user_id"], user_info["list_friend_circle_id"]):
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

            user_id = content["user_id"] if "user_id" in content else None
            creator_user_id = content["creator_user_id"] if "creator_user_id" in content else None
            friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None

            if user_id is None or creator_user_id is None or friend_circle_id is None:
                current_app.logger.error("Required parameters are not sent (user_id, content_user_id, friend_circle_id)")
                print("Required parameters are not sent (user_id, content_user_id, friend_circle_id)")
                return {"status":"Failure"}, 400
            list_category_id = content["list_category_id"] if "list_category_id" in content else None #this should be list of hashs with each member having category_id and vote
            list_subcategory_id = content[
                "list_subcategory_id"] if "list_subcategory_id" in content else None  # this should be list of hashs with each member having subcategory_id and vote
            request_id = content["request_id"] if "request_id" in content else None

            objGDBUser = GDBUser()
            print ("THe list category id is", list_category_id)
            if request_id == 1: # link use to category and sub category
                print ("The request is", request.path, request.host_url, request.date, request.blueprint, request.endpoint, request.environ)
                hshOutput = {}
                if objGDBUser.check_user_in_friend_circle( user_id,creator_user_id, friend_circle_id, hshOutput) :
                    if  len(hshOutput) > 0 and  hshOutput["relation_type"] != "SECRET_FRIEND":

                        if len(list_category_id) > 0  and not objGDBUser.link_user_to_web_category(user_id, creator_user_id, friend_circle_id, list_category_id):
                            print ("Issue inserting the relationship")
                            return {"status":"Failure"}, 400
                        if len(list_subcategory_id) > 0 and not objGDBUser.link_user_to_web_subcategory( user_id, creator_user_id,friend_circle_id, list_subcategory_id):
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
        #interest_category_id = content["interest_category_id"] if "interest_category_id" in content else None
        request_id = request.args.get("request_id", type=int)
        objGDBUser = GDBUser()
        loutput = []

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
            data = json.dumps(loutput)
        else:
            return {"status": "failure"}, 400
        return {'categories': data}, 200

        objCategory = CategoryManagementDB()

        if requestid == 1:
            if not objCategory.get_web_category(loutput):
                current_app.logger.error("Unable to get the categories")
                return {"status" : "Failure"}, 400
            return {"category" : json.dumps(loutput)}, 200

        if request_id == 2:
            if not objGDBUser.get_subcategory_smart_recommendation(friend_circle_id, age_hi, age_lo, gender,
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
