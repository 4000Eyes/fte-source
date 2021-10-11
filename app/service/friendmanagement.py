from flask import Response, request, current_app
from model.gdbmethods import GDBUser
from model.friendlistdb import FriendListDB
from flask_restful import Resource
from model.models import EmailUserQueue, FriendCircleApprovalQueue
from .classhelper import FriendCircleHelper
from flask_jwt_extended import jwt_required
import datetime
import json


class ManageFriendCircle(Resource):
    FAKE_USER_TYPE = 1
    FAKE_USER_PASSWORD = "TeX54Esa"

    @jwt_required()
    def post(self):
        print ("Inside the authorization function")
        try:
            objGDBUser = GDBUser()
            objFriend = FriendListDB()
            content = request.get_json()
            if content is None:
                current_app.logger.error("No parameters send into the friend circle api (post). Check")
                return {"status":"failure"}, 500
            request_id = content["request_id"] if "request_id" in content else None
            user_list = content["user_list"] if "user_list" in content else None

            user_info = {}
            user_info["creator_user_id"] = content["creator_user_id"] if "creator_user_id" in content else None
            user_info["referred_user_id"] = content["referred_user_id"] if "referred_user_id" in content else None
            user_info["referrer_user_id"] =  content["referrer_user_id"] if "referrer_user_id" in content else None
            user_info["friend_circle_id"] = content["friend_circle_id"] if "friend_circle_id" in content else None
            user_info["first_name"] = content["first_name"] if "first_name" in content else None
            user_info["last_name"] = content["last_name"] if "last_name" in content else None
            user_info["email_address"]  = content["email_address"] if "email_address" in content else None
            user_info["gender"]= content["gender"] if "gender" in content else None
            user_info["location"] = content["location"] if "location" in content else None
            user_info["gender"] = content["email_address"] if "email_address" in content else None
            user_info["friend_circle_name"] = content["friend_circle_name"] if "friend_circle_name" in content else None

            # clean up the inputs before calling the functions
            output = []
            # various post methods
            # request_id : 1 --> referring an existing member to an existing friend circle - This will require referrer_user_id, friend_circle_id, friend_user_id
            # request_id : 2 --> referring a non-existing user friend to an existing friend circle - This will require referrer_user_id, friend_circle_id, email_address, name.
            # requests_id : 3 --> creating a friend circle for an existing member as the secret friend - This will require creator_user_id, friend_id, circle name
            # request_id : 4 --> creating a friend circle for a non-existing member as the secret friend - This will require creator_user_id, email_address, name, circle_name
            # request_id : 5 --> a list of friends or contacts from whatsapp to create friend circles.

            # Note: Admin should be able to add friends without approval.

            output.clear()
            if request_id == 1:
                if objGDBUser.check_user_in_friend_circle(user_info["creator_user_id"], user_info["referred_user_id"], user_info["friend_circle_id"], output) and output[0] > 0:
                    return {"status": "friend is already part of the friend circle"}, 400
                if objGDBUser.check_user_is_secret_friend(user_info["creator_user_id"], user_info["referred_user_id"], user_info["friend_circle_id"], output) and output[0] > 0:
                    return {"status": "secret friend cannot be added as friend"}, 400
                if objGDBUser.check_user_is_admin(user_info["referred_user_id"], user_info["friend_circle_id"], output) and output[0] > 0:
                    is_admin = 1
                if is_admin:
                    if objFriend.insert_friend(user_info, output) and objGDBUser.add_contributor_friend_circle(user_info["creator_user_id"], user_info["referred_user_id"],
                                                                                                               user_info["friend_circle_id"], output) :
                        return {"status": "successfully added friend to the circle"}
                if not is_admin:
                    # insert into the approval table.
                    FriendCircleApprovalQueue.friend_circle_id = user_info["friend_circle_id"]
                    FriendCircleApprovalQueue.referred_user_id = user_info["referred_user_id"]
                    FriendCircleApprovalQueue.referring_user_id = user_info["referrer_user_id"]
                    FriendCircleApprovalQueue.friend_circle_admin_id = user_info["creator_user_id"]
                    FriendCircleApprovalQueue.first_name = user_info["first_name"]
                    FriendCircleApprovalQueue.last_name = user_info["last_name"]
                    FriendCircleApprovalQueue.gender = user_info["gender"]
                    FriendCircleApprovalQueue.location = user_info["location"]
                    FriendCircleApprovalQueue.status = 0
                    FriendCircleApprovalQueue.save()
                return {"status": "success"}, 200

            if request_id == 2:
                # This is an invitation recommended for a non existing friend circle member by an existing member
                # Check if there is a friend circle exists with the recommended email address as a member or a secret
                # friend
                output.clear()
                if objGDBUser.check_user_in_friend_circle_by_email( user_info["creator_user_id"],user_info["email_address"],
                                                                    user_info["friend_circle_id"],
                                                                   output) and output[0] > 0:
                    return {"status": "The user already exists in the friend circle"}, 400
                if objGDBUser.check_user_is_secret_friend_by_email(user_info["creator_user_id"],
                                                                   user_info["email_address"],
                                                                   output) and output[0] > 0:
                    return {"status": "The user exists and the person is also the secret friend of the circle"}, 400
                if objGDBUser.check_user_is_admin_by_email(user_info["creator_user_id"],
                                                           user_info["email_address"],
                                                           user_info["friend_circle_id"],output) and \
                        output[0] > 0:
                    is_admin = 1
                if not objFriend.insert_friend(user_info, output):
                    return {"status": "Error"}, 400
                # Send an email asking the user to register
                EmailUserQueue.friend_circle_id = user_info["friend_circle_id"]
                EmailUserQueue.email = user_info["email_address"]
                EmailUserQueue.friend_circle_admin_id = user_info["creator_user_id"]
                EmailUserQueue.referred_user_id = user_info["referred_user_id"]
                EmailUserQueue.status = 0
                EmailUserQueue.save()
                # Insert a row into friend circle approval queue and the registration status should be off until the
                # user registers
                FriendCircleApprovalQueue.friend_circle_id = user_info["friend_circle_id"]
                FriendCircleApprovalQueue.email_address = user_info["email_address"]
                FriendCircleApprovalQueue.referring_user_id = user_info["referrer_user_id"]
                FriendCircleApprovalQueue.friend_circle_admin_id = user_info["creator_user_id"]
                FriendCircleApprovalQueue.first_name = user_info["first_name"]
                FriendCircleApprovalQueue.last_name = user_info["last_name"]
                FriendCircleApprovalQueue.gender = user_info["gender"]
                FriendCircleApprovalQueue.location = user_info["location"]
                FriendCircleApprovalQueue.status = 0
                FriendCircleApprovalQueue.registration_complete_status = 0
                FriendCircleApprovalQueue.save()
                return {"status": "Success"}, 200
            if request_id == 3:
                output.clear()
                print ("Calling request 3 function")
                if objGDBUser.check_friend_circle_with_admin_and_secret_friend(user_info["refferer_user_id"],
                                                                               user_info["referred_user_id"],
                                                                               output) and output[0] > 0:
                    return {"status": "Secret Group with this combination exists"}, 400
                if not objFriend.insert_friend(user_info, output):
                    return {"status": "Error"}, 400
                if objGDBUser.insert_friend_circle(user_info["referrer_user_id"],
                                                   user_info["referred_user_id"],
                                                   user_info["friend_circle_name"], output):
                    return {'friend_circle_id': str(user_info["friend_circle_id"])}, 200
                return {"status" : "Error in creating the secret group"}, 200
            if request_id == 4:
                if objGDBUser.check_friend_circle_with_admin_and_secret_friend_by_email(user_info["creator_user_id"],
                                                                                        user_info["referred_email_address"],
                                                                                        output) and output[0] > 0:
                    return {"status" : "secret circle for this email exists"}, 400
                if not objFriend.insert_friend(user_info, output):
                    print ("Unable to insert or get the friend id for the given email address ", user_info["email_address"])
                    return {"status": "Failed to create friend circle"}, 400

                if objGDBUser.insert_friend_circle(user_info["creator_user_id"], output[0], user_info["friend_circle_name"], output):
                    return {'friend_circle_id': str(output[0])}, 200
                return {"status" : "Error in creating the secret group"}, 400

            if request_id == 5: # this is for whatsapp integration
                print ("The user list is ", user_list)
                user_info = list(eval(user_list))
                objFriendCircleHelper = FriendCircleHelper()
                if not objFriendCircleHelper.create_circles_from_whatsapp(user_info,user_info["creator_user_id"]):
                    return {"status": "Failure in creating circles from the whatsapp contact"}, 400
                return {"status": "success"}, 400
            if request_id == 100: # purely for testing purposes
                output.clear()
                print ("Inside request 5")
                if objGDBUser.check_user_in_friend_circle(user_info["referred_user_id"], user_info["friend_circle_id"],
                                                                   output) and output[0] > 0:
                    print ("True")
                    return {"status": "The user already exists in the friend circle"}, 400
                else:
                    print ("False")
                    return {"status": "Error"}, 400
        except Exception as e:
            print ("The error is ", e)
            return {"Error": "Error in inserting the circle"}, 400

    @jwt_required()
    def get(self):

        content = request.get_json(force=True)
        if content is None:
            current_app.logger.error("No parameters send into the friend circle api (get). Check")
            return {"status": "failure"}, 500
        user_id = content["user_id"] if "user_id" in content else None
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        request_id = content["request_id"] if "request_id" in content else None
        output = []
        objGDBUser = GDBUser()

        # request == 1 :Get friend circle data by friend circle id
        # request == 2: Get all friend circle data for a given user

        if request_id == 1:
            # Get specific friend circle data
            print("Enter in the request id 1 loop")
            if objGDBUser.get_friend_circle(friend_circle_id, output):
                xx = json.dumps(str(output))
                return {'friend_circle_id': xx}, 200
        if request_id == 2:
            if not objGDBUser.get_friend_circles(user_id, output):
                print('There is an issue getting friend_circle_data for ', user_id)
                return {"Erros": "unable to get friend circle information. retry"}, 500
            xx = json.dumps(str(output))
            return {'friend_circle_id': xx}, 200

    def delete(self):
        return {"Item successfully deleted": "thank you"}, 200



class InterestManagement(Resource):

    # get count of users by interest in friend circle
    # interest count by various attributes
    @jwt_required()
    def post(self):
        try:
            content = request.get_json()
            if content is None:
                current_app.logger.error("No parameters send into the interest api (post). Check")
                return {"status":"failure"}, 500
            user_id = content["user_id"] if "user_id" in content else None
            friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
            list_category_id = content["list_category_id"] if "list_category_id" in content else None #this should be list of hashs with each member having category_id and vote
            list_subcategory_id = content[
                "list_subcategory_id"] if "list_subcategory_id" in content else None  # this should be list of hashs with each member having subcategory_id and vote
            request_id = content["request_id"] if "request_id" in content else None

            objGDBUser = GDBUser()
            print ("THe list category id is", list_category_id)
            if request_id == 1: # link use to category and sub category
                print ("The request is", request.path, request.host_url, request.date, request.blueprint, request.endpoint, request.environ)
                loutput = []
                if objGDBUser.check_user_in_friend_circle( user_id,friend_circle_id, loutput) and loutput[0] > 0 and loutput[1] != "SECRET_FRIEND":
                    print ("Checked the user successfully")
                    if len(list_category_id) > 0  and not objGDBUser.link_user_to_web_category(friend_circle_id, user_id, list_category_id):
                        print ("Issue inserting the relationship")
                        return {"status":"Failure"}, 400
                    if len(list_subcategory_id) > 0 and not objGDBUser.link_user_to_web_subcategory(friend_circle_id, user_id, list_category_id):
                        print ("Issue inserting the relationship")
                        return {"status":"Failure"}, 400
                else:
                    print ("The user is not part of the circle or a secret friend trying to hack the circle")
                    return {"status": "Failure"}, 400
            return {"status": "success"}, 200
        except Exception as e:
            print ("The error is " , e)
            return {"status":"Failure"}, 400


    @jwt_required()
    def get(self):

        content = request.get_json()
        if content is None:
            current_app.logger.error("No parameters send into the interest api (get). Check")
            return {"status": "failure"}, 500
        user_id = content["user_id"] if "user_id" in content else None
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        interest_category_id = content["interest_category_id"] if "interest_category_id" in content else None
        request_id = content["request_id"] if "request_id" in content else None

        objGDBUser = GDBUser()
        loutput = []

        # get all interests by friend circle
        if objGDBUser.get_category_interest(friend_circle_id, loutput):
            print("successfully retrieved the interest categories for friend circle id:", friend_circle_id)
            data = json.dumps(str(loutput))
        else:
            return {"status": "failure"}, 400
        loutput1 = []
        if objGDBUser.get_subcategory_interest(friend_circle_id, loutput1):
            print("successfully retrieved the interest categories for friend circle id:", friend_circle_id)
            loutput.append(loutput1)
            data = json.dumps(str(loutput))
        else:
            return {"status": "failure"}, 400
        return {'categories': data}, 200


# Here is how the occasion management has been implemented.


# Any user from the friend circle can set the occasion for the secret friend
# Any occasion set by a member of the friend circle will be approved by the admin of the friend circle.
# The admin will receive a notification for all the occasions that are in status of 0, which means the occasion value is waiting for admin
# Any user from the friend circle can vote against the accuracy of the occasion date and provide a new value.
# When this happens the new value will be entered with a status of 0. The admin is expeccted to make the call based on the feedback received

class OccasionManagement:

    @jwt_required()
    def post(self):

        content = request.get_json()
        if content is None:
            current_app.logger.error("No parameters send into the occasion api (post). Check")
            return {"status": "failure"}, 500
        user_id = content["user_id"] if "user_id" in content else None
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        occasion_id = content["occasion_id"] if "occasion_id" in content else None
        status = content["status"] if "status" in content else None
        contributor_user_id = content["contributor_user_id"] if "contributor_user_id" in content else None
        occasion_type = content["occasion_type"] if "occasion_type" in content else None
        occasion_date = content["occasion_date"] if "occasion_date" in content else None
        request_id = content["request_id"] if "request_id" in content else None

        objGDBUser = GDBUser()

        # add
        if request_id == 1:  # add occasion
            if objGDBUser.add_occasion(contributor_user_id, friend_circle_id, occasion_id, occasion_date):
                return {"status": "Success"}, 200
            else:
                return {"Status:": "Failure"}, 500
        if request_id == 2:  # vote for occasion
            if objGDBUser.vote_occasion(occasion_id, user_id, friend_circle_id, status, occasion_date):
                return {"status" : "Success"}, 200
            else:
                return {"Failure:": "Error in voting for the occasion"}, 500
        if request_id == 3:
            if objGDBUser.approve_occasion(friend_circle_id, user_id, contributor_user_id, occasion_id, status):
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

    @jwt_required()
    def get(self):
        content = request.get_json()
        if content is None:
            current_app.logger.error("No parameters send into the occasion api (get). Check")
            return {"status": "failure"}, 500
        user_id = content["user_id"]
        friend_circle_id = content["friend_circle_id"]
        request_id = content["request_id"]

        objGDBUser = GDBUser()
        loutput1 = []
        loutput2 = []
        if request_id == 1:  # request_id = 1 means get the occasion by friend circle id
            if not friend_circle_id:
                return {"Error:", "Unexpected value for circle id"}, 500
            if objGDBUser.get_occasion(friend_circle_id, loutput1) and objGDBUser.get_occasion_votes(friend_circle_id,
                                                                                                     loutput2):
                loutput1.append(loutput2)
                data = json.dumps(str(loutput1))
                return {"occasions", data}, 200
            else:
                return {"status:": "Error in getting the occasions. try again"}, 500
