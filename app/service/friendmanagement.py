from flask import Response, request, current_app
from model.gdbmethods import GDBUser
from flask_restful import Resource
from model.models import EmailUserQueue, FriendCircleApprovalQueue
from .classhelper import ManageFriendCircleHelper
import datetime
import json


class ManageFriendCircle(Resource):
    FAKE_USER_TYPE = 1
    FAKE_USER_PASSWORD = "TeX54Esa"
    def post(self):
        friend_circle_id = [0]
        objGDBUser = GDBUser()
        content = request.get_json()
        creator_user_id = content["creator_user_id"] if "creator_user_id" in content else None
        referred_user_id = content["referred_user_id"] if "referred_user_id" in content else None
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        referred_email_address = content["email_address"] if "email_address" in content else None
        referred_name = content["name"] if "name" in content else None
        friend_user_id = content["friend_user_id"] if "friend_user_id" in content else None
        request_id = content["request_id"] if "request_id" in content else None
        friend_circle_name = content["friend_circle_name"] if "friend_circle_name" in content else None
        # clean up the inputs before calling the functions
        output = []
        is_admin = 0
        objHelper = ManageFriendCircleHelper()

        # various post methods
        # request_id : 1 --> referring an existing member to an existing friend circle - This will require referrer_user_id, friend_circle_id, friend_user_id
        # request_id : 2 --> referring a non-existing user friend to an existing friend circle - This will require referrer_user_id, friend_circle_id, email_address, name.
        # requests_id : 3 --> creating a friend circle for an existing member as the secret friend - This will require creator_user_id, friend_id, circle name
        # request_id : 4 --> creating a friend circle for a non-existing member as the secret friend - This will require creator_user_id, email_address, name, circle_name

        # Note: Admin should be able to add friends without approval.


        if request_id == 1:
            if objGDBUser.check_user_in_friend_circle(referred_user_id, friend_circle_id, output) and output[0] > 0:
                return {"status": "friend is already part of the friend circle"}, 400
            if objGDBUser.check_user_is_secret_friend(referred_user_id, friend_circle_id, output) and output[0] > 0:
                return {"status": "secret friend cannot be added as friend"}, 400
            if objGDBUser.check_user_is_admin(referred_user_id, friend_circle_id, output) and output[0] > 0:
                is_admin = 1
            if is_admin and objGDBUser.add_contributor_friend_circle(friend_user_id, friend_circle_id, output):
                return {"status": "successfully added friend to the circle"}
            if not is_admin:
                # insert into the approval table.
                FriendCircleApprovalQueue.friend_circle_id = friend_circle_id
                FriendCircleApprovalQueue.referred_user_id = referred_user_id
                FriendCircleApprovalQueue.referring_user_id = friend_user_id
                FriendCircleApprovalQueue.friend_circle_admin_id = creator_user_id
                FriendCircleApprovalQueue.status = 0
                FriendCircleApprovalQueue.save()
            return {"status": "success"}, 200

        if request_id == 2:
            # This is an invitation recommended for a non existing friend circle member by an existing member
            # Check if there is a friend circle exists with the recommended email address as a member or a secret
            # friend
            if objGDBUser.check_user_in_friend_circle_by_email(friend_circle_id, referred_email_address,
                                                               output) and output[0] > 0:
                return {"status": "The user already exists in the friend circle"}, 400
            if objGDBUser.check_user_is_secret_friend_by_email(friend_circle_id, referred_email_address,
                                                               output) and output[0] > 0:
                return {"status": "The user exists and the person is also the secret friend of the circle"}, 400
            if objGDBUser.check_user_is_admin_by_email(referred_email_address, friend_circle_id, output) and \
                    output[0] > 0:
                is_admin = 1
            # Send an email asking the user to register
            EmailUserQueue.friend_circle_id = friend_circle_id
            EmailUserQueue.email = referred_email_address
            EmailUserQueue.friend_circle_admin_id = creator_user_id
            EmailUserQueue.referred_user_id = friend_user_id
            EmailUserQueue.status = 0
            EmailUserQueue.save()
            # Insert a row into friend circle approval queue and the registration status should be off until the
            # user registers
            FriendCircleApprovalQueue.friend_circle_id = friend_circle_id
            FriendCircleApprovalQueue.email_address = referred_email_address
            FriendCircleApprovalQueue.referring_user_id = friend_user_id
            FriendCircleApprovalQueue.friend_circle_admin_id = creator_user_id
            FriendCircleApprovalQueue.status = 0
            FriendCircleApprovalQueue.registration_complete_status = 0
            FriendCircleApprovalQueue.save()
            return {"status": "Success"}, 200
        if request_id == 3:
            if objGDBUser.check_friend_circle_with_admin_and_secret_friend(creator_user_id, referred_user_id,
                                                                           output) and output[0] > 0:
                return {"status": "Secret Group with this combination exists"}, 400
            if objGDBUser.insert_friend_circle(creator_user_id, friend_user_id, friend_circle_name, output):
                return {'friend_circle_id': str(friend_circle_id)}, 200
            return {"status" : "Error in creating the secret group"}, 200
        if request_id == 4:
            if objGDBUser.check_friend_circle_with_admin_and_secret_friend_by_email(creator_user_id, referred_email_address, output) and output[0] > 0:
                return {"status" : "secret circle for this email exists"}, 400
            if objGDBUser.insert_user(referred_email_address, self.FAKE_USER_PASSWORD, self.FAKE_USER_TYPE, output) and output[0] is not None:
                if objGDBUser.insert_friend_circle(creator_user_id, output[0], friend_circle_name, output):
                    return {'friend_circle_id': str(friend_circle_id)}, 200
            return {"status" : "Error in creating the secret group"}, 400
        if request_id == 5:
            print ("Inside request 5")
            if objGDBUser.check_user_in_friend_circle(referred_user_id, friend_circle_id,
                                                               output) and output[0] > 0:
                print ("True")
                return {"status": "The user already exists in the friend circle"}, 400
            else:
                print ("False")
                return {"status": "Error"}, 400

    def get(self):
        content = request.get_json(force=True)
        creator_user_id = content["creator_use  r_id"] if "creator_user_id" in content else None
        friend_circle_id = content["friend_circle_id"] if "friend_circle_id" in content else None
        referrer_flag = content["referrer_friend_flag"] if "referrer_friend_flag" in content else None
        referred_email_address = content["email_address"] if "email_address" in content else None
        referred_name = content["name"] if "name" in content else None
        friend_user_id = content["friend_user_id"] if "friend_user_id" in content else None
        request_id = content["request_id"] if "request_id" in content else None
        output = []
        objGDBUser = GDBUser()
        if request_id == 1:
            print("Enter in the request id 1 loop")
            if objGDBUser.get_friend_circle(friend_circle_id, output):
                xx = json.dumps(str(output))
                return {'friend_circle_id': xx}, 200
        if request_id == 2:
            if not objGDBUser.get_all_contributors_to_friend_circle_by_circle_and_user_id(creator_user_id,
                                                                                          friend_circle_id, output):
                print("There is an issue getting friend_circle_data for ", creator_user_id, friend_circle_id)
                return {"Erros": "unable to get friend circle information. retry"}, 500

            if not objGDBUser.get_all_contributors_to_friend_circle(content["creator_user_id"], output):
                print('There is an issue getting friend_circle_data for ', creator_user_id)
                return {"Erros": "unable to get friend circle information. retry"}, 500
            xx = json.dumps(str(output))
            print("Calling get all contributors", xx)
            return {'friend_circle_id': xx}, 200


    def delete(self):
        return {"Item successfully deleted": "thank you"}, 200


class CreatorAdminManagement:
    def post(self):
        # Make a contributor to be an admin
        # Remove admin access
        return 0

    def get(self):
        # Show all the admins

        return 0


class InterestManagement:

    # get count of users by interest in friend circle
    # interest count by various attributes

    def post(self):

        content = request.get_json()
        user_id = content["user_id"]
        friend_circle_id = content["friend_circle_id"]
        contributor_user_id = content["contributor_user_id"]
        secret_friend_id = content["secret_friend_id"]
        interest_category_id = content["interest_category_id"]
        request_id = content["request_id"]

        objGDBUser = GDBUser()

        if objGDBUser.add_interest(secret_friend_id, friend_circle_id, contributor_user_id, interest_category_id):
            print("Successfully added the interest recommended by", contributor_user_id)
            return {"Success": "added recommended interest"}, 200
        return {"Error": "Unable to add interest. Try again"}, 500

    def get(self):

        content = request.get_json()
        user_id = content["user_id"]
        friend_circle_id = content["friend_circle_id"]
        contributor_user_id = content["contributor_user_id"]
        secret_friend_id = content["secret_friend_id"]
        interest_category_id = content["interest_category_id"]
        request_id = content["request_id"]

        objGDBUser = GDBUser()
        loutput = []

        # get all interests by friend circle
        if objGDBUser.get_interest_by_friend_circle(friend_circle_id, loutput):
            print("successfully retrieved the interest categories for friend circle id:", friend_circle_id)
            data = json.dumps(str(loutput))
            return {'categories': data}, 200
        return {'Error': 'Unable to get the interests for the friend circle'}, 500


class OccasionManagement:
    def post(self):

        content = request.get_json()
        user_id = content["user_id"]
        friend_circle_id = content["friend_circle_id"]
        occasion_id = content["occasion_id"]
        status = content["status"]
        referrer_user_id = content["referrer_user_id"]
        occasion_type = content["occasion_type"]
        occasion_date = content["occasion_date"]
        request_id = content["request_id"]

        objGDBUser = GDBUser()

        # add
        if request_id == 1:  # add occasion
            if objGDBUser.add_occasion(user_id, friend_circle_id, occasion_type, occasion_date):
                return {}, 200
            else:
                return {"Failure:": "Error in adding occasion"}, 500
        if request_id == 2:  # vote for occasion
            if objGDBUser.vote_occasion(occasion_id, user_id, status, occasion_date):
                return {}, 200
            else:
                return {"Failure:": "Error in voting for the occasion"}, 500

    def update(self):
        content = request.get_json()
        user_id = content["user_id"]
        friend_circle_id = content["friend_circle_id"]
        occasion_id = content["occasion_id"]
        status = content["status"]
        referrer_user_id = content["referrer_user_id"]
        occasion_type = content["occasion_type"]
        occasion_date = content["occasion_date"]
        request_id = content["request_id"]

        objGDBUser = GDBUser()
        if request_id == 1:
            if objGDBUser.update_occasion(user_id, friend_circle_id, occasion_type, status):
                return {}, 200
            else:
                return {"Failure": "Error in updating the occasion"}, 500

    def get(self):

        content = request.get_json()
        user_id = content["user_id"]
        friend_circle_id = content["friend_circle_id"]
        occasion_id = content["occasion_id"]
        status = content["status"]
        referrer_user_id = content["referrer_user_id"]
        occasion_type = content["occasion_type"]
        occasion_date = content["occasion_date"]
        request_id = content["request_id"]

        objGDBUser = GDBUser()
        loutput1 = []
        loutput2 = []
        if request_id == 1:  # request_id = 1 means get the occasion by friend circle id
            if not friend_circle_id:
                return {"Eror:", "Unexpected value for circle id"}, 500
            if objGDBUser.get_occasion(friend_circle_id, loutput1) and objGDBUser.get_occasion_votes(friend_circle_id,
                                                                                                     loutput2):
                loutput1.append(loutput2)
                data = json.dumps(str(loutput1))
                return {"occasions", data}, 200
            else:
                return {"status:": "Error in getting the occasions. try again"}, 500
