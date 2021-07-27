from flask import Response, request, current_app
from model.gdbmethods import GDBUser
from flask_restful import Resource
from .classhelper import ManageFriendCircleHelper
import datetime
import json


class ManageFriendCircle(Resource):
    def post(self):
        friend_circle_id = [0]
        objGDBUser = GDBUser()
        content = request.get_json()
        creator_user_id = content["creator_user_id"]
        friend_circle_id = content["friend_circle_id"]
        referrer_flag = content["referrer_friend_flag"]
        referred_email_address = content["email_address"]
        referred_name = content["name"]
        friend_user_id = content["friend_user_id"]
        # clean up the inputs before calling the functions
        output = [0]
        objHelper = ManageFriendCircleHelper()

        try:
            if referrer_flag:
                if objHelper.refer_friend_to_circle(creator_user_id, friend_user_id,
                                                    friend_circle_id,
                                                    referred_email_address, referred_name,
                                                    objGDBUser):
                    return {
                               "success": "friend successfully referred. Pl ask your friend to expect an invitation "
                                          "to join "
                                          "the group"}, 200
                else:
                    return {"Failure": "There is error processing the request"}, 500

            print("The ids are ", creator_user_id, friend_user_id)
            if objGDBUser.get_friend_circle_by_user_id(creator_user_id, friend_user_id,
                                                       output) and int(output[0]) < 1:
                print("Circle doesnt exist with the creator and secret friend combination", creator_user_id,
                      friend_user_id)
            if objGDBUser.insert_friend_circle(creator_user_id, friend_user_id, friend_circle_id):
                return {'friend_circle_id': str(friend_circle_id)}, 200
            print("Friend circle with this combination of creator and secret friend exist", creator_user_id,
                  friend_user_id)
            return {'friend_circle': "friend circle with this combination exists"}, 200

        except TestFailure:
            return False

    def get(self):
        content = request.get_json()
        creator_user_id = content["creator_user_id"]
        friend_circle_id = content["friend_circle_id"]
        referrer_flag = content["referrer_friend_flag"]
        referred_email_address = content["email_address"]
        referred_name = content["name"]
        friend_user_id = content["friend_user_id"]
        output = []
        objGDBUser = GDBUser()
        if not objGDBUser.get_all_contributors_to_friend_circle_by_circle_and_userr_id(creator_user_id,
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


class CreatorManagement:
    def post(self):
        return 0

    def get(self):
        return 0


class InterestManagement:

    # get count of users by interest in friend circle
    # interest count by various attributes

    def post(self):
        return 0

    def get(self):
        return 0

class OccasionManagement:
    def post(self):
        return 0

    def get(self):
        return 0
class SecretFriendManagement:
    def post(self):
        # add interest to secret friend
        # add address to secret friend
        # add occasions to secret friend
        content = request.get_json()

        creator_user_id = content["creator_user_id"]  # in this context both admin and contributors are called creators
        secret_friend_id = content["secret_friend_id"]
        interest_id = content["interest_id"]
        occasion_id = content["occasion_id"]
        occasion_date = content["occasion_date"]
        secret_friend_address1 = content["address1"]
        secret_friend_address2 = content["address2"]
        secret_friend_city = content["city"]
        secret_friend_state = content["state"]
        secret_friend_country = content["country"]
        secret_friend_zipcode = content["zipcode"]

        return True
