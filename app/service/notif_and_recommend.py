from flask import request, current_app, jsonify
from app.model.noti_recommend_db import NotificationAndRecommendationDB
from app.model.gdbmethods import GDBUser
from app.model.friendlistdb import FriendListDB
from app.service.general import SiteGeneralFunctions
from flask_restful import Resource
import json
import copy


class NotificationAndRecommendation(Resource):
    def get(self):
        try:
            request_id = request.args.get("request_id", type=int)
            user_id = request.args.get("user_id", type=str)
            phone_number = request.args.get("phone_number", type=str)
            obj_notification = NotificationAndRecommendationDB()
            list_output = []
            list_data = []
            l_friend_circle = []
            user_output = []
            obj_gdb = GDBUser()
            objFriend = FriendListDB()

            if not obj_gdb.get_friend_circles(user_id, user_output):
                current_app.logger.error("Unable to get the friend circle information")
                return False
            hsh = {}
            for row in user_output:
                if row["relationship"] != "SECRET_FRIEND":
                    if row["friend_circle_id"] not in hsh:
                        hsh[row["friend_circle_id"]] = 1
                        l_friend_circle.append(row["friend_circle_id"])

            if request_id == 1:

                if not obj_notification.friend_circle_with_no_occasion(l_friend_circle, list_output):
                    return {"status": "Failure: Unable to get occasion reminders data"}, 400

                # for item in user_output:
                #     if item["relationship"] == "SECRET_FRIEND":
                #         continue
                #     ret = self.check_id(item["friend_circle_id"], list_output)
                #     if ret == 0:
                #         list_output.append({"friend_circle_id": item["friend_circle_id"], "secret_friend": item["secret_friend_id"], "secret_first_name": item["secret_first_name"], "secret_last_name": item["secret_last_name"], "flag":0})

                list_data.append({"no_occasions": list_output})

                days_since_output = []
                if not obj_notification.days_since_last_occasion(l_friend_circle, days_since_output):
                    return {"status": "Failure. Unable to get the occasions created N days ago"}, 400

                list_data.append({"days_since_occasion": days_since_output})

                interest_output = []
                if not obj_notification.get_interest_reminders(user_id, l_friend_circle, interest_output):
                    return {"status": "Failure: Unable to get interest reminder data"}, 400

                list_data.append({"interest": interest_output})

                no_interest_output = []
                if not obj_notification.get_no_interest_users(user_id, l_friend_circle, no_interest_output):
                    return {"status": "Failure: Unable to get no interest reminder data"}, 400

                # nox_interest_output = []
                # for item in user_output:
                #     if item["relationship"] == "SECRET_FRIEND":
                #         continue
                #     hsh_out = {}
                #     ret = self.check_interest_count(item["friend_circle_id"], no_interest_output, hsh_out)
                #     if ret == 0:
                #         flag = hsh_out["flag"] if "flag" in hsh_out else 0
                #         days_since = hsh_out["days_since"] if "days_since" in hsh_out else 0
                #         nox_interest_output.append({"friend_circle_id": item["friend_circle_id"], "secret_friend": item["secret_friend_id"], "secret_first_name": item["secret_first_name"], "secret_last_name": item["secret_last_name"], "flag": flag, "days_since":days_since})
                #
                list_data.append({"no interest": no_interest_output})

                relationship_output = []
                if not obj_notification.get_relationship_status(user_id, l_friend_circle, relationship_output):
                    current_app.logger.error("Unable to get the relationship data")
                    return {"status": "Failure: Unable to get relationship data"}, 400

                list_data.append({"relationship": relationship_output})

                approval_output = []
                if not obj_notification.get_approval_requests(user_id, approval_output):
                    return {"status": "Failure: Unable to get approval data"}, 400
                if len(list_output) > 0:
                    list_data.append({"approval": approval_output})

                contributor_approval_output = []

                if not objFriend.get_open_invites(phone_number, contributor_approval_output):
                    return {"status" : "Failure: Unable to get the list of open contributor approvals"}, 400
                list_data.append({"contributor_invites" : contributor_approval_output})

            list_unapproved_occasions = []

            if not obj_gdb.get_unapproved_occasions(user_id, l_friend_circle, list_unapproved_occasions):
                return {"status": "Error in getting the occasion approval data"}, 200

            list_data.append({"unapproved_occasions": list_unapproved_occasions})

            return json.loads(json.dumps(list_data)), 200

            if request_id == 2: # for the app notification page
                final_output = []
                if not obj_gdb.get_occasion_by_user(user_id, list_output, 1):
                    return {"status": "Error in getting the occasion approval data"}, 200
                final_output.append(list_output)

                contributor_approval_output = []
                if not objFriend.get_open_invites(phone_number, contributor_approval_output):
                    return {"status" : "Failure: Unable to get the list of open contributor approvals"}, 400
                final_output.append({"contributor_invites" : contributor_approval_output})

                approval_output = []
                if not obj_notification.get_approval_requests(user_id, approval_output):
                    return {"status": "Failure: Unable to get approval data"}, 400
                if len(list_output) > 0:
                    final_output.append({"approval": approval_output})

                return {"data": json.loads(json.dumps(final_output))}, 200
            if request_id == 3: # Get the interest data only
                pass
            if request_id == 4: # Get the relationship data only
                pass
            if request_id == 5: # get the circle approval data only
                pass
            if request_id == 6: # get occasion approval
                list_unapproved_occasions = []
                if not obj_gdb.get_unapproved_occasions(user_id, l_friend_circle, list_unapproved_occasions):
                    return {"status": "Error in getting the occasion approval data"}, 200
                return {"data":json.loads(json.dumps(list_unapproved_occasions))}, 200
            if request_id == 7: # Just get the occasion reminders only
                pass


            """
            check if the user is admin for any friend circles
            """

        except Exception as e:
            return {"status" : "Failure in processing the notification request"}, 400

    def check_id(self, xid, hsh: dict):
        try:
            for item in hsh:
                if item["friend_circle_id"] == xid:
                    return 1
            return 0
        except Exception as e:
            current_app.logger.error("Unable to loop through")
            return -1

    def check_interest_count(self, xid, hsh, hsh_out: dict):
        try:
            for item in hsh:
                hsh_out.clear()
                if item["friend_circle_id"] == xid :
                    hsh_out["days_since"] = item["days_since"]
                    hsh_out["flag"] = item["flag"]
                    if item["interest_count"] > 0 :
                        return 1
            return 0
        except Exception as e:
            current_app.logger.error("Unable to loop through")
            return -1


"""

Possible notifications:
1) Circle approval
2) Contributor approval
3) Occasion approval


What will go in app notification vs landing page?

app notification:
    all approvals
        - circle approval
        - contributor approval
        - occasion approval
    Wallet management
        Paid confirmation
        Balance reminder
Landing page:
    all content(Landing page should host all content for viewing)
        - Upcoming Occasions
        - Approvals (all)
        - Recently bought products for the occasion
        - Interests
        - Other sections as mentioned in the mock
    all reminders
        interest - full mode
        occasion - full mode 
        relationship - full mode
        Balance reminder - full mode (this is the only content that will be shown on both app notification and full mode)
        product recommendation for occasions - full mode
        team buy - opt in or opt out

Team Buy (Wallet management):
    - Active team buy transactions (This will show all the initiated team buys by the user or the friend circle)
    - Balance to be paid by the user
    - Expected money to be spent by the user
    - Delayed payment
    - Recently closed transactions
"""
