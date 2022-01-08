from flask import request, current_app, jsonify
from app.model.noti_recommend_db import NotificationAndRecommendationDB
from app.model.gdbmethods import GDBUser
from flask_restful import Resource
import json
import copy


class NotoficationAndRecommendation(Resource):
    def get(self):
        try:
            request_id = request.args.get("request_id", type=int)
            user_id = request.args.get("user_id", type=str)

            obj_notification = NotificationAndRecommendationDB()
            list_output = []
            list_data = []
            l_friend_circle = []
            user_output = []
            obj_gdb = GDBUser()
            if not obj_gdb.get_friend_circles(user_id, user_output):
                current_app.logger.error("Unable to get the friend circle information")
                return False
            for row in user_output:
                if row["relationship"] != "SECRET_FRIEND":
                    l_friend_circle.append(row["friend_circle_id"])

            if request_id == 1:

                if not obj_notification.get_occasion_reminder(user_id, list_output):
                    return {"status": "Failure: Unable to get occasion reminders data"}, 400

                for item in user_output:
                    if item["relationship"] == "SECRET_FRIEND":
                        continue
                    ret = self.check_id(item["friend_circle_id"], list_output)
                    if ret == 0:
                        list_output.append({"friend_circle_id": item["friend_circle_id"], "secret_friend": item["secret_friend_id"], "secret_first_name": item["secret_first_name"], "secret_last_name": item["secret_last_name"], "flag":0})

                list_data.append({"occasions": list_output})


                interest_output = []
                if not obj_notification.get_interest_reminders(user_id, l_friend_circle, interest_output):
                    return {"status": "Failure: Unable to get interest reminder data"}, 400

                list_data.append({"interest": interest_output})

                no_interest_output = []
                if not obj_notification.get_no_interest_users(user_id, l_friend_circle, no_interest_output):
                    return {"status": "Failure: Unable to get interest reminder data"}, 400

                nox_interest_output = []
                for item in user_output:
                    if item["relationship"] == "SECRET_FRIEND":
                        continue
                    hsh_out = {}
                    ret = self.check_interest_count(item["friend_circle_id"], no_interest_output, hsh_out)
                    if ret == 0:
                        flag = hsh_out["flag"] if "flag" in hsh_out else 0
                        days_since = hsh_out["days_since"] if "days_since" in hsh_out else 0
                        nox_interest_output.append({"friend_circle_id": item["friend_circle_id"], "secret_friend": item["secret_friend_id"], "secret_first_name": item["secret_first_name"], "secret_last_name": item["secret_last_name"], "flag": flag, "days_since":days_since})

                list_data.append({"interest": nox_interest_output})

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

                return {"data": json.dumps(list_data)}, 200

            if request_id == 2: # Just get the occasion reminders only
                pass
            if request_id == 3: # Get the interest data only
                pass
            if request_id == 4: # Get the relationship data only
                pass
            if request_id == 5: # get the approval data only
                pass

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


