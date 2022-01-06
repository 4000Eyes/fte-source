from flask import request, current_app, jsonify
from app.model.noti_recommend_db import NotificationAndRecommendationDB
from app.model.gdbmethods import GDBUser
from flask_restful import Resource
import json


class OccasionAndRecommendation(Resource):
    def get(self):
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
            l_friend_circle.append(row["friend_circle_id"])

        if request_id == 1:

            if not obj_notification.get_occasion_reminder(user_id, list_output):
                return {"status": "Failure: Unable to get occasion reminders data"}, 400
            list_data.append({"occasions" : list_output})

            list_output = []
            if not obj_notification.get_interest_reminders(user_id, l_friend_circle, list_output):
                return {"status": "Failure: Unable to get interest reminder data"}, 400
            if len(list_output) <= 0:
                list_data.append({"no interest" : l_friend_circle})
            else:
                list_data.append({"interest": list_output})
            list_output = []
            if not obj_notification.get_relationship_status(user_id, l_friend_circle, list_output):
                return {"status": "Failure: Unable to get relationship data"}, 400
            list_data.append({"relationship": list_output})

            list_output = []
            if not obj_notification.get_approval_requests(user_id, list_output):
                return {"status": "Failure: Unable to get approval data"}, 400
            list_data.append({"approval": list_output})

            return {"data": json.dumps(list_data)}, 200
