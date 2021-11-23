from flask import request, current_app, jsonify
from app.model.noti_recommend_db import NotificationAndRecommendationDB
from flask_restful import Resource
import json

class OccasionAndRecommendation(Resource):
    def get(self):
        request_id = request.args.get("request_id", type=int)
        user_id = request.args.get("user_id", type=str)

        objOccasion = NotificationAndRecommendationDB()
        list_output = []
        if request_id == 1:
            if objOccasion.get_reminder(user_id,list_output):
                current_app.logger.error("Unable to get reminders for user" + user_id)
                return {"status": "Failure. Unable to fetch reminders for user " + user_id}, 400
            data = json.dumps(list_output)
            return {"data": data}, 200