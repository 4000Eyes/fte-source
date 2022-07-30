from flask import Response, request, current_app, jsonify, json
from app.model.searchdb import SearchDB
from app.model.gdbmethods import GDBUser
from app.model.registereduserpreferencedb import RegisteredUserPreferenceDB
from app.model.friendlistdb import FriendListDB
from app.service.general import SiteGeneralFunctions
from flask_restful import Resource

class RegisteredUserPreference(Resource):
    def post(self):

        content = request.get_json()
        request_type = content["request_type"]
        user_id = content["user_id"]
        list_category_id = content["list_category_id"] if "list_category_id" in content else None
        list_subcategory_id = content["list_subcategory_id"] if "list_subcategory_id" in content else None
        obj_gdb = GDBUser()
        list_output = []
        if request_type == "add_category":
            if not obj_gdb.link_user_to_personal_web_category(user_id, list_category_id):
                current_app.logger.error("Unable to link category to the personal user")
                return {"status": "Failure"}, 400
            return {"status": "Success"}, 200

        if request_type == "add_subcategory":
            if not obj_gdb.link_user_to_personal_web_subcategory(user_id, list_subcategory_id):
                current_app.logger.error("Unable to link category to the personal user")
                return {"status": "Failure"}
            return {"status": "Success"}, 200


    def get(self):

        request_type = request.args.get("request_type", type=str)
        user_id = request.args.get("user_id",type=str)
        obj_gdb = GDBUser()
        hsh_output = {}
        list_output = []
        if request_type == "get_category":
            if not obj_gdb.get_personal_category_interest_by_user(user_id, list_output):
                current_app.logger.error("Unable to get the interest for the personal user")
                return {"status": "Failure"}, 400
            return {'data': json.loads(json.dumps(list_output))}, 200

        if request_type == "get_subcategory":
            if not obj_gdb.get_personal_subcategory_interest_by_user(user_id, list_output):
                current_app.logger.error("Unable to get the interest for the personal user")
                return {"status": "Failure"}, 400
            return {'data': json.loads(json.dumps(list_output))}, 200

        if request_type == "get_match_score":
            obj_personal_user = RegisteredUserPreferenceDB()
            if not obj_personal_user.get_match_index(user_id,list_output):
                current_app.logger.error("Unable to get the match index for the user")
                return {"status": "Failure"}, 400
            return {"data":json.loads(json.dumps(list_output))}, 200

        if request_type == "get_stats":
            if not obj_gdb.get_total_interests_stats(list_output):
                current_app.logger.error("Unable to get interest stats")
                return {"status":"Failure"}, 400

            if not obj_gdb.get_total_occasion_stats(list_output):
                current_app.logger.error("Unable to get interest stats")
                return {"status":"Failure"}, 400

            if not obj_gdb.get_total_friend_circle_stats(list_output):
                current_app.logger.error("Unable to get interest stats")
                return {"status":"Failure"}, 400

            return {"data": json.loads(json.dumps(list_output))}, 200
