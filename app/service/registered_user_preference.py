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
        obj_gdb = GDBUser()
        list_output = []
        if request_type == "add_category":
            if obj_gdb.link_user_to_personal_web_category(user_id, list_output):
                current_app.logger.error("Unable to link category to the personal user")
                return {"status": "Failure"}


        if request_type == "add_subcategory":
            if obj_gdb.link_user_to_personal_web_subcategory(user_id, list_output):
                current_app.logger.error("Unable to link category to the personal user")
                return {"status": "Failure"}



    def get(self):

        request_type = request.args.get("request_type", type=str)
        user_id = request.args.get("user_id",type=str)
        obj_gdb = GDBUser()
        hsh_output = {}

        if request_type == "get_personal_category":
            if obj_gdb.get_personal_category_interest_by_user(user_id, hsh_output):
                current_app.logger.error("Unable to get the interest for the personal user")
                return {"status": "Failure"}

        if request_type == "get_personal_subcategory":
            if obj_gdb.get_personal_subcategory_interest_by_user(user_id, hsh_output):
                current_app.logger.error("Unable to get the interest for the personal user")
                return {"status": "Failure"}

        if request_type == "get_match_score":
            obj_personal_user = RegisteredUserPreferenceDB()
            if obj_personal_user.get_match_index(user_id,hsh_output):
                current_app.logger.error("Unable to get the match index for the user")
                return {"status": "Failure"}