from app.model.email_manager import EmailManagement
from flask import request, current_app, jsonify
from flask_restful import Resource
import json


class EmailService(Resource):
    def post(self):
        try:
            content = request.get_json()
            request_id = content["request_id"]
            email_to = content["email_to"]
            if email_to is None:
                current_app.logger.error("Email id cannot be none")
                return {"status" : "Email to id is none"}, 400
            email_to_first_name = content["email_to_first_name"]
            email_to_last_name = content["email_to_last_name"]
            secret_friend_name = content["secret_first_name"]
            friend_list = content["friend_list"]
            call_to_action = content["call_to_action"]

            objEmail = EmailManagement()
            objEmail.init_service()
            if request_id == 1: # sign up thank you email
                if not objEmail.send_signup_email(email_to,email_to_first_name,email_to_last_name, call_to_action):
                    current_app.logger.error("Unable to send email to " + email_to)
                    return {"status" : "Failure"}, 401

            if request_id == 2: # friend invitation email
                if secret_friend_name is None or friend_list is None or call_to_action is None:
                    current_app.logger.error("Secret friend or friend list cannot or call to action cannot be null")
                    return {"status" : "Failure. Secret friend name or friend list or call to action cannot be null"}
                if not objEmail.send_friend_invitation_email(email_to,email_to_first_name,email_to_last_name,secret_friend_name,friend_list,call_to_action):
                    current_app.logger.error("Unable to send email to " + email_to)
                    return {"status" : "Failure"}, 400
        except Exception as e:
            current_app.logger.error("something happened with sending email " + e)
            return {"status" : "Failure"}, 400