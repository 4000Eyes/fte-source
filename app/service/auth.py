import os

from flask import Response, request, current_app, g
from flask_jwt_extended import create_access_token, decode_token


from app.model.models import UserHelperFunctions
from app.model.gdbmethods import GDBUser
from app.config import config_by_name
from flask_restful import Resource
import vonage
from app.model.kafka_producer import KafkaMessageProducer

import datetime
import json


class SignupApi(Resource):
    def post(self):
        try:
            print ("Entering /api/auth/signup")
            body = request.get_json()
            if body is None:
                current_app.logger.error("No parameters send into the sign up api (post). Check")
                return {"status": "failure"}, 500
            data = {}
            print(body)
            user_hash = {}
            user_hash["email_address"] = body["email"]
            user_hash["password"] = body["password"]
            user_hash["user_type"] = body["user_type"]
            user_hash["user_status"] = 0 # Meaning inactive
            user_hash["phone_number"] = body["phone_number"]
            user_hash["gender"] = body["gender"]
            user_hash["first_name"] = body["first_name"]
            user_hash["last_name"] = body["last_name"]
            user_hash["location"] = body["location"]
            user_hash["mongo_indexed"] = "N"
            user_hash["image_url"] = body["image_url"] if "image_url" in body else None

            if user_hash.get("email_address") is None or user_hash.get("user_type") is None or user_hash.get("password") is None or user_hash.get("phone_number") is None or user_hash.get("gender") is None or user_hash.get("first_name") is None or user_hash.get("last_name") is None:
                current_app.logger.error("Missing one or many inputs including email, phone, password, gender, first_name, last_name, user_type")
                return {"status": "Missing one or many inputs including email, phone, password, gender, first_name, last_name, user_type"}, 400

            objGDBUser = GDBUser()
            print("Before calling get user")
            if objGDBUser.get_user(user_hash.get("email_address"), data):
                if len(data) > 0:
                    current_app.logger.info("User id exists for " + user_hash.get("email_address") + "user id is" + data.get("user_id") )
                    return {'status': ' User already exists'}, 400
            else:
                current_app.logger.error("User with this email address already exists" + user_hash.get("email_address"))
                return {'status': ' User already exists for ' + user_hash.get("email_address")}, 400

            data = {}

            objHelper = UserHelperFunctions()

            pwd = objHelper.hash_password(user_hash.get("password"))
            print ("The password is ", pwd, user_hash.get("password"))
            user_hash["password"] = pwd
            if not objGDBUser.insert_user(user_hash, data):
                current_app.logger.error("There is an issue in inserting the user")
                return {'status': ' User already exists'}, 400

            if int(os.environ.get("GEMIFT_VERSION")) == 2:
                user_hash.update({"info_type":"new_user_email"})
                kafka_producer =  KafkaMessageProducer(current_app.config["KAFKA_BROKER"])
                kafka_producer.send_msg(json.dumps(user_hash))

            """
            objEmail = EmailManagement()
            registration_token = objHelper.generate_confirmation_token(user_hash["email_address"])

            if registration_token is not None:
                confirmation_url = 'https://www.gemift.com/token=' + registration_token
                if objEmail.send_signup_email(user_hash["email_address"], user_hash["first_name"], user_hash["last_name"], confirmation_url):
                    current_app.logger.error("Error completing registration. Unable to send email to the user")
                    return {"Status" : "Failure in completing registration. Unable to send the email or the email is invalid"}, 401
            """
            return {'data': json.loads(json.dumps(data))}, 200
                # Check if there is an approved invitation request for this user. If so, automatically add them to the circle
                # and take them to the circle home page.
        except Exception as e:
                # Check and delete from graph and mongodb as implementing transaction at this point looks major refactoring.
                print ("The erros is", e)
                current_app.logger.error(e)
                return {'status': 'Failure in inserting the user'}, 500

class PhoneSignUpAPI(Resource):
    def post(self):
        try:
            print("Entering /api/auth/signup")
            body = request.get_json()
            if body is None:
                current_app.logger.error("No parameters send into the sign up api (post). Check")
                return {"status": "failure"}, 500
            data = {}
            print("The body is ", body)
            user_hash = {}
            if "email" not in body:
                return {"status": "No Value for email is sent"}, 400

            user_hash["email_address"] = body["email"]
            user_hash["password"] = body["password"]
            user_hash["user_type"] = body["user_type"]
            user_hash["user_status"] = 0  # Meaning inactive
            user_hash["phone_number"] = body["phone_number"]
            user_hash["gender"] = body["gender"]
            user_hash["first_name"] = body["first_name"]
            user_hash["last_name"] = body["last_name"]
            user_hash["location"] = body["location"]
            user_hash["mongo_indexed"] = "N"
            user_hash["image_url"] = body["image_url"] if "image_url" in body else None

            if user_hash.get("phone_number") is None or user_hash.get("user_type") is None or user_hash.get(
                    "password") is None or user_hash.get("phone_number") is None or user_hash.get(
                    "gender") is None or user_hash.get("first_name") is None or user_hash.get("email_address") is None or user_hash.get("last_name") is None:
                current_app.logger.error(
                    "Missing one or many inputs including email, phone, password, gender, first_name, last_name, user_type")
                return {
                           "status": "Missing one or many inputs including email, phone, password, gender, first_name, last_name, user_type"}, 400

            objGDBUser = GDBUser()
            print("Before calling get user")
            if objGDBUser.get_user_by_phone(user_hash.get("phone_number"), data):
                if len(data) > 0:
                    current_app.logger.info(
                        "User id exists for " + user_hash.get("phone_number") + "user id is" + data.get("user_id"))
                    return {'status': ' User already exists'}, 400
            else:
                current_app.logger.error("User with this phone number already exists" + user_hash.get("phone_number"))
                return {'status': ' User already exists for ' + user_hash.get("phone_number")}, 400

            data = {}

            objHelper = UserHelperFunctions()
            pwd = objHelper.hash_password(user_hash.get("password"))
            print("The password is ", pwd, user_hash.get("password"))
            user_hash["password"] = pwd
            user_hash.update()
            if not objGDBUser.insert_user_by_phone(user_hash, data):
                current_app.logger.error("There is an issue in inserting the user")
                return {'status': ' User already exists'}, 400

            return {'data': json.loads(json.dumps(data))}, 200
            # Check if there is an approved invitation request for this user. If so, automatically add them to the circle
            # and take them to the circle home page.
        except Exception as e:
            # Check and delete from graph and mongodb as implementing transaction at this point looks major refactoring.
            print("The erros is", e)
            current_app.logger.error(e)
            return {'status': 'Failure in inserting the user'}, 500


class RegistrationConfirmation(Resource):
    def post(self):
        content = request.get_json()
        token = content["token"]
        if token is None:
            return {"Status" : "Unable to validate the user"}, 400
        objHelper = UserHelperFunctions()
        objGDBUser = GDBUser()
        loutput = []
        email_address = objHelper.confirm_token(token)
        if email_address is not None:
            if not objGDBUser.get_user_by_email(email_address,loutput):
                if not objGDBUser.activate_user(loutput[0]):
                    current_app.logger.error("Unable to activate the user " +  email_address)
                    return {"status" : "Failure. Unable to activate the user"}, 400
                return {"status": "Successfully activated"}, 200
            else:
                return {"status": "Failure. Unable to find the user information"}, 400
        else:
            return {"status" : "Failure. Invalid email address"}, 400


class LoginApi(Resource):
    def post(self):
        try:
            print ("I am inside the login api function")
            body = request.get_json()
            if body is None:
                current_app.logger.error("No parameters send into the login api (post). Check")
                return {"status":"failure"}, 500
            objGDBUser = GDBUser()
            objUser = UserHelperFunctions()
            ack_hash = {}
            ack_hash["user_id"] = None
            ack_hash["authorized"] = False
            if not objUser.validate_login_gdb(body["email"], body["password"], ack_hash):
                return {'error': 'System issue. Unable to verify the credentials'}, 401
            if not ack_hash["authorized"]:
                return {"error": "password didnt match"}, 401
            print ("The password from the db is ", ack_hash["authorized"])
            if ack_hash["user_id"] is None:
                return {"Error": "User id is empty. Some technical issues"}, 401
            expires = datetime.timedelta(days=7)
            access_token = create_access_token(identity=str(ack_hash["user_id"]), expires_delta=expires)
            hshoutput = {}
            list_output = []
            if not objGDBUser.get_user_summary(ack_hash["user_id"], hshoutput, txn=None, list_output=list_output):
                current_app.logger.error("Unable to get friend circles for user" + ack_hash["user_id"])
                return {"status": "failure"}, 401
            hshoutput.clear()
            hshoutput["logged_user_id"] =  ack_hash["user_id"]
            hshoutput["email_address"] = ack_hash["email_address"]
            hshoutput["phone_number"] = ack_hash["phone_number"]
            hshoutput["first_name"] = ack_hash["first_name"]
            hshoutput["last_name"] = ack_hash["last_name"]
            hshoutput["location"] =  ack_hash["location"]
            hshoutput["gender"] = ack_hash["gender"]
            return {'token': access_token, 'data' : json.loads(json.dumps(hshoutput))}, 200
        except Exception as e:
            print ("The error is ", e)
            return {'token': 'n/a'}, 400

class LoginPhoneAPI(Resource):
    def post(self):
        try:
            print("I am inside the login api function")
            body = request.get_json()
            if body is None:
                current_app.logger.error("No parameters send into the login api (post). Check")
                return {"status": "failure"}, 500
            objGDBUser = GDBUser()
            objUser = UserHelperFunctions()
            ack_hash = {}
            ack_hash["user_id"] = None
            ack_hash["authorized"] = False
            if not objUser.validate_phone_login_gdb(body["phone_number"], ack_hash):
                return {'error': 'System issue. Unable to verify the credentials'}, 401
            if not ack_hash["authorized"]:
                return {"error": "phone number didnt match"}, 401
            print("The password from the db is ", ack_hash["authorized"])
            if ack_hash["user_id"] is None:
                return {"Error": "User id is empty. Some technical issues"}, 401
            expires = datetime.timedelta(days=7)
            access_token = create_access_token(identity=str(ack_hash["user_id"]), expires_delta=expires)
            hshoutput = {}
            list_output = []
            # if not objGDBUser.get_user_summary(ack_hash["user_id"], hshoutput, txn=None, list_output=list_output):
            #     current_app.logger.error("Unable to get friend circles for user" + ack_hash["user_id"])
            #     return {"status": "failure"}, 401
            hshoutput.clear()
            hshoutput["logged_user_id"] = ack_hash["user_id"]
            hshoutput["email_address"] = ack_hash["email_address"]
            hshoutput["phone_number"] = ack_hash["phone_number"]
            hshoutput["first_name"] = ack_hash["first_name"]
            hshoutput["last_name"] = ack_hash["last_name"]
            hshoutput["location"] =  ack_hash["location"]
            hshoutput["gender"] = ack_hash["gender"]
            hshoutput["image_url"] = ack_hash["image_url"] if "image_url" in ack_hash else None
            list_output.append(hshoutput)
            return {'token': access_token, 'data': json.loads(json.dumps(list_output))}, 200
        except Exception as e:
            print("The error is ", e)
            return {'token': 'n/a'}, 400

class GemiftVonageOTP(Resource):
    def post(self):
        try:
            content = request.get_json()
            if "request_id" not in content:
                return {"status" : "Required parameters are missing (phone number, request_id)"}, 400

            request_id = content["request_id"]
            vonage_api_key = os.environ.get("VONAGE_API_KEY")
            vonage_api_secret = os.environ.get("VONAGE_API_SECRET")

            print("The keys are ", vonage_api_key, vonage_api_secret)
            client = vonage.Client(key=vonage_api_key, secret=vonage_api_secret)
            verify = vonage.Verify(client)
            hshOutput = {}
            if request_id == 1:
                if "phone_number" not in content:
                    return {"status" : "Required parameter (phone) is missine"}, 400
                phone_number = content["phone_number"]
                response = verify.start_verification(number=phone_number, brand="Gemift")
                if response["status"] == "0":
                    print("Started verification request_id is %s" % (response["request_id"]))
                    hshOutput["vonage_request_id"] = response["request_id"]
                else:
                    print("Error: %s" % response["error_text"])
                    return {"status": "Error in validating teh request"}, 400

            if request_id == 2:
                if "vonage_request_id" not in content or "vonage_response_code" not in content:
                    return {"status" : "Required parameters are missing"}, 400
                response = verify.check(content["vonage_request_id"], code=content["vonage_response_code"])

                if response["status"] == "0":
                    print("Verification successful, event_id is %s" % (response["event_id"]))
                    hshOutput["status"] = "success"
                    hshOutput["event_id"] = response["event_id"]
                else:
                    print("Error: %s" % response["error_text"])
                    return {"status": "Error in validting the code"}, 400

            return {"status" : json.loads(json.dumps(hshOutput))}

        except Exception as e:
            return {"status" : "Failure in vonage OTP processing"}    , 400



class ForgotPassword(Resource):
    def post(self):
        url = request.host_url + 'reset/'
        try:
            body = request.get_json()
            if body is None:
                current_app.logger.error("No parameters send into the forgot password api (post). Check")
                return {"status":"failure"}, 500
            email = body.get('email')
            if not email:
                return None
            objUser = UserHelperFunctions()
            output_hash = {}
            if objUser.get_user_info_gdb(body["email"], output_hash):
                if "user_id" in output_hash:
                    expires = datetime.timedelta(hours=24)
                    reset_token = create_access_token(str(output_hash["user_id"]), expires_delta=expires)
                    return reset_token
            return {'Error': 'Unable to find the user with the email address'}, 400
        except Exception as e:
            current_app.logger.error("Error executing this function " + e)
            return {'Error': 'Unable to execute the forgot password function'}, 400


class ResetPassword(Resource):
    def post(self):
        url = request.host_url + 'reset/'
        try:
            objUser = UserHelperFunctions()
            body = request.get_json()
            if body is None:
                current_app.logger.error("No parameters send into the reset api (post). Check")
                return {"status":"failure"}, 500
            reset_token = body.get('reset_token')
            password = body.get('password')
            if not reset_token or not password:
                current_app.logger.error("The expected values password and oor reset token is missing")
                return {'Error' : 'The expected values password or reset token are missing'}
            user_id = decode_token(reset_token)['identity']
            if objUser.modify_user_credentials_gdb(user_id, password):
                return {"status" : "successfully reset"},  200
            return {"status": "Unsuccessful in resetting the password"}, 400
        except Exception as e:
            current_app.logger.error("Error executing this function " + e)
            return {'Error': 'Unable to execute the reset password function'},400
