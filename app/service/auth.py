from flask import Response, request, current_app
from flask_jwt_extended import create_access_token, decode_token
from model.models import User
from model.gdbmethods import GDBUser
from flask_restful import Resource
from .fte_exceptions import SchemaValidationError, EmailDoesNotExistError, UserInsertionError
import datetime
import json


class SignupApi(Resource):
    def post(self):
        print ("Entering /api/auth/signup")
        body = request.get_json()
        if body is None:
            current_app.logger.error("No parameters send into the sign up api (post). Check")
            return {"status": "failure"}, 500
        data = {}
        print(body)

        email_address = body["email"] if "email" in body else None
        user_type = body["user_type"] if "user_type" in body else None
        password = body["password"] if "password" in body else None
        phone_number = body["phone_number"] if "phone_number" in body else None
        gender = body["gender"] if "gender" in body else None
        objGDBUser = GDBUser()
        if objGDBUser.get_user(body["email"], data):
            if len(data) > 0:
                current_app.logger.info("User id exists for " + body["email"] + "user id is" + data.get("user_id") )
                return {'status': ' User already exists'}, 400
        else:
            current_app.logger.error("User with this email address already exists" + email_address)
            return {'status': ' User already exists'}, 400

        data = {}
        user_hash = {}

        user_hash["email_address"] = email_address
        user_hash["password"] = password
        user_hash["user_type"] = user_type
        user_hash["phone_number"] = phone_number
        user_hash["gender"] = gender
        if not objGDBUser.insert_user(user_hash, data):
            current_app.logger.error("There is an issue in inserting the user")
            return {'status': ' User already exists'}, 400

        try:
            objUser = User()
            objUser.email = email_address
            objUser.password = password
            objUser.user_type = user_type
            objUser.user_id = data.get("user_id")
            print("The user id is", data.get("user_id"))
            objUser.hash_password()
            objUser.save()
            print ("User successfully inserted", email_address, user_type, data.get("user_id"))
            # Check if there is an approved invitation request for this user. If so, automatically add them to the circle
            # and take them to the circle home page.
        except Exception as e:
            print ("The erros is", e)
            return {'status': 'Failure in inserting the user'}, 400
        return {'id': str(data[0])}, 200


class LoginApi(Resource):
    def post(self):
        try:
            print ("I am inside the login api function")
            body = request.get_json()
            if body is None:
                current_app.logger.error("No parameters send into the login api (post). Check")
                return {"status":"failure"}, 500
            objUser = User()
            objUser = User.objects.get(email=body.get('email'))
            authorized = objUser.check_password(body.get('password'))
            print ("The password from the db is ", authorized)
            if not authorized:
                return {'error': 'Email or password invalid'}, 401
            expires = datetime.timedelta(days=7)
            access_token = create_access_token(identity=str(objUser.user_id), expires_delta=expires)
            return {'token': access_token}, 200
        except Exception as e:
            print ("The error is ", e)
            return {'token': 'n/a'}, 400


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
                raise SchemaValidationError
            objUser = User()
            objUser = User.objects.get(email=email)
            if not objUser:
                return {'Error': 'Unable to find the user with the email address'}, 400
            expires = datetime.timedelta(hours=24)
            reset_token = create_access_token(str(objUser.user_id), expires_delta=expires)
            return reset_token

        except Exception as e:
            current_app.logger.error("Error executing this function " + e)
            return {'Error': 'Unable to execute the forgot password function'}, 400


class ResetPassword(Resource):
    def post(self):
        url = request.host_url + 'reset/'
        try:
            objUser = User()
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
            objUser = User.objects.get(id=user_id)
            objUser.modify(password=password)
            objUser.hash_password()
            objUser.save()
            return {"status" : "successfully reset"},  200
        except Exception as e:
            current_app.logger.error("Error executing this function " + e)
            return {'Error': 'Unable to execute the reset password function'}, 400
