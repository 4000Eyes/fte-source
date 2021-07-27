from flask import Response, request, current_app
from flask_jwt_extended import create_access_token
from model.models import User
from model.gdbmethods import GDBUser
from flask_restful import Resource
import datetime
import json


class SignupApi(Resource):
    def post(self):
        print ("Entering /api/auth/signup")
        body = request.get_json()
        strvalue = json.dumps(body)
        current_app.logger.info("The incoming request is" + strvalue)
        user_id = None
        data = None
        print(body)
        objGDBUser = GDBUser()
        if objGDBUser.get_gdbuser(body["email"], user_id, data, 1):
            if user_id is not None:
                current_app.logger.info("User id exists for " + body["email"] + "user id is" + user_id)
                return {'User Exists'}, 400
        if not objGDBUser.insert_gdbuser(body["email"], body["password"], user_id):
            current_app.logger.error("There is an issue in inserting the user")
        user = User(**body)
        user.hash_password()
        user.save()
        uid = user.id

        # Check if there is an approved invitation request for this user. If so, automatically add them to the circle
        # and take them to the circle home page.

        return {'id': str(user_id)}, 200


class LoginApi(Resource):
    def post(self):
        body = request.get_json()
        user = User.objects.get(email=body.get('email'))
        authorized = User.check_password(body.get('password'))
        if not authorized:
            return {'error': 'Email or password invalid'}, 401
        expires = datetime.timedelta(days=7)
        access_token = create_access_token(identity=str(user.id), expires_delta=expires)
        return {'token': access_token}, 200



