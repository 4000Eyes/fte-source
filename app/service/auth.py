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
        user_id = [0]
        data = None
        print(body)
        objGDBUser = GDBUser()
        if objGDBUser.get_gdbuser(body["email"], user_id, data, 1):
            if user_id is not None:
                current_app.logger.info("User id exists for " + body["email"] + "user id is" + user_id[0])
                raise UserInsertionError
        if not objGDBUser.insert_gdbuser(body["email"], body["password"], body["user_type"], user_id[0]):
            current_app.logger.error("There is an issue in inserting the user")
            raise UserInsertionError
        User.email = body["email"]
        User.password = body["password"]
        User.user_id = user_id[0]
        User.hash_password()
        User.save()

        # Check if there is an approved invitation request for this user. If so, automatically add them to the circle
        # and take them to the circle home page.

        return {'id': str(user_id[0])}, 200


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


class ForgotPassword(Resource):
    def post(self):
        url = request.host_url + 'reset/'
        try:
            body = request.get_json()
            email = body.get('email')
            if not email:
                raise SchemaValidationError
            user = User.objects.get(email=email)
            if not user:
                raise EmailDoesNotExistError
            expires = datetime.timedelta(hours=24)
            reset_token = create_access_token(str(user.id), expires_delta=expires)
            return reset_token

            """
            return send_email('[Movie-bag] Reset Your Password',
                              sender='support@fte.com',
                              recipients=[user.email],
                              text_body=render_template('email/reset_password.txt',
                                                        url=url + reset_token),
                              html_body=render_template('email/reset_password.html',
                                                        url=url + reset_token))
            """
        except SchemaValidationError:
            raise SchemaValidationError
        except EmailDoesnotExistsError:
            raise EmailDoesnotExistsError
        except Exception as e:
            raise InternalServerError


class ResetPassword(Resource):
    def post(self):
        url = request.host_url + 'reset/'
        try:
            body = request.get_json()
            reset_token = body.get('reset_token')
            password = body.get('password')

            if not reset_token or not password:
                raise SchemaValidationError
            user_id = decode_token(reset_token)['identity']
            user = User.objects.get(id=user_id)
            user.modify(password=password)
            user.hash_password()
            user.save()
            return {"status" : "successfully reset"},  200
        except SchemaValidationError:
            raise SchemaValidationError
        except ExpiredSignatureError:
            raise ExpiredTokenError
        except (DecodeError, InvalidTokenError):
            raise BadTokenError
        except Exception as e:
            raise InternalServerError
