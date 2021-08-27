from flask_restful import Resource
from service.auth import SignupApi, LoginApi
from service.friendmanagement import ManageFriendCircle
from app import api

def initialize_routes(api):
   api.add_resource(SignupApi, '/api/auth/signup')
   api.add_resource(LoginApi, '/api/login')
   api.add_resource(ManageFriendCircle, '/api/friend/circle')

   return 0
