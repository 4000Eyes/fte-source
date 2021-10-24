from flask_restful import Resource
from service.auth import SignupApi, LoginApi, ResetPassword, ForgotPassword
from service.friendmanagement import ManageFriendCircle, InterestManagement, OccasionManagement
from service.category import CategoryManagement
from service.userproductmanagement import UserProductManagement, UserSearchManagement
from app import api

def initialize_routes(api):
   api.add_resource(SignupApi, '/api/auth/signup')
   api.add_resource(LoginApi, '/api/login')
   api.add_resource(ResetPassword, '/api/reset')
   api.add_resource(ForgotPassword, '/api/forgotpassword')
   api.add_resource(ManageFriendCircle, '/api/friend/circle')
   api.add_resource(CategoryManagement, '/api/category')
   api.add_resource(InterestManagement, '/api/interest')
   api.add_resource(UserProductManagement, '/api/prod/search')
   api.add_resource(UserSearchManagement, '/api/user/search')
   api.add_resource(OccasionManagement, '/api/user/occasion')
   return 0
