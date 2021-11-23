from flask_restful import Resource

from app.service.auth import SignupApi, LoginApi, ResetPassword, ForgotPassword
from app.service.friendmanagement import ManageFriendCircle, InterestManagement, OccasionManagement
from app.service.category import CategoryManagement
from app.service.userproductmanagement import UserProductManagement, UserSearchManagement
from app.service.notif_and_recommend import NotificationAndRecommendationDB
from app.main import api

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
   api.add_resource(NotificationAndRecommendationDB, '/api/user/notify')

   return 0
