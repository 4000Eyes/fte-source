#from flask_restful import Resource

from app.service.auth import SignupApi, LoginApi, ResetPassword, ForgotPassword, RegistrationConfirmation, PhoneSignUpAPI, LoginPhoneAPI, GemiftVonageOTP
from app.service.friendmanagement import ManageFriendCircle, InterestManagement, OccasionManagement,SecretFriendAttributeManagement, FriendAttributes
from app.service.category import CategoryManagement
from app.service.userproductmanagement import UserProductManagement, UserSearchManagement, AllCategoryRelatedManagement
from app.service.notif_and_recommend import OccasionAndRecommendation
from app.service.email_svc import EmailManagement
#from app.main import api

def initialize_routes(api):
   #api.add_resource(SignupApi, '/api/auth/signup')
   api.add_resource(PhoneSignUpAPI, '/api/auth/signup')
   #api.add_resource(PhoneSignUpAPI, '/api/auth/phone/signup')
   api.add_resource(RegistrationConfirmation, '/api/user/confirm')
   api.add_resource(LoginApi, '/api/login')
   api.add_resource(LoginPhoneAPI, '/api/phone/login')
   api.add_resource(GemiftVonageOTP, '/api/otp')
   api.add_resource(ResetPassword, '/api/reset')
   api.add_resource(ForgotPassword, '/api/forgotpassword')
   api.add_resource(ManageFriendCircle, '/api/friend/circle')
   api.add_resource(SecretFriendAttributeManagement, '/api/sfriend/age')
   api.add_resource(CategoryManagement, '/api/category')
   api.add_resource(InterestManagement, '/api/interest')
   api.add_resource(UserProductManagement, '/api/prod/search')
   api.add_resource(UserSearchManagement, '/api/user/search')
   api.add_resource(AllCategoryRelatedManagement, '/api/ch/search')
   api.add_resource(OccasionManagement, '/api/user/occasion')
   api.add_resource(OccasionAndRecommendation, '/api/notify')
   api.add_resource(EmailManagement, '/api/email')
   api.add_resource(FriendAttributes, '/api/attr')


   return 0
