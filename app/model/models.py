#from app.app import g
#import pymongo.collection
from flask import current_app, g
import pymongo.collection, pymongo.errors
from flask_bcrypt import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer
from app.model.gdbmethods import GDBUser
"""

class User(dbx.Document):

    user_id = dbx.StringField(required=True, max_length=64)
    email = dbx.EmailField(required=True, unique=True)
    password = dbx.StringField(required=True, min_length=6)
    user_type = dbx.IntField(required=True)
    first_name = dbx.StringField(required=True, max_length=64)
    last_name = dbx.StringField(required=True, max_length=64)
    gender = dbx.StringField(required=True, max_length=2)
    phone_number = dbx.StringField(required=True, max_length=64)

    def hash_password(self):
        self.password = generate_password_hash(self.password).decode('utf8')

    def check_password(self, password):
        return check_password_hash(self.password, password)

    def delete(self, user_id):
        dbx.user.objects.get({"user_id":user_id}).delete()


class EmailUserQueue(dbx.Document):
    email = dbx.EmailField()
    phone_number = dbx.StringField(max_length=32)
    name = dbx.StringField(max_length=128)
    friend_circle_id = dbx.StringField(max_length=128)
    referred_user_id = dbx.ObjectIdField(required=True)
    friend_circle_admin_id = dbx.StringField(max_length=64)
    comm_type = dbx.StringField(max_length=64) # email, whatsapp, etc...
    status = dbx.IntField()


class FriendCircleApprovalQueue(dbx.Document):
    referring_user_id = dbx.StringField(max_length=64)
    referred_user_id = dbx.StringField(max_length=64)
    friend_circle_id = dbx.StringField(max_length=64)
    friend_circle_admin_id = dbx.StringField(max_length=64)
    email_address = dbx.StringField(max_length=256)
    first_name = dbx.StringField(max_length=256)
    last_name = dbx.StringField(max_length=256)
    location = dbx.StringField(max_length=128)
    gender = dbx.StringField(max_length=64)
    registration_complete_status = dbx.IntField()  # This should be 0 until the registration is complete and the
    # completion process updates this row.
    status = dbx.IntField()


class ProductDetail(dbx.Document):
    product_id = dbx.StringField(max_length=64)
    product_name = dbx.StringField(max_length=256)
    product_desc = dbx.StringField(max_length=1028)
    product_merch_category = dbx.StringField(max_length=64)
    price = dbx.DecimalField(precision=2)
    created_dt = dbx.DateTimeField()
    updated_dt = dbx.DateTimeField()
    currency = dbx.StringField(max_length=10)
    site_id = dbx.StringField(max_length=128)
    country_id = dbx.IntField()
"""
class UserHelperFunctions():
    def hash_password(self, pwd):
        return generate_password_hash(pwd).decode('utf8')

    def check_password(self, pwd, upwd):
        return check_password_hash(pwd, upwd)

    def validate_login_gdb(self, email, pwd, ack_hash):
        try:
            objDBUser = GDBUser()
            loutput = {}
            if not objDBUser.get_user_by_email(email, loutput):
                current_app.logger.error("Unable to get the user from the db for email " + email)
                return False
            if loutput["user_id"] is None or loutput["password"] is None:
                current_app.logger.error("Unable to get the user from the db for email " + email)
                return False
            ack_hash["user_id"] = loutput["user_id"]
            ack_hash["authorized"] = self.check_password(loutput["password"], pwd)
            return True
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def validate_phone_login_gdb(self, phone_number, ack_hash):
        try:
            objDBUser = GDBUser()
            loutput = {}
            if not objDBUser.get_user_by_phone(phone_number, loutput):
                current_app.logger.error("Unable to get the user from the db for phone  " + phone_number)
                return False
            if loutput["user_id"] is None :
                current_app.logger.error("Unable to get the user from the db for phone " + phone_number)
                return False
            ack_hash["user_id"] = loutput["user_id"]
            ack_hash["authorized"] = 1
            ack_hash["email_address"] = loutput["email_address"]
            ack_hash["phone_number"] = loutput["phone_number"]
            ack_hash["first_name"] = loutput["first_name"]
            ack_hash["last_name"] = loutput["last_name"]
            ack_hash["gender"] = loutput["gender"]
            ack_hash["location"] = loutput["location"]
            ack_hash["image_url"] = loutput["image_url"]
            return True
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def validate_login(self, email,pwd, ack_hash):
        try:
            mongo_user = pymongo.collection.Collection(g.db, "user")
            result = mongo_user.find({"email": email})
            if result is not None:
                for row in result:
                    ack_hash["authorized"] = self.check_password(row["password"], pwd)
                    ack_hash["user_id"] = row["user_id"]
                    ack_hash["email_address"] = row["email_address"]
                    ack_hash["phone_number"] = row["phone_number"]
                    ack_hash["first_name"] = row["first_name"]
                    ack_hash["last_name"] = row["last_name"]
                    ack_hash["gender"] = row["gender"]
                    ack_hash["location"] = row["location"]
                    ack_hash["image_url"] = ack_hash["image_url"]
            return True
        except pymongo.errors as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_user_info_gdb(self, email, output_hash):
        try:
            objDBUser = GDBUser()
            loutput = {}
            if not objDBUser.get_user_by_email(email, loutput):
                current_app.logger.error("Unable to get the user from the db for email " + email)
                return False
            if loutput["user_id"] is None or loutput["password"] is None:
                current_app.logger.error("Unable to get the user from the db for email " + email)
                return False
            return True
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_user_info(self, email, output_hash):
        try:
            mongo_user = pymongo.collection.Collection(g.db, "User")
            result = mongo_user.find({"email_address": email})
            if result is not None:
                output_hash["user_id"] = result["user_id"]
                return True
            return False
        except pymongo.errors as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def modify_user_credentials_gdb(self, user_id, password):
        try:
            objDBUser = GDBUser()
            input_hash = {}
            input_hash["user_id"] = user_id
            input_hash["password"] = self.hash_password(password)
            if not objDBUser.update_user_password(input_hash):
                current_app.logger.error("Unable to update the password for user" + user_id)
                return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def modify_user_credentials(self, user_id, password):
        try:
            mongo_user = pymongo.collection.Collection(g.db, "user")
            result = mongo_user.find({"user_id": user_id})
            if result is not None:
                hsh_pwd = self.hash_password(password)
                xres = mongo_user.update_one({"user_id":user_id}, {"password":hsh_pwd},upsert=False)
                if xres is not None:
                    return True
            return False
        except pymongo.errors as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def generate_confirmation_token(self, email):
        serializer = URLSafeTimedSerializer(current_app.config['SECRET_KEY'])
        return serializer.dumps(email, salt=current_app.config['SECURITY_PASSWORD_SALT'])

    def confirm_token(self, token, expiration=3600):
        serializer = URLSafeTimedSerializer(current_app.config['SECRET_KEY'])
        try:
            email = serializer.loads(token,
                                     salt = current_app.config['SECURITY_PASSWORD_SALT'],
                                     max_age= expiration)
            return email
        except Exception as e:
            return None
