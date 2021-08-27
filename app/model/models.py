from app import db
from flask_bcrypt import generate_password_hash, check_password_hash


class User(db.Document):
   user_id = db.StringField(required=True, max_length=32)
   email = db.EmailField(required=True, unique=True)
   password = db.StringField(required=True, min_length=6)
   user_type = db.IntField(required=True)


   def hash_password(self):
      self.password = generate_password_hash(self.password).decode('utf8')

   def check_password(self, password):
       return check_password_hash(self.password, password)


class EmailUserQueue(db.Document):
   email = db.EmailField(required=True)
   friend_circle_id = db.StringField(max_length=128)
   referred_user_id = db.ObjectIdField(required=True)
   friend_circle_admin_id = db.StringField(max_length = 32)
   status = db.IntField()

class FriendCircleApprovalQueue(db.Document):
   referring_user_id = db.StringField(max_length = 32)
   referred_user_id = db.StringField(max_length = 32)
   friend_circle_id = db.StringField(max_length = 32)
   friend_circle_admin_id = db.StringField(max_length = 32)
   email_address = db.StringField(max_length = 256)
   registration_complete_status = db.IntField() # This should be 0 until the registration is complete and the completion process updates this row.
   status = db.IntField()




