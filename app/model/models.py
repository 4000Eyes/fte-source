from app import dbx
from flask_bcrypt import generate_password_hash, check_password_hash


class User(dbx.Document):
    user_id = dbx.StringField(required=True, max_length=64)
    email = dbx.EmailField(required=True, unique=True)
    password = dbx.StringField(required=True, min_length=6)
    user_type = dbx.IntField(required=True)

    def hash_password(self):
        self.password = generate_password_hash(self.password).decode('utf8')

    def check_password(self, password):
        return check_password_hash(self.password, password)


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
