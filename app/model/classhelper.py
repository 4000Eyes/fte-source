# this class will have all the helper functions for the API classes
# decision should be made later if we have to seperate the helpfer function by API
from flask import current_app
from .gdbmethods import GDBUser
#from .models import EmailUserQueue
from .friendlistdb import FriendListDB
from .mongodbfunc import MongoDBFunctions
from .extensions import NeoDB
class FriendCircleHelper:
    # Check and create user accounts if do not exist
    # create friend circles for all emails if not specified

    def create_circles_from_whatsapp(self, user_info, admin_friend_id):
        # user info is a list of dictionaries with each dictionary having email, name, phone_number, gender,
        # user_type etc..
        loutput = []
        friend_hash = {}
        ncounter = 0
        user_email_hash = {}
        user_output = {}
        friend_circle_name = None
        try:
            objMongo = MongoDBFunctions()
            objFriend = FriendListDB()
            objUser = GDBUser()
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            for user in user_info:
                user["admin_friend_id"] = admin_friend_id
                if objUser.get_user_by_phone(user["phone_number"], user_output) :
                    user["linked_status"] = 1 if "user_id" in user_output else 0
                    user["linked_user_id"] = user_output["user_id"] if "linked_user_id" in user_output else None

                    #Below code is need for emailing purposes
                    if "user_id" not in user:
                        user["user_type"] = "New"
                    else:
                        user["user_type"] = "Existing"

                    if objFriend.insert_friend(user, txn, friend_hash):
                        key = admin_friend_id + user.get("phone_number")
                        user_email_hash[user.get("phone_number")] = friend_hash[key]["user_id"]
                        print ("The value of key is", friend_hash.get(key)["user_id"])
                    else:
                        current_app.logger.error("Error in inserting the user" + user["email_address"])
                        txn.rollback()
                        return False
                #    user_email_hash[user.get("phone_number")] = friend_hash.get("user_id")
            friend_circle_name = "Friend circle for"
            for user in user_info:

                print ("Before forming the circle name")
                #friend_circle_name = " " .join(standard_text, member.get("first_name"),  member.get("last_name"))
                friend_circle_id = {}
                if not objFriend.insert_friend_circle(user_email_hash.get(user.get("phone_number")), admin_friend_id,
                                                    friend_circle_name, friend_circle_id, txn):
                    txn.rollback()
                    current_app.logger.error(
                        "Unable to create friend cirlcle for " + admin_friend_id + " " + user.get("phone_number"))
                    return False
                ncounter = 0
                for member in user_info:
                    if user["phone_number"] != member["phone_number"]:
                        print("Contribu values" , user_email_hash.get(member.get("phone_number")), friend_circle_id.get("friend_circle_id"))
                        if not objFriend.add_contributor_to_friend_circle(user_email_hash.get(member.get("phone_number")), admin_friend_id, friend_circle_id.get("friend_circle_id"), txn) :
                            txn.rollback()
                            current_app.logger.error("Error in inserting the friend circle " + user_email_hash.get(member.get("phone_number")))
                            return False
                        email_hash = {}
                        email_hash["referrer_user_id"] = admin_friend_id
                        email_hash["referred_user_id"] = user_email_hash.get(member.get("phone_number"))
                        email_hash["email_address"] = member["email_address"]
                        email_hash["phone_nunber"] = member["phone_number"]
                        email_hash["first_name"] = member["first_name"]
                        email_hash["last_name"] = member["last_name"]
                        email_hash["user_type"] = member["user_type"]
                        email_hash["comm_type"] = "Whatsapp"
                        email_hash["gender"] = member["gender"]
                        email_hash["friend_circle_id"] = friend_circle_id.get("friend_circle_id")
                        if not objMongo.insert_approval_queue(email_hash):
                            txn.rollback()
                            current_app.logger.error("Unable to insert a row into approval queue for " + email_hash["referred_user_id"] )
                            return False
            txn.commit()
            return True
        except Exception as e:
            current_app.logger.error("Error in executing the whatsapp integration" + e)
            print ("I am going mad", e)
            txn.rollback()
            return False

    def connect_circles(self, user_id, list_friend_circle_id):
        return True