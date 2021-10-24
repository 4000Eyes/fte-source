# this class will have all the helper functions for the API classes
# decision should be made later if we have to seperate the helpfer function by API
from flask import current_app
from .gdbmethods import GDBUser
from .models import EmailUserQueue
from .friendlistdb import FriendListDB
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

            objFriend = FriendListDB()
            objUser = GDBUser()
            driver = NeoDB.get_session()
            txn = driver.begin_transaction()
            for user in user_info:
                user["admin_friend_id"] = admin_friend_id
                if objUser.get_user_by_phone(user["phone_number"], user_output) :
                    user["friend_id"] = user_output["user_id"] if "user_id" in friend_hash else None
                    user["linked_status"] = 1 if "user_id" in friend_hash else 0
                    user["linked_user_id"] = user_output["user_id"] if "user_id" in friend_hash else None

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
                """
                ObjUserQueue = EmailUserQueue()
                ObjUserQueue.friend_circle_id = None
                ObjUserQueue.phone_number = user["phone_number"]
                ObjUserQueue.email = user["email_address"] if "email_address" in user else None
                ObjUserQueue.friend_circle_admin_id = admin_friend_id
                ObjUserQueue.referred_user_id = user_email_hash.get("phone_number")
                ObjUserQueue.comm_type = "whatsapp"
                ObjUserQueue.status = 0
                ObjUserQueue.save()
            """
            print ("Before committingt he transaction")
            txn.commit()
            return True
        except Exception as e:
            current_app.logger.error("Error in executing the whatsapp integration" + e)
            print ("I am going mad", e)
            txn.rollback()
            return False

    def connect_circles(self, user_id, list_friend_circle_id):
        return True