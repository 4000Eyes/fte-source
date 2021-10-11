# this class will have all the helper functions for the API classes
# decision should be made later if we have to seperate the helpfer function by API
from flask import current_app
from model.gdbmethods import GDBUser
from model.models import EmailUserQueue
from model.friendlistdb import FriendListDB
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
        try:
            objFriend = FriendListDB()
            objUser = GDBUser()
            for user in user_info:
                if objUser.get_user_by_phone(user_info["phone_number"], user_output) :
                    friend_hash["friend_id"] = user_output["user_id"] if "user_id" in friend_hash else None
                    friend_hash["linked_status"] = 1 if "user_id" in friend_hash else 0
                    friend_hash["linked_user_id"] = user_output["user_id"] if "user_id" in friend_hash else None
                    if objFriend.insert_friend(user, friend_hash):
                        user_email_hash[user_info.get("phone_number")] = friend_hash.get("user_id")
                    else:
                        current_app.logger.error("Error in inserting the user" + user["email_address"])
                        return False

                    user_email_hash[user_info.get("phone_number")] = friend_hash.get("user_id")

            for user in user_info:
                friend_circle_id = []
                ncounter = 0
                for member in user_info:
                    if not ncounter:
                        friend_circle_name = "Friend Circle for " + member["name"]
                        if not objUser.insert_friend_circle(admin_friend_id, user_email_hash.get(member["phone_number"]), friend_circle_name, friend_circle_id):
                            current_app.logger.error("Unable to create friend cirlcle for " + admin_friend_id + " " + member["phone_number"])
                            return False
                        ncounter = 1
                    if user["phone_number"] != member["phone_number"]:
                        if not objUser.add_contributor_to_friend_circle(user_email_hash.get(member["phone_number"]), friend_circle_id[0]) :
                            current_app.logger.error("Error in inserting the friend circle " + user_email_hash.get(member["phone_number"]))
                            return False
                ObjUserQueue = EmailUserQueue()
                ObjUserQueue.friend_circle_id = None
                ObjUserQueue.phone_number = user["phone_number"]
                ObjUserQueue.email = user["email_address"] if "email_address" in user else None
                ObjUserQueue.friend_circle_admin_id = admin_friend_id
                ObjUserQueue.referred_user_id = user_email_hash.get("phone_number")
                ObjUserQueue.comm_type = "whatsapp"
                ObjUserQueue.status = 0
                ObjUserQueue.save()
            return True
        except Exception as e:
            current_app.logger.error("Error in executing the whatsapp integration" + e)
            return False

    def connect_circles(self, user_id, list_friend_circle_id):
        return True