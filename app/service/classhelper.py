# this class will have all the helper functions for the API classes
# decision should be made later if we have to seperate the helpfer function by API

from model.gdbmethods import GDBUser

class ManageFriendCircleHelper:
    def refer_friend_to_circle(self, creator_user_id, friend_circle_id, referrer_user_id, referred_email_address, referred_name, objGDBUser):
        # Check if the referrer is part of the friend circle
        # Check if the referred is not the secret friend
        # if the referrer is admin insert the request to the email table.
        loutput1 = []
        loutput2 = []
        if objGDBUser.check_user_in_friend_circle(friend_circle_id, creator_user_id, loutput1) and objGDBUser.check_user_in_friend_circle(friend_circle_id, referrer_user_id, loutput2) :
            if loutput2[0] <= 0 and loutput1[0] <= 0 :
                if objGDBUser.check_user_is_secret_friend(friend_circle_id, referred_email_address, loutput1):
                    if loutput1[0] <= 0:
                        print ("The referred user is valid and an invitation will be sent")
                        return True
        return True
