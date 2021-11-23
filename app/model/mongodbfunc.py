from flask import current_app, g
import pymongo.collection, pymongo.errors
from datetime import datetime, date
import dateutil
class MongoDBFunctions():
    def get_current_date(self):
        today = date.today()
        return today.strftime("%d/%m/%Y")

    def insert_email_queue(self, hshuser):
        try:
            mongo_email_queue_collection = pymongo.collection.Collection(g.db, "email_queue")

            mongo_email_queue_collection.insert_one({"referrer_user_id": hshuser["referrer_user_id"],
                                              "referred_user_id" : hshuser["referred_user_id"],
                                              "email_address": hshuser["email_address"],
                                              "friend_circle_id": hshuser["friend_circle_id"],
                                              "phone_number": hshuser["phone_number"],
                                              "gender" : hshuser["gender"],
                                              "first_name" : hshuser["first_name"],
                                              "last_name" : hshuser["last_name"],
                                              "comm_type" : "email",
                                              "inserted_date" : self.get_current_date(),
                                              "email_status": "N",
                                              "email_sent_date": "TBS"
                                              })

            return True
        except pymongo.errors as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def insert_approval_queue(self, hshuser):
        try:
            if "user_type" not in hshuser and "comm_type" not in hshuser:
                current_app.logger.error("User type is a required key for approval queue. missing for " + hshuser["referred_user_id"])
                return False
            mongo_friend_approval_collection = pymongo.collection.Collection(g.db, "approval_queue")
            mongo_friend_approval_collection.insert_one({"referrer_user_id": hshuser["referrer_user_id"],
                                                         "referred_user_id": hshuser["referred_user_id"],
                                                         "email_address": hshuser["email_address"],
                                                         "friend_circle_id": hshuser["friend_circle_id"],
                                                         "phone_number": hshuser["phone_number"],
                                                         "gender": hshuser["gender"],
                                                         "first_name": hshuser["first_name"],
                                                         "last_name": hshuser["last_name"],
                                                         "user_type" : hshuser["user_type"],
                                                         "comm_type" : hshuser["comm_type"],
                                                         "inserted_dt" : self.get_current_date(),
                                                         "registration_complete_status": 0,
                                                         "approved_flag" : 0,
                                                         "approved_dt": "TBS"
                                                         })
            return True
        except pymongo.errors as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False


