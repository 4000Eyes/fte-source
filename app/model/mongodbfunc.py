from flask import current_app, g
import pymongo.collection, pymongo.errors

class MongoDBFunctions():
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
                                              "comm_type" : "email"
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
            mongo_friend_approval_collection = pymongo.collection.Collection(g.db, "approval_queue")
            mongo_friend_approval_collection.insert_one({"referrer_user_id": hshuser["referrer_user_id"],
                                                         "referred_user_id": hshuser["referred_user_id"],
                                                         "email_address": hshuser["email_address"],
                                                         "friend_circle_id": hshuser["friend_circle_id"],
                                                         "phone_number": hshuser["phone_number"],
                                                         "gender": hshuser["gender"],
                                                         "first_name": hshuser["first_name"],
                                                         "last_name": hshuser["last_name"],
                                                         "registration_complete_status": 0
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


    def insert_user(self, hshuser):
        return True