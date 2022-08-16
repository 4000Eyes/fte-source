from flask import current_app, g
import pymongo
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
                                                     "referred_user_id": hshuser["referred_user_id"],
                                                     "email_address": hshuser["email_address"],
                                                     "friend_circle_id": hshuser["friend_circle_id"],
                                                     "phone_number": hshuser["phone_number"],
                                                     "gender": hshuser["gender"],
                                                     "first_name": hshuser["first_name"],
                                                     "last_name": hshuser["last_name"],
                                                     "comm_type": "email",
                                                     "inserted_date": self.get_current_date(),
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
                current_app.logger.error(
                    "User type is a required key for approval queue. missing for " + hshuser["referred_user_id"])
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
                                                         "user_type": hshuser["user_type"],
                                                         "comm_type": hshuser["comm_type"],
                                                         "inserted_dt": self.get_current_date(),
                                                         "registration_complete_status": 0,
                                                         "approved_flag": 0,
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

    def insert_user(self, user_hash):
        #mage chagnes on 01/13/2022 to add age to mongo db
        try:
            if "user_id" not in user_hash or "email_address" not in user_hash or "phone_number"  not in user_hash:
                current_app.logger.error("Required field either user id or email address is missing")
                return False
            user_hash["password"] = user_hash["password"] if "password" in user_hash else None
            user_hash["phone_number"] = user_hash["phone_number"] if "phone_number" in user_hash else None
            user_hash["gender"] = user_hash["gender"] if "gender" in user_hash else None
            user_hash["first_name"] = user_hash["first_name"] if "first_name" in user_hash else None
            user_hash["last_name"] = user_hash["last_name"] if "last_name" in user_hash else None
            user_hash["user_type"] = user_hash["user_type"] if "user_type" in user_hash else None
            user_hash["location"] = user_hash["location"] if "location" in user_hash else None
            user_hash["age"] = user_hash["age"] if "age" in user_hash else None
            user_hash["referrer_user_id"] = user_hash["referrer_user_id"] if "referrer_user_id" in user_hash else None

            mongo_user_collection = pymongo.collection.Collection(g.db, "user")
            full_name = None

            result = mongo_user_collection.find_one({"$or":[{"user_id": user_hash.get("user_id")},{"phone_number":user_hash.get("phone_number")}]})
            if result is None:
                full_name = str(user_hash.get("first_name")) + " " + str(user_hash.get("last_name"))
                mongo_user_collection.insert_one({"user_id": user_hash.get("user_id"),
                                                  "email": user_hash.get("email_address"),
                                                  "password": user_hash.get("password"),
                                                  "phone_number": user_hash.get("phone_number"),
                                                  "gender": user_hash.get("gender"),
                                                  "age": user_hash.get("age"),
                                                  "first_name": user_hash.get("first_name"),
                                                  "last_name": user_hash.get("last_name"),
                                                  "full_name": full_name,
                                                  "location": user_hash.get("location"),
                                                  "user_type": user_hash.get("user_type"),
                                                  "referrer_user_id" : user_hash.get("referrer_user_id")
                                                  })
                return True
            else:
                current_app.logger.info("User already exists" + str(user_hash.get("user_id")) + " " + str(user_hash.get("email_address")))
                return True
        except pymongo.errors as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False

    def get_interest(self, age, gender, page_size, page_number, list_output):
        try:
            interest_collection = pymongo.collection.Collection(g.db, "interests")
            skip_size = page_size * ( page_number - 1 )
            """
            result = interest_collection.find(
                {"age_lo": {"$gte", age_lo}},
                {"age_hi": {"$lte", age_hi}},
                {"gender": gender}).skip(int(skip_size)).limit(page_size)
            """
            if age > 0 and gender is not None:
                result = interest_collection.aggregate([{"$match": {"$and":[{"age_lo": {"$lte" : age}},
                                                            {"age_hi": {"$gte" : age}}, {"gender": gender}]}},
                                                     {"$project": {"interest_id": 1, "interest_name": 1, "image_url":1}},
                                                     {"$sort": {"interest_id": pymongo.ASCENDING}},
                                                    {"$skip": skip_size},
                                                        {"$limit": page_size}
                                                     ])

            else:
                result = interest_collection.aggregate([
                                                     {"$project": {"interest_id": 1, "interest_name": 1, "image_url":1}},
                                                     {"$sort": {"interest_id": pymongo.ASCENDING}},
                                                    {"$skip": skip_size},
                                                        {"$limit": page_size}
                                                     ])
            for row in result:
                list_output.append({"interest_id":row["interest_id"], "interest_name":row["interest_name"], "image_url":row["image_url"]})
            return True
        except Exception as e:
            current_app.logger.error(e)
            print("The error is ", e)
            return False


