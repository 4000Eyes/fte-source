import json
import neo4j.exceptions
import logging
from flask import current_app, g
from .extensions import NeoDB
import uuid
from flask_restful import Resource
import pymongo.collection
from datetime import datetime
from pymongo import errors

class NotificationAndRecommendationDB(Resource):
    def get_reminder(self, user_id, list_output):
        try:
            list_output = []
            notification_collection = pymongo.collection.Collection(g.db, "notification_and_recommendation")
            result = notification_collection.find_one({"user_id":user_id}).sort({"entered_dt":1})
            for row in result:
                list_output.append(row)
        except pymongo.errors.PyMongoError as e:
            list_output = None
            return False
        except Exception as e:
            list_output = None
            return False

    def new_user_recommendation(self):
        try:
            return True
        except pymongo.errors.PyMongoError as e:
            return False

    def get_home_page_metrics(self, hshoutput):
        try:
            # Total gifts
            product_collection = pymongo.collection.Collection(g.db, "gemift_product_db")
            result = product_collection.find().count()
            for row in result:
                hshoutput["gift_count"] = result
            # total circles

            driver = NeoDB.get_session()
            query = "MATCH (u:friend_circle) " \
                    " return count(u) as circle_count"
            result = driver.run(query)
            record = result.single()
            if record is not None:
                hshoutput["friend_circle_count"] = result["circle_count"]
            else:
                hshoutput["friend_circle_count"] = 0

            # total interests
            user_count = "MATCH ()-[r:INTEREST{r:friend_circle_id}->() " \
                         " return count(r.friend_circle_id) as interest_count"
            result = driver.run(query)
            record = result.single()
            if record is not None:
                hshoutput["interest_count"] = result["interest_count"]
            else:
                hshoutput["interest_count"] = 0

            # total users

            user_count = "MATCH (u:user) " \
                        " return count(u) as user_count"
            result = driver.run(query)
            record = result.single()
            if record is not None:
                hshoutput["user_count"] = result["user_count"]
            else:
                hshoutput["user_count"] = 0

            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False
        except Exception as e:
            return False

    def get_testmonials(self, loutput):
        try:
            return True
        except Exception as e:
            return False

    def get_approval_requests(self, user_id, loutput):
        try:
            product_collection = pymongo.collection.Collection(g.db, "approval_queue")
            result = product_collection.find({"approval_status" : "N"})
            for row in result:
                loutput.append(result)
            update_result = product_collection.update_one({"approval_status" : "Y"})
            return True
        except Exception as e:
            return False