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