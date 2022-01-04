import json
import os

import neo4j.exceptions
import logging
from flask import current_app, g
from .extensions import NeoDB
import uuid
from flask_restful import Resource
import pymongo.collection
from datetime import datetime
from pymongo import errors
from app.model.gdbmethods import GDBUser


class NotificationAndRecommendationDB(Resource):
    def get_occasion_reminder(self, user_id, list_output):
        try:
            notification_collection = pymongo.collection.Collection(g.db, "birthday_reminder")
            result = notification_collection.find({"$or": [{"creator_user_id": user_id},
                                                            {"secret_linked_user_id": user_id},
                                                            {"secret_user_id": user_id}]},
                                                   {"status": 0})

            if result is not None:
                for row in result:
                    list_output.append(row)
            return True
        except pymongo.errors.PyMongoError as e:
            current_app.logger.error("The exception is" + str(e))
            return False
        except Exception as e:
            current_app.logger.error("The exception is" + str(e))
            list_output = None
            return False

    def get_relationship_status(self, user_id, l_friend_circle, list_output):
        try:
            obj_gdb = GDBUser()
            driver = NeoDB.get_session()

            query = "match (x:User{user_id:$user_id_}),(n:friend_circle) " \
                    "where not exists ((x)-[:RELATION]->(n)) " \
                    "and n.friend_circle_id in $l_friend_circle_ " \
                    " return n.friend_circle_id as friend_circle_id, " \
                    " n.secret_friend_id as secret_friend_id," \
                    " n.secret_first_name as secret_first_name," \
                    " n.secret_last_name as secret_last_name, " \
                    "  x.user_id as user_id "
            result = driver.run(query,  user_id_ = user_id , l_friend_circle_ = l_friend_circle)
            for record in result:
                list_output.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("The error is " + e)
            return False
        except Exception as e:
            current_app.logger.error("The error is " + e)
            return False

    def get_interest_reminders(self, user_id, l_friend_circle, list_output):
        try:
            driver = NeoDB.get_session()
            interest_reminder_days = 2
            interest_reminder_days = os.environ.get("INTEREST_REMINDER_DAYS")
            query = "MATCH (u:User)-[r:INTEREST]->(w:WebCat), (fc:friend_circle)" \
                    " WHERE duration.inDays(date(datetime({epochmillis: apoc.date.parse(r.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')})), date()).days >= $interest_reminder_days_ " \
                    " AND r.friend_circle_id = fc.friend_circle_id " \
                    " AND u.user_id = $user_id_" \
                    " AND fc.friend_circle_id in $l_friend_circle_" \
                    " return " \
                    " max(date(datetime({epochmillis: apoc.date.parse(r.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')}))) as xdate," \
                    " u.user_id as user_id, " \
                    " u.first_name as first_name ," \
                    " u.last_name as last_name," \
                    " r.friend_circle_id as fci," \
                    " fc.secret_first_name as secret_first_name, " \
                    " fc.secret_last_name as secret_last_name, " \
                    " fc.secret_friend_id as secret_friend_id "
            result = driver.run(query, interest_reminder_days_ = interest_reminder_days, user_id_ = user_id , l_friend_circle_ = l_friend_circle)
            for record in result:
                list_output.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("The error is " + e)
            return False
        except Exception as e:
            current_app.logger.error("The error is " + e)
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
            result = product_collection.find({"approval_status": "N"})
            for row in result:
                loutput.append(row)
            return True
        except Exception as e:
            return False
