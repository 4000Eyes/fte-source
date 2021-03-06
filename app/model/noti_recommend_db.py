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

    def friend_circle_with_no_occasion(self, l_friend_circle, list_output):
        try:
            obj_gdb = GDBUser()
            driver = NeoDB.get_session()

            no_occasion_query = "match (u:User)-[:CIRCLE_CREATOR]->(fc:friend_circle) " \
                                "where not exists {(u:User)-[x:OCCASION]->(friend_occasion)}  and " \
                                "fc.friend_circle_id in $l_friend_circle_ " \
                                "return " \
                                "fc.friend_circle_id as friend_circle_id, " \
                                "fc.secret_friend_id as secret_friend_id, " \
                                "fc.secret_friend_name as secret_first_name," \
                                "fc.secret_last_name as secret_last_name," \
                                 "duration.inDays(date(datetime({epochmillis: apoc.date.parse(fc.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')})), date()).days as days_since "

            result = driver.run(no_occasion_query, l_friend_circle_=l_friend_circle)
            for record in result:
                list_output.append(record.data())

            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("The error is " + str(e))
            return False
        except Exception as e:
            current_app.logger.error("The error is " + str(e))
            return False

    def days_since_last_occasion(self, l_friend_circle, list_output):
        try:
            obj_gdb = GDBUser()
            driver = NeoDB.get_session()
            days_since_occasion_query = " match (fc:friend_occasion) , (f:friend_circle) " \
                                        "where f.friend_circle_id = fc.friend_circle_id and " \
                                        " fc.friend_circle_id in $l_friend_circle_ and " \
                                        "duration.inDays(date(datetime({epochmillis: apoc.date.parse(fc.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')})), date()).days >= 30 " \
                                        "return " \
                                        "max(fc.created_dt) as last_created_date, " \
                                        "fc.friend_circle_id as friend_circle_id, " \
                                        "duration.inDays(date(datetime({epochmillis: apoc.date.parse(fc.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')})), date()).days as days_since, " \
                                        "f.secret_friend_name as secret_first_name, " \
                                        "f.secret_last_name as secret_last_name," \
                                        "f.secret_friend_id as secret_friend_id" \

            result = driver.run(days_since_occasion_query,  l_friend_circle_=l_friend_circle)

            for record in result:
                list_output.append(record.data())

            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("The error is " + str(e))
            return False
        except Exception as e:
            current_app.logger.error("The error is " + str(e))
            return False

    def get_no_interest_users(self, user_id, l_friend_circle, list_output: list):
        try:
            obj_gdb = GDBUser()
            driver = NeoDB.get_session()


            # query = " match(fc: friend_circle ) " \
            #         " WHERE fc.friend_circle_id in $l_friend_circle_id_ " \
            #         " call " \
            #         "{  with fc optional match (a:User)-[r:INTEREST]-() " \
            #         " where " \
            #         " a.user_id = $user_id_ and fc.friend_circle_id = r.friend_circle_id " \
            #         "return a.user_id as user_id, r.friend_circle_id as fs, count(r.friend_circle_id) as " \
            #         "interest_count " \
            #         "}" \
            #         " return fc.friend_circle_id as friend_circle_id, user_id, interest_count, 0 as flag, " \
            #         " duration.inDays(date(datetime({epochmillis: apoc.date.parse(fc.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')})), date()).days as days_since "

            query = " match(fc: friend_circle ) " \
                    " WHERE fc.friend_circle_id in $l_friend_circle_id_ and " \
                    " not exists {(a:User)-[r:INTEREST]->()}" \
                    " return fc.friend_circle_id as friend_circle_id," \
                    " fc.creator_id as user_id,  " \
                    " 0 as flag, " \
                    " duration.inDays(date(datetime({epochmillis: apoc.date.parse(fc.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')})), date()).days as days_since," \
                    " fc.secret_friend_id as secret_friend_id," \
                    " fc.secret_friend_name as secret_first_name," \
                    " fc.secret_last_name as secret_last_name "

            result = driver.run(query, user_id_=user_id, l_friend_circle_id_=l_friend_circle)

            for record in result:
                list_output.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("The error is " + str(e))
            return False
        except Exception as e:
            current_app.logger.error("The error is " + str(e))
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
                    " n.secret_friend_name as secret_first_name," \
                    " n.secret_last_name as secret_last_name, " \
                    "  x.user_id as user_id, " \
                    " duration.inDays(date(datetime({epochmillis: apoc.date.parse(n.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')})), date()).days as days_since," \
                    " 0 as flag "
            result = driver.run(query, user_id_=user_id, l_friend_circle_=l_friend_circle)

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
                    " AND fc.friend_circle_id in $l_friend_circle_ " \
                    " return distinct " \
                    " duration.inDays(date(datetime({epochmillis: apoc.date.parse(r.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')})), date()).days as days_since, " \
                    " u.user_id as user_id, " \
                    " u.first_name as first_name ," \
                    " u.last_name as last_name," \
                    " r.friend_circle_id as friend_circle_id," \
                    " fc.secret_friend_name as secret_first_name, " \
                    " fc.secret_last_name as secret_last_name, " \
                    " fc.secret_friend_id as secret_friend_id ," \
                    " 1 as flag"

            # " max(date(datetime({epochmillis: apoc.date.parse(r.created_dt, 'ms', 'dd/MM/yyyy HH:mm:ss')}))) as xdate," \

            result = driver.run(query, interest_reminder_days_=int(interest_reminder_days), user_id_=user_id,
                                l_friend_circle_=l_friend_circle)
            for record in result:
                list_output.append(record.data())
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("The error is " + str(e))
            return False
        except Exception as e:
            current_app.logger.error("The error is " + str(e))
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
