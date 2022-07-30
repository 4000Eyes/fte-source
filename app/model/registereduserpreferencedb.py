import json
import operator

import neo4j.exceptions
import logging
from flask import current_app, g
from flask_restful import Resource
from app.model.gdbmethods import GDBUser
from .extensions import NeoDB
import uuid
from app.model.mongodbfunc import MongoDBFunctions
import pymongo.collection
from datetime import datetime
from pymongo import errors
import collections
import pytz
from datetime import datetime, tzinfo, timedelta
from dateutil.relativedelta import relativedelta
import re


class RegisteredUserPreferenceDB(Resource):

    def find_match(self, list_parent, hsh_child, hsh_output):
        try:
            counter = 0
            hsh_output["category"] = 0
            hsh_output["subcategory"] = 0

            for interest_id in list_parent:
                if interest_id in hsh_child:
                    if hsh_child[interest_id] == "category":
                        hsh_output["category"] = hsh_output["category"] + 1
                    else:
                        hsh_output["subcategory"] = hsh_output["subcategory"] + 1
            return True
        except Exception as e:
            current_app.logger.error("Unable to perform matching" +  str(e))
            return False


    def get_match_index(self, user_id, list_output):

        try:

            obj_gdb_user = GDBUser()
            list_friend_circle_info = []
            list_secret_friend_ids = []
            list_friend_circle_ids = []
            hsh_temp = {}
            hsh_friend_circle_lookup = {}
            hsh_friend_circle_attr = {}
            list_friend_circle_output = []
            # Get all the friend circles that the current user belongs
            if not obj_gdb_user.get_friend_circles(user_id, list_friend_circle_info):
                current_app.logger.error("Unable to get friend circle information")
                return False

            for r_friend_circle in list_friend_circle_info:
                if r_friend_circle["friend_circle_id"] not in hsh_temp:
                    hsh_temp[r_friend_circle["friend_circle_id"]] = 1
                    list_friend_circle_ids.append(r_friend_circle["friend_circle_id"])
                    list_secret_friend_ids.append(r_friend_circle["secret_friend_id"])
                    hsh_friend_circle_lookup[r_friend_circle["secret_friend_id"]] = r_friend_circle["friend_circle_id"]
                    hsh_friend_circle_attr[r_friend_circle["secret_friend_id"]] = [r_friend_circle["friend_circle_name"] , r_friend_circle["image_url"]]

            hsh_secret_friend_interests = {}

            # Get the secret friend's personal interest

            if not obj_gdb_user.get_personal_interest(list_secret_friend_ids, hsh_secret_friend_interests):
                current_app.logger.error("Unable to get the interests of the secret friends")
                return False

            hsh_friend_circle_interests = {}

            # Get all the interests created by the friend circle for the secret friends

            if not obj_gdb_user.get_all_interest_data_by_friend_circle(list_friend_circle_ids, hsh_friend_circle_interests):
                current_app.logger.error("Unable to get the interests of all the friend circle the user belongs to")
                return False

            for secret_friend_id in list_secret_friend_ids:
                # get the category interests

                if secret_friend_id not in hsh_secret_friend_interests:
                    continue
                list_category_interests = list(map(operator.itemgetter("interest_id"),  hsh_secret_friend_interests[secret_friend_id]["category"]))
                list_subcategory_interests = list(
                    map(operator.itemgetter("interest_id"), hsh_secret_friend_interests[secret_friend_id]["subcategory"]))
                list_interests = []
                list_interests = list_category_interests + list_subcategory_interests

                hsh_friend_circle_user = hsh_friend_circle_interests[hsh_friend_circle_lookup[secret_friend_id]]
                for key in hsh_friend_circle_user:
                    hsh_friend_circle_user_interests = {}
                    hsh_current_user = {}
                    for item in hsh_friend_circle_user[key]:
                        if item["interest_type"] == "category":
                            hsh_friend_circle_user_interests[item["interest_id"]] = "category"
                        else:
                            hsh_friend_circle_user_interests[item["interest_id"]] = "subcategory"
                        if key == user_id:
                            if item["interest_type"] == "category":
                                hsh_current_user[item["interest_id"]] = "category"
                            else:
                                hsh_current_user[item["interest_id"]] = "subcategory"

                    hsh_values = {}

                    if not self.find_match(list_interests, hsh_friend_circle_user_interests, hsh_values):
                        current_app.logger.error("We have an issue finding the interest match for the secret friend "
                                                 "and the corresponding friend circle")
                        return False

                    friend_circle_cat_match_score = hsh_values["category"]/len(list_category_interests) if hsh_values["category"] > 0 else 0
                    friend_circle_scat_match_score = hsh_values["subcategory"]/len(list_subcategory_interests) if hsh_values["subcategory"] > 0 else 0
                    total_friend_circle_score = friend_circle_scat_match_score + friend_circle_cat_match_score


                hsh_values = {}

                if not self.find_match(list_interests, hsh_current_user, hsh_values):
                    current_app.logger.error("We have an issue finding the interest match for the "
                                             "secret friend compared to the current user in the friend circle")
                    return False

                current_user_cat_match_score =  hsh_values["category"]/len(list_category_interests) if hsh_values["category"] > 0 else 0
                current_user_scat_match_score = hsh_values["subcategory"] / len(list_subcategory_interests) if hsh_values[
                                                                                                                    "subcategory"] > 0 else 0

                total_current_user_score = current_user_scat_match_score + current_user_cat_match_score
                hshscoremessage = {}
                self.set_match_score_message(total_current_user_score, hshscoremessage)
                list_friend_circle_output.append({"friend_circle_id":hsh_friend_circle_lookup[secret_friend_id],
                                    "friend_circle_name": hsh_friend_circle_attr[secret_friend_id][0],
                                    "image_url": hsh_friend_circle_attr[secret_friend_id][1],
                                    "friend_circle_score": float(total_friend_circle_score),
                                    "current_user_score": float(total_current_user_score),
                                    "message": hshscoremessage["message"],
                                    "info_type": "friend_circle_id"})

                total_current_user_score = 0
                total_friend_circle_score = 0
                #friend circle and user score compared to secret friend

            list_output.append({"friend_circle_score": list_friend_circle_output})
            hsh_current_user_interests = {}

            # Get the interests of the current user
            if not obj_gdb_user.get_personal_interest([user_id], hsh_current_user_interests):
                current_app.logger.error("Unable to get the user's personal interests")
                return False

            list_current_friend_circles_attr = []

            # Get the friend circles where the current user is the secret friend

            if not obj_gdb_user.get_friend_circle_attributes_by_user_id(user_id, list_current_friend_circles_attr):
                current_app.logger.error("Unable to get all the friend circles where the current user is the secret_friend")
                return False

            if len(list_current_friend_circles_attr) <= 0:
                current_app.logger.error("The current user is not secret friend to any one")
                list_output.append({"current_userscore":[{"user_id": user_id,"info_type": "current_user", "current_user_id_match": "N/A"}]})
                return True

            hsh_user_secret_friend_interests = {}
            list_current_user_friend_circles = list(map(operator.itemgetter("friend_circle_id"),
                                                        list_current_friend_circles_attr))

            # Get the interests created by the friend circle for the current users.

            if not obj_gdb_user.get_all_interest_data_by_friend_circle(list_current_user_friend_circles,
                                                                   hsh_user_secret_friend_interests ):
                current_app.logger.error("unable to get the interests added by the friend circle to the current user ")
                return False

            #extract current user interests from the personal interest hash

            list_category_interests = list(
                map(operator.itemgetter("interest_id"), hsh_current_user_interests[user_id]["category"]))
            list_subcategory_interests = list(
                 map(operator.itemgetter("interest_id"), hsh_current_user_interests[user_id]["subcategory"]))
            list_interests = list_category_interests + list_subcategory_interests

            hsh_current_user_friend_circle_interest = {}
            category_match = 0
            subcategory_match = 0

            for friend_circle_id in hsh_user_secret_friend_interests:
                for key in hsh_user_secret_friend_interests[friend_circle_id]:
                    for item in hsh_user_secret_friend_interests[friend_circle_id][key]:
                        hsh_values = {}
                        if item["interest_type"] == "category":
                            hsh_current_user_friend_circle_interest["interest_id"] = "category"
                        else:
                            hsh_current_user_friend_circle_interest["interest_id"] = "subcategory"

                    if not self.find_match(list_interests, hsh_current_user_friend_circle_interest,hsh_values):
                        current_app.logger.error("Unable to get the match score between the current user "
                                                 "and one of the friend circles " + str(friend_circle_id) )
                        return False

                    category_match = category_match + int(hsh_values["category"])
                    subcategory_match = subcategory_match + int(hsh_values["subcategory"])

            cat_match_score = category_match/len(list_category_interests) if hsh_values["category"] > 0 else 0
            scat_match_score = subcategory_match["subcategory"]/len(list_category_interests) if hsh_values["subcategory"] > 0 else 0
            total_score = scat_match_score + cat_match_score


            list_output.append({"current_user_score": [{"user_id": user_id, "current_user_id_match" : total_score,
                                                  "info_type": "current_user"}]})

            return True
        except Exception as e:
            current_app.logger.error("There is an issue calculating match score " + str(e))
            return False





    def set_match_score_message(self, score, hshoutput):

        if float(score) >= 0 and float(score) <= 25:
            hshoutput["message"] = "Keep Learning"
        elif float(score) >= 26 and float(score) <= 50:
            hshoutput["message"] = "Getting there"
        elif float(score) >= 51 and float(score) <= 75:
            hshoutput["message"] = "Good Job. Keep Going!"
        elif float(score) >= 76:
            hshoutput["message"] = "You are the best"
        else:
            hshoutput["message"] = None