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
        counter = 0

        hsh_output["category"] = 0
        hsh_output["subcategory"] = 0

        for interest_id in list_parent:
            if interest_id in hsh_child:

                if hsh_child[interest_id] == "category":
                    hsh_output["category"] = hsh_output["category"] + 1
                else:
                    hsh_output["subcategory"] = hsh_output["subcategory"] + 1
        return False


    def get_match_index(self, user_id, hsh_output):

        obj_gdb_user = GDBUser()
        list_friend_circle_info = []
        list_secret_friend_ids = []
        list_friend_circle_ids = []
        hsh_temp = {}
        hsh_friend_circle_lookup = {}

        # Get all the friend circles that the current user belongs
        if obj_gdb_user.get_friend_circles(user_id, list_friend_circle_info):
            current_app.logger.error("Unable to get friend circle information")
            return False

        for r_friend_circle in list_friend_circle_info:
            if r_friend_circle["friend_circle_id"] not in hsh_temp:
                hsh_temp[r_friend_circle["friend_circle_id"]] = 1
                list_friend_circle_ids.append(r_friend_circle["friend_circle_id"])
                list_secret_friend_ids.append(r_friend_circle["secret_friend_id"])
                hsh_friend_circle_lookup[r_friend_circle["secret_friend_id"]] = r_friend_circle["friend_circle_id"]

        hsh_secret_friend_interests = {}

        # Get the secret friend's personal interest

        if obj_gdb_user.get_personal_interest(list_secret_friend_ids, hsh_secret_friend_interests):
            current_app.logger.error("Unable to get the interests of the secret friends")
            return False

        hsh_friend_circle_interests = []

        # Get all the interests created by the friend circle for the secret friends

        if obj_gdb_user.get_all_interest_data_by_friend_circle(list_friend_circle_ids, hsh_friend_circle_interests):
            current_app.logger.error("Unable to get the interests of all the friend circle the user belongs to")
            return False

        for secret_friend_id in list_secret_friend_ids:
            # get the category interests
            hsh_friend_circle_interests = []
            hsh_current_user = {}
            list_category_interests = list(map(operator.itemgetter("category_id") ,  hsh_secret_friend_interests[secret_friend_id]["category"]))
            list_subcategory_interests = list(
                map(operator.itemgetter("subcategory_id"), hsh_secret_friend_interests[secret_friend_id]["subcategory"]))
            list_interests = []
            list_interests.append(list_category_interests)
            list_interests.append(list_subcategory_interests)
            hsh_friend_circle_user = hsh_friend_circle_interests[hsh_friend_circle_lookup[secret_friend_id]]
            for key, value in hsh_friend_circle_user:
                hsh_friend_circle_interests[hsh_friend_circle_user[key]["category_id"]] = "category"
                hsh_friend_circle_interests[hsh_friend_circle_user][key]["subcategory_id"] = "subcategory"
                if key == user_id:
                    hsh_current_user[hsh_friend_circle_user[key]]["category_id"] = "category"
                    hsh_current_user[hsh_friend_circle_user[key]]["subcategory_id"] = "subcategory"


            hsh_values = {}

            if not self.find_match(list_interests, hsh_friend_circle_interests, hsh_values):
                current_app.logger.error("We have an issue finding the interest match for the secret friend "
                                         "and the corresponding friend circle")
                return False


            friend_circle_cat_match_score = hsh_values["category"]/len(list_category_interests) if hsh_values["category"] > 0 else 0
            friend_circle_scat_match_score = hsh_values["subcategory"]/len(list_subcategory_interests) if hsh_values["subcategory"] > 0 else 0

            total_score = friend_circle_scat_match_score + friend_circle_cat_match_score

            hsh_output[secret_friend_id] = []
            hsh_output[secret_friend_id] = {"friend_circle_score" : total_score}

            hsh_values = {}

            if not self.find_match(list_interests, hsh_current_user, hsh_values):
                current_app.logger.error("We have an issue finding the interest match for the "
                                         "secret friend compared to the current user in the friend circle")
                return False

            current_user_cat_match_score =  hsh_values["category"]/len(list_category_interests) if hsh_values["category"] > 0 else 0
            current_user_scat_match_score = hsh_values["subcategory"] / len(list_subcategory_interests) if hsh_values[
                                                                                                                "subcategory"] > 0 else 0

            total_score = current_user_scat_match_score + current_user_cat_match_score
            hsh_output[secret_friend_id] = {"current_user_score" :  total_score}


            #friend circle and user score compared to secret friend



        hsh_current_user_interests = []

        # Get the interests of the current user
        if obj_gdb_user.get_personal_interest([user_id], hsh_current_user_interests):
            current_app.logger.error("Unable to get the user's personal interests")
            return False

        list_current_friend_circles_attr = []

        # Get the friend circles where the current user is the secret friend

        if obj_gdb_user.get_friend_circle_attributes_by_user_id(user_id, list_current_friend_circles_attr):
            current_app.logger.error("Unable to get all the friend circles where the current user is the secret_friend")
            return False

        hsh_user_secret_friend_interests = {}
        list_current_user_friend_circles = list(map(operator.itemgetter("friend_circle_id"),
                                                    list_current_friend_circles_attr))

        # Get the interests created by the friend circle for the current users.

        if obj_gdb_user.get_all_interest_data_by_friend_circle(list_current_user_friend_circles,
                                                               hsh_user_secret_friend_interests ):
            current_app.logger.error("unable to get the interests added by the friend circle to the current user ")
            return False

        #extract current user interests from the personal interest hash

        list_current_user_cat_interest = list(map(operator.itemgetter("category_id"),hsh_current_user_interests[user_id]["category_id"]))
        list_current_user_subcategory_interest = list(map(operator.itemgetter("subcategory_id"),hsh_current_user_interests[user_id]["subcategory_id"]))

        list_category_interests = list(
            map(operator.itemgetter("category_id"), hsh_current_user_interests[user_id]["category"]))
        list_subcategory_interests = list(
            map(operator.itemgetter("subcategory_id"), hsh_current_user_interests[user_id]["subcategory"]))
        list_interests = []
        list_interests.append(list_category_interests)
        list_interests.append(list_subcategory_interests)

        hsh_friend_circle_interest = {}
        category_match = 0
        subcategory_match = 0

        for friend_circle_id in hsh_user_secret_friend_interests:
            for key, value in hsh_user_secret_friend_interests[friend_circle_id]:
                hsh_values = {}
                hsh_friend_circle_interest[hsh_user_secret_friend_interests[friend_circle_id]["category_id"]] = "category"
                hsh_friend_circle_interest[hsh_user_secret_friend_interests[friend_circle_id]["subcategory_id"]] = "subcategory"

                if self.find_match(list_interests, hsh_friend_circle_interest,hsh_output):
                    current_app.logger.error("Unable to get the match score between the current user "
                                             "and one of the friend circles " + str(friend_circle_id) )
                    return False

                category_match = category_match + hsh_values["category"]
                subcategory_match = subcategory_match + hsh_values["subcategory"]

        cat_match_score = category_match/len(list_category_interests) if hsh_values["category"] > 0 else 0
        scat_match_score = subcategory_match["subcategory"]/len(list_category_interests) if hsh_values["subcategory"] > 0 else 0
        total_score = scat_match_score + cat_match_score
        hsh_values[user_id] = {"current_user_id_match" : total_score}





