from flask import Response, request, current_app
from ..model.gdbmethods import GDBUser, UserProductDBManagement
from flask_restful import Resource
from .classhelper import ManageFriendCircleHelper
import datetime
import json
from ..model.ftesearch import FTESearch


class UserProductManagement:
    def post(self):
        content = request.get_json()
        friend_circle_id = content["friend_circle_id"]
        product_id = content["product_id"]
        price_lower_bound = content["price_lower_bound"]
        price_upper_bound = content["price_upper_bound"]
        category_id = content["category_id"]

        return True

    def get(self):
        return True

    def update(self):
        return True

    def delete(self):
        return True


class UserProductManagementHelper:
    def search_product_by_friend_circle_id(self, friend_circle_id, product_id, category_id, location_id, loutput):
        output = []
        objUserProductDB = UserProductDBManagement()

        if friend_circle_id is None:
            print ("The friend circle id cannot be Null")
            return False
            # get the secret friend attributes
        objGDBUser = GDBUser()

        if not objGDBUser.get_friend_circle_info(friend_circle_id, output) or output["user_id"] is None:
            print ("There is an issue getting information about the secret friend or the friend doesnt exist anymore")
            return False

        # Get secret friend interest
        loutput_interest = []
        if not objGDBUser.get_interest_by_friend_circle(friend_circle_id, loutput_interest):
            print ("Error in extracting the interests for secret friend")
            return False
        for row in loutput_interest:
            category_list = category_list + row["category_id"]

        if output["user_age"]:
            user_age_lo = int(output["user_age"]) - int(output["user_age"]) % 15
            user_age_hi = user_age_lo + 15

        if objUserProductDB.get_product(
                product_id,
                category_id,
                location_id,
                output["user_id"],
                user_age_lo,
                user_age_hi,
                output["user_location"],
                loutput):
            print ("Successfully retrieved the product selection")
            return True
        return True

    def search_product(self):
        # NOTE: BROAD SEARCH WITHOUT THE FRIEND CIRCLE ID WILL NOT BE AVAILABLE IN V1
        return True


# structure of tagged products
#  (n:tagged_product {product_id: XYZ, tagged_category: ["x', "y", "z"], "last_updated_date": "dd-mon-yyyy",
#  "location":"country", "age_lower" : 45, "age_upper": 43, "price_upper": 3, "price_lower": 54, "gender": "M",
#  "color": [red,blue, white], "category_relevance" : [3,4,5], uniqueness index: [1..10]
# )

# required functions for this API
# List all products for a given circle id
# Get all products by price range
# Get all products by category

# how should search condition pruning should work.
# unless it is a product specific search
# location is key
# if the result is 0 expand to other neighbouring categories (may be by relevance)
#


# USERS DYNAMICALLY ADDING INTEREST FOR SECRET FRIENDS WILL NOT BE SUPPORTED IN V1
