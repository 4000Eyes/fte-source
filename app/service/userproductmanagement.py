import re

from flask import Response, request, current_app, jsonify, json
from app.model.searchdb import SearchDB
from app.model.gdbmethods import GDBUser
from app.model.friendlistdb import FriendListDB
from app.service.general import SiteGeneralFunctions
from flask_restful import Resource
from bson import json_util, ObjectId
from flask_jwt_extended import jwt_required
import datetime
import json


class UserProductManagement(Resource):
    def get(self):
        try:
            content = {}
            content["request_id"] = request.args.get("request_id", type=int)
            content["price_to"] = request.args.get("price_to", type=float)
            content["price_from"] = request.args.get("price_from", type=float)
            content["product_id"] = request.args.getlist("product_id", type=str)
            content["sort_order"] = request.args.get("sort_order", type=str)
            content["friend_circle_id"] = request.args.get("friend_circle_id", type=str)
            content["user_id"] = request.args.get("user_id", type=str)
            content["occasion_name"] = request.args.get("occasion_name", type=str)
            content["occasion_year"] = request.args.get("occasion_year", type=str)
            content["occasion_id"] = request.args.get("occasion_id", type=str)
            content["comment"] = request.args.get("comment", type=str)
            content["vote"] = request.args.get("vote", type=int)
            content["age"] = request.args.get("age", type=int)
            content["color"] = request.args.getlist("color_list")
            raw_occasion_list = request.args.getlist("occasion_list")
            content["page_size"] = request.args.get("page_size", type=int)
            content["page_number"] = request.args.get("page_number", type=int)

            print("The values are", content["request_id"], content["product_id"])

            if "request_id" not in content:
                return {"Error": "Input argument (request_id) is empty"}, 400

            # #regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')
            # regex = re.compile('()')
            is_weird = 0
            output_list = []
            objSearch = SearchDB()
            objGDBUser = GDBUser()
            objFriend = FriendListDB()
            if content["request_id"] == 1:
                sstr = "("
                is_weird = 0
                for occ in raw_occasion_list:
                    if str(occ).find(sstr) >= 0:
                        occ = occ.replace('(', " ")
                        occ = occ.replace(')', " ")
                        occ = occ.replace('"', " ")
                        occ = occ.replace(" ", "")
                        content["occasion"] = list(occ.split(","))
                        is_weird = 1
                if not is_weird:
                    content["occasion"] = raw_occasion_list

                if len(raw_occasion_list) <= 0:
                    objGDBUser = GDBUser()
                    list_occasion_name = []
                    if not objGDBUser.get_occasion_names(list_occasion_name, content["friend_circle_id"]):
                        current_app.logger.error("Unable to get the occasion names for this occasion id")
                        return False
                    if len(list_occasion_name) > 0:
                        content["occasion"] = []
                        for occasion_row in list_occasion_name:
                            content["occasion"].append(occasion_row["occasion_id"])

                if "occasion" not in content or len(content["occasion"]) <= 0:
                    current_app.logger.error("There are no occasion")
                    return False

                is_weird = 0
                raw_category_list = request.args.getlist("category_list")
                for occ in raw_category_list:
                    if str(occ).find(sstr) >= 0:
                        occ = occ.replace('(', " ")
                        occ = occ.replace(')', " ")
                        occ = occ.replace('"', " ")
                        occ = occ.replace(" ", "")
                        content["category"] = list(occ.split(","))
                        is_weird = 1
                if not is_weird:
                    content["category"] = raw_category_list

                is_weird = 0
                raw_subcategory_list = request.args.getlist("subcategory_list")
                for occ in raw_subcategory_list:
                    if str(occ).find(sstr) >= 0:
                        occ = occ.replace('(', " ")
                        occ = occ.replace(')', " ")
                        occ = occ.replace('"', " ")
                        occ = occ.replace(" ", "")
                        content["subcategory"] = list(occ.split(","))
                        is_weird = 1
                if not is_weird:
                    content["subcategory"] = raw_subcategory_list

                is_weird = 0
                raw_gender_list = request.args.getlist("gender_list")
                for occ in raw_gender_list:
                    if str(occ).find(sstr) >= 0:
                        occ = occ.replace('(', " ")
                        occ = occ.replace(')', " ")
                        occ = occ.replace('"', " ")
                        occ = occ.replace(" ", "")
                        content["gender"] = list(occ.split(","))
                        is_weird = 1
                if not is_weird:
                    content["gender"] = raw_gender_list

                hsh = {}
                hage = {}

                if content["age"] is None and content["friend_circle_id"] is not None:
                    if not objGDBUser.get_friend_circle_attributes(content["friend_circle_id"], hsh):
                        current_app.logger.error("Unable to get friend circle_attributes")
                        return {"Error": "Failure: Unable to get the age and gender from friend circle"}, 400
                    content["age"] = hsh["age"] if "age" in hsh else 0
                    if "gender" not in content or content["gender"] is None:
                        content["gender"] = hsh["gender"] if "gender" in hsh else None
                    if content["age"] is None or int(content["age"]) <= 0 or content["gender"] is None or \
                            len(set(content["gender"]) & set(["M", "F", "A"])) != len(content["gender"]):
                        current_app.logger.error("Age and gender do not have valid values")
                        return {"Error": "Age and Gender do not have valid values"}, 400

                lcat = []
                lsubcat = []

                if len(content["subcategory"]) <= 0:
                    if objGDBUser.get_subcategory_interest(content["friend_circle_id"], lsubcat):
                        print("successfully retrieved the interest categories for friend circle id:",
                              content["friend_circle_id"])
                    else:
                        return {"Error": "failure: Unable to get subcategories for the friend circle id"}, 400

                    if len(lsubcat) > 0:
                        content["subcategory"] = []
                        for row in lsubcat:
                            if row["subcategory_id"] is not None:
                                content["subcategory"].append(row["subcategory_id"])

                if content["page_size"] is None or content["page_number"] is None or "sort_order" not in content:
                    current_app.logger.error("Page size or page number cannot be null")
                    return {"failure": "Page size and/or page number is missing"}, 400

                loutput = []

                ret = objSearch.search_gemift_products(content, output_list)
                if ret == 1:
                    print("The result is ", output_list)
                    return {"data": json.loads(json_util.dumps(output_list))}, 200
                elif ret == 2:  # no result
                    current_app.logger.error("No data for the search")
                    return {
                               "data": "no data for search. No occasion , subcategory list or subcategory for friend circle "}, 200
                else:
                    current_app.logger.error("Error executing the search by occasion function")
                    print("Error executing the search by occasion function ")
                    return {"Error": "Error executing the search"}, 400

            if content["request_id"] == 2:
                # get occasions for a secret friend and by given price point
                if objSearch.search_by_occasion_price(content, output_list):
                    current_app.logger.error("Unable to get search data by occasion and price point")
                    return {"Error": "Failure Unable to get search data by occasion and price point"}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200

            if content["request_id"] == 3: # to get all the categories
                loutput = []
                is_weird = 0
                sstr = "("
                raw_occasion_list = request.args.getlist("occasion_list")
                for occ in raw_occasion_list:
                    if str(occ).find(sstr) >= 0:
                        occ = occ.replace('(', " ")
                        occ = occ.replace(')', " ")
                        occ = occ.replace('"', " ")
                        occ = occ.replace(" ", "")
                        content["occasion"] = list(occ.split(","))
                        is_weird = 1
                if not is_weird:
                    content["occasion"] = raw_occasion_list
                is_weird = 0

                raw_gender_list = request.args.getlist("gender_list")
                for occ in raw_gender_list:
                    if str(occ).find(sstr) >= 0:
                        occ = occ.replace('(', " ")
                        occ = occ.replace(')', " ")
                        occ = occ.replace('"', " ")
                        occ = occ.replace(" ", "")
                        content["gender"] = list(occ.split(","))
                        is_weird = 1
                if not is_weird:
                    content["gender"] = raw_gender_list

                if not objSearch.get_categories(content, output_list):
                    current_app.logger.error("Unable to get distinct categories")
                    return -1


                return {"data": json.loads(json_util.dumps(output_list))}, 200

            if content["request_id"] == 4:
                # get products by category
                loutput = []
                if not objGDBUser.get_category_interest(content["friend_circle_id"], loutput):
                    current_app.logger.error(
                        "Unable to get the interest for friend circle id " + content["friend_circle_id"])
                    return {"Error": "Failure in extracting interests"}, 400
                content["category"] = list(record["category_name"] for record in loutput)
                if not objSearch.search_by_category(content, output_list):
                    current_app.logger.error("Unable to get search data by category")
                    return {"Error": "Failure Unable to get search data by category"}, 400
                return {"data": json.dumps(output_list)}, 200

            if content["request_id"] == 5:
                # get all the voted products
                if "friend_circle_id" not in content or \
                        "occasion_name" not in content or \
                        "occasion_id" not in content or \
                        "occasion_year" not in content:
                    current_app.logger.error(
                        "Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing")
                    print(
                        "Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing")
                    return {
                               "status": "Failure Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing"}, 400
                if not objSearch.get_voted_products(content, output_list):
                    current_app.logger.error("Unable to get voted products")
                    print("Friend Circle ID , Unable to get votes products")
                    return {"Error": "Unable to get voted products"}, 400
                return {"data": json.loads(json.dumps(output_list))}, 200
            if content["request_id"] == 7:
                # get the product detail.
                if "product_id" not in content:
                    current_app.logger.error("Product id is missing in the request")
                    return {"Error": "product id is required"}, 400
                if not objSearch.get_product_detail(content, output_list):
                    current_app.logger.error("Unable to product detail for " + content["product_id"])
                    print("Unable to product detail for ", content["product_id"])
                    return {"Error", "Unable to product detail for ", content["product_id"]}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 8:  # get the total votes for the recommended product
                if content["friend_circle_id"] is None or \
                        content["occasion_year"] is None or \
                        content["occasion_id"] is None or \
                        content["occasion_name"] is None :
                    current_app.logger.error("One or more required parameters to vote for recommended product is "
                                             "missing")
                    return {"Error": "One of the required parameters missing"}, 400
                if not objSearch.get_recommended_product_vote_count(content["friend_circle_id"],
                                                                    content["occasion_id"],
                                                                    content["occasion_year"],
                                                                    output_list):
                    current_app.logger.error("Unable to get vote count for recommended products")
                    return {"Error": "Unable to get vote count for recommended products"}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
        except Exception as e:
            current_app.logger.error("Error in get product related API " + str(e))
            return {"Error": "Error in get product related API" + str(e)}, 400

    def post(self):
        try:
            # vote product
            content = {}
            content = request.get_json()
            objSearch = SearchDB()
            objGDBUser = GDBUser()
            objFriend = FriendListDB()


            if content["request_id"] == 8:
                if "user_id" not in content or \
                        "product_id" not in content or \
                        "product_title" not in content or \
                        "price" not in content or \
                        "vote" not in content or \
                        "friend_circle_id" not in content or \
                        "comment" not in content or \
                        "occasion_id" not in content or \
                        "occasion_year" not in content:
                    current_app.logger.error("one or more of the required parameters are missing ")
                    print("one or more of the required parameters are missing ")
                    return {"status", "one or more of the required parameters are missing "}, 400
                if not objSearch.vote_product(content):
                    current_app.logger.error("Issue with inserting vote for product", content["product_id"])
                    print("Unable to insert the vote", content["product_id"])
                    return {"Error", "Unable to insert vote for ", content["product_id"]}, 400
                return {"Status": "Success"}
            if content["request_id"] == 9:  # vote for the recommended product by the group
                if content["product_id"] is None or \
                        content["friend_circle_id"] is None or \
                        content["occasion_year"] is None or \
                        content["user_id"] is None or \
                        content["occasion_id"] is None:
                    current_app.logger.error("One or more required parameters to vote for recommended product is missing")
                    return {"Error": "One of the required parameters missing for voting for recommended product is missing"}, 400

                if "vote" not in content or content["vote"] is None:
                    content["vote"] = 0
                if not objSearch.vote_recommended_product(content):
                    current_app.logger.error("Unable to vote for recommended product" + str(content["product_id"]))
                    return {"Error": "Unable to vote for recommended product"}, 400
                return {"status": "Successfully voted"}
        except Exception as e:
            current_app.logger.error(e)
            print("The error is  catch all excception ", e)
            return {"Error": "Exception occured" + str(e)}


class UserSearchManagement(Resource):
    # @jwt_required()
    def get(self):
        content = {}
        output = []
        content["text"] = request.args.get("text")
        objSearch = SearchDB()

        if objSearch.get_users(content["text"], output):
            print("The output is", output)
            return {"data": json.loads(json_util.dumps(output))}, 200
        return {"Error": "Error in search"}, 400


class AllCategoryRelatedManagement(Resource):
    def get(self):
        content = {}
        output = []
        content["text"] = request.args.get("text")
        objSearch = SearchDB()
        if objSearch.get_cat_hierarchy(content["text"], output):
            print("The output is", output)
            return {"data": json.loads(json_util.dumps(output))}, 200
        return {"Error": "Error in Search"}, 400
