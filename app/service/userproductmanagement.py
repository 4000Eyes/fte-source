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
            content={}
            content["request_id"] = request.args.get("request_id",type=int)
            content["price_to"] = request.args.get("price_to", type=float)
            content["price_from"] = request.args.get("price_from", type=float)
            content["product_id"] = request.args.getlist("product_id", type=str)
            content["sort_order"] = request.args.get("sort_order",type=str)
            content["friend_circle_id"] = request.args.get("friend_circle_id", type=str)
            content["user_id"] = request.args.get("user_id", type=str)
            content["occasion_name"] = request.args.get("occasion_name", type=str)
            content["occasion_year"] = request.args.get("occasion_year", type=str)
            content["comment"] = request.args.get("comment", type=str)
            content["vote"] = request.args.get("vote", type=int)
            content["age"] = request.args.get("age", type=int)
            content["color"] = request.args.getlist("color_list")
            raw_occasion_list = request.args.getlist("occasion_list")
            content["page_size"] = request.args.get("page_size", type=int)
            content["page_number"] = request.args.get("page_number", type=int)

            print ("The values are", content["request_id"], content["product_id"])

            if "request_id" not in content:
                return {"status": "Input argument (request_id) is empty"}, 400

            # #regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]')
            #regex = re.compile('()')
            is_weird = 0
            sstr = "("
            for occ in raw_occasion_list:
                if str(occ).find(sstr) >= 0:
                    occ = occ.replace('(', " ")
                for occasion_id in raw_occasion_list:
                    if len(occasion_id) > 14: # GEM-OCC-000100. STANDARD
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
                    content["occasion"] = raw_occasion_list

            is_weird = 0
            raw_category_list = request.args.getlist("category_list")
            for occ in raw_category_list:
                if str(occ).find(sstr) >= 0:
                    occ = occ.replace('(', " ")
                    occ = occ.replace(')', " ")
                    occ = occ.replace('"'," ")
                    occ = occ.replace(" ","")
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
                    occ = occ.replace('"'," ")
                    occ = occ.replace(" ","")
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
                    occ = occ.replace('"'," ")
                    occ = occ.replace(" ","")
                    content["gender"] = list(occ.split(","))
                    is_weird = 1
            if not is_weird:
                content["gender"] = raw_gender_list

            output_list = []
            objSearch = SearchDB()
            objGDBUser = GDBUser()
            objFriend = FriendListDB()
            if content["request_id"] == 1:
                """
                if "subcategory_list" in content:
                    content["subcategory"] = "'%s'" % "','".join(content["subcategory_list"])
                if "color_list" in content:
                    content["color"] = "'%s'" % "','".join(content["color_list"])
                if "category_list" in content:
                    content["category"] = "'%s'" % "','".join(content["category_list"])
                if "occasion_list" in content:
                    content["occasion"] = "'%s'" % "','".join(content["occasion_list"])
                """

                hsh = {}
                hage = {}

                if content["age"] is None and content["friend_circle_id"] is not None:
                    if not objGDBUser.get_friend_circle_attributes(content["friend_circle_id"], hsh):
                        current_app.logger.error("Unable to get friend circle_attributes")
                        return {"status": "Failure: Unable to get the age and gender from friend circle"}, 400
                    content["age"] = hsh["age"] if "age" in hsh else 0
                    if "gender" not in content or content["gender"] is None:
                        content["gender"] = hsh["gender"] if "gender" in hsh else None

                    if content["age"] is None or int(content["age"]) <= 0 or content["gender"] is None or \
                            len(set(content["gender"])& set( ["M","F","A"])) != len(content["gender"]):
                        current_app.logger.error ("Age and gender do not have valid values")
                        return False

                lcat = []
                lsubcat = []


                if len(content["subcategory"]) <= 0:
                    if objGDBUser.get_subcategory_interest(content["friend_circle_id"], lsubcat):
                        print("successfully retrieved the interest categories for friend circle id:", content["friend_circle_id"])
                    else:
                        return {"status": "failure: Unable to get subcategories for the friend circle id"}, 400

                    if len(lsubcat) > 0:
                        content["subcategory"] = []
                        for row in lsubcat:
                            if row["subcategory_id"] is not None:
                                content["subcategory"].append(row["subcategory_id"])

                if content["page_size"] is None or content["page_number"] is None or "sort_order" not in content:
                    current_app.logger.error("Page size or page number cannot be null")
                    return {"failure": "Page size and/or page number is missing"}, 400
                    return False
                loutput = []

                ret = objSearch.search_gemift_products(content, output_list)
                if ret == 1:
                    print("The result is ", output_list)
                    return {"data": json.loads(json_util.dumps(output_list))}, 200
                elif ret == 2: #no result
                    current_app.logger.error("No data for the search")
                    return {"data": "no data for search. No occasion , subcategory list or subcategory for friend circle "}, 200
                else:
                    current_app.logger.error("Error executing the search by occasion function")
                    print("Error executing the search by occasion function ")
                    return {"status": "Error executing the search" }, 400

            if content["request_id"] == 2:
                # get occasions for a secret friend and by given price point
                if objSearch.search_by_occasion_price(content, output_list):
                    current_app.logger.error("Unable to get search data by occasion and price point")
                    return {"status": "Failure Unable to get search data by occasion and price point"}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 3:
                loutput = []
                if not objGDBUser.get_subcategory_interest(content["friend_circle_id"], loutput):
                    current_app.logger.error("Unable to get the interest for friend circle id " + content["friend_circle_id"])
                    return {"status": "Failure in extracting interests"}, 400
                content["subcategory"] = list(record["subcategory_name"] for record in loutput )
                if objSearch.search_by_subcategory(content, output_list):
                    current_app.logger.error("Unable to get search data by subcategory")
                    return {"status": "Failure Unable to get search data by subcategory"}, 400
                # get products by subcategory
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 4:
                # get products by category
                loutput = []
                if not objGDBUser.get_category_interest(content["friend_circle_id"], loutput):
                    current_app.logger.error("Unable to get the interest for friend circle id " + content["friend_circle_id"])
                    return {"status": "Failure in extracting interests"}, 400
                content["category"] = list(record["category_name"] for record in loutput )
                content["category"] = "Electronics"
                content["color"] = "Blue, Black"
                if not objSearch.search_by_category(content, output_list):
                    current_app.logger.error("Unable to get search data by category")
                    return {"status": "Failure Unable to get search data by category"}, 400
                return {"data": json.dumps(output_list)}, 200
            if content["request_id"] == 5:
                # get all the voted products
                if "friend_circle_id" not in content or \
                    "occasion_name" not in content or \
                    "occasion_year" not in content:
                    current_app.logger.error("Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing")
                    print("Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing")
                    return {"status": "Failure Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing"}, 400
                if not objSearch.get_voted_products(content, output_list):
                    current_app.logger.error("Unable to get voted products")
                    print("Friend Circle ID , Unable to get votes products")
                    return {"status": "Unable to get voted products"}, 400
                return {"data": json.loads(json.dumps(output_list))}, 200
            if content["request_id"] == 7:
                # get the product detail.
                if "product_id" not in content:
                    current_app.logger.error("Product id is missing in the request")
                    return {"status": "product id is required"},400
                if not objSearch.get_product_detail(content, output_list):
                    current_app.logger.error("Unable to product detail for " + content["product_id"])
                    print("Unable to product detail for ", content["product_id"])
                    return {"status", "Unable to product detail for ", content["product_id"]}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
        except Exception as e:
            current_app.logger.error(e)
            print("The error is  catch all excception ", e)
            return False

    def post(self):
        try:
            # vote product
            content={}
            content= request.get_json()
            objSearch = SearchDB()
            objGDBUser = GDBUser()
            objFriend = FriendListDB()

            if "user_id" not in content or \
                    "product_id" not in content or \
                    "product_title" not in content or \
                    "price" not in content or \
                    "vote" not in content or \
                    "friend_circle_id" not in content or \
                    "comment" not in content or \
                    "occasion_name" not in content or \
                    "occasion_year" not in content:
                current_app.logger.error("one or more of the required parameters are missing ")
                print("one or more of the required parameters are missing ")
                return {"status", "one or more of the required parameters are missing "}, 400
            if content["request_id"] == 8:
                if not objSearch.vote_product(content):
                    current_app.logger.error("Issue with inserting vote for product", content["product_id"])
                    print("Unable to insert the vote", content["product_id"])
                    return {"status", "Unable to insert vote for ", content["product_id"]}, 400
                return {"Status": "Success"}
        except Exception as e:
            current_app.logger.error(e)
            print("The error is  catch all excception ", e)
            return False



class UserSearchManagement(Resource):
   # @jwt_required()
    def get(self):
        content = {}
        output = []
        content["text"] = request.args.get("text")
        objSearch = SearchDB()

        if objSearch.get_users(content["text"], output):
            print ("The output is", output)
            return {"data": json.loads(json_util.dumps(output))}, 200
        return {"status": "Error in Search"}, 400

class AllCategoryRelatedManagement(Resource):
    def get(self):
        content = {}
        output = []
        content["text"] = request.args.get("text")
        objSearch = SearchDB()
        if objSearch.get_cat_hierarchy(content["text"], output):
            print ("The output is", output)
            return {"data": json.loads(json_util.dumps(output))}, 200
        return {"status": "Error in Search"}, 400