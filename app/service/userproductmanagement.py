from flask import Response, request, current_app, jsonify, json
from app.model.searchdb import SearchDB
from app.model.gdbmethods import GDBUser
from flask_restful import Resource
from bson import json_util, ObjectId
from flask_jwt_extended import jwt_required
import datetime
import json


class UserProductManagement(Resource):
    def get(self):
        try:
            #print ("The json is", request.get_json(force=True))
            #content = request.get_json(force=True)
            content={}
            #rere


            content["request_id"] = request.args.get("request_id",type=int)
            content["product_id"] = []
            content["product_id"] = request.args.getlist("product_id", type=int)
            print ("The values are", content["request_id"], content["product_id"])
            content["age_floor"] = request.args.get("age_floor",type=int)
            content["age_ceiling"] = request.args.get("age_ceiling", type=int)
            content["sort_order"] = request.args.get("sort_order",type=str)
            content["subcategory"] = request.args.getlist("subcategory_list")
            content["category"] = request.args.getlist("category_list")
            content["color"] = request.args.getlist("color_list")
            content["occasion"] = request.args.getlist("occasion_list")


            print ("The values are", content["request_id"], content["product_id"])
            if content is None:
                return {"status": "Failure"}, 400
            if "request_id" not in content:
                return {"status": "Input argument (request_id) is empty"}, 400

            output_list = []
            objSearch = SearchDB()
            objGDBUser = GDBUser()

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
                if "age_floor" not in content or \
                        "age_ceiling" not in content or \
                        "sort_order" not in content:
                    current_app.logger.error(
                        "Age floor , age ceiling, sort order are required for any search and one or more is missing")
                    print("Key attributes age floor, ceilinng, sort order is missing")
                    return {"status": "failure. missing key inputs age floor or age celing or sort order"}, 400
                loutput = []
                if objSearch.search_gemift_products(content, output_list):
                    print("The result is ", output_list)
                    return {"data": json.loads(json_util.dumps(output_list))}, 200
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
                    "product_id" not in content or \
                    "occasion_name" not in content or \
                    "occasion_year" not in content:
                    current_app.logger.error("Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing")
                    print("Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing")
                    return {"status": "Failure Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing"}, 400
                if not objSearch.get_product_votes(content, output_list):
                    current_app.logger.error("Unable to get voted products")
                    print("Friend Circle ID , Unable to get votes products")
                    return {"status": "Unable to get voted products"}, 400
                return {"data": json.dumps(output_list)}, 200
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
            if content["request_id"] == 8:
                #vote product

                if "user_id" not in content or \
                    "product_id" not in content or \
                    "vote" not in content or \
                    "friend_circle_id" not in content or \
                    "comment" not in content or \
                    "occasion_name" not in content or \
                    "occasion_year" not in content or \
                    "friend_id" not in content:
                    current_app.logger.error("one or more of the required parameters are missing ")
                    print("one or more of the required parameters are missing ")
                    return {"status", "one or more of the required parameters are missing "}, 400

                if not objSearch.vote_product(content):
                    current_app.logger.error("Issue with inserting vote for product", content["product_id"])
                    print("Unable to insert the vote", content["product_id"])
                    return {"status", "Unable to insert vote for ", content["product_id"]}, 400

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