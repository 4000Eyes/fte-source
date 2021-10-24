from flask import Response, request, current_app, jsonify
from model.searchdb import SearchDB
from model.gdbmethods import GDBUser
from flask_restful import Resource
from bson import json_util, ObjectId
import datetime
import json


class UserProductManagement(Resource):
    def get(self):
        try:
            content = request.get_json()
            print("Inside the search request")
            if content is None:
                return {"status": "Failure"}, 400
            if content["request_id"] is None:
                return {"status": "Input argument (request_id) is empty"}, 400
            content["friend_circle_id"] = None if "friend_circle_id" not in content else content["friend_circle_id"]
            output_list = []
            objSearch = SearchDB()
            objGDBUser = GDBUser()
            if content["subcategory_list"] is not None:
                content["subcategory"] = "'%s'" % "','".join(content["subcategory_list"])
            if content["color_list"] is not None:
                content["color"]= "'%s'" % "','".join(content["color_list"])
            if content["category_list"] is not None:
               content["category"]= "'%s'" % "','".join(content["category_list"])
            if  content["age_floor"] is None or \
                content["age_ceiling"] is None or \
                content["sort_order"] is None :
                current_app.logger.error("Age floor , age ceiling, sort order are required for any search and one or more is missing")
                print ("Key attributes age floor, ceilinng, sort order is missing")
                return {"status":"failure. missing key inputs age floor or age celing or sort order"}, 400
            if content["request_id"] == 1:
                #content["sort_order"} should be ASC or DSC
                # Note: Filter is applied for price only
                # if objSearch.search_by_occasion("birthday", 2, 32, output_list):
                subcategory_list = []
                if content["friend_circle_id"] is None or \
                        content["occasion_names"] is None :
                    current_app.logger.error("some of the key inputs are required, but missing to get the search results")
                    return {"status": "failure. Friend circle id and or occasion names is missing"}, 400

                if not objGDBUser.get_subcategory_interest(content["friend_circle_id"], content["subcategory"]):
                    current_app.logger.error("Unable to get the subcategories for secret friend")
                    return {"status": "failure. Unable to get subcategories for secret friend"}, 400

                if objSearch.search_by_occasion(content, output_list):
                    print("The result is ", output_list)
                    return {"data": json.loads(json_util.dumps(output_list))}, 200
                else:
                    current_app.logger.error("Error executing the search by occasion function for friend circle id", content["friend_circle_id"])
                    print("Error executing the search by occasion function for friend circle id", content["friend_circle_id"])
                    return {"status": "Error executing the search by occasion function for friend circle id"+ content["friend_circle_id"]}, 400

            if content["request_id"] == 2:
                # get occasions for a secret friend and by given price point
                if objSearch.search_by_occasion_price(content, output_list):
                    current_app.logger.error("Unable to get search data by occasion and price point")
                    return {"status": "Failure Unable to get search data by occasion and price point"}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 3:
                if objSearch.search_by_subcategory(content, output_list):
                    current_app.logger.error("Unable to get search data by subcategory")
                    return {"status": "Failure Unable to get search data by subcategory"}, 400
                # get products by subcategory
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 4:
                # get products by category
                if objSearch.search_by_category(content, output_list):
                    current_app.logger.error("Unable to get search data by category")
                    return {"status": "Failure Unable to get search data by category"}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 5:
                # get all the voted products
                if content["friend_circle_id"] is  None or \
                    content["product_id"] is None or \
                    content["occasion_name"] is None or \
                    content["occasion_year"]:
                    current_app.logger.error("Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing")
                    print("Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing")
                    return {"status": "Failure Friend Circle ID , product id, occasion name, occasion year are required and one or more parameters is missing"}, 400
                if not objSearch.get_product_votes(inputs, output_list):
                    current_app.logger.error("Unable to get voted products")
                    print("Friend Circle ID , Unable to get votes products")
                    return {"status": "Unable to get voted products"}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 6:
                # get products by color
                if objSearch.search_by_occasion_color(content, output_list):
                    current_app.logger.error("Unable to get search data by category")
                    return {"status": "Failure Unable to get search data by category"}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 7:
                # get the product detail.
                if not objSearch.get_product_detail(inputs, output_list):
                    current_app.logger.error("Unable to product detail for ", inputs["product_id"])
                    print("Unable to product detail for ", inputs["product_id"])
                    return {"status", "Unable to product detail for ", inputs["product_id"]}, 400
                return {"data": json.loads(json_util.dumps(output_list))}, 200
            if content["request_id"] == 8:
                #vote product
                if not objSearch.vote_product(inputs):
                    current_app.logger.error("Issue with inserting vote for product", inputs["product_id"])
                    print("Unable to insert the vote", inputs["product_id"])
                    return {"status", "Unable to insert vote for ", inputs["product_id"]}, 400
            if content["request_id"] == 100:
                # apply all conditions including category, subcategory, color, brand, occasions
                return {"data": json.loads(json_util.dumps(output_list))}, 200
        except Exception as e:
            current_app.logger.error(e)
            return False


class UserSearchManagement(Resource):
    def get(self):
        return {"status": "succcess"}, 200
