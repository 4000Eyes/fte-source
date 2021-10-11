from flask import Response, request, current_app
from model.gdbmethods import GDBUser
from model.categorydb import CategoryManagementDB
from flask_restful import Resource
from model.models import EmailUserQueue, FriendCircleApprovalQueue
from .classhelper import FriendCircleHelper
from .cachebuilder import get_brands_by_subcategory, load_category_cache, load_subcat_brand_cache, get_value
import datetime
import json

class CategoryManagement(Resource):
    def post(self):
        try:
            content = request.get_json()
            if content is None:
                current_app.logger.error("No arguments passed to the api call /api/category(post). check parameters")
                return {"status": "failure"}, 500
            print ("The content is ", content)
            request_id = content["request_id"] if "request_id" in content else None
            value = content["value"] if "value" in content else None
            description = content["description"] if "description" in content else None
            web_category_id = content["web_category_id"] if "web_category_id" in content else None
            merch_category_id = content["merch_category_id"] if "merch_category_id" in content else None
            brand_id = content["brand_id"] if "brand_id" in content else None
            web_subcategory_id = content["web_subcategory_id"] if "web_subcategory_id" in content else None
            web_subcategory_list = content["web_subcategory_list"] if "web_subcategory_list" in content else None
            print ("Request id is ", request_id)
            # Insert merch category
            objCategory = CategoryManagementDB()
            output_hash = {}
            if request_id == 1:
                if not objCategory.add_merch_category(value,description, output_hash):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("merch_category_id"))
                return {"merch_category_id": output_hash.get("merch_category_id")}, 200
            if request_id == 2:
                if not objCategory.add_web_category(value,description,output_hash):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("web_category_id"))
                return {"web_category_id": output_hash.get("web_category_id")}, 200
            if request_id == 3:
                if not objCategory.add_web_subcategory(value,description, output_hash):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("web_subcategory_id"))
                return {"web_subcategory_id": output_hash.get("web_subcategory_id")}, 200
            if request_id == 4:
                if not objCategory.add_brand(value,description, output_hash):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("brand_id"))
                return {"brand_id": output_hash.get("brand_id")}, 200
            if request_id == 5: # link subcategory to category
                if not objCategory.link_subcategory(web_category_id, web_subcategory_id,output_hash):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("web_subcategory_id"))
                return {"status": "success"}, 200
            if request_id == 6: # link brand to subcategory
                #lsubcat = list(eval(web_subcategory_list))
                print ("Calling brand mapping function")
                if not objCategory.link_brand_to_subcategory(web_subcategory_id,brand_id):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("web_subcategory_id"))
                return {"status": "success"}, 200
            if request_id == 7: # link merch to web nodes
                if not objCategory.link_merch_to_web_nodes(merch_category_id, web_category_id):
                    return {"status": "Failure"},400
                print ("Successfully added ", merch_category_id, web_category_id)
                return {"status": "success"}, 200
        except Exception as e:
            current_app.logger.error("Error in processing the request" + str(e))
            print ("The error is ", e)
            return {"status": "Failure"}, 500

    def get(self):
        loutput = []
        content = request.get_json()
        if content is None:
            current_app.logger.error("No arguments passed to the api call /api/category(get). check parameters")
            return {"status": "failure"}, 500
        objCategory = CategoryManagementDB()
        request_id = content["request_id"] if "request_id" in content else None
        category_id = content["category_id"] if "category_id" in content else None
        subcategory_id = content["subcategory_id"] if "subcategory_id" in content else None
        brand_id = content["brand_id"] if "brand_id" in content else None
        subcategory_brand_list = content["subcategory_brand_list"] if "subcategory_brand_list" in content else None
        output = {}
        if request_id == 1 :
            if not objCategory.get_category(output):
                return {"status": "Error in fetching the category"}, 400
            return output, 200

        if request_id == 2:
            if not objCategory.get_subcategory(category_id, output):
                return {"status": "Error in fetching the category"}, 400
            return output, 200

        if request_id == 3:
            if not objCategory.get_brands(output):
                return {"status": "Error in fetching the category"}, 400
            return output, 200

        if request_id == 4:
            if not objCategory.get_web_subcategory_brands(subcategory_brand_list, output):
                return {"status": "Error in fetching the category"}, 400
            return output, 200


