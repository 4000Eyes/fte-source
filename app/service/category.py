from flask import request, current_app
from app.model.categorydb import CategoryManagementDB
from flask_restful import Resource
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
            age_hi = content["age_hi"] if "age_hi" in content else None
            age_lo = content["age_lo"] if "age_lo" in content else None
            gender = content["gender"] if "gender" in content else None

            print ("Request id is ", request_id)
            # Insert merch category
            objCategory = CategoryManagementDB()
            output_hash = {}
            if request_id == 1:
                if not objCategory.add_merch_category(value,description,age_lo, age_hi, gender, output_hash):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("merch_category_id"))
                return {"merch_category_id": output_hash.get("merch_category_id")}, 200
            if request_id == 2:
                if not objCategory.add_web_category(value,description,age_lo, age_hi, gender, output_hash):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("web_category_id"))
                return {"web_category_id": output_hash.get("web_category_id")}, 200
            if request_id == 3:
                if not objCategory.add_web_subcategory(value,description, age_lo, age_hi, gender,output_hash):
                    return {"status": "Failure"},400
                print ("Successfully added ", value, output_hash.get("web_subcategory_id"))
                return {"web_subcategory_id": output_hash.get("web_subcategory_id")}, 200
            if request_id == 4:
                if not objCategory.add_brand(value,description, age_lo, age_hi, gender, output_hash):
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

        objCategory = CategoryManagementDB()
        request_id = request.args.get("request_id", type=int)
        category_id = request.args.get("category_id", type=int)
        subcategory_id = request.args.get("subcategory_id", type=str)
        brand_id = request.args.get("brand_id", type=str)
        subcategory_brand_list = request.args.getlist("subcategory_brand_list")
        age_hi = request.args.get("age_hi", type=int)
        age_lo = request.args.get("age_lo", type=int)
        gender = request.args.get("gender", type=str)
        if age_lo is None or age_hi is None or gender is None:
            return {"status": " Age and gender cannot be null"}, 400
        output = {}
        if request_id == 1 :
            if not objCategory.get_category(age_lo, age_hi, gender, output):
                return {"status": "Error in fetching the category"}, 400
            return output, 200

        if request_id == 2:
            if not objCategory.get_subcategory(category_id,age_lo, age_hi, gender, output):
                return {"status": "Error in fetching the category"}, 400
            return output, 200

        if request_id == 3:
            if not objCategory.get_brands( age_lo, age_hi, gender,  output):
                return {"status": "Error in fetching the category"}, 400
            return {"data": json.dumps(output)}, 200

        if request_id == 4:
            if not objCategory.get_web_subcategory_brands(subcategory_brand_list, age_lo, age_hi, gender, output):
                return {"status": "Error in fetching the category"}, 400
            return output, 200


