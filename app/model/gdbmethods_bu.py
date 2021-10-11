import json
import neo4j.exceptions
import logging
from flask import current_app
from flask_restful import Resource
from .extensions import NeoDB
import uuid
from datetime import datetime


# User object

# user_id
# email_address
# user_name
# location
# gender
# password
# birth date

class GDBUser(Resource):
    FAKE_USER_TYPE = 1
    FAKE_USER_PASSWORD = "TeX54Esa"

    def __init__(self):
        self.__dttime = None
        self.__uid = None

    def get_datetime(self):
        self.__dttime = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return self.__dttime

    def get_id(self):
        self.__uid = str(uuid.uuid4())
        return self.__uid

    def get_user(self, email_address, output_hash):
        driver = NeoDB.get_session()
        if driver is None:
            current_app.logger.info("Driver to the database is not initiated")
            print("Driver is not initiated")
            return 0
        print("Inside the gdb user all function")
        try:
            query = "MATCH (u:User) " \
                    "WHERE u.email_address = $email_address_ " \
                    "RETURN u.user_id, u.user_type"
            results = driver.run(query, email_address_=email_address)
            if results is None:
                print("user does not exist")
                output_data = None
                return True
            for record in results:
                print("The user is", record)
                output_hash['user_id'] = record["u.user_id"]
                output_hash["user_type"] = record["u.user_type"]
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_data = None
            return False

    def get_user_by_id(self, user_id, output_data):
        driver = NeoDB.get_session()
        if driver is None:
            current_app.logger.info("Driver to the database is not initiated")
            print("Driver is not initiated")
            return 0
        print("Inside the gdb user all function")
        try:
            query = "MATCH (u:User) " \
                    "WHERE u.user_id = $user_id_ " \
                    "RETURN u.email_address, user_type"
            results = driver.run(query, user_id_=user_id)
            if results is None:
                print("user does not exist")
                output_data = None
                return True
            for record in results:
                print("The user is", record)
                output_data.append(record["email_address"])
                output_data.append(record["user_type"])
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_data = None
            return False

    def get_user_by_phone(self, phone_number, output_data):
        driver = NeoDB.get_session()
        if driver is None:
            current_app.logger.info("Driver to the database is not initiated in get_user_by_phone")
            print("Driver is not initiated get_user_by_phone")
            return 0
        print("Inside the gdb user all function")
        try:
            query = "MATCH (u:User) " \
                    "WHERE u.phone_number = $phone_number_ " \
                    "RETURN u.user_id, u.email_address, u.user_type"

            results = driver.run(query, phone_number_=phone_number)
            if results is None:
                print("user does not exist")
                output_data = None
                return True
            for record in results:
                print("The user is", record)
                output_data.append(record["u.user_id"])
                output_data.append(record["u.email_address"])
                output_data.append(record["u.user_type"])
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_data = None
            return False

    def insert_user(self, user_hash, output_hash):
        try:
            driver = NeoDB.get_session()
            query = "CREATE (u:User) " \
                    " SET u.email_address = $email_address_, u.user_id = $user_id, u.phone_number = $phone_number_, " \
                    " u.gender = $gender_, u.user_type = $user_type_, u.first_name=$first_name_, u.last_name=$last_name_" \
                    " RETURN u.email_address, u.user_id"

            result = driver.run(query, email_address_=str(user_hash.get('email_address')),
                                password=str(user_hash.get('password')),
                                user_id_=str(self.get_id()),
                                gender_=user_hash.get("gender"),
                                user_type_=user_hash.get("user_type"),
                                first_name_ = user_hash.get("first_name"),
                                last_name= user_hash.get("last_name"))
            record = result.single()
            info = result.consume().counters.nodes_created
            if info > 0 and record is not None:
                print("The user id is", record["u.user_id"])
                output_hash["email_address"] = record["u.email_address"]
                output_hash["user_id"] = record["u.user_id"]
                return True
            return False
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error("There is a error " + e.message)
            return False

    # friend circle object definition

    # friend_circle_id
    # friend_circle_name
    # friend_circle_created_dt
    # friend_circle_updated_dt

    def insert_friend_circle_V0(self, user_id, friend_id, friend_circle_name, friend_circle_id):

        driver = NeoDB.get_session()
        fid = uuid.uuid4()
        try:
            query = "MATCH (n:User), (m:User)" \
                    " WHERE n.user_id = $user_id_ AND m.user_id = $friend_id_ " \
                    " CREATE (n)-[:CIRCLE_CREATOR]->(x:friend_circle{friend_circle_id:$friend_circle_id_, " \
                    "friend_circle_name:$friend_circle_name_, friend_circle_created_dt:datetime()})-[" \
                    ":SECRET_FRIEND]->(m) " \
                    " RETURN x.friend_circle_id as circle_id"

            result = driver.run(query, user_id_=str(user_id), friend_id_=str(friend_id), friend_circle_id_=str(fid),
                                friend_circle_name_=friend_circle_name)

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            record = result.single()
            info = result.consume().counters.nodes_created
            print("The ids are ", user_id, friend_id, info)
            friend_circle_id[0] = record["circle_id"]
            print("Successfully inserted friend circle", friend_circle_id[0])
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            friend_circle_id[0] = None
            return False

        def insert_friend_circle(self, user_id, friend_id, friend_circle_name, friend_circle_id):

            driver = NeoDB.get_session()
            fid = uuid.uuid4()
            try:
                query = "MATCH (n:User), (m:friend_list)" \
                        " WHERE n.user_id = $user_id_ AND m.friend_id = $friend_id_ and m.user_id=$user_id_" \
                        " CREATE (n)-[:CIRCLE_CREATOR]->(x:friend_circle{friend_circle_id:$friend_circle_id_, " \
                        "friend_circle_name:$friend_circle_name_, friend_circle_created_dt:datetime()})-[" \
                        ":SECRET_FRIEND]->(m) " \
                        " RETURN x.friend_circle_id as circle_id"

                result = driver.run(query, user_id_=str(user_id), friend_id_=str(friend_id), friend_circle_id_=str(fid),
                                    friend_circle_name_=friend_circle_name)

                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                record = result.single()
                info = result.consume().counters.nodes_created
                print("The ids are ", user_id, friend_id, info)
                friend_circle_id[0] = record["circle_id"]
                print("Successfully inserted friend circle", friend_circle_id[0])
                return True
            except neo4j.exceptions.Neo4jError as e:
                print("THere is a syntax error", e.message)
                friend_circle_id[0] = None
                return False

    def add_contributor_to_friend_circle(self, user_id, friend_id, friend_circle_id, output):
        try:
            driver = NeoDB.get_session()
            query = "MATCH  (n:friend_list)-[rr]->(x:friend_circle) " \
                    " WHERE n.user_id = $user_id_ " \
                    " AND n.friend_id = $friend_id_" \
                    " AND x.friend_circle_id_ = $friend_circle_id " \
                    " AND type(rr) = 'CONTRIBUTOR' " \
                    " CREATE (n)-[:CONTRIBUTOR]->(x:friend_circle) " \
                    " RETURN x.friend_circle_id as friend_circle_id"

            result = driver.run(query, user_id_=user_id, friend_id_ = friend_id, friend_circle_id_=friend_circle_id)
            if result.peek() is None:
                output = []
                return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            friend_circle_id[0] = None
            return False


    def add_contributor_to_friend_circle_by_email(self, contributor_email_address, friend_circle_id, output):
        driver = NeoDB.get_session()
        query = "MATCH  (n:User)-[rr]->(x:friend_circle) " \
                " WHERE n.email_address = $email_address_ " \
                " AND x.friend_circle_id_ = $friend_circle_id " \
                " AND type(rr) = 'CONTRIBUTOR' " \
                " CREATE (n)-[:CONTRIBUTOR]->(x:friend_circle)"

        result = driver.run(query, email_address_=contributor_email_address, friend_circle_id_=friend_circle_id)
        return True

    def add_contributor_to_friend_circle_V0(self, friend_id, friend_circle_id, output):
        driver = NeoDB.get_session()
        query = "MATCH  (n:User)-[rr]->(x:friend_circle) " \
                " WHERE n.user_id = $user_id_ " \
                " AND x.friend_circle_id_ = $friend_circle_id " \
                " AND type(rr) = 'CONTRIBUTOR' " \
                " CREATE (n)-[:CONTRIBUTOR]->(x:friend_circle)"

        result = driver.run(query, user_id_=friend_id, friend_circle_id_=friend_circle_id)
        return True

    def get_friend_circle(self, friend_circle_id, loutput):
        driver = NeoDB.get_session()
        result = None
        try:

            query = "MATCH (n:User)-[rr]->(x:friend_circle)<-[yy]->(m:User) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ " \
                    " return  n.user_id, n.user_name, n.email_address, " \
                    " type(rr) as creator_contributor_relationship, m.user_id, m.email_address, m.user_name, " \
                    "type(yy) as secret_friend"
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            counter = 0

            for record in result:
                if not counter:
                    loutput.append({"user_id": record["m.user_id"], "user_name": record["m.user_name"],
                                    "email_address": record["m.email_address"], "friend_circle_id": record["x.id"],
                                    "relation": record["secret_friend"]})
                    counter = 1
                loutput.append({"user_id": record["n.user_id"], "user_name": record["n.user_name"],
                                "email_address": record["n.email_address"], "friend_circle_id": record["x.id"],
                                "relation": record["creator_contributor_relationship"]})
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            return False

    def insert_friend_circle_email_address(self, user_id, friend_id, friend_circle_id, friend_circle_name):
        driver = NeoDB.get_session()
        fid = uuid.uuid4()
        try:
            query = "MATCH (n:User), (m:User)" \
                    " WHERE n.email_address = $user_id_ AND m.email_address = $friend_id_ " \
                    " CREATE (n)-[:CIRCLE_CREATOR]->(x:friend_circle{friend_circle_id:$friend_circle_id_, " \
                    " friend_circle_name:$friend_circle_name_, friend_created_dt:datetime()})-[:SECRET_FRIEND]->(m)"

            result = driver.run(query, user_id_=str(user_id), friend_id_=str(friend_id), friend_circle_id_=str(fid),
                                friend_circle_name_=friend_circle_name)

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            record = result.single()
            info = result.consume().counters.nodes_created
            print("The ids are ", user_id, friend_id, info)
            friend_circle_id[0] = fid
            print("Successfully inserted friend circle", friend_circle_id[0])
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message)
            friend_circle_id[0] = None
            return False

    def get_friend_circles(self, creator_user_id, output):
        # Looks like use less method
        driver = NeoDB.get_session()
        query = "MATCH (n:User)-[rr]->(x:friend_circle)-[yy]->(m)" \
                " WHERE n.user_id = $creator_user_id_ " \
                " RETURN n.user_id, n.user_name, n.email_address,  m.user_id, m.user_name, m.email_address" \
                ", x.id, type(rr) as creator_contributor_relationship, type(yy) as secret_friend " \
                " ORDER by x.id "

        result = driver.run(query, creator_user_id_=str(creator_user_id))
        counter = 0
        output1 = []
        output2 = []
        for record in result:
            if not counter:
                output1.append({"user_id": record["m.user_id"], "user_name": record["m.user_name"],
                                "email_address": record["m.email_address"], "friend_circle_id": record["x.id"],
                                "relation": record["secret_friend"]})
                counter = 1
            output1.append({"user_id": record["n.user_id"], "user_name": record["n.user_name"],
                            "email_address": record["n.email_address"], "friend_circle_id": record["x.id"],
                            "relation": record["creator_contributor_relationship"]})
        print("The  query is ", result.consume().query)
        print("The  parameters is ", result.consume().parameters)
        return True

    def get_all_contributors_to_friend_circle_by_circle_and_user_id(self, creator_user_id, friend_circle_id, output):
        driver = NeoDB.get_session()
        query = "MATCH (n:User)-[rr]->(x:friend_circle)-[yy]->(m)" \
                " WHERE n.user_id = $creator_user_id_ and " \
                " x.friend_circle_id = $friend_circle_id_ " \
                " RETURN n, m, x, rr, yy "
        result = driver.run(query, creator_user_id_=str(creator_user_id), friend_circle_id_=str(friend_circle_id))
        counter = 0
        output1 = []
        output2 = []
        for record in result:
            if not counter:
                output1.append({"creator_id": record["n"]["user_id"], "creator_name": record["n"]["email_address"]})
                counter = 1
            output2.append({"friend_id": record["m"]["user_id"], "friend_name": record["m"]["email_address"]})
            print("The records are ", record["n"]["email_address"])
            print("The records are ", record["m"]["email_address"])
        output1.append(output2)
        output.append(output1)
        print("The output json is", output[0])
        print("The  query is ", result.consume().query)
        print("The  parameters is ", result.consume().parameters)
        return True

    def check_user_in_friend_circle(self, user_id, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:User)-[rr]->(x:friend_circle)" \
                    " WHERE x.id = $friend_circle_id_ AND" \
                    " n.user_id= $user_id_ AND " \
                    " (type(rr) = 'CIRCLE_CREATOR' OR type(rr) = 'CONTRIBUTOR') " \
                    " RETURN count(n) as user_exists, type(rr) as relation_type"
            result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=user_id)
            if result.peek() is None:
                return False

            for record in result:
                print("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
                loutput.append(record["relation_type"])

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)

            return True

            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            loutput.append(None)
            return False

    def check_user_in_friend_circle_by_email(self, friend_circle_id, email_address, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (n:User)-[:CIRCLE_CREATOR || :CONTRIBUTOR]->(x:friend_circle)" \
                    " WHERE x.id = $friend_circle_id_ AND" \
                    " n.email_address= $email_address_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_=email_address)
            for record in result:
                print("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            loutput.append(None)
            return False

    def check_user_is_secret_friend(self, user_id, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:friend_circle)-[:SECRET_FRIEND]->(n:User)" \
                    " WHERE x.id = $friend_circle_id_ AND" \
                    " n.user_id= $user_id_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=user_id)
            for record in result:
                print("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            loutput.append(None)
            return False

    def check_user_is_secret_friend_by_email(self, email_address, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:friend_circle)-[:SECRET_FRIEND]->(n:User)" \
                    " WHERE x.id = $friend_circle_id_ AND" \
                    " n.email_address= $email_address_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_=email_address)
            for record in result:
                print("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            loutput.append(None)
            return False

    def check_user_is_admin(self, user_id, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:friend_circle)-[:CIRCLE_CREATOR]->(n:User)" \
                    " WHERE x.id = $friend_circle_id_ AND" \
                    " n.user_id= $user_id_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=user_id)
            for record in result:
                print("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            loutput.append(None)
            return False

    def check_user_is_admin_by_email(self, email_address, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:friend_circle)-[:CIRCLE_CREATOR]->(n:User)" \
                    " WHERE x.id = $friend_circle_id_ AND" \
                    " n.email_address= $email_address_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_=email_address)
            for record in result:
                print("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            loutput.append(None)
            return False

    def check_friend_circle_with_admin_and_secret_friend(self, friend_user_id, secret_user_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:User)-[:CIRCLE_CREATOR]->(n:friend_circle)-[SECRET_FRIEND]->(y:User)" \
                    " WHERE x.user_id = $friend_user_id_ AND" \
                    " y.user_id = $secret_friend_id_  AND" \
                    " n.friend_circle_id = $friend_circle_id_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_user_id_=friend_user_id, secret_user_id_=secret_user_id)
            for record in result:
                print("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            loutput.append(None)
            return False

    def check_friend_circle_with_admin_and_secret_friend_by_email(self, friend_user_id, email_address, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (x:User-[:CIRCLE_CREATOR]->(n:friend_circle)-[SECRET_FRIEND]->(y:User)" \
                    " WHERE x.user_id = $friend_user_id_ AND" \
                    " y.email_address = $email_address_  AND" \
                    " n.friend_circle_id = $friend_circle_id_ " \
                    " RETURN count(n) as user_exists"
            result = driver.run(query, friend_user_id_=friend_user_id, email_address_=email_address)

            for record in result:
                print("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
                loutput.append(record["relation_type"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print(e.message)
            current_app.logger.error(e.message)
            loutput.append(None)
            return False

    # interest object
    # friend_category_id
    # friend_circle_id
    # friend_category_create_dt
    # friend_category_update_dt

    def link_user_to_web_category(self, friend_circle_id, user_id, lweb_category_id):
        try:
            driver = NeoDB.get_session()
            for hsh_web_category_id in lweb_category_id:
                query = "MATCH (a:User{user_id:$user_id_}), (" \
                        "b:WebCat{" \
                        "web_category_id:$web_category_id_})" \
                        " MERGE (a)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b) " \
                        " ON CREATE set r.friend_circle_id =$friend_circle_id_ , r.vote = $nvote_," \
                        " r.created_dt = $created_dt_" \
                        " ON MATCH set r.updated_dt = $updated_dt_, r.vote = $nvote_ " \
                        " RETURN r.friend_circle_id"
                result = driver.run(query, user_id_=user_id,
                                    friend_circle_id_=friend_circle_id,
                                    web_category_id_=hsh_web_category_id["web_category_id"],
                                    created_dt_=self.get_datetime(),
                                    updated_dt_=self.get_datetime(),
                                    nvote_=hsh_web_category_id["vote"])
                print("The peek is", result.peek())
                record = result.single()
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                if record is None:
                    print("The combination of user and category does not exist", user_id,
                          hsh_web_category_id["web_category_id"])
                    return False
        except neo4j.exceptions.Neo4jError as e:
            print("The error is ", e.message)

    def link_user_to_web_subcategory(self, friend_circle_id, user_id, lweb_subcategory_id):
        try:
            driver = NeoDB.get_session()
            for hsh_web_subcategory_id in lweb_subcategory_id:
                query = "MATCH (a:User{user_id:$user_id_),(" \
                        "b:WebCat{" \
                        "web_subcategory_id:$web_subcategory_id_}) " \
                        " MERGE (a)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b)" \
                        " ON CREATE set r.friend_circle_id =$friend_circle_id_ , r.vote = $nvote_," \
                        " r.created_dt = $created_dt_" \
                        " ON MATCH set r.updated_dt = $updated_dt_, r.vote = $nvote_ " \
                        " RETURN r.friend_circle_id"
                result = driver.run(query, user_id_=user_id,
                                    friend_circle_id_=friend_circle_id,
                                    web_subcategory_id_=hsh_web_subcategory_id["web_subcategory_id"],
                                    created_dt_=self.get_datetime(),
                                    updated_dt_=self.get_datetime(),
                                    nvote_=hsh_web_subcategory_id["vote]"])
                record = result.single()
                print("The  query is ", result.consume().query)
                print("The  parameters is ", result.consume().parameters)
                if record is None:
                    print("The combination of user and category does not exist", user_id,
                          hsh_web_subcategory_id["web_subcategory_id"])
                    return False
        except neo4j.exceptions.Neo4jError as e:
            print("The error is ", e.message)

    def delete_link_user_to_category(self, friend_circle_id, user_id, web_category_id):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User{user_id:$user_id_})-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebCat{web_category_id:$web_category_id_})" \
                    " DELETE r " \
                    " RETURN r.friend_circle_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                web_category_id_=web_category_id)
            record = result.single()
            if record is None:
                return False
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL", e)
            return False

    def delete_link_user_to_subcategory(self, friend_circle_id, user_id, web_subcategory_id):
        try:
            driver = NeoDB.get_session()
            query = None
            # query = "MATCH (a:User{user_id:$user_id_})-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebSubCat{web_subcategory_id:$web_subcategory_id_}) " \
            #        " DELETE r " \
            #       " return r.friend_circle_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                web_subcategory_id_=web_subcategory_id)
            record = result.single()
            if record is None:
                return False
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL", e)
            return False

    def get_category_interest(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebCat) " \
                    "RETURN count(a.user) as users, " \
                    "sum(r.vote) as votes, " \
                    "b.category_id as category_id," \
                    " b.category_name as category_name "
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            counter = 0
            for record in result:
                counter = counter + 1
                loutput.append(record)
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False
        print("The  query is ", result.consume().query)
        print("The  parameters is ", result.consume().parameters)
        return True

    def get_subcategory_interest(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST{friend_circle_id:$friend_circle_id_}]->(b:WebSubCat) " \
                    "RETURN count(a.user) as users, " \
                    "sum(r.vote) as votes, " \
                    "b.subcategory_id as subcategory_id," \
                    " b.subcategory_name as subcategory_name "
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            counter = 0
            for record in result:
                counter = counter + 1
                loutput.append(record)
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False
        print("The  query is ", result.consume().query)
        print("The  parameters is ", result.consume().parameters)
        return True

    # friend_occasion

    # friend_occasion_id
    # friend_occasion_date
    # friend_occasion_status
    # friend_occasion_create_dt
    # friend_occasion_update_dt

    def add_occasion(self, user_id, friend_circle_id, occasion_id, occasion_date):
        try:

            driver = NeoDB.get_session()
            query = " MATCH(a:User{user_id:$user_id_})," \
                    " (f:friend_occasion{friend_circle_id:$friend_circle_id_, occasion_id:$occasion_id_})," \
                    " (b:occasion{occasion_id:$occasion_id_}) " \
                    " MERGE (a:User)-[r:OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    " ON CREATE set f.occasion_id = $occasion_id_, f.fc_create_dt = created_dt_, " \
                    " r.friend_circle_id=$friend_circle_id_" \
                    " f.friend_occasion_date = occasion_date_ " \
                    " f.friend_occasion_status = 0 " \
                    " ON MATCH set b.status = $status_, updated_dt = $updated_dt_ " \
                    " f.friend_occasion_date = $occasion_date_" \
                    " f.friend_occasion_status = $status" \
                    " RETURN f.friend_circle_id"
            result = driver.run(query, created_dt_=self.get_datetime(),
                                friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                occasion_id_=occasion_id,
                                occasion_date_=occasion_date,
                                updated_dt_=self.get_datetime()
                                )

            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)

            if result.peek() is None:
                return False
            record = result.single()
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error is ", e.message)
            return False

    def approve_occasion(self, friend_circle_id, admin_user_id, referrer_user_id, occasion_id, status):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(o:occasion),  " \
                    " (b:User)-[:CIRCLE_CREATOR]->(fc:friend_circle)" \
                    " ON MATCH set f.friend_occasion_status = $status_ " \
                    " WHERE a.user_id = $referrer_user_id_ " \
                    " AND b.user_id = $admin_user_id-" \
                    " AND f.friend_circle_id = $friend_circle_id_" \
                    " AND fc.friend_circle_id = $friend_circle_id_ " \
                    " AND f.occasion_id = o.occasion_id " \
                    " AND o.occasion_id = $occasion_id_ " \
                    " RETURN f.friend_occasion_id"

            result = driver.run(query,
                                referrer_user_id_=referrer_user_id,
                                admin_user_id_=admin_user_id,
                                friend_circle_id_=friend_circle_id,
                                status_=status,
                                user_id_=referrer_user_id,
                                occasion_id_=occasion_id)
            if result.peek() is None:
                return False
            record = result.single()
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            return False

    def get_occasion(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    " WHERE " \
                    " f.friend_circle_id = $friend_circle_id_ " \
                    " AND r.friend_circle_id = $friend_circle_id_" \
                    " RETURN a.user_id, f.friend_occasion_date, f.friend_occasion_id, b.occasion_name "
            result = driver.run(query,
                                friend_circle_id_=friend_circle_id)
            counter = 0
            for record in result:
                loutput.append({"occasion_id": record["occasion_id"],
                                "occasion_type": record["occasion_type"],
                                "occasion_date": record["occasion_date"],
                                "contributor_user_id": record["contributor_user_id"]})
                counter = counter + 1
                print("The occasions are ", record["occasion_type"], record["occasion_date"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            return False

    def vote_occasion(self, occasion_id, user_id, friend_circle_id, flag, value):
        try:
            driver = NeoDB.get_session()
            query = " MATCH (a:User{user_id:$user_id_}), " \
                    "(b:friend_occasion{friend_circle_id:$friend_circle_id,  occasion_id :$occasion_id_})" \
                    " MERGE (a:User)-[r:VOTE_OCCASION]->(b:friend_occasion) " \
                    " ON MATCH set r.status= flag, r.updated_dt= $updated_dt_ " \
                    " r.value = $value_" \
                    " ON CREATE set r.status = flag, r.created_dt=$created_dt_, " \
                    " r.friend_circle_id = $friend_circle_id_" \
                    " r. value = $value_" \
                    " RETURN b.occasion_id as occasion_id"

            result = driver.run(query, user_id_=user_id, friend_circle_id_=friend_circle_id, occasion_id_=occasion_id,
                                value_=value)
            if result.peek() is None:
                return False
            record = result.single()
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL", e)
            return False

    def get_occasion_votes(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:VOTE_OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    " WHERE f.fid = $friend_circle_id_" \
                    " AND f.friend_circle_id = $friend_circle_id_ " \
                    " RETURN a.User, r.status, r.value, occasion_id "
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            counter = 0
            for record in result:
                loutput.append(
                    {"occasion_id": record["occasion_id"], "status": record["status"], "users": record["total_users"]})
                counter = counter + 1
            loutput.append({"rows": counter})
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL")
            return False

# structure of tagged products
#  (n:tagged_product {product_id: XYZ, tagged_category: ["x', "y", "z"], "last_updated_date": "dd-mon-yyyy",
#  "location":"country", "age_lower" : 45, "age_upper": 43, "price_upper": 3, "price_lower": 54, "gender": "M",
#  "color": [red,blue, white], "category_relevance" : [3,4,5], uniqueness index: [1..10]
# )
