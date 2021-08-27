import json

import neo4j.exceptions
import logging
from flask import current_app
from flask_restful import Resource
from .extensions import NeoDB
from .dbhelper import ft_serialize_datetime
import uuid


# User object

# user_id
# email_address
# user_name
# location
# gender
# password
# birth date

class GDBUser(Resource):

    def get_user(self, email_address, user_id, output_data, flag):
        driver = NeoDB.get_session()
        if driver is None:
            current_app.logger.info("Driver to the database is not initiated")
            print("Driver is not initiated")
            return 0
        print("Inside the gdb user all function")
        try:
            if flag == 1:
                results = driver.run("MATCH (u:User {email_address : $email_address}) return u.user_id",
                                     {"email_address": str(email_address)})
            else:
                results = driver.run("MATCH (u:User {user_id : $user_id}) return u.email_address",
                                     {"user_id": str(user_id)})
            if results is None:
                print("user does not exist")
                output_data = None
                return True
            for record in results:
                print("The user is", record)
                if flag == 1:
                    output_data[0] = record["u.user_id"]
                else:
                    output_data[0] = record["u.email_address"]
                print("The output data is", output_data[0])
                return True
        except neo4j.exceptions.Neo4jError as e:
            print("THere is a syntax error", e.message, e.metadata)
            output_data = None
            return False

    def insert_user(self, email_address, password, user_type, user_id):
        driver = NeoDB.get_session()
        user_id = uuid.uuid4()
        result = driver.run(
            "CREATE (u:User {email_address:$email_address, password:$password, user_id:$user_id})"
            " RETURN u.email_address, u.user_id",
            {"email_address": str(email_address), "password": str(password), "user_id": str(user_id)})
        record = result.single()
        info = result.consume().counters.nodes_created
        if info > 0 and record is not None:
            print("The user id is", record["u.user_id"])
            return True
        return False

    # friend circle object definition

    # friend_circle_id
    # friend_circle_name
    # friend_circle_created_dt
    # friend_circle_updated_dt

    def insert_friend_circle(self, user_id, friend_id, friend_circle_name, friend_circle_id):

        driver = NeoDB.get_session()
        fid = uuid.uuid4()
        try:
            query = "MATCH (n:User), (m:User)" \
                    " WHERE n.user_id = $user_id_ AND m.user_id = $friend_id_ " \
                    " CREATE (n)-[:CIRCLE_CREATOR]->(x:friend_circle{friend_circle_id:$friend_circle_id_, " \
                    "friend_circle_name:$friend_circle_name_, friend_circle_created_dt:datetime()})-[" \
                    ":SECRET_FRIEND]->(m) "

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

    def add_contributor_to_friend_circle_by_email(self, contributor_email_address, friend_circle_id, output):
        driver = NeoDB.get_session()
        query = "MATCH  (n:User)-[rr]->(x:friend_circle) " \
                " WHERE n.email_address = $email_address_ " \
                " AND x.friend_circle_id_ = $friend_circle_id " \
                " AND type(rr) = 'CONTRIBUTOR' " \
                " CREATE (n)-[:CONTRIBUTOR]->(x:friend_circle)"

        result = driver.run(query, email_address_=contributor_email_address, friend_circle_id_=friend_circle_id)
        return True


    def add_contributor_to_friend_circle(self, friend_id, friend_circle_id, output):
        driver = NeoDB.get_session()
        query = "MATCH  (n:User)-[rr]->(x:friend_circle) " \
                " WHERE n.user_id = $user_id_ " \
                " AND x.friend_circle_id_ = $friend_circle_id " \
                " AND type(rr) = 'CONTRIBUTOR' " \
                " CREATE (n)-[:CONTRIBUTOR]->(x:friend_circle)"

        result = driver.run(query, user_id_ = friend_id, friend_circle_id_=friend_circle_id)
        return True


    def get_friend_circle(self, friend_circle_id, loutput):
        driver = NeoDB.get_session()
        result = None
        try:

            query = "MATCH (n:User)-[rr]->(x:friend_circle)<-[:SECRET_FRIEND]->(m:User) " \
                    "WHERE x.friend_circle_id = $friend_circle_id_ " \
                    " return  n.user_id, n.user_name, n.user_location, " \
                    " type(rr) as rel_name, m.user_id, m.user_location, m.user_name"
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            is_once = 0
            for record in result:
                print("Inside the friend circle loop")
                if record["rel_name"] == "CIRCLE_CREATOR":
                    loutput.append(
                        {"user_id": record["n.user_id"], "user_name": record["n.user_name"], "user_type": "admin"})
                else:
                    loutput.append(
                        {"user_id": record["n.user_id"], "user_name": record["n.user_name"],
                         "user_type": "contributor"})
                if is_once == 0:
                    loutput.append(
                        {"user_id": record["m.user_id"], "user_name": record["m.user_name"],
                         "user_type": "secret friend"})
                    is_once == 1

            current_app.logger.info(result.consume().query)
            current_app.logger.info(result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            return False

    def get_friend_circle_by_user_id(self, creator_user_id, friend_user_id, output):
        # This will return multiple friend circles if exist
        driver = NeoDB.get_session()
        result = None
        try:
            if creator_user_id and friend_user_id:
                query = "match (n:User)-[rr]->(x:friend_circle)<-[yy]->(m:User) " \
                        "where " \
                        "n.user_id = $creator_user_id_ and " \
                        "m.user_id = $friend_user_id_ and " \
                        " type(rr) = $creator_rel_type_" \
                        " return  count(n) as total_count"
                result = driver.run(query, creator_user_id_=creator_user_id,
                                    friend_user_id_=friend_user_id, creator_rel_type_="CIRCLE_CREATOR")
                counter = 0
                output1 = []
                output2 = []
                for record in result:
                    if not counter:
                        output1.append(
                            {"creator_id": record["n.user_id"], "creator_name": record["n. email_address"]})
                        output2.append(
                            {"friend_id": record["m.user_id"], "friend_name": record["m.email_address"]})
                        counter = 1
                    print("The value is ", record["total_count"])
                    output1.append(output2)
                    output[0] = json.dumps(output1)
                    print("The output json is", output[0])
                current_app.logger.info(result.consume().query)
                current_app.logger.info(result.consume().parameters)
                return True
        except neo4j.exceptions.Neo4jError as e:
            current_app.logger.error(e.message)
            return False

    def get_friend_circle_by_email_addresses(self, user_email_address, friend_email_address, output):
        driver = NeoDB.get_session()
        result = None
        try:
            if user_email_address and friend_email_address:
                query = "match (n:User)-[rr]->(x:friend_circle)<-[yy]->(m:User) " \
                        "where " \
                        "n.email_address = $user_email_address_ and " \
                        "m.email_address = $friend_email_address_ and " \
                        " type(rr) = $creator_rel_type_" \
                        " return  count(n) as total_count"
                result = driver.run(query, user_email_address_=user_email_address,
                                    friend_email_address_=friend_email_address, creator_rel_type_="CIRCLE_CREATOR")
                for record in result:
                    print("The value is ", record["total_count"])
                    output[0] = str(record["total_count"])
                current_app.logger.info(result.consume().query)
                current_app.logger.info(result.consume().parameters)
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

    def get_all_contributors_to_friend_circle(self, creator_user_id, output):
        # Looks like use less method
        driver = NeoDB.get_session()
        query = "MATCH (n:User)-[rr]->(x:friend_circle)-[yy]->(m)" \
                " WHERE n.user_id = $creator_user_id_ " \
                " RETURN n, m, x, rr, yy "
        result = driver.run(query, creator_user_id_=str(creator_user_id))
        counter = 0
        output1 = []
        output2 = []
        for record in result:
            if not counter:
                output1.append({"creator_id": record["n.user_id"], "creator_name": record["n.email_address"]})
                counter = 1
            output2.append({"friend_id": record["m.user_id"], "friend_name": record["m.email_address"]})
            print("The records are ", record["n"]["email_address"])
            print("The records are ", record["m"]["email_address"])
        output1.append(output2)
        output.append(output1)
        print("The output json is", output[0])
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
            print ("Before printing")
            for record in result:
                print ("The user is ", record["user_exists"])
                loutput.append(record["user_exists"])
                loutput.append(record["relation_type"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print (e.message)
            current_app.logger.error(e.message)
            loutput[0] = None
            loutput[1] = None
            return False


    def check_user_in_friend_circle_by_email(self, friend_circle_id , email_address):
        driver = NeoDB.get_session()
        query = "MATCH (n:User)-[:CIRCLE_CREATOR || :CONTRIBUTOR]->(x:friend_circle)" \
                " WHERE x.id = $friend_circle_id_ AND" \
                " n.email_address= $email_address_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_=email_address)
        return True

    def check_user_is_secret_friend(self, user_id,friend_circle_id, loutput):
        driver = NeoDB.get_session()
        query = "MATCH (x:friend_circle)-[:SECRET_FRIEND]->(n:User)" \
                " WHERE x.id = $friend_circle_id_ AND" \
                " n.user_id= $user_id_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=user_id)
        return True

    def check_user_is_secret_friend_by_email(self, email_address,friend_circle_id, loutput):
        driver = NeoDB.get_session()
        query = "MATCH (x:friend_circle)-[:SECRET_FRIEND]->(n:User)" \
                " WHERE x.id = $friend_circle_id_ AND" \
                " n.email_address= $email_address_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_=email_address)
        return True

    def check_user_is_admin(self, user_id,friend_circle_id, loutput):
        driver = NeoDB.get_session()
        query = "MATCH (x:friend_circle)-[:CIRCLE_CREATOR]->(n:User)" \
                " WHERE x.id = $friend_circle_id_ AND" \
                " n.user_id= $user_id_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=user_id)
        return True

    def check_user_is_admin_by_email(self, email_address,friend_circle_id, loutput):
        driver = NeoDB.get_session()
        query = "MATCH (x:friend_circle)-[:CIRCLE_CREATOR]->(n:User)" \
                " WHERE x.id = $friend_circle_id_ AND" \
                " n.email_address= $email_address_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_circle_id_=friend_circle_id, email_address_ = email_address)
        return True

    def check_friend_circle_with_admin_and_secret_friend(self, friend_user_id, secret_user_id, loutput):
        driver = NeoDB.get_session()
        query = "MATCH (x:User-[:CIRCLE_CREATOR]->(n:friend_circle)-[SECRET_FRIEND]->(y:User)" \
                " WHERE x.user_id = $friend_user_id_ AND" \
                " y.user_id = $secret_friend_id_  AND" \
                " n.friend_circle_id = $friend_circle_id_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_user_id_ = friend_user_id, secret_user_id_ = secret_user_id)
        return True

    def check_friend_circle_with_admin_and_secret_friend_by_email(self, friend_user_id, email_address, loutput):
        driver = NeoDB.get_session()
        query = "MATCH (x:User-[:CIRCLE_CREATOR]->(n:friend_circle)-[SECRET_FRIEND]->(y:User)" \
                " WHERE x.user_id = $friend_user_id_ AND" \
                " y.email_address = $email_address_  AND" \
                " n.friend_circle_id = $friend_circle_id_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_user_id_ = friend_user_id, email_address_ = email_address)
        return True

    # interest object
    # friend_category_id
    # friend_circle_id
    # friend_category_create_dt
    # friend_category_update_dt

    def add_interest(self, secret_friend_id, friend_circle_id, contributor_user_id, interest_id):
        try:
            friend_category_id = uuid.uuid4()
            driver = NeoDB.get_session()
            query = "MERGE (a:User)-[r:REOMMENDED_INTEREST]->(f:friend_category)<-[x:IS_MAPPED]-(b:category) " \
                    " ON CREATE set f.friend_category_id = $friend_category_id, f.friend_category_create_dt = $fc_create_dt_, " \
                    " r.fid=$friend_circle_id_, x.friend_circle_id = $friend_circle_id_ " \
                    " ON MATCH set friend_category_create_dt = $fc_create_dt_" \
                    " WHERE r.fid = $friend_circle_id_ AND " \
                    " b.category_id = $interest_id_" \
                    " a.user_id = $contributor_user_id_" \
                    "RETURN f.fc_id"
            result = driver.run(query, contributor_user_id_=contributor_user_id,
                                friend_circle_id_=friend_circle_id,
                                interest_id_=interest_id,
                                friend_category_id_=friend_category_id)
            record = result.single()
            if record["fc_id"]:
                return True
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
        except neo4j.exceptions.Neo4jError as e:
            print("The error is ", e)
        return True

    def delete_interest(self, friend_circle_id, user_id, interest_id):
        try:
            driver = NeoDB.get_session()
            query = " MATCH (a:User)-[r:RECOMMEDED_INTEREST]->(f:friend_category)<-[x:IS_MAPPED]-(b:category)" \
                    " WHERE r.fid = $friend_circle_id_ AND " \
                    " b.interest_id = $interest_id_ AND " \
                    "a.user_id = $user_id_" \
                    " REMOVE r " \
                    " return r.fc_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                interest_id_=interest_id)
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL", e)
            return False
        return True

    def get_interest_by_category(self, friend_circle_id, interest_id, loutput):
        # intent to get the number of recommended interests by count
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST]->(f:friend_category)<-[x:IS_MAPPED]-(b:category) " \
                    "WHERE r.fid = $friend_circle_id_ AND " \
                    "b.category_id = $interest_category_id_" \
                    "RETURN count(a.user) as users, b.category_id, b.category_name"
            result = driver.run(query, friend_circle_id_=friend_circle_id, interest_id_=interest_id)
            counter = 0
            for record in result:
                counter = counter + 1
                loutput.append({"category_name": record["users"], "category_id": record["category_id"]})
            loutput.append({"rows": counter})
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the query", e)
            return False
        print("The  query is ", result.consume().query)
        print("The  parameters is ", result.consume().parameters)
        return True

    def get_interest_by_friend_circle(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST]->(f:friend_category)<-[x:IS_MAPPED]-(b:category) " \
                    "WHERE r.fid = $friend_circle_id_ " \
                    "RETURN count(a.user) as users, b.interest_id, b.interest_name"
            result = driver.run(query, friend_circle_id_=friend_circle_id)
            counter = 0
            for record in result:
                counter = counter + 1
                loutput.append({"interest_name": record["interest_name"], "interest_id": record["interest_id"]})
            loutput.append({"rows": counter})
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
            fo_id = uuid.uuid4()
            driver = NeoDB.get_session()
            query = "MERGE (a:User)-[r:OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion) " \
                    "ON CREATE set f.friend_occasion_id = $fo_id_, f.fc_create_dt = $fc_create_dt_, " \
                    "r.friend_circle_id=$friend_circle_id_, x.friend_circle_id = $friend_circle_id_ " \
                    " f.friend_occasion_date = occasion_date_ " \
                    " f.friend_occasion_status = 0 " \
                    " ON MATCH set friend_occasion_create_dt = $fc_create_dt_" \
                    " WHERE r.friend_circle_id = $friend_circle_id_ AND " \
                    " b.occasion_id = $occasion_id_" \
                    " a.user_id = $user_id_" \
                    " RETURN f.friend_occasion_id"
            result = driver.run(query, fc_create_dt_=datetime(),
                                friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                occasion_id_=occasion_id,
                                fo_id_=fo_id)
            record = result.single()
            if record["fo_id"]:
                return True
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            print("The error is ", e)
        return True

    def approve_occasion(self, friend_circle_id, creator_user_id, referrer_user_id, occasion_uid, status):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:OCCASION]->(f:friend_occasion)<-[x:IS_MAPPED]-(b:occasion),  " \
                    " MATCH (b:User)-[:CIRCLE_CREATOR]->(fc:friend_circle)" \
                    " ON MATCH set f.friend_occasion_status = $status_ " \
                    " WHERE a.user_id = $referrer_user_id_ " \
                    " AND b.user_id = $creator_user_id_" \
                    " AND f.friend_circle_id = $friend_circle_id_" \
                    " AND x.friend_circle_id = $friend_circle_id_" \
                    " AND fc.friend_circle_id = $friend_circle_id_ " \
                    " RETURN f.friend_occasion_id"

            result = driver.run(query,
                                referrer_user_id_=referrer_user_id,
                                creator_user_id_=creator_user_id,
                                friend_circle_id_=friend_circle_id,
                                status_=status,
                                user_id_=referrer_user_id,
                                occasion_uid_=occasion_uid)
            record = result.single()
            if record["fc_id"]:
                return True
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            return False
        return True

    def get_occasion(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:OCCASION]->(f:friend_occasion)<-(b:occasion) " \
                    " WHERE f.friend_circle_id = $friend_circle_id" \
                    " AND f.friend_circle_id = $friend_circle_id_ " \
                    " AND r.friend_circle_id = $friend_circle_id_" \
                    " AND f.friend_occasion_status = 1" \
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
            loutput.append({"rows": counter})
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            return False
        return True

    def vote_occasion(self, occasion_id, user_id, friend_circle_id, flag, occasion_date):
        try:
            driver = NeoDB.get_session()
            query = "MERGE (a:User)-[r:VOTE_OCCASION]->(b:friend_occasion) " \
                    " ON MATCH set r.status= flag, r.updated_dt=datetime() " \
                    " ON CREATE set r.status = flag, r.created_dt=datetime(), " \
                    " r.friend_circle_id = $friend_circle_id_" \
                    " WHERE b.occasion_id = $occasion_id_ AND " \
                    " r.friend_circle_id = $friend_circle_id_ AND" \
                    " a.user_id = $user_id_" \
                    " RETURN b.occasion_id as occasion_id"

            result = driver.run(query, user_id_=user_id, occasion_id_=occasion_id)
            record = result.single()
            if record["occasion_id"]:
                print("Sucessfully added")
                return True
        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the SQL", e)
            return False
        return True

    def get_occasion_votes(self, friend_circle_id, loutput):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:VOTE_OCCASION]->(f:friend_occasion)<-(b:occasion) " \
                    " WHERE f.fid = $friend_circle_id_" \
                    " AND f.friend_circle_id = $friend_circle_id_ " \
                    " RETURN count(a.User) as total_users, r.status, occasion_id "
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

    def update_occasion(self, user_id, friend_circle_id, occasion_id, status):
        try:
            driver = NeoDB.get_session()
            query = "MERGE (a:User)-[r:OCCASION]->(f:friend_occasion)<-(b:occasion) " \
                    " ON MATCH set b.status = $status_, updated_dt = datetime() " \
                    " WHERE f.friend_circle_id = $friend_circle_id" \
                    " AND f.friend_occasion_id = $occasion_id_" \
                    " AND a.user_id = $user_id_" \
                    " RETURN a.user_id, f.friend_occasion_id, f.friend_occasion_date, b.occasion_name "

            result = driver.run(query,
                                friend_circle_id_=friend_circle_id, status_=status, user_id_=user_id,
                                occasion_id_=occasion_id)
            record = result.single()
            if record["user_id"]:
                return True
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            return False
        return True


class UserProductDBManagement(Resource):
    def get_product(self,
                    product_id,
                    category_id,
                    location_id,
                    user_id,
                    user_age_lo,
                    user_age_hi,
                    user_location,
                    loutput):

        query = "MATCH (a:tagged_product) " \
                " WHERE  "
        is_first = 0
        placeholder = None
        if product_id:
            query = query + "a.product_id = $product_id_"
            placeholder = "product_id_ = product_id"
            is_first = 1

        if category_id:
            if is_first:
                query = query + "AND a.category_id IN $category_id_"
            else:
                query = query + "a.category_id = $category_id_ "
                is_first = 1
            placeholder = placeholder + ", category_id_ = category_id"

        if location_id:
            if is_first:
                query = query + "AND a.location_id = $location_id_"
            else:
                query = query + "a.location_id = $location_id_ "
                is_first = 1
            placeholder = placeholder + ", location_id_ = location_id"

        query = query + " LIMIT BY 100"
        try:
            driver = NeoDB.get_session()
            result = driver.run(query, placeholder)
            for record in result:
                loutput.append({"product_id": record["product_id"],
                                "product_name": record["product_name"],
                                "product_desc": record["product_desc"],
                                "price": record["price"]})

        except neo4j.exceptions.Neo4jError as e:
            print("Error in executing the sql", query)
        return True

# structure of tagged products
#  (n:tagged_product {product_id: XYZ, tagged_category: ["x', "y", "z"], "last_updated_date": "dd-mon-yyyy",
#  "location":"country", "age_lower" : 45, "age_upper": 43, "price_upper": 3, "price_lower": 54, "gender": "M",
#  "color": [red,blue, white], "category_relevance" : [3,4,5], uniqueness index: [1..10]
# )

