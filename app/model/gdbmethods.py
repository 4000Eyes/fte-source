import json

import neo4j.exceptions
import logging
from flask import current_app
from flask_restful import Resource
from .extensions import NeoDB
from .dbhelper import ft_serialize_datetime
import uuid


class GDBUser(Resource):
    def get_user(self, user_id):
        driver = NeoDB.get_session()
        if driver is None:
            current_app.logger.info("The graph db driver is not valid")
            return None

    def get_gdbuser(self, email_address, user_id, output_data, flag):
        driver = NeoDB.get_session()
        if driver is None:
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

    def insert_gdbuser(self, email_address, password, user_id):
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

    def get_friend_circle_by_user_id(self, creator_user_id, friend_user_id, output):
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
                            {"creator_id": record["n"]["user_id"], "creator_name": record["n"]["email_address"]})
                        output2.append(
                            {"friend_id": record["m"]["user_id"], "friend_name": record["m"]["email_address"]})
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

    def insert_friend_circle(self, user_id, friend_id, friend_circle_id):
        driver = NeoDB.get_session()
        fid = uuid.uuid4()
        try:
            query = "MATCH (n:User), (m:User)" \
                    " WHERE n.user_id = $user_id_ AND m.user_id = $friend_id_ " \
                    " CREATE (n)-[:CIRCLE_CREATOR]->(x:friend_circle{id:$friend_circle_id_, " \
                    " friend_circle_name:$friend_circle_name_, friend_created_dt:datetime()})-[:SECRET_FRIEND_ID]->(m)"

            result = driver.run(query, user_id_=str(user_id), friend_id_=str(friend_id), friend_circle_id_=str(fid),
                                friend_circle_name_=str("This is X test"))
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

    def insert_friend_circle_email_address(self, user_id, friend_id, friend_circle_id):
        driver = NeoDB.get_session()
        fid = uuid.uuid4()
        try:
            query = "MATCH (n:User), (m:User)" \
                    " WHERE n.email_address = $user_id_ AND m.email_address = $friend_id_ " \
                    " CREATE (n)-[:CIRCLE_CREATOR]->(x:friend_circle{id:$friend_circle_id_, " \
                    " friend_circle_name:$friend_circle_name_, friend_created_dt:datetime()})-[:SECRET_FRIEND_ID]->(m)"

            result = driver.run(query, user_id_=str(user_id), friend_id_=str(friend_id), friend_circle_id_=str(fid),
                                friend_circle_name_=str("This is X test"))
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

    def check_user_in_friend_circle(self, friend_circle_id, user_id, loutput):
        driver = NeoDB.get_session()
        query = "MATCH (n:User)-[:CIRCLE_CREATOR || :CONTRIBUTOR]->(x:friend_circle)" \
                " WHERE x.id = $friend_circle_id_ AND" \
                " n.user_id= $user_id_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=user_id)
        return True

    def check_user_is_secret_friend(self, friend_circle_id, user_id, loutput):
        driver = NeoDB.get_session()
        query = "MATCH (x:friend_circle)-[:SECRET_FRIEND]->(n:User)" \
                " WHERE x.id = $friend_circle_id_ AND" \
                " n.user_id= $user_id_ " \
                " RETURN count(n) as user_exists"
        result = driver.run(query, friend_circle_id_=friend_circle_id, user_id_=user_id)
        return True

    def add_contributor_to_friend_circle(self, contributor_email_address, friend_circle_id, output):
        driver = NeoDB.get_session()
        query = "MATCH  (n:User)-[rr]->(x:friend_circle) " \
                " WHERE n.email_address = $email_address_ " \
                " AND x.friend_circle_id_ = $friend_circle_id " \
                " AND type(rr) = 'CONTRIBUTOR' " \
                " CREATE (n)-[:CONTRIBUTOR]->(x:friend_circle)"

        result = driver.run(query, email_address_=contributor_email_address, friend_circle_id_=friend_circle_id)
        return True

    def add_interest(self, secret_friend_id, friend_circle_id, contributor_user_id, interest_category_id):

        try:
            fc_id = uuid.uuid4()
            driver = NeoDB.get_session()
            query = "MERGE (a:User)-[r:INTEREST]->(f:friend_category)<-(b:category) " \
                    " ON CREATE set fc_id = $fc_id_, f.fc_create_dt = $fc_create_dt_, r.fid=$friend_circle_id_ " \
                    " ON MATCH set ft_create_dt = $fc_create_dt_" \
                    " WHERE r.fid = $friend_circle_id_ AND " \
                    " b.category_id = $interest_category_id_" \
                    " a.user_id = $contributor_user_id_" \
                    "RETURN f.fc_id"
            result = driver.run(query, contributor_user_id_=contributor_user_id,
                                friend_circle_id_=friend_circle_id,
                                interest_category_id_=interest_category_id,
                                fc_id_ = fc_id)
            record = result.single()
            if record["fc_id"]:
                return True
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
        except neo4j.exceptions.Neo4jError as e:
            print("The error is ", e)
        return True

    def delete_interest(self, friend_circle_id, user_id, interest_category_id):
        try:
            driver = NeoDB.get_session()
            query = " MATCH (a:User)-[r:INTEREST]->(b:category) " \
                    " WHERE r.fid = $friend_circle_id_ AND " \
                    " b.category_id = $interest_category_id_ AND " \
                    "a.user_id = $user_id_" \
                    " REMOVE r " \
                    " return r.fc_id"
            result = driver.run(query, friend_circle_id_=friend_circle_id,
                                user_id_ = user_id,
                                interest_category_id_=interest_category_id)
        except neo4j.exceptions.Neo4jError as e:
            print ("Error in executing the SQL", e)
            return False

        return True

    def get_interests_by_friend_circle(self, friend_circle_id, interest_category_id, loutput):
        # intent to get the number of recommended interests by count
        try:
            driver= NeoDB.get_session()
            query = "MATCH (a:User)-[r:INTEREST]->(f:friend_category)<-(b:category) " \
                        "WHERE r.fid = $friend_circle_id_ AND " \
                        "b.category_id = $interest_category_id_" \
                        "RETURN count(a.user) as users, b.category_id, b.category_name"
            result = driver.run(query, friend_circle_id_= friend_circle_id, interest_category_id_=interest_category_id)
        except neo4j.exceptions.Neo4jError as e:
            print ("Error in executing the query", e)
            return False
        print("The  query is ", result.consume().query)
        print("The  parameters is ", result.consume().parameters)
        return True


    def add_occasion(self, user_id, friend_circle_id, occasion_type, occasion_date):
        try:
            fo_id = uuid.uuid4()
            driver = NeoDB.get_session()
            query = "MERGE (a:User)-[r:OCCASION]->(f:friend_occasion)<-(b:occasion) " \
                    " ON CREATE set f.fo_id = $fo_id_, f.fc_create_dt = $fc_create_dt_, r.fid=$friend_circle_id_, " \
                    " f.occasion_date = occasion_date_ " \
                    " f.status = 0 " \
                    " ON MATCH set ft_create_dt = $fc_create_dt_" \
                    " WHERE r.fid = $friend_circle_id_ AND " \
                    " b.category_id = $interest_category_id_" \
                    " a.user_id = $user_id_" \
                    " RETURN f.fc_id"
            result = driver.run(query, fc_create_dt_ = datetime(),
                                friend_circle_id_=friend_circle_id,
                                user_id_=user_id,
                                occasion_type_ = occasion_type,
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
            query = "MATCH (a:User)-[r:OCCASION]->(f:friend_occasion)<-(b:occasion) " \
                    " MATCH (b:User)-[:CIRCLE_CREATOR]->(fc:friend_circle)" \
                    " ON MATCH set f.status = $status_ " \
                    " WHERE a.user_id = $referrer_user_id " \
                    " AND f.fid = $friend_circle_id" \
                    " AND fc.friend_circle_id = $friend_circle_id_ " \
                    " RETURN f.fo_id"

            result = driver.run(query,
                                friend_circle_id_=friend_circle_id,
                                user_id_=referrer_user_id,
                                occasion_uid_ = occasion_uid)
            record = result.single()
            if record["fc_id"]:
                return True
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            return False
        return True

    def get_occasion(self, friend_circle_id):
        try:
            driver = NeoDB.get_session()
            query = "MATCH (a:User)-[r:OCCASION]->(f:friend_occasion)<-(b:occasion) " \
                    " WHERE f.fid = $friend_circle_id" \
                    " AND f.friend_circle_id = $friend_circle_id_ " \
                    " AND f.status = 1" \
                    " RETURN a.user_id, f.fo_id, f.occasion_date, f.occasion_uid, b.occasion_type "

            result = driver.run(query,
                                friend_circle_id_=friend_circle_id)
            for record in result:
                print ("The occasions are ", record["occasion_type"], record["occasion_date"])
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            return False
        return True

    def vote_occasion(self, occasion_id, user_id, flag, occasion_date):

        try:
            driver = NeoDB.get_session()
            query = "MERGE (a:User)-[:VOTE_OCCASION]->(b:friend_occasion) " \
                    " ON MATCH set b.status= flag, b.updated_dt=datetime() " \
                    " ON CREATE set b.status = flag, b.created_dt=datetime() " \
                    " WHERE b.occasion_id = $occasion_id_ AND " \
                    " a.user_id = $user_id_" \
                    " RETURN b.occasion_id as occasion_id"

            result = driver.run(query, user_id_ = user_id, occasion_id_ = occasion_id)
            record = result.single()
            if record["occasion_id"]:
                print ("Sucessfully added")
                return True
        except neo4j.exceptions.Neo4jError as e:
            print ("Error in executing the SQL", e)
            return False
        return True

    def update_occasion(self, user_id, friend_circle_id, occasion_id, status):

        try:
            driver = NeoDB.get_session()
            query = "MERGE (a:User)-[r:OCCASION]->(f:friend_occasion)<-(b:occasion) " \
                    " ON MATCH set b.status = $status_, updated_dt = datetime() " \
                    " WHERE f.fid = $friend_circle_id" \
                    " AND f.occasion_id = $occasion_id_" \
                    " AND a.user_id = $user_id_" \
                    " RETURN a.user_id, f.fo_id, f.occasion_date, f.occasion_uid, b.occasion_type "

            result = driver.run(query,
                                friend_circle_id_=friend_circle_id, status_ = status, user_id_ = user_id, occasion_id_ = occasion_id)
            record = result.single()
            if record["user_id"] :
                return True
            print("The  query is ", result.consume().query)
            print("The  parameters is ", result.consume().parameters)
            return True
        except neo4j.exceptions.Neo4jError as e:
            return False
        return True

