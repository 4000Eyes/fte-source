from __future__ import print_function
import os
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from flask import current_app, g
from flask_restful import Resource

# Types of emails
# Registration thank you email
# Registration confirmation email
# Invitation email
# Forgot password email
# Reminder about Invitation email
# Product email


class EmailManagement(Resource):

    def __init__(self):
        self.configuration = None
    # Configure API key authorization: api-key

    def init_service(self):
        try:
            self.configuration = sib_api_v3_sdk.Configuration()
            self.configuration.api_key['api-key'] = os.environ.get('EMAIL_SERVICE_API_KEY')
            self.signup_email_template_id = 1
            self.friend_invitation_email_template_id = 2
            self.company_name = os.environ.get("COMPANY_NAME_FOR_EMAIL")
        except ApiException as e:
            current_app.logger.error(e)

    def send_signup_email(self, email_to, first_name, last_name, call_to_action):
        subject = "My Subject"
        # Uncomment below lines to configure API key authorization using: partner-key
        # configuration = sib_api_v3_sdk.Configuration()
        # configuration.api_key['partner-key'] = 'YOUR_API_KEY'

        # create an instance of the API class
        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(self.configuration))
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=[{"email": email_to,"name": first_name + " " + last_name}],
                                                       template_id=self.signup_email_template_id,
                                                       params={"GEM_NAME": self.company_name,"FIRSTNAME": first_name},
                                                       headers={"Content-Type":"application/json", "accept":"application/json", "charset": "iso-8859-1"}) # SendSmtpEmail | Values to send a transactional email

        try:
            # Send a transactional email
            api_response = api_instance.send_transac_email(send_smtp_email)
            current_app.logger.info(api_response)
            #pprint(api_response)
        except ApiException as e:
            print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
            current_app.logger.error(e)

    def send_friend_invitation_email(self, email_to, first_name, last_name, secret_friend_name, friend_list, call_to_action):
        subject = "My Subject"
        # Uncomment below lines to configure API key authorization using: partner-key
        # configuration = sib_api_v3_sdk.Configuration()
        # configuration.api_key['partner-key'] = 'YOUR_API_KEY'

        # create an instance of the API class
        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(self.configuration))
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=[{"email":email_to,"name":first_name + " " + last_name}],
                                                       template_id=self.friend_invitation_email_template_id,
                                                       params={"FRIEND_NAMES": friend_list, "SECRET_FRIEND": secret_friend_name, "FIRSTNAME": first_name},
                                                       headers={"Content-Type":"application/json", "accept":"application/json", "charset": "iso-8859-1"}) # SendSmtpEmail | Values to send a transactional email

        try:
            # Send a transactional email
            api_response = api_instance.send_transac_email(send_smtp_email)
            current_app.logger.info(api_response)
            #pprint(api_response)
        except ApiException as e:
            print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
            current_app.logger.error(e)


    def create_contact(self, email, first_name, last_name):
        api_instance = sib_api_v3_sdk.ContactsApi(sib_api_v3_sdk.ApiClient(self.configuration))
        create_contact = sib_api_v3_sdk.CreateContact(email=email, update_enabled=True,
                                                      attributes={'FIRSTNAME': first_name, 'LASTNAME': last_name}, list_ids=[1])

        try:
            api_response = api_instance.create_contact(create_contact)
            current_app.logger.info(api_response)
            #print(api_response)
        except ApiException as e:
            print("Exception when calling ContactsApi->create_contact: %s\n" % e)
            current_app.logger.error(e)