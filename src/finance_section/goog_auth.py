'''build GCP authentication strings'''
import os
from google.oauth2 import service_account
from dotenv import load_dotenv


load_dotenv()


def gcp_credentials():
    '''get authenticate credentials'''
    try:
        secrets = {}
        for i in ['type', 'project_id', 'private_key_id',
                'client_email', 'client_id', 'auth_uri', 'token_uri',
                'auth_provider_x509_cert_url', 'client_x509_cert_url']:
            secrets[i] = os.environ.get(i)
        # need to clean up private_key
        secrets['private_key'] = os.getenv('private_key').replace('\\n', '\n')

        cred = service_account.Credentials.from_service_account_info(secrets)
    except:
        '''reset back to default if encountered issues'''
        cred = None

    return cred