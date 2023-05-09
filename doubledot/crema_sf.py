# AUTOGENERATED! DO NOT EDIT! File to edit: ../crema_sf.ipynb.

# %% auto 0
__all__ = ['Salesforce']

# %% ../crema_sf.ipynb 2
from nbdev.showdoc import *
import requests
import json
import jmespath as jp
import re
from time import sleep
from fastcore.basics import patch
import fileinput
import pandas as pd
import os
from doubledot import ATMS_api

# %% ../crema_sf.ipynb 3
## Module for Salesforce API

class Salesforce:
    """Class for Salesforce API"""
    class_download_dir = 'sf_download'

    def __init__(self):
        # set up access token 
        self._sf_access_token = self.get_token_with_REST()
        self.bulk_job_id = None
        self.atms = None

        # create unique download directory per instance
        if not os.path.exists(Salesforce.class_download_dir):
            os.makedirs(Salesforce.class_download_dir)
            print(f"Directory 'atms_download' created successfully.")
        else:
            print(f"Directory 'atms_download' already exists.")

    def get_token_with_REST(self ):
        """retieve the access token from Salesforce

        Returns:
            string: the access token 
        """
        with open('secrets.json') as f:
            secrets = json.load(f)
        
        DOMAIN = secrets['instance']
        payload = {
            'grant_type': 'password',
            'client_id': secrets['client_id'],
            'client_secret': secrets['client_secret'],
            'username': secrets['username'],
            'password': secrets['password'] + secrets['security_token']
        }
        oauth_url = f'{DOMAIN}/services/oauth2/token'

        auth_response = requests.post(oauth_url, data=payload)
        return auth_response.json().get('access_token') ######## <<<<<<<<<<<<<<<< .       


    @property
    def sf_access_token(
        self 
     ) -> str : #the access toke
        """a @property
        retrieve token for Salesforce - verifies that token is still valid and attempts to get a new one from Salesforce site if not
        """
        if not(self.test_token()):
            self._sf_access_token = self.get_token_with_REST()
            # check to see if getting token worked
            assert (self.sf_access_token), "Fetching new token didn't fix problem"
        return self._sf_access_token

show_doc(Salesforce.sf_access_token)
   

# %% ../crema_sf.ipynb 4
@patch
def get_token_with_REST(self: Salesforce):
    """retieve the access token from Salesforce

    Returns:
        string: the access token 
    """
    with open('secrets.json') as f:
        secrets = json.load(f)
    
    DOMAIN = secrets['instance']
    payload = {
        'grant_type': 'password',
        'client_id': secrets['client_id'],
        'client_secret': secrets['client_secret'],
        'username': secrets['username'],
        'password': secrets['password'] + secrets['security_token']
    }
    oauth_url = f'{DOMAIN}/services/oauth2/token'

    auth_response = requests.post(oauth_url, data=payload)
    return auth_response.json().get('access_token') ######## <<<<<<<<<<<<<<<< .       



# %% ../crema_sf.ipynb 5
@patch
def test_token(self: Salesforce):
    """Verify that token is still valid. If it isn't, it attempts to get a new one.

    Returns:
        boolean: true if token is valid, false otherwise
    """
    sf_headers = { 'Authorization': f"Bearer {self._sf_access_token}", 'Content-Type': 'application/json' }
    end_point ="https://cremaconsulting-dev-ed.develop.my.salesforce.com"
    service = "/services/data/v57.0/"
    r = requests.request("GET", end_point+service+f"limits", headers=sf_headers, data={})
    valid_token = r.status_code == 200
    if not(valid_token): print(r.status_code, type(r.status_code))
    return valid_token
    


# %% ../crema_sf.ipynb 6
@patch
def create_job(self: Salesforce, 
                sf_object: str ='Contact', # the Salesforce object were going to operate on. 
                operation: str ='insert', # the database operation to use. Can be "insert","upsert" or "delete"
                external_id: str = 'External_Id__c' # when using "upsert", this field is used to identify the record
                )-> requests.Response :
    """Get job_id from Salesforce Bulk API

    """
    # Args: 
    #     sf_object (str, optional): the Salesforce object were going to operate on. Defaults to 'Contact'.
    #     operation (str, optional): ∆. Defaults to 'insert'.
    #     external_id (str, optional): the external id field for upsert operations. Defaults to 'External_Id__c'.
    #     sf_object (str, optional): the Salesforce object were going to operate on. Defaults to 'Contact'.
    #     operation (str, optional): the operation that will be used against the object. Defaults to 'insert'.
    #     external_id (str, optional): the external id field for upsert operations. Defaults to 'External_Id__c'.
    #     contentType (str, optional): the content type of the file. Defaults to 'CSV', 'JSON' also accepted.
    # Returns: 
    #     response: a response object containg the job_id. For more information on the response object see https://www.w3schools.com/python/ref_requests_response.asp
    #     a response object see https://www.w3schools.com/python/ref_requests_response.asp
        
    # Salesforce API docs: https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/create_job.htm    
    
    url = "https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest"

    # https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/datafiles_prepare_csv.htm
    ## we can set columnDelimiter to `,^,|,;,<tab>, and the default <comma>
    # sets the object to Contact, the content type to CSV, and the operation to insert
    payload_d = {
        "object": sf_object,
        "contentType": "CSV",
        # set columnDelimiter to TAB instead of comma for ease of dealing with commas in address fields
        #https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/create_job.htm
        "columnDelimiter": "TAB", 
        "operation": operation
    }
    
    # as per https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/walkthrough_upsert.htm
    if operation=='upsert':
        payload_d['externalIdFieldName']=external_id
    print(operation, payload_d)        
    payload = json.dumps(payload_d)
    
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {self.sf_access_token}'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)
    self.bulk_job_id = response.json()['id']
    return response 


# %% ../crema_sf.ipynb 7
@patch
def upload_csv(self : Salesforce, 
                file_path_s: str = "", # the path to the csv file
                num_rows: int = 100, # the number of rows to upload 
                ):
    """Using the job_id from the previous step, upload the csv file to the job

    Args:
        file (filepointer): file pointer to the csv filek
    """
    # if not(file):
    #     # throw error
    #     assert False, "File not found"

    if len("")==0:
        if not(self.atms):
            # throw error
            assert False, "File not found"
        else:
            file_path_s = self.atms.download_dir 

    url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/batches"

    # replace all occurrences of '\2019' with \'
    # we may have done this in ATMS already, but just in case
    for line in fileinput.input(files=file_path_s, inplace=True):
        line = line.replace('\u2019', "'")
        print(line, end='')

    _df : pd.Dataframe = pd.read_csv(file_path_s, sep='\t')
    payload = _df[- num_rows:].to_dict()
    # with open(file_path_s,'r') as payload:
    headers = {
    'Content-Type': 'text/csv',
    'Authorization': f'Bearer {self.sf_access_token}'
    }
    response = requests.request("PUT", url, headers=headers, data=payload)
    ## Need error handling here
    

    print("response: ", response.text)
   

# %% ../crema_sf.ipynb 8
@patch
def close_job(self: Salesforce):
    # close the job (from Postman)
    url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}"

    payload = json.dumps({
        "state": "UploadComplete"
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {self.sf_access_token}'
    }

    response = requests.request("PATCH", url, headers=headers, data=payload)

    print(response.text)
     

# %% ../crema_sf.ipynb 9
# get job status (from Postman)
@patch
def job_status(self: Salesforce):
    url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}"

    payload = {}
    headers = {
    'Authorization': f'Bearer {self.sf_access_token}'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()



# %% ../crema_sf.ipynb 10
@patch
def successful_results(self : Salesforce):
    url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/successfulResults"

    payload = {}
    headers = {
        'Authorization': f'Bearer {self.sf_access_token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    print( response.text)


# %% ../crema_sf.ipynb 11
@patch
def failed_results(self: Salesforce):
    url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/failedResults"

    payload = {}
    headers = {
        'Authorization': f'Bearer {self.sf_access_token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    print( response.text)


# %% ../crema_sf.ipynb 12
@patch
def get_sf_object_ids(self: Salesforce, 
                      object: str = 'Contact' # REST endpoint for data object
                      ):
    """Get Safesforce IDs for a the specified object

    """
    print(f"Retrieving Object Ids for {object} from Salesforce")
    sf_headers = { 'Authorization': f"Bearer {self.sf_access_token}", 'Content-Type': 'application/json' }
    end_point ="https://cremaconsulting-dev-ed.develop.my.salesforce.com"
    service = "/services/data/v57.0/"
    r = requests.request("GET", end_point+service+f"query/?q=SELECT+Id+FROM+{object}", headers=sf_headers, data={})
    assert isinstance(r.json(), dict), f"response: {r.json()}, header: {sf_headers}"
    object_ids = [d.get('Id') for d in r.json()['records']]
    while r.json()['done'] == False:
        new_url = end_point+r.json()['nextRecordsUrl']
        print(new_url)
        r = requests.request("GET", new_url, headers=sf_headers, data={})
        print((r.json()))
        fresh_object_ids = [d.get('Id') for d in r.json()['records']]
        print(len(fresh_object_ids))   
        object_ids+=fresh_object_ids
        
    print('total number of objects = ',len(object_ids))
    return object_ids


# %% ../crema_sf.ipynb 13
@patch
def delete_sf_objects(self: Salesforce, 
                      obj_s: str = 'Contact'
                      ):
    object_ids = self.get_sf_object_ids(obj_s)
    with open('objs2delete.csv', 'w') as f:
        f.write('Id\n')
        for id in object_ids:
            f.write(id+'\n')
    job_id = self.create_job( obj_s, 'delete').json()['id']
    print("Job id is: ", job_id)
    self.upload_csv('objs2delete.csv')
    sleep(2)
    self.close_job()
    sleep(10)
    self.successful_results()
        


# %% ../crema_sf.ipynb 14
@patch
def test_sf_object_load_and_delete(self: Salesforce, 
        sf_object_s : str = None, # Salesforce API endpoint
        input_file_s: str = None, # local file name
        remove_sf_objs: bool = False # remove the data just added to Salesforce
        ):
    """Test loading a Salesforce object with data from a local file"""
    assert sf_object_s
    assert input_file_s

    # sf.create_job('MembershipMembers__c', contentType='CSV')
    self.create_job(sf_object_s, contentType='CSV')
    print("Salesforce job id: ", self.bulk_job_id)

    #replace 
    # culprit is \u2019 - it cannot be encoded in latin-1 codec
    self.upload_csv(input_file_s)
    
        

    self.close_job()
    self.failed_results()
    self.successful_results()
    self.job_status()

    if remove_sf_objs:
        self.delete_sf_objects('membershipTerm__c')
