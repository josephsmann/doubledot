import requests
import json
# import observable_jupyter as obs
import jmespath as jp
import re
from time import sleep

## Module for Salesforce API

#| export
class Salesforce:
    def __init__(self):
        self._sf_access_token = self.get_salesforce_token()
        self.bulk_job_id = None

    
    @property
    def sf_access_token(self):
        """access token for Salesforce - verifies that token is still valid and attempts to get a new one if not

        Returns:
            string: the access token
        """
        if not(self.test_token()):
            self._sf_access_token = self.get_salesforce_token()
            # check to see if getting token worked
            assert (self.sf_access_token), "Fetching new token didn't fix problem"
        return self._sf_access_token
    
    def get_salesforce_token(self):
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
    
    def test_token(self):
        """Verify that token is still valid

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
        
    def create_job(self, sf_object: str ='Contact', operation: str ='insert', 
                   external_id: str = 'External_Id__c', contentType: str = 'CSV'):
        """Get job_id from Salesforce Bulk API

        Args: 
            sf_object (str, optional): the Salesforce object were going to operate on. Defaults to 'Contact'.
            operation (str, optional): the operation that will be used against the object. Defaults to 'insert'.
            external_id (str, optional): the external id field for upsert operations. Defaults to 'External_Id__c'.
            sf_object (str, optional): the Salesforce object were going to operate on. Defaults to 'Contact'.
            operation (str, optional): the operation that will be used against the object. Defaults to 'insert'.
            external_id (str, optional): the external id field for upsert operations. Defaults to 'External_Id__c'.
            contentType (str, optional): the content type of the file. Defaults to 'CSV', 'JSON' also accepted.
        Returns: 
            response: a response object containg the job_id. For more information on the response object see https://www.w3schools.com/python/ref_requests_response.asp
            a response object see https://www.w3schools.com/python/ref_requests_response.asp
            
        Salesforce API docs: https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/create_job.htm    
        """
        
        url = "https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest"

        # https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/datafiles_prepare_csv.htm
        ## we can set columnDelimiter to `,^,|,;,<tab>, and the default <comma>
        # sets the object to Contact, the content type to CSV, and the operation to insert
        payload_d = {
            "object": sf_object,
            "contentType": contentType,
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

    def upload_csv(self, file):
        """Using the job_id from the previous step, upload the csv file to the job

        Args:
            file (filepointer): file pointer to the csv file
        """
        # if not(file):
        #     # throw error
        #     assert False, "File not found"
        assert file, "File not found"

        url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/batches"

        payload = file #open('contacts_20K.csv', 'rb').read() ##  small payload 500 rows
        headers = {
        'Content-Type': 'text/csv',
        'Authorization': f'Bearer {self.sf_access_token}'
        }

        response = requests.request("PUT", url, headers=headers, data=payload)
        ## Need error handling here
        

        print("response: ", response.text)
        
    def close_job(self):
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
             
    # get job status (from Postman)
    def job_status(self):
        url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}"

        payload = {}
        headers = {
        'Authorization': f'Bearer {self.sf_access_token}'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        return response.json()

    def successful_results(self):
        url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/successfulResults"

        payload = {}
        headers = {
            'Authorization': f'Bearer {self.sf_access_token}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        print( response.text)

    # get failed results (from Postman)
    def failed_results(self):
        url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/failedResults"

        payload = {}
        headers = {
            'Authorization': f'Bearer {self.sf_access_token}'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        print( response.text)
    
    # get Ids of all contacts
    def get_sf_object_ids(self, object: str = 'Contact'):
        """Delete all objects (default "Contact") from Salesforce that I own

        Args:
        """
        sf_headers = { 'Authorization': f"Bearer {self.sf_access_token}", 'Content-Type': 'application/json' }
        end_point ="https://cremaconsulting-dev-ed.develop.my.salesforce.com"
        service = "/services/data/v57.0/"
        r = requests.request("GET", end_point+service+f"query/?q=SELECT+Id+FROM+{object}", headers=sf_headers, data={})
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

    def delete_sf_objects(self, obj_s: str = 'Contact'):
        object_ids = self.get_sf_object_ids(obj_s)
        with open('objs2delete.csv', 'w') as f:
            f.write('Id\n')
            for id in object_ids:
                f.write(id+'\n')
        job_id = self.create_job( obj_s, 'delete').json()['id']
        print("Job id is: ", job_id)
        with open('objs2delete.csv', 'rb') as f:
            self.upload_csv(f)
        sleep(2)
        self.close_job()
        sleep(10)
        self.successful_results()
            

class ATMS_api:
    def __init__(self):
        self.telus_access_token = ATMS_api.get_atms_authentication()
        self.obj_d = {}
        
    
    
    @staticmethod    
    def get_atms_authentication():
        """get access token for ATMS API

        Returns:
            response object: response object from the API call 
        """
        vantix_url = "http://crm-api-telus.atmsplus.com/auth"
        
        with open('secrets.json') as f:
            secrets = json.load(f)

        payload = json.dumps({
            "username": secrets['vantix_user'],
            "password": secrets['vantix_pw'],
            "rememberMe": True
        })
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", vantix_url, headers=headers, data=payload)
        assert response.status_code == 200, f"response code is {response.status_code}, not 200"
        return response.json().get('access_token')

                
    #make a function that takes a string and returns a string with the original semi-colon separated emails replaced with a list of emails with quotes around each
    @staticmethod
    def __mutate_email_list(s):
        pat_s = r"""\"emails\": \[.*?address\": (\"(?P<email1>.*?)\")"""
        pattern=re.compile(pat_s)
        matches = re.search(pattern,s) 
        if matches and matches.group(0) and matches.group(1) and matches.group(2):
            og_emails_list_s = matches.group(2)
            emails_l = [f"\"{email}\"" for email in og_emails_list_s.split(';')]
            # emails_l = [f"{email}" for email in og_emails_list_s.split(';')]
            emails_list_s = '[' +','.join(emails_l)+']'
            return re.sub(matches.group(1), emails_list_s,s)
        else:
            return s

    def __get_telus_data(self, obj, offset=0, count=1000):
        """retrieve data from ATMS API, should be private method

        Args:
            obj (string): api endpoint to retrieve data from
            offset (int, optional): first row to begin retrieval. Defaults to 0.
            count (int, optional): number of rows to retrieve. Defaults to 1000.

        Returns:
            response object: response object from the API call
        """
        vantix_data_url = f"http://crm-api-telus.atmsplus.com/api/{obj}?offset={offset}&count={count}"
        v_headers = {'Authorization': f"Bearer {self.telus_access_token}"}

        print(vantix_data_url)    
        response = requests.request("GET", vantix_data_url, headers=v_headers, data={}).json()
        
        # inform caller we're done if we get fewer records than requested
        return {"response": response, "done":  len(response) < count}

    def write_obj_to_file(self, obj, initial_offset=0, max_offset=2000, count=1000):
        """Retrieve data from ATMS API and write to file
           public method

        Args:
            obj (string): a valid ATMS REST API object
            max_offset (int, optional): starting row to begin retrieval. Defaults to 2000.
            count (int, optional): maximum number of rows to retieve. Defaults to 1000.
        """
        done = False
        offset = initial_offset 
        filename_s = f'atms_{obj}.json'
        print("Writing to file: ", filename_s)
        with open(filename_s, 'w') as f:
            f.write("[ \n")
            print('max_offset: ', max_offset)
            print('offset: ', offset)
            while (not done and  offset < max_offset):
                print('offset: ', offset)
                # read another file
                resp_d = self.__get_telus_data(obj,offset=offset, count=count)
                obj_l = resp_d['response']
                done = resp_d['done'] #or offset > max_offset
                for i,r in enumerate(obj_l):
                    ## this is so ugly...
                    line_ending = "\n" if done or (offset >= max_offset-offset and i>=len(obj_l)-1) else ",\n"
                    f.write(json.dumps(r)+ line_ending)
                offset += count
            f.write("]")

        # write_atms_obj_to_file('items', max_offset=1000, count=1000)
    
    
    # def clean_obj_file(self, obj_s : str): 
    def clean_data_file(self, obj_s : str): 
        """clean up the atms_contacts.json file, this is necessary before loading into Saleforce
        
        """
        # read original contacts file   
        in_filename_s = f'atms_{obj_s}.json'
        print("Cleaning file: ", in_filename_s)
        with open(in_filename_s,'r') as f:
            # write modified contacts file 
            out_filename_s = f'atms_transformed_{obj_s}.json'
            print("Writing file: ", out_filename_s)
            with open(out_filename_s,'w') as f2:
                s = f.read()
                for l in s.split('\n'):
                    new_s = ATMS_api.__mutate_email_list(l)+'\n'
                    f2.write(new_s)
                
    # make dict from json file
    def load_data_file_to_dict(self, obj_s : str):
        """load data from file into a dictionary

        Args:
            obj_s (str): name of the object to load
        """
        file_name_s = f'atms_transformed_{obj_s}.json'
        print('Loading file: ', file_name_s)
        with open(file_name_s,'r') as f2:
            s2 = f2.read()
            self.obj_d[obj_s] = json.loads(s2)
                