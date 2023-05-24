# AUTOGENERATED! DO NOT EDIT! File to edit: ../crema_sf.ipynb.

# %% auto 0
__all__ = ['mem_s', 'memTerm_s', 'memMembers_s', 'search_s', 'Salesforce', 'escape_quotes', 'match_df']

# %% ../crema_sf.ipynb 4
import io
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
from doubledot.ATMS_api import ATMS_api as ATMS
import time

# %% ../crema_sf.ipynb 6
## Module for Salesforce API

class Salesforce:
    """Class for Salesforce API"""
    class_download_dir = os.path.join(os.getcwd(),'sf_download')
    class_upload_dir = os.path.join(os.getcwd(),'sf_upload')
    transfer_lock = False # lock to prevent multiple transfers at once - not implemented yet. but probably necessary to work with withh nbdev_test 

    ## Salesforce table relationships 
    ## these are also orderd by dependency
    model_d = {     'Contact'               :{ 'lookups_d': dict(), 'external_id':'contactId__c'},
                    'Membership__c'         :{ 'lookups_d': dict(), 'external_id':'membershipId__c'},
                    'MembershipTerm__c'     :{ 'lookups_d': {'membershipKey__c': 'Membership__c'}, 'external_id':'membershipTermId__c'},
                    'MembershipMember__c'   :{ 'lookups_d': {'contactKey__c': 'Contact', 'membershipTermKey__c': 'MembershipTerm__c' }, 'external_id':'membershipMemberId__c'},
                    # in SF change saleId__c to saleId__c
                    'Sale__c'               :{ 'lookups_d': {'booking_contactKey__c': 'Contact'}, 'external_id':'saleId__c'},
                    # in SF change membershipTermKey__c to membershipTermKey__c
                    'SaleDetail__c'         :{ 'lookups_d': {'membershipTermKey__c':'MembershipTerm__c', 'saleId__c': 'Sale__c'}, 'external_id':'saleDetailId__c'},
                    # in SF change tickeKey__c to ticketId__c
                    # change saleId__c to saleKey__c in Ticket__c
                    'Ticket__c'             :{ 'lookups_d': {'saleKey__c': 'Sale__c', 'saleDetailKey__c': 'SaleDetail__c'}, 'external_id':'ticketId__c'} 
            }

    def __init__(self):
        # set up access token 
        self._sf_access_token = self.get_token_with_REST()
        self.bulk_job_id = None
        self._atms = None

        # create unique download directory per instance
        if not os.path.exists(Salesforce.class_download_dir):
            os.makedirs(Salesforce.class_download_dir)
            print(f"Directory 'atms_download' created successfully.")
        else:
            print(f"Directory 'atms_download' already exists.")


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


    @property
    def atms( self):
        if not self._atms:
            self._atms = ATMS()
        return self._atms
    
    @staticmethod
    def list_files():
        return os.listdir(Salesforce.class_download_dir)

show_doc(Salesforce.sf_access_token)


   

# %% ../crema_sf.ipynb 8
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



# %% ../crema_sf.ipynb 10
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
    


# %% ../crema_sf.ipynb 12
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
    payload = json.dumps(payload_d)
    
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {self.sf_access_token}'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    # print(response.text)
    try:
        self.bulk_job_id = response.json()['id']
    except TypeError:
        print("Bad response in Salesforce.create_job :\n", response.text)
        
    print(f"Created job {self.bulk_job_id} for {sf_object} with operation {operation}") 
    return response 


# %% ../crema_sf.ipynb 14
@patch
def upload_csv(self : Salesforce, 
                obj_s: str = "", # Salesforce object to upload 
                # num_rows: int = 100, # the number of rows to upload 
                ) -> requests.Response:
    """Using the job_id from the previous step, upload the csv file to the job

    Args:
        file (filepointer): file pointer to the csv filek
    """
    # if not(file):
    #     # throw error
    #     assert False, "File not found"


    assert obj_s in ['Contact', 'Membership__c', 'MembershipTerm__c', 'MembershipMember__c', 'Sale__c', 'Ticket__c', 'SaleDetail__c']

    print(f"Uploading job {self.bulk_job_id} of object {obj_s}")

    # file_path_s = os.path.join(Salesforce.class_download_dir , f"{obj_s}.csv")
    file_path_s = os.path.join(Salesforce.class_upload_dir , f"{obj_s}.csv")

    url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/batches"

    # replace all occurrences of '\2019' with \'
    # we may have done this in ATMS already, but just in case
    try:
        for line in fileinput.input(files=file_path_s, inplace=True):
            line = line.replace('\u2019', "'")
            print(line, end='') # this prints to the file instead of stdout (!!)

        with open(file_path_s,'r') as payload:
            headers = {
                'Content-Type': 'text/csv',
                'Authorization': f'Bearer {self.sf_access_token}'
                }
            response = requests.request("PUT", url, headers=headers, data=payload)
    except FileNotFoundError:
        print("File not found error in Saleforce.upload_csv: ", file_path_s)
        return None
    
    return response
   

# %% ../crema_sf.ipynb 16
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

    # print(response.text)
    return response.json()
     

# %% ../crema_sf.ipynb 18
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



# %% ../crema_sf.ipynb 20
@patch
def successful_results(self : Salesforce):
    url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/successfulResults"

    payload = {}
    headers = {
        'Authorization': f'Bearer {self.sf_access_token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    
    return response


# %% ../crema_sf.ipynb 22
@patch
def failed_results(self: Salesforce):
    url = f"https://cremaconsulting-dev-ed.develop.my.salesforce.com/services/data/v57.0/jobs/ingest/{self.bulk_job_id}/failedResults"

    payload = {}
    headers = {
        'Authorization': f'Bearer {self.sf_access_token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    # 
    return response


# %% ../crema_sf.ipynb 24
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


# %% ../crema_sf.ipynb 26
@patch
def delete_sf_objects(self: Salesforce, 
                      obj_s: str = 'Contact'
                      ):
    """Delete Salesforce objects"""
    # assert False, "want to catch test calls to this function"
    print(f"Deleting {obj_s} objects from Salesforce")
    object_ids = self.get_sf_object_ids(obj_s)
    file_path_s = os.path.join(Salesforce.class_download_dir , f"{obj_s}.csv")
    print(f"In Salesforce.delete_sf_objects: Deleting {len(object_ids)} {obj_s} objects using {file_path_s}")
    with open(file_path_s, 'w') as f:
        f.write('Id\n')
        for id in object_ids:
            f.write(id+'\n')
            
    # force execute_job to use the csv file we just created        
    self.execute_job(obj_s, 'delete', use_ATMS_data=False)
        


# %% ../crema_sf.ipynb 28
@patch
def test_sf_object_load_and_delete(self: Salesforce, 
        sf_object_s : str = None, # Salesforce API endpoint
        input_file_s: str = None, # local file name
        remove_sf_objs: bool = False # remove the data just added to Salesforce
        ):
    """Test loading a Salesforce object with data from a local file"""
    assert sf_object_s
    assert input_file_s
    assert False, "This function hasn't been maintained"

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

# %% ../crema_sf.ipynb 29
def escape_quotes(text):
    # Escape single quotes
    # text = re.sub(r"\'", r"\\'", text)
    text = re.sub(r"\'", r"_", text)
    # Escape double quotes
    text = re.sub(r'\"', r'_', text)
    # text = re.sub(r',', r'*', text) ## shouldn't be necessary with tab delimiter
    # text = re.sub(r'\"', r'\\"', text)
    return text.strip()

# %% ../crema_sf.ipynb 31
 #### modify so parent fields use correct shit 
#### maybe use a spreadsheet to make life easier 
# Name,Mother_Of_Child__r.contactId
# CustomObject1,123456

mem_s = "[].{membershipId__c: membershipId, \
    memberSince__c: memberSince, \
    updateDate__c: updateDate}"

memTerm_s = "[].membershipTerms[].{membershipTermId__c: membershipTermId,\
membershipKey__r_1_membershipId__c:membershipKey,\
effectiveDate__c:effectiveDate,\
expiryDate__c:expiryDate,\
membershipType__c:membershipType,\
upgradeFromTermKey__c:upgradeFromTermKey,\
giftMembership__c:giftMembership,\
refunded__c:refunded,\
saleDetailKey__c:saleDetailKey,\
itemKey__c:itemKey}"

memMembers_s = "[].membershipTerms[].membershipMembers[].{membershipMemberId__c:membershipMemberId,\
membershipTermKey__r_1_membershipTermId__c:membershipTermKey,\
cardNumber__c:cardNumber,\
membershipNumber__c:membershipNumber,\
cardStatus__c:cardStatus,\
contactKey__r_1_contactId__c:contactKey,\
displayName__c:displayName}"

@patch
def process_memberships(self: Salesforce ):
    """Unpack memberships data from atms object and write to membership, membership_terms, and membership_members csv files.
    
    We could modify this function to only process one of Memmbership, MembershipTerm, or MembershipMember.
    """
    print("Processing memberships data")
    # custom objects need '__c' suffix
    mem_d = { 'memberships': {'fname':'Membership__c.csv', 'jmespath': mem_s},
               'membership_terms': {'fname':'MembershipTerm__c.csv','jmespath': memTerm_s},
               'membership_members': {'fname': 'MembershipMember__c.csv', 'jmespath': memMembers_s}
                }
            

    if not ('memberships' in self.atms.obj_d):
        self.atms.load_data_file_to_dict('memberships')
        assert 'memberships' in self.atms.obj_d, f"memberships not in atms.obj_d {self.atms.obj_d.keys()}"
    
    atms_d = self.atms.obj_d['memberships']

    for key, v_pair in mem_d.items():
        file_path_s = os.path.join(Salesforce.class_download_dir, v_pair['fname'])
        dict_l = jp.search(v_pair['jmespath'], atms_d)
        print(f"Salesforce: Writing {len(dict_l)} {key} objects to {file_path_s}")
        with open(file_path_s, 'w') as f:
            if len(dict_l) == 0:
                print(f"Warning: no {key} objects found")
                continue
            # hack to create header with a dot in it, jmespath won't do it
            f.write('\t'.join([s.replace('_1_','.') for s in dict_l[0].keys()]) + '\n') # header
            for d in dict_l:
                #changed this to not write None for empty values, eg "" for null and false (a default value)
                f.write('\t'.join([str(v) if v else "" for v in d.values()]) + '\n')
    

# %% ../crema_sf.ipynb 33
@patch
def process_sales(self: Salesforce ):
    search_s = "[].{saleId__c : saleKey,\
            saleAmount__c : saleAmount,\
            paymentAmount__c : paymentAmount,\
            saleDate__c : saleDate,\
            active__c : active,\
            terminalKey__c : terminalKey,\
            booking_bookingId__c   : booking.bookingId,\
            booking_bookingContactKey__c : booking.bookingContactKey,\
            booking_contactKey__r_1_contactId__c : booking.contactKey,\
            booking_contactIndividualKey__c : booking.contactIndividualKey,\
            booking_contactOrganizationKey__c : booking.contactOrganizationKey,\
            booking_displayName__c : booking.displayName,\
            booking_firstName__c : booking.firstName,\
            booking_lastName__c : booking.lastName,\
            booking_email__c : booking.email,\
            booking_phone__c : booking.phone}"
            # eventDate__c : eventDate,\
            # booking_contactKey__c : booking_contactKey,\
    
    assert 'sales' in self.atms.obj_d, f"sales not in atms.obj_d {self.atms.obj_d.keys()}"
    dict_l = jp.search(search_s, self.atms.obj_d['sales'])

    file_path_s = os.path.join(Salesforce.class_download_dir, 'Sale__c.csv')

    print(f"Salesforce: Writing {len(dict_l)} 'Sales' objects to {file_path_s}")
    columnDelimiter = '\t'
    with open(file_path_s, 'w') as f:
        # hack to create header with a dot in it, jmespath won't do it
        header = '\t'.join([s.replace('_1_','.') for s in dict_l[0].keys()])
        f.write(header + '\n') # header
        for item in dict_l:
            # changed this from single space to empty string if null
            l = [escape_quotes(str(v)) if v else "" for v in item.values()]
            f.write(columnDelimiter.join(l)+'\n')


        

# %% ../crema_sf.ipynb 35
@patch
def process_tickets(self: Salesforce ):
    search_s = "[].tickets[].{ticketId__c : ticketKey,\
        saleKey__r_1_saleId__c : saleKey,\
        saleDetailKey__r_1_saleDetailId__c : saleDetailKey,\
        itemDescription__c : itemDescription,\
        ticketDisplay__c : ticketDisplay}"
        # scheduleDate__c : scheduleDate,\
        # scheduleEndDate__c : scheduleEndDate,\

    assert 'sales' in self.atms.obj_d, f"sales not in atms.obj_d {self.atms.obj_d.keys()}"
    dict_l = jp.search(search_s, self.atms.obj_d['sales'])

    file_path_s = os.path.join(Salesforce.class_download_dir, 'Ticket__c.csv')

    print(f"Salesforce: Writing {len(dict_l)} 'Ticket' objects to {file_path_s}")
    columnDelimiter = '\t'
    with open(file_path_s, 'w') as f:
        # hack to create header with a dot in it, jmespath won't do it
        header = '\t'.join([s.replace('_1_','.') for s in dict_l[0].keys()])
        f.write(header + '\n') # header
        for item in dict_l:
            # changed this from single space to empty string if null
            l = [escape_quotes(str(v)) if v else "" for v in item.values()]
            f.write(columnDelimiter.join(l)+'\n')


        

# %% ../crema_sf.ipynb 37
@patch
def process_saleDetails(self: Salesforce ):
    search_s = "[].saleDetails[].{\
        saleDetailId__c : saleDetailId,\
        itemKey__c : itemKey,\
        scheduleKey__c : scheduleKey,\
        rateKey__c : rateKey,\
        categoryKey__c : categoryKey,\
        itemCategory__c : itemCategory,\
        pricingPriceKey__c : pricingPriceKey,\
        itemPrice__c : itemPrice,\
        itemTotal__c : itemTotal,\
        couponTotal__c : couponTotal,\
        discountTotal__c : discountTotal,\
        total__c : total,\
        revenueDate__c : revenueDate,\
        refundReason__c : refundReason,\
        refundReasonKey__c : refundReasonKey,\
        systemPriceOverride__c : systemPriceOverride,\
        membershipTermKey__r_1_membershipTermId__c : membershipTermKey,\
        saleId__r_1_saleId__c : saleId}" 
    
    assert 'sales' in self.atms.obj_d, f"sales not in atms.obj_d {self.atms.obj_d.keys()}"
    dict_l = jp.search(search_s, self.atms.obj_d['sales'])

    file_path_s = os.path.join(Salesforce.class_download_dir, 'SaleDetail__c.csv')

    print(f"Salesforce: Writing {len(dict_l)} 'SaleDetail' objects to {file_path_s}")
    columnDelimiter = '\t'
    with open(file_path_s, 'w') as f:
        # hack to create header with a dot in it, jmespath won't do it
        header = '\t'.join([s.replace('_1_','.') for s in dict_l[0].keys()])
        f.write(header + '\n') # header
        for item in dict_l:
            # changed this from single space to empty string if null
            l = [escape_quotes(str(v)) if v else "" for v in item.values()]
            f.write(columnDelimiter.join(l)+'\n')


        

# %% ../crema_sf.ipynb 40
search_s = "[].{LastName: lastName,\
    FirstName: firstName,\
    MailingPostalCode: addresses[0].postalZipCode,\
    MailingCity: addresses[0].city,\
    MailingStreet: addresses[0].line1, \
    MailingCountry: addresses[0].country, \
    Phone: phones[?phoneType == 'Business'].phoneNumber | [0],\
    Email: emails[0].address[0],\
    contactId__c: contactId}"

import re



@patch
def process_contacts(self: Salesforce ):
    """ unpack contacts data from atms object and write to contacts csv file."""
    print("process_contacts")
    if not ('contacts' in self.atms.obj_d):
        self.atms.load_data_file_to_dict('contacts')
        assert 'contacts' in self.atms.obj_d, f"contacts not in atms.obj_d {self.atms.obj_d.keys()}"
    
    file_path_s = os.path.join(Salesforce.class_download_dir, 'Contact.csv')
    dict_l = jp.search(search_s, self.atms.obj_d['contacts'])

    # if contact record has no LastName then
    for r in dict_l:
        if r['LastName'] == None:
            r['LastName'] = 'Not Provided'

    print(f"Salesforce: Writing {len(dict_l)} 'Contact' objects to {file_path_s}")
    columnDelimiter = '\t'
    with open(file_path_s, 'w') as f:
        header = columnDelimiter.join(dict_l[0].keys())
        f.write(header+'\n')
        for item in dict_l:
            l = [escape_quotes(str(v)) if v else " " for v in item.values()]
            f.write(columnDelimiter.join(l)+'\n')

# %% ../crema_sf.ipynb 42
@patch
def process_objects(self: Salesforce,
                    sf_object_s: str = "",
                    use_ATMS_data: bool = True
                    ):

    valid_obj_l = ['Contact', 'Membership__c', 'MembershipTerm__c', 'MembershipMember__c', 'Sale__c', 'Ticket__c','SaleDetail__c']
    if sf_object_s not in valid_obj_l:
        print(f"sf_object_s must be one of {', '.join(valid_obj_l)}")
        return
        
    print("Salesforce.process_objects: sf_object_s :",sf_object_s)

    if sf_object_s == 'SaleDetail__c' and use_ATMS_data:
        # this creates a file Sales__c.csv in the class_download_dir
        self.process_saleDetails()

    if sf_object_s == 'Ticket__c' and use_ATMS_data:
        # this creates a file Sales__c.csv in the class_download_dir
        self.process_tickets()

    if sf_object_s == 'Sale__c' and use_ATMS_data:
        # this creates a file Sales__c.csv in the class_download_dir
        self.process_sales()
    
    if sf_object_s == 'Contact' and use_ATMS_data:
        # this creates a file Contact.csv in the class_download_dir
        self.process_contacts()
    
    if sf_object_s in ['Membership__c', 'MembershipMember__c', 'MembershipTerm__c'] and use_ATMS_data:
        # this creates files Membership__c.csv, MembershipMember__c.csv, MembershipTerm__c.csv in the class_download_dir
        self.process_memberships()

# %% ../crema_sf.ipynb 44
@patch
def execute_job(self: Salesforce, 
        sf_object_s : str = None, # Salesforce API object name
        operation : str = None, # REST operation, e.g. insert, upsert, delete
        max_trys : int = 20, # max number of times to try to get job status
        external_id : str = 'External_Id__c', # name of the field that is the unique identifier to ATMS
        use_ATMS_data : bool = True, # if True, update SF data directly from ATMS data, otherwise use previous data
        # **kwargs : dict # additional parameters to pass to the REST API
        ):
    """Test loading a Salesforce object with data from a local file"""
    print("execute_job")
    

    ## translate dictionaries to csv files
    self.process_objects(sf_object_s=sf_object_s, use_ATMS_data=use_ATMS_data)

    ## start data transfer to Salesforce server
    self.create_job(sf_object=sf_object_s, operation=operation, external_id=external_id)
    self.upload_csv(sf_object_s)
    self.close_job()

    counter = 0
    sleep_time = 3
    
    job_status = self.job_status()['state']
    print("job status:", self.job_status()['state'])

    while job_status !='JobComplete' and job_status !='Failed' and counter < max_trys:
        print(f"waiting for job to complete, try {counter}, status: {self.job_status()['state']}")
        counter += 1
        time.sleep(sleep_time)
        job_status = self.job_status()['state']

    print("Failed results:")
    print(self.failed_results().text)

# %% ../crema_sf.ipynb 50
@patch
def retreive_atms_records_by_contactId(
    self: Salesforce, # the Salesforce object
    contactId_l : list # list of contactIds to retrieve
    ):
    """Retreive ATMS records for a list of contactIds and write them to json files"""
    for obj in ['sales', 'contacts', 'memberships']:
        # does this not get written to file?, no. it does not. and we don't want it to because we're working on proccessing rn.
        sf.atms.fetch_data_by_contactIds(obj, contactId_l) 

    # write data to json files
    sf.atms.write_data_to_json_files()

# %% ../crema_sf.ipynb 103
## this will have only contacts that are members, and only members that are contacts. 
## is possible to have duplicates if a contact is a member of more than one membership?
## yes but the duplicated fields will only be in one group or the other
## so we separate them and then reduce them
## but we should really just make a function for this...

## given two dataframes and two fields return two dataframes that are subsets of the original dataframes for which the two fields match
def match_df(df1, df2, field1, field2):
    df1 = df1[df1[field1].isin(df2[field2])]
    df2 = df2[df2[field2].isin(df1[field1])]
    return df1, df2

# nContact_df, nMembershipMember_df = match_df(Contact_df, MembershipMember_df, 'External_Id__c', 'contactKey__r.External_Id__c')
# nnMembershipMember_df, nMembershipTerm_df = match_df(nMembershipMember_df, MembershipTerm_df, 'membershipTermKey__r.membershipTermId__c', 'membershipTermId__c')
# nnMembershipTerm, nMembership_df = match_df(nMembershipTerm_df, Membership_df, 'membershipKey__r.membershipId__c', 'membershipId__c')

# nnContact_df, nnnMembershipMember_df = match_df(nContact_df, nnMembershipMember_df, 'External_Id__c', 'contactKey__r.External_Id__c')
# fContact_df, fMembershipMember_df, fMembershipTerm_df, fMembership_df = nnContact_df, nnnMembershipMember_df, nnMembershipTerm, nMembership_df
