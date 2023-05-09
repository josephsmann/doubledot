# AUTOGENERATED! DO NOT EDIT! File to edit: ../ATMS_api.ipynb.

# %% auto 0
__all__ = ['ATMS_api']

# %% ../ATMS_api.ipynb 2
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
from fastcore.test import test_eq
import glob
import time
import random

# %% ../ATMS_api.ipynb 3
class ATMS_api:
    class_download_dir = os.path.join(os.getcwd(),'atms_download')

    def __init__(self):
        self.telus_access_token = ATMS_api.get_atms_authentication()
        self.obj_d = {}

        # create unique download directory per instance
        if not os.path.exists(ATMS_api.class_download_dir):
            os.makedirs(ATMS_api.class_download_dir)
            print(f"Directory 'atms_download' created successfully.")
        else:
            print(f"Directory 'atms_download' already exists.")

        # generate a unique directory name with a random string
        random_string = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))
        self.download_dir  = os.path.join(ATMS_api.class_download_dir, random_string)

        # Check if the new director already exists in the directory
        while os.path.exists(self.download_dir):
            # If the directory name already exists, generate a new one
            random_string  = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))
            self.download_dir  = os.path.join(ATMS_api.class_download_dir, random_string)
        
        os.mkdir(self.download_dir)
        self.id = random_string
        print("my id is", self.id)
        
    def list_files(self):
        """ returns list of files in download folder """
        print('id is:', self.id)
        if os.path.exists(self.download_dir):
            return os.listdir(self.download_dir)
        else:
            return []

    @staticmethod
    def delete_all_data():
        """ delete all the subdirectories and files in the download directory """
        for dir_path in glob.glob(os.path.join(ATMS_api.class_download_dir, "*")):
            if os.path.isdir(dir_path):
                for file_path in glob.glob(os.path.join(dir_path, "*")):
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        print(f"File '{file_path}' deleted successfully.")
                os.removedirs(dir_path) 
                print(f"Directory '{dir_path}' deleted successfully.")  
        print(f"Directory '{ATMS_api.class_download_dir}' deleted successfully.")   

    
    def clean_data_dir(self,
                       obj_s: str = None):
        glob_s = os.path.join(self.download_dir, f"*{obj_s if obj_s else ''}*.json")
        file_l = glob.glob(glob_s)
        # print(file_l)
        for file_path in file_l:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"File '{file_path}' deleted successfully.")


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
    def _mutate_email_list(s:str # a string like: ' ... "emails": [ ... "address": "pam.jenkins@blackgold.ca;don.what@this.com"}], ...  '
                            ) -> str : # the same string but with ' ... "address": ["pam.jenkins@blackgold.ca", "don.what@this.com"]} ...
        """ 

        """
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
    
    def get_telus_data(self, 
                        obj: str, # telus endpoint 
                        offset :int = 0, # the row to begin retrieval
                        since_date: str = "", # optionally, the date from which to retrieve
                        count :int =1000 # the number of rows to retrieve
                        ):
        """retrieve data from ATMS API, should be private method

        Args:
            obj (string): api endpoint to retrieve data from
            offset (int, optional): first row to begin retrieval. Defaults to 0.
            count (int, optional): number of rows to retrieve. Defaults to 1000.

        Returns:
            response object: response object from the API call
        """
        vantix_data_url = f"http://crm-api-telus.atmsplus.com/api/{obj}?offset={offset}&count={count}"
        if len(since_date)> 0:
            vantix_data_url = f"http://crm-api-telus.atmsplus.com/api/{obj}/lastupdate?count={count}&offset={offset}&updateDate={since_date}"

        v_headers = {'Authorization': f"Bearer {self.telus_access_token}"}

        # print(vantix_data_url)    
        response = requests.request("GET", vantix_data_url, headers=v_headers, data={}).json()
        
        # inform caller we're done if we get fewer records than requested
        return {"response": response, "done":  len(response) < count}

    def retrieve_and_clean(self, 
                          obj : str = 'contacts', # ATMS object to retrieve
                          initial_offset : int =0, # start retrieval at row initial_offset
                          rows_per_batch :int =1000, # number records retrieved at once
                          since_date : str = "", # if given, it will be used instead of `initial_offset` 
                          max_rows :int = 2000 # maximum number of rows to retrieve
                          ):
        """Retrieve data from ATMS API, clean data and write to file"""
        self.write_obj_to_file(obj, initial_offset, rows_per_batch, since_date, max_rows)
        self.clean_data_file(obj)

    def write_obj_to_file(self, 
                          obj : str = 'contacts', # ATMS object to retrieve
                          initial_offset : int =0, # start retrieval at row initial_offset
                          rows_per_batch :int =1000, # number records retrieved at once
                          since_date : str = "", # if given, it will be used instead of `initial_offset` 
                          max_rows :int = 2000 # maximum number of rows to retrieve
                          ):
        """Retrieve data from ATMS API and write to file
           private method

        Args:
            obj (string): a valid ATMS REST API object
            rows_per_batch (int, optional): maximum number of rows to retieve. Defaults to 1000.
        """
        done = False
        # offset is the starting row for the next batch
        offset = initial_offset 

        filename_s = f'atms_{obj}.json'
        print('download dir is: ', self.download_dir)
        file_path_s = os.path.join(self.download_dir, filename_s)
        # print("Writing to file: ", file_path_s)
        
        with open(file_path_s, 'w') as f:
            f.write("[ \n")
            num_rows_for_next_batch = min(rows_per_batch, max_rows)
            max_remaining_rows = max_rows
            num_rows_for_next_batch = min(rows_per_batch, max_remaining_rows) 
            first_line = True
            while (not done and (num_rows_for_next_batch > 0)):
                # print('offset: ', offset)
                # print('num rows already loaded: ', offset - initial_offset)
                # print('num_rows_for_next_batch: ', num_rows_for_next_batch)
                # print('max remaining rows: ', max_remaining_rows)

                # read another batch
                # the since_date parameter just acts like an initial offset, in theory we should be able 
                # to test it by making a small rows_per_batch 
                
                print(f"resp_d = self.get_telus_data({obj},offset={offset}, count= {num_rows_for_next_batch}, since_date={since_date})")
                resp_d = self.get_telus_data(obj,offset=offset, count= num_rows_for_next_batch, since_date=since_date)
                obj_l = resp_d['response']
                done = resp_d['done'] 
                s = ",\n".join([json.dumps(o) for o in obj_l])
                if first_line:
                    f.write(s)
                    first_line = False
                else:
                    f.write(",\n"+s)
                offset += rows_per_batch
                max_remaining_rows = max_rows - (offset - initial_offset)
                num_rows_for_next_batch = min(rows_per_batch, max_remaining_rows)
            f.write("\n]")
    
    
    # def clean_obj_file(self, obj_s : str): 
    def clean_data_file(self, 
                        obj_s : str
                        ): 
        """ Massage formatting to work with Salesfore Bulk API
        
        """
        # read original contacts file   
        in_filename_s = f'atms_{obj_s}.json'
        in_file_path_s = os.path.join(self.download_dir, in_filename_s)
        print("cleaning_data_file - download dir is: ", self.download_dir)
        # print("Cleaning file: ", in_file_path_s)
        try:
            with open(in_file_path_s,'r') as f:
                # write modified contacts file 
                out_filename_s = f'atms_transformed_{obj_s}.json'
                out_file_path_s = os.path.join(self.download_dir, out_filename_s)
                print("creating file: ", out_file_path_s)
                with open(out_file_path_s,'w') as f2:
                    s = f.read()
                    for l in s.split('\n'):
                        # remove "O`Brien" problem
                        l2 = re.sub('\u2019',"'",l)
                        # fix emails
                        new_s = ATMS_api._mutate_email_list(l2)+'\n'
                        f2.write(new_s)
                print(f"Finished cleaning {in_filename_s} -> {out_file_path_s}")
        except FileNotFoundError:
            print('the files in our download dir:', os.listdir(self.download_dir) )
            print('our in_file_path: ', in_file_path_s)
               
    
    def load_data_file_to_dict(
            self,
            obj_s : str # ATMS object. eg. contacts|items|memberships|membership
            ):
            """ `load_data_file_to_dict` will attempt to load a cleaned json file into a dict for future parsing. 
            If the cleaned file doesn't exist, it will look for a dirty one to clean.
            If the dirty once doesn't exist, it will raise an exception. It won't be downloaded because we don't know how much to get.
            
            """
            file_name_s = f'atms_transformed_{obj_s}.json'
            file_path_s = os.path.join(self.download_dir, file_name_s)
            print('Attempting to load: ', file_path_s)
            data=""
            try:
                with open(file_path_s, 'r') as f:
                    data = f.read()
            except FileNotFoundError:  # clean file not found
                print("File not found. Check that the dirty file is there")
                dirty_file_name_s = f'atms_{obj_s}.json'
                dirty_file_path_s = os.path.join(self.download_dir, dirty_file_name_s)

                if os.path.exists(dirty_file_path_s):
                    print(f"found dirty file: {dirty_file_path_s}")
                    self.clean_data_file(obj_s)
                    with open(file_path_s,'r') as f:
                        data = f.read()
                else:
                    raise FileNotFoundError # we give up
            finally: # this will be executed regardless of first 'try' failing or not
                if len(data) > 0:
                    try: # data might be buggy
                        self.obj_d[obj_s] = json.loads(data)
                    except json.JSONDecodeError:
                        print("We've got buggy data, or something")
                        raise Exception('Data is not JSON formatted')
                # assert obj_s in self.obj_d, f" '{obj_s}' not in {self.obj_d.keys()}"

            
