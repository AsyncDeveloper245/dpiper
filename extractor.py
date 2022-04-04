import json
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

class PiperExtractorBase(object):
    # todo : extract data from data sources based on the method supplied.
    #! What type of data source to handle?
    def __init__(self,data_source: str ='',credentials: dict = None,method: str = 'GET',data: dict = None,headers: dict = None):
        self.data_source = data_source
        self.credentials = credentials
        self.method = method
        self.data = data
        self.headers = headers
        #self.base_url = base_url


    def extract(self,file_path: str = None,url: str = None,elements_to_scrape: tuple = (),auth_type: str = None,column_name: str = 'data',query_parameters: str = '',file_type: str = None,):
        if self.data_source == 'webapi':
            #! credentials is either the Api-Key,Access-Token or the Refresh Token
            headers =  self.headers.update(self.credentials)
            return self.extract_from_webapi(url = url,headers=headers,data=self.data)

        elif self.data_source == 'file':
            return self.extract_from_file(file_path=file_path,file_type=file_type)



        elif self.data_source == 'db':
            return self.extract_from_db()
        

        elif self.data_source == 'url':
            self.base_url=url
            
            return self.extract_from_url(elements_to_scrape=elements_to_scrape,column_name=column_name,query_parameters=query_parameters)

        else:
            raise Exception('Invalid data source')




    @staticmethod    
    def check_response_code(response):
        if response.status_code == 200:
                return {"status":'success','data':response.json()}
        elif response.status_code == 404:
                return {'status': 'Not Found'}

        elif response.status_code == 401:
                return {'status': 'Unauthorized'}

        elif response.status_code == 500:
                return {'status': 'Internal Server Error'}



    def extract_from_webapi(self,url,headers,data=None,method='GET'):
        if data is None:
            response = requests.get(url,headers=headers)
            return check_response_code(response)
            

        else:
            response = requests.get(url,headers=headers,data=data)
            return check_response_code(response)



    def extract_from_file(self,file_type: str = 'txt',file_path: str =None):
        assert file_type in ['txt','json','csv','xlsx'], 'Invalid file type'
        if file_type == 'txt':
            if file_path:
                with open(file_path,'r') as f:
                    data = f.read()
                    return data
            else:
                raise Exception('Invalid file path')

        elif file_type == 'json':
            if file_path:
                with open(file_path,'r') as f:
                    data = json.load(f)
                    return data

            else:
                raise Exception('Invalid file path')

        elif file_type == 'csv':
            if file_path:
                return file_path
            else:
                raise Exception('Invalid file path')
        
        elif file_type == 'xlsx':
            if file_path:
                df = pd.read_excel(file_path)
                return df
            else:
                raise Exception('Invalid file path')

    def extract_from_db(self):
        # todo : extract data from database with supplied db credentials.
        raise Exception('Not implemented')


    def extract_from_url(self,elements_to_scrape: tuple = (),column_name: str = 'data',query_parameters: str = ''):
        #! Elements to scrape is a tuple that contains html tags in the order they need to be scraped.
        # todo: scrape data from the supplied url
        #query_parameters: str = ''
        #? Use query parameters to add additional url params in the form of ?param1=value1&param2=value2
        url = self.base_url + query_parameters
        soup = bs(requests.get(url).content,'html.parser')

        assert len(elements_to_scrape) > 0, 'No elements to scrape'
        # todo : implement a longer deep element version of this.

        data_ = [data.find(elements_to_scrape[1]) for data in soup.findAll(elements_to_scrape[0])]
        print(data_)
        new_data = [data.text for data in data_ if data]
        new_data = [data for data in new_data if data != '']
        df = pd.DataFrame({column_name:new_data},index=range(1,len(new_data)+1))
        return df#.to_csv(f'{column_name}.txt', sep='t')


    def __str__(self):
        return f"PiperExtractorBase(data_source={self.data_source})"
