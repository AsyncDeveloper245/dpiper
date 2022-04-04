"""
Dpiper is a mini ETL data pipeline that extracts , transforms and loads data to destination sources
"""
import pandas as pd
from extractor import PiperExtractorBase
from loader import DpiperLoader
from transformer import PiperTransformerBase    

def get_file_extension(file):
    file_parts = file.split('.')
    file_type = file_parts[1]
    return file_type

def get_file_from_path(file_path):
    file_parts = file_path.split('/')
    file_ = file_parts[-1]
    return file_

def main():
    data_source: str = input("Enter Data Source ('webapi','file','url','db')>> ")
    if data_source.lower() == 'webapi':
        credentials = {}
        credential_ = input("Enter credentials ('api_key','access-token')>> ")
        credentials['Authorization'] = credential_
        headers = {}

        print(f"Collecting Headers............")
        num_of_headers = int(input("Number of headers: "))
        for i in range(num_of_headers):
            header_: str = input("Enter headers (a dictionary)>> ")
            header = header_.split(":")
            headers[header[0]] = header[1]

        print('Creating the Extractor Object...........')
        extractor_pipe = PiperExtractorBase(credentials=credentials,data_source=data_source,headers=headers)
        print("Object {} created".format(extractor_pipe))

        print("..................................................")
        api_url: str = input("Enter API Endpoint to query:>> ")
        print(f"Extracting data from api endpoint of url {api_url}")
        extracted_data = extractor_pipe.extract(url=api_url)

        print(extracted_data)


    elif data_source.lower() == 'file':
        extractor_pipe = PiperExtractorBase(data_source=data_source)
        print(f"Extracting data from local filesystem")
        num_of_files = int(input('Enter number of files to extract: '))
        files_ = {}
        if num_of_files > 1:
            for i in range(num_of_files):
                file_path = input("Enter File Path: ")
                file_type = get_file_extension(get_file_from_path(file_path))
                extracted_data = extractor_pipe.extract(file_path=file_path,file_type=file_type)
                transformer_pipe = PiperTransformerBase(extracted_data,file_type)

                data = transformer_pipe.transform()
                print(data)
                
        elif num_of_files==1:
            file_path = input("Enter File Path: ")
            file_type = get_file_extension(get_file_from_path(file_path))
            print(file_type)
            extracted_data = extractor_pipe.extract(file_path=file_path,file_type=file_type)
            print(extracted_data)
            transformer_pipe = PiperTransformerBase(extracted_data,file_type)

            data = transformer_pipe.transform()
            print(data)
        else:
            raise Exception("Supply a file path")
        


    elif data_source.lower() == 'url':
        url = input("Enter Url to Query: ")
        element_to_scrape = input("Enter Elements you want to scrape seperated by commas in order of precendence. ")
        elements_to_scrape = element_to_scrape.split(',')
        print(" querying --->   https://{}".format(url))
        extractor_pipe = PiperExtractorBase(data_source='url')
        data = extractor_pipe.extract(url=url,elements_to_scrape=elements_to_scrape)
        # transformer_pipe = PiperTransformerBase(data,data_type='df')
        # data = transformer_pipe.transform()
        print(data)


    elif data_source.lower() == 'db':
        pass

    else:
        raise Exception("Invalid data source")
    




    


    #load_pipe = DpiperLoader()

    





if __name__ == '__main__':
    main()





    