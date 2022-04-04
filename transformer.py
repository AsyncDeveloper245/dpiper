import pandas as pd
import numpy
import json
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline  import Pipeline


def convert_to_df(data,data_type: str = ''):
        if data_type == 'json':
            df = pd.read_json(data)
            # df = pd.DataFrame({'data':df})
            return df

        elif data_type == 'csv':
            df = pd.read_csv(data)
            return df

        elif data_type == 'tsv':
            df = pd.read_tsv(data)
            # df = pd.DataFrame({'data':df})
            return df
        elif data_type == 'xlsx':
            df = pd.read_excel(data)
            # df = pd.DataFrame({'data':df})
            return df

        elif data_type == 'df':
            pass

        else:
            return Exception('Invalid data type')



class PiperTransformerBase(object):
    def __init__(self,data: str = None,data_type: str=None):
        self.data = data
        self.data_type = data_type
    # todo: Transform data by removing null values and scaling the dataframe if necessary.
    #? Function to convert all incoming data to a dataframe , for more easier transformation.
    # todo: try and add a function to parse html data

    

    # todo : add a function to manipulate different parts of the dataset
    # todo : add a function to join different dataframes.

    def transform(self):
        data_ = convert_to_df(self.data,self.data_type)
        imputed_df = self.remove_null_values(data_)
        if  not imputed_df.columns:
            df = pd.DataFrame(imputed_df,columns=data_.columns)
            return df

        else:
            return df

    def remove_null_values(self,df):
        imputer = SimpleImputer(strategy='most_frequent')
        df = imputer.fit_transform(df)
        return df


    def handle_nominal_data(self,df,column):
        le = LabelEncoder()
        df[column] = le.fit_transform(df[column])
        print(f"Data Transformed Successfully")

    def handle_ordinal_data(self,df,column):
    # pipeline = Pipeline()
        pass








    # @staticmethod
    # def transform_data_from_webapi(self,data,column_name):
    #     assert type(data) == dict,"Invalid data type {0}".format(type(data))
    #     df = convert_to_df(data)

    #     return df


    # @staticmethod
    # def transform_data_from_file(self,data,column_name):
    #     assert type(data) == dict,"Invalid data type {0}".format(type(data))
    #     df = convert_to_df(data)



    # @staticmethod
    # def transform_data_from_db(self,data,column_name):
    #     assert type(data) == dict,"Invalid data type {0}".format(type(data))
    #     df = convert_to_df(data)


    # @staticmethod
    # def transform_data_from_url(self,data,column_name):
    #     assert type(data) == dict,"Invalid data type {0}".format(type(data))
    #     df = convert_to_df(data)