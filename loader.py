import json
import requests




class DpiperLoader(object):
    def __init__(self,data_to_load,destination : dict = None):
        # todo : check if the data type of the data_to_load variable is a data frame type
        #assert type(data_to_load) == pandas.DataType
        self.data_to_load = data_to_load
        self.destination = destination

    def load_to_db(self):
        raise Exception('Not Implememted.')

    def load_to_url(self):
        raise Exception('Not implemented.')

    @staticmethod
    def get_transformed_data():
        pass






