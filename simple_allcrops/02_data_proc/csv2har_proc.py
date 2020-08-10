#from GeoEDF.framework.processor.GeoEDFProcessorPlugin import GeoEDFProcessorPlugin
#from GeoEDF.framework.helper.GeoEDFError import GeoEDFError
import os
import harpy
from harpy import HarFileObj
from harpy import header_array
import numpy as np
import csv
import ntpath
import re
import pandas as pd

class CSV2HAR():
    data_names = []

    """ Module for converting csv files to har files """

    __required_params = ['csv']
    __optional_params = []
    # we use just kwargs since this makes it easier to instantiate the object from the
    # GeoEDFProcessor class
    def __init__(self, **kwargs):

        # check that all required params have been provided
        for param in self.__required_params:
            if param not in kwargs:
                raise GeoEDFError('Required parameter %s for CSV2HAR not provided' % param)

        # set all required parameters
        for key in self.__required_params:
            setattr(self, key, kwargs.get(key))

        # set optional parameters
        for key in self.__optional_params:
            # if key not provided in optional arguments, defaults value to None
            setattr(self, key, kwargs.get(key, None))

        self.l_val = list(kwargs.values())
       #super().__init__()


    # # constructor accepting beg and end years
    # def __init__(self, beg_year, end_year):
    #     self.start_year = beg_year
    #     self.end_year = end_year
    #
    #     # Datanames taken from R file (01_data_clean.R)
    #     self.data_names = ["INC","POP","QCROP","VCROP","QLAND"]
    # the process method that performs the masking and aggregating operation and saves resulting
    # shapefile to the target directory.
    def process(self):
        try:
            # source filepath of csv files
            # "/home/ubuntu/DURI/GeoEDF/JSON_CSV_HAR/SIMPLE-AllCrops-Base\ Data-07-16-2018/01_data_clean/01_data_clean.r"
            #csv_filename = self.path + str(year) + "/" + name + ".csv"
            csv_filename = ntpath.basename(self.l_val[0])
            #print('h1') 
            # destination filepath for har files
            #har_filename = self.output + str(year) + "/" + name + ".har"
            m = re.search(r'(.*)\.',csv_filename)
            har_filename = str(m.group(0)) + 'har'
            file_content = harpy.HarFileObj()
            #print('h2')
            # build har object
            data_frame = pd.read_csv(self.l_val[0], header=0, index_col=0)
            print (data_frame)
            names = data_frame.index.values.astype('<U12')
            values = data_frame.values.flatten().astype('float32')
            sets = [{'name': 'REG', 'status': 'k', 'dim_type': 'Set','dim_desc': names}]
            #print('h3')
            # first header
            har_obj= harpy.HeaderArrayObj.HeaderArrayFromData(name='SET1', long_name='Set REG inferred from CSV file', array=names, storage_type='FULL', data_type='1C')
            file_content.addHeaderArrayObj(har_obj, idx=None)
            #print('h4')
            # second header
            har_obj = harpy.HeaderArrayObj.HeaderArrayFromData(name='CSV', long_name='Set REG inferred from CSV file', array=values, coeff_name='CSV DATA', version=1, storage_type='Full', data_type='RE', sets=sets)
            #print('h5')
            file_content.addHeaderArrayObj(har_obj, idx=None)
            # write to disk
            file_content.writeToDisk(filename=har_filename)
            #print('h6')
        except:
            raise GeoEDFError('Error reprojecting input csvfile, cannot proceed with masking harfile data')





