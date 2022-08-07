from pandas import *
from relationalUploadFunctions import *
import os
from additionalClasses import RelationalProcessor

class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        pass    

    def uploadData(self, path):           #connection to the database
        filesize = os.path.getsize(path) #filepath
        if filesize != 0:
            if path[-4:] == ".csv":
                upload_csv(path, self.dbPath)       #goes to the upload function for csv
                return True
            if path[-5:] == ".json": 
                upload_json(path, self.dbPath)       #it goes to the upload function for json
                return True
            else: 
                return False
        else: 
            raise Exception("This file is empty!")
