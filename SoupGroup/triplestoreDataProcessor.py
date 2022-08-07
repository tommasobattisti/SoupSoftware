from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from triplestoreFunctions import * 
from URIs import *
from additionalClasses import *

    
class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):
        if type(path) == str:
            addiotional_data_path = name_additional_data_file(self.endpointUrl)
            soup_graph = Graph()
            deletions_graph = Graph()
            base_url = "https://github.com/EISG/DSP/"
            store = SPARQLUpdateStore() 

            if path[-4:] == ".csv":
                empty = True
                if csv_exists(self.endpointUrl):
                    empty = False
                    csv_to_csv(base_url, self.endpointUrl, store, soup_graph, deletions_graph, path, addiotional_data_path)
                if json_exists(self.endpointUrl):
                    empty = False
                    csv_to_json(base_url, self.endpointUrl, store, soup_graph, path, addiotional_data_path)
                if empty:
                    csv_upload(base_url, self.endpointUrl, store, soup_graph, path, addiotional_data_path)
                return True

            elif path[-5:] == ".json":
                empty = True
                if json_exists(self.endpointUrl):
                    empty = False
                    json_to_json(base_url, self.endpointUrl, store, soup_graph, deletions_graph, path)
                if csv_exists(self.endpointUrl):
                    empty = False
                    json_to_csv(base_url, self.endpointUrl, store, soup_graph, path, addiotional_data_path)
                if empty:
                    json_upload(base_url, self.endpointUrl, store, soup_graph, path)
                return True
            else:
                return False
        else:
            return False

