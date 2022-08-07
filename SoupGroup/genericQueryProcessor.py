
from classesDataModel import *
from pandas import *
from relationalQueryProcessor import *
from rdflib import *
from triplestoreFunctions import *
from triplestoreQueryProcessor import *
from genericFunctions import *


class GenericQueryProcessor(object): 

    def __init__(self):
        self.queryProcessor = []
        
    def cleanQueryProcessor(self): 
        self.queryProcessor = []
        return True

    def addQueryProcessor(self, processor):
        self.queryProcessor.append(processor)
        return True

    def getPublicationsPublishedInYear(self, year):
        publications_by_year = set()
        venues_obj_dict = {}
        impossible_objects = set()
        
        publication_objects_dict = {}
        query_processor_df = DataFrame({})

        for processor in self.queryProcessor: 
            temp_df = processor.getPublicationsPublishedInYear(year)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("")

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                publications_by_year.add(publication_obj)
            else:
                if row["publication_id"] not in impossible_objects:
                    print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")
                    impossible_objects.add(row["publication_id"])

        return list(publications_by_year)

    def getPublicationsByAuthorId(self, author_id): 
        
        publications_by_author_id = set()
        publication_objects_dict = {}
        query_processor_df = DataFrame({})
        venues_obj_dict = {}

        for processor in self.queryProcessor: 
            temp_df = processor.getPublicationsByAuthorId(author_id)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                publications_by_author_id.add(publication_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.") 
                 
        return list(publications_by_author_id)

    def getMostCitedPublication(self):

        venues_obj_dict = {}

        query_processor_df = DataFrame({})
        for processor in self.queryProcessor: 
            temp_df = processor.getMostCitedPublication()
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="citation").fillna("").drop_duplicates()

        count_dict = {}
        publication_objects_dict = {}

        for idx, row in query_processor_df.iterrows():
            if row["citation"] not in count_dict:
                count_dict[row["citation"]] = 1
            else: 
                count_dict[row["citation"]] += 1

        most_cited_info_tuple = getMostCitedInDataframe(count_dict, publication_objects_dict, self.queryProcessor, venues_obj_dict)
        most_cited_object = most_cited_info_tuple[0]
        return most_cited_object
        
    def getMostCitedVenue(self):

        venues_obj_dict = {}

        query_processor_df = DataFrame({})
        for processor in self.queryProcessor: 
            temp_df = processor.getMostCitedVenue()
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="citation").fillna("").drop_duplicates()

        count_dict = {}
        publication_objects_dict = {}
        venues_count = {}

        for idx, row in query_processor_df.iterrows():
            if row["citation"] not in count_dict:
                count_dict[row["citation"]] = 1
            else: 
                count_dict[row["citation"]] += 1

        for doi in count_dict:
            publication_info = createPublicationObject(doi, publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                venue_obj = publication_obj.getPublicationVenue()
                if venue_obj != None:
                    if venue_obj not in venues_count:
                        venues_count[venue_obj] = count_dict[doi]
                    else:
                        venues_count[venue_obj] += count_dict[doi]
            else:
                print("The object with id:", doi, "cannot be created because some required data are missing.")
                print("This means that also the venue in which it has been published cannot be considered in searching for the most cited venue.") 
        
        most_cited_venue = max(venues_count, key=venues_count.get)

        return most_cited_venue
            
    def getVenuesByPublisherId(self, publisher_id):

        venues_by_publisher_id = set()
        venue_ids_set = set()
        venues_obj_dict = {
            "id": set(),
            "type": "",
            "title":"",
            "publisher":""
        }
        publication_objects_dict = {}

        """
        v = {
            doi1:{
                ven_tit:"",
                ven_id:"",
                ven_type:""
            }
        }
        """

        publisher_dict = {
            "publisher_id": publisher_id,
            "publisher_name": ""
        }

        query_processor_df = DataFrame({})
        for processor in self.queryProcessor:
            temp_df = processor.getVenuesByPublisherId(publisher_id)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        

        for idx, row in query_processor_df.iterrows():
            if publisher_dict["publisher_name"] == "":
                if row["name"] != "":
                    publisher_dict["publisher_name"] = row["name"]
            if row["venue_id"] not in venue_ids_set:
                pass



            if venues_obj_dict["title"] == "" and row["venue_title"] != "":
                    venues_obj_dict["title"] = row["venue_title"]
            if venues_obj_dict["type"] == "" and row["venue_type"] != "":
                    venues_obj_dict["type"] = row["venue_type"]
            venue_ids_df = DataFrame({})
            for processor in self.queryProcessor:
                temp_df = processor.getVenuesById(row["venue_id"])
                venue_ids_df = concat([venue_ids_df, temp_df])
            venue_ids_df = venue_ids_df.drop_duplicates()
            for idx, row in venue_ids_df.iterrows():
                venues_obj_dict["id"].add(row["venue_id"])
            
                
            
            




            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                venue_obj = publication_obj.getPublicationVenue()
                venues_by_publisher_id.add(venue_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")
                print("This means that also the venue in which it has been published cannot be considered in searching for the relation between \n therefore, neither the link between publisher and venue has been considered.")

        return list(venues_by_publisher_id)
    
    def getPublicationInVenue(self, venue_id):

        venues_obj_dict = {}
        
        publication_in_venue = set()
        publication_objects_dict = {}
        query_processor_df = DataFrame({})

        for processor in self.queryProcessor:
            temp_df = processor.getPublicationInVenue(venue_id)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                publication_in_venue.add(publication_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")
                print("Therefore, the connection that it has with the input venue cannot be considered.")
            

        return list(publication_in_venue)

    def getJournalArticlesInIssue(self, issue_value, volume_value, venue_id):
        
        venues_obj_dict = {}
        journal_articles_in_issue = set()
        venue_ids = set()
        publication_objects_dict = {}

        query_processor_df = DataFrame({})
        for processor in self.queryProcessor:
            temp_df = processor.getJournalArticlesInIssue(issue_value, volume_value, venue_id)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                venue_obj = publication_obj.getPublicationVenue()
                for venue_single_id in venue_obj.getIds():
                    venue_ids.add(venue_single_id)
                if venue_id in venue_ids:
                    if publication_obj.getIssue() == issue_value:
                        if publication_obj.getVolume() == volume_value:
                            journal_articles_in_issue.add(publication_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")

        return list(journal_articles_in_issue)

    def getJournalArticlesInVolume(self, volume_value, venue_id):
        
        venues_obj_dict = {}
        journal_articles_in_volume = set()
        venue_ids = set()
        publication_objects_dict = {}
        query_processor_df = DataFrame({})

        for processor in self.queryProcessor:
            temp_df = processor.getJournalArticlesInVolume(volume_value, venue_id)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                venue_obj = publication_obj.getPublicationVenue()
                for venue_single_id in venue_obj.getIds():
                    venue_ids.add(venue_single_id)
                if venue_id in venue_ids:
                    if publication_obj.getVolume() == volume_value:
                            journal_articles_in_volume.add(publication_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")

        return list(journal_articles_in_volume)

    def getJournalArticlesInJournal(self, venue_id):
        
        venues_obj_dict = {}
        journal_articles_in_journal = set()
        venue_ids = set()
        publication_objects_dict = {}
        query_processor_df = DataFrame({})

        for processor in self.queryProcessor:
            temp_df = processor.getJournalArticlesInJournal(venue_id)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                venue_obj = publication_obj.getPublicationVenue()
                for venue_single_id in venue_obj.getIds():
                    venue_ids.add(venue_single_id)
                if venue_id in venue_ids:
                    journal_articles_in_journal.add(publication_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")
                print("Therefore, it cannot be added to the result.")

        return list(journal_articles_in_journal)

    def getProceedingsByEvent(self, event_input):

        venues_obj_dict = {}
        proceedings_by_event = set()
        publication_objects_dict = {}
        query_processor_df = DataFrame({})

        for processor in self.queryProcessor:
            temp_df = processor.getProceedingsByEvent(event_input)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                venue_obj = publication_obj.getPublicationVenue()
                proceedings_by_event.add(venue_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")
                print("Therefore, its venue cannot be added to the result.")

        return list(proceedings_by_event)

    def getPublicationAuthors(self, input_doi):

        venues_obj_dict = {}
        authors_of_publication = set()
        publication_objects_dict = {}
        query_processor_df = DataFrame({})

        for processor in self.queryProcessor:
            temp_df = processor.getPublicationAuthors(input_doi)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                authors_list_obj = publication_obj.getAuthors()
                for single_author in authors_list_obj:
                    authors_of_publication.add(single_author)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")
                print("Therefore, its author cannot be added to the result because the link bewteen objects is missing.")

        return list(authors_of_publication)

    def getPublicationsByAuthorName(self, author_name):
        
        venues_obj_dict = {}
        publication_by_author_name = set()
        publication_objects_dict = {}
        query_processor_df = DataFrame({})

        for processor in self.queryProcessor:
            temp_df = processor.getPublicationsByAuthorName(author_name)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                publication_by_author_name.add(publication_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")

        return list(publication_by_author_name)

    def getDistinctPublisherOfPublications(self, pubIdList):
        
        venues_obj_dict = {}
        publication_objects_dict = {}
        publishers_by_publications = set()
        query_processor_df = DataFrame({})

        for processor in self.queryProcessor:
            temp_df = processor.getDistinctPublisherOfPublications(pubIdList)
            query_processor_df = concat([query_processor_df, temp_df])
        query_processor_df = query_processor_df.sort_values(by="publication_id").fillna("").drop_duplicates()

        for idx, row in query_processor_df.iterrows():
            publication_info = createPublicationObject(row["publication_id"], publication_objects_dict, self.queryProcessor, venues_obj_dict)
            publication_obj = publication_info[0]
            venues_obj_dict = publication_info[1]
            if publication_obj != "empty":
                venue_obj = publication_obj.getPublicationVenue()
                publisher_obj = venue_obj.getPublisher()
                publishers_by_publications.add(publisher_obj)
            else:
                print("The object with id:", row["publication_id"], "cannot be created because some required data are missing.")
                print("Therefore, its publisher cannot be added to the result because the link bewteen objects is missing.")

        return list(publishers_by_publications)
            