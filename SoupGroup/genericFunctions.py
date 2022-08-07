from classesDataModel import *
from pandas import *
from relationalQueryProcessor import *
from rdflib import *
from triplestoreFunctions import *
from triplestoreQueryProcessor import *

    

def getPublicationObject(doi, citations_dict, df, query_processors_list, venues_obj_dict):
    
    authors = {}
    authors_objects = {}

    venues = {}
    venues_ids = venues_obj_dict
    venues_ids_set = set()

    publishers = {}

    publications = {}
    citations_dict = citations_dict

    i = 0
    venue_type_found = False
    for idx, row in df.iterrows():
        i += 1
        if row["publication_id"] not in publications:
            publication_has_type = False
            if row["publication_type"] == "journal-article":
                publication_has_type = True
                publications[row["publication_id"]] = {"type":"JA", 
                                                        "authors":set(), 
                                                        "venues_ids":set(),
                                                        "publisher":row["publisher_id"], 
                                                        "title":row["publication_title"],
                                                        "publication_year":row["publication_year"],
                                                        "issue":row["issue"], 
                                                        "volume":row["volume"],
                                                        "citations":set()}
                

            elif row["publication_type"] == "book-chapter":
                publication_has_type = True
                publications[row["publication_id"]] = {"type":"BC", 
                                                        "authors":set(), 
                                                        "venues_ids":set(),
                                                        "publisher":row["publisher_id"], 
                                                        "title":row["publication_title"],
                                                        "publication_year":row["publication_year"],
                                                        "chapter":row["chapter_number"],
                                                        "citations":set()}
                                      
                
            elif row["publication_type"] == "proceedings-paper":
                publication_has_type = True
                publications[row["publication_id"]] = {"type":"PP", 
                                                        "authors":set(), 
                                                        "venues_ids":set(),
                                                        "publisher":row["publisher_id"], 
                                                        "title":row["publication_title"],
                                                        "publication_year":row["publication_year"],
                                                        "citations":set()}
               
            elif row["publication_type"] == "":
                publications[row["publication_id"]] = {"authors":set(), 
                                                        "venues_ids":set(),
                                                        "citations":set()}

        # CITATIONS
        if row["citation"] not in publications[row["publication_id"]]["citations"] and row["citation"] != "":
            publications[row["publication_id"]]["citations"].add(row["citation"])  

        # AUTHORS
        if row["publication_id"] not in authors:
            authors[row["publication_id"]] = {}
        if row["author_id"] in authors_objects:
            if row["author_id"] not in authors[row["publication_id"]]:
                single_author = authors_objects[row["author_id"]]
                authors[row["publication_id"]]["author_id"] = single_author
        else: # row["author_id"] non è mai stato incontrato
            if row["author_id"] != "":
                single_author_id_list = []
                single_author_id_list.append(row["author_id"])
                single_author = Person(single_author_id_list, row["given_name"], row["family_name"])
                authors_objects[row["author_id"]] = single_author
                authors[row["publication_id"]][row["author_id"]] = single_author
                publications[row["publication_id"]]["authors"].add(single_author)
            
                    
        # PUBLISHERS
        if row["publisher_id"] != "": # questo serve perche se non abbiamo un publisher allora la publication non ha una venue
            if row["publisher_id"] not in publishers:
                if row["name"] != "":
                    single_publisher_id_list = []
                    single_publisher_id_list.append(row["publisher_id"])
                    single_publisher = Organization(single_publisher_id_list, row["name"])
                    publishers[row["publisher_id"]] = single_publisher
                else: #row["publisher_name"] == "" -> l'id del publisher lo abbiamo sia in CSV che in JSON, ma il name solo in JSON
                    for secondary_processor in query_processors_list:
                        x = secondary_processor.getPublisherById(row["publisher_id"])
                        for idx, publisher_row in x.iterrows():
                            if publisher_row["name"] != "":
                                single_publisher_id_list = []
                                single_publisher_id_list.append(row["publisher_id"])
                                single_publisher = Organization(single_publisher_id_list, row["name"])
                                publishers[row["publisher_id"]] = single_publisher
                                break
                        if row["publisher_id"] in publishers:
                            break
            
            # VENUES
            # sono nel caso in cui un publisher sta gia nel df
            if row["publication_id"] not in venues:
                if row["venue_id"] not in venues_ids: 
                    venues_df = DataFrame({})
                    for secondary_processor in query_processors_list:
                        x = secondary_processor.getVenuesByDoi(row["publication_id"]) 
                        venues_df = concat([venues_df, x])
                    venues_df = venues_df.sort_values(by="publication_id").drop_duplicates()

                    for idx, venues_row in venues_df.iterrows():
                        if venues_row["venue_id"] != "":
                            venues_ids_set.add(venues_row["venue_id"])
                    if venues_ids_set != set():
                        venue_ids_df = DataFrame({})
                        for secondary_processor in query_processors_list:
                            x = secondary_processor.getVenuesById(venues_ids_set) #metodo che restituisce gli id delle venue sulla base del publisher che le ha pubblicate
                            venue_ids_df = concat([venue_ids_df, x])
                        venue_ids_df = venue_ids_df.sort_values(by="venue_id").drop_duplicates()

                        for idx, venues_row in venue_ids_df.iterrows():
                            if venues_row["venue_id"] != "":
                                venues_ids_set.add(venues_row["venue_id"])
                                if venues_row["venue_id"] not in venues_ids:
                                    venues_ids[venues_row["venue_id"]] = ""

                    if not venue_type_found:
                        if row["venue_type"] == "journal":
                            venue_type_found = True
                            single_venue = Journal(venues_ids_set, row["venue_title"], publishers[row["publisher_id"]])
                            venues[row["publication_id"]] = single_venue
                            for venue_id in venues_ids_set:
                                if venue_id in venues_ids:
                                    venues_ids[venues_row["venue_id"]] = single_venue
                        elif row["venue_type"] == "book":
                            venue_type_found = True
                            single_venue = Book(venues_ids_set, row["venue_title"], publishers[row["publisher_id"]])
                            venues[row["publication_id"]] = single_venue
                            for venue_id in venues_ids_set:
                                if venue_id in venues_ids:
                                    venues_ids[venues_row["venue_id"]] = single_venue
                        elif row["venue_type"] == "proceedings":
                            venue_type_found = True
                            single_venue = Proceedings(venues_ids_set, row["venue_title"], publishers[row["publisher_id"]], row["event"])
                            venues[row["publication_id"]] = single_venue
                            for venue_id in venues_ids_set:
                                if venue_id in venues_ids:
                                    venues_ids[venues_row["venue_id"]] = single_venue
                        else:
                            single_venue = ""

                        if venues_ids_set != set():
                            for venue_id in venues_ids_set:
                                if single_venue != "":
                                    venues_ids[venue_id] = single_venue
                                    publications[row["publication_id"]]["venues_ids"].add(single_venue)
                else:
                    single_venue = venues_ids[row["venue_id"]] 
                    venues[row["publication_id"]] = single_venue
                    publications[row["publication_id"]]["venues_ids"].add(single_venue)

            

        
        if not publication_has_type:
            if row["publication_type"] != "":
                publications[row["publication_id"]]["title"] = row["publication_title"]
                publications[row["publication_id"]]["publication_year"] = row["publication_year"]
                publications[row["publication_id"]]["publisher"] = row["publisher_id"]

                if row["publication_type"] == "journal-article":
                    publications[row["publication_id"]]["type"] = "JA"
                    publications[row["publication_id"]]["issue"] = row["issue"]
                    publications[row["publication_id"]]["volume"] = row["volume"]
                elif row["publication_type"] == "book_chapter":
                    publications[row["publication_id"]]["type"] = "BC"
                    publications[row["publication_id"]]["chapter"] = row["chapter_number"]
                elif row["publication_type"] == "proceedings-paper":
                    publications[row["publication_id"]]["type"] = "PP"
                publication_has_type = True
            
            
        if i == len(df):
            if not publication_has_type:
                # non esiste la publication
                publication_obj = "empty"
                return (citations_dict, publication_obj, venues_ids)
            if publications[row["publication_id"]]["authors"] != set():
                # Ho una venue e il publisher
                if publications[row["publication_id"]]["publisher"] in publishers: #prima c'era anche il != 0
                    #manca il check sulle venue
                    publisher_obj = publishers[publications[row["publication_id"]]["publisher"]]
                    publications[row["publication_id"]]["publisher"] = publisher_obj
                    if publications[row["publication_id"]]["venues_ids"] != []:
                        # we have a venue
                        venue_obj = venues[row["publication_id"]]
                else: # publisher == ""
                    publisher_obj = None
                    venue_obj = None
                authors_objs = publications[row["publication_id"]]["authors"]
                doi_list = []
                if publications[row["publication_id"]]["type"] == "JA":
                    doi_list.append(row["publication_id"])
                    publication_obj = JournalArticle(
                                                        doi_list, 
                                                        int(publications[row["publication_id"]]["publication_year"]), 
                                                        publications[row["publication_id"]]["title"], 
                                                        authors_objs, 
                                                        venue_obj, 
                                                        publications[row["publication_id"]]["issue"],
                                                        publications[row["publication_id"]]["volume"]
                                                    )
                    citations_dict[doi] = publication_obj
                    
                elif publications[row["publication_id"]]["type"] == "BC":
                    doi_list.append(row["publication_id"])
                    publication_obj = BookChapter(
                                                        doi_list, 
                                                        int(publications[row["publication_id"]]["publication_year"]), 
                                                        publications[row["publication_id"]]["title"], 
                                                        authors_objs, 
                                                        venue_obj, 
                                                        int(publications[row["publication_id"]]["chapter"])
                                                    )
                    citations_dict[doi] = publication_obj

                elif publications[row["publication_id"]]["type"] == "PP":
                    doi_list.append(row["publication_id"])
                    publication_obj = ProceedingsPaper(
                                                        doi_list, 
                                                        int(publications[row["publication_id"]]["publication_year"]), 
                                                        publications[row["publication_id"]]["title"], 
                                                        authors_objs, 
                                                        venue_obj
                                                    )
                    citations_dict[doi] = publication_obj
            else:
                #publication non esiste
                publication_obj = "empty"
                return (citations_dict, publication_obj, venues_ids) 
            venues_ids_set = set()
            doi_list = []
            # iterare sui doi citation di ogni oggetto in publications
            citations_dict = searchForCitationObject(publications, citations_dict, query_processors_list, venues_obj_dict) 
        
    return (citations_dict, publication_obj, venues_ids)

def createPublicationObject(doi, publication_objects_dict, query_processors_list, venues_obj_dict):
    venues_obj_dict = venues_obj_dict
    if doi not in publication_objects_dict:
        publication_info_df = DataFrame({})
        for secondary_processor in query_processors_list:
            temp_df = secondary_processor.getPublicationByDoi(doi)
            publication_info_df = concat([publication_info_df, temp_df])
        publication_info_df = publication_info_df.sort_values(by="publication_id").fillna("").drop_duplicates()
        check_tuple = getPublicationObject(doi, publication_objects_dict, publication_info_df, query_processors_list, venues_obj_dict)
        publication_objects_dict = check_tuple[0]
        publication_obj = check_tuple[1]
        venues_obj_dict = check_tuple[2]
    else:
        publication_obj = publication_objects_dict[doi]
    
    return (publication_obj, venues_obj_dict)

def searchForCitationObject(publications_dict, citations_dict, query_processors_list, venues_obj_dict):
    venues_obj_dict = venues_obj_dict
    for doi in publications_dict:
        if doi in citations_dict:
            for cited_doi in publications_dict[doi]["citations"]:
                # controllare se l'oggetto esiste già e in caso usare metodo addCitation
                if cited_doi in citations_dict:
                    citations_dict[doi].addCitedPublication(citations_dict[cited_doi])
                # se non esiste uso il metodo getPubByDoi per crearlo e aggiungerlo al dizionario delle citation
                else:
                    citations_processor_df = DataFrame({})
                    for processor in query_processors_list: 
                        x = processor.getPublicationByDoi(cited_doi)
                        # quando lo trovo uso metodo add citation
                        citations_processor_df = concat([citations_processor_df, x])
                    citations_processor_df = citations_processor_df.sort_values(by="publication_id").fillna("")
                    
                    check_tuple = getPublicationObject(cited_doi, citations_dict, citations_processor_df, query_processors_list, venues_obj_dict)
                    citations_dict = check_tuple[0]
                    publication_obj = check_tuple[1]
                    if publication_obj == "empty":
                        #our citation cannot be created
                        print("\n\n---------------------------------\nerror:\n", "The publication with id:", cited_doi, "can not be cited because there are not enough data to define the actual publication object! \n This missing object may cause cascade errors in data related to citations. \n Check your data.\n---------------------------------\n")
                        #raise Exception("The publication with id:", cited_doi, "can not be cited because there are not enough data to define the actual publication object! \n This missing object blocks the return method because the software cannot create a publication object, necessary in a cascade way to define the object you are asking to return. \n Check your data and try again.")   
                    else:
                        citations_dict[doi].addCitedPublication(citations_dict[cited_doi]) 
    return citations_dict

def addPublicationToDict(row, publications_dict, no_type_dict):
    if row["publication_type"] == "journal-article" or str(row["publication_type"]) == "http://purl.org/spar/fabio/JournalArticle":
        publications_dict[row["publication_id"]] = {
                                                "type":"JA", 
                                                "authors":[], 
                                                "publisher":row["publisher_id"], 
                                                "title":row["publication_title"],
                                                "venues_ids":[], 
                                                "publication_year":row["publication_year"], 
                                                "issue":row["issue"], 
                                                "volume":row["volume"],
                                                "citations":[]
                                            }
    elif row["publication_type"] == "book-chapter" or str(row["publication_type"]) == "http://purl.org/spar/fabio/BookChapter":
        publications_dict[row["publication_id"]] = {
                                                "type":"BC",
                                                "authors":[], 
                                                "publisher":row["publisher_id"], 
                                                "title":row["publication_title"],
                                                "venues_ids":[],  
                                                "publication_year":row["publication_year"], 
                                                "chapter":row["chapter_number"],
                                                "citations":[]
                                            }
    elif row["publication_type"] == "proceedings-paper" or str(row["publication_type"]) == "http://purl.org/spar/fabio/ProceedingsPaper":
        publications_dict[row["publication_id"]] = {
                                                "type":"PP",
                                                "authors":[], 
                                                "publisher":row["publisher_id"], 
                                                "title":row["publication_title"],
                                                "venues_ids":[],  
                                                "publication_year":row["publication_year"],
                                                "citations":[]
                                            }
    elif row["publication_type"] == "":
        no_type_dict[row["publication_id"]] = True
        publications_dict[row["publication_id"]] = {
                                                "type":"",
                                                "authors":[], 
                                                "publisher":"", 
                                                "title":"",
                                                "venues_ids":[],  
                                                "publication_year":"",
                                                "citations":[]
                                            }

    return publications_dict, no_type_dict

def updatePublicationWithType(row, publications_dict, no_type_dict):
    if row["publication_type"] != "":
        publications_dict[row["publication_id"]]["title"] = row["publication_title"]
        publications_dict[row["publication_id"]]["publication_year"] = row["publication_year"]
        publications_dict[row["publication_id"]]["publisher"] = row["publisher_id"]

        if row["publication_type"] == "journal-article":
            publications_dict[row["publication_id"]]["type"] = "JA"
            publications_dict[row["publication_id"]]["issue"] = row["issue"]
            publications_dict[row["publication_id"]]["volume"] = row["volume"]
        elif row["publication_type"] == "book_chapter":
            publications_dict[row["publication_id"]]["type"] = "BC"
            publications_dict[row["publication_id"]]["chapter"] = row["chapter_number"]
        elif row["publication_type"] == "proceedings-paper":
            publications_dict[row["publication_id"]]["type"] = "PP"
        del no_type_dict[row["publication_id"]]

    return publications_dict, no_type_dict

def getMostCitedInDataframe(count_dict, publication_objects_dict, query_processors_list, venues_obj_dict):
    venues_obj_dict = venues_obj_dict
    most_cited = max(count_dict, key=count_dict.get)
        
    df = DataFrame({})

    for processor in query_processors_list:
        temp_df = processor.getPublicationByDoi(most_cited)
        df = concat([df, temp_df])
    df = df.sort_values(by="citation").fillna("")

    most_cited_object_info = getPublicationObject(most_cited, publication_objects_dict, df, query_processors_list, venues_obj_dict)
    publication_objects_dict = most_cited_object_info[0]
    most_cited_object = most_cited_object_info[1]

    if most_cited_object == "empty":
        print("In theory, the most cited id should have the id:", most_cited, "but is not possible to define the actual object because some data are missing.")
        print("The software will check for the following most cited id and return its object if it can be created.")
        del count_dict[most_cited]
        getMostCitedInDataframe(count_dict, query_processors_list, query_processors_list, venues_obj_dict)
    else:
        return (most_cited_object, count_dict, publication_objects_dict)