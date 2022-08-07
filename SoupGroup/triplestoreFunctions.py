from rdflib import URIRef, Literal, RDF
from json import load, dump
from pandas import *
from sparql_dataframe import get
from os.path import exists
from collections import deque
from URIs import *
import re

# Queries for functions

query_publications_data = """
SELECT ?publication ?doi ?type ?publication_year ?publication_title ?issue ?volume ?chapter_number ?venue
WHERE {
    ?publication <http://purl.org/dc/terms/identifier> ?doi .

    OPTIONAL { ?publication a ?type }
    OPTIONAL { ?publication <http://purl.org/dc/terms/title> ?publication_title } 
    OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year }
    OPTIONAL { ?publication <https://schema.org/issueNumber> ?issue }
    OPTIONAL { ?publication <https://schema.org/volumeNumber> ?volume }
    OPTIONAL { ?publication <http://purl.org/spar/fabio/hasSequenceIdentifier> ?chapter_number }
    OPTIONAL { ?publication <https://schema.org/isPartOf> ?venue }
    FILTER regex ( ?doi, "^doi" )
}"""

query_publications_table = """
SELECT  ?publication ?doi ?venue ?venue_title ?publisher ?publisher_id 
WHERE {
        ?publication <http://purl.org/dc/terms/identifier> ?doi .
        {
            {
                ?publication <https://schema.org/isPartOf> ?venue .
                FILTER NOT EXISTS { ?venue <http://purl.org/dc/terms/title> ?venue_title } .
                OPTIONAL { ?venue <https://schema.org/publisher> ?publisher .
                                ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id } .
            }
            UNION
            {
                ?publication <https://schema.org/isPartOf> ?venue .
                ?venue <http://purl.org/dc/terms/title> ?venue_title .
            } .
            OPTIONAL { ?venue <https://schema.org/publisher> ?publisher .
                        ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id . }
        }
        UNION
        { FILTER NOT EXISTS {?publication <https://schema.org/isPartOf> ?venue } }
        
        FILTER regex ( ?doi, "^doi" )
    }
"""

query_publications_doi = """
SELECT ?publication ?doi
WHERE {
    ?publication <http://purl.org/dc/terms/identifier> ?doi .
    FILTER regex ( ?doi, "^doi" )
}"""

query_publications_venues_link = """
    SELECT ?publication ?doi ?venue
    WHERE {
    ?publication <http://purl.org/dc/terms/identifier> ?doi .
    OPTIONAL { ?publication <https://schema.org/isPartOf> ?venue } .
    FILTER regex ( ?doi, "^doi" )
    }
"""

query_publishers_crossref = """
SELECT ?publisher ?crossref
WHERE {
    ?publisher a <https://schema.org/Organization> ;
        <http://purl.org/dc/terms/identifier> ?crossref . 
}"""

query_publishers_names = """
    SELECT ?publisher ?crossref ?name
    WHERE {
        ?publisher a <https://schema.org/Organization> ;
            <http://purl.org/dc/terms/identifier> ?crossref .
        OPTIONAL { ?publisher <https://schema.org/name> ?name } .
    }
"""

query_authors_number = """
    SELECT ?author ?orcid
    WHERE {
        ?author a <https://schema.org/Person> ;
            <http://purl.org/dc/terms/identifier> ?orcid .
    }
"""

query_authors_data = """
    SELECT ?author ?orcid ?given_name ?family_name
    WHERE { 
        ?author a <https://schema.org/Person> ;
            <http://purl.org/dc/terms/identifier> ?orcid ;
            <https://schema.org/givenName> ?given_name ;
            <https://schema.org/familyName> ?family_name .
    }
"""

query_venues_ids = """
    SELECT DISTINCT ?venue ?venue_id 
    WHERE {
        ?venue <http://purl.org/dc/terms/identifier> ?venue_id .
        FILTER regex ( ?venue_id, "^is" )
}
"""

query_venues_number = """
    SELECT DISTINCT ?venue
    WHERE {
        ?venue <http://purl.org/dc/terms/identifier> ?venue_id .
        FILTER regex ( ?venue_id, "^is" )
}
"""

# Queries for existence

query_csv_inside = """
    SELECT ?publication ?title
    WHERE {
        ?publication <http://purl.org/dc/terms/identifier> ?doi ;
            <http://purl.org/dc/terms/title> ?title .
        FILTER regex ( ?doi, "^doi" )
    }
    LIMIT 1
"""

query_json_inside = """
    SELECT ?author 
    WHERE {
        ?author a <https://schema.org/Person> .
    }
    LIMIT 1
"""

# Existence functions

def csv_exists(endpoint):
    csv_database = get(endpoint, query_csv_inside, True)
    return not csv_database.empty

def json_exists(endpoint):
    json_database = get(endpoint, query_json_inside, True)
    return not json_database.empty

# Replacement functions

def replace_entity_reference(df):
    df["title"] = df["title"].str.lower().replace("&amp;", "&", regex = True).str.title()
    df["publication_venue"] = df["publication_venue"].str.lower().replace("&amp;", "&", regex = True).str.title()
    df["title"] = df["title"].str.lower().replace("&quot;", "\"", regex = True).str.title()
    df["publication_venue"] = df["publication_venue"].str.lower().replace("&quot;", "\"", regex = True).str.title()
    
    return df

def convert_type_related_strings(df, publication_type, venue_type):
    if publication_type:
        df["publication_type"] = df["publication_type"].str.replace("http://purl.org/spar/fabio/JournalArticle", "journal-article", regex = True)
        df["publication_type"] = df["publication_type"].str.replace("http://purl.org/spar/fabio/BookChapter", "book-chapter", regex = True)
        df["publication_type"] = df["publication_type"].str.replace("http://purl.org/spar/fabio/ProceedingsPaper", "proceedings-paper", regex = True)
    if venue_type:
        df["venue_type"] = df["venue_type"].str.replace("http://purl.org/spar/fabio/Journal", "journal", regex = True)
        df["venue_type"] = df["venue_type"].str.replace("http://purl.org/spar/fabio/Book", "book", regex = True)
        df["venue_type"] = df["venue_type"].str.replace("http://purl.org/spar/fabio/AcademicProceedings", "proceedings", regex = True)
    return df

def fix_df_columns_encoding(df, publication, venue, event):
    if publication:
        df["publication_title"] = df["publication_title"].str.encode("latin-1").str.decode("utf-8")
        df["given_name"] = df["given_name"].str.encode("latin-1").str.decode("utf-8")
        df["family_name"] = df["family_name"].str.encode("latin-1").str.decode("utf-8")
    if venue:
        df["venue_title"] = df["venue_title"].str.encode("latin-1").str.decode("utf-8")
        df["name"] = df["name"].str.encode("latin-1").str.decode("utf-8")
    if event:
        df["event"] = df["event"].str.encode("latin-1").str.decode("utf-8")
    return df

def  replace_dot_zeros(df, chapter_number):
    df = df.astype({"issue": "string", "volume": "string", "publication_year": "string"})
    df["issue"] = df["issue"].str.replace("\.0", "", regex=True)
    df["volume"] = df["volume"].str.replace("\.0", "", regex=True)
    df["publication_year"] = df["publication_year"].str.replace("\.0", "", regex=True)
    if chapter_number:
        df = df.astype({"chapter_number": "string"})
        df["chapter_number"] = df["chapter_number"].str.replace("\.0", "", regex=True)
    return df

# Basic functions

def upload_in_store(store, endpoint, graph): 
    store.open((endpoint, endpoint))

    for triple in graph.triples((None, None, None)):
        store.add(triple)    
    
    store.close()

def update_store(store, endpoint, addition_graph, deletions_graph):
    store.open((endpoint, endpoint))

    for triple in deletions_graph.triples((None, None, None)):
        store.remove(triple, context=None)
    for triple in addition_graph.triples((None, None, None)):
        store.add(triple)
            
    store.close()

def read_csv_file(path):
    csv_file = read_csv(path,
                    keep_default_na=False,
                    encoding= "utf-8",
                    dtype= {
                        "id": "string",               
                        "title" : "string",            
                        "type" : "string" ,             
                        "publication_year" : "string",   #TO BE CONVERTED IN "int" WHEN RETURNING A PYTHON OBJ
                        "issue" : "string",              
                        "volume" :"string",              
                        "chapter" :"string",                #TO BE CONVERTED IN "int" WHEN CREATING THE BOOKCHAPTER TABLE
                        "publication_venue" : "string",  
                        "venue_type" :"string",           
                        "publisher" :"string",          
                        "event" :"string"   
                    })
    
    replace_entity_reference(csv_file)
    return csv_file

def read_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        json_data = load(f)
    return json_data

def upload_additional_data(additional_data_path, additional_data_dictionary):
    with open(additional_data_path, "w", encoding="utf-8") as f:
        dump(additional_data_dictionary, f, ensure_ascii=False, indent=4)

def name_additional_data_file(endpoint):
    return "misoSoup"+endpoint.replace("http", "").replace("blazegraph", "").replace("sparql", "").replace("/", "").replace(".", "").replace(":", "")+".json"

#for query processor

def retrieve_doi(endpoint, to_convert_list, to_fill_list):
    to_convert_list = deque(to_convert_list)
    if len(to_convert_list) == 0:
        return to_fill_list
    else:
        internal_id = to_convert_list.popleft()
        retrieve_query = """
            SELECT ?doi
            WHERE {
                """+"<"+internal_id+">"+""" <http://purl.org/dc/terms/identifier> ?doi .
            }
        """
        doi_df = get(endpoint, retrieve_query, True)

    to_fill_list.append(doi_df.iloc[0]["doi"])
    
    return retrieve_doi(endpoint, to_convert_list, to_fill_list)

def retrieve_crossref(endpoint, to_convert_list, to_fill_list):
    to_convert_list = deque(to_convert_list)
    if len(to_convert_list) == 0:
        return to_fill_list
    else:
        internal_id = to_convert_list.popleft()
        retrieve_query = """
            SELECT ?crossref
            WHERE {
                """+"<"+internal_id+">"+""" <http://purl.org/dc/terms/identifier> ?crossref .
            }
        """
        doi_df = get(endpoint, retrieve_query, True)

    to_fill_list.append(doi_df.iloc[0]["crossref"])
    
    return retrieve_crossref(endpoint, to_convert_list, to_fill_list)



def additional_data_dataframe(starting_df_headers, additional_data_path, list_of_internal_ids, endpoint, key, publisher_internal_id, identifier, event_partial_string):
    additional_data_dict = read_json_file(additional_data_path)
    internal_ids = [] #I create this list where I put the information that I already have in the list_of_internal_ids because there is also the possibility to have some publications without venue. With this approach I create rows only in the case in which the publication has data about its venue.
    venue_titles = []
    venue_types = []
    venue_publishers = []
    venue_events = []

    if key == "title":
        for pub_internal_id in list_of_internal_ids:
            if pub_internal_id in additional_data_dict["venue_title"]:
                internal_ids.append(pub_internal_id)
                venue_titles.append(additional_data_dict["venue_title"][pub_internal_id])
                venue_types.append(additional_data_dict["venue_type"][pub_internal_id])
                venue_publishers.append(additional_data_dict["venue_publisher"][pub_internal_id])
                if pub_internal_id in additional_data_dict["venue_event"]:
                    venue_events.append(additional_data_dict["venue_event"][pub_internal_id])
                else:
                    venue_events.append("")
            else:
                pass
        
        doi_conversion_list = []
        crossref_conversion_list = []
        retrieve_doi(endpoint, internal_ids, doi_conversion_list)
        retrieve_crossref(endpoint, venue_publishers, crossref_conversion_list)
    

    elif key == "publisher":
        for pub_internal_id in additional_data_dict["venue_publisher"]:
            if additional_data_dict["venue_publisher"][pub_internal_id] == publisher_internal_id:
                internal_ids.append(pub_internal_id)
                venue_publishers.append(identifier)
                venue_titles.append(additional_data_dict["venue_title"][pub_internal_id])
                venue_types.append(additional_data_dict["venue_type"][pub_internal_id])
                if pub_internal_id in additional_data_dict["venue_event"]:
                    venue_events.append(additional_data_dict["venue_event"][pub_internal_id])
                else:
                    venue_events.append("")
        doi_conversion_list = []
        retrieve_doi(endpoint, internal_ids, doi_conversion_list)

    elif key == "event":
        for pub_internal_id in additional_data_dict["venue_event"]:
            if re.search(event_partial_string, additional_data_dict["venue_event"][pub_internal_id].lower()):
                internal_ids.append(pub_internal_id)
                venue_publishers.append(additional_data_dict["venue_publisher"][pub_internal_id])
                venue_titles.append(additional_data_dict["venue_title"][pub_internal_id])
                venue_types.append(additional_data_dict["venue_type"][pub_internal_id])
                venue_events.append(additional_data_dict["venue_publisher"][pub_internal_id])
        doi_conversion_list = []
        crossref_conversion_list = []
        retrieve_doi(endpoint, internal_ids, doi_conversion_list)
        retrieve_crossref(endpoint, venue_publishers, crossref_conversion_list)


    extension_df = DataFrame({})
    
    for header in starting_df_headers:
        extension_df[header] = ""

    extension_df["publication_id"] = Series(doi_conversion_list, dtype="string")
    extension_df["venue_title"] = Series(venue_titles, dtype="string")
    extension_df["venue_type"] = Series(venue_types, dtype="string")
    if key == "title" or key == "event":
        extension_df["publisher_id"] = Series(crossref_conversion_list, dtype="string")
    elif key == "publisher":
        extension_df["publisher_id"] = Series(venue_publishers, dtype="string")
    extension_df["event"] = Series(venue_events, dtype="string")
    
    return extension_df.fillna("")

# Upload functions

# Empty DB

def csv_upload(base_url, endpoint, store, graph, csv_path, additional_data_path):

    additional_data = {"venue_title": {}, "venue_type": {}, "venue_publisher": {}, "venue_event": {}}
    publications_csv = read_csv_file(csv_path)
    publisher_internal_ids = {}
    publisher_idx = 0

    for idx, row in publications_csv.iterrows():

        publication_local_id = "publication-" + str(idx+1)
        publication_entity = base_url + publication_local_id
        
    # general properties
        graph.add((URIRef(publication_entity), hasTitle, Literal(row["title"])))
        graph.add((URIRef(publication_entity), hasIdentifier, Literal(row["id"])))

    # publication type properties
        if row["type"] == "journal-article":
            graph.add((URIRef(publication_entity), RDF.type, JournalArticleURI))
            if row["issue"] != "":
                graph.add((URIRef(publication_entity), hasIssue, Literal(row["issue"])))
            if row["volume"] != "":
                graph.add((URIRef(publication_entity), hasVolume, Literal(row["volume"])))
        elif row["type"] == "book-chapter":
            graph.add((URIRef(publication_entity), RDF.type, BookChapterURI))
            graph.add((URIRef(publication_entity), hasChapterNumber, Literal(row["chapter"])))
        else:
            graph.add((URIRef(publication_entity), RDF.type, ProceedingsPaperURI))
            additional_data["venue_event"][publication_entity] = row["event"]
        
    # arity[0..1] properties
        if row["publication_year"] != "":
            graph.add((URIRef(publication_entity), hasPublicationYear, Literal(row["publication_year"])))
        
        if row["publication_venue"] != "":
            additional_data["venue_title"][publication_entity] = row["publication_venue"]
            # if a the publicationVenue property exists we define the venue_type properties
            if row["venue_type"] == "book":
                additional_data["venue_type"][publication_entity] = BookURI
            elif row["venue_type"] == "journal":
                additional_data["venue_type"][publication_entity] = JournalURI
            else:
                additional_data["venue_type"][publication_entity] = ProceedingsURI

    # publisher property
        if row["publisher"] != "": #here we define an entity for our publishers called https://.../publisher-1
            if row["publisher"] in publisher_internal_ids:
                publisher_entity = publisher_internal_ids[row["publisher"]]
            else:
                publisher_idx += 1
                publisher_local_id = "publisher-" + str(publisher_idx)
                publisher_entity = base_url + publisher_local_id
                publisher_internal_ids[row["publisher"]] = publisher_entity
                graph.add((URIRef(publisher_entity), RDF.type, OrganizationURI))
                graph.add((URIRef(publisher_entity), hasIdentifier, Literal(row["publisher"])))
                
            additional_data["venue_publisher"][publication_entity] = publisher_entity

    upload_additional_data(additional_data_path, additional_data)
    upload_in_store(store, endpoint, graph)

def json_upload(base_url, endpoint, store, graph, json_path):
    
    json_data = read_json_file(json_path)

    authors_json = json_data["authors"]
    venues_ids_json = json_data["venues_id"]
    references_json = json_data["references"]
    publishers_json = json_data["publishers"]

    publisher_idx = 0
    for single_publisher_id in publishers_json:
        publisher_idx += 1
        publisher_local_id = "publisher-" + str(publisher_idx)
        publisher_entity = base_url + publisher_local_id
        graph.add((URIRef(publisher_entity), RDF.type, OrganizationURI))
        graph.add((URIRef(publisher_entity), hasIdentifier, Literal(publishers_json[single_publisher_id]["id"])))
        graph.add((URIRef(publisher_entity), hasName, Literal(publishers_json[single_publisher_id]["name"])))

    publication_idx = 0
    author_idx = 0
    authors_internal_ids = {}
    #we will reuse this next one later for assigning the right internal id to the reference and cited publications
    publications_internal_ids = {}

    for doi in authors_json:
        publication_idx += 1
        publication_local_id = "publication-" + str(publication_idx)
        publication_entity = base_url + publication_local_id
        publications_internal_ids[doi] = publication_entity
        # graph addition
        graph.add((URIRef(publication_entity), hasIdentifier, Literal(doi)))
        
        for single_author in authors_json[doi]:
            if single_author["orcid"] in authors_internal_ids:
                author_entity = authors_internal_ids[single_author["orcid"]]
            else:
                author_idx += 1
                author_local_id = "author-" + str(author_idx)
                author_entity = base_url + author_local_id
                authors_internal_ids[single_author["orcid"]] = author_entity
                # graph additions
                graph.add((URIRef(author_entity), hasGivenName, Literal(single_author["given"])))
                graph.add((URIRef(author_entity), hasFamilyName, Literal(single_author["family"])))
                graph.add((URIRef(author_entity), hasIdentifier, Literal(single_author["orcid"])))
                graph.add((URIRef(author_entity), RDF.type, PersonURI))
            graph.add((URIRef(publication_entity), hasAuthor, URIRef(author_entity)))


    for doi in references_json:
        reference_entity = publications_internal_ids[doi]
        # now we must iterate over all the dois inside the list used as value of every specific reference_entity
        for cited_doi in references_json[doi]:
            cited_entity = publications_internal_ids[cited_doi]
            # graph addition
            graph.add((URIRef(reference_entity), hasCited, URIRef(cited_entity)))

    
    venue_idx = 1
    venues_internal_ids = {}

    for doi in venues_ids_json:
        venue_exists = False
        index_increment = False
        publication_entity = publications_internal_ids[doi]
        # now we iterate over the elements of the list of venues_ids (issn, isbn), that is placed as value of every related DOI
        for venue_single_id in venues_ids_json[doi]: #venues_json[doi] is basically the list of venue ids (issn, isbn...)
            # we check whether we have yet encountered the same venue_id (because every time we face 
            # a different venue_id we store it inside the "venues_internal_ids" dictionary).
            if venue_single_id in venues_internal_ids:
                venue_exists = True
                venue_entity = venues_internal_ids[venue_single_id]
                break
        
        for venue_single_id in venues_ids_json[doi]:
            if venue_exists:
                graph.add((URIRef(venue_entity), hasIdentifier, Literal(venue_single_id)))
            else:
                venue_local_id = "venue-" + str(venue_idx)
                venue_entity = base_url + venue_local_id
                venues_internal_ids[venue_single_id] = venue_entity
                # graph addition and change the value of index_increment variable
                index_increment = True
                graph.add((URIRef(venue_entity), hasIdentifier, Literal(venue_single_id)))
        graph.add((URIRef(publication_entity), hasPublicationVenue, URIRef(venue_entity)))
        # If index increment == True, then we increase by one the counter for the idx of our venue entities
        if index_increment:
            venue_idx += 1

    upload_in_store(store, endpoint, graph)

# DB with a CSV inside 

def csv_to_csv(base_url, endpoint, store, addition_graph, deletions_graph, csv_path, additional_data_path):

    publications_DF = get(endpoint, query_publications_data, True).fillna("")
    publishers_ids_DF = get(endpoint, query_publishers_crossref, True)

    publishers_ids_from_DF = {}
    # remember to add URIRef when adding values to a dictionary for 'graph things', or it will add the element as a string and not as an rdflib type.
    for idx, row in publishers_ids_DF.iterrows():
        publishers_ids_from_DF[row["crossref"]] = row["publisher"]

    publications_csv = read_csv_file(csv_path)
    # Read our MisoSoup
    additional_data = read_json_file(additional_data_path)

    publication_idx = publications_DF.last_valid_index() + 1
    publisher_idx = publishers_ids_DF.last_valid_index() + 1

    for idx, row in publications_csv.iterrows():
        if row["id"] in publications_DF["doi"].unique():
            existing_doi = publications_DF.index[publications_DF["doi"] == row["id"]].to_list()
            exact_index = existing_doi[0]
            publication_entity = publications_DF.iloc[exact_index]["publication"] #the entity is returned as a str obj

            if row["title"] != publications_DF.iloc[exact_index]["publication_title"] and row["title"] != "":
                addition_graph.add((URIRef(publication_entity), hasTitle, Literal(row["title"])))
                deletions_graph.add((URIRef(publication_entity), hasTitle, Literal(publications_DF.iloc[exact_index]["publication_title"])))

            if row["publication_year"] != str(publications_DF.iloc[exact_index]["publication_year"]).replace(".0",""):
                if row["publication_year"] != "":
                    addition_graph.add((URIRef(publication_entity), hasPublicationYear, Literal(row["publication_year"])))
                deletions_graph.add((URIRef(publication_entity), hasPublicationYear, Literal(publications_DF.iloc[exact_index]["publication_year"])))
            
            #qui è cosi perche issue e volume hanno arity 0 o 1, quindi se uno decide di eliminarne uno dei due deve poterlo fare
            #e ovviamente non aggiungiamo una tripla senza literal al nnostro DB, quindi avviene solo l'eliminazione
            if row["type"] == "journal-article":
                addition_graph.add((URIRef(publication_entity), RDF.type, JournalArticleURI))
                if row["issue"] != str(publications_DF.iloc[exact_index]["issue"]):
                    if row["issue"] != "":
                        addition_graph.add((URIRef(publication_entity), hasIssue, Literal(row["issue"])))
                    deletions_graph.add((URIRef(publication_entity), hasIssue, Literal(publications_DF.iloc[exact_index]["issue"])))
                if row["volume"] != str(publications_DF.iloc[exact_index]["volume"]):
                    if row["volume"] != "":
                        addition_graph.add((URIRef(publication_entity), hasVolume, Literal(row["volume"])))
                    deletions_graph.add((URIRef(publication_entity), hasVolume, Literal(publications_DF.iloc[exact_index]["volume"])))

            # non diamo la possibilità di sostituire un chapter esistente con un chapter vuoto perche l'arity è 1.
            # non può esserci solo eliminazione ma al massimo una SOSTITUZIONE
            if row["type"] == "book-chapter":
                addition_graph.add((URIRef(publication_entity), RDF.type, BookChapterURI))
                if row["chapter"] != str(publications_DF.iloc[exact_index]["chapter_number"]).replace(".0", "") and row["chapter"] != "":
                    deletions_graph.add((URIRef(publication_entity), hasChapterNumber, Literal(publications_DF.iloc[exact_index]["chapter_number"])))
                    addition_graph.add((URIRef(publication_entity), hasChapterNumber, Literal(row["chapter"])))
            # non diamo la possibilità di sostituire un event esistente con un event vuoto perche l'arity è 1.
            # non può esserci solo eliminazione ma al massimo una SOSTITUZIONE 
            if row["type"] == "proceedings-paper":
                addition_graph.add((URIRef(publication_entity), RDF.type, ProceedingsPaperURI))
                if row["event"] != "":
                    if publication_entity not in additional_data["venue_event"]:
                        additional_data["venue_event"][publication_entity] = row["event"]
                    elif row["event"] != additional_data["venue_event"][publication_entity]:
                        additional_data["venue_event"][publication_entity] = row["event"] 
            
            if row["publication_venue"] != "":
                if publication_entity not in additional_data["venue_title"]:
                    additional_data["venue_title"][publication_entity] = row["publication_venue"]
                elif row["publication_venue"] != additional_data["venue_title"][publication_entity]:
                    additional_data["venue_title"][publication_entity] = row["publication_venue"]
            
        else:  
            publication_idx += 1
            publication_local_id = ("publication-" + str(publication_idx))
            publication_entity = base_url + publication_local_id
            addition_graph.add((URIRef(publication_entity), hasIdentifier, Literal(row["id"])))
            addition_graph.add((URIRef(publication_entity), hasTitle, Literal(row["title"])))

            if row["type"] == "journal-article":
                addition_graph.add((URIRef(publication_entity), RDF.type, JournalArticleURI))
                if row["issue"] != "":
                    addition_graph.add((URIRef(publication_entity), hasIssue, Literal(row["issue"])))
                if row["volume"] != "":
                    addition_graph.add((URIRef(publication_entity), hasVolume, Literal(row["volume"])))

            elif row["type"] == "proceedings-paper":
                addition_graph.add((URIRef(publication_entity), RDF.type, ProceedingsPaperURI))
                additional_data["venue_event"][publication_entity] = row["event"]

            else:
                addition_graph.add((URIRef(publication_entity), RDF.type, BookChapterURI))
                addition_graph.add((URIRef(publication_entity), hasChapterNumber, Literal(row["chapter"])))

            if row["publication_year"] != "":
                addition_graph.add((URIRef(publication_entity), hasPublicationYear, Literal(row["publication_year"])))

            if row["publication_venue"] != "": 
                additional_data["venue_title"][publication_entity] = row["publication_venue"]
                if row["venue_type"] == "book":
                    additional_data["venue_type"][publication_entity] = BookURI
                elif row["venue_type"] == "journal":
                    additional_data["venue_type"][publication_entity] = JournalURI
                else:
                    additional_data["venue_type"][publication_entity] = ProceedingsURI
                    
            if row["publisher"] != "": 
                if row["publisher"] in publishers_ids_from_DF:
                    publisher_entity = publishers_ids_from_DF[row["publisher"]]
                else:
                    publisher_idx += 1
                    publisher_local_id = "publisher-" + str(publisher_idx)
                    publisher_entity = base_url + publisher_local_id
                    publishers_ids_from_DF[row["publisher"]] = publisher_entity
                    addition_graph.add((URIRef(publisher_entity), RDF.type, OrganizationURI))
                    addition_graph.add((URIRef(publisher_entity), hasIdentifier, Literal(row["publisher"])))
                
                additional_data["venue_publisher"][publication_entity] = publisher_entity

    upload_additional_data(additional_data_path, additional_data)
    update_store(store, endpoint, addition_graph, deletions_graph)

def json_to_csv(base_url, endpoint, store, addition_graph, json_path, additional_data_path):
    
    venues_json = read_json_file(json_path)

    authors_json = venues_json["authors"]
    venues_ids_json = venues_json["venues_id"]
    references_json = venues_json["references"]
    publishers_json = venues_json["publishers"]

    additional_data = read_json_file(additional_data_path)

    publications_ids_DF = get(endpoint, query_publications_doi, True)
    publications_ids_from_DF_and_json = {}
    for idx, row in publications_ids_DF.iterrows():
        publications_ids_from_DF_and_json[row["doi"]] = row["publication"]

    publication_idx = publications_ids_DF.last_valid_index() + 1

    # dictionary that will contain all the orcid as keys and the entities of authors as values
    authors_internal_ids = {}
    
    #Check to see if we are in a case in which information about authors are yet in DB or not
    authors_quantity_check = get(endpoint, query_authors_number, True)
    
    if authors_quantity_check.empty:
        author_idx = 0
    else:
        author_idx = authors_quantity_check.last_valid_index() + 1
        for idx, row in authors_quantity_check.iterrows():
            authors_internal_ids[row["orcid"]] = row["author"]

    
    for doi in authors_json:
        if doi in publications_ids_from_DF_and_json:
            publication_entity = publications_ids_from_DF_and_json[doi]
        else:
            publication_idx += 1
            publication_local_id = "publication-" + str(publication_idx)
            publication_entity = base_url + publication_local_id
            publications_ids_from_DF_and_json[doi] = publication_entity
            # it is correct that this one stays inside the else statement
            addition_graph.add((URIRef(publication_entity), hasIdentifier, Literal(doi)))

        for single_author in authors_json[doi]:
            if single_author["orcid"] in authors_internal_ids:
                author_entity = authors_internal_ids[single_author["orcid"]]
            else:
                author_idx += 1
                author_local_id = "author-" + str(author_idx)
                author_entity = base_url + author_local_id
                authors_internal_ids[single_author["orcid"]] = author_entity

                addition_graph.add((URIRef(author_entity), RDF.type, PersonURI))
                addition_graph.add((URIRef(author_entity), hasGivenName, Literal(single_author["given"])))
                addition_graph.add((URIRef(author_entity), hasFamilyName, Literal(single_author["family"])))
                addition_graph.add((URIRef(author_entity), hasIdentifier, Literal(single_author["orcid"])))

            addition_graph.add((URIRef(publication_entity), hasAuthor, URIRef(author_entity)))


    for doi in references_json:
        reference_entity = publications_ids_from_DF_and_json[doi]
        for cited_doi in references_json[doi]:
            cited_entity = publications_ids_from_DF_and_json[cited_doi]
            addition_graph.add((URIRef(reference_entity), hasCited, URIRef(cited_entity)))

    crossref_DF = get(endpoint, query_publishers_crossref, True)

    publishers_ids_from_DF = {}
    for idx, row in crossref_DF.iterrows():
        publishers_ids_from_DF[row["crossref"]] = row["publisher"]

    publisher_idx = crossref_DF.last_valid_index() + 1

    for crossref in publishers_json:
        if crossref in publishers_ids_from_DF:
            publisher_entity = publishers_ids_from_DF[crossref]
        else:
            publisher_idx += 1
            publisher_local_id = "publisher-" + str(publisher_idx)
            publisher_entity = base_url + publisher_local_id
            publishers_ids_from_DF[crossref] = publisher_entity 
            addition_graph.add((URIRef(publisher_entity), RDF.type, OrganizationURI))
            addition_graph.add((URIRef(publisher_entity), hasIdentifier, Literal(publishers_json[crossref]["id"])))
            
        addition_graph.add((URIRef(publisher_entity), hasName, Literal(publishers_json[crossref]["name"])))


    venues_quantity_check = get(endpoint, query_venues_number, True)
    if venues_quantity_check.empty:
        venue_idx = 1
    else:
        # venues_idx is +2 because of the fact that the first case in the following iteration do not increment the idx, so 
        # it will start from one position back with respect to the one in which it has to start
        venue_idx = venues_quantity_check.last_valid_index() + 2

    venues_internal_ids = {}
    venues_ids_DF = get(endpoint, query_venues_ids, True)
    for idx, row in venues_ids_DF.iterrows():
        if row["venue_id"] not in venues_internal_ids:
            venues_internal_ids[row["venue_id"]] = row["venue"]
    
    for doi in venues_ids_json:
        venue_exists = False
        index_increment = False
        publication_entity = publications_ids_from_DF_and_json[doi] #for example, the subject will be like "https://.../publication-1"

        for venue_single_id in venues_ids_json[doi]:
            if venue_single_id in venues_internal_ids:
                venue_exists = True
                venue_entity = venues_internal_ids[venue_single_id]
                break
         
        for venue_single_id in venues_ids_json[doi]:
            if venue_exists:
                addition_graph.add((URIRef(venue_entity), hasIdentifier, Literal(venue_single_id)))
            else:
                venue_local_id = "venue-" + str(venue_idx)
                venue_entity = base_url + venue_local_id
                index_increment = True
                venues_internal_ids[venue_single_id] = venue_entity
                addition_graph.add((URIRef(venue_entity), hasIdentifier, Literal(venue_single_id)))
        #the following must be outside the for statement

        addition_graph.add((URIRef(publication_entity), hasPublicationVenue, URIRef(venue_entity)))

        # now we check against our additional_data JSON file, to see if data contained in it can be linked to our new data
        if publication_entity in additional_data["venue_type"]:
            addition_graph.add((URIRef(venue_entity), RDF.type, URIRef(additional_data["venue_type"][publication_entity])))
            del additional_data["venue_type"][publication_entity]

        if publication_entity in additional_data["venue_title"]:
            addition_graph.add((URIRef(venue_entity), hasTitle, Literal(additional_data["venue_title"][publication_entity])))
            del additional_data["venue_title"][publication_entity]

        if publication_entity in additional_data["venue_event"]:
            addition_graph.add((URIRef(venue_entity), hasEvent, Literal(additional_data["venue_event"][publication_entity])))
            del additional_data["venue_event"][publication_entity]

        if publication_entity in additional_data["venue_publisher"]:
            addition_graph.add((URIRef(venue_entity), hasPublisher, URIRef(additional_data["venue_publisher"][publication_entity])))
            del additional_data["venue_publisher"][publication_entity]

        if index_increment:
            venue_idx += 1

    
    upload_additional_data(additional_data_path, additional_data)
    upload_in_store(store, endpoint, addition_graph)

# DB with a JSON inside

def json_to_json(base_url, endpoint, store, addition_graph, deletions_graph, json_path):
    
    venues_json = read_json_file(json_path)

    authors_json = venues_json["authors"]
    venues_ids_json = venues_json["venues_id"]
    references_json = venues_json["references"]
    publishers_json = venues_json["publishers"]

    publishers_names_DF = get(endpoint, query_publishers_names, True).fillna("")
    publisher_idx = publishers_names_DF.last_valid_index() + 1

    publishers_names = {}
    publishers_entities = {}
    for idx, row in publishers_names_DF.iterrows():
        publishers_names[row["crossref"]] = row["name"]
        publishers_entities[row["crossref"]] = row["publisher"]

    for publisher_id in publishers_json:
        if publisher_id in publishers_entities:
            publisher_entity = publishers_entities[publisher_id]
            # We want to help the one who upload data
            if publishers_json[publisher_id]["name"] != publishers_names[publisher_id]:
                deletions_graph.add((URIRef(publisher_entity), hasName, Literal(publishers_names[publisher_id])))
                addition_graph.add((URIRef(publisher_entity), hasName, Literal(publishers_json[publisher_id]["name"])))
        else:
            publisher_idx += 1
            publisher_local_id = "publisher-" + str(publisher_idx)
            publisher_entity = base_url + publisher_local_id
            publishers_names[publisher_id] = publishers_json[publisher_id]["name"]
            publishers_entities[publisher_id] = publisher_entity
            addition_graph.add((URIRef(publisher_entity), RDF.type, OrganizationURI))
            addition_graph.add((URIRef(publisher_entity), hasIdentifier, Literal(publishers_json[publisher_id]["id"])))
            addition_graph.add((URIRef(publisher_entity), hasName, Literal(publishers_json[publisher_id]["name"])))

    authors_DF = get(endpoint, query_authors_data, True)

    publications_DF = get(endpoint, query_publications_doi, True)

    publications_data_from_DF = {}

    for idx, row in publications_DF.iterrows():
        publications_data_from_DF[row["doi"]] = row["publication"]

    publication_idx = publications_DF.last_valid_index() + 1
    author_idx = authors_DF.last_valid_index() + 1

    authors_memo = {}

    for doi in authors_json:
        if doi in publications_data_from_DF:
            publication_entity = publications_data_from_DF[doi]
        else:
            publication_idx += 1
            publication_local_id = "publication-" + str(publication_idx)
            publication_entity = base_url + publication_local_id
            publications_data_from_DF[doi] = publication_entity
            addition_graph.add((URIRef(publication_entity), hasIdentifier, Literal(doi)))

        for single_author in authors_json[doi]:
            if single_author["orcid"] in authors_memo:
                author_entity = authors_memo[single_author["orcid"]]
            elif single_author["orcid"] in authors_DF["orcid"].unique():
                existing_author = authors_DF.index[authors_DF["orcid"] == single_author["orcid"]].to_list()
                exact_index = existing_author[0]
                author_entity = authors_DF.iloc[exact_index]["author"]
                
                if single_author["given"] != authors_DF.iloc[exact_index]["given_name"].encode("latin-1").decode("utf-8"):
                    deletions_graph.add((URIRef(author_entity), hasGivenName, Literal(authors_DF.iloc[exact_index]["given_name"])))
                    addition_graph.add((URIRef(author_entity), hasGivenName, Literal(single_author["given"])))
                    
                if single_author["family"] != authors_DF.iloc[exact_index]["family_name"].encode("latin-1").decode("utf-8"):
                    deletions_graph.add((URIRef(author_entity), hasFamilyName, Literal(authors_DF.iloc[exact_index]["family_name"])))
                    addition_graph.add((URIRef(author_entity), hasFamilyName, Literal(single_author["family"])))
            
            else:
                author_idx += 1
                author_local_id = "author-" + str(author_idx)
                author_entity = base_url + author_local_id
                authors_memo[single_author["orcid"]] = author_entity
                addition_graph.add((URIRef(author_entity), hasGivenName, Literal(single_author["given"])))
                addition_graph.add((URIRef(author_entity), hasFamilyName, Literal(single_author["family"])))
                addition_graph.add((URIRef(author_entity), hasIdentifier, Literal(single_author["orcid"])))
                addition_graph.add((URIRef(author_entity), RDF.type, PersonURI))
                
            addition_graph.add((URIRef(publication_entity), hasAuthor, URIRef(author_entity)))


    for reference in references_json:
        reference_entity = publications_data_from_DF[reference]
        for citation in references_json[reference]:
            cited_entity = publications_data_from_DF[citation]
            addition_graph.add((URIRef(reference_entity), hasCited, URIRef(cited_entity)))


    venues_idx_DF = get(endpoint, query_venues_number, True)
    # venues_idx is +2 because of the fact that the first case in the following iteration do not increment the idx, so 
    # it will start from one position back with respect to the one in which it has to start
    venues_idx = venues_idx_DF.last_valid_index() + 2

    venues_DF = get(endpoint, query_venues_ids, True)

    venues_internal_dict = {}

    for idx, row in venues_DF.iterrows():
        venues_internal_dict[row["venue_id"]] = row["venue"]

    for doi in venues_ids_json:
        publication_entity = publications_data_from_DF[doi]
        index_increment = False
        venue_exists = False
        
        for ven_id in venues_ids_json[doi]:
            if ven_id in venues_internal_dict:
                venue_exists = True
                venue_entity = venues_internal_dict[ven_id]
                break
        
        for ven_id in venues_ids_json[doi]:
            if venue_exists:
                addition_graph.add((URIRef(venue_entity), hasIdentifier, Literal(ven_id)))
                venues_internal_dict[ven_id] = venue_entity
            else:
                index_increment = True
                venue_local_id = "venue-" + str(venues_idx)
                venue_entity = base_url + venue_local_id
                venues_internal_dict[ven_id] = venue_entity
                addition_graph.add((URIRef(venue_entity), hasIdentifier, Literal(ven_id)))

        addition_graph.add((URIRef(publication_entity), hasPublicationVenue, URIRef(venue_entity)))

        if index_increment:
            venues_idx += 1

    update_store(store, endpoint, addition_graph, deletions_graph)

def csv_to_json(base_url, endpoint, store, addition_graph, csv_path, additional_data_path):

    file_exists = exists(additional_data_path)
    if file_exists:
        additional_data = read_json_file(additional_data_path)
    else:
        additional_data = additional_data = {"venue_title": {}, "venue_type": {}, "venue_publisher": {}, "venue_event": {}}

    publications_csv = read_csv_file(csv_path)

    publishers_ids_DF = get(endpoint, query_publishers_crossref, True)
    
    publishers_ids_from_DF = {}
    for idx, row in publishers_ids_DF.iterrows():
        publishers_ids_from_DF[row["crossref"]] = row["publisher"]

    publications_venues_DF = get(endpoint, query_publications_data, True).fillna("")
    

    publication_idx = publications_venues_DF.last_valid_index() + 1
    publisher_idx = publishers_ids_DF.last_valid_index() + 1

    for idx, row in publications_csv.iterrows():
        #definition of publisher_entity
        if row["publisher"] != "":
            if row["publisher"]  in publishers_ids_from_DF:
                publisher_entity = publishers_ids_from_DF[row["publisher"]]
            else:
                publisher_idx += 1
                local_publisher_id = "publisher-" + str(publisher_idx)
                publisher_entity = base_url + local_publisher_id
                publishers_ids_from_DF[row["publisher"] ] = publisher_entity
                addition_graph.add((URIRef(publisher_entity), RDF.type, OrganizationURI))
                addition_graph.add((URIRef(publisher_entity), hasIdentifier, Literal(row["publisher"])))

        if row["id"] in publications_venues_DF["doi"].unique():
            existing_doi = publications_venues_DF.index[publications_venues_DF["doi"] == row["id"]].to_list()
            exact_index = existing_doi[0]
            # Pub_entity exists either from a json or from a csv
            publication_entity = publications_venues_DF.iloc[exact_index]["publication"]
            
            if publications_venues_DF.iloc[exact_index]["venue"] == "":
                if row["publication_venue"] != "":
                    if row["venue_type"] == "book":
                        additional_data["venue_type"][publication_entity] = BookURI
                    elif row["venue_type"] == "journal":
                        additional_data["venue_type"][publication_entity] = JournalURI
                    else:
                        additional_data["venue_type"][publication_entity] = ProceedingsURI
                        additional_data["venue_event"][publication_entity] = row["event"]
                    additional_data["venue_title"][publication_entity] = row["publication_venue"]
                    additional_data["venue_publisher"][publication_entity] = publisher_entity
            else:
                venue_entity = publications_venues_DF.iloc[exact_index]["venue"]
                if row["publication_venue"] != "":
                    if row["venue_type"] == "book":
                        addition_graph.add((URIRef(venue_entity), RDF.type, BookURI))
                    elif row["venue_type"] == "journal":
                        addition_graph.add((URIRef(venue_entity), RDF.type, JournalURI))
                    else:
                        addition_graph.add((URIRef(venue_entity), RDF.type, ProceedingsURI))
                        addition_graph.add((URIRef(venue_entity), hasEvent, Literal(row["event"])))
                        # It is correct for this one to stay inside the else of Proceedings
                        if publication_entity in additional_data["venue_event"]:
                            del additional_data["venue_event"][publication_entity]

                    if publication_entity in additional_data["venue_type"]:
                            del additional_data["venue_type"][publication_entity]

                    #the publisher property addition must be inside the < if row["publication_venue"] != "" >
                    addition_graph.add((URIRef(venue_entity), hasPublisher, URIRef(publisher_entity)))
                    if publication_entity in additional_data["venue_publisher"]:
                            del additional_data["venue_publisher"][publication_entity]
                    addition_graph.add((URIRef(venue_entity), hasTitle, Literal(row["publication_venue"])))
                    if publication_entity in additional_data["venue_title"]:
                            del additional_data["venue_title"][publication_entity]
# Aggiunta
            if publications_venues_DF.iloc[exact_index]["type"] == "":
                if row["type"] == "journal-article":
                    addition_graph.add((URIRef(publication_entity), RDF.type, JournalArticleURI))
                    addition_graph.add((URIRef(publication_entity), hasIssue, Literal(row["issue"])))
                    addition_graph.add((URIRef(publication_entity), hasVolume, Literal(row["volume"])))
                elif row["type"] == "book-chapter":
                    addition_graph.add((URIRef(publication_entity), RDF.type, BookChapterURI))
                    addition_graph.add((URIRef(publication_entity), hasChapterNumber, Literal(row["chapter"])))
                else:
                    addition_graph.add((URIRef(publication_entity), RDF.type, ProceedingsPaperURI))
                if row["publication_year"] != "":
                    addition_graph.add((URIRef(publication_entity), hasPublicationYear, Literal(row["publication_year"])))
                addition_graph.add((URIRef(publication_entity), hasTitle, Literal(row["title"])))
# ------------------------------------
#the following is the else of the if that checks for the existence of a publication entity in the dataframe
        else:
            publication_idx += 1
            local_id = "publication-" + str(publication_idx)
            publication_entity = base_url + local_id
# QUA------------------------------------------
#prima tutti i row["type"] e l'aggiunta del title erano di default fuori dall'else, ora li ho sdoppiati e messi sia nell'if che nell'else, con casistiche differenti
            addition_graph.add((URIRef(publication_entity), hasIdentifier, Literal(row["id"])))
            addition_graph.add((URIRef(publication_entity), hasTitle, Literal(row["title"])))
            if row["type"] == "journal-article":
                addition_graph.add((URIRef(publication_entity), RDF.type, JournalArticleURI))
                addition_graph.add((URIRef(publication_entity), hasIssue, Literal(row["issue"])))
                addition_graph.add((URIRef(publication_entity), hasVolume, Literal(row["volume"])))
            elif row["type"] == "book-chapter":
                addition_graph.add((URIRef(publication_entity), RDF.type, BookChapterURI))
                addition_graph.add((URIRef(publication_entity), hasChapterNumber, Literal(row["chapter"])))
            else:
                addition_graph.add((URIRef(publication_entity), RDF.type, ProceedingsPaperURI))
            if row["publication_year"] != "":
                addition_graph.add((URIRef(publication_entity), hasPublicationYear, Literal(row["publication_year"])))

            if row["publication_venue"] != "":
                if row["venue_type"] == "book":
                    additional_data["venue_type"][publication_entity] = BookURI
                elif row["venue_type"] == "journal":
                    additional_data["venue_type"][publication_entity] = JournalURI
                else:
                    additional_data["venue_type"][publication_entity] = ProceedingsURI
                    additional_data["venue_event"][publication_entity] = row["event"]  
                additional_data["venue_title"][publication_entity] = row["publication_venue"]
                additional_data["venue_publisher"][publication_entity] = publisher_entity
        
    upload_additional_data(additional_data_path, additional_data)
    upload_in_store(store, endpoint, addition_graph)

