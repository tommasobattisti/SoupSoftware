from sparql_dataframe import get
from rdflib import *
from pandas import *
from triplestoreFunctions import *
from URIs import *
from additionalClasses import *


class TriplestoreQueryProcessor(QueryProcessor, TriplestoreProcessor):
    def __init__(self):
        super().__init__()
    
    def getPublicationsPublishedInYear(self, year):
        query_publications_by_year = """
        SELECT ?publication ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?chapter_number ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?event ?citation
        WHERE {
            ?publication <http://purl.org/dc/terms/identifier> ?publication_id ;
                <https://schema.org/datePublished> ?publication_year .

            OPTIONAL { ?publication a ?publication_type ;
                                    <http://purl.org/dc/terms/title> ?publication_title } .
            OPTIONAL { ?publication <https://schema.org/author> ?author .
                        ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                <https://schema.org/givenName> ?given_name ;
                                <https://schema.org/familyName> ?family_name . } .                    
            OPTIONAL { ?publication <https://schema.org/issueNumber> ?issue } .
            OPTIONAL { ?publication <https://schema.org/volumeNumber> ?volume } .
            OPTIONAL { ?publication <http://purl.org/spar/fabio/hasSequenceIdentifier> ?chapter_number } .
            OPTIONAL { ?publication <https://schema.org/isPartOf> ?publication_venue .
                        ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id .
                        OPTIONAL { ?publication_venue a ?venue_type ;
                                                    <http://purl.org/dc/terms/title> ?venue_title ;
                                                    <https://schema.org/publisher> ?publisher .
                                    ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                                                        <https://schema.org/name> ?name .   
                                                                
                                    OPTIONAL { ?publication_venue <https://schema.org/description> ?event } .
                                 } . 
                     } .
            

            OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                        ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .
                
            FILTER ( ?publication_year = \""""+str(year)+"""\" )
            FILTER regex ( ?publication_id, "^doi" )
        }
        """

        execute_publications_by_year = get(self.endpointUrl, query_publications_by_year, True).fillna("")

        #I make a list of the internal ids of the response of my query
        publications_ids = list(execute_publications_by_year["publication"].drop_duplicates())
                                                         
        #.columns.values ritorna una lista con i nomi degli headers
        headers = execute_publications_by_year.columns.values

        additional_data_path = name_additional_data_file(self.endpointUrl)
        df_from_additional_data = additional_data_dataframe(headers, additional_data_path, publications_ids, self.endpointUrl, "title", "", "", "")

        to_return_df = concat([execute_publications_by_year, df_from_additional_data]).reset_index(drop=True)

        fix_df_columns_encoding(to_return_df, True, True, True)
        convert_type_related_strings(to_return_df, True, True)
        to_return_df = replace_dot_zeros(to_return_df, True)
        
        del to_return_df["publication"]
        return to_return_df


    def getPublicationsByAuthorId(self, identifier):
        query_pub_by_aut = """
            SELECT ?publication ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?chapter_number ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?event ?citation
            WHERE {
                ?publication_q <https://schema.org/author> ?author_q .
                ?author_q <http://purl.org/dc/terms/identifier>  \""""+str(identifier)+"""\"  .

        
                ?publication <https://schema.org/author> ?author.
                ?author <http://purl.org/dc/terms/identifier>  ?author_id;
                        <https://schema.org/givenName> ?given_name ;
                        <https://schema.org/familyName> ?family_name .
                ?publication <http://purl.org/dc/terms/identifier> ?publication_id .
                OPTIONAL { ?publication a ?publication_type ;
                                        <http://purl.org/dc/terms/title> ?publication_title ;
                                        <https://schema.org/datePublished> ?publication_year  } .                 
                OPTIONAL { ?publication <https://schema.org/issueNumber> ?issue } .
                OPTIONAL { ?publication <https://schema.org/volumeNumber> ?volume } .
                OPTIONAL { ?publication <http://purl.org/spar/fabio/hasSequenceIdentifier> ?chapter_number } .
                OPTIONAL { ?publication <https://schema.org/isPartOf> ?publication_venue .
                            ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id .
                            OPTIONAL { ?publication_venue a ?venue_type ;
                                                        <http://purl.org/dc/terms/title> ?venue_title ;
                                                        <https://schema.org/publisher> ?publisher .
                                        ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                                                            <https://schema.org/name> ?name .   
                                                                    
                                        OPTIONAL { ?publication_venue <https://schema.org/description> ?event } .
                                    } . 
                        } .
                OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                            ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .
                
                FILTER ( ?publication =  ?publication_q )
            }
        """

        execute_pub_by_aut = get(self.endpointUrl, query_pub_by_aut, True).fillna("")


        #I make a list of the internal ids of the response of my query
        publications_ids = list(execute_pub_by_aut["publication"].drop_duplicates()) 
        headers = execute_pub_by_aut.columns.values
        
        additional_data_path = name_additional_data_file(self.endpointUrl)
        df_from_additional_data = additional_data_dataframe(headers, additional_data_path, publications_ids, self.endpointUrl, "title", "", "", "")


        to_return_df = concat([execute_pub_by_aut, df_from_additional_data]).reset_index(drop=True).drop_duplicates()

        fix_df_columns_encoding(to_return_df, True, True, True)
        convert_type_related_strings(to_return_df, True, True)
        to_return_df = replace_dot_zeros(to_return_df, True)

        del to_return_df["publication"]

        return to_return_df
    



    def getMostCitedPublication(self):
        query_most_cited_p = """
                SELECT ?reference ?citation
                WHERE {
                    ?reference_pub <http://purl.org/spar/cito/cites> ?citation_pub ;
                                <http://purl.org/dc/terms/identifier>  ?reference .
                    ?citation_pub <http://purl.org/dc/terms/identifier>  ?citation .


                    } 

                """
                
        execute_most_cited_p = get(self.endpointUrl, query_most_cited_p, True).fillna("")
        return execute_most_cited_p




    
    def getMostCitedVenue(self):
        query_most_cited_v = """
                SELECT ?reference ?citation ?venue_id
                WHERE {
                    ?reference_pub <http://purl.org/spar/cito/cites> ?citation_pub ;
                                    <http://purl.org/dc/terms/identifier> ?reference .
                    ?citation_pub <https://schema.org/isPartOf> ?venue ;
                                    <http://purl.org/dc/terms/identifier> ?citation .
                    ?venue <http://purl.org/dc/terms/identifier> ?venue_id .
                        
                    } 

        """
        most_cited_v_df = get(self.endpointUrl, query_most_cited_v, True).reset_index(drop=True).fillna("")
        
        return most_cited_v_df

    



    def getVenuesByPublisherId(self, identifier):
        query_venues_by_publisher = """
                                    SELECT ?publication_id ?venue ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?event
                                    WHERE {
                                        ?publication <https://schema.org/isPartOf> ?venue ;
                                                      <http://purl.org/dc/terms/identifier> ?publication_id .  
                                        ?venue <https://schema.org/publisher> ?publisher ;
                                                <http://purl.org/dc/terms/identifier> ?venue_id ;
                                                <http://purl.org/dc/terms/title> ?venue_title ;
                                                a ?venue_type .
                                        ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                                                    <https://schema.org/name> ?name  .
                                        
                                        OPTIONAL { ?venue <https://schema.org/description> ?event } .
                                        
                                        FILTER ( ?publisher_id = \""""+identifier+"""\" )
                                        
                                        } 
                            """

        query_publisher_internal_id = """
                SELECT ?publisher_internal_id
                WHERE {
                        ?publisher_internal_id <http://purl.org/dc/terms/identifier> \""""+identifier+"""\" .
                }
                """


        execute_venues_by_publisher = get(self.endpointUrl, query_venues_by_publisher, True).fillna("")

        execute_publisher_internal_id = get(self.endpointUrl, query_publisher_internal_id, True)
        if not execute_publisher_internal_id.empty:
                publisher_internal_id = execute_publisher_internal_id.iloc[0]["publisher_internal_id"]  #se non trova nulla non funziona e dà errore, trova un modo per farlo funzionare lo stesso al posto di creare un errore
                headers = execute_venues_by_publisher.columns.values

                additional_data_path = name_additional_data_file(self.endpointUrl)
                df_from_additional_data = additional_data_dataframe(headers, additional_data_path, "", self.endpointUrl, "publisher", publisher_internal_id, identifier, "")
                to_return_df = concat([execute_venues_by_publisher, df_from_additional_data]).reset_index(drop=True).fillna("")
        else:
                fix_df_columns_encoding(execute_venues_by_publisher, False, True, True)
                convert_type_related_strings(execute_venues_by_publisher, False, True)
                return execute_venues_by_publisher


        fix_df_columns_encoding(to_return_df, False, True, True)
        convert_type_related_strings(to_return_df, False, True)
        return to_return_df






    def getPublicationInVenue(self, identifier):
        query_publications_in_venue = """
                SELECT ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?chapter_number ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?event ?citation
                WHERE {
                    ?publication <https://schema.org/isPartOf> ?publication_venue_q .
                    ?publication_venue_q <http://purl.org/dc/terms/identifier> \""""+identifier+"""\" .
                    
                    ?publication <http://purl.org/dc/terms/identifier> ?publication_id .
                    ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id .
                    OPTIONAL { ?publication_venue a ?venue_type ;
                                                <http://purl.org/dc/terms/title> ?venue_title ;
                                                <https://schema.org/publisher> ?publisher .
                                ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                                                    OPTIONAL { ?publisher <https://schema.org/name> ?name . } .   
                                                            
                                OPTIONAL { ?publication_venue <https://schema.org/description> ?event } .
                            } . 
               
                    OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year } .

                    OPTIONAL { ?publication a ?publication_type ;
                                            <http://purl.org/dc/terms/title> ?publication_title } .
                    OPTIONAL { ?publication <https://schema.org/author> ?author .
                                ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                        <https://schema.org/givenName> ?given_name ;
                                        <https://schema.org/familyName> ?family_name . } .                    
                    OPTIONAL { ?publication <https://schema.org/issueNumber> ?issue } .
                    OPTIONAL { ?publication <https://schema.org/volumeNumber> ?volume } .
                    OPTIONAL { ?publication <http://purl.org/spar/fabio/hasSequenceIdentifier> ?chapter_number } .
                    

                    OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                                ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .

                    FILTER ( ?publication_venue = ?publication_venue_q  )
                    
                }
        """

        execute_publications_in_venue = get(self.endpointUrl, query_publications_in_venue, True).fillna("")

        fix_df_columns_encoding(execute_publications_in_venue, True, True, True)
        convert_type_related_strings(execute_publications_in_venue, True, True)
        execute_publications_in_venue = replace_dot_zeros(execute_publications_in_venue, True)
        return execute_publications_in_venue

    



    def getJournalArticlesInIssue(self, issue, volume, journal_id):
        query_articles_in_issue = """
            SELECT ?publication ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?citation
            WHERE {
                ?publication <https://schema.org/isPartOf> ?publication_venue_q .
                ?publication_venue_q <http://purl.org/dc/terms/identifier> \""""+journal_id+"""\" .
                ?publication <https://schema.org/issueNumber> ?issue ;
                            <https://schema.org/volumeNumber> ?volume ;
                            <http://purl.org/dc/terms/identifier> ?publication_id ;
                            a ?publication_type ;
                            <http://purl.org/dc/terms/title> ?publication_title .
                ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id .

                OPTIONAL { ?publication_venue a ?venue_type ;
                                            <http://purl.org/dc/terms/title> ?venue_title ;
                                            <https://schema.org/publisher> ?publisher .
                            ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                            
                            OPTIONAL { ?publisher <https://schema.org/name> ?name . } .   
                        } . 
                
                OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year } .
                
                OPTIONAL { ?publication <https://schema.org/author> ?author .
                            ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                    <https://schema.org/givenName> ?given_name ;
                                    <https://schema.org/familyName> ?family_name . } .                    
                
                OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                            ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .

                FILTER ( ?publication_venue = ?publication_venue_q  )
                FILTER ( ?issue = \""""+issue+"""\" )
                FILTER ( ?volume = \""""+volume+"""\")
                }
                """
                
        query_incomplete_articles_in_issue = """
                SELECT ?publication ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?citation
                WHERE {
                    FILTER NOT EXISTS { ?publication <https://schema.org/isPartOf> ?publication_venue } .
                    ?publication <https://schema.org/issueNumber> ?issue ;
                                <https://schema.org/volumeNumber> ?volume ;
                                <http://purl.org/dc/terms/identifier> ?publication_id ;
                                a ?publication_type ;
                                <http://purl.org/dc/terms/title> ?publication_title .

                    OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year } .
                    
                    OPTIONAL { ?publication <https://schema.org/author> ?author .
                                ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                        <https://schema.org/givenName> ?given_name ;
                                        <https://schema.org/familyName> ?family_name . } .                    

                    OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                                ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .

                    FILTER ( ?issue = \""""+issue+"""\" )
                    FILTER ( ?volume = \""""+volume+"""\")
                }
        """
        execute_articles_in_issue = get(self.endpointUrl, query_articles_in_issue, True).fillna("")
        execute_incomplete_articles_in_issue = get(self.endpointUrl, query_incomplete_articles_in_issue, True).fillna("")
        headers = execute_articles_in_issue.columns.values
        publications_ids = list(execute_incomplete_articles_in_issue["publication"].drop_duplicates()) 

        additional_data_path = name_additional_data_file(self.endpointUrl)
        df_from_additional_data = additional_data_dataframe(headers, additional_data_path, publications_ids, self.endpointUrl, "title", "", "", "")
        to_return_df = concat([execute_articles_in_issue, execute_incomplete_articles_in_issue, df_from_additional_data]).reset_index(drop=True).fillna("")
        
        fix_df_columns_encoding(to_return_df, True, True, False)
        convert_type_related_strings(to_return_df, True, True)
        to_return_df = replace_dot_zeros(to_return_df, False)
        

        del to_return_df["publication"]
        del to_return_df["event"]
        return to_return_df




    
    def getJournalArticlesInVolume(self, volume, journal_id):
        query_articles_in_volume = """
            SELECT ?publication ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?citation
            WHERE {
                {
                ?publication <https://schema.org/isPartOf> ?publication_venue_q .
                ?publication_venue_q <http://purl.org/dc/terms/identifier> \""""+journal_id+"""\" .
                ?publication <https://schema.org/volumeNumber> ?volume ;
                            <http://purl.org/dc/terms/identifier> ?publication_id ;
                            a ?publication_type ;
                            <http://purl.org/dc/terms/title> ?publication_title .
                ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id .
                OPTIONAL { ?publication  <https://schema.org/issueNumber> ?issue } .
                OPTIONAL { ?publication_venue a ?venue_type ;
                                            <http://purl.org/dc/terms/title> ?venue_title ;
                                            <https://schema.org/publisher> ?publisher .
                            ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                            
                            OPTIONAL { ?publisher <https://schema.org/name> ?name . } .   
                        } . 
                
                OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year } .
                
                OPTIONAL { ?publication <https://schema.org/author> ?author .
                            ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                    <https://schema.org/givenName> ?given_name ;
                                    <https://schema.org/familyName> ?family_name . } .                    
                
                OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                            ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .

                FILTER ( ?publication_venue = ?publication_venue_q )
                FILTER ( ?volume = \""""+volume+"""\")
                }
                
                UNION
                
                {
                FILTER NOT EXISTS { ?publication <https://schema.org/isPartOf> ?publication_venue } .
                ?publication <https://schema.org/volumeNumber> ?volume ;
                            <http://purl.org/dc/terms/identifier> ?publication_id ;
                            a ?publication_type ;
                            <http://purl.org/dc/terms/title> ?publication_title .

                OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year } .
                OPTIONAL { ?publication <https://schema.org/issueNumber> ?issue } .
                
                OPTIONAL { ?publication <https://schema.org/author> ?author .
                            ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                    <https://schema.org/givenName> ?given_name ;
                                    <https://schema.org/familyName> ?family_name . } .                    

                OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                            ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .

                FILTER ( ?volume = \""""+volume+"""\")
                }
            }
        """

        execute_articles_in_volume = get(self.endpointUrl, query_articles_in_volume, True).fillna("")
        headers = execute_articles_in_volume.columns.values
        publications_ids = list(execute_articles_in_volume["publication"].drop_duplicates()) 

        additional_data_path = name_additional_data_file(self.endpointUrl)
        df_from_additional_data = additional_data_dataframe(headers, additional_data_path, publications_ids, self.endpointUrl, "title", "", "", "")
        to_return_df = concat([execute_articles_in_volume, df_from_additional_data]).reset_index(drop=True).fillna("")

        fix_df_columns_encoding(to_return_df, True, True, True)
        convert_type_related_strings(to_return_df, True, True)
        to_return_df = replace_dot_zeros(to_return_df, False)

        del to_return_df["event"]
        return to_return_df





    def getJournalArticlesInJournal(self, journal_id):
        query_articles_in_journal = """
            SELECT ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?citation
            WHERE {
                {
                ?publication <https://schema.org/isPartOf> ?publication_venue_q .
                ?publication_venue_q <http://purl.org/dc/terms/identifier> \""""+journal_id+"""\" .
                ?publication <http://purl.org/dc/terms/identifier> ?publication_id ;
                                <http://purl.org/dc/terms/title> ?publication_title ;
                                a ?publication_type .
                ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id ;
                                    a ?venue_type ;
                                    <http://purl.org/dc/terms/title> ?venue_title ;
                                    <https://schema.org/publisher> ?publisher .
                                    
                ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id .
                            
                OPTIONAL { ?publisher <https://schema.org/name> ?name } .
                OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year } .
                OPTIONAL { ?publication <https://schema.org/issueNumber> ?issue } .
                OPTIONAL { ?publication <https://schema.org/volumeNumber> ?volume } .
                OPTIONAL { ?publication <https://schema.org/author> ?author .
                            ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                    <https://schema.org/givenName> ?given_name ;
                                    <https://schema.org/familyName> ?family_name . } .                    
                
                OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                            ?cited_pub <http://purl.org/dc/terms/identifier> ?citation . } .

                FILTER ( ?publication_venue = ?publication_venue_q )
                FILTER ( ?venue_type = <http://purl.org/spar/fabio/Journal> )
                FILTER ( ?publication_type = <http://purl.org/spar/fabio/JournalArticle> )
            }
            UNION
            {
                
                ?publication <https://schema.org/isPartOf> ?publication_venue_q .
                ?publication_venue_q <http://purl.org/dc/terms/identifier> \""""+journal_id+"""\" .

                ?publication <http://purl.org/dc/terms/identifier> ?publication_id .

                ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id .
                
                FILTER NOT EXISTS { ?publication_venue a ?venue_type } .
                            
                OPTIONAL { ?publication <https://schema.org/author> ?author .
                            ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                    <https://schema.org/givenName> ?given_name ;
                                    <https://schema.org/familyName> ?family_name . } .                    
                
                OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                            ?cited_pub <http://purl.org/dc/terms/identifier> ?citation . } .

                FILTER ( ?publication_venue = ?publication_venue_q )
            }
            }
        """

        execute_articles_in_journal = get(self.endpointUrl, query_articles_in_journal, True).fillna("").drop_duplicates()
        #IN QUESTO CASO NON AGGIUNGO NULLA CHE PROVIENE DAL JSON ESTERNO PERCHÈ ANDRò A RITROVARE LE COSE A PARTIRE DAL GENERIC

        fix_df_columns_encoding(execute_articles_in_journal, True, True, False)
        convert_type_related_strings(execute_articles_in_journal, True, True)
        execute_articles_in_journal = replace_dot_zeros(execute_articles_in_journal, False)

        return execute_articles_in_journal





    def getProceedingsByEvent(self, event):
        query_proceeding_by_event = """
            SELECT ?publication_id ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?event
            WHERE {
                ?publication <https://schema.org/isPartOf> ?publication_venue ;
                            <http://purl.org/dc/terms/identifier> ?publication_id .
                ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id;
                                    a ?venue_type ;
                                    <http://purl.org/dc/terms/title> ?venue_title ;
                                    <https://schema.org/description> ?event ;
                                    <https://schema.org/publisher> ?publisher .
                ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                            
                OPTIONAL { ?publisher <https://schema.org/name> ?name } .

                FILTER (regex ( ?event, \""""+event+"""\", "i"))
                }
            """

        execute_proceedings_by_event = get(self.endpointUrl, query_proceeding_by_event, True).fillna("")
        headers = execute_proceedings_by_event.columns.values

        additional_data_path = name_additional_data_file(self.endpointUrl)
        df_from_additional_data = additional_data_dataframe(headers, additional_data_path, "", self.endpointUrl, "event", "", "", event)
        to_return_df = concat([execute_proceedings_by_event, df_from_additional_data]).reset_index(drop=True).fillna("")

        fix_df_columns_encoding(to_return_df, False, True, True)
        convert_type_related_strings(to_return_df, False, True)

        return to_return_df





    def getPublicationAuthors(self, doi):
        query_publication_authors = """
        SELECT ?publication_id ?author_id ?given_name ?family_name
        WHERE {
            ?publication <http://purl.org/dc/terms/identifier>  ?publication_id ;
                        <https://schema.org/author> ?author .
            ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                    <https://schema.org/givenName> ?given_name ;
                    <https://schema.org/familyName> ?family_name .
            FILTER ( ?publication_id = \""""+doi+"""\" ) 
        }
        """
        execute_publication_authors = get(self.endpointUrl, query_publication_authors, True).fillna("")

        execute_publication_authors["given_name"] = execute_publication_authors["given_name"].str.encode("latin-1").str.decode("utf-8")
        execute_publication_authors["family_name"] = execute_publication_authors["family_name"].str.encode("latin-1").str.decode("utf-8")
        
        return execute_publication_authors

    



    def getPublicationsByAuthorName(self, authorPartialName):
        query_publications_by_author_name = """
            SELECT ?publication ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?chapter_number ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?event ?citation
            WHERE {
                ?publication <http://purl.org/dc/terms/identifier> ?publication_id ;
                            <https://schema.org/author> ?author .
                ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                        <https://schema.org/givenName> ?given_name ;
                        <https://schema.org/familyName> ?family_name .   
                OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year } .
                OPTIONAL { ?publication a ?publication_type ;
                                        <http://purl.org/dc/terms/title> ?publication_title . } .                  
                OPTIONAL { ?publication <https://schema.org/issueNumber> ?issue } .
                OPTIONAL { ?publication <https://schema.org/volumeNumber> ?volume } .
                OPTIONAL { ?publication <http://purl.org/spar/fabio/hasSequenceIdentifier> ?chapter_number } .
                OPTIONAL { ?publication <https://schema.org/isPartOf> ?publication_venue .
                            ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id .
                            OPTIONAL { ?publication_venue <https://schema.org/description> ?event } .
                            OPTIONAL { ?publication_venue a ?venue_type ;
                                                        <http://purl.org/dc/terms/title> ?venue_title ;
                                                        <https://schema.org/publisher> ?publisher .
                                        ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                                                            <https://schema.org/name> ?name . } .                           
                        } .
    
            OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                        ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .

            BIND (CONCAT (?given_name, " ", ?family_name) as ?name ) .
            FILTER ( regex ( ?name, \""""+authorPartialName+"""\", "i" ) )
            }
        """

        execute_publications_by_author_name = get(self.endpointUrl, query_publications_by_author_name, True).fillna("")
        fix_df_columns_encoding(execute_publications_by_author_name, True, True, True)
        convert_type_related_strings(execute_publications_by_author_name, True, True)
        execute_publications_by_author_name = replace_dot_zeros(execute_publications_by_author_name, True)

        return execute_publications_by_author_name

    




    def getDistinctPublisherOfPublications(self, pubIdList):
        to_return_df = DataFrame()
        for doi in pubIdList:
            query_publisher_of_publication = """
                    SELECT ?publication_id ?name ?publisher_id
                    WHERE {
                    ?publication <http://purl.org/dc/terms/identifier> ?publication_id ;
                        <https://schema.org/isPartOf> ?venue .
                    ?venue <https://schema.org/publisher> ?publisher .
                    ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id .
                    
                    OPTIONAL { ?publisher <https://schema.org/name> ?name } .

                    FILTER ( ?publication_id = \""""+doi+"""\" )

                    } 
                    """
            execute_publisher_of_publication = get(self.endpointUrl, query_publisher_of_publication, True).fillna("")
            to_return_df = concat([to_return_df, execute_publisher_of_publication])
        
        to_return_df["name"] = to_return_df["name"].str.encode("latin-1").str.decode("utf-8")
        return to_return_df






    def getPublicationByDoi(self, doi):
        query_publication_by_doi = """
                    SELECT ?publication ?publication_id ?publication_title ?publication_year ?publication_type ?issue ?volume ?chapter_number ?author_id ?given_name ?family_name ?venue_id ?venue_type ?venue_title ?publisher_id ?name ?event ?citation
                    WHERE {
                        ?publication <http://purl.org/dc/terms/identifier> ?publication_id .
                        OPTIONAL { ?publication <https://schema.org/datePublished> ?publication_year } .
                        OPTIONAL { ?publication a ?publication_type ;
                                                <http://purl.org/dc/terms/title> ?publication_title } .
                        OPTIONAL { ?publication <https://schema.org/author> ?author .
                                    ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                                            <https://schema.org/givenName> ?given_name ;
                                            <https://schema.org/familyName> ?family_name . } .                    
                        OPTIONAL { ?publication <https://schema.org/issueNumber> ?issue } .
                        OPTIONAL { ?publication <https://schema.org/volumeNumber> ?volume } .
                        OPTIONAL { ?publication <http://purl.org/spar/fabio/hasSequenceIdentifier> ?chapter_number } .
                        OPTIONAL { ?publication <https://schema.org/isPartOf> ?publication_venue .
                                    ?publication_venue <http://purl.org/dc/terms/identifier> ?venue_id .
                                    OPTIONAL { ?publication_venue a ?venue_type ;
                                                                <http://purl.org/dc/terms/title> ?venue_title ;
                                                                <https://schema.org/publisher> ?publisher .
                                                ?publisher <http://purl.org/dc/terms/identifier> ?publisher_id ;
                                                                    <https://schema.org/name> ?name .   
                                                                            
                                                OPTIONAL { ?publication_venue <https://schema.org/description> ?event } .
                                            } . 
                                } .
                        
                        OPTIONAL { ?publication <http://purl.org/spar/cito/cites> ?cited_pub .
                                    ?cited_pub <http://purl.org/dc/terms/identifier> ?citation } .
                            
                        FILTER ( ?publication_id = \""""+doi+"""\" )
                    }
            """

        execute_publication_by_doi = get(self.endpointUrl, query_publication_by_doi, True).fillna("")
        
        publications_ids = list(execute_publication_by_doi["publication"].drop_duplicates())                                    
        headers = execute_publication_by_doi.columns.values
        additional_data_path = name_additional_data_file(self.endpointUrl)

        df_from_additional_data = additional_data_dataframe(headers, additional_data_path, publications_ids, self.endpointUrl, "title", "", "", "")

        to_return_df = concat([execute_publication_by_doi, df_from_additional_data]).reset_index(drop=True)

        fix_df_columns_encoding(to_return_df, True, True, True)
        convert_type_related_strings(to_return_df, True, True)
        to_return_df = replace_dot_zeros(to_return_df, True)
        del to_return_df["publication"] 
        
        
        return to_return_df



    def getAuthorById(self, publication_id):
        query_author_by_id = """
            SELECT ?author_id ?given_name ?family_name
            WHERE {
                ?publication <http://purl.org/dc/terms/identifier> \""""+publication_id+"""\" ;
                    <https://schema.org/author> ?author .
                ?author <http://purl.org/dc/terms/identifier> ?author_id ;
                    <https://schema.org/givenName> ?given_name ;
                    <https://schema.org/familyName> ?family_name .
            }
        """
        to_return_df = get(self.endpointUrl, query_author_by_id, True).fillna("").drop_duplicates()
        return to_return_df



    def getPublisherById(self, publisher_id):
        query_publisher_by_id = """
            SELECT ?name
            WHERE {
                ?publisher <http://purl.org/dc/terms/identifier> \""""+publisher_id+"""\" ;
                    <https://schema.org/name> ?name .
            }
        """
        to_return_df = get(self.endpointUrl, query_publisher_by_id, True).drop_duplicates().fillna("")
        return to_return_df



    def getVenuesByDoi(self, publication_id):
        query_venues_by_doi = """ 
            SELECT ?publication_id ?venue_id
            WHERE {
                ?publication <http://purl.org/dc/terms/identifier> ?publication_id ;
                            <https://schema.org/isPartOf> ?venue .
                ?venue <http://purl.org/dc/terms/identifier> ?venue_id .

                FILTER ( ?publication_id = \""""+publication_id+"""\" )
            }
        """
        to_return_df = get(self.endpointUrl, query_venues_by_doi, True).drop_duplicates().fillna("")
        return to_return_df



    def getVenuesById(self, venues_ids_set):
        to_return_df = DataFrame({})
        for venue_id in venues_ids_set:
            query = """
            SELECT ?venue_id
            WHERE {
                ?venue_q <http://purl.org/dc/terms/identifier> \""""+venue_id+"""\" .
                ?venue <http://purl.org/dc/terms/identifier> ?venue_id .
                FILTER ( ?venue = ?venue_q)
            }
            """
            df = get(self.endpointUrl, query, True).drop_duplicates().fillna("")
            to_return_df = concat([to_return_df, df])

        return to_return_df