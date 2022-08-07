from sqlite3 import connect
from pandas import *
from pprint import pprint
from logging import exception
from json import load
from additionalClasses import *
import re

class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):
    def __init__(self):
        pass
            

# finished query
    def getPublicationsPublishedInYear(self, year):  
        with connect(self.dbPath) as con:      
                    
            query1 = """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, issue, volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name,
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, citation as cites
                        FROM JournalArticles 
                        LEFT JOIN PublicationsIds On JournalArticles.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON JournalArticles.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON JournalArticles.publication_id == Citations.reference
                        
                        LEFT JOIN Journals ON JournalArticles.venue_id == Journals.venue_id
                        LEFT JOIN VenuesIds ON Journals.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                        WHERE publication_year = '"""+str(year)+"""'                      
                        ;           
                        """

            JournalArticles = read_sql(query1, con)                               
            publication_type = []
            
            if len(JournalArticles) > 0:
                while len(JournalArticles) != len(publication_type):
                    publication_type.append("journal-article")
                                            
                JournalArticles.insert(3, "publication_type", Series(publication_type, dtype="string"))

                venueType = []
                for idx, row in JournalArticles.iterrows(): 
                    if row["venue_id"] != "":
                        venueType.append("journal")
                    else: 
                        venueType.append("")
                JournalArticles.insert(11, "venue_type", Series(venueType, dtype="string"))
                    
            query2 = """SELECT PublicationsIds.id as publication_id, publication_title,  publication_year, NULL as issue, NULL as volume, chapter_number, AuthorsIds.id as author_id, given_name, family_name,
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, citation as cites
                        FROM BookChapters
                        LEFT JOIN PublicationsIds ON BookChapters.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON BookChapters.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON BookChapters.publication_id == Citations.reference

                        LEFT JOIN Books ON BookChapters.venue_id == Books.venue_id
                        LEFT JOIN VenuesIds ON Books.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Books.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                        
                        WHERE publication_year = '"""+str(year)+"""' 
                        ;
                    """
            BookChapters = read_sql(query2, con)
            publication_type = []
            
            if len(BookChapters) > 0:
                while len(BookChapters) != len(publication_type):
                    publication_type.append("book-chapter")
                                            
                BookChapters.insert(3, "publication_type", Series(publication_type, dtype="string"))    
                
                venueType = []
                for idx, row in BookChapters.iterrows(): 
                    if BookChapters.loc[idx, "venue_id"] != "":
                        venueType.append("book")
                    else: 
                        venueType.append("")
                BookChapters.insert(11, "venue_type", Series(venueType, dtype="string"))
            
            query3 = """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, NULL as issue, NULL as volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name, 
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, event, citation as cites
                        FROM ProceedingsPapers
                        LEFT JOIN PublicationsIds On ProceedingsPapers.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON ProceedingsPapers.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON ProceedingsPapers.publication_id == Citations.reference

                        LEFT JOIN Proceedings ON ProceedingsPapers.venue_id == Proceedings.venue_id
                        LEFT JOIN VenuesIds ON Proceedings.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Proceedings.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                        
                        WHERE publication_year = '"""+str(year)+"""' 
                        ;
                        """
            ProceedingsPapers = read_sql(query3, con)
            publication_type = []

            if len(ProceedingsPapers) > 0:
                while len(ProceedingsPapers) != len(publication_type):
                    publication_type.append("proceedings-paper")
                                            
                ProceedingsPapers.insert(3, "publication_type", Series(type, dtype="string"))  
                
                venueType = []
                for idx, row in ProceedingsPapers.iterrows(): 
                    if ProceedingsPapers.loc[idx, "venue_id"] != "":
                        venueType.append("proceedings")
                    else: 
                        venueType.append("")
                ProceedingsPapers.insert(11, "venue_type", Series(venueType, dtype="string"))   

            # create publications tables
            publications = concat([JournalArticles, BookChapters, ProceedingsPapers], ignore_index=True)

            # substitute doi to publication_id in citation column
            query4 = """SELECT citation as cites1, id as citation_id
                        FROM Citations LEFT JOIN PublicationsIds ON Citations.citation == PublicationsIds.publication_id
                        ;
                        """
            citations = read_sql(query4, con)        
        
            result = merge(publications, citations, left_on="cites", right_on="cites1", how="left").drop_duplicates()
            result = result.drop(columns=["cites","cites1"]).reset_index(drop=True)
            result = result.rename(columns = {"citation_id" : "citation"})

            # append data from venues from external json
            Venues = getVenues(self.dbPath, result)   

            if len(Venues) > 0: 
                listOfIdx = []
                for idx, row in Venues.iterrows(): 
                    if Venues.loc[idx, "publisher_id"] not in result["publication_id"]:
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    Venues.drop(Venues.index[listOfIdx], inplace=True)              
                
                result = concat([result, Venues], ignore_index=True)

            result = result.replace(float("NaN"), "")  
            result["publication_year"] = result["publication_year"].astype("string")
            result["publication_year"] = result["publication_year"].str.replace("\.0", "", regex=True)   
            result["issue"] = result["issue"].astype("string")
            result["issue"] = result["issue"].str.replace("\.0", "", regex=True) 
            result["volume"] = result["volume"].astype("string")
            result["volume"] = result["volume"].str.replace("\.0", "", regex=True)  
            result["chapter_number"] = result["chapter_number"].astype("string")
            result["chapter_number"] = result["chapter_number"].str.replace("\.0", "", regex=True)
            return result

# finished query
    def getPublicationsByAuthorId(self, id): 
        with connect(self.dbPath) as con:      

            queryJA= """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, issue, volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name, 
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, citation as cites
                        FROM JournalArticles 
                        LEFT JOIN PublicationsIds On JournalArticles.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON JournalArticles.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON JournalArticles.publication_id == Citations.reference
                        
                        LEFT JOIN Journals ON JournalArticles.venue_id == Journals.venue_id
                        LEFT JOIN VenuesIds ON Journals.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                        
                        WHERE JournalArticles.publication_id IN (
                        SELECT JournalArticles.publication_id
                        FROM JournalArticles 
                        LEFT JOIN PublicationsIds On JournalArticles.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON JournalArticles.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN AuthorsIds ON PublicationsAuthors.author_id == AuthorsIds.author_id 
                        WHERE AuthorsIds.id = '"""+id+"""')
                        """
            JournalArticles = read_sql(queryJA, con)
            publication_type = []
                
            while len(JournalArticles) != len(publication_type):
                publication_type.append("journal-article")
                    
            JournalArticles.insert(3, "publication_type", Series(publication_type, dtype="string"))

            venueType = []
            for idx, row in JournalArticles.iterrows(): 
                if JournalArticles.loc[idx, "venue_id"] != "":
                    venueType.append("journal")
                else: 
                    venueType.append("")
            JournalArticles.insert(11, "venue_type", Series(venueType, dtype="string"))

            queryBC =   """
                        SELECT PublicationsIds.id as publication_id, publication_title, publication_year, NULL as issue, NULL as volume, chapter_number, AuthorsIds.id as author_id, given_name, family_name,
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, citation as cites
                        FROM BookChapters 
                        LEFT JOIN PublicationsIds On BookChapters.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON BookChapters.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON BookChapters.publication_id == Citations.reference

                        LEFT JOIN Books ON BookChapters.venue_id == Books.venue_id
                        LEFT JOIN VenuesIds ON Books.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Books.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id

                        WHERE BookChapters.publication_id IN (
                        SELECT BookChapters.publication_id
                        FROM BookChapters 
                        LEFT JOIN PublicationsIds On BookChapters.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON BookChapters.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN AuthorsIds ON PublicationsAuthors.author_id == AuthorsIds.author_id 
                        WHERE AuthorsIds.id = '"""+id+"""')
                        """

            BookChapters = read_sql(queryBC, con)
            publication_type = []
                
            while len(BookChapters) != len(publication_type):
                publication_type.append("book-chapter")
                    
            BookChapters.insert(3, "publication_type", Series(publication_type, dtype="string"))

            venueType = []
            for idx, row in BookChapters.iterrows(): 
                if row["venue_id"] != "":
                    venueType.append("book")
                else: 
                    venueType.append("")
            BookChapters.insert(11, "venue_type", Series(venueType, dtype="string"))

            queryPP = """
                        SELECT PublicationsIds.id as publication_id, publication_title, publication_year, NULL as issue, NULL as volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name, 
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, event, citation as cites
                        FROM ProceedingsPapers
                        LEFT JOIN PublicationsIds On ProceedingsPapers.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON ProceedingsPapers.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON ProceedingsPapers.publication_id == Citations.reference

                        LEFT JOIN Proceedings ON ProceedingsPapers.venue_id == Proceedings.venue_id
                        LEFT JOIN VenuesIds ON Proceedings.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Proceedings.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id

                        WHERE ProceedingsPapers.publication_id IN (
                        SELECT ProceedingsPapers.publication_id
                        FROM ProceedingsPapers 
                        LEFT JOIN PublicationsIds On ProceedingsPapers.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON ProceedingsPapers.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN AuthorsIds ON PublicationsAuthors.author_id == AuthorsIds.author_id 
                        WHERE AuthorsIds.id = '"""+id+"""')
                        """

            ProceedingsPapers = read_sql(queryPP, con)
            publication_type = []
                
            while len(ProceedingsPapers) != len(publication_type):
                publication_type.append("proceedings-paper")
                    
            ProceedingsPapers.insert(3, "publication_type", Series(publication_type, dtype="string"))

            venueType = []
            for idx, row in ProceedingsPapers.iterrows(): 
                if row["venue_id"] != "":
                    venueType.append("proceedings")
                else: 
                    venueType.append("")
            ProceedingsPapers.insert(11, "venue_type", Series(venueType, dtype="string"))

            queryPub = """
                    SELECT PublicationsIds.id as publication_id, NULL as publication_title, NULL as publication_year, NULL as issue, NULL as volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name, 
                            VenuesIds.id as venue_id, NULL as venue_title, NULL as publisher_id, NULL as name, NULL as event, citation as cites
                    FROM PublicationsIds
                    LEFT JOIN PublicationsAuthors ON PublicationsIds.publication_id == PublicationsAuthors.publication_id
                    LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                    LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                    LEFT JOIN Citations ON PublicationsIds.publication_id == Citations.reference
                    LEFT JOIN PublicationsVenues ON PublicationsIds.publication_id == PublicationsVenues.publication_id
                    LEFT JOIN VenuesIds ON PublicationsVenues.venue_id  == VenuesIds.venue_id 

                    WHERE PublicationsIds.publication_id IN (
                    SELECT PublicationsIds.publication_id
                    FROM PublicationsIds 
                    LEFT JOIN PublicationsAuthors ON PublicationsIds.publication_id == PublicationsAuthors.publication_id
                    LEFT JOIN AuthorsIds ON PublicationsAuthors.author_id == AuthorsIds.author_id 
                    WHERE AuthorsIds.id = '"""+id+"""')
                    """

            Publications = read_sql(queryPub, con)
            publication_type = []
                
            while len(Publications) != len(publication_type):
                publication_type.append("")
                    
            Publications.insert(3, "publication_type", Series(publication_type, dtype="string"))

            venue_type = []
                
            while len(Publications) != len(venue_type):
                venue_type.append("")
                    
            Publications.insert(11, "venue_type", Series(venue_type, dtype="string"))

            query2 = """SELECT citation as cites1, id as citation
                        FROM Citations LEFT JOIN PublicationsIds ON Citations.citation == PublicationsIds.publication_id
                        ;
                        """

            citations = read_sql(query2, con)

            AllPublications = concat([JournalArticles, BookChapters, ProceedingsPapers])

            if len(Publications) > 0: 
                listOfIdx = []
                for idx, row in Publications.iterrows(): 
                    if Publications.loc[idx, "publication_id"] in AllPublications["publication_id"]:
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    Publications = Publications.drop(listOfIdx, inplace=True)

                AllPublications = concat([JournalArticles, BookChapters, ProceedingsPapers, Publications])
                
            AllPublications = merge(AllPublications, citations, left_on="cites", right_on="cites1", how="left").drop_duplicates()
            AllPublications = AllPublications.drop(columns=["cites","cites1"]).reset_index(drop=True) 
            Venues = getVenues(self.dbPath, AllPublications)

            if len(Venues) > 0:
                for idx, row in Venues.iterrows(): 
                    listOfIdx = []
                    if Venues.loc[idx, "publication_id"] not in AllPublications:
                        listOfIdx.append(idx)
                Venues.drop(Venues.index[listOfIdx], inplace=True)
                
                AllPublications = concat([AllPublications, Venues], ignore_index=True)

            AllPublications.replace(float("NaN"), "", inplace=True)  

            AllPublications["publication_year"] = AllPublications["publication_year"].astype("string")
            AllPublications["publication_year"] = AllPublications["publication_year"].str.replace("\.0", "", regex=True)   
            AllPublications["issue"] = AllPublications["issue"].astype("string")
            AllPublications["issue"] = AllPublications["issue"].str.replace("\.0", "", regex=True) 
            AllPublications["volume"] = AllPublications["volume"].astype("string")
            AllPublications["volume"] = AllPublications["volume"].str.replace("\.0", "", regex=True)  
            AllPublications["chapter_number"] = AllPublications["chapter_number"].astype("string")
            AllPublications["chapter_number"] = AllPublications["chapter_number"].str.replace("\.0", "", regex=True)
            return AllPublications

    
    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:
            query1 = """SELECT * FROM Citations"""
            query2 = """SELECT * FROM PublicationsIds"""
                    
            citedPub = read_sql(query1, con)  
            dois = read_sql(query2, con)

            citations = merge(citedPub, dois, left_on="citation", right_on="publication_id")[["id", "reference"]].rename(columns={"id": "citation"})
            citations = merge(citations, dois, left_on="reference", right_on="publication_id")[["citation", "id"]].rename(columns={"id":"reference"})
            return citations       
  

# finished query
    def getMostCitedVenue(self): 
        with connect(self.dbPath) as con:    

            query1 = """SELECT reference, citation, VenuesIds.id as venue_id
                        FROM Citations JOIN PublicationsVenues ON Citations.citation == PublicationsVenues.publication_id  
                        JOIN VenuesIds ON PublicationsVenues.venue_id == VenuesIds.venue_id                                       
                            ;                                        
                    """            
            citedPub = read_sql(query1, con) 

            query2 = """SELECT * FROM PublicationsIds"""
            dois = read_sql(query2, con)   

            citations = merge(citedPub, dois, left_on="citation", right_on="publication_id")[["id", "reference", "venue_id"]].rename(columns={"id": "citation"})
            citations = merge(citations, dois, left_on="reference", right_on="publication_id")[["citation", "id", "venue_id"]].rename(columns={"id":"reference"})
            return citations
            

    def getVenuesByPublisherId(self, id):
        with connect(self.dbPath) as con:

            queryJ = """SELECT PublicationsIds.id as publication_id, VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event
                        FROM Journals 
                        LEFT JOIN VenuesIds ON Journals.venue_id == VenuesIds.venue_id
                        LEFT JOIN PublicationsVenues ON Journals.venue_id == PublicationsVenues.venue_id
                        LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                        WHERE Journals.venue_id IN (
                        SELECT Journals.venue_id
                        FROM Journals
                        LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id
                        WHERE PublishersIds.id = '"""+id+"""' 
                        )
                        """
            
            Journals = read_sql(queryJ, con)
            venueType = []
            while len(Journals) != len(venueType):
                venueType.append("journal")

            Journals.insert(3, "venue_type", Series(venueType, dtype="string"))

            queryB = """
                    SELECT PublicationsIds.id as publication_id, VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event
                    FROM Books 
                    LEFT JOIN VenuesIds ON Books.venue_id == VenuesIds.venue_id
                    LEFT JOIN PublicationsVenues ON Books.venue_id == PublicationsVenues.venue_id
                    LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                    LEFT JOIN PublishersIds ON Books.publisher_id == PublishersIds.publisher_id
                    LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                    WHERE Books.venue_id IN (
                        SELECT Books.venue_id
                        FROM Books
                        LEFT JOIN PublishersIds ON Books.publisher_id == PublishersIds.publisher_id
                        WHERE PublishersIds.id = '"""+id+"""' 
                    )
                    """
            Books = read_sql(queryB, con)
            venueType = []
            while len(Books) != len(venueType):
                venueType.append("book")

            Books.insert(3, "venue_type", Series(venueType, dtype="string"))

            queryP = """
                    SELECT PublicationsIds.id as publication_id, VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, event
                    FROM Proceedings 
                    LEFT JOIN VenuesIds ON Proceedings.venue_id == VenuesIds.venue_id
                    LEFT JOIN PublicationsVenues ON Proceedings.venue_id == PublicationsVenues.venue_id
                    LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                    LEFT JOIN PublishersIds ON Proceedings.publisher_id == PublishersIds.publisher_id
                    LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                    WHERE Proceedings.venue_id IN (
                        SELECT Proceedings.venue_id
                        FROM Proceedings
                        LEFT JOIN PublishersIds ON Proceedings.publisher_id == PublishersIds.publisher_id
                        WHERE PublishersIds.id = '"""+id+"""' 
                    )
                    """
            Proceedings = read_sql(queryP, con)
            venueType = []
            while len(Proceedings) != len(venueType):
                venueType.append("proceedings")

            Proceedings.insert(3, "venue_type", Series(venueType, dtype="string"))

            result = concat([Journals, Books, Proceedings])

            result = result.drop_duplicates(ignore_index=True)

            Venues = getVenues(self.dbPath, result)

            if len(Venues) > 0:
                listOfIdx = []
                for idx, row in Venues.iterrows(): 
                    if Venues.loc[idx, "publisher_id"] != id:
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    Venues.drop(Venues.index[listOfIdx], inplace=True)

                result = concat([result, Venues]).drop_duplicates()

            result = result.replace(float("NaN"), "") 
            return result


    def getPublicationInVenue(self, venueId):  
        with connect(self.dbPath) as con:      
                    
            query1 = """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, issue, volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name, citation 
                        FROM PublicationsIds JOIN JournalArticles ON PublicationsIds.publication_id == JournalArticles.publication_id
                        LEFT JOIN PublicationsAuthors ON PublicationsIds.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN AuthorsIds ON PublicationsAuthors.author_id == AuthorsIds.author_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id
                        LEFT JOIN Citations ON JournalArticles.publication_id == Citations.reference                                                     
                    ;           
                    """

            JournalArticles = read_sql(query1, con)                               
            publication_type = []
            
            while len(JournalArticles) != len(publication_type):
                publication_type.append("journal-article")
                                        
            JournalArticles.insert(3, "publication_type", Series(publication_type, dtype="string"))
            
                    
            query2 = """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, NULL as issue, NULL as volume, chapter_number, AuthorsIds.id as author_id, given_name, family_name, citation 
                        FROM PublicationsIds JOIN BookChapters ON PublicationsIds.publication_id == BookChapters.publication_id
                        LEFT JOIN PublicationsAuthors ON PublicationsIds.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN AuthorsIds ON PublicationsAuthors.author_id == AuthorsIds.author_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id
                        LEFT JOIN Citations ON BookChapters.publication_id == Citations.reference
                        ;
                    """
            BookChapters = read_sql(query2, con)
            publication_type = []
            
            while len(BookChapters) != len(publication_type):
                publication_type.append("book-chapter")
                                        
            BookChapters.insert(3, "publication_type", Series(publication_type, dtype="string"))    
            
            
            query3 = """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, NULL as issue, NULL as volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name, citation 
                        FROM PublicationsIds JOIN ProceedingsPapers ON PublicationsIds.publication_id == ProceedingsPapers.publication_id
                        LEFT JOIN PublicationsAuthors ON PublicationsIds.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN AuthorsIds ON PublicationsAuthors.author_id == AuthorsIds.author_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id
                        LEFT JOIN Citations ON ProceedingsPapers.publication_id == Citations.reference
                        ;
                        """
            ProceedingsPapers = read_sql(query3, con)
            publication_type = []
        
            while len(ProceedingsPapers) != len(publication_type):
                publication_type.append("proceedings-paper")
                                        
            ProceedingsPapers.insert(3, "publication_type", Series(type, dtype="string"))     

            # create publications tables
            publications = concat([JournalArticles, BookChapters, ProceedingsPapers], ignore_index=True)
        
            query2 = """SELECT VenuesIds.venue_id as int_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, PublicationsIds.id as publication_id
                    FROM Journals
                    JOIN PublicationsVenues ON Journals.venue_id == PublicationsVenues.venue_id
                    LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                    LEFT JOIN VenuesIds ON PublicationsVenues.venue_id == VenuesIds.venue_id
                    LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id                     
                    LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                            WHERE VenuesIds.id  = '"""+venueId+"""'    
                        ;
                        """
            Journals = read_sql(query2, con)                    
            venue_type = []
            
            if len (Journals) > 0:
                while len(Journals) != len(venue_type):
                    venue_type.append("journal")
                    
                Journals.insert(2, "venue_type", Series(venue_type, dtype="string"))
                            
                                
            query3 =  """SELECT VenuesIds.venue_id as int_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, PublicationsIds.id as publication_id
                        FROM Books
                        JOIN PublicationsVenues ON Books.venue_id == PublicationsVenues.venue_id
                        LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                        LEFT JOIN VenuesIds ON PublicationsVenues.venue_id == VenuesIds.venue_id
                        LEFT JOIN PublishersIds ON Books.publisher_id == PublishersIds.publisher_id                     
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                                WHERE VenuesIds.id  = '"""+venueId+"""'  
                        ;
                        """
            Books = read_sql(query3, con)
            venue_type = []
            if len (Books) > 0:
                while len(Books) != len(venue_type):
                    venue_type.append("book")
                
                Books.insert(2, "venue_type", Series(venue_type, dtype="string"))
            
            query4 = """SELECT VenuesIds.venue_id as int_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, PublicationsIds.id as publication_id
                        FROM Proceedings
                        JOIN PublicationsVenues ON Proceedings.venue_id == PublicationsVenues.venue_id
                        LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                        LEFT JOIN VenuesIds ON PublicationsVenues.venue_id == VenuesIds.venue_id
                        LEFT JOIN PublishersIds ON Proceedings.publisher_id == PublishersIds.publisher_id                     
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                                WHERE  VenuesIds.id  = '"""+venueId+"""' 
                        ;           
                        """

            Proceedings = read_sql(query4, con)
            venue_type = []
            if len (Proceedings) > 0:
                while len(Proceedings) != len(venue_type):
                    venue_type.append("proceedings")
                
                Proceedings.insert(2, "venue_type", Series(type, dtype="string"))            

            venues = concat([Journals, Books, Proceedings], ignore_index=True)                     
            
            # merge venue with publications if there are venues
            if len(venues) > 0:
                publications = merge(venues, publications, left_on="publication_id", right_on="publication_id") #i do it on publication id because I don't necessarily have venue_ids

            query5 = """SELECT * from VenuesIds; """
            allVenues = read_sql(query5, con)
            publications = merge(publications, allVenues, left_on="int_id", right_on = "venue_id")               
            publications = publications.drop(columns=["int_id", "venue_id"])
            publications = publications.rename(columns = {"id" : "venue_id"})

            # substitute doi to publication_id in citation column
            query4 = """SELECT citation as cites1, id as citation_id
                        FROM Citations LEFT JOIN PublicationsIds ON Citations.citation == PublicationsIds.publication_id
                        ;
                        """
            citations = read_sql(query4, con)        
        
            result = merge(publications, citations, left_on="citation", right_on="cites1", how="left").drop_duplicates()

            result = result.drop(columns=["citation","cites1"]).reset_index(drop=True)
            result = result.rename(columns = {"citation_id" : "citation"})

            result = result.replace(float("NaN"), "")
            result = result[["publication_id", "publication_title", "publication_year", "publication_type", "issue", "volume", "chapter_number", "author_id", "given_name", "family_name", "venue_id", "venue_type", "venue_title", "publisher_id", "name", "event", "citation"]]
            result["publication_year"] = result["publication_year"].astype("string")
            result["publication_year"] = result["publication_year"].str.replace("\.0", "", regex=True)   
            result["issue"] = result["issue"].astype("string")
            result["issue"] = result["issue"].str.replace("\.0", "", regex=True) 
            result["volume"] = result["volume"].astype("string")
            result["volume"] = result["volume"].str.replace("\.0", "", regex=True)  
            result["chapter_number"] = result["chapter_number"].astype("string")
            result["chapter_number"] = result["chapter_number"].str.replace("\.0", "", regex=True)
            return result
            

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        with connect(self.dbPath) as con:

            query = """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, issue, volume, AuthorsIds.id as author_id, given_name, family_name,
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, citation as cites
                        FROM JournalArticles 
                        LEFT JOIN PublicationsIds On JournalArticles.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON JournalArticles.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON JournalArticles.publication_id == Citations.reference
                        
                        LEFT JOIN Journals ON JournalArticles.venue_id == Journals.venue_id
                        LEFT JOIN VenuesIds ON Journals.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                        
                        WHERE JournalArticles.publication_id IN (     
                            SELECT JournalArticles.publication_id
                            FROM JournalArticles 
                            LEFT JOIN Journals ON JournalArticles.venue_id == Journals.venue_id 
                            LEFT JOIN VenuesIds ON Journals.venue_id == VenuesIds.venue_id
                            WHERE VenuesIds.id = '"""+journalId+"""' OR VenuesIds.id is NULL AND volume = """+volume+""" AND issue = """+issue+"""
                        )
                        """
            
            result = read_sql(query, con)  

            query2 = """
                    SELECT citation as cites1, id as citation
                    FROM Citations LEFT JOIN PublicationsIds ON Citations.citation == PublicationsIds.publication_id
                    ;
                    """

            citations = read_sql(query2, con)  
            result = merge(result, citations, left_on="cites", right_on="cites1", how="left").drop_duplicates()
            result = result.drop(columns=["cites","cites1"]).reset_index(drop=True)   
            
            Venues = getVenues(self.dbPath, result)

            if len(Venues) > 0:
                listOfIdx = []
                for idx, row in Venues.iterrows(): 
                    if Venues.loc[idx, "publication_id"] not in result["publication_id"]:
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    Venues.drop(Venues.index[listOfIdx], inplace=True)           
                
                result = concat([result, Venues], ignore_index=True) 

            result = result.replace(float("NaN"), "").drop_duplicates()     
            result["publication_year"] = result["publication_year"].astype("string")
            result["publication_year"] = result["publication_year"].str.replace("\.0", "", regex=True)   
            result["issue"] = result["issue"].astype("string")
            result["issue"] = result["issue"].str.replace("\.0", "", regex=True) 
            result["volume"] = result["volume"].astype("string")
            result["volume"] = result["volume"].str.replace("\.0", "", regex=True)  
            return result


    def getJournalArticlesInVolume(self, volume, journalId): 
        with connect(self.dbPath) as con:

            query = """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, issue, volume, AuthorsIds.id as author_id, given_name, family_name,
                               VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, citation as cites
                        FROM JournalArticles 
                        LEFT JOIN PublicationsIds On JournalArticles.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON JournalArticles.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON JournalArticles.publication_id == Citations.reference
                        
                        LEFT JOIN Journals ON JournalArticles.venue_id == Journals.venue_id
                        LEFT JOIN VenuesIds ON Journals.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                        
                        WHERE JournalArticles.publication_id IN (     
                            SELECT JournalArticles.publication_id
                            FROM JournalArticles 
                            LEFT JOIN Journals ON JournalArticles.venue_id == Journals.venue_id 
                            LEFT JOIN VenuesIds ON Journals.venue_id == VenuesIds.venue_id
                            WHERE VenuesIds.id = '"""+journalId+"""' OR VenuesIds.id is NULL AND volume = """+volume+"""
                        )
                        """
            
            result = read_sql(query, con) 
            query2 = """
                    SELECT citation as cites1, id as citation
                    FROM Citations LEFT JOIN PublicationsIds ON Citations.citation == PublicationsIds.publication_id
                    ;
                    """

            citations = read_sql(query2, con)  
            result = merge(result, citations, left_on="cites", right_on="cites1", how="left").drop_duplicates()
            result = result.drop(columns=["cites","cites1"]).reset_index(drop=True)  

            Venues = getVenues(self.dbPath, result)

            if len(Venues) > 0:
                listOfIdx = []
                for idx, row in Venues.iterrows(): 
                    if Venues.loc[idx, "publication_id"] not in result["publication_id"]:
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    Venues.drop(Venues.index[listOfIdx], inplace=True)             
                
                result = concat([result, Venues], ignore_index=True)    

            result = result.replace(float("NaN"), "").drop_duplicates()     
            result["publication_year"] = result["publication_year"].astype("string")
            result["publication_year"] = result["publication_year"].str.replace("\.0", "", regex=True)   
            result["issue"] = result["issue"].astype("string")
            result["issue"] = result["issue"].str.replace("\.0", "", regex=True) 
            result["volume"] = result["volume"].astype("string")
            result["volume"] = result["volume"].str.replace("\.0", "", regex=True)  
            return result


    def getJournalArticlesInJournal(self, journalId):        
        with connect(self.dbPath) as con:

            query1 =  """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, issue, volume, AuthorsIds.id as author_id, given_name, family_name, citation 
                            FROM PublicationsIds LEFT JOIN JournalArticles ON PublicationsIds.publication_id == JournalArticles.publication_id
                            LEFT JOIN PublicationsAuthors ON PublicationsIds.publication_id == PublicationsAuthors.publication_id
                            LEFT JOIN AuthorsIds ON PublicationsAuthors.author_id == AuthorsIds.author_id
                            LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id
                            LEFT JOIN Citations ON JournalArticles.publication_id == Citations.reference               
                            ;
                            """
               
            JournalArticles = read_sql(query1, con)                               
        
            # call venues        
            query2 = """SELECT VenuesIds.venue_id as int_id, VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, PublicationsIds.id as publication_id
                    FROM Journals
                    JOIN PublicationsVenues ON Journals.venue_id == PublicationsVenues.venue_id
                    LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                    LEFT JOIN VenuesIds ON PublicationsVenues.venue_id == VenuesIds.venue_id
                    LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id                     
                    LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                            WHERE VenuesIds.id  = '"""+journalId+"""'    
                        ;
                        """
            Journals = read_sql(query2, con)                    

            query3 = """
                    SELECT VenuesIds.venue_id as int_id, VenuesIds.id as venue_id, NULL as venue_title, NULL as publisher_id, NULL as name, PublicationsIds.id as publication_id
                    FROM VenuesIds
                    LEFT JOIN PublicationsVenues ON VenuesIds.venue_id == PublicationsVenues.venue_id
                    LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                    WHERE VenuesIds.venue_id NOT IN (
                        SELECT VenuesIds.venue_id 
                        FROM Books
                        LEFT JOIN VenuesIds ON Books.venue_id == VenuesIds.venue_id 
                        WHERE VenuesIds.id = '"""+journalId+"""' 
                        UNION
                        SELECT VenuesIds.venue_id
                        FROM Proceedings
                        LEFT JOIN VenuesIds ON Proceedings.venue_id == VenuesIds.venue_id 
                        WHERE VenuesIds.id = '"""+journalId+"""' )
                        AND VenuesIds.id = '"""+journalId+"""'
                         ;
                                      """  

            Venues = read_sql(query3, con)    

            if len(Venues) > 0:
                listOfIdx = []
                # check if the id found in venues ids is already in journals (csv info), if it is it drops the duplicate
                for idx, row in Venues.iterrows(): 
                    if Venues.loc[idx, "venue_id"] in Journals["venue_id"].unique():
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    Venues.drop(Venues.index[listOfIdx], inplace=True)
        
                Journals = concat([Venues, Journals])

            publications = merge(JournalArticles, Journals, left_on="publication_id", right_on="publication_id", how="right") #i do it on publication id because I don't necessarily have venue_ids
            publications = publications.drop(columns=["int_id"])
     
            # substitute doi to publication_id in citation column
            query4 = """SELECT citation as cites1, id as citation_id
                        FROM Citations LEFT JOIN PublicationsIds ON Citations.citation == PublicationsIds.publication_id
                        ;
                        """
            citations = read_sql(query4, con)        
        
        result = merge(publications, citations, left_on="citation", right_on="cites1", how="left").drop_duplicates()

        result = result.drop(columns=["citation","cites1"]).reset_index(drop=True)
        result = result.rename(columns = {"citation_id" : "citation"})

        result = result.replace(float("NaN"), "")
        result["publication_year"] = result["publication_year"].astype("string")
        result["publication_year"] = result["publication_year"].str.replace("\.0", "", regex=True)   
        result["issue"] = result["issue"].astype("string")
        result["issue"] = result["issue"].str.replace("\.0", "", regex=True) 
        result["volume"] = result["volume"].astype("string")
        result["volume"] = result["volume"].str.replace("\.0", "", regex=True)  
        return result

    def getProceedingsByEvent(self, eventPartialName): 
        eventPartialName = eventPartialName.lower()
        with connect(self.dbPath) as con:

            query = """SELECT PublicationsIds.id as publication_id, VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, event
                    FROM Proceedings
                    JOIN PublicationsVenues ON Proceedings.venue_id == PublicationsVenues.venue_id
                    LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                    LEFT JOIN VenuesIds ON PublicationsVenues.venue_id == VenuesIds.venue_id
                    LEFT JOIN PublishersIds ON Proceedings.publisher_id == PublishersIds.publisher_id                     
                    LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id             
                    
                    WHERE LOWER(event) LIKE '%"""+eventPartialName+"""%'
                    ;

                    """
            Proceedings = read_sql(query, con)  

            eventsFromJson = getVenues(self.dbPath, Proceedings) 
            
            if len(eventsFromJson) > 0:
                listOfIdx = []
                for idx, row in eventsFromJson.iterrows():
                    txt = str(row["event"])
                    if re.search(eventPartialName, txt.lower()) is None:
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    eventsFromJson.drop(eventsFromJson.index[listOfIdx], inplace =True)
            
                Proceedings = concat([Proceedings, eventsFromJson], ignore_index=True)
            Proceedings = Proceedings.fillna("")
                    
            return Proceedings


    def getPublicationAuthors(self, id):     
        with connect(self.dbPath) as con:

            query = """SELECT PublicationsIds.id as publication_id, AuthorsIds.id as author_id, given_name, family_name
                        FROM Authors JOIN PublicationsAuthors ON Authors.author_id == PublicationsAuthors.author_id 
                        JOIN PublicationsIds ON PublicationsAuthors.publication_id == PublicationsIds.publication_id
                        JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                        WHERE PublicationsIds.id = '"""+id+"""'
                            ;        

                        """ 
                    
            result = read_sql(query, con)    
            return result


    def getPublicationsByAuthorName(self,partName):
        partName = str(partName).lower()
        fullName = list(partName.split(" "))   
        if len(fullName) > 1: 
            givenName = partName[0]  
            familyName = partName[1]    
        else: 
            givenName = partName
            familyName = partName
        with connect(self.dbPath) as con:     

            queryJA= """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, issue, volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name,
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, citation as cites
                        FROM JournalArticles 
                        LEFT JOIN PublicationsIds On JournalArticles.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON JournalArticles.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON JournalArticles.publication_id == Citations.reference
                        
                        LEFT JOIN Journals ON JournalArticles.venue_id == Journals.venue_id
                        LEFT JOIN VenuesIds ON Journals.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                        
                        WHERE JournalArticles.publication_id IN (
                        SELECT JournalArticles.publication_id
                        FROM JournalArticles 
                        LEFT JOIN PublicationsIds On JournalArticles.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON JournalArticles.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        WHERE LOWER(family_name) LIKE '%"""+familyName+"""%' AND LOWER(given_name) LIKE '%"""+givenName+"""%' 
                        OR LOWER(family_name) LIKE '%"""+familyName+"""%' OR LOWER(given_name) LIKE '%"""+givenName+"""%')
                        """
            JournalArticles = read_sql(queryJA, con)
            publicationType = []
                
            while len(JournalArticles) != len(publicationType):
                publicationType.append("journal-article")
                    
            JournalArticles.insert(3, "publication_type", Series(publicationType, dtype="string"))

            venueType = []
            for idx, row in JournalArticles.iterrows(): 
                if JournalArticles.loc[idx, "venue_id"] != "":
                    venueType.append("journal")
                else: 
                    venueType.append("")
            JournalArticles.insert(11, "venue_type", Series(venueType, dtype="string"))

            queryBC =   """
                        SELECT PublicationsIds.id as publication_id, publication_title, publication_year, NULL as issue, NULL as volume, chapter_number, AuthorsIds.id as author_id, given_name, family_name,
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, citation as cites
                        FROM BookChapters 
                        LEFT JOIN PublicationsIds On BookChapters.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON BookChapters.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON BookChapters.publication_id == Citations.reference

                        LEFT JOIN Books ON BookChapters.venue_id == Books.venue_id
                        LEFT JOIN VenuesIds ON Books.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Books.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id

                        WHERE BookChapters.publication_id IN (
                        SELECT BookChapters.publication_id
                        FROM BookChapters 
                        LEFT JOIN PublicationsIds On BookChapters.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON BookChapters.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        WHERE LOWER(family_name) LIKE '%"""+familyName+"""%' AND LOWER(given_name) LIKE '%"""+givenName+"""%'
                        OR LOWER(family_name) LIKE '%"""+familyName+"""%' OR LOWER(given_name) LIKE '%"""+givenName+"""%'
                        )
                        
                        """

            BookChapters = read_sql(queryBC, con)
            publicationType = []
                
            while len(BookChapters) != len(publicationType):
                publicationType.append("book-chapter")
                    
            BookChapters.insert(3, "publication_type", Series(publicationType, dtype="string"))

            venueType = []
            for idx, row in BookChapters.iterrows(): 
                if row["venue_id"] != "":
                    venueType.append("book")
                else: 
                    venueType.append("")
            BookChapters.insert(11, "venue_type", Series(venueType, dtype="string"))

            queryPP = """
                        SELECT PublicationsIds.id as publication_id, publication_title, publication_year, NULL as issue, NULL as volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name, 
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, event, citation as cites
                        FROM ProceedingsPapers
                        LEFT JOIN PublicationsIds On ProceedingsPapers.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON ProceedingsPapers.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON ProceedingsPapers.publication_id == Citations.reference

                        LEFT JOIN Proceedings ON ProceedingsPapers.venue_id == Proceedings.venue_id
                        LEFT JOIN VenuesIds ON Proceedings.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Proceedings.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id

                        WHERE ProceedingsPapers.publication_id IN (
                        SELECT ProceedingsPapers.publication_id
                        FROM ProceedingsPapers 
                        LEFT JOIN PublicationsIds On ProceedingsPapers.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON ProceedingsPapers.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        WHERE LOWER(family_name) LIKE '%"""+familyName+"""%' AND LOWER(given_name) LIKE '%"""+givenName+"""%'
                        OR LOWER(family_name) LIKE '%"""+familyName+"""%' OR LOWER(given_name) LIKE '%"""+givenName+"""%')
                        """

            ProceedingsPapers = read_sql(queryPP, con)
            publicationType = []
                
            while len(ProceedingsPapers) != len(publicationType):
                publicationType.append("proceedings-paper")
                    
            ProceedingsPapers.insert(3, "publication_type", Series(publicationType, dtype="string"))

            venueType = []
            for idx, row in ProceedingsPapers.iterrows(): 
                if row["venue_id"] != "":
                    venueType.append("proceedings")
                else: 
                    venueType.append("")

            ProceedingsPapers.insert(11, "venue_type", Series(venueType, dtype="string"))

            query2 = """SELECT citation as cites1, id as citation
                        FROM Citations LEFT JOIN PublicationsIds ON Citations.citation == PublicationsIds.publication_id
                        ;
                        """

            citations = read_sql(query2, con)

            Publications = concat([JournalArticles, BookChapters, ProceedingsPapers])
            result = merge(Publications, citations, left_on="cites", right_on="cites1", how="left").drop_duplicates()
            result = result.drop(columns=["cites","cites1"]).reset_index(drop=True) 
            Venues = getVenues(self.dbPath, result)

            if len(Venues) > 0: 
                listOfIdx = []
                for idx, row in Venues.iterrows(): 
                    if Venues.loc[idx, "publication_id"] not in result["publication_id"]:
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    Venues.drop(Venues.index[listOfIdx], inplace=True)

                result = concat([result, Venues]).drop_duplicates()
            result.replace(float("NaN"), "", inplace=True) 
            
            result["publication_year"] = result["publication_year"].astype("string")
            result["publication_year"] = result["publication_year"].str.replace("\.0", "", regex=True)   
            result["issue"] = result["issue"].astype("string")
            result["issue"] = result["issue"].str.replace("\.0", "", regex=True) 
            result["volume"] = result["volume"].astype("string")
            result["volume"] = result["volume"].str.replace("\.0", "", regex=True)  
            result["chapter_number"] = result["chapter_number"].astype("string")
            result["chapter_number"] = result["chapter_number"].str.replace("\.0", "", regex=True)
            return result


    #GET DISTINCT PUBLISHER OF PUBLICATIONS
    def getDistinctPublisherOfPublications(self, pubIdList):
        with connect(self.dbPath) as con:
            
            AllPublishers = DataFrame({})
            externalJson = DataFrame({})
            for id in pubIdList:
                query = """
                        SELECT PublicationsIds.id as publication_id, PublishersIds.id as publisher_id, name
                        FROM PublishersIds
                        LEFT JOIN Publishers ON PublishersIds.publisher_id = Publishers.publisher_id
                        LEFT JOIN Journals ON PublishersIds.publisher_id == Journals.publisher_id
                        LEFT JOIN PublicationsVenues ON Journals.venue_id == PublicationsVenues.venue_id
                        LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                        WHERE Publishers.publisher_id = (
                            SELECT Publishers.publisher_id 
                            FROM Publishers
                            LEFT JOIN Journals ON Publishers.publisher_id == Journals.publisher_id
                            LEFT JOIN PublicationsVenues ON Journals.venue_id == PublicationsVenues.venue_id
                            LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                            WHERE PublicationsIds.id = '"""+id+"""')
                        UNION ALL
                        SELECT PublicationsIds.id as publication_id, PublishersIds.id as publisher_id, name
                        FROM PublishersIds
                        LEFT JOIN Publishers ON PublishersIds.publisher_id = Publishers.publisher_id
                        LEFT JOIN Books ON PublishersIds.publisher_id == Books.publisher_id
                        LEFT JOIN PublicationsVenues ON Books.venue_id == PublicationsVenues.venue_id
                        LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                        WHERE Publishers.publisher_id = (
                            SELECT Publishers.publisher_id 
                            FROM Publishers
                            LEFT JOIN Books ON Publishers.publisher_id == Books.publisher_id
                            LEFT JOIN PublicationsVenues ON Books.venue_id == PublicationsVenues.venue_id
                            LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                            WHERE PublicationsIds.id = '"""+id+"""')
                        UNION ALL
                        SELECT PublicationsIds.id as publication_id, PublishersIds.id as publisher_id, name
                        FROM PublishersIds
                        LEFT JOIN Publishers ON PublishersIds.publisher_id = Publishers.publisher_id
                        LEFT JOIN Proceedings ON PublishersIds.publisher_id == Proceedings.publisher_id
                        LEFT JOIN PublicationsVenues ON Proceedings.venue_id == PublicationsVenues.venue_id
                        LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                        WHERE Publishers.publisher_id = (
                            SELECT Publishers.publisher_id 
                            FROM Publishers
                            LEFT JOIN Proceedings ON Publishers.publisher_id == Proceedings.publisher_id
                            LEFT JOIN PublicationsVenues ON Proceedings.venue_id == PublicationsVenues.venue_id
                            LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                            WHERE PublicationsIds.id = '"""+id+"""')
                        """

                Publishers = read_sql(query, con)
                AllPublishers = concat([AllPublishers, Publishers])
                
                Venues = getVenues(self.dbPath, AllPublishers)
                
                listOfIdx = []
                if len(Venues) > 0: 
                    for idx, row in Venues.iterrows(): 
                        if Venues.loc[idx, "publication_id"] != id or Venues.loc[idx, "publisher_id"] in AllPublishers["publisher_id"]:
                            listOfIdx.append(idx)
                    if len(listOfIdx) > 0:
                        Venues.drop(Venues.index[listOfIdx], inplace=True)
                
                    Venues = Venues.drop(columns=["publication_id","venue_title", "event", "venue_type"]).fillna("")
                    externalJson = concat([externalJson, Venues])


            AllPublishers = concat([AllPublishers, externalJson])
            AllPublishers = AllPublishers.drop_duplicates()
            AllPublishers = AllPublishers.replace(float("NaN"), "") 
            return AllPublishers

    def getPublisherById(self, id):
        with connect(self.dbPath) as con:
            query ="""
                    SELECT id as publisher_id, name
                    FROM Publishers
                    LEFT JOIN PublishersIds ON Publishers.publisher_id == PublishersIds.publisher_id
                    WHERE id = '"""+id+"""'
                    """
            Publisher = read_sql(query, con)
        return Publisher

    def getAuthorByDoi(self, doi):
        with connect(self.dbPath) as con:
            query = """
                    SELECT AuthorsIds.id as author_id, given_name,  family_name
                    FROM Authors
                    LEFT JOIN PublicationsAuthors ON Authors.author_id == PublicationsAuthors.author_id
                    LEFT JOIN PublicationsIds ON PublicationsAuthors.publication_id == PublicationsIds.publication_id
                    LEFT JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                    WHERE PublicationsIds.id = '"""+doi+"""'
                    """
            Author = read_sql(query, con)
        return Author

    def getVenuesByDoi(self, doi):
        with connect(self.dbPath) as con:
            query = """
                    SELECT PublicationsIds.id as publication_id, VenuesIds.id as venue_id
                    FROM VenuesIds
                    LEFT JOIN PublicationsVenues ON VenuesIds.venue_id == PublicationsVenues.venue_id
                    LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                    WHERE PublicationsIds.id = '"""+doi+"""'
                    """
            VenuesIds = read_sql(query, con)
        return VenuesIds

    def getVenuesById(self, setOfIds): 
        with connect(self.dbPath) as con:
            VenuesIds = DataFrame({"venue_id": Series(dtype = "string")})
            for i in setOfIds: 
                query = """
                        SELECT VenuesIds.id as venue_id
                        FROM VenuesIds
                        LEFT JOIN PublicationsVenues ON VenuesIds.venue_id == PublicationsVenues.venue_id
                        LEFT JOIN PublicationsIds ON PublicationsVenues.publication_id == PublicationsIds.publication_id
                        WHERE PublicationsIds.publication_id IN (
                            SELECT PublicationsIds.publication_id 
                            FROM PublicationsIds
                            LEFT JOIN PublicationsVenues ON PublicationsIds.publication_id = PublicationsVenues.publication_id
                            LEFT JOIN VenuesIds ON PublicationsVenues.venue_id = VenuesIds.venue_id
                            WHERE VenuesIds.id = '"""+i+"""'
                        )
                        """
                OtherIds = read_sql(query, con)
                VenuesIds = concat([VenuesIds, OtherIds])
            VenuesIds = VenuesIds.drop_duplicates()
        return VenuesIds

    def getPublicationByDoi(self, doi): 
        with connect(self.dbPath) as con: 
            AllCitations = DataFrame({})

            queryJA= """SELECT PublicationsIds.id as publication_id, publication_title, publication_year, issue, volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name,
                        VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, citation as cites
                    FROM JournalArticles 
                    LEFT JOIN PublicationsIds On JournalArticles.publication_id == PublicationsIds.publication_id
                    LEFT JOIN PublicationsAuthors ON JournalArticles.publication_id == PublicationsAuthors.publication_id
                    LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                    LEFT JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                    LEFT JOIN Citations ON JournalArticles.publication_id == Citations.reference
                    
                    LEFT JOIN Journals ON JournalArticles.venue_id == Journals.venue_id
                    LEFT JOIN VenuesIds ON Journals.venue_id  == VenuesIds.venue_id 
                    LEFT JOIN PublishersIds ON Journals.publisher_id == PublishersIds.publisher_id
                    LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id
                    
                    WHERE PublicationsIds.id = \""""+doi+"""\"
                    """
            JournalArticles = read_sql(queryJA, con)
            publicationType = []
                
            while len(JournalArticles) != len(publicationType):
                publicationType.append("journal-article")
                    
            JournalArticles.insert(3, "publication_type", Series(publicationType, dtype="string"))

            venueType = []
            for idx, row in JournalArticles.iterrows(): 
                if JournalArticles.loc[idx, "venue_id"] != "":
                    venueType.append("journal")
                else: 
                    venueType.append("")
            JournalArticles.insert(11, "venue_type", Series(venueType, dtype="string"))

            queryBC =   """
                        SELECT PublicationsIds.id as publication_id, publication_title, NULL as issue, NULL as volume, publication_year, chapter_number, AuthorsIds.id as author_id, given_name, family_name,
                                VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, NULL as event, citation as cites
                        FROM BookChapters 
                        LEFT JOIN PublicationsIds On BookChapters.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON BookChapters.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON BookChapters.publication_id == Citations.reference

                        LEFT JOIN Books ON BookChapters.venue_id == Books.venue_id
                        LEFT JOIN VenuesIds ON Books.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Books.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id

                        WHERE PublicationsIds.id = \""""+doi+"""\"
                        """

            BookChapters = read_sql(queryBC, con)
            publicationType = []
                
            while len(BookChapters) != len(publicationType):
                publicationType.append("book-chapter")
                    
            BookChapters.insert(3, "publication_type", Series(publicationType, dtype="string"))

            venueType = []
            for idx, row in BookChapters.iterrows(): 
                if row["venue_id"] != "":
                    venueType.append("book")
                else: 
                    venueType.append("")
            BookChapters.insert(11, "venue_type", Series(venueType, dtype="string"))

            queryPP = """
                        SELECT PublicationsIds.id as publication_id, publication_title, publication_year, NULL as issue, NULL as volume, NULL as chapter_number, AuthorsIds.id as author_id, given_name, family_name, 
                               VenuesIds.id as venue_id, venue_title, PublishersIds.id as publisher_id, name, event, citation as cites
                        FROM ProceedingsPapers
                        LEFT JOIN PublicationsIds On ProceedingsPapers.publication_id == PublicationsIds.publication_id
                        LEFT JOIN PublicationsAuthors ON ProceedingsPapers.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id 
                        LEFT JOIN AuthorsIds On Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON ProceedingsPapers.publication_id == Citations.reference

                        LEFT JOIN Proceedings ON ProceedingsPapers.venue_id == Proceedings.venue_id
                        LEFT JOIN VenuesIds ON Proceedings.venue_id  == VenuesIds.venue_id 
                        LEFT JOIN PublishersIds ON Proceedings.publisher_id == PublishersIds.publisher_id
                        LEFT JOIN Publishers ON PublishersIds.publisher_id == Publishers.publisher_id

                        WHERE PublicationsIds.id = \""""+doi+"""\"
                        """

            ProceedingsPapers = read_sql(queryPP, con)
            publicationType = []
                
            while len(ProceedingsPapers) != len(publicationType):
                publicationType.append("proceedings-paper")
                    
            ProceedingsPapers.insert(3, "publication_type", Series(publicationType, dtype="string"))

            venueType = []
            for idx, row in ProceedingsPapers.iterrows(): 
                if row["venue_id"] != "":
                    venueType.append("book")
                else: 
                    venueType.append("")
            ProceedingsPapers.insert(11, "venue_type", Series(venueType, dtype="string"))

            AllCitations = concat([AllCitations, JournalArticles, BookChapters, ProceedingsPapers])

            if len(AllCitations) == 0:
                queryPub = """
                        SELECT PublicationsIds.id as publication_id, NULL as publication_title, NULL as issue, NULL as volume, NULL as chapter_number, NULL as publication_year, AuthorsIds.id as author_id, family_name, given_name, 
                            PublicationsVenues.venue_id as int_id, VenuesIds.id as venue_id, NULL as venue_title, NULL as publisher_id, NULL as name, NULL as event, citation as cites
                        FROM PublicationsIds
                        LEFT JOIN PublicationsVenues ON PublicationsIds.publication_id == PublicationsVenues.publication_id
                        LEFT JOIN VenuesIds ON PublicationsVenues.venue_id == VenuesIds.venue_id
                        LEFT JOIN PublicationsAuthors ON PublicationsIds.publication_id == PublicationsAuthors.publication_id
                        LEFT JOIN Authors ON PublicationsAuthors.author_id == Authors.author_id
                        LEFT JOIN AuthorsIds ON Authors.author_id == AuthorsIds.author_id
                        LEFT JOIN Citations ON PublicationsIds.publication_id == Citations.reference
                        WHERE PublicationsIds.id = '"""+doi+"""'
                        """
                Publications = read_sql(queryPub, con)

                publicationType = []
            
                while len(Publications) != len(publicationType):
                    publicationType.append("")
                        
                Publications.insert(3, "publication_type", Series(publicationType, dtype="string"))

                venue_type = []
                    
                while len(Publications) != len(venue_type):
                    venue_type.append("")
                        
                Publications.insert(11, "venue_type", Series(venue_type, dtype="string"))

                AllCitations = concat([AllCitations, Publications])
            
            query2 = """SELECT citation as cites1, id as citation
            FROM Citations LEFT JOIN PublicationsIds ON Citations.citation == PublicationsIds.publication_id
            ;
            """

            citations = read_sql(query2, con)
            
            AllCitations = merge(AllCitations, citations, left_on="cites", right_on="cites1", how="left").drop_duplicates()
            AllCitations = AllCitations.drop(columns=["cites","cites1"]).reset_index(drop=True)

            Venues = getVenues(self.dbPath, AllCitations)

            if len(Venues) > 0: 
                listOfIdx = []
                for idx, row in Venues.iterrows(): 
                    if Venues.loc[idx, "publication_id"] != doi:
                        listOfIdx.append(idx)
                if len(listOfIdx) > 0:
                    Venues.drop(Venues.index[listOfIdx], inplace=True)

                AllCitations = concat([AllCitations, Venues], ignore_index=True).drop_duplicates().reset_index(drop=True)

            
            AllCitations.replace(float("NaN"), "", inplace=True)  

            AllCitations["publication_year"] = AllCitations["publication_year"].astype("string")
            AllCitations["publication_year"] = AllCitations["publication_year"].str.replace("\.0", "", regex=True)   
            AllCitations["issue"] = AllCitations["issue"].astype("string")
            AllCitations["issue"] = AllCitations["issue"].str.replace("\.0", "", regex=True) 
            AllCitations["volume"] = AllCitations["volume"].astype("string")
            AllCitations["volume"] = AllCitations["volume"].str.replace("\.0", "", regex=True)  
            AllCitations["chapter_number"] = AllCitations["chapter_number"].astype("string")
            AllCitations["chapter_number"] = AllCitations["chapter_number"].str.replace("\.0", "", regex=True)
            
            return AllCitations

def getVenues(dbPath, result):   
    
    headers = result.columns.values
    try:                
        nameOfTheDb = dbPath.split(".")
        nameOfJson = nameOfTheDb[0]+".json"
        with open(nameOfJson, "r", encoding="utf-8") as jsonData: 
            dataVenues = load(jsonData)

        if len(dataVenues) > 0:
            dois = []
            venueTitle = []
            publisher = []
            venueType = []
            event = []
            for k in dataVenues:
                dois.append(k)
                venueTitle.append(dataVenues[k]["venue_title"])
                publisher.append(dataVenues[k]["publisher"])
                venueType.append(dataVenues[k]["venue_type"])
                event.append(dataVenues[k]["event"])

            Venues = DataFrame(columns = headers)        
            Venues["publication_id"] = Series(dois)                
            Venues["venue_title"] = Series(venueTitle)                
            Venues["publisher_id"] = Series(publisher)                
            Venues["venue_type"] = Series(venueType)                
            Venues["event"] = Series(event)                
            Venues["event"].replace("False", "", inplace=True) 

        else: 
            Venues = DataFrame(columns = headers)  
    except: 
        Venues = DataFrame(columns = headers)

    return Venues
