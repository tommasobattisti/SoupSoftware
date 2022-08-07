from sqlite3 import connect
from pandas import *
from json import load, dump 

def replace_entity_reference(df):
    df["title"] = df["title"].str.lower().replace("&amp;", "&", regex = True).str.title()
    df["publication_venue"] = df["publication_venue"].str.lower().replace("&amp;", "&", regex = True).str.title()
    df["title"] = df["title"].str.lower().replace("&quot;", "&", regex = True).str.title()
    df["publication_venue"] = df["publication_venue"].str.lower().replace("&quot;", "&", regex = True).str.title()
    df["title"] = df["title"].str.lower().replace("&lt;", "&", regex = True).str.title()

    return df

#UPLOAD CSV FUNCTION
def upload_csv(path, dbPath):
    #OPEN THE CONNECTION TO THE DATABASE
    with connect(dbPath) as con:

        #READ THE CSV FILE
        publications = read_csv(path,
        keep_default_na=False,
        dtype= {
            "id": "string",               
            "title" : "string",            
            "type" : "string" ,             
            "publication_year" : "string",   
            "issue" : "string",              
            "volume" :"string",              
            "chapter" :"string" ,         
            "publication_venue" : "string" ,  
            "venue_type" :"string",           
            "publisher" :"string" ,          
            "event" :"string"   
        })

        publications = replace_entity_reference(publications)
        publications.rename(columns = {"title" : "publication_title", "chapter" : "chapter_number"}, inplace=True)

        #SEE IF ON THE DATABASE THERE ARE DATA COMING FROM A JSON FILE
        try: 
            query = "SELECT * FROM PublicationsIds"
            PublicationsIds = read_sql(query, con)

            query = "SELECT * FROM PublishersIds"
            PublishersIds = read_sql(query, con)

            #UPDATE THE PUBLICATIONS IDS TABLE
           
            lastIndex = PublicationsIds.drop_duplicates(subset=["publication_id"]).reset_index().last_valid_index()

            publicationsIdsUpdate = []
            doisUpdate = []

            for doi in publications["id"].unique(): 
                if doi not in PublicationsIds["id"].unique() and doi not in doisUpdate: 
                    lastIndex += 1     
                    publicationsIdsUpdate.append("publication-" + str(lastIndex))
                    doisUpdate.append(doi)  

            S1 = concat([PublicationsIds["publication_id"], Series(publicationsIdsUpdate, dtype="string")])
            S2 = concat([PublicationsIds["id"], Series(doisUpdate, dtype="string")])

            PublicationsIds = DataFrame({"publication_id": S1, "id": S2})
            PublicationsIds = PublicationsIds.reset_index(drop=True)

            #UPDATE THE PUBLISHERS IDS TABLE
            lastIndex = PublishersIds.drop_duplicates(subset=["publisher_id"]).reset_index().last_valid_index()

            publishersIdsUpdate = []
            crossrefsUpdate = []

            for crossref in publications["publisher"].unique():
                if crossref not in PublishersIds["id"].unique() and crossref not in crossrefsUpdate and crossref != "":    
                    lastIndex += 1
                    publishersIdsUpdate.append("publisher-" + str(lastIndex))
                    crossrefsUpdate.append(crossref)     

            S1 = concat([PublishersIds["publisher_id"], Series(publishersIdsUpdate, dtype="string")])
            S2 = concat([PublishersIds["id"], Series(crossrefsUpdate, dtype="string")])

            PublishersIds = DataFrame({"publisher_id": S1, "id": S2})
            PublishersIds = PublishersIds.reset_index(drop=True)

        except: #create the new tables of PublicationsIds and PublishersIds
            
            #CREATE THE PUBLICATIONS IDS TABLE
            PublicationsIds = publications[["id"]].drop_duplicates() #creates a new subdataframe with values in column id

            publicationsInternalIds = []
            for idx, row in PublicationsIds.iterrows():
                publicationsInternalIds.append("publication-"+ str(idx)) #creates an internal id

            PublicationsIds.insert(0, "publication_id", Series(publicationsInternalIds, dtype="string")) #finally, creates a new sub dataframe with publication in Id and Publication DOI
       
            #CREATE THE PUBLISHERS IDS TABLE
            PublishersIds = publications["publisher"] #publisher column

            PublishersIds.replace("", float("NaN"), inplace=True)  # to replace all empty strings with NaN values
            PublishersIds.dropna(inplace=True)  # remove all the NaN values

            PublishersIds = PublishersIds.drop_duplicates().reset_index(drop=True)
            
            internalIds = []

            idx = 0
            for publisher in PublishersIds:
                internalIds.append("publisher-"+ str(idx)) #creates an internal id
                idx += 1

            PublishersIds = DataFrame({"publisher_id": Series(internalIds, dtype="string"), "id": PublishersIds})

        #OUT OF THE TRY/EXCEPT 

        #CREATE THE JOURNAL ARTICLES TABLE
        journalArticles = publications.query("type == 'journal-article'")

        journalArticlesDf = merge(journalArticles, PublicationsIds, left_on = "id", right_on="id") #merges all journal article types with internal id

        JournalArticles = journalArticlesDf[["publication_id", "publication_title", "issue", "volume", "publication_year"]] #creates the actual JournalArticles table

        #CREATE THE BOOK CHAPTERS TABLE
        bookChapters = publications.query("type == 'book-chapter'")

        bookChaptersDf = merge(bookChapters, PublicationsIds, left_on = "id", right_on="id")

        BookChapters = bookChaptersDf[["publication_id", "publication_title", "chapter_number", "publication_year"]] 
        
        #CREATE PROCEEDINGS PAPERS TABLE
        proceedingsPapers = publications.query("type == 'proceedings-paper'")
        
        proceedingsPapersDf = merge(proceedingsPapers, PublicationsIds, left_on = "id", right_on="id")

        ProceedingsPapers = proceedingsPapersDf[["publication_id", "publication_title", "publication_year"]] 
        
        try: #see if there are the tables of the publications
                      
            query = "SELECT * FROM JournalArticles"
            JournalArticlesOld = read_sql(query, con)

            query = "SELECT * FROM BookChapters"
            BookChaptersOld = read_sql(query, con)

            query = "SELECT * FROM ProceedingsPapers"
            ProceedingsPapersOld = read_sql(query, con)
            
            query = "SELECT * FROM PublicationsVenues"
            PublicationsVenues = read_sql(query, con)

            #UPDATE THE OLD JOURNAL ARTICLES TABLE
            JournalArticles = concat([JournalArticlesOld, JournalArticles]).drop_duplicates(subset= ["publication_id"], keep="last").reset_index(drop=True).drop(["venue_id"], axis = 1) 

            #UPDATE THE OLD BOOK CHAPTERS TABLE
            BookChapters = concat([BookChaptersOld, BookChapters]).drop_duplicates(subset= ["publication_id"], keep="last").reset_index(drop=True).drop(["venue_id"], axis = 1) 

            #UPDATE THE OLD PROCEEDINGS PAPERS TABLE
            ProceedingsPapers = concat([ProceedingsPapersOld, ProceedingsPapers]).drop_duplicates(subset= ["publication_id"], keep="last").reset_index(drop=True).drop(["venue_id"], axis = 1) 
  
            query = "SELECT * FROM Journals"
            JournalsOld = read_sql(query, con)

            query = "SELECT * FROM Books"
            BooksOld = read_sql(query, con)

            query = "SELECT * FROM Proceedings"
            ProceedingsOld = read_sql(query, con)

            #CREATE THE JOURNAL TABLE (VENUES)
            Journals = publications.query("venue_type == 'journal'")[["id", "publication_venue", "publisher"]]

            mergeIds = merge(Journals, PublishersIds, left_on = "publisher", right_on="id")[["publication_venue", "publisher_id", "id_x"]]

            journalsMergedDf = merge(mergeIds, PublicationsIds, left_on="id_x", right_on="id") #this table will be used for the second passage
            journalsMergedDf = merge(journalsMergedDf, PublicationsVenues, left_on="publication_id", right_on="publication_id")

            Journals = journalsMergedDf[["venue_id", "publication_venue", "publisher_id"]].drop_duplicates(subset=["venue_id"]).reset_index(drop=True)
            Journals.rename(columns = {"publication_venue" : "venue_title"}, inplace=True)

            #UPDATE THE TABLE OF JOURNALS
            Journals = concat([JournalsOld, Journals]).drop_duplicates(subset=["venue_id"], keep="last").reset_index(drop=True)

            #CREATE THE BOOKS TABLE (VENUES)
            Books = publications.query("venue_type == 'book'")

            mergedIds = merge(Books, PublishersIds, left_on = "publisher", right_on="id")[["publication_venue", "publisher_id", "id_x"]]

            booksMergedDf = merge(mergedIds, PublicationsIds, left_on="id_x", right_on="id")  #this table will be used for the second passage
            booksMergedDf = merge(booksMergedDf, PublicationsVenues, left_on="publication_id", right_on="publication_id")

            Books = booksMergedDf[["venue_id", "publication_venue", "publisher_id"]].drop_duplicates(subset=["venue_id"]).reset_index(drop=True)
            Books.rename(columns = {"publication_venue" : "venue_title"}, inplace=True)
            #UPDATE THE TABLE OF BOOKS
            Books = concat([BooksOld, Books]).drop_duplicates(subset=["venue_id"], keep="last").reset_index(drop=True)

            #CREATE THE PROCEEDINGS TABLE (VENUES)
            Proceedings = publications.query("venue_type == 'proceedings'")
            mergedIds = merge(Proceedings, PublishersIds, left_on = "publisher", right_on="id")[["publication_venue", "publisher_id", "event", "id_x"]]
            proceedingsMergedDf = merge(mergedIds, PublicationsIds, left_on="id_x", right_on="id") #this table will be used for the second passage
            proceedingsMergedDf = merge(proceedingsMergedDf, PublicationsVenues, left_on="publication_id", right_on="publication_id")
       

            Proceedings = proceedingsMergedDf[["venue_id", "publication_venue", "publisher_id", "event"]].drop_duplicates(subset=["publication_venue"]).reset_index(drop=True)
            Proceedings.rename(columns = {"publication_venue" : "venue_title"}, inplace=True)
           
            #UPDATE THE TABLE OF PROCEEDINGS
            Proceedings = concat([ProceedingsOld, Proceedings]).drop_duplicates(subset=["venue_id"], keep="last").reset_index(drop=True)
            loadedVenues = list(concat([journalsMergedDf["id"], booksMergedDf["id"], proceedingsMergedDf["id"]]))
                      
            #CREATION OF THE JSON DICTIONARY FOR THE REMAINING VALUES OF THE VENUES
            jsonDict = {}
            listOfInnerDict = []
            doiWithVenues = []
            index = 0
            for idx, row in publications.iterrows():
                innerDict = {}
                doi = publications.loc[idx, "id"]
                venueTitle = publications.loc[idx, "publication_venue"]
                publisher = publications.loc[idx,"publisher"]
                event = publications.loc[idx, "event"]
                venueType = publications.loc[idx, "venue_type"]
                
                if doi not in loadedVenues: 
                    if venueTitle != "":
                        
                        doiWithVenues.append(doi)

                        innerDict["venue_title"] = venueTitle
                        innerDict["publisher"] = publisher
                        innerDict["venue_type"] = venueType
                        
                        if event != "":
                            innerDict["event"] = event
                        else:
                            innerDict["event"] = False

                        listOfInnerDict.append(innerDict)
                
            for doi in doiWithVenues:
                jsonDict[doi] = listOfInnerDict[index]
                index += 1

            nameOfTheDb = dbPath.split(".")
            nameOfJson = nameOfTheDb[0]+".json"
    

            try: 
                with open(nameOfJson, "r", encoding="utf-8") as jsonVenue:
                    venueData = load(jsonVenue)
                
                venueData.update(jsonDict)
                
                #DUMP THE DICTIONARY IN AN EXTERNAL JSON FILE
                with open(nameOfJson, "w", encoding="utf-8") as jFile: #then set a path
                    dump(venueData, jFile, ensure_ascii=False, indent=4)
            
            except: 
                #DUMP THE DICTIONARY IN AN EXTERNAL JSON FILE
                with open(nameOfJson, "w", encoding="utf-8") as jFile: #then set a path
                    dump(jsonDict, jFile, ensure_ascii=False, indent=4)
            
        except: #now we are sure there is no json, create the empty dataframe of the tables created by the json
            
            #CREATION OF JSON DICTIONARY FOR THE VENUES  
            jsonDict = {}
            listOfInnerDict = []
            doiWithVenues = []
            index = 0
            for idx, row in publications.iterrows():
                innerDict = {}
                doi = publications.loc[idx, "id"]
                venueTitle = publications.loc[idx, "publication_venue"]
                publisher = publications.loc[idx,"publisher"]
                event = publications.loc[idx, "event"]
                venueType = publications.loc[idx, "venue_type"]

                if venueTitle != "":
                    
                    doiWithVenues.append(doi)

                    innerDict["venue_title"] = venueTitle
                    innerDict["publisher"] = publisher
                    innerDict["venue_type"] = venueType
                    
                    if event != "":
                        innerDict["event"] = event
                    else:
                        innerDict["event"] = False

                    listOfInnerDict.append(innerDict)
                
            for doi in doiWithVenues:
                jsonDict[doi] = listOfInnerDict[index]
                index += 1
            
            nameOfTheDb = dbPath.split(".")
            nameOfJson = nameOfTheDb[0]+".json"
            #DUMP THE DICTIONARY IN THE JSON FILE
            with open(nameOfJson, "w", encoding="utf-8") as jFile: #then set a path
                dump(jsonDict, jFile, ensure_ascii=False, indent=4)

            #CREATE THE EMPTY DATAFRAME OF THE VENUESIDS TABLE
            VenuesIds = DataFrame({"venue_id": Series(dtype="string"), "id": Series(dtype="string")})

            #CREATE THE EMPTY DATAFRAME OF THE AUTHORS AND AUTHORSIDS
            Authors = DataFrame({"author_id": Series(dtype="string"), "given_name": Series(dtype="string"), "family_name": Series(dtype="string")})
            AuthorsIds = DataFrame({"author_id": Series(dtype="string"), "id": Series(dtype="string")})

            #CREATE THE EMPTY DATAFRAME OF THE PUBLISHERS TABLE
            Publishers = DataFrame({"publisher_id": Series(dtype="string"), "name": Series(dtype="string")})

            #CREATE THE EMPTY DATAFRAME OF THE CITATIONS TABLE
            Citations = DataFrame({"reference": Series(dtype="string"), "citation": Series(dtype="string")})

            #CREATE THE EMPTY DATAFRAME OF THE PUBLICATIONSAUTHORS TABLE
            PublicationsAuthors = DataFrame({"publication_id": Series(dtype="string"), "author_id": Series(dtype="string")})

            #CREATE THE EMPTY DATAFRAME OF THE PUBLICATIONSVENUES TABLE
            PublicationsVenues = DataFrame({"publication_id": Series(dtype="string"), "venue_id": Series(dtype="string")})
            
            #COMMIT THE NEW TABLES TO THE DATABASE
            VenuesIds.to_sql("VenuesIds", con, if_exists="replace", index=False)
            Authors.to_sql("Authors", con, if_exists="replace", index=False)
            AuthorsIds.to_sql("AuthorsIds", con, if_exists="replace", index=False)
            Publishers.to_sql("Publishers", con, if_exists="replace", index=False)
            Citations.to_sql("Citations", con, if_exists="replace", index=False)
            PublicationsAuthors.to_sql("PublicationsAuthors", con, if_exists="replace", index=False)
            PublicationsVenues.to_sql("PublicationsVenues", con, if_exists="replace", index=False)

            #CREATE THE EMPTY DATAFRAME OF THE VENUES TABLES
            Journals = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string")})
            Books = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string")})
            Proceedings = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string"), "event": Series(dtype="string")})

        #OUT OF THE TRY/EXCEPT 

        #ADD THE VENUE_ID COLUMN TO THE JOURNAL ARTICLES TABLE (the PublicationsVenues has been retrieved from the database or created empty by this function)
        JournalArticles = merge(JournalArticles, PublicationsVenues, left_on="publication_id", right_on="publication_id", how="left")
        JournalArticles.replace(float("NaN"), "", inplace=True)
        JournalArticles = JournalArticles[["publication_id", "publication_title", "issue", "volume", "publication_year", "venue_id"]]
        
        #ADD THE VENUE_ID COLUMN TO THE BOOK CHAPTERS TABLE
        BookChapters = merge(BookChapters, PublicationsVenues, left_on="publication_id", right_on="publication_id", how="left")
        BookChapters.replace(float("NaN"), "", inplace=True)
        BookChapters = BookChapters[["publication_id", "publication_title", "chapter_number", "publication_year", "venue_id"]]

        #ADD THE VENUE_ID COLUMN TO THE PROCEEDINGS PAPERS TABLE
        ProceedingsPapers = merge(ProceedingsPapers, PublicationsVenues, left_on="publication_id", right_on="publication_id", how="left")
        ProceedingsPapers.replace(float("NaN"), "", inplace=True)
        ProceedingsPapers = ProceedingsPapers[["publication_id", "publication_title", "publication_year", "venue_id"]]


        #UPLOAD THE VENUES TABLES ON THE DATABASE
        Journals.to_sql("Journals", con, if_exists="replace", index=False)
        Books.to_sql("Books", con, if_exists="replace", index=False)
        Proceedings.to_sql("Proceedings", con, if_exists="replace", index=False)
        #UPLOAD THE PUBLICATIONS TABLES ON THE DATABASE
        PublicationsIds.to_sql("PublicationsIds", con, if_exists="replace", index=False)
        PublishersIds.to_sql("PublishersIds", con, if_exists="replace", index=False)
        JournalArticles.to_sql("JournalArticles", con, if_exists="replace", index=False)
        BookChapters.to_sql("BookChapters", con, if_exists="replace", index=False)
        ProceedingsPapers.to_sql("ProceedingsPapers", con, if_exists="replace", index=False)


#UPDATE JSON FUNCTION
def upload_json(path, dbPath):                                      
    with connect(dbPath) as con: #we connect to the database
        with open(path, "r", encoding="utf-8") as otherData:
            relationalOtherData = load(otherData)

        #DIVIDE THE DICTIONARY IN SUB-DICTIONARIES
        authorsDict = relationalOtherData.get("authors")
        venuesIdsDict = relationalOtherData.get("venues_id")
        publishersDict = relationalOtherData.get("publishers")
        citationsDict = relationalOtherData.get("references")

        #ITERATING OVER CITATIONS
        citingPub = []
        citedPub = []
        dois = []

        for k in citationsDict:
            dois.append(k)
            for d in citationsDict[k]:
                citingPub.append(k)
                citedPub.append(d)

        #ITERATIONS ON AUTHORS DICT
        fNameList = []
        gNameList = []
        orcidList = []
        publicationList = []

        for k in authorsDict:
            for d in authorsDict[k]:
                fNameList.append(d["family"])
                gNameList.append(d["given"])
                orcidList.append(d["orcid"])
                publicationList.append(k)

        try: 
            #Publications Ids
            query = "SELECT * FROM PublicationsIds"
            PublicationsIds = read_sql(query, con)

            #PublishersIds
            query = "SELECT * FROM PublishersIds"
            PublishersIds = read_sql(query, con)

            #PublishersIds
            query = "SELECT * FROM Publishers"
            Publishers = read_sql(query, con)

            #VenuesIds
            query = "SELECT * FROM VenuesIds"
            VenuesIds = read_sql(query, con)

            #ITERATION ON VENUES IDS
            pubDois = []
            venuesId = []
            dictIds = dict()

            if len(VenuesIds) > 0:
                lastIndex = VenuesIds.drop_duplicates(subset=["venue_id"]).reset_index(drop=True).last_valid_index()+1
            else:
                lastIndex = 0

            for k in venuesIdsDict:
                check = False
                venueExists = False
                venueInVenues = False
                for d in venuesIdsDict[k]:
                    if d in dictIds: 
                        venueExists = True
                        venueIdx = dictIds[d]
                        break
                for d in venuesIdsDict[k]: 
                    for idx, row in VenuesIds.iterrows(): 
                        if d == row["id"]: 
                            oldVenue = row["venue_id"]
                            venueInVenues = True
                            break
                for d in venuesIdsDict[k]:
                    if venueInVenues: 
                        dictIds[d] = oldVenue
                    else: 
                        if venueExists: 
                            dictIds[d] = venueIdx
                        else:
                            dictIds[d] = "venue-"+str(lastIndex)
                            check = True
                    venuesId.append(d)
                    pubDois.append(k)
                if check == True: 
                    lastIndex += 1

            #CREATION OF THE SERIES WITH ALL THE DOIS
            Dois = concat([Series(dois, dtype="string"), Series(publicationList, dtype ="string")])
            Dois = concat([Series(Dois, dtype="string"), Series(pubDois, dtype ="string")]).drop_duplicates()
         
            #UPDATE THE VENUES IDS TABLE
            if len(dictIds) > 0:
                venuesIds = DataFrame(list(dictIds.items())).rename(columns={0: "id", 1: "venue_id"})
                VenuesIds = concat([VenuesIds, venuesIds]).drop_duplicates().reset_index(drop=True)

            #UPDATE OF THE PUBLICATIONSIDS TABLE
            lastIndex = PublicationsIds.drop_duplicates(subset=["publication_id"]).reset_index().last_valid_index()

            publicationsIdsUpdate = []
            doisUpdate = []

            for doi in Dois.unique(): 
                if doi not in PublicationsIds["id"].unique() and doi not in doisUpdate: 
                    lastIndex += 1     
                    publicationsIdsUpdate.append("publication-" + str(lastIndex))
                    doisUpdate.append(doi)   

            S1 = concat([PublicationsIds["publication_id"], Series(publicationsIdsUpdate, dtype="string")])
            S2 = concat([PublicationsIds["id"], Series(doisUpdate, dtype="string")])

            PublicationsIds = DataFrame({"publication_id": S1, "id": S2})
            PublicationsIds = PublicationsIds.reset_index(drop=True)                       

            #UPDATE OF THE PUBLISHERS IDS TABLE
            lastIndex = PublishersIds.drop_duplicates(subset="publisher_id").reset_index().last_valid_index()
            
            crossrefs = []
            publishersNames = []

            for k in publishersDict:
                crossrefs.append(publishersDict[k]["id"])
                publishersNames.append(publishersDict[k]["name"])
    
            publishersIdsUpdate = []
            crossrefsUpdate = []
            
            for crossref in crossrefs:
                if crossref not in PublishersIds["id"].unique() and crossref not in crossrefsUpdate:    
                    lastIndex += 1
                    publishersIdsUpdate.append("publisher-" + str(lastIndex))
                    crossrefsUpdate.append(crossref)     

            S1 = concat([PublishersIds["publisher_id"], Series(publishersIdsUpdate, dtype="string")])
            S2 = concat([PublishersIds["id"], Series(crossrefsUpdate, dtype="string")])

            PublishersIds = DataFrame({"publisher_id": S1, "id": S2})
            PublishersIds = PublishersIds.reset_index(drop=True)      

            #UPDATE THE PUBLISHERS TABLE
            publishers = merge(Publishers, PublishersIds, left_on="publisher_id", right_on="publisher_id")
            S1 = concat([publishers["id"], Series(crossrefs, dtype="string")])
            S2 = concat([publishers["name"], Series(publishersNames, dtype="string")])
            publishers = DataFrame({"id": S1, "name": S2})
            Publishers = merge(publishers, PublishersIds, left_on="id", right_on="id")[["publisher_id", "name"]]
            Publishers = Publishers.drop_duplicates(keep="last").reset_index(drop=True)

            #Citations
            query = "SELECT * FROM Citations"
            Citations = read_sql(query, con)

            #AuthorsIds
            query = "SELECT * FROM AuthorsIds"
            AuthorsIds = read_sql(query, con)

            #Authors
            query = "SELECT * FROM Authors"
            Authors = read_sql(query, con)

            #PublicationsAuthors
            query = "SELECT * FROM PublicationsAuthors"
            PublicationsAuthors = read_sql(query, con)

            #PublicationsVenues
            query = "SELECT * FROM PublicationsVenues"
            PublicationsVenues = read_sql(query, con)

            #UPDATE OF THE CITATIONS TABLE
            citationsUpdate = DataFrame({"reference": Series(citingPub, dtype="string"), "citation": Series(citedPub, dtype="string")})
            citationsUpdate = merge(citationsUpdate, PublicationsIds, left_on="citation", right_on="id")[["reference", "publication_id"]].rename(columns={"publication_id": "citation"})

            citationsUpdate = merge(citationsUpdate, PublicationsIds, left_on="reference", right_on="id")[["publication_id", "citation"]].rename(columns={"publication_id": "reference"})

            S1 = concat([Citations["reference"], citationsUpdate["reference"]])
            S2 = concat([Citations["citation"], citationsUpdate["citation"]])

            Citations = DataFrame({"reference": S1, "citation": S2}).drop_duplicates().reset_index(drop=True)

            #UPDATE OF THE AUTHORS IDS TABLE
            if len(AuthorsIds) > 0:
                lastIndex = AuthorsIds.drop_duplicates(subset=["author_id"]).reset_index().last_valid_index()
            else:
                lastIndex = 0

            authorIdUpdate = []
            idUpdate = []

            for id in orcidList:
                if id not in AuthorsIds["id"].unique() and id not in idUpdate:    
                    lastIndex += 1
                    authorIdUpdate.append("author-" + str(lastIndex))
                    idUpdate.append(id)

            S1 = concat([AuthorsIds["author_id"], Series(authorIdUpdate, dtype="string")])
            S2 = concat([AuthorsIds["id"], Series(idUpdate, dtype="string")])

            AuthorsIds = DataFrame({"author_id": S1, "id": S2})
            AuthorsIds = AuthorsIds.reset_index(drop=True)
            
            #UPDATE OF THE AUTHORS TABLE
            authors = DataFrame({"given_name": Series(gNameList, dtype="string"), "family_name": Series(fNameList, dtype="string"), "orcid": Series(orcidList, dtype="string"), "doi":Series(publicationList, dtype = "string") })
            authors = merge(AuthorsIds, authors, left_on="id", right_on="orcid")
            
            S1 = concat([Authors["given_name"], authors["given_name"]])
            S2 = concat([Authors["family_name"], authors["family_name"]])
            S3 = concat([Authors["author_id"], authors["author_id"]])

            Authors = DataFrame({"author_id": S3, "given_name": S1, "family_name": S2})
            Authors = Authors.drop_duplicates(subset=["author_id"], keep="last").reset_index(drop=True)
            
            #UPDATE OF THE PUBLICATIONS-AUTHORS TABLE
            publicationsAuthors = authors[["orcid", "doi"]]
            publicationsAuthors = merge(publicationsAuthors, PublicationsIds, left_on="doi", right_on="id")[["publication_id", "orcid"]]
            publicationsAuthors = merge(publicationsAuthors, AuthorsIds, left_on="orcid", right_on="id")[["publication_id", "author_id"]]
            S1 = concat([PublicationsAuthors["publication_id"], publicationsAuthors["publication_id"]])
            S2 = concat([PublicationsAuthors["author_id"], publicationsAuthors["author_id"]])
            PublicationsAuthors = DataFrame({"publication_id": S1, "author_id": S2})
            PublicationsAuthors = PublicationsAuthors.drop_duplicates().reset_index(drop=True)
            
            #UPDATE OF THE PUBLICATIONS VENUES TABLE
            publicationsVenues = merge(PublicationsIds, PublicationsVenues, left_on="publication_id", right_on="publication_id")[["id", "venue_id"]].rename(columns={"id":"doi"})
            publicationsVenues = merge(publicationsVenues, VenuesIds, left_on="venue_id", right_on="venue_id")[["id", "doi"]]

            S1 = concat([publicationsVenues["doi"], Series(pubDois, dtype="string")])
            S2 = concat([publicationsVenues["id"], Series(venuesId, dtype="string")])
            publicationsVenues = DataFrame({"doi": S1, "id": S2})

            PublicationsVenues = merge(publicationsVenues, VenuesIds, left_on="id", right_on="id")[["doi", "venue_id"]]  
            PublicationsVenues = merge(PublicationsVenues, PublicationsIds, left_on="doi", right_on="id")[["publication_id", "venue_id"]] 
            PublicationsVenues = PublicationsVenues.drop_duplicates().reset_index(drop=True)

        except: 
            #ITERATION ON VENUES IDS DICT
            pubDois = []
            venuesId = []
            dictIds = dict()

            idx = 0
            for k in venuesIdsDict:
                venue_exists = False
                check = False
                for d in venuesIdsDict[k]:
                    if d in dictIds: 
                        venue_exists = True
                        venueIdx = dictIds[d]
                        break
                for d in venuesIdsDict[k]:
                    if venue_exists:
                        dictIds[d] = venueIdx
                    else:
                        dictIds[d] = "venue-"+str(idx)
                        check = True
                    venuesId.append(d)
                    pubDois.append(k)
                if check == True: 
                    idx += 1

            Dois = concat([Series(dois, dtype="string"), Series(publicationList, dtype ="string"), Series(pubDois, dtype ="string")]).drop_duplicates().reset_index(drop=True)

            #ITERATION ON PUBLISHERS DICT
            crossrefs = []
            publishersNames = []

            for k in publishersDict:
                crossrefs.append(publishersDict[k]["id"])
                publishersNames.append(publishersDict[k]["name"])

            Publishers = DataFrame({"id": Series(crossrefs, dtype="string"), "name": Series(publishersNames, dtype="string")})
            
            #COMPLETE THE PUBLISHERS TABLES 
            pubIntIds = []
            for idx, row in Publishers.iterrows():
                pubIntIds.append("publisher-"+ str(idx)) #creates an internal id

            Publishers.insert(0, "publisher_id", Series(pubIntIds, dtype="string"))

            #PUBLISHERSIDS TABLE
            PublishersIds = Publishers[["publisher_id", "id"]]

            #PUBLISHER TABLE
            Publishers = Publishers[["publisher_id", "name"]]

            #PUBLICATIONS IDS TABLE
            pubInternalIds = []
            idx = 0
            for doi in Dois:
                intId = "publication-" + str(idx)
                pubInternalIds.append(intId)
                idx += 1

            PublicationsIds = DataFrame({"publication_id": Series(pubInternalIds, dtype="string"), "id": Series(Dois, dtype="string")})

            #VENUES IDS TABLE
            if len(dictIds) > 0: 
                VenuesIds = DataFrame(list(dictIds.items())).rename(columns={0: "id", 1: "venue_id"})
                VenuesIds = VenuesIds[["venue_id", "id"]]

            #CITATIONS TABLE
            Citations = DataFrame({"reference": Series(citingPub, dtype="string"), "citation": Series(citedPub, dtype="string")})

            Citations = merge(Citations, PublicationsIds, left_on="citation", right_on="id")[["reference", "publication_id"]].rename(columns={"publication_id": "citation"})
            Citations = merge(Citations, PublicationsIds, left_on="reference", right_on="id")[["publication_id", "citation"]].rename(columns={"publication_id": "reference"})

            #CREATION OF THE TABLES RELATED TO AUTHORS
            Authors = DataFrame({"given_name": Series(gNameList, dtype="string"), "family_name": Series(fNameList, dtype="string"), "id": Series(orcidList, dtype="string")})

            Authors = Authors.drop_duplicates(subset=["id"]).reset_index(drop=True)

            authorOrcid = Authors[["id"]] #creates a new subdataframe with values in column id
            authorInternalId = []
            for idx, row in authorOrcid.iterrows():
                authorInternalId.append("author-"+ str(idx)) #creates an internal id

            Authors.insert(0, "author_id", Series(authorInternalId, dtype="string")) #finally, creates a new sub dataframe with publication in Id and Publication DOI              

            #AUTHORSIDS TABLE
            AuthorsIds = Authors[["author_id", "id"]]

            #AUTHORS TABLE
            Authors = Authors[["author_id", "given_name", "family_name"]]

            #PUBLICATIONSAUTHORS TABLE
            PublicationsAuthors = DataFrame({"doi": Series(publicationList, dtype="string"), "orcid": Series(orcidList, dtype="string")})

            PublicationsAuthors = merge(PublicationsAuthors, AuthorsIds, left_on="orcid", right_on="id")[["doi", "author_id"]]
            PublicationsAuthors = merge(PublicationsAuthors, PublicationsIds, left_on="doi", right_on="id")[["publication_id", "author_id"]]

            #CREATION OF THE PUBLICATION_VENUE table
            publicationsVenues = DataFrame({"doi": Series(pubDois, dtype="string"), "id": Series(venuesId, dtype="string")})

            PublicationsVenues = merge(publicationsVenues, VenuesIds, left_on="id", right_on="id")[["doi", "venue_id"]]
            PublicationsVenues = merge(PublicationsVenues, PublicationsIds, left_on="doi", right_on="id")[["publication_id", "venue_id"]]
            PublicationsVenues = PublicationsVenues.drop_duplicates().reset_index(drop=True)
        
        try: 
            #RETRIEVE ALL THE TABLES OF THE PUBLICATIONS FROM THE DATABASE
            query = "SELECT * from JournalArticles"
            JournalArticles = read_sql(query, con)

            query = "SELECT * from BookChapters"
            BookChapters = read_sql(query, con)

            query = "SELECT * from ProceedingsPapers"
            ProceedingsPapers = read_sql(query, con)

            #RETRIEVE ALL THE VENUES TABLES FROM THE DATABASE 
            query = "SELECT * FROM Journals"
            Journals = read_sql(query, con)

            query = "SELECT * FROM Books"
            Books = read_sql(query, con)

            query = "SELECT * FROM Proceedings"
            Proceedings = read_sql(query, con)

            #UPDATE JOURNAL ARTICLES TABLE
            journalArticles = merge(JournalArticles, PublicationsVenues, left_on = "publication_id", right_on="publication_id", how="left")
            JournalArticles = journalArticles[["publication_id", "publication_title", "issue", "volume", "publication_year", "venue_id_y"]].rename(columns={"venue_id_y":"venue_id"})
            JournalArticles.replace(float("NaN"), "", inplace=True)

            #UPDATE BOOK CHAPTERS TABLE
            bookChapters = merge(BookChapters, PublicationsVenues, left_on = "publication_id", right_on="publication_id", how="left")
            BookChapters = bookChapters[["publication_id", "publication_title", "chapter_number", "publication_year", "venue_id_y"]].rename(columns={"venue_id_y":"venue_id"})
            BookChapters.replace(float("NaN"), "", inplace=True)
                 
            #UPDATE PROCEEDINGS PAPERS TABLE
            proceedingsPapers = merge(ProceedingsPapers, PublicationsVenues, left_on = "publication_id", right_on="publication_id",  how="left") 
            ProceedingsPapers = proceedingsPapers[["publication_id", "publication_title", "publication_year", "venue_id_y"]].rename(columns={"venue_id_y":"venue_id"})
            ProceedingsPapers.replace(float("NaN"), "", inplace=True)
      

            try:
                #TAKE THE VALUES OF THE VENUES FROM THE EXTERNAL JSON AND SEE IF THEY MATCH OUR TABLES
                nameOfTheDb = dbPath.split(".")
                nameOfJson = nameOfTheDb[0]+".json"
                with open(nameOfJson, "r", encoding="utf-8") as jsonData: 
                    dataVenues = load(jsonData)
                    oldDoi = list(dataVenues.keys())

                pubVen = merge(PublicationsVenues, PublicationsIds, left_on="publication_id", right_on="publication_id")
                
                dois = []
                venueTitle = []
                publisher = []
                venueType = []
                event = []
                for k in dataVenues:
                    if k in pubVen["id"].unique(): 
                        dois.append(k)
                        venueTitle.append(dataVenues[k]["venue_title"])
                        publisher.append(dataVenues[k]["publisher"])
                        venueType.append(dataVenues[k]["venue_type"])
                        event.append(dataVenues[k]["event"])
                
                for doi in oldDoi: 
                    if doi in dois:
                        del dataVenues[doi]
                
                Venues = DataFrame({"id":Series(dois, dtype="string"), "venue_title": Series(venueTitle, dtype="string"), "publisher":Series(publisher, dtype="string"), "venue_type": Series(venueType, dtype="string"), "event":Series(event, dtype="string")})
                Venues["event"].replace("False", "", inplace=True)
          
                nameOfTheDb = dbPath.split(".")
                nameOfJson = nameOfTheDb[0]+".json"
                with open(nameOfJson, "w", encoding="utf-8") as jData:
                    dump(dataVenues, jData, ensure_ascii=False, indent=4)

                #CREATE THE JOURNALS TABLE
                JournalsUpdate = Venues.query("venue_type == 'journal'")[["id", "venue_title", "publisher"]]

                mergeIds = merge(JournalsUpdate, PublishersIds, left_on = "publisher", right_on="id")[["venue_title", "publisher_id", "id_x"]]
                mergeIds = merge(mergeIds, PublicationsIds, left_on="id_x", right_on="id")

                JournalsUpdate = merge(mergeIds, PublicationsVenues, left_on="publication_id", right_on="publication_id")[["venue_id", "venue_title", "publisher_id"]].drop_duplicates().reset_index(drop=True)
                
                #CREATE THE BOOKS TABLE
                BooksUpdate = Venues.query("venue_type == 'book'")[["id", "venue_title", "publisher"]]

                mergeIds = merge(BooksUpdate, PublishersIds, left_on = "publisher", right_on="id")[["venue_title", "publisher_id", "id_x"]]
                mergeIds = merge(mergeIds, PublicationsIds, left_on="id_x", right_on="id")
                
                BooksUpdate = merge(mergeIds, PublicationsVenues, left_on="publication_id", right_on="publication_id")[["venue_id", "venue_title", "publisher_id"]].drop_duplicates().reset_index(drop=True)
                
                #CREATE THE PROCEEDINGS TABLE
                ProceedingsUpdate = Venues.query("venue_type == 'proceedings'")[["id", "venue_title", "publisher", "event"]]
                mergeIds = merge(ProceedingsUpdate, PublishersIds, left_on = "publisher", right_on="id")[["venue_title", "publisher_id", "id_x", "event"]]
                mergeIds = merge(mergeIds, PublicationsIds, left_on="id_x", right_on="id")

                ProceedingsUpdate = merge(mergeIds, PublicationsVenues, left_on="publication_id", right_on="publication_id")[["venue_id", "venue_title", "publisher_id", "event"]].drop_duplicates().reset_index(drop=True)

                #UPDATE THE EXSISTENT TABLES WITH THE NEW VALUES
                Journals = concat([JournalsUpdate, Journals]).drop_duplicates(subset=["venue_id"], keep="last").reset_index(drop=True)
                Books = concat([BooksUpdate, Books]).drop_duplicates(subset=["venue_id"], keep="last").reset_index(drop=True)
                Proceedings = concat([ProceedingsUpdate, Proceedings]).drop_duplicates(subset=["venue_id"], keep="last").reset_index(drop=True)

            except: 
                #CREATE THE EMPTY DATAFRAME OF THE VENUES
                Journals = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string")})
                Books = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string")})
                Proceedings = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string"), "event": Series(dtype="string")})
    
        except: 
            try: #c'è già csv?
                nameOfTheDb = dbPath.split(".")
                nameOfJson = nameOfTheDb[0]+".json"
                with open(nameOfJson, "r", encoding="utf-8") as jsonVenue:
                    venueData = load(jsonVenue)

                    if len(venueData) > 0: 
                        venuesDoi = merge(PublicationsVenues, PublicationsIds, left_on="publication_id", right_on="publication_id")
                        DoiInPublicationVenues = venuesDoi.loc[:,"id"]
                        DoiInJson = list(venueData.keys())

                        rows = [] #list of list of rows to append to df

                        for doi in DoiInPublicationVenues:
                            if doi in DoiInJson:
                                dataDict = venueData.get(doi)
                                type = dataDict.get("venue_type")
                                venue_title = dataDict.get("venue_title")
                                publisher = dataDict.get("publisher")
                                event = dataDict.get("event")
                                rows.append([doi, venue_title, type, publisher, event]) #could be a tuple?
                                del venueData[doi]
                                            
                        generalVenues = DataFrame(rows, columns = ["id", "venue_title", "venue_type", "publisher", "event"]) 
                        mergePvVen = merge(venuesDoi, generalVenues, left_on="id", right_on="id")

                        Venues = merge(mergePvVen, PublishersIds, left_on="publisher", right_on="id")     

                #LOAD THE REMAINING DATA INTO THE JSON
                with open(nameOfJson, "w", encoding="utf-8") as jsonData:
                    dump(venueData, jsonData, ensure_ascii=False, indent=4)

                #CREATE JOURNALS TABLE
                venueJournals = Venues.query("venue_type == 'journal'") 
                Journals = venueJournals[["venue_id","venue_title", "publisher_id"]].drop_duplicates().reset_index(drop=True)

                #CREATE BOOKS TABLE
                venueBooks = Venues.query("venue_type == 'book'") 
                Books = venueBooks[["venue_id","venue_title", "publisher_id"]].drop_duplicates().reset_index(drop=True)

                #CREATE PROCEEDINGS TABLE
                venueProceedings = Venues.query("venue_type == 'proceedings'") 
                Proceedings = venueProceedings[["venue_id","venue_title", "publisher_id", "event"]].drop_duplicates().reset_index(drop=True)
                Proceedings["event"].replace(False, "", inplace=True)

            except: 
                #CREATE THE EMPTY DATAFRAME OF THE VENUES
                Journals = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string")})
                Books = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string")})
                Proceedings = DataFrame({"venue_id": Series(dtype="string"), "venue_title": Series(dtype="string"), "publisher_id": Series(dtype="string"), "event": Series(dtype="string")})
            
            #CREATE THE EMPTY DATAFRAME OF THE PUBLICATIONS
            JournalArticles = DataFrame({"publication_id": Series(dtype="string"), "publication_title": Series(dtype="string"), "issue": Series(dtype="string"), "volume": Series(dtype="string"), "publication_year": Series(dtype="string"), "venue_id":  Series(dtype="string")})
            BookChapters = DataFrame({"publication_id": Series(dtype="string"), "publication_title": Series(dtype="string"), "chapter_number": Series(dtype="string"), "publication_year": Series(dtype="string"), "venue_id":  Series(dtype="string")})
            ProceedingsPapers = DataFrame({"publication_id": Series(dtype="string"), "publication_title": Series(dtype="string"), "publication_year": Series(dtype="string"), "venue_id":  Series(dtype="string")})

        #UPLOAD THE NEW TABLES IN THE DATABASE
        Journals.to_sql("Journals", con, if_exists="replace", index=False)
        Books.to_sql("Books", con, if_exists="replace", index=False)
        Proceedings.to_sql("Proceedings", con, if_exists="replace", index=False)
        #UPLOAD THE PUBLICATIONS TABLES ON THE DATABASE
        JournalArticles.to_sql("JournalArticles", con, if_exists="replace", index=False)
        BookChapters.to_sql("BookChapters", con, if_exists="replace", index=False)
        ProceedingsPapers.to_sql("ProceedingsPapers", con, if_exists="replace", index=False)
        #UPLOAD ALL THE TABLES CREATED FROM THE JSON FILE
        PublicationsIds.to_sql("PublicationsIds", con, if_exists="replace", index=False)
        Citations.to_sql("Citations", con, if_exists="replace", index=False)
        AuthorsIds.to_sql("AuthorsIds", con, if_exists="replace", index=False)
        Authors.to_sql("Authors", con, if_exists="replace", index=False)
        PublicationsAuthors.to_sql("PublicationsAuthors", con, if_exists="replace", index=False)
        VenuesIds.to_sql("VenuesIds", con, if_exists="replace", index=False)
        PublicationsVenues.to_sql("PublicationsVenues", con, if_exists="replace", index=False)
        PublishersIds.to_sql("PublishersIds", con, if_exists="replace", index=False)
        Publishers.to_sql("Publishers", con, if_exists="replace", index=False)