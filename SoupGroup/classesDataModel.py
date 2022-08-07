#IDENTIFIABLE ENTITY
class IdentifiableEntity(object):
    def __init__(self, ids):
        self.id = set()
        for identifier in ids:
            self.id.add(identifier)
        #the set didn't work it just split the string into single letters
    def getIds(self):
        return list(self.id) #the set of all the ids of a single object



#PERSON
class Person(IdentifiableEntity):
    def __init__(self, id, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName

        super().__init__(id)
    
    def getGivenName(self):
        return self.givenName
    
    def getFamilyName(self):
        return self.familyName



#PUBLICATION
class Publication(IdentifiableEntity):
    def __init__(self, id, publicationYear, title, author, publicationVenue):
        self.publicationYear = publicationYear
        self.title = title
        self.author = author
        self.publicationVenue = publicationVenue 
        self.cites = [] #None only if None, list if there is more than one
        super().__init__(id)
    
    def addCitedPublication(self, publication):
        self.cites.append(publication)
        return self.cites

    def getPublicationYear(self):
        if self.publicationYear != None:
            return self.publicationYear
        else:
            return None
    
    def getTitle(self):
        return self.title
    
    def getAuthors(self):
       return self.author
            
    def getCitedPublications(self):
        return self.cites               #it must be a list of publications
    
    def getPublicationVenue(self):
        return self.publicationVenue



#JOURNAL ARTICLE

class JournalArticle(Publication):
    def __init__(self, id, publicationYear, title, author, publicationVenue, issue, volume):
        self.issue = issue
        self.volume = volume
        
        super().__init__(id, publicationYear, title, author, publicationVenue)

    def getIssue(self):
        if self.issue != None: #None is the parameter we will pass if the issue is not present
            return self.issue
        else:
            return None

    def getVolume(self):
        if self.volume != None:
            return self.volume
        else:
            return None




#BOOK CHAPTER
class BookChapter(Publication):
    def __init__(self, id, publicationYear, title, author, publicationVenue, chapterNumber):
        self.chapterNumber = chapterNumber
        
        super().__init__(id, publicationYear, title, author, publicationVenue)
    

    def getChapterNumber(self):
        return self.chapterNumber

#PROCEDINGSPAPER
class ProceedingsPaper(Publication):
    def __init__(self, id, publicationYear, title, author, publicationVenue):
        super().__init__(id, publicationYear, title, author, publicationVenue)


#VENUE
class Venue(IdentifiableEntity):
    def __init__(self, id, title, publisher):
        self.title = title
        self.publisher = publisher
        super().__init__(id)

    def getTitle(self):
        return self.title
    
    def getPublisher(self):
        return self.publisher

#Journal
class Journal(Venue):
    def __init__(self, id, title, publisher):
        super().__init__(id, title, publisher)

#Book
class Book(Venue):
    def __init__(self, id, title, publisher):
        super().__init__(id, title, publisher)

#Proceedings
class Proceedings(Venue):
    def __init__(self, id, title, publisher, event):
        self.event = event

        super().__init__(id, title, publisher)

    def getEvent(self):
        return self.event

#ORGANIZATION
class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name
