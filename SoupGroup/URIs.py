from rdflib import URIRef

# Sub-classes of Publications:
JournalArticleURI = URIRef("http://purl.org/spar/fabio/JournalArticle")
BookChapterURI = URIRef("http://purl.org/spar/fabio/BookChapter")
ProceedingsPaperURI = URIRef("http://purl.org/spar/fabio/ProceedingsPaper") 
# Sub-classes of Venues:
JournalURI = URIRef("http://purl.org/spar/fabio/Journal")
BookURI = URIRef("http://purl.org/spar/fabio/Book")
ProceedingsURI = URIRef("http://purl.org/spar/fabio/AcademicProceedings")
# Class of publishers:
OrganizationURI = URIRef("https://schema.org/Organization")
# Class of authors:
PersonURI = URIRef("https://schema.org/Person")

#-----      PROPERTIES:

# General properties:
hasIdentifier = URIRef("http://purl.org/dc/terms/identifier")                   #DataProperty   - exp Literal (str, int)
hasTitle = URIRef("http://purl.org/dc/terms/title")                             #DataProperty   - exp Literal (str)
#   Properties related to Publication and its sub-classes:
hasPublicationYear = URIRef("https://schema.org/datePublished")                 #DataProperty   - exp Literal (ISO 8601 date format)
hasCited = URIRef("http://purl.org/spar/cito/cites")                            #ObjectProperty - exp Entity (Bibliographic entity)
hasPublicationVenue = URIRef("https://schema.org/isPartOf")                     #ObjectProperty - exp Entity (CreativeWork, URL)
hasAuthor = URIRef("https://schema.org/author")                                 #ObjectProperty - exp Entity (Organization, Person)
#       Properties for JournalArticle:
hasIssue = URIRef("https://schema.org/issueNumber")                             #DataProperty   - exp Literal (str, int)
hasVolume = URIRef("https://schema.org/volumeNumber")                           #DataProperty   - exp Literal (str, int)
#       Property for BookChapter:
hasChapterNumber = URIRef("http://purl.org/spar/fabio/hasSequenceIdentifier")   #DataProperty   - exp Literal (str, int)
#   Properties related to Venue and its sub-classes:
hasPublisher = URIRef("https://schema.org/publisher")                           #ObjectProperty - exp Entity (Organization, Person)
#       Property for Proceedings:
hasEvent = URIRef("https://schema.org/description")                             #DataProperty   - exp Literal (str)
#   Property related to Organization:
hasName = URIRef("https://schema.org/name")                                     #DataProperty   - exp Literal (str)
#   Properties related to Person:
hasGivenName = URIRef("https://schema.org/givenName")                           #DataProperty   - exp Literal (str)
hasFamilyName = URIRef("https://schema.org/familyName")                         #DataProperty   - exp Literal (str)