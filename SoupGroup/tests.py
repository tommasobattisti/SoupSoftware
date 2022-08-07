from genericQueryProcessor import GenericQueryProcessor
from relationalDataProcessor import RelationalDataProcessor
from relationalQueryProcessor import RelationalQueryProcessor
from triplestoreDataProcessor import *
from triplestoreQueryProcessor import *

"""
dp = RelationalDataProcessor()
setDb = dp.setDbPath("relationalDb.db")
upload = dp.uploadData("data/relational_other_data.json")


"""

bg = TriplestoreDataProcessor()
bg.setEndpointUrl("http://192.168.1.24:9999/blazegraph/sparql")
"""
bg.uploadData("data/graph_publications.csv")
bg.uploadData("data/graph_other_data.json")
bg.uploadData("data/relational_publications.csv")
bg.uploadData("data/relational_other_data.json")
"""




rp = RelationalQueryProcessor()
rp.setDbPath("relationalDb.db")

tp = TriplestoreQueryProcessor()
tp.setEndpointUrl("http://192.168.1.24:9999/blazegraph/sparql")


gp = GenericQueryProcessor()
gp.addQueryProcessor(tp)

(print("__________________\nget publications published in 2020:\n"))
publication_in_2020 = gp.getPublicationsPublishedInYear("2020")
count = 0
for el in publication_in_2020:
    count += 1
    print("the publication with doi:", el.getIds(), "\nhas year of publication:", el.getPublicationYear(), "\nhasVenue", el.getPublicationVenue())
    if el.getPublicationVenue() != None:
        print("\nWhose ids are:", el.getPublicationVenue().getIds(), "\n__________________\n")
    else:
        print("the publication has no venue", "\n__________________\n")

print("Length ==", count)
