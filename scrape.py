from classes import MetaFarms
import time

GROUP_ARR = ["1538Mern12", "1542Nieb", "1546Mern34", "1546Huel24", "1548Wadd", ]
##GROUP_ARR = ["1527WarnFin", "1529Tole", "1530W2F", "1525Mern34", "1532Bear", "1531ProbSte", "1532Huel18"]
FEED_ARR = ["home_mill", "south_central_fs"]

DOWNLOAD_PATH_MARKET = "C:\Users\Jonathan.WSF\SharePoint\Walk Stock Farm - Management\Jonathan\MetaFarms\Market Sales Weight Projection"
DOWNLOAD_PATH_GROUP = "C:\Users\Jonathan.WSF\SharePoint\Walk Stock Farm - Management\Jonathan\MetaFarms\Group Detail Closeout"
DOWNLOAD_PATH = "C:\Users\Jonathan.WSF\Downloads"
DOWNLOAD_PATH_MF = "C:\Users\Jonathan.WSF\Desktop\MetaFarms\downloads"

mf = MetaFarms("", DOWNLOAD_PATH_MARKET)

mf.getGroupDetailCloseout(GROUP_ARR)
##mf.getFeedUsageReport("12/01/2015", "12/31/2015", "producer", "Walk Stock Farm", FEED_ARR)
##mf.getGroupList("producer", "All Producers")

##mf.getProducers()
##mf.getSites()

time.sleep(2)
mf.close()

print "END"
