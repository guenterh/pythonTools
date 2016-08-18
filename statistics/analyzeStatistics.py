import os
from argparse import ArgumentParser
import re
import sys
import gzip
import datetime

from mongo.MongoWrapper import MongoWrapper


class AnalyzeStatistics:

    def __init__(self, mW):

        self.botQueries = re.compile('Googlebot|bingbot|Slurp|YandexBot|AhrefsBot|MegaIndex\.ru',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.googleBot = re.compile('Googlebot',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.bingBot = re.compile('bingbot',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.slurp = re.compile('Slurp',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.yandex = re.compile('yandex',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.ahRef = re.compile('AhrefsBot',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.Megaindex = re.compile('MegaIndex\.ru',re.UNICODE | re.DOTALL | re.IGNORECASE)


        self.searchQueries = re.compile('\/Search\/Results',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.fullView = re.compile('\/Record\/',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.home = re.compile('GET \/ HTTP|\/Search/Home',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.sru = re.compile('GET \/sru\/search',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.crap = re.compile('the\+art\+of\+computer\+programming|TouchPoint|GET \/themes|GET \/apple-touch-icon|GET \/favicon.ico|GET \/\?lng',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.help = re.compile('GET \/helpPage|GET \/Help/Home',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.myResearch = re.compile('GET \/MyResearch',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.robots = re.compile('GET \/robots\.txt',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.openSearch = re.compile('\/Search\/OpenSearch',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.searchAdvanced = re.compile('GET \/Search\/Advanced',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.mapportal = re.compile('GET \/mapportal\.json',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.searchHistory = re.compile('GET \/Search/History',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.postAjax = re.compile('POST \/AJAX/JSON|OPTIONS \/AJAX/JSON',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.shibboleth = re.compile('GET \/Shibboleth.sso/Session|GET \/Shibboleth.sso/Login|POST \/Shibboleth.sso\/SAML2',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.headRequests = re.compile('HEAD \/',re.UNICODE | re.DOTALL | re.IGNORECASE)


        #self.dateFromLine = re.compile('- - \[(.*?)\]|= \[(.*?)\]',re.UNICODE | re.IGNORECASE)
        self.dateFromLine = re.compile('\[(.*?)\]|\[(.*?)\]',re.UNICODE | re.IGNORECASE)

        # + 1 because of \n
        self.minimumLength = len('"-" - - [31/Dec/2015:03:51:06 +0100] "GET /" 200 17939 "-" "-"') + 1

        self.mongoWrapper = mW

        #self.minmumLineLength =


    def analyzeLine(self,line):

        if len(line) <= self.minimumLength:
            return

        try:
            logDate = self.getDateFromLine(line)

            if self.botQueries.search(line):
                self.mongoWrapper.insertLine(collection='bots',
                                             rawLine=line,
                                             date=logDate,
                                             type=self.getBotType(line))
                return

            if self.home.search(line):
                self.mongoWrapper.insertLine(collection='all',
                                             rawLine=line,
                                             date=logDate,
                                             type='home')

                return

            if self.crap.search(line):
                self.mongoWrapper.insertLine(collection='all',
                                             rawLine=line,
                                             date=logDate,
                                             type='crap')

                return

            if self.searchQueries.search(line):
                self.mongoWrapper.insertLine(collection='search',
                                             rawLine=line,
                                             date=logDate,
                                             type='search')

                return


            if self.openSearch.search(line):
                self.mongoWrapper.insertLine(collection='search',
                                             rawLine=line,
                                             date=logDate,
                                             type='open')


                return

            if self.searchHistory.search(line):
                self.mongoWrapper.insertLine(collection='search',
                                             rawLine=line,
                                             date=logDate,
                                             type='history')

                return

            if self.searchAdvanced.search(line):
                self.mongoWrapper.insertLine(collection='search',
                                             rawLine=line,
                                             date=logDate,
                                             type='advanced')


                return

            if self.fullView.search(line):
                self.mongoWrapper.insertLine(collection='search',
                                             rawLine=line,
                                             date=logDate,
                                             type='full')

                return


            if self.robots.search(line):
                self.mongoWrapper.insertLine(collection='all',
                                             rawLine=line,
                                             date=logDate,
                                             type='robots')

                return
            if self.myResearch.search(line):
                self.mongoWrapper.insertLine(collection='search',
                                             rawLine=line,
                                             date=logDate,
                                             type='myresearch')
                return

            if self.mapportal.search(line):
                self.mongoWrapper.insertLine(collection='all',
                                             rawLine=line,
                                             date=logDate,
                                             type='mapportal')
                return

            if self.help.search(line):
                self.mongoWrapper.insertLine(collection='all',
                                             rawLine=line,
                                             date=logDate,
                                             type='help')
                return

            if self.postAjax.search(line):
                self.mongoWrapper.insertLine(collection='all',
                                             rawLine=line,
                                             date=logDate,
                                             type='ajax')

                return

            if self.shibboleth.search(line):

                self.mongoWrapper.insertLine(collection='all',
                                             rawLine=line,
                                             date=logDate,
                                             type='shibboleth')

                return

            if self.headRequests.search(line):
                self.mongoWrapper.insertLine(collection='all',
                                             rawLine=line,
                                             date=logDate,
                                             type='head')


                return

            self.mongoWrapper.insertLine(collection='all',
                                         rawLine=line,
                                         date=logDate,
                                         type='notknown')


        except Exception as pyE:
            print ("no valid date: " + line)

    def getDateFromLine(self,line):

        try:
            myDate = self.dateFromLine.search(line).group(1)
            dTime = datetime.datetime.strptime(myDate, '%d/%b/%Y:%H:%M:%S +%f')
            return dTime
        except Exception as ex:
            raise ex

    def getBotType(self,line):
        if self.googleBot.search(line):
            return 'google'
        elif self.bingBot.search(line):
            return 'bingbot'
        elif self.yandex.search(line):
            return 'yandex'
        elif self.ahRef.search(line):
            return 'ahRef'
        elif self.Megaindex.search(line):
            return 'megaindex'
        elif self.slurp.search(line):
            return 'slurp'
        else:
            return 'unknown'






if __name__ == '__main__':

    oParser = ArgumentParser()
    oParser.add_argument("-d", "--dir", dest="directory")

    args = oParser.parse_args()


    tDir = args.directory


    correctFileName =re.compile('access.*?.gz')
    numberOfLines = 0
    os.chdir(tDir)
    mW = MongoWrapper()
    for fname in os.listdir(tDir):
        if not correctFileName.search(fname):
            print (fname + ' not correct ')
        sys.stdout.write("".join(["\n\n", "-----", fname, "-----", "\n"]))

        with gzip.open(fname, 'rb') as f:

            pLog = AnalyzeStatistics(mW)
            for line in f:
                pLog.analyzeLine(line)


    sys.stdout.write("".join(["\n\n","-----","number of queries: ",str(numberOfLines),"-----","\n"]))


