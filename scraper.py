import scrapy
from scrapy.crawler import CrawlerProcess
from json import loads
from time import gmtime, strftime


class polParser(scrapy.Spider):
    name = "polParser"
    start_urls = ["https://boards.4chan.org/pol/archive"]

    def parse(self, response):
        # creates a list of all the thread ids
        threadIdList = response.xpath('//table[@class="flashListing"]/tbody/tr/td[1]/text()').getall()

        for threadId in threadIdList:
            threadJsonUrl = "https://a.4cdn.org/pol/thread/{threadId}.json".format(threadId=threadId)
            yield scrapy.Request(threadJsonUrl, callback=self.parseThread, cb_kwargs=dict(threadId=threadId))

    def parseThread(self, response, threadId):
        # converts content of JSON request into json dict
        threadData = loads(response.body.decode('utf-8'))

        # parses each post in the thread
        for post in threadData['posts']:
            postNo = post['no']
            threadUrl = "https://boards.4chan.org/pol/thread/{threadNo}#p{postNo}".format(threadNo=threadId, postNo=postNo)
            time = post['now'].split(")")[1]
            date = post['now'].split("(")[0]
            try:
                countryCode = post['country']
            except KeyError:
                countryCode = ""
            try:
                country = post['country_name']
            except KeyError:
                country = ""
            try:
                replies = post['replies']
            except KeyError:
                replies = ""
            try:
                imageUrl = "https://is2.4chan.org/pol/{tim}{ext}".format(tim=post['tim'], ext=post['ext'])
            except KeyError:
                imageUrl = ""
            try:
                name = post['name']
            except KeyError:
                name = ""
            try:
                title = post['sub']
            except KeyError:
                title = ""
            try:
                text = post['com']
                sel = scrapy.Selector(text=text.replace("<br>", "\n"))
                text = [t.replace(r"\'", "'") for t in sel.xpath("//*/descendant-or-self::*/text()").extract()]
                text = "".join(text)
            except KeyError:
                text = ""

            data = {"Post Number": postNo, "Thread Number": threadId, "Thread URL": threadUrl,
                    "Time": time, "Date": date, "Country Code": countryCode,
                    "Country": country, "Replies": replies, "Image URL": imageUrl, "Name": name,
                    "Title": title, "Content": text}

            yield data


filename = "{n}.csv".format(n=strftime("%Y-%m-%d", gmtime()))
process = CrawlerProcess(settings={
    "FEEDS": {
        filename: {"format": "csv",
                   "encoding": 'utf-8'}
    }
})

process.crawl(polParser)
process.start()
