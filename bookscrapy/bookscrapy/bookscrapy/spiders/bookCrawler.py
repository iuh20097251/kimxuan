import scrapy
from bookscrapy.items import BookscrapyItem


class BookcrawlerSpider(scrapy.Spider):
    name = "bookCrawler"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com"]

    def parse(self, response):
        courseList = response.xpath('//*[@id="default"]/div/div/div/div/section/div[2]/descendant::ol/li/article/div/a/@href').getall()
        for courseItem in courseList:
            item = BookscrapyItem()
            item['BookUrl'] = response.urljoin(courseItem)
            request = scrapy.Request(url = response.urljoin(courseItem), callback=self.parseCourseDetailPage)
            request.meta['datacourse'] = item
            yield request
            
        nextPage = response.xpath('normalize-space(string(//*[@id="default"]/div/div/div/div/section/div[2]/div/ul/li[last()]/a/@href))').get()
        if(nextPage!=''):
            yield scrapy.Request(url=response.urljoin(nextPage), callback=self.parse)
    
    def parseCourseDetailPage(self, response):
        item = response.meta['datacourse']
        item['bookname'] = response.xpath('normalize-space(string(//*[@id="content_inner"]/article/div[1]/div[2]/h1))').get()
        item['cost'] = response.xpath('normalize-space(string(//*[@id="content_inner"]/article/div[1]/div[2]/p[1]))').get()
        item['stock'] = response.xpath('normalize-space(string(//*[@id="content_inner"]/article/div[1]/div[2]/p[2]))').get()
        item['description'] = response.xpath('normalize-space(string(//*[@id="content_inner"]/article/p))').get()
        yield item
        
    
    
