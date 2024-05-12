from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from bookscrapy.spiders.bookCrawler import BookcrawlerSpider


process = CrawlerProcess(get_project_settings())
process.crawl(BookcrawlerSpider)
process.start()
