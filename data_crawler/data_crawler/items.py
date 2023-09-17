# items
# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class UniformCrawlerItem(scrapy.Item):
    date = scrapy.Field()
    parse_date = scrapy.Field()
    status = scrapy.Field()
    table = scrapy.Field()
    items = scrapy.Field()

    def keys(self):
        return ['date', 'parse_date', 'status', 'table', 'items']

class RevenueCrawlerItem(scrapy.Item):
    code = scrapy.Field()
    revenue = scrapy.Field()
    mom = scrapy.Field()
    yoy = scrapy.Field()
    cum_revenue = scrapy.Field()
    cum_yoy = scrapy.Field()
    note = scrapy.Field()