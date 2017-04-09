# -*- coding: utf-8 -*-
import time

from scrapy import signals
from scrapy.exceptions import NotConfigured


class IdleSpider(object):
    def __init__(self, crawler, timeout):
        self.timeout = timeout
        self.crawler = crawler
        self.checked = time.time()
        self.items = 0

    @classmethod
    def from_crawler(cls, crawler):
        timeout = crawler.settings.getint('IDLESPIDER_TIMEOUT', 0)
        if timeout <= 0:
            raise NotConfigured

        ext = cls(crawler, timeout)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.request_scheduled, signal=signals.request_scheduled)

        return ext

    def request_scheduled(self, spider):
        now = time.time()
        if now - self.checked >= self.timeout:
            if self.items == 0:
                self.crawler.engine.close_spider(spider, 'no item scraped')
            else:
                spider.log("Scraped %d item in last %d seconds." % (self.items, self.timeout))
                self.items = 0
            self.checked = now

    def item_scraped(self, item, spider):
        self.items += 1

