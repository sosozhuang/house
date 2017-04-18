# -*- coding: utf-8 -*-
import json
import time

from scrapy import signals
from scrapy.exceptions import NotConfigured
import logging
from twisted.internet import protocol
from scrapy.utils.reactor import listen_tcp
from twisted.internet.protocol import Protocol

from house.items import SecondhandHouseItem, SoldHouseItem

logger = logging.getLogger(__name__)


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


class SparkStreamingExt(protocol.ServerFactory):
    def __init__(self, host, port_range):
        self.host = host
        self.port_range = port_range
        self.port = None
        self.protocols = []

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('SPARK_STREAMING_ENABLED'):
            raise NotConfigured

        host = crawler.settings.get('SPARK_STREAMING_HOST', 'localhost')
        port_range = [9990, crawler.settings.getint('SPARK_STREAMING_PORT')]
        ext = cls(host, port_range)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.start_listening, signal=signals.engine_started)
        crawler.signals.connect(ext.stop_listening, signal=signals.engine_stopped)

        return ext

    def item_scraped(self, item, spider):
        for p in self.protocols:
            p.send(item)

    def start_listening(self):
        self.port = listen_tcp(self.port_range, self.host, self)
        h = self.port.getHost()
        logger.debug('listening on %(host)s: %(port)d',
                     {'host': h.host, 'port': h.port})

    def stop_listening(self):
        if self.port:
            self.port.stopListening()

    def protocol(self):
        p = JsonLineItemProcotol()
        self.protocols.append(p)
        return p


class JsonLineItemProcotol(Protocol):

    def send(self, item):
        data = {}
        if isinstance(item, SecondhandHouseItem):
            data['type'] = 1
        elif isinstance(item, SoldHouseItem):
            data['type'] = 2
            for x in ['comm', 'deal', 'unit']:
                data.setdefault(x, item[x])
        else:
            return

        for x in ['city', 'main']:
            data.setdefault(x, item[x])
        self.transport.write('%s\n' % json.dumps(data))
