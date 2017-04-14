# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import hashlib
import threading
from random import Random

import time

import thread
from hbase import Hbase
from scrapy import signals
from scrapy.exceptions import IgnoreRequest, NotConfigured
from w3lib.url import safe_url_string

from house.hbase_wrapper import HbaseWrapper


class RandomUserAgentMiddleware(object):

    r = Random()

    def __init__(self, user_agents):
        if not user_agents:
            raise NotConfigured
        self.user_agents = user_agents

    @classmethod
    def from_crawler(cls, crawler):
        s = cls(crawler.settings.get('USER_AGENTS', False))
        # crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        ua = self.r.choice(self.user_agents)
        if ua:
            request.headers.setdefault(b'User-Agent', ua)


class RandomHttpProxyMiddleware(object):
    r = Random()

    @classmethod
    def _get_proxies(cls):
        try:
            cls.mutex.acquire()
            cls.http_proxies = cls.hbase.scan_and_get(cls.tscan)
        finally:
            cls.mutex.release()
        cls.checked = time.time()

    @classmethod
    def from_crawler(cls, crawler):
        # cls.http_proxies = crawler.settings.get('HTTP_PROXIES', False)
        # if not cls.http_proxies:
        #     raise NotConfigured
        host = crawler.settings.get('HBASE_HOST')
        port = crawler.settings.get('HBASE_PORT')
        table = crawler.settings.get('PROXY_TABLE')
        # cls.stats = crawler.stats
        cls.hbase = HbaseWrapper(host, port, table)
        cls.mutex = thread.allocate_lock()
        cls.timeout = crawler.settings.get('PROXIES_TIMEOUT')
        cls.tscan = Hbase.TScan(columns=['cf:0'], caching=True, batchSize=20)
        cls._get_proxies()
        s = cls()
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    @classmethod
    def remove_proxy(cls, proxy):
        try:
            cls.mutex.acquire()
            if proxy in cls.http_proxies:
                cls.http_proxies.remove(proxy)
                cls.hbase.delete(proxy)
        finally:
            cls.mutex.release()
        if not cls.http_proxies:
            return 'proxy list empty'

    @classmethod
    def spider_closed(cls):
        cls.hbase.close()

    def process_request(self, request, spider):
        now = time.time()
        if now - self.checked >= self.timeout:
            self._get_proxies()

        if not self.http_proxies:
            raise IgnoreRequest('proxy list empty')

        proxy = self.r.choice(self.http_proxies)
        if proxy:
            request.meta['proxy'] = proxy
            spider.log('Using proxy: %s' % proxy)


class CaptchaRedirectMiddleware(object):
    allowed_status = (301, 302, 303, 307)

    def __init__(self, crawler):
        self.crawler = crawler
        self.captcha = crawler.settings.get('CAPTCHA_URL')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if 'Location' not in response.headers \
                or response.status not in self.allowed_status:
            return response

        location = safe_url_string(response.headers['Location'])
        if self.captcha in location:
            if 'proxy' in request.meta:
                spider.log('Request <%s> redirected to captcha using proxy: %s.' % (request.url, request.meta['proxy']))
                if RandomHttpProxyMiddleware.remove_proxy(request.meta['proxy']):
                    self.crawler.engine.close_spider(spider, 'proxy list empty')
            redirected = request.replace(url=request.url, method='GET', body='')
            redirected.headers.pop('Content-Type', None)
            redirected.headers.pop('Content-Length', None)
            return redirected

        return response


class ProxyTimeoutMiddleware(object):
    def __init__(self, crawler):
        self.proxy_max_retry_times = crawler.settings.getint('PROXY_RETRY_TIMES', 0)
        if not crawler.settings.getbool('RETRY_ENABLED') \
                or not self.proxy_max_retry_times:
            raise NotConfigured
        self.crawler = crawler
        self.retry_proxies = {}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_request(self, request, spider):
        retries = request.meta.get('retry_times', 0)
        if retries > 0 \
                and 'proxy' in request.meta:
            proxy = request.meta['proxy']
            spider.log('Crawl <%s> timeout with proxy: %s.' % (request.url, proxy))
            count = self.retry_proxies.get(proxy, 0) + 1
            self.retry_proxies[proxy] = count
            if count >= self.proxy_max_retry_times:
                if RandomHttpProxyMiddleware.remove_proxy(proxy):
                    self.crawler.engine.close_spider(spider, 'proxy list empty')
                spider.log('Remove proxy: %s.' % proxy)
                del self.retry_proxies[proxy]


class IgnoreRequestMiddleware(object):
    column_family = 'cf'
    qualifier = '0'

    def __init__(self, settings):
        self.host = settings.get('HBASE_HOST')
        self.port = settings.get('HBASE_PORT')
        self.table = settings.get('HISTORY_TABLE')

        self.hbase = HbaseWrapper(self.host, self.port, self.table)
        column_families = (Hbase.ColumnDescriptor(name=self.column_family, maxVersions=1, timeToLive=86400),)
        self.hbase.create_table_if_not_exists(column_families)

    def _is_crawled(self, row_key):
        columns = (self.hbase.column(self.column_family, self.qualifier),)
        rows = self.hbase.get(row_key, columns)
        return True if len(rows) > 0 else False

    @classmethod
    def from_crawler(cls, crawler):
        s = cls(crawler.settings)
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    def spider_closed(self):
        self.hbase.close()

    def process_request(self, request, spider):
        if request.meta.get('check_crawled', False):
            spider.log('Checking history for <%s>.' % request.url)
            m = hashlib.md5(request.url)
            if 'suffix' in request.meta:
                m.update(request.meta['suffix'])
            if self._is_crawled(m.hexdigest()):
                spider.log('Request crawled: <%s>, ignore it.' % request.url)
                raise IgnoreRequest('request crawled')


class UrlRecordMiddleware(object):
    column_family = 'cf'
    qualifier = '0'

    @classmethod
    def from_crawler(cls, crawler):
        s = cls(crawler.settings)
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    def __init__(self, settings):
        self.host = settings.get('HBASE_HOST')
        self.port = settings.get('HBASE_PORT')
        self.table = settings.get('HISTORY_TABLE')

        self.hbase = HbaseWrapper(self.host, self.port, self.table)
        column_families = (Hbase.ColumnDescriptor(name=self.column_family, maxVersions=1, timeToLive=86400),)
        self.hbase.create_table_if_not_exists(column_families)

    def spider_closed(self):
        self.hbase.close()

    def _record(self, row_key):
        mutations = (self.hbase.mutation(self.column_family, self.qualifier),)
        self.hbase.put(row_key, mutations)

    def process_spider_input(self, response, spider):
        if response.meta.get('check_crawled', False) and 200 <= response.status < 300:
            spider.log('Recording <%s> into request history.' % response.url)
            m = hashlib.md5(response.url)
            if 'suffix' in response.meta:
                m.update(response.meta['suffix'])
            self._record(m.hexdigest())
            return

