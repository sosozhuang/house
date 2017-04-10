# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import hashlib
from random import Random

from hbase import Hbase
from scrapy.exceptions import IgnoreRequest, CloseSpider, NotConfigured
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
    http_proxies = [
        # 'http://171.36.135.207:8998',
        # 'http://171.37.246.170:8998',
        # 'http://36.249.28.168:808',
        # 'http://222.94.145.40:808',
        # 'http://112.85.104.48:808',
        # 'http://112.85.236.85:808',
        # 'http://112.87.8.244:8998',
        # 'http://221.229.44.50:808',
        # 'http://112.93.150.32:8998',
        # 'http://112.82.170.63:808',
    ]

    failed_proxies = set()
    r = Random()

    @classmethod
    def from_crawler(cls, crawler):
        cls.http_proxies = crawler.settings.get('HTTP_PROXIES', False)
        if not cls.http_proxies:
            raise NotConfigured
        s = cls()
        return s

    @classmethod
    def remove_proxy(cls, proxy, stats=None):
        if proxy in cls.http_proxies:
            cls.http_proxies.remove(proxy)
            cls.failed_proxies.add(proxy)
            if stats:
                stats.set_value('proxy_failed', cls.failed_proxies)
        if len(cls.http_proxies) == 0:
            return 'proxy list empty'

    def process_request(self, request, spider):
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
        self.stats = crawler.stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if 'Location' not in response.headers \
                or response.status not in self.allowed_status:
            return response

        location = safe_url_string(response.headers['Location'])
        if 'captcha' in location:
            if 'proxy' in request.meta:
                spider.log('Request <%s> redirected to captcha using proxy: %s.' % (request.url, request.meta['proxy']))
                if RandomHttpProxyMiddleware.remove_proxy(request.meta['proxy'], self.stats):
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
        self.stats = crawler.stats

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
                if RandomHttpProxyMiddleware.remove_proxy(proxy, self.stats):
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
        return cls(crawler.settings)

    def process_request(self, request, spider):
        if request.meta.get('check_crawled', False):
            spider.log('Checking requested history for <%s>.' % request.url)
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
        return cls(crawler.settings)

    def __init__(self, settings):
        self.host = settings.get('HBASE_HOST')
        self.port = settings.get('HBASE_PORT')
        self.table = settings.get('HISTORY_TABLE')

        self.hbase = HbaseWrapper(self.host, self.port, self.table)
        column_families = (Hbase.ColumnDescriptor(name=self.column_family, maxVersions=1, timeToLive=86400),)
        self.hbase.create_table_if_not_exists(column_families)

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

