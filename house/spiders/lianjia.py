# -*-coding: utf-8 -*-
import datetime

__author__ = 'sosozhuang'

import json
import logging
from urlparse import urljoin

from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.utils.response import get_base_url

from house.items import SecondhandHouseItem, NewHouseItem, SoldHouseItem
from house.loaders import SecondhandHouseLoader, NewHouseLoader, SoldHouseLoader


class LianjiaSpider(Spider):
    name = 'lianjia'
    allowed_domains = ['lianjia.com']
    start_urls = ['http://gz.lianjia.com/sitemap/']
    target_cities = [u'北京', u'广州', u'深圳']
                    # u'厦门', u'杭州', u'成都',
                    # u'武汉', u'重庆', u'南京']

    def __init__(self):
        self.target_navs = {u'二手房': (self.parse_seconhand_house_area, ''),
                            u'新房1': (self.parse_new_house_page, '/loupan')}
        self.crawled_day = datetime.datetime.now().strftime('%y%m%d')

    def parse(self, response):
        sel = Selector(response)
        navs = sel.xpath('//div[@class="city_nav"]/ul/li/a')
        cities = navs.xpath('text()').extract()
        if not cities:
            self.log('Crawled %s cant find any city.' % response.url, logging.WARN)
            return
        links = navs.xpath('@href').extract()
        for city in self.target_cities:
            yield Request(url='http:' + links[cities.index(city)],
                          meta={'city': city, 'dont_redirect': True, 'handle_httpstatus_list': [302]},
                          callback=self.parse_city)

    def parse_city(self, response):
        sel = Selector(response)
        navs = sel.xpath('//header[@class="lianjia-header "]//div[@class="fr nav  "]//a')
        links = navs.xpath('@href').extract()
        texts = navs.xpath('text()').extract()

        for i, j in enumerate(texts):
            if j in self.target_navs:
                yield Request(url=urljoin(links[i], self.target_navs[j][1]),
                              meta={'city': response.meta['city']},
                              callback=self.target_navs[j][0])

    def parse_seconhand_house_area(self, response):
        sel = Selector(response)

        areas = sel.xpath('//div[@data-role="ershoufang"]//a/text()').extract()
        links = sel.xpath('//div[@data-role="ershoufang"]//a/@href').extract()
        base_url = get_base_url(response)

        sold_url = sel.xpath('//div[@class="menu"]//ul[@class="typeList"]/li[2]/a/@href').extract_first()
        yield Request(url=urljoin(base_url, sold_url),
                      meta=response.meta,
                      callback=self.parse_sold_house_area)

        for i, area in enumerate(areas):
            yield Request(url=urljoin(base_url, links[i]),
                          meta={'city': response.meta['city'], 'main_area': area},
                          callback=self.parse_secondhand_house_page)

    def parse_secondhand_house_page(self, response):
        sel = Selector(response)
        # house_links = sel.xpath('//div[@class="title"]/a/@href').extract()
        # if not house_links:
        #     self.log('Crawled page %s cant find any house link.' % response.url, logging.WARN)
        #     return

        titles = sel.xpath('//div[@class="title"]/a/text()').extract()
        house_infos = sel.xpath('//div[@class="address"]/div[@class="houseInfo"]')
        communities = house_infos.xpath('a/text()').extract()
        infos = house_infos.xpath('text()').extract()
        built_years = sel.xpath('//div[@class="flood"]/div[@class="positionInfo"]/text()').extract()
        sub_areas = sel.xpath('//div[@class="flood"]/div[@class="positionInfo"]/a/text()').extract()
        tag_path = sel.xpath('//div[@class="tag"]')
        tags = [tag.xpath('span/@class').extract() for tag in tag_path]
        price_infos = sel.xpath('//div[@class="priceInfo"]')
        totals = price_infos.xpath('div[@class="totalPrice"]/span/text()').extract()
        units = price_infos.xpath('div[@class="unitPrice"]/@data-price').extract()
        ids = price_infos.xpath('div[@class="unitPrice"]/@data-hid').extract()

        attrs = [titles, communities, infos, built_years, sub_areas, tags, totals, units, ids]
        min_length = min(len(i) for i in attrs)
        if min_length <= 0:
            self.log('Crawled %s cant find any house info.' % response.url, logging.WARN)
            return
        for i in range(0, min_length):
            loader = SecondhandHouseLoader(item=SecondhandHouseItem())
            loader.add_value('city', response.meta['city'])
            loader.add_value('title', titles[i])
            room, space = infos[i].strip(' |').split('|')[:2]
            loader.add_value('room', room)
            loader.add_value('b_year', built_years[i])
            loader.add_value('comm', communities[i])
            loader.add_value('id', ids[i])
            loader.add_value('main', response.meta['main_area'])
            loader.add_value('sub', sub_areas[i])
            loader.add_value('space', space)
            # loader.add_value('listed', listed)
            loader.add_value('tags', tags[i])
            loader.add_value('total', totals[i])
            loader.add_value('unit', units[i])
            yield loader.load_item()

        # for link in house_links:
        #     meta = dict(response.meta)
        #     meta['check_crawled'] = True
        #     meta['suffix'] = self.crawled_day
        #     yield Request(url=link,
        #                   meta=meta,
        #                   callback=self.parse_secondhand_house)
        page_box = sel.xpath('//div[@class="page-box house-lst-page-box"]')
        page_url = page_box.xpath('@page-url').extract_first()
        if not page_url:
            self.log('Crawled %s cant find page url' % response.url, logging.WARN)
            return
        page_data = page_box.xpath('@page-data').extract_first()
        if not page_data:
            self.log('Crawled %s cant find page data' % response.url, logging.WARN)
            return
        page = json.loads(page_data)
        cur_page = page['curPage']
        if cur_page == 1:
            base_url = get_base_url(response)
            # sold_url = sel.xpath('//div[@class="menu"]//ul[@class="typeList"]/li[2]/a/@href').extract_first()
            # u = urljoin(base_url, sold_url)
            # yield Request(url=u,
            #               meta=response.meta,
            #               callback=self.parse_sold_house_page)

            meta = dict(response.meta)
            meta['check_crawled'] = True
            meta['suffix'] = self.crawled_day
            for i in range(cur_page + 1, page['totalPage'] + 1):
                u = urljoin(base_url, page_url.replace('{page}', '%d' % i))
                yield Request(url=u,
                              meta=meta,
                              callback=self.parse_secondhand_house_page)

    def parse_sold_house_area(self, response):
        sel = Selector(response)

        areas = sel.xpath('//div[@data-role="ershoufang"]//a/text()').extract()
        links = sel.xpath('//div[@data-role="ershoufang"]//a/@href').extract()
        base_url = get_base_url(response)

        for i, area in enumerate(areas):
            yield Request(url=urljoin(base_url, links[i]),
                          meta={'city': response.meta['city'], 'main_area': area},
                          callback=self.parse_sold_house_page)

    def parse_sold_house_page(self, response):
        sel = Selector(response)

        house_links = sel.xpath('//div[@class="title"]/a/@href').extract()
        house_ids = [link.split('/')[-1].split('.')[0] for link in house_links]
        titles = sel.xpath('//div[@class="title"]/a/text()').extract()
        content = sel.xpath('//ul[@class="listContent"]')
        address = content.xpath('.//div[@class="address"]')
        house_infos = address.xpath('div[@class="houseInfo"]/text()').extract()
        deals = address.xpath('div[@class="dealDate"]/text()').extract()
        totals = address.xpath('div[@class="totalPrice"]/span/text()').extract()
        flood = content.xpath('.//div[@class="flood"]')
        built_years = flood.xpath('div[@class="positionInfo"]/text()').extract()
        units = flood.xpath('div[@class="unitPrice"]/span/text()').extract()
        hangs = content.xpath('.//div[@class="dealCycleeInfo"]/span[@class="dealCycleTxt"]/span[1]/text()').extract()
        periods = content.xpath('.//div[@class="dealCycleeInfo"]/span[@class="dealCycleTxt"]/span[2]/text()').extract()

        attrs = [house_ids, titles, house_infos, deals, totals, built_years, units, hangs, periods]
        min_length = min(len(i) for i in attrs)
        if min_length <= 0:
            self.log('Crawled %s cant find house infos.' % response.url, logging.WARN)
            return
        for i in range(0, min_length):
            loader = SoldHouseLoader(item=SoldHouseItem())
            loader.add_value('city', response.meta['city'])
            loader.add_value('main', response.meta['main_area'])
            community, room, space = titles[i].split()[:3]
            loader.add_value('comm', community)
            loader.add_value('info', house_infos[i])
            loader.add_value('deal', deals[i])
            loader.add_value('total', totals[i])
            loader.add_value('unit', units[i])
            loader.add_value('hang', hangs[i])
            loader.add_value('period', periods[i])
            # loader.add_value('browse', browse)
            loader.add_value('room', room)
            loader.add_value('space', space)
            loader.add_value('b_year', built_years[i])
            loader.add_value('id', house_ids[i])
            yield loader.load_item()

        page_box = sel.xpath('//div[@class="page-box house-lst-page-box"]')
        page_url = page_box.xpath('@page-url').extract_first()
        if not page_url:
            self.log('Crawled %s cant find page url.' % response.url, logging.WARN)
            return
        page_data = page_box.xpath('@page-data').extract_first()
        if not page_data:
            self.log('Crawled %s cant find page data.' % response.url, logging.WARN)
            return
        page = json.loads(page_data)
        cur_page = page['curPage']
        if cur_page == 1:
            base_url = get_base_url(response)
            for i in range(cur_page + 1, page['totalPage'] + 1):
                u = urljoin(base_url, page_url.replace('{page}', '%d' % i))
                meta = dict(response.meta)
                meta['check_crawled'] = True
                meta['suffix'] = self.crawled_day
                yield Request(url=u,
                              meta=meta,
                              callback=self.parse_sold_house_page)

    def parse_new_house_page(self, response):
        sel = Selector(response)
        house_links = sel.xpath(
            '//ul[@id="house-lst"]//div[@class="info-panel"]//a[@data-el="xinfang"]/@href').extract()
        base_url = get_base_url(response)
        for house_link in house_links:
            meta = dict(response.meta)
            meta['check_crawled'] = True
            meta['house_id'] = house_link.strip('/').split('/')[-1]
            yield Request(url=urljoin(base_url, house_link.rstrip('/') + '/xiangqing'),
                          meta=meta,
                          callback=self.parse_new_house)

        page_box = sel.xpath('//div[@class="page-box house-lst-page-box"]')
        page_url = page_box.xpath('@page-url').extract()[0]
        page_data = page_box.xpath('@page-data').extract()[0]
        page = json.loads(page_data)
        cur_page = page['curPage']
        if cur_page == 1:
            for i in range(cur_page + 1, page['totalPage']):
                u = urljoin(base_url, page_url.replace('{page}', '%d' % i))
                yield Request(url=u,
                              meta=response.meta,
                              callback=self.parse_new_house_page)

    def parse_secondhand_house(self, response):
        sel = Selector(response)
        title = sel.xpath('//div[@class="title"]/h1[@class="main"]/text()').extract()
        content = sel.xpath('//div[@class="overview"]/div[@class="content"]')
        room = content.xpath('div[@class="houseInfo"]/div[@class="room"]/div[@class="mainInfo"]/text()').extract()
        built_year = content.xpath('div[@class="houseInfo"]/div[@class="area"]/div[@class="subInfo"]/text()').extract()
        around = content.xpath('div[@class="aroundInfo"]')
        community = around.xpath('div[@class="communityName"]/a[@class="info"]/text()').extract()
        house_id = around.xpath('div[@class="houseRecord"]/span[@class="info"]/text()').extract()
        area = around.xpath('div[@class="areaName"]/span[@class="info"]')
        main_area = area.xpath('a[1]/text()').extract()
        sub_area = area.xpath('a[2]/text()').extract()

        intro_content = sel.xpath('//*[@id="introduction"]/div/div[@class="introContent"]')
        space = intro_content.xpath('div[@class="base"]/div[@class="content"]/ul/li[3]/text()').extract()

        transaction = sel.xpath('//*[@id="introduction"]/div/div/div[2]')
        listed = transaction.xpath('div[2]/ul/li[1]/text()').extract()

        tags = sel.xpath('//div[@class="tags clear"]/div[@class="content"]/a/@class').extract()
        price = content.xpath('div[@class="price "]')
        total_price = price.xpath('span[@class="total"]/text()').extract()
        unit_price = price.xpath('.//span[@class="unitPriceValue"]/text()').extract()

        loader = SecondhandHouseLoader(item=SecondhandHouseItem())
        loader.add_value('city', response.meta['city'])
        # loader.add_value('type', response.meta['type'])
        loader.add_value('title', title)
        loader.add_value('room', room)
        loader.add_value('b_year', built_year)
        loader.add_value('comm', community)
        loader.add_value('id', house_id)
        loader.add_value('main', main_area)
        loader.add_value('sub', sub_area)
        loader.add_value('space', space)
        loader.add_value('listed', listed)
        loader.add_value('tags', tags)
        loader.add_value('total', total_price)
        loader.add_value('unit', unit_price)
        item = loader.load_item()
        # empty = False
        # for i in item:
        #     if len(item[i]) == 0:
        #         self.log('Crawl page %s with [%s] empty' % (response, i), logging.WARN)
        #         empty = True

        return item

    def parse_sold_house(self, response):
        sel = Selector(response)
        community = sel.xpath('//div[@class="house-title"]/div[@class="wrapper"]/text()').extract()
        sold = sel.xpath('//div[@class="house-title"]/div[@class="wrapper"]/span/text()').extract()
        info = sel.xpath('//div[@class="overview"]/div[@class="info fr"]')
        price = info.xpath('div[@class="price"]')
        total_price = price.xpath('span[@class="dealTotalPrice"]/i/text()').extract()
        unit_price = price.xpath('b/text()').extract()
        msg = info.xpath('div[@class="msg"]')
        hang_price = msg.xpath('span[1]/label/text()').extract()
        period = msg.xpath('span[2]/label/text()').extract()
        browse = msg.xpath('span[6]/label/text()').extract()

        base = sel.xpath('//div[@id="introduction"]//div[@class="base"]//div[@class="content"]/ul')
        room = base.xpath('li[1]/text()').extract()
        space = base.xpath('li[3]/text()').extract()
        built_year = base.xpath('li[8]/text()').extract()

        transaction = sel.xpath('//div[@id="introduction"]//div[@class="transaction"]//div[@class="content"]/ul')
        id = transaction.xpath('li[1]/text()').extract()

        loader = SoldHouseLoader(item=SoldHouseItem())
        loader.add_value('city', response.meta['city'])
        loader.add_value('comm', community)
        loader.add_value('sold', sold)
        loader.add_value('total', total_price)
        loader.add_value('unit', unit_price)
        loader.add_value('hang', hang_price)
        loader.add_value('period', period)
        loader.add_value('browse', browse)
        loader.add_value('room', room)
        loader.add_value('space', space)
        loader.add_value('b_year', built_year)
        loader.add_value('id', id)
        item = loader.load_item()

        return item

    def parse_new_house(self, response):
        sel = Selector(response)
        title = sel.xpath('//div[@class="resb-name"]/text()').extract()
        infos = sel.xpath('//div[@class="big-left fl"]')
        base_info = infos.xpath('ul[1]')
        property_type = base_info.xpath('li[1]/span[2]/text()').extract()
        price = base_info.xpath('li[2]/span[2]/span/text()').extract()
        area = base_info.xpath('li[4]/span[2]')
        # main_area = area.xpath('text()').extract()
        sub_area = area.xpath('a/text()').extract()
        developer = base_info.xpath('li[7]/span[2]/text()').extract()

        open_info = infos.xpath('ul[2]')
        open = open_info.xpath('li[2]/span[1]/span[1]/text()').extract()
        handover = open_info.xpath('li[position()>1]/span[3]/span[1]/text()').extract()

        list_info = infos.xpath('ul[3]')
        floor_space = list_info.xpath('li[3]/span[2]/text()').extract()
        built_space = list_info.xpath('li[5]/span[2]/text()').extract()
        property_year = list_info.xpath('li[8]/span[2]/text()').extract()

        loader = NewHouseLoader(item=NewHouseItem())
        loader.add_value('city', response.meta['city'])
        # loader.add_value('type', response.meta['type'])
        loader.add_value('title', title)
        loader.add_value('p_type', property_type)
        loader.add_value('price', price)
        loader.add_value('id', response.meta['house_id'])
        # loader.add_value('main', main_area)
        loader.add_value('sub', sub_area)
        loader.add_value('dev', developer)
        loader.add_value('open', open)
        loader.add_value('over', handover)
        loader.add_value('f_space', floor_space)
        loader.add_value('b_space', built_space)
        loader.add_value('p_year', property_year)
        item = loader.load_item()
        # empty = False
        # for i in item:
        #     if len(item[i]) == 0:
        #         self.log('Crawl page %s with [%s] empty' % (response, i), logging.WARN)
        #         empty = True

        return item


class LianjiaSHSpider(Spider):
    name = 'shanghai'
    allowed_domains = ['lianjia.com']
    start_urls = ['http://sh.lianjia.com/ershoufang']
    city = u'上海'

    def __init__(self):
        self.crawled_day = datetime.datetime.now().strftime('%y%m%d')

    def parse(self, response):
        sel = Selector(response)
        areas = sel.xpath('//div[@class="option-list gio_district"]//div[@class="item-list"]/a/text()').extract()
        links = sel.xpath('//div[@class="option-list gio_district"]//div[@class="item-list"]/a/@href').extract()
        base_url = get_base_url(response)

        sold_url = urljoin(base_url, '/chengjiao')
        yield Request(url=sold_url,
                      callback=self.parse_sold_house_area)

        for i, area in enumerate(areas):
            yield Request(url=urljoin(base_url, links[i]),
                          callback=self.parse_secondhand_house_page)

    def parse_secondhand_house_page(self, response):
        sel = Selector(response)

        house_infos = sel.xpath('//ul[@id="house-lst"]/li/div[@class="info-panel"]')

        titles = house_infos.xpath('h2/a/text()').extract()
        ids = house_infos.xpath('h2/a/@key').extract()

        col_1 = house_infos.xpath('div[@class="col-1"]')
        where = col_1.xpath('div[@class="where"]')
        communities = where.xpath('.//span[@class="nameEllipsis"]/text()').extract()
        rooms = where.xpath('span[1]/text()').extract()
        spaces = where.xpath('span[2]/text()').extract()
        other = col_1.xpath('div[@class="other"]/div[@class="con"]')
        main_areas = other.xpath('a[1]/text()').extract()
        sub_areas = other.xpath('a[2]/text()').extract()
        labels = col_1.xpath('.//div[@class="view-label left"]')
        tags = [label.xpath('span[not(contains(@class,"-ex"))]/@class').extract() for label in labels]

        col_3 = house_infos.xpath('div[@class="col-3"]')
        totals = col_3.xpath('div[@class="price"]/span/text()').extract()
        units = col_3.xpath('div[@class="price-pre"]/text()').extract()

        attrs = [titles, ids, communities, rooms, spaces, main_areas, sub_areas, tags, totals, units]
        min_length = min(len(i) for i in attrs)
        if min_length <= 0:
            self.log('Crawled page %s cant find any house info' % response.url, logging.WARN)
            return
        for i in range(0, min_length):
            loader = SecondhandHouseLoader(item=SecondhandHouseItem())
            loader.add_value('city', self.city)
            loader.add_value('title', titles[i])
            loader.add_value('room', rooms[i])
            # loader.add_value('b_year', None)
            loader.add_value('comm', communities[i])
            loader.add_value('id', ids[i])
            loader.add_value('main', main_areas[i])
            loader.add_value('sub', sub_areas[i])
            loader.add_value('space', spaces[i])
            # loader.add_value('listed', listed)
            loader.add_value('tags', tags[i])
            loader.add_value('total', totals[i])
            loader.add_value('unit', units[i])
            yield loader.load_item()

        page_box = sel.xpath('//div[@class="page-box house-lst-page-box"]')
        cur_page = page_box.xpath('a[@class="on"]/text()').extract_first()
        if cur_page == '1':
            pages = page_box.xpath('a[@gahref="results_totalpage"]/text()').extract_first()
            if not pages:
                pages = page_box.xpath('a[@gahref="last()-1"]/text()').extract_first()
            if not pages:
                self.log('Crawled %s cant find any page.' % response.url)
                return

            pages = int(pages) if isinstance(pages, unicode) and pages.isdigit() else 100

            base_url = get_base_url(response)
            area = base_url.strip('/').split('/')[-1]
            for i in range(2, pages + 1):
                next_page_url = '/ershoufang/%s/d%d' % (area, i)
                # next_page_url = page_box.xpath('a[last()]/@href').extract_first()
                u = urljoin(base_url, next_page_url)
                meta = dict(response.meta)
                meta['check_crawled'] = True
                meta['suffix'] = self.crawled_day
                yield Request(url=u,
                              meta=meta,
                              callback=self.parse_secondhand_house_page)

    def parse_sold_house_area(self, response):
        sel = Selector(response)
        areas = sel.xpath('//div[@class="option-list gio_district"]/a[@class!="on"]/text()').extract()
        links = sel.xpath('//div[@class="option-list gio_district"]/a[@class!="on"]/@href').extract()
        base_url = get_base_url(response)

        for i, area in enumerate(areas):
            yield Request(url=urljoin(base_url, links[i]),
                          callback=self.parse_sold_house_page)

    def parse_sold_house_page(self, response):
        sel = Selector(response)

        house = sel.xpath('//ul[@class="clinch-list"]//div[@class="info-panel clear"]')
        house_ids = house.xpath('h2[@class="clear"]/a/@key').extract()
        titles = house.xpath('h2[@class="clear"]/a/text()').extract()
        col_1 = house.xpath('div[@class="col-1 fl"]')
        main_areas = col_1.xpath('div[@class="other"]/div[@class="con"]/a[1]/text()').extract()
        sub_areas = col_1.xpath('div[@class="other"]/div[@class="con"]/a[2]/text()').extract()
        col_2 = house.xpath('div[@class="col-2 fr"]')
        deals = col_2.xpath('div[@class="dealType"]/div[1]/div[@class="div-cun"]/text()').extract()
        units = col_2.xpath('div[@class="dealType"]/div[2]/div[@class="div-cun"]/text()').extract()
        totals = col_2.xpath('div[@class="dealType"]/div[3]/div[@class="div-cun"]/text()').extract()

        attrs = [house_ids, titles, main_areas, sub_areas, deals, units, totals]
        min_length = min(len(i) for i in attrs)
        if min_length <= 0:
            self.log('Crawled %s cant find any house info.' % response.url, logging.WARN)
            return
        for i in range(0, min_length):
            loader = SoldHouseLoader(item=SoldHouseItem())
            loader.add_value('city', self.city)
            community, room, space = titles[i].split()[:3]
            loader.add_value('comm', community)
            # loader.add_value('info', house_infos[i])
            loader.add_value('deal', deals[i])
            loader.add_value('total', totals[i])
            loader.add_value('unit', units[i])
            loader.add_value('main', main_areas[i])
            loader.add_value('sub', sub_areas[i])
            # loader.add_value('browse', browse)
            loader.add_value('room', room)
            loader.add_value('space', space)
            # loader.add_value('b_year', built_years[i])
            loader.add_value('id', house_ids[i])
            yield loader.load_item()

        page_box = sel.xpath('//div[@class="page-box house-lst-page-box"]')
        cur_page = page_box.xpath('a[@class="on"]/text()').extract_first()
        # if next_page == 'results_next_page':
        # for i in range(2, pages + 1):
        # next_page_url = 'd%d' % i
        # next_page_url = page_box.xpath('a[last()]/@href').extract_first()
        # u = urljoin(base_url, next_page_url)
        if cur_page == '1':
            base_url = get_base_url(response)
            area = base_url.strip('/').split('/')[-1]
            # total = sel.xpath('//div[@class="list-head clear"]/h2/span/text()').extract_first().strip()
            # pages = int(total)/min_length if total.isdigit() else 100
            # if next_page == 'results_next_page':
            # next_page_url = page_box.xpath('a[last()]/@href').extract_first()
            pages = page_box.xpath('a[@gahref="results_totalpage"]/text()').extract_first()
            if not pages:
                pages = page_box.xpath('a[@gahref="last()-1"]/text()').extract_first()
            if not pages:
                self.log('Crawled %s cant find any page.' % response.url)
                return

            pages = int(pages) if isinstance(pages, unicode) and pages.isdigit() else 100
            for i in range(2, pages + 1):
                next_page_url = '/chengjiao/%s/d%d' % (area, i)
                # next_page_url = page_box.xpath('a[last()]/@href').extract_first()
                u = urljoin(base_url, next_page_url)
                meta = dict(response.meta)
                meta['check_crawled'] = True
                meta['suffix'] = self.crawled_day
                yield Request(url=u,
                              meta=meta,
                              callback=self.parse_sold_house_page)

