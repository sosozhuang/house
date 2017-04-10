# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import datetime

from hbase import Hbase

from house.hbase_wrapper import HbaseWrapper
from house.items import SecondhandHouseItem, NewHouseItem, SoldHouseItem


# reload(sys)
# sys.setdefaultencoding("utf-8")


class HousePipeline(object):
    cf_basic = 'basic'

    def __init__(self, stats, host, port, table):
        self.host = host
        self.port = port
        self.table = table
        self.created_at = datetime.datetime.now().strftime('%y%m%d')
        self.reversed_day = 991231 - int(self.created_at)
        self.hbase = HbaseWrapper(self.host, self.port, self.table)
        self.ctime = self.hbase.mutation(self.cf_basic, 'ctime', self.created_at)
        self.stats = stats
        self.invalid_items = []

    def open_spider(self, spider):
        column_families = (Hbase.ColumnDescriptor(name=self.cf_basic, maxVersions=1),)
        self.hbase.create_table_if_not_exists(column_families)

    def close_spider(self, spider):
        self.hbase.close()
        # if self.transport.isOpen():
        #     # self.transport.flush()
        #     self.transport.close()


class SecondhandHousePipeline(HousePipeline):
    cf_price = 'price'

    def __init__(self, stats, host, port, table):
        super(SecondhandHousePipeline, self).__init__(stats, host, port, table)
        self.ctime = self.hbase.mutation(self.cf_price, 'ctime', self.created_at)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            stats=crawler.stats,
            host=crawler.settings.get('HBASE_HOST', 'localhost'),
            port=crawler.settings.get('HBASE_PORT', 9090),
            table=crawler.settings.get('SECONDHAND_HOUSE_TABLE')
        )

    def open_spider(self, spider):
        column_families = (Hbase.ColumnDescriptor(name=self.cf_basic, maxVersions=1),
                           Hbase.ColumnDescriptor(name=self.cf_price, maxVersions=1, timeToLive=365*24*60*60))
        self.hbase.create_table_if_not_exists(column_families)

    def process_item(self, item, spider):
        if not isinstance(item, SecondhandHouseItem):
            return item
        if not item['id']:
            self.invalid_items.append(item)
            self.stats.set_value('invalid_items', self.invalid_items)
            return item

        mutations = []
        row_key = item['id']
        for qualifier in ('city', 'title', 'room',
                  'comm', 'id', 'main',
                  'sub', 'space', 'tags', 'b_year'):
            value = item.get(qualifier)
            if qualifier:
                m = self.hbase.mutation(self.cf_basic, qualifier, value)
                mutations.append(m)

        # for qualifier in (item['city'], item['title'], item['room'],
        #                   item['comm'], item['id'], item['main'],
        #                   item['sub'], item['space'], item['tags'],
        #                   item['b_year']):
        #     m = self.hbase.mutation(self.cf_basic, qualifier, item[qualifier])
        #     mutations.append(m)
        self.hbase.put(row_key, mutations)

        mutations = [self.ctime]
        row_key = '%s-%d' % (item['id'], self.reversed_day)
        for qualifier in ('total', 'unit'):
            value = item.get(qualifier)
            if qualifier:
                m = self.hbase.mutation(self.cf_price, qualifier, value)
                mutations.append(m)
        self.hbase.put(row_key, mutations)
        return item


class SoldHousePipeline(HousePipeline):
    def __init__(self, stats, host, port, table):
        super(SoldHousePipeline, self).__init__(stats, host, port, table)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            stats=crawler.stats,
            host=crawler.settings.get('HBASE_HOST', 'localhost'),
            port=crawler.settings.get('HBASE_PORT', 9090),
            table=crawler.settings.get('SOLD_HOUSE_TABLE')
        )

    def process_item(self, item, spider):
        if not isinstance(item, SoldHouseItem):
            return item
        if not item['id']:
            self.invalid_items.append(item)
            self.stats.set_value('invalid_items', self.invalid_items)
            return item

        mutations = []
        row_key = '%s-%s' % (item['id'], item['deal'])
        for qualifier in item:
            m = self.hbase.mutation(self.cf_basic, qualifier, item[qualifier])
            mutations.append(m)
        self.hbase.put(row_key, mutations)

        return item


class NewHousePipeline(HousePipeline):
    def __init__(self, stats, host, port, table):
        super(NewHousePipeline, self).__init__(stats, host, port, table)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            stats=crawler.stats,
            host=crawler.settings.get('HBASE_HOST', 'localhost'),
            port=crawler.settings.get('HBASE_PORT', 9090),
            table=crawler.settings.get('NEW_HOUSE_TABLE')
        )

    def process_item(self, item, spider):
        if not isinstance(item, NewHouseItem):
            return item
        if None in item.values():
            self.invalid_items.append(item)
            self.stats.set_value('invalid_items', self.invalid_items)
            return item

        mutations = []
        row_key = '%s-%d' % (item['id'], self.reversed_day)
        for qualifier in item:
            m = self.hbase.mutation(self.column_family, qualifier, item[qualifier])
            mutations.append(m)
        self.hbase.put(row_key, mutations)

        return item

