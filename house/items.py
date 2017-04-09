# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.org/en/latest/topics/items.html

from scrapy import Field, Item


class SecondhandHouseItem(Item):
    city = Field()
    title = Field()
    room = Field()
    b_year = Field()
    comm = Field()
    id = Field()
    main = Field()
    sub = Field()
    space = Field()
    tags = Field()
    total = Field()
    unit = Field()


class SoldHouseItem(Item):
    city = Field()
    comm = Field()
    info = Field()
    deal = Field()
    total = Field()
    unit = Field()
    hang = Field()
    period = Field()
    room = Field()
    space = Field()
    b_year = Field()
    id = Field()
    main = Field()
    sub = Field()


class NewHouseItem(Item):
    city = Field()
    title = Field()
    p_type = Field()
    price = Field()
    id = Field()
    sub = Field()
    dev = Field()
    open = Field()
    over = Field()
    f_space = Field()
    b_space = Field()
    p_year = Field()

