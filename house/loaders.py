import re
import sys

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join

reload(sys)
sys.setdefaultencoding("utf-8")


def filter_digit(value):
    m = re.search(r'\d+', value)
    if m:
        return m.group()
    return '0'


def filter_year(value):
    m = re.search(r'\d{4}', value)
    if m:
        return m.group()
    return '1900'


def filter_hang(hang):
    m = re.search(r'\d+\.?\d*', hang)
    if m:
        return m.group()
    return '0'


def filter_space(space):
    return space.strip(u' ,\u33a1\n\u5e73\u7c73\xa0').replace(',', '')


def filter_comm(comm):
    return comm.strip(' ')


def filter_room(room):
    return room.strip(u'\xa0 ')


def filter_tag(tag):
    return tag.lstrip('tag ')


def filter_main_area(main_area):
    return main_area.rstrip('-')


def filter_deal(deal):
    return deal.replace('.', '').replace('-', '')


def encode_field(field):
    return field.encode('utf-8')


class SecondhandHouseLoader(ItemLoader):
    default_input_processor = MapCompose(encode_field)
    default_output_processor = TakeFirst()
    b_year_in = MapCompose(filter_year, encode_field)
    space_in = MapCompose(filter_space, encode_field)
    room_in = MapCompose(filter_room, encode_field)
    comm_in = MapCompose(filter_space, encode_field)
    tags_in = MapCompose(filter_tag, encode_field)
    tags_out = Join(',')
    unit_in = MapCompose(filter_digit)


class SoldHouseLoader(ItemLoader):
    default_input_processor = MapCompose(encode_field)
    default_output_processor = TakeFirst()
    deal_in = MapCompose(filter_deal, encode_field)
    b_year_in = MapCompose(filter_year, encode_field)
    space_in = MapCompose(filter_space, encode_field)
    hang_in = MapCompose(filter_hang, encode_field)
    period_in = MapCompose(filter_digit, encode_field)


class NewHouseLoader(ItemLoader):
    default_input_processor = MapCompose(encode_field)
    default_output_processor = TakeFirst()
    price_in = MapCompose(filter_digit, encode_field)
    f_space_in = MapCompose(filter_space, encode_field)
    b_space_in = MapCompose(filter_space, encode_field)
    p_year_in = MapCompose(filter_digit, encode_field)

