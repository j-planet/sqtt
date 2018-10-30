from bs4 import BeautifulSoup
import requests
import re
from pprint import pprint
import datetime


# constants
ROOT_URL = 'http://weather.unisys.com'
MONTH_TO_INT = {'JAN': 1, 'FEB': 2, 'MAR':3, 'APR': 4,
                'MAY': 5, 'JUN': 6, 'JUL':7, 'AUG': 8,
                'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}

"""
1. what's the starting page for 2017?
"""

# This is the page containing the data:
# http://weather.unisys.com/hurricanes/search?field_ocean_target_id=All&year[year_val]=All&category=All&type=All&items_per_page=12
# We can simply filter by 2017 at the top of the page. The number of results per
# page can also be altered. So it can be on the first page.
# With all the options set as default values (no filter, 12 results per page), year
# '2017' is starts on the bottom of page 7.


"""
2. extract all links
"""

homepage_urls = ['http://weather.unisys.com/hurricanes/search?field_ocean_target_id=All&year[year_val]=2017&category=All&type=All&sort_bef_combine=field_start_date_value%20DESC&items_per_page=36',
                 'http://weather.unisys.com/hurricanes/search?field_ocean_target_id=All&year[0]=2017-01-01&year[1]=2017-12-31&year[2]=2017&category=All&type=All&sort_bef_combine=field_start_date_value%20DESC&items_per_page=36&sort_by=field_start_date_value&sort_order=DESC&page=1']

regex = re.compile(r'/hurricanes/2017/.*/.*')
page_links = []

def get_links_in_page(url):
    soup = BeautifulSoup(requests.get(url).text)

    for l in soup.find_all('a'):
        p = l.get('href')
        if p:
            yield p

for url in homepage_urls:
    for subpath in get_links_in_page(url):
        if regex.match(subpath):
            page_links.append('%s%s' % (ROOT_URL, subpath))

print('>>> %d links read.' % len(page_links))


"""
3. remove bad links
"""

def get_download_table_link(url):

    # first check whether the table exists
    if requests.get(url).status_code != 200:
        return None

    # then extract the download link
    regex = re.compile(r'/file/.*/download\?token=')

    res = list(filter(lambda l: regex.match(l),
                      get_links_in_page(url)))

    if not res:
        return None

    return ROOT_URL + res[0]

table_links = []

for i, l in enumerate(page_links):
    print('>>> Processing %d/%d links...' % (i+1, len(page_links)))
    tablelink = get_download_table_link(l)

    if not tablelink:
        print('>>> Removing link without table content:', l)

        # We shouldn't do this in practice because it's very slow.
        # But because the list is short here, it's "fine".
        page_links.remove(l)

    else:
        table_links.append(tablelink)

print('>>> Found links to %d tables.' % len(table_links))


"""
4. read text files to memory
"""

raw_data = [requests.get(l).text for l in table_links]

print('>>> Fetched %d tables to memory.' % len(raw_data))


"""
5. convert raw data to a list of dicts
Date | Category | Name | Table
"""


def parse_date_range(s):
    """
    :param s: e.g. 'Date: 27-28 DEC 2017'
    :return: begin date, end date
    """

    date_regex_1 = re.compile(r'Date: ([0-9]+)-([0-9]+) ([A-Z]+) ([0-9]+)')
    date_regex_2 = re.compile(r'Date: ([0-9]+) ([A-Z]+)-([0-9]+) ([A-Z]+) ([0-9]+)')

    parsed = date_regex_1.findall(s)

    if parsed:      # format 1: 'Date: 27-28 DEC 2017'
        d1, d2, month, year = parsed[0]
        m1 = m2 = month
    else:           # format 2: 'Date: 29 NOV-05 DEC 2017'
        d1, m1, d2, m2, year = date_regex_2.findall(s)[0]

    date_begin = datetime.date(year=int(year), month=MONTH_TO_INT[m1], day=int(d1))
    date_end = datetime.date(year=int(year), month=MONTH_TO_INT[m2], day=int(d2))

    return date_begin, date_end



s1 = raw_data[0].split('\n')[0]
s2 = raw_data[3].split('\n')[0]

parse_date_range(s1)
parse_date_range(s2)