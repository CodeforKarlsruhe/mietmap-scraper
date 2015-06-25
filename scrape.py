#!/usr/bin/env python
# vim: set fileencoding=utf-8 :

# Copyright (c) 2015 Code for Karlsruhe (http://codefor.de/karlsruhe)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
Scraper for renting costs in Karlsruhe.
"""

from __future__ import unicode_literals

import cgi
import codecs
import urllib2

from bs4 import BeautifulSoup


# Immobilienscout24 URLs for listings in Karlsruhe
BASE_URL = 'http://www.immobilienscout24.de/Suche/S-T/Wohnung-Miete/Baden-Wuerttemberg/Karlsruhe'
PAGE_URL = 'http://www.immobilienscout24.de/Suche/S-T/P-%d/Wohnung-Miete/Baden-Wuerttemberg/Karlsruhe?pagerReporting=true'


def download_as_unicode(url):
    """
    Download document at URL and return it as a Unicode string.
    """
    request = urllib2.urlopen(url)
    return unicode(request.read(), request.headers.getparam('charset'))


def get_page(number):
    """
    Get a result page.

    The return value is a ``BeautifulSoup`` instance.
    """
    if number == 1:
        url = BASE_URL
    else:
        url = PAGE_URL % number
    data = download_as_unicode(url)
    return BeautifulSoup(data, 'html.parser')


def parse_german_float(s):
    """
    Parse a German float string.

    German uses a dot for the thousands separator and a comma for the
    decimal mark.
    """
    return float(s.replace('.', '').replace(',', '.'))


def extract_listings(soup):
    """
    Extract individual listings from a page.

    Returns a dict that maps listing IDs to listing details.
    """
    listings = {}
    for div in soup.find_all('div', class_='resultlist_entry_data'):
        listing_a = div.find('a', class_='headline-link')
        listing_id = listing_a.get('href').split('/')[-1]
        street_span = div.find('span', class_='street')
        if not street_span:
            continue
        address = unicode(street_span.string)
        for dd in div.find_all('dd', class_='value'):
            content = unicode(dd.string).strip()
            if content.endswith('€'):
                rent = parse_german_float(content.split()[0])
            elif content.endswith('m²'):
                area = parse_german_float(content.split()[0])
        listings[listing_id] = {
            'address': address,
            'rent': rent,
            'area': area,
        }
    return listings


def extract_number_of_pages(soup):
    """
    Extract the number of result pages from a result page.
    """
    pager_span = soup.find('span', class_='smallPager')
    return int(pager_span.string.split()[-1])


if __name__ == '__main__':
    from pprint import pprint
    import sys

    number_of_pages = None
    page_index = 1
    listings = {}
    while (not number_of_pages) or (page_index <= number_of_pages):
        sys.stdout.write('.')
        sys.stdout.flush()
        page = get_page(page_index)
        number_of_pages = number_of_pages or extract_number_of_pages(page)
        listings.update(extract_listings(page))
        page_index += 1
    print
    print pprint(listings)

