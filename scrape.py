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

from __future__ import division, unicode_literals

import cgi
import codecs
import contextlib
import errno
import functools
import json
import pickle
import re
import sqlite3
import time
import urllib2

from bs4 import BeautifulSoup
import clusterpolate
from geopy.geocoders import Nominatim
import numpy as np


# Immobilienscout24 URLs for listings in Karlsruhe
BASE_URL = 'http://www.immobilienscout24.de/Suche/S-T/Wohnung-Miete/Baden-Wuerttemberg/Karlsruhe'
PAGE_URL = 'http://www.immobilienscout24.de/Suche/S-T/P-%d/Wohnung-Miete/Baden-Wuerttemberg/Karlsruhe?pagerReporting=true'


@contextlib.contextmanager
def prepare_database(filename):
    """
    Context manager that provides a database.
    """
    db = sqlite3.connect(filename)
    db.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id TEXT PRIMARY KEY,
            street TEXT,
            number TEXT,
            suburb TEXT,
            rent REAL,
            area REAL,
            latitude REAL,
            longitude REAL,
            date DATE DEFAULT CURRENT_TIMESTAMP
        ) WITHOUT ROWID;
    ''')
    try:
        yield db
    finally:
        db.close()


def store_listings(db, listings):
    """
    Store listings in database.

    Listings already contained in the database are ignored.

    Returns the number of listings that were stored.
    """
    cursor = db.cursor()
    tuples = [(x, y['street'], y['number'], y['suburb'], y['rent'],
              y['area']) for x, y in listings.iteritems()]
    sql = '''INSERT OR IGNORE INTO listings (id, street, number, suburb, rent,
             area) VALUES (?, ?, ?, ?, ?, ?);'''
    cursor.executemany(sql, tuples)
    db.commit()
    return cursor.rowcount


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


def parse_address(address):
    """
    Parse an address string into street, house number, and suburb.
    """
    fields = [s.strip() for s in address.split(',')]
    if len(fields) == 2:
        street = None
        number = None
        suburb = fields[0]
    else:
        street, number = fields[0].rsplit(' ', 1)
        street = re.sub(r'([Ss])(trasse|tr.)\Z', r'\1traße', street)
        suburb = fields[1]
    return (street, number, suburb)


def extract_listings(soup):
    """
    Extract individual listings from a page.

    Returns a dict that maps listing IDs to listing details.
    """
    listings = {}
    for div in soup.find_all('div', class_='resultlist_entry_data'):
        for a in div.find_all('a'):
            if a.get('href', '').startswith('/expose/'):
                listing_id = a.get('href').split('/')[-1]
                break
        else:
            # Couldn't find listing's ID
            continue
        street_span = div.find('span', class_='street')
        if not street_span:
            continue
        street, number, suburb = parse_address(unicode(street_span.string))
        for dd in div.find_all('dd', class_='value'):
            content = unicode(dd.string).strip()
            if content.endswith('€'):
                rent = parse_german_float(content.split()[0])
            elif content.endswith('m²'):
                area = parse_german_float(content.split()[0])
        listings[listing_id] = {
            'street': street,
            'number': number,
            'suburb': suburb,
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


def rate_limited(calls=1, seconds=1):
    """
    Decorator for rate limiting function calls.

    Makes sure that the decorated function is executed at most ``calls``
    times in ``seconds`` seconds. Calls to the decorated function which
    exceed this limit are delayed as necessary.
    """
    def decorator(f):
        last_calls = []

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            now = time.time()
            last_calls[:] = [x for x in last_calls if now - x <= seconds]
            if len(last_calls) >= calls:
                if calls == 1:
                    delta = last_calls[-1] + seconds - now
                else:
                    delta = last_calls[1] + seconds - now
                time.sleep(delta)
            last_calls.append(time.time())
            return f(*args, **kwargs)

        return wrapper
    return decorator


def memoize_persistently(filename):
    """
    Persistently memoize a function's return values.

    This decorator memoizes a function's return values persistently
    over multiple runs of the program. The return values are stored
    in the given file using ``pickle``. If the decorated function is
    called again with arguments that it has already been called with
    then the return value is retrieved from the cache and returned
    without calling the function. If the function is called with
    previously unseen arguments then its return value is added to the
    cache and the cache file is updated.

    Both return values and arguments of the function must support the
    pickle protocol. The arguments must also be usable as dictionary
    keys.
    """
    try:
        with open(filename, 'rb') as cache_file:
            cache = pickle.load(cache_file)
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
        cache = {}

    def decorator(f):

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            key = args + tuple(sorted(kwargs.items()))
            try:
                return cache[key]
            except KeyError:
                value = cache[key] = f(*args, **kwargs)
                with open(filename, 'wb') as cache_file:
                    pickle.dump(cache, cache_file)
                return value

        return wrapper
    return decorator


_geolocator = Nominatim()

@memoize_persistently('address_location_cache.pickle')
@rate_limited()
def get_coordinates(address, timeout=5):
    """
    Geolocate an address.

    Returns the latitude and longitude of the given address using
    OpenStreetMap's Nominatim service. If the coordinates of the
    address cannot be found then ``(None, None)`` is returned.

    As per Nominatim's terms of service this function is rate limited
    to at most one call per second.

    ``timeout`` gives the timeout in seconds.
    """
    location = _geolocator.geocode(address, timeout=timeout)
    if not location:
        return None, None
    return location.latitude, location.longitude


if __name__ == '__main__':
    import argparse
    import logging
    import logging.handlers
    import os.path
    import sys

    import matplotlib.cm

    HERE = os.path.abspath(os.path.dirname(__file__))

    DB_FILE = os.path.join(HERE, 'listings.sqlite')
    JSON_FILE = os.path.join(HERE, 'listings.json')
    HEATMAP_FILE = os.path.join(HERE, 'heatmap.png')
    HEATMAP_AREA = ((8.28, -49.08), (8.53, -48.92))
    HEATMAP_SIZE = (250, 160)
    HEATMAP_COLORMAP = matplotlib.cm.rainbow
    HEATMAP_RADIUS = 0.01

    parser = argparse.ArgumentParser(description='Rent scraper')
    parser.add_argument('--database', help='Database file', default=DB_FILE)
    parser.add_argument('--json', help='JSON output file', default=JSON_FILE)
    parser.add_argument('--verbose', '-v', help='Output log to STDOUT',
                        default=False, action='store_true')
    args = parser.parse_args()
    args.database = os.path.abspath(args.database)
    args.json = os.path.abspath(args.json)

    LOG_FILE = os.path.join(HERE, 'scrape.log')
    logger = logging.getLogger()
    formatter = logging.Formatter('[%(asctime)s] <%(levelname)s> %(message)s')
    handler = logging.handlers.TimedRotatingFileHandler(
        LOG_FILE, when='W0', backupCount=4, encoding='utf8')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    if args.verbose:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.info('Started')
    logger.info('Using database "%s"' % args.database)

    def get_new_listings(db):
        num_pages = None
        page_index = 1
        while (not num_pages) or (page_index <= num_pages):
            logger.info("Fetching page %d" % page_index)
            page = get_page(page_index)
            num_pages = num_pages or extract_number_of_pages(page)
            listings = extract_listings(page)
            new_count = store_listings(db, listings)
            logger.info("Extracted %d listings (%d new)" % (len(listings),
                        new_count))
            page_index += 1

    def add_coordinates(db):
        logger.info('Looking up address coordinates (this might take a while)')
        c = db.cursor()
        c.execute('''SELECT id, street, number, suburb FROM listings
                  WHERE latitude ISNULL;''')
        updates = []
        for row in c:
            id, street, number, suburb = row
            candidates = []
            if street:
                if number:
                    candidates.append('%s %s, %s' % (street, number, suburb))
                candidates.append('%s, %s' % (street, suburb))
            candidates.append(suburb)
            coordinates = None
            for candidate in candidates:
                coordinates = get_coordinates(candidate + ', Karlsruhe')
                if coordinates[0]:
                    break
            else:
                coordinates = (-1, -1)
            updates.append((coordinates[0], coordinates[1], id))
        c.executemany('''UPDATE listings SET latitude=?, longitude=? WHERE
                      id=?;''', updates)
        db.commit()
        rowcount = max(0, c.rowcount)
        logger.info('Updated %d listings with coordinates' % rowcount)

    def export_to_json(db, filename):
        logger.info('Exporting data to JSON file "%s"' % filename)
        c = db.cursor()
        c.execute('''SELECT latitude, longitude, area, rent FROM listings
                     WHERE (latitude NOT NULL) AND (number NOT NULL);''')
        data = [(round(row[0], 5), round(row[1], 5), round(row[3] / row[2], 1))
                for row in c]
        with codecs.open(filename, 'w', encoding='utf8') as f:
            json.dump(data, f, separators=(',', ':'))

    def create_heatmap(db, filename):
        logger.info('Creating heatmap "%s"' % filename)
        c = db.cursor()
        c.execute('''SELECT latitude, longitude, area, rent FROM listings
                     WHERE (latitude NOT NULL) AND (number NOT NULL);''')
        points = []
        values = []
        for row in c:
            points.append((row[1], -row[0]))
            values.append(row[3] / row[2])

        # Trim values
        points = np.array(points)
        values = np.array(values)
        min_value = values.min()
        max_value = values.max()
        spread = max_value - min_value
        trim = 0.01 * spread
        upper_limit = max_value - trim
        lower_limit = min_value + trim
        keep = (values < upper_limit) & (values > lower_limit)
        points = points[keep, :]
        values = values[keep]

        img = clusterpolate.image(points, values, size=HEATMAP_SIZE,
                                  area=HEATMAP_AREA, radius=HEATMAP_RADIUS,
                                  colormap=HEATMAP_COLORMAP)[3]
        img.save(filename)

    try:
        with prepare_database(args.database) as db:
            get_new_listings(db)
            add_coordinates(db)
            export_to_json(db, args.json)
            create_heatmap(db, HEATMAP_FILE)
    except Exception as e:
        logger.exception(e)

    logger.info('Finished')

