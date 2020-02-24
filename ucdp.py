#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
UCDP:
-----

Generates HXlated API urls from the UCDP website.

"""
import logging
from collections import OrderedDict
from copy import deepcopy
from os.path import join
from urllib.parse import quote_plus

from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.resource_view import ResourceView
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import write_list_to_csv, dict_of_lists_add
from slugify import slugify

logger = logging.getLogger(__name__)
hxltags = {'year': '#date+year', 'start_year': '#date+year+start', 'end_year': '#date+year+end',
           'side_a': '#group+name+first', 'side_b': '#group+name+second', 'source_article': '#meta+source',
           'source_headline': '#description', 'where_coordinates': '#loc+name', 'adm_1': '#adm1+name',
           'adm_2': '#adm2+name', 'latitude': '#geo+lat', 'longitude': '#geo+lon', 'country': '#country+name',
           'iso3': '#country+code', 'region': '#region+name', 'date_start': '#date+start', 'date_end': '#date+end',
           'best': '#affected+killed'}


def get_countriesdata(download_url, downloader):
    countrynameisomapping = dict()
    countriesdata = dict()
    headers, iterator = downloader.get_tabular_rows(download_url, headers=1, dict_form=True)
    countries = list()
    for row in iterator:
        countryname = row['country']
        countryiso = countrynameisomapping.get(countryname)
        if countryiso is None:
            countryiso, _ = Country.get_iso3_country_code_fuzzy(countryname, exception=ValueError)
            countrynameisomapping[countryname] = countryiso
            countries.append({'iso3': countryiso, 'countryname': Country.get_country_name_from_iso3(countryiso), 'origname': countryname})
        row['iso3'] = countryiso
        dict_of_lists_add(countriesdata, countryiso, row)
    headers.insert(30, 'iso3')
    headers.insert(3, 'end_year')
    headers.insert(3, 'start_year')
    return countries, headers, countriesdata


def generate_dataset_and_showcase(folder, country, countrydata, headers):
    """
    """
    countryiso  = country['iso3']
    countryname = country['countryname']
    title = '%s - Conflict Data' % countryname
    logger.info('Creating dataset: %s' % title)
    slugified_name = slugify('UCDP Data for %s' % countryname).lower()
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    dataset.set_expected_update_frequency('Every day')
    dataset.set_subnational(True)
    dataset.add_country_location(countryiso)
    tags = ['hxl', 'violence and conflict', 'protests', 'security incidents']
    dataset.add_tags(tags)

    filename = 'conflict_data_%s.csv' % countryiso
    resourcedata = {
        'name': 'Conflict Data for %s' % countryname,
        'description': 'Conflict data with HXL tags'
    }

    def process_year(years, row):
        start_year = int(row['date_start'][:4])
        end_year = int(row['date_end'][:4])
        years.add(start_year)
        years.add(end_year)
        row['start_year'] = start_year
        row['end_year'] = end_year

    quickcharts = {'cutdown': 2, 'cutdownhashtags': ['#date+year+end', '#adm1+name', '#affected+killed']}
    success, results = dataset.generate_resource_from_download(headers, countrydata, hxltags, folder, filename,
                                                               resourcedata, year_function=process_year,
                                                               quickcharts=quickcharts)
    if success is False:
        logger.warning('%s has no data!' % countryname)
        return None, None

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title,
        'notes': 'Conflict Data Dashboard for %s' % countryname,
        'url': 'https://ucdp.uu.se/#country/%s' % countrydata[0]['country_id'],
        'image_url': 'https://pbs.twimg.com/profile_images/832251660718178304/y-LWa5iK_200x200.jpg'
    })
    showcase.add_tags(tags)
    return dataset, showcase
