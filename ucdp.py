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
quickchart_resourceno = 1


def get_countriesdata(download_url, downloader):
    countriesdata = dict()
    headers = list()
    for row in downloader.get_tabular_rows(download_url, dict_rows=True, headers=1):
        headers = deepcopy(downloader.response.headers)
        dict_of_lists_add(countriesdata, row['country'], row)
    return countriesdata, headers


def generate_dataset_and_showcase(folder, countryname, countrydata, headers):
    """
    """
    countryiso, _ = Country.get_iso3_country_code_fuzzy(countryname, exception=ValueError)
    countryname = Country.get_country_name_from_iso3(countryiso)
    title = '%s - Conflict Data' % countryname
    logger.info('Creating dataset: %s' % title)
    slugified_name = slugify('UCDP Data for %s' % countryname).lower()
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    dataset.set_expected_update_frequency('As needed')
    dataset.set_subnational(True)
    dataset.add_country_location(countryiso)
    tags = ['hxl', 'violence and conflict', 'protests', 'security incidents']
    dataset.add_tags(tags)

    earliest_year = 10000
    latest_year = 0
    rows = list()
    qcrows = list()
    # hxlate
    hxlrow = {'year': '#date+year', 'side_a': '#group+name+first', 'side_b': '#group+name+second',
              'source_article': '#meta+source', 'source_headline': '#description', 'where_coordinates': '#loc+name',
              'adm_1': '#adm1+name', 'adm_2': '#adm2+name', 'latitude': '#geo+lat', 'longitude': '#geo+lon',
              'country': '#country+name', 'region': '#region+name', 'date_start': '#date+start',
              'date_end': '#date+end', 'best': '#affected+killed', 'iso3': '#country+code'}
    rows.append(hxlrow)
    hxlrow = OrderedDict([('year', '#date+year'), ('where_coordinates', '#loc+name'),
                          ('adm_1', '#adm1+name'), ('adm_2', '#adm2+name'), ('date_start', '#date+start'),
                          ('date_end', '#date+end'), ('best', '#affected+killed')])
    qcrows.append(hxlrow)
    for row in countrydata:
        date_start = int(row['date_start'][:4])
        date_end = int(row['date_end'][:4])
        if date_start < earliest_year:
            earliest_year = date_start
        if date_end > latest_year:
            latest_year = date_end
        row['iso3'] = countryiso
        rows.append(row)
        qcrow = OrderedDict([('year', row['year']), ('where_coordinates', row['where_coordinates']),
                             ('adm_1', row['adm_1']), ('adm_2', row['adm_2']), ('date_start', row['date_start']),
                             ('date_end', row['date_end']), ('best', row['best'])])
        qcrows.append(qcrow)
    if earliest_year == 10000 or latest_year == 0:
        logger.warning('%s has no data!' % countryname)
        return None, None
    dataset.set_dataset_year_range(earliest_year, latest_year)

    file_type = 'csv'
    resource_data = {
        'name': 'Conflict Data for %s' % countryname,
        'description': 'Conflict data with HXL tags',
    }
    resource = Resource(resource_data)
    resource.set_file_type(file_type)
    file_to_upload = join(folder, 'conflict_data_%s.csv' % countryname)
    write_list_to_csv(rows, file_to_upload, headers=headers)
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)
    resource_data = {
        'name': 'QuickCharts Conflict Data for %s' % countryname,
        'description': 'Conflict data with HXL tags with columns removed',
    }
    resource = Resource(resource_data)
    resource.set_file_type(file_type)
    file_to_upload = join(folder, 'qc_conflict_data_%s.csv' % countryname)
    write_list_to_csv(qcrows, file_to_upload, headers=list(qcrows[0].keys()))
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)
    dataset.set_quickchart_resource(quickchart_resourceno)
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title,
        'notes': 'Conflict Data Dashboard for %s' % countryname,
        'url': 'https://ucdp.uu.se/#country/%s' % countrydata[0]['country_id'],
        'image_url': 'https://pbs.twimg.com/profile_images/832251660718178304/y-LWa5iK_200x200.jpg'
    })
    showcase.add_tags(tags)
    return dataset, showcase


def generate_resource_view(dataset):
    resourceview = ResourceView({'resource_id': dataset.get_resource(quickchart_resourceno)['id']})
    resourceview.update_from_yaml()
    return resourceview
