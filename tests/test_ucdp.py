#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for UCDP

"""
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from ucdp import generate_dataset_and_showcase, get_countriesdata


class TestUCDP():
    dataset = {'name': 'ucdp-data-for-bangladesh', 'title': 'Bangladesh - Conflict Data',
               'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'owner_org': 'hdx', 'data_update_frequency': '-2',
               'subnational': '1', 'groups': [{'name': 'bgd'}],
               'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'violence and conflict', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'protests', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'security incidents', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
               'dataset_date': '01/01/1989-12/31/2018'}

    resources = [{'name': 'Conflict Data for Bangladesh', 'description': 'Conflict data with HXL tags', 'format': 'csv',
                  'resource_type': 'file.upload', 'url_type': 'upload'},
                 {'name': 'QuickCharts-Conflict Data for Bangladesh',
                  'description': 'Cut down data for QuickCharts', 'format': 'csv',
                  'resource_type': 'file.upload', 'url_type': 'upload'}]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(user_agent='test', hdx_key='12345',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'bgd', 'title': 'Bangladesh'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'violence and conflict'}, {'name': 'protests'}, {'name': 'security incidents'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

    @pytest.fixture(scope='class')
    def data(self):
        download_url = 'tests/fixtures/download.zip'
        return get_countriesdata(download_url, Download(user_agent='test'))

    def test_get_countriesdata(self, data):
        countries, headers, countriesdata = data
        assert headers == ['id', 'year', 'active_year', 'start_year', 'end_year', 'type_of_violence', 'conflict_new_id', 'conflict_name', 'dyad_new_id', 'dyad_name', 'side_a_new_id', 'gwnoa', 'side_a', 'side_b_new_id', 'gwnob', 'side_b', 'number_of_sources', 'source_article', 'source_office', 'source_date', 'source_headline', 'source_original', 'where_prec', 'where_coordinates', 'adm_1', 'adm_2', 'latitude', 'longitude', 'geom_wkt', 'priogrid_gid', 'country', 'country_id', 'iso3', 'region', 'event_clarity', 'date_prec', 'date_start', 'date_end', 'deaths_a', 'deaths_b', 'deaths_civilians', 'deaths_unknown', 'low', 'best', 'high']
        assert len(countriesdata) == 2
        assert countriesdata['BGD'][50] == {'id': '73656', 'year': '1990', 'active_year': '1', 'type_of_violence': '1', 'conflict_new_id': '322', 'conflict_name': 'Bangladesh: Chittagong Hill Tracts', 'dyad_new_id': '705', 'dyad_name': 'Government of Bangladesh - JSS/SB', 'side_a_new_id': '143', 'gwnoa': '771', 'side_a': 'Government of Bangladesh', 'side_b_new_id': '285', 'gwnob': '', 'side_b': 'JSS/SB', 'number_of_sources': '-1', 'source_article': 'Reuters (11 May 1990):  "SOLDIERS KILL REBEL COMMANDER, ASSOCIATE IN JUNGLE FIGHT".', 'source_office': '', 'source_date': '', 'source_headline': '', 'source_original': 'military sources', 'where_prec': '4', 'where_coordinates': 'Chittagong Division', 'adm_1': 'Chittagong Division', 'adm_2': '', 'latitude': '22.916667', 'longitude': '91.5', 'geom_wkt': 'POINT (91.500000 22.916667)', 'priogrid_gid': '162544', 'country': 'Bangladesh', 'country_id': '771', 'iso3': 'BGD', 'region': 'Asia', 'event_clarity': '1', 'date_prec': '1', 'date_start': '1990-05-10', 'date_end': '1990-05-10', 'deaths_a': '0', 'deaths_b': '2', 'deaths_civilians': '0', 'deaths_unknown': '0', 'low': '2', 'best': '2', 'high': '2'}
        assert countries == [{'countryname': 'Myanmar', 'iso3': 'MMR', 'origname': 'Myanmar (Burma)'},
                             {'countryname': 'Bangladesh', 'iso3': 'BGD', 'origname': 'Bangladesh'}]

    def test_generate_dataset_and_showcase(self, configuration, data):
        with temp_dir('ucdp') as folder:
            countries, headers, countriesdata = data
            dataset, showcase = generate_dataset_and_showcase(folder, countries[1], countriesdata['BGD'], headers)
            assert dataset == TestUCDP.dataset

            resources = dataset.get_resources()
            assert resources == TestUCDP.resources
            file = 'conflict_data_BGD.csv'
            assert_files_same(join('tests', 'fixtures', file), join(folder, file))
            file = 'qc_conflict_data_BGD.csv'
            assert_files_same(join('tests', 'fixtures', file), join(folder, file))

            assert showcase == {'name': 'ucdp-data-for-bangladesh-showcase', 'title': 'Bangladesh - Conflict Data',
                                'notes': 'Conflict Data Dashboard for Bangladesh',
                                'url': 'https://ucdp.uu.se/#country/771',
                                'image_url': 'https://pbs.twimg.com/profile_images/832251660718178304/y-LWa5iK_200x200.jpg',
                                'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'violence and conflict', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'protests', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                                         {'name': 'security incidents', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}
