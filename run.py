#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from hdx.facades import logging_kwargs
from hdx.utilities.path import temp_dir

from ucdp import generate_dataset_and_showcase, get_countriesdata, generate_resource_view

logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-dhs'


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    download_url = configuration['download_url']
    with temp_dir('ucdp') as folder:
        with Download() as downloader:
            countriesdata, headers = get_countriesdata(download_url, downloader)
            logger.info('Number of countries: %d' % len(countriesdata))
            for countryname in sorted(countriesdata):
                dataset, showcase = generate_dataset_and_showcase(folder, countryname, countriesdata[countryname], headers)
                if dataset:
                    dataset.update_from_yaml()
                    dataset['notes'] = dataset['notes'].replace('\n', '  \n')  # ensure markdown has line breaks
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                    resource_view = generate_resource_view(dataset)
                    resource_view.create_in_hdx()
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
