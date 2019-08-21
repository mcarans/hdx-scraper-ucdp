### Collector for UCDP's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdx-scraper-ucdp.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdx-scraper-ucdp) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-ucdp/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-ucdp?branch=master)

This script connects to the [UCDP website](https://ucdp.uu.se/) and reads from the large download of all data creating a dataset per country in HDX. The scraper takes around half an hour to run. It makes 1 large read (<20Mb)) from UCDP and 1000 read/writes (API calls) to HDX in total. It creates temporary files which will be no larger than 10Mb which it uploads into HDX. It is run when UCDP make changes, in practice this is in the order of once or twice a year. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-ucdp** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY