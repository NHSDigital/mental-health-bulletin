# Mental Health Bulletin

**_Warning - this repository is a snapshot of a repository internal to NHS England. This means that links to videos and some URLs may not work._**

**Repository owner:** Analytical Services: Community and Mental Health

**Email:** mh.analysis@nhs.net

To contact us raise an issue on Github or via email and we will respond promptly.

## Introduction

This code base is used in the creation of the Mental Health Bulletin annual publication. This repository includes all of the code used to create the CSV which is presented as part of the publication. The publication uses the Mental Health Services Dataset (MHSDS), further information about the dataset can be found at https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set.

The full publication series can be found at https://digital.nhs.uk/data-and-information/publications/statistical/mental-health-bulletin.

Other Mental Health related publications and dashboards can be found at the Mental Health Data Hub: https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/mental-health-data-hub 

## Folder structure

The repository is structured as follows:
```bash
└───mh_bulletin
    │   main.py
    │
    ├───agg
    │       01.chapter_1_agg.py
    │       02.chapter_4_agg.py
    │       03.chapter_5_agg.py
    │       04.chapter_6_agg.py
    │       05.chapter_7_agg.py
    │       06.chapter_9_agg.py
    │       07.chapter_10_agg.py
    │       08.chapter_11_agg.py
    │       09.chapter_12_agg.py
    │       10.chapter_13_agg.py
    │       11.chapter_14_agg.py
    │       12.chapter_15_agg.py
    │       13.chapter_16_agg.py
    │       14.chapter_17_agg.py
    │
    ├───agg_phase_2
    │       01.chapter_1_phase_2.py
    │       02.chapter_4_phase_2.py
    │       03.chapter_5_phase_2.py
    │       04.chapter_7_phase_2.py
    │       05.chapter_9_phase_2.py
    │       06.chapter_10_phase_2.py
    │       07.chapter_13_phase_2.py
    │
    ├───prep
    │       01.generic_prep.py
    │       02.chapter_1_prep.py
    │       03.chapter_1_standardisation.py
    │       04.chapter_4_prep.py
    │       05.chapter_5_prep.py
    │       06.chapter_6_prep.py
    │       07.chapter_7_standardisation.py
    │       08.chapter_9_prep.py
    │       09.chapter_10_prep.py
    │       10.chapter_11_prep.py
    │       11.chapter_12_prep.py
    │       12.chapter_13_prep.py
    │       13.chapter_14_15_prep.py
    │       14.chapter_16_prep.py
    │       15.chapter_17_prep.py
    │       16.dq_prep.py
    │
    └───setup
            create_tables.py
            csv_master.py
            mh_bulletin_assets.py
            mh_bulletin_measure_params.py
            outputs_and_suppression.py
            population.py
```
This repository contains all of the code used to produce the CSV output for the Mental Health Bulletin. As such all metrics and breakdowns are included in this code base. 


## Installation and running
Please note that the code included in this project is designed to be run on Databricks within the NHS England systems. As such some of the code included here may not run on other MHSDS assets. The logic and methods used to produce the metrics included in this codebase remain the same though. 

## Understanding the Mental Health Services Dataset

MHSDS is collected on a monthly basis from providers of secondary mental health services. On average around 210 million rows of data flow into the dataset on a monthly basis. More information on the data quality of the dataset, including the numbers of providers submitting data and the volumes of data flowing to each table can be found in the Data Quality Dashboard: https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/mental-health-data-hub/data-quality/mental-health-services-dataset---data-quality-dashboard 

The MHSDS tables and fields used within the code are all documented within the MHSDS tools and guidance. This guidance can be found here: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance

Within the guidance are three key documents:

1) MHSDS Technical Output Specification - This provides technical details of all of the fields and tables contained within the dataset. It also contains details of the validations applied to specific tables and fields. The specification also includes details of derivations and how they are constructed.
2) MHSDS Data Model - This details all of the tables and fields within the dataset and how they relate to each other.
3) MHSDS User Guidance - This document provides details of all of the tables and fields within the dataset and gives examples of how a user might populate the data in certain scenarios.

Additionally, users might want to consult the Data Dictionary for specific fields within the dataset: https://www.datadictionary.nhs.uk/ 

## Appendix and Notes

In places the notebooks above use some acronyms. The main ones used are as follows:

- MHSDS: Mental Health Services Dataset
- CCG: Clinical Commissioning Group. These were replaced by Sub Integrated Care Boards (ICBs) in July 2022.
- ICB: Integrated Care Board. These came into effect on July 1st 2022. Further information can be found at https://www.kingsfund.org.uk/publications/integrated-care-systems-explained#development.
- Provider: The organisation is providing care. This is also the submitter of MHSDS data
-LA: Local Authority

## Support
If you have any questions or issues regarding the constructions or code within this repository please contact mh.analysis@nhs.net

## Authors and acknowledgment
Community and Mental Health Team, NHS England
mh.analysis@nhs.net

## License
The menh_bbrb codebase is released under the MIT License.
The documentation is © Crown copyright and available under the terms of the [Open Government 3.0] (https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.
