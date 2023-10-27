# LES ETl pipeline

This repository hosts the ETL that creates the Algoritmica data lakehouse.

The data is located on GCS inside a bucket where raw data from external providers (ex: EDW, Quandl, etc) is safely stored for processing.

The Lakehouse architecture has been chosen to leverage the advantages of the data lake and data warehouse architecture.

## Schema

![lakehouse schema](Lakehouse_v1.png "Algoritmica Lakehouse diagram")

## Main design

By leveraging GCP Dataproc Serverless, the original data from EDW is processed as following:

- **Bronze layer**: one-to-one copy of the raw file, profiled with a lower level of rules and enhanced with new columns to support Slow Changign Dimension Type 2.
- **Silver layer**: normalized data from the previous layer where dimensions are separated and data is prepared for BI queries and/or ML feature preparation.
- **Gold layer**: enhanced data for business index metrics or features ready to be feed into ML model factory.

The **Bronze** and **Silver** layers are manipulated via Dataproc Serverless for Apache Spark. The **Gold** one can be processed differently according to the usage:

- For BI metrics generation (a.k.a. indexes), _Looker Studio_ is used to prepare dashboards and plots.
- For ML feature engineering, _Dataproc Serverless_ or _DataFlow_ is used to elaborate futher the data.

Data profiling rules are applied to the ETL. These are a set of rules that check basic data quality for the EDW raw data after storing the files in the _Bronze layer_. Some examples are:

- primary key columns are unique and complete
- tables are not empty
- columns that should not hold **NULL** values are correct
- minimum set of compulsory columns are not missing.

All of them are available in `src/les_etl_pipeline/utils/validation_rules.py`

## How to run the project

Clone the repository onto a local machine and make sure that `gcloud cli` is installed and prepared to be used. It is required to have credentials set up to access the `dataops-369610` project on GCP.

The `Makefile` should be edited accordingly in case the data to process is allocated to another bucket/folder.

Run the following command to prepare the code and uploa it onto GCP:

```bash
> make setup && make build
```

Upload the desired DAG file onto Google Cloud Composer and start the workflow manually.
