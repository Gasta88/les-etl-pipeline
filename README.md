# Loan Data Exploration

This repository is used as a stepping stone towards the implementation of an ETL that will create the Algoritmica data lakehouse.

A small subset of SME data is loaded locally and used to investigate the ESB template (soon to be decomissioned) and understand how to manipulate the data at different levels.

## Schema
TBD

## Main design
By leveraging GCP Dataproc Serverless, the original data from EDW is processed as following:

- **Bronze layer**: one-to-one copy of the raw file, profiled with a lower level of rules and enhanced with new columns to support Slow Changign Dimension Type 2.
- **Silver layer**: normalized data from the previous layer where dimensions are separated and data is prepared for BI queries and/or ML feature preparation.
- **Gold layer**: enhanced data for business index metrics or features ready to be feed into ML model factory.

The **Bronze** and **Silver** layers are manipulated via Dataproc Serverless for Apache Spark. The **Gold** one canbe processed differently according to the usage:

- For BI metrics generation (a.k.a. indexes), *Looker Studio* is used to prepare dashboards and plots.
- For ML feature engineering, *Dataproc Serverless* or *DataFlow* is used to elaborate futher the data.

Two stages of data profiling are applied to the ETL. These are divided into:

- **bronze level profiling**: set of rules that check basic data quality for the EDW raw data before storing the files in the *Bronze layer*. Some examples are: primary key columns are unique and complete, tables are not empty, columns that should not hold **NULL** values are correct and minimum set of compulsory columns are not missing.
- **silver level profiling**: set of rules that check quality of *Silver layer* tables before allowing them to be processed in the *Gold layer*. Depending on the asset class and file type, these rules can be very heterogenous.

## Data assumptions
Most of the assumptions are explored via the notebooks hosted in `experiments`.

For `assets` the primary key column is a combination of the *ed code*
(extracted from the file name) and the column AS3.

For `collateral` the primary colum is a combination of the *ed code* and the column CS1.

For `bond info` the primary colum is a combination of the *ed code* and the column BS1 and BS2.

For `amortisation` the primary colum is a combination of the *ed code* and the column AS3.

## How to run the project
TBD