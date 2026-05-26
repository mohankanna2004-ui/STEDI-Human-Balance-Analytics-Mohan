# STEDI-Human-Balance-Analytics-Mohan


## Project Overview

This project builds a data lakehouse solution for STEDI using AWS services such as AWS Glue, Amazon S3, Athena, Python, and Spark.

The objective of this project is to ingest raw JSON data into a Landing Zone, process and filter trusted customer data into a Trusted Zone, and finally generate curated datasets for machine learning analysis in the Curated Zone.

The project follows a medallion architecture:

- Landing Zone
- Trusted Zone
- Curated Zone

---

# Technologies Used

- AWS Glue Studio
- AWS Glue Data Catalog
- Amazon S3
- Amazon Athena
- PySpark
- Python
- SQL

---

# Project Architecture

Landing Zone → Trusted Zone → Curated Zone

The pipeline processes three datasets:

1. Customer Data
2. Accelerometer Data
3. Step Trainer Data

---

# Datasets Used

## Customer Data
Contains customer information and consent details.

Fields:
- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

---

## Accelerometer Data
Contains mobile accelerometer readings.

Fields:
- user
- timestamp
- x
- y
- z

---

## Step Trainer Data
Contains IoT step trainer readings.

Fields:
- sensorreadingtime
- serialnumber
- distancefromobject

---

# AWS S3 Structure

```text
customer_landing/
accelerometer_landing/
step_trainer_landing/

customer_trusted/
accelerometer_trusted/
step_trainer_trusted

customer_curated/
machine_learning_curated/
