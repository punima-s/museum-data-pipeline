# Museum exhibition data pipeline

## Files Explained

- `README.md`
  - This is the file you are currently reading
- `.gitignore`
  - This file is used to tell Git what files to ignore for any changes. This can be safely ignored.

- `pipeline` folder
  - consumer.py : extracts and loads stream data from Kakfa topic
  - extract.py : extract batch data from S3
  - pipeline.py : transform and load data into the database in RDS
  - ETL pipeline, both batch-processing and Kakfa, and database schema
  - rest_db.py : connects to RDS to clear database and setup database schema
  - schema.sql
  - test_consumer.py
  - test_extract.py
  - analysis.ipynb

- `terraform` folder : terraform AWS RDS and public access set up
  - main.tf
  - output.tf
  - variables.tf
