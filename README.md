# ETL and Data Warehouse Architecture for Sparkify’s Music Data
## Description:
Mobax, a rapidly growing music streaming startup, has expanded its user base and song database. As part of their growth, they are migrating their data processes and storage to the cloud. Their data, including user activity logs and song metadata, is stored in S3. The goal of this project is to build an ETL (Extract, Transform, Load) pipeline that extracts this data from S3, stages it in AWS Redshift, and transforms it into dimensional tables for their analytics team to derive insights.

### Diagram
![Diagram](https://github.com/Joshh90/ETL-and-Data-Warehouse-Architecture-for-Sparkify-Music-Data/blob/main/datawarehouse.jpeg)

### ETL Pipeline design
#### 1.Extract data from JSON logs on user activity and song metadata stored in S3.
#### 2.Transform the data into a structured format that can be loaded into Redshift.
#### 3.Load the transformed data into Redshift for analytics.
This project will help Mobax's team analyze user behavior, song preferences, and other key metrics to drive their business decisions.

### Project ER Model
![Model Diagram](https://github.com/Joshh90/ETL-and-Data-Warehouse-Architecture-for-Sparkify-Music-Data/blob/main/ETL%20%20%26%20DATAWAREHOUSE%20ERD.drawio.png)

#### Prerequisites
###### Before running the Python scripts, make sure you have the following setup:
1. AWS Account with access to S3 and Redshift.
2. Python 3.x installed.
3. PostgreSQL/Redshift Database credentials.
4. Required Python Libraries (listed in requirements.txt).
5. AWS CLI installed and configured if running locally.
6. Instantiate your redshift cluster using aws console or boto 3


##### How to Run the Python Scripts
1. ###### Set up the environment
Ensure you have all the required libraries installed by running:
```
pip install psycopg2
pip install boto3
pip install pandas
pip install sqlalchemy

```
2. ###### Update the dwh.cfg  file
The dwh.cfg file should contain your AWS and Redshift credentials. 

#### dwh.cfg 

##### AWS Credentials
```
[IAM_ROLE]
ARN='Your role arn'

[S3]
LOG_DATA=s3://udacity-dend/log_data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
SONG_DATA=s3://udacity-dend/song_data
REGION='Region where your s3 bucket is hosted'

```

##### Redshift Credentials
```
HOST = 'your_redshift_cluster_endpoint'
DB_NAME = 'your_db_name'
DB_USER = 'your_db_user'
DB_PASSWORD = 'your_db_password'
DB_PORT = 5439  # default Redshift port

```

3. ###### Run the ETL script:
To run the ETL process, execute the **etl.py** script.

**Etl.py** will:
* Extract data from S3 (logs and song metadata).
* Transform the data to match the schema required by Redshift.
* Load the data into Redshift tables.

#### Explanation of Files
**etl.py:**
This is the main Python script that handles the ETL process. It extracts data from the S3 buckets, transforms it into the appropriate format, and loads it into Redshift. It connects to the Redshift cluster and uses the SQL queries from sql_queries.py to stage and transform the data.

Functions in etl.py include:
* Extract_data_from_s3( ): Extracts raw data from S3 log files and song metadata.
* Transform_data( ): Transforms the extracted data into a structured format suitable for loading into Redshift.
* Load_data_to_redshift( ): Loads the transformed data into the appropriate tables in Redshift.
* Full_table_update( ): Optionally truncates and reloads tables to update data.

**sql_queries.py:**
This file contains the SQL queries used in the ETL pipeline, such as:
* Creating staging and fact tables (e.g., staging_events, songplays, etc.).
* Inserting data into these tables using INSERT INTO queries.
* Selecting and transforming data using SQL to prepare it for analysis.

**dwh.cfg:**
This file contains the configuration for AWS and Redshift credentials:
* AWS access keys to interact with S3.
* Redshift connection details to load data into the database.

**create_table.py:**
Fact and dimension tables are created here for star schema in Redshift.

**data_quality_check.py:** This script runs data quality checks to ensure the ETL process has correctly loaded data into the Redshift tables. It checks for record counts, non-null constraints, and consistency across the tables.

###### Key checks include:

* Checking if the tables have the expected number of records.
* Ensuring that there are no missing or null values in essential columns.
##### How to run:
Run data_quality_check.py after executing the ETL process to ensure data quality.

**validation_check.py:** This script validates the transformed data after it has been loaded into Redshift. It checks the results of key business questions, such as:
Counting records in critical tables (users, songs, artists, etc.).
##### How to run:
Run validation_check.py to validate that the ETL pipeline has correctly transformed and loaded data.

### Data Flow
#### Extract:

* Raw data from S3 logs (log_data), which contains user activity on the app.
* Song metadata from S3 (song_data), which contains JSON files describing songs in the app.
#### Transform:

Extracted logs and song metadata are transformed to fit the schema required by Redshift tables.
##### The transformation process involves:
1.  Parsing JSON data from logs and song metadata.
11. Joining tables to create relationships, such as linking user activity to specific songs.
111. Filtering and cleaning the data.
#### Load:

The transformed data is loaded into Redshift tables, including:
1. song_plays: Facts about each song playback event
11. users: Information about each user.
111. songs: Details of each song.
4. artists: Artist details.
5. time: Time-based dimensions for user activity (e.g., hour, day, week).
### Troubleshooting
1.  Relation does not exist: Ensure the table is created and exists in the schema you are querying.
11. Permission issues: Check IAM role permissions to ensure Redshift can access the S3 buckets.
111. Data type mismatches: Double-check the data types in your Redshift schema to ensure they match the data from the source files.

